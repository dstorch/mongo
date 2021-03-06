/**
 *    Copyright (C) 2013 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/s/write_ops/batch_downconvert.h"

#include "mongo/util/assert_util.h"

namespace mongo {

    Status BatchSafeWriter::extractGLEErrors( const BSONObj& gleResponse, GLEErrors* errors ) {

        // DRAGONS
        // Parsing GLE responses is incredibly finicky.
        // The order of testing here is extremely important.

        const bool isOK = gleResponse["ok"].trueValue();
        const string err = gleResponse["err"].str();
        const string errMsg = gleResponse["errmsg"].str();
        const string wNote = gleResponse["wnote"].str();
        const string jNote = gleResponse["jnote"].str();
        const int code = gleResponse["code"].numberInt();
        const bool timeout = gleResponse["wtimeout"].trueValue();

        if ( err == "norepl" || err == "noreplset" ) {
            // Know this is legacy gle and the repl not enforced - write concern error in 2.4
            errors->wcError.reset( new WCErrorDetail );
            errors->wcError->setErrCode( ErrorCodes::WriteConcernFailed );
            if ( !errMsg.empty() ) {
                errors->wcError->setErrMessage( errMsg );
            }
            else if ( !wNote.empty() ) {
                errors->wcError->setErrMessage( wNote );
            }
            else {
                errors->wcError->setErrMessage( err );
            }
        }
        else if ( timeout ) {
            // Know there was no write error
            errors->wcError.reset( new WCErrorDetail );
            errors->wcError->setErrCode( ErrorCodes::WriteConcernFailed );
            if ( !errMsg.empty() ) {
                errors->wcError->setErrMessage( errMsg );
            }
            else {
                errors->wcError->setErrMessage( err );
            }
            errors->wcError->setErrInfo( BSON( "wtimeout" << true ) );
        }
        else if ( code == 10990 /* no longer primary */
                  || code == 16805 /* replicatedToNum no longer primary */
                  || code == 14830 /* gle wmode changed / invalid */) {
            // Write concern errors that get returned as regular errors (result may not be ok: 1.0)
            errors->wcError.reset( new WCErrorDetail );
            errors->wcError->setErrCode( code );
            errors->wcError->setErrMessage( errMsg );
        }
        else if ( !isOK ) {

            //
            // !!! SOME GLE ERROR OCCURRED, UNKNOWN WRITE RESULT !!!
            //

            return Status( DBException::convertExceptionCode(
                               code ? code : ErrorCodes::UnknownError ),
                           errMsg );
        }
        else if ( !err.empty() ) {
            // Write error
            errors->writeError.reset( new WriteErrorDetail );
            errors->writeError->setErrCode( code == 0 ? ErrorCodes::UnknownError : code );
            errors->writeError->setErrMessage( err );
        }
        else if ( !jNote.empty() ) {
            // Know this is legacy gle and the journaling not enforced - write concern error in 2.4
            errors->wcError.reset( new WCErrorDetail );
            errors->wcError->setErrCode( ErrorCodes::WriteConcernFailed );
            errors->wcError->setErrMessage( jNote );
        }

        // See if we had a version error reported as a writeback id - this is the only kind of
        // write error where the write concern may still be enforced.
        // The actual version that was stale is lost in the writeback itself.
        const int opsSinceWriteback = gleResponse["writebackSince"].numberInt();
        const bool hadWriteback = !gleResponse["writeback"].eoo();

        if ( hadWriteback && opsSinceWriteback == 0 ) {

            // We shouldn't have a previous write error
            dassert( !errors->writeError.get() );
            if ( errors->writeError.get() ) {
                // Somehow there was a write error *and* a writeback from the last write
                warning() << "both a write error and a writeback were reported "
                          << "when processing a legacy write: " << errors->writeError->toBSON()
                          << endl;
            }

            errors->writeError.reset( new WriteErrorDetail );
            errors->writeError->setErrCode( ErrorCodes::StaleShardVersion );
            errors->writeError->setErrInfo( BSON( "downconvert" << true ) ); // For debugging
            errors->writeError->setErrMessage( "shard version was stale" );
        }

        return Status::OK();
    }

    void BatchSafeWriter::extractGLEStats( const BSONObj& gleResponse, GLEStats* stats ) {
        stats->n = gleResponse["n"].numberInt();
        if ( !gleResponse["upserted"].eoo() ) {
            stats->upsertedId = gleResponse["upserted"].wrap( "upserted" );
        }
        if ( gleResponse["lastOp"].type() == Timestamp ) {
            stats->lastOp = gleResponse["lastOp"]._opTime();
        }
    }

    static BSONObj fixWCForConfig( const BSONObj& writeConcern ) {
        BSONObjBuilder fixedB;
        BSONObjIterator it( writeConcern );
        while ( it.more() ) {
            BSONElement el = it.next();
            if ( StringData( el.fieldName() ).compare( "w" ) != 0 ) {
                fixedB.append( el );
            }
        }
        return fixedB.obj();
    }

    void BatchSafeWriter::safeWriteBatch( DBClientBase* conn,
                                          const BatchedCommandRequest& request,
                                          BatchedCommandResponse* response ) {

        const NamespaceString nss( request.getNS() );

        // N starts at zero, and we add to it for each item
        response->setN( 0 );

        for ( size_t i = 0; i < request.sizeWriteOps(); ++i ) {

            // Break on first error if we're ordered
            if ( request.getOrdered() && response->isErrDetailsSet() )
                break;

            BatchItemRef itemRef( &request, static_cast<int>( i ) );
            bool isLastItem = ( i == request.sizeWriteOps() - 1 );

            BSONObj writeConcern;
            if ( isLastItem && request.isWriteConcernSet() ) {
                writeConcern = request.getWriteConcern();
                // Pre-2.4.2 mongods react badly to 'w' being set on config servers
                if ( nss.db() == "config" )
                    writeConcern = fixWCForConfig( writeConcern );
            }

            BSONObj gleResult;
            GLEErrors errors;
            Status status = _safeWriter->safeWrite( conn, itemRef, writeConcern, &gleResult );
            if ( status.isOK() ) {
                status = extractGLEErrors( gleResult, &errors );
            }

            if ( !status.isOK() ) {
                response->clear();
                response->setErrCode( status.code() );
                response->setErrMessage( status.reason() );
                return;
            }

            //
            // STATS HANDLING
            //

            GLEStats stats;
            extractGLEStats( gleResult, &stats );

            // Special case for making legacy "n" field result for insert match the write
            // command result.
            if ( request.getBatchType() == BatchedCommandRequest::BatchType_Insert
                 && !errors.writeError.get() ) {
                // n is always 0 for legacy inserts.
                dassert( stats.n == 0 );
                stats.n = 1;
            }

            response->setN( response->getN() + stats.n );

            if ( !stats.upsertedId.isEmpty() ) {
                BatchedUpsertDetail* upsertedId = new BatchedUpsertDetail;
                upsertedId->setIndex( i );
                upsertedId->setUpsertedID( stats.upsertedId );
                response->addToUpsertDetails( upsertedId );
            }

            response->setLastOp( stats.lastOp );

            //
            // WRITE ERROR HANDLING
            //

            // If any error occurs (except stale config) the previous GLE was not enforced
            bool enforcedWC = !errors.writeError.get()
                              || errors.writeError->getErrCode() == ErrorCodes::StaleShardVersion;

            // Save write error
            if ( errors.writeError.get() ) {
                errors.writeError->setIndex( i );
                response->addToErrDetails( errors.writeError.release() );
            }

            //
            // WRITE CONCERN ERROR HANDLING
            //

            // The last write is weird, since we enforce write concern and check the error through
            // the same GLE if possible.  If the last GLE was an error, the write concern may not
            // have been enforced in that same GLE, so we need to send another after resetting the
            // error.
            if ( isLastItem ) {

                // Try to enforce the write concern if everything succeeded (unordered or ordered)
                // OR if something succeeded and we're unordered.
                bool needToEnforceWC =
                    !response->isErrDetailsSet()
                    || ( !request.getOrdered()
                         && response->sizeErrDetails() < request.sizeWriteOps() );

                if ( !enforcedWC && needToEnforceWC ) {
                    dassert( !errors.writeError.get() ); // emptied above

                    // Might have gotten a write concern validity error earlier, these are
                    // enforced even if the wc isn't applied, so we ignore.
                    errors.wcError.reset();

                    Status status = _safeWriter->enforceWriteConcern( conn,
                                                                      nss.db().toString(),
                                                                      writeConcern,
                                                                      &gleResult );

                    if ( status.isOK() ) {
                        status = extractGLEErrors( gleResult, &errors );
                    }

                    if ( !status.isOK() ) {
                        response->clear();
                        response->setErrCode( status.code() );
                        response->setErrMessage( status.reason() );
                        return;
                    }
                }
                // END Write concern retry

                if ( errors.wcError.get() ) {
                    response->setWriteConcernError( errors.wcError.release() );
                }
            }
        }

        response->setOk( true );
        dassert( response->isValid( NULL ) );
    }
}
