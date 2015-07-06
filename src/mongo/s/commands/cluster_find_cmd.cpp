/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include <boost/optional.hpp>

#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/cursor_responses.h"
#include "mongo/s/cluster_explain.h"
#include "mongo/s/query/cluster_find.h"
#include "mongo/s/strategy.h"
#include "mongo/util/timer.h"

namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::vector;

/**
 * Implements the find command on mongos.
 */
class ClusterFindCmd : public Command {
    MONGO_DISALLOW_COPYING(ClusterFindCmd);

public:
    ClusterFindCmd() : Command("find") {}

    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }

    virtual bool slaveOk() const {
        return false;
    }

    virtual bool slaveOverrideOk() const {
        return true;
    }

    virtual bool maintenanceOk() const {
        return false;
    }

    virtual bool adminOnly() const {
        return false;
    }

    virtual void help(std::stringstream& help) const {
        help << "query for documents";
    }

    /**
     * In order to run the find command, you must be authorized for the "find" action
     * type on the collection.
     */
    virtual Status checkAuthForCommand(ClientBasic* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) {
        AuthorizationSession* authzSession = AuthorizationSession::get(client);
        ResourcePattern pattern = parseResourcePattern(dbname, cmdObj);

        if (authzSession->isAuthorizedForActionsOnResource(pattern, ActionType::find)) {
            return Status::OK();
        }

        return Status(ErrorCodes::Unauthorized, "unauthorized");
    }

    virtual Status explain(OperationContext* txn,
                           const std::string& dbname,
                           const BSONObj& cmdObj,
                           ExplainCommon::Verbosity verbosity,
                           BSONObjBuilder* out) const {
        const string fullns = parseNs(dbname, cmdObj);
        const NamespaceString nss(fullns);
        if (!nss.isValid()) {
            return {ErrorCodes::InvalidNamespace,
                    str::stream() << "Invalid collection name: " << nss.ns()};
        }

        // Parse the command BSON to a LiteParsedQuery.
        bool isExplain = true;
        auto lpqStatus = LiteParsedQuery::makeFromFindCommand(std::move(nss), cmdObj, isExplain);
        if (!lpqStatus.isOK()) {
            return lpqStatus.getStatus();
        }

        auto& lpq = lpqStatus.getValue();

        BSONObjBuilder explainCmdBob;
        ClusterExplain::wrapAsExplain(cmdObj, verbosity, &explainCmdBob);

        // We will time how long it takes to run the commands on the shards.
        Timer timer;

        vector<Strategy::CommandResult> shardResults;
        Strategy::commandOp(dbname,
                            explainCmdBob.obj(),
                            lpq->getOptions(),
                            fullns,
                            lpq->getFilter(),
                            &shardResults);

        long long millisElapsed = timer.millis();

        const char* mongosStageName = ClusterExplain::getStageNameForReadOp(shardResults, cmdObj);

        return ClusterExplain::buildExplainResult(
            shardResults, mongosStageName, millisElapsed, out);
    }

    /**
     * TODO:
     *  --fix op counters
     *  --what to do with curop?
     */
    virtual bool run(OperationContext* txn,
                     const std::string& dbname,
                     BSONObj& cmdObj,
                     int options,
                     std::string& errmsg,
                     BSONObjBuilder& result) {
        const std::string fullns = parseNs(dbname, cmdObj);
        const NamespaceString nss(fullns);
        if (!nss.isValid()) {
            return appendCommandStatus(result,
                                       {ErrorCodes::InvalidNamespace,
                                        str::stream() << "Invalid collection name: " << nss.ns()});
        }

        const bool isExplain = false;
        auto statusWithLpq = LiteParsedQuery::makeFromFindCommand(nss, cmdObj, isExplain);
        if (!statusWithLpq.isOK()) {
            return appendCommandStatus(result, statusWithLpq.getStatus());
        }

        auto statusWithCQ = CanonicalQuery::canonicalize(statusWithLpq.getValue().release());
        if (!statusWithCQ.isOK()) {
            return appendCommandStatus(result, statusWithCQ.getStatus());
        }
        std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        // TODO: have to settle on how to pass read pref.
        ReadPreferenceSetting readPref(ReadPreference::PrimaryOnly, TagSet::primaryOnly());
        BSONElement readPrefElt = cmdObj["$readPreference"];
        if (!readPrefElt.eoo()) {
            if (readPrefElt.type() != BSONType::Object) {
                return appendCommandStatus(
                    result,
                    {ErrorCodes::TypeMismatch,
                     str::stream() << "read preference must be a nested object in : " << cmdObj});
            }

            auto statusWithReadPref = ReadPreferenceSetting::fromBSON(readPrefElt.Obj());
            if (!statusWithReadPref.isOK()) {
                return appendCommandStatus(result, statusWithReadPref.getStatus());
            }
            readPref = statusWithReadPref.getValue();
        }

        std::vector<BSONObj> batch;
        auto statusWithCursorId = ClusterFind::runQuery(txn, *cq, readPref, &batch);
        if (!statusWithCursorId.isOK()) {
            return appendCommandStatus(result, statusWithCursorId.getStatus());
        }

        BSONArrayBuilder arr;
        for (const auto& obj : batch) {
            arr.append(obj);
        }
        appendCursorResponseObject(statusWithCursorId.getValue(), nss.ns(), arr.arr(), &result);
        return true;
    }

} cmdFindCluster;

}  // namespace
}  // namespace mongo
