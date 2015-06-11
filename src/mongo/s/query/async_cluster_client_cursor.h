/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#pragma once

#include <boost/optional.hpp>
#include <queue>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/s/query/cluster_client_cursor_params.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/stdx/mutex.h"

namespace mongo {

    /**
     * AsyncClusterClientCursor is used to generate results from cursor-generating commands on one
     * or more remote hosts. A cursor-generating command (e.g. the find command) is one that
     * establishes a ClientCursor and a matching cursor id on the remote host. In order to retrieve
     * all command results, getMores must be issued against each of the remote cursors until they
     * are exhausted.
     *
     * The ACCC offers a non-blocking interface: if no results are immediately available on this
     * host for retrieval, calling nextEvent() schedules work on the remote hosts in order to
     * generate further results. The event is signalled when further results are available.
     *
     * Work on remote nodes is accomplished by scheduling remote work in TaskExecutor's event loop.
     *
     * Task-scheduling behavior differs depending on whether there is a sort. If the result
     * documents must be sorted, we pass the sort through to the remote nodes and then merge the
     * sorted streams. This requires waiting until we have a response from every remote before
     * returning results. Without a sort, we are ready to return results as soon as we have *any*
     * response from a remote.
     *
     * Does not throw exceptions.
     */
    class AsyncClusterClientCursor {
    public:
        /**
         * Construct a new AsyncClusterClientCursor. The TaskExecutor* and
         * ClusterClientCursorParams& must remain valid for the lifetime of the ACCC.
         */
        AsyncClusterClientCursor(executor::TaskExecutor* executor,
                                 const ClusterClientCursorParams& params,
                                 const std::vector<HostAndPort>& remotes);

        /**
         * Returns true if there is no need to schedule remote work in order to take the next
         * action. This means that either
         *   --there is a buffered result which we can return,
         *   --or all of the remote cursors have been closed and we are done,
         *   --or an error was received and the next call to nextReady() will return an error
         *   status.
         *
         * A return value of true indicates that it is safe to call nextReady().
         */
        bool ready();

        /**
         * If there is a result available that has already been retrieved from a remote node and
         * buffered, then return it along with an ok status.
         *
         * If we have reached the end of the stream of results, returns boost::none along with an ok
         * status.
         *
         * If there has been an error received from one of the shards, or there is an error in
         * processing results from a shard, then a non-ok status is returned.
         *
         * Invalid to call unless ready() has returned true (i.e., invalid to call if getting the
         * next result requires scheduling remote work).
         */
        StatusWith<boost::optional<BSONObj>> nextReady();

        /**
         * Schedules remote work as required in order to make further results available. If there is
         * an error in scheduling this work, returns a non-ok status. On success, returns an event
         * handle. The caller can pass this event handle to 'executor' in order to be blocked until
         * further results are available.
         *
         * Invalid to call unless ready() has returned false (i.e. invalid to call if the next
         * result is available without scheduling remote work).
         */
        StatusWith<executor::TaskExecutor::EventHandle> nextEvent();

    private:
        /**
         * We instantiate one of these per remote host. It contains the buffer of results we've
         * retrieved from the host but not yet returned, as well as the cursor id, and any error
         * reported from the remote.
         */
        struct RemoteCursorData {

            RemoteCursorData(const HostAndPort& host);

            /**
             * Returns whether there is another buffered result available for this remote node.
             */
            bool hasNext() const;

            /**
             * Returns whether the remote has given us all of its results (i.e. whether it has
             * closed its cursor).
             */
            bool exhausted() const;

            HostAndPort hostAndPort;
            boost::optional<CursorId> cursorId;
            std::queue<BSONObj> docBuffer;
            repl::ReplicationExecutor::CallbackHandle cbHandle;
            Status status = Status::OK();
        };

        //
        // Helpers for ready().
        //

        bool ready_inlock();
        bool readySorted_inlock();
        bool readyUnsorted_inlock();

        //
        // Helpers for nextReady().
        //

        boost::optional<BSONObj> nextReadySorted();
        boost::optional<BSONObj> nextReadyUnsorted();

        /**
         * When nextEvent() schedules remote work, it passes this method as a callback. The
         * TaskExecutor will call this function, passing the response from the remote.
         *
         * 'remoteIndex' is the position of the relevant remote node in '_remotes', and therefore
         * indicates which node the response came from and where the new result documents should be
         * buffered.
         */
        void handleRemoteCommandResponse(
                const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData,
                size_t remoteIndex);

        /**
         * If there is a valid unsignalled event that has been requested via nextReady(), signals
         * that event.
         */
        void signalCurrentEvent_inlock();

        // Not owned here.
        executor::TaskExecutor* _executor;

        const ClusterClientCursorParams& _params;

        // Must be acquired before accessing any data members (other than _params, which is
        // read-only). Must also be held when calling any of the '_inlock()' helper functions.
        stdx::mutex _mutex;

        // Data tracking the state of our communication with each of the remote nodes.
        std::vector<RemoteCursorData> _remotes;

        class MergingComparator {
        public:
            MergingComparator(const std::vector<RemoteCursorData>& remotes,
                              const BSONObj& sort)
                : _remotes(remotes),
                  _sort(sort) { }

            bool operator()(const size_t& lhs, const size_t& rhs);

        private:
            const std::vector<RemoteCursorData>& _remotes;

            const BSONObj& _sort;
        };

        // The top of this priority queue is the index into '_remotes' for the remote host that has
        // the next document to return, according to the sort order. Used only if there is a sort.
        std::priority_queue<size_t, std::vector<size_t>, MergingComparator> _mergeQueue;

        // The index into '_remotes' for the remote from which we are currently retrieving results.
        // Used only if there is *not* a sort.
        size_t _gettingFromRemote = 0;

        Status _status = Status::OK();

        executor::TaskExecutor::EventHandle _currentEvent;
    };

} // namespace mongo
