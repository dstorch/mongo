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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/s/query/async_cluster_client_cursor.h"

#include "mongo/client/remote_command_runner.h"
#include "mongo/db/query/getmore_request.h"
#include "mongo/db/query/getmore_response.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

    AsyncClusterClientCursor::AsyncClusterClientCursor(
            executor::TaskExecutor* executor,
            const ClusterClientCursorParams& params,
            const std::vector<HostAndPort>& remotes)
                : _executor(executor),
                  _params(params),
                  _mergeQueue(MergingComparator(_remotes, _params.sort)) {
        for (const auto& remote : remotes) {
            _remotes.push_back(RemoteCursorData(remote));
        }
    }

    bool AsyncClusterClientCursor::ready() {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        return ready_inlock();
    }

    bool AsyncClusterClientCursor::ready_inlock() {
        // First check whether any of the remotes reported an error.
        for (const auto& remote : _remotes) {
            if (!remote.status.isOK()) {
                _status = remote.status;
                return true;
            }
        }

        const bool hasSort = !_params.sort.isEmpty();
        return hasSort ? readySorted_inlock() : readyUnsorted_inlock();
    }

    bool AsyncClusterClientCursor::readySorted_inlock() {
        for (const auto& remote : _remotes) {
            if (!remote.hasNext() && !remote.exhausted()) {
                return false;
            }
        }

        return true;
    }

    bool AsyncClusterClientCursor::readyUnsorted_inlock() {
        bool allExhausted = true;
        for (const auto& remote : _remotes) {
            if (!remote.exhausted()) {
                allExhausted = false;
            }

            if (remote.hasNext()) {
                return true;
            }
        }

        return allExhausted;
    }

    StatusWith<boost::optional<BSONObj>> AsyncClusterClientCursor::nextReady() {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        dassert(ready_inlock());
        if (!_status.isOK()) {
            return _status;
        }

        const bool hasSort = !_params.sort.isEmpty();
        return hasSort ? nextReadySorted() : nextReadyUnsorted();
    }

    boost::optional<BSONObj> AsyncClusterClientCursor::nextReadySorted() {
        if (_mergeQueue.empty()) {
            return boost::none;
        }

        size_t smallestRemote = _mergeQueue.top();
        _mergeQueue.pop();

        invariant(!_remotes[smallestRemote].docBuffer.empty());
        invariant(_remotes[smallestRemote].status.isOK());

        BSONObj front = _remotes[smallestRemote].docBuffer.front();
        _remotes[smallestRemote].docBuffer.pop();

        // Re-populate the merging queue with the next result from 'smallestRemote', if it has a
        // next result.
        if (!_remotes[smallestRemote].docBuffer.empty()) {
            _mergeQueue.push(smallestRemote);
        }

        return front;
    }

    boost::optional<BSONObj> AsyncClusterClientCursor::nextReadyUnsorted() {
        size_t remotesAttempted = 0;
        while (remotesAttempted < _remotes.size()) {

            // It is illegal to call this method if there is an error received from any shard.
            invariant(_remotes[_gettingFromRemote].status.isOK());

            if (_remotes[_gettingFromRemote].hasNext()) {
                BSONObj front = _remotes[_gettingFromRemote].docBuffer.front();
                _remotes[_gettingFromRemote].docBuffer.pop();
                return front;
            }

            // Nothing from the current remote so move on to the next one.
            ++remotesAttempted;
            if (++_gettingFromRemote == _remotes.size()) {
                _gettingFromRemote = 0;
            }
        }

        return boost::none;
    }

    StatusWith<executor::TaskExecutor::EventHandle> AsyncClusterClientCursor::nextEvent() {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        auto eventStatus = _executor->makeEvent();
        if (!eventStatus.isOK()) {
            return eventStatus;
        }

        // Schedule remote work on hosts for which we need more results.
        for (size_t i = 0; i < _remotes.size(); ++i) {
            auto& remote = _remotes[i];

            // It is illegal to call this method if there is an error received from any shard.
            invariant(remote.status.isOK());

            if (!remote.hasNext() && !remote.exhausted()) {
                // If we already have established a cursor with this remote send a getMore with the
                // appropriate cursorId. Otherwise, send the cursor-establishing command.
                BSONObj cmdObj = remote.cursorId ? GetMoreRequest(_params.nsString.ns(),
                                                                  *remote.cursorId,
                                                                  _params.batchSize).toBSON()
                                                 : _params.cmdObj;

                RemoteCommandRequest request(remote.hostAndPort,
                                             _params.nsString.db().toString(),
                                             cmdObj);

                auto callbackStatus = _executor->scheduleRemoteCommand(
                        request,
                        stdx::bind(&AsyncClusterClientCursor::handleRemoteCommandResponse,
                                   this,
                                   stdx::placeholders::_1,
                                   i));
                if (!callbackStatus.isOK()) {
                    return callbackStatus.getStatus();
                }

                remote.cbHandle = callbackStatus.getValue();
            }
        }

        _currentEvent = eventStatus.getValue();
        return _currentEvent;
    }

    void AsyncClusterClientCursor::handleRemoteCommandResponse(
            const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData,
            size_t remoteIndex) {
        stdx::lock_guard<stdx::mutex> lk(_mutex);

        // Every early return should signal anyone waiting on an event, if ready() is true.
        ScopeGuard signaller = MakeGuard(&AsyncClusterClientCursor::signalCurrentEvent_inlock,
                                         this);

        if (!cbData.response.isOK()) {
            _remotes[remoteIndex].status = cbData.response.getStatus();
            return;
        }

        auto getMoreParseStatus = GetMoreResponse::parseFromBSON(cbData.response.getValue().data);
        if (!getMoreParseStatus.isOK()) {
            _remotes[remoteIndex].status = getMoreParseStatus.getStatus();
            return;
        }

        auto getMoreResponse = getMoreParseStatus.getValue();
        auto& remote = _remotes[remoteIndex];

        // If we have a cursor established, and we get a non-zero cursorid that is not equal to the
        // established cursorid, we will fail the operation.
        if (remote.cursorId && getMoreResponse.cursorId != 0
                && *remote.cursorId != getMoreResponse.cursorId) {
            _remotes[remoteIndex].status = Status(ErrorCodes::BadValue,
                str::stream() << "Expected cursorid " << *remote.cursorId
                              << " but received " << getMoreResponse.cursorId);
            return;
        }

        remote.cursorId = getMoreResponse.cursorId;

        for (const auto& obj : getMoreResponse.batch) {
            remote.docBuffer.push(obj);
        }

        // If we're doing a sorted merge, then we have to make sure to put this remote onto the
        // merge queue.
        if (!_params.sort.isEmpty() && !getMoreResponse.batch.empty()) {
            _mergeQueue.push(remoteIndex);
        }

        // ScopeGuard requires dismiss on success, but we want waiter to be signalled on success as
        // well as failure.
        signaller.Dismiss();
        signalCurrentEvent_inlock();
    }

    void AsyncClusterClientCursor::signalCurrentEvent_inlock() {
        if (ready_inlock() && _currentEvent.isValid()) {
            // To prevent ourselves from signalling the event twice, we set '_currentEvent' as
            // invalid after signalling it.
            _executor->signalEvent(_currentEvent);
            _currentEvent = executor::TaskExecutor::EventHandle();
        }
    }

    //
    // AsyncClusterClientCursor::RemoteCursorData
    //

    AsyncClusterClientCursor::RemoteCursorData::RemoteCursorData(const HostAndPort& host)
            : hostAndPort(host) { }

    bool AsyncClusterClientCursor::RemoteCursorData::hasNext() const {
        return !docBuffer.empty();
    }

    bool AsyncClusterClientCursor::RemoteCursorData::exhausted() const {
        return cursorId && (*cursorId == 0);
    }

    //
    // AsyncClusterClientCursor::MergingComparator
    //

    bool AsyncClusterClientCursor::MergingComparator::operator()(const size_t& lhs,
                                                                 const size_t& rhs) {
        const BSONObj& leftDoc = _remotes[lhs].docBuffer.front();
        const BSONObj& rightDoc = _remotes[rhs].docBuffer.front();

        return leftDoc.woSortOrder(rightDoc, _sort, true /*useDotted*/) > 0;
    }

} // namespace mongo
