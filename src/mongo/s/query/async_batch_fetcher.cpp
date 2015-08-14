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

#include "mongo/platform/basic.h"

#include "mongo/s/query/async_batch_fetcher.h"

#include "mongo/db/query/getmore_request.h"
#include "mongo/db/query/killcursors_request.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

AsyncBatchFetcher::AsyncBatchFetcher(executor::TaskExecutor* executor,
                                     HostAndPort hostAndPort,
                                     NamespaceString nss,
                                     BSONObj cmdObj,
                                     boost::optional<long long> batchSize)
    : _executor(executor),
      _hostAndPort(std::move(hostAndPort)),
      _nss(std::move(nss)),
      _cmdObj(std::move(cmdObj)),
      _batchSize(batchSize) {}

AsyncBatchFetcher::~AsyncBatchFetcher() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    // Either we exhausted the cursor or it got killed.
    invariant((_cursorId && *_cursorId == 0) || _lifecycleState == kKillComplete);
}

bool AsyncBatchFetcher::hasReadyBatch() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return hasReadyBatch_inlock();
}

bool AsyncBatchFetcher::hasReadyBatch_inlock() {
    if (_lifecycleState != kAlive) {
        // We're ready to return a "killed" error.
        return true;
    }

    return !_status.isOK() || _currentBatch;
}

StatusWith<GetMoreResponse> AsyncBatchFetcher::getReadyBatch() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(hasReadyBatch_inlock());

    if (_lifecycleState != kAlive) {
        return {ErrorCodes::IllegalOperation, "AsyncBatchFetcher killed"};
    }

    if (!_status.isOK()) {
        return _status;
    }

    auto batch = std::move(*_currentBatch);
    _currentBatch = boost::none;
    return batch;
}

StatusWith<executor::TaskExecutor::EventHandle> AsyncBatchFetcher::scheduleBatchRequest() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(!hasReadyBatch_inlock());

    if (_lifecycleState != kAlive) {
        // Can't schedule further network operations if the ARM is being killed.
        return {ErrorCodes::IllegalOperation,
                "can't schedule another batch request on a killed AsyncBatchFetcher"};
    }

    if (_nextBatchEvent.isValid()) {
        // We can't make a new event if there's still an unsignaled one, as every event must
        // eventually be signaled.
        return {ErrorCodes::IllegalOperation,
                "scheduleBatchRequest() called before an outstanding event was signaled"};
    }

    // There shouldn't be an outstanding callback.
    invariant(!_cbHandle.isValid());

    BSONObj cmdObj =
        _cursorId ? GetMoreRequest(_nss, *_cursorId, _batchSize, boost::none).toBSON() : _cmdObj;

    executor::RemoteCommandRequest request(_hostAndPort, _nss.db().toString(), cmdObj);

    auto callbackStatus = _executor->scheduleRemoteCommand(
        request, stdx::bind(&AsyncBatchFetcher::handleBatchResponse, this, stdx::placeholders::_1));
    if (!callbackStatus.isOK()) {
        return callbackStatus.getStatus();
    }

    auto event = _executor->makeEvent();
    if (!event.isOK()) {
        return event;
    }
    _nextBatchEvent = event.getValue();
    return _nextBatchEvent;
}

void AsyncBatchFetcher::handleBatchResponse(
    const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    // Clear the callback handle. This indicates that we are no longer waiting on a response.
    _cbHandle = executor::TaskExecutor::CallbackHandle();

    if (_lifecycleState != kAlive) {
        invariant(_lifecycleState == kKillStarted);

        signalEvent_inlock();

        if (_killCursorsScheduledEvent.isValid()) {
            scheduleKillCursors_inlock();
            _executor->signalEvent(_killCursorsScheduledEvent);
        }

        _lifecycleState = kKillComplete;
        return;
    }

    // Signal anyone waiting if there is an error.
    ScopeGuard signaller = MakeGuard(&AsyncBatchFetcher::signalEvent_inlock, this);

    if (!cbData.response.isOK()) {
        _status = cbData.response.getStatus();
        return;
    }

    auto getMoreParseStatus = GetMoreResponse::parseFromBSON(cbData.response.getValue().data);
    if (!getMoreParseStatus.isOK()) {
        _status = getMoreParseStatus.getStatus();
        return;
    }
    auto getMoreResponse = std::move(getMoreParseStatus.getValue());

    // If we have a cursor established, and we get a non-zero cursorid that is not equal to the
    // established cursorid, we will fail the operation.
    if (_cursorId && getMoreResponse.cursorId != 0 && *_cursorId != getMoreResponse.cursorId) {
        _status = Status(ErrorCodes::BadValue,
                         str::stream() << "Expected cursorid " << *_cursorId << " but received "
                                       << getMoreResponse.cursorId);
        return;
    }

    _cursorId = getMoreResponse.cursorId;
    _currentBatch = std::move(getMoreResponse);

    signaller.Dismiss();
    signalEvent_inlock();
}

void AsyncBatchFetcher::signalEvent_inlock() {
    if (hasReadyBatch_inlock() && _nextBatchEvent.isValid()) {
        // To prevent ourselves from signalling the event twice, we set '_currentEvent' as
        // invalid after signalling it.
        _executor->signalEvent(_nextBatchEvent);
        _nextBatchEvent = executor::TaskExecutor::EventHandle();
    }
}

executor::TaskExecutor::EventHandle AsyncBatchFetcher::kill() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);

    if (_killCursorsScheduledEvent.isValid()) {
        invariant(_lifecycleState != kAlive);
        return _killCursorsScheduledEvent;
    }

    _lifecycleState = kKillStarted;

    // Cancel the callback if necessary.
    if (_cbHandle.isValid()) {
        _executor->cancel(_cbHandle);
    }

    // Make '_killCursorsScheduledEvent', which we will signal as soon as we have scheduled a
    // killCursors command to run on all the remote shards.
    auto event = _executor->makeEvent();
    if (event.getStatus().code() == ErrorCodes::ShutdownInProgress) {
        // The underlying task executor is shutting down.
        if (!_cbHandle.isValid()) {
            _lifecycleState = kKillComplete;
        }
        return executor::TaskExecutor::EventHandle();
    }
    fassertStatusOK(28782, event);
    _killCursorsScheduledEvent = event.getValue();

    // If we're not waiting for responses from the remote, we can schedule a killCursors command now
    // and signal the event immediately.
    if (!_cbHandle.isValid()) {
        scheduleKillCursors_inlock();
        _lifecycleState = kKillComplete;
        _executor->signalEvent(_killCursorsScheduledEvent);
    }

    return _killCursorsScheduledEvent;
}

void AsyncBatchFetcher::scheduleKillCursors_inlock() {
    invariant(_lifecycleState == kKillStarted);
    invariant(_killCursorsScheduledEvent.isValid());
    invariant(!_cbHandle.isValid());

    if (_status.isOK() && _cursorId && *_cursorId != 0) {
        BSONObj cmdObj = KillCursorsRequest(_nss, {*_cursorId}).toBSON();

        executor::RemoteCommandRequest request(_hostAndPort, _nss.db().toString(), cmdObj);

        _executor->scheduleRemoteCommand(
            request,
            stdx::bind(&AsyncBatchFetcher::handleKillCursorsResponse, stdx::placeholders::_1));
    }
}

void AsyncBatchFetcher::handleKillCursorsResponse(
    const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData) {
    // killCursors command responses are ignored.
}

}  // namespace mongo
