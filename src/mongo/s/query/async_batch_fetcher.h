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

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/cursor_id.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/getmore_response.h"
#include "mongo/executor/task_executor.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

class AsyncBatchFetcher {
    MONGO_DISALLOW_COPYING(AsyncBatchFetcher);

public:
    AsyncBatchFetcher(executor::TaskExecutor* executor,
                      HostAndPort hostAndPort,
                      NamespaceString nss,
                      BSONObj cmdObj,
                      boost::optional<long long> batchSize);

    ~AsyncBatchFetcher();

    bool hasReadyBatch();

    StatusWith<GetMoreResponse> getReadyBatch();

    StatusWith<executor::TaskExecutor::EventHandle> scheduleBatchRequest();

    executor::TaskExecutor::EventHandle kill();

private:
    enum LifecycleState { kAlive, kKillStarted, kKillComplete };

    bool hasReadyBatch_inlock();

    void handleBatchResponse(const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData);

    static void handleKillCursorsResponse(
        const executor::TaskExecutor::RemoteCommandCallbackArgs& cbData);

    void signalEvent_inlock();

    void scheduleKillCursors_inlock();

    stdx::mutex _mutex;

    executor::TaskExecutor* _executor;

    HostAndPort _hostAndPort;

    NamespaceString _nss;

    BSONObj _cmdObj;

    boost::optional<long long> _batchSize;

    Status _status = Status::OK();

    boost::optional<CursorId> _cursorId;

    boost::optional<GetMoreResponse> _currentBatch;

    executor::TaskExecutor::EventHandle _nextBatchEvent;

    executor::TaskExecutor::CallbackHandle _cbHandle;

    //
    // Killing
    //

    LifecycleState _lifecycleState = kAlive;

    // Signaled when all outstanding batch request callbacks have run, and all killCursors commands
    // have been scheduled. This means that the ARM is safe to delete.
    executor::TaskExecutor::EventHandle _killCursorsScheduledEvent;
};

}  // namespace mongo
