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

#include "mongo/s/query/cluster_client_cursor_impl.h"

#include "mongo/s/query/router_stage_limit.h"
#include "mongo/s/query/router_stage_merge.h"
#include "mongo/s/query/router_stage_skip.h"
#include "mongo/s/query/router_stage_tailable.h"
#include "mongo/stdx/memory.h"

namespace mongo {

ClusterClientCursorImpl::ClusterClientCursorImpl(executor::TaskExecutor* executor,
                                                 ClusterClientCursorParams params)
    : _isTailable(params.isTailable), _root(buildMergerPlan(executor, std::move(params))) {}

StatusWith<boost::optional<BSONObj>> ClusterClientCursorImpl::next() {
    return _root->next();
}

void ClusterClientCursorImpl::kill() {
    _root->kill();
}

bool ClusterClientCursorImpl::isTailable() const {
    return _isTailable;
}

std::unique_ptr<RouterExecStage> ClusterClientCursorImpl::buildMergerPlan(
    executor::TaskExecutor* executor, ClusterClientCursorParams params) {
    // The leaf stage is either a special stage for tailable cursors or, in the normal case, the
    // stage that merges results from the shards.
    std::unique_ptr<RouterExecStage> root;
    if (params.isTailable) {
        // Tailable cursors must act on capped collections and capped collections cannot be sharded.
        // Therefore, we expect there to be only one remote node.
        //
        // TODO check invariant higher up. Do we want to check a sort invariant?
        invariant(params.remotes.size() == 1U);

        root = stdx::make_unique<RouterStageTailable>(executor,
                                                      params.remotes[0].hostAndPort,
                                                      params.nsString,
                                                      params.remotes[0].cmdObj,
                                                      params.batchSize);
    }
    else {
        root = stdx::make_unique<RouterStageMerge>(executor, params);
    }

    if (params.skip) {
        root = stdx::make_unique<RouterStageSkip>(std::move(root), *params.skip);
    }

    if (params.limit) {
        root = stdx::make_unique<RouterStageLimit>(std::move(root), *params.limit);
    }

    return root;
}

}  // namespace mongo
