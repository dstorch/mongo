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

#include "mongo/s/query/cluster_find.h"

#include <set>
#include <vector>

#include "mongo/client/remote_command_targeter.h"
#include "mongo/s/catalog/catalog_cache.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/config.h"
#include "mongo/s/grid.h"
#include "mongo/s/query/cluster_client_cursor.h"

namespace mongo {

StatusWith<CursorId> ClusterFind::runQuery(OperationContext* txn,
                                           const CanonicalQuery& query,
                                           const ReadPreferenceSetting& readPref,
                                           std::vector<BSONObj>* results) {
    invariant(results);

    auto statusWithDBConfig = grid.catalogCache()->getDatabase(query.nss().db().toString());
    if (!statusWithDBConfig.isOK()) {
        return statusWithDBConfig.getStatus();
    }
    auto dbConfig = statusWithDBConfig.getValue();

    auto shardRegistry = grid.shardRegistry();

    std::vector<std::shared_ptr<Shard>> shards;
    std::shared_ptr<ChunkManager> manager;
    std::shared_ptr<Shard> primary;
    dbConfig->getChunkManagerOrPrimary(query.nss().ns(), manager, primary);
    if (primary) {
        shards.push_back(primary);
    } else {
        invariant(manager);

        std::set<ShardId> shardIds;
        manager->getShardIdsForQuery(shardIds, query);

        for (auto id : shardIds) {
            shards.push_back(shardRegistry->getShard(id));
        }
    }

    std::vector<HostAndPort> remotes;
    for (const auto& shard : shards) {
        auto targeter = shard->getTargeter();
        auto statusWithHost = targeter->findHost(readPref);
        if (!statusWithHost.isOK()) {
            return statusWithHost.getStatus();
        }
        remotes.push_back(statusWithHost.getValue());
    }

    // TODO: handle other query options.
    ClusterClientCursorParams params(query.nss());
    params.cmdObj = query.getParsed().asFindCommand();

    ClusterClientCursor ccc(shardRegistry->getExecutor(), params, remotes);

    // TODO: this should implement the batching logic rather than fully exhausting the cursor.
    StatusWith<boost::optional<BSONObj>> statusWithNext(boost::none);
    while ((statusWithNext = ccc.next()).isOK()) {
        auto obj = statusWithNext.getValue();
        if (!obj) {
            break;
        }
        results->push_back(*obj);
    }

    if (!statusWithNext.isOK()) {
        return statusWithNext.getStatus();
    }

    // TODO: this needs to allocate real cursor ids and register cursors with the mongos cursor
    // manager.
    return CursorId(0);
}

}  // namespace mongo
