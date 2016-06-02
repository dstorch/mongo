// cursor_manager.h

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

#pragma once

#include <boost/optional.hpp>
#include <map>
#include <unordered_set>
#include <vector>

#include "mongo/db/clientcursor.h"
#include "mongo/db/invalidation_type.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/record_id.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/concurrency/mutex.h"

namespace mongo {

class OperationContext;
class PseudoRandom;
class PlanExecutor;

class CursorManager {
public:
    CursorManager(StringData ns);

    /**
     * will kill() all PlanExecutor instances it has
     */
    ~CursorManager();

    // -----------------

    /**
     * @param collectionGoingAway Pass as true if the Collection instance is going away.
     *                            This could be because the db is being closed, or the
     *                            collection/db is being dropped.
     * @param reason              The motivation for invalidating all cursors. Will be used
     *                            for error reporting and logging when an operation finds that
     *                            the cursor it was operating on has been killed.
     */
    void invalidateAll(bool collectionGoingAway, const std::string& reason);

    /**
     * Broadcast a document invalidation to all relevant PlanExecutor(s).  invalidateDocument
     * must called *before* the provided RecordId is about to be deleted or mutated.
     */
    void invalidateDocument(OperationContext* txn, const RecordId& dl, InvalidationType type);

    /*
     * timesout cursors that have been idle for too long
     * note: must have a readlock on the collection
     * @return number timed out
     */
    std::size_t timeoutCursors(int millisSinceLastCall);

    // -----------------

    /**
     * Register an executor so that it can be notified of deletion/invalidation during yields.
     * Must be called before an executor yields.  If an executor is cached (inside a
     * ClientCursor) it MUST NOT be registered; the two are mutually exclusive.
     *
     * Returns a token which the caller must pass back to us in order to deregister the cursor. See
     * deregisterExecutor().
     */
    size_t registerExecutor(PlanExecutor* exec);

    /**
     * Remove an executor from the registry. The value of 'registrationToken' must be the value
     * given to the caller when 'exec' was registered with registerExecutor().
     */
    void deregisterExecutor(PlanExecutor* exec, size_t registrationToken);

    // -----------------

    CursorId registerCursor(ClientCursor* cc);
    void deregisterCursor(ClientCursor* cc);

    /**
     * Returns an OK status if the cursor was successfully erased.
     *
     * Returns error code CursorNotFound if the cursor id is not owned by this manager. Returns
     * error code OperationFailed if attempting to erase a pinned cursor.
     *
     * If 'shouldAudit' is true, will perform audit logging.
     */
    Status eraseCursor(OperationContext* txn, CursorId id, bool shouldAudit);

    /**
     * Returns true if the space of cursor ids that cursor manager is responsible for includes
     * the given cursor id.  Otherwise, returns false.
     *
     * The return value of this method does not indicate any information about whether or not a
     * cursor actually exists with the given cursor id.  Use the find() method for that purpose.
     */
    bool ownsCursorId(CursorId cursorId) const;

    void getCursorIds(std::set<CursorId>* openCursors) const;
    std::size_t numCursors() const;

    /**
     * @param pin - if true, will try to pin cursor
     *                  if pinned already, will assert
     *                  otherwise will pin
     */
    ClientCursor* find(CursorId id, bool pin);

    void unpin(ClientCursor* cursor);

    // ----------------------

    static CursorManager* getGlobalCursorManager();

    static int eraseCursorGlobalIfAuthorized(OperationContext* txn, int n, const char* ids);
    static bool eraseCursorGlobalIfAuthorized(OperationContext* txn, CursorId id);

    static bool eraseCursorGlobal(OperationContext* txn, CursorId id);

    /**
     * @return number timed out
     */
    static std::size_t timeoutCursorsGlobal(OperationContext* txn, int millisSinceLastCall);

private:
    struct ExecutorSet {
        // Synchronizes access to 'executors'. Rather than locking this mutex directly, use the
        // ExecutorRegistryPartitionGuard.
        stdx::mutex mutex;

        // Prefer to access via ExecutorRegistryPartitionGuard::operator->().
        std::unordered_set<PlanExecutor*> executors;
    };

    // A partitioned data structure with which PlanExecutors are registered in order to receive
    // notifications of events such as collection drops or invalidations. If the PlanExecutor is
    // owned by a ClientCursor, it is instead registered in '_cursors'.
    //
    // In order to avoid a performance bottleneck, the executors are divided into n partitions, and
    // access to each partition is synchronized separately. Locking of partitions should be done via
    // the ExecutorRegistryPartitionGuard.
    struct PartitionedExecutorRegistry {
        static const size_t kNumPartitions = 8;

        PartitionedExecutorRegistry() : partitions(kNumPartitions) {}

        // Returns the index of the partition in the 'partitions' vector to which a new plan
        // executor should be assigned.
        size_t nextPartition() {
            return _counter.fetchAndAdd(1) % kNumPartitions;
        }

        std::vector<ExecutorSet> partitions;

    private:
        AtomicUInt32 _counter;
    };

    // Used to protect access to either all partitions in the executor registry, or to protect
    // access to a single partition.
    //
    // If also locking '_cursorMapMutex', the cursor map mutex must be acquired *after* acquiring
    // this partition guard.
    class ExecutorRegistryPartitionGuard {
    public:
        // Acquires locks for every partition.
        ExecutorRegistryPartitionGuard(PartitionedExecutorRegistry* registry)
            : _registry(registry) {
            for (auto&& partition : registry->partitions) {
                _lockGuards.emplace_back(stdx::unique_lock<stdx::mutex>(partition.mutex));
            }
        }

        // Acquires locks for the ith partition.
        ExecutorRegistryPartitionGuard(PartitionedExecutorRegistry* registry, size_t partition)
            : _registry(registry), _partition(partition) {
            _lockGuards.emplace_back(
                stdx::unique_lock<stdx::mutex>(registry->partitions[partition].mutex));
        }

        // Returns a pointer to the set of plan executors which this guard is guarding. Only valid
        // to use if there is a single locked partition.
        std::unordered_set<PlanExecutor*>* operator->() {
            invariant(_lockGuards.size() == 1);
            invariant(_partition);
            invariant(_partition < _registry->partitions.size());
            return &_registry->partitions[*_partition].executors;
        }

    private:
        PartitionedExecutorRegistry* _registry;

        boost::optional<size_t> _partition;

        std::vector<stdx::unique_lock<stdx::mutex>> _lockGuards;
    };

    CursorId _allocateCursorId_inlock();
    void _deregisterCursor_inlock(ClientCursor* cc);

    NamespaceString _nss;
    unsigned _collectionCacheRuntimeId;

    PartitionedExecutorRegistry _planExecutorRegistry;

    // Synchronizes access to '_cursors' and '_random'. If also locking the _planExecutorRegistry,
    // the ExecutorRegistryPartitionGuard must be acquired *before* taking this lock.
    mutable SimpleMutex _cursorMapMutex;

    typedef std::map<CursorId, ClientCursor*> CursorMap;
    CursorMap _cursors;

    std::unique_ptr<PseudoRandom> _random;
};
}
