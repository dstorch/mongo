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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/cursor_manager.h"

#include "mongo/base/data_cursor.h"
#include "mongo/base/init.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/cursor_server_params.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/kill_sessions_common.h"
#include "mongo/db/logical_session_cache.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session_catalog.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/startup_test.h"

namespace mongo {

using std::vector;

constexpr int CursorManager::kNumPartitions;

namespace {

// TODO: Make this a decoration on ServiceContext.
std::unique_ptr<CursorManager> globalCursorManager;

MONGO_INITIALIZER(GlobalCursorManager)(InitializerContext* context) {
    globalCursorManager.reset(new CursorManager({}));
    return Status::OK();
}

bool _killCursor(OperationContext* opCtx, CursorId id, bool checkAuth) {
    // Figure out what the namespace of this cursor is.
    auto pin = globalCursorManager->pinCursor(opCtx, id, CursorManager::kNoCheckSession);
    if (!pin.isOK()) {
        invariant(pin == ErrorCodes::CursorNotFound || pin == ErrorCodes::Unauthorized);
        // No such cursor.  TODO: Consider writing to audit log here (even though we don't
        // have a namespace).
        return false;
    }
    NamespaceString nss = pin.getValue().getCursor()->nss();
    invariant(nss.isValid());

    // Check if we are authorized to kill this cursor.
    if (checkAuth) {
        auto status = CursorManager::withCursorManager(
            opCtx, id, nss, [nss, id, opCtx](CursorManager* manager) {
                auto ccPin = manager->pinCursor(opCtx, id, CursorManager::kNoCheckSession);
                if (!ccPin.isOK()) {
                    return ccPin.getStatus();
                }
                AuthorizationSession* as = AuthorizationSession::get(opCtx->getClient());
                auto cursorOwner = ccPin.getValue().getCursor()->getAuthenticatedUsers();
                return as->checkAuthForKillCursors(nss, cursorOwner);
            });
        if (!status.isOK()) {
            audit::logKillCursorsAuthzCheck(opCtx->getClient(), nss, id, status.code());
            return false;
        }
    }

    Status killStatus = globalCursorManager->killCursor(opCtx, id, checkAuth);
    massert(28697,
            killStatus.reason(),
            killStatus.code() == ErrorCodes::OK || killStatus.code() == ErrorCodes::CursorNotFound);
    return killStatus.isOK();
}

}  // namespace

// TODO: Gotta work out how to kill this off.
template <typename Visitor>
void visitGlobalCursorManager(OperationContext* opCtx, Visitor* visitor) {
    (*visitor)(*globalCursorManager);
}

// ---

CursorManager* CursorManager::getGlobalCursorManager() {
    return globalCursorManager.get();
}

void CursorManager::appendAllActiveSessions(OperationContext* opCtx, LogicalSessionIdSet* lsids) {
    getGlobalCursorManager()->appendActiveSessions(lsids);
}

std::vector<GenericCursor> CursorManager::getAllCursors(OperationContext* opCtx) {
    std::vector<GenericCursor> cursors;
    getGlobalCursorManager()->appendActiveCursors(&cursors);
    return cursors;
}

std::pair<Status, int> CursorManager::killCursorsWithMatchingSessions(
    OperationContext* opCtx, const SessionKiller::Matcher& matcher) {
    auto eraser = [&](CursorManager& mgr, CursorId id) {
        uassertStatusOK(mgr.killCursor(opCtx, id, true));
    };

    auto visitor = makeKillSessionsCursorManagerVisitor(opCtx, matcher, std::move(eraser));
    visitGlobalCursorManager(opCtx, &visitor);

    return std::make_pair(visitor.getStatus(), visitor.getCursorsKilled());
}

std::size_t CursorManager::timeoutCursorsGlobal(OperationContext* opCtx, Date_t now) {
    return getGlobalCursorManager()->timeoutCursors(opCtx, now);
}

int CursorManager::killCursorGlobalIfAuthorized(OperationContext* opCtx, int n, const char* _ids) {
    ConstDataCursor ids(_ids);
    int numDeleted = 0;
    for (int i = 0; i < n; i++) {
        if (killCursorGlobalIfAuthorized(opCtx, ids.readAndAdvance<LittleEndian<int64_t>>()))
            numDeleted++;
        if (globalInShutdownDeprecated())
            break;
    }
    return numDeleted;
}
bool CursorManager::killCursorGlobalIfAuthorized(OperationContext* opCtx, CursorId id) {
    return _killCursor(opCtx, id, true);
}
bool CursorManager::killCursorGlobal(OperationContext* opCtx, CursorId id) {
    return _killCursor(opCtx, id, false);
}

Status CursorManager::withCursorManager(OperationContext* opCtx,
                                        CursorId id,
                                        const NamespaceString& nss,
                                        stdx::function<Status(CursorManager*)> callback) {
    CursorManager* cursorManager = CursorManager::getGlobalCursorManager();
    return callback(cursorManager);
}

// --------------------------

CursorManager::CursorManager(NamespaceString nss)
    : _nss(std::move(nss)),
      _secureRandom(SecureRandom::create()),
      _random(stdx::make_unique<PseudoRandom>(_secureRandom->nextInt64())),
      _cursorMap(stdx::make_unique<Partitioned<stdx::unordered_map<CursorId, ClientCursor*>>>()) {}

CursorManager::~CursorManager() {
    // All cursors should have been deleted already.
    invariant(_cursorMap->empty());
}

void CursorManager::invalidateAll(OperationContext* opCtx,
                                  bool collectionGoingAway,
                                  const std::string& reason) {
    invariant(!isGlobalManager());  // The global cursor manager should never need to kill cursors.
    dassert(opCtx->lockState()->isCollectionLockedForMode(_nss.ns(), MODE_X));
    fassert(28819, !BackgroundOperation::inProgForNs(_nss));

    // Mark all cursors as killed, but keep around those we can in order to provide a useful error
    // message to the user when they attempt to use it next time.
    std::vector<std::unique_ptr<ClientCursor, ClientCursor::Deleter>> toDisposeWithoutMutex;
    {
        auto allCurrentPartitions = _cursorMap->lockAllPartitions();
        for (auto&& partition : allCurrentPartitions) {
            for (auto it = partition.begin(); it != partition.end();) {
                auto* cursor = it->second;
                cursor->markAsKilled({ErrorCodes::QueryPlanKilled, reason});

                // If there's an operation actively using the cursor, then that operation is now
                // responsible for cleaning it up.  Otherwise we can immediately dispose of it.
                if (cursor->_operationUsingCursor) {
                    it = partition.erase(it);
                    continue;
                }

                if (!collectionGoingAway) {
                    // We keep around unpinned cursors so that future attempts to use the cursor
                    // will result in a useful error message.
                    ++it;
                } else {
                    toDisposeWithoutMutex.emplace_back(cursor);
                    it = partition.erase(it);
                }
            }
        }
    }

    // Dispose of the cursors we can now delete. This might involve lock acquisitions for safe
    // cleanup, so avoid doing it while holding mutexes.
    for (auto&& cursor : toDisposeWithoutMutex) {
        cursor->dispose(opCtx);
    }
}

bool CursorManager::cursorShouldTimeout_inlock(const ClientCursor* cursor, Date_t now) {
    if (cursor->isNoTimeout() || cursor->_operationUsingCursor) {
        return false;
    }
    return (now - cursor->_lastUseDate) >= Milliseconds(getCursorTimeoutMillis());
}

std::size_t CursorManager::timeoutCursors(OperationContext* opCtx, Date_t now) {
    std::vector<std::unique_ptr<ClientCursor, ClientCursor::Deleter>> toDisposeWithoutMutex;

    for (size_t partitionId = 0; partitionId < kNumPartitions; ++partitionId) {
        auto lockedPartition = _cursorMap->lockOnePartitionById(partitionId);
        for (auto it = lockedPartition->begin(); it != lockedPartition->end();) {
            auto* cursor = it->second;
            if (cursorShouldTimeout_inlock(cursor, now)) {
                toDisposeWithoutMutex.emplace_back(cursor);
                it = lockedPartition->erase(it);
            } else {
                ++it;
            }
        }
    }

    // Be careful not to dispose of cursors while holding the partition lock.
    for (auto&& cursor : toDisposeWithoutMutex) {
        cursor->dispose(opCtx);
    }
    return toDisposeWithoutMutex.size();
}

StatusWith<ClientCursorPin> CursorManager::pinCursor(OperationContext* opCtx,
                                                     CursorId id,
                                                     AuthCheck checkSessionAuth) {
    auto lockedPartition = _cursorMap->lockOnePartition(id);
    auto it = lockedPartition->find(id);
    if (it == lockedPartition->end()) {
        return {ErrorCodes::CursorNotFound, str::stream() << "cursor id " << id << " not found"};
    }

    ClientCursor* cursor = it->second;
    uassert(ErrorCodes::CursorInUse,
            str::stream() << "cursor id " << id << " is already in use",
            !cursor->_operationUsingCursor);
    if (cursor->getExecutor()->isMarkedAsKilled()) {
        // This cursor was killed while it was idle.
        Status error = cursor->getExecutor()->getKillStatus();
        deregisterAndDestroyCursor(std::move(lockedPartition),
                                   opCtx,
                                   std::unique_ptr<ClientCursor, ClientCursor::Deleter>(cursor));
        return error;
    }

    if (checkSessionAuth == kCheckSession) {
        auto cursorPrivilegeStatus = checkCursorSessionPrivilege(opCtx, cursor->getSessionId());
        if (!cursorPrivilegeStatus.isOK()) {
            return cursorPrivilegeStatus;
        }
    }

    cursor->_operationUsingCursor = opCtx;

    // We use pinning of a cursor as a proxy for active, user-initiated use of a cursor.  Therefor,
    // we pass down to the logical session cache and vivify the record (updating last use).
    if (cursor->getSessionId()) {
        LogicalSessionCache::get(opCtx)->vivify(opCtx, cursor->getSessionId().get());
    }

    return ClientCursorPin(opCtx, cursor);
}

void CursorManager::unpin(OperationContext* opCtx,
                          std::unique_ptr<ClientCursor, ClientCursor::Deleter> cursor) {
    // Avoid computing the current time within the critical section.
    auto now = opCtx->getServiceContext()->getPreciseClockSource()->now();

    auto partition = _cursorMap->lockOnePartition(cursor->cursorid());
    invariant(cursor->_operationUsingCursor);

    // We must verify that no interrupts have occurred since we finished building the current
    // batch. Otherwise, the cursor will be checked back in, the interrupted opCtx will be
    // destroyed, and subsequent getMores with a fresh opCtx will succeed.
    auto interruptStatus = cursor->_operationUsingCursor->checkForInterruptNoAssert();
    cursor->_operationUsingCursor = nullptr;
    cursor->_lastUseDate = now;

    // If someone was trying to kill this cursor with a killOp or a killCursors, they are likely
    // interesting in proactively cleaning up that cursor's resources. In these cases, we
    // proactively delete the cursor. In other cases we preserve the error code so that the client
    // will see the reason the cursor was killed when asking for the next batch.
    if (interruptStatus == ErrorCodes::Interrupted || interruptStatus == ErrorCodes::CursorKilled) {
        LOG(0) << "removing cursor " << cursor->cursorid()
               << " after completing batch: " << interruptStatus;
        return deregisterAndDestroyCursor(std::move(partition), opCtx, std::move(cursor));
    } else if (!interruptStatus.isOK()) {
        cursor->markAsKilled(interruptStatus);
    }

    // The cursor will stay around in '_cursorMap', so release the unique pointer to avoid deleting
    // it.
    cursor.release();
}

void CursorManager::appendActiveSessions(LogicalSessionIdSet* lsids) const {
    auto allPartitions = _cursorMap->lockAllPartitions();
    for (auto&& partition : allPartitions) {
        for (auto&& entry : partition) {
            auto cursor = entry.second;
            if (auto id = cursor->getSessionId()) {
                lsids->insert(id.value());
            }
        }
    }
}

void CursorManager::appendActiveCursors(std::vector<GenericCursor>* cursors) const {
    auto allPartitions = _cursorMap->lockAllPartitions();
    for (auto&& partition : allPartitions) {
        for (auto&& entry : partition) {
            auto cursor = entry.second;
            cursors->emplace_back();
            auto& gc = cursors->back();
            gc.setId(cursor->_cursorid);
            gc.setNs(cursor->nss());
            gc.setLsid(cursor->getSessionId());
        }
    }
}

stdx::unordered_set<CursorId> CursorManager::getCursorsForSession(LogicalSessionId lsid) const {
    stdx::unordered_set<CursorId> cursors;

    auto allPartitions = _cursorMap->lockAllPartitions();
    for (auto&& partition : allPartitions) {
        for (auto&& entry : partition) {
            auto cursor = entry.second;
            if (cursor->getSessionId() == lsid) {
                cursors.insert(cursor->cursorid());
            }
        }
    }

    return cursors;
}

size_t CursorManager::numCursors() const {
    return _cursorMap->size();
}

CursorId CursorManager::allocateCursorId_inlock() {
    for (int i = 0; i < 10000; i++) {
        // Generate a random number to act as the new cursor id. Make sure the first bit is 0, since
        // this number will be reinterpreted as a 64 bit signed number.
        uint64_t mask = 0x7FFFFFFFFFFFFFFF;
        CursorId id = (_random->nextInt64() & mask);

        auto partition = _cursorMap->lockOnePartition(id);
        if (partition->count(id) == 0)
            return id;
    }
    fassertFailed(17360);
}

ClientCursorPin CursorManager::registerCursor(OperationContext* opCtx,
                                              ClientCursorParams&& cursorParams) {
    // Avoid computing the current time within the critical section.
    auto now = opCtx->getServiceContext()->getPreciseClockSource()->now();

    // Make sure the PlanExecutor isn't registered, since we will register the ClientCursor wrapping
    // it.
    invariant(cursorParams.exec);
    cursorParams.exec.get_deleter().dismissDisposal();

    // Note we must hold the registration lock from now until insertion into '_cursorMap' to ensure
    // we don't insert two cursors with the same cursor id.
    stdx::lock_guard<SimpleMutex> lock(_registrationLock);
    CursorId cursorId = allocateCursorId_inlock();
    std::unique_ptr<ClientCursor, ClientCursor::Deleter> clientCursor(
        new ClientCursor(std::move(cursorParams), this, cursorId, opCtx, now));

    // Register this cursor for lookup by transaction.
    if (opCtx->getLogicalSessionId() && opCtx->getTxnNumber()) {
        invariant(opCtx->getLogicalSessionId());
    }

    // Transfer ownership of the cursor to '_cursorMap'.
    auto partition = _cursorMap->lockOnePartition(cursorId);
    ClientCursor* unownedCursor = clientCursor.release();
    partition->emplace(cursorId, unownedCursor);
    return ClientCursorPin(opCtx, unownedCursor);
}

void CursorManager::deregisterCursor(ClientCursor* cursor) {
    _cursorMap->erase(cursor->cursorid());
}

void CursorManager::deregisterAndDestroyCursor(
    Partitioned<stdx::unordered_map<CursorId, ClientCursor*>, kNumPartitions>::OnePartition&& lk,
    OperationContext* opCtx,
    std::unique_ptr<ClientCursor, ClientCursor::Deleter> cursor) {
    {
        auto lockWithRestrictedScope = std::move(lk);
        lockWithRestrictedScope->erase(cursor->cursorid());
    }
    // Dispose of the cursor without holding any cursor manager mutexes. Disposal of a cursor can
    // require taking lock manager locks, which we want to avoid while holding a mutex. If we did
    // so, any caller of a CursorManager method which already held a lock manager lock could induce
    // a deadlock when trying to acquire a CursorManager lock.
    cursor->dispose(opCtx);
}

Status CursorManager::killCursor(OperationContext* opCtx, CursorId id, bool shouldAudit) {
    auto lockedPartition = _cursorMap->lockOnePartition(id);
    auto it = lockedPartition->find(id);
    if (it == lockedPartition->end()) {
        if (shouldAudit) {
            audit::logKillCursorsAuthzCheck(
                opCtx->getClient(), _nss, id, ErrorCodes::CursorNotFound);
        }
        return {ErrorCodes::CursorNotFound, str::stream() << "Cursor id not found: " << id};
    }
    auto cursor = it->second;

    if (cursor->_operationUsingCursor) {
        // Rather than removing the cursor directly, kill the operation that's currently using the
        // cursor. It will stop on its own (and remove the cursor) when it sees that it's been
        // interrupted.
        {
            stdx::unique_lock<Client> lk(*cursor->_operationUsingCursor->getClient());
            cursor->_operationUsingCursor->getServiceContext()->killOperation(
                cursor->_operationUsingCursor, ErrorCodes::CursorKilled);
        }

        if (shouldAudit) {
            audit::logKillCursorsAuthzCheck(opCtx->getClient(), _nss, id, ErrorCodes::OK);
        }
        return Status::OK();
    }
    std::unique_ptr<ClientCursor, ClientCursor::Deleter> ownedCursor(cursor);

    if (shouldAudit) {
        audit::logKillCursorsAuthzCheck(opCtx->getClient(), _nss, id, ErrorCodes::OK);
    }

    deregisterAndDestroyCursor(std::move(lockedPartition), opCtx, std::move(ownedCursor));
    return Status::OK();
}

Status CursorManager::checkAuthForKillCursors(OperationContext* opCtx, CursorId id) {
    auto lockedPartition = _cursorMap->lockOnePartition(id);
    auto it = lockedPartition->find(id);
    if (it == lockedPartition->end()) {
        return {ErrorCodes::CursorNotFound, str::stream() << "cursor id " << id << " not found"};
    }

    ClientCursor* cursor = it->second;
    // Note that we're accessing the cursor without having pinned it! This is okay since we're only
    // accessing nss() and getAuthenticatedUsers() both of which return values that don't change
    // after the cursor's creation. We're guaranteed that the cursor won't get destroyed while we're
    // reading from it because we hold the partition's lock.
    AuthorizationSession* as = AuthorizationSession::get(opCtx->getClient());
    return as->checkAuthForKillCursors(cursor->nss(), cursor->getAuthenticatedUsers());
}

}  // namespace mongo
