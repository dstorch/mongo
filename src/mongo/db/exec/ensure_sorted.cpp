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

#include "mongo/db/exec/ensure_sorted.h"

#include <memory>

#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/sort.h"
#include "mongo/db/exec/working_set_computed_data.h"

namespace mongo {

const char* EnsureSortedStage::kStageType = "ENSURE_SORTED";

EnsureSortedStage::EnsureSortedStage(OperationContext* opCtx,
                                     BSONObj pattern,
                                     WorkingSet* ws,
                                     PlanStage* child)
    : _ws(ws), _child(child), _commonStats(kStageType) {
    _pattern = FindCommon::transformSortSpec(pattern);
}

bool EnsureSortedStage::isEOF() {
    return child()->isEOF();
}

PlanStage::StageState EnsureSortedStage::work(WorkingSetID* out) {
    ++_commonStats.works;

    // Adds the amount of time taken by work() to executionTimeMillis.
    ScopedTimer timer(&_commonStats.executionTimeMillis);

    StageState stageState = child()->work(out);

    if (PlanStage::ADVANCED == stageState) {
        // We extract the sort key from the WSM's computed data. This must have been generated
        // by a SortKeyGeneratorStage descendent in the execution tree.
        WorkingSetMember* member = _ws->get(*out);
        auto sortKeyComputedData =
            static_cast<const SortKeyComputedData*>(member->getComputed(WSM_SORT_KEY));
        BSONObj curSortKey = sortKeyComputedData->getSortKey();
        invariant(!curSortKey.isEmpty());

        if (!_prevSortKey.isEmpty() && !isInOrder(_prevSortKey, curSortKey)) {
            // 'member' is out of order. Drop it from the result set.
            _ws->free(*out);
            ++_specificStats.nDropped;
            ++_commonStats.needTime;
            return PlanStage::NEED_TIME;
        }

        invariant(curSortKey.isOwned());
        _prevSortKey = curSortKey;
        ++_commonStats.advanced;
        return PlanStage::ADVANCED;
    }

    if (PlanStage::NEED_TIME == stageState) {
        ++_commonStats.needTime;
    } else if (PlanStage::NEED_YIELD == stageState) {
        ++_commonStats.needYield;
    }

    return stageState;
}

std::vector<PlanStage*> EnsureSortedStage::getChildren() const {
    std::vector<PlanStage*> children;
    children.push_back(_child.get());
    return children;
}

void EnsureSortedStage::saveState() {
    ++_commonStats.yields;
    _child->saveState();
}

void EnsureSortedStage::restoreState(OperationContext* opCtx) {
    ++_commonStats.unyields;
    _child->restoreState(opCtx);
}

void EnsureSortedStage::invalidate(OperationContext* txn,
                                   const RecordId& dl,
                                   InvalidationType type) {
    ++_commonStats.invalidates;
    _child->invalidate(txn, dl, type);
}

PlanStageStats* EnsureSortedStage::getStats() {
    _commonStats.isEOF = isEOF();
    std::unique_ptr<PlanStageStats> ret(new PlanStageStats(_commonStats, STAGE_ENSURE_SORTED));
    ret->specific = make_unique<EnsureSortedStats>(_specificStats);
    ret->children.emplace_back(child()->getStats());
    return ret.release();
}

const CommonStats* KeepMutationsStage::getCommonStats() {
    return &_commonStats;
}

const SpecificStats* EnsureSortedStage::getSpecificStats() {
    return &_specificStats;
}

bool EnsureSortedStage::isInOrder(const BSONObj& lhsSortKey, const BSONObj& rhsSortKey) const {
    return lhsSortKey.woCompare(rhsSortKey, _pattern, /*considerFieldName*/ false) <= 0;
}

}  // namespace mongo
