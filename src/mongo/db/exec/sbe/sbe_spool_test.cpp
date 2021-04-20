/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */


#include "mongo/db/exec/sbe/values/value.h"
#include "mongo/platform/basic.h"

#include "mongo/db/exec/sbe/sbe_plan_stage_test.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/spool.h"

namespace mongo::sbe {

class SbeSpoolTest : public PlanStageTestFixture {
public:
    SpoolId generateSpoolId() {
        return _spoolIdGenerator.generate();
    }

    /**
     * Constructs the following plan tree:
     *
     *   nlj
     *     limit 1 -> espool -> mock scan
     *     [c|s]spool
     *
     *  In other words, the outer branch spools the mock input collection. The inner branch returns
     *  the data after unspooling it. The inner branch's spool consumer may be either a stack spool
     * or regular (non-stack) spool, depending on the value of the template parameter.
     */
    template <bool IsStack>
    std::pair<value::SlotId, std::unique_ptr<PlanStage>> makeSpoolUnspoolUnionPlan(
        value::SlotId mockScanSlot, std::unique_ptr<PlanStage> mockScanStage) {
        auto spoolId = generateSpoolId();
        auto eagerSpoolProducer = makeS<SpoolEagerProducerStage>(
            std::move(mockScanStage), spoolId, makeSV(mockScanSlot), kEmptyPlanNodeId);

        auto outerBranch =
            makeS<LimitSkipStage>(std::move(eagerSpoolProducer), 1, boost::none, kEmptyPlanNodeId);

        auto spoolOutputSlot = generateSlotId();
        auto spoolConsumer =
            makeS<SpoolConsumerStage<IsStack>>(spoolId, makeSV(spoolOutputSlot), kEmptyPlanNodeId);

        auto loopJoin = makeS<LoopJoinStage>(std::move(outerBranch),
                                             std::move(spoolConsumer),
                                             makeSV(),
                                             makeSV(),
                                             nullptr,
                                             kEmptyPlanNodeId);
        return std::make_pair(spoolOutputSlot, std::move(loopJoin));
    };

private:
    sbe::value::SpoolIdGenerator _spoolIdGenerator;
};

TEST_F(SbeSpoolTest, SpoolEagerProducerBasic) {
    auto inputArray = BSON_ARRAY("a"
                                 << "b"
                                 << "c");
    auto [inputTag, inputVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard inputGuard{inputTag, inputVal};

    // we expect the input to be returned unchanged after being buffered in the spool and then
    // returned in fifo order.
    auto [expectedTag, expectedVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard expectedGuard{expectedTag, expectedVal};

    auto makeStageFn = [this](value::SlotId mockScanSlot,
                              std::unique_ptr<PlanStage> mockScanStage) {
        auto eagerSpoolProducer = makeS<SpoolEagerProducerStage>(
            std::move(mockScanStage), generateSpoolId(), makeSV(mockScanSlot), kEmptyPlanNodeId);
        return std::make_pair(mockScanSlot, std::move(eagerSpoolProducer));
    };

    inputGuard.reset();
    expectedGuard.reset();
    runTest(inputTag, inputVal, expectedTag, expectedVal, makeStageFn);
}

TEST_F(SbeSpoolTest, SpoolLazyProducerBasic) {
    auto inputArray = BSON_ARRAY("a"
                                 << "b"
                                 << "c");
    auto [inputTag, inputVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard inputGuard{inputTag, inputVal};

    // We expect the input to be returned unchanged since it is returned as it is being buffered in
    // the spool
    auto [expectedTag, expectedVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard expectedGuard{expectedTag, expectedVal};

    auto makeStageFn = [this](value::SlotId mockScanSlot,
                              std::unique_ptr<PlanStage> mockScanStage) {
        auto lazySpoolProducer = makeS<SpoolLazyProducerStage>(std::move(mockScanStage),
                                                               generateSpoolId(),
                                                               makeSV(mockScanSlot),
                                                               nullptr,
                                                               kEmptyPlanNodeId);
        return std::make_pair(mockScanSlot, std::move(lazySpoolProducer));
    };

    inputGuard.reset();
    expectedGuard.reset();
    runTest(inputTag, inputVal, expectedTag, expectedVal, makeStageFn);
}


TEST_F(SbeSpoolTest, SpoolAndConsumeNonStack) {
    auto inputArray = BSON_ARRAY("a"
                                 << "b"
                                 << "c");
    auto [inputTag, inputVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard inputGuard{inputTag, inputVal};

    // We expect the input to be returned unchanged after being buffered in the spool and then
    // consumed in FIFO order.
    auto [expectedTag, expectedVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard expectedGuard{expectedTag, expectedVal};

    inputGuard.reset();
    expectedGuard.reset();
    runTest(inputTag,
            inputVal,
            expectedTag,
            expectedVal,
            [this](value::SlotId mockScanSlot, std::unique_ptr<PlanStage> mockScanStage) {
                return makeSpoolUnspoolUnionPlan<false>(mockScanSlot, std::move(mockScanStage));
            });
}

TEST_F(SbeSpoolTest, SpoolAndConsumeStack) {
    auto inputArray = BSON_ARRAY("a"
                                 << "b"
                                 << "c");
    auto [inputTag, inputVal] = stage_builder::makeValue(inputArray);
    value::ValueGuard inputGuard{inputTag, inputVal};

    auto [expectedTag, expectedVal] = stage_builder::makeValue(BSON_ARRAY("c"
                                                                          << "b"
                                                                          << "a"));
    value::ValueGuard expectedGuard{expectedTag, expectedVal};

    inputGuard.reset();
    expectedGuard.reset();
    runTest(inputTag,
            inputVal,
            expectedTag,
            expectedVal,
            [this](value::SlotId mockScanSlot, std::unique_ptr<PlanStage> mockScanStage) {
                return makeSpoolUnspoolUnionPlan<true>(mockScanSlot, std::move(mockScanStage));
            });
}

// TODO -- test eager producer getting fully closed and re-opened
// TODO -- test lazy producer getting fully closed and re-opened
// TODO -- test case where nested loop join gets closed and re-opened with eager producer
// TODO -- test case where nested loop join gets closed and re-opened with lazy producer

}  // namespace mongo::sbe
