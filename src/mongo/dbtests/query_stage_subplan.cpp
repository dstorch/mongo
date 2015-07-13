/**
 *    Copyright (C) 2014 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/subplan.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/dbtests/dbtests.h"

namespace QueryStageSubplan {

class QueryStageSubplanBase {
public:
    QueryStageSubplanBase() : _client(&_txn) {}

    virtual ~QueryStageSubplanBase() {
        Client::WriteContext ctx(&_txn, ns());
        _client.dropCollection(ns());
    }

    void addIndex(const BSONObj& obj) {
        ASSERT_OK(dbtests::createIndex(&_txn, ns(), obj));
    }

    void insert(const BSONObj& doc) {
        _client.insert(ns(), doc);
    }

    static const char* ns() {
        return "unittests.QueryStageSubplan";
    }

protected:
    /**
     * Parses the json string 'findCmd', specifying a find command, to a CanonicalQuery.
     */
    std::unique_ptr<CanonicalQuery> cqFromFindCommand(const std::string& findCmd) {
        BSONObj cmdObj = fromjson(findCmd);

        const NamespaceString nss("testns.testcoll");
        bool isExplain = false;
        LiteParsedQuery* rawLpq;
        Status lpqStatus = LiteParsedQuery::make(nss, cmdObj, isExplain, &rawLpq);
        ASSERT_OK(lpqStatus);
        std::unique_ptr<LiteParsedQuery> lpq(rawLpq);

        CanonicalQuery* rawCq;
        Status cqStatus = CanonicalQuery::canonicalize(lpq.release(), &rawCq);
        ASSERT_OK(cqStatus);

        return std::unique_ptr<CanonicalQuery>(rawCq);
    }

    OperationContextImpl _txn;

private:
    DBDirectClient _client;
};

/**
 * SERVER-15012: test that the subplan stage does not crash when the winning solution
 * for an $or clause uses a '2d' index. We don't produce cache data for '2d'. The subplanner
 * should gracefully fail after finding that no cache data is available, allowing us to fall
 * back to regular planning.
 */
class QueryStageSubplanGeo2dOr : public QueryStageSubplanBase {
public:
    void run() {
        Client::WriteContext ctx(&_txn, ns());
        addIndex(BSON("a"
                      << "2d"
                      << "b" << 1));
        addIndex(BSON("a"
                      << "2d"));

        BSONObj query = fromjson(
            "{$or: [{a: {$geoWithin: {$centerSphere: [[0,0],10]}}},"
            "{a: {$geoWithin: {$centerSphere: [[1,1],10]}}}]}");

        CanonicalQuery* rawCq;
        ASSERT_OK(CanonicalQuery::canonicalize(ns(), query, &rawCq));
        boost::scoped_ptr<CanonicalQuery> cq(rawCq);

        Collection* collection = ctx.getCollection();

        // Get planner params.
        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_txn, collection, cq.get(), &plannerParams);

        WorkingSet ws;
        boost::scoped_ptr<SubplanStage> subplan(
            new SubplanStage(&_txn, collection, &ws, plannerParams, cq.get()));

        // Null means that 'subplan' will not yield during plan selection. Plan selection
        // should succeed due to falling back on regular planning.
        ASSERT_OK(subplan->pickBestPlan(nullptr));
    }
};

/**
 * Test the SubplanStage's ability to plan an individual branch using the plan cache.
 */
class QueryStageSubplanPlanFromCache : public QueryStageSubplanBase {
public:
    void run() {
        Client::WriteContext ctx(&_txn, ns());

        addIndex(BSON("a" << 1 << "b" << 1));
        addIndex(BSON("a" << 1 << "c" << 1));

        for (int i = 0; i < 10; i++) {
            insert(BSON("a" << 1 << "b" << i << "c" << i));
        }

        // This query should result in a plan cache entry for the first branch. The second
        // branch should tie, meaning that nothing is inserted into the plan cache.
        BSONObj query = fromjson("{$or: [{a: 1, b: 3}, {a: 1}]}");

        Collection* collection = ctx.getCollection();

        CanonicalQuery* rawCq;
        ASSERT_OK(CanonicalQuery::canonicalize(ns(), query, &rawCq));
        boost::scoped_ptr<CanonicalQuery> cq(rawCq);

        // Get planner params.
        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_txn, collection, cq.get(), &plannerParams);

        WorkingSet ws;
        boost::scoped_ptr<SubplanStage> subplan(
            new SubplanStage(&_txn, collection, &ws, plannerParams, cq.get()));

        // Null means that 'subplan' should not yield during plan selection.
        ASSERT_OK(subplan->pickBestPlan(nullptr));

        // Nothing is in the cache yet, so neither branch should have been planned from
        // the plan cache.
        ASSERT_FALSE(subplan->branchPlannedFromCache(0));
        ASSERT_FALSE(subplan->branchPlannedFromCache(1));

        // If we repeat the same query, then the first branch should come from the cache,
        // but the second is re-planned due to tying on the first run.
        ws.clear();
        subplan.reset(new SubplanStage(&_txn, collection, &ws, plannerParams, cq.get()));

        ASSERT_OK(subplan->pickBestPlan(nullptr));

        ASSERT_TRUE(subplan->branchPlannedFromCache(0));
        ASSERT_FALSE(subplan->branchPlannedFromCache(1));
    }
};

/**
 * Unit test the subplan stage's canUseSubplanning() method.
 */
class QueryStageSubplanCanUseSubplanning : public QueryStageSubplanBase {
public:
    void run() {
        // We won't try and subplan something that doesn't have an $or.
        {
            std::string findCmd = "{find: 'testns', filter: {$and:[{a:1}, {b:1}]}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Don't try and subplan if there is no filter.
        {
            std::string findCmd = "{find: 'testns'}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // We won't try and subplan two contained ORs.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or:[{a:1}, {b:1}], $or:[{c:1}, {d:1}], e:1}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning if there is a hint.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or: [{a:1, b:1}, {c:1, d:1}]},"
                "hint: {a:1, b:1}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning with min.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or: [{a:1, b:1}, {c:1, d:1}]},"
                "options: {min: {a:1, b:1}}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning with max.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or: [{a:1, b:1}, {c:1, d:1}]},"
                "options: {max: {a:2, b:2}}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning with tailable.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or: [{a:1, b:1}, {c:1, d:1}]},"
                "options: {tailable: true}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning with snapshot.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or: [{a:1, b:1}, {c:1, d:1}]},"
                "options: {snapshot: true}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can use subplanning for rooted $or.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$or: [{a:1, b:1}, {c:1, d:1}]}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_TRUE(SubplanStage::canUseSubplanning(*cq));

            std::string findCmd2 =
                "{find: 'testns',"
                "filter: {$or: [{a:1}, {c:1}]}}";
            std::unique_ptr<CanonicalQuery> cq2 = cqFromFindCommand(findCmd2);
            ASSERT_TRUE(SubplanStage::canUseSubplanning(*cq2));
        }

        // Can use subplanning for a single contained $or.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {e: 1, $or: [{a:1, b:1}, {c:1, d:1}]}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_TRUE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can use subplanning if the contained $or query has a geo predicate.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {loc: {$geoWithin: {$centerSphere: [[0,0], 1]}},"
                "e: 1, $or: [{a:1, b:1}, {c:1, d:1}]}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_TRUE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning if the contained $or query also has a $text predicate.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {$text: {$search: 'foo'},"
                "e: 1, $or: [{a:1, b:1}, {c:1, d:1}]}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }

        // Can't use subplanning if the contained $or query also has a $near predicate.
        {
            std::string findCmd =
                "{find: 'testns',"
                "filter: {loc: {$near: [0, 0]},"
                "e: 1, $or: [{a:1, b:1}, {c:1, d:1}]}}";
            std::unique_ptr<CanonicalQuery> cq = cqFromFindCommand(findCmd);
            ASSERT_FALSE(SubplanStage::canUseSubplanning(*cq));
        }
    }
};

/**
 * Unit test the subplan stage's rewriteToRootedOr() method.
 */
class QueryStageSubplanRewriteToRootedOr : public QueryStageSubplanBase {
public:
    void run() {
        // Rewrite (AND (OR a b) e) => (OR (AND a e) (AND b e))
        {
            BSONObj queryObj = fromjson("{$or:[{a:1}, {b:1}], e:1}");
            StatusWithMatchExpression statusWithExpr = MatchExpressionParser::parse(queryObj);
            ASSERT_OK(statusWithExpr.getStatus());
            std::unique_ptr<MatchExpression> expr(statusWithExpr.getValue());
            std::unique_ptr<MatchExpression> rewrittenExpr =
                SubplanStage::rewriteToRootedOr(std::move(expr));

            std::string findCmdRewritten =
                "{find: 'testns',"
                "filter: {$or:[{a:1,e:1}, {b:1,e:1}]}}";
            std::unique_ptr<CanonicalQuery> cqRewritten = cqFromFindCommand(findCmdRewritten);

            ASSERT(rewrittenExpr->equivalent(cqRewritten->root()));
        }

        // Rewrite (AND (OR a b) e f) => (OR (AND a e f) (AND b e f))
        {
            BSONObj queryObj = fromjson("{$or:[{a:1}, {b:1}], e:1, f:1}");
            StatusWithMatchExpression statusWithExpr = MatchExpressionParser::parse(queryObj);
            ASSERT_OK(statusWithExpr.getStatus());
            std::unique_ptr<MatchExpression> expr(statusWithExpr.getValue());
            std::unique_ptr<MatchExpression> rewrittenExpr =
                SubplanStage::rewriteToRootedOr(std::move(expr));

            std::string findCmdRewritten =
                "{find: 'testns',"
                "filter: {$or:[{a:1,e:1,f:1}, {b:1,e:1,f:1}]}}";
            std::unique_ptr<CanonicalQuery> cqRewritten = cqFromFindCommand(findCmdRewritten);

            ASSERT(rewrittenExpr->equivalent(cqRewritten->root()));
        }

        // Rewrite (AND (OR (AND a b) (AND c d) e f) => (OR (AND a b e f) (AND c d e f))
        {
            BSONObj queryObj = fromjson("{$or:[{a:1,b:1}, {c:1,d:1}], e:1,f:1}");
            StatusWithMatchExpression statusWithExpr = MatchExpressionParser::parse(queryObj);
            ASSERT_OK(statusWithExpr.getStatus());
            std::unique_ptr<MatchExpression> expr(statusWithExpr.getValue());
            std::unique_ptr<MatchExpression> rewrittenExpr =
                SubplanStage::rewriteToRootedOr(std::move(expr));

            std::string findCmdRewritten =
                "{find: 'testns',"
                "filter: {$or:[{a:1,b:1,e:1,f:1},"
                "{c:1,d:1,e:1,f:1}]}}";
            std::unique_ptr<CanonicalQuery> cqRewritten = cqFromFindCommand(findCmdRewritten);

            ASSERT(rewrittenExpr->equivalent(cqRewritten->root()));
        }
    }
};

/**
 * Test the subplan stage's ability to answer a contained $or query.
 */
class QueryStageSubplanPlanContainedOr : public QueryStageSubplanBase {
public:
    void run() {
        Client::WriteContext ctx(&_txn, ns());
        addIndex(BSON("b" << 1 << "a" << 1));
        addIndex(BSON("c" << 1 << "a" << 1));

        BSONObj query = fromjson("{a: 1, $or: [{b: 2}, {c: 3}]}");

        // Two of these documents match.
        insert(BSON("_id" << 1 << "a" << 1 << "b" << 2));
        insert(BSON("_id" << 2 << "a" << 2 << "b" << 2));
        insert(BSON("_id" << 3 << "a" << 1 << "c" << 3));
        insert(BSON("_id" << 4 << "a" << 1 << "c" << 4));

        std::unique_ptr<CanonicalQuery> cq;
        {
            CanonicalQuery* rawCq;
            Status canonStatus = CanonicalQuery::canonicalize(ns(), query, &rawCq);
            ASSERT_OK(canonStatus);
            cq.reset(rawCq);
        }

        Collection* collection = ctx.getCollection();

        // Get planner params.
        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_txn, collection, cq.get(), &plannerParams);

        WorkingSet ws;
        std::unique_ptr<SubplanStage> subplan(
            new SubplanStage(&_txn, collection, &ws, plannerParams, cq.get()));

        // Plan selection should succeed due to falling back on regular planning.
        ASSERT_OK(subplan->pickBestPlan(nullptr));

        // Work the stage until it produces all results.
        size_t numResults = 0;
        PlanStage::StageState stageState = PlanStage::NEED_TIME;
        while (stageState != PlanStage::IS_EOF) {
            WorkingSetID id = WorkingSet::INVALID_ID;
            stageState = subplan->work(&id);
            ASSERT_NE(stageState, PlanStage::DEAD);
            ASSERT_NE(stageState, PlanStage::FAILURE);

            if (stageState == PlanStage::ADVANCED) {
                ++numResults;
                WorkingSetMember* member = ws.get(id);
                ASSERT(member->hasObj());
                ASSERT(member->obj.value() == BSON("_id" << 1 << "a" << 1 << "b" << 2) ||
                       member->obj.value() == BSON("_id" << 3 << "a" << 1 << "c" << 3));
            }
        }

        ASSERT_EQ(numResults, 2U);
    }
};

/**
 * Test the subplan stage's ability to answer a rooted $or query with a $ne and a sort.
 */
class QueryStageSubplanPlanRootedOrNE : public QueryStageSubplanBase {
public:
    void run() {
        Client::WriteContext ctx(&_txn, ns());
        addIndex(BSON("a" << 1 << "b" << 1));
        addIndex(BSON("a" << 1 << "c" << 1));

        // Every doc matches.
        insert(BSON("_id" << 1 << "a" << 1));
        insert(BSON("_id" << 2 << "a" << 2));
        insert(BSON("_id" << 3 << "a" << 3));
        insert(BSON("_id" << 4));

        BSONObj query = fromjson("{$or: [{a: 1}, {a: {$ne:1}}]}");
        BSONObj sort = BSON("d" << 1);
        BSONObj projection;

        std::unique_ptr<CanonicalQuery> cq;
        {
            CanonicalQuery* rawCq;
            Status canonStatus =
                CanonicalQuery::canonicalize(ns(), query, sort, projection, &rawCq);
            ASSERT_OK(canonStatus);
            cq.reset(rawCq);
        }

        Collection* collection = ctx.getCollection();

        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_txn, collection, cq.get(), &plannerParams);

        WorkingSet ws;
        std::unique_ptr<SubplanStage> subplan(
            new SubplanStage(&_txn, collection, &ws, plannerParams, cq.get()));

        ASSERT_OK(subplan->pickBestPlan(nullptr));

        size_t numResults = 0;
        PlanStage::StageState stageState = PlanStage::NEED_TIME;
        while (stageState != PlanStage::IS_EOF) {
            WorkingSetID id = WorkingSet::INVALID_ID;
            stageState = subplan->work(&id);
            ASSERT_NE(stageState, PlanStage::DEAD);
            ASSERT_NE(stageState, PlanStage::FAILURE);
            if (stageState == PlanStage::ADVANCED) {
                ++numResults;
            }
        }

        ASSERT_EQ(numResults, 4U);
    }
};

class All : public Suite {
public:
    All() : Suite("query_stage_subplan") {}

    void setupTests() {
        add<QueryStageSubplanGeo2dOr>();
        add<QueryStageSubplanPlanFromCache>();
        add<QueryStageSubplanCanUseSubplanning>();
        add<QueryStageSubplanRewriteToRootedOr>();
        add<QueryStageSubplanPlanContainedOr>();
        add<QueryStageSubplanPlanRootedOrNE>();
    }
};

SuiteInstance<All> all;

}  // namespace QueryStageSubplan
