/**
 * Confirms that the expected values are reported for the 'nFiltered' metric in the system.profile
 * collection.
 *
 * @tags: [
 *   does_not_support_stepdowns,
 *   requires_getmore,
 *   requires_profiling,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/analyze_plan.js");
load("jstests/libs/profiler.js");

const testDb = db.getSiblingDB("profile_nfiltered");
assert.commandWorked(testDb.dropDatabase());
const coll = testDb.getCollection("test");
const comment = "profile_nfiltered_comment";
const profileEntryFilter = {
    "command.comment": {$eq: comment}
};

for (let i = 0; i < 10; ++i) {
    assert.commandWorked(coll.insert({_id: i, a: i, b: i, c: i, arr: [1, 2]}));
}

assert.commandWorked(coll.createIndex({a: 1}));
assert.commandWorked(coll.createIndex({b: 1}));

assert.commandWorked(testDb.setProfilingLevel(2));

function assertSummaryMetrics({
    keysExamined: keysExamined,
    docsExamined: docsExamined,
    nFiltered: nFiltered,
    nReturned: nReturned
}) {
    const profileObj = getLatestProfilerEntry(testDb, profileEntryFilter);
    if (keysExamined)
        assert.eq(profileObj.keysExamined, keysExamined);
    if (docsExamined)
        assert.eq(profileObj.docsExamined, docsExamined);
    if (nFiltered)
        assert.eq(profileObj.nFiltered, nFiltered);
    if (nReturned)
        assert.eq(profileObj.nreturned, nReturned);
}

// Run a COLLSCAN. It should examine all 10 documents, filter out 3, and return 7.
assert.eq(7, coll.find({a: {$lt: 7}}).comment(comment).hint({$natural: 1}).itcount());
assertSummaryMetrics({keysExamined: 0, docsExamined: 10, nFiltered: 3, nReturned: 7});

// Repeat the test, but this time for an agg operation.
assert.eq(
    7,
    coll.aggregate([{$match: {a: {$lt: 7}}}], {comment: comment, hint: {$natural: 1}}).itcount());
assertSummaryMetrics({keysExamined: 0, docsExamined: 10, nFiltered: 3, nReturned: 7});

// Match both before and after a $group. Only the $match prior to grouping should influence the
// 'nFiltered' value.
assert.eq(0,
          coll.aggregate(
                  [
                      {$match: {a: {$lt: 7}}},
                      {$group: {_id: null, count: {$sum: 1}}},
                      {$match: {count: {$gt: 10}}}
                  ],
                  {comment: comment, hint: {$natural: 1}})
              .itcount());
assertSummaryMetrics({keysExamined: 0, docsExamined: 10, nFiltered: 3, nReturned: 0});

// Match both before and after an $unwind. Only the $match prior to grouping should influence the
// 'nFiltered' value.
assert.eq(7,
          coll.aggregate([{$match: {a: {$lt: 7}}}, {$unwind: "$arr"}, {$match: {arr: {$eq: 2}}}],
                         {comment: comment, hint: {$natural: 1}})
              .itcount());
assertSummaryMetrics({keysExamined: 0, docsExamined: 10, nFiltered: 3, nReturned: 7});

// Test a query that should use an indexed plan, where some documents are filtered out after being
// fetched.
assert.eq(4, coll.find({a: {$lt: 7}, b: {$lt: 4}}).comment(comment).hint({a: 1}).itcount());
assertSummaryMetrics({docsExamined: 7, nFiltered: 3, nReturned: 4});

// Test a query where the filter can be attached to the IXSCAN node itself.
assert.eq(4, coll.find({a: {$lt: 7, $mod: [2, 0]}}).comment(comment).hint({a: 1}).itcount());
assertSummaryMetrics({docsExamined: 4, nFiltered: 3, nReturned: 4});

// Test an indexed $or query where the branches produce duplicates that need to be eliminated, after
// which filtering takes place.
assert.eq(4,
          coll.find({$or: [{a: {$lt: 7}}, {b: {$lt: 7}}], c: {$lt: 4}}).comment(comment).itcount());
let explain = coll.find({$or: [{a: {$lt: 7}}, {b: {$lt: 7}}], c: {$lt: 4}}).explain();
assert(planHasStage(db, explain, "OR"), explain);
assertSummaryMetrics({docsExamined: 7, nFiltered: 3, nReturned: 4});

// TODO text query
// TODO getMore
// TODO count
// TODO distinct
// TODO findAndModify
// TODO update
// TODO delete
}());
