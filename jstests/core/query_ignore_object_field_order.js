/**
 * Tests for queries which specify that comparisons ignore object field order.
 */
(function() {
"use strict";

const coll = db.ignore_object_field_order;
coll.drop();

assert.commandWorked(coll.insert({obj: {a: 1, b: 1}}));
assert.commandWorked(coll.insert({obj: {b: 1, a: 1}}));
assert.commandWorked(coll.insert({obj: {a: 1, b: 1, c: 1}}));

// Test that a simple $eq predicate ignores field order when instructed to do so by the client.
assert.eq(1, coll.find({obj: {a: 1, b: 1}}).itcount());
assert.eq(
    2,
    coll.find({obj: {a: 1, b: 1}}).collation({locale: "simple", ignoreFieldOrder: true}).itcount());
assert.eq(
    2,
    coll.find({obj: {b: 1, a: 1}}).collation({locale: "simple", ignoreFieldOrder: true}).itcount());

// Repeat the above test but use $expr.
assert.eq(1, coll.find({$expr: {$eq: ["$obj", {$literal: {a: 1, b: 1}}]}}).itcount());
assert.eq(2,
          coll.find({$expr: {$eq: ["$obj", {$literal: {a: 1, b: 1}}]}})
              .collation({locale: "simple", ignoreFieldOrder: true})
              .itcount());
assert.eq(2,
          coll.find({$expr: {$eq: ["$obj", {$literal: {b: 1, a: 1}}]}})
              .collation({locale: "simple", ignoreFieldOrder: true})
              .itcount());

// Test that the aggregate command $match and $project stages respect the unordered fields
// collation.
assert.eq(2,
          coll.aggregate([{$match: {obj: {a: 1, b: 1}}}],
                         {collation: {locale: "simple", ignoreFieldOrder: true}})
              .itcount());
assert.eq(2,
          coll.aggregate(
                  [
                      {$project: {isEqual: {$eq: ["$obj", {$literal: {a: 1, b: 1}}]}}},
                      {$match: {isEqual: {$eq: true}}}
                  ],
                  {collation: {locale: "simple", ignoreFieldOrder: true}})
              .itcount());

// Cannot use 'ignoreFieldOrder' against a view if the view itself is not an 'ignoreFieldOrder'
// view.
const viewName = "ignore_object_field_order_view";
assert.commandWorked(db.createView(viewName, coll.getName(), []));
const view = db[viewName];
assert.eq(1, view.find({obj: {a: 1, b: 1}}).itcount());
let error = assert.throws(() => view.find({obj: {a: 1, b: 1}})
                                    .collation({locale: "simple", ignoreFieldOrder: true})
                                    .itcount());
assert.commandFailedWithCode(error, ErrorCodes.OptionNotSupportedOnView);
}());
