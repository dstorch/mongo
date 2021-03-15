/**
 * Tests for queries which specify that comparisons ignore object field order.
 */
(function() {
"use strict";

const coll = db.ignore_object_field_order;
coll.drop();

assert.commandWorked(coll.insert({obj: {a: 1, b: 1}}));
assert.commandWorked(coll.insert({obj: {b: 1, a: 1}}));

// Test that a simple $eq predicate ignores field order when instructed to do so by the client.
assert.eq(1, coll.find({obj: {a: 1, b: 1}}).itcount());
assert.eq(2, coll.find({obj: {a: 1, b: 1}}).ignoreFieldOrder().itcount());
assert.eq(2, coll.find({obj: {b: 1, a: 1}}).ignoreFieldOrder().itcount());
}());
