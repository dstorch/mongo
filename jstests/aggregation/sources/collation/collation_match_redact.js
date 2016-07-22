// Test that the $match and $redact stages respect the collation.
(function() {
    "use strict";

    var caseInsensitive = {collation: {locale: "en_US", strength: 2}};

    var coll = db.collation_match;
    coll.drop();
    assert.writeOK(coll.insert({a: "a"}));

    // Test that the $match respects an explicit collation when it can be pushed down into the query
    // layer.
    assert.eq(1, coll.aggregate([{$match: {a: "A"}}], caseInsensitive).itcount());

    // Test that the $match respects an explicit collation when it cannot be pushed down into the
    // query layer.
    assert.eq(
        1, coll.aggregate([{$project: {b: "B"}}, {$match: {b: "b"}}], caseInsensitive).itcount());

    // Test that $redact respects an explicit collation. Since the top-level of the document gets
    // pruned, we end up redacting the entire document and returning no results.
    assert.eq(0,
              coll.aggregate([{$redact: {$cond: [{$eq: ["A", "a"]}, "$$PRUNE", "$$KEEP"]}}],
                             caseInsensitive)
                  .itcount());

    coll.drop();
    assert.commandWorked(db.createCollection(coll.getName(), caseInsensitive));
    assert.writeOK(coll.insert({a: "a"}));

    // Test that the $match respects the inherited collation when it can be pushed down into the
    // query layer.
    assert.eq(1, coll.aggregate([{$match: {a: "A"}}]).itcount());

    // Test that the $match respects the inherited collation when it cannot be pushed down into the
    // query layer.
    assert.eq(1, coll.aggregate([{$project: {b: "B"}}, {$match: {b: "b"}}]).itcount());

    // Test that $redact respects the inherited collation. Since the top-level of the document gets
    // pruned, we end up redacting the entire document and returning no results.
    assert.eq(
        0,
        coll.aggregate([{$redact: {$cond: [{$eq: ["A", "a"]}, "$$PRUNE", "$$KEEP"]}}]).itcount());
})();
