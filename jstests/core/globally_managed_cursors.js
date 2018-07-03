// Test features related to globally managed cursors.
//
// TODO: This test should be broken up into different integration tests for the different relevant
// cases (queries surviving renames, queries surviving index drops, index drop/rebuild, etc.).
(function() {
    "use strict";

    db.c.drop();
    db.d.drop();

    print("test drop collection");
    assert.writeOK(db.c.insert([{a: 1}, {a: 2}, {a: 3}]));
    let cursor = assert.commandWorked(db.runCommand({find: "c", batchSize: 2}));
    db.c.drop();
    assert.commandFailed(db.runCommand({getMore: cursor.cursor.id, collection: "c"}));

    print("test rename collection");
    assert.writeOK(db.c.insert([{a: 1}, {a: 2}, {a: 3}]));
    cursor = assert.commandWorked(db.runCommand({find: "c", batchSize: 2}));
    printjson(cursor);
    assert.commandWorked(db.c.renameCollection("d"));
    assert.commandWorked(db.runCommand({getMore: cursor.cursor.id, collection: "c"}));

    print("test drop same index");
    db.c.drop();
    db.d.drop();
    assert.writeOK(db.c.insert([{a: 1}, {a: 2}, {a: 3}]));
    assert.commandWorked(db.c.createIndex({a: 1}));
    cursor = assert.commandWorked(db.runCommand({find: "c", batchSize: 2, hint: {a: 1}}));
    assert.commandWorked(db.c.dropIndex({a: 1}));
    assert.commandFailedWithCode(db.runCommand({getMore: cursor.cursor.id, collection: "c"}),
                                 ErrorCodes.IndexNotFound);

    print("test drop different index");
    db.c.drop();
    assert.writeOK(db.c.insert([{a: 1}, {a: 2}, {a: 3}]));
    assert.commandWorked(db.c.createIndex({a: 1}));
    assert.commandWorked(db.c.createIndex({b: 1}));
    cursor = assert.commandWorked(db.runCommand({find: "c", batchSize: 2, hint: {a: 1}}));
    assert.commandWorked(db.c.dropIndex({b: 1}));
    assert.commandWorked(db.runCommand({getMore: cursor.cursor.id, collection: "c"}));

    print("test drop and recreate index");
    db.c.drop();
    assert.writeOK(db.c.insert([{a: 1}, {a: 2}, {a: 3}]));
    assert.commandWorked(db.c.createIndex({a: 1}));
    cursor = assert.commandWorked(db.runCommand({find: "c", batchSize: 2, hint: {a: 1}}));
    assert.commandWorked(db.c.dropIndex({a: 1}));
    assert.commandWorked(db.c.createIndex({a: 1}));
    assert.commandFailedWithCode(db.runCommand({getMore: cursor.cursor.id, collection: "c"}),
                                 ErrorCodes.QueryPlanKilled);
}());
