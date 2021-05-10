'use strict';

/**
 * Concurrently performs DDL commands and FCV changes and verifies guarantees are
 * not broken.
 *
 * @tags: [
 *   requires_sharding,
 *   # TODO (SERVER-54881): ensure the new DDL paths work with balancer, autosplit
 *   # and causal consistency.
 *   assumes_balancer_off,
 *   assumes_autosplit_off,
 *   does_not_support_causal_consistency,
 *   # TODO (SERVER-54881): ensure the new DDL paths work with add/remove shards
 *   does_not_support_add_remove_shards,
 *   # TODO (SERVER-56789) Enable stepdown on DDL FSM workloads
 *   does_not_support_stepdowns,
 *   # Can be removed once PM-1965-Milestone-1 is completed.
 *   does_not_support_transactions,
 *   # Requires all nodes to be running the latest binary.
 *   multiversion_incompatible,
 *  ]
 */

load('jstests/concurrency/fsm_libs/extend_workload.js');
load('jstests/concurrency/fsm_workloads/random_DDL_CRUD_operations.js');
load("jstests/libs/override_methods/mongos_manual_intervention_actions.js");

var $config = extendWorkload($config, function($config, $super) {
    $config.states.setFCV = function(db, collName, connCache) {
        const fcvValues = [lastLTSFCV, lastContinuousFCV, latestFCV];
        const targetFCV = fcvValues[Random.randInt(3)];
        jsTestLog('Executing FCV state, setting to:' + targetFCV);
        try {
            assertAlways.commandWorked(
                db.adminCommand({setFeatureCompatibilityVersion: targetFCV}));
        } catch (e) {
            if (e.code === 5147403) {
                // Invalid fcv transition (e.g lastContinuous -> lastLTS)
                jsTestLog('setFCV: Invalid transition');
                return;
            }
            throw e;
        }
        jsTestLog('setFCV state finished');
    };

    $config.states.create = function(db, collName, connCache) {
        assert.soon(() => {
            try {
                $super.states.create.apply(this, arguments);
                return true;
            } catch (e) {
                if (e.code === ErrorCodes.ConflictingOperationInProgress) {
                    // Legacy dropCollection interferes with catalog cache refreshes. Retry.
                    // TODO SERVER-54879: No longer needed after 5.0 has branched out
                    return false;
                }
                throw e;
            }
        });
    };

    $config.states.rename = function(db, collName, connCache) {
        try {
            $super.states.rename.apply(this, arguments);
        } catch (e) {
            if (e.code === ErrorCodes.IllegalOperation) {
                // This is expected when attempting to rename a sharded collection in FCV prior to
                // 5.0
                // TODO SERVER-54879: No longer needed after 5.0 has branched out
                return;
            }
            throw e;
        }
    };

    $config.transitions = {
        init: {create: 0.30, drop: 0.30, rename: 0.30, setFCV: 0.10},
        create: {create: 0.30, drop: 0.30, rename: 0.30, setFCV: 0.10},
        drop: {create: 0.30, drop: 0.30, rename: 0.30, setFCV: 0.10},
        rename: {create: 0.30, drop: 0.30, rename: 0.30, setFCV: 0.10},
        setFCV: {create: 0.30, drop: 0.30, rename: 0.30, setFCV: 0.10}
    };

    $config.teardown = function(db, collName, cluster) {
        assert.commandWorked(db.adminCommand({setFeatureCompatibilityVersion: latestFCV}));
    };

    return $config;
});
