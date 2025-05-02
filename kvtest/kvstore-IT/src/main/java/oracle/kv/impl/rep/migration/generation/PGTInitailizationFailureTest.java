/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.TopologyUtil;
import oracle.kv.impl.util.PollCondition;

import org.junit.Test;

/**
 * A collection of unit tests that test initialization failure of
 * the partition generation table
 */
public class PGTInitailizationFailureTest extends PartitionGenerationTestBase {

    @Override
    public void setUp() throws Exception {
        trace_on_screen = false;
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }


    /**
     * Tests generation table initialization failure on migration source
     */
    @Test
    public void testSourceInitFailure() {
        testInitFailure(false);
    }

    /**
     * Tests generation table initialization failure on migration target
     */
    @Test
    public void testTargetInitFailure() {
        testInitFailure(true);
    }

    private void testInitFailure(boolean targetFailed) {
        config.startRepNodeServices();
        trace("Store started with topo=" +
              TopologyUtil.getRGIdPartMap(config.getTopology()));

        TestHook<Integer> pmdeHook = arg -> {
            throw new PartitionMDException(
                "PartitionGenMD", "init failure",
                new IllegalArgumentException("test"));
        };

        RepNode source = config.getRN(sourceId);
        RepNode target = config.getRN(targetId);
        RepGroupId srcGroupId = new RepGroupId(sourceId.getGroupId());
        RepGroupId tarGroupId = new RepGroupId(targetId.getGroupId());
        PartitionGenerationTable sourcePgt =
            source.getPartitionManager().getPartGenTable();
        PartitionGenerationTable targetPgt =
            target.getPartitionManager().getPartGenTable();

        RepNode failedRN;
        if (targetFailed) {
            targetPgt.setBeforeInitDoneHook(pmdeHook);
            failedRN = target;
        } else {
            sourcePgt.setBeforeInitDoneHook(pmdeHook);
            failedRN = source;
        }
        trace("Starting migrating partition=" + p1);

        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(p1, srcGroupId)
                           .getPartitionMigrationState());
        trace("Migration status pending");

        boolean success = new PollCondition(100, 10_000) {
            @Override
            protected boolean condition() {

                if (targetFailed) {
                    return failedRN.getMigrationStatus(p1).getState() ==
                           PartitionMigrationState.ERROR;
                }
                return failedRN.getMigrationManager().getMigrationService()
                               .getRequestErrors() > 0;
            }
        }.await();
        assertTrue(success);
        if (targetFailed) {
            trace("Target migration status=" + PartitionMigrationState.ERROR);
        } else {
            trace("Source migration #errors=" + failedRN.getMigrationManager()
                                                        .getMigrationService()
                                                        .getRequestErrors());
        }

        /* initialization should fail, PGT not ready */
        if (targetFailed) {
            assertInitDone(sourcePgt);
            assertInitFailed(targetPgt);
        } else {
            /*
             * When source initialization failed,
             * both ends won't be ready
             */
            assertInitFailed(sourcePgt);
            assertInitFailed(targetPgt);
        }
        trace("PGT status verified");


        /* clear the hook and retry migration should succeed */
        sourcePgt.setBeforeInitDoneHook(null);
        targetPgt.setBeforeInitDoneHook(null);
        target.canCancel(p1);
        source.canceled(p1, tarGroupId);
        migratePartition(p1, sourceId, targetId);

        /* partition migrated, opened on target */
        assertInitDone(sourcePgt);
        assertInitDone(targetPgt);
        assertNotNull(targetPgt.getOpenGen(p1));
        trace("Test completed");
    }

    private void assertInitFailed(PartitionGenerationTable pgt) {
        assertFalse(pgt.isReady());
        assertFalse(pgt.isTableInitBefore());
    }

    private void assertInitDone(PartitionGenerationTable pgt) {
        assertTrue(pgt.isReady());
        assertTrue(pgt.isTableInitBefore());
    }
}
