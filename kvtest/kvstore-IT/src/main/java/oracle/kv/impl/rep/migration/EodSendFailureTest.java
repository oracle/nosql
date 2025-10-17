/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTestBase;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;

import org.junit.Test;

/**
 * The unit test case tests the scenario of failure in partition migration
 * where the failure occurs after the local topology of the source shard has
 * been updated, but the EOD is not sent. The source shard is no longer the
 * owner of the partition, and due to the failure, the target shard does not
 * receive the EOD and cannot take ownership of the migrating partition by
 * updating its local topology. As a result, the partition is not owned by
 * either the source shard or the target shard. [KVSTORE-456]
 *
 * The resolution to this issue is to set the migration state to ERROR instead
 * of PENDING on the migration target. The ERROR state is received by the
 * {@link TargetMonitorExecutor}, which fails the migration and removes the
 * source migration record. This rollback restores the previous topology and
 * transfers ownership of the partition back to the source shard.
 *
 *
 * Partition migration state flow before fix :
 * PENDING --> RUNNING --> PENDING
 *
 * Partition migration state flow after fix :
 * PENDING --> RUNNING --> ERROR
 *
 * How the unit test works:
 * There are three test hooks used in this test.
 *
 * - MigrationSource.eodSendFailureHook:
 *   Fails sending the EOD at the source side after persisting the migration
 *   record and updating the local topology.
 *
 * - TargetMonitorExecutor.assertRemoveRecordHook:
 *   Checks whether the TargetMonitorExecutor#failed method calls
 *   manager.removeRecord.
 *
 * - MigrationSource.noMonitorTargetHook:
 *   Makes the source thread exit after MigrationSource#persistTransferComplete
 *   without starting the TargetMonitorExecutor.
 *
 * Steps:
 * 1. Replication node service is started.
 * 2. Data is added to the store because partition migration can have different
 *    flows with or without data in the store.
 * 3. Set the noMonitorTargetHook and eodSendFailureHook.
 *
 *
 * 4. Start the migration for the very first time.
 * 5. Verify migration failure due to EOD send failure on the source side,
 *    and canceling the migration at the target by calling the cancel method,
 *    resulting in the partition migration state to ERROR.
 * 6. The cancel method calls setCanceled, which sets the canceled flag to true
 *    and calls cleanup. The cleanup method closes the channel and removes the
 *    partition db at the target.
 * 7. Since the Reader thread dropped the partition, executing an operation on
 *    the partition db results in:
 *    "java.lang.IllegalStateException: Database was closed."
 * 8. There is no try/catch in consumeOps, so this IllegalStateException (which
 *    in the earlier flow was catching an IOException) propagates to
 *    runMigration, which catches it in its catch Exception block.
 * 9. This catch Exception block calls error(), which calls setCanceled and then
 *    manager.removeRecord, removing the target migration record from the
 *    migration db.
 * 10. The finally block in runMigration returns to its caller,
 *     MigrationTarget.call.
 * 11. Since waitTime < 0, control breaks out of the while loop, and
 *     MigrationTarget's call method returns null.
 * 12. TargetExecutor's afterExecute will not re-schedule the target since null
 *     was returned by the target thread.
 * 13. We are querying for the migration status, calling
 *     MigrationManager.getMigrationState, which detects that the target is null
 *     and TargetRecord is null (because manager.removeRecord was called). So,
 *     getMigrationState returns UNKNOWN repeatedly for a minute.
 * 14. Even though the source thread has exited, the TargetMonitor thread in the
 *     source checks the completed source migration record and sees its
 *     corresponding target state as UNKNOWN.
 * 15. After **1 minute**, the migratePartition task is re-scheduled/re-run.
 * 16. The partition migration state change to ERROR is not recognized by the
 *     TargetMonitorExecutor because it was not initialized.
 * 17. Assert that the partition was not owned by the source or target shard.
 *
 *
 * 18. Unset the noMonitorTargetHook.
 * 19. Start partition migration for the second time. Since noMonitorTargetHook
 *     is unset, TargetMonitorExecutor sees the target state as ERROR, leading
 *     to the failed method being called. The failed method calls
 *     manager.removeRecord, removing the source migration record and updating
 *     the local topology to rollback ownership of the partition to the
 *     source shard.
 * 20. The assertRemoveRecordHook confirms that manager.removeRecord was called,
 *     indicating that the code flow has changed.
 * 21. Assert that the partition was now owned by the source shard and not the
 *     target shard, confirming that the fix worked.
 *
 *
 * 22. Unset the eodSendFailureHook.
 * 23. Start the partition migration for third time. Since no error was
 *     injected, the migration completes normally.
 * 24. Assert that the partition is now owned by the target shard and not the
 *     source shard.
 */

public class EodSendFailureTest extends PartitionGenerationTestBase {

    private static final PartitionId PARTITION_ID = p1;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        MigrationSource.eodSendFailureHook = null;
        MigrationSource.noMonitorTargetHook = null;
        TargetMonitorExecutor.checkRemoveRecordHook = null;
        super.tearDown();
    }

    @Test
    public void testEodSendFailure() {
        /* RepNode service is started */
        config.startRepNodeServices();

        /* Data is added in the store */
        addDataInStore();

        /* Setting the hooks required in the first migration run */
        setNoMonitorTargetTestHook();
        setEodSendFailureTestHook();

        /* Start the migration for the first time */
        startMigration(sourceId, targetId);
        trace("Migration started for pid=" + PARTITION_ID);

        /* This will set the migration state to ERROR. But since the
         * TargetMonitorExecutor is not initialised this will not be seen by it
         * and it will go through another flow as described above steps.
         */
        verifyMigrationFailure(sourceId, targetId);
        trace("Migration failed for pid=" + PARTITION_ID);

        MigrationSource.noMonitorTargetHook = null;
        trace("No monitor target hook unset");

        setCheckRemoveRecordTestHook();

        /* Start the migration for the second time */
        startMigration(sourceId, targetId);
        trace("Migration started for pid=" + PARTITION_ID);

        /* This will set the migration state to ERROR. Since the
         * TargetMonitorExecutor is initialised this will be seen by it
         * and it will restore the topology of the source shard transferring
         * back the ownership of the partition
         */
        verifyMigrationFailure(sourceId, targetId);
        trace("Migration failed for pid=" + PARTITION_ID);

        MigrationSource.eodSendFailureHook = null;
        trace("Eod send failure hook unset");

        /* Start the migration for the third time */
        startMigration(sourceId, targetId);
        trace("Migration started for pid=" + PARTITION_ID);

        verifyMigrationComplete(sourceId, targetId);
        trace("Done migrating pid=" + PARTITION_ID +
              " from shard=" + sourceId + " to shard=" + targetId);
    }

    private void setCheckRemoveRecordTestHook() {
        TargetMonitorExecutor.checkRemoveRecordHook = pid1 -> {
            if (!PARTITION_ID.equals(pid1)) {
                /* not my partition */
                return;
            }
            trace("Pointer reached failed method in TargetMonitorExecutor");
        };
    }

    private void setEodSendFailureTestHook() {
        MigrationSource.eodSendFailureHook = pid1 -> {
            if (!PARTITION_ID.equals(pid1)) {
                /* not my partition */
                return;
            }
            trace("Failing sending EOD for partition: " + pid1);
            try {
                throw new IOException();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void setNoMonitorTargetTestHook() {
        MigrationSource.noMonitorTargetHook = pid1 -> {
            if (!PARTITION_ID.equals(pid1)) {
                /* not my partition */
                return;
            }
            trace("No monitor target hook set");
        };
    }


    private void startMigration(RepNodeId srcId, RepNodeId tgtId) {

        trace("Start migrate pid=" + PARTITION_ID + " from=" + srcId + " t0" +
              tgtId);

        final RepGroupId srcGroupId = new RepGroupId(srcId.getGroupId());
        final RepNode target = config.getRN(tgtId);
        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(PARTITION_ID, srcGroupId)
                           .getPartitionMigrationState());
    }

    private void verifyMigrationFailure(RepNodeId srcId, RepNodeId tgtId) {

        final RepNode source = config.getRN(srcId);
        final RepNode target = config.getRN(tgtId);

        /* The sendEOD failure should have set the migration state to ERROR */
        waitForMigrationState(target, PARTITION_ID,
                              RepNodeAdmin.PartitionMigrationState.ERROR);

        if (MigrationSource.noMonitorTargetHook != null) {

            /* In the first run, noMonitorTargetHook is set, so
             * the partition is not restored instantly. It must
             * wait for rescheduling, and it won't be owned by
             * either the source or the target.
             */
            waitForPartition(target, PARTITION_ID, false);
            waitForPartition(source, PARTITION_ID, false);

        } else {

            /* In case of second run, noMonitorTargetHook is not
             * set, so the TargetMonitorExecutor will see the
             * partition migration state as error and will call
             * the fail method, which in turn will remove the
             * migration record and update the local topology,
             * transferring the ownership back to the source.
             */

            waitForPartition(target, PARTITION_ID, false);
            waitForPartition(source, PARTITION_ID, true);
        }
    }

    private void verifyMigrationComplete(RepNodeId srcId, RepNodeId tgtId) {
        final RepNode source = config.getRN(srcId);
        final RepNode target = config.getRN(tgtId);

        waitForMigrationState(target, PARTITION_ID,
                              RepNodeAdmin.PartitionMigrationState.SUCCEEDED);

        /* The source and target should have changed their partition map */
        waitForPartition(target, PARTITION_ID, true);
        waitForPartition(source, PARTITION_ID, false);

        /* Should be able to call again and get success */
        assertEquals(RepNodeAdmin.PartitionMigrationState.SUCCEEDED,
                     target.getMigrationState(PARTITION_ID).
                           getPartitionMigrationState());
    }

    /**
     * Add dummy data in the store
     */
    private void addDataInStore() {
        KVStore kvStore = KVStoreFactory.getStore(config.getKVSConfig());
        int value = 1;
        for (int i = 1; i <= 1000; i++, value++) {
            Key key = Key.createKey(String.valueOf(i));
            byte[] byteArray = ByteBuffer.allocate(4).putInt(value).array();
            Value value1 = Value.createValue(byteArray);
            kvStore.put(key, value1);
        }
        trace("Added data in store");
    }
}
