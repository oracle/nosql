/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */


package oracle.kv.impl.rep.migration.generation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.migration.MigrationSource;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.ThreadUtils;
import oracle.kv.impl.util.WaitableCounter;

import org.junit.Test;

/**
 * Unit tests that {@link MigrationSource} would wait for pending operations
 * with noop migration stream handler to complete before starting up scanning.
 */
public class PendingOperationTest extends PartitionGenerationTestBase {

    /**
     * Polling timeout and interval in ms
     */
    private static final long POLL_TIMEOUT_MS = 60 * 1000;
    private static final int POLL_TIMEOUT_INTV_MS =  1000;
    private static final int SIMULATED_COUNTER = 10;
    private static final PartitionId PARTITION_ID = p1;
    private final AtomicBoolean inWaiting = new AtomicBoolean(false);
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        MigrationSource.pendingOperationHook = null;
        super.tearDown();
    }

    @Test
    public void testPendingOperation() {
        /* start store */
        config.startRepNodeServices();

        /* set up test hook */
        final WaitableCounter wc = initWaitableCounter();
        setTestHook(wc);
        trace("Test hook set, counter=" + wc.get());

        /* start a migration */
        startMigration(sourceId, targetId);
        trace("Migration started for pid=" + PARTITION_ID);

        /* verify migration source is waiting */
        verifyMigrationSourceWaiting();
        trace("Migration of pid=" + PARTITION_ID + " is waiting");

        /* spawn a thread to decrement the counter */
        spawnThreadToDecrementCounter(wc);
        trace("Thread started to decrement simulated counter");

        /* verify the counter is 0 and RN continues migration */
        verifyMigrationSourceContinue();
        trace("Migration unblocked");
        assertEquals(0, wc.get());

        /* verify migration completed */
        verifyMigrationComplete(sourceId, targetId);
        trace("Done migrating pid=" + PARTITION_ID +
              " from shard=" + sourceId + " to shard=" + targetId);
    }


    private WaitableCounter initWaitableCounter() {
        final WaitableCounter ret = new WaitableCounter();
        ret.set(SIMULATED_COUNTER);
        return ret;
    }

    private void spawnThreadToDecrementCounter(WaitableCounter wc) {
        final Runnable runnable =
            () -> {
            trace("Thread running, id=" +
                  ThreadUtils.threadId(Thread.currentThread()));
            while (wc.get() > 0) {
                try {
                    Thread.sleep(POLL_TIMEOUT_INTV_MS / 2);
                } catch (InterruptedException e) {
                    fail("Interrupted");
                }
                trace("#pending=" + wc.decrementAndGet());
            }
        };
        final Thread thread = new Thread(runnable);
        thread.start();
    }

    private void setTestHook(WaitableCounter wc) {
        MigrationSource.pendingOperationHook = pid1 -> {
            if (!PARTITION_ID.equals(pid1)) {
                /* not my partition */
                return;
            }
            final int count = wc.get();
            if (count == 0) {
                fail("Simulated pending operations must be greater than 0");
            }
            inWaiting.set(true);
            trace("Simulated #pending operation=" + count + ", pid=" + pid1);
            wc.awaitZero(POLL_TIMEOUT_INTV_MS, POLL_TIMEOUT_MS);
            trace("Simulated #pending down to zero, continue");
            inWaiting.set(false);
        };
    }

    private void verifyMigrationSourceWaiting() {
        final boolean succ =
            new PollCondition(POLL_TIMEOUT_INTV_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final boolean ret = inWaiting.get();
                    trace("Waiting=" + ret);
                    return ret;
                }
            }.await();
        if (!succ) {
            fail("Timeout in waiting for migration source to wait, " +
                 "timeoutMs=" + POLL_TIMEOUT_MS);
        }
    }

    private void verifyMigrationSourceContinue() {
        final boolean succ =
            new PollCondition(POLL_TIMEOUT_INTV_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final boolean ret = !inWaiting.get();
                    trace("Continue=" + ret);
                    return ret;
                }
            }.await();
        if (!succ) {
            fail("Timeout in waiting for migration source to wait, " +
                 "timeoutMs=" + POLL_TIMEOUT_MS);
        }
    }

    private void startMigration(RepNodeId srcId, RepNodeId tgtId) {

        trace("Start migrate pid=" + PARTITION_ID +
              " from=" + srcId + " t0" + tgtId);

        final RepGroupId srcGroupId = new RepGroupId(srcId.getGroupId());
        final RepNode target = config.getRN(tgtId);
        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                     target.migratePartition(PARTITION_ID, srcGroupId).
                           getPartitionMigrationState());
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
}
