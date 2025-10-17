/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.log.entry.LNEntryInfo;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.OutputWireRecord;

public class PartitionMigrationTestBase extends RepNodeTestBase {
    protected static final RepNodeId sourceId = new RepNodeId(1, 1);
    protected static final RepNodeId targetId = new RepNodeId(2, 1);

    protected KVRepTestConfig config;
    protected KVStore kvs;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        /*
         * This will create two RGs.
         * RG1 will start with partitions 1,3,5,7,9
         * RG2 will start with partitions 2,4,6,8,10
         */
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     2, /* nSN */
                                     3, /* repFactor */
                                     10 /* nPartitions */);

        /*
         * Individual tests need to start rep node services after setting
         * any test specific configuration parameters.
         */
    }

    @Override
    public void tearDown() throws Exception {

        config.stopRepNodeServices();
        config = null;
        if (kvs != null) {
            kvs.close();
        }
        super.tearDown();
    }

    /**
     * Checks for a partition database's existence on the specified node.
     * If present is true, the partition Db must be present. If present is false
     * then the partition Db must be absent. Any other condition will throw
     * an assert error.
     */
    protected void checkPartition(RepNode rn, PartitionId pId, boolean present) {
        try {
            rn.getPartitionDB(pId);
            assertTrue(present);
        } catch (IncorrectRoutingException ire) {
            assertFalse(present);
        }
    }

    /**
     * Waits for a partition to appear (present == true) or disappear
     * (present == false) from the specified node.
     */
    protected void waitForPartition(final RepNode rn,
                                    final PartitionId pId,
                                    final boolean present) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                try {
                    rn.getPartitionDB(pId);
                    return (present == true);
                } catch (IncorrectRoutingException ire) {
                    return (present == false);
                }
            }
        }.await();
        assert(success);
    }

    /**
     * Waits for a partition migration to reach a specified state.
     * If the wait times out without reaching the state an assertion error
     * is thrown.
     */
    static public void
        waitForMigrationState(final RepNode target,
                              final PartitionId pId,
                              final PartitionMigrationState... requiredStates)
    {
        waitForMigrationState(target, pId, 20000, requiredStates);
    }

    static public void
        waitForMigrationState(final RepNode target,
                              final PartitionId pId,
                              final long timeoutMillis,
                              final PartitionMigrationState... requiredStates)
    {
        final AtomicReference<PartitionMigrationStatus> observedStatus =
            new AtomicReference<>(null);
        boolean success = new PollCondition(500, timeoutMillis) {

            @Override
            protected boolean condition() {

                /*
                 * Using getMigrationStatus bypasses an optimization in
                 * getMigrationState that can report SUCCESS a little early for
                 * the test. Getting the status will not remove the migration
                 * state from the RN.
                 */
                final PartitionMigrationStatus status =
                    target.getMigrationStatus(pId);
                observedStatus.set(status);
                if (status == null) {
                    return false;
                }

                for (PartitionMigrationState requiredState : requiredStates) {
                    if (status.getState().equals(requiredState)) {
                        return true;
                    }
                }
                return false;
            }
        }.await();
        assertTrue("wait failed for " + pId + " on "
            + target.getRepNodeId().getFullName() + ", state(s): "
            + Arrays.toString(requiredStates) + ", last observed: "
            + observedStatus.get(), success);
    }

    /*
     * Block the feeder for given replica right after record meet the given
     * condition by injecting a feeder filter using wait latch in this test.
     */
    protected void blockFeeder(RepNode master,
                               RepNode replica,
                               Predicate<DatabaseImpl> blockCondition,
                               CountDownLatch waitLatch) {
        final FeederFilter laggingFilter = new FeederFilter() {
            private transient LNEntryInfo lnInfo;

            @Override
            public OutputWireRecord execute(OutputWireRecord record,
                                            RepImpl repImpl) {
                if (lnInfo == null) {
                    lnInfo = new LNEntryInfo();
                }

                if (!record.getLNEntryInfo(lnInfo)) {
                    return record;
                }
                DatabaseId dbId = new DatabaseId(lnInfo.databaseId);
                DbTree dbTree = repImpl.getDbTree();
                DatabaseImpl impl = dbTree.getDb(dbId);

                try {
                    if (impl == null) {
                        return record;
                    }
                    if (blockCondition.test(impl)) {
                        try {
                            logger.info(
                                String.format(
                                    "Blocking feeder from %s to %s",
                                    master.getRepNodeId(),
                                    replica.getRepNodeId()));
                            waitLatch.await();
                            logger.info(
                                String.format(
                                    "Unblocked feeder from %s to %s",
                                    master.getRepNodeId(),
                                    replica.getRepNodeId()));
                        } catch (InterruptedException e) {
                        }
                    }
                    return record;
                } finally {
                    if (impl != null) {
                        dbTree.releaseDb(impl);
                    }
                }
            }

            @Override
            public String[] getTableIds() {
                return null;
            }

            @Override
            public void setLogger(Logger logger) {
            }
        };

        RepInternal.getRepImpl(master.getEnv(100))
                   .getRepNode().feederManager()
                   .getFeeder(replica.getRepNodeId().getFullName())
                   .setFeederFilter(laggingFilter);
    }

}
