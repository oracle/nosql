/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.logging.Logger;

import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.PollCondition;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.log.entry.LNEntryInfo;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.OutputWireRecord;

/**
 * A collection of unit tests that test read operation with consistency
 * during the partition migration.
 */
public class MigrationReadConsistencyTestBase
    extends PartitionMigrationTestBase
{
    protected static CountDownLatch waitLatch;

    protected final RepNodeId laggingRNId = new RepNodeId(2, 2);
    protected RepNode source;
    protected RepNode target;
    protected RepNode laggingRN;

    @Override
    public void tearDown()
        throws Exception {

        if (waitLatch != null) {
            waitLatch.countDown();
        }
        super.tearDown();
    }

    protected void migratePartition(PartitionId pid) {
        migratePartition(pid, true);
    }

    protected void migratePartition(PartitionId pid,
                                    boolean checkPartition) {
        RepGroupId rgId = new RepGroupId(source.getRepNodeId().getGroupId());
        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(pid, rgId).
                         getPartitionMigrationState());
        waitForMigrationState(target, pid, PartitionMigrationState.SUCCEEDED);
        PartitionMigrationStatus status = target.getMigrationStatus(pid);
        assertNotNull(status);
        assertEquals(PartitionMigrationState.SUCCEEDED, status.getState());
        assertTrue(status.forTarget());
        status = source.getMigrationStatus(pid);
        assert status != null;
        assertNull(status.getState());
        assertTrue(status.forSource());
        if (checkPartition) {
            checkPartition(source, pid, false);
            checkPartition(target, pid, true);
        }
    }

    /*
     * Block the feeder for given replica right after record meet the given
     * condition by injecting a feeder filter using wait latch in this test.
     */
    protected void blockFeeder(RepNode master,
                               RepNode replica,
                               Predicate<DatabaseImpl> blockCondition) {
        FeederFilter laggingFilter = new FeederFilter() {
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
                            waitLatch.await();
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

    /**
     * Waits for a partition generation is opened on this node.
     */
    protected void waitForPartitionGenOpen(final RepNode rn,
                                         final PartitionId pid) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                PartitionGenerationTable pgt = rn.getPartGenTable();
                if (!pgt.isReady()) {
                    return false;
                }
                return pgt.isPartitionOpen(pid);
            }
        }.await();
        assertTrue(success);
    }
}
