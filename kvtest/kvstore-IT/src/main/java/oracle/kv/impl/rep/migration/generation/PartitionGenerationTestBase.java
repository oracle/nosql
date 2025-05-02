/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static oracle.kv.impl.rep.migration.generation.PartitionGenDBManager.PARTITION_GEN_MD_DB_NAME;
import static oracle.kv.impl.rep.migration.generation.PartitionGenDBManager.TXN_CONFIG;
import static oracle.kv.impl.rep.migration.generation.PartitionGenDBManager.buildKey;
import static oracle.kv.impl.rep.migration.generation.PartitionGenDBManager.buildValue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.logging.FileHandler;

import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.TxnUtil;
import oracle.nosql.common.contextlogger.LogFormatter;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

public class PartitionGenerationTestBase extends RepNodeTestBase {
    static boolean trace_on_screen = false;

    protected static final PartitionId p1 = new PartitionId(1);
    protected static final PartitionId p2 = new PartitionId(2);
    protected static final PartitionId p3 = new PartitionId(3);
    protected static final PartitionId p5 = new PartitionId(5);
    protected static final RepGroupId rg1 = new RepGroupId(1);
    static final RepNodeId sourceId = new RepNodeId(1, 1);
    protected static final RepNodeId targetId = new RepNodeId(2, 1);
    static final RepNodeId rg1Master = new RepNodeId(1, 1);
    static final RepNodeId rg2Master = new RepNodeId(2, 1);

    protected KVRepTestConfig config;

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

        addLoggerFileHandler();
        /*
         * Individual tests need to start rep node services after setting
         * any test specific configuration parameters.
         */
    }

    @Override
    public void tearDown() throws Exception {

        config.stopRepNodeServices();
        config = null;

        super.tearDown();
    }

    void migratePartition(PartitionId pid, RepNodeId srcId, RepNodeId tgtId) {

        trace("Start migrate " + pid + " from " + srcId + " to " + tgtId);

        final RepGroupId srcGroupId = new RepGroupId(srcId.getGroupId());

        final RepNode source = config.getRN(srcId);
        final RepNode target = config.getRN(tgtId);

        assertEquals(RepNodeAdmin.PartitionMigrationState.PENDING,
                    target.migratePartition(pid, srcGroupId).
                        getPartitionMigrationState());
        waitForMigrationState(target, pid,
                              RepNodeAdmin.PartitionMigrationState.SUCCEEDED);

        /* The source and target should have changed their partition map */
        waitForPartition(target, pid, true);
        waitForPartition(source, pid, false);

        /* Should be able to call again and get success */
        assertEquals(RepNodeAdmin.PartitionMigrationState.SUCCEEDED,
                     target.getMigrationState(pid).
                         getPartitionMigrationState());

        trace("Done migrating " + pid + " from " + srcId + " to " + tgtId);
        trace("=====================================\n");
        trace("PGT on node " + srcId + ":\n");
        PartitionGenerationTable generationTable =
        source.getPartitionManager().getPartGenTable();
        trace(generationTable.dumpTable());
        trace("=====================================\n");
        trace("PGT on node " + tgtId + ":\n");
        generationTable = target.getPartitionManager().getPartGenTable();
        trace(generationTable.dumpTable());
        trace("=====================================\n");
    }

    protected void trace(String msg) {
        logger.info(msg);

        if (trace_on_screen) {
            System.out.println(msg);
        }
    }

    /**
     * Waits for a partition migration to reach a specified state.
     * If the wait times out without reaching the state an assertion error
     * is thrown.
     */
    void waitForMigrationState(RepNode rn,
                               PartitionId pId,
                               RepNodeAdmin.PartitionMigrationState st) {
        boolean success = new PollCondition(1000, 30000) {

            @Override
            protected boolean condition() {
                final PartitionMigrationStatus status =
                    rn.getMigrationStatus(pId);
                return status != null && status.getState().equals(st);
            }
        }.await();

        if (!success) {
            trace(pId + " on " + rn.getRepNodeId() +
                  ": exp st: " + st +
                  " act st: " + rn.getMigrationStatus(pId).getState());
        }
        assert (success);
    }

    void waitForPartition(RepNode rn, PartitionId pId, boolean present) {
        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                try {
                    rn.getPartitionDB(pId);
                    return present;
                } catch (IncorrectRoutingException ire) {
                    return !present;
                }
            }
        }.await();
        assert (success);
    }

    static void dbPutInTxn(PartitionGenDBManager dbManager,
                           PartitionId pid,
                           PartitionGeneration pg,
                           Consumer<String> logger) {

        final Database dbHandle = dbManager.getDB();
        if (dbHandle == null) {
            throw new IllegalStateException("Generation db not open");
        }

        final DatabaseEntry key = buildKey(pid, pg.getGenNum());
        final DatabaseEntry val = buildValue(pg);

        Transaction txn = null;
        try {
            txn = dbHandle.getEnvironment().beginTransaction(null, TXN_CONFIG);
            dbHandle.put(txn, key, val);
            txn.commit();
            txn = null;
            logger.accept("Done write db=" + PARTITION_GEN_MD_DB_NAME +
                          ", generation=" + pg);
        } catch (Exception exp) {
            logger.accept("Fail to write db=" + PARTITION_GEN_MD_DB_NAME +
                          ", with key=" + key + ", val=" + pg +
                          ", error=" + exp);
            throw new PartitionMDException(PARTITION_GEN_MD_DB_NAME,
                                           "Cannot write db=" +
                                           PARTITION_GEN_MD_DB_NAME +
                                           ", generation=" + pg,
                                           pg, exp);
        } finally {
            TxnUtil.abort(txn);
        }
    }

    private void addLoggerFileHandler() throws IOException {
        final String fileName = "testlog";
        final String testPath = TestUtils.getTestDir().getAbsolutePath();
        final File loggerFile = new File(new File(testPath), fileName);
        final FileHandler handler =
            new FileHandler(loggerFile.getAbsolutePath(), false);
        handler.setFormatter(new LogFormatter(null));
        tearDowns.add(() -> logger.removeHandler(handler));
        logger.addHandler(handler);
        logger.info("Add test log file handler: path=" + testPath +
                    ", log file name=" + fileName +
                    ", file exits?=" + loggerFile.exists());
    }
}
