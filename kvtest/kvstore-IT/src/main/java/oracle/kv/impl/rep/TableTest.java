/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.rep.table.TableMetadataPersistence;
import oracle.kv.impl.test.TestIOHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.table.Index;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.Load;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.util.DbVerify;

import org.junit.Test;

/**
 */
public class TableTest extends RepNodeTestBase {

    static final int NUM_ROWS = 2000;
    private static final RepNodeId rg1masterId = new RepNodeId(1, 1);
    private static final RepNodeId rg1replicaId = new RepNodeId(1, 2);
    private static final RepNodeId rg1replica3Id = new RepNodeId(1, 3);
    private static final RepNodeId rg2masterId = new RepNodeId(2, 1);
    private static final RepNodeId rg2replicaId = new RepNodeId(1, 2);

    private static final TableImpl userTable =
        TableBuilder.createTableBuilder("User")
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .primaryKey("id")
        .shardKey("id")
        .buildTable();

    private KVRepTestConfig config;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        /*
         * Create a 2x3 store.
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

        TableMetadataPersistence.PERSISTENCE_TEST_HOOK = null;
    }

    @Override
    public void tearDown() throws Exception {
        TableMetadataPersistence.PERSISTENCE_TEST_HOOK = null;

    	if (config != null) {
            config.stopRepNodeServices();
            config = null;
    	}
        super.tearDown();
    }

    /**
     * Tests populating an index with existing records.
     */
    @Test
    public void testPopulate() {
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        TableImpl table = addTable(md, userTable);

        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        addRows(table, NUM_ROWS, apiImpl);

        /*
         * Multiple indexes are needed to exercise the path that led to
         * [#24691].
         */
        addIndex(md, "FirstName", table, "firstName");
        addIndex(md, "LastName", table, "lastName");
        addIndex(md, "Age", table, "age");
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        waitForPopulate(rn1, "FirstName", table);
        waitForPopulate(rn2, "FirstName", table);

        table = getTable(rn1, table.getFullName());
        assertNotNull(table);

        /*
         * The index should have been filtered because it's not yet official.
         */
        Index idx = table.getIndex("FirstName");
        assertNull(idx);

        /* mark the index as ready */
        md.updateIndexStatus(null, "FirstName", table.getFullName(),
                             IndexImpl.IndexStatus.READY);

        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        /* assert that the index is visible */
        table = getTable(rn1, table.getFullName());
        idx = table.getIndex("FirstName");
        assertNotNull(idx);

        final SecondaryDatabase rn1db = rn1.getIndexDB(null,
                                                       "FirstName",
                                                       table.getFullName());
        final SecondaryDatabase rn2db = rn2.getIndexDB(null,
                                                       "FirstName",
                                                       table.getFullName());
        assertNotNull(rn1db);
        assertNotNull(rn2db);
        assertEquals(NUM_ROWS,
                   countSecondaryRecords(rn1db) + countSecondaryRecords(rn2db));
        kvs.close();
    }

    @Test
    public void testChildPopulate() {
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        TableImpl table = addTable(md, userTable);

        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        // Add rows to the parent table - the populate should ignore these
        addRows(table, NUM_ROWS, apiImpl);

        final TableImpl childTable =
            TableBuilder.createTableBuilder("Child", "Child table", table)
            .addInteger("income")
            .primaryKey("income")
            .buildTable();

        table = addTable(md, childTable);

        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        addChildRows(table, NUM_ROWS, apiImpl);
        addIndex(md, "income", table, "income");
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);
        waitForPopulate(rn1, "income", table);
        waitForPopulate(rn2, "income", table);

        final SecondaryDatabase rn1db = rn1.getIndexDB(null,
                                                       "income",
                                                       table.getFullName());
        final SecondaryDatabase rn2db = rn2.getIndexDB(null,
                                                       "income",
                                                       table.getFullName());
        assertNotNull(rn1db);
        assertNotNull(rn2db);
        assertEquals(NUM_ROWS,
                   countSecondaryRecords(rn1db) + countSecondaryRecords(rn2db));
        kvs.close();
    }

    /*
     * Test replica opening secondary db before master creates it
     */
    @Test
    public void testReplica() {
        config.startRepNodeServices();

        final RepNode master = config.getRN(rg1masterId);
        final RepNode replica = config.getRN(rg1replicaId);
        final RepNode master2 = config.getRN(rg2masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        TableImpl table = addTable(md, userTable);

        /* broadcast table */
        updateMetadata(master, md);
        updateMetadata(replica, md);
        updateMetadata(master2, md);

        addRows(table, NUM_ROWS, apiImpl);
        addIndex(md, "FirstName", table, "firstName");

        updateMetadata(replica, md);

        try {
            replica.getIndexDB(null, "FirstName", table.getFullName());
            fail("Expected RNUnavailableException");
        } catch (RNUnavailableException rue) {/* Expected */
        }

        /*
         * Updating the master will cause the secondary to be created,
         * eventually the replica will see the new db.
         */
        updateMetadata(master, md);

        waitForSecondary(replica, "FirstName", table);
        kvs.close();
    }

    /*
     * Tests removing a table and its associated data.
     */
    @Test
    public void testTableRemove() {
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        TableImpl table = addTable(md, userTable);

        updateMetadata(rn1, md);
        updateMetadata(rn2, md);
        addRows(table, NUM_ROWS, apiImpl);

        assertEquals(NUM_ROWS, countPrimaryRecords(kvs));

        /* Drop the table, removing the data */
        md.dropTable(null, table.getName(), true);
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        /* The table should still be present, just marked for delete */
        table = getTable(rn1, table.getName());
        assertNotNull(table);
        assert(table.getStatus().isDeleting());

        waitForDelete(rn1, table);
        waitForDelete(rn2, table);

        /* Should be all gone */
        assertEquals(0, countPrimaryRecords(kvs));

        kvs.close();
    }

    /*
     * This test reproduces a bug in DbVerify that happens after a table is
     * dropped, KVSTORE-655.  This only applies if DbVerify is run separately
     * from KV, such as running the utility on the commandline to verify a
     * backup or when debugging.  After a table is dropped there is a short
     * window of time before the extinction scanner is run but after the
     * cleaner is run where it appears active records are pointing to reserve
     * files.  These active records are part of the dropped table, and when
     * verify is run as part of a KV process it can access the
     * RepExtinctionFilter class that would identify them as extinct.  When
     * DbVerify was run without KV it does not have access to that class, and
     * reports the records as corruption.
     */
    @Test
    public void testTableRemoveAndVerify() throws IOException {
    	/*
    	 * Set the log file size to the minimum so they can be cleaned faster,
    	 * and disable the extinction scanner only in the replica to be tested,
    	 * as disabling it in a master causes exceptions in the
    	 * MaintenanceThread class.
    	 */
    	final String verifyDir = config.getStorePath() + "/sn2/rg1-rn2/env";
    	final File propertyFile = new File(verifyDir, "je.properties");
    	final FileWriter writer = new FileWriter(propertyFile, true);
        writer.append("je.log.fileMax=1048576\n");
        writer.append("je.env.runExtinctRecordScanner=false\n");
        writer.flush();
        writer.close();
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);
        final RepNode replica = config.getRN(rg1replicaId);

        /*
         * Disable cleaning and eraser so the test can control when they are
         * run, also up the cleaner minimum utilization so cleaning will
         * happen as soon as it is requested.
         */
        final Environment env = replica.getEnv(1000);
        final EnvironmentMutableConfig mutConfig = env.getMutableConfig();
        mutConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        mutConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_ERASER.getName(), "false");
        mutConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "90");
        env.setMutableConfig(mutConfig);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        final TableMetadata md = new TableMetadata(false);

        final TableImpl table = addTable(md, userTable);

        /* Fill the User table with data so the log files rollover. */
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);
        updateMetadata(replica, md);
        addRows(table, 20 * NUM_ROWS, apiImpl);
        assertEquals(20 * NUM_ROWS, countPrimaryRecords(kvs));

        /* Drop the table. */
        md.dropTable(null, table.getName(), true);
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);
        updateMetadata(replica, md);

        /*
         * Clean the logs so a reserved file is created, the extinct records in
         * the dropped table will reference the reserved file, which would be a
         * form of corruption except the ExtinctionFilter shows those records as
         * deleted.
         */
        assert(env.cleanLog() > 0);

        /* Shutdown everything to run the verifier. */
        kvs.close();
        config.stopRepNodeServices();
        config = null;

        /* Test that verify reports errors when run without the -kv option. */
        final String[] verifyFailArgs = { "-h", verifyDir, "-q"};
        DbVerify verifier = new DbVerify();
        VerifySummary summary = verifier.runVerify(verifyFailArgs, true);
        assert(summary != null && summary.hasErrors());

        /* Test that verify succeeds when run with the -kv option. */
        final String[] verifySucceedArgs = { "-h", verifyDir, "-q", "-kv"};
        verifier = new DbVerify();
        summary = verifier.runVerify(verifySucceedArgs, true);
        assert(summary != null && !summary.hasErrors());
    }

    /*
     * This test is to verify the fix to Load about setting ExtinctionFilter
     * when open JE environment in snapshots of RNs. KVSTORE-1881.
     *
     * This test refers to the steps in testTableRemoveAndVerify() to reproduce
     * that the active records point to reserve files after table dropped, and
     * mimic creating snapshot by copying db files to snapshot directory, then
     * run Load to read snapshot and write to store.
     */
    @Test
    public void testTableRemoveAndLoad() throws Exception {
        /*
         * Set the log file size to the minimum so they can be cleaned faster,
         * and disable the extinction scanner only in the replica to be tested,
         * as disabling it in a master causes exceptions in the
         * MaintenanceThread class.
         */
        final String replicaDir = config.getStorePath() + "/sn2/rg1-rn2";
        final String replicaEnvDir = replicaDir + "/env";
        final String snapshotDir = replicaDir + "/snapshot";

        final File propertyFile = new File(replicaEnvDir, "je.properties");
        final FileWriter writer = new FileWriter(propertyFile, true);
        writer.append("je.log.fileMax=1048576\n");
        writer.append("je.env.runExtinctRecordScanner=false\n");
        writer.flush();
        writer.close();
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);
        final RepNode replica = config.getRN(rg1replicaId);

        /*
         * Disable cleaning and eraser so the test can control when they are
         * run, also up the cleaner minimum utilization so cleaning will
         * happen as soon as it is requested.
         */
        final Environment env = replica.getEnv(1000);
        final EnvironmentMutableConfig mutConfig = env.getMutableConfig();
        mutConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
        mutConfig.setConfigParam
            (EnvironmentParams.ENV_RUN_ERASER.getName(), "false");
        mutConfig.setConfigParam
            (EnvironmentParams.CLEANER_MIN_UTILIZATION.getName(), "90");
        env.setMutableConfig(mutConfig);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        final TableMetadata md = new TableMetadata(false);
        final TableImpl table = addTable(md, userTable);

        /* Fill the User table with data so the log files rollover. */
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);
        updateMetadata(replica, md);
        addRows(table, 20 * NUM_ROWS, apiImpl);
        assertEquals(20 * NUM_ROWS, countPrimaryRecords(kvs));

        /* Drop the table. */
        md.dropTable(null, table.getName(), true);
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);
        updateMetadata(replica, md);

        /*
         * Clean the logs so a reserved file is created, the extinct records in
         * the dropped table will reference the reserved file, which would be a
         * form of corruption except the ExtinctionFilter shows those records as
         * deleted.
         */
        assertTrue(env.cleanLog() > 0);

        /* Shutdown everything to run the verifier and load. */
        kvs.close();
        config.stopRepNodeServices();

        /*
         * Mimic creating snapshot on rg1-rn2
         * copy sn2/rg1-rn2/env/*.jdb to sn2/rg1-rn2/snapshot
         */
        copyJdbFiles(replicaEnvDir, snapshotDir);

        /* Test that verify reports errors when run without the -kv option. */
        final String[] verifyFailArgs = { "-h", snapshotDir, "-q"};
        DbVerify verifier = new DbVerify();
        VerifySummary summary = verifier.runVerify(verifyFailArgs, true);
        assertTrue(summary != null && summary.hasErrors());

        /* start service again */
        config.startRepNodeServices();

        /* run load, should be no error */
        StorageNode sn = config.getTopology().getSortedStorageNodes().get(0);
        runLoad(snapshotDir, kvstoreName, sn.getHostname(),
                sn.getRegistryPort());
    }

    private void runLoad(String envDir, String storeName, String host, int port)
        throws Exception {

        /*
         * Set je.testMode to true to fail fast when access an LSN that refers
         * to a reserved file.
         */
        Map<String, String> jeEnvProps = new HashMap<>();
        jeEnvProps.put("je.testMode", "true");

        final Load load = new Load(new File[] {new File(envDir)},
                                   storeName,
                                   host,
                                   port,
                                   null, /* user */
                                   null, /* securityFile */
                                   null, /* checkpointDir */
                                   null, /* statsDir */
                                   jeEnvProps,
                                   0, 0, 0, 0, 0,
                                   false, /* verbose */
                                   TestUtils.NULL_PRINTSTREAM);
        load.run();
    }

    private void copyJdbFiles(String envDir, String snapshotDir)
        throws IOException {

        File env = new File(envDir);
        File[] jdbFiles = env.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jdb");
            }
        });

        assertTrue(jdbFiles != null && jdbFiles.length > 0);
        assertTrue(new File(snapshotDir).mkdir());

        for (File src : jdbFiles) {
            File dest = new File(snapshotDir, src.getName());
            FileUtils.copyFile(src, dest);
            assertTrue(dest.exists());
        }
    }

    /**
     * Tests cleaning the secondary cleaner. This test will move a partition
     * after an index is created. It then checks to see if there are the
     * proper number of records in the old secondary.
     */
    @Test
    public void testPrimaryCleaner() {
        config.getRepNodeParams(rg2masterId).getMap().
                    setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");

        config.startRepNodeServices();

        final RepNode source = config.getRN(rg1masterId);
        final RepNode target = config.getRN(rg2masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        final TableMetadata md = new TableMetadata(false);

        final TableImpl table = addTable(md, userTable);

        updateMetadata(source, md);
        updateMetadata(target, md);

        addRows(table, NUM_ROWS, apiImpl);
        addIndex(md, "FirstName", table, "firstName");
        updateMetadata(source, md);
        updateMetadata(target, md);

        waitForPopulate(source, "FirstName", table);
        waitForPopulate(target, "FirstName", table);

        /* mark the index as ready */
        md.updateIndexStatus(null, "FirstName", table.getFullName(),
                             IndexImpl.IndexStatus.READY);

        updateMetadata(source, md);
        updateMetadata(target, md);

        final SecondaryDatabase sourceDB =
            source.getIndexDB(null,
                              "FirstName",
                              table.getFullName());
        final SecondaryDatabase targetDB =
            target.getIndexDB(null,
                              "FirstName",
                              table.getFullName());
        assertNotNull(sourceDB);
        assertNotNull(targetDB);

        int nSourceRecords = countSecondaryRecords(sourceDB);
        int nTargetRecords = countSecondaryRecords(targetDB);
        assertEquals(NUM_ROWS, nSourceRecords + nTargetRecords);

        final PartitionId p1 = new PartitionId(1);

        assertEquals(PartitionMigrationState.PENDING,
                     target.migratePartition(p1, new RepGroupId(1)).
                         getPartitionMigrationState());
        PartitionMigrationTest.
           waitForMigrationState(target, p1, PartitionMigrationState.SUCCEEDED);

        final PartitionMigrationStatus status =
            source.getMigrationStatus(p1);

        /*
         * We should find the number of original records in the target and
         * source +/- the number of records moved.
         */
        assert(status.getRecordsSent() > 0);
        assertEquals(nSourceRecords - status.getRecordsSent(),
                     countSecondaryRecords(sourceDB));
        assertEquals(nTargetRecords + status.getRecordsSent(),
                     countSecondaryRecords(targetDB));
        kvs.close();
    }

    /**
     * Tests the restart of a master and that it re-opens the secondary DB.
     */
    @Test
    public void testRestart() {
        config.startRepNodeServices();
        RepNode rn1 = config.getRN(rg1masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());

        final TableMetadata md = new TableMetadata(false);
        final TableImpl table = addTable(md, userTable);

        updateMetadata(rn1, md);

        addIndex(md, "FirstName", table, "firstName");
        updateMetadata(rn1, md);

        /* Kill one of the masters */
        RepNodeService rg1Service = config.getRepNodeService(rg1masterId);
        rg1Service.stop(true, "test");

        config.startRepNodeSubset(rg1masterId);

        rn1 = config.getRN(rg1masterId);

        waitForSecondary(rn1, "FirstName", table);
        kvs.close();
    }

    /**
     * Tests the restart of a secondary populate after a rep node failover.
     */
    @Test
    public void testRestartPopulate() {
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);
        final RepNode replica = config.getRN(rg1replica3Id);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(false);

        TableImpl table = addTable(md, userTable);

        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        addRows(table, NUM_ROWS, apiImpl);
        addIndex(md, "FirstName", table, "firstName");
        updateMetadata(rn1, md);
        updateMetadata(rn2, md);

        /* Kill one of the masters */
        RepNodeService rg1Service = config.getRepNodeService(rg1masterId);
        rg1Service.stop(true, "test");

        /* One of the replicas will take over and finish */
        waitForPopulate(replica, "FirstName", table);
        waitForPopulate(rn2, "FirstName", table);

        kvs.close();
    }

    /**
     * Tests metadata update and propagation.
     *
     * The test modifies the table metadata and then updates one master.
     * It then waits for it's replica, the other master, and it's
     * replica to update, checking that the metadata is the same.
     * At each modification, the update to the first master (by the test)
     * will be a full metadata update. The second master will be updated
     * (by the first master via propagation) using change sets. So both types
     * of updates are being tested in one pass.
     */
    @Test
    public void testMetadataReplication() {
        config.startRepNodeServices();

        final RepNode master1 = config.getRN(rg1masterId);
        final RepNode replica1 = config.getRN(rg1replicaId);
        final RepNode master2 = config.getRN(rg2masterId);
        final RepNode replica2 = config.getRN(rg2replicaId);

        final TableMetadata md = new TableMetadata(true /*keepChanges*/);

        /* Add table */
        TableImpl table = addTable(md, userTable);
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        addIndex(md, "FirstName", table, "firstName");
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        waitForPopulate(master1, "FirstName", table);
        waitForPopulate(master2, "FirstName", table);

        final TableImpl childTable =
            TableBuilder.createTableBuilder("Child", "Child table", table)
            .addInteger("income")
            .primaryKey("income")
            .buildTable();

        /* Add child table */
        addTable(md, childTable);
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        /* Drop child table */
        md.dropTable(null, childTable.getFullNamespaceName(), true);
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        md.dropTable(null, childTable.getFullNamespaceName(), false);
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        /* Drop the index */
        md.dropIndex(null, "FirstName", table.getFullNamespaceName());
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        /* Drop top level table */
        md.dropTable(null, table.getName(), true);
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        md.dropTable(null, table.getName(), false);
        updateMetadata(master1, md);
        check(replica1, md);
        check(master2, md);
        check(replica2, md);

        for (int i = 0; i < 100; i++) {
            addTable(md, makeTable(i));
            updateMetadata(master1, md);
        }
        check(replica1, md);
        check(master2, md);
        check(replica2, md);
    }

    /*
     * Tests that a table metadata serialization failure does not prevent the
     * RN from starting and that it can be corrected.
     */
    @Test
    public void testMDCorruption() throws Exception {
        config.startRepNodeServices();

        RepNode rg1Master = config.getRN(rg1masterId);
        final RepNode rn2 = config.getRN(rg2masterId);

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableMetadata md = new TableMetadata(true);

        /* Add two tables */
        final TableImpl table1 = addTable(md, userTable);
        final TableImpl table2 = addTable(md, makeTable(2));

        updateMetadata(rg1Master, md);
        updateMetadata(rn2, md);

        addRows(table1, NUM_ROWS, apiImpl);

        /* This hook will thow an IOE whenever table1 is de-serialized */
        TableMetadataPersistence.PERSISTENCE_TEST_HOOK =
            new TestIOHook<TableImpl>() {
                @Override
                public void doHook(TableImpl t) throws IOException {
                    if (t.getId() == table1.getId()) {
                        throw new IOException("Injected exception");
                    }
                }
            };

        /*
         * Restart all nodes of RG1. The hook will cause an exception while
         * deserializing table1 during startup, rending the table inaccessible
         * on that shard.
         */
        waitForRestart(rg1masterId);
        waitForRestart(rg1replicaId);
        waitForRestart(rg1replica3Id);

        /* Find the new rg1 master */
        if (config.getRN(rg1masterId).getIsAuthoritativeMaster()) {
            rg1Master = config.getRN(rg1masterId);
        } else if (config.getRN(rg1replicaId).getIsAuthoritativeMaster()) {
            rg1Master = config.getRN(rg1replicaId);
        }  else if (config.getRN(rg1replica3Id).getIsAuthoritativeMaster()) {
            rg1Master = config.getRN(rg1replica3Id);
        } else {
            fail("Could not find rg1 master");
        }

        /* Table 1 should now be missing on rg1. */
        Table t = rg1Master.getTable(table1.getId());
        assertNull("Target table should be missing, found " + t, t);

        /* Reads should not work. Assumes SOME keys go to rg1 */
        try {
            countRecords(table1, apiImpl);
            fail("Expected exception");
        } catch (StoreIteratorException expected) { }

        t = rg1Master.getTable(table2.getId());
        assertNotNull("Unexpected missing table", t);

        final String NEW_FIELD = "NewField";
        /*
         * In order to restore table1 we cause an update by evolving the table.
         * Only change the description string to keep the change begin.
         */
        final int nextSeqNum = md.getSequenceNumber() + 1;
        final FieldMap newFieldMap = table1.getFieldMap().clone();
        newFieldMap.put(NEW_FIELD, FieldDefImpl.Constants.integerDef, false,
                        FieldDefImpl.Constants.integerDef.createInteger(1));
        md.evolveTable(table1, table1.numTableVersions(),
                       newFieldMap,
                       table1.getDefaultTTL(),
                       table1.getDescription(),
                       false,
                       table1.getIdentityColumnInfo(),
                       table1.getRemoteRegions());

        assertEquals("Table was not evolved",
                     nextSeqNum, table1.getSequenceNumber());

        updateMetadata(rg1Master, md);
        updateMetadata(rn2, md);

        /* The table should be back on rg1, sporting the new field */
        t = rg1Master.getTable(table1.getId());
        assertNotNull("Unexpected missing table", t);
        assertNotNull("Expected added field", t.getField(NEW_FIELD));

        /* The records should still be there. */
        assertEquals("Wrong number of rows",
                     NUM_ROWS, countRecords(t, apiImpl));
    }

    /*
     * Restarts and then waits for an RN to become a master or replica.
     */
    private void waitForRestart(RepNodeId rnId) throws RemoteException {
        config.restartRH(rnId);
        boolean success = new PollCondition(500, 15000) {
            @Override
            protected boolean condition() {
                final RepNode rn = config.getRN(rnId);
                final ReplicatedEnvironment repEnv = rn.getEnv(0L);
                if (repEnv == null) {
                    return false;
                }
                if (repEnv.getState().isMaster() ||
                    repEnv.getState().isReplica()) {
                    return true;
                }
                return false;
            }
        }.await();
        assert success;
    }


    /*
     * Returns the count of records in the specified table.
     */
    private int countRecords(Table t, TableAPI api) {
        final TableIterator<Row> itr =
                    api.tableIterator(t.createPrimaryKey(), null, null);
        int count = 0;
        while (itr.hasNext()) {
            itr.next();
            count++;
        }
        return count;
    }

    private TableImpl makeTable(int i) {
        return TableBuilder.createTableBuilder("User" + i)
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .primaryKey("id")
        .shardKey("id")
        .buildTable();
    }

    /**
     * Checks that the metadata on the specified RN matches the given metadata.
     */
    private void check(RepNode rn, TableMetadata md2) {
        final TableMetadata md1 = waitForMetadata(rn, md2.getSequenceNumber());
        assert md1.compareMetadata(md2) : "Metadata on " +
                                          rn.getRepNodeId() + ": " +
                                          md1 + " is not equal to " + md2;
    }

    /**
     * Waits for the metadata on the specified RN to reach seqNum. Returns
     * that metadata.
     */
    static TableMetadata waitForMetadata(final RepNode rn, int seqNum) {

        final AtomicReference<TableMetadata> md = new AtomicReference<>();
        boolean success = new PollCondition(200, 10000) {

            @Override
            protected boolean condition() {
                md.set((TableMetadata)rn.getMetadata(MetadataType.TABLE));
                //System.out.println("FOUND " + md + " on " + rn.getRepNodeId());
                return md.get().getSequenceNumber() >= seqNum;
            }
        }.await();
        assert(success);
        assert md.get().getSequenceNumber() >= seqNum;
        return md.get();
    }

    /**
     * Returns the number of records in the store.
     */
    private int countPrimaryRecords(KVStore kvs) {
        final Iterator<Key> itr =
                                kvs.storeKeysIterator(Direction.UNORDERED, 100);

        int count = 0;
        while (itr.hasNext()) {
            itr.next();
            count++;
        }
        return count;
    }

    /**
     * Returns the number of records in the specified secondary database.
     */
    static int countSecondaryRecords(SecondaryDatabase db) {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();

        int nRecords = 0;

        Cursor cursor = null;
        try {
            cursor = db.openCursor(null, CursorConfig.DEFAULT);

            while (true) {
                switch (cursor.getNext(key, data, LockMode.DEFAULT)) {
                    case SUCCESS:
                        nRecords++;
                        break;

                    case NOTFOUND:
                        return nRecords;

                    case KEYEMPTY:
                        throw new AssertionError("Received KEYEMPTY after" +
                                                 " reading " + nRecords +
                                                 " records");
                    case KEYEXIST:
                        throw new AssertionError("Received KEYEXIST after" +
                                                 " reading " + nRecords +
                                                 " records");
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Waits for a populate to finish.
     */
    static void waitForPopulate(final RepNode rn,
                                 final String indexName,
                                 final TableImpl table) {

        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                return rn.addIndexComplete(null, /* namespace */
                                           indexName,
                                           table.getFullName());
            }
        }.await();
        assert(success);
    }

    /**
     * Waits for a secondary db to appear.
     */
    private SecondaryDatabase waitForSecondary(final RepNode rn,
                                               final String indexName,
                                               final TableImpl table) {

        boolean success = new PollCondition(500, 15000) {

            @Override
            protected boolean condition() {
                try {
                    rn.getIndexDB(null, indexName, table.getFullName());
                    return true;
                } catch (RNUnavailableException rue) {
                    return false;
                }
            }
        }.await();
        assert(success);
        return rn.getIndexDB(null,
                             indexName,
                             table.getFullName());
    }

    /**
     * Waits for a populate to finish.
     */
    private void waitForDelete(final RepNode rn, final TableImpl table) {

        boolean success = new PollCondition(500, 15000) {

                @Override
            protected boolean condition() {
                    return rn.removeTableDataComplete(null, /* namespace */
                                                      table.getFullName());
            }
        }.await();
        assert(success);
    }

    static void addRows(Table table, int numRows, TableAPI apiImpl) {
        addRows(table, 0, numRows, apiImpl);
    }

    static void addRows(Table table,
                        int offset,
                        int numRows,
                        TableAPI apiImpl) {
        final Row row = table.createRow();
        for (int i = offset; i < numRows + offset; i++) {
            row.put("id", i);
            row.put("firstName", "joe" + i);
            row.put("lastName", "cool" + i);
            row.put("age", 10 + i);
            apiImpl.put(row, null, null);
        }
    }

    private void addChildRows(Table table, int numRows, TableAPI apiImpl) {
        Row row = table.createRow();
        for (int i = 0; i < numRows; i++) {
            row.put("id", i);
            row.put("income", 1000 + i*100);
            apiImpl.put(row, null, null);
        }
    }

    private TableImpl getTable(final RepNode rn,
                               String tableName) {
        return
            (TableImpl)rn.getMetadata(MetadataType.TABLE,
                                    new TableMetadata.TableMetadataKey(tableName),
                                      0,
                                      SerialVersion.CURRENT);
    }

    static void updateMetadata(RepNode rn, TableMetadata md) {
        rn.updateMetadata(clone(md));
    }

    /* Clone the  MD to simulate remote call to the RN */
    private static Metadata<?> clone(Metadata<?> md) {
        return SerializationUtil.getObject(SerializationUtil.getBytes(md),
                                           md.getClass());
    }

    static TableImpl addTable(TableMetadata md, TableImpl table) {
        return md.addTable(table.getInternalNamespace(),
                           table.getName(),
                           table.getParentName(),
                           table.getPrimaryKey(),
                           null,
                           table.getShardKey(),
                           table.getFieldMap(),
                           null, null,
                           false, 0,
                           null, null);
    }

    static void addIndex(TableMetadata md,
                                 String indexName,
                                 TableImpl table,
                                 String fieldName) {
        md.addIndex(table.getInternalNamespace(),
                    indexName,
                    table.getFullName(),
                    makeIndexList(fieldName),
                    null, true, false, null);
    }
}
