/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.table.Row;

import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This test is currently disabled in the unit test run in build.xml.  It
 * contains 2 test cases (testErasureBackupAndDirectRestore and
 * testErasureBackupAndLoad) that test erasure with snapshot backup and
 * recover which is not currently supported.  This functionality may become
 * supported in future at which point we can run this test.
 */

public class TableErasureTest extends TableTestBase {

    private static final int N_USERS = 100000;
    private static final long ERASURE_PERIOD_S = 120;
    private static final long CHKPT_BYTES_INTERVAL = 1000000;
    private static final long LOG_MAX_SIZE=1000000;

    /*
     * We disable JE cleaner in order to reproduce the problem with backup and
     * restore, where the snapshot is using hard links to JE log files.  If
     * cleaner is run, the log files would be removed thus hard links would
     * not be created.
     */
    private static void initPolicyMap() {
        final String erasureParams = String.format(
            "je.env.runEraser=true;" +
            "je.erase.period=%d s;" +
            "je.erase.deletedDatabases=true;" +
            "je.erase.extinctRecords=true;" +
            "je.checkpointer.bytesInterval=%d;" +
            "je.log.fileMax=%d;" +
            "je.env.runCleaner=false",
            ERASURE_PERIOD_S,
            CHKPT_BYTES_INTERVAL,
            LOG_MAX_SIZE);

        policyMap = new ParameterMap();
        policyMap.setParameter(ParameterState.JE_MISC, erasureParams);
    }

    /* Taken from TableBackupTest */
    private TableImpl buildUserTable() throws Exception {
        TableImpl table = TableBuilder.createTableBuilder("User")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addString("address")
            .addBinary("binary", null)
            .primaryKey("lastName", "firstName")
            .shardKey("lastName")
            .buildTable();

        table = addTable(table);
        table = addIndex(table, "FirstName",
                         new String[] {"firstName"}, true);
        table = addIndex(table, "Age",
                         new String[] {"age"}, true);
        return table;
    }

    /* Taken from TableBackupTest */
    private void addUserRows(TableImpl table, int num, String prefix) {
        for (int i = 0; i < num; i++) {
            byte[] bytes = new byte[0];
            Row row = table.createRow();
            row.put("firstName", (prefix + "first" + i));
            row.put("lastName", (prefix + "last" + i));
            row.put("age", i+10);
            row.put("address", "10 Happy Lane");
            row.put("binary", bytes);
            tableImpl.put(row, null, null);
        }
    }

    private static void verifyJEParams() throws Exception {
        CommandServiceAPI cs = createStore.getAdmin();
        RepNodeId rnId = new RepNodeId(1, 1);
        ParameterMap map = cs.getRepNodeParameters(rnId);
        String jeParams = map.get(ParameterState.JE_MISC).asString();
        System.out.printf("JE parameters from %s: %s\n", rnId.toString(),
                jeParams);
    }

    @BeforeClass
    public static void staticSetUp() throws Exception {
        initPolicyMap();
        TableTestBase.staticSetUp();
        System.out.println("Started store with erasure parameters");
    }

    /*
     * 1) Create and populate table.
     * 2) Create snapshot.
     * 3) Drop the table and wait for erasure to occur.
     * 4) Restore from the snapshot.
     * Because currently the snapshot uses hard links to DB log files,
     * the snapshot will be corrupted after erasure occurs.  We will
     * not be able to restart the store from the snapshot.
     */
    @Test
    public void testErasureBackupAndDirectRestore() throws Exception {
        verifyJEParams();
        final TableImpl userTable = buildUserTable();
        System.out.printf("Created table %s, table id: %d\n",
                userTable.getName(), userTable.getId());
        addUserRows(userTable, N_USERS, "");
        System.out.printf("Inserted %d rows\n", N_USERS);
        int numRows = countTableRows(userTable.createPrimaryKey(), null);
        assertEquals(N_USERS, numRows);
        System.out.printf("Counted %d rows\n", numRows);

        String snapshotName = createSnapshot("table");
        System.out.println("Created snapshot");

        executeDdl("DROP TABLE " + userTable.getName(), true);
        final long waitTimeMs = ERASURE_PERIOD_S * 1000 * 3;
        System.out.printf("Dropped table, waiting %d ms for erasure\n",
                waitTimeMs);
        Thread.sleep(waitTimeMs);

        createStore.setRestoreSnapshot(snapshotName);
        System.out.println("Restarting from snapshot");
        restartStore();
        System.out.println("Restarted");

        numRows = countTableRows(userTable.createPrimaryKey(), null);
        System.out.printf("Counted %d rows\n", numRows);
        assertEquals(N_USERS, numRows);
    }

    /*
     * Same as above, except load from the snapshot to different store.
     * This will fail for the same reason.
     */
    @Test
    public void testErasureBackupAndLoad() throws Exception {

        final TableImpl userTable = buildUserTable();
        System.out.printf("Created table %s, table id: %d\n",
                userTable.getName(), userTable.getId());
        addUserRows(userTable, N_USERS, "");
        System.out.printf("Inserted %d rows\n", N_USERS);
        int numRows = countTableRows(userTable.createPrimaryKey(), null);
        assertEquals(N_USERS, numRows);
        System.out.printf("Counted %d rows\n", numRows);

        String snapshotName = createSnapshot("table");
        System.out.println("Created snapshot");

        executeDdl("DROP TABLE " + userTable.getName(), true);
        final long waitTimeMs = ERASURE_PERIOD_S * 1000 * 3;
        System.out.printf("Dropped table, waiting %d ms for erasure\n",
                waitTimeMs);
        Thread.sleep(waitTimeMs);

        if (restoreStore == null) {
            startRestoreStore();
        }
        loadToRestoreStore(snapshotName);

        boolean equal = compareStoreTableMetadata(createStore, restoreStore);
        assertTrue(equal);
        equal = compareStoreKVData(store, rsStore);
        assertTrue(equal);

        TableImpl userTable2 = (TableImpl)restoreTableImpl.getTable(
                userTable.getName());
        assertNotNull(userTable2);
        int numRows2 = countTableRows(userTable2.createPrimaryKey(), null);
        assertEquals(numRows2, numRows);
    }

}
