/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl.TableMetadataCallback;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.Index;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests both the TableMetadataCallback handler and the use
 * of an application-supplied TableMetadataHelper.
 *
 * NOTE: These tests are highly sensitive to changes in several areas and in
 * a sense do not add much value. Things that affect the metadata callabacks
 * (and success of the test) include:
 *  - The metadata sequence numbers govern when a callback is needed so updates
 *    outside of the call being made in the test can affect the counts. This is
 *    particularly true for DDL creation as that uses the region mapper. System
 *    table may also impact the test if that is delayed.
 *  - Though unlikely the metadata broadcast may cause a failure to get the
 *    correct table metadata. This is reduced by the 1x1 store.
 *  - Any improvements in making callbacks more timely to metadata changes will
 *    obviously change the counts.
 */
public class TableMetadataCallbackTest extends TableTestBase {

    private final static String createTableT1 =
        "CREATE TABLE t1(id INTEGER, name STRING, PRIMARY KEY(id))";
    private final static String createTableT2 =
        "CREATE TABLE t2(id INTEGER, name STRING, PRIMARY KEY(id))";

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /*
         * Create 1x1 store to avoid timing issues with metadata propagation
         * and to speed up the test.
         *
         * Use a separate KVStore to mock stream agent. Some tests
         * check the number of times that TableMetadataCallback is
         * notified about the metadata changes. This number is stored
         * in the TableAPI instance associated with the kvstore and
         * extra tableapi calls invoked by the mock stream agent
         * cause the number to not match the expected number, so
         * a separate kvstore instance is used for the mock agent.
         * */
        TableTestBase.staticSetUp(1, 1, 1, false /*excludeTombstone*/,
                                  true /*separateMRStore*/);
    }

    /**
     * Use 2 TableAPI handler to perform ddl operations and data access
     * operations in a single thread, verify the TableMetadataCallback
     * handler is invoked after the table metadata has been changed.
     */
    @Test
    public void testSingleThread() {

        KVStoreImpl store0 = (KVStoreImpl)store;
        KVStoreImpl store1 =
            (KVStoreImpl)KVStoreFactory.getStore(createKVConfig(createStore));
        int invokedCount0 = 0, invokedCount1 = 0;

        TableAPIImpl tableHdl0 = (TableAPIImpl) store0.getTableAPI();
        TableAPIImpl tableHdl1 = (TableAPIImpl) store1.getTableAPI();

        /*
         *  Make sure the store is up and system table activity is done
         *  and we use the MD table for metadata operations.
         */
        waitForStoreReady(createStore.getAdmin(), true);
        tableHdl0.setEnableTableMDSysTable(true);
        tableHdl1.setEnableTableMDSysTable(true);

        tableHdl0.setTableMetadataCallback(new MetadataCallbackImpl());
        tableHdl1.setTableMetadataCallback(new MetadataCallbackImpl());
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /* sets metadatahelper */
        configureHelper(tableHdl0);
        configureHelper(tableHdl1);

        /*
         * Use store0 handle: execute "create table"
         */
        executeDdl(createTableT1);
        invokedCount0++;
        getTableAndCheckVersion(tableHdl0, "t1", 1);
        invokedCount0++;
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);

        /*
         * Use store1 handle: put a row
         */
        Table t1 = getTableAndCheckVersion(tableHdl1, "t1", 1);
        invokedCount1++;
        Row row = createRow(t1, 1);
        tableHdl1.put(row, null, null);
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - Execute "alter table.."
         *   - get a row
         */
        executeDdl("ALTER TABLE t1(ADD i1 INTEGER)");
        t1 = getTableAndCheckVersion(tableHdl0, "t1", 2);
        invokedCount0++;
        tableHdl0.get(createKey(t1, 0), null);
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);

        /*
         * Use store1 handle:
         *   - get a row
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 2);
        PrimaryKey key = createKey(t1, 1);
        tableHdl1.get(key, null);
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "alter table.."
         */
        executeDdl("ALTER TABLE t1(ADD i2 INTEGER)");
        getTableAndCheckVersion(tableHdl0, "t1", 3);
        invokedCount0++;

        /*
         * Use store1 handle:
         *   - delete a row.
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 3);
        tableHdl1.delete(key, null, null);
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "alter table.."
         */
        executeDdl("ALTER TABLE t1(ADD i3 INTEGER)");
        getTableAndCheckVersion(tableHdl0, "t1", 4);
        invokedCount0++;
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);
        /*
         * Use store1 handle:
         *   - multiDelete op.
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 4);
        tableHdl1.multiDelete(createKey(t1, 1), null, null);
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "alter table.."
         */
        executeDdl("ALTER TABLE t1(ADD i4 INTEGER)");
        getTableAndCheckVersion(tableHdl0, "t1", 5);
        invokedCount0++;
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);
        /*
         * Use store1 handle:
         *   - multiGet op.
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 5);
        tableHdl1.multiGet(createKey(t1, 1), null, null);
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "alter table.."
         */
        executeDdl("ALTER TABLE t1(ADD i5 INTEGER)");
        getTableAndCheckVersion(tableHdl0, "t1", 6);
        invokedCount0++;
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);
        /*
         * Use store1 handle:
         *   - tableIterator(PrimaryKey).
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 6);
        key = t1.createPrimaryKey();
        TableIterator<Row> iter = tableHdl1.tableIterator(key, null, null);
        while(iter.hasNext()) {
            iter.next();
        }
        iter.close();
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "alter table.."
         */
        executeDdl("ALTER TABLE t1(ADD i6 INTEGER)");
        getTableAndCheckVersion(tableHdl0, "t1", 7);
        invokedCount0++;
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);
        /*
         * Use store1 handle:
         *   - bulk put
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 7);
        tableHdl1.put(Arrays.asList(new LoadStream(t1, 100)), null);
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "create table t2.."
         */
        String namespace = "namespace2";
        executeDdl(createTableT2, namespace);
        getTableAndCheckVersion(tableHdl0, namespace, "t2", 1);
        invokedCount0++;

        /*
         * Use store1 handle:
         *   - query
         */
        store1.executeSync("SELECT * FROM t2",
            new ExecuteOptions().setNamespace(namespace, false));
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "create index idx1.."
         */
        executeDdl("CREATE INDEX idx1 ON t1(name)");
        t1 = getTableAndCheckVersion(tableHdl0, "t1", 7);
        assertTrue(t1.getIndex("idx1") != null);
        invokedCount0++;

        /*
         * Use store1 handle:
         *   - bulk get
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 7);
        assertTrue(t1.getIndex("idx1") != null);
        iter = tableHdl1.tableIterator(new KeyStream(t1, 100), null, null);
        while(iter.hasNext()) {
            iter.next();
        }
        iter.close();
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "create index idx2.."
         */
        executeDdl("CREATE INDEX idx2 ON t1(name, id)");
        t1 = getTableAndCheckVersion(tableHdl0, "t1", 7);
        assertTrue(t1.getIndex("idx2") != null);
        invokedCount0++;
        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);

        /*
         * Use store1 handle:
         *   - tableIterator(IndexKey)
         */
        t1 = getTableAndCheckVersion(tableHdl1, "t1", 7);
        Index idx = t1.getIndex("idx2");
        iter = tableHdl1.tableIterator(idx.createIndexKey(), null, null);
        while(iter.hasNext()) {
            iter.next();
        }
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);

        /*
         * Use store0 handle:
         *   - execute "drop table t1"
         */
        executeDdl("DROP TABLE t1");
        assertTrue(tableHdl0.getTable(null, "t1") == null);

        /*
         * Use store1 handle:
         *   - get a row from t1
         */
        try {
            tableHdl1.get(createKey(t1, 1), null);
            fail("Expect to fail but not");
        } catch (MetadataNotFoundException ignored) {
        }
        invokedCount1++;
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);
        assertTrue(tableHdl1.getTable("t1") == null);

        /*
         * Use store0 handle:
         *   - execute "drop table t2"
         *   - execute "create table t2"
         */
        executeDdl("DROP TABLE t2", namespace);
        final String createTableT2New = "CREATE TABLE t2 (" +
            "id INTEGER, name STRING, age INTEGER, PRIMARY KEY(id))";
        executeDdl(createTableT2New, namespace);
        Table t2 = getTableAndCheckVersion(tableHdl0, namespace, "t2", 1);
        assertTrue(t2.getField("age") != null);
        invokedCount0++;

        /*
         * Use store1 handle:
         *   - get a row from t2
         */
        t2 = getTableAndCheckVersion(tableHdl1, namespace, "t2", 1);
        assertTrue(t2.getField("age") != null);
        invokedCount1++;

        checkMetadataCallbackInvoked(tableHdl0, invokedCount0);
        checkMetadataCallbackInvoked(tableHdl1, invokedCount1);
        checkHelperInvoked(tableHdl0, true);
        checkHelperInvoked(tableHdl1, true);
    }

    /**
     * 2 kinds of threads using 2 TableAPI handles:
     *
     *  - A single DDL thread
     *    A single thread to evolve tables t1, t2 and t3 with a TableAPI
     *    Handle.
     *
     *  - 10 exercise threads:
     *    Multiple threads to perform get operation on tables t1, t2 and t3
     *    sharing another TableAPI handle.
     */
    @Test
    public void testMultipleThreads()
        throws InterruptedException {

        final int numRows = 100;
        final int numEvolve = 5;
        final int numTables = 3;
        /* The number of exercise threads. */
        final int nThreads = 10;

        final KVStore store0 = store;
        final String createTableT3 =
            "CREATE TABLE t3(id INTEGER, name STRING, PRIMARY KEY(id))";
        KVStoreImpl store1 =
            (KVStoreImpl)KVStoreFactory.getStore(createKVConfig(createStore));

        TableAPIImpl tableHdl0 = (TableAPIImpl) store0.getTableAPI();
        TableAPIImpl tableHdl1 = (TableAPIImpl) store1.getTableAPI();

        /*
         *  Make sure the store is up and system table activity is done
         *  and we use the MD table for metadata operations.
         */
        waitForStoreReady(createStore.getAdmin(), true);
        tableHdl0.setEnableTableMDSysTable(true);
        tableHdl1.setEnableTableMDSysTable(true);

        executeDdl(createTableT1);
        executeDdl(createTableT2);
        executeDdl(createTableT3);

        Table t1 = getTableAndCheckVersion(tableHdl0, "t1", 1);
        Table t2 = getTableAndCheckVersion(tableHdl0, "t2", 1);
        Table t3 = getTableAndCheckVersion(tableHdl0, "t3", 1);

        loadRows(tableHdl0, t1, numRows);
        loadRows(tableHdl0, t2, numRows);
        loadRows(tableHdl0, t3, numRows);

        tableHdl0.setTableMetadataCallback(new MetadataCallbackImpl());
        tableHdl1.setTableMetadataCallback(new MetadataCallbackImpl());
        /* sets metadatahelper */
        configureHelper(tableHdl0);
        configureHelper(tableHdl1);

        DdlThread ddl = new DdlThread(store0, numTables, numEvolve);
        ddl.start();

        final List<ExerciseThread> tasks = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            ExerciseThread t = new ExerciseThread(i, store1, numTables, numRows);
            tasks.add(t);
            t.start();
        }

        /* Wait for the completion of ddl thread */
        ddl.join();

        /* Stop all exercise threads */
        for (ExerciseThread t : tasks) {
            t.done();
        }

        /* Wait for completion of all exercises threads */
        for (ExerciseThread t : tasks) {
            t.join();
        }

        /**
         * Callback 0 is called for each evolve. However due to the multiple
         * threads the actual count may vary due to timing variations.
         */
        checkMetadataCallbackInvoked(tableHdl0, (numTables * numEvolve), 1);
        checkMetadataCallbackInvoked(tableHdl1, (numTables * numEvolve), 4);
        checkHelperInvoked(tableHdl0, true);
        checkHelperInvoked(tableHdl1, false);
    }

    /**
     * DDL thread to evolve tables.
     */
    private class DdlThread extends Thread {

        private final KVStore storeHdl;
        private final int numEvolve;
        private final int numTables;

        DdlThread(KVStore store, int numTables, int numEvolve) {
            this.storeHdl = store;
            this.numTables = numTables;
            this.numEvolve = numEvolve;
            setName("DdlThread");
        }

        @Override
        public void run() {
            int version = 1;
            for (int i = 0; i < numEvolve; i++) {
                version++;
                for (int n = 0; n < numTables; n++) {
                    String tableName = "t" + (n + 1);
                    String ddl = getEvolveDdl(tableName, i);

                    StatementResult sr = storeHdl.executeSync(ddl);
                    assertTrue("Fail to execute " + ddl,
                        sr.isSuccessful() && sr.isDone());

                    Table table = storeHdl.getTableAPI().getTable(null, tableName);

                    assertTrue("Wrong table version: " + table.getTableVersion(),
                        table.getTableVersion() == version);
                }
            }
        }

        private String getEvolveDdl(String tableName, int n) {
            return "ALTER TABLE " + tableName + " (ADD i" + n + " INTEGER)";
        }
    }

    /**
     * The Exercise thread to perform get operation on the tables and verify
     * the table version of TableImpl instance with the evolution of tables.
     */
    private class ExerciseThread extends Thread {
        private final Random rand;
        private final TableAPI tableHdl;
        private final int numTables;
        private final int numRows;
        private final Map<String, Integer> versions;
        private volatile boolean isDone;

        ExerciseThread(int threadId,
                       KVStore storeHdl,
                       int numTables,
                       int numRows) {
            this.tableHdl = storeHdl.getTableAPI();
            this.numRows = numRows;
            this.numTables = numTables;
            isDone = false;
            rand = new Random(threadId);
            versions = new HashMap<String, Integer>();
            setName("exercise-" + threadId);
        }

        public void done() {
            isDone = true;
        }

        @Override
        public void run() {
            while (!isDone) {
                for (int i = 0; i < numRows; i++) {
                    if (isDone) {
                        break;
                    }

                    String tableName = getTableName(rand.nextInt(numTables));
                    Table table = tableHdl.getTable(tableName);
                    Integer version = versions.get(tableName);
                    if (version != null) {
                        if (table.getTableVersion() < version) {
                            fail("Wrong table version, it should not less " +
                                 "than the previous value " + version +
                                 ": " + table.getTableVersion());
                        }
                    }

                    if (version == null || table.getTableVersion() > version) {
                        versions.put(tableName, table.getTableVersion());
                    }

                    PrimaryKey key = createKey(table, i);
                    assertTrue("get row failed: " + key.toJsonString(false),
                        tableHdl.get(key, null) != null);
                }
            }
        }
    }

    private String getTableName(int i) {
        return "t" + (i + 1);
    }

    private Table getTableAndCheckVersion(TableAPIImpl tableHdl,
                                          String tableName,
                                          int expVersion) {
        return getTableAndCheckVersion(tableHdl, null, tableName, expVersion);
    }

    private Table getTableAndCheckVersion(TableAPIImpl tableHdl,
                                          String namespace,
                                          String tableName,
                                          int expVersion) {
        Table t = tableHdl.getTable(namespace, tableName, true /*bypassCache*/);
        assertEquals("Wrong table version", expVersion, t.getTableVersion());

        /* The get w/bypass==true should have updated the cache */
        Table t2 = tableHdl.getTable(namespace, tableName);
        assertEquals("Wrong table version from cache",
                     expVersion, t2.getTableVersion());
        return t;
    }

    /**
     * Checks the actual callback count with the expected count +/- the
     * value of plusMinus.
     */
    private void checkMetadataCallbackInvoked(TableAPI tableHdl,
                                              int expectedCount) {
        checkMetadataCallbackInvoked(tableHdl, expectedCount, 0);
    }

    /**
     * Checks the actual callback count with the expected count +/- the
     * value of plusMinus.
     */
    private void checkMetadataCallbackInvoked(TableAPI tableHdl,
                                              int expectedCount,
                                              int plusMinus) {
        final MetadataCallbackImpl mcb =
            (MetadataCallbackImpl)((TableAPIImpl)tableHdl).
                                                    getTableMetadataCallback();
        final int actual = mcb.getInvokeCount();

        if ((actual > expectedCount + plusMinus) ||
            (actual < expectedCount - plusMinus)) {
            assertEquals("Wrong callback count", expectedCount, actual);
        }
    }

    private void checkHelperInvoked(TableAPI tableHdl, boolean called) {
        MetadataHelperImpl mh =
            (MetadataHelperImpl)((TableAPIImpl)tableHdl)
            .getTableMetadataHelper();
        if (called) {
            assertTrue(mh.getInvokeCount() > 0);
        } else {
            assertTrue(mh.getInvokeCount() == 0);
        }
    }

    private void loadRows(TableAPI tableHdl, Table table, int numRows) {
        for (int i = 0; i < numRows; i++) {
            Row row = createRow(table, i);
            assertTrue(tableHdl.put(row, null, null) != null);
        }
    }

    private static Row createRow(Table table, int id) {
        Row row = table.createRow();
        for (String name : table.getFields()) {
            if (name.equalsIgnoreCase("id")) {
                row.put("id", id);
            } else if (name.equalsIgnoreCase("name")) {
                row.put("name", "name_" + id);
            } else {
                if (table.getField(name).isInteger()) {
                    row.put(name, id);
                } else {
                    assert(table.getField(name).isString());
                    row.put(name, "s_" + id);
                }
            }
        }
        return row;
    }

    private static PrimaryKey createKey(Table table, int id) {
        PrimaryKey key = table.createPrimaryKey();
        key.put("id", id);
        return key;
    }

    private static class MetadataCallbackImpl implements TableMetadataCallback {

        private int invokeCnt = 0;

        @Override
        public void metadataChanged(int oldSeqNum, int newSeqNum) {
            assertTrue("Local seqNum should less than remote SeqNum, but " +
                "actual " + oldSeqNum + " vs " + newSeqNum,
                oldSeqNum < newSeqNum);
            invokeCnt++;
        }

        public int getInvokeCount() {
            return invokeCnt;
        }
    }

    private MetadataHelperImpl configureHelper(TableAPIImpl impl) {
        return new MetadataHelperImpl(impl);
    }

    private static class MetadataHelperImpl implements TableMetadataHelper {

        private int invokeCnt = 0;
        private final TableMetadataHelper helper;

        MetadataHelperImpl(TableAPIImpl tableAPI) {
            helper = tableAPI.getTableMetadataHelper();
            tableAPI.setCachedMetadataHelper(this);
        }

        @Override
        public TableImpl getTable(String namespace, String tableName) {
            invokeCnt++;
            return helper.getTable(namespace, tableName);

        }

        @Override
        public TableImpl getTable(String namespace,
                                  String[] tablePath,
                                  int cost) {
            invokeCnt++;
            return helper.getTable(namespace, tablePath, cost);
        }

        @Override
        public RegionMapper getRegionMapper() {
            return helper.getRegionMapper();
        }

        public int getInvokeCount() {
            return invokeCnt;
        }
    }

    private static class KeyStream implements Iterator<PrimaryKey> {

        private final Table table;
        private final int num;
        private int id;

        KeyStream(Table table, int num) {
            this.table = table;
            this.num = num;
        }

        @Override
        public boolean hasNext() {
            return id < num;
        }

        @Override
        public PrimaryKey next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return createKey(table, id++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    private static class LoadStream implements EntryStream<Row> {

        private final Table table;
        private final int numRows;
        private int id;

        LoadStream(Table table, int numRows) {
            this.table = table;
            this.numRows = numRows;
            id = 0;
        }

        @Override
        public String name() {
            return "load stream";
        }

        @Override
        public Row getNext() {
            return (id == numRows) ? null : createRow(table, id++);
        }

        @Override
        public void completed() {}

        @Override
        public void keyExists(Row entry) {}

        @Override
        public void catchException(RuntimeException exception, Row entry) {
            fail("Load row fail '" + exception.getMessage() + "': " +
                entry.toJsonString(false));
        }
    }
}
