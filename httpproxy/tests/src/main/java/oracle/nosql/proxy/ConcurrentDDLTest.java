/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

//import oracle.nosql.driver.IndexLimitException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
//import oracle.nosql.driver.TableLimitException;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.proxy.security.SecureTestUtil;
import oracle.nosql.util.tmi.TableRequestLimits;

import org.junit.Test;

/**
 * Concurrently DDL test:
 *  o testSingleTable:
 *    Execute ddls asynchronously to a single table.
 *  o testMultipleTables
 *    Execute ddls on multiple tables.
 *  o testMultiTenants:
 *    Execute ddls on tables in multiple tenants.
 *  o testTableIndexLimits:
 *    Create tables/indexes to reach the limits of tables/indexes.
 */
public class ConcurrentDDLTest extends ProxyTestBase {

    /*
     * The number of threads to run ddl test.
     */
    private final static int CONCURRENT_NUM = 3;
    private final static int waitMillis = 60_000;

    private final Map<String, NoSQLHandle> handleCache =
        new HashMap<String, NoSQLHandle>();

    private DDLExecutor<TableResult> ddlExecutor;

    @Override
    public void setUp() throws Exception{
        super.setUp();
        ddlExecutor = new ClientExecutor();
    }

    @Override
    public void tearDown() throws Exception {
        clearHandleCache();
        super.tearDown();
    }

    private void clearHandleCache() {
        final Iterator<Entry<String, NoSQLHandle>> iter =
            handleCache.entrySet().iterator();

        while(iter.hasNext()) {
            Entry<String, NoSQLHandle> e = iter.next();
            String tenantId = e.getKey();
            if (tenantId.equals(getTenantId())) {
                continue;
            }
            NoSQLHandle nosqlHandle = e.getValue();
            dropAllTables(nosqlHandle, false);

            if (getSCURL() != null) {
                deleteTier(tenantId);
            }
            nosqlHandle.close();
            iter.remove();
        }
    }

    @Test
    public void testSingleTable() {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        TableRequestLimits requestLimits =
            tenantLimits.getStandardTableLimits();
        final int numFields = Math.min(requestLimits.getColumnsPerTable(), 10);
        final int numIndexes = Math.min(requestLimits.getIndexesPerTable(), 3);
        final int evolveLimit = requestLimits.getSchemaEvolutions();
        final int numAddField = Math.min((evolveLimit + 1) / 2, 3);
        final int numDropField = Math.min(((evolveLimit > numAddField) ?
                                           (evolveLimit - numAddField) : 0),
                                          numAddField);

        final int numRows = 100;
        final TableLimits tableLimits = new TableLimits(1000, 500, 10);
        final String tenantId = getTenantId();
        final NoSQLHandle nosqlHandle = getTenantHandle(tenantId);

        final DDLGenerator plan = new DDLGenerator(tenantId);
        final String tableName = makeTableName(tenantId, 0);
        List<DDLInfo> ddls;

        /*
         * Create table
         */
        ddls = plan.createTable(numFields, 1, tableLimits).build(true);
        execAsyncAndWait(ddls);

        /* Load rows to table */
        loadRows(nosqlHandle, tableName, numFields, numRows);

        /*
         * Create indexes, alter table add field ..
         */
        ddls = plan.createIndex(tableName, numIndexes)
                .addField(tableName, numAddField)
                .build(true);
        execAsyncAndWait(ddls);

        /* Verify the existence of indexes and do counting by index */
        assertNumIndexes(tenantId, tableName, numIndexes);
        for (int idxIndex = 0; idxIndex < numIndexes; idxIndex++) {
            assertRowCountByIndex(nosqlHandle, tableName, idxIndex,
                                  numRows);
        }
        /* Check row value and expiration time */
        putRow(nosqlHandle, tableName, numRows, numFields);
        checkRow(nosqlHandle, tableName, numRows, numFields, numAddField);

        /*
         * Drop index, alter table drop field ..
         */
        ddls = plan.dropIndex(tableName, numIndexes)
                .dropField(tableName, numDropField)
                .build(true);
        execAsyncAndWait(ddls);

        /*
         * Verify no index existed
         */
        assertNumIndexes(tenantId, tableName, 0);
        checkRow(nosqlHandle, tableName, numRows - 1, numFields,
                 (numAddField - numDropField));
    }

    @Test
    public void testMultipleTables() {
        assumeTrue("Skip the test if not minicloud or cloud test or " +
                   "tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        TableRequestLimits requestLimits =
            tenantLimits.getStandardTableLimits();
        final int numTables = Math.min(tenantLimits.getNumTables(), 3);
        final int numFields = Math.min(requestLimits.getColumnsPerTable(), 5);
        final int numIndexes = Math.min(requestLimits.getIndexesPerTable(), 3);
        final int evolveLimit = requestLimits.getSchemaEvolutions();
        final int numAddField = Math.min((evolveLimit + 1) / 2, 3);
        final int numDropField = Math.min(((evolveLimit > numAddField) ?
                                           (evolveLimit - numAddField) : 0),
                                          numAddField);

        final int numRows = 100;
        final TableLimits tableLimits = new TableLimits(1000, 500, 10);

        final NoSQLHandle nosqlHandle = getTenantHandle(getTenantId());
        final String tenantId = getTenantId();
        final String[] tableNames = getTableNames(tenantId, numTables);

        final DDLGenerator plan = new DDLGenerator(tenantId);
        List<DDLInfo> ddls;

        /*
         * Create tables
         */
        ddls = plan.createTable(numFields, numTables, tableLimits).build(true);
        execWithThreads(CONCURRENT_NUM, ddls);

        /* Load rows to tables*/
        loadRowsToTables(nosqlHandle, CONCURRENT_NUM, tableNames,
                         numFields, numRows);

        /*
         * Create index, alter table add field ..
         */
        for (String tableName : tableNames) {
            plan.createIndex(tableName, numIndexes)
                .addField(tableName, numAddField);
        }
        ddls = plan.build(true);
        execWithThreads(CONCURRENT_NUM, ddls);

        /* Check index and row value */
        for (String tableName : tableNames) {
            assertNumIndexes(tenantId, tableName, numIndexes);
            assertRowCountByIndex(nosqlHandle, tableName,
                                  numIndexes - 1, numRows);
            checkRow(nosqlHandle, tableName, numRows - 1,
                     numFields, numAddField);
        }

        /*
         * Drop index, drop field
         */
        for (String tableName : tableNames) {
            plan.dropIndex(tableName, numIndexes)
                .dropField(tableName, numDropField);

        }
        ddls = plan.build(true);
        execWithThreads(CONCURRENT_NUM, ddls);

        /* Check index and row value */
        for (String tableName : tableNames) {
            assertNumIndexes(tenantId, tableName, 0);
            checkRow(nosqlHandle, tableName, numRows - 1, numFields,
                     (numAddField - numDropField));
        }

        /*
         * Drop tables
         */
        ddls = plan.dropTable(numTables).build(true);
        execWithThreads(CONCURRENT_NUM, ddls);
        assertNumTables(tenantId, 0);
    }

    @Test
    public void testMultiTenants() {
        /* This test needs 3 tenants, it is not applicable in cloud test */
        assumeTrue("Skip this test if not minicloud test", useMiniCloud);

        final int numTenants = 3;

        TableRequestLimits requestLimits =
            tenantLimits.getStandardTableLimits();
        final int numTables = Math.min(tenantLimits.getNumTables(), 3);
        final int numFields = Math.min(requestLimits.getColumnsPerTable(), 5);
        final int numIndexes = Math.min(requestLimits.getIndexesPerTable(), 3);
        final int evolveLimit = requestLimits.getSchemaEvolutions();
        final int numAddField = Math.min((evolveLimit + 1) / 2, 3);
        final int numDropField = Math.min(((evolveLimit > numAddField) ?
                                           (evolveLimit - numAddField) : 0),
                                          numAddField);

        final int numRows = 100;
        final TableLimits tableLimits = new TableLimits(1000, 500, 10);
        final String[] tenantIds = new String[numTenants];
        for (int i = 0; i < numTenants; i++) {
            String tenantId = makeTenantId(i);
            if (getSCURL() != null) {
                addTier(tenantId, tenantLimits);
            }
            tenantIds[i] = tenantId;
        }

        final DDLGenerator plan = new DDLGenerator(getTenantId());
        List<DDLInfo> ddls = null;

        /*
         * Create tables
         */
        for (String tenantId : tenantIds) {
            plan.setTenantId(tenantId)
                .createTable(numFields, numTables, tableLimits);
        }
        ddls = plan.build(true);
        execWithThreads(CONCURRENT_NUM, ddls);

        /* Load rows to tables */
        for (String tenantId : tenantIds) {
            NoSQLHandle nosqlHandle = getTenantHandle(tenantId);
            String[] tableNames = getTableNames(tenantId, numTables);
            loadRowsToTables(nosqlHandle, CONCURRENT_NUM, tableNames,
                             numFields, numRows);
        }

        /*
         * Create Indexes, alter table add fields
         */
        for (int tableIndex = 0; tableIndex < numTables; tableIndex++) {
            for (String tenantId : tenantIds) {
                String tableName = makeTableName(tenantId, tableIndex);
                plan.setTenantId(tenantId)
                    .createIndex(tableName, numIndexes)
                    .addField(tableName, numAddField);
            }
        }
        ddls = plan.build(true);
        execWithThreads(CONCURRENT_NUM, ddls);

        /* Verify after create index, alter table add field */
        for (String tenantId : tenantIds) {
            NoSQLHandle nosqlHandle = getTenantHandle(tenantId);
            for (int tableIndex = 0; tableIndex < numTables; tableIndex++) {
                String tableName = makeTableName(tenantId, tableIndex);
                assertNumIndexes(tenantId, tableName, numIndexes);
                assertRowCountByIndex(nosqlHandle, tableName,
                                      numIndexes - 1, numRows);
                /* Check row value */
                checkRow(nosqlHandle, tableName, numRows - 1, numFields,
                         numAddField);
            }
        }

        /*
         * Drop Indexes and drop field
         */
        for (int tableIndex = 0; tableIndex < numTables; tableIndex++) {
            for (String tenantId : tenantIds) {
                String tableName = makeTableName(tenantId, tableIndex);
                plan.setTenantId(tenantId)
                    .dropIndex(tableName, numIndexes)
                    .dropField(tableName, numDropField);
            }
        }
        ddls = plan.build(true);
        execWithThreads(CONCURRENT_NUM, ddls);

        /* Verify after drop index and drop field */
        for (String tenantId : tenantIds) {
            NoSQLHandle nosqlHandle = getTenantHandle(tenantId);
            for (int tableIndex = 0; tableIndex < numTables; tableIndex++) {
                String tableName = makeTableName(tenantId, tableIndex);
                assertNumIndexes(tenantId, tableName, 0);
                /* Check row value */
                checkRow(nosqlHandle, tableName, numRows - 1, numFields,
                         (numAddField - numDropField));
            }
        }

        /*
         * Drop tables
         */
        for (String tenantId : tenantIds) {
            plan.setTenantId(tenantId).dropTable(numTables);
        }
        ddls = plan.build(true);
        execWithThreads(CONCURRENT_NUM, ddls);
        /* No table exists */
        for (String tenantId : tenantIds) {
            assertNumTables(tenantId, 0);
        }
    }

    @Test
    public void testTableIndexLimits() {
        /*
         * This test aim to create the max number of tables in a tenant, it is
         * not applicable for cloud testing
         */
        assumeTrue("Skip this test if not minicloud test", useMiniCloud);

		TableRequestLimits requestLimits =
            tenantLimits.getStandardTableLimits();
        final int numTables = tenantLimits.getNumTables();
        final int numIndexes = requestLimits.getIndexesPerTable();
        final int numFields = numIndexes + 1;

        final TableLimits tableLimits = new TableLimits(1000, 500, 10);
        final String tenantId = getTenantId();

        final DDLGenerator plan = new DDLGenerator(tenantId);
        List<DDLInfo> ddls = null;

        /* Create 2 tables with X column, X is column number limit per table */
        ddls = plan.createTable(numFields, 2, tableLimits).build(true);
        execAsyncAndWait(ddls);
        assertNumTables(tenantId, 2);

        /*
         * Create M indexes on a table, M is the index number limit per table.
         */
        final String table0Name = makeTableName(tenantId, 0);
        ddls = plan.createIndex(table0Name, numIndexes).build(true);
        execAsyncAndWait(ddls);
        assertNumIndexes(tenantId, table0Name, numIndexes);

        /*
         * Create M + 1 indexes on a table, M is the index number limit per
         * table. Creating last index should have failed.
         *
         * TODO: bug?
         */
        /*final String table1Name = makeTableName(tenantId, 1);
        ddls = plan.createIndex(table1Name, numIndexes + 1).build(true);
        execWithThreads(numThreads, ddls, IndexLimitException.class, 1);
        assertNumIndexes(tenantId, table1Name,  numIndexes); */

        /* drop tables */
        ddls = plan.dropTable(2).build(true);
        execAsyncAndWait(ddls);
        assertNumTables(tenantId, 0);

        /*
         * Create N tables, N is the table number limit.
         */
        ddls = plan.createTable(numFields, numTables, tableLimits).build(true);
        execWithThreads(CONCURRENT_NUM, ddls);
        assertNumTables(tenantId, numTables);

        /*
         * Drop last i tables, then create i + 1 tables concurrently, the total
         * number of tables is N + 1 that exceeded the limit N, so creating
         * the last table should have failed.
         *
         * TODO: bug?
         */
        /*
        int nd = 2;
        String[] tableNames = getTableNames(tenantId, numTables - nd, nd);
        ddls = plan.dropTable(tableNames).build(true);
        execWithThreads(numThreads, ddls);
        assertNumTables(tenantId, numTables - nd);

        tableNames = getTableNames(tenantId, numTables - nd, nd + 1);
        ddls = plan.createTable(numFields, tableNames, tableLimits).build(true);
        execWithThreads(numThreads, ddls, TableLimitException.class, 1);
        assertNumTables(tenantId, numTables); */

        /* Drop all tables */
        ddls = plan.dropTable(numTables).build(true);
        execWithThreads(CONCURRENT_NUM, ddls);
        assertNumTables(tenantId, 0);
    }

    private static String[] getTableNames(String tenantId, int numTables) {
        return getTableNames(tenantId, 0, numTables);
    }

    private static String[] getTableNames(String tenantId, int from, int num) {
        final String[] tableNames = new String[num];
        for (int i = 0; i < tableNames.length; i++) {
            tableNames[i] = makeTableName(tenantId, from + i);
        }
        return tableNames;
    }

    private void loadRows(NoSQLHandle nosqlHandle,
                          String tableName,
                          int numFields,
                          int numRows) {
        final PutRequest putReq = new PutRequest().setTableName(tableName);

        PutResult putRet;
        for (int i = 0; i < numRows; i++) {
            MapValue row = createRow(i, numFields);
            putReq.setValue(row);
            try {
                putRet = nosqlHandle.put(putReq);
                assertNotNull(putRet.getVersion());
            } catch (Exception ex) {
                fail("Failed to put row to table " + tableName + ": " +
                     ex.getMessage());
            }
        }
    }

    private void execAsyncAndWait(List<DDLInfo> ddls) {

        final Map<String, List<TableResult>> results =
            new HashMap<String, List<TableResult>>();

        String tenantId;
        TableResult tret;
        List<TableResult> trets;
        for (DDLInfo ddl : ddls) {
            tenantId = ddl.getTenantId();
            try {
                tret = ddlExecutor.execNoWait(ddl);
            } catch (Throwable t) {
                fail("Execute " + ddl + " failed: " + t);
                return;
            }
            if (results.containsKey(tenantId)) {
                trets = results.get(tenantId);
            } else {
                trets = new ArrayList<TableResult>();
                results.put(tenantId, trets);
            }
            trets.add(tret);
        }

        /* Wait for completion of ddls' executions */
        for (Entry<String, List<TableResult>> e : results.entrySet()) {
            for (TableResult ret : e.getValue()) {
                try {
                    ddlExecutor.waitForDone(e.getKey(), waitMillis, ret);
                } catch (Throwable t) {
                    fail("WaitForDone failed: " + t);
                }
            }
        }
    }

    private void execWithThreads(int numThreads, List<DDLInfo> ddls) {
        execWithThreads(numThreads, ddls, null, 0);
    }

    private void execWithThreads(int numThreads,
                                 List<DDLInfo> ddls,
                                 Class<?> expectedExceptionClass,
                                 int expNumException) {

        final ArrayBlockingQueue<DDLInfo> ddlQueue =
            new ArrayBlockingQueue<DDLInfo>(ddls.size(), true, ddls);

        /* Start threads */
        final List<Thread> threads = new ArrayList<Thread>(numThreads);
        final TestExceptionHandler handler =
            new TestExceptionHandler(expectedExceptionClass);

        for (int i = 0; i < numThreads; i++) {
            Thread thd = new DDLThread(ddlQueue, "ddlThd" + i);
            threads.add(thd);
            thd.setUncaughtExceptionHandler(handler);
            thd.start();
        }

        /* Join all threads */
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (Exception ex) {
                fail("Wait for thread " + thread + ": " + ex);
            }
        }

        /* Check if expect to catch exception */
        Map<String, Throwable> failures = handler.getUnexpectedException();
        if (!failures.isEmpty()) {
            for (Entry<String, Throwable> e : failures.entrySet()) {
                System.err.println(
                    "Unexpected exception caught from " + e.getKey());
                e.getValue().printStackTrace();
            }
            fail("DDLThreads execute failed " + failures.keySet());
        }

        if (expectedExceptionClass != null) {
            String exCls = expectedExceptionClass.getName();
            int actNumEx = handler.getNumExpectedException();
            if (expNumException > 0) {
                assertEquals("Expect to catch " + exCls + " " +
                             expNumException + " times but caught " +
                             actNumEx + " times", expNumException, actNumEx);
            } else {
                assertTrue("Expect to catch " + exCls + " " + expNumException +
                           " times but not",
                           actNumEx > 0);
            }
        }
    }

    /**
     * Load rows to the specified tables
     */
    private void loadRowsToTables(NoSQLHandle nosqlHandle,
                                  int numThreads,
                                  String[] tableNames,
                                  int numFields,
                                  int numRows) {

        final ExecutorService executor =
            Executors.newFixedThreadPool(numThreads);
        final ArrayList<Future<Integer>> futures =
            new ArrayList<Future<Integer>>(numThreads);

        for (String tableName : tableNames) {
            LoadTask task = new LoadTask(nosqlHandle, tableName, numFields,
                                         numRows);
            futures.add(executor.submit(task));
        }
        executor.shutdown();

        for (Future<Integer> f : futures) {
            try {
                int count = f.get().intValue();
                assertEquals(numRows, count);
            } catch (Exception ex) {
                fail("LoadTask failed: " + ex);
            }
        }
    }

    private void putRow(NoSQLHandle nosqlHandle,
                        String tableName,
                        int id,
                        int numFields) {
        final MapValue row = createRow(id, numFields, 0);
        PutRequest putReq = new PutRequest()
            .setTableName(tableName)
            .setValue(row);
        try {
            PutResult putRet = nosqlHandle.put(putReq);
            assertNotNull(putRet.getVersion());
        } catch (Exception ex) {
            fail("Failed to put row to table " + tableName + ": " +
                 ex.getMessage());

        }
    }

    private void checkRow(NoSQLHandle nosqlHandle,
                          String tableName,
                          int id,
                          int numFields,
                          int numNewFields) {

        final MapValue expRow = createRow(id, numFields, numNewFields);

        final MapValue key = createKey(id);
        final GetRequest getReq = new GetRequest()
            .setTableName(tableName)
            .setKey(key);

        try {
            GetResult getRet = nosqlHandle.get(getReq);
            assertEquals(expRow, getRet.getValue());
        } catch (Exception ex) {
            fail("Failed to get row: " + ex.getMessage());
        }
    }

    private void assertRowCountByIndex(NoSQLHandle nosqlHandle,
                                       String tableName,
                                       int idxIndex,
                                       int expCount) {
        final String fieldName = makeFieldName(idxIndex);
        final String query = "SELECT count(" + fieldName+ ") FROM " + tableName;
        long ret = execCountQuery(nosqlHandle, query);
        assertEquals(expCount, ret);
    }

    private long countRows(NoSQLHandle nosqlHandle, String tableName) {
        final String query = "SELECT count(*) FROM " + tableName;
        return execCountQuery(nosqlHandle, query);
    }

    private long execCountQuery(NoSQLHandle nosqlHandle, String query) {

        try {
            PrepareRequest prepReq = new PrepareRequest()
                .setStatement(query);
            PrepareResult prepRet = nosqlHandle.prepare(prepReq);

            QueryRequest queryReq = new QueryRequest()
                .setPreparedStatement(prepRet);
            List<MapValue> results = new ArrayList<MapValue>();
            do {
                QueryResult result = nosqlHandle.query(queryReq);
                if (!result.getResults().isEmpty()) {
                    results.addAll(result.getResults());
                }
            } while (!queryReq.isDone());
            assertEquals(1, results.size());
            MapValue value = results.get(0);
            return value.get("Column_1").asLong().getLong();
        } catch (Exception ex) {
            fail("Failed to execute [" + query + "]: " + ex.getMessage());
        }
        return -1;
    }

    private void assertNumTables(String tenantId, int exp) {
        assertEquals(exp, getNumTables(tenantId));
    }

    private int getNumTables(String tenantId) {
        final ListTablesRequest ltReq = new ListTablesRequest();
        try {
            NoSQLHandle nosqlHandle = getTenantHandle(tenantId);
            ListTablesResult ltRet = nosqlHandle.listTables(ltReq);
            String[] tables = ltRet.getTables();
            return tables.length;
        } catch (Exception ex) {
            fail("Failed to get index: " + ex.getMessage());
        }
        return -1;
    }

    private void assertNumIndexes(String tenantId, String tableName, int exp) {
        assertEquals(exp, getNumIndexes(tenantId, tableName));
    }

    private int getNumIndexes(String tenantId, String tableName) {
        final NoSQLHandle nosqlHandle = getTenantHandle(tenantId);
        final GetIndexesRequest giReq = new GetIndexesRequest()
            .setTableName(tableName);
        try {
            GetIndexesResult giRet = nosqlHandle.getIndexes(giReq);
            return giRet.getIndexes().length;
        } catch (Exception ex) {
            fail("Failed to get index: " + ex.getMessage());
        }
        return -1;
    }

    private TableResult execTableRequestNoWait(NoSQLHandle nosqlHandle,
                                               String statement,
                                               TableLimits limits) {
        TableRequest request = new TableRequest().setStatement(statement).
                                                  setTableLimits(limits).
                                                  setTimeout(waitMillis);
        return nosqlHandle.tableRequest(request);
    }

    void waitForDone(NoSQLHandle nosqlHandle, int waitMs, TableResult result) {
        if (result.getTableState() == State.ACTIVE ||
            result.getTableState() == State.DROPPED) {
            return;
        }
        result.waitForCompletion(nosqlHandle, waitMs, 1500);
    }

    /* Create Row value */
    private MapValue createRow(int id, int numFields) {
        return createRow(id, numFields, 0);
    }

    private MapValue createRow(int id, int numFields, int numNewFields) {
        MapValue row = createKey(id);
        for (int i = 0; i < numFields; i++) {
            String value = makeString((id % (63 - numFields)) + i + 1, i);
            row.put(makeFieldName(i), value);
        }
        for (int i = 0; i < numNewFields; i++) {
            row.put(makeNewFieldName(i), NullValue.getInstance());
        }
        return row;
    }

    /* Create primary key value */
    private MapValue createKey(int id) {
        return new MapValue().put("id", id);
    }

    /* Create tenantId */
    private static String makeTenantId(int index) {
        return "CDTTenant" + index;
    }

    /* Create table name */
    private static String makeTableName(String tenantId, int index) {
        return tenantId + "T" + index;
    }

    /* Create index name */
    private static String makeIndexName(int index, int ... fieldIndex) {
        final StringBuilder sb = new StringBuilder();
        sb.append("idx");
        sb.append(index);
        for (int f : fieldIndex) {
            sb.append("_f");
            sb.append(f);
        }
        return sb.toString();
    }

    private static String makeFieldName(int index) {
        return "f" + index;
    }

    private static String makeNewFieldName(int index) {
        return "nf" + index;
    }

    private static String makeString(int length, int from) {
        StringBuilder sb = new StringBuilder();
        for (int i = from; i < length + from; i++) {
            sb.append((char)('a' + (i % 26)));
        }
        return sb.toString();
    }

    private NoSQLHandle getTenantHandle(String tenantId) {
        NoSQLHandle nosqlHandle = handleCache.get(tenantId);
        if (nosqlHandle != null) {
            return nosqlHandle;
        }
        synchronized(handleCache) {
            nosqlHandle = handleCache.get(tenantId);
            if (nosqlHandle == null) {
                if (tenantId.equals(getTenantId())) {
                    nosqlHandle = handle;
                } else {
                    try {
                        nosqlHandle = configHandle(getProxyURL(), tenantId);
                    } catch (Exception ex) {
                        fail("Failed to get nosql handle for tenant: " +
                             tenantId);
                        return null;
                    }
                }
                handleCache.put(tenantId, nosqlHandle);
            }
            return nosqlHandle;
        }
    }

    private NoSQLHandle configHandle(URL url, String tenantId) {

        NoSQLHandleConfig hconfig = new NoSQLHandleConfig(url);
        hconfig.configureDefaultRetryHandler(5, 0);
        hconfig.setRequestTimeout(10_000);
        SecureTestUtil.setAuthProvider(hconfig, isSecure(), onprem, tenantId);
        return getHandle(hconfig);
    }

    /**
     * A class to generate the sequence of DDL objects.
     */
    private static class DDLGenerator {
        private final List<DDLInfo[]> ddlsList;
        private String tenantId;

        DDLGenerator(String defaultTenantId) {
            ddlsList = new ArrayList<DDLInfo[]>();
            tenantId = defaultTenantId;
        }

        DDLGenerator createTable(int numFields, int num, TableLimits limits) {
            if (num > 0) {
                String[] tableNames = getTableNames(tenantId, num);
                createTable(numFields, tableNames, limits);
            }
            return this;
        }

        DDLGenerator createTable(int numFields,
                                 String[] tableNames,
                                 TableLimits limits) {
            if (tableNames != null && tableNames.length > 0) {
                addDDLs(DDLType.CREATE_TABLE, tableNames,
                        makeCreateTableDDLs(tableNames, numFields),
                        limits);
            }
            return this;
        }

        DDLGenerator createIndex(String tableName, int num) {
            if (num > 0) {
                addDDLs(DDLType.CREATE_INDEX, tableName,
                        makeCreateIndexDDLs(tableName, num));
            }
            return this;
        }

        DDLGenerator addField(String tableName, int num) {
            if (num > 0) {
                addDDLs(DDLType.ALTER_TABLE, tableName,
                        makeAddFieldDDLs(tableName, num));
            }
            return this;
        }

        DDLGenerator dropField(String tableName, int num) {
            if (num > 0) {
                addDDLs(DDLType.ALTER_TABLE, tableName,
                        makeDropFieldDDLs(tableName, num));
            }
            return this;
        }

        DDLGenerator dropIndex(String tableName, int num) {
            if (num > 0) {
                addDDLs(DDLType.DROP_INDEX, tableName,
                        makeDropIndexDDLs(tableName, num));
            }
            return this;
        }

        DDLGenerator dropTable(int num) {
            if (num > 0) {
                String[] tableNames = getTableNames(tenantId, num);
                dropTable(tableNames);
            }
            return this;
        }

        DDLGenerator dropTable(String[] tableNames) {
            if (tableNames != null && tableNames.length > 0) {
                addDDLs(DDLType.DROP_TABLE, tableNames,
                        makeDropTableDDLs(tableNames),
                        null);
            }
            return this;
        }

        private void addDDLs(DDLType type, String tableName, String[] ddls) {
            DDLInfo[] ddlInfos = new DDLInfo[ddls.length];
            for (int i = 0; i < ddls.length; i++) {
                String ddl = ddls[i];
                String indexName = null;
                if (type == DDLType.CREATE_INDEX ||
                    type == DDLType.DROP_INDEX) {
                    indexName = makeIndexName(i, i);
                }
                if (tableName == null) {
                    if (type == DDLType.CREATE_TABLE ||
                        type == DDLType.DROP_TABLE) {
                        tableName = makeTableName(getTenantId(), i);
                    }
                }
                DDLInfo info = new DDLInfo(getTenantId(), tableName,
                                           indexName, type, ddl);
                ddlInfos[i] = info;
            }
            ddlsList.add(ddlInfos);
        }

        private void addDDLs(DDLType type,
                             String[] tableNames,
                             String[] ddls,
                             TableLimits limits) {
            DDLInfo[] ddlInfos = new DDLInfo[ddls.length];
            for (int i = 0; i < ddls.length; i++) {
                String tableName = tableNames[i];
                String ddl = ddls[i];
                DDLInfo info = new DDLInfo(getTenantId(), tableName, type,
                                           ddl, limits);
                ddlInfos[i] = info;
            }
            ddlsList.add(ddlInfos);
        }

        DDLGenerator setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        String getTenantId() {
            return tenantId;
        }

        /*
         * Merges the elements in multiple DDLInfo[] into a queue.
         */
        List<DDLInfo> build(boolean clear) {
            final List<DDLInfo> queue = new ArrayList<DDLInfo>();
            @SuppressWarnings("unchecked")
            Iterator<DDLInfo>[] ddlIters = new Iterator[ddlsList.size()];
            List<Integer> indexes = new ArrayList<Integer>(ddlIters.length);
            for (int i = 0; i < ddlIters.length; i++) {
                DDLInfo[] ddls = ddlsList.get(i);
                ddlIters[i] = Arrays.asList(ddls).iterator();
                indexes.add(i);
            }

            Iterator<Integer> indIter = indexes.iterator();
            while(indIter.hasNext()) {
                int ind = indIter.next();
                Iterator<DDLInfo> ddlIter = ddlIters[ind];

                if (ddlIter.hasNext()) {
                    queue.add(ddlIter.next());
                } else {
                    indIter.remove();
                }
                if (!indIter.hasNext()) {
                    if (indexes.isEmpty()) {
                        break;
                    }
                    indIter = indexes.iterator();
                }
            }
            if (clear) {
                clear();
            }
            return queue;
        }

        void clear() {
            ddlsList.clear();
        }

        private static String[] makeCreateTableDDLs(String[] tableNames,
                                                    int numFields) {
            final String[] ddls = new String[tableNames.length];
            int i = 0;
            for (String tableName : tableNames) {
                ddls[i++] = makeCreateTableDDL(tableName, numFields);
            }
            return ddls;
        }

        private static String makeCreateTableDDL(String tableName,
                                                 int numFields) {
            final StringBuilder sb = new StringBuilder("CREATE TABLE ");
            sb.append(tableName);
            sb.append("(");

            sb.append("id INTEGER, ");
            for (int i = 0; i < numFields; i++) {
                sb.append(makeFieldName(i));
                sb.append(" STRING, ");
            }
            sb.append("PRIMARY KEY(id))");
            return sb.toString();
        }

        private static String[] makeDropTableDDLs(String[] tableNames) {
            final String[] ddls = new String[tableNames.length];
            int i = 0;
            for (String table : tableNames) {
                ddls[i++] = makeDropTableDDL(table);
            }
            return ddls;
        }

        private static String makeDropTableDDL(String tableName) {
            final StringBuilder sb = new StringBuilder("DROP TABLE ");
            sb.append(tableName);
            return sb.toString();
        }

        private static String[] makeCreateIndexDDLs(String tableName, int num) {
            final String[] ddls = new String[num];
            for (int i = 0; i < ddls.length; i++) {
                final int fidx = i;
                ddls[i] = makeCreateIndexDDL(tableName, i, fidx);
            }
            return ddls;
        }

        private static String makeCreateIndexDDL(String tableName,
                                                 int idxIndex,
                                                 int... fieldIndex) {
            final StringBuilder sb = new StringBuilder("CREATE INDEX ");
            sb.append(makeIndexName(idxIndex, fieldIndex));
            sb.append(" on ");
            sb.append(tableName);
            sb.append("(");

            boolean firstField = true;
            for (int fidx : fieldIndex) {
                if (firstField) {
                    firstField = false;
                } else {
                    sb.append(", ");
                }
                sb.append(makeFieldName(fidx));
            }
            sb.append(")");
            return sb.toString();
        }

        private static String[] makeAddFieldDDLs(String tableName, int num) {
            final String[] ddls = new String[num];
            for (int i = 0; i < ddls.length; i++) {
                ddls[i] = makeAddDropFieldDDL(tableName, true, i);
            }
            return ddls;
        }

        private static String[] makeDropFieldDDLs(String tableName, int num) {
            final String[] ddls = new String[num];
            for (int i = 0; i < ddls.length; i++) {
                ddls[i] = makeAddDropFieldDDL(tableName, false, i);
            }
            return ddls;
        }

        private static String makeAddDropFieldDDL(String tableName,
                                                  boolean addField,
                                                  int newFieldIndex) {
            final StringBuilder sb = new StringBuilder("ALTER TABLE ");
            sb.append(tableName);
            sb.append("(");
            sb.append(addField ? "ADD " : "DROP ");
            sb.append(makeNewFieldName(newFieldIndex));
            if (addField) {
                sb.append(" STRING");
            }
            sb.append(")");
            return sb.toString();
        }

        private static String[] makeDropIndexDDLs(String tableName, int num) {
            final String[] ddls = new String[num];
            for (int i = 0; i < ddls.length; i++) {
                final int fidx = i;
                ddls[i] = makeDropIndexDDL(tableName, i, fidx);
            }
            return ddls;
        }

        private static String makeDropIndexDDL(String tableName,
                                               int idxIndex,
                                               int ... fieldIndex) {
            final StringBuilder sb = new StringBuilder("DROP INDEX ");
            sb.append(makeIndexName(idxIndex, fieldIndex));
            sb.append(" ON ");
            sb.append(tableName);
            return sb.toString();
        }
    }

    /* DDL types */
    static enum DDLType {
        CREATE_TABLE,
        CREATE_INDEX,
        DROP_TABLE,
        DROP_INDEX,
        ALTER_TABLE
    }

    /**
     * A thread to execute DDL statements that read from a queue.
     */
    private class DDLThread extends Thread {
        private final BlockingQueue<DDLInfo> queue;

        DDLThread(BlockingQueue<DDLInfo> queue,
                  String name) {
            this.queue = queue;
            setName(name);
        }

        @Override
        public void run() {
            DDLInfo ddlInfo;
            while((ddlInfo = queue.poll()) != null) {
                TableResult ret = ddlExecutor.execNoWait(ddlInfo);
                ddlExecutor.waitForDone(ddlInfo.getTenantId(), waitMillis, ret);
            }
        }
    }

    /**
     *  A class encapsulates ddl and the target tenantId.
     */
    static class DDLInfo {
        private final String tenantId;
        private final String tableName;
        private final String indexName;
        private final String ddl;
        private final TableLimits limits;
        private final DDLType type;

        DDLInfo(String tenantId,
                String tableName,
                DDLType type,
                String ddl) {
            this(tenantId, tableName, null, type, ddl);
        }

        DDLInfo(String tenantId,
                String tableName,
                DDLType type,
                String ddl,
                TableLimits limits) {
            this(tenantId, tableName, null, type, ddl, limits);
        }

        DDLInfo(String tenantId,
                String tableName,
                String indexName,
                DDLType type,
                String ddl) {
            this(tenantId, tableName, indexName, type, ddl, null);
        }

        DDLInfo(String tenantId,
                String tableName,
                String indexName,
                DDLType type,
                String ddl,
                TableLimits limits) {
            this.tenantId = tenantId;
            this.tableName = tableName;
            this.indexName = indexName;
            this.ddl = ddl;
            this.type = type;
            this.limits = limits;
        }

        String getTenantId() {
            return tenantId;
        }

        String getTableName() {
            return tableName;
        }

        String getIndexName() {
            return indexName;
        }

        String getDDL() {
            return ddl;
        }

        TableLimits getTableLimits() {
            return limits;
        }

        DDLType getType() {
            return type;
        }

        @Override
        public String toString() {
            return "tenantId=" + tenantId + "; tableName=" + tableName +
                    "; type=" + type + "; ddl=" + ddl;
        }
    }

    /**
     * Load N rows to the specified table.
     */
    private class LoadTask implements Callable<Integer> {

        private final NoSQLHandle nosqlHandle;
        private final String tableName;
        private final int nFields;
        private final int nRows;

        LoadTask(NoSQLHandle nosqlHandle,
                 String tableName,
                 int nFields,
                 int nRows) {
            this.nosqlHandle = nosqlHandle;
            this.tableName = tableName;
            this.nFields = nFields;
            this.nRows = nRows;
        }

        @Override
        public Integer call() throws Exception {
            loadRows(nosqlHandle, tableName, nFields, nRows);
            return (int)countRows(nosqlHandle, tableName);
        }
    }

    interface DDLExecutor<T> {
        T execNoWait(DDLInfo ddlInfo);
        void waitForDone(String tenantId, int waitMs, T result);
    }

    private class ClientExecutor implements DDLExecutor<TableResult> {
        @Override
        public TableResult execNoWait(DDLInfo ddl) {
            NoSQLHandle nosqlHandle = getTenantHandle(ddl.getTenantId());
            return execTableRequestNoWait(nosqlHandle, ddl.getDDL(),
                                          ddl.getTableLimits());
        }

        @Override
        public void waitForDone(String tenantId,
                                int waitMs,
                                TableResult result) {
            NoSQLHandle nosqlHandle = getTenantHandle(tenantId);
            ConcurrentDDLTest.this.waitForDone(nosqlHandle, waitMs, result);
        }
    }

    /* UncaughtExceptionHandler for DDLThead */
    private class TestExceptionHandler implements UncaughtExceptionHandler {

        private Class<?> expectedExceptionCls;
        private AtomicInteger numExpectedException;
        private Map<String, Throwable> unexpectedExceptions;

        public TestExceptionHandler(Class<?> expExCls) {
            expectedExceptionCls = expExCls;
            numExpectedException = new AtomicInteger();
            unexpectedExceptions =
                Collections.synchronizedMap(new HashMap<String, Throwable>());
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e.getClass() == expectedExceptionCls) {
                numExpectedException.incrementAndGet();
            } else {
                unexpectedExceptions.put(t.getName(), e);
            }
        }

        public int getNumExpectedException() {
            return numExpectedException.get();
        }

        public Map<String, Throwable> getUnexpectedException() {
            return unexpectedExceptions;
        }
    }
}
