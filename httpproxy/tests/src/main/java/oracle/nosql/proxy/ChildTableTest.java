/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import oracle.nosql.driver.IndexExistsException;
import oracle.nosql.driver.IndexLimitException;
import oracle.nosql.driver.IndexNotFoundException;
import oracle.nosql.driver.KeySizeLimitException;
import oracle.nosql.driver.RowSizeLimitException;
import oracle.nosql.driver.TableExistsException;
import oracle.nosql.driver.TableLimitException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableResult.State;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleRequest.OperationRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteMultipleResult.OperationResult;
import oracle.nosql.driver.values.MapValue;

/**
 * Test child table operations and data access operations.
 */
public class ChildTableTest extends ProxyTestBase {
    private final static TableLimits limits = new TableLimits(500, 500, 1);
    private final static int WAIT_MS = 10000;

    private final static String createTDdl =
        "create table t(id integer, name string, s string, primary key(id))";
    private final static String createTADdl =
        "create table t.a(ida integer, name string, s string, primary key(ida))";
    private final static String createIfNotExistsTADdl =
        "create table if not exists t.a(" +
        "ida integer, name string, s string, primary key(ida))";
    private final static String createTABDdl =
        "create table t.a.b(idb integer, name string, s string, primary key(idb))";
    private final static String createTGDdl =
        "create table t.g(idg integer, name string, s string, primary key(idg))";
    private final static String createXDdl =
        "create table x(id integer, name string, s string, primary key(id))";

    /**
     * Test child table related table operations:
     *   1. create table
     *   2. get table
     *   3. list tables
     *   4. create/drop index
     *   5. alter table
     *   6. drop table
     */
    @Test
    public void testBasicTableOps() {

        TableResult tr;

        /*
         * Create table
         */
        tr = tableOperation(handle, createTDdl, limits, WAIT_MS);
        if (!onprem) {
            assertNotNull(tr.getTableLimits());
        }
        checkTableInfo(tr, "t", limits);

        tr = tableOperation(handle, createTADdl, null, WAIT_MS);
        checkTableInfo(tr, "t.a", null);

        /* create table t.a again, expect to get TableExistsException */
        tableOperation(handle, createTADdl,
                       null /* tableLimits */,
                       null /* tableName */,
                       TableResult.State.ACTIVE,
                       TableExistsException.class);

        /* creating table with if not exists should succeed */
        tr = tableOperation(handle, createIfNotExistsTADdl, null, WAIT_MS);

        tr = tableOperation(handle, createTABDdl, null, WAIT_MS);
        checkTableInfo(tr, "t.a.b", null);

        tr = tableOperation(handle, createTGDdl, null, WAIT_MS);
        checkTableInfo(tr, "t.g", null);

        /*
         * Get table
         */
        tr = getTable("t.a", handle);
        checkTableInfo(tr, "t.a", null);

        tr = getTable("t.a.b", handle);
        checkTableInfo(tr, "t.a.b", null);

        /*
         * List tables
         */
        if (!onprem) {
            ListTablesResult lsr = listTables(handle);
            assertEquals(4, lsr.getTables().length);
            String[] tables = lsr.getTables();
            Arrays.sort(tables);
            assertTrue(Arrays.equals(new String[] {"t", "t.a", "t.a.b", "t.g"},
                                     tables));
        }

        /*
         * Create/Drop index
         */
        String ddl;

        ddl = "create index idx1 on t.a.b(s)";
        tr = tableOperation(handle, ddl, null, WAIT_MS);

        /* create index again, expect to get IndexExistsException */
        tableOperation(handle, ddl, null, null, TableResult.State.ACTIVE,
                       IndexExistsException.class);

        ddl = "create index if not exists idx1 on t.a.b(s)";
        tr = tableOperation(handle, ddl, null, WAIT_MS);

        ddl = "drop index idx1 on t.a.b";
        tr = tableOperation(handle, ddl, null, WAIT_MS);

        /* drop index again, expect to get IndexNotFoundException */
        tableOperation(handle, ddl, null, null, TableResult.State.ACTIVE,
                       IndexNotFoundException.class);

        ddl = "drop index if exists idx1 on t.a.b";
        tr = tableOperation(handle, ddl, null, WAIT_MS);

        /*
         * Alter table
         */
        ddl = "alter table t.a (add i integer)";
        tr = tableOperation(handle, ddl, null, WAIT_MS);

        ddl = "alter table t.g (drop s)";
        tr = tableOperation(handle, ddl, null, WAIT_MS);

        /*
         * Drop table
         */

        ddl = "drop table t.a.b";
        tableOperation(handle, ddl, null, WAIT_MS);

        ddl = "drop table if exists t.a.b";
        tableOperation(handle, ddl, null, WAIT_MS);
    }

    /**
     * Test that the child table will be counted against the tenancy's total
     * number of tables
     */
    @Test
    public void testLimitTables() {
        assumeTrue("Skipping testLimitTables if not minicloud or cloud test " +
                   "or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final int tableLimit = tenantLimits.getNumTables();
        if (tableLimit > NUM_TABLES) {
            /*
             * To prevent this test from running too long, skip the test if the
             * table number limit > ProxyTestBase.NUM_TABLES
             */
            return;
        }

        String ddl = "create table p(id integer, s string, primary key(id))";
        tableOperation(handle, ddl, new TableLimits(50, 50, 1), WAIT_MS);

        String fmt = "create table %s(%s integer, s string, primary key(%s))";
        for (int i = 0; i < tableLimit; i++) {
            String table = "p.c" + i;
            ddl = String.format(fmt, table, "ck", "ck");
            try {
                tableOperation(handle, ddl, null, WAIT_MS);
                if (i == tableLimit - 1) {
                    fail("create table should have failed, num create table: "
                          + i + ", limit: " + tableLimit);
                }
            } catch (TableLimitException tle) {
                if (i < tableLimit - 1) {
                    fail("create table should succeed: " + table);
                }
            }
        }

        /*
         * List tables
         */
        ListTablesResult lsr = listTables(handle);
        assertEquals(tableLimit, lsr.getTables().length);
    }

    /**
     * Test that the number of columns in child table is subjected to the
     * TableRequestLimits.columnsPerTable.
     *
     * The child table inherits the primary key of its parent, its parent
     * table's primary key fields should be counted.
     */
    @Test
    public void testLimitColumns() {
        assumeTrue("Skipping testLimitColumns if not minicloud or cloud test " +
                   "or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final int columnLimit = tenantLimits.getStandardTableLimits().
                                             getColumnsPerTable();

        String ddl = "create table p(" +
                     "  k1 integer, " +
                     "  k2 integer, " +
                     "  k3 integer, " +
                     "  s string, " +
                     "  primary key(k1, k2, k3))";
        tableOperation(handle, ddl, new TableLimits(50, 50, 1), WAIT_MS);

        /*
         * Create table p.c with N columns, N is the number of column per table.
         */
        StringBuilder sb;
        sb = new StringBuilder("create table p.c(c1 integer, primary key(c1)");
        for (int i = 4; i < columnLimit; i++) {
            sb.append(", s").append(i).append(" string");
        }
        sb.append(")");
        tableOperation(handle, sb.toString(), null, WAIT_MS);

        /*
         * Create table p.c.d with N + 1 columns, N is the number of column per
         * table.
         */
        sb = new StringBuilder("create table p.c.d(d1 integer, primary key(d1)");
        for (int i = 5; i < columnLimit + 1; i++) {
            sb.append(", s").append(i).append(" string");
        }
        sb.append(")");
        try {
            tableOperation(handle, sb.toString(), null, WAIT_MS);
            fail("create table should have failed as its column number " +
                 "exceeded the max number of column per table: " + columnLimit);
        } catch (IllegalArgumentException iae) {
            /* expected */
        }

        /*
         * Adding more field to p.c should fail as the columns number will
         * exceed the limit
         */
        ddl = "alter table p.c(add n1 integer)";
        try {
            tableOperation(handle, ddl, null, WAIT_MS);
            fail("adding column should have failed as its column number " +
                 "exceeded the max number of column per table: " + columnLimit);
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
    }

    @Test
    public void testLimitIndexes() {
        assumeTrue("Skipping testLimitIndexes if not minicloud or cloud test " +
                   "or tenantLimits is not provided",
                   cloudRunning && tenantLimits != null);

        final int indexLimit = tenantLimits.getStandardTableLimits().
                                            getIndexesPerTable();

        /* Create table t */
        tableOperation(handle, createTDdl, limits, WAIT_MS);

        /* Create table t.c and N indexes (N = indexLimit - 2) */
        StringBuilder sb = new StringBuilder("create table t.c(idc integer, ");
        for (int i = 0; i < indexLimit; i++) {
            sb.append("c").append(i).append(" integer, ");
        }
        sb.append("primary key(idc))");
        tableOperation(handle, sb.toString(), null, WAIT_MS);

        for (int i = 0; i < indexLimit; i++) {
            sb.setLength(0);
            sb.append("create index idx").append(i)
              .append(" on t.c(c").append(i).append(")");

            tableOperation(handle, sb.toString(), null, WAIT_MS);
        }

        String ddl = "create index idxC0C1 on t.c(c0, c1)";
        tableOperation(handle, ddl, null, null, TableResult.State.ACTIVE,
                       IndexLimitException.class);
    }

    @Test
    public void testLimitKeyValueSize() {

        assumeTrue("Skipping testLimitKeyValueSize if onprem test",
                   !onprem && tenantLimits != null);

        String ddl;

        ddl = "create table t(k1 string, s string, primary key(k1))";
        tableOperation(handle, ddl, limits, WAIT_MS);

        ddl = "create table t.c(k2 string, s string, primary key(k2))";
        tableOperation(handle, ddl, null, WAIT_MS);

        ddl = "create table t.c.g(k3 string, s string, primary key(k3))";
        tableOperation(handle, ddl, null, WAIT_MS);

        MapValue row = new MapValue();
        String s1 = "a";

        final int maxPKeySize = tenantLimits.getStandardTableLimits().
                                                getPrimaryKeySizeLimit();
        final int maxValSize = tenantLimits.getStandardTableLimits().
                                               getRowSizeLimit();
        final int maxIdxKeySize =
            (cloudRunning ? tenantLimits.getStandardTableLimits().
                                         getIndexKeySizeLimit() : 64);

        PutRequest req;
        String sval;

        /*
         * Primary key size exceed size limit
         */

        /* Put row to t.c with max key size, should succeed */
        sval = genString(maxPKeySize - 1);
        row.put("k1", s1)
           .put("k2", sval)
           .put("s", s1);
        req = new PutRequest().setTableName("t.c").setValue(row);
        handle.put(req);

        /*
         * Put row with max key size + 1, should have failed with
         * KeySizeLimitException
         */
        row.put("k1", sval + "a");
        try {
            handle.put(req);
            fail("Expect to catch KeySizeLimitException but not");
        } catch (KeySizeLimitException ex) {
            /* expected */
        }

        /* Put row to t.c.g with max key size, should succeed */
        sval = genString(maxPKeySize - 2);
        row.put("k1", s1)
           .put("k2", s1)
           .put("k3", sval)
           .put("s", s1);
        req = new PutRequest().setTableName("t.c.g").setValue(row);
        handle.put(req);

        /*
         * Put row to t.c.g with max key size + 1, should have failed with
         * KeySizeLimitException
         */
        row.put("k3", sval + "a");
        req = new PutRequest().setTableName("t.c.g").setValue(row);
        try {
            handle.put(req);
            fail("Expect to catch KeySizeLimitException but not");
        } catch (KeySizeLimitException ex) {
            /* expected */
        }

        /*
         * Row size exceed size limit
         */

        /*
         * Put row to t.c with length > max value size, should fail with
         * RowSizeLimitException
         */
        sval = genString(maxValSize);
        row.put("k1", s1)
           .put("k2", s1)
           .put("s", sval);
        req = new PutRequest().setTableName("t.c").setValue(row);
        try {
            handle.put(req);
            fail("Expect to catch RowSizeLimitException but not");
        } catch (RowSizeLimitException ex) {
            /* expected */
        }

        /*
         * Put row to t.c.g with length > max value size, should fail with
         * RowSizeLimitException
         */
        row.put("k1", s1)
           .put("k2", s1)
           .put("k3", s1)
           .put("s", sval);
        req = new PutRequest().setTableName("t.c.g").setValue(row);
        try {
            handle.put(req);
            fail("Expect to catch RowSizeLimitException but not");
        } catch (RowSizeLimitException ex) {
            /* expected */
        }

        /*
         * Index key size exceed size limit
         */
        String[] indexDdls = new String[] {
            "create index idxc1 on t.c(s)",
            "create index idxg1 on t.c.g(s)",
        };

        for (String idxDdl : indexDdls) {
            tableOperation(handle, idxDdl, null, WAIT_MS);
        }

        /* Put row to t.c with max index key size, should succeed */
        sval = genString(maxIdxKeySize);
        row.put("k1", s1)
           .put("k2", s1)
           .put("s", sval);
        req = new PutRequest().setTableName("t.c").setValue(row);
        handle.put(req);

        /*
         * Put row to t.c with max index key size + 1, should fail with
         * KeySizeLimitException
         */
        row.put("s", sval + "a");
        try {
            handle.put(req);
            fail("Expected to catch KeySizeLimitException");
        } catch (KeySizeLimitException ex) {
            /* expected */
        }

        /* Put row to t.c.g with max index key size, should succeed */
        row.put("k1", s1)
           .put("k2", s1)
           .put("k3", s1)
           .put("s", sval);
        req = new PutRequest().setTableName("t.c.g").setValue(row);
        handle.put(req);

        /*
         * Put row to t.c.g with max index key size + 1, should fail with
         * KeySizeLimitException
         */
        row.put("s", sval + "a");
        try {
            handle.put(req);
            fail("Expect to catch KeySizeLimitException but not");
        } catch (KeySizeLimitException ex) {
            /* expected */
        }
    }

    /**
     * Test invalid table operations on child table:
     *   1. Can't set limits on child table when create table
     *   2. Can't create table if its parent doesn't exist
     *   3. Don't allow to update limits of child table
     *   4. Can't drop the parent table if referenced by any child
     *   5. Don't allow to get table usage of child table
     */
    @Test
    public void testInvalidTableOps() {
        /* Cannot set limits on child table */
        tableOperation(handle, createTADdl, limits, null,
                       TableResult.State.ACTIVE,
                       (cloudRunning ? IllegalArgumentException.class :
                                       TableNotFoundException.class));

        /* The parent table of t.a does not exist */
        tableOperation(handle, createTADdl, null, null,
                       TableResult.State.ACTIVE,
                       (cloudRunning ? IllegalArgumentException.class :
                                       TableNotFoundException.class));

        tableOperation(handle, createTDdl, limits, WAIT_MS);
        tableOperation(handle, createTADdl, null, WAIT_MS);

        /* Don't allow to update limits of child table */
        if (!onprem) {
            tableOperation(handle, null, new TableLimits(600, 400, 1), "t.a",
                           TableResult.State.ACTIVE,
                           IllegalArgumentException.class);
        }

        /* Cannot drop the parent table still referenced by child table */
        String ddl = "drop table t";
        tableOperation(handle, ddl, null, null, TableResult.State.DROPPED,
                       IllegalArgumentException.class);

        if (cloudRunning) {
            /* Don't allow to get table usage of child table */
            TableUsageRequest tuReq = new TableUsageRequest().setTableName("t.a");
            try {
                handle.getTableUsage(tuReq);
                fail("GetTableUsage on child table should have failed");
            } catch (IllegalArgumentException iae) {
                /* expected */
            }
        }
    }

    /**
     * Test put/get/delete row of child table.
     */
    @Test
    public void testPutGetDelete() {
        int recordKB = 2;
        tableOperation(handle, createTDdl, limits, WAIT_MS);
        tableOperation(handle, createTADdl, null, WAIT_MS);
        tableOperation(handle, createTABDdl, null, WAIT_MS);

        MapValue row;
        MapValue key;

        String longStr = genString((recordKB - 1) * 1024);
        /* put a row to table t */
        row = makeTRow(1, longStr);
        doPutRow("t", row, recordKB);

        /* put a row to table t.a */
        row = makeTARow(1, 2, longStr);
        doPutRow("t.a", row, recordKB);
        key = new MapValue().put("id", 1).put("ida", 2);
        doGetRow("t.a", key, row, recordKB, null);
        doDeleteRow("t.a", key, recordKB);

        /* put a row to table t.a.b */
        row = makeTABRow(1, 2, 3, longStr);
        doPutRow("t.a.b", row, recordKB);
        key = new MapValue().put("id", 1).put("ida", 2).put("idb", 3);
        doGetRow("t.a.b", key, row, recordKB, null);
        doDeleteRow("t.a.b", key, recordKB);
    }

    /**
     * Test query against child table
     */
    @Test
    public void testQuery() {
        tableOperation(handle, createTDdl, limits, WAIT_MS);
        tableOperation(handle, createTADdl, null, WAIT_MS);
        tableOperation(handle, createTABDdl, null, WAIT_MS);

        final int keyCost = getMinRead();

        final int numT = 30;
        final int numAPerT = 2;
        final int numBPerA = 1;

        final int numA = numT * numAPerT;
        final int numB = numT * numAPerT * numBPerA;

        final int rkbT = 1;
        final int rkbA = 2;
        final int rkbB = 2;
        final int rkbMaxTA = Math.max(rkbT, rkbA);
        final int rkbMax = Math.max(Math.max(rkbT, rkbA), rkbB);

        final String s1 = genString(1);
        final String s1K = genString(1024);

        for (int i = 0; i < numT; i++) {
            doPutRow("t", makeTRow(i, s1), rkbT);
            for (int j = 0; j < numAPerT; j++) {
                doPutRow("t.a", makeTARow(i, j, s1K), rkbA);
                for (int k = 0; k < numBPerA; k++) {
                    doPutRow("t.a.b", makeTABRow(i, j, k, s1K), rkbB);
                }
            }
        }

        String query;
        int count;
        int cost;
        int limit = 10;

        query = "select id, ida from t.a";
        count = numA;
        cost = numA * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, keyCost);

        query = "select * from t.a";
        count = numA;
        cost = numA * rkbA;
        if (!dontDoubleChargeKey()) {
            cost += numA * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbA);

        query = "select id, ida, idb from t.a.b where idb = 100";
        count = 0;
        cost = numB * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, keyCost);

        query = "select * from t.a.b where s is null";
        count = 0;
        cost = numB * rkbB;
        if (!dontDoubleChargeKey()) {
            cost += numB * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbB);

        query = "select t.id, a.ida from nested tables(t descendants(t.a a))";
        count = numA;
        cost = (numT + numA) * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, keyCost);

        query = "select * from nested tables(t descendants(t.a a))";
        count = numA;
        cost = numT * rkbT + numA * rkbA;
        if (!dontDoubleChargeKey()) {
            cost += (numT + numA) * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbMaxTA);

        query = "select t.id, a.ida, b.idb " +
                "from nested tables(t descendants(t.a a, t.a.b b))";
        count = numB;
        cost = (numT + numA + numB) * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, keyCost);

        query = "select * from nested tables(t descendants(t.a a, t.a.b b))";
        count = numB;
        cost = numT * rkbT + numA * rkbA + numB * rkbB;
        if (!dontDoubleChargeKey()) {
            cost += (numT + numA + numB) * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbMax);

        query = "select a.ida, t.id from nested tables(t.a a ancestors(t))";
        count = numA;
        cost = (numA + numT) * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, 2 * keyCost);

        query = "select * from nested tables(t.a a ancestors(t))";
        count = numA;
        cost = numA * rkbA + numT * rkbT;
        if (!dontDoubleChargeKey()) {
            cost += (numA + numT) * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbT + rkbA);

        query = "select b.idb, a.ida, t.id " +
                "from nested tables(t.a.b b ancestors(t, t.a a))";
        count = numB;
        cost = (numB + numA + numT) * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, 3 * keyCost);

        query = "select * from nested tables(t.a.b b ancestors(t, t.a a))";
        count = numB;
        cost = numB * rkbB + numA * rkbA + numT * rkbT;
        if (!dontDoubleChargeKey()) {
            cost += (numB + numA + numT) * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbT + rkbA + rkbB);

        query = "select a.ida, t.id, b.idb " +
                "from nested tables(t.a a ancestors(t) descendants(t.a.b b))";
        count = numA;
        cost = (numA + numT + numB) * keyCost;
        runQueryWithLimit(query, cost/5, limit, count, cost, 2 * keyCost);

        query = "select * " +
                "from nested tables(t.a a ancestors(t) descendants(t.a.b b))";
        count = numB;
        cost = numA * rkbA + numT * rkbT + numB * rkbB;
        if (!dontDoubleChargeKey()) {
            cost += (numA + numT + numB) * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbT + rkbA);

        String ddl = "create index if not exists idxName on t.a(name)";
        tableOperation(handle, ddl, null, WAIT_MS);

        query = "select a.id, a.ida, a.name, b.idb " +
                "from nested tables(t.a a ancestors(t) descendants(t.a.b b)) " +
                "where a.name > 'n'";
        count = numB;
        /*
         * TODO: NOSQL-719
         * Enable the cost check in cloud test after fix it
         */
        if (useCloudService) {
            cost = 0;
        } else {
            cost = (numA + numT + numB) * keyCost;
            if (!dontDoubleChargeKey()) {
                cost += numA * keyCost;
            }
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, 2 * keyCost);

        query = "select * " +
                "from nested tables(t.a a ancestors(t) descendants(t.a.b b)) " +
                "where a.name > 'n'";
        count = numB;
        cost = numA * rkbA + numT * rkbT + numB * rkbB;
        if (!dontDoubleChargeKey()) {
            cost += (2 * numA + numT + numB) * keyCost;
        }
        runQueryWithLimit(query, cost/5, limit, count, cost, rkbT + rkbA);
    }

    @Test
    public void testWriteMultiple()
        throws Exception {

        tableOperation(handle, createTDdl, limits, WAIT_MS);
        tableOperation(handle, createTADdl, null, WAIT_MS);
        tableOperation(handle, createTABDdl, null, WAIT_MS);
        tableOperation(handle, createXDdl, limits, WAIT_MS);

        final int rkb = 2;
        final String s1K = genString((rkb - 1) * 1024 + 1);

        WriteMultipleRequest req = new WriteMultipleRequest();
        WriteMultipleResult res;
        MapValue key;
        MapValue row;
        PutRequest put;
        DeleteRequest del;

        /* Put operations */
        int id = 1;
        row = makeTRow(id, s1K);
        put = new PutRequest().setTableName("t").setValue(row);
        req.add(put, true);

        row = makeTARow(id, 1, s1K);
        put = new PutRequest().setTableName("t.a").setValue(row);
        req.add(put, true);

        row = makeTABRow(id, 1, 1, s1K);
        put = new PutRequest().setTableName("t.a.b").setValue(row);
        req.add(put, true);

        row = makeTABRow(id, 1, 2, s1K);
        put = new PutRequest().setTableName("t.a.b").setValue(row);
        req.add(put, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        if (!onprem) {
            assertEquals(rkb * req.getNumOperations(), res.getWriteKB());
        }

        Version verT11 = res.getResults().get(0).getVersion();
        Version verTA11 = res.getResults().get(1).getVersion();
        Version verTAB11 = res.getResults().get(2).getVersion();

        /*
         * Test ReturnInfo
         */
        req.clear();

        /* putIfAbsent with existing row */
        row = makeTRow(id, s1K);
        put = new PutRequest().setTableName("t")
                .setOption(Option.IfAbsent)
                .setReturnRow(true)
                .setValue(row);
        req.add(put, false);

        /* putIfVersion with unmatched version */
        row = makeTARow(id, 1, s1K);
        put = new PutRequest()
                .setTableName("t.a")
                .setMatchVersion(verT11)
                .setReturnRow(true)
                .setValue(row);
        req.add(put, false);

        /* deleteIfVersion with unmatched version*/
        key = makeTABKey(id, 1, 1);
        del = new DeleteRequest()
                .setTableName("t.a.b")
                .setMatchVersion(verTA11)
                .setReturnRow(true)
                .setKey(key);
        req.add(del, false);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        assertEquals(0, res.getWriteKB());

        List<OperationResult> results = res.getResults();
        OperationResult r = results.get(0);
        assertFalse(r.getSuccess());
        assertEquals(makeTRow(id, s1K), r.getExistingValue());
        assertTrue(Arrays.equals(verT11.getBytes(),
                                 r.getExistingVersion().getBytes()));
        assertTrue(r.getExistingModificationTime() > 0);

        r = results.get(1);
        assertFalse(r.getSuccess());
        assertEquals(makeTARow(id, 1, s1K), r.getExistingValue());
        assertTrue(Arrays.equals(verTA11.getBytes(),
                                 r.getExistingVersion().getBytes()));
        assertTrue(r.getExistingModificationTime() > 0);

        r = results.get(2);
        assertFalse(r.getSuccess());
        assertEquals(makeTABRow(id, 1, 1, s1K), r.getExistingValue());
        assertTrue(Arrays.equals(verTAB11.getBytes(),
                                 r.getExistingVersion().getBytes()));
        assertTrue(r.getExistingModificationTime() > 0);

        /*
         * abortIfUnsuccessful = true, check failedOperation only.
         */
        req.clear();
        row = makeTRow(id, s1K);
        put = new PutRequest().setTableName("t")
                .setReturnRow(true)
                .setValue(row);
        req.add(put, true);

        row = makeTABRow(id, 1, 1, s1K + "_u");
        put = new PutRequest().setTableName("t.a.b")
                .setReturnRow(true)
                .setOption(Option.IfAbsent)
                .setReturnRow(true)
                .setValue(row);
        req.add(put, true);

        res = handle.writeMultiple(req);
        assertFalse(res.getSuccess());
        assertEquals(1, res.getFailedOperationIndex());
        r = res.getFailedOperationResult();
        assertFalse(r.getSuccess());
        assertEquals(makeTABRow(id, 1, 1, s1K), r.getExistingValue());
        assertNotNull(Arrays.equals(verTAB11.getBytes(),
                                   r.getExistingVersion().getBytes()));
        assertTrue(r.getExistingModificationTime() > 0);

        /*
         * Delete operations
         */
        req.clear();
        key = makeTKey(id);
        del = new DeleteRequest().setTableName("t").setKey(key);
        req.add(del, true);

        key = makeTAKey(id, 1);
        del = new DeleteRequest().setTableName("t.a").setKey(key);
        req.add(del, true);

        key = makeTABKey(id, 1, 1);
        del = new DeleteRequest()
                    .setTableName("t.a.b")
                    .setMatchVersion(verTAB11)
                    .setKey(key);
        req.add(del, true);

        key = makeTABKey(id, 1, 2);
        del = new DeleteRequest().setTableName("t.a.b").setKey(key);
        req.add(del, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        if (!onprem) {
            assertEquals(rkb * req.getNumOperations(), res.getWriteKB());
        }

        /*
         * Test GeneratedValue for Identity columns
         */
        req.clear();

        String ddl;
        ddl = "alter table t(add seq integer generated always as identity)";
        tableOperation(handle, ddl, null, WAIT_MS);
        ddl = "alter table t.a.b(add seq long generated always as identity" +
                                "(start with 100 increment by -2))";
        tableOperation(handle, ddl, null, WAIT_MS);

        id++;
        row = makeTRow(id, s1K);
        put = new PutRequest().setTableName("T").setValue(row);
        req.add(put, true);

        row = makeTARow(id, 1, s1K);
        put = new PutRequest().setTableName("T.A").setValue(row);
        req.add(put, true);

        int numRows = 3;
        for (int i = 0; i < numRows; i++) {
            row = makeTABRow(id, 1, i, s1K);
            put = new PutRequest().setTableName("T.A.B").setValue(row);
            req.add(put, true);
        }

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        if (!onprem) {
            assertEquals(rkb * req.getNumOperations(), res.getWriteKB());
        }

        List<OperationRequest> ops = req.getOperations();
        String tname;
        int seqT = 1;
        int seqTAB = 100;
        int seqStep = -2;
        for (int i = 0; i < res.getResults().size(); i++) {
            r = res.getResults().get(i);
            tname = ops.get(i).getRequest().getTableName();
            if (tname.equalsIgnoreCase("t.a")) {
                assertNull(r.getGeneratedValue());
            } else {
                /*
                 * TODO: NOSQL-720
                 * enable below check in cloud test after fix it
                 */
                if (!useCloudService) {
                    assertNotNull(r.getGeneratedValue());
                    if (tname.equalsIgnoreCase("t")) {
                        assertEquals(seqT, r.getGeneratedValue().getInt());
                    } else {
                        /* t.a.b */
                        assertEquals(seqTAB, r.getGeneratedValue().getInt());
                        seqTAB += seqStep;
                    }
                }
            }
        }

        /* Test puts to single table */
        req.clear();

        id++;
        row = makeTARow(id, 0, s1K);
        put = new PutRequest().setTableName("t.a").setValue(row);
        req.add(put, true);

        row = makeTARow(id, 1, s1K);
        put = new PutRequest().setTableName("T.a").setValue(row);
        req.add(put, true);

        row = makeTARow(id, 2, s1K);
        put = new PutRequest()
                .setTableName("T.A")
                .setOption(Option.IfAbsent)
                .setValue(row);
        req.add(put, true);

        key = makeTAKey(id, 3);
        del = new DeleteRequest().setTableName("t.A").setKey(key);
        req.add(del, false);

        key = makeTAKey(id, 4);
        del = new DeleteRequest().setTableName("t.a").setKey(key);
        req.add(del, false);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        int i = 0;
        for (OperationResult or : res.getResults()) {
            if (i++ < 3) {
                assertTrue(or.getSuccess());
            } else {
                assertFalse(or.getSuccess());
            }
        }

        /*
         * Negative cases
         */

        /*
         * Table not found: t.unknown.
         *
         * Sub requests:
         *  put -> t.unknown
         *  put -> t
         */
        req.clear();

        row = makeTRow(1, s1K);
        put = new PutRequest().setTableName("t.unknown").setValue(row);
        req.add(put, true);

        put = new PutRequest().setTableName("t").setValue(row);
        req.add(put, true);
        try {
            handle.writeMultiple(req);
            fail("Operation should have failed with TableNotFoundException");
        } catch (TableNotFoundException e) {
            /* expected */
            checkErrorMessage(e);
        }

        /*
         * Table not found: t.unknown.
         *
         * Sub requests:
         *  put -> t
         *  put -> t.unknown
         */
        req.clear();

        row = makeTRow(1, s1K);
        put = new PutRequest().setTableName("t").setValue(row);
        req.add(put, true);

        put = new PutRequest().setTableName("t.unknown").setValue(row);
        req.add(put, true);

        try {
            handle.writeMultiple(req);
            fail("Operation should have failed with TableNotFoundException");
        } catch (TableNotFoundException e) {
            /* expected */
            checkErrorMessage(e);
        }

        /*
         * IllegalArgumentException: Tables not related: t x
         *
         * Sub requests:
         *  put -> t
         *  put -> x
         */
        req.clear();

        try {
            row = makeTRow(1, s1K);
            put = new PutRequest().setTableName("t").setValue(row);
            req.add(put, true);

            put = new PutRequest().setTableName("x").setValue(row);
            req.add(put, true);
            handle.writeMultiple(req);
            fail("Operation should have failed with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected */
            checkErrorMessage(e);
        }


        /*
         * IllegalArgumentException: Shard key does not match
         *
         * Sub requests:
         *  put {id=1,..} -> t.a
         *  put {id=1,..} -> t.a.b
         *  put {id=2,..} -> T.A
         *  put {id=2,..} -> T.A.B
         */
        req.clear();

        row = makeTARow(1, 1, s1K);
        put = new PutRequest().setTableName("t.a").setValue(row);
        req.add(put, true);

        row = makeTABRow(1, 1, 1, s1K);
        put = new PutRequest().setTableName("t.a.b").setValue(row);
        req.add(put, true);

        key = makeTAKey(2, 1);
        del = new DeleteRequest().setTableName("T.A").setKey(key);
        req.add(del, true);

        key = makeTABKey(2, 1, 1);
        del = new DeleteRequest().setTableName("T.A.B").setKey(key);
        req.add(del, true);

        try {
            handle.writeMultiple(req);
            fail("Operation should have failed with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected */
            checkErrorMessage(e);
        }

        /*
         * IllegalArgumentException: Missing primary key field: idb
         *
         * Sub requests:
         *  put {id=1,ida=1} -> t.a
         *  put {id=1,ida=1} -> t.a.b
         */
        req.clear();

        row = makeTARow(1, 1, s1K);
        put = new PutRequest().setTableName("t.a").setValue(row);
        req.add(put, true);

        put = new PutRequest().setTableName("t.a.b").setValue(row);
        req.add(put, true);

        try {
            handle.writeMultiple(req);
            fail("Operation should have failed with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            /* expected */
            checkErrorMessage(e);
        }
    }

    /* Test using table ocid in put/delete operations */
    @Test
    public void testWriteMultipleWithOcid() throws Exception {
        assumeTrue("Skipping testWriteMulitpleWithOcid if not minicloud test",
                   cloudRunning);

        tableOperation(handle, createTDdl, limits, WAIT_MS);
        tableOperation(handle, createTADdl, null, WAIT_MS);
        tableOperation(handle, createTABDdl, null, WAIT_MS);

        String ddl = "create table t1(id integer, s string, primary key(id))";
        tableOperation(handle, ddl, limits, WAIT_MS);

        int rkb = 1;
        final String s1 = genString(1);

        WriteMultipleRequest req = new WriteMultipleRequest();
        WriteMultipleResult res;
        MapValue key;
        MapValue row;
        PutRequest put;
        DeleteRequest del;

        String ocidT = getTable("t", handle).getTableId();
        String ocidTA = getTable("t.a", handle).getTableId();
        String ocidTAB = getTable("t.a.b", handle).getTableId();
        String ocidT1 = getTable("t1", handle).getTableId();

        /* put ops */
        int id = 1;
        row = makeTRow(id, s1);
        put = new PutRequest().setTableName(ocidT).setValue(row);
        req.add(put, true);

        row = makeTARow(id, 1, s1);
        put = new PutRequest().setTableName(ocidTA).setValue(row);
        req.add(put, true);

        row = makeTARow(id, 2, s1);
        put = new PutRequest().setTableName(ocidTA).setValue(row);
        req.add(put, true);

        row = makeTABRow(id, 1, 1, s1);
        put = new PutRequest().setTableName(ocidTAB).setValue(row);
        req.add(put, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        if (!onprem) {
            assertEquals(rkb * req.getNumOperations(), res.getWriteKB());
        }

        /* delete ops */
        req.clear();

        key = makeTKey(id);
        del = new DeleteRequest().setTableName(ocidT).setKey(key);
        req.add(del, true);

        key = makeTAKey(id, 1);
        del = new DeleteRequest().setTableName(ocidTA).setKey(key);
        req.add(del, true);

        key = makeTAKey(id, 2);
        del = new DeleteRequest().setTableName(ocidTA).setKey(key);
        req.add(del, true);

        key = makeTABKey(id, 1, 1);
        del = new DeleteRequest().setTableName(ocidTAB).setKey(key);
        req.add(del, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());
        if (!onprem) {
            assertEquals(rkb * req.getNumOperations(), res.getWriteKB());
        }

        /*
         * All operations should be on same table or tables belongs to same
         * parents
         */
        req.clear();

        row = makeTABRow(id, 1, 1, s1);
        put = new PutRequest().setTableName(ocidTAB).setValue(row);
        req.add(put, true);

        row = new MapValue().put("id", 1).put("s", s1);
        put = new PutRequest().setTableName(ocidT1).setValue(row);
        req.add(put, true);

        try {
            res = handle.writeMultiple(req);
            fail("Operation should have failed with IAE");
        } catch (IllegalArgumentException ex) {
            checkErrorMessage(ex);
        }
    }

    @Test
    public void testWriteMultipleTTL() {

        final TimeToLive tTTL = TimeToLive.ofDays(1);
        tableOperation(handle, addUsingTTL(createTDdl, tTTL), limits, WAIT_MS);

        final TimeToLive aTTL = TimeToLive.ofDays(3);
        tableOperation(handle, addUsingTTL(createTADdl, aTTL), null, WAIT_MS);

        final TimeToLive bTTL = TimeToLive.ofDays(5);
        tableOperation(handle, addUsingTTL(createTABDdl, bTTL), null, WAIT_MS);

        final TimeToLive userTTL = TimeToLive.ofDays(10);

        final String s1 = genString(1);
        final int rowKB = 1;
        WriteMultipleRequest req = new WriteMultipleRequest();
        WriteMultipleResult res;
        PutRequest put;

        /* Use table default TTL */
        int id = 1;
        MapValue trow = makeTRow(id, s1);
        put = new PutRequest()
                .setTableName("t")
                .setValue(trow);
        req.add(put, true);

        MapValue arow = makeTARow(id, 1, s1);
        put = new PutRequest()
                .setTableName("t.a")
                .setValue(arow);
        req.add(put, true);

        MapValue brow = makeTABRow(id, 1, 1, s1);
        put = new PutRequest()
                .setTableName("t.a.b")
                .setValue(brow);
        req.add(put, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());

        doGetRow("t", makeTKey(id), trow, rowKB, tTTL);
        doGetRow("t.a", makeTAKey(id, 1), arow, rowKB, aTTL);
        doGetRow("t.a.b", makeTABKey(id, 1, 1), brow, rowKB, bTTL);

        /* Update to user specified TTL */
        req.clear();

        put = new PutRequest()
                .setTableName("t")
                .setValue(trow)
                .setTTL(userTTL);
        req.add(put, true);

        put = new PutRequest()
                .setTableName("t.a")
                .setValue(arow)
                .setTTL(userTTL);
        req.add(put, true);

        put = new PutRequest()
                .setTableName("t.a.b")
                .setValue(brow)
                .setTTL(userTTL);
        req.add(put, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());

        doGetRow("t", makeTKey(id), trow, rowKB, userTTL);
        doGetRow("t.a", makeTAKey(id, 1), arow, rowKB, userTTL);
        doGetRow("t.a.b", makeTABKey(id, 1, 1), brow, rowKB, userTTL);

        /* Update back to default TTL */
        req.clear();

        put = new PutRequest()
                .setTableName("t")
                .setValue(trow)
                .setUseTableDefaultTTL(true);
        req.add(put, true);

        put = new PutRequest()
                .setTableName("t.a")
                .setValue(arow)
                .setUseTableDefaultTTL(true);
        req.add(put, true);

        put = new PutRequest()
                .setTableName("t.a.b")
                .setValue(brow)
                .setUseTableDefaultTTL(true);
        req.add(put, true);

        res = handle.writeMultiple(req);
        assertTrue(res.getSuccess());

        doGetRow("t", makeTKey(id), trow, rowKB, tTTL);
        doGetRow("t.a", makeTAKey(id, 1), arow, rowKB, aTTL);
        doGetRow("t.a.b", makeTABKey(id, 1, 1), brow, rowKB, bTTL);
    }

    private void runQueryWithLimit(String query, int maxReadKB, int limit,
                                   int expCount, int expReadKB, int recordKB) {

        runQuery(query, 0, 0, expCount, expReadKB, recordKB);

        if (maxReadKB > 0) {
            runQuery(query, maxReadKB, 0, expCount, expReadKB, recordKB);
        }

        if (limit > 0) {
            runQuery(query, 0, limit, expCount, expReadKB, recordKB);
        }

        if (checkKVVersion(21, 2, 18)) {
            /* Query should always make progress with small limit */
            for (int kb = 1; kb <= 5; kb++) {
                runQuery(query, kb, 0, expCount, expReadKB, recordKB);
            }
        }
    }

    private void runQuery(String statement,
                          int maxReadKB,
                          int limit,
                          int expCount,
                          int expCost,
                          int recordKB) {

        final boolean dispResult = false;

        QueryRequest req = new QueryRequest();
        PrepareRequest prepReq = new PrepareRequest()
                .setStatement(statement);
        PrepareResult prepRet = handle.prepare(prepReq);
        req.setPreparedStatement(prepRet);

        if (maxReadKB > 0) {
            req.setMaxReadKB(maxReadKB);
        }
        if (limit > 0) {
            req.setLimit(limit);
        }

        QueryResult ret;
        int cnt = 0;
        int batches = 0;
        int cost = 0;
        int batchSize = 0;
        do {
            ret = handle.query(req);
            batches++;
            batchSize = ret.getResults().size();

            if (maxReadKB > 0) {
                if (checkKVVersion(21, 2, 18)) {
                    /*
                     * Query should suspend after read the table row or key
                     * (for key only query) if current read cost exceeded size
                     * limit, so at most the readKB over the size limit.
                     */
                    assertTrue("The read cost should be at most " + recordKB +
                               " kb beyond the maximum readKB " + maxReadKB +
                               ", but actual " + ret.getReadKB(),
                               ret.getReadKB() <= maxReadKB + recordKB);
                } else {
                    assertTrue("The read cost should be at most 1" +
                               " kb beyond the maximum readKB " + maxReadKB +
                               ", but actual " + ret.getReadKB(),
                                ret.getReadKB() <= maxReadKB + 1);
                }
            }

            if (limit > 0) {
                assertTrue("The record count should not exceed the limit of " +
                           limit + ": " + batchSize, batchSize <= limit);
            }

            cost += ret.getReadKB();
            cnt += batchSize;

            for (MapValue mv : ret.getResults()) {
                if (dispResult) {
                    String json = mv.toJson();
                    if (json.length() > 50) {
                        json = json.substring(0, 50) + "..." +
                               (json.length() - 50) + " bytes ...";
                    }
                    System.out.println(json);
                }
            }
        } while(!req.isDone());

        if (expCount > 0) {
            assertEquals("'" + statement + "'\nshould return " + expCount +
                         " rows but actual got " + cnt + " rows",
                         expCount, cnt);
        }

        if (maxReadKB == 0 && limit == 0 && expCost < 2 * 1024 * 1024) {
            assertEquals("'" + statement + "' + " +
                         "should be done in single batch but actual " +
                         batches + " batches", 1, batches);
        }

        if (checkKVVersion(22, 1, 1) == false) {
            return;
        }

        if (!onprem) {
            assertTrue(cost > 0);

            if (expCost > 0) {
                if (batches == 1) {
                    assertEquals("'" + statement + "'\nexpect read cost " +
                                 expCost + "kb, but actual " + cost + " kb",
                                 expCost, cost);
                }
            }
        }
    }

    private void doPutRow(String tableName, MapValue row, int recordKB) {
        PutRequest req = new PutRequest()
                            .setTableName(tableName)
                            .setValue(row);
        PutResult ret = handle.put(req);
        assertNotNull(ret.getVersion());
        assertCost(ret, 0, recordKB);
    }

    private void doGetRow(String tableName,
                          MapValue key,
                          MapValue expRow,
                          int recordKB,
                          TimeToLive ttl) {
        GetRequest req = new GetRequest()
                            .setTableName(tableName)
                            .setKey(key);
        GetResult ret = handle.get(req);
        assertEquals(expRow, ret.getValue());
        assertCost(ret, recordKB, 0);
        if (ttl != null) {
            assertTimeToLive(ttl, ret.getExpirationTime());
        }
    }

    private void doDeleteRow(String tableName, MapValue key, int readKB) {
        DeleteRequest req = new DeleteRequest()
                            .setTableName(tableName)
                            .setKey(key);
        DeleteResult ret = handle.delete(req);
        assertTrue(ret.getSuccess());
        assertCost(ret, getMinRead() * 2 /* key read in absolute consistency */,
                   readKB);
    }

    @Override
    void dropAllTables() {
        dropAllTables(handle, true);
    }

    private MapValue makeTRow(int id, String longStr) {
        MapValue row = makeTKey(id);
        row.put("name", "n" + id)
           .put("s", longStr);
        return row;
    }

    private MapValue makeTKey(int id) {
        MapValue row = new MapValue();
        row.put("id", id);
        return row;
    }

    private MapValue makeTARow(int id, int ida, String longStr) {
        MapValue row = makeTAKey(id, ida);
        row.put("name", "n" + id + "_" + ida)
           .put("s", longStr);
        return row;
    }

    private MapValue makeTAKey(int id, int ida) {
        MapValue row = new MapValue();
        row.put("id", id)
           .put("ida", ida);
        return row;
    }

    private MapValue makeTABRow(int id, int ida, int idb, String longStr) {
        MapValue row = makeTABKey(id, ida, idb);
        row.put("name", "n" + id + "_" + ida + "_" + idb)
           .put("s", longStr);
        return row;
    }

    private MapValue makeTABKey(int id, int ida, int idb) {
        MapValue row = new MapValue();
        row.put("id", id)
           .put("ida", ida)
           .put("idb", idb);
        return row;
    }

    private void checkTableInfo(TableResult tr,
                                String tableName,
                                TableLimits limits) {
        assertEquals(tableName, tr.getTableName());
        assertEquals(State.ACTIVE, tr.getTableState());
        if (onprem) {
            return;
        }
        if (limits != null) {
            TableLimits tl = tr.getTableLimits();
            assertEquals(limits.getReadUnits(), tl.getReadUnits());
            assertEquals(limits.getWriteUnits(), tl.getWriteUnits());
            assertEquals(limits.getStorageGB(), tl.getStorageGB());
        } else {
            assertNull(tr.getTableLimits());
        }
    }

    private static String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    private static String addUsingTTL(final String ddl, TimeToLive ttl) {
        String newDdl = ddl;
        if (ttl != null) {
            newDdl += " using ttl " + ttl.getValue() + " " + ttl.getUnit();
        }
        return newDdl;
    }

    private void assertTimeToLive(TimeToLive ttl, long actual) {
        final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000;
        long expected = ttl.toExpirationTime(System.currentTimeMillis());
        assertTrue("Actual TTL duration " + actual + "ms differs by " +
                   "more than a day from expected duration of " + expected +"ms",
                   Math.abs(actual - expected) < DAY_IN_MILLIS);
    }
}
