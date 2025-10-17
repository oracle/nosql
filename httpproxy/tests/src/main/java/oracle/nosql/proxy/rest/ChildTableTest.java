/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */
package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.model.ChangeTableCompartmentDetails;
import com.oracle.bmc.nosql.model.Column;
import com.oracle.bmc.nosql.model.CreateTableDetails;
import com.oracle.bmc.nosql.model.Schema;
import com.oracle.bmc.nosql.model.Table;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.TableSummary;
import com.oracle.bmc.nosql.model.UpdateTableDetails;
import com.oracle.bmc.nosql.requests.ChangeTableCompartmentRequest;
import com.oracle.bmc.nosql.requests.CreateIndexRequest;
import com.oracle.bmc.nosql.requests.CreateTableRequest;
import com.oracle.bmc.nosql.requests.DeleteRowRequest;
import com.oracle.bmc.nosql.requests.DeleteTableRequest;
import com.oracle.bmc.nosql.requests.GetIndexRequest;
import com.oracle.bmc.nosql.requests.GetRowRequest;
import com.oracle.bmc.nosql.requests.ListTableUsageRequest;
import com.oracle.bmc.nosql.requests.ListTablesRequest;
import com.oracle.bmc.nosql.requests.ListTablesRequest.SortBy;
import com.oracle.bmc.nosql.requests.ListTablesRequest.SortOrder;
import com.oracle.bmc.nosql.requests.UpdateTableRequest;
import com.oracle.bmc.nosql.responses.DeleteRowResponse;
import com.oracle.bmc.nosql.responses.GetIndexResponse;
import com.oracle.bmc.nosql.responses.GetRowResponse;
import com.oracle.bmc.nosql.responses.GetTableResponse;
import com.oracle.bmc.nosql.responses.ListTablesResponse;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/* Child tables test */
public class ChildTableTest extends RestAPITestBase {
    @Rule
    public final TestName test = new TestName();

    private final static String createTDdl =
        "create table t(id integer, s string, primary key(id))";
    private final static String createTADdl =
        "create table t.a(ida integer, s string, primary key(ida))";
    private final static String createIfNotExistsTADdl =
        "create table if not exists t.a(ida integer, s string, primary key(ida))";
    private final static String createTABDdl =
        "create table t.a.b(idb integer, s string, primary key(idb))";
    private final static String createTGDdl =
        "create table t.g(idg integer, s string, primary key(idg))";

    private final String newCompartmentId = getCompartmentIdMoveTo();

    @Override
    public void tearDown() throws Exception {
        if (test.getMethodName().equals("testMoveCompartment")) {
            /* Cleanup the tables in newCompartmentId */
            removeAllTables(newCompartmentId);
        }
        super.tearDown();
    }

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
        /*
         * Create table
         */
        createTable("t", createTDdl);
        createTable("t.a", createTADdl, null /* limits */);

        /* create table t.a again, expect to get TableExistsException */
        createChildTableFail("t.a", createTADdl, "TableAlreadyExists");

        /* create table with if not exists should succeed */
        createTable("t.a", createIfNotExistsTADdl, null /* limits */);

        /*
         * create table with non-null limits, expect to get InvalidParameter
         * error
         */
        CreateTableRequest ctReq =
            buildCreateTableRequest(getCompartmentId(), "t.a.b",
                                    createTABDdl, defaultLimits);
        executeDdlFail(ctReq, "InvalidParameter");

        createTable("t.a.b", createTABDdl, null /* limits */);
        createTable("t.g", createTGDdl, null /* limits */);

        /*
         * Get table
         */
        GetTableResponse gtRet;
        Map<String, String> columns = new HashMap<>();
        gtRet = getTable("t.a");
        columns.clear();
        columns.put("id", "INTEGER");
        columns.put("ida", "INTEGER");
        columns.put("s", "STRING");
        checkTableInfo(gtRet.getTable(),
                      "t.a",
                      columns,
                      new String[] {"id", "ida"},
                      new String[] {"id"},
                      null);

        gtRet = getTable("t.a.b");
        columns.clear();
        columns.put("id", "INTEGER");
        columns.put("ida", "INTEGER");
        columns.put("idb", "INTEGER");
        columns.put("s", "STRING");
        checkTableInfo(gtRet.getTable(),
                      "t.a.b",
                      columns,
                      new String[] {"id", "ida", "idb"},
                      new String[] {"id"},
                      null);

        /*
         * List tables
         */
        ListTablesRequest ltReq;
        ListTablesResponse ltRes;
        ltReq = ListTablesRequest.builder()
                    .compartmentId(getCompartmentId())
                    .sortBy(SortBy.Name)
                    .sortOrder(SortOrder.Asc)
                    .build();
        ltRes = client.listTables(ltReq);
        String[] expTables = new String[] {"t", "t.a", "t.a.b", "t.g"};
        if (cloudRunning) {
            int i = 0;
            for (TableSummary ts : ltRes.getTableCollection().getItems()) {
                assertEquals(expTables[i++], ts.getName());
            }
        } else {
            assertEquals(expTables.length,
                         ltRes.getTableCollection().getItems().size());
        }

        /*
         * Create/Drop index
         */
        createIndex("t.a.b", "idx1", new String[]{"s"});
        dropIndex("t.a.b", "idx1", true);

        /*
         * Alter table
         */
        String ddl = "alter table t.a (add i integer)";
        alterTable("t.a", ddl);
        gtRet = getTable("t.a");
        columns.clear();
        columns.put("id", "INTEGER");
        columns.put("ida", "INTEGER");
        columns.put("s", "STRING");
        columns.put("i", "INTEGER");
        checkTableInfo(gtRet.getTable(),
                      "t.a",
                      columns,
                      new String[] {"id", "ida"},
                      new String[] {"id"},
                      null);

        ddl = "alter table t.g (drop s)";
        alterTable("t.g", ddl);
        gtRet = getTable("t.g");
        columns.clear();
        columns.put("id", "INTEGER");
        columns.put("idg", "INTEGER");
        checkTableInfo(gtRet.getTable(),
                      "t.g",
                      columns,
                      new String[] {"id", "idg"},
                      new String[] {"id"},
                      null);
        /*
         * Drop table
         */
        dropTable("t.a.b");
        dropTable("t.a.b", true /* ifExists */, true /* wait */);
    }

    @Test
    public void testOpsWithOcid() {
        assumeTrue("Skipping testOpsWithOcid if not minicloud or cloud test",
                   cloudRunning);

        /*
         * Create table
         */
        createTable("t", createTDdl, defaultLimits);
        createTable("t.a", createTADdl, null /* limits */);

        String tOcid = getTableId("t");
        String taOcid = getTableId("t.a");

        /* create/drop index */
        createIndex(tOcid, "idxt1", new String[]{"s"});

        /* create index idxa1 on t.a(s) */
        createIndex(taOcid, "idxa1", new String[]{"s"});

        /* create index if not exists idxa1 on t.a(s) */
        createIndex(taOcid, "idxa1", new String[]{"s"}, true /* ifNotExists */,
                    true /* wait */);

        /* get index */
        GetIndexRequest giReq = GetIndexRequest.builder()
                .tableNameOrId(taOcid)
                .indexName("idxa1")
                .build();
        GetIndexResponse giRes = client.getIndex(giReq);
        assertEquals("idxa1", giRes.getIndex().getName());

        /* IllegalArgument: Invalid index field 'invalid' */
        CreateIndexRequest ciReq =
            buildCreateIndexRequest(taOcid,
                                    "idxa2",
                                    new String[] {"s", "invalid"},
                                    false /* ifNotExists */);
        executeDdlFail(ciReq, "IllegalArgument");
        /*
         * IllegalArgument: Index is a duplicate of an existing index with
         * another name
         */
        ciReq = buildCreateIndexRequest(taOcid,
                                        "idxa2",
                                        new String[] {"s"},
                                        false /* ifNotExists */);
        executeDdlFail(ciReq, "IllegalArgument");

        /* drop index idxa1 on t.a */
        dropIndex(taOcid, "idxa1", true /* wait */);

        /* drop index if exists idxa1 on t.a */
        dropIndex(taOcid, "idxa1", true /* ifExists */, true /* wait */);

        /* Alter table */
        GetTableResponse gtRes = getTable(taOcid);

        String ddl = "alter table t.a(add n1 integer)";
        alterTable(taOcid, ddl, true /* wait */);

        gtRes = getTable(taOcid);
        assertTrue(gtRes.getTable()
                    .getDdlStatement()
                    .toLowerCase()
                    .contains("n1 integer"));

        /* Update table limits */
        UpdateTableRequest utReq = buildUpdateTableRequest(taOcid, defaultLimits);
        executeDdlFail(utReq, "InvalidParameter");

        /* Update table tags */
        Map<String, String> ftags = new HashMap<>();
        ftags.put("name", "nosql");

        UpdateTableDetails utInfo = UpdateTableDetails.builder()
                                        .freeformTags(ftags)
                                        .build();
        utReq = UpdateTableRequest.builder()
                    .tableNameOrId(taOcid)
                    .updateTableDetails(utInfo)
                    .build();
        executeDdl(utReq);

        gtRes = getTable(taOcid);
        assertNotNull(gtRes.getTable().getFreeformTags());

        /* Change compartment */
        ChangeTableCompartmentDetails info =
            ChangeTableCompartmentDetails.builder()
                .toCompartmentId(newCompartmentId)
                .build();
        ChangeTableCompartmentRequest ctcReq =
            ChangeTableCompartmentRequest.builder()
                .tableNameOrId(taOcid)
                .changeTableCompartmentDetails(info)
                .build();
        executeDdlFail(ctcReq, "IllegalArgument");

        /* list table usage */
        ListTableUsageRequest ltuReq = ListTableUsageRequest.builder()
                .tableNameOrId(taOcid)
                .build();
        try {
            client.listTableUsage(ltuReq);
            fail("Expect to fail but not");
        } catch (BmcException ex) {
            assertEquals("InvalidParameter", ex.getServiceCode());
        }

        /*
         * Row operations
         */
         Map<String, Object> row = makeTARow(1, 1);
         putRow(taOcid, row);

         List<String> key = Arrays.asList(new String[] {"id:1", "ida:1"});
         GetRowRequest grReq = GetRowRequest.builder()
                                 .tableNameOrId(taOcid)
                                 .key(key)
                                 .build();
         GetRowResponse grRes = client.getRow(grReq);
         assertNotNull(grRes.getRow());

         DeleteRowRequest drReq = DeleteRowRequest.builder()
                                     .tableNameOrId(taOcid)
                                     .key(key)
                                     .build();
         DeleteRowResponse drRes = client.deleteRow(drReq);
         assertTrue(drRes.getDeleteRowResult().getIsSuccess());

         /* Drop table */
         String[] ocids = new String[] {taOcid, tOcid};
         for (String ocid : ocids) {
             dropTable(ocid);

             /*
              * TODO: NOSQL-715
              * Enable below case in cloud test after fix it
              */
             if (!useCloudService) {
                 dropTable(ocid, true /* ifExists */, true /* wait */);
                 DeleteTableRequest dtReq;
                 dtReq = DeleteTableRequest.builder()
                             .tableNameOrId(ocid)
                             .build();
                 executeDdlFail(dtReq, "TableNotFound");
             }
         }
    }

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
        createTable("p", ddl);

        String fmt = "create table %s(%s integer, s string, primary key(%s))";
        String table;
        for (int i = 0; i < tableLimit - 1; i++) {
            table = "p.c" + i;
            ddl = String.format(fmt, table, "ck", "ck");
            createTable(table, ddl, null /* limits */);

            if ((++i) < tableLimit - 1) {
                table += ".d";
                ddl = String.format(fmt, table, "dk", "dk");
                createTable(table, ddl, null /* limits */);
            }
        }

        table = "p.c" + tableLimit;
        ddl = String.format(fmt, table, "ck", "ck");
        createChildTableFail(table, ddl, "TableLimitExceeded");

        /*
         * List tables
         */
        ListTablesRequest ltReq;
        ListTablesResponse ltRes;
        ltReq = ListTablesRequest.builder()
                    .compartmentId(getCompartmentId())
                    .build();
        ltRes = client.listTables(ltReq);
        assertEquals(tableLimit, ltRes.getTableCollection().getItems().size());
    }

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
        createTable("p", ddl);

        /*
         * Create table p.c with N columns, N is the number of column per table.
         */
        StringBuilder sb;
        sb = new StringBuilder("create table p.c(c1 integer, primary key(c1)");
        for (int i = 4; i < columnLimit; i++) {
            sb.append(", s").append(i).append(" string");
        }
        sb.append(")");
        createTable("p.c", sb.toString(), null /* limits */);

        /*
         * Create table p.c.d with N + 1 columns, N is the number of column per
         * table.
         */
        sb = new StringBuilder("create table p.c.d(d1 integer, primary key(d1)");
        for (int i = 5; i < columnLimit + 1; i++) {
            sb.append(", s").append(i).append(" string");
        }
        sb.append(")");
        createChildTableFail("p.c.d", sb.toString(), "IllegalArgument");

        /*
         * Adding more field to p.c should fail as the columns number will
         * exceed the limit
         */
        UpdateTableDetails atInfo;
        UpdateTableRequest atReq;

        ddl = "alter table p.c(add n1 integer)";
        atInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .ddlStatement(ddl)
                .build();
        atReq = UpdateTableRequest.builder()
                .updateTableDetails(atInfo)
                .tableNameOrId("p.c")
                .build();
        executeDdlFail(atReq, "IllegalArgument");
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
        /*
         * Cannot set limits on child table
         * TODO: add this case after modify the TableLimits of
         * CreateTableDetails to be optional
         */

        /* The parent table of t.a does not exist */
        createChildTableFail("t.a", createTADdl,
                    (cloudRunning ? "IllegalArgument" : "InvalidParameter"));

        createTable("t", createTDdl);
        createTable("t.a", createTADdl, null /* limits */);

        /* Don't allow to update limits of child table */
        UpdateTableDetails utInfo = UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .tableLimits(defaultLimits)
                .build();
        UpdateTableRequest utReq = UpdateTableRequest.builder()
                .updateTableDetails(utInfo)
                .tableNameOrId("t.a")
                .build();
        executeDdlFail(utReq, "InvalidParameter");

        /* Cannot drop the parent table still referenced by child table */
        DeleteTableRequest req = DeleteTableRequest.builder()
                .compartmentId(getCompartmentId())
                .tableNameOrId("t")
                .build();
        executeDdlFail(req,
                (cloudRunning ? "IllegalArgument": "InvalidParameter"));

        if (cloudRunning) {
            /* Don't allow to get table usage of child table */
            ListTableUsageRequest ltuReq = ListTableUsageRequest.builder()
                    .tableNameOrId("t.a")
                    .compartmentId(getCompartmentId())
                    .build();
            try {
                client.listTableUsage(ltuReq);
            } catch (BmcException ex) {
                assertEquals("InvalidParameter" , ex.getServiceCode());
                checkErrorMessage(ex);
            }

            /*
             * The Child table can't not be auto-reclaimable if its parent table
             * is not auto reclaimable.
             */
            createChildTableFail("t.g", createTGDdl,
                                 true /* isAutoReclaimable*/,
                                 "IllegalArgument");
        }
    }

    /**
     * Test put/get/delete row of child table.
     */
    @Test
    public void testPutGetDelete() {
        createTable("t", createTDdl);
        createTable("t.a", createTADdl, null /* limits */);
        createTable("t.a.b", createTABDdl, null /* limits */);

        Map<String, Object> row;
        GetRowRequest grReq;
        GetRowResponse grRes;
        DeleteRowRequest drReq;
        DeleteRowResponse drRes;

        /* put a row to table t */
        row = makeTRow(1);
        putRow("t", row);

        /* put a row to table t.a */
        row = makeTARow(1, 2);
        putRow("t.a", row);

        List<String> key = Arrays.asList(new String[] {"id:1", "ida:2"});
        grReq = GetRowRequest.builder()
                    .tableNameOrId("t.a")
                    .compartmentId(getCompartmentId())
                    .key(key)
                    .build();
        grRes = client.getRow(grReq);
        assertNotNull(grRes.getRow());

        drReq = DeleteRowRequest.builder()
                    .tableNameOrId("t.a")
                    .compartmentId(getCompartmentId())
                    .key(key)
                    .build();
        drRes = client.deleteRow(drReq);
        assertTrue(drRes.getDeleteRowResult().getIsSuccess());

        /* put a row to table t.a.b */
        row = makeTABRow(1, 2, 3);
        putRow("t.a.b", row);

        key = Arrays.asList(new String[] {"id:1", "ida:2", "idb:3"});
        grReq = GetRowRequest.builder()
                    .tableNameOrId("t.a.b")
                    .compartmentId(getCompartmentId())
                    .key(key)
                    .build();
        grRes = client.getRow(grReq);
        assertNotNull(grRes.getRow());

        drReq = DeleteRowRequest.builder()
                    .tableNameOrId("t.a.b")
                    .compartmentId(getCompartmentId())
                    .key(key)
                    .build();
        drRes = client.deleteRow(drReq);
        assertTrue(drRes.getDeleteRowResult().getIsSuccess());
    }

    @Test
    public void testTags() {
        assumeTrue("Skipping testTags for non-minicloud test", cloudRunning);

        Map<String, String> ftags = new HashMap<>();
        Map<String, Map<String, Object>> dtags = new HashMap<>();
        Map<String, Object> dtProps = new HashMap<>();

        /* freeform tags */
        ftags.put("name", "nosql");

        /* predefined tags */
        dtProps = new HashMap<>();
        dtProps.put(DEFINED_TAG_PROP, "true");
        dtags.put(DEFINED_TAG_NAMESPACE, dtProps);

        /* create table t */
        createTable("t", createTDdl);

        GetTableResponse gtRet;
        /* create table t.a with freeformTags/definedTags */
        createTable("t.a", createTADdl, null/* limits */, ftags, dtags);
        gtRet = getTable("t.a");
        checkTableTags(gtRet.getTable(), ftags, dtags, null);

        ftags.put("company", "oracle");
        dtProps.put(DEFINED_TAG_PROP, "false");

        /* Update tags */
        UpdateTableDetails utInfo =
            UpdateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .freeformTags(ftags)
                .definedTags(dtags)
                .build();
        UpdateTableRequest utReq =
            UpdateTableRequest.builder()
                .tableNameOrId("t.a")
                .updateTableDetails(utInfo)
                .build();
        executeDdl(utReq);

        gtRet = getTable("t.a");
        checkTableTags(gtRet.getTable(), ftags, dtags, null);
    }

    @Test
    public void testMoveCompartment() {
        assumeTrue("Skipping testMoveCompartment if not run against minicloud",
                   cloudRunning);

        final String[] tableNames = new String[] {"t", "t.a", "t.a.b", "t.g"};
        final String[] tableOcids = new String[4];
        String tOcid = null;

        /* Create table t and its descendant tables t.a, t.a.b and t.g */
        createTable("t", createTDdl);
        tOcid = getTableId("t");
        tableOcids[0] = tOcid;
        createTable("t.a", createTADdl, null /* limits */);
        tableOcids[1] = getTableId("t.a");
        createTable("t.a.b", createTABDdl, null /* limits */);
        tableOcids[2] = getTableId("t.a.b");
        createTable("t.g", createTGDdl, null /* limits */);
        tableOcids[3] = getTableId("t.g");

        ChangeTableCompartmentRequest req;
        ChangeTableCompartmentDetails info;

        /* Cannot change child table's compartment */
        info = ChangeTableCompartmentDetails.builder()
                .fromCompartmentId(getCompartmentId())
                .toCompartmentId(newCompartmentId)
                .build();
        req = ChangeTableCompartmentRequest.builder()
                .tableNameOrId("t.a")
                .changeTableCompartmentDetails(info)
                .build();
        executeDdlFail(req, "IllegalArgument");

        /*
         * Move the top table's compartment, its descendants table should be
         * moved as well
         */
        req = ChangeTableCompartmentRequest.builder()
                .tableNameOrId("t")
                .changeTableCompartmentDetails(info)
                .build();
        executeDdl(req);

        GetTableResponse gtRes;
        /* Get table using ocid */
        for (String ocid : tableOcids) {
            gtRes = getTable(ocid);
            assertEquals(newCompartmentId, gtRes.getTable().getCompartmentId());
        }

        /*
         * Move compartment using table ocid from newCompartment to
         * testCompartment.
         */
        info = ChangeTableCompartmentDetails.builder()
                .toCompartmentId(getCompartmentId())
                .build();
        req = ChangeTableCompartmentRequest.builder()
                .tableNameOrId(tOcid)
                .changeTableCompartmentDetails(info)
                .build();
        executeDdl(req);

        /* Get table using compartmentId + table name */
        for (String tname : tableNames) {
            gtRes = getTable(getCompartmentId(), tname);
            assertEquals(getCompartmentId(), gtRes.getTable().getCompartmentId());
        }
    }

    private void createChildTableFail(String tableName,
                                      String ddl,
                                      String expError) {
        createChildTableFail(tableName, ddl, false, expError);
    }

    private void createChildTableFail(String tableName,
                                      String ddl,
                                      boolean isAutoReclaimable,
                                      String expError) {
        CreateTableDetails ctInfo;
        CreateTableRequest ctReq;

        ctInfo = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl)
                .isAutoReclaimable(isAutoReclaimable)
                .build();
        ctReq = CreateTableRequest.builder()
                .createTableDetails(ctInfo)
                .build();
        executeDdlFail(ctReq, expError);
    }

    private Map<String, Object> makeTRow(int id) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("s", "s" + id);
        return row;
    }

    private Map<String, Object> makeTARow(int id, int ida) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("ida", ida);
        row.put("s", "s" + id + "_" + ida);
        return row;
    }

    private Map<String, Object> makeTABRow(int id, int ida, int idb) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("ida", ida);
        row.put("idb", idb);
        row.put("s", "s" + id + "_" + ida + "_" + idb);
        return row;
    }

    private void checkTableInfo(Table table,
                                String tableName,
                                Map<String, String> columns,
                                String[] primaryKeys,
                                String[] shardKeys,
                                TableLimits limits) {
        assertNotNull(table);

        assertEquals(getCompartmentId(), table.getCompartmentId());
        assertEquals(tableName, table.getName());
        assertNotNull(table.getTimeCreated());
        assertNotNull(table.getDdlStatement());

        Schema schema = table.getSchema();
        assertNotNull(schema);

        if (columns != null) {
            List<Column> cols = schema.getColumns();
            assertEquals(columns.size(), cols.size());
            for (Column col : cols) {
                String name = col.getName();
                assertTrue(columns.containsKey(name));
                assertTrue(columns.get(name).toUpperCase()
                            .equalsIgnoreCase(col.getType()));

                if (schema.getPrimaryKey().contains(name)) {
                    assertFalse(col.getIsNullable());
                } else {
                    assertTrue(col.getIsNullable());
                }
            }
        }

        if (primaryKeys != null) {
            assertEquals(primaryKeys.length, schema.getPrimaryKey().size());
            int i = 0;
            for (String key : schema.getPrimaryKey()) {
                assertTrue(key.equalsIgnoreCase(primaryKeys[i++]));
            }

            String[] skeys = (shardKeys != null) ? shardKeys : primaryKeys;
            assertEquals(skeys.length, schema.getShardKey().size());
            i = 0;
            for (String key : schema.getShardKey()) {
                assertTrue(key.equalsIgnoreCase(skeys[i++]));
            }
        }

        if (limits != null) {
            assertEquals(limits, table.getTableLimits());
        } else {
            assertNull(table.getTableLimits());
        }
        /* TODO: more validation */
    }

    private void checkTableTags(Table table,
                                Map<String, String> freeformTags,
                                Map<String, Map<String, Object>> definedTags,
                                Map<String, Map<String, Object>> systemTags) {

        if (freeformTags != null) {
            assertEquals(freeformTags, table.getFreeformTags());
        }

        if (definedTags != null) {
            assertDefinedTags(definedTags, table.getDefinedTags());
        }

        if (systemTags != null) {
            assertEquals(systemTags, table.getSystemTags());
        }
    }
}
