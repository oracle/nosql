/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.BeforeClass;
import org.junit.Test;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;

public class MultiRegionTableTest extends ProxyTestBase {
    private static final ReadOptions readOptions =
        new ReadOptions(Consistency.ABSOLUTE, 0, null);

    private static KVStore store;
    private static TableAPIImpl tableAPI;

    private static final String MR_TABLE = "mrtable";

    private final TableLimits limits = new TableLimits(100, 100, 1);
    private final int WAIT_MS = 10000;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        assumeTrue("Skipping MultiRegionTableTest if run in onprem mode",
                   !Boolean.getBoolean(ONPREM_PROP));

        System.setProperty(TEST_MRTABLE_PROP, MR_TABLE);
        ProxyTestBase.staticSetUp();

        if (!cloudRunning) {
            store = getStore(false /* excludeTombstone */);
            tableAPI = (TableAPIImpl)store.getTableAPI();
        }
    }

    @Test
    public void testBasicOps() {
        /* local proxy test only */
        assumeTrue("Skipping testBasicOps in minicloud test", !cloudRunning);
        runBasicOps(MR_TABLE, true);
        runBasicOps("singleton", false);
    }

    public void runBasicOps(String tableName, boolean isMRTable) {
        String tableDdl = "CREATE TABLE IF NOT EXISTS " + tableName +
                          "(sid INTEGER, " +
                          " id INTEGER, " +
                          " s STRING, " +
                          " j JSON, " +
                          " PRIMARY KEY(SHARD(sid), id))";

        TableResult tret = tableOperation(handle, tableDdl, limits, WAIT_MS);
        String kvTableName = getKVTableName(tret);

        int sid = 0;
        int id = 1;
        MapValue row = createRow(sid, id);
        Version ver;

        /* put */
        ver = doPut(tableName, row, null /* Option */, null /* matchVersion */);
        assertNotNull(ver);
        checkRow(row, tableName, kvTableName);

        /* putIfPresent */
        ver = doPut(tableName, row, Option.IfPresent, null /* matchVersion */);
        assertNotNull(ver);
        checkRow(row, tableName, kvTableName);

        /* putIfVersion */
        ver = doPut(tableName, row, Option.IfVersion, ver);
        assertNotNull(ver);
        checkRow(row, tableName, kvTableName);

        /* delete */
        boolean deleted = doDelete(tableName, row, null /* matchVersion */);
        assertTrue(deleted);
        checkRowDeleted(row, isMRTable, tableName, kvTableName);

        /* putIfAbsent */
        ver = doPut(tableName, row, Option.IfAbsent, null /* matchVersion */);
        assertNotNull(ver);
        checkRow(row, tableName, kvTableName);

        /* deleteIfVersion */
        deleted = doDelete(tableName, row, ver);
        assertTrue(deleted);
        checkRowDeleted(row, isMRTable, tableName, kvTableName);

        /* writeMultiple */
        sid++;
        int numOps = 3;
        List<MapValue> rows = new ArrayList<>();
        for (int i = 0; i < numOps; i++) {
            rows.add(createRow(sid, i));
        }
        doWriteMultiple(tableName, rows, true /* putOp */);
        for (MapValue val : rows) {
            checkRow(val, tableName, kvTableName);
        }
        doWriteMultiple(tableName, rows, false /* putOp */);
        for (MapValue val : rows) {
            checkRowDeleted(val, isMRTable, tableName, kvTableName);
        }

        /* multiDelete */
        sid++;
        rows.clear();
        for (int i = 0; i < numOps; i++) {
            row = createRow(sid, i);
            assertNotNull(doPut(tableName, row, null, null));
            rows.add(createRow(sid, i));
        }

        MapValue key = new MapValue().put("sid", sid);
        int ndel = doMultiDelete(tableName, key);
        assertEquals(numOps, ndel);
        for (MapValue val : rows) {
            checkRowDeleted(val, isMRTable, tableName, kvTableName);
        }

        /* query */
        String query;
        List<MapValue> results;
        Map<String, FieldValue> values = new HashMap<>();

        sid++;
        row = createRow(sid, 0);
        values.put("$sid", row.get("sid"));
        values.put("$id", row.get("id"));
        values.put("$s", row.get("s"));
        values.put("$j", row.get("j"));

        query = "DECLARE $sid INTEGER; $id INTEGER; $s STRING; $j JSON; " +
                "INSERT INTO " + tableName +
                "(sid, id, s, j) VALUES($sid, $id, $s, $j)";
        results = doQuery(query, values);
        checkRow(row, tableName, kvTableName);

        FieldValue sval = new StringValue(row.get("s").getString() + "_upd");
        row.put("s", sval);
        values.put("$s", sval);
        values.remove("$j");
        query = "DECLARE $sid INTEGER; $id INTEGER; $s STRING;" +
                "UPDATE " + tableName +
                " SET s = $s WHERE sid = $sid and id = $id";
        results = doQuery(query, values);
        assertEquals(1, results.size());
        checkRow(row, tableName, kvTableName);

        query = "SELECT * FROM " + tableName;
        results = doQuery(query);
        assertEquals(1, results.size());
        assertEquals(row, results.get(0));

        values.remove("$s");
        values.remove("$j");
        query = "DECLARE $sid INTEGER; $id INTEGER; DELETE FROM " +
                tableName + " WHERE sid = $sid and id = $id";
        results = doQuery(query, values);
        assertEquals(1, results.size());
        checkRowDeleted(row, isMRTable, tableName, kvTableName);

        /* query without being prepared */
        sid++;
        id = 1;
        row = createRow(sid, id);
        assertNotNull(doPut(tableName, row, null, null));
        query = "DELETE FROM " + tableName + " WHERE sid = " + sid +
                " and id = " + id;
        results = doQuery(query);
        assertEquals(1, results.size());
        checkRowDeleted(row, isMRTable, tableName, kvTableName);

        dropTable(handle, tableName);
    }

    @Test
    public void testCRDT() {
        runCRDTTest("testCRDT");
    }

    private void runCRDTTest(String tableName) {

        String tableDdl = "CREATE TABLE IF NOT EXISTS " + tableName +
                          "(sid INTEGER, " +
                          " id INTEGER, " +
                          " s STRING, " +
                          " ci INTEGER AS MR_COUNTER, " +
                          " cn NUMBER AS MR_COUNTER, " +
                          " j JSON(ci AS INTEGER MR_COUNTER, " +
                          "        cl as LONG MR_COUNTER), " +
                          " PRIMARY KEY(SHARD(sid), id))";

        TableResult tr = tableOperation(handle, tableDdl, limits, WAIT_MS);
        String kvTableName = getKVTableName(tr);

        int sid = 0;
        int id = 1;
        int step = 2;

        MapValue row = createRow(sid, id);
        assertNotNull(doPut(tableName, row, null, null));

        List<MapValue> results;
        Map<String, FieldValue> values = new HashMap<>();
        values.put("$sid", new IntegerValue(sid));
        values.put("$id", new IntegerValue(id));

        String query = "DECLARE $sid INTEGER; $id INTEGER; " +
                       "UPDATE " + tableName +
                           "$t SET ci = ci + " + step +
                           ", cn = cn + " + step +
                           ", $t.j.ci = $t.j.ci - " + step +
                           ", $t.j.cl = $t.j.cl + " + step +
                       " WHERE sid = $sid and id = $id " +
                       " RETURNING ci, cn, $t.j.ci as jci, $t.j.cl as jcl";
        results = doQuery(query, values);
        assertEquals(1, results.size());

        FieldValue ciVal = new IntegerValue(2);
        FieldValue cnVal = new NumberValue("2");
        FieldValue jciVal = new IntegerValue(-2);
        FieldValue jclVal = new LongValue(2);

        MapValue rec = results.get(0);
        assertEquals(ciVal, rec.get("ci"));
        assertEquals(cnVal, rec.get("cn"));
        assertEquals(jciVal, rec.get("jci"));
        assertEquals(jclVal, rec.get("jcl"));

        row.put("ci", ciVal);
        row.put("cn", cnVal);
        row.get("j").asMap().put("ci", jciVal).put("cl", jclVal);
        checkRow(row, tableName, kvTableName);

        dropTable(handle, tableName);
    }

    @Test
    public void testFreezeTable() {
        /*
         * Skip this test in local proxy test, because freeze/unfreeze schema
         * is managed by SC
         */
        assumeTrue("Skipping testBasicOps in local test", cloudRunning);

        final String tableName = "testFreezeTable";
        String freezeDdl = "ALTER TABLE " + tableName + " FREEZE SCHEMA";
        String unfreezeDdl = "ALTER TABLE " + tableName + " UNFREEZE SCHEMA";
        TableLimits limits = new TableLimits(100, 100, 1);
        TableLimits newLimits = new TableLimits(200, 150, 1);
        TableResult tr;
        String ddl;

        /*
         * 0. Create table with schema frozen.
         */
        String tableDdl = "CREATE TABLE " + tableName +
                          "(id INTEGER, s STRING, j JSON, PRIMARY KEY(id)) " +
                          "WITH SCHEMA FROZEN";
        tableOperation(handle, tableDdl, limits, WAIT_MS);

        /* freeze schema of the table already frozen, do nothing */
        tableOperation(handle, freezeDdl, null, WAIT_MS);

        /*
         * 1. Test update table after freeze table: cannot alter table schema
         *    but able to update ttl or limits
         */

        /* altering table schema should fail */
        ddl = "ALTER TABLE " + tableName + "(ADD i INTEGER)";
        tableOperation(handle, ddl, null /* limits */, null /* tableName */,
                       TableResult.State.ACTIVE,
                       IllegalArgumentException.class);

        /* updating ttl or limits should succeed */
        ddl = "ALTER TABLE " + tableName + " USING TTL 1 days";
        tr = tableOperation(handle, ddl, null, WAIT_MS);
        assertTrue(tr.getSchema().contains("\"ttl\":\"1 DAYS\""));

        ddl = "ALTER TABLE " + tableName + " USING TTL 1 days";
        tr = tableOperation(handle, null /* statement */, newLimits, tableName,
                            TableResult.State.ACTIVE, WAIT_MS);
        assertEquals(newLimits.getWriteUnits(),
                     tr.getTableLimits().getWriteUnits());

        /*
         * 2. Test alter table after unfreeze schema.
         */

        /* unfreeze schema */
        tableOperation(handle, unfreezeDdl, null, WAIT_MS);
        /* unfreeze schema again, do nothing */
        tableOperation(handle, unfreezeDdl, null, WAIT_MS);

        /* dropping JSON field should succeed after unfreezed schema */
        ddl = "ALTER TABLE " + tableName + "(DROP j)";
        tableOperation(handle, ddl, null, WAIT_MS);

        /*
         * 3. Test cannot freeze table without a JSON field
         */

        /* freezing table without a JSON field should fail */
        tableOperation(handle, freezeDdl, null /* limits */,
                       null /* tableName */, TableResult.State.ACTIVE,
                       IllegalArgumentException.class);

        /* Add a JSON field, freezing table should succeed */
        ddl = "ALTER TABLE " + tableName + "(ADD j1 JSON)";
        tableOperation(handle, ddl, null, WAIT_MS);
        tableOperation(handle, freezeDdl, null, WAIT_MS);

        /*
         * Creating table with schema frozen but without a JSON field
         * should fail
         */
        ddl = "CREATE TABLE tnojson(id INTEGER, s STRING, PRIMARY KEY(id)) " +
              "WITH SCHEMA FROZEN";
        tableOperation(handle, ddl, null /* limits */,
                       null /* tableName */, TableResult.State.ACTIVE,
                       IllegalArgumentException.class);

        /*
         * Test freeze table force
         */

        /*
         * Create a table without a JSON field and freeze it using
         * "with schema frozen force"
         */
        ddl = "CREATE TABLE tnojson(id INTEGER, s STRING, PRIMARY KEY(id)) " +
              "WITH SCHEMA FROZEN FORCE";
        tableOperation(handle, ddl, limits, WAIT_MS);

        /* Alter the TTL of the frozen table */
        ddl = "ALTER TABLE tnojson USING TTL 3 days";
        tableOperation(handle, unfreezeDdl, null, WAIT_MS);

        /* Fail: can't alter table schema */
        ddl = "ALTER TABLE tnojson (ADD i INTEGER)";
        tableOperation(handle, ddl, null /* limits */,
                       null /* tableName */, TableResult.State.ACTIVE,
                       IllegalArgumentException.class);

        /* unfreeze table */
        ddl = "ALTER TABLE tnojson UNFREEZE SCHEMA";
        tableOperation(handle, ddl, null, WAIT_MS);
        /* unfreeze table again, do nothing */
        tableOperation(handle, ddl, null, WAIT_MS);

        /* The table is mutable now, add a new field */
        ddl = "ALTER TABLE tnojson (ADD i INTEGER)";
        tableOperation(handle, ddl, null /* limits */, WAIT_MS);

        /* freeze table using "freeze schema force" */
        ddl = "ALTER TABLE tnojson FREEZE SCHEMA FORCE";
        tableOperation(handle, ddl, null /* limits */, WAIT_MS);
        /* freeze table again, do nothing */
        tableOperation(handle, ddl, null /* limits */, WAIT_MS);

        /* Fail: can't alter frozen table's schema */
        ddl = "ALTER TABLE tnojson (DROP i)";
        tableOperation(handle, ddl, null /* limits */,
                       null /* tableName */, TableResult.State.ACTIVE,
                       IllegalArgumentException.class);
    }

    private void checkRow(MapValue row,
                          String tableName,
                          String kvTableName) {

        MapValue retRow = doGet(tableName, row);
        assertEquals(row, retRow);

        /* skip checking the raw value if not local test */
        if (store == null) {
            return;
        }

        final int regionId = getRegionId();
        PrimaryKeyImpl pkey = getKVPrimaryKey(kvTableName, row);
        Value value = getKVValue(pkey);
        assertEquals(Value.Format.TABLE_V1, value.getFormat());
        assertTrue(value.getValue().length > 0);

        TableImpl table = pkey.getTableImpl();
        if (table.hasSchemaMRCounters()) {
            RowImpl kvRow = getKVRow(pkey);
            for (int i = 0; i < table.getFields().size(); i++) {
                if (table.isPrimKeyAtPos(i)) {
                    continue;
                }

                FieldDefImpl fdef = table.getFieldDef(i);
                FieldValueImpl fval;
                if (fdef.isMRCounter()) {
                    fval = kvRow.get(table.getFields().get(i));
                    checkMRCounterValue(fval, regionId);
                } else if (fdef.hasJsonMRCounter()) {
                    for (TablePath path : table.getSchemaMRCounterPaths(i)) {
                        fval = kvRow.evaluateScalarPath(path, 0);
                        checkMRCounterValue(fval, regionId);
                    }
                }
            }
        }
    }

    private void checkMRCounterValue(FieldValueImpl fval, int regionId) {
        if (fval != null && !fval.isNull()) {
            assertTrue(fval.isMRCounter());
            assertTrue(!fval.getMRCounterMap().isEmpty());
            if (fval.toString().startsWith("-")) {
                regionId = -regionId;
            }
            assertTrue(fval.getMRCounterMap().containsKey(regionId));
        }
    }

    private void checkRowDeleted(MapValue key,
                                 boolean isMRTable,
                                 String tableName,
                                 String kvTableName) {
        assertNull(doGet(tableName, key));

        /* skip checking the raw value if not local test */
        if (store == null) {
            return;
        }

        PrimaryKeyImpl pkey = getKVPrimaryKey(kvTableName, key);
        Value value = getKVValue(pkey);
        if (isMRTable) {
            checkTombStoneNone(value);
        } else {
            assertNull(value);
        }
    }

    private void checkTombStoneNone(Value value) {
        assertEquals(Value.Format.NONE, value.getFormat());
        assertTrue(value.getValue().length == 0);
    }

    private PrimaryKeyImpl getKVPrimaryKey(String kvTableName, MapValue key) {
        TableImpl table = getKVTable(kvTableName);
        assertNotNull("table not found: " + kvTableName, table);
        return table.createPrimaryKeyFromJson(key.toJson(), false);
    }

    private Value getKVValue(PrimaryKeyImpl pkey) {
        ValueVersion vv = store.get(pkey.getPrimaryKey(false),
                                    readOptions.getConsistency(),
                                    readOptions.getTimeout(),
                                    readOptions.getTimeoutUnit());
        if (vv != null) {
            return vv.getValue();
        }
        return null;
    }

    private RowImpl getKVRow(PrimaryKey pkey) {
        return (RowImpl)tableAPI.get(pkey, readOptions);
    }

    private TableImpl getKVTable(String kvTableName) {
        return (TableImpl)tableAPI.getTable(kvTableName);
    }

    private Version doPut(String tableName,
                          MapValue row,
                          Option option,
                          Version matchVersion) {

        PutRequest req = new PutRequest()
                .setTableName(tableName)
                .setOption(option)
                .setValue(row);
        if (matchVersion != null) {
            req.setMatchVersion(matchVersion);
        }

        PutResult ret = handle.put(req);
        return ret.getVersion();
    }

    private MapValue doGet(String tableName, MapValue key) {
        GetRequest req = new GetRequest()
                .setTableName(tableName)
                .setKey(key);
        GetResult ret = handle.get(req);
        assertTrue(ret.getReadKB() > 0);
        return ret.getValue();
    }

    private boolean doDelete(String tableName,
                             MapValue key,
                             Version matchVersion) {

        DeleteRequest req = new DeleteRequest()
                .setTableName(tableName)
                .setKey(key)
                .setMatchVersion(matchVersion);
        DeleteResult ret = handle.delete(req);
        assertTrue(ret.getWriteKB() > 0);
        return ret.getSuccess();
    }

    private void doWriteMultiple(String tableName,
                                 List<MapValue> rows,
                                 boolean putOp) {

        WriteMultipleRequest req = new WriteMultipleRequest();
        for (MapValue row : rows) {
            if (putOp) {
                req.add(new PutRequest()
                            .setTableName(tableName)
                            .setValue(row), true /* abortIfUnsucessful */);
            } else {
                req.add(new DeleteRequest()
                            .setTableName(tableName)
                            .setKey(row), true /* abortIfUnsucessful */);
            }
        }

        WriteMultipleResult ret = handle.writeMultiple(req);
        assertTrue(ret.getSuccess());
        assertTrue (ret.getWriteKB() > 0);
    }

    private int doMultiDelete(String tableName, MapValue key) {
        MultiDeleteRequest req = new MultiDeleteRequest()
                .setTableName(tableName)
                .setKey(key);
        MultiDeleteResult ret = handle.multiDelete(req);
        return ret.getNumDeletions();
    }

    private List<MapValue> doQuery(String query,
                                   Map<String, FieldValue> values) {

        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement pstmt = prepRet.getPreparedStatement();

        if (values != null) {
            for (Entry<String, FieldValue> e : values.entrySet()) {
                pstmt.setVariable(e.getKey(), e.getValue());
            }
        }

        List<MapValue> results = new ArrayList<>();
        try (@SuppressWarnings("resource")
            QueryRequest req = new QueryRequest().setPreparedStatement(pstmt)) {
            try (QueryIterableResult ret = handle.queryIterable(req)) {
                for (MapValue row : ret) {
                    results.add(row);
                }
            }
        }
        return results;
    }

    private List<MapValue> doQuery(String query) {
        List<MapValue> results = new ArrayList<>();
        try (@SuppressWarnings("resource")
            QueryRequest req = new QueryRequest().setStatement(query)) {
            try (QueryIterableResult ret = handle.queryIterable(req)) {
                for (MapValue row : ret) {
                    results.add(row);
                }
            }
        }
        return results;
    }

    private MapValue createRow(int sid, int id) {
        MapValue row = createPrimaryKey(sid, id);
        row.put("s", "s" + sid + id);

        String json = "{\"cl\":0, \"ci\":0}";
        FieldValue jval = MapValue.createFromJson(json, null);
        row.put("j", jval);
        return row;
    }

    private MapValue createPrimaryKey(int sid, int id) {
        MapValue row = new MapValue();
        row.put("sid", sid);
        row.put("id", id);
        return row;
    }

    private String getKVTableName(TableResult ret) {
        String tid = ret.getTableId();
        return (cloudRunning ? tid.replace(".", "_") : tid);
    }

    private static KVStore getStore(boolean excludeTombstone) {
        if (kvlite == null) {
            KVStoreConfig config = new KVStoreConfig(getStoreName(),
                                                    "localhost:5000");
            config.setExcludeTombstones(excludeTombstone);
            config.setEnableTableCache(false);
            return KVStoreFactory.getStore(config);
        }

        String hostPort = getHostName() + ":" + getKVPort();
        KVStoreConfig config = new KVStoreConfig(getStoreName(), hostPort);
        config.setExcludeTombstones(excludeTombstone);
        config.setEnableTableCache(false);
        return KVStoreFactory.getStore(config);
    }
}
