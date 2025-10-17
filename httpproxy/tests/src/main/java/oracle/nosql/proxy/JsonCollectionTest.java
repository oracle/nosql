/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;


import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.TimestampValue;

import org.junit.Ignore;
import org.junit.Test;


public class JsonCollectionTest extends ProxyTestBase {

    /*
     * Basic data tests for JSON Collection
     *  put, get, writemultiple, simple query
     */
    @Test
    public void testJsonCollection() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table noschema " +
            "(id integer, sid integer, primary key(shard(id), sid)) " +
            "as json collection";
        String insertQ = "insert into noschema(id, sid, name) values(5, 6, 'jack') returning *";

        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);
        PrepareRequest prepReq = new PrepareRequest()
            .setStatement(insertQ);
        PrepareResult prepRet = handle.prepare(prepReq);


        QueryRequest qReq = new QueryRequest().setPreparedStatement(prepRet);
        QueryResult qRes = handle.query(qReq);

        /* expected value from above insert query */
        MapValue value = new MapValue()
            .put("id", 5)
            .put("sid", 6)
            .put("name", "jack");

        for (MapValue res : qRes.getResults()) {
            assertEquals(value, res);
        }

        qReq = new QueryRequest()
            .setStatement("select * from noschema");

        qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertEquals(value, res);
        }

        value = new MapValue()
            .put("a", "aval")
            .put("id", 10)
            .put("name", "jane")
            .put("sid", 7);
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("noschema");

        PutResult pres = handle.put(putRequest);
        assertNotNull("Put failed", pres.getVersion());

        qReq = new QueryRequest()
            .setStatement("select * from noschema");

        qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            if (res.get("id").getInt() == 10) {
                assertEquals(res, value);
            }
        }

        /* do a write multiple */
        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        for (int i = 0; i < 10; i++) {
            /* need new MapValue -- it is not copied */
            value = new MapValue()
                .put("a", "aval")
                .put("name", "jane")
                .put("id", 10)      // needs to be the same, it's shard key
                .put("sid", i + 20)
                .put("multindex", i);
            PutRequest pr = new PutRequest()
                .setValue(value)
                .setTableName("noschema");
            wmReq.add(pr, false);
        }

        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());
        qReq = new QueryRequest()
            .setStatement("select * from noschema");

        int count = 0;
        do {
            qRes = handle.query(qReq);
            for (MapValue res : qRes.getResults()) {
                ++count;
            }
        } while (!qReq.isDone());
        assertEquals(count, 12); // 12 rows

        /* do a delete query */
        qReq = new QueryRequest()
            .setStatement("delete from noschema where id = 10 and sid = 20");
        qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertEquals(1, res.get("numRowsDeleted").getInt());
        }
    }

    /*
     * Exercise code that allows valid non-information-losing casts for
     * primary key fields
     */
    @Test
    public void testJsonCollectionKeyCast() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table noschema " +
            "(id integer, id1 long, id2 number, id3 string, id4 double, " +
            "primary key(shard(id), id1, id2, id3, id4)) " +
            "as json collection";
        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);

        /* normal types */
        MapValue value = new MapValue()
            .put("id", 5)
            .put("id1", 6L)
            .put("id2", 7.6)
            .put("id3", "jack")
            .put("id4", 5.6);

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("noschema");

        PutResult pres = handle.put(putRequest);
        assertNotNull("Put failed", pres.getVersion());

        qry("select * from noschema");

        /* use some coercion, success means no exception */
        value.put("id", "6")    // string to int
            .put("id1", "6789") // string to long
            .put("id2", 5L)     // long to number
            .put("id3", "joe")  // strings must be strings
            .put("id4", 7L);    // long to double
        pres = handle.put(putRequest);
        assertNotNull("Put failed", pres.getVersion());
        qry("select * from noschema");

        value.put("id", 5678L)      // long to int, no loss
            .put("id1", 1.0)        // float/double to long
            .put("id2", 56.67F)     // float to number
            .put("id3", "jane")     // strings must be strings
            .put("id4", "56.0005"); // string to double
        pres = handle.put(putRequest);
        assertNotNull("Put failed", pres.getVersion());
        qry("select * from noschema");

        /* invalid coercion */
        value.put("id", 56780000000L) // long to int, data loss
            .put("id1", 1.0)      // float/double to long
            .put("id2", 56.67F)   // float to number
            .put("id3", "jane")  // strings must be strings
            .put("id4", true);
        try {
            pres = handle.put(putRequest);
            fail("should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /* valid, except for non-string for string */
        value.put("id", 7);
        value.put("id3", 8);
        value.put("id4", 5.6);
        try {
            pres = handle.put(putRequest);
            fail("should have failed");
        } catch (IllegalArgumentException iae) {
        }
    }

    private void qry(String query) {
        if (!verbose) {
            return;
        }
        QueryRequest qReq = new QueryRequest()
            .setStatement(query);
        QueryResult qRes = handle.query(qReq);
        System.out.println("Results of " + query + ":");
        for (MapValue res : qRes.getResults()) {
            System.out.println("\t" + res);
        }
    }

    @Test
    public void testPutIf() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table noschema(" +
            "majorKey1 STRING, " +
            "majorKey2 STRING, " +
            "minorKey STRING, " +
            "PRIMARY KEY (SHARD(majorKey1, majorKey2), minorKey))" +
            "as json collection";

        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);

        final MapValue mapVal = new MapValue()
            .put("majorKey1", "k020f3dd0")
            .put("majorKey2", "80")
            .put("minorKey", "1e")
            .put("firstThread", false)
            .put("operation", "POPULATE")
            .put("index", 27777);

        final MapValue mapVal1 = new MapValue()
            .put("majorKey1", "k020f3dd1")
            .put("majorKey2", "81")
            .put("minorKey", "1f")
            .put("firstThread", false)
            .put("operation", "POPULATE")
            .put("index", 27777);

        final MapValue mapVal2 = new MapValue()
            .put("majorKey1", "k020f3dd2")
            .put("majorKey2", "81")
            .put("minorKey", "1f")
            .put("firstThread", false)
            .put("operation", "POPULATE")
            .put("index", 27777);

        final MapValue mapVal3 = new MapValue()
            .put("majorKey1", "k020f3dd3")
            .put("majorKey2", "81")
            .put("minorKey", "1f")
            .put("firstThread", false)
            .put("operation", "POPULATE")
            .put("index", 27777);

        /* Put a row */
        PutRequest putReq = new PutRequest()
            .setValue(mapVal)
            .setTableName("noschema");
        PutResult putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(0, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(1, putRes.getWriteKB(), putRes.getWriteUnits());

        /* Put a row again with SetReturnRow(false).
         * expect no row returned.
         */
        putReq.setReturnRow(false);
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(0, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(2, putRes.getWriteKB(), putRes.getWriteUnits());
        Version oldVersion = putRes.getVersion();

        /*
         * Put row again with SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertEquals(oldVersion, putRes.getExistingVersion());
        assertNotNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(2, putRes.getWriteKB(), putRes.getWriteUnits());
        oldVersion = putRes.getVersion();

        /*
         * Put a new row with SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq = new PutRequest()
            .setValue(mapVal1)
            .setTableName("noschema")
            .setReturnRow(true);
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(0, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(1, putRes.getWriteKB(), putRes.getWriteUnits());

        /* PutIfAbsent an existing row, it should fail */
        putReq = new PutRequest()
            .setValue(mapVal)
            .setTableName("noschema")
            .setOption(PutRequest.Option.IfAbsent);
        putRes = handle.put(putReq);
        assertNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(0, putRes.getWriteKB(), putRes.getWriteUnits());


        /*
         * PutIfAbsent fails + SetReturnRow(true),
         * return existing value and version
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        assertNull(putRes.getVersion());
        assertEquals(mapVal, putRes.getExistingValue());
        assertEquals(oldVersion, putRes.getExistingVersion());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(0, putRes.getWriteKB(), putRes.getWriteUnits());

        /* PutIfPresent an existing row, it should succeed */
        putReq = new PutRequest()
            .setValue(mapVal)
            .setTableName("noschema")
            .setOption(PutRequest.Option.IfPresent);
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(2, putRes.getWriteKB(), putRes.getWriteUnits());
        oldVersion = putRes.getVersion();

        /*
         * PutIfPresent succeed + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertEquals(mapVal, putRes.getExistingValue());
        assertEquals(oldVersion, putRes.getExistingVersion());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(2, putRes.getWriteKB(), putRes.getWriteUnits());
        Version ifVersion = putRes.getVersion();

        /* PutIfPresent a new row, it should fail */
        putReq = new PutRequest()
            .setValue(mapVal2)
            .setTableName("noschema")
            .setOption(PutRequest.Option.IfPresent);
        putRes = handle.put(putReq);
        assertNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(0, putRes.getWriteKB(), putRes.getWriteUnits());

        /*
         * PutIfPresent fail + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        assertNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(0, putRes.getWriteKB(), putRes.getWriteUnits());

        /* PutIfAbsent a new row, it should succeed */
        putReq = new PutRequest()
            .setOption(PutRequest.Option.IfAbsent)
            .setValue(mapVal2)
            .setTableName("noschema");
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(1, putRes.getWriteKB(), putRes.getWriteUnits());

        /* PutIfAbsent success + SetReturnRow(true) */
        putReq = new PutRequest()
            .setOption(PutRequest.Option.IfAbsent)
            .setValue(mapVal3)
            .setTableName("noschema");
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(1, putRes.getWriteKB(), putRes.getWriteUnits());

        /*
         * PutIfVersion an existing row with unmatched version, it should fail.
         */
        putReq = new PutRequest()
            .setOption(PutRequest.Option.IfVersion)
            .setMatchVersion(oldVersion)
            .setValue(mapVal)
            .setTableName("noschema");
        putRes = handle.put(putReq);
        assertNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(0, putRes.getWriteKB(), putRes.getWriteUnits());

        /*
         * PutIfVersion fails + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        assertNull(putRes.getVersion());
        assertEquals(ifVersion, putRes.getExistingVersion());
        assertEquals(mapVal, putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(0, putRes.getWriteKB(), putRes.getWriteUnits());

        /*
         * Put an existing row with matching version, it should succeed.
         */
        putReq = new PutRequest()
            .setOption(PutRequest.Option.IfVersion)
            .setMatchVersion(ifVersion)
            .setValue(mapVal)
            .setTableName("noschema");
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(2, putRes.getWriteKB(), putRes.getWriteUnits());
        ifVersion = putRes.getVersion();

        /*
         * PutIfVersion succeed + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setMatchVersion(ifVersion).setReturnRow(true);
        putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());
        assertNull(putRes.getExistingVersion());
        assertNull(putRes.getExistingValue());
        assertReadKB(1, putRes.getReadKB(), putRes.getReadUnits(), true);
        assertWriteKB(2, putRes.getWriteKB(), putRes.getWriteUnits());
    }

    @Test
    public void testIndexes() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table noschema " +
            "(iD integer, SiD integer, primary key(shard(id), sid)) " +
            "as json collection";
        String createIndex = "create index idx on noschema(name as string)";
        String createIndex1 = "create index idx1 on noschema(age as integer)";

        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, createIndex, null, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, createIndex1, null, null,
                       TableResult.State.ACTIVE, null);

        MapValue value = new MapValue()
            .put("a", "aval")
            .put("sid", 7);

        PutRequest putRequest = new PutRequest()
            .setTableName("noschema");
        for (int i = 0; i < 10; i++) {
            value.put("id", i)
                .put("nAme", ("jane" + i))
                .put("age", i);
            putRequest.setValue(value);
            PutResult pres = handle.put(putRequest);
            assertNotNull("Put failed", pres.getVersion());
        }

        QueryRequest qReq =
            new QueryRequest().setStatement("select * from noschema");
        QueryResult qRes = handle.query(qReq);
        assertEquals(10, qRes.getResults().size());
        for (MapValue res : qRes.getResults()) {
            /* assert case-preservation */
            assertTrue(res.toString().contains("SiD"));
            assertTrue(res.toString().contains("nAme"));
        }

        qReq =
            new QueryRequest().setStatement(
                "select * from noschema where age > 3 order by age");

        ArrayList<MapValue> results = new ArrayList<MapValue>();
        do {
            qRes = handle.query(qReq);
            results.addAll(qRes.getResults());
        } while (!qReq.isDone());
        assertEquals(6, results.size());
        for (MapValue res : results) {
            /* assert case-preservation */
            assertTrue(res.toString().contains("nAme"));
            assertTrue(res.toString().contains("SiD"));
        }
    }

    @Ignore
    public void testGeoIndexes() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table geo " +
            "(id integer, primary key(id)) as json collection";
        final String pointIndex =
            "create index idx_kind_ptn on geo(info.kind as string," +
            "info.point as point)";
        final String geoIndex =
            "create index idx_geom on geo(info.geom as geometry " +
            "{\"max_covering_cells\":400})";

        final String[] data = new String[] {
            "insert into geo values(1, {\"info\": { " +
            "\"kind\": \"farm\", \"point\": {\"type\":\"point\", " +
            "\"coordinates\": [23.549, 35.2908]}}})",
            "insert into geo values(2, {\"info\": { " +
            "\"kind\": \"park\", \"point\": {\"type\":\"point\", " +
            "\"coordinates\": [24.9, 35.4]}}})"
        };

        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, pointIndex, null, null,
                       TableResult.State.ACTIVE, null);
        tableOperation(handle, geoIndex, null, null,
                       TableResult.State.ACTIVE, null);

        for (String q : data) {
            QueryRequest qReq = new QueryRequest().setStatement(q);
            QueryResult qRes = handle.query(qReq);
            System.out.println(qRes);
        }

        QueryRequest qReq =
            new QueryRequest().setStatement(
                "select /* FORCE_PRIMARY_INDEX(geo) */ * from geo g where geo_near(g.info.point, " +
                "{\"type\": \"point\", \"coordinates\": [24.0175, 35.5156]}," +
                "5000)");
        QueryResult qRes = handle.query(qReq);
        for (MapValue val : qRes.getResults()) {
            System.out.println(val);
        }
    }

    /*
     * Check edge and invalid situations for JSON Collection.
     * Invalid:
     *  o invalid (not JSON) types
     *  o attempt to schema evolve
     *  o bad key
     * Edge:
     *  o identity column as key, evolve sequence
     *  o TTL, with evolution
     */
    @Test
    public void testJsonCollectionEdge() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table noschema " +
            "(id integer, primary key(id)) as json collection";
        tableOperation(handle, createTable, limits, null,
                       TableResult.State.ACTIVE, null);

        /*
         * Bad types
         */
        final Timestamp ts = Timestamp.valueOf("2018-05-02 10:23:42.123");
        final FieldValue tsVal = new TimestampValue(ts);
        badType("time", tsVal, "noschema");

        badType("bin", new BinaryValue(new byte[4]), "noschema");

        /*
         * Try to evolve in an illegal manner
         */
        final String alter = "alter table noschema(add name string)";
        TableResult tres = tableOperation(handle, alter, null, null,
                                          TableResult.State.ACTIVE,
                                          IllegalArgumentException.class);

        /*
         * new table, with identity col, evolve it to change sequence start
         */
        createTable = "create table noschema1 " +
            "(id integer generated always as identity, " +
            "primary key(id)) as json collection";
        tres = tableOperation(handle, createTable, limits, 5000);

        tres = tableOperation(handle, "alter table noschema1 (modify id " +
                              "generated always as identity(start with 1002))",
                              null, 5000);

        /*
         * Put a row and verify that the generated value is 1002
         */
        MapValue value = new MapValue()
            .put("name", "myname")
            .put("nullval", JsonNullValue.getInstance());
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("noschema1");
        PutResult pres = handle.put(putRequest);

        QueryRequest qReq = new QueryRequest()
            .setStatement("select * from noschema1");
        QueryResult qRes = handle.query(qReq);
        for (MapValue res : qRes.getResults()) {
            assertTrue(res.get("id").getInt() == 1002);
        }

        /*
         * Add a TTL
         */
        tres = tableOperation(handle, "alter table noschema1 using TTL 5 days",
                              null, 5000);
        assertTrue(tres.getDdl().toLowerCase().contains("5 days"));

        tres = tableOperation(handle, "alter table noschema1 using TTL 2 hours",
                              null, 5000);
        assertTrue(tres.getDdl().toLowerCase().contains("2 hours"));
    }

    @Test
    public void testNested() {
        TableLimits limits = new TableLimits(10, 10, 1);
        final String createTable = "create table noschema(id long, " +
            "primary key(id)) as json collection";
        TableResult tres = tableOperation(handle, createTable, limits, 5000);
        String json = "{" +
            "\"id\":0," +
            "\"name\": \"Foo\"," +
            "\"tags\": [\"rock\",\"metal\",\"bar\"]" +
            "}";

        String json1 = "{" +
            "\"id\":1," +
            "\"name\": \"Foo\"," +
            "\"obj\": {\"a\":1,\"b\":2,\"c\":3, " +
            "\"tags\": [\"rock\",\"metal\",\"bar\"]" +
            "}}";

        String json2 = "{" +
            "\"id\":2," +
            "\"obj\": {\"a\":1,\"b\":2,\"c\":3, " +
            "\"obj1\": {\"d\":1,\"e\":2,\"f\":3} " +
            "}}";

        String[] docs = new String[]{json, json1, json2};
        int i = 0;
        for (String doc : docs) {
            MapValue val = (MapValue)FieldValue.createFromJson(doc,null);
            PutRequest pr = new PutRequest()
                .setValue(val)
                .setTableName("noschema");
            PutResult pres = handle.put(pr);
            assertNotNull("Put failed", pres.getVersion());

            GetRequest gr = new GetRequest()
                .setKey(new MapValue().put("id", i++))
                .setTableName("noschema");
            GetResult gres = handle.get(gr);
            assertTrue(JsonUtils.jsonEquals(val.toString(),
                                            gres.getValue().toString()));
        }
    }

    @Test
    public void testDelete() {
        TableLimits limits = new TableLimits(10, 10, 1);
        String createTable = "create table noschema(" +
            "majorKey1 STRING, " +
            "majorKey2 STRING, " +
            "minorKey STRING, " +
            "PRIMARY KEY (SHARD(majorKey1, majorKey2), minorKey))" +
            "as json collection";

        tableOperation(handle, createTable, limits, null,
            TableResult.State.ACTIVE, null);

        final MapValue key = new MapValue()
            .put("majorKey1", "k020f3dd0")
            .put("majorKey2", "80")
            .put("minorKey", "1e");

        /* put a row */
        PutRequest putReq = new PutRequest()
            .setTableName("noschema")
            .setValue(key);
        PutResult putRes = handle.put(putReq);
        assertNotNull(putRes.getVersion());

        /* Delete a row */
        DeleteRequest delReq = new DeleteRequest()
            .setKey(key)
            .setTableName("noschema");
        DeleteResult delRes = handle.delete(delReq);
        assertTrue(delRes.getSuccess());
        assertNull(delRes.getExistingVersion());
        assertNull(delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(1, delRes.getWriteKB(), delRes.getWriteUnits());

        /* Put the row back to store */
        putReq = new PutRequest().setValue(key).setTableName("noschema");
        putRes = handle.put(putReq);
        Version oldVersion = putRes.getVersion();
        assertNotNull(oldVersion);

        /* Delete succeed + setReturnRow(true), existing row returned. */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        assertTrue(delRes.getSuccess());
        assertEquals(oldVersion, delRes.getExistingVersion());
        assertEquals(key, delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(1, delRes.getWriteKB(), delRes.getWriteUnits());

        /* Delete fail + setReturnRow(true), no existing row returned. */
        delRes = handle.delete(delReq);
        assertFalse(delRes.getSuccess());
        assertNull(delRes.getExistingVersion());
        assertNull(delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(0, delRes.getWriteKB(), delRes.getWriteUnits());

        /* Put the row back to store */
        putReq = new PutRequest().setValue(key).setTableName("noschema");
        putRes = handle.put(putReq);
        Version ifVersion = putRes.getVersion();
        assertNotNull(ifVersion);

        /* DeleteIfVersion with unmatched version, it should fail */
        delReq = new DeleteRequest()
            .setMatchVersion(oldVersion)
            .setKey(key)
            .setTableName("noschema");
        delRes = handle.delete(delReq);
        assertFalse(delRes.getSuccess());
        assertNull(delRes.getExistingVersion());
        assertNull(delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(0, delRes.getWriteKB(), delRes.getWriteUnits());

        /*
         * DeleteIfVersion with unmatched version + setReturnRow(true),
         * the existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        assertFalse(delRes.getSuccess());
        assertEquals(ifVersion, delRes.getExistingVersion());
        assertEquals(key, delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(0, delRes.getWriteKB(), delRes.getWriteUnits());

        /* DeleteIfVersion with matched version, it should succeed. */
        delReq = new DeleteRequest()
                .setMatchVersion(ifVersion)
                .setKey(key)
                .setTableName("noschema");
        delRes = handle.delete(delReq);
        assertTrue(delRes.getSuccess());
        assertNull(delRes.getExistingVersion());
        assertNull(delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(1, delRes.getWriteKB(), delRes.getWriteUnits());

        /* Put the row back to store */
        putReq = new PutRequest().setValue(key).setTableName("noschema");
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();
        assertNotNull(ifVersion);

        /*
         * DeleteIfVersion with matched version + setReturnRow(true),
         * it should succeed but no existing row returned.
         */
        delReq.setMatchVersion(ifVersion).setReturnRow(true);
        delRes = handle.delete(delReq);
        assertTrue(delRes.getSuccess());
        assertNull(delRes.getExistingVersion());
        assertNull(delRes.getExistingValue());
        assertReadKB(1, delRes.getReadKB(), delRes.getReadUnits(), true);
        assertWriteKB(1, delRes.getWriteKB(), delRes.getWriteUnits());
    }

    private void badType(String fieldName, FieldValue val, String tableName) {
        MapValue value = new MapValue()
            .put("id", 10)
            .put(fieldName, val);
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);

        try {
            PutResult pres = handle.put(putRequest);
            fail("operation should have thrown IAE");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("Invalid JSON type"));
        }
    }
}
