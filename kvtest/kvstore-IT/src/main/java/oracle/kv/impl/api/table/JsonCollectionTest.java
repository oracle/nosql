/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.kv.StatementResult;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.query.ExecuteOptions;

import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.TimestampValue;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/*
 * JsonCollection table test cases
 */
public class JsonCollectionTest extends TableTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        staticSetUp(true /* excludeTombstone */);
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testCreationOfJsonCollectionTable() {
        //create a json collection table with ttl
        final String createTable =
                "create table myTable(id integer, primary key(id)) " +
                        "as json collection using ttl 5 days";

        executeDdl(createTable);

        /* alter the ttl value */
        executeDdl("alter table myTable using ttl 15 days", true);

        /* try creating and dropping index on json collection table */
        executeDdl("create index myindex1 on myTable(salary as long)",true);
        executeDdl("drop index myindex1 on myTable",true);

        /* put some rows which has different type values for a field[ex:person.age] */
        TableImpl table = getTable("myTable");

        RowImpl row = table.createRow();
        row.put("id", 1);
        row.put("name", "myname");
        row.putJson("person", "{\"age\":\"thirty\"}");
        // put the row
        tableImpl.put(row, null, null);

        row.put("id", 2);
        row.put("name", "myname2");
        row.putJson("person", "{\"age\":[1,2,3]}");
        // put the row
        tableImpl.put(row, null, null);

        row.put("id", 3);
        row.put("name", "myname3");
        row.putJson("person", "{\"age\":{\"dob\":\"fifth oct\"}}");
        // put the row
        tableImpl.put(row, null, null);

        row.put("id", 4);
        row.put("name", "myname4");
        row.putJson("person", "{\"age\":56.89}");
        // put the row
        tableImpl.put(row, null, null);

        /* Try creating index on person.age which is holding different values for different rows
         should fail */
        executeDdl("create index idx1 on myTable (person.age as integer)",false);
        executeDdl("create index idx2 on myTable (person.age as string)",false);

        row.put("id", 5);
        row.put("name", "myname5");
        row.putJson("person", "{\"salary\":56000}");
        // put the row
        tableImpl.put(row, null, null);

        row.put("id", 6);
        row.put("name", "myname6");
        row.putJson("person", "{\"salary\":66000}");
        // put the row
        tableImpl.put(row, null, null);

        /* create an index of long type and try inserting a different type value, should fail */
        executeDdl("create index idx3 on myTable (person.salary as long)",true);

        try {
            row.put("id", 7);
            row.put("name", "myname7");
            row.putJson("person", "{\"salary\":\"rupali\"}");
            // put the row
            tableImpl.put(row, null, null);
            fail("Attempt to put person.salary should have failed");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().toLowerCase().contains(
                    "invalid type for json index field"));
        }

        /* try to create a ANYATOMIC type index and insert some atomic values to indexed field */
        executeDdl("create index idx4 on myTable(person.houseNumber as anyAtomic)",true);
        StatementResult sr = executeDml(
                "insert into myTable (id,name,person) values(8, \"myname8\",{\"houseNumber\":66})");
        assertTrue(sr.isSuccessful());

        sr = executeDml(
                "insert into myTable (id,name,person) values(9, \"myname9\",{\"houseNumber\":\"sixty\"})");
        assertTrue(sr.isSuccessful());

        sr = executeDml(
                "insert into myTable (id,name,person) values(10, \"myname10\",{\"houseNumber\":true})");
        assertTrue(sr.isSuccessful());
        sr = executeDml(
                "insert into myTable (id,name,person) values(11, \"myname11\",{\"houseNumber\":6600000000000000})");
        assertTrue(sr.isSuccessful());

        /* Type not supported in json indexes: ARRAY, should fail while trying to put ARRAY on an AnyAtomic index field */
        try {
            row.put("id", 12);
            row.put("name", "myname12");
            row.putJson("person", "{\"houseNumber\":[1,2,3]}");
            // put the row
            tableImpl.put(row, null, null);
            fail("Attempt to put person.houseNumber should have failed");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().toLowerCase().contains(
                    "type not supported in json indexes"));
        }
        /* create non mr normal table with mr_counter should fail */
        if (!mrTableMode) {
            executeDdl("create table normal" +
                       "  (id integer," +
                       "   counter integer as mr_counter, primary key(id))",
                       false);
            executeDdl("create table normal" +
                       "  (id integer," +
                       "   json json(count as integer mr_counter)," +
                       "   primary key(id))",
                       false);
        }
    }

    @Test
    public void testJsonCollection() {
        /* make "iD" odd to test case-insensitivity paths */
        final String createTable =
            "create table noschema(iD string, primary key(id)) " +
            "as json collection";

        executeDdl(createTable);
        TableImpl table = getTable("noschema");

        RowImpl row = table.createRow();
        row.put("id", "12345");
        row.put("name", "myname");
        row.put("age", 45);
        row.put("bool", true);
        row.put("double", 1.67);
        row.putJson("json", "{\"a\": 1}");
        row.putJson("json1", "{\"a\": 10, \"b\":{\"c\":null}}");
        row.putJson("jsonArray", "[1,2,3, {\"x\":true}]");

        // put the row
        tableImpl.put(row, null, null);

        row.put("id", "12346");
        row.put("age", 66);
        row.putJson("json", "{\"a\": 10}");
        assertNotNull(tableImpl.put(row, null, null));

        PrimaryKey key = table.createPrimaryKey();
        key.put("id", "12346");
        RowImpl getRow = (RowImpl) tableImpl.get(key, null);
        assertEquals(getRow, row);

        String query1 = "SELECT * FROM noschema";
        String query2 =
            "SELECT ID, age, m.json1.a, m.json1.b.c, m.jsonArray[3] " +
            "FROM noschema m";
        ExecuteOptions options = new ExecuteOptions();
        options.setAsync(false);

        PreparedStatementImpl ps =
            (PreparedStatementImpl) store.prepare(query1, null);
        assertTrue(ps.getResultDef().toString().contains("RECORD"));

        StatementResult sr = executeDml(query1, options);
        /* use iterable */
        int count = 0;

        for (RecordValue val : sr) {
            assertTrue(val.get("Id").asString().get().length() > 3);
            ++count;
        }
        assertEquals(count, 2);

        sr = executeDml(query2, options);
        Iterator<RecordValue> iter = sr.iterator();
        count = 0;
        while (iter.hasNext()) {
            ++count;
            RecordValue val = iter.next();
            /* assert that use of ID returned that vs the schema name */
            assertTrue(val.toString().contains("ID"));
            assertNotNull(val.get("Column_5"));
        }
        assertEquals(count, 2);

        // add a couple of indexes
        executeDdl("create index idx on noschema(age as integer)");
        executeDdl("create index idx1 on noschema(json.a as integer)");
        table = getTable("noschema");

        Index index = table.getIndex("idx");
        IndexKey ikey = index.createIndexKey();

        ikey.put("age", 45);
        TableIterator<Row> idxIter =
            tableImpl.tableIterator(ikey, null, null);
        count = 0;
        while(idxIter.hasNext()) {
            row = (RowImpl) idxIter.next();
            count++;
        }
        assertEquals(count, 1);

        index = table.getIndex("idx1");
        ikey = index.createIndexKey();
        ikey.put("json.a", 10);

        idxIter =
            tableImpl.tableIterator(ikey, null, null);
        count = 0;
        while(idxIter.hasNext()) {
            row = (RowImpl) idxIter.next();
            count++;
        }
        assertEquals(count, 1);

        String idxQ = "select * from noschema where age > 50";
        PreparedStatementImpl ps2 =
            (PreparedStatementImpl) store.prepare(idxQ, null);
        /* assert there's an index used */
        assertTrue(ps2.getQueryPlan().display(true).contains("ALL_SHARDS"));

        sr = executeDml(idxQ, options);
        iter = sr.iterator();
        count = 0;
        while (iter.hasNext()) {
            count++;
            RecordValue resRec = iter.next();
            assertTrue(resRec.get("age").asInteger().get() > 50);
        }
        assertEquals(count, 1);

        sr = executeDml(
            "insert into noschema values(1, {\"a\":67}) returning *",
            options);
        assertTrue(sr.isSuccessful());
        iter = sr.iterator();
        count = 0;
        while (iter.hasNext()) {
            RecordValue resRec = iter.next();
            assertTrue(resRec.get("a").asInteger().get() == 67);
            ++count;
        }
        assertEquals(count, 1);

        /*
         * try to alter, should fail
         */
        executeDdl("alter table noschema(add name string)", false);
        executeDdl("alter table noschema(add address.pincode integer)",false);

        /*
         * try to alter, should succeed
         */
        executeDdl("create table identity(id integer GENERATED ALWAYS AS IDENTITY , primary key(id)) as json collection");
        executeDdl("alter table identity (MODIFY id GENERATED BY DEFAULT AS IDENTITY)",true);

        /* try to create an index which is already existing, should fail */
        executeDdl("create index idx on noschema(age as integer)",false);

        /*
         * Test JSON collection in PrepareCallback
         */
        DmlTest.PrepareCB cb = new DmlTest.PrepareCB();
        ExecuteOptions opts = new ExecuteOptions();
        opts.setPrepareCallback(cb);
        DmlTest.prepareInternal(
            "create table zzz(id integer, primary key(id)) as json collection",
            opts);
        assertTrue(cb.getJsonCollection());

        /* Test delete query */
        sr = executeDml("delete from noschema where id='12345'");
        for (RecordValue val : sr) {
            assertTrue(val.get("numRowsDeleted").asLong().get() == 1);
        }

        /*
         * Test row equality (equals/hashCode on JsonCollectionRowImpl)
         */
        row = table.createRow();
        RowImpl row1 = table.createRow();
        row.put("id", "1");
        row1.put("id", "1");
        assertEquals(row, row1);
        assertEquals(row.hashCode(), row1.hashCode());
        row.put("age", 6);
        row1.put("age", 6);
        assertEquals(row, row1);
        assertEquals(row.hashCode(), row1.hashCode());
        row1.put("age", 7);
        assertFalse(row.equals(row1));
        assertFalse(row.hashCode() == row1.hashCode());
        row1.put("age", 6);
        assertEquals(row, row1);
        assertEquals(row.hashCode(), row1.hashCode());
    }

    @Test
    public void testJsonCollectionWithReturnRow() {
        final String createTable =
                "create table noschema(id string, primary key(id)) " +
                        "as json collection";

        executeDdl(createTable);
        TableImpl table = getTable("noschema");
        ReturnRow rr = table.createReturnRow(ReturnRow.Choice.ALL);
        RowImpl row = table.createRow();
        row.put("id", "12345");
        row.put("name", "xyz");
        row.put("age", 45);
        row.put("bool", true);
        row.put("double", 1.67);
        row.putJson("json", "{\"a\": 1}");

        // put the row
        tableImpl.put(row, null, null);

        PrimaryKey key = table.createPrimaryKey();
        key.put("id", "12345");
        Row getRow = tableImpl.get(key, null);

        getRow.put("id", "12345");
        getRow.put("age", 66);
        getRow.putJson("json", "{\"a\": 10}");

        assertNotNull(tableImpl.put(getRow, rr, null));
        JsonCollectionRowImpl newRow = new JsonCollectionRowImpl((JsonCollectionRowImpl) rr);
        assertEquals( newRow, row);
        assertEquals(newRow.getLastModificationTime(),
                row.getLastModificationTime());
    }

    /*
     * Illegal combinations
     */
    @Test
    public void testBadTables() {
        /* non-key field  */
        final String badSchema1 = "create table ns(id string, " +
            "primary key(id), age integer) as json collection";

        executeDdl(badSchema1, false);
    }

    @Test
    public void testIdentity() {
        /*
         * Identity column tables:
         *  1. generated by default, one field
         *  2. generated by default, two fields
         *  3. generated always, one field
         *  4. generated always, two fields
         *  5. UUID, one field
         *  5. UUID, two fields
         * Try inserting with and without defaults (as appropriate)
         * Test failure for generated always cases
         * Validate results
         */
        final String defOne = "defOne";
        final String defTwo = "defTwo";
        final String alOne = "alOne";
        final String alTwo = "alTwo";
        final String uuidOne = "uuidOne";
        final String uuidTwo = "uuidTwo";
        //id string as uuid generated by default
        final String createDefOne =
            "create table " + defOne + "(id integer " +
            "generated by default as identity, primary key(id)) " +
            "as json collection";
        final String createDefTwo =
            "create table " + defTwo + "(sid integer, id integer " +
            "generated by default as identity, primary key(shard(sid), id)) " +
            "as json collection";
        final String createAlOne =
            "create table " + alOne+ "(id integer " +
            "generated always as identity, primary key(id)) " +
            "as json collection";
        final String createAlTwo =
            "create table " + alTwo+ "(sid integer, id integer " +
            "generated always as identity, primary key(shard(sid), id)) " +
            "as json collection";
        final String createUuidOne =
            "create table " + uuidOne + "(id string as uuid generated by " +
            "default, primary key(id)) as json collection";
        final String createUuidTwo =
            "create table " + uuidTwo + "(sid integer, id string as uuid " +
            "generated by default, primary key(shard(sid)id)) " +
            "as json collection";

        executeDdl(createDefOne);
        executeDdl(createDefTwo);
        executeDdl(createAlOne);
        executeDdl(createAlTwo);
        executeDdl(createUuidOne);
        executeDdl(createUuidTwo);

        insert(defOne, "default, {\"a\":67}", "a", 67, true);
        insert(defTwo, "1, default, {\"a\":67}", "a", 67, true);
        /* override default */
        insert(defOne, "8, {\"a\":67}", "id", 8, true);
        insert(defTwo, "1, 10, {\"a\":67}", "id", 10, true);

        /* generated always */
        insert(alOne, "default, {\"a\":67}", "a", 67, true);
        insert(alTwo, "1, default, {\"a\":67}", "a", 67, true);
        /* try to override default, should fail */
        insert(alOne, "10, {\"a\":67}", "a", 67, false);
        insert(alTwo, "1, 10, {\"a\":67}", "a", 67, false);

        /* UUID */
        insert(uuidOne, "default, {\"a\":60}", "a", 60, true);
        insert(uuidTwo, "1, default, {\"a\":61}", "a", 61, true);
        /* override default, must still be a valid UUID string */
        insert(uuidOne, "random_uuid(), {\"a\":68}", "a", 68, true);
        insert(uuidTwo, "1, random_uuid(), {\"a\":70}", "a", 70, true);
    }

    /*
     * Generate an insert statement and make assertion about the
     * value inserted
     */
    private void insert(String tableName, String valuesString,
                        String fieldName, int value, boolean shouldSucceed) {
        String statement = "insert into " + tableName + " values(" +
            valuesString + ") returning *";
        try {
            StatementResult sr = executeDml(statement, new ExecuteOptions());
            if (shouldSucceed) {
                assertTrue(sr.isSuccessful());
            } else {
                assertFalse(("Should have failed: " +
                             statement),sr.isSuccessful());
                return;
            }
            if (fieldName != null) {
                Iterator<RecordValue> iter = sr.iterator();
                while (iter.hasNext()) {
                    RecordValue val = iter.next();
                    assertTrue(val.get(fieldName).asInteger().get() == value);
                }
            }
        } catch (IllegalArgumentException iae) {
            assertFalse(iae.getMessage(), shouldSucceed);
        }
    }

    /*
     * Update. This is very simple for now. It can be modified/extended
     * as needed.
     */
    @Test
    public void testUpdate() throws Exception {
        final String createTable =
            "create table noschema(id integer, primary key(id)) " +
            "as json collection";

        executeDdl(createTable);

        String iStatement = "insert into noschema values(1, " +
            "{\"a\": 3}) returning *";
        StatementResult sr = executeDml(iStatement);
        assertTrue(sr.isSuccessful());
        for (RecordValue val : sr) {
            /* todo assert that value is expected */
            assertTrue(val.get("id").asInteger().get() == 1);
        }

        String uStatement =
            "update noschema $n put $n {\"b\":4} where id = 1 returning *";
        sr = executeDml(uStatement);
        for (RecordValue val : sr) {
            /* todo assert that value is expected */
            assertTrue(val.get("b").asInteger().get() == 4);
        }
        assertTrue(sr.isSuccessful());
    }

    @SuppressWarnings("unused")
    @Test
    public void testWriteMultiple() throws Exception {

        final String createTable = "create table noschema(id integer, " +
            "sid integer, primary key(shard(id), sid)) as json collection";
        executeDdl(createTable);
        TableImpl table = getTable("noschema");

        TableOperationFactory factory = tableImpl.getTableOperationFactory();
        List<TableOperation> opList = new ArrayList<TableOperation>();

        for (int i = 0; i < 10; i++) {
            RowImpl row = table.createRow();
            row.put("id", 10);
            row.put("sid", 20 + i);
            row.put("name", "myname");
            row.put("multindex", i);
            opList.add(factory.createPut(row.clone(), null, false));
        }
        List<TableOperationResult> results = tableImpl.execute(opList, null);

        String query = "SELECT * FROM noschema";
        ExecuteOptions options = new ExecuteOptions();
        StatementResult sr = executeDml(query, options);

        int count = 0;
        for (RecordValue val : sr) {
            ++count;
            assertEquals(
                val.get("id").asInteger().get(), 10);
        }
        assertEquals(count, 10);
    }

    @Test
    public void testTypes() {
        final String createTable = "create table noschema(id integer, " +
            "primary key(id)) as json collection";
        executeDdl(createTable);
        TableImpl table = getTable("noschema");
        roundTripTable(table);

        RowImpl row = table.createRow();
        row.put("id", 1);
        /* types that are expected to work */
        FieldValue stringValue = FieldValueFactory.createString("a_string");
        FieldValue intValue = FieldValueFactory.createInteger(5000);
        FieldValue longValue = FieldValueFactory.createLong(5000000L);
        FieldValue doubleValue = FieldValueFactory.createDouble(6.789);
        FieldValue nullValue = FieldValueFactory.createJsonNull();
        FieldValue numberValue = FieldValueFactory.createNumber(10000L);
        FieldValue booleanValue = FieldValueFactory.createBoolean(false);

        row.put("s", stringValue);
        row.put("int", intValue);
        row.put("l", longValue);
        row.put("d", doubleValue);
        row.put("jn", nullValue);
        row.put("b", booleanValue);
        row.put("n", numberValue);

        assertNotNull(tableImpl.put(row, null, null));

        PrimaryKey pkey = row.createPrimaryKey();
        RowImpl newRow = (RowImpl) tableImpl.get(pkey, null);
        /* did the row round-trip correctly? */
        assertEquals(row, newRow);

        testBadTypes(row);
    }

    private void testBadTypes(RowImpl row) {

        FieldValue bad = FieldValueFactory.createBinary(new byte[4]);
        badType(row, bad);

        bad = FieldValueFactory.createTimestamp(1956, 7, 8, 10, 45, 5, 0, 0);
        badType(row, bad);

        bad = new ArrayDefImpl(FieldDefImpl.Constants.longDef).createArray();
        badType(row, bad);

        RecordDefImpl recordDef =
            (RecordDefImpl) TableBuilder.createRecordBuilder("rec")
            .addString("name")
            .addInteger("age")
            .build();
        bad = recordDef.createRecord();
        badType(row, bad);

        MapDefImpl md = (MapDefImpl) TableBuilder.createMapBuilder()
            .addLong().build();
        bad = md.createMap();
        badType(row, bad);

        EnumDefImpl edi = new EnumDefImpl(new String[]{"a", "b"}, null);
        bad = edi.createEnum(1);
        badType(row, bad);

        /* test (illegal) put methods of directly not supported types:
         *  Timestamp
         *  byte[]
         */
        try {
            row.put("foo", new Timestamp(12345L));
            fail("Attempt to put Timestamp should have failed");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().toLowerCase().contains(
                           "cannot insert a field of type timestamp"));
        }

        try {
            row.put("foo", new byte[4]);
            fail("Attempt to put byte[] should have failed");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().toLowerCase().contains(
                           "cannot insert a field of type byte[]"));
        }

        try {
            row.putEnum("foo", "day");
            fail("Attempt to put enum should have failed");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().toLowerCase().contains(
                           "cannot insert a field of type enum"));
        }
    }

    private void badType(Row row, FieldValue badType) {
        /*
         * Type is validated in Row put. Query inserts into JSON collection
         * tables must use JSON in the first place so type mismatches are not
         * an issue
         */
        try {
            row.put("bad", badType);
            fail("Put should have failed: " + badType);
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().toLowerCase().
                       contains("type mismatch"));
        }
    }

    @Test
    public void testMRCounter() {
        final String tableName = "noschema_crdt";
        final String createTableCRDT =
            "create table " + tableName + "(id integer, " +
            "primary key(id), crdt as integer mr_counter, " +
            "long_crdt as long mr_counter, num_crdt as number mr_counter)" +
            "as json collection";
        executeDdl(createTableCRDT);
        TableImpl table = getTable(tableName);

        /* make sure that it round-trips via JSON */
        roundTripTable(table);

        for (int i = 0; i < 10; i++) {
            RowImpl row = table.createRow();
            row.put("id", i);
            row.put("crdt", 10 + i);
            row.put("long_crdt", 100L + i);
            row.put("num_crdt", NullJsonValueImpl.getInstance());
            assertNotNull(tableImpl.put(row, null, null));

            /*
             * Try various non-null return rows. This failed at one time.
             */
            Choice[] choices = {Choice.ALL, Choice.VALUE, Choice.VERSION,
                                Choice.NONE};
            for (Choice choice: choices) {
                ReturnRow rr = table.createReturnRow(choice);
                assertNull(tableImpl.putIfAbsent(row, rr, null));
                assertNotNull(tableImpl.put(row, rr, null));
            }
        }

        DDLGenerator ddlGen = new DDLGenerator(table,
                                               tableImpl.getRegionMapper());
        String ddl = ddlGen.getDDL().toLowerCase();
        assertTrue(ddl.contains("crdt") &&
                   ddl.contains("long_crdt") &&
                   ddl.contains("num_crdt") &&
                   ddl.contains("json collection"));

        /* try to create an index on an MR counter, should fail */
        executeDdl("create index idx on " + tableName +
                   "(crdt as integer)", false);
    }

    /*
     * Enum, Timestamp not allowed as a key type
     */
    @Test
    public void testTypesNotAllowed() {
        final String createTable1 = "create table noschema(time Timestamp(6), " +
            "primary key(time)) as json collection";
        final String createTable2 = "create table noschema(day enum(m,t,w), " +
            "primary key(day)) as json collection";
        executeDdl(createTable1, false);
        executeDdl(createTable2, false);
    }

    /*
     * Child tables not yet supported, try variants:
     *  jcoll, jcoll
     *  regular, jcoll
     *  jcoll, regular
     */
    @Test
    public void testChildNotSupported() {
        final String parent = "create table parent(id integer, " +
            "primary key(id))";
        final String jparent = "create table jparent(id integer, " +
            "primary key(id)) as json collection";
        final String child = "create table jparent.child(idc integer, " +
            "primary key(idc))";
        final String jchild = "create table jparent.jchild(idc integer, " +
            "primary key(idc)) as json collection";
        final String jchild1 = "create table parent.jchild(idc integer, " +
            "primary key(idc)) as json collection";

        executeDdl(parent);
        executeDdl(jparent);
        executeDdl(child, false);
        executeDdl(jchild, false);
        executeDdl(jchild1, false);
    }

    /*
     * Timestamp is not yet allowed as a key type
     */
    @Ignore
    public void testTimestampKey() {
        final String createTable = "create table noschema(time Timestamp(6), " +
            "primary key(time)) as json collection";
        executeDdl(createTable);
        TableImpl table = getTable("noschema");
        TimestampValue ts =
            FieldValueFactory.createTimestamp("2016-07-21T14:56:01.12345Z", 6);
        Row row = table.createRow();
        row.put("time", ts);
        row.put("name", "joe");
        assertNotNull(tableImpl.put(row, null, null));
        PrimaryKey pkey = table.createPrimaryKey();
        pkey.put("time", ts);
        row = tableImpl.get(pkey, null);
        assertTrue(row.get("name").asString().get().equals("joe"));

        /* try illegal put of timestamp into non-key */
        try {
            row.put("time_value", ts);
            fail("Put of timestamp into non-key should fail");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testFromJson() {
        final String createTable = "create table noschema(id integer, " +
            "primary key(id)) as json collection";
        executeDdl(createTable);
        TableImpl table = getTable("noschema");
        Row row = table.createRow();

        /*
         * try some cases that should fail
         */

        /* can't putJson to primary key */
        try {
            row.putJson("id", "{}");
            fail("putJson should fail");
        } catch (IllegalArgumentException iae) {}

        /* positional put on key field works */
        row.put(0, 3);
        assertTrue(row.get("id").asInteger().get() == 3);

        /* out of range put is IndexOutOfBoundsException */
        try {
            row.put(1, "abc");
            fail("positional put should fail");
        } catch (IndexOutOfBoundsException ioob) {}

        /* create the row from JSON */
        row = table.createRowFromJson("{\"id\": 6}", true /* exact */);
        assertTrue(row.get("id").asInteger().get() == 6);

        row = table.createRowFromJson("{\"id\": 4, \"name\": \"joe\"}",
                                      true /* exact */);
        assertTrue(row.get("id").asInteger().get() == 4);
        assertTrue(row.get("name").asString().get().equals("joe"));

        PrimaryKey pkey =
            table.createPrimaryKeyFromJson("{\"id\": 4, \"name\": \"joe\"}",
                                           false /* exact */);
        assertTrue(pkey.get("id").asInteger().get() == 4);

        try {
            row = table.createRowFromJson("{\"name\": \"joe\"}",
                                          true /* exact */);
            fail("should fail, missing primary key");
        } catch (IllegalArgumentException iae) {}

        row = table.createRowFromJson("{\"name\": \"joe\"}",
                                      false /* exact */);
        assertTrue(row.get("name").asString().get().equals("joe"));

        try {
            row.putArrayAsJson("xxx", "[]", false);
            fail("putArrayAsJson not supported");
        } catch (IllegalArgumentException iae) {}
        try {
            /* empty string doesn't matter for error */
            row.putMapAsJson("xxx", "", false);
            fail("putMapAsJson not supported");
        } catch (IllegalArgumentException iae) {}
    }

    /*
     * test miscellaneous methods that work and do not
     */
    @Test
    public void testMisc() {
        final String createTable = "create table noschema(id integer, " +
            "primary key(id)) as json collection";
        executeDdl(createTable);
        TableImpl table = getTable("noschema");
        Row row = table.createRow();
        row.put("id", 1);

        /* test createPrimaryKey(Record) */
        PrimaryKey pkey = table.createPrimaryKey(row);
        assertTrue(pkey.get("id").asInteger().get() == 1);

        /* test createRow(Record) -- throws */
        try {
            table.createRow(row);
            fail("Should have thrown");
        } catch (IllegalArgumentException iae) {
        }

        /* test createRowWithDefaults -- throws */
        try {
            row = table.createRowWithDefaults();
            fail("Should have thrown");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * test use of FieldRange, key iterator
         */
        for (int i = 0; i < 20; i++) {
            row = table.createRow();
            row.put("id", i);
            row.put("name", ("name" + i));
            assertNotNull(tableImpl.put(row, null, null));
            /* assert that all fields are returned by thie method */
            assertEquals(2, row.getFieldNames().size());
        }

        FieldRange range = table.createFieldRange("id").setStart(15, false);
        MultiRowOptions mro = range.createMultiRowOptions();
        pkey = table.createPrimaryKey();
        int numInRange = countTableRecords1(pkey, mro);
        assertEquals(4, numInRange);

        /*
         * Test RecordValue methods
         */
        row.put("id", 5);
        row.put("name", "jane");

        Row newRow = table.createRow();
        newRow.put("id", 5);
        newRow.put("name", "jane");

        assertTrue(row.equals(newRow));
        assertEquals(0, row.compareTo(newRow));
        newRow.put("name", "joe");
        assertFalse(row.equals(newRow));
        assertTrue(row.compareTo(newRow) < 0);
        assertTrue(newRow.contains("id"));
        assertTrue(newRow.contains("name"));

        /* reset, test copyFrom */
        newRow = table.createRow();
        newRow.copyFrom(row);
        assertEquals("jane", newRow.get("name").asString().get());


    }
}
