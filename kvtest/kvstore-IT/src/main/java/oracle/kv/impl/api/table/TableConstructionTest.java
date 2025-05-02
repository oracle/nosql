/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static oracle.kv.impl.api.table.TableTestBase.makeIndexTypeList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;

import org.junit.Test;

public class TableConstructionTest extends TestBase {

    /*
     * Use members to allow more sharing of tables among test cases
     */
    TableImpl userTable;
    TableImpl addressTable;
    TableImpl tableWithNullable;

    @Test
    public void simpleTable() {

        /*
         * Top-level table
         */
        createUserTable();
        String jsonString = userTable.toJsonString(true);
        TableImpl newTable =
            TableBuilder.fromJsonString(jsonString, null);
        assertTrue(userTable.equals(newTable));

        /*
         * Nested table
         */
        createAddressTable(userTable);
        jsonString = addressTable.toJsonString(true);
        newTable = TableBuilder.fromJsonString(jsonString, userTable);
        assertTrue(addressTable.equals(newTable));
    }

    @Test
    public void complexTable() {
        RecordDefImpl nestedRecord =
            (RecordDefImpl) TableBuilder.createRecordBuilder("nested_record")
            .addString("name")
            .addInteger("age", "this_is_age", null, 12)
            .build();

        TableImpl complexTable =
            TableBuilder.createTableBuilder("Complex")
            .addInteger("id")
            .addEnum("workday",
                     new String[]{"monday", "tuesday", "wednesday",
                                  "thursday", "friday"}, null, null, null)
            .addField("likes", TableBuilder.createArrayBuilder()
                      .addString().build())
            .addField("this_is_a_map", TableBuilder.createMapBuilder()
                      .addInteger().build())
            .addField("nested_record", nestedRecord)
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        complexTable.addIndex
            (new IndexImpl("testIndex", complexTable,
                           makeIndexList("workday", "likes[]"),
                           null,
                           "this is a test"));

        /*
         * Test a couple of interfaces on complex objects.
         */
        Row row = complexTable.createRow();
        RecordValue record = row.putRecord("nested_record");
        MapValue map = row.putMap("this_is_a_map");
        ArrayValue array = row.putArray("likes");
        assertTrue(record.getFieldNames().size() ==
                   nestedRecord.getFieldNames().size());
        assertTrue(record.getFieldNames().equals(
                       nestedRecord.getFieldNames()));
        assertTrue(map.getFields().isEmpty());
        map.put("f1", 1);
        assertTrue(map.getFields().size() == 1);
        assertTrue(array.toList().isEmpty());

        /*
         * Try some illegal operations on immutable fields.
         */
        List<String> l = record.getFieldNames();
        try {
            l.add("x");
            fail("Add should have failed; list is immutable");
        } catch (UnsupportedOperationException e) {}

        Map<String, FieldValue> mapFields = map.getFields();
        try {
            mapFields.put("x", null);
            fail("Put should have failed; map is immutable");
        } catch (UnsupportedOperationException e) {}

        l = complexTable.getPrimaryKey();
        try {
            l.add("x");
            fail("Add should have failed; list is immutable");
        } catch (UnsupportedOperationException e) {}

        l = complexTable.getShardKey();
        try {
            l.add("x");
            fail("Add should have failed; list is immutable");
        } catch (UnsupportedOperationException e) {}

        List<FieldValue> al = array.toList();
        try {
            al.add(null);
            fail("Add should have failed; list is immutable");
        } catch (UnsupportedOperationException e) {}
    }

    /*
     * Test good and bad PrimaryKey and IndexKey operations. Bad operations
     * are attempts to put values that do not belong. This test is very simple
     * at this time. It can be expanded to include more complex testing if
     * needed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testKeys() throws Exception {
        TableImpl table =
            TableBuilder.createTableBuilder("keytable")
            .addInteger("id")
            .addString("name")
            .addInteger("age")
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        table.addIndex
            (new IndexImpl("testIndex", table,
                           makeIndexList("age"), null, null));

        PrimaryKey pkey = table.createPrimaryKey();
        pkey.put("id", 1);
        try {
            pkey.put("age", 4);
            fail("Add should have failed, age is not a key field");
        } catch (IllegalArgumentException iae) {
        }

        Index index = table.getIndex("testIndex");
        IndexKey ikey = index.createIndexKey();
        ikey.put("age", 4);
        try {
            ikey.put("id", 3);
            fail("Add should have failed, id is not indexed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * test some construction, assert that only key fields are copied.
         */
        Row row = table.createRow();
        row.put("id", 1);
        row.put("age", 3);
        row.put("name", "joe");

        pkey = table.createPrimaryKey(row);
        assertTrue(pkey.get("id").asInteger().get() == 1);
        TableTestBase.assertFieldAbsent(pkey, "age");

        ikey = index.createIndexKey(row);
        assertTrue(ikey.get("age").asInteger().get() == 3);
        TableTestBase.assertFieldAbsent(ikey, "name");

        /*
         * Verify that these throw IAE
         */
        try {
            table.createRow(ikey);
            fail("createRow should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        try {
            table.createPrimaryKey(ikey);
            fail("createPrimaryKey should have thrown");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void evolve() {

        /*
         * Top-level table
         */
        createUserTable();
        TableEvolver evolver = TableEvolver.createTableEvolver(userTable.clone());
        evolver.removeField("age");
        assertEquals("should be 1 version",
                     evolver.getTable().numTableVersions(), 1);
        evolver.evolveTable();
        assertEquals("should be 2 versions",
                     evolver.getTable().numTableVersions(), 2);

        /*
         * This will fail.
         */
        try {
            evolver.removeField("id");
            fail("Remove should have failed");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Rules for names:
     * 1.  alphanumeric + "_" only
     * 2.  must start with alphabetic
     * 3.  table name limit is 256 chars
     * 4.  field name limit is 64 chars
     */
    @Test
    public void testValidNames() {

        TableBuilder builder;
        String s64 = makeName(64, "A64CharString");
        String s65 = makeName(65, "A65CharString");
        String s256 = makeName(256, "A256CharString");
        String s257 = makeName(257, "A257CharString");

        /*
         * Build a table with legitimate characters.
         */
        TableBuilder.createTableBuilder("Foo_table")
            .addInteger("i8d")
            .addString("first_name")
            .addString("last_name")
            .addInteger("age_")
            .addBinary("nolimit7", null)
            .addFixedBinary("limited", 75)
            .primaryKey("i8d")
            .buildTable();

        /*
         * Use a bunch of invalid strings
         */
        try {
            builder = TableBuilder.createTableBuilder("Foo-table");
            fail("Bad character ('-') in table name");
        } catch (IllegalCommandException ice) {
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Use a bunch of invalid strings
         */
        try {
            builder = TableBuilder.createTableBuilder("Foo.table");
            fail("Bad character ('.') in table name");
        } catch (IllegalCommandException ice) {
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder =
                TableBuilder.createTableBuilder("7oo");
            fail("Bad character (start with '7') in table name");
        } catch (IllegalCommandException ice) {
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Use strings at limit of valid length
         */
        builder =
            TableBuilder.createTableBuilder(s256);
        builder.addInteger(s64);

        /*
         * Now, just over the limit
         */
        try {
            builder.addInteger(s65);
            fail("Field name too long (> 64)");
        } catch (IllegalCommandException ice) {
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder = TableBuilder.createTableBuilder(s257);
            fail("Table name too long (> 256)");
        } catch (IllegalCommandException ice) {
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder.addInteger("bad.name");
            fail("Field name contains dot");
        } catch (IllegalCommandException ice) {
        } catch (IllegalArgumentException iae) {
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void badFields() {

        /* illegal primary key */
        try {
            TableImpl complexTable =
                TableBuilder.createTableBuilder("Bad1",
                                                "Can't add complex to key",
                                                null)
                .addInteger("id")
                .addEnum("workday",
                         new String[]{"monday", "tuesday", "wednesday",
                                      "thursday", "friday"}, null, null, null)
                .addField("likes", TableBuilder.createArrayBuilder()
                          .addString().build())
                .addField("this_is_a_map", TableBuilder.createMapBuilder()
                          .addInteger().build())
                .primaryKey("this_is_a_map")
                .shardKey("this_is_a_map")
                .buildTable();
            fail("Table build should have failed");
        } catch (IllegalCommandException ice) {
            /* success */
        } catch (IllegalArgumentException iae) {
        }

        /* illegal major key */
        try {
            TableImpl complexTable =
                TableBuilder.createTableBuilder("Bad1",
                                                "Can't add complex to key",
                                                null)
                .addInteger("id")
                .addEnum("workday",
                         new String[]{"monday", "tuesday", "wednesday",
                                      "thursday", "friday"},
                         null, null, null)
                .addField("likes", TableBuilder.createArrayBuilder()
                          .addString().build())
                .addField("this_is_a_map", TableBuilder.createMapBuilder()
                          .addInteger().build())
                .primaryKey("id")
                .shardKey("workday")
                .buildTable();
            fail("Table build should have failed");
        } catch (IllegalCommandException ice) {
            /* success */
        } catch (IllegalArgumentException iae) {
        }

    }

    /*
     * Make sure that fields appear in declaration order in the external view.
     * This means parsing the JSON and verifying order.
     */
    @Test
    public void testFieldOrder() {
        String[] fields = {"z", "a", "x", "y"};
        TableBuilder builder =
            TableBuilder.createTableBuilder("Order");
        for (String field : fields) {
            builder.addInteger(field);
        }
        builder.primaryKey(fields[0]);
        builder.shardKey(fields[0]);
        TableImpl table = builder.buildTable();
        /*
         * Compare field order directly
         */
        compareOrder(table.getFields(), fields);

        /*
         * to/from JSON and compare tables, which also compares order.  This
         * tests the JSON because fields are added in order from the JSON.
         */
        String json = table.toJsonString(false);
        TableImpl newTable = TableJsonUtils.fromJsonString(json, null);
        assertTrue(table.equals(newTable));
        assertTrue(JsonUtils.jsonStringsEqual(json,
                                              newTable.toJsonString(false)));

        /*
         * Test order for a Record which has a slightly different
         * implementation than TableImpl.
         */
        RecordBuilder recordBuilder =
            TableBuilder.createRecordBuilder("record");
        for (String field : fields) {
            recordBuilder.addInteger(field);
        }
        RecordDefImpl record = (RecordDefImpl) recordBuilder.build();
        compareOrder(record.getFieldNames(), fields);
    }

    /**
     * Test field removal from builder.
     */
    @Test
    public void testRemove() {
        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("a")
            .addInteger("b")
            .addInteger("c")
            .primaryKey("a");
        assertEquals("Unexpected size", 3, builder.size());
        builder.removeField("b");
        assertEquals("Unexpected size", 2, builder.size());
        try {
            builder.removeField("x");
            fail("remove should have failed");
        } catch (IllegalArgumentException iae) {
        }
        builder.buildTable();
    }

    @Test
    public void testPrimaryKeySize() {

        int keyLen1 = createConstrainedTable(0);
        int keyLen2 = createConstrainedTable(2);

        /* keyLen1 is unconstrained so it should be larger */
        assertTrue(keyLen1 > keyLen2);

        /*
         * Try some things that should fail
         */
        try {
            TableBuilder builder = TableBuilder.createTableBuilder("foo");
            builder.addFloat("id").
                primaryKey("id").
                primaryKeySize("id", 3);
            fail("Table build should have failed");
        } catch (IllegalArgumentException iae) {
            /* ok */
        }
    }

    /* Test TableKey.getKeySize(boolean skipTableId) method */
    @Test
    public void testKeySizeSkipTableId() {
        TableImpl tableA = TableBuilder.createTableBuilder("A")
                .addString("ida")
                .addString("sa")
                .primaryKey("ida")
                .buildTable();
        TableImpl tableB =
            TableBuilder.createTableBuilder("B", null, tableA)
                .addString("idb")
                .addString("sb")
                .primaryKey("idb")
                .buildTable();
        TableImpl tableC =
            TableBuilder.createTableBuilder("C", null, tableB)
                .addString("idc1")
                .addString("idc2")
                .addString("sc")
                .primaryKey("idc1", "idc2")
                .buildTable();

        RowImpl row = tableC.createRow();
        row.put("ida", "ka")
           .put("idb", "kb")
           .put("idc1", "kc1")
           .put("idc2", "kc2")
           .put("sc", "xyz");

        TableKey key;

        key = TableKey.createKey(tableC, row, false);
        assertEquals(10, key.getKeySize(true));
        assertEquals(tableA.getIdString().length() +
                     tableB.getIdString().length() +
                     tableC.getIdString().length() + 10,
                     key.getKeySize(false));

        row = tableB.createRow();
        row.put("ida", "ka")
           .put("idb", "kb")
           .put("sb", "xyz");
        key = TableKey.createKey(tableB, row, false);
        assertEquals(4, key.getKeySize(true));
        assertEquals(tableA.getIdString().length() +
                     tableB.getIdString().length() + 4,
                     key.getKeySize(false));

        row = tableA.createRow();
        row.put("ida", "ka")
           .put("sa", "xyz");
        key = TableKey.createKey(tableA, row, false);
        assertEquals(2, key.getKeySize(true));
        assertEquals(tableA.getIdString().length() + 2, key.getKeySize(false));
    }

    /**
     * This is a simple test that just generates code coverage for the
     * Try some valid, and invalid types in primary keys.
     */
    @Test
    public void testPrimaryKey() {
        TableImpl table = TableBuilder.createTableBuilder("foo")
        .addInteger("a")
        .addString("b")
        .addFloat("c")
        .primaryKey("a", "b", "c")
        .buildTable();

        PrimaryKey key = table.createPrimaryKey();

        /* valid fields */
        key.put("a", 1);
        key.put("b", "c");
        key.put("c", 2F);

        /* bad field type */
        FieldValue val = FieldValueFactory.createLong(4);
        badKey(key, "a", val);

        /* bad field name */
        val = FieldValueFactory.createInteger(4);
        badKey(key, "d", val);
    }

    private void badKey(PrimaryKey key, String name, FieldValue value) {
        try {
            key.put(name, value);
            fail("Key field should have failed: " + name + ", value: " + value);
        } catch (IllegalArgumentException iae) {}
    }

    /**
     * DDLGenerator class that generates DDL from a Table. This class is
     * tested in the admin package but this simple test ensures that all
     * of the types supported are exercised.
     */
    @Test
    public void testDDLGenerator() {

        /* Create a table that has all types */
        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        builder.addField("likes", TableBuilder.createArrayBuilder()
                         .addField(TableBuilder.
                                   createRecordBuilder("nested_record")
                                   .addString("name").build())
                         .build())
            .addInteger("i")
            .addInteger("j")
            .primaryKey("i", "j")
            .primaryKeySize("i", 2) // restrict size on "i"
            .shardKey("i")
            .addLong("l")
            .addDouble("d")
            .addFloat("f")
            .addBoolean("bool")
            .addBinary("by")
            .addFixedBinary("fb", 4)
            .addField("map",
                      TableBuilder.createMapBuilder().addString().build())
            /* add a not-nullable field with a default and description */
            .addInteger("i1", "description", false, 1)
            .addTimestamp("ts", 3, "timestmap type with percision of 3",
                          true, null)
            .addNumber("decimal", "decimal type", false, new BigDecimal("1E1"))
            .setDefaultTTL(TimeToLive.ofHours(5));
        TableImpl table = builder.buildTable();

        table.addIndex
            (new IndexImpl("testIndex", table,
                           makeIndexList("d", "f"),
                           null,
                           "this is a test"));
        DDLGenerator ddlGen = new DDLGenerator(table, null);

        assertNotNull(ddlGen.getDDL());
        assertTrue(ddlGen.getAllIndexDDL().size() == 1);
        /*
         * Could parse the DDL and compare the table but the admin's
         * DdlSyntaxTest does that.
         */

        /*
         * For code coverage
         */

        TableImpl newTable =
            TableJsonUtils.fromJsonString(table.toJsonString(true), null);
        assertTrue(table.equals(newTable));
        assertTrue(table.fieldsEqual(newTable));

        List<String> path1 = new ArrayList<String>(1);
        path1.add("i");
        List<String> path2 = new ArrayList<String>(1);
        path2.add("l");
        List<String> path3 = new ArrayList<String>(1);
        path3.add("fb");
        List<String> path4 = new ArrayList<String>(1);
        path4.add("likes");
        List<String> path5 = new ArrayList<String>(2);
        path5.add("likes");
        path5.add("[]");
        path5.add("name");
        List<String> path6 = new ArrayList<String>(1);
        path6.add("map");
        List<String> path7 = new ArrayList<String>(1);
        path7.add("ts");
        List<List<String>> paths = new ArrayList<List<String>>(6);
        paths.add(path1);
        paths.add(path2);
        paths.add(path3);
        paths.add(path4);
        paths.add(path5);
        paths.add(path6);
        paths.add(path7);
        table.formatTable(true, paths, null);

        try {
            List<String> path8 = new ArrayList<String>(1);
            path8.add("not_a_field");
            paths.clear();
            paths.add(path8);

            table.formatTable(true, paths, null);
            fail("should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        table.toString();
    }

    @Test
    public void testDDLGeneratorUUID() {
        final FieldDef uuidDef =
            new StringDefImpl(null /* description */,
                              true /* isUUID */,
                              false /* generatedByDefault */);
        final FieldDef uuidGenByDef =
            new StringDefImpl(null /* description */,
                              true /* isUUID */,
                              true /* generatedByDefault */);
        TableImpl table;
        DDLGenerator ddlGen;
        String ddl;

        table = TableBuilder.createTableBuilder("foo")
                    .addField("id", uuidGenByDef)
                    .addString("name")
                    .addField("token", uuidDef)
                    .primaryKey("id")
                    .buildTable();

        ddlGen = new DDLGenerator(table, null);
        ddl = ddlGen.getDDL();
        assertTrue(ddl.contains("id STRING AS UUID GENERATED BY DEFAULT"));
        assertTrue(ddl.contains("token STRING AS UUID"));

        ddlGen = new DDLGenerator(table.toJsonString(false));
        ddl = ddlGen.getDDL();
        assertTrue(ddl.contains("id STRING AS UUID GENERATED BY DEFAULT"));
        assertTrue(ddl.contains("token STRING AS UUID"));
    }

    @Test
    public void testDDLGeneratorFromJson() {
        final String expected =
            "CREATE TABLE foo (i INTEGER, j INTEGER, PRIMARY KEY(SHARD(i, j)))";

        final String table1 = "{" +
            /*"\"json_version\" : 1," + */
            "\"name\" : \"foo\"," +
            "\"primaryKey\" : [ \"i\", \"j\"]," +
            "\"fields\" : [ " +
            "  {\"name\": \"i\", \"type\": \"integer\"}," +
            "  {\"name\": \"j\", \"type\": \"integer\"}]" +
            "}";
        DDLGenerator ddlGen = new DDLGenerator(table1);
        assertTrue(expected.equalsIgnoreCase(ddlGen.getDDL().trim()));
    }

    /*
     * Create a table with an index on a record where at least one of the
     * fields is not nullable.
     */
    @Test
    public void testIndexNull() {
        TableBuilder builder = TableBuilder.createTableBuilder("testnull");
        builder.addField("address",
                         TableBuilder.createRecordBuilder("address")
                         .addString("city", null, false, "ME") // not nullable
                         .addString("state") // nullable
                         .build())
            .addInteger("i")
            .primaryKey("i");
        TableImpl table = builder.buildTable();
        table.addIndex
            (new IndexImpl("aindex", table,
                           makeIndexList("address.city"),
                           null, null));
        IndexImpl index = (IndexImpl) table.getIndex("aindex");
        IndexKeyImpl ikey = index.createIndexKey();
        ikey.putNull("address.city");

        byte[] ibytes = index.serializeIndexKey(ikey);
        RecordValueImpl row = index.getIndexEntryDef().createRecord();
        index.rowFromIndexEntry(row, null, ibytes);
    }

    @Test
    public void testTimestampFields() {

        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        Timestamp ts = new Timestamp(0);
        ts.setNanos(987654321);
        TableImpl table = builder.addTimestamp("ts0", 0)
            .addTimestamp("ts1", 1, "timestamp with percision 1", true, ts)
            .addTimestamp("ts2", 3, null, false, ts)
            .addTimestamp("ts3", 6, null, false, ts)
            .addTimestamp("ts4", 9, null, false, ts)
            .addInteger("i")
            .shardKey("i")
            .primaryKey("i", "ts3")
            .buildTable();

        table.addIndex(new IndexImpl("testIndex", table,
                                     makeIndexList("ts0", "ts1", "ts2", "ts3"),
                                     null, true, false,
                                     "this is a test"));
        String jsonString = table.toJsonString(true);
        TableImpl newTable = TableBuilder.fromJsonString(jsonString, null);
        assertTrue(table.equals(newTable));
    }

    @Test
    public void testNumberFields() {
        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        TableImpl table = builder.addTimestamp("ts0", 0)
            .addNumber("dec1")
            .addNumber("dec2", "decimal value 2", false,
                        new BigDecimal("-1.1E100"))
            .addNumber("dec3", "decimal value 3", true,
                        BigDecimal.ZERO)
            .addField("array",
                      TableBuilder.createArrayBuilder().addNumber().build())
            .addInteger("i")
            .shardKey("i")
            .primaryKey("i", "dec1")
            .buildTable();

        table.addIndex(new IndexImpl("testIndex", table,
                                     makeIndexList("dec2", "array[]"),
                                     null, true, false,
                                     "this is a test"));
        String jsonString = table.toJsonString(true);
        TableImpl newTable = TableBuilder.fromJsonString(jsonString, null);
        assertTrue(table.equals(newTable));
    }

    /*
     * TODO: Enable the below test once the generic JSON index is supported
     * Add some JSON indexes to a table and make sure that it round-trips
     * to/from JSON string output.
     */
    //@Test
    public void testJsonIndex() {
        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        TableImpl table = builder.addJson("json", null)
            .addInteger("i")
            .primaryKey("i")
            .buildTable();

        table.addIndex(new IndexImpl("jsonIndex", table,
                                     makeIndexList("json.name"),
                                     null, true, false, null));
        table.addIndex(new IndexImpl("jsonIndex1", table,
                                     makeIndexList("json.name1"),
                                     makeIndexTypeList(FieldDef.Type.STRING),
                                     true, false,
                                     null));
        table.addIndex(new IndexImpl("jsonIndex2", table,
                                     makeIndexList("json.a", "json.name1"),
                                     makeIndexTypeList(null,
                                                       FieldDef.Type.STRING),
                                     true, false,
                                     null));
        table.addIndex(new IndexImpl("jsonIndex3", table,
                                     makeIndexList("json.a", "i",
                                                   "json.name1", "json.x"),
                                     makeIndexTypeList(FieldDef.Type.STRING,
                                                       null, null,
                                                       FieldDef.Type.LONG),
                                     true, false,
                                     null));
        String jsonString = table.toJsonString(true);
        TableImpl newTable = TableBuilder.fromJsonString(jsonString, null);
        assertTrue(table.equals(newTable));
    }

    @Test
    public void testJsonTypedIndex() {
        final FieldDef.Type[] types = new FieldDef.Type[]{
            FieldDef.Type.INTEGER,
            FieldDef.Type.LONG,
            FieldDef.Type.DOUBLE,
            FieldDef.Type.NUMBER,
            FieldDef.Type.STRING,
            FieldDef.Type.BOOLEAN
        };

        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        TableImpl table = builder.addJson("json", null)
            .addInteger("i")
            .primaryKey("i")
            .buildTable();

        for (FieldDef.Type type : types) {
            String idxName = "jsonIndex" + type.name().toLowerCase();
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList("json"),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);
            table.removeIndex(idxName);
        }

        for (FieldDef.Type type : types) {
            String idxName = "jsonIndex" + type.name().toLowerCase();
            String fieldName = "json." + type.name().toLowerCase();
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList(fieldName),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);
        }

        String idxName = "jsonIndexComp";
        List<String> fields = new ArrayList<String>();
        for (FieldDef.Type type : types) {
            fields.add("json.rec." + type.name().toLowerCase());
        }
        table.addIndex(new IndexImpl(idxName, table, fields,
                                     makeIndexTypeList(types),
                                     true,
                                     false,
                                     null));
        assertTrue(table.getIndex(idxName) != null);

        /* Test invalid types */
        final FieldDef.Type[] invalidTypes = new FieldDef.Type[]{
            FieldDef.Type.FLOAT,
            FieldDef.Type.TIMESTAMP,
            FieldDef.Type.ENUM,
            FieldDef.Type.BINARY,
            FieldDef.Type.FIXED_BINARY
        };
        for (FieldDef.Type type : invalidTypes) {
            idxName = "jsonIndex" + type.name().toLowerCase();
            try {
                table.addIndex(new IndexImpl(idxName, table,
                                             makeIndexList("json"),
                                             makeIndexTypeList(type),
                                             true,
                                             false,
                                             null));
                fail("Except to catch IAE but not when add index " + idxName +
                     " on json as " + type.name());
            } catch (IllegalArgumentException iae) {
            }
        }

        /* Array(JSON), Map(JSON), Record(json JSON) */
        builder = TableBuilder.createTableBuilder("tabComplex");
        table = builder.addField("a", TableBuilder.createArrayBuilder("a")
                                                  .addJson()
                                                  .build())
                .addField("m", TableBuilder.createArrayBuilder("m")
                                           .addJson()
                                           .build())
                .addField("r", TableBuilder.createRecordBuilder("r")
                                           .addInteger("id")
                                           .addJson("json", null)
                                           .build())
                .addInteger("i")
                .primaryKey("i")
                .buildTable();

        for (FieldDef.Type type : types) {
            idxName = "jsonArrayIndex" + type.name().toLowerCase();
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList("a[]"),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);
            table.removeIndex(idxName);
        }

        for (FieldDef.Type type : types) {
            idxName = "jsonMapIndex" + type.name().toLowerCase();
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList("m[]"),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);
            table.removeIndex(idxName);
        }

        for (FieldDef.Type type : types) {
            idxName = "jsonRecordJsonIndex" + type.name().toLowerCase();
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList("r.id", "r.json"),
                                         makeIndexTypeList(null, type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);
            table.removeIndex(idxName);
        }

        for (FieldDef.Type type : types) {
            String tname = type.name().toLowerCase();
            idxName = "jsonArrayIndex" + tname;
            String field = "a[]." + tname;
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList(field),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);

            idxName = "jsonMapIndex" + tname;
            field = "m[]." + tname;
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList(field),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);

            idxName = "jsonRecordIndex" + tname;
            field = "r.json." + tname;
            table.addIndex(new IndexImpl(idxName, table,
                                         makeIndexList(field),
                                         makeIndexTypeList(type),
                                         true,
                                         false,
                                         null));
            assertTrue(table.getIndex(idxName) != null);
        }

        table.addIndex(new IndexImpl("idxMisc1", table,
                                     makeIndexList("a[].long",
                                                   "a[].string",
                                                   "r.json.boolean"),
                                     makeIndexTypeList(FieldDef.Type.LONG,
                                                       FieldDef.Type.STRING,
                                                       FieldDef.Type.BOOLEAN),
                                     true,
                                     false,
                                     null));
        assertTrue(table.getIndex("idxMisc1") != null);

        table.addIndex(new IndexImpl("idxMisc2", table,
                                     makeIndexList("m[].double",
                                                   "m[].boolean",
                                                   "r.json.long",
                                                   "r.id"),
                                     makeIndexTypeList(FieldDef.Type.LONG,
                                                       FieldDef.Type.STRING,
                                                       FieldDef.Type.BOOLEAN,
                                                       null),
                                     true,
                                     false,
                                     null));
        assertTrue(table.getIndex("idxMisc2") != null);
    }

    /*
     * Test partial builds. The CLI uses show() which uses this path.
     * Call the builder's toJsonString() method along the way.
     */
    @Test
    public void testPartialBuild() {

        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        builder.toJsonString(true);
        builder.addInteger("id");
        builder.toJsonString(true);
        builder.primaryKey("id");
        builder.toJsonString(true);
        builder.addString("name");
        builder.toJsonString(true);
        assertNotNull(builder.buildTable());
    }

    /*
     * Test system table construction. Try some invalid and valid names
     */
    @Test
    public void testSystemTables() {

        validateSysTableName("foo", true);
        validateSysTableName("foo$foo", true);
        validateSysTableName("SYS$foo$bar", true);
        validateSysTableName("SYS$foo$bar", true);
        validateSysTableName("SYS$foo", false);
        validateSysTableName("SYS$foo.x", true);
        validateSysTableName("SYS$foo_x", false);
    }

    private void validateSysTableName(String name, boolean shouldFail) {
        try {
            TableBuilder.createSystemTableBuilder(name);
            if (shouldFail) {
                fail("Build of table should have failed");
            }
        } catch (IllegalCommandException ice) {
            if (!shouldFail) {
                fail("Build of table should have succeeded: " + ice);
            }
        } catch (IllegalArgumentException iae) {
            if (!shouldFail) {
                fail("Build of table should have succeeded: " + iae);
            }
        }
    }

    private int createConstrainedTable(int sizeLimit) {
        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id").
            primaryKey("id");
        if (sizeLimit > 0) {
            builder.primaryKeySize("id", sizeLimit);
        }
        TableImpl table = builder.buildTable();
        PrimaryKey key = table.createPrimaryKey();
        key.put("id", 1);
        if (sizeLimit > 0) {
            try {
                key.put("id", 65536);
                fail("Put of 65536 should have failed");
            } catch (IllegalArgumentException iae) {
                /* ok */
            }
        }

        TableKey tkey = TableKey.createKey(table, key, false);
        return tkey.getKey().toString().length();
    }

    /*
     * Test utilities
     */

    /*
     * Verify that the list and array match in size and values.
     */
    private void compareOrder(List<String> order, String[] fields) {
        assertTrue(order.size() == fields.length);
        for (int i = 0; i < fields.length; i++) {
            assertTrue(order.get(i).equals(fields[i]));
        }
    }

    /*
     * This table includes 2 binary fields, one with a limit and one without.
     * This will test the ability to generate both "fixed" and "byte"
     * types from BinaryDef.
     */
    private void createUserTable() {
        userTable = TableBuilder.createTableBuilder("User")
            .addInteger("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addBinary("nolimit", null)
            .addFixedBinary("limited", 75)
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
    }

    private void createAddressTable(Table parent) {
        addressTable =
            TableBuilder.createTableBuilder("Address",
                                            "Table of addresses for users",
                                            parent)
            .addEnum("type",
                     new String[]{"home", "work", "other"}, null, null, null)
            .addString("street")
            .addString("city")
            .addString("state")
            .addInteger("zip")
            .primaryKey("type")
            .buildTable();
    }

    private String makeName(int len, String start) {
        byte[] bytes = Arrays.copyOf(start.getBytes(), len);
        for (int i = start.length(); i < len; i++) {
            bytes[i] = (byte) 'x';
        }
        return new String(bytes);
    }

    @Test
    public void testTableImplSerialVersion() {
        // Basic atomic types
        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");
        builder.addBinary("fBin");
        builder.addBoolean("fBool");
        builder.addDouble("fDouble");
        builder.addEnum("fEnum", new String[] {"a", "b"}, null);
        builder.addFixedBinary("fFixedBin", 5);
        builder.addInteger("fInt");
        builder.addFloat("fFloat");
        builder.addLong("fLong");
        builder.addString("fString");
        TableImpl t = builder.buildTable();
        assertTrue(t.getRequiredSerialVersion() ==
            SerialVersion.MINIMUM);

        // Array, Map, Record of basic types
        builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");
        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addInteger();
        ArrayDef ad = ab.build();
        builder.addField("fArrayInt", ad);
        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addLong();
        MapDef md = mb.build();
        builder.addField("fMapLong", md);
        RecordBuilder rb = TableBuilder.createRecordBuilder("rec");
        rb.addFloat("fRecFloat");
        rb.addDouble("fRecDouble");
        RecordDef rd = rb.build();
        builder.addField("fRecFD", rd);
        t = builder.buildTable();
        assertTrue(t.getRequiredSerialVersion() ==
            SerialVersion.MINIMUM);

        // Json
        builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");
        builder.addJson("fJson", null);
        t = builder.buildTable();
        assertTrue(t.getRequiredSerialVersion() ==
            SerialVersion.MINIMUM);

        // Array of Json
        builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");
        ab = TableBuilder.createArrayBuilder();
        ab.addJson();
        ad = ab.build();
        builder.addField("fArrayJson", ad);
        t = builder.buildTable();
        assertTrue(t.getRequiredSerialVersion() ==
            SerialVersion.MINIMUM);

        // Number, Timestamp
        builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");
        builder.addNumber("fNumber");
        builder.addTimestamp("fTs", 3);
        t = builder.buildTable();
        assertTrue(t.getRequiredSerialVersion() ==
            SerialVersion.MINIMUM);

        // Map, Record of Number, Timestamp
        builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");
        mb = TableBuilder.createMapBuilder();
        mb.addNumber();
        md = mb.build();
        builder.addField("fMapNumber", md);
        rb = TableBuilder.createRecordBuilder("rec");
        rb.addNumber("fRecNumber");
        rb.addTimestamp("fRecTs", 5);
        rd = rb.build();
        builder.addField("fRecNoTs", rd);
        t = builder.buildTable();
        assertTrue(t.getRequiredSerialVersion() ==
            SerialVersion.MINIMUM);

        // Indexes
        builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.addString("fString");
        mb = TableBuilder.createMapBuilder();
        mb.addLong();
        md = mb.build();
        builder.addField("fMapLong", md);
        ab = TableBuilder.createArrayBuilder();
        ab.addString();
        ad = ab.build();
        builder.addField("fArrayString", ad);
        builder.primaryKey("id");
        t = builder.buildTable();
        assertEquals(SerialVersion.MINIMUM, t.getRequiredSerialVersion());

        /* index idxFString on (fString) */
        Index idx = new IndexImpl("idxFString",
                                  t,
                                  Arrays.asList("fString"),
                                  null, null);
        t.addIndex(idx);
        assertTrue(t.getRequiredSerialVersion() == SerialVersion.MINIMUM);

        /* index idxFJsonSkipNulls on (fMapLong.keys()) with no nulls */
        idx = new IndexImpl("idxFJsonSkipNulls",
                            t,
                            Arrays.asList("fMapLong.keys()"),
                            null,
                            false,  /* indexNulls */
                            false,  /* isUnique */
                            null);
        t.addIndex(idx);

        /* index idxFArrayString on (fArrayString[]) with unique keys per row */
        idx = new IndexImpl("idxFArrayString",
                            t,
                            Arrays.asList("fArrayString[]"),
                            null,
                            true,   /* indexNulls */
                            true,   /* isUnique */
                            null);
        t.addIndex(idx);
        assertEquals(SerialVersion.MINIMUM,
                     t.getRequiredSerialVersion());
    }

    /*
     * Verify that attempts to set default values for complex types fails.
     */
    @Test
    public void testBadDefaults() throws Exception {
        /* create some defaults */
        MapBuilder mb = TableBuilder.createMapBuilder();
        mb.addLong();
        MapDef md = mb.build();
        MapValue map = md.createMap();

        ArrayBuilder ab = TableBuilder.createArrayBuilder();
        ab.addInteger();
        ArrayDef ad = ab.build();
        ArrayValue array = ad.createArray();

        RecordBuilder rb = TableBuilder.createRecordBuilder("rec");
        rb.addFloat("fRecFloat");
        RecordDef rd = rb.build();
        RecordValue rec = rd.createRecord();

        TableBuilder builder = TableBuilder.createTableBuilder("foo");
        builder.addInteger("id");
        builder.primaryKey("id");

        FieldValueImpl iv =
            (FieldValueImpl) FieldDefImpl.Constants.jsonDef.createInteger(5);

        try {
            builder.addField("map", md, false, (FieldValueImpl) map);
            fail("Map with default should fail");
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder.addField("array", ad, false, (FieldValueImpl) array);
            fail("Array with default should fail");
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder.addField("record", rd, false, (FieldValueImpl) rec);
            fail("Record with default should fail");
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder.addField("record", rd, false, (FieldValueImpl) rec);
            fail("Record with default should fail");
        } catch (IllegalArgumentException iae) {
        }

        try {
            builder.addField("json", FieldDefImpl.Constants.jsonDef,
                             false, iv);
            fail("JSON with default should fail");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test
    public void testJsonCollection() {
        TableBuilder builder = TableBuilder.createTableBuilder("jsonCollection");
        builder.setJsonCollection();
        builder.addInteger("i")
            .addInteger("j")
            .primaryKey("i")
            .shardKey("i")
            .primaryKey("j");
        TableImpl table = builder.buildTable();
        RowImpl row = table.createRow();
        row.put("i", 1);
        row.put("j", 2);
        row.put("name", "myname");
        row.put("age", 45);
        row.put("bool", true);
        row.put("double", 1.67);
        row.putJson("json", "{\"a\": 1}");

        /* add an index; top-level field (a) is ignored, and allowed */
        table.addIndex(new IndexImpl("myindex", table,
                                     makeIndexList("a.b.c"),
                                     makeIndexTypeList(FieldDef.Type.LONG),
                                     true, false, null));

        /* try to add index key */
        IndexImpl index = (IndexImpl) table.getIndex("myindex");
        IndexKeyImpl ikey = index.createIndexKey();
        ikey.put("a.b.c", 6L);
        index.serializeIndexKey(ikey);
    }
}
