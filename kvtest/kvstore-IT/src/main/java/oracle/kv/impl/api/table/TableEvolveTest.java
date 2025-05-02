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
import static org.junit.Assert.fail;

import java.math.BigDecimal;

import oracle.kv.StatementResult;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TimeToLive;

import oracle.nosql.nson.values.JsonUtils;

import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This set of tests exercises schema evolution for tables.  It also does
 * some exercising of default and nullable values as well as generation and
 * use of schemas for tables.
 *
 * See TableEvolver header comments for the types of evolution that are
 * allowed.
 *
 * Schema evolution rules are enforced several ways. The TableEvolver class has
 * methods for addition and removal of fields.  Some validation is done at that
 * time. Nested RecordDef fields are evolved by first creating a RecordEvolver
 * instance for the nested field, then calling its methods to add/remove state.
 * Individual fields can be modified in place.  When the actual evolution call
 * is made all fields are again validated and rules enforced.
 *
 * Examples of features and options to try:
 *  - use of nullable in schemas and verification of ser-deserialization of
 *    nulls.
 *  - use of default values everywhere, both user-defined and "default"
 *    defaults.
 *  - addition/removal of fields for schema evolution, simple, complex, and
 *    nested.
 *  - key-only tables, evolving into tables with schema (add a non-key field)
 *    and back (remove non-key fields), etc
 * Nearly all of these cases affect the value portion of table
 * records vs keys, which are not allowed to be very flexible.
 *
 * Rules that the system implements and enforces and are tested here:
 *
 * 1.  Default values.  All fields have default values, explicitly set or
 * defaulted by the system.  There are 2 reasons for this:
 *  o support for schema evolution on all fields
 *  o the ability to default unset values in a new record
 * This latter feature is a bit unique but potentially useful to users. If, on
 * a put, a field is not set it's default value will be
 * automatically inserted.  "Default" default values are as follows:
 *  Integer, Long, Double, Float -- 0
 *  String -- empty string
 *  Boolean -- false
 *  Binary -- empty byte[]
 *  FixedBinary -- empty byte[] of the correct size
 *  Enumerations -- the first value in the array.  This is odd but there seems
 *  to be no reasonable alternative.
 *  Record -- a record of default values for its entries
 *  Map -- empty map
 *  Array -- empty array
 *
 * Default values cannot be explicitly set by users for the 3 complex types
 * at this time; only "default" defaults are used.  Complex types can be
 * nullable which means that their default value would be null.
 *
 * 2. Nullable.  A field may be set as a nullable value, which is false by
 * default.  This means that it can be set to null (using the NullValue
 * object).  It also means that its value may be the NullValue object.
 * Nullable fields have the additional property that their "default" default
 * becomes the null value vs the default discussed above.  We requires that
 * when unions are involved and there is a default value, the type of the
 * default value must be declared first in the schema.  This means that if a
 * default value changes so does the schema ordering.
 */
public class TableEvolveTest extends TableTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        TableTestBase.staticSetUp(true /* excludeTombstones */);
    }

    private TableImpl buildUserTable() {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder("User",
                                                "Table of Users",
                                                null)
                .addInteger("id")
                .addString("firstName")
                .addString("lastName")
                .addInteger("age")
                .addEnum("type",
                         new String[]{"home", "work", "other"}, null,
                         null, "home") /* default value is home */
                .primaryKey("id")
                .shardKey("id");
            addTable(builder, true);
            return getTable("User");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }

    @Test
    public void testEvolve() {

        int tableVersion = 1;

        TableImpl userTable = buildUserTable();

        /* put a row that will work */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe" );
        row.put("lastName", "cool");
        row.putEnum("type", "work");
        row.put("age", 43);
        tableImpl.put(row, null, null);

        PrimaryKey key = userTable.createPrimaryKey();
        key.put("id", 1);
        Row retrieved = tableImpl.get(key, null);
        assertEquals(userTable.getTableVersion(), retrieved.getTableVersion());

        /*
         * Puts a row that leaves out fields, gets the row and verifies that
         * the non-key fields are all defaulted.
         */
        Row sparseRow = userTable.createRow();
        sparseRow.put("id", 1);
        tableImpl.put(sparseRow, null, null);
        sparseRow = tableImpl.get(sparseRow.createPrimaryKey(), null);

        /* defaults do not include the primary key fields */
        assertTrue(sparseRow.equals(
                       userTable.createRowWithDefaults().put("id",1)));

        /*
         * add an optional,nullable field, leave it out, and verify that
         * it ends up as a null.
         */
        assertEquals("Unexpected table version", tableVersion,
                     userTable.getTableVersion());
        TableEvolver evolver = TableEvolver.createTableEvolver(userTable);
        evolver.addInteger("optInt", null, true, /* nullable */ null);
        evolver.evolveTable();
        /*
         * update metadata and retrieve new table
         */
        TableImpl evolvedTable = evolveAndGet(evolver);
        ++tableVersion;
        assertEquals("Unexpected table version", tableVersion,
                     evolvedTable.getTableVersion());

        /*
         * retrieve the old record using the new table and verify
         * presence of new field and that it's got the default value
         */
        try {
            key = evolvedTable.createPrimaryKey();
            key.put("id", 1);
            retrieved = tableImpl.get(key, null);
            /* the row's table version is still that of the original table */
            assertEquals(userTable.getTableVersion(),
                         retrieved.getTableVersion());
            FieldValue opt = retrieved.get("optInt");
            assertTrue(opt.isNull());
        } catch (Exception e) {
            fail("Exception on retrieve: " + e);
        }

        /*
         * retrieve the old record using the old table and verify that
         * the new field is not present.
         */
        key = userTable.createPrimaryKey();
        key.put("id", 1);
        retrieved = tableImpl.get(key, null);
        assertFieldAbsent(retrieved, "optInt");

        /*
         * Using new version, put a record leaving out the new field.  Leave
         * out the enum "type" to check the default value, which should be the
         * first value of the enum ("home");
         */
        row = evolvedTable.createRow();
        row.put("id", 2);
        row.put("firstName", "joe" );
        row.put("lastName", "cool");
        row.put("age", 43);
        tableImpl.put(row, null, null);

        /*
         * A put will update the table version of the row.
         */
        assertEquals(evolvedTable.getTableVersion(), row.getTableVersion());

        /* this method copies the key values from the Row */
        key = row.createPrimaryKey();
        retrieved = tableImpl.get(key, null);
        assertEquals(evolvedTable.getTableVersion(),
                     retrieved.getTableVersion());

        FieldValue opt = retrieved.get("optInt");
        assertTrue(opt.isNull());
        EnumValueImpl enumVal = (EnumValueImpl) retrieved.get("type").asEnum();
        assertTrue(enumVal.get().equals("home"));

        /*
         * Evolve again, using same Evolver above, to add a Map of
         * records.  This tests some of the nested complex type code
         * on both put and get.
         */
        evolver = TableEvolver.createTableEvolver(evolvedTable);
        evolver.addField("this_is_a_map",
                         TableBuilder.createMapBuilder()
                         .addField(TableBuilder.createRecordBuilder("person")
                                  .addString("name")
                                   .addInteger("age", "age", null, 12)
                                  .build()).build());
        evolver.evolveTable(); /* modifies in-place */
        /*
         * update metadata and retrieve new table
         */
        evolvedTable = evolveAndGet(evolver);
        assertTrue(evolvedTable.numTableVersions() == 3);
        ++tableVersion;
        assertEquals("Unexpected table version", tableVersion,
                     evolvedTable.getTableVersion());

        /* add the map to the row used above and put it again */
        Row newRow = evolvedTable.createRow();
        copyRow(row, newRow);
        MapValue map = newRow.putMap("this_is_a_map");
        RecordValue val = map.putRecord("r1");
        val.put("name", "x");
        val.put("age", 10);
        val = map.putRecord("r2");
        val.put("name", "y");
        /* let the "age" field in this record default (12, see above) */

        tableImpl.put(newRow, null, null);
        assertEquals(evolvedTable.getTableVersion(),
                     newRow.getTableVersion());

        key = newRow.createPrimaryKey();
        retrieved = tableImpl.get(key, null);
        assertEquals(evolvedTable.getTableVersion(),
                     retrieved.getTableVersion());
        MapValue rmap = retrieved.get("this_is_a_map").asMap();
        RecordValue rval = rmap.get("r2").asRecord();
        /* verify the default value */
        assertTrue(rval.get("age").asInteger().get() == 12);

        /* put the record again, without the map and verify the null field */
        newRow = evolvedTable.createRow();
        copyRow(row, newRow);
        tableImpl.put(newRow, null, null);
        retrieved = tableImpl.get(key, null);
        FieldValue mapVal = retrieved.get("this_is_a_map");
        assertTrue(mapVal.isNull());

        /* put the map back, reusing the original MapValue */
        newRow.put("this_is_a_map", map);
        tableImpl.put(newRow, null, null);

        /*
         * Evolve something in the Record that is the element of the
         * new map field.
         */
        evolver = TableEvolver.createTableEvolver(evolvedTable);
        MapDef mapDef = evolver.getMap("this_is_a_map").asMap();
        RecordEvolver recordEvolver =
            evolver.createRecordEvolver((RecordDefImpl)mapDef.getElement());
        recordEvolver.removeField("age");
        evolver.evolveTable();
        evolvedTable = evolveAndGet(evolver);

        ++tableVersion;
        assertEquals("Unexpected table version", tableVersion,
                     evolvedTable.getTableVersion());

        /*
         * Look at the state.  The removed fields are still in the record but
         * removed from this view of it.
         *
         * Need a new version of the primary key based on current table
         */
        key = evolvedTable.createPrimaryKey();
        key.put("id", 2);
        retrieved = tableImpl.get(key, null);
        rmap = retrieved.get("this_is_a_map").asMap();

        /*
         * The age field of the record in the map should be gone in this
         * version.
         */
        rval = rmap.get("r2").asRecord();
        assertFieldAbsent(rval, "age");

        /*
         * Try some evolutions that will fail.
         * 1.  remove key field
         * 2.  make invalid change to a field
         * 3.  try to change primary key
         */
        TableEvolver evolver1 =
            TableEvolver.createTableEvolver(evolvedTable);
        try {
            evolver1.removeField("id");
            fail("Remove should have failed");
        } catch (Exception iae) {
        }
        try {
            evolver1.primaryKey("id");
            fail("Attempt to set primary key should have failed");
        } catch (Exception iae) {
        }
        try {
            evolver1.shardKey("id");
            fail("Attempt to set major key should have failed");
        } catch (Exception iae) {
        }
        /* evolver1 is in a bad state at this time, do not reuse */

        /*
         * Try to evolve an older version of the table.  This fails immediately
         */
        try {
            evolver1 = TableEvolver.createTableEvolver
                (evolvedTable.getVersion(evolvedTable.numTableVersions() - 1));
            fail("Evolver creation should have failed");
        } catch (Exception iae) {
        }

        /*
         * Try to evolve an older version of the table.  This is trickier and
         * validation will occur when the attempt to evolve is made on the
         * server side.
         *
         * 1.  create evolver for current version
         * 2.  create another evolver, evolve
         * 3.  attempt to evolve first version
         */
        evolver1 = TableEvolver.createTableEvolver(evolvedTable);
        TableEvolver evolver2 = TableEvolver.createTableEvolver(evolvedTable);
        evolver2.addInteger("an_integer");
        evolver2.evolveTable(); /* a local operation */
        try {
            evolveTable(evolver2, true); /* updates metadata */
        } catch (Exception wont_happen) {
            fail("Evolve should have worked");
        }

        /*
         * Back to evolver1...
         */
        evolver1.addInteger("another_integer");
        evolver1.evolveTable(); /* a local operation */

        /*
         * This should fail on the server side.  The assertion of failure is
         * made in evolveTable() because it's an asynchronous failures that
         * occurs during plan execution.  It must happen there because that is
         * where the system has metadata locked.
         */
        try {
            evolveTable(evolver1, false);
        } catch (Exception wont_happen) {
        }
    }


    /**
     * Evolve by adding a new field with a non-null default value.
     */
    @Test
    public void testEvolveWithDefault() {

        int tableVersion = 1;

        TableImpl userTable = buildUserTable();

        /* put a row that will work */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe" );
        row.put("lastName", "cool");
        row.putEnum("type", "work");
        row.put("age", 43);
        tableImpl.put(row, null, null);

        PrimaryKey key = userTable.createPrimaryKey();
        key.put("id", 1);
        Row retrieved = tableImpl.get(key, null);
        assertEquals(userTable.getTableVersion(), retrieved.getTableVersion());

        /*
         * add an optional field with a default value, verify the default.
         */
        assertEquals("Unexpected table version", tableVersion,
                     userTable.getTableVersion());
        TableEvolver evolver = TableEvolver.createTableEvolver(userTable);
        evolver.addInteger("optInt", null, false, 7);
        evolver.evolveTable();
        /*
         * update metadata and retrieve new table
         */
        TableImpl evolvedTable = evolveAndGet(evolver);
        ++tableVersion;
        assertEquals("Unexpected table version", tableVersion,
                     evolvedTable.getTableVersion());

        /*
         * retrieve the old record using the new table and verify
         * presence of new field and that it's got the default value
         */
        try {
            key = evolvedTable.createPrimaryKey();
            key.put("id", 1);
            retrieved = tableImpl.get(key, null);
            /* the row's table version is still that of the original table */
            assertEquals(userTable.getTableVersion(),
                         retrieved.getTableVersion());
            FieldValue opt = retrieved.get("optInt");
            assertTrue(opt.asInteger().get() == 7);
        } catch (Exception e) {
            fail("Exception on retrieve: " + e);
        }
    }

    /*
     * Start with a key-only table and evolve it to add fields, then remove
     * them again.
     */
    @Test
    public void testKeyOnly()
        throws Exception {

        int tableVersion = 1;

        TableBuilder builder = (TableBuilder)
            TableBuilder.createTableBuilder("KeyOnly",
                                            "Key only table",
                                            null)
            .addString("firstName")
            .addString("lastName")
            .primaryKey("firstName", "lastName")
            .shardKey("firstName");
        addTable(builder, true);
        TableImpl keyOnlyTable = getTable("KeyOnly");

        assertEquals("Unexpected table version", tableVersion,
                     keyOnlyTable.getTableVersion());

        /* put a row that will work */
        Row row = keyOnlyTable.createRow();
        row.put("firstName", "joe" );
        row.put("lastName", "cool");
        tableImpl.put(row, null, null);

        PrimaryKey key = keyOnlyTable.createPrimaryKey();
        key.put("firstName", "joe");
        key.put("lastName", "cool");
        Row retrieved = tableImpl.get(key, null);

        TableEvolver evolver = TableEvolver.createTableEvolver(keyOnlyTable);
        evolver.addInteger("age");
        evolver.evolveTable();
        keyOnlyTable = evolveAndGet(evolver);
        ++tableVersion;
        assertEquals("Unexpected table version", tableVersion,
                     keyOnlyTable.getTableVersion());
        /*
         * Put a new record into evolved table.  The table is no longer
         * key-only.
         */
        row = keyOnlyTable.createRow();
        row.put("firstName", "jane" );
        row.put("lastName", "doe");
        row.put("age", 7);
        tableImpl.put(row, null, null);

        /*
         * Create a new PrimaryKey based on the new table version and read
         * the original (key-only) record, the age should be defaulted (null).
         */
        key = keyOnlyTable.createPrimaryKey();
        key.put("firstName", "joe");
        key.put("lastName", "cool");
        retrieved = tableImpl.get(key, null);
        assertTrue(retrieved.get("age").isNull());

        /*
         * Get the record that had actual age.  It should be present
         * as put above.
         */
        key = keyOnlyTable.createPrimaryKey();
        key.put("firstName", "jane");
        key.put("lastName", "doe");
        retrieved = tableImpl.get(key, null);
        assertTrue(retrieved.get("age").asInteger().get() == 7);

        /*
         * Remove the age field, going back to key-only
         */
        evolver = TableEvolver.createTableEvolver(keyOnlyTable);
        evolver.removeField("age");
        evolver.evolveTable();
        keyOnlyTable = evolveAndGet(evolver);
        ++tableVersion;
        assertEquals("Unexpected table version", tableVersion,
                     keyOnlyTable.getTableVersion());

        /*
         * Get the original record, age should be gone.
         */
        key = keyOnlyTable.createPrimaryKey();
        key.put("firstName", "joe");
        key.put("lastName", "cool");
        retrieved = tableImpl.get(key, null);
        assertFieldAbsent(retrieved, "age");

        /*
         * Get the record that had actual age.  It should also be gone,
         * filtered out by the evolved schema.
         */
        key = keyOnlyTable.createPrimaryKey();
        key.put("firstName", "jane");
        key.put("lastName", "doe");
        retrieved = tableImpl.get(key, null);
        assertFieldAbsent(retrieved, "age");
    }

    /**
     * Test cases where dealing with differing versions results in
     * overwriting data:
     * 1.  field removed
     * 2.  field added
     */
    @Test
    public void testDataLoss()
        throws Exception {

        TableImpl userTableAge = buildUserTable();

        /* put a row that will work */
        Row row = userTableAge.createRow();
        row.put("id", 1);
        row.put("firstName", "joe" );
        row.put("lastName", "cool");
        row.putEnum("type", "work");
        row.put("age", 43);
        tableImpl.put(row, null, null);

        /*
         * remove the age field
         */
        TableEvolver evolver = TableEvolver.createTableEvolver(userTableAge);
        evolver.removeField("age");
        evolver.evolveTable();
        /*
         * update metadata and retrieve new table
         */
        TableImpl userTableNoAge = evolveAndGet(evolver);

        /* put a row that will work */
        PrimaryKey key = userTableNoAge.createPrimaryKey();
        key.put("id", 1);
        Row noAgeRow = tableImpl.get(key, null);

        assertFieldAbsent(noAgeRow, "age");

        assertEquals("Table version mismatch", noAgeRow.getTableVersion(),
                     userTableAge.getTableVersion());

        /* get the row with the original table version */
        row = tableImpl.get(userTableAge.createPrimaryKey(row), null);
        /* age is present */
        assertNotNull(row.get("age"));

        /* the row's table version is still the old version */
        assertEquals("Table version mismatch", row.getTableVersion(),
                     userTableAge.getTableVersion());

        /*
         * Overwrite using new version (without age)
         */
        noAgeRow = userTableNoAge.createRow();
        noAgeRow.put("id", 1);
        noAgeRow.put("firstName", "joe" );
        noAgeRow.put("lastName", "cool");
        noAgeRow.putEnum("type", "work");
        assertNotNull(tableImpl.put(noAgeRow, null, null));

        /*
         * Get it with the old version.  Age should have reverted and
         * should now be the default (null).
         */
        Row newRow = tableImpl.get(userTableAge.createPrimaryKey(row), null);
        assertTrue("Age should be default value (null)",
                   newRow.get("age").isNull());

        /* the row's table version is new */
        assertEquals("Table version mismatch", newRow.getTableVersion(),
                     userTableNoAge.getTableVersion());

        /*
         * Another case.  Delete the row using the older table to capture
         * a ReturnRow.  This exercises another path that handles the
         * fact that the Table associated with a Row is not the latest one.
         */
        ReturnRow rr = userTableAge.createReturnRow(ReturnRow.Choice.ALL);
        boolean val = tableImpl.delete(userTableAge.createPrimaryKey(row),
                                       rr, null);
        assertTrue(val);
        /*
         * The table version of the ReturnRow should be that of the table
         * that did the put, and not the one that created the ReturnRow.
         */
        assertEquals("Table version mismatch", rr.getTableVersion(),
                     userTableNoAge.getTableVersion());

        /*
         * Now do it the other way -- add a field and overwrite it with
         * the older version.
         */
        evolver = TableEvolver.createTableEvolver(userTableNoAge);
        evolver.addInteger("newInt");
        evolver.evolveTable();
        /*
         * update metadata and retrieve new table
         */
        TableImpl userTableNewInt = evolveAndGet(evolver);

        Row newIntRow = userTableNewInt.createRow();
        newIntRow.put("id", 2);
        newIntRow.put("firstName", "joe" );
        newIntRow.put("lastName", "cool");
        newIntRow.putEnum("type", "work");
        newIntRow.put("newInt", 7);
        assertNotNull(tableImpl.put(newIntRow, null, null));

        /* the row's table version is new */
        assertEquals("Table version mismatch", newIntRow.getTableVersion(),
                     userTableNewInt.getTableVersion());

        /* overwrite with userTableNoAge table, the previous version */
        noAgeRow = userTableNoAge.createRow(newIntRow);
        assertNotNull(tableImpl.put(noAgeRow, null, null));

        /* get with new table and assert that the value reverted */
        newRow = tableImpl.get
            (userTableNewInt.createPrimaryKey(newIntRow), null);
        assertTrue("NewInt  should be default value (null)",
                   newRow.get("newInt").isNull());
        /* the row's table version is new */
        assertEquals("Table version mismatch", newRow.getTableVersion(),
                     userTableNoAge.getTableVersion());
    }

    /**
     * Test removing, then recreating a field with the same name with both
     * the same, and different types.
     */
    @Test
    public void testFieldRecreate()
        throws Exception {

        TableImpl userTableAge = buildUserTable();

        /* put a row */
        Row row = userTableAge.createRow();
        row.put("id", 1);
        row.put("age", 43);
        row.put("firstName", "joe" );
        row.put("lastName", "cool");
        row.putEnum("type", "work");
        tableImpl.put(row, null, null);

        /*
         * remove the age field
         */
        TableEvolver evolver = TableEvolver.createTableEvolver(userTableAge);
        evolver.removeField("age");
        evolver.evolveTable();

        /*
         * update metadata and retrieve new table
         */
        TableImpl userTableNoAge = evolveAndGet(evolver);


        PrimaryKey key = userTableAge.createPrimaryKey();
        key.put("id", 1);
        Row ageRow = tableImpl.get(key, null);

        assertNotNull(ageRow.get("age"));

        /*
         * Get the no-age row
         */
        key = userTableNoAge.createPrimaryKey();
        key.put("id", 1);
        Row noAgeRow = tableImpl.get(key, null);
        assertFieldAbsent(noAgeRow, "age");

        assertEquals("Table version mismatch", ageRow.getTableVersion(),
                     userTableAge.getTableVersion());

        /*
         * Add the age field back in, same type
         */
        evolver = TableEvolver.createTableEvolver(userTableNoAge);
        evolver.addInteger("age");
        evolver.evolveTable();
        TableImpl userTableAge2 = evolveAndGet(evolver);

        /*
         * Get the original row using new table, which has age field again.
         * This works and resurrects the age field.
         */
        key = userTableAge2.createPrimaryKey();
        key.put("id", 1);
        Row ageRow2 = tableImpl.get(key, null);
        assertNotNull(ageRow2.get("age"));

        /*
         * Remove it again, then add it back as a different type.  This will
         * fail.
         */
        evolver = TableEvolver.createTableEvolver(userTableAge2);
        evolver.removeField("age");
        evolver.evolveTable();
        TableImpl userTableNoAge2 = evolveAndGet(evolver);

        evolver = TableEvolver.createTableEvolver(userTableNoAge2);
        try {
            evolver.addString("age");
            fail("Addition of string should have failed");
        } catch (IllegalArgumentException iae) {
        }
    }


    /**
     * Test cases where fields are added or droped from records inside maps
     * and arrays.
     */
    @Test
    public void testAddDropRecFields()
        throws Exception {

        String statement =
            "create table t24049 (id integer, " +
                "map1 map(record(i1 integer))," +
                "arr1 array(record(i2 integer)), " +
                "rec1 record(rec2 record(i3 integer))," +
                "primary key(id))";
        executeDdl(statement);

        /* put v1 data */
        Table t = tableImpl.getTable("t24049");
        int v1 = t.getTableVersion();

        MapValue map1 = t.getField("map1").createMap();
        RecordValue rec = t.getField("map1").asMap().getElement().createRecord();
        rec.put("i1", 11);
        map1.put("map1.f1", rec);

        ArrayValue arr1 = t.getField("arr1").createArray();
        rec = t.getField("arr1").asArray().getElement().createRecord();
        rec.put("i2", 12);
        arr1.add(rec);

        RecordValue rec1 = t.getField("rec1").createRecord();
        rec = t.getField("rec1").asRecord().getFieldDef("rec2")
            .createRecord();
        rec.put("i3", 13);
        rec1.put("rec2", rec);

        Row row = t.createRow();
        row.put("id", 1);
        row.put("map1", map1);
        row.put("arr1", arr1);
        row.put("rec1", rec1);
        tableImpl.put(row, null, null);

        /* add new fields in record types */
        statement = "alter table t24049 (add map1.valuEs().new1 integer)";
        executeDdl(statement);
        statement = "alter table t24049 (add arr1[].new2 integer, " +
            "add rec1.rec2.new3 integer)";
        executeDdl(statement);

        /* put v2 data */
        t = tableImpl.getTable("t24049");
        int v2 = t.getTableVersion();

        map1 = t.getField("map1").createMap();
        rec = t.getField("map1").asMap().getElement().createRecord();
        rec.put("i1", 101);
        rec.put("new1", 201);
        map1.put("map1.f1", rec);

        arr1 = t.getField("arr1").createArray();
        rec = t.getField("arr1").asArray().getElement().createRecord();
        rec.put("i2", 102);
        rec.put("new2", 202);
        arr1.add(rec);

        rec1 = t.getField("rec1").createRecord();
        rec = t.getField("rec1").asRecord().getFieldDef("rec2")
            .createRecord();
        rec.put("i3", 103);
        rec.put("new3", 203);
        rec1.put("rec2", rec);

        row = t.createRow();
        row.put("id", 2);
        row.put("map1", map1);
        row.put("arr1", arr1);
        row.put("rec1", rec1);
        tableImpl.put(row, null, null);

        /* drop old fields in record types */
        statement = "alter table t24049 (drop map1.vaLUes().i1, " +
            "drop arr1[].i2, drop rec1.rec2.i3)";
        executeDdl(statement);

        /* put v3 data */
        t = tableImpl.getTable("t24049");
        int v3 = t.getTableVersion();

        map1 = t.getField("map1").createMap();
        rec = t.getField("map1").asMap().getElement().createRecord();
        rec.put("new1", 301);
        map1.put("map1.f1", rec);

        arr1 = t.getField("arr1").createArray();
        rec = t.getField("arr1").asArray().getElement().createRecord();
        rec.put("new2", 302);
        arr1.add(rec);

        rec1 = t.getField("rec1").createRecord();
        rec = t.getField("rec1").asRecord().getFieldDef("rec2")
            .createRecord();
        rec.put("new3", 303);
        rec1.put("rec2", rec);

        row = t.createRow();
        row.put("id", 3);
        row.put("map1", map1);
        row.put("arr1", arr1);
        row.put("rec1", rec1);
        tableImpl.put(row, null, null);


        /* verify data using v1 schema */
        t = tableImpl.getTable("t24049");

        Table tv1 = t.getVersion(v1);
        assertEquals("Table version mismatch", v1, tv1.getTableVersion());

        PrimaryKey key = tv1.createPrimaryKey();
        key.put("id", 1);

        Row rid1v1 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v1, rid1v1.getTableVersion());

        assertNotNull(rid1v1.get("map1"));
        assertNotNull(rid1v1.get("map1").asMap().get("map1.f1"));
        assertNotNull(rid1v1.get("map1").asMap().get("map1.f1").asRecord().
            get("i1"));
        assertEquals(11, rid1v1.get("map1").asMap().get("map1.f1").asRecord().
            get("i1").asInteger().get());
        assertFieldAbsent(rid1v1.get("map1").asMap().get("map1.f1").asRecord(),
                          "new1");

        assertNotNull(rid1v1.get("arr1"));
        assertNotNull(rid1v1.get("arr1").asArray().get(0));
        assertNotNull(rid1v1.get("arr1").asArray().get(0).asRecord().
            get("i2"));
        assertEquals(12, rid1v1.get("arr1").asArray().get(0).asRecord()
            .get("i2").asInteger().get());
        assertFieldAbsent(rid1v1.get("arr1").asArray().get(0).asRecord(),
                          "new2");
        assertNotNull(rid1v1.get("rec1"));
        assertNotNull(rid1v1.get("rec1").asRecord().get("rec2"));
        assertNotNull(rid1v1.get("rec1").asRecord().get("rec2").asRecord().
            get("i3"));
        assertEquals(13, rid1v1.get("rec1").asRecord().get("rec2").asRecord().
            get("i3").asInteger().get());
        assertFieldAbsent(rid1v1.get("rec1").asRecord().get("rec2").asRecord(),
                          "new3");

        key = tv1.createPrimaryKey();
        key.put("id", 2);

        Row rid2v1 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v2, rid2v1.getTableVersion());

        assertNotNull(rid2v1.get("map1"));
        assertNotNull(rid2v1.get("map1").asMap().get("map1.f1"));
        assertNotNull(rid2v1.get("map1").asMap().get("map1.f1").asRecord().
            get("i1"));
        assertEquals(101, rid2v1.get("map1").asMap().get("map1.f1").asRecord().
            get("i1").asInteger().get());
        assertFieldAbsent(rid2v1.get("map1").asMap().get("map1.f1").asRecord(),
                          "new1");

        assertNotNull(rid2v1.get("arr1"));
        assertNotNull(rid2v1.get("arr1").asArray().get(0));
        assertNotNull(rid2v1.get("arr1").asArray().get(0).asRecord().
            get("i2"));
        assertEquals(102, rid2v1.get("arr1").asArray().get(0).asRecord()
            .get("i2").asInteger().get());
        assertFieldAbsent(rid2v1.get("arr1").asArray().get(0).asRecord(),
                          "new2");

        assertNotNull(rid2v1.get("rec1"));
        assertNotNull(rid2v1.get("rec1").asRecord().get("rec2"));
        assertNotNull(rid2v1.get("rec1").asRecord().get("rec2").asRecord().
            get("i3"));
        assertEquals(103, rid2v1.get("rec1").asRecord().get("rec2").asRecord().
            get("i3").asInteger().get());
        assertFieldAbsent(rid2v1.get("rec1").asRecord().get("rec2").asRecord(),
                          "new3");


        key = tv1.createPrimaryKey();
        key.put("id", 3);

        Row rid3v1 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v3, rid3v1.getTableVersion());

        assertNotNull(rid3v1.get("map1"));
        assertNotNull(rid3v1.get("map1").asMap().get("map1.f1"));
        assertNotNull(rid3v1.get("map1").asMap().get("map1.f1").asRecord().
            get("i1"));
        assertTrue(rid3v1.get("map1").asMap().get("map1.f1").asRecord().
            get("i1").isNull());
        assertFieldAbsent(rid3v1.get("map1").asMap().get("map1.f1").asRecord(),
                          "new1");

        assertNotNull(rid3v1.get("arr1"));
        assertNotNull(rid3v1.get("arr1").asArray().get(0));
        assertNotNull(rid3v1.get("arr1").asArray().get(0).asRecord().
            get("i2"));
        assertTrue(rid3v1.get("arr1").asArray().get(0).asRecord().
            get("i2").isNull());
        assertFieldAbsent(rid3v1.get("arr1").asArray().get(0).asRecord(),
                          "new2");

        assertNotNull(rid3v1.get("rec1"));
        assertNotNull(rid3v1.get("rec1").asRecord().get("rec2"));
        assertNotNull(rid3v1.get("rec1").asRecord().get("rec2").asRecord().
            get("i3"));
        assertTrue(rid3v1.get("rec1").asRecord().get("rec2").asRecord().
            get("i3").isNull());
        assertFieldAbsent(rid3v1.get("rec1").asRecord().get("rec2").asRecord(),
                          "new3");


        /* verify data using v2 schema */
        t = store.getTableAPI().getTable("t24049");

        Table tv2 = t.getVersion(v2);
        assertEquals("Table version mismatch", v2, tv2.getTableVersion());

        key = tv2.createPrimaryKey();
        key.put("id", 1);

        Row rid1v2 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v1, rid1v2.getTableVersion());

        assertNotNull(rid1v2.get("map1"));
        assertNotNull(rid1v2.get("map1").asMap().get("map1.f1"));
        assertNotNull(rid1v2.get("map1").asMap().get("map1.f1").asRecord().
            get("i1"));
        assertEquals(11, rid1v2.get("map1").asMap().get("map1.f1").asRecord().
            get("i1").asInteger().get());
        assertNotNull(rid1v2.get("map1").asMap().get("map1.f1").asRecord().
            get("new1"));
        assertTrue(rid1v2.get("map1").asMap().get("map1.f1").asRecord().
            get("new1").isNull());


        assertNotNull(rid1v2.get("arr1"));
        assertNotNull(rid1v2.get("arr1").asArray().get(0));
        assertNotNull(rid1v2.get("arr1").asArray().get(0).asRecord().
            get("i2"));
        assertEquals(12, rid1v2.get("arr1").asArray().get(0).asRecord()
            .get("i2").asInteger().get());
        assertNotNull(rid1v2.get("arr1").asArray().get(0).asRecord().
            get("new2"));
        assertTrue(rid1v2.get("arr1").asArray().get(0).asRecord().
            get("new2").isNull());

        assertNotNull(rid1v2.get("rec1"));
        assertNotNull(rid1v2.get("rec1").asRecord().get("rec2"));
        assertNotNull(rid1v2.get("rec1").asRecord().get("rec2").asRecord().
            get("i3"));
        assertEquals(13, rid1v2.get("rec1").asRecord().get("rec2").asRecord().
            get("i3").asInteger().get());
        assertNotNull(rid1v2.get("rec1").asRecord().get("rec2").asRecord().
            get("new3"));
        assertTrue(rid1v2.get("rec1").asRecord().get("rec2").asRecord().
            get("new3").isNull());


        key = tv2.createPrimaryKey();
        key.put("id", 2);

        Row rid2v2 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v2, rid2v2.getTableVersion());

        assertNotNull(rid2v2.get("map1"));
        assertNotNull(rid2v2.get("map1").asMap().get("map1.f1"));
        assertNotNull(rid2v2.get("map1").asMap().get("map1.f1").asRecord().
            get("i1"));
        assertEquals(101, rid2v2.get("map1").asMap().get("map1.f1").asRecord().
            get("i1").asInteger().get());
        assertNotNull(rid2v2.get("map1").asMap().get("map1.f1").asRecord().
            get("new1"));
        assertEquals(201, rid2v2.get("map1").asMap().get("map1.f1").asRecord().
            get("new1").asInteger().get());

        assertNotNull(rid2v2.get("arr1"));
        assertNotNull(rid2v2.get("arr1").asArray().get(0));
        assertNotNull(rid2v2.get("arr1").asArray().get(0).asRecord().
            get("i2"));
        assertEquals(102, rid2v2.get("arr1").asArray().get(0).asRecord()
            .get("i2").asInteger().get());
        assertNotNull(rid2v2.get("arr1").asArray().get(0).asRecord().
            get("new2"));
        assertEquals(202, rid2v2.get("arr1").asArray().get(0).asRecord()
            .get("new2").asInteger().get());

        assertNotNull(rid2v2.get("rec1"));
        assertNotNull(rid2v2.get("rec1").asRecord().get("rec2"));
        assertNotNull(rid2v2.get("rec1").asRecord().get("rec2").asRecord().
            get("i3"));
        assertEquals(103, rid2v2.get("rec1").asRecord().get("rec2").asRecord().
            get("i3").asInteger().get());
        assertNotNull(rid2v2.get("rec1").asRecord().get("rec2").asRecord().
            get("new3"));
        assertEquals(203, rid2v2.get("rec1").asRecord().get("rec2").asRecord().
            get("new3").asInteger().get());


        key = tv2.createPrimaryKey();
        key.put("id", 3);

        Row rid3v2 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v3, rid3v2.getTableVersion());

        assertNotNull(rid3v2.get("map1"));
        assertNotNull(rid3v2.get("map1").asMap().get("map1.f1"));
        assertNotNull(rid3v2.get("map1").asMap().get("map1.f1").asRecord().
            get("i1"));
        assertTrue(rid3v2.get("map1").asMap().get("map1.f1")
            .asRecord().get("i1").isNull());
        assertNotNull(rid3v2.get("map1").asMap().get("map1.f1").asRecord().
            get("new1"));
        assertEquals(301, rid3v2.get("map1").asMap().get("map1.f1").asRecord().
            get("new1").asInteger().get());

        assertNotNull(rid3v2.get("arr1"));
        assertNotNull(rid3v2.get("arr1").asArray().get(0));
        assertNotNull(rid3v2.get("arr1").asArray().get(0).asRecord().
            get("i2"));
        assertTrue(rid3v2.get("arr1").asArray().get(0).asRecord()
            .get("i2").isNull());
        assertNotNull(rid3v2.get("arr1").asArray().get(0).asRecord().
            get("new2"));
        assertEquals(302, rid3v2.get("arr1").asArray().get(0).asRecord()
            .get("new2").asInteger().get());

        assertNotNull(rid3v2.get("rec1"));
        assertNotNull(rid3v2.get("rec1").asRecord().get("rec2"));
        assertNotNull(rid3v2.get("rec1").asRecord().get("rec2").asRecord().
            get("i3"));
        assertTrue(rid3v2.get("rec1").asRecord().get("rec2").asRecord().
            get("i3").isNull());
        assertNotNull(rid3v2.get("rec1").asRecord().get("rec2").asRecord().
            get("new3"));
        assertEquals(303, rid3v2.get("rec1").asRecord().get("rec2").asRecord().
            get("new3").asInteger().get());


        /* verify data using v3 schema */
        t = store.getTableAPI().getTable("t24049");

        Table tv3 = t.getVersion(v3);
        assertEquals("Table version mismatch", v3, tv3.getTableVersion());

        key = tv3.createPrimaryKey();
        key.put("id", 1);

        Row rid1v3 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v1, rid1v3.getTableVersion());

        assertNotNull(rid1v3.get("map1"));
        assertNotNull(rid1v3.get("map1").asMap().get("map1.f1"));
        assertFieldAbsent(rid1v3.get("map1").asMap().get("map1.f1").asRecord(),
                          "i1");
        assertNotNull(rid1v3.get("map1").asMap().get("map1.f1").asRecord().
            get("new1"));
        assertTrue(rid1v3.get("map1").asMap().get("map1.f1").asRecord().
            get("new1").isNull());


        assertNotNull(rid1v3.get("arr1"));
        assertNotNull(rid1v3.get("arr1").asArray().get(0));
        assertFieldAbsent(rid1v3.get("arr1").asArray().get(0).asRecord(),
                          "i2");
        assertFieldAbsent(rid1v3.get("map1").asMap().get("map1.f1").asRecord(),
                          "new2");


        assertNotNull(rid1v3.get("rec1"));
        assertNotNull(rid1v3.get("rec1").asRecord().get("rec2"));
        assertFieldAbsent(rid1v3.get("rec1").asRecord().get("rec2").asRecord(),
                          "i3");
        assertFieldAbsent(rid1v3.get("map1").asMap().get("map1.f1").asRecord(),
                          "new3");


        key = tv3.createPrimaryKey();
        key.put("id", 2);

        Row rid2v3 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v2, rid2v3.getTableVersion());

        assertNotNull(rid2v3.get("map1"));
        assertNotNull(rid2v3.get("map1").asMap().get("map1.f1"));
        assertFieldAbsent(rid2v3.get("map1").asMap().get("map1.f1").asRecord(),
                          "i1");
        assertNotNull(rid2v3.get("map1").asMap().get("map1.f1").asRecord().
            get("new1"));
        assertEquals(201, rid2v3.get("map1").asMap().get("map1.f1").asRecord().
            get("new1").asInteger().get());

        assertNotNull(rid2v3.get("arr1"));
        assertNotNull(rid2v3.get("arr1").asArray().get(0));
        assertFieldAbsent(rid2v3.get("arr1").asArray().get(0).asRecord(),
                          "i2");
        assertNotNull(rid2v3.get("arr1").asArray().get(0).asRecord().
            get("new2"));
        assertEquals(202, rid2v3.get("arr1").asArray().get(0).asRecord()
            .get("new2").asInteger().get());

        assertNotNull(rid2v3.get("rec1"));
        assertNotNull(rid2v3.get("rec1").asRecord().get("rec2"));
        assertFieldAbsent(rid2v3.get("rec1").asRecord().get("rec2").asRecord(),
                          "i3");
        assertNotNull(rid2v3.get("rec1").asRecord().get("rec2").asRecord().
            get("new3"));
        assertEquals(203, rid2v3.get("rec1").asRecord().get("rec2").asRecord().
            get("new3").asInteger().get());


        key = tv3.createPrimaryKey();
        key.put("id", 3);

        Row rid3v3 = tableImpl.get(key, null);
        assertEquals("Row version mismatch", v3, rid3v3.getTableVersion());

        assertNotNull(rid3v3.get("map1"));
        assertNotNull(rid3v3.get("map1").asMap().get("map1.f1"));
        assertFieldAbsent(rid3v3.get("map1").asMap().get("map1.f1").asRecord(),
                          "i1");
        assertNotNull(rid3v3.get("map1").asMap().get("map1.f1").asRecord().
            get("new1"));
        assertEquals(301, rid3v3.get("map1").asMap().get("map1.f1").asRecord().
            get("new1").asInteger().get());

        assertNotNull(rid3v3.get("arr1"));
        assertNotNull(rid3v3.get("arr1").asArray().get(0));
        assertFieldAbsent(rid3v3.get("arr1").asArray().get(0).asRecord(),
                          "i2");
        assertNotNull(rid3v3.get("arr1").asArray().get(0).asRecord().
            get("new2"));
        assertEquals(302, rid3v3.get("arr1").asArray().get(0).asRecord()
            .get("new2").asInteger().get());

        assertNotNull(rid3v3.get("rec1"));
        assertNotNull(rid3v3.get("rec1").asRecord().get("rec2"));
        assertFieldAbsent(rid3v3.get("rec1").asRecord().get("rec2").asRecord(),
                          "i3");
        assertNotNull(rid3v3.get("rec1").asRecord().get("rec2").asRecord().
            get("new3"));
        assertEquals(303, rid3v3.get("rec1").asRecord().get("rec2").asRecord().
            get("new3").asInteger().get());
    }

    /**
     * This test [#25595] where a DDL change resulted in allowing a named
     * field to change type (drop, re-add with a different type). This resulted
     * in the inability to deserialize existing records. The bug went in with
     * the fix for schema evolution on nested records, in changeset 29ea550fb1d8
     *
     * Additional invalid cases were added for thoroughness and the fact that
     * the fix refactored some of the name validation code.
     *
     * NOTE: this test does *not* validate the contents of existing/new rows
     * after evolution other than verifying that a row still exists. It focuses
     * on valid and invalid statements and the results of the evolution itself.
     */
    @Test
    public void testInvalidEvolution()
        throws Exception {

        String createStatement = "create table foo (id integer, name string, " +
            "age integer, address record(city string, street string), " +
            "primary key(id))";
        String createIndexStatement = "create index idx on foo(age)";

        String dropStatement = "alter table foo(drop name)";
        String dropRecStatement = "alter table foo(drop address.street)";
        String addStatementOK = "alter table foo(add name string)";
        String addRecStatementOK = "alter table foo(add address.street string)";

        /* these all fail */

        /* type mismatch */
        String addStatement = "alter table foo(add name integer)";
        /* type mismatch */
        String addRecStatement = "alter table foo(add address.street integer)";
        /* can't drop primary key */
        String dropKeyStatement = "alter table foo(drop id)";
        /* can't drop index key */
        String dropIndexKeyStatement = "alter table foo(drop age)";
        /* path doesn't exist */
        String addBadField = "alter table foo(add a.b integer)";
        /* path doesn't exist 1 */
        String dropBadRecField = "alter table foo(drop address.number)";
        /* field name too long */
        String addBadLongField = "alter table foo(add " +
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx string)";

        String[] badStatements =
            new String[]{addStatement, addRecStatement,
                         dropKeyStatement, dropIndexKeyStatement,
                         addBadField, dropBadRecField, addBadLongField};

        executeDdl(createStatement);
        executeDdl(createIndexStatement);

        Table table = getTable("foo");
        for (int i = 0; i < 10; i++) {
            Row row = table.createRow();
            row.put("id", i).put("name", ("name" + i)).put("age", i+10);
            assertNotNull(tableImpl.put(row, null, null));
        }

        displayRow("Before drop", table, 1);
        executeDdl(dropStatement);
        table = getTable("foo");
        displayRow("After drop", table, 1);

        executeDdl(dropRecStatement);
        table = getTable("foo");
        displayRow("After drop rec", table, 1);

        /*
         * these will fail
         */
        for (String query : badStatements) {
            executeDdl(query, false);
            table = getTable("foo");
            displayRow(("After query: " + query), table, 1);
        }

        /* these will work */
        executeDdl(addStatementOK);
        executeDdl(addRecStatementOK);
        table = getTable("foo");
        displayRow("After re-add", table, 1);
    }

    @Test
    public void test25782() {

        String tableDDL =
            "CREATE TABLE Users (                " +
            "   id INTEGER,                      " +
            "   firstName STRING,                " +
            "   lastName STRING,                 " +
            "   address RECORD(street STRING,    " +
            "                  city STRING,      " +
            "                  state STRING,     " +
            "                  phones ARRAY( RECORD(kind ENUM(work, home)," +
            "                                       areaCode INTEGER,     " +
            "                                       number INTEGER) ) ),  " +
            "   PRIMARY KEY (id)                                          " +
            ")";

        String indexDDL =
            "create index index1 on users (    " +
            "    address.phones[].areaCode,    " +
            "    address.phones[].kind)        ";

        String alterDDL = "alter table users (ADD expenses MAP(Integer))";

        executeDdl(tableDDL);
        executeDdl(indexDDL);
        executeDdl(alterDDL);
    }

    @SuppressWarnings("unused")
    @Test
    public void testDataToKeyOnly()
        throws Exception {

        final String selectQuery = "select * from ToKeyOnlyQuery";

        executeDdl("CREATE TABLE ToKeyOnlyQuery " +
                   "(id integer, json json, primary key(id))");

        Table twoColumnTable = tableImpl.getTable("ToKeyOnlyQuery");

        /* put a row that will work */
        Row row = twoColumnTable.createRow();
        row.put("id", 1);
        row.putJson("json", "11");
        tableImpl.put(row, null, null);

        PrimaryKey key = twoColumnTable.createPrimaryKey();
        key.put("id", 1);

        StatementResult sr = store.executeSync(selectQuery);
        int count = 0;
        for (RecordValue rec : sr) {
            ++count;
        }

        assertTrue(count == 1);

        executeDdl("ALTER TABLE ToKeyOnlyQuery (drop json)");

        twoColumnTable = tableImpl.getTable("ToKeyOnlyQuery");

        sr = store.executeSync(selectQuery);
        count = 0;
        for (RecordValue rec : sr) {
            ++count;
        }
        assertTrue(count == 1);

        TableIterator<Row> iter =
            tableImpl.tableIterator(twoColumnTable.createPrimaryKey(),
                                    null, null);
        count = 0;
        while(iter.hasNext()) {
            ++count;
            iter.next();
        }
        assertTrue(count == 1);
    }

    /**
     * Test on auto-generated name of the nested fixed binary, enum and
     * record of array and map fields.
     */
    @Test
    public void testAutoGeneratedName() {
        final String createTableDDL =
            "CREATE TABLE tabWithAutoGenNames (" +
                "id integer, " +
                "mapFixed map(binary(10)), " +
                "mapEnum map(enum(a,b,c)), " +
                "mapRec map(record(rid integer, rname string)), " +
                "arrayFixed array(binary(10)), " +
                "arrayEnum array(enum(a,b,c)), " +
                "arrayRec array(record(rid integer, rname string)), " +
                "primary key (id)" +
            ")";

        final String alterTableAddMapArrayFixed =
            "ALTER TABLE tabWithAutoGenNames (" +
                "add mapFixed1 map(binary(10)), " +
                "add arrayFixed1 array(binary(10))" +
            ")";

        final String alterTableAddMapArrayEnum =
            "ALTER TABLE tabWithAutoGenNames (" +
                "add mapEnum1 map(enum(t1,t2,t3)), " +
                "add arrayEnum1 array(enum(t1,t2,t3))" +
            ")";

        final String alterTableAddMapArrayRecord =
            "ALTER TABLE tabWithAutoGenNames (" +
                "add mapRec1 map(record(rid integer, rname string)), " +
                "add arrayRec1 array(record(rid integer, rname string))" +
            ")";

        final String alterTableAddFields =
            "ALTER TABLE tabWithAutoGenNames (" +
                "add mapFixed2 map(binary(10)), " +
                "add arrayFixed2 array(binary(10)), " +
                "add mapEnum2 map(enum(t1,t2,t3)), " +
                "add arrayEnum2 array(enum(t1,t2,t3)), " +
                "add mapRec2 map(record(rid integer, rname string)), " +
                "add arrayRec2 array(record(rid integer, rname string))" +
            ")";

        executeDdl(createTableDDL);

        executeDdl(alterTableAddMapArrayFixed);
        TableImpl table = getTable("tabWithAutoGenNames");
        assertNotNull(table.getField("mapFixed1"));
        assertNotNull(table.getField("arrayFixed1"));

        executeDdl(alterTableAddMapArrayEnum);
        table = getTable("tabWithAutoGenNames");
        assertNotNull(table.getField("mapEnum1"));
        assertNotNull(table.getField("arrayEnum1"));

        executeDdl(alterTableAddMapArrayRecord);
        table = getTable("tabWithAutoGenNames");
        assertNotNull(table.getField("mapRec1"));
        assertNotNull(table.getField("arrayRec1"));

        executeDdl(alterTableAddFields);
        table = getTable("tabWithAutoGenNames");
        assertNotNull(table.getField("mapFixed2"));
        assertNotNull(table.getField("arrayFixed2"));
        assertNotNull(table.getField("mapEnum2"));
        assertNotNull(table.getField("arrayEnum2"));
        assertNotNull(table.getField("mapRec2"));
        assertNotNull(table.getField("arrayRec2"));
    }

    @Test
    public void testEvolveTableWithJsonIndex() {
        String createTable = "create table evol (" +
                                  "id integer, " +
                                  "lng long, " +
                                  "json json, " +
                                  "primary key(id)" +
                             ")";
        String createJsonIndex = "create index idx1 on evol (json.long as long)";
        String alterTableAddField = "alter table evol (add json1 JSON)";
        String alterTableAddFieldFailed = "alter table evol (add json STRING)";
        String alterTableDropFieldFailed = "alter table evol (drop json)";
        String alterTableDropField = "alter table evol (drop json1)";

        executeDdl(createTable);
        executeDdl(createJsonIndex);
        executeDdl(alterTableAddField);
        executeDdl(alterTableAddFieldFailed, false);
        executeDdl(alterTableDropFieldFailed, false);
        executeDdl(alterTableDropField);
    }

    @Test
    public void testTableLimitsAfterCreateIndex() throws Exception {
        String createTableDdl =
            "create table foo(id integer, name string, primary key(id))";
        String createIndexDdl = "create index idxName on foo(name)";

        /* Create table */
        executeDdl(createTableDdl);

        /* Set table limits */
        TableImpl table = (TableImpl)tableImpl.getTable("foo");
        setTableLimits(table, new TableLimits(2000, 1000,
                                              TableLimits.NO_CHANGE));
        /* Check table limits */
        table = (TableImpl)tableImpl.getTable("foo");
        TableLimits limits = table.getTableLimits();
        assertTrue("TableLimit should not be null", limits != null);
        assertTrue(limits.getReadLimit() == 2000);
        assertTrue(limits.getWriteLimit() == 1000);

        /* Create index */
        executeDdl(createIndexDdl);

        /* Check table limits after creating index */
        table = (TableImpl)tableImpl.getTable("foo");
        limits = table.getTableLimits();
        assertTrue("TableLimit after creating index should not be null",
                    limits != null);
        assertTrue(limits.getReadLimit() == 2000);
        assertTrue(limits.getWriteLimit() == 1000);
    }

    @Test
    public void testTableEvolveWithTTL() throws Exception {
        String createTableDdl =
            "create table foo(id integer, name string, primary key(id)) " +
            "using ttl 2 days";
        String alterTableAddFieldDdl = "alter table foo (add age integer)";
        String alterTableUpdateTTLDdl = "alter table foo using ttl 24 hours";
        String alterTableDropFieldDdl = "alter table foo(drop age)";

        /* Create table */
        executeDdl(createTableDdl);

        /* Check table default ttl */
        TimeToLive tableTTL = TimeToLive.ofDays(2);
        TableImpl table = (TableImpl)tableImpl.getTable("foo");
        assertEquals(tableTTL, table.getDefaultTTL());

        /* Alter table add new field */
        executeDdl(alterTableAddFieldDdl);

        /* Check table default ttl after add new field */
        table = (TableImpl)tableImpl.getTable("foo");
        assertEquals(tableTTL, table.getDefaultTTL());

        final int tableVersion = table.getTableVersion();

        /* Alter table update ttl to 24 hours */
        executeDdl(alterTableUpdateTTLDdl);
        tableTTL = TimeToLive.ofHours(24);

        /* Check table default ttl after update ttl */
        table = (TableImpl)tableImpl.getTable("foo");
        assertEquals(tableTTL, table.getDefaultTTL());

        /* Changing TTL should not version the table */
        assertEquals("Unexpected table version", tableVersion,
                     table.getTableVersion());

        /* Alter table drop a field */
        executeDdl(alterTableDropFieldDdl);

        /* Check table default ttl after drop field */
        table = (TableImpl)tableImpl.getTable("foo");
        assertEquals(tableTTL, table.getDefaultTTL());
    }

    /**
     * Checks that changing non-versioned state does not change the table
     * version. Non-version state is TTL, Description, Regions, etc. As DDL
     * supports changing non-version state, please add test cases here.
     */
    @Test
    public void testEvolveNonVersionState() throws Exception {
        /* Create table */
        //TODO: allow MRTable mode after MRTable supports TTL tables.
        executeDdl("create table foo(id integer, name string, primary key(id))",
                   true, true);

        /* Get initial version */
        TableImpl table = (TableImpl)tableImpl.getTable("foo");
        final int tableVersion = table.getTableVersion();

        /* Update ttl  */
        executeDdl("alter table foo using ttl 24 hours");

        /* Changing TTL should not version the table */
        table = (TableImpl)tableImpl.getTable("foo");
        assertEquals("Unexpected table version", tableVersion,
                     table.getTableVersion());
    }

    /*
     * Test evolution where the row is serialized NSON from the pre-evolved
     * table. This can happen when streaming in a multi-region table/GAT
     * situation. [KVSTORE-2398]
     * 1. create a table with a specific schema
     * 2. create multiple NSON values directly:
     *   a. missing fields from the schema
     *   b. with extra fields not in the schem
     * 3. serialize the NSON as Avro directly
     */
    @Test
    public void testTableEvolveWithNson() throws Exception {
        /*
         * A simple transient table to start
         */
        TableBuilder builder = (TableBuilder)
            TableBuilder.createTableBuilder("nson")
            .addInteger("id")
            .addInteger("age")
            .addString("name")
            .primaryKey("id");
        TableImpl table = builder.buildTable();

        /* some row values to turn to nson for testing */
        String nsonEmpty = "{}";
        String nsonComplete = "{ \"age\": 4, \"name\":\"joe\"}";
        String nsonCompleteOO = "{\"name\":\"joe\", \"age\": 4}";
        String nsonMissing = "{\"name\":\"joe\"}";
        String nsonExtra1 = "{\"age\":4, \"name\":\"joe\", \"extra\": 6}";
        String nsonExtra2 = "{\"age\":4, \"extra\": 6}";
        String nsonExtraCase = "{\"nAmE\":\"joe\", \"aGe\":4, \"extra\": 6}";

        byte[] valEmpty = createNsonFromJson(nsonEmpty);
        byte[] valComplete = createNsonFromJson(nsonComplete);
        byte[] valCompleteOO = createNsonFromJson(nsonCompleteOO);
        byte[] valMissing = createNsonFromJson(nsonMissing);
        byte[] valExtra1 = createNsonFromJson(nsonExtra1);
        byte[] valExtra2 = createNsonFromJson(nsonExtra2);
        byte[] valExtraCase = createNsonFromJson(nsonExtraCase);

        /* expect nsonComplete, above, for complete* and extra1 */
        String expectMissing = "{ \"age\": null, \"name\":\"joe\"}";
        String expectExtra2 = "{ \"age\": 4, \"name\": null}";
        String expectEmpty = "{ \"age\": null, \"name\": null}";


        RecordValueImpl rowValue;
        rowValue = createValueFromNson(table, valComplete, "complete");
        assertTrue(compare(rowValue, nsonComplete));

        rowValue = createValueFromNson(table, valCompleteOO, "completeOO");
        assertTrue(compare(rowValue, nsonComplete));

        rowValue = createValueFromNson(table, valMissing, "missing");
        assertTrue(compare(rowValue, expectMissing));

        rowValue = createValueFromNson(table, valExtra1, "extra1");
        assertTrue(compare(rowValue, nsonComplete));

        rowValue = createValueFromNson(table, valExtra2, "extra2");
        assertTrue(compare(rowValue, expectExtra2));

        rowValue = createValueFromNson(table, valEmpty, "empty");
        assertTrue(compare(rowValue, expectEmpty));

        rowValue = createValueFromNson(table, valExtraCase, "extraCase");
        assertTrue(compare(rowValue, nsonComplete));

        /*
         * Add atomic types with defaults. Validate that the defaults are
         * used correctly
         */
        TableEvolver evolver = TableEvolver.createTableEvolver(table);
        evolver.addInteger("newInt", null, false, 7);
        evolver.addLong("newLong", null, false, 10L);
        evolver.addDouble("newDouble", null, false, 20.0);
        evolver.addFloat("newFloat", null, false, 21.0F);
        evolver.addString("newString", null, false, "newstringdef");
        evolver.addBoolean("newBool", null, false, true);
        evolver.addNumber("newNumber", null, false, new BigDecimal("0.99999"));
        evolver.addTimestamp("newTime", 6, null, true, new java.sql.Timestamp(
                                 1722007264));
        evolver.addEnum("newEnum",
                        new String[] {"a", "b", "c"}, null, true, "a");
        byte[] bytes = new byte[] {(byte)0xa0, (byte)0xe3,
                                   (byte)0xd4, (byte)0xf3};
        evolver.addBinary("newBin", null, true, bytes);
        evolver.addFixedBinary("newFixedBin", 4, null, true, bytes);
        evolver.addJson("newJson", null);
        evolver.addField("this_is_a_map",
                         TableBuilder.createMapBuilder()
                         .addField(TableBuilder.createRecordBuilder("person")
                                  .addString("name")
                                   .addInteger("age", "age", null, 12)
                                  .build()).build());

        TableImpl newTable = evolver.evolveTable();
        /*
         * Note: defaults for complex types are always null
         */
        final String expectAfterEvolve =
            "{\"age\":4,\"name\":\"joe\",\"newInt\":7,\"newLong\":10,\"newDouble\":20.0,\"newFloat\":21.0,\"newString\":\"newstringdef\",\"newBool\":true,\"newNumber\":0.99999,\"newTime\":\"1970-01-20T22:20:07.264000Z\",\"newEnum\":\"a\",\"newBin\":\"oOPU8w==\",\"newFixedBin\":\"oOPU8w==\",\"newJson\":null,\"this_is_a_map\":null}";
        rowValue = createValueFromNson(newTable, valComplete, "(new) complete");
        assertTrue(compare(rowValue, expectAfterEvolve));

        /*
         * a few more cases for nested record
         */
        builder = (TableBuilder) TableBuilder.createTableBuilder("nson")
            .addInteger("id")
            .addField("person", TableBuilder.createRecordBuilder("person")
                      .addString("name")
                      .addInteger("age", "age", null, 12)
                      .build())
            .primaryKey("id");
        table = builder.buildTable();
        nsonComplete = "{\"person\": {\"name\": \"joe\", \"age\": 4}}";
        nsonMissing = "{\"person\": {\"age\": 4}}";
        nsonExtra1 = "{\"person\": {\"name\": \"joe\", \"age\": 4, \"ex\":5}}";

        valComplete = createNsonFromJson(nsonComplete);
        valMissing = createNsonFromJson(nsonMissing);
        valExtra1 = createNsonFromJson(nsonExtra1);

        rowValue = createValueFromNson(table, valComplete, "nested complete");
        assertTrue(compare(rowValue, nsonComplete));

        rowValue = createValueFromNson(table, valMissing, "nested missing");
        assertTrue(compare(rowValue,
                           "{\"person\": {\"name\": null,  \"age\": 4}}"));

        rowValue = createValueFromNson(table, valExtra1, "nested extra1");
        assertTrue(compare(rowValue, nsonComplete));

        /* add/remove field, evolve */
        evolver = TableEvolver.createTableEvolver(table);
        evolver.addInteger("person.newAge", null, false, 7);
        evolver.removeField("person.age");
        newTable = evolver.evolveTable();
        rowValue =
            createValueFromNson(newTable, valComplete, "nested complete");
        /* age is gone, newAge is in, with default value */
        assertTrue(compare(rowValue, "{\"person\": {\"name\": \"joe\", " +
                           "\"newAge\": 7}}"));
    }

    private boolean compare(RecordValueImpl rowValue, String expected) {
        /* this does a schema-independent comparison */
        return JsonUtils.jsonEquals(rowValue.toString(), expected);
    }

    @SuppressWarnings("unused")
    private void displayRow(String msg, Table table, int i) {
        PrimaryKey pkey = table.createPrimaryKey();
        pkey.put("id", i);
        Row row = tableImpl.get(pkey, null);
        assertNotNull(row);
    }

    private void copyRow(Row from, Row to) {
        ((RowImpl)to).copyFields((RowImpl) from);
    }
}
