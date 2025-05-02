/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;

import org.junit.Test;


public class JsonTest extends TestBase {

    TableImpl userTable;

    @Test
    public void complexTable() {

        /*
         * Top-level table
         */
        createUserTable();

        /*
         * Create and initialize a row.  Lots to do for this row.
         */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe");
        row.put("lastName", "doe");
        row.put("age", 76);
        row.put("bin", new byte[] {1, 2, 3, 4, 5});
        row.putFixed("fixedBin", new byte[] {68, 69, 70});
        row.put("bool", true);
        row.put("birthday", new Timestamp(0));
        ArrayValue array = row.putArray("likes");
        array.add(new String[] {"a", "b", "c"});

        RecordValue record = row.putRecord("address");
        record.put("street", "17 Happy Lane");
        record.put("city", "Whoville");
        record.put("zip", 12345);
        record.putEnum("type", "home");

        MapValue map = row.putMap("friends");
        map.put("best", "joe");
        map.put("sister", "jane");
        map.put("friend", "annie");

        ArrayValue array1 = row.putArray("array_of_maps");
        for (long l = 0; l < 5L; l++) {
            MapValue map1 = array1.addMap();
            RecordValue record1 = map1.putRecord("record");
            record1.put("a", "a_string");
            record1.put("d", 7.567);
            record1.put("l", l);
        }

        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(true);
        Row newRow = userTable.createRowFromJson(jsonString, false);
        assertTrue(newRow.equals(row));
    }

    /*
     * This table includes some nesting and various data types to be sure
     * that they translate to/from JSON cleanly.
     */
    private void createUserTable() {
        userTable = TableBuilder.createTableBuilder("User",
                                                    "Table of Users",
                                                    null)
            .addInteger("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addBinary("bin")
            .addFixedBinary("fixedBin", 3)
            .addBoolean("bool")
            .addTimestamp("birthday", 0)
            .addField("likes", TableBuilder.createArrayBuilder()
                      .addString(null).build())
            .addField("address", TableBuilder.createRecordBuilder("address")
                      .addString("street")
                      .addString("city")
                      .addInteger("zip")
                      .addEnum("type",
                               new String[]{"home", "work", "other"},
                               null, null, null)
                      .build())
            .addField("friends", TableBuilder.createMapBuilder()
                      .addString(null).build())
            .addField("array_of_maps", TableBuilder.createArrayBuilder()
                      .addField(TableBuilder.createMapBuilder()
                                .addField(TableBuilder.
                                          createRecordBuilder("foo")
                                          .addString("a")
                                          .addDouble("d")
                                          .addLong("l")
                                          .build())
                                .build())
                      .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
    }

    @Test
    public void testOptionalJson() {

        final String fullRow = "{\"i\":1,\"s\":\"s\"}";
        final String shortRow = "{\"i\":1}";
        final String extraRow = "{\"i\":1,\"no\":\"no\",\"s\":\"s\"}";
        final String extraRowWithRecord =
            "{\"i\":1,\"record\": {\"r1\" : 1, \"i\" : \"7\"},\"s\":\"s\"}";
        final String extraRowWithArray =
            "{\"i\":1,\"array\": [1, 2, 3, 4, 5],\"s\":\"s\"}";
        final String extraRowWithNesting =
            "{\"i\":1,\"record\": {\"r1\" : 1, \"a\" : [4,5], \"i\" : \"7\"},\"s\":\"s\"}";

        TableImpl table = TableBuilder.createTableBuilder("test",
                                                          null, null)
            .addInteger("i")
            .addString("s")
            .primaryKey("i")
            .shardKey("i")
            .buildTable();

        Row originalRow = table.createRowFromJson(fullRow, true);
        Row row = table.createRowFromJson(shortRow, false);
        assertFalse(row.equals(originalRow));
        row = table.createRowFromJson(extraRow, false);
        assertTrue(row.equals(originalRow));

        try {
            row = table.createRowFromJson(shortRow, true);
            fail("Row creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        try {
            row = table.createRowFromJson(extraRow, true);
            fail("Row creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        try {
            row = table.createRowFromJson(extraRowWithRecord, false);
            assertTrue(row.equals(originalRow));
        } catch (IllegalArgumentException iae) {
            fail("Row creation should have worked");
        }

        try {
            row = table.createRowFromJson(extraRowWithArray, false);
            assertTrue(row.equals(originalRow));
        } catch (IllegalArgumentException iae) {
            fail("Row creation should have worked");
        }

        try {
            row = table.createRowFromJson(extraRowWithNesting, false);
            assertTrue(row.equals(originalRow));
        } catch (IllegalArgumentException iae) {
            fail("Row creation should have worked (nesting)");
        }
    }

    /*
     * Make sure nullable fields work correctly.
     */
    @Test
    public void testNullable() {
        TableImpl table =
            TableBuilder.createTableBuilder("TableWithNullable",
                                            null, null)
            .addInteger("id")
            .addString("s1")
            .addString("nullable", null,
                       true,  /* nullable */
                       "defaultStringValue")  /* default value */
            .addField("array_with_nullable", TableBuilder.createArrayBuilder()
                      .addInteger(null, null, true, null).build())
            .addField("map_with_nullable", TableBuilder.createMapBuilder()
                      .addInteger(null, null, true, null).build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        originalRow.put("s1", "string");
        originalRow.putNull("nullable");
        ArrayValue array = originalRow.putArray("array_with_nullable");
        array.add(1);
        array.add(3);
        MapValue map = originalRow.putMap("map_with_nullable");
        map.put("7", 7);
        map.put("8", 8);

        String jsonString = originalRow.toJsonString(true);
        Row row = table.createRowFromJson(jsonString, true);
        assertTrue(row.equals(originalRow));
    }

    /**
     * Test construction of a PrimaryKey from JSON
     */
    @Test
    public void testPrimaryKey() {

        final String shortKey = "{\"a\":\"a\"}";
        final String extraKey = "{\"a\":\"a\",\"i\": 1,\"b\":\"b\"}";

        TableImpl table = TableBuilder.createTableBuilder("test",
                                                          null, null)
            .addInteger("i")
            .addString("a")
            .addString("b")
            .addString("c")
            .primaryKey("a", "b")
            .shardKey("a")
            .buildTable();

        /*
         * Create a row
         */
        Row row = table.createRow();
        row.put("i", 1);
        row.put("a", "a");
        row.put("b", "a");
        row.put("c", "a");


        String rowString = row.toJsonString(true);
        PrimaryKey originalKey = table.createPrimaryKeyFromJson(rowString,
                                                                false);
        String keyString = originalKey.toJsonString(true);
        PrimaryKey key = table.createPrimaryKeyFromJson(keyString, true);
        assertTrue(key.equals(originalKey));

        /*
         * Create key from the full row, will fail -- too many fields
         */
        try {
            key = table.createPrimaryKeyFromJson(rowString, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a short key -- will fail
         */
        try {
            key = table.createPrimaryKeyFromJson(shortKey, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a long key -- will fail
         */
        try {
            key = table.createPrimaryKeyFromJson(extraKey, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a short key, valid but not equal
         */
        try {
            key = table.createPrimaryKeyFromJson(shortKey, false);
            assertFalse(key.equals(originalKey));
        } catch (IllegalArgumentException iae) {
        }
    }


    /**
     * Test construction of a IndexKey from JSON
     */
    @Test
    public void testIndexKey() {

        final String shortKeyOK = "{\"d\":\"a\"}";
        final String shortKeyBad = "{\"e\": 1}";
        final String fullKey = "{\"d\":\"a\", \"e\": 1}";
        final String badKey = "{\"b\":\"a\"}";
        final String badJSON = "{this is not json}";


        TableImpl table = TableBuilder.createTableBuilder("test",
                                                          null, null)
            .addInteger("i")
            .addString("a")
            .addString("b")
            .addString("c")
            .addString("d")
            .addInteger("e")
            .primaryKey("a", "b")
            .shardKey("a")
            .buildTable();

        /*
         * Create an index on fields d, e
         */
        IndexImpl index = new IndexImpl("testindex", table,
                                        makeIndexList("d", "e"), null, null);
        table.addIndex(index);

        /*
         * Create a row
         */
        Row row = table.createRow();
        row.put("i", 1);
        row.put("a", "a");
        row.put("b", "a");
        row.put("c", "a");
        row.put("d", "d");
        row.put("e", 76);

        String rowString = row.toJsonString(true);
        /* the input string has extra fields */
        IndexKey originalKey = index.createIndexKeyFromJson(rowString,
                                                            false);
        String keyString = originalKey.toJsonString(true);
        /* exact match */
        IndexKey key = index.createIndexKeyFromJson(keyString,
                                                    true);
        assertTrue(key.equals(originalKey));

        /*
         * Create key from the short key, will succeed if exact is false.
         * The resulting key is not equal to the original.  It has fewer
         * fields.
         */
        try {
            key = index.createIndexKeyFromJson(shortKeyOK, false);
            assertFalse(key.equals(originalKey));
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a short key -- will fail if exact is true
         */
        try {
            key = index.createIndexKeyFromJson(shortKeyOK, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a short key, not exact, but in the wrong order.
         * It is not legal to specify fields out of order.
         */
        try {
            key = index.createIndexKeyFromJson(shortKeyBad, false);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a key using a full key but different values
         */
        try {
            key = index.createIndexKeyFromJson(fullKey, true);
            assertFalse(key.equals(originalKey));
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Create a long key, but ask for exact.  This will fail.
         */
        try {
            key = index.createIndexKeyFromJson(rowString, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Use a bad key, containing a field that's not in the index.
         */
        try {
            key = index.createIndexKeyFromJson(badKey, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Use a bad JSON string
         */
        try {
            key = index.createIndexKeyFromJson(badJSON, true);
            fail("Key creation should have failed");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Test invalid use of BigInteger/BigDecimal in JSON
     */
    @Test
    public void testBigNumbers() {

        BigInteger bi = new BigInteger("12345678901234567899");
        final String biString = "{\"i\":" + bi.toString() +"}";
        final String floatString = "{\"i\":567.4}"; /* float to int */
        final String numberString = "{\"s\":567.4}"; /* float to string */

        TableImpl table = TableBuilder.createTableBuilder("test",
                                                          null, null)
            .addInteger("i")
            .addDouble("d")
            .addString("s")
            .primaryKey("i")
            .buildTable();
        try {
            table.createPrimaryKeyFromJson(biString, false);
            fail("JSON cast should have failed");
        } catch (IllegalArgumentException iae) {
        }

        try {
            table.createPrimaryKeyFromJson(floatString, false);
            fail("JSON cast should have failed");
        } catch (IllegalArgumentException iae) {
        }

        try {
            table.createRowFromJson(numberString, false);
            fail("JSON cast should have failed");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Test invalid use JSON in other types.
     * Important: this test uses strings to construct out of range values vs
     * casts between double and float.  The reason is that such casts may add
     * precision that changes the value of a > or < comparison.  For example,
     * Float.MAX_VALUE is 3.4028235E+38 but if you construct a double based on
     * Float.MAX_VALUE you'll see 3.4028234663852886E38, which rounds to
     * Float.MAX_VALUE but doesn't compare properly.  The validation code
     * that this tests is also careful to use strings.
     */
    @Test
    public void testBadJson() {

        /* min/max positive strings */
        String floatMin = "1.4E-45";
        String floatMax = "3.4028235E38";
        String negFloatMin = "-1.4E-45";
        String negFloatMax = "-3.4028235E38";

        final String[] okLongs = new String[] {"1234", "1030792151039"};
        final String[] okInts = new String[] {"1234"};
        final String[] badInts = new String[] {"1030792151039"};
        final String[] okFloats = new String[] {"1234", "1234.7689",
            "01234.7689", "NaN", floatMin /* min */, floatMax /* max */,
            negFloatMin /* negative min */, negFloatMax /* negative max */,
            "1030792151039876" /* a long */, "-1030792151039.78999", "0.0"
        };
        final String[] badFloats = new String[] {
            "INF" /* INF not supported */,
            "3.4028236E38" /* too large */,
            /* badFloatMax , */"1.4E-46" /* too small */
        };
        final String[] okDoubles = new String[] {
            "1234", "01234.7689", "1030792151039.78999", "NaN"
        };
        final String[] badDoubles = new String[] {
            "\"1234\"", "true"
        };

        TableImpl table = TableBuilder.createTableBuilder("test",
                                                          null, null)
            .addInteger("i")
            .addLong("l")
            .addDouble("d")
            .addFloat("f")
            .addString("s")
            .addField("ai",
                      TableBuilder.createArrayBuilder("ai").addInteger().build())
            .addField("al",
                      TableBuilder.createArrayBuilder("al").addLong().build())
            .addField("af",
                      TableBuilder.createArrayBuilder("af").addFloat().build())
            .addField("ad",
                      TableBuilder.createArrayBuilder("ad").addDouble().build())
            .addField("mi",
                      TableBuilder.createMapBuilder("mi").addInteger().build())
            .addField("ml",
                      TableBuilder.createMapBuilder("ml").addLong().build())
            .addField("mf",
                      TableBuilder.createMapBuilder("mf").addFloat().build())
            .addField("md",
                      TableBuilder.createMapBuilder("md").addDouble().build())
            .primaryKey("i")
            .buildTable();

        /*
         * Success cases
         */
        String fmtLong = "{\"l\":%s, \"al\":[%s], \"ml\":{\"k\":%s}}";
        for (String s : okLongs) {
            String json = String.format(fmtLong, s, s, s);
            table.createRowFromJson(json, false);
        }

        String fmtInt = "{\"i\":%s, \"ai\":[%s], \"mi\":{\"k\":%s}}";
        for (String s : okInts) {
            String json = String.format(fmtInt, s, s, s);
            table.createRowFromJson(json, false);
        }

        String fmtFloat = "{\"f\":%s, \"af\":[%s], \"mf\":{\"k\":%s}}";
        for (String s : okFloats) {
            String json = String.format(fmtFloat, s, s, s);
            table.createRowFromJson(json, false);
        }

        String fmtDouble = "{\"d\":%s, \"ad\":[%s], \"md\":{\"k\":%s}}";
        for (String s : okDoubles) {
            String json = String.format(fmtDouble, s, s, s);
            table.createRowFromJson(json, false);
        }

        /* Failed cases */
        String fmtRec = "{\"%s\": %s}";
        String fmtArray = "{\"%s\": [%s]}";
        String fmtMap = "{\"%s\": {\"k\":%s}";

        for (String s : badInts) {
            try {
                table.createRowFromJson(String.format(fmtRec, "i", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }

            try {
                table.createRowFromJson(String.format(fmtArray, "ai", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }

            try {
                table.createRowFromJson(String.format(fmtMap, "mi", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }
        }

        for (String s : badFloats) {
            try {
                table.createRowFromJson(String.format(fmtRec, "f", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }

            try {
                table.createRowFromJson(String.format(fmtArray, "af", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }

            try {
                table.createRowFromJson(String.format(fmtMap, "mf", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }

        }

        for (String s : badDoubles) {
            try {
                table.createRowFromJson(String.format(fmtRec, "d", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }
            try {
                table.createRowFromJson(String.format(fmtArray, "ad", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }
            try {
                table.createRowFromJson(String.format(fmtMap, "md", s), false);
                fail("JSON cast should have failed");
            } catch (IllegalArgumentException iae) {
            }
        }
    }

    /**
     * Create a table including a record field.
     */
    private TableImpl createTableIncludingSimpleRecordField() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
            .addInteger("id")
            .addField("address", TableBuilder.createRecordBuilder("address")
                      .addString("street")
                      .addString("city")
                      .addInteger("zip")
                      .addEnum("type",
                               new String[]{"home", "work", "other"},
                               null, null, null)
                      .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        return table;
    }

    @Test
    public void testPutRecordAsEmptyJson() {
        TableImpl table = createTableIncludingSimpleRecordField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        RecordValue addressRec = originalRow.putRecord("address");
        ((RecordValueImpl)addressRec).addMissingFields();

        Row newRow = table.createRow();
        newRow.put("id", 1);
        newRow.putRecordAsJson("address", "{}", false);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutRecordAsNormalJson(){
        TableImpl table = createTableIncludingSimpleRecordField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        RecordValue record = originalRow.putRecord("address");
        record.put("street", "17 Happy Lane");
        record.put("city", "Whoville");
        record.put("zip", 12345);
        record.putEnum("type", "home");

        Row newRow = table.createRow();
        newRow.put("id", 1);
        String recordJson = record.toJsonString(true);
        newRow.putRecordAsJson("address", recordJson, true);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutRecordAsBadJson(){
        TableImpl table = createTableIncludingSimpleRecordField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        RecordValue record = originalRow.putRecord("address");
        record.put("street", "17 Happy Lane");
        record.put("city", "Whoville");
        record.put("zip", 12345);
        record.putEnum("type", "home");

        Row newRow = originalRow.clone();
        String badRecordJson = "{\"street\":\"abc\", \"city\":\"def\"";
        // try to overwrite existing value using bad json.
        try {
            newRow.putRecordAsJson("address", badRecordJson, false);
            fail("JSON cast should have failed");
        } catch(IllegalArgumentException iae){
        }
        // newRow should be original state on error.
        assertTrue(newRow.equals(originalRow));
    }

    /**
     * Create a table including a map field.
     */
    private TableImpl createTableIncludingSimpleMapField() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
            .addInteger("id")
            .addField("friends", TableBuilder.createMapBuilder()
                      .addString(null).build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        return table;
    }

    @Test
    public void testPutMapAsEmptyJson() {
        TableImpl table = createTableIncludingSimpleMapField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        originalRow.putMap("friends");

        Row newRow = table.createRow();
        newRow.put("id", 1);
        newRow.putMapAsJson("friends", "{}", true);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutMapAsNormalJson(){
        TableImpl table = createTableIncludingSimpleMapField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        MapValue map = originalRow.putMap("friends");
        map.put("best", "joe");
        map.put("sister", "jane");
        map.put("friend", "annie");

        Row newRow = table.createRow();
        newRow.put("id", 1);
        String mapJson = map.toJsonString(true);
        newRow.putMapAsJson("friends", mapJson, true);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutMapAsBadJson(){
        TableImpl table = createTableIncludingSimpleMapField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        MapValue map = originalRow.putMap("friends");
        map.put("best", "joe");
        map.put("sister", "jane");
        map.put("friend", "annie");

        Row newRow = originalRow.clone();
        String badRecordJson = "{\"best\":\"abc\", \"sister\":\"def\"";
        // try to overwrite existing value using bad json.
        try {
            newRow.putMapAsJson("friends", badRecordJson, true);
            fail("JSON cast should have failed");
        } catch(IllegalArgumentException iae){
        }
        // newRow should be original state on error.
        assertTrue(newRow.equals(originalRow));
    }

    /**
     * Create a table including a map field.
     */
    private TableImpl createTableIncludingSimpleArrayField() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
            .addInteger("id")
            .addField("likes", TableBuilder.createArrayBuilder()
                      .addString(null).build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        return table;
    }

    @Test
    public void testPutArrayAsEmptyJson() {
        TableImpl table = createTableIncludingSimpleArrayField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        originalRow.putArray("likes");

        Row newRow = table.createRow();
        newRow.put("id", 1);
        newRow.putArrayAsJson("likes", "[]", true);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutArrayAsNormalJson(){
        TableImpl table = createTableIncludingSimpleArrayField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        ArrayValue array = originalRow.putArray("likes");
        array.add(new String[] {"a", "b", "c"});

        Row newRow = table.createRow();
        newRow.put("id", 1);
        String arrayJson = array.toJsonString(false);
        newRow.putArrayAsJson("likes", arrayJson, true);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutArrayAsBadJson(){
        TableImpl table = createTableIncludingSimpleArrayField();
        Row originalRow = table.createRow();
        originalRow.put("id", 1);
        ArrayValue array = originalRow.putArray("likes");
        array.add(new String[] {"a", "b", "c"});

        Row newRow = originalRow.clone();
        String badRecordJson = "[\"a\",\"b\", \"c\"";
        // try to overwrite existing value using bad json.
        try {
            newRow.putArrayAsJson("likes", badRecordJson, true);
            fail("JSON cast should have failed");
        } catch(IllegalArgumentException iae){
        }
        // newRow should be original state on error.
        assertTrue(newRow.equals(originalRow));
    }

    /**
     * Create a table including complex nested fields:
     * array<map<record>>, map<array<record>>, and record<map<array>,array<map>>
     */
    private TableImpl createTableIncludingComplexNestedField() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
            .addInteger("id")
            .addField("nestedarray", TableBuilder.createArrayBuilder()
                .addField(TableBuilder.createMapBuilder()
                          .addField(TableBuilder.
                                    createRecordBuilder("recordf1")
                                    .addInteger("intf1")
                                    .addString("strf1")
                                    .build())
                          .build())
                .build())
            .addField("nestedmap", TableBuilder.createMapBuilder()
                .addField(TableBuilder.createArrayBuilder()
                          .addField(TableBuilder.
                                    createRecordBuilder("recordf2")
                                    .addInteger("intf2")
                                    .addString("strf2")
                                    .build())
                          .build())
                .build())
            .addField("nestedrecord", TableBuilder
                      .createRecordBuilder("nestedrecord")
                          .addField("mapf3", TableBuilder.createMapBuilder()
                                    .addField(TableBuilder.createArrayBuilder()
                                              .addInteger()
                                              .build())
                                    .build())
                          .addField("arrayf3", TableBuilder.createArrayBuilder()
                                    .addField(TableBuilder.createMapBuilder()
                                              .addString()
                                              .build())
                                    .build())
                      .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        return table;
    }

    @Test
    public void testPutComplexNestedFieldAsFullJson() {
        TableImpl table = createTableIncludingComplexNestedField();
        Row originalRow = table.createRow();
        originalRow.put("id", 0);

        ArrayValue narray = originalRow.putArray("nestedarray");
        MapValue map1 = narray.addMap();
        RecordValue record1 = map1.putRecord("recordf1");
        record1.put("intf1", 1);
        record1.put("strf1", "1");

        MapValue nmap = originalRow.putMap("nestedmap");
        ArrayValue array2 = nmap.putArray("array2");
        RecordValue record2 = array2.addRecord();
        record2.put("intf2", 2);
        record2.put("strf2", "2");

        RecordValue nrecord = originalRow.putRecord("nestedrecord");
        MapValue map3_1 = nrecord.putMap("mapf3");
        ArrayValue array3_1 = map3_1.putArray("arrayf3_1");
        array3_1.add(3);
        ArrayValue array3_2 = nrecord.putArray("arrayf3");
        MapValue map3_2 = array3_2.addMap();
        map3_2.put("map3key", "map3value");

        Row newRow = table.createRow();
        newRow.put("id", 0);
        String narrayJson = narray.toJsonString(true);
        String nmapJson = nmap.toJsonString(true);
        String nrecordJson = nrecord.toJsonString(true);
        newRow.putArrayAsJson("nestedarray", narrayJson, true);
        newRow.putMapAsJson("nestedmap", nmapJson, true);
        newRow.putRecordAsJson("nestedrecord", nrecordJson, true);
        assertTrue(newRow.equals(originalRow));
    }

    @Test
    public void testPutComplexNestedFieldAsOptionalJson() {
        TableImpl table = createTableIncludingComplexNestedField();
        Row originalRow = table.createRow();
        originalRow.put("id", 0);

        ArrayValue narray = originalRow.putArray("nestedarray");
        MapValue map1 = narray.addMap();
        RecordValue record1 = map1.putRecord("recordf1");
        record1.put("strf1", "1");

        MapValue nmap = originalRow.putMap("nestedmap");
        ArrayValue array2 = nmap.putArray("array2");
        RecordValue record2 = array2.addRecord();
        record2.put("intf2", 2);

        RecordValue nrecord = originalRow.putRecord("nestedrecord");
        MapValue map3_1 = nrecord.putMap("mapf3");
        ArrayValue array3_1 = map3_1.putArray("arrayf3_1");
        array3_1.add(3);
        ArrayValue array3_2 = nrecord.putArray("arrayf3");
        MapValue map3_2 = array3_2.addMap();
        map3_2.put("map3key", "map3value");

        Row newRow = table.createRow();
        newRow.put("id", 0);
        // backup
        Row backup = newRow.clone();
        // miss setting intf1 field of the Record nested in narray
        String narrayJson = narray.toJsonString(true);
        // miss setting strf2 field of the Record nested in nmap
        String nmapJson = nmap.toJsonString(true);
        // add extra doublef3 field to the Record in nrecord.
        String nrecordJson = "{\"mapf3\":{\"arrayf3_1\":[3]},"
            + "\"arrayf3\":[{\"map3key\":\"map3value\"}],"
            + "\"doublef3\":1.0}";

        // All put operations should failed since exact is true
        try {
            newRow.putArrayAsJson("nestedarray", narrayJson, true);
            fail("Put Array should have failed");
        } catch (IllegalArgumentException iae) {
        }
        try {
            newRow.putMapAsJson("nestedmap", nmapJson, true);
            fail("Put map should have failed");
        } catch (IllegalArgumentException iae) {
        }
        try {
            newRow.putRecordAsJson("nestedrecord", nrecordJson, true);
            fail("Put record should have failed");
        } catch (IllegalArgumentException iae) {
        }
        // newRow should be original state on error.
        assert(newRow.equals(backup));

        // All put operations should succeed now since exact is false
        newRow.putArrayAsJson("nestedarray", narrayJson, false);
        newRow.putMapAsJson("nestedmap", nmapJson, false);
        newRow.putRecordAsJson("nestedrecord", nrecordJson, false);

        ((RecordValueImpl)record1).addMissingFields();
        ((RecordValueImpl)record2).addMissingFields();
        ((RecordValueImpl)nrecord).addMissingFields();

        assertTrue(newRow.equals(originalRow));
    }

    /**
     * Tests  Row.toJsonString(false);
     */
    @Test
    public void testJsonFast() {

        /*
         * Top-level table
         */
        createUserTable();

        /*
         * Create and initialize a row.  Lots to do for this row.
         */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe");
        row.put("lastName", "doe");
        row.put("age", 76);
        row.put("bin", new byte[] {1, 2, 3, 4, 5});
        row.putFixed("fixedBin", new byte[] {68, 69, 70});
        row.put("bool", true);
        row.put("birthday", new Timestamp(0));
        ArrayValue array = row.putArray("likes");
        array.add(new String[] {"a", "b", "c"});

        RecordValue record = row.putRecord("address");
        record.put("street", "17 Happy Lane");
        record.put("city", "Whoville");
        record.put("zip", 12345);
        record.putEnum("type", "home");

        MapValue map = row.putMap("friends");
        map.put("best", "joe");
        map.put("sister", "jane");
        map.put("friend", "annie");

        ArrayValue array1 = row.putArray("array_of_maps");
        for (long l = 0; l < 5L; l++) {
            MapValue map1 = array1.addMap();
            RecordValue record1 = map1.putRecord("record");
            record1.put("a", "a_string");
            record1.put("d", 7.567);
            record1.put("l", l);
        }

        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(true);
        Row newRow = userTable.createRowFromJson(jsonString, false);
        assertTrue(newRow.equals(row));
    }


    /**
     * Tests  Row.toJsonString(false);
     */
    @Test
    public void testJsonFastEmptyArray() {

        /*
         * Top-level table
         */
        createUserTable();

        /*
         * Create and initialize a row.  Lots to do for this row.
         */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe");
        row.put("lastName", "doe");
        row.put("age", 76);
        row.put("bin", new byte[] {1, 2, 3, 4, 5});
        row.putFixed("fixedBin", new byte[] {68, 69, 70});
        row.put("bool", true);
        row.put("birthday", new Timestamp(0));
        ArrayValue array = row.putArray("likes");
        array.add(new String[] {"a", "b", "c"});

        RecordValue record = row.putRecord("address");
        record.put("street", "17 Happy Lane");
        record.put("city", "Whoville");
        record.put("zip", 12345);
        record.putEnum("type", "home");

        MapValue map = row.putMap("friends");
        map.put("best", "joe");
        map.put("sister", "jane");
        map.put("friend", "annie");

        row.putArray("array_of_maps");

        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(true);
        Row newRow = userTable.createRowFromJson(jsonString, false);
        assertTrue(newRow.equals(row));
    }


    /**
     * Tests  Row.toJsonString(false);
     */
    @Test
    public void testJsonFastEmptyMap() {

        /*
         * Top-level table
         */
        createUserTable();

        /*
         * Create and initialize a row.  Lots to do for this row.
         */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe");
        row.put("lastName", "doe");
        row.put("age", 76);
        row.put("bin", new byte[] {1, 2, 3, 4, 5});
        row.putFixed("fixedBin", new byte[] {68, 69, 70});
        row.put("bool", true);
        row.put("birthday", new Timestamp(0));
        ArrayValue array = row.putArray("likes");
        array.add(new String[] {"a", "b", "c"});

        row.putRecord("address");
        /* leave record empty on purpose */

        /*MapValue map = */row.putMap("friends");
        /* leave map empty on purpose */

        ArrayValue array1 = row.putArray("array_of_maps");
        for (long l = 0; l < 5L; l++) {
            MapValue map1 = array1.addMap();
            RecordValue record1 = map1.putRecord("record");
            record1.put("a", "a_string");
            record1.put("d", 7.567);
            record1.put("l", l);
        }

        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(true);
        Row newRow = userTable.createRowFromJson(jsonString, false);
        assertTrue(newRow.equals(row));
    }


    /**
     * Tests  Row.toJsonString(false);
     */
    @Test
    public void testJsonFastNullValues() {

        /*
         * Top-level table
         */
        createUserTable();

        /*
         * Create and initialize a row.  Lots to do for this row.
         */
        Row row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "joe");
        row.put("lastName", "doe");
        row.put("age", 76);
        row.put("bin", new byte[]{1, 2, 3, 4, 5});
        row.putFixed("fixedBin", new byte[]{68, 69, 70});
        row.put("bool", true);
        row.put("birthday", new Timestamp(0));
        ArrayValue array = row.putArray("likes");
        array.add(new String[]{"a", "b", "c"});

        RecordValue record = row.putRecord("address");
        record.putNull("street");
        record.put("city", "Whoville");
        record.put("zip", 12345);
        record.putEnum("type", "home");

        row.putNull("friends");

        row.putNull("array_of_maps");

        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(true);
        Row newRow = userTable.createRowFromJson(jsonString, false);
        assertTrue(newRow.equals(row));
    }

    @Test
    public void testTimestamp() {
        TableImpl table = createTableIncludingTimestamps();
        Timestamp ts = new Timestamp(0);
        ts.setNanos(987654321);
        Row row = table.createRow();
        row.put("id", 1);
        row.put("ts0", ts);
        row.put("ts1", ts);
        row.put("ts2", ts);
        row.put("ts3", ts);

        ArrayValue array = row.putArray("array_ts0");
        array.add(ts);
        array.add(new Timestamp(ts.getNanos() + 1000));
        array.add(new Timestamp(ts.getNanos() + 2000));

        MapValue map = row.putMap("map_ts1");
        map.put("key1", ts);
        map.put("key2", new Timestamp(ts.getNanos() + 1000));
        map.put("key3", new Timestamp(ts.getNanos() + 2000));

        RecordValue record = row.putRecord("record_ts2_ts3");
        record.put("ts2", ts);
        record.put("ts3", ts);
        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(false);
        Row newRow = table.createRowFromJson(jsonString, false);
        assertTrue(newRow.equals(row));
    }

    @Test
    public void testNumber() {
        TableImpl table = createTableDecimals();
        Row row = table.createRow();
        row.put("id", 1);
        row.putNumber("intVal", 0);
        row.putNumber("longVal", 12345678901234567L);
        row.putNumber("floatVal", 132.11f);
        row.putNumber("doubleVal", -1.234567890123456E-100d);
        row.putNumber("decimalVal",
                new BigDecimal("-1.234567890123456789012345678901234567E1024"));

        ArrayValue array = row.putArray("array");
        array.addNumber(0);
        array.addNumber(new BigDecimal("1234567890.123456789"));
        array.addNumber(new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE),
                                 Integer.MAX_VALUE));

        MapValue map = row.putMap("map");
        map.putNumber("key1", 0);
        map.putNumber("key2", 1234567890.123456789d);
        map.putNumber("key3",
                      new BigDecimal("-12345678901234567890.123456789"));

        RecordValue record = row.putRecord("rec");
        record.put("rid", 1);
        record.putNumber("rdec",
                         new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE),
                                        Integer.MAX_VALUE));
        /*
         * Done with row.  Turn it to JSON, reverse row construction and
         * compare results.
         */
        String jsonString = row.toJsonString(true);
        Row newRow = table.createRowFromJson(jsonString, true);
        assertTrue(newRow.equals(row));

        jsonString = "{\"id\":1," +
                      "\"intVal\":0," +
                      "\"longVal\":12345678901234567," +
                      "\"floatVal\":132.11," +
                      "\"doubleVal\":-1.234567890123456E-100," +
                      "\"decimalVal\":-1.2345678901234567890123456789E+1024," +
                      "\"array\":[0," +
                                 "1234567890.123456789," +
                                 "9.223372036854775807E-2147483629]," +
                      "\"map\":{\"key1\":1E+3," +
                               "\"key2\":1234567890.123456789," +
                               "\"key3\":-12345678901234567890.123456789}," +
                      "\"rec\":{\"rid\":1," +
                               "\"rdec\":9.223372036854775807E-2147483629}}";
        row = table.createRowFromJson(jsonString, true);
        assertTrue(row.toJsonString(false).equals(jsonString));
    }

    /**
     * Test the parsing BigDecimal value in JSON.
     */
    @Test
    public void testParseBigDecimalValue() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
                .addInteger("id")
                .addNumber("num")
                .addField("a", TableBuilder.createArrayBuilder()
                                  .addNumber().build())
                .addField("m", TableBuilder.createMapBuilder()
                                  .addNumber().build())
                .addField("r", TableBuilder.createRecordBuilder("r")
                                  .addNumber("n").build())
                .primaryKey("id")
                .buildTable();

        String[] jsons = new String[] {
            "{\"id\":0,\"num\":%s}",
            "{\"id\":1,\"a\":[%s]}",
            "{\"id\":2,\"m\":{\"k1\":%s}}",
            "{\"id\":3,\"r\":{\"n\":%s}}",
        };

        String[] validDoubles = new String[] {
            "1.7976931348623157e+308"     /* Double.MAX_VALUE */,
            "4.9e-324"                    /* Double.MIN_VALUE */,
            "123132132.13213144315415",
            "0.0",
        };

        String[] validBigDecimals = new String[] {
            "1.7976931348623157e+309"     /* > Double.MAX_VALUE */,
            "4.9e-325"                    /* < Double.MIN_VALUE */,
            "1e-2147483647",
            "1e+2147483647",
            "2.67499999999999982236431605997495353221893310546875",
            genLongNumber(100),
        };

        String[] invalidBigDecimals = new String[] {
            "1e+2147483649",
            "1e-2147483649",
        };

        List<String> valid = new ArrayList<String>();
        valid.addAll(Arrays.asList(validDoubles));
        valid.addAll(Arrays.asList(validBigDecimals));

        /* Test parsing decimal value for a row */
        for (String val : valid) {
            for (String json : jsons) {
                String s = String.format(json, val);
                Row row = table.createRowFromJson(s, false);
                assertTrue(row != null);

                FieldValue fv = null;
                if (row.get("num") != null) {
                    fv = row.get("num");
                } else if (row.get("a") != null) {
                    fv = row.get("a").asArray().get(0);
                } else if (row.get("m") != null) {
                    fv = row.get("m").asMap().get("k1");
                } else if (row.get("r") != null) {
                    fv = row.get("r").asRecord().get("n");
                }

                BigDecimal exp = new BigDecimal(val);
                assertTrue(fv != null &&
                           fv.asNumber().get().compareTo(exp) == 0);
            }
        }

        for (String val : invalidBigDecimals) {
            for (String json : jsons) {
                String s = String.format(json, val);
                try {
                    table.createRowFromJson(s, false);
                    fail("Expect to catch exception but not");
                } catch (IllegalArgumentException iae) {
                    assertTrue(iae.getMessage()
                               .contains("Malformed numeric value"));
                }
            }
        }

        /* Test parsing decimal value for JSON type field */
        table = TableBuilder.createTableBuilder("test", null, null)
                .addInteger("id")
                .addJson("json", null)
                .primaryKey("id")
                .buildTable();

        jsons = new String[] {
            "{\"id\":0,\"json\":%s}",
            "{\"id\":1,\"json\":{\"a\":[%s]}}",
            "{\"id\":2,\"json\":{\"m\":{\"k1\":%s}}}",
            "{\"id\":3,\"json\":{\"r\":{\"n\":%s}}}",
        };

        List<String> validDoublesWithNaN =
            new ArrayList<String>(Arrays.asList(validDoubles));
        validDoublesWithNaN.add("NaN");
        validDoublesWithNaN.add("Infinity");
        validDoublesWithNaN.add("-Infinity");
        for (String val : validDoublesWithNaN) {
            for (String json : jsons) {
                String s = String.format(json, val);
                Row row = table.createRowFromJson(s, false);
                assertTrue(row != null);

                FieldValue fv = null;
                if (row.get("json").isNumeric()) {
                    fv = row.get("json");
                } else {
                    MapValue jsonVal = row.get("json").asMap();
                    if (jsonVal.get("a") != null) {
                        fv = jsonVal.get("a").asArray().get(0);
                    } else if (jsonVal.get("m") != null) {
                        fv = jsonVal.get("m").asMap().get("k1");
                    } else if (jsonVal.get("r") != null) {
                        fv = jsonVal.get("r").asMap().get("n");
                    }
                }

                assertTrue(fv != null);
                if (val.equals("NaN")) {
                    assertTrue(fv.isDouble() &&
                               Double.valueOf(fv.asDouble().get()).isNaN());
                } else if (val.contains("Infinity")) {
                    assertTrue(fv.isDouble() &&
                               Double.valueOf(fv.asDouble().get()).isInfinite());
                } else {
                    BigDecimal exp = new BigDecimal(val);
                    if (fv.isDouble()) {
                        double dbl = fv.asDouble().get();
                        assertTrue(exp.compareTo(BigDecimal.valueOf(dbl)) == 0);
                    } else {
                        assertTrue(fv.isNumber());
                        assertTrue(exp.compareTo(fv.asNumber().get()) == 0);
                    }
                }
            }
        }

        for (String val : validBigDecimals) {
            for (String json : jsons) {
                String s = String.format(json, val);
                Row row = table.createRowFromJson(s, false);
                assertTrue(row != null);

                FieldValue fv = null;
                if (row.get("json").isNumeric()) {
                    fv = row.get("json");
                } else {
                    MapValue jsonVal = row.get("json").asMap();
                    if (jsonVal.get("a") != null) {
                        fv = jsonVal.get("a").asArray().get(0);
                    } else if (jsonVal.get("m") != null) {
                        fv = jsonVal.get("m").asMap().get("k1");
                    } else if (jsonVal.get("r") != null) {
                        fv = jsonVal.get("r").asMap().get("n");
                    }
                }

                assertTrue(fv != null && fv.isNumber());
                assertTrue(new BigDecimal(val).equals(fv.asNumber().get()));
            }
        }

        for (String val : invalidBigDecimals) {
            for (String json : jsons) {
                String s = String.format(json, val);
                try {
                    table.createRowFromJson(s, false);
                    fail("Expect to catch exception but not");
                } catch (IllegalArgumentException iae) {
                    assertTrue(iae.getMessage()
                                   .contains("Malformed numeric value"));
                }
            }
        }

        /* Test FieldValueFactory.createValueFromJson() for number type */
        for (String val : valid) {
            FieldValue fv1 = FieldValueFactory.createValueFromJson
                    (FieldDefImpl.Constants.numberDef, val);
            FieldValue fv2 = null;
            try {
                fv2 = FieldValueFactory.createValueFromJson
                        (FieldDefImpl.Constants.numberDef,
                         new StringReader(val));
            } catch (IOException ioe) {
                fail("Failed to create number value from json string: " + val);
            }

            FieldValue fv3 = null;
            try {
                fv3 = FieldValueFactory.createValueFromJson
                        (FieldDefImpl.Constants.numberDef,
                         new ByteArrayInputStream(val.getBytes("UTF-8")));
            } catch (IOException ioe) {
                fail("Failed to create number value from json string: " + val);
            }

            assertTrue(fv1 != null && fv2 != null && fv3 != null);
            assertTrue(fv1.equals(fv2) && fv2.equals(fv3));
            assertTrue(fv1.asNumber().get().compareTo(new BigDecimal(val)) == 0);
        }

        for (String val : invalidBigDecimals) {
            try {
                FieldValueFactory.createValueFromJson(
                    FieldDefImpl.Constants.numberDef, val);
                fail("Expect to catch exception but not");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("Malformed numeric value"));
            }

            try {
                FieldValueFactory.createValueFromJson(
                    FieldDefImpl.Constants.numberDef, new StringReader(val));
                fail("Expect to catch exception but not");
            } catch (IOException e) {
                assertTrue(e.getMessage().contains("Malformed numeric value"));
            }

            try {
                FieldValueFactory.createValueFromJson
                    (FieldDefImpl.Constants.numberDef,
                     new ByteArrayInputStream(val.getBytes("UTF-8")));
                fail("Expect to catch exception but not");
            } catch (IOException e) {
                assertTrue(e.getMessage().contains("Malformed numeric value"));
            }
        }
    }

    @Test
    public void testEmtryJsonString() {
        /*
         * Top-level table
         */
        TableImpl table = TableBuilder.createTableBuilder("table")
                .addInteger("id")
                .addJson("json", null)
                .addField("rec", TableBuilder.createRecordBuilder("rec")
                                             .addInteger("rid")
                                             .addString("rs").build())
                .addField("m", TableBuilder.createMapBuilder("m")
                                           .addString().build())
                .addField("a", TableBuilder.createArrayBuilder("a")
                                           .addInteger().build())
                .primaryKey("id")
                .buildTable();


        FieldDef recDef = table.getField("rec");
        FieldDef mapDef = table.getField("m");
        FieldDef arrayDef = table.getField("a");

        FieldValue fv;

        String[] emptyJsons = new String[] {
            "    ",
            "",
            "{}",
            " {}"
        };
        for (String s : emptyJsons) {
            assertTrue(table.createRowFromJson(s, false).isEmpty());
        }

        emptyJsons = new String[] {
            "{\"id\":1,\"rec\": {}}",
            "{\"id\":2,\"rec\":   {       }}",
        };
        for (String s : emptyJsons) {
            Row row = table.createRowFromJson(s, false);
            RecordValue rv = row.get("rec").asRecord();
            assertTrue(!rv.isNull() && rv.isEmpty());
        }

        emptyJsons = new String[] {
            "{\"id\":1,\"m\": {}}",
            "{\"id\":2,\"m\":   {       }}",
        };
        for (String s : emptyJsons) {
            Row row = table.createRowFromJson(s, false);
            MapValue mv = row.get("m").asMap();
            assertTrue(!mv.isNull() && mv.size() == 0);
        }

        emptyJsons = new String[] {
            "{\"id\":1,\"a\": []}",
            "{\"id\":2,\"a\":   [       ]}",
        };
        for (String json : emptyJsons) {
            Row row = table.createRowFromJson(json, false);
            ArrayValue av = row.get("a").asArray();
            assertTrue(!av.isNull() && av.size() == 0);
        }

        emptyJsons = new String[] {
            "    ",
            "",
            "{}",
            "   {   }"
        };
        for (String s : emptyJsons) {
            fv = FieldValueFactory.createValueFromJson(recDef, s);
            assertTrue(!fv.isNull());
            if (s.trim().isEmpty()) {
                assertTrue(fv.asRecord().isEmpty());
            }
        }

        String[] noneEmptyJsons = new String[] {
            " {\"rid\":1}",
            "     {  \"rs\"  :  \"s\"  }"
        };
        for (String s : noneEmptyJsons) {
            fv = FieldValueFactory.createValueFromJson(recDef, s);
            assertTrue(!fv.isNull() && !fv.asRecord().isEmpty());
        }

        emptyJsons = new String[] {
            "",
            "   ",
            "[]",
            "   [   ]"
        };
        for (String s : emptyJsons) {
            fv = FieldValueFactory.createValueFromJson(arrayDef, s);
            assertTrue(!fv.isNull() && fv.asArray().size() == 0);
        }

        noneEmptyJsons = new String[] {
            " [1, 2]",
            "      [   1]",
        };
        for (String s : noneEmptyJsons) {
            fv = FieldValueFactory.createValueFromJson(arrayDef, s);
            assertTrue(!fv.isNull() && fv.asArray().size() > 0);
        }

        emptyJsons = new String[] {
            "    ",
            "",
            "{}",
            "   {   }"
        };
        for (String s : emptyJsons) {
            fv = FieldValueFactory.createValueFromJson(mapDef, s);
            assertTrue(!fv.isNull() && fv.asMap().size() == 0);
        }

        noneEmptyJsons = new String[] {
            " {\"k1\":\"v1\"}",
            "     {  \"k1\"  :  \"v1\"  }"
        };
        for (String s : noneEmptyJsons) {
            fv = FieldValueFactory.createValueFromJson(mapDef, s);
            assertTrue(!fv.isNull() && fv.asMap().size() > 0);
        }
    }

    @Test
    public void testDoubleNaN() {
        TableImpl table = TableBuilder.createTableBuilder("foo")
                .addInteger("id")
                .addDouble("val")
                .primaryKey("id")
                .buildTable();

        double[] values = new double[] {
                Double.NaN,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.MAX_VALUE,
                Double.MIN_VALUE
        };
        int i = 0;
        for (double value : values) {
            Row row = table.createRow();
            row.put("id", i++);
            row.put("val", value);

            /* Create row from pretty printing JSON */
            String json = row.toJsonString(true);
            Row row1 = table.createRowFromJson(json, true);
            assertTrue(row.equals(row1));

            /* Create row from single-line JSON */
            json = row.toJsonString(true);
            row1 = table.createRowFromJson(json, true);
            assertTrue(row.equals(row1));
        }
    }

    private String genLongNumber(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append("1");
        }
        return sb.toString();
    }

    /**
     * Create a table including timestamp fields.
     */
    private TableImpl createTableIncludingTimestamps() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
            .addInteger("id")
            .addTimestamp("ts0", 0)
            .addTimestamp("ts1", 3)
            .addTimestamp("ts2", 6)
            .addTimestamp("ts3", 9)
            .addField("array_ts0",
                      TableBuilder.createArrayBuilder("array_ts0")
                                  .addTimestamp(0).build())
            .addField("map_ts1",
                      TableBuilder.createMapBuilder("map_ts1")
                      .addTimestamp(1).build())
            .addField("record_ts2_ts3",
                      TableBuilder.createRecordBuilder("record_ts2_ts3")
                      .addTimestamp("ts2", 2)
                      .addTimestamp("ts3", 4)
                      .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        return table;
    }

    /**
     * Create a table including timestamp fields.
     */
    private TableImpl createTableDecimals() {
        TableImpl table = TableBuilder.createTableBuilder("test", null, null)
            .addInteger("id")
            .addNumber("intVal")
            .addNumber("longVal")
            .addNumber("floatVal")
            .addNumber("doubleVal")
            .addNumber("decimalVal")
            .addField("array",
                      TableBuilder.createArrayBuilder("array_decimal")
                                  .addNumber().build())
            .addField("map",
                      TableBuilder.createMapBuilder("map_decimal")
                      .addNumber().build())
            .addField("rec",
                      TableBuilder.createRecordBuilder("record_ts2_ts3")
                      .addInteger("rid")
                      .addNumber("rdec")
                      .build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        return table;
    }
}
