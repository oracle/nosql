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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.EntryStream;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MapValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.Test;

/*
 * JSON type tests that use persistent tables
 */
public class JsonTableTest extends TableTestBase {

    /*
     * Miscellaneous simple tests.
     */
    @Test
    public void testMisc()
        throws Exception {

        final String jsonTableQuery =
            "create table JsonTable(id integer, json JSON, primary key(id))";

        /*
         * A number of valid JSON strings to use for the JSON field
         */
        final String json1 = "1";
        final String json2 = "\"a string\"";
        final String json3 = "17.56";
        final String json4 = "{}";
        final String json5 = "[]";
        final String json6 = "{\"a\": 1, \"b\": 2, \"map\":{\"m1\":6}}";
        final String json7 = "[1,2.7,3]";
        final String json8 = "null";
        final String json9 = "{\"a\": 1, \"b\": null, \"bool\": true," +
            "\"map\": {\"m1\": 5}, \"ar\" : [1,2.7,3]}";
        final String json10 = "{\"a\": 9234567890123456}";

        /*
         * TODO: add additional, and more complex documents and some invalid
         * documents. See JsonTypeTest as well.
         */
        final String[] jsonDocs = {json1, json2, json3, json4, json5, json6,
                                   json7, json8, json9, json10};

        executeDdl(jsonTableQuery);
        TableImpl jsonTable = getTable("JsonTable");

        Row row = jsonTable.createRow();
        row.put("id", 1);
        row.put("json", 8);
        SerializationTest.roundTrip(row);

        tableImpl.put(row, null, null);
        Row newRow = tableImpl.get(jsonTable.createPrimaryKey(row), null);

        /*
         * Put a number of different JSON docs into the field and test
         * round-tripping
         */
        for (String json : jsonDocs) {
            row.putJson("json", json);
            SerializationTest.roundTrip(row);
            tableImpl.put(row, null, null);
            newRow = tableImpl.get(jsonTable.createPrimaryKey(row), null);
            assertEquals(row, newRow);
        }

        /*
         * Test that an attempt to specify a default value for a JSON field
         * fails.
         */
        final String jsonWithDefault =
            "create table BadJson(id integer, json JSON not null default 1," +
            " primary key(id))";
        final String recordNotNull =
            "create table BadRecord(id integer, rec record(name string) not null, " +
            " primary key(id))";
        final String mapNotNull =
            "create table BadRecord(id integer, map map(string) not null, " +
            " primary key(id))";
        final String arrayNotNull =
            "create table BadRecord(id integer, array array(string) not null, " +
            " primary key(id))";

        /*
         * assert failure
         *
         * Think about asserting specific error messages
         */
        executeDdl(jsonWithDefault, false);
        executeDdl(recordNotNull, false);
        executeDdl(mapNotNull, false);
        executeDdl(arrayNotNull, false);
    }

    /**
     * Test maps and arrays of JSON, which are handled differently from
     * JSON that happens to represent objects and arrays. These are fixed-type
     * fields where the type happens to be JSON, allowing any valid JSON to
     * be inserted.
     */
    @Test
    public void testMapAndArray() throws Exception {
        final String jsonTableQuery =
            "create table JsonTable(id integer, arrayJ array(JSON), " +
            "mapJ map(JSON), primary key(id))";

        executeDdl(jsonTableQuery);
        TableImpl jsonTable = getTable("JsonTable");

        Row row = jsonTable.createRow();
        row.put("id", 1);
        ArrayValue array = row.putArray("arrayJ");
        for (int i = 0; i < 20; i++) {
            array.addJson("{\"a\": 1}");
            array.addJson("{\"a\": \"abcde\"}");
            array.addJson("\"abc\"");
            array.addJson("null");
        }

        MapValue map = row.putMap("mapJ");
        for (int i = 0; i < 20; i++) {
            map.putJson("a", "{\"a\": 1}");
            map.putJson("b", "{\"a\": \"abcde\"}");
            map.putJson("c", "\"abc\"");
            map.putJson("d", "null");
        }

        SerializationTest.roundTrip(row);
        tableImpl.put(row, null, null);
        Row newRow = tableImpl.get(jsonTable.createPrimaryKey(row), null);
        assertTrue(row.equals(newRow));
    }

    /*
     * Index tests
     */

    /**
     * Tests a generic JSON index, no type declarations
     * TODO:
     *  o lots more edge case testing, range testing and null testing
     *  o when the code to allow index creation to fail gracefully is
     *  integrated, test that feature by putting rows that will cause the
     *  failure because they would have multiple arrays in an index.
     *
     *  NYI - generic JSON index
     */
    //@Test
    public void testJsonIndex()
        throws Exception {

        final String jsonTableQuery =
            "create table JsonTable(id integer, json JSON, primary key(id))";

        final String jsonIndexQuery =
            "create index JsonIndex on JsonTable(json)";

        final String jsonIndexQuery2 =
            "create index JsonNameIndex on JsonTable(json.name as string)";

        executeDdl(jsonTableQuery);
        executeDdl(jsonIndexQuery);
        executeDdl(jsonIndexQuery2);

        TableImpl jsonTable = getTable("JsonTable");
        Index index = jsonTable.getIndex("JsonIndex");
        Row row = jsonTable.createRow();

        /*
         * Put an object in the JSON field.
         */
        row.put("id", 0);

        /*
         * Put several atomics of different types in the JSON field.
         */
        row.put("id", 1);
        row.put("json", 8);
        tableImpl.put(row, null, null);

        row.put("id", 2);
        row.put("json", "foo");
        tableImpl.put(row, null, null);

        row.put("id", 3);
        row.put("json", true);
        tableImpl.put(row, null, null);

        /*
         * Use a JSON null
         */
        row.put("id", 4);
        row.putJson("json", "null");
        tableImpl.put(row, null, null);

        /*
         * Null for the JSON altogether (default for a nullable field)
         */
        row.put("id", 5);
        tableImpl.put(row, null, null);

        TableIteratorOptions options =
            new TableIteratorOptions(Direction.FORWARD, null, 0, null);
        TableIteratorOptions roptions =
            new TableIteratorOptions(Direction.REVERSE, null, 0, null);
        /*
         * Note the sort order. Entries should be ordered by type, all else
         * being equal, with types with smaller ordinals coming first.
         */
        int currentTypeOrd = 0;

        IndexKey ikey = index.createIndexKey();

        TableIterator<Row> iter = tableImpl.tableIterator(ikey, null, roptions);
        Row lastRow = null;
        while (iter.hasNext()) {
            row = iter.next();
            if (lastRow != null) {
                assertTrue(row.compareTo(lastRow) < 0);
            }
            lastRow = row;
        }
        iter.close();

        /*
         * Use JSON null for search, reverse sort
         */
        ikey.putJsonNull("json");
        iter = tableImpl.tableIterator(ikey, null, roptions);
        lastRow = null;
        while (iter.hasNext()) {
            row = iter.next();
            if (lastRow != null) {
                assertTrue(row.compareTo(lastRow) < 0);
            }
            lastRow = row;
        }
        iter.close();

        FieldRange range = index.createFieldRange("json");
        range.setStart(7, true);

        ikey = index.createIndexKey();
        iter = tableImpl.tableIterator(ikey,
                                       range.createMultiRowOptions(),
                                       options);
        /* the iteration should have returned only integers */
        while (iter.hasNext()) {
            row = iter.next();
            for (int i = 0; i < row.size(); i++) {
                assertTrue(row.get(i).isInteger());
            }
        }
        iter.close();

        TableIterator<KeyPair> keyIter = tableImpl.tableKeysIterator(
            index.createIndexKey(), null, options);
        /*
         * Use the ordinal of the current type to assert sort order according to
         * type.
         */
        currentTypeOrd = 0;
        while (keyIter.hasNext()) {
            KeyPair kp = keyIter.next();
            IndexKey ik = kp.getIndexKey();
            FieldValue val = ik.get(0);
            if (val.isNull()) {
                /*
                 * null sorts after values. Set this to greater than any value
                 * to trip the assertion below if there's a problem
                 */
                currentTypeOrd = 30;
            } else if (val.isJsonNull()) {
                /*
                 * json null sorts last. Set this to greater than any null or
                 * value to trip the assertion below if there's a problem
                 */
                currentTypeOrd = 35;
            } else {
                if (val.getType().ordinal() != currentTypeOrd) {
                    assertTrue(val.getType().ordinal() > currentTypeOrd);
                    currentTypeOrd = val.getType().ordinal();
                }
            }
        }
        keyIter.close();
    }

    /*
     * Test type-specific indexes on JSON
     */
    @Test
    public void testJsonTypedIndex()
        throws Exception {

        basicTestJsonTypedIndex();
        runPutWithJsonTypedIndexTest();

        runIterateJsonTypedIndexTest();
        runIterateJsonSubFieldTypedIndexTest();

        runIterateArrayOfJsonTypedIndexTest();
        runIterateArrayOfJsonSubFieldTypedIndexTest();

        runIterateMapOfJsonTypedIndexTest();
        runIterateMapOfJsonSubFieldTypedIndexTest();

        runIterateRecordJsonTypedIndexTest();
        runIterateRecordJsonSubFieldTypedIndexTest();

        runIterateJsonTypedIndexWithNullsTest();
        runIterateJsonSubFieldTypedIndexWithNullsTest();

        runIterateJsonTypedMultiKeyIndexTest();
        runIterateJsonSubFieldTypedMultiKeyIndexTest();
    }

    @SuppressWarnings("deprecation")
    private void basicTestJsonTypedIndex() {
        final String jsonTableQuery =
            "create table JsonTable(id integer, json JSON, primary key(id))";

        final String jsonIndexQuery1 =
            "create index JsonNameIndex on JsonTable(json.name as string)";

        final String jsonIndexQuery2 =
            "create index JsonAgeIndex on JsonTable(json.age as long)";

        final String jsonIndexQuery3 =
            "create index JsonAvgIndex on JsonTable(json.avg as double)";

        final String jsonIndexQuery4 =
            "create index JsonBoolIndex on JsonTable(json.bool as boolean)";

        final String jsonIndexQuery5 =
            "create index JsonNumberIndex on JsonTable(json.number as number)";

        final String jsonIndexQuery6 =
            "create index JsonNameIdIndex on JsonTable(json.name as string, id)";

        /* this will fail because id is not a JSON field */
        final String jsonIndexBad1 =
            "create index Bad1 on JsonTable(id as scalar)";

        executeDdl(jsonTableQuery);
        executeDdl(jsonIndexQuery1);
        executeDdl(jsonIndexQuery2);
        executeDdl(jsonIndexQuery3);
        executeDdl(jsonIndexQuery5);
        executeDdl(jsonIndexQuery6);
        executeDdl(jsonIndexBad1, false); /* should fail */

        /* Save the boolean index for later to test index creation failure */

        TableImpl jsonTable = getTable("JsonTable");

        /*
         * Test round-tripping of the table that includes typed indexes.
         * This is done in TableConstructionTest as well.
         */
        TableImpl newTable =
            TableBuilder.fromJsonString(jsonTable.toJsonString(true),null);
        assertTrue("Tables should be equal", jsonTable.equals(newTable));

        /*
         * Now try some valid, and invalid data in the table
         */
        Row row = jsonTable.createRow();

        /*
         * Success cases
         */

        /*
         * Put an object in the JSON field, including the typed, indexed fields.
         * These will succeed. The integer, 5, will be coerced into a long for
         * indexing purposes.
         */
        row.put("id", 0);
        row.putJson(
            "json",
            "{\"name\":\"joe\", \"age\":5, \"avg\":5.6, \"bool\":true}");

        tableImpl.put(row, null, null);

        assertTrue(row.get("json").asMap().get("age").getType() ==
                   FieldDef.Type.INTEGER);
        assertTrue(row.get("json").asMap().get("avg").getType() ==
                   FieldDef.Type.DOUBLE);

        /* use an actual long value this time */
        row.put("id", 1);
        row.putJson(
            "json",
            "{\"name\":\"joe\", \"age\":55555555555555555, " +
            " \"avg\":0.678, \"bool\":false}");
        tableImpl.put(row, null, null);

        assertTrue(row.get("json").asMap().get("age").getType() ==
                   FieldDef.Type.LONG);

        /* use a Number value */
        row.put("id", 2);
        row.putJson(
            "json",
            "{\"name\":\"joe\", \"number\":55555555555555555778888888888, " +
            " \"age\":3, \"avg\":0.678, \"bool\":false}");
        tableImpl.put(row, null, null);

        assertTrue(row.get("json").asMap().get("number").getType() ==
                   FieldDef.Type.NUMBER);

        /*
         * Failure cases
         */

        /*
         * Attempt to put a non-string in a field with an explicit string index
         */
        row.put("id", 0);
        row.putJson("json", "{\"name\": 1}");
        try {
            tableImpl.put(row, null, null);
            fail("constraint (string) should have failed");
        } catch (Exception e) {} // success

        /*
         * Use an invalid number.
         */
        row.putJson("json", "{\"age\": 1.5}");
        try {
            tableImpl.put(row, null, null);
            fail("constraint (long) should have failed");
        } catch (Exception e) {} // success

        /*
         * Use an invalid double.
         */
        row.putJson("json", "{\"avg\": 1111111111111111111111111111111111111111111}");
        try {
            tableImpl.put(row, null, null);
            fail("constraint (double) should have failed");
        } catch (Exception e) {} // success

        /*
         * Look at some rows obtained using the age index and compare
         * types.
         */
        Index index = jsonTable.getIndex("JsonAgeIndex");
        IndexKey ikey = index.createIndexKey();
        TableIterator<Row> iter = tableImpl.tableIterator(ikey, null, null);
        IndexKey prev = null;
        while (iter.hasNext()) {
            row = iter.next();
            FieldDef.Type ageType = row.get("json").asMap().get("age").getType();
            assertTrue(ageType == FieldDef.Type.LONG ||
                       ageType == FieldDef.Type.INTEGER);

            ikey = index.createIndexKey(row);
            if (prev != null) {
                assertTrue(ikey.compareTo(prev) >= 0);
            }
            prev = (IndexKey)ikey.clone();
        }
        iter.close();

        /*
         * Assert there is at least one entry in the number index
         */
        index = jsonTable.getIndex("JsonNumberIndex");
        assertTrue(countIndexRows(index.createIndexKey(), jsonTable) > 0);

        /*
         * Put an invalid boolean and try to create the boolean index
         */
        row.put("id", 4);
        row.putJson("json", "{\"bool\": 1}");
        tableImpl.put(row, null, null);

        /* An attempt to create an index with the bad boolean will fail */
        executeDdl(jsonIndexQuery4, false);

        /* Fix the row and create the index */
        row.putJson("json", "{\"bool\": true}");
        tableImpl.put(row, null, null);
        executeDdl(jsonIndexQuery4);

        /* Put the bad boolean back and verify that an attempt to put fails */
        row.putJson("json", "{\"bool\": 1}");
        try {
            tableImpl.put(row, null, null);
            fail("constraint (boolean) should have failed");
        } catch (Exception e) {} // success

        executeDdl("drop table " + jsonTable.getFullName());
    }

    /**
     * Test creating index on table with records and putting record to table
     * with Json typed index, both putting row or attempting to create index
     * may fail if breaks the type constraint.
     */
    private void runPutWithJsonTypedIndexTest() {

        final String jsonTableQuery =
            "create table JsonTable(id integer, json JSON, primary key(id))";
        executeDdl(jsonTableQuery);
        TableImpl jsonTable = getTable("JsonTable");

        /* String type */
        String jsonIndexStmt =
            "create index JsonNameIndex on JsonTable(json.name as string)";

        String[] failedJsons = new String[] {
            "{\"name\":1}",
            "{\"name\":1.1}",
            "{\"name\":false}",
            "{\"name\":99999999999999999999}",
            "[\"a\"]"
        };
        String[] validJsons = new String[] {
            "{\"name\":\"joe\"}",
            "{\"name\":\"long long long long long long long long string\"}",
            "{\"name\":\"\"}"
        };
        String[] nullJsons = new String[] {
            "{\"name\":null}",
            "{}",
            "null",
        };

        putWithJsonTypedIndex(jsonTable, jsonIndexStmt, "name",
                              failedJsons, validJsons, nullJsons,
                              FieldDef.Type.STRING);

        /* Long type */
        jsonIndexStmt =
            "create index JsonAgeIndex on JsonTable(json.age as long)";
        failedJsons = new String[] {
            "{\"age\":\"joe\"}",
            "{\"age\":1.1}",
            "{\"age\":false}",
            "{\"age\":99999999999999999999}",
            "{\"age\":{}}",
            "{\"age\":[1,2]}"
        };
        validJsons = new String[] {
            "{\"age\":30}",
            "{\"age\":0}",
            "{\"age\":" + Long.MIN_VALUE + "}",
            "{\"age\":" + Long.MAX_VALUE + "}"
        };

        nullJsons = new String[] {
            "{\"age\":null}",
            "{}",
            "null",
        };

        putWithJsonTypedIndex(jsonTable, jsonIndexStmt, "age",
                              failedJsons, validJsons, nullJsons,
                              FieldDef.Type.LONG);

        /* Double type */
        jsonIndexStmt =
            "create index JsonAvgIndex on JsonTable(json.avg as double)";

        failedJsons = new String[] {
            "{\"avg\":\"joe\"}",
            "{\"avg\":false}",
            "{\"avg\":99999999999999999999}",
            "{\"avg\":{}}",
            "{\"avg\":[1,2]}"
        };
        validJsons = new String[] {
            "{\"avg\":12314.1231}",
            "{\"avg\":0.0}",
            "{\"avg\":" + Double.MIN_VALUE + "}",
            "{\"avg\":" + Double.MAX_VALUE + "}",
            "{\"avg\":" + -Double.MIN_VALUE + "}",
            "{\"avg\":" + -Double.MAX_VALUE + "}",
        };
        nullJsons = new String[] {
            "{\"avg\":null}",
            "{}",
            "null",
        };

        putWithJsonTypedIndex(jsonTable, jsonIndexStmt, "avg",
                              failedJsons, validJsons, nullJsons,
                              FieldDef.Type.DOUBLE);

        /* Number type */
        jsonIndexStmt =
            "create index JsonNumberIndex on JsonTable(json.number as number)";
        failedJsons = new String[] {
            "{\"number\":\"joe\"}",
            "{\"number\":false}",
            "{\"number\":{}}",
            "{\"number\":[1,2]}"
        };
        validJsons = new String[] {
            "{\"number\":999999999999999999991231241414214214214214}",
            "{\"number\":0.0}",
            "{\"number\":" + Long.MIN_VALUE + "}",
            "{\"number\":" + Long.MAX_VALUE + "}",
            "{\"number\":" + Double.MIN_VALUE + "}",
            "{\"number\":" + Double.MAX_VALUE + "}"
        };
        nullJsons = new String[] {
            "{\"number\":null}",
            "{}",
            "null",
        };

        putWithJsonTypedIndex(jsonTable, jsonIndexStmt, "number",
                              failedJsons, validJsons, nullJsons,
                              FieldDef.Type.NUMBER);

        /* Boolean type */
        jsonIndexStmt =
            "create index JsonBoolIndex on JsonTable(json.bool as boolean)";
        failedJsons = new String[] {
            "{\"bool\":\"joe\"}",
            "{\"bool\":1231}",
            "{\"bool\":1.1}",
            "{\"bool\":{}}",
            "{\"bool\":[1,2]}"
        };
        validJsons = new String[] {
            "{\"bool\":false}",
            "{\"bool\":true}"
        };
        nullJsons = new String[] {
            "{\"bool\":null}",
            "{}",
            "null",
        };
        putWithJsonTypedIndex(jsonTable, jsonIndexStmt, "bool",
                              failedJsons, validJsons, nullJsons,
                              FieldDef.Type.BOOLEAN);

        executeDdl("drop table " + jsonTable.getFullName());
    }

    private void putWithJsonTypedIndex(TableImpl jsonTable,
                                       String jsonIndexStmt,
                                       String indexField,
                                       String[] failedJsons,
                                       String[] validJsons,
                                       String[] nullJsons,
                                       FieldDef.Type type) {

        Row row = jsonTable.createRow();

        for (String s : failedJsons) {
            row.put("id", 0).putJson("json", s);
            tableImpl.put(row, null, null);
            executeDdl(jsonIndexStmt, false); /* should fail */
        }

        for (int i = 0; i < validJsons.length; i++) {
            row.put("id", i);
            row.putJson("json", validJsons[i]);
            tableImpl.put(row, null, null);
            row = tableImpl.get(jsonTable.createPrimaryKey(row), null);
            FieldValue val = row.get("json").asMap().get(indexField);
            if (type == FieldDef.Type.NUMBER) {
                assertTrue(val.isNumeric());
            } else if (type == FieldDef.Type.LONG) {
                FieldDef.Type valType = val.getType();
                assertTrue(valType == FieldDef.Type.LONG ||
                           valType == FieldDef.Type.INTEGER);
            } else {
                assertTrue(val.getType() == type);
            }
        }

        for (int i = 0; i < nullJsons.length; i++) {
            row.put("id", 100 + i);
            row.putJson("json", nullJsons[i]);
            tableImpl.put(row, null, null);
        }

        executeDdl(jsonIndexStmt); /* should succeed */
        jsonTable = getTable(jsonTable.getFullName());

        PrimaryKey key = jsonTable.createPrimaryKey();
        key.put("id", 1);
        Row savedRow = tableImpl.get(key, new ReadOptions(Consistency.ABSOLUTE,
                                                          0, null));
        for (String s : failedJsons) {
            row.put("id", 1);
            row.putJson("json", s);
            try {
                tableImpl.put(row, null, null);
                fail("constraint (" + type + ") should have failed");
            } catch (Exception e) {
            } //succeed

            try {
                ReturnRow rrow = jsonTable.createReturnRow(Choice.ALL);
                tableImpl.put(row, rrow, null);
                fail("constraint (" + type + ") should have failed");
            } catch (Exception e) {
            } //succeed

            try {
                tableImpl.putIfPresent(row, null, null);
                fail("constraint (" + type + ") should have failed");
            } catch (Exception e) {
            } //succeed

            try {
                tableImpl.putIfVersion(row, savedRow.getVersion(), null, null);
                fail("constraint (" + type + ") should have failed");
            } catch (Exception e) {
            } //succeed

            try {
                row.put("id", 200);
                tableImpl.putIfAbsent(row, null, null);
                fail("constraint (" + type + ") should have failed");
            } catch (Exception e) {
            } //succeed

            try {
                EntryStream<Row> stream = new TestStream(row);
                tableImpl.put(Collections.singletonList(stream), null);
                fail("constraint (" + type + ") should have failed");
            } catch (Exception e) {
            } //succeed
        }
    }

    private void runIterateJsonTypedIndexTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, json json, primary key(id))";
        final String createIndexJsonLong =
            "create index idxJson on jsonTable(json as long)";
        final String createIndexJsonDouble =
            "create index idxJson on jsonTable(json as double)";
        final String createIndexJsonNumber =
            "create index idxJson on jsonTable(json as number)";
        final String createIndexJsonString =
            "create index idxJson on jsonTable(json as string)";
        final String createIndexJsonBoolean =
            "create index idxJson on jsonTable(json as boolean)";
        final String dropIndex = "drop index idxJson on jsonTable";

        executeDdl(createTableDDL);
        final Table table = getTable("jsonTable");
        final int nRows = 100;

        /* Long type */
        FieldDef.Type type = FieldDef.Type.LONG;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonLong);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Double type */
        type = FieldDef.Type.DOUBLE;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonDouble);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Number type */
        type = FieldDef.Type.NUMBER;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonNumber);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* String type */
        type = FieldDef.Type.STRING;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonString);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Boolean type */
        type = FieldDef.Type.BOOLEAN;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonBoolean);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        executeDdl("drop table jsonTable");
    }

    private void runIterateJsonSubFieldTypedIndexTest() {

        final String createTableDDL = "create table jsonTable (" +
            "id integer, json json, primary key(id))";
        final String createIndexJsonLong = "create index " +
            "idxJsonSubAsLong on jsonTable(json.long as long)";
        final String createIndexJsonDouble = "create index " +
            "idxJsonSubAsDouble on jsonTable(json.double as double)";
        final String createIndexJsonNumber = "create index " +
            "idxJsonSubAsNumber on jsonTable(json.number as number)";
        final String createIndexJsonString = "create index " +
            "idxJsonSubAsString on jsonTable(json.string as string)";
        final String createIndexJsonBoolean = "create index " +
            "idxJsonSubAsBoolean on jsonTable(json.boolean as boolean)";

        executeDdl(createTableDDL);
        executeDdl(createIndexJsonLong);
        executeDdl(createIndexJsonDouble);
        executeDdl(createIndexJsonNumber);
        executeDdl(createIndexJsonString);
        executeDdl(createIndexJsonBoolean);

        final Table table = getTable("jsonTable");
        final int nRows = 100;
        loadRows(table, nRows, null);

        /* Long type */
        runIterateJsonIndex(table, "idxJsonSubAsLong", "json.long",
                            FieldDef.Type.LONG, nRows);

        /* Double type */
        runIterateJsonIndex(table, "idxJsonSubAsDouble", "json.double",
                            FieldDef.Type.DOUBLE, nRows);

        /* Number type */
        runIterateJsonIndex(table, "idxJsonSubAsNumber", "json.number",
                            FieldDef.Type.NUMBER, nRows);

        /* String type */
        runIterateJsonIndex(table, "idxJsonSubAsString", "json.string",
                            FieldDef.Type.STRING, nRows);

        /* Boolean type */
        runIterateJsonIndex(table, "idxJsonSubAsBoolean", "json.boolean",
                            FieldDef.Type.BOOLEAN, nRows);

        executeDdl("drop table jsonTable");
    }

    private void runIterateArrayOfJsonTypedIndexTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, a array(json), primary key(id))";

        final String createIndexJsonLong =
            "create index idxJson on jsonTable(a[] as long)";
        final String createIndexJsonDouble =
            "create index idxJson on jsonTable(a[] as double)";
        final String createIndexJsonNumber =
            "create index idxJson on jsonTable(a[] as number)";
        final String createIndexJsonString =
            "create index idxJson on jsonTable(a[] as string)";
        final String createIndexJsonBoolean =
            "create index idxJson on jsonTable(a[] as boolean)";
        final String dropIndex = "drop index idxJson on jsonTable";

        executeDdl(createTableDDL);
        final Table table = getTable("jsonTable");
        final int nRows = 100;
        final int nIdxKeys = 3 * nRows;
        final String idxField = "a[]";

        /* Long type */
        FieldDef.Type type = FieldDef.Type.LONG;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonLong);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Double type */
        type = FieldDef.Type.DOUBLE;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonDouble);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Number type */
        type = FieldDef.Type.NUMBER;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonNumber);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* String type */
        type = FieldDef.Type.STRING;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonString);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Boolean type */
        type = FieldDef.Type.BOOLEAN;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonBoolean);
        runIterateJsonIndex(table, "idxJson", idxField, type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        executeDdl("drop table jsonTable");
    }

    private void runIterateArrayOfJsonSubFieldTypedIndexTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, a array(json), primary key(id))";

        final String createIndexJsonLong =
            "create index idxLong on jsonTable(a[].long as long)";
        final String createIndexJsonDouble =
            "create index idxDouble on jsonTable(a[].double as double)";
        final String createIndexJsonNumber =
            "create index idxNumber on jsonTable(a[].number as number)";
        final String createIndexJsonString =
            "create index idxString on jsonTable(a[].string as string)";
        final String createIndexJsonBoolean =
            "create index idxBoolean on jsonTable(a[].boolean as boolean)";

        executeDdl(createTableDDL);
        executeDdl(createIndexJsonLong);
        executeDdl(createIndexJsonDouble);
        executeDdl(createIndexJsonNumber);
        executeDdl(createIndexJsonString);
        executeDdl(createIndexJsonBoolean);

        final Table table = getTable("jsonTable");
        final int nRows = 100;
        final int nIdxKeys = 3 * nRows;
        loadRows(table, nRows, null);

        /* Long type */
        runIterateJsonIndex(table, "idxLong", "a[].long",
                            FieldDef.Type.LONG, nIdxKeys);

        /* Double type */
        runIterateJsonIndex(table, "idxDouble", "a[].double",
                            FieldDef.Type.DOUBLE, nIdxKeys);

        /* Number type */
        runIterateJsonIndex(table, "idxNumber", "a[].number",
                            FieldDef.Type.NUMBER, nIdxKeys);

        /* String type */
        runIterateJsonIndex(table, "idxString", "a[].string",
                            FieldDef.Type.STRING, nIdxKeys);

        /* Boolean type */
        runIterateJsonIndex(table, "idxBoolean", "a[].boolean",
                            FieldDef.Type.BOOLEAN, nRows);

        executeDdl("drop table jsonTable");
    }

    private void runIterateMapOfJsonTypedIndexTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, m map(json), primary key(id))";

        final String createIndexJsonLong =
            "create index idxJson on jsonTable(m.values() as long)";
        final String createIndexJsonDouble =
            "create index idxJson on jsonTable(m.values() as double)";
        final String createIndexJsonNumber =
            "create index idxJson on jsonTable(m.values() as number)";
        final String createIndexJsonString =
            "create index idxJson on jsonTable(m.values() as string)";
        final String createIndexJsonBoolean =
            "create index idxJson on jsonTable(m.values() as boolean)";
        final String dropIndex = "drop index idxJson on jsonTable";

        executeDdl(createTableDDL);
        final Table table = getTable("jsonTable");
        final int nRows = 100;
        final int nIdxKeys = 3 * nRows;
        final String idxField = "m.values()";

        /* Long type */
        FieldDef.Type type = FieldDef.Type.LONG;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonLong);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Double type */
        type = FieldDef.Type.DOUBLE;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonDouble);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Number type */
        type = FieldDef.Type.NUMBER;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonNumber);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* String type */
        type = FieldDef.Type.STRING;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonString);
        runIterateJsonIndex(table, "idxJson", idxField, type, nIdxKeys);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Boolean type */
        type = FieldDef.Type.BOOLEAN;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonBoolean);
        runIterateJsonIndex(table, "idxJson", idxField, type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        executeDdl("drop table jsonTable");
    }

    private void runIterateMapOfJsonSubFieldTypedIndexTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, m map(json), primary key(id))";

        final String createIndexJsonLong =
            "create index idxLong on jsonTable(m.values().long as long)";
        final String createIndexJsonDouble =
            "create index idxDouble on jsonTable(m.values().double as double)";
        final String createIndexJsonNumber =
            "create index idxNumber on jsonTable(m.values().number as number)";
        final String createIndexJsonString =
            "create index idxString on jsonTable(m.values().string as string)";
        final String createIndexJsonBoolean =
            "create index idxBoolean on jsonTable(m.values().boolean as boolean)";

        executeDdl(createTableDDL);
        executeDdl(createIndexJsonLong);
        executeDdl(createIndexJsonDouble);
        executeDdl(createIndexJsonNumber);
        executeDdl(createIndexJsonString);
        executeDdl(createIndexJsonBoolean);

        final Table table = getTable("jsonTable");
        final int nRows = 100;
        final int nIdxKeys = 3 * nRows;
        loadRows(table, nRows, null);

        /* Long type */
        runIterateJsonIndex(table, "idxLong", "m.values().long",
                            FieldDef.Type.LONG, nIdxKeys);

        /* Double type */
        runIterateJsonIndex(table, "idxDouble", "m.values().double",
                            FieldDef.Type.DOUBLE, nIdxKeys);

        /* Number type */
        runIterateJsonIndex(table, "idxNumber", "m.values().number",
                            FieldDef.Type.NUMBER, nIdxKeys);

        /* String type */
        runIterateJsonIndex(table, "idxString", "m.values().string",
                            FieldDef.Type.STRING, nIdxKeys);

        /* Boolean type */
        runIterateJsonIndex(table, "idxBoolean", "m.values().boolean",
                            FieldDef.Type.BOOLEAN, nRows);

        executeDdl("drop table jsonTable");
    }

    private void runIterateRecordJsonTypedIndexTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, r record(json json, j2 json), primary key(id))";
        final String createIndexJsonLong =
            "create index idxJson on jsonTable(r.json as long)";
        final String createIndexJsonDouble =
            "create index idxJson on jsonTable(r.json as double)";
        final String createIndexJsonNumber =
            "create index idxJson on jsonTable(r.json as number)";
        final String createIndexJsonString =
            "create index idxJson on jsonTable(r.json as string)";
        final String createIndexJsonBoolean =
            "create index idxJson on jsonTable(r.json as boolean)";
        final String dropIndex = "drop index idxJson on jsonTable";

        executeDdl(createTableDDL);
        final Table table = getTable("jsonTable");
        final int nRows = 100;

        /* Long type */
        FieldDef.Type type = FieldDef.Type.LONG;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonLong);
        runIterateJsonIndex(table, "idxJson", "r.json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Double type */
        type = FieldDef.Type.DOUBLE;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonDouble);
        runIterateJsonIndex(table, "idxJson", "r.json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Number type */
        type = FieldDef.Type.NUMBER;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonNumber);
        runIterateJsonIndex(table, "idxJson", "r.json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* String type */
        type = FieldDef.Type.STRING;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonString);
        runIterateJsonIndex(table, "idxJson", "r.json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Boolean type */
        type = FieldDef.Type.BOOLEAN;
        loadRows(table, nRows, type);
        executeDdl(createIndexJsonBoolean);
        runIterateJsonIndex(table, "idxJson", "r.json", type, nRows);
        executeDdl(dropIndex);
        removeTableData(table);

        executeDdl("drop table jsonTable");
    }

    private void runIterateRecordJsonSubFieldTypedIndexTest() {

        final String createTableDDL = "create table jsonTable (" +
            "id integer, r record(json json), primary key(id))";
        final String createIndexJsonLong = "create index " +
            "idxJsonSubAsLong on jsonTable(r.json.long as long)";
        final String createIndexJsonDouble = "create index " +
            "idxJsonSubAsDouble on jsonTable(r.json.double as double)";
        final String createIndexJsonNumber = "create index " +
            "idxJsonSubAsNumber on jsonTable(r.json.number as number)";
        final String createIndexJsonString = "create index " +
            "idxJsonSubAsString on jsonTable(r.json.string as string)";
        final String createIndexJsonBoolean = "create index " +
            "idxJsonSubAsBoolean on jsonTable(r.json.boolean as boolean)";

        executeDdl(createTableDDL);
        executeDdl(createIndexJsonLong);
        executeDdl(createIndexJsonDouble);
        executeDdl(createIndexJsonNumber);
        executeDdl(createIndexJsonString);
        executeDdl(createIndexJsonBoolean);

        final Table table = getTable("jsonTable");
        final int nRows = 100;
        loadRows(table, nRows, null);

        /* Long type */
        runIterateJsonIndex(table, "idxJsonSubAsLong", "r.json.long",
                            FieldDef.Type.LONG, nRows);

        /* Double type */
        runIterateJsonIndex(table, "idxJsonSubAsDouble", "r.json.double",
                            FieldDef.Type.DOUBLE, nRows);

        /* Number type */
        runIterateJsonIndex(table, "idxJsonSubAsNumber", "r.json.number",
                            FieldDef.Type.NUMBER, nRows);

        /* String type */
        runIterateJsonIndex(table, "idxJsonSubAsString", "r.json.string",
                            FieldDef.Type.STRING, nRows);

        /* Boolean type */
        runIterateJsonIndex(table, "idxJsonSubAsBoolean", "r.json.boolean",
                            FieldDef.Type.BOOLEAN, nRows);

        executeDdl("drop table jsonTable");
    }

    void runIterateJsonTypedIndexWithNullsTest() {
        final String createTableDDL = "create table jsonTable (" +
                "id integer, json json, primary key(id))";
        final String createIndexJsonLong =
            "create index idxJson on jsonTable(json as long)";
        final String createIndexJsonDouble =
            "create index idxJson on jsonTable(json as double)";
        final String createIndexJsonNumber =
            "create index idxJson on jsonTable(json as number)";
        final String createIndexJsonString =
            "create index idxJson on jsonTable(json as string)";
        final String createIndexJsonBoolean =
            "create index idxJson on jsonTable(json as boolean)";
        final String dropIndex = "drop index idxJson on jsonTable";

        executeDdl(createTableDDL);

        final Table table = getTable("jsonTable");
        final int nRows = 100;
        final SimpleNullGenerator sng = new SimpleNullGenerator(4);

        /* Long type */
        FieldDef.Type type = FieldDef.Type.LONG;
        loadRows(table, nRows, type, sng);
        executeDdl(createIndexJsonLong);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows, sng);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Double type */
        sng.clear();
        type = FieldDef.Type.DOUBLE;
        loadRows(table, nRows, type, sng);
        executeDdl(createIndexJsonDouble);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows, sng);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Number type */
        sng.clear();
        type = FieldDef.Type.NUMBER;
        loadRows(table, nRows, type, sng);
        executeDdl(createIndexJsonNumber);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows, sng);
        executeDdl(dropIndex);
        removeTableData(table);

        /* String type */
        sng.clear();
        type = FieldDef.Type.STRING;
        loadRows(table, nRows, type, sng);
        executeDdl(createIndexJsonString);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows, sng);
        executeDdl(dropIndex);
        removeTableData(table);

        /* Boolean type */
        sng.clear();
        type = FieldDef.Type.BOOLEAN;
        loadRows(table, nRows, type, sng);
        executeDdl(createIndexJsonBoolean);
        runIterateJsonIndex(table, "idxJson", "json", type, nRows, sng);
        executeDdl(dropIndex);
        removeTableData(table);

        executeDdl("drop table jsonTable");
    }

    private void runIterateJsonSubFieldTypedIndexWithNullsTest() {
        final String createTableDDL = "create table jsonTable (" +
            "id integer, json json, primary key(id))";
        final String createIndexJsonLong = "create index " +
            "idxJsonSubAsLong on jsonTable(json.long as long)";
        final String createIndexJsonDouble = "create index " +
            "idxJsonSubAsDouble on jsonTable(json.double as double)";
        final String createIndexJsonNumber = "create index " +
            "idxJsonSubAsNumber on jsonTable(json.number as number)";
        final String createIndexJsonString = "create index " +
            "idxJsonSubAsString on jsonTable(json.string as string)";
        final String createIndexJsonBoolean = "create index " +
            "idxJsonSubAsBoolean on jsonTable(json.boolean as boolean)";

        executeDdl(createTableDDL);
        executeDdl(createIndexJsonLong);
        executeDdl(createIndexJsonDouble);
        executeDdl(createIndexJsonNumber);
        executeDdl(createIndexJsonString);
        executeDdl(createIndexJsonBoolean);

        final Table table = getTable("jsonTable");
        final int nRows = 100;
        final SimpleNullGenerator sng = new SimpleNullGenerator(4);

        loadRows(table, nRows, null, sng);

        /* Long type */
        runIterateJsonIndex(table, "idxJsonSubAsLong", "json.long",
                            FieldDef.Type.LONG, nRows, sng);

        /* Double type */
        runIterateJsonIndex(table, "idxJsonSubAsDouble", "json.double",
                            FieldDef.Type.DOUBLE, nRows, sng);

        /* Number type */
        runIterateJsonIndex(table, "idxJsonSubAsNumber", "json.number",
                            FieldDef.Type.NUMBER, nRows, sng);

        /* String type */
        runIterateJsonIndex(table, "idxJsonSubAsString", "json.string",
                            FieldDef.Type.STRING, nRows, sng);

        /* Boolean type */
        runIterateJsonIndex(table, "idxJsonSubAsBoolean", "json.boolean",
                            FieldDef.Type.BOOLEAN, nRows, sng);

        executeDdl("drop table jsonTable");
    }

    private void runIterateJsonTypedMultiKeyIndexTest() {
        final String jsonTableDDL =
            "create table JsonTable(id integer, jsonA JSON, " +
                                   "jsonM JSON, primary key(id))";

        final String indexJsonArrayDDL =
            "create index idxJsonArray on JsonTable(jsonA[] as integer)";

        final String indexJsonMapKeysValuesDDL =
            "create index idxJsonMapKeysValues on JsonTable(" +
                 "jsonM.keys(), jsonM.values() as string)";

        final String indexJsonMapKey1DDL =
            "create index idxJsonMapKey1 on JsonTable(" +
                "jsonM.key1 as string)";

        executeDdl(jsonTableDDL);
        executeDdl(indexJsonArrayDDL);
        executeDdl(indexJsonMapKeysValuesDDL);
        executeDdl(indexJsonMapKey1DDL);

        TableImpl table = getTable("jsonTable");
        IndexImpl indexJsonArray = getIndex("jsonTable", "idxJsonArray");
        IndexImpl idxJsonMapKVs = getIndex("jsonTable", "idxJsonMapKeysValues");
        IndexImpl idxJsonMapK1 = getIndex("jsonTable", "idxJsonMapKey1");

        int num = 100;
        Set<Integer> idSqlNull = new HashSet<Integer>();
        Set<Integer> idJsonNull = new HashSet<Integer>();
        Set<Integer> idEmpty = new HashSet<Integer>();
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            if (i % 10 == 7) {
                idSqlNull.add(i);
            } else if (i % 10 == 8) {
                row.putJsonNull("jsonA");
                row.putJsonNull("jsonM");
                idJsonNull.add(i);
            } else {
                ArrayValue av = row.putArray("jsonA");
                MapValue mv = row.putMap("jsonM");
                if (i % 10 == 9) {
                    idEmpty.add(i);
                } else {
                    for (int n = 0; n < 3; n++) {
                        int ni = 3 * i + n;
                        av.add(ni);
                        mv.put("key" + n, String.format("value%03d", ni));
                    }
                }
            }
            tableImpl.put(row, null, null);
        }

        Set<Integer> idNulls = new HashSet<Integer>();
        idNulls.addAll(idSqlNull);
        idNulls.addAll(idJsonNull);
        idNulls.addAll(idEmpty);

        int expCnt = num;
        int cnt = countTableRecords(table.createPrimaryKey(), null);
        assertEquals(expCnt, cnt);

        /* Verify the record count of indexes */
        int numNonNulls = num - idNulls.size();
        expCnt = 3 * numNonNulls + idNulls.size();
        cnt = countIndexRecords(indexJsonArray.createIndexKey(), null);
        assertEquals(expCnt, cnt);

        expCnt = 3 * numNonNulls + idNulls.size();
        cnt = countIndexRecords(idxJsonMapKVs.createIndexKey(), null);
        assertEquals(expCnt, cnt);

        expCnt = num;
        cnt = countIndexRecords(idxJsonMapK1.createIndexKey(), null);
        assertEquals(expCnt, cnt);

        /*
         * Range search on idxArray
         * {
         *   "Name" : jsonA[],
         *   "Type" : INTEGER,
         *   "Start" : 30,
         *   "End" : null,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        int idStart = num - 10;
        int start = idStart * 3;
        expCnt = (num - idStart - getNumIds(idStart, num, idNulls)) * 3;
        FieldRange fr = indexJsonArray.createFieldRange("jsonA[]")
                                      .setStart(start, true);
        iterateWithJsonIndex(indexJsonArray.createIndexKey(), fr,
                             Direction.FORWARD, expCnt);

        /*
         * Range search on idxArray
         * {
         *   "Name" : jsonA[],
         *   "Type" : INTEGER,
         *   "Start" : 30,
         *   "End" : 45,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        int idEnd = (num - 5);
        int end = idEnd * 3;
        fr.setEnd(end, false);
        expCnt = end - start - (getNumIds(idStart, idEnd, idNulls) * 3);
        iterateWithJsonIndex(indexJsonArray.createIndexKey(), fr,
                             Direction.REVERSE, expCnt);

        /*
         * Range search on idxArray
         * {
         *   "Name" : jsonA[],
         *   "Type" : INTEGER,
         *   "Start" : null,
         *   "End" : 45,
         *   "StartInclusive" : false,
         *   "EndInclusive" : true
         * }
         */
        fr = indexJsonArray.createFieldRange("jsonA[]").setEnd(end, true);
        expCnt = end + 1 - (getNumIds(0, idEnd, idNulls) * 3);
        iterateWithJsonIndex(indexJsonArray.createIndexKey(), fr,
                             Direction.FORWARD, expCnt);

        /*
         * Index key search on idxArray: {"jsonA[]":<EMPTY>}
         */
        IndexKeyImpl iKey = indexJsonArray.createIndexKey();
        iKey.putEMPTY("jsonA[]");
        expCnt = idEmpty.size();
        iterateWithJsonIndex(iKey, null, null, expCnt);

        /*
         * Index key search on idxArray: {"jsonA[]":null}
         */
        iKey.clear();
        iKey.putNull("jsonA[]");
        expCnt = idSqlNull.size();
        iterateWithJsonIndex(iKey, null, null, expCnt);

        /*
         * Index key search on idxJsonMapKVs: {"jsonM.keys()":"key1"}
         */
        iKey = idxJsonMapKVs.createIndexKey();
        iKey.put("jsonM.keys()", "key1");
        expCnt = num - idNulls.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapKVs: {"jsonM.keys()":<EMPTY>}
         */
        iKey = idxJsonMapKVs.createIndexKey();
        iKey.putEMPTY("jsonM.keys()");
        expCnt = idJsonNull.size() + idEmpty.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapKVs and jsonM.values():
         * {
         *   "jsonM.keys()":<EMPTY>,
         *   "jsonM.values()":<EMPTY>
         * }
         */
        iKey.putEMPTY("jsonM.values()");
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapKVs and jsonM.values():
         * {
         *   "jsonM.keys()":"key1",
         *   "jsonM.values()":"value016"
         * }
         */
        iKey.clear();
        iKey.put("jsonM.keys()", "key1");
        iKey.put("jsonM.values()", "value016");
        expCnt = 1;
        iterateWithJsonIndex(iKey, null, null, expCnt);

        /*
         * Range search on idxJsonMapKVs:
         * {
         *   "Name" : jsonM.keys(),
         *   "Type" : STRING,
         *   "Start" : key1,
         *   "End" : key2,
         *   "StartInclusive" : true,
         *   "EndInclusive" : true
         * }
         */
        iKey.clear();
        fr = idxJsonMapKVs.createFieldRange("jsonM.keys()");
        fr.setStart("key1", true);
        fr.setEnd("key2", true);
        expCnt = 2 * (num - idNulls.size());
        iterateWithJsonIndex(iKey, fr, Direction.FORWARD, expCnt);

        /*
         * Range search on idxJsonMapKVs:
         * Index prefix key :
         * {
         *   jsonM.keys()":"key1"
         * }
         * Range:
         * {
         *   "Name" : jsonM.values(),
         *   "Type" : STRING,
         *   "Start" : value001,
         *   "End" : value100,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        iKey.clear();
        iKey.put("jsonM.keys()", "key1");
        fr = idxJsonMapKVs.createFieldRange("jsonM.values()");
        fr.setStart("value001", true);
        fr.setEnd("value100", false);
        expCnt = 24;
        iterateWithJsonIndex(iKey, fr, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapK1:
         * {
         *   "jsonM.key1":<EMPTY>
         * }
         */
        iKey = idxJsonMapK1.createIndexKey();
        iKey.putEMPTY("jsonM.key1");
        expCnt = idJsonNull.size() + idEmpty.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapK1:
         * {
         *   "jsonM.key1":null
         * }
         */
        iKey.clear();
        iKey.putNull("jsonM.key1");
        expCnt = idSqlNull.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Range search on idxJsonMapK1:
         * {
         *   "Name" : jsonM.key1,
         *   "Type" : STRING,
         *   "Start" : value001,
         *   "End" : value100,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        iKey.clear();
        fr = idxJsonMapK1.createFieldRange("jsonM.key1");
        fr.setStart("value001", true);
        fr.setEnd("value100", false);
        expCnt = 24;
        iterateWithJsonIndex(iKey, fr, Direction.FORWARD, expCnt);

        executeDdl("drop table jsonTable");
    }

    private void runIterateJsonSubFieldTypedMultiKeyIndexTest() {
        final String jsonTableDDL =
            "create table JsonTable(id integer, jsonA JSON, " +
                                   "jsonM JSON, jsonNR JSON," +
                                   "primary key(id))";

        final String indexJsonArrayDDL =
            "create index idxJsonArray on JsonTable(jsonA.a[] as integer)";

        final String indexJsonMapKeysValuesDDL =
            "create index idxJsonMapKeysValues on JsonTable (" +
                 "jsonM.m.keys(), " +
                 "jsonM.m.values() as long)";

        final String indexJsonMapKey1RecStringDDL =
            "create index indexJsonMapKey1RecString on JsonTable (" +
                 "jsonNR.m.key1.s as string)";

        executeDdl(jsonTableDDL);
        executeDdl(indexJsonArrayDDL);
        executeDdl(indexJsonMapKeysValuesDDL);
        executeDdl(indexJsonMapKey1RecStringDDL);

        TableImpl table = getTable("jsonTable");
        IndexImpl indexJsonArray = getIndex("jsonTable", "idxJsonArray");
        IndexImpl idxJsonMapKVs = getIndex("jsonTable", "idxJsonMapKeysValues");
        IndexImpl indexJsonMapKey1RecString =
             getIndex("jsonTable", "indexJsonMapKey1RecString");

        int num = 100;
        Set<Integer> idSqlNull = new HashSet<Integer>();
        Set<Integer> idJsonNull = new HashSet<Integer>();
        Set<Integer> idEmpty = new HashSet<Integer>();

        StringBuilder av = new StringBuilder();
        StringBuilder mv = new StringBuilder();
        StringBuilder mnr = new StringBuilder();
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            if (i % 10 == 7) {
                idSqlNull.add(i);
            } else if (i % 10 == 8) {
                row.putJsonNull("jsonA");
                row.putJsonNull("jsonM");
                row.putJsonNull("jsonNR");
                idJsonNull.add(i);
            } else {
                if (i % 10 == 9) {
                    row.putJson("jsonA", "{\"a\":[]}");
                    row.putJson("jsonM", "{\"m\":{}}");
                    row.putJson("jsonNR", "{\"m\":{}}");
                    idEmpty.add(i);
                } else {
                    av.setLength(0);
                    mv.setLength(0);
                    mnr.setLength(0);

                    av.append("{\"a\":[");
                    mv.append("{\"m\":{");
                    mnr.append("{\"m\":{");
                    for (int n = 0; n < 3; n++) {
                        int ni = 3 * i + n;
                        if (n > 0) {
                            av.append(",");
                            mv.append(",");
                            mnr.append(",");
                        }
                        av.append(ni);

                        mv.append("\"key");
                        mv.append(n);
                        mv.append("\":");
                        mv.append(ni);

                        mnr.append("\"key");
                        mnr.append(n);
                        mnr.append("\":{\"s\":\"");
                        mnr.append(String.format("s%03d", ni));
                        mnr.append("\"}");
                    }
                    av.append("]}");
                    mv.append("}}");
                    mnr.append("}}");

                    row.putJson("jsonA", av.toString());
                    row.putJson("jsonM", mv.toString());
                    row.putJson("jsonNR", mnr.toString());
                }
            }
            tableImpl.put(row, null, null);
        }

        Set<Integer> idNulls = new HashSet<Integer>();
        idNulls.addAll(idSqlNull);
        idNulls.addAll(idJsonNull);
        idNulls.addAll(idEmpty);

        int expCnt = num;
        int cnt = countTableRecords(table.createPrimaryKey(), null);
        assertEquals(expCnt, cnt);

        /* Verify the record count of indexes */
        int numNonNulls = num - idNulls.size();
        expCnt = 3 * numNonNulls + idNulls.size();
        cnt = countIndexRecords(indexJsonArray.createIndexKey(), null);
        assertEquals(expCnt, cnt);

        expCnt = 3 * numNonNulls + idNulls.size();
        cnt = countIndexRecords(idxJsonMapKVs.createIndexKey(), null);
        assertEquals(expCnt, cnt);

        expCnt = num;
        cnt = countIndexRecords(indexJsonMapKey1RecString.createIndexKey(),
                                null);
        assertEquals(expCnt, cnt);

        /*
         * Range search on idxArray
         * {
         *   "Name" : jsonA.a[],
         *   "Type" : INTEGER,
         *   "Start" : 30,
         *   "End" : null,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        int idStart = num - 10;
        int start = idStart * 3;
        expCnt = (num - idStart - getNumIds(idStart, num, idNulls)) * 3;
        FieldRange fr = indexJsonArray.createFieldRange("jsonA.a[]")
                                      .setStart(start, true);
        iterateWithJsonIndex(indexJsonArray.createIndexKey(), fr,
                             Direction.FORWARD, expCnt);

        /*
         * Range search on idxArray
         * {
         *   "Name" : jsonA[],
         *   "Type" : INTEGER,
         *   "Start" : 30,
         *   "End" : 45,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        int idEnd = (num - 5);
        int end = idEnd * 3;
        fr.setEnd(end, false);
        expCnt = end - start - (getNumIds(idStart, idEnd, idNulls) * 3);
        iterateWithJsonIndex(indexJsonArray.createIndexKey(), fr,
                             Direction.REVERSE, expCnt);

        /*
         * Range search on idxArray
         * {
         *   "Name" : jsonA[],
         *   "Type" : INTEGER,
         *   "Start" : null,
         *   "End" : 45,
         *   "StartInclusive" : false,
         *   "EndInclusive" : true
         * }
         */
        fr = indexJsonArray.createFieldRange("jsonA.a[]").setEnd(end, true);
        expCnt = end + 1 - (getNumIds(0, idEnd, idNulls) * 3);
        iterateWithJsonIndex(indexJsonArray.createIndexKey(), fr,
                             Direction.FORWARD, expCnt);

        /*
         * Index key search on idxArray: {"jsonA.a[]":<EMPTY>}
         */
        IndexKeyImpl iKey = indexJsonArray.createIndexKey();
        iKey.putEMPTY("jsonA.a[]");
        expCnt = idJsonNull.size() + idEmpty.size();
        iterateWithJsonIndex(iKey, null, null, expCnt);

        /*
         * Index key search on idxArray: {"jsonA.a[]":null}
         */
        iKey.clear();
        iKey.putNull("jsonA.a[]");
        expCnt = idSqlNull.size();
        iterateWithJsonIndex(iKey, null, null, expCnt);

        /*
         * Index key search on idxJsonMapKVs: {"jsonM.m.keys()":<EMPTY>}
         */
        iKey = idxJsonMapKVs.createIndexKey();
        iKey.putEMPTY("jsonM.m.keys()");
        expCnt = idJsonNull.size() + idEmpty.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapKVs and jsonM.values():
         * {
         *   "jsonM.m.keys()":<EMPTY>,
         *   "jsonM.m.values()":<EMPTY>
         * }
         */
        iKey.putEMPTY("jsonM.m.values()");
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapKVs and jsonM.m.values():
         * {
         *   "jsonM.m.keys()":"key1",
         *   "jsonM.m.values()":"16"
         * }
         */
        iKey.clear();
        iKey.put("jsonM.m.keys()", "key1");
        iKey.put("jsonM.m.values()", 16L);
        expCnt = 1;
        iterateWithJsonIndex(iKey, null, null, expCnt);

        /*
         * Range search on idxJsonMapKVs:
         * {
         *   "Name" : jsonM.keys(),
         *   "Type" : STRING,
         *   "Start" : key1,
         *   "End" : key2,
         *   "StartInclusive" : true,
         *   "EndInclusive" : true
         * }
         */
        iKey.clear();
        fr = idxJsonMapKVs.createFieldRange("jsonM.m.keys()");
        fr.setStart("key1", true);
        fr.setEnd("key2", true);
        expCnt = 2 * (num - idNulls.size());
        iterateWithJsonIndex(iKey, fr, Direction.FORWARD, expCnt);

        /*
         * Range search on idxJsonMapKVs:
         * Index prefix key :
         * {
         *   jsonM.m.keys()":"key1"
         * }
         * Range:
         * {
         *   "Name" : jsonM.m.values(),
         *   "Type" : STRING,
         *   "Start" : 1,
         *   "End" : 100,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        iKey.clear();
        iKey.put("jsonM.m.keys()", "key1");
        fr = idxJsonMapKVs.createFieldRange("jsonM.m.values()");
        fr.setStart(1L, true);
        fr.setEnd(100L, false);
        expCnt = 24;
        iterateWithJsonIndex(iKey, fr, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapK1:
         * {
         *   "jsonNR.m.key1.s":<EMPTY>
         * }
         */
        iKey = indexJsonMapKey1RecString.createIndexKey();
        iKey.putEMPTY("jsonNR.m.key1.s");
        expCnt = idJsonNull.size() + idEmpty.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Index key search on idxJsonMapK1:
         * {
         *   "jsonNR.key1.s":null
         * }
         */
        iKey.clear();
        iKey.putNull("jsonNR.m.key1.s");
        expCnt = idSqlNull.size();
        iterateWithJsonIndex(iKey, null, Direction.FORWARD, expCnt);

        /*
         * Range search on idxJsonMapK1:
         * {
         *   "Name" : jsonNR.key1.s,
         *   "Type" : STRING,
         *   "Start" : value001,
         *   "End" : value100,
         *   "StartInclusive" : true,
         *   "EndInclusive" : false
         * }
         */
        iKey.clear();
        fr = indexJsonMapKey1RecString.createFieldRange("jsonNR.m.key1.s");
        fr.setStart("s001", true);
        fr.setEnd("s100", false);
        expCnt = 24;
        iterateWithJsonIndex(iKey, fr, Direction.FORWARD, expCnt);

        executeDdl("drop table jsonTable");
    }

    private int getNumIds(int from, int to, Set<Integer> idNulls) {
        int cnt = 0;
        for (int i = from; i < to; i++) {
            if (idNulls.contains(i)) {
                cnt++;
            }
        }
        return cnt;
    }

    private void runIterateJsonIndex(Table table,
                                     String indexName,
                                     String jsonField,
                                     FieldDef.Type type,
                                     int nRows) {

        runIterateJsonIndex(table, indexName, jsonField, type, nRows, null);
    }

    private void runIterateJsonIndex(Table table,
                                     String indexName,
                                     String jsonField,
                                     FieldDef.Type type,
                                     int nRows,
                                     SimpleNullGenerator ng) {

        final Direction[] directions = new Direction[] {
            Direction.FORWARD, Direction.REVERSE
        };
        final Index idxJson = getIndex(table.getFullName(), indexName);

        for (Direction dir : directions) {
            iterateWithJsonIndex(idxJson.createIndexKey(), null, dir, nRows);
        }

        if (type != FieldDef.Type.BOOLEAN) {
            int idFrom = 10;
            int idTo = 30;
            FieldRange[] ranges = genFieldRanges(idxJson, jsonField, type,
                                                 idFrom, idTo);
            for (Direction dir : directions) {
                for (FieldRange range : ranges) {
                    int expNumRows = getExpectedNum(range, idFrom, idTo,
                                                    nRows, ng);
                    iterateWithJsonIndex(idxJson.createIndexKey(), range,
                                         dir, expNumRows);
                }
            }
        }

        if (ng != null) {
            if (!ng.getIdsOfSqlNull().isEmpty()) {
                IndexKey ikey = idxJson.createIndexKey();
                ikey.putNull(jsonField);
                iterateWithJsonIndex(ikey,
                                     null/* FieldRange */,
                                     null /* Direction */,
                                     ng.getIdsOfSqlNull().size(),
                                     ng.getIdsOfSqlNull());
            }

            if (!ng.getIdsOfEmpty().isEmpty()) {
                IndexKeyImpl ikey = (IndexKeyImpl)idxJson.createIndexKey();
                ikey.putEMPTY(jsonField);
                iterateWithJsonIndex(ikey,
                                     null/* FieldRange */,
                                     null /* Direction */,
                                     ng.getIdsOfEmpty().size(),
                                     ng.getIdsOfEmpty());
            }

            if (!ng.getIdsOfJsonNull().isEmpty()) {
                IndexKey ikey = idxJson.createIndexKey();
                ikey.putJsonNull(jsonField);
                iterateWithJsonIndex(ikey,
                                     null/* FieldRange */,
                                     null /* Direction */,
                                     ng.getIdsOfJsonNull().size(),
                                     ng.getIdsOfJsonNull());
            }
        }
    }

    private void loadRows(Table table, int num, FieldDef.Type jsonAsType) {
        loadRows(table, num, jsonAsType, null);
    }

    private void loadRows(Table table, int num,
                          FieldDef.Type jsonAsType,
                          NullGenerator nullGenerator) {
        for (int i = 0; i < num; i++) {
            Row row = genRowForJsonTable(table, i, jsonAsType,
                                         num, nullGenerator);
            assertTrue(tableImpl.put(row, null, null) != null);
        }
        assertTrue(countTableRecords(table.createPrimaryKey(), null) == num);
    }

    private void removeTableData(Table table) {
        TableIterator<PrimaryKey> iter =
            tableImpl.tableKeysIterator(table.createPrimaryKey(), null, null);

        while(iter.hasNext()) {
            PrimaryKey key = iter.next();
            tableImpl.delete(key, null, null);
        }

        assertTrue(countTableRows(table.createPrimaryKey(), null) == 0);
    }

    private FieldRange[] genFieldRanges(Index index,
                                        String field,
                                        FieldDef.Type type,
                                        int idFrom, int idTo) {

        final boolean[] inclusives = new boolean[] {true, false};
        final FieldValue start = genFieldValue(type, idFrom);
        final FieldValue end = genFieldValue(type, idTo);

        List<FieldRange> ranges = new ArrayList<FieldRange>();

        for (boolean inclusive : inclusives) {
            FieldRange range = index.createFieldRange(field);
            range.setStart(start, inclusive);
            ranges.add(range);
        }

        for (boolean startInclusive : inclusives) {
            for (boolean endInclusive : inclusives) {
                FieldRange range = index.createFieldRange(field);
                range.setStart(start, startInclusive);
                range.setEnd(end, endInclusive);
                ranges.add(range);
            }
        }

        for (boolean endInclusive : inclusives) {
            FieldRange range = index.createFieldRange(field);
            range.setEnd(end, endInclusive);
            ranges.add(range);
        }
        return ranges.toArray(new FieldRange[ranges.size()]);
    }

    private FieldValue genFieldValue(FieldDef.Type type, int id) {
        switch (type) {
        case LONG:
            return FieldDefImpl.Constants.longDef.createLong(getLong(id));
        case DOUBLE:
            return FieldDefImpl.Constants.doubleDef.createDouble(
                getDouble(id));
        case NUMBER:
            return FieldDefImpl.Constants.numberDef.createNumber(
                getBigDecimal(id));
        case STRING:
            return FieldDefImpl.Constants.stringDef.createString(
                getString(id));
        case BOOLEAN:
            return FieldDefImpl.Constants.booleanDef.createBoolean(
                getBoolean(id));
        default:
            fail("Unexpected type: " + type);
        }
        return null;
    }

    private int getExpectedNum(FieldRange range, int idFrom,
                               int idTo, int total,
                               SimpleNullGenerator nullGenerator) {
        int from = (range.getStart() != null) ? idFrom : 0;
        int to =  (range.getEnd() != null) ? idTo : total;
        int num = to - from + 1;
        if (nullGenerator != null) {
            num -= getNullsNum(from, to, nullGenerator);
        }
        if (range.getStart() != null && !range.getStartInclusive()) {
            num--;
        }
        if (!range.getEndInclusive()) {
            num--;
        }
        return num;
    }

    private int getNullsNum(int from, int to, SimpleNullGenerator ng) {
        int cnt = 0;
        for (int id = from; id <= to; id++) {
            if (ng.isIdOfNulls(id)) {
                cnt++;
            }
        }
        return cnt;
    }

    private void iterateWithJsonIndex(IndexKey idxKey,
                                      FieldRange range,
                                      Direction direction,
                                      int expNumRows) {
        iterateWithJsonIndex(idxKey, range, direction, expNumRows, null);
    }

    private void iterateWithJsonIndex(IndexKey idxKey,
                                      FieldRange range,
                                      Direction direction,
                                      int expNumRows,
                                      List<Integer> ids) {

        final TableIteratorOptions tto =
            (direction != null) ?
                new TableIteratorOptions(direction, null, 0, null) : null;
        final MultiRowOptions mro =
            (range != null) ? range.createMultiRowOptions() : null;

        TableIterator<KeyPair> iter =
            tableImpl.tableKeysIterator(idxKey, mro, tto);

        int cnt = 0;
        IndexKey prev = null;

        while(iter.hasNext()) {
            KeyPair kp = iter.next();
            IndexKey ikey = kp.getIndexKey();
            cnt++;
            if (prev != null) {
                checkOrder(direction, prev, ikey);
            }
            prev = (IndexKey)ikey.clone();
            if (ids != null) {
                int id = kp.getPrimaryKey().get("id").asInteger().get();
                assertTrue(ids.contains(id));
            }
        }
        iter.close();
        assertTrue("row count is expected to be " + expNumRows + " but " + cnt,
                   cnt == expNumRows);
    }

    private void checkOrder(Direction direction,
                            IndexKey prev,
                            IndexKey current) {

        if (direction == null || direction == Direction.UNORDERED) {
            return;
        }

        int ret = prev.compareTo(current);
        assertTrue(((direction == Direction.FORWARD) ? ret <= 0 : ret >= 0));
    }

    interface NullGenerator {
        boolean isSQLNull(int id);
        boolean isJsonNull(int id);
        boolean isEmpty(int id);
    }

    final class SimpleNullGenerator implements NullGenerator {

        private final List<Integer> sqlNulls = new ArrayList<Integer>();
        private final List<Integer> jsonNulls = new ArrayList<Integer>();
        private final List<Integer> emptys = new ArrayList<Integer>();
        private final int base;

        SimpleNullGenerator(int percent) {
            base = 100 / percent;
        }

        @Override
        public boolean isSQLNull(int id) {
            if ((id % base) == 1) {
                sqlNulls.add(id);
                return true;
            }
            return false;
        }

        @Override
        public boolean isEmpty(int id) {
            if ((id % base) == 2) {
                emptys.add(id);
                return true;
            }
            return false;
        }

        @Override
        public boolean isJsonNull(int id) {
            if ((id % base) == 3) {
                jsonNulls.add(id);
                return true;
            }
            return false;
        }

        public List<Integer> getIdsOfSqlNull() {
            return sqlNulls;
        }

        public List<Integer> getIdsOfJsonNull() {
            return jsonNulls;
        }

        public List<Integer> getIdsOfEmpty() {
            return emptys;
        }

        public boolean isIdOfNulls(int id) {
            return isIdOfSqlNulls(id) ||
                   isIdOfJsonNulls(id) ||
                   isIdOfEmptys(id);
        }

        public boolean isIdOfSqlNulls(int id) {
            return sqlNulls.contains(id);
        }

        public boolean isIdOfJsonNulls(int id) {
            return jsonNulls.contains(id);
        }

        public boolean isIdOfEmptys(int id) {
            return emptys.contains(id);
        }

        @Override
        public String toString() {
            return "sqlNulls: " + sqlNulls.toString() + " | " +
                   "jsonNulls: " + jsonNulls.toString() + " | " +
                   "emptys: " + emptys.toString();
        }

        public void clear() {
            if (!sqlNulls.isEmpty()) {
                sqlNulls.clear();
            }

            if (!jsonNulls.isEmpty()) {
                jsonNulls.clear();
            }

            if (!emptys.isEmpty()) {
                emptys.clear();
            }
        }
    }

    private Row genRowForJsonTable(Table table, int id,
                                   FieldDef.Type jsonAsType,
                                   int num,
                                   NullGenerator nullGenerator) {

        final String EMPTY_JSON = "{}";
        final String NULLS_JSON = "{\"long\":null, \"double\":null," +
            "\"number\":null, \"string\":null, \"boolean\":null}";

        Row row = table.createRow();
        for (String name : row.getFieldNames()) {
            if (name.equals("id")) {
                row.put("id", id);
                continue;
            }
            FieldDef def = table.getField(name);
            if (def.isJson()) {
                if (jsonAsType != null) {
                    if (nullGenerator != null) {
                        if (nullGenerator.isSQLNull(id)) {
                            row.putNull(name);
                            continue;
                        }
                    }
                    switch (jsonAsType) {
                    case LONG:
                        row.put(name, getLong(id));
                        break;
                    case DOUBLE:
                        row.put(name, getDouble(id));
                        break;
                    case NUMBER:
                        row.putNumber(name, getBigDecimal(id));
                        break;
                    case STRING:
                        row.put(name, getString(id));
                        break;
                    case BOOLEAN:
                        row.put(name, getBoolean(id));
                        break;
                    default:
                        fail("Unexpected type: " + jsonAsType);
                    }
                } else {
                    if (nullGenerator != null) {
                        if (nullGenerator.isSQLNull(id)) {
                            row.putNull(name);
                            continue;
                        }
                        if (nullGenerator.isJsonNull(id)) {
                            row.putJson(name, NULLS_JSON);
                            continue;
                        }
                        if (nullGenerator.isEmpty(id)) {
                            row.putJson(name, EMPTY_JSON);
                            continue;
                        }
                    }
                    row.putJson(name, getJsonValue(id));
                }
            } else if (def.isArray()) {
                ArrayValue av = row.putArray(name);
                assert def.asArray().getElement().isJson();
                for (int i = 0; i < 3; i++) {
                    int vid = (i % 3) * num + id;
                    if (jsonAsType != null) {
                        switch (jsonAsType) {
                        case LONG:
                            av.add(getLong(vid));
                            break;
                        case DOUBLE:
                            av.add(getDouble(vid));
                            break;
                        case NUMBER:
                            av.addNumber(getBigDecimal(vid));
                            break;
                        case STRING:
                            av.add(getString(vid));
                            break;
                        case BOOLEAN:
                            av.add(getBoolean(vid));
                            break;
                        default:
                            fail("Unexpected type: " + jsonAsType);
                        }
                    } else {
                        av.addJson(getJsonValue(vid));
                    }
                }
            } else if (def.isMap()) {
                MapValue mv = row.putMap(name);
                assert def.asMap().getElement().isJson();
                for (int i = 0; i < 3; i++) {
                    int vid = (i % 3) * num + id;
                    String key = "key" + vid;
                    if (jsonAsType != null) {
                        switch (jsonAsType) {
                        case LONG:
                            mv.put(key, getLong(vid));
                            break;
                        case DOUBLE:
                            mv.put(key, getDouble(vid));
                            break;
                        case NUMBER:
                            mv.putNumber(key, getBigDecimal(vid));
                            break;
                        case STRING:
                            mv.put(key, getString(vid));
                            break;
                        case BOOLEAN:
                            mv.put(key, getBoolean(vid));
                            break;
                        default:
                            fail("Unexpected type: " + jsonAsType);
                        }
                    } else {
                        mv.putJson(key, getJsonValue(vid));
                    }
                }
            } else {
                assert def.isRecord();
                RecordDef rdef = def.asRecord();
                assert rdef.getFieldDef(0).isJson();
                String jsonField = rdef.getFieldName(0);
                RecordValue rv = row.putRecord(name);
                if (jsonAsType != null) {
                    switch (jsonAsType) {
                    case LONG:
                        rv.put(jsonField, getLong(id));
                        break;
                    case DOUBLE:
                        rv.put(jsonField, getDouble(id));
                        break;
                    case NUMBER:
                        rv.putNumber(jsonField, getBigDecimal(id));
                        break;
                    case STRING:
                        rv.put(jsonField, getString(id));
                        break;
                    case BOOLEAN:
                        rv.put(jsonField, getBoolean(id));
                        break;
                    default:
                        fail("Unexpected type: " + jsonAsType);
                    }
                } else {
                    rv.putJson(jsonField, getJsonValue(id));
                }
            }
        }
        return row;
    }

    private String getJsonValue(int id) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        addJsonField(sb, "long", FieldDef.Type.LONG, id);
        sb.append(",");
        addJsonField(sb, "double", FieldDef.Type.DOUBLE, id);
        sb.append(",");
        addJsonField(sb, "number", FieldDef.Type.NUMBER, id);
        sb.append(",");
        addJsonField(sb, "string", FieldDef.Type.STRING, id);
        sb.append(",");
        addJsonField(sb, "boolean", FieldDef.Type.BOOLEAN, id);
        sb.append("}");
        return sb.toString();
    }

    private void addJsonField(StringBuilder sb,
                              String name,
                              FieldDef.Type type,
                              int id) {
        sb.append(quotedString(name));
        sb.append(":");
        switch (type) {
        case LONG:
            sb.append(getLong(id));
            break;
        case DOUBLE:
            sb.append(getDouble(id));
            break;
        case NUMBER:
            sb.append(getBigDecimal(id));
            break;
        case STRING:
            sb.append(quotedString(getString(id)));
            break;
        case BOOLEAN:
            sb.append(getBoolean(id));
            break;
        default:
            fail("Unexpected type: " + type);
        }
    }

    private String quotedString(String str) {
        return "\"" + str + "\"";
    }

    private long getLong(int id) {
        return id;
    }

    private double getDouble(int id) {
        return 1.1 * id;
    }

    private BigDecimal getBigDecimal(int id) {
        return new BigDecimal(BigInteger.valueOf(Integer.MAX_VALUE), -id + 9);
    }

    private String getString(int id) {
        return SortableString.toSortable(id).replace("\\", "\\\\");
    }

    private boolean getBoolean(int id) {
        return (id % 2 == 0);
    }

    private static class TestStream implements EntryStream<Row> {

        private final List<Row> list;
        private final Iterator<Row> iterator;

        TestStream(Row... entries) {
            list = new ArrayList<Row>(entries.length);
            for (Row entry : entries) {
                list.add(entry);
            }
            iterator = list.iterator();
        }

        @Override
        public String name() {
            return "TestBulkPutStream";
        }

        @Override
        public Row getNext() {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }

        @Override
        public void completed() {
        }

        @Override
        public void keyExists(Row entry) {
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   Row entry) {
            throw runtimeException;
        }
    }
}
