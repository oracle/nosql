/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Random;
import java.io.ByteArrayOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MapValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.Test;

public class MapIndexTest extends TableTestBase {

    final TableIteratorOptions forwardOpts;
    final TableIteratorOptions reverseOpts;
    final TableIteratorOptions unorderedOpts;
    final String anonMapKey = MapValue.ANONYMOUS;

    public MapIndexTest() {
        forwardOpts = new TableIteratorOptions(Direction.FORWARD,
                                               Consistency.ABSOLUTE,
                                               0, null, /* timeout, unit */
                                               0, 0);

        reverseOpts = new TableIteratorOptions(Direction.REVERSE,
                                               Consistency.ABSOLUTE,
                                               0, null, /* timeout, unit */
                                               0, 0);
        unorderedOpts = new TableIteratorOptions(Direction.UNORDERED,
                                                 Consistency.ABSOLUTE,
                                                 0, null, /* timeout, unit */
                                                 0, 0);
    }

    /*
     * Test various cases for map indexes.
     * o key-only and key+value indexes for
     *  - simple map of scalar
     *  - map of record
     *  - map of array (key-only is ok)
     * o indexed maps as top-level fields and as embedded fields (todo)
     * single- and multi-component indexes (todo)
     * o failure conditions:
     *  - map containging and array, key+value
     *  - map containing a map, key+value (todo)
     *  - array containing a map (todo)
     */
    @Test
    public void testMapIndexes()
        throws Exception {

        final int numRows = 10;
        Index index = null;
        IndexKey ikey = null;
        TableImpl table = null;
        TableImpl complexTable = null;
        TableIterator<Row> rowIter = null;
        TableIterator<KeyPair> keyIter = null;
        FieldRange range;
        MultiRowOptions mro;
        int count;

        table = buildSimpleTable();
        populateSimpleTable(table, numRows);

        complexTable = buildComplexTable(numRows);

        /*
         * Test map index key sorting.
         */
        testMapIndexSorting(table);

        /*
         * Test the key only index
         */
        index = table.getIndex("intMapKeyOnly");
        ikey = index.createIndexKey();
        rowIter = tableImpl.tableIterator(ikey, null, null);
        count = 0;
        while (rowIter.hasNext()) {
            count ++;
            rowIter.next();
        }
        rowIter.close();


        /*
         * Create a range based on the key value.
         */
        range = index.createFieldRange("map.kEYs()");
        range.setStart("key6", true);
        range.setEnd("key8", true);
        mro = range.createMultiRowOptions();
        rowIter = tableImpl.tableIterator(ikey, mro, null);
        count = 0;
        while (rowIter.hasNext()) {
            count ++;
            rowIter.next();
        }
        rowIter.close();
        assertEquals("Unexpected count", 3, count);

        /*
         * Use a simple equality key and verify that there is one
         * entry that matches.  In this path create the key from
         * JSON to exercise a path used by the client drivers.
         * This is equivalent to:
         *   ikey.putMap("map").putNull("key4");
         * The "false" parameter means that it does not need to be an
         * exact match to all fields in the index key.
         */
        ikey = index.createIndexKeyFromJson("{\"map.keys()\": \"key4\"}", false);
        assertEquals("Unexpected count", 1, countIndexRecords(ikey, table));

        /*
         * Use the value index.
         */
        index = table.getIndex("intMapValue");
        ikey = index.createIndexKey();

        keyIter = tableImpl.tableKeysIterator(ikey, null, null);
        count = 0;
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        keyIter.close();
        assertEquals("Unexpected count", 20, count);

        /*
         * use a value range
         */
        range = index.createFieldRange("map.Values()");
        range.setStart(8, true);
        range.setEnd(13, true);
        mro = range.createMultiRowOptions();
        keyIter = tableImpl.tableKeysIterator(ikey, mro, null);
        count = 0;
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        keyIter.close();
        assertEquals("Unexpected count", 6, count);

        /*
         * Use the key + value index.
         */
        index = table.getIndex("intMapKeyValue");
        ikey = index.createIndexKey();
        count = 0;
        keyIter = tableImpl.tableKeysIterator(ikey, null, null);
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        keyIter.close();
        assertEquals("Unexpected count", 20, count);

        /*
         * Use bad key (value, but no key)
         */
        ikey.put("map.vaLues()", 2);
        try {
            keyIter = tableImpl.tableKeysIterator(ikey, null, null);
            fail("Index iteration should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Use a range
         */
        ikey = index.createIndexKey();
        ikey.put("map.keyS()", "key2");
        count = 0;
        keyIter = tableImpl.tableKeysIterator(ikey, null, null);
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        keyIter.close();
        assertEquals("Unexpected count", 1, count);

        /*
         * There are 10 entries with "common" in the map.
         */
        ikey = index.createIndexKey();
        ikey.put("map.Keys()", "common");
        count = 0;
        keyIter = tableImpl.tableKeysIterator(ikey, null, null);
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        keyIter.close();
        assertEquals("Unexpected count", 10, count);
        //        assertEquals("Unexpected count", 10, countIndexRows(ikey, table));

        /*
         * Add a range to the "common" key.
         */
        range = index.createFieldRange("map.VAlues()");
        range.setStart(13, true);
        range.setEnd(19, true);
        mro = range.createMultiRowOptions();

        rowIter = tableImpl.tableIterator(ikey, mro, forwardOpts);
        count = 0;
        while (rowIter.hasNext()) {
            count ++;
            rowIter.next();
        }
        rowIter.close();
        assertEquals("Unexpected count", 7, count);

        /*************************************/
        /* use the complex table and indexes */
        /*************************************/

        index = complexTable.getIndex("mapValueCityStreet");
        ikey = index.createIndexKey();
        range = index.createFieldRange("addresses.vaLues().city");
        range.setStart("city2", true);
        range.setEnd("city5", true);
        mro = range.createMultiRowOptions();
        keyIter = tableImpl.tableKeysIterator(ikey, mro, forwardOpts);
        count = 0;
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        assertEquals("Unexpected count", 4 * 5, count);
        keyIter.close();

        /* put a range on the second component */
        ikey = index.createIndexKey();
        ikey.put("addresses.values().city", "city1");
        range = index.createFieldRange("addresses.values().street");
        range.setStart("street11", true);
        range.setEnd("street13", true);
        mro = range.createMultiRowOptions();
        keyIter = tableImpl.tableKeysIterator(ikey, mro, forwardOpts);
        count = 0;
        while (keyIter.hasNext()) {
            count ++;
            keyIter.next();
        }
        assertEquals("Unexpected count", 3, count);
        keyIter.close();
    }


    /*
     * This is a test for the bug fix done in the context of [#24608]
     */
    @Test
    public void testMapMapIndex() throws Exception {

        String tableStatement =
            "CREATE TABLE testTable(                                 \n" +
            "    id INTEGER,                                         \n" +
            "    foo RECORD(map_map MAP(MAP(INTEGER))),              \n" +
            "    primary key (id)                                    \n" +
            ")";

        String json0 =
            "{                                                         \n" +
            "  \"map_map\" : { \"a\" : { \"a1\" : 10, \"a2\" : 20 } }  \n" +
            "}";

        String jdocs[] = { json0 };

        String indexStatement =
            "CREATE INDEX idx_map_map ON testTable (foo.map_map.a.values())";

        String indexStatement2 =
            "CREATE INDEX idx_map_entry ON testTable (foo.map_map.b.a1)";

        executeDdl(tableStatement);
        executeDdl(indexStatement);
        executeDdl(indexStatement2);

        Table table = tableImpl.getTable("testTable");

        Index index = table.getIndex("idx_map_map");

        int numRows = 1;

        for (int i = 0; i < numRows; i++) {

            Row row = table.createRow();
            row.put("id", i);
            row.putRecordAsJson("foo", jdocs[i], true);

            tableImpl.put(row, null, null);
        }

        // Query index idx_map_map
        IndexKey idxKey = index.createIndexKey();

        idxKey.put("foo.map_map.a.values()", 10);

        TableAPI tableApi = store.getTableAPI();

        TableIterator<Row> iter = tableApi.tableIterator(idxKey, null, null);

        while (iter.hasNext()) {
            iter.next();
        }
    }

    /**
     * Tests that map index key sorting works as expected.  Assumes that the
     * simple user table has been created with indexes, and populated.
     */
    private void testMapIndexSorting(TableImpl table)
        throws Exception {

        /*
         * Test the trickiest case where the index is on the key + value on the
         * same map.  This should sort by key, value in that order.
         */
        Index index = table.getIndex("intMapKeyValue");
        IndexKeyImpl ikey = (IndexKeyImpl)index.createIndexKey();
        IndexKeyImpl ikey1 = (IndexKeyImpl)index.createIndexKey();

        ikey.put("map.keys()", "key8"); /* map key */
        ikey1.put("map.keys()", "common");

        ikey.put("map.values()", 8);          /* map value */
        ikey1.put("map.values()", 19);

        assertTrue("Unexpected comparison", ikey.compareTo(ikey1) > 0);
        ikey.clear();
        ikey1.clear();

        ikey.put("map.keys()", "key");
        ikey1.put("map.keys()", "key");
        assertTrue("Unexpected comparison", ikey.compareTo(ikey1) == 0);
        ikey.clear();
        ikey1.clear();

        ikey.put("map.values()", 1);
        ikey1.put("map.values()", 1);
        assertTrue("Unexpected comparison", ikey.compareTo(ikey1) == 0);
        ikey.put("map.keys()", "key");
        ikey1.put("map.keys()", "key");
        assertTrue("Unexpected comparison", ikey.compareTo(ikey1) == 0);
    }

    /**
     * Do some tests on a multi-component array index that has both components
     * in the same record in the array of records.
     */
    @Test
    public void testArrays()
        throws Exception {

        final int numRows = 10;
        Index index = null;
        IndexKey ikey = null;
        TableImpl table = null;
        FieldRange range = null;
        MultiRowOptions mro = null;
        TableIterator<Row> rowIter = null;
        int count = 0;

        table = buildComplexTable(numRows);
        index = table.getIndex("arrayTwoValue");
        ikey = index.createIndexKey();
        assertEquals("Unexpected count", numRows, countIndexRows(ikey, table));

        ikey.put("arrayOfRec[].iField", 5);
        assertEquals("Unexpected count", 1, countIndexRows(ikey, table));
        ikey.put("arrayOfRec[].sField", "s5");
        assertEquals("Unexpected count", 1, countIndexRows(ikey, table));

        /* only first key component set */
        ikey = index.createIndexKey();
        ikey.put("arrayOfRec[].iField", 4);
        rowIter = tableImpl.tableIterator(ikey, mro, forwardOpts);
        count = 0;
        while (rowIter.hasNext()) {
            count ++;
            rowIter.next();
        }
        rowIter.close();
        assertEquals("Unexpected count", 1, count);

        /* range on first key component */
        ikey = index.createIndexKey();
        range = index.createFieldRange("arrayOfRec[].iField");
        range.setStart(3, true);
        range.setEnd(8, true);
        mro = range.createMultiRowOptions();
        rowIter = tableImpl.tableIterator(ikey, mro, forwardOpts);
        count = 0;
        while (rowIter.hasNext()) {
            count ++;
            rowIter.next();
        }
        rowIter.close();
        assertEquals("Unexpected count", 6, count);

        /* range on second key component */
        ikey = index.createIndexKey();
        ikey.put("arrayOfRec[].iField", 5);
        range = index.createFieldRange("arrayOfRec[].sField");
        /* range includes the sole value, s5 */
        range.setStart("s3", true);
        range.setEnd("s9", true);
        mro = range.createMultiRowOptions();
        rowIter = tableImpl.tableIterator(ikey, mro, forwardOpts);
        count = 0;
        while (rowIter.hasNext()) {
            count ++;
            rowIter.next();
        }
        rowIter.close();

        assertEquals("Unexpected count", 1, count);
    }

    @Test
    public void testDuplicates()
        throws Exception {

        final int numRows = 10;
        final int numDups = 5;
        Index index = null;
        IndexKey ikey = null;
        TableImpl table = null;

        table = buildSimpleTable();
        populateDupsTable(table, numRows, numDups);

        index = table.getIndex("intArrayIndex");
        ikey = index.createIndexKey();
        assertEquals("Unexpected count", numRows, countIndexRows(ikey, table));

        ikey.put("intArray[]", 4);
        assertEquals("Unexpected count", 1, countIndexRows(ikey, table));

        index = table.getIndex("intMapValue");
        ikey = index.createIndexKey();
        assertEquals("Unexpected count", numRows, countIndexRows(ikey, table));
    }

    @Test
    public void testKeyWithEmptyString() {
        String tableDDL =
            "create table Simple (" +
                    "id INTEGER," +
                    "map MAP(INTEGER)," +
                    "primary key (id))";

        executeDdl(tableDDL);

        String indexDDL;

        /* map index on map of integer, key-only */
        indexDDL =
            "create index intMapKeyOnly on Simple" +
            "(map.keys())";
        executeDdl(indexDDL);

        Table table = tableImpl.getTable("Simple");
        Row row = table.createRow();
        int numRows = 3;
        for (int i = 0; i < numRows; i++) {
            row.put("id", i).putMap("map").put("", 0).put("key" + i, i);
            assertTrue(tableImpl.put(row, null, null) != null);
        }

        Index index = table.getIndex("intMapKeyOnly");
        IndexKey ikey = index.createIndexKey();
        TableIterator<Row> iter = tableImpl.tableIterator(ikey, null, null);
        int cnt = 0;
        while (iter.hasNext()) {
            iter.next();
            cnt++;
        }
        assertEquals(numRows, cnt);
        iter.close();
    }

    /**
     *  Test insertion of a row in a deeply nested map
     *  of depth 5 with 5 elements in each axis.
     *  Total number of index entries is 5 to the power 5 = 3125
     *  which is within permissible limit of
     *  ParameterState.RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT
     *  [KVSTORE-945] index creation hangs after inserting row in a table
     *                with deeply nested array
     */
    @Test
    public void testValidDeeplyNestedMapIndex() throws Exception {
        final String tableName = "nestedTable";
        final String indexName = "idx";
        final int validDepth = 5;
        final int dim = 5;

        String tableDDL =
            "create table " + tableName + "(" +
            "id INTEGER, " +
            "nestedMap " + getDeeplyNestedMapDefn(validDepth) + " ," +
            "primary key (id))";
        executeDdl(tableDDL);

        String indexDDL =
            "create index " + indexName + " " +
            "on " + tableName + "(" +
            "nestedMap" + getDeeplyNestedMapIndexDefn(validDepth) + ")";
        executeDdl(indexDDL);

        TableImpl table = getTable(tableName);

        ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
        BufferedWriter jsonWriter =
            new BufferedWriter(new OutputStreamWriter(jsonBytes));
        genNestedMap(jsonWriter, validDepth, dim);
        jsonWriter.flush();

        InputStream jsonStream =
            new ByteArrayInputStream(jsonBytes.toByteArray());
        Row row = table.createRow();
        row.put("id", 1);
        row.putMapAsJson("nestedMap", jsonStream, false);

        try {
            assertNotNull("Failed to insert row in table with nested map",
                          tableImpl.put(row, null, null));
        } finally {
            String droptableDDL = "drop table " + tableName;
            executeDdl(droptableDDL);

            jsonBytes.close();
            jsonWriter.close();
            jsonStream.close();
        }
    }

    /**
     *  Test insertion of a row in a deeply nested map
     *  of depth 6 with 5 elements in each axis.
     *  Total number of inxex entries is 5 to the power 6 = 15,625
     *  which exceeds the permissible limit of
     *  ParameterState.RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT
     *  [KVSTORE-945] index creation hangs after inserting row in a table
     *                with deeply nested array
     */
    @Test
    public void testInvalidDeeplyNestedMapIndex() throws Exception {
        final String tableName = "nestedTable";
        final String indexName = "idx";
        final int invalidDepth = 6;
        final int dim = 5;

        String tableDDL =
            "create table " + tableName + "(" +
            "id INTEGER, " +
            "nestedMap " + getDeeplyNestedMapDefn(invalidDepth) + " ," +
            "primary key (id))";
        executeDdl(tableDDL);

        String indexDDL =
            "create index " + indexName + " " +
            "on " + tableName + "(" +
            "nestedMap" + getDeeplyNestedMapIndexDefn(invalidDepth) + ")";
        executeDdl(indexDDL);

        TableImpl table = getTable(tableName);

        ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
        BufferedWriter jsonWriter =
            new BufferedWriter(new OutputStreamWriter(jsonBytes));
        genNestedMap(jsonWriter, invalidDepth, dim);
        jsonWriter.flush();

        InputStream jsonStream =
            new ByteArrayInputStream(jsonBytes.toByteArray());
        Row row = table.createRow();
        row.put("id", 1);
        row.putMapAsJson("nestedMap", jsonStream, false);

        try {
            tableImpl.put(row, null, null);
            fail("put should have failed but succeeded");
        } catch (IllegalArgumentException e) {
            assertTrue("put did not fail with the expected exception",
                       e.getMessage().contains(
                           "Number of index keys per row exceeds the limit"));
        } finally {
            String droptableDDL = "drop table " + tableName;
            executeDdl(droptableDDL);

            jsonBytes.close();
            jsonWriter.close();
            jsonStream.close();
        }
    }

    private String getDeeplyNestedMapDefn(int depth) {
        StringBuffer colDefn = new StringBuffer();

        for (int i = 1; i <= depth; i++) {
            colDefn.append("MAP(");
        }

        colDefn.append("INTEGER");

        for (int i = 1; i <= depth; i++) {
            colDefn.append(")");
        }

        return colDefn.toString();
    }

    private String getDeeplyNestedMapIndexDefn(int depth) {
        StringBuffer indexDefn = new StringBuffer();

        for (int i = 1; i <= depth; i++) {
            indexDefn.append(".values()");
        }

        return indexDefn.toString();
    }

    private void genNestedMap(BufferedWriter jsonWriter,
                              int depth,
                              int dim) throws IOException {
        Random random = new Random();

        if (depth == 1) {
            jsonWriter.write("{");
            for (int i = 1; i <= dim; i++) {
                jsonWriter.write("\"key" +
                               Math.abs(random.nextInt()) +
                               "\":" +
                               random.nextInt());
                if (i < dim) {
                    jsonWriter.write(", ");
                }
            }
            jsonWriter.write("}");
        } else {
            jsonWriter.write("{");
            for (int i = 1; i <= dim; i++) {
                jsonWriter.write("\"key" + Math.abs(random.nextInt()) + "\":");
                genNestedMap(jsonWriter, depth-1, dim);
                if (i < dim) {
                    jsonWriter.write(", ");
                }
            }
            jsonWriter.write("}");
        }
    }

    private TableImpl buildSimpleTable()
        throws Exception {

        String tableDDL =
            "create table Simple (" +
            "id INTEGER," +
            "intArray ARRAY(INTEGER)," +
            "map MAP(INTEGER)," +
            "primary key (id))";

        executeDdl(tableDDL);

        String indexDDL;

        /* map index on map of integer, key-only */
        indexDDL =
            "create index intMapKeyOnly on Simple" +
            "(map.keys())";
        executeDdl(indexDDL);

        /* map index on map of integer value */
        indexDDL =
            "create index intMapValue on Simple" +
            "(map.values())";
        executeDdl(indexDDL);

        /* map index on map of integer, key+value */
        indexDDL =
            "create index intMapKeyValue on Simple" +
            "(map.keys(), map.values())";
        executeDdl(indexDDL);

        /* array index on array of integer */
        indexDDL =
            "create index intArrayIndex on Simple" +
            "(intArray[])";
        executeDdl(indexDDL);

        return getTable("Simple");
    }

    private void populateSimpleTable(TableImpl table, int numRows) {
        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("id", i);

            /*
             * put 2 entries in the map -- one that has a unique key for
             * each row and one common key with unique values in each row.
             */
            row.putMap("map").put(("key" + i), i).put("common", i + 10);
            row.putArray("intArray").add(i);
            tableImpl.put(row, null, null);
        }
    }

    private void populateDupsTable(TableImpl table, int numRows, int numDups) {

        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("id", i);

            /*
             * put numDups duplicates into map and array.
             */
            MapValue map = row.putMap("map");
            ArrayValue array = row.putArray("intArray");
            for (int j = 0; j < numDups; j++) {
                map.put(("key" + i + j), i);
                array.add(i);
            }
            tableImpl.put(row, null, null);
        }
    }

    /**
     * This table includes maps and arrays of complex types.
     */
    private TableImpl buildComplexTable(int numRows)
        throws Exception {

        String tableDDL =
            "create table Complex(" +
            "id INTEGER," +
            "addresses MAP(RECORD(number INTEGER, " +
            "                     zip INTEGER," +
            "                     street STRING," +
            "                     city STRING))," +
            "arrayOfRec ARRAY(RECORD(iField INTEGER, sField STRING))," +
            "primary key (id))";

        executeDdl(tableDDL);

        String indexDDL;

        /* map index on a map value */
        indexDDL =
            "create index mapValueCity on Complex " +
            "(addresses.values().city)";
        executeDdl(indexDDL);

        /* map index on 2 map values */
        indexDDL =
            "create index mapValueCityStreet on Complex " +
            "(addresses.values().city, addresses.values().street)";
        executeDdl(indexDDL);

        /* array index on an array value */
        indexDDL =
            "create index arrayValue on Complex " +
            "(arrayOfRec[].iField)";
        executeDdl(indexDDL);

        /* array index on two array values */
        indexDDL =
            "create index arrayTwoValue on Complex " +
            "(arrayOfRec[].iField, arrayOfRec[].sField)";
        executeDdl(indexDDL);

        TableImpl t = getTable("Complex");

        for (int i = 0; i < numRows; i++) {
            Row row = t.createRow();
            row.put("id", i);

            /*
             * put multiple entries in the map, all with the same city,
             * but multiple streets.
             */
            MapValue map = row.putMap("addresses");
            /* put several map entries */
            for (int j = 0; j < 5; j++) {
                map.putRecord(("addr" + i + j))
                    .put("city", ("city" + i))
                    .put("street", ("street" + i + j));
            }
            row.putArray("arrayOfRec").addRecord(0)
                .put("iField", i).put("sField", ("s" + i));
            tableImpl.put(row, null, null);
        }
        return t;
    }
}
