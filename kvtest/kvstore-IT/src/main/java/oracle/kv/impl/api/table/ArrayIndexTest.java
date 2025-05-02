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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.ByteArrayOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.impl.param.ParameterState;

import org.junit.Test;

/**
 * Test semantics of array indexes.  Array indexes can be confusing.  The
 * important thing to note is that index scans are just that -- index scans.
 * They are low-level iteration over btrees and not queries that filter
 * results.
 *
 * Array indexes add an index entry for every value of the array for a given
 * record.  This can, and does result in duplicate return values for an
 * index scan, especially those that use a range for the operation.
 */
public class ArrayIndexTest extends TableTestBase {

    @Test
    public void testSimpleArrays()
    throws Exception {

        final int numRows = 10;
        final int numLikes = 5;

        String tableDDL =
            "create table array(" +
            "id INTEGER," +
            "likes ARRAY(STRING)," +
            "int_array ARRAY(INTEGER)," +
            "name STRING," +
            "primary key (id))";

        executeDdl(tableDDL);

        String indexDDL;

        indexDDL = "create index NameLikes on array (name, likes[])";
        executeDdl(indexDDL);

        indexDDL = "create index LikesName on array (likes[], name)";
        executeDdl(indexDDL);

        indexDDL = "create index IntArray on array (int_array[])";
        executeDdl(indexDDL);

        /*
         * This should fail because 2 arrays are not allowed.
         */
        indexDDL = "create index ShouldFail on array (likes[], int_array[])";
        executeDdl(indexDDL, false);

        TableImpl arrayTable = getTable("Array");

        addRows(arrayTable, numRows, numLikes);

        Index index = arrayTable.getIndex("NameLikes");
        IndexKey ikey = index.createIndexKey();
        TableIterator<KeyPair> iter = tableImpl.tableKeysIterator(ikey, null, null);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        assertEquals("Unexpected count", numLikes * numRows, count);
        iter.close();

        /*
         * Now, try to specify a range using the array.  Do this with a
         * single-element array.
         */
        ikey.put("name", "name2");

        FieldRange range = index.createFieldRange("likes[]")
            .setStart("like2", true);
        iter = tableImpl.tableKeysIterator(ikey, range.createMultiRowOptions(),
                                           null);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        assertEquals("Unexpected count", numLikes - 2, count);
        iter.close();

        /*
         * Now use the (likes, names) index and do a query to find all
         * matches with a given like in the array ("array contains value").
         * Since each row has the same likes array there should be a match
         * for each one.
         */
        index = arrayTable.getIndex("LikesName");
        ikey = index.createIndexKey();
        ikey.put("likes[]", "like3");
        iter = tableImpl.tableKeysIterator(ikey, null, null);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        assertEquals("Unexpected count", numRows, count);
        iter.close();

        index = arrayTable.getIndex("IntArray");
        ikey = index.createIndexKey();
        ikey.put("int_array[]", 1);
        TableIterator<Row> rowIter = tableImpl.tableIterator(ikey, null, null);
        count = 0;
        while (rowIter.hasNext()) {
            rowIter.next();
            ++count;
        }
        /*
         * Only the first 2 rows have 1 in their integer array.
         */
        assertEquals("Unexpected count", 2, count);
        rowIter.close();

        /*
         * Use a range. The results will be in index key order with:
         * 1.  rows that have 1 in the array
         * 2.  rows that have 2 in the array
         * 3.  rows that have 3 in the array
         * Rows with id 0 and 1 have all 3 values in the range. Row with id 2
         * has values 2 and 3, and row with id 3 has value 3 only.
         * We must check that the result does not contain duplicates.
         */
        ikey = index.createIndexKey();
        range = index.createFieldRange("int_array[]")
            .setStart(1, true).setEnd(3, true);

        rowIter = tableImpl.tableIterator(ikey, range.createMultiRowOptions(),
                                          null);
        count = 0;
        while (rowIter.hasNext()) {
            rowIter.next();
            ++count;
        }
        assertEquals("Unexpected count", 4, count);
        rowIter.close();
    }

    private void addRows(TableImpl table, int numRows, int numLikes) {
        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("name", ("name" + i));
            ArrayValue array = row.putArray("likes");
            for (int j = 0; j < numLikes; j++) {
                array.add(("like" + j));
            }
            /* this can be empty. it's not used at this time */
            array = row.putArray("int_array");
            for (int j = 0; j < numLikes; j++) {
                array.add(i + j);
            }
            tableImpl.put(row, null, null);
        }
    }

    @Test
    public void testHugeArray() throws Exception {

        int NUM_ARRAY_ELEMENTS = Integer.parseInt(
            ParameterState.RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT) + 1;

        String tableDDL =
            "create table array(" +
            "id INTEGER," +
            "int_array ARRAY(INTEGER)," +
            "primary key (id))";

        executeDdl(tableDDL);

        String indexDDL = "create index idx on array(int_array[])";
        executeDdl(indexDDL);

        TableImpl table = getTable("array");

        Row row = table.createRow();
        row.put("id", 1);
        ArrayValue array = row.putArray("int_array");
        for (int j = 0; j < NUM_ARRAY_ELEMENTS; j++) {
            array.add(j);
        }

        try {
            tableImpl.put(row, null, null);
            assert(false);
            fail("Statement should have failed");
        } catch (IllegalArgumentException e) {
            assert(e.getMessage().contains(
                "Number of index keys per row exceeds the limit"));
        }
    }

    /**
     *  Test insertion of a row in a deeply nested array
     *  of depth 5 with 5 elements in each axis.
     *  Total number of index entries is 5 to the power 5 = 3125
     *  which is within permissible limit of
     *  ParameterState.RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT
     *  [KVSTORE-945] index creation hangs after inserting row in a table
     *                with deeply nested array
     */
    @Test
    public void testValidDeeplyNestedArrayIndex() throws Exception {
        final String tableName = "nestedTable";
        final String indexName = "idx";
        final int validDepth = 5;
        final int dim = 5;

        String tableDDL =
            "create table " + tableName + "(" +
            "id INTEGER, " +
            "nestedArray " + getDeeplyNestedArrayDefn(validDepth) + " ," +
            "primary key (id))";
        executeDdl(tableDDL);

        String indexDDL =
            "create index " + indexName + " " +
            "on " + tableName + "(" +
            "nestedArray" + getDeeplyNestedArrayIndexDefn(validDepth) + ")";
        executeDdl(indexDDL);

        TableImpl table = getTable(tableName);

        ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
        BufferedWriter jsonWriter =
            new BufferedWriter(new OutputStreamWriter(jsonBytes));
        genNestedArray(jsonWriter, validDepth, dim);
        jsonWriter.flush();

        InputStream jsonStream =
            new ByteArrayInputStream(jsonBytes.toByteArray());
        Row row = table.createRow();
        row.put("id", 1);
        row.putArrayAsJson("nestedArray", jsonStream, false);

        try {
            assertNotNull("Failed to insert row in table with nested array",
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
     *  Test insertion of a row in a deeply nested array
     *  of depth 6 with 5 elements in each axis.
     *  Total number of index entries is 5 to the power 6 = 15,625
     *  which exceeds the permissible limit of
     *  ParameterState.RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT
     *  [KVSTORE-945] index creation hangs after inserting row in a table
     *                with deeply nested array
     */
    @Test
    public void testInvalidDeeplyNestedArrayIndex() throws Exception {
        final String tableName = "nestedTable";
        final String indexName = "idx";
        final int invalidDepth = 6;
        final int dim = 5;

        String tableDDL =
            "create table " + tableName + "(" +
            "id INTEGER, " +
            "nestedArray " + getDeeplyNestedArrayDefn(invalidDepth) + " ," +
            "primary key (id))";
        executeDdl(tableDDL);

        String indexDDL =
            "create index " + indexName + " " +
            "on " + tableName + "(" +
            "nestedArray" + getDeeplyNestedArrayIndexDefn(invalidDepth) + ")";
        executeDdl(indexDDL);

        TableImpl table = getTable(tableName);

        ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
        BufferedWriter jsonWriter =
            new BufferedWriter(new OutputStreamWriter(jsonBytes));
        genNestedArray(jsonWriter, invalidDepth, dim);
        jsonWriter.flush();

        InputStream jsonStream =
            new ByteArrayInputStream(jsonBytes.toByteArray());
        Row row = table.createRow();
        row.put("id", 1);
        row.putArrayAsJson("nestedArray", jsonStream, false);

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

    private String getDeeplyNestedArrayDefn(int depth) {
        StringBuffer colDefn = new StringBuffer();

        for (int i = 1; i <= depth; i++) {
            colDefn.append("ARRAY(");
        }

        colDefn.append("INTEGER");

        for (int i = 1; i <= depth; i++) {
            colDefn.append(")");
        }

        return colDefn.toString();
    }

    private String getDeeplyNestedArrayIndexDefn(int depth) {
        StringBuffer indexDefn = new StringBuffer();

        for (int i = 1; i <= depth; i++) {
            indexDefn.append("[]");
        }

        return indexDefn.toString();
    }

    private void genNestedArray(BufferedWriter jsonWriter,
                                int depth,
                                int dim) throws IOException {
        Random random = new Random();

        if (depth == 1) {

            jsonWriter.write("[");
            for (int i = 1; i <= dim; i++) {
                jsonWriter.write("" + random.nextInt());
                if (i < dim) {
                    jsonWriter.write(", ");
                }
            }
            jsonWriter.write("]");
        } else {
            jsonWriter.write("[");
            for (int i = 1; i <= dim; i++) {
                genNestedArray(jsonWriter, depth-1, dim);
                if (i < dim) {
                    jsonWriter.write(", ");
                }
            }
            jsonWriter.write("]");
        }
    }

    @Test
    public void testComplexArrays()
    throws Exception {

        boolean showResult = false;

        String tableStatement =
            "CREATE TABLE ARRAYS(                                    \n" +
            "    id INTEGER,                                         \n" +
            "    arrays RECORD(                                      \n" +
            "        arr_int     ARRAY(INTEGER),                     \n" +
            "        arr_rec     ARRAY(RECORD(a INTEGER)),           \n" +
            "        arr_arr_int ARRAY(ARRAY(INTEGER)),              \n" +
            "        arr_arr_rec ARRAY(ARRAY(RECORD(a INTEGER))),    \n" +
            "        arr_map_int ARRAY(MAP(INTEGER)),                \n" +
            "        arr_map_arr ARRAY(MAP(ARRAY(INTEGER))),         \n" +
            "        arr_map_rec ARRAY(MAP(RECORD(a INTEGER)))       \n" +
            "    ),                                                  \n" +
            "    primary key (id)                                    \n" +
            ")";

        String doc0 =
            "{                                                                   \n" +
            "  \"arr_int\" :     [ 1, 2, 3 ],                                    \n" +
            "  \"arr_rec\" :     [ {\"a\" : 1}, {\"a\" : 2} ],                   \n" +
            "  \"arr_arr_int\" : [ [1, 2, 3], [1, 2, 3] ],                       \n" +
            "  \"arr_arr_rec\" : [ [{\"a\" : 1}, {\"a\" : 2}], [{\"a\" : 3}] ],  \n" +
            "  \"arr_map_int\" : [ {\"b\" : 1}, {\"b\" : 2}, {\"b\" : 3} ],      \n" +
            "  \"arr_map_arr\" : [ {\"b\" : [1, 2]}, {\"b\" : [2, 4]}, {\"b\" : [3, 5]} ],\n" +
            "  \"arr_map_rec\" : [ { \"c\" : {\"a\" : 1}, \"d\" : {\"a\" : 2 } },\n" +
            "                      { \"e\" : {\"a\" : 3} } ]                     \n" +
            "}";

        String doc1 =
            "{                                                                   \n" +
            "  \"arr_int\" :     [ 10, 20, 3 ],                                  \n" +
            "  \"arr_rec\" :     [ {\"a\" : 1}, {\"a\" : 20} ],                  \n" +
            "  \"arr_arr_int\" : [ [1, 2, 3], [1, 2, 3] ],                       \n" +
            "  \"arr_arr_rec\" : [ [{\"a\" : 5}, {\"a\" : 2}], [{\"a\" : 6}] ],  \n" +
            "  \"arr_map_int\" : [ {\"b\" : 5}, {\"b\" : 2}, {\"b\" : 6} ],      \n" +
            "  \"arr_map_arr\" : [ { \"b\" : [1, 5] },                           \n" +
            "                      { \"b\" : [1, 6] },                           \n" +
            "                      { \"b\" : [3] } ],                            \n" +
            "  \"arr_map_rec\" : [ { \"c\" : {\"a\" : 7}, \"d\" : {\"a\" : 3 } },\n" +
            "                      { \"e\" : {\"a\" : 3} },                      \n" +
            "                      { \"c\" : {\"a\" : 5} } ]                     \n" +
            "}";

        String docs[] = { doc0, doc1 };

        /* It works */
        String index1ddl =
            "CREATE INDEX idx_arr_map_int ON ARRAYS (arrays.arr_map_int[].b)";

        /* Raises correct error, because the type of the path expr is RECORD */
        String index2ddl =
           "CREATE INDEX idx_arr_map_rec ON ARRAYS (arrays.arr_map_rec[].c)";

        /* It works */
        String index3ddl =
            "CREATE INDEX idx_arr_map_rec ON ARRAYS (arrays.arr_map_rec[].c.a)";

        /*
         * This used to raise IndexOutOfBoundsException during index population
         * before the fix to [#24494]. Instead kvstore should detect this as an
         * invalid index definition.
         */
        String index4ddl =
           "CREATE INDEX idx_arr_map_arr ON ARRAYS (arrays.arr_map_arr[].b)";

        /*
         * This used to raise IndexOutOfBoundsException during index population
         * before the fix to [#24494]. Instead kvstore should detect this as an
         * invalid index definition.
         */
        String index5ddl =
           "CREATE INDEX idx_arr_arr_rec ON ARRAYS (arrays.arr_arr_rec[].a)";

        /*
         * This used to raise IndexOutOfBoundsException during index query
         * before the fix to [#24494].
         */
        String index6ddl =
           "CREATE INDEX idx_arr_int ON ARRAYS (arrays.arr_int[])";

        /* It works */
        String index7ddl =
            "CREATE INDEX idx_arr_rec ON ARRAYS (arrays.arr_rec[].a)";

        /*
         * As per [#24494] this should be detected as a duplicate of index7,
         * and thus rejected.
         */
       String index8ddl =
            "CREATE INDEX idx_arr_rec2 ON ARRAYS (arrays.arr_rec[].a)";

        /*
         * Can not index an array as a whole; use arr_arr_int[][] to index the
         * elements of the array
         */
        String index9ddl =
            "CREATE INDEX idx_arr_arr_int1 ON ARRAYS (arrays.arr_arr_int)";

        /*
         * Can not index an array as a whole; use arr_arr_int[][] to index the
         * elements of the array
         */
        String index10ddl =
            "CREATE INDEX idx_arr_arr_int2 ON ARRAYS (arrays.arr_arr_int[])";

        executeDdl(tableStatement);

        /* Create the indexes */

        executeDdl(index1ddl);

        executeDdl(index3ddl);

        executeDdl(index6ddl);

        executeDdl(index7ddl);

        executeDdl(index2ddl, false);

        executeDdl(index4ddl, false);

        executeDdl(index5ddl, false);

        executeDdl(index8ddl, false);

        executeDdl(index9ddl, false);

        executeDdl(index10ddl, false);

        TableAPI tapi = store.getTableAPI();
        Table table = tapi.getTable("ARRAYS");
        Index index1 = table.getIndex("idx_arr_map_int");
        Index index3 = table.getIndex("idx_arr_map_rec");
        Index index6 = table.getIndex("idx_arr_int");

        /* Populate the table (and the indexes) */

        int numRows = 2;

        for (int i = 0; i < numRows; i++) {

            Row row = table.createRow();
            row.put("id", i);
            row.putRecordAsJson("arrays", docs[i], true);

            tableImpl.put(row, null, null);
        }

        IndexKey idxKey;
        FieldRange range;
        int count;
        TableIterator<Row> iter;

        // Query index 1
        idxKey = index1.createIndexKey();
        idxKey.put("arrays.arr_map_int[].b", 2);

        iter = tapi.tableIterator(idxKey, null, null);

        count = 0;
        while (iter.hasNext()) {
            Row result = iter.next();
            ++count;
            if (showResult) {
                System.out.println(result);
            }
        }

        assertEquals("Unexpected count", 2, count);

        // Query index 3
        idxKey = index3.createIndexKey();
        idxKey.put("arrays.arr_map_rec[].c.a", 7);

        iter = tapi.tableIterator(idxKey, null, null);

        count = 0;
        while (iter.hasNext()) {
            Row result = iter.next();
            ++count;
            if (showResult) {
                System.out.println(result);
            }
        }

        assertEquals("Unexpected count", 1, count);

        idxKey = index3.createIndexKey();
        range = index3.createFieldRange("arrays.arr_map_rec[].c.a");
        range.setStart(1, true);

        iter = tapi.tableIterator(idxKey, range.createMultiRowOptions(), null);

        count = 0;
        while (iter.hasNext()) {
            Row result = iter.next();
            ++count;
            if (showResult) {
                System.out.println(result);
            }
        }

        assertEquals("Unexpected count", 2, count);

        // Query index 6
        idxKey = index6.createIndexKey();
        idxKey.put("arrays.arr_int[]", 2);

        iter = tapi.tableIterator(idxKey, null, null);

        count = 0;
        while (iter.hasNext()) {
            Row result = iter.next();
            ++count;
            if (showResult) {
                System.out.println(result);
            }
        }

        assertEquals("Unexpected count", 1, count);
    }

    @Test
    public void testNestedArrays() {

        boolean verbose = false;

        /*
         * Test 1
         */
        String tableDDL = 
        "CREATE TABLE Foo(\n" +
        "   id INTEGER,\n" +
        "   record RECORD(long LONG,\n" +
        "                 int INTEGER,\n" +
        "                 string STRING,\n" +
        "                 map MAP(RECORD(foo double,\n" +
        "                                bar string,\n" +
        "                                array ARRAY(RECORD(map2 MAP(ARRAY(integer)),\n" +
        "                                                   array2 ARRAY(RECORD(foo2 integer,\n" +
        "                                                                       bar2 string\n" +
        "                                                  )\n" +
        "                                            )\n" +
        "                              )\n" +
        "                        )\n" +
        "           )\n" +
        "       )\n" +
        " ),\n" +
        "info JSON,\n" +
        " primary key (id)\n" +
        ")";

        String jsonText =
        "{                                                                  \n" +
        "     \"long\" : 50,                                                \n" +
        "     \"int\" : 20,                                                 \n" +
        "     \"string\" : \"xyz\",                                         \n" +
        "     \"map\" : {                                                   \n" +
        "         \"rec1\" : {                                              \n" +
        "             \"foo\" : 2.3,                                        \n" +
        "             \"bar\" : \"abc\",                                    \n" +
        "             \"array\" : [                                         \n" +
        "                 {                                                 \n" +
        "                     \"map2\" : { \"arr1\": [ 1, 2, 34 ],          \n" +
        "                                  \"arr2\": [ 34, 23, 76 ] },      \n" +
        "                     \"array2\" :  [                               \n" +
        "                         { \"foo2\" : 4, \"bar2\" : \"fgr\" },     \n" +
        "                         { \"foo2\" : 6, \"bar2\" : \"krp\"}       \n" +
        "                     ]                                             \n" +
        "                 },                                                \n" +
        "                 {                                                 \n" +
        "                     \"map2\" : { \"arr1\": [ 10, 2, 64 ],         \n" +
        "                                  \"arr2\": [ 4, 23, 45 ] },       \n" +
        "                     \"array2\" :  [                               \n" + 
        "                         { \"foo2\" : 2, \"bar2\" : \"wqe\" },     \n" +
        "                         { \"foo2\" : 5, \"bar2\" : \"krp\"}       \n" +
        "                     ]                                             \n" +
        "                 }                                                 \n" +
        "             ]                                                     \n" +
        "         }                                                         \n" +
        "     }                                                             \n" +
        "}";

        executeDdl(tableDDL);

        TableImpl table = (TableImpl)tableImpl.getTable(getNamespace(), "foo");

        RowImpl row = table.createRow();
        row.put("id", 1);
        row.putRecordAsJson("record", jsonText, true);

        IndexImpl index = new IndexImpl("idx_long_foo2_bar2", table,
                          makeIndexList("record.long",
                                        "record.map.values().array[].array2[].foo2",
                                        "record.map.values().array[].array2[].bar2"),
                          null, null);
        table.addIndex(index);

        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "record.map.values().array[].array2[foo2@1 , bar2@2]"));

        List<byte[]> keys;
        // = index.extractIndexKeys(row);

        //System.out.println("Num Keys = " + keys.size());
        //assert(keys.size() == 4);

        try {
            index = new IndexImpl("idx_bad0", table,
                    makeIndexList("record.long",
                                  "record.map.values().array.array2[].foo2",
                                  "record.map.values().array[].array2[].bar2"),
                                  null, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad0\n" + e + "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad1", table,
                    makeIndexList("record.long",
                                  "record.map.values().array[].array2[].foo2",
                                  "record.map.values().array[].map2.values()[]"),
                                  null, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad1\n" + e + "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad2", table,
                    makeIndexList("record.long",
                                  "record.map.values().array[].map2.values()[]",
                                  "record.map.values().array[].array2[].foo2"),
                                  null, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad2\n" + e + "\n");
            }
        }

        /*
         * Test 2
         */
        tableDDL =
            "CREATE TABLE ARRAYS(                                    \n" +
            "    id INTEGER,                                         \n" +
            "    arrays RECORD(                                      \n" +
            "        arr_int     ARRAY(INTEGER),                     \n" +
            "        arr_rec     ARRAY(RECORD(a INTEGER)),           \n" +
            "        arr_arr_int ARRAY(ARRAY(INTEGER)),              \n" +
            "        arr_arr_rec ARRAY(ARRAY(RECORD(a INTEGER))),    \n" +
            "        arr_map_int ARRAY(MAP(INTEGER)),                \n" +
            "        arr_map_arr ARRAY(MAP(ARRAY(INTEGER))),         \n" +
            "        arr_map_rec ARRAY(MAP(RECORD(a INTEGER)))       \n" +
            "    ),                                                  \n" +
            "    matrixes ARRAY(RECORD(mid INTEGER,                  \n" +
            "                          mdate TIMESTAMP(0),           \n" +
            "                          matrix ARRAY(MAP(ARRAY(RECORD(foo INTEGER,    \n" +
            "                                                        bar INTEGER)))) \n" +
            "                         )                              \n" +
            "                  ),                                    \n" +
            "    primary key (id)                                    \n" +
            ")";

        String doc0 =
            "{                                                                   \n" +
            "  \"arr_int\" :     [ 1, 2, 3 ],                                    \n" +
            "  \"arr_rec\" :     [ {\"a\" : 1}, {\"a\" : 2} ],                   \n" +
            "  \"arr_arr_int\" : [ [1, 2, 3], [10, 2, 8] ],                      \n" +
            "  \"arr_arr_rec\" : [ [{\"a\" : 1}, {\"a\" : 2}], [{\"a\" : 3}] ],  \n" +
            "  \"arr_map_int\" : [ {\"b\" : 1}, {\"b\" : 2}, {\"b\" : 3} ],      \n" +
            "  \"arr_map_arr\" : [ {\"b\" : [1, 2]}, {\"b\" : [2, 4]}, {\"b\" : [3, 5]} ],\n" +
            "  \"arr_map_rec\" : [ { \"c\" : {\"a\" : 1}, \"d\" : {\"a\" : 2 } },\n" +
            "                      { \"e\" : {\"a\" : 3} } ]                     \n" +
            "}";

        executeDdl(tableDDL);

        table = (TableImpl)tableImpl.getTable(getNamespace(), "arrays");

        row = table.createRow();
        row.put("id", 1);
        row.putRecordAsJson("arrays", doc0, true);

        index = new IndexImpl("idx_arr_arr_rec", table,
                              makeIndexList("arrays.arr_arr_rec[][].a"),
                              null, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "arrays.arr_arr_rec[][a@0]"));

        keys = index.extractIndexKeys(row, 100);
        assert(keys.size() == 3);

        index = new IndexImpl("idx_arr_arr_int", table,
                              makeIndexList("arrays.arr_arr_int[][]"),
                              null, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "arrays.arr_arr_int[][@0]"));

        keys = index.extractIndexKeys(row, 100);
        assert(keys.size() == 6);

        index = new IndexImpl("idx_arr_map_rec", table,
                              makeIndexList("arrays.arr_map_rec[].c.a"),
                              null, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "arrays.arr_map_rec[c.a@0]"));

        keys = index.extractIndexKeys(row, 100);
        assert(keys.size() == 2);

        index = new IndexImpl("idx_matrixes1", table,
                              makeIndexList("matrixes[].matrix[].values()[].foo"),
                              null, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "matrixes[].matrix[].values()[foo@0]"));

        index = new IndexImpl("idx_matrixes2", table,
                              makeIndexList("matrixes[].mdate",
                                            "matrixes[].matrix[].keys()",
                                            "matrixes[].matrix[].values()[].bar"),
                              null, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "matrixes[mdate@0].matrix[].values(@@1)[bar@2]"));

        /*
         * Test 3
         */
        tableDDL =
            "CREATE TABLE Bar(         \n" +
            "    id INTEGER,           \n" +
            "    info JSON,            \n" +
            "    primary key (id)      \n" +
            ")";

        executeDdl(tableDDL);

        table = (TableImpl)tableImpl.getTable(getNamespace(), "bar");

        ArrayList<Type> types = new ArrayList<Type>();
        types.add(Type.INTEGER);
        types.add(Type.ANY_ATOMIC);
        types.add(Type.STRING);

        ArrayList<Type> types2 = new ArrayList<Type>();
        types2.add(null);
        types2.add(Type.ANY_ATOMIC);
        types2.add(Type.STRING);

        ArrayList<Type> types3 = new ArrayList<Type>();
        types3.add(Type.ANY_ATOMIC);
        types3.add(null);

        ArrayList<Type> types4 = new ArrayList<Type>();
        types4.add(null);
        types4.add(Type.ANY_ATOMIC);
        types4.add(null);

        index = new IndexImpl("idx_areacode_kind_state", table,
                makeIndexList("info.addresses[].phones[][][].areacode",
                              "info.addresses[].phones[][][].kind",
                              "info.addresses[].state"),
                              types, null);
        table.addIndex(index);

        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.addresses[state@2].phones[][][areacode@0 , kind@1]"));

       index = new IndexImpl("idx_state_areacode_kind", table,
               makeIndexList("info.addresses[].state",
                             "info.addresses[].phones[][][].areacode",
                             "info.addresses[].phones[][][].kind"),
                             types, null);
        table.addIndex(index);

        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.addresses[state@0].phones[][][areacode@1 , kind@2]"));

       index = new IndexImpl("idx_keys_areacode_number", table,
               makeIndexList("info.addresses.phones.values().keys()",
                             "info.addresses.phones.values().values().areacode",
                             "info.addresses.phones.values().values().number"),
                             types2, null);
        table.addIndex(index);

        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.addresses.phones.values().values(areacode@1 , number@2 , @@0)"));

        index = new IndexImpl("idx1", table,
                    makeIndexList("info.a.b[].c",
                                  "info.a.b[].values().f",
                                  "info.g"),
                                  types, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[c@0].values(f@1)"));

        index = new IndexImpl("idx2", table,
                              makeIndexList("info.a.b[].values().f",
                                            "info.a.b[].c",
                                            "info.g"),
                              types, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[c@1].values(f@0)"));

        index = new IndexImpl("idx3", table,
                makeIndexList("info.a.b.c.values()[]",
                              "info.a.b.c.keys()"),
                              types3, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b.c.values(@@1)[@0]"));

        index = new IndexImpl("idx4", table,
                makeIndexList("info.a.b.c.keys()",
                              "info.a.b.c.values()[]"),
                              types2, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b.c.values(@@0)[@1]"));

        index = new IndexImpl("idx5", table,
                makeIndexList("info.a.b[].c",
                              "info.a.b[].keys()"),
                              types3, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[c@0].values(@@1)"));

        index = new IndexImpl("idx6", table,
                makeIndexList("info.a.b[].keys()",
                              "info.a.b[].c"),
                              types2, null);
        if (verbose) {
        System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[c@1].values(@@0)"));

        index = new IndexImpl("idx7", table,
                makeIndexList("info.a.b[].g",
                              "info.a.b[].c.e.f[]"),
                              types, null);
        if (verbose) {
        System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[g@0].c.e.f[@1]"));

        index = new IndexImpl("idx8", table,
                makeIndexList("info.a.b[].c.e.f[]",
                              "info.a.b[].g"),
                              types, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[g@1].c.e.f[@0]"));

        index = new IndexImpl("idx9", table,
                makeIndexList("info.a.b[].c.e[]",
                              "info.a.b[].g"),
                              types, null);
        if (verbose) {
            System.out.println("Created index " + index.getName());
        }
        assertTrue(index.getMultiKeyPaths().getPathName(true).equals(
                   "info.a.b[g@1].c.e[@0]"));

        try {
            index = new IndexImpl("idx_bad3", table,
                    makeIndexList("info.addresses.phones[][][].areacode",
                                  "info.addresses.phones[].state",
                                  "info.addresses.phones[][][].kind"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad3\n" + e + "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad4", table,
                    makeIndexList("info.addresses.phones[].state",
                                  "info.addresses.phones[][][].areacode",
                                  "info.addresses.phones[][][].kind"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad4\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad41", table,
                    makeIndexList("info.addresses.phones[].state",
                                  "info.addresses.phones[][].areacode",
                                  "info.kind"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad41\n" + e+ "\n");
            }
        }


        try {
            index = new IndexImpl("idx_bad5", table,
                    makeIndexList("info.addresses.phones[]",
                                  "info.addresses.phones[].state"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad5\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad6", table,
                    makeIndexList("info.addresses.phones[].state",
                                  "info.addresses.phones[]"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad6\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad7", table,
                    makeIndexList("info.addresses.phones.keys()",
                                  "info.addresses.phones.values().state",
                                  "info.addresses.phones.keys()"),
                                  types4, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad7\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad8", table,
                    makeIndexList("info.addresses.phones.keys()",
                                  "info.addresses.phones.values().state",
                                  "info.addresses.phones.values()"),
                                  types2, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad8\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad9", table,
                    makeIndexList("info.a.b.c[][]",
                                  "info.a.b.c[]"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad9\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad10", table,
                    makeIndexList("info.a.b.c.values()[]",
                                  "info.a.b.c.values()"),
                                  types, null);
            assert(false);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad10\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad11", table,
                    makeIndexList("info.a.b[].c",
                                  "info.a.b.keys()"),
                                  types3, null);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad11\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad12", table,
                    makeIndexList("info.a.b.c[]",
                                  "info.a.b.keys()"),
                                  types3, null);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad12\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad13", table,
                    makeIndexList("info.a.b.keys()",
                                  "info.a.b.c[]"),
                                  types2, null);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad13\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad14", table,
                    makeIndexList("info.a.b.c[]",
                                  "info.a.b.values()"),
                                  types, null);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad14\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad15", table,
                    makeIndexList("info.a.b.values()",
                                  "info.a.b.c[]"),
                                  types, null);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad15\n" + e+ "\n");
            }
        }

        try {
            index = new IndexImpl("idx_bad16", table,
                    makeIndexList("info.a.b[].c",
                                  "info.a.b.keys()"),
                                  types3, null);
        } catch (Exception e) {
            if (verbose) {
                System.out.println("Failed to create index idx_bad16\n" + e+ "\n");
            }
        }
    }
}
