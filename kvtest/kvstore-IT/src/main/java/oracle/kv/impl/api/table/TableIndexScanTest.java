/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableLimits.NO_CHANGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.Direction;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldRange;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.MultiGetResult;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.TableIteratorOptions;

import org.junit.Test;

/*
 * Tests the below scan APIs with the support of continuation and
 * TableIterateOption.maxReadKB limit.
 *
 * - Table scan
 *   TableAPIImpl.multGet(PrimaryKey key,
 *                        byte[] continuationKey,
 *                        MultiRowOptions getOptions,
 *                        TableIterateOptions iterateOptions)
 *   TableAPIImpl.multGetKeys(PrimaryKey key,
 *                            byte[] continuationKey,
 *                            MultiRowOptions getOptions,
 *                            TableIterateOptions iterateOptions)
 * - Index scan
 *   TableAPIImpl.multGet(IndexKey key,
 *                        byte[] continuationKey,
 *                        MultiRowOptions getOptions,
 *                        TableIterateOptions iterateOptions)
 *
 *   TableAPIImpl.multGetKeys(IndexKey key,
 *                            byte[] continuationKey,
 *                            MultiRowOptions getOptions,
 *                            TableIterateOptions iterateOptions)
 */
public class TableIndexScanTest extends TableTestBase {

    /**
     * Test partition-based table scan.
     */
    @Test
    public void testMultiGetWithPrimaryKey()
        throws Exception {

        final int rowKBSize = 2;

        TableImpl shardTable = buildShardedTable();

        setTableLimits(shardTable, new TableLimits(40000, 10000, NO_CHANGE));

        int numMajor1 = 5;
        int numPerMajor1 = 6;
        int numPerMajor2 = 4;

        addShardTableRows(shardTable, numMajor1, numPerMajor1,
                          numPerMajor2, rowKBSize);

        /* row */
        runPrimaryKeyMultiGetTest(shardTable, numMajor1, numPerMajor1,
                                  numPerMajor2, false, rowKBSize);

        /* key only */
        runPrimaryKeyMultiGetTest(shardTable, numMajor1, numPerMajor1,
                                  numPerMajor2, true, rowKBSize);

    }

    private void runPrimaryKeyMultiGetTest(TableImpl shardTable,
                                           int numMajor1,
                                           int numPerMajor1,
                                           int numPerMajor2,
                                           boolean keyOnly,
                                           int rowKBSize) {
        PrimaryKey key = null;
        FieldRange range = null;
        MultiRowOptions mro = null;
        int rowCount = numMajor1 * numPerMajor1 * numPerMajor2;

        /**
         * Iterate entire table
         */
        key = shardTable.createPrimaryKey();
        int expCount = rowCount;
        int[] limitNums = new int[]{1, 5, 50, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /**
         * Iterate with incomplete shard key
         */
        key = shardTable.createPrimaryKey();
        key.put("sid1", 1);
        expCount = numPerMajor1 * numPerMajor2;
        limitNums = new int[]{1, 5, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /**
         * Iterate with complete shard key
         */
        key = shardTable.createPrimaryKey();
        key.put("sid1", 1);
        key.put("sid2", 2);
        expCount = numPerMajor2;
        limitNums = new int[]{1, (expCount + 1)/2, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /**
         * Iterate with complete primary key
         */
        key = shardTable.createPrimaryKey();
        key.put("sid1", 1);
        key.put("sid2", 2);
        key.put("id3", numPerMajor2 - 1);
        expCount = 1;
        limitNums = new int[]{1, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /**
         * Iterate with complete primary key, 0 row returned.
         */
        key = shardTable.createPrimaryKey();
        key.put("sid1", 1);
        key.put("sid2", 2);
        key.put("id3", numPerMajor2);
        expCount = 0;
        limitNums = new int[]{10};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /**
         * Iterate with incomplete shard key and a sub range
         */

        /* key: {"sid1":1}, range: sid2 >= 1 */
        key = shardTable.createPrimaryKey();
        key.put("sid1", 1);
        range = shardTable.createFieldRange("sid2").setStart(1, true);
        mro = range.createMultiRowOptions();
        expCount = (numPerMajor1 - 1) * numPerMajor2;
        limitNums = new int[]{1, 10, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, mro, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /* key: {"sid1":1}, range: sid2 < 3 */
        range = shardTable.createFieldRange("sid2").setEnd(3, false);
        mro = range.createMultiRowOptions();
        expCount = 3 * numPerMajor2;
        limitNums = new int[]{1, 5, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, mro, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /* key: {"sid1":1}, range: 1 <= sid2 < 5 */
        range = shardTable.createFieldRange("sid2").setStart(1, true)
                .setEnd(numPerMajor1 - 1 , false);
        mro = range.createMultiRowOptions();
        expCount = (numPerMajor1 - 2) * numPerMajor2;
        limitNums = new int[]{1, 5, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, mro, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /**
         * Iterate with complete shard key and a sub range
         */

        /* key: {"sid1":1, "sid2":2}, range: id3 >= 1 */
        key = shardTable.createPrimaryKey();
        key.put("sid1", 1);
        key.put("sid2", 2);
        range = shardTable.createFieldRange("id3").setStart(1, true);
        mro = range.createMultiRowOptions();
        expCount = numPerMajor2 - 1;
        limitNums = new int[]{1, 2, expCount, expCount + 1};
        for (int limit : limitNums) {
            runGetMultiRowsTest(key, mro, limit, keyOnly, expCount,
                rowKBSize);
        }

        /* key: {"sid1":1, "sid2":2}, range: id3 < 3 */
        range = shardTable.createFieldRange("id3").setEnd(3, false);
        mro = range.createMultiRowOptions();
        expCount = numPerMajor2 - 1;
        limitNums = new int[]{1, 2, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, mro, limitNum, keyOnly, expCount,
                rowKBSize);
        }

        /* key: {"sid1":1, "sid2":2}, range: 1 <= id3 < 3 */
        range = shardTable.createFieldRange("id3").setStart(1, true)
                .setEnd(numPerMajor2 - 1 , false);
        mro = range.createMultiRowOptions();
        expCount = numPerMajor2 - 2;
        limitNums = new int[]{expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, mro, limitNum, keyOnly, expCount,
                rowKBSize);
        }
    }

    /**
     * Test shared-base index scan.
     */
    @Test
    public void testMultiGetWithIndexKey()
        throws Exception {

        final int rowKBSize = 2;
        TableImpl shardTable = buildShardedTable();
        addIndex(shardTable, "idxInt1", new String[] {"int1"}, true);
        addIndex(shardTable, "idxInt1Int2", new String[] {"int1", "int2"}, true);

        shardTable = getTable("Sharded");
        setTableLimits(shardTable, new TableLimits(40000, 10000, NO_CHANGE));

        int numMajor1 = 5;
        int numPerMajor1 = 6;
        int numPerMajor2 = 4;

        addShardTableRows(shardTable, numMajor1, numPerMajor1, numPerMajor2,
                          rowKBSize);

        runIndexMultiGetTest(shardTable, numMajor1, numPerMajor1, numPerMajor2,
                             false, rowKBSize);

        runIndexMultiGetTest(shardTable, numMajor1, numPerMajor1, numPerMajor2,
                             true, rowKBSize);
    }

    private void runIndexMultiGetTest(TableImpl shardTable,
                                      int numMajor1,
                                      int numPerMajor1,
                                      int numPerMajor2,
                                      boolean keyOnly,
                                      int rowKBSize) {
        IndexKey key = null;
        FieldRange range = null;
        MultiRowOptions mro = null;
        int rowCount = numMajor1 * numPerMajor1 * numPerMajor2;

        Index idxInt1Shard = shardTable.getIndex("idxInt1");
        Index idxInt1Int2Shard = shardTable.getIndex("idxInt1Int2");

        /**
         * Iterate with an empty index key.
         */
        key = idxInt1Shard.createIndexKey();
        int expCount = rowCount;
        int[] limitNums = new int[]{1, 5, 10, 50, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount, rowKBSize);
        }

        /**
         * Iterate with a partial index key.
         */
        key = idxInt1Int2Shard.createIndexKey();
        key.put("int1", 1);
        expCount = numPerMajor1 * numPerMajor2;
        limitNums = new int[]{1, 5, 10, 50, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, null, limitNum, keyOnly, expCount, rowKBSize);
        }

        /**
         * Iterate with a partial index key and a range.
         */
        key = idxInt1Int2Shard.createIndexKey();
        key.put("int1", 1);
        range = idxInt1Int2Shard.createFieldRange("int2");
        range.setStart(1, true).setEnd(3, true);
        mro = range.createMultiRowOptions();
        expCount = 3 * numPerMajor2;
        limitNums = new int[]{1, 5, 10, expCount, expCount + 1};
        for (int limitNum : limitNums) {
            runGetMultiRowsTest(key, mro, limitNum, keyOnly, expCount, rowKBSize);
        }
    }

    /**
     * Test batch KB size limit in partition-based table scan and shard-based
     * index scan.
     */
    @Test
    public void testMultiGetWithSizeLimit()
        throws Exception {

        final int numRows = 200;
        final int rowKBSize = 2;
        TableImpl table = buildSizeLimitTable();
        Index idx1 = table.getIndex("idx1");

        setTableLimits(table, new TableLimits(40000, 10000, NO_CHANGE));
        PrimaryKey pKey = table.createPrimaryKey();
        IndexKey iKey = idx1.createIndexKey();

        /* Run test, 0 row returned. */
        runMultiGetWithLimits(pKey, null, 0 /*limitNum*/, 0 /*limitKB*/,
                              0 /*expCount*/, rowKBSize);
        runMultiGetWithLimits(iKey, null, 0 /*limitNum*/, 0 /*limitKB*/,
                              0 /*expCount*/, rowKBSize);

        /* Put 200 rows to table */
        addSizeLimitRows(table, numRows, rowKBSize);
        runMultiGetWithLimits(pKey, null, 0 /*limitNum*/, 0 /*limitKB*/,
                              numRows, rowKBSize);
        runMultiGetWithLimits(iKey, null, 0 /*limitNum*/, 0 /*limitKB*/,
                              numRows, rowKBSize);

        int[] limitNums = new int [] {0, 10, 100, 200, 201};
        int[] limitKBs = new int[] {5, 10, 50, 100, 400};
        for (int lmtNum : limitNums) {
            for (int lmtKB : limitKBs) {
                runMultiGetWithLimits(pKey, null, lmtNum, lmtKB, numRows,
                                      rowKBSize);
                runMultiGetWithLimits(iKey, null, lmtNum, lmtKB, numRows,
                                      rowKBSize);
            }
        }
    }

    private void runMultiGetWithLimits(RecordValue key,
                                       MultiRowOptions mro,
                                       int limitNum, int limitKB,
                                       int expCount, int rowKBSize) {

        runGetMultiRowsTest(key, mro, limitNum, limitKB, false,
                            expCount, rowKBSize);
        runGetMultiRowsTest(key, mro, limitNum, limitKB, true,
                            expCount, rowKBSize);
    }

    @Test
    public void testMultiGetMultiKeyIndexWithSizeLimit()
        throws Exception {

        final int numRows = 200;
        final int rowKBSize = 2;
        final int sizeOfArray = 3;
        final int sizeOfMap = 4;

        TableImpl table = buildComplexTable();
        Index idxArray = table.getIndex("idxArray");
        Index idxMap = table.getIndex("idxMap");
        setTableLimits(table, new TableLimits(10000, 10000, NO_CHANGE));
        addComplexRows(table, numRows, sizeOfArray, sizeOfMap, rowKBSize);

        IndexKey arrayIdxKey = idxArray.createIndexKey();
        IndexKey mapIdxKey = idxMap.createIndexKey();
        int expNumRowsIdxArray = numRows * sizeOfArray;
        int expNumRowsIdxMap = numRows * sizeOfMap;

        int[] limitKBs = new int[] {5, 10, 50, 100, 3000};
        for (int limitKB : limitKBs) {
            runGetMultiRowsTest(arrayIdxKey, null, 0, limitKB,
                                false, expNumRowsIdxArray, rowKBSize);
            runGetMultiRowsTest(mapIdxKey, null, 0, limitKB,
                                true, expNumRowsIdxMap, rowKBSize);
        }
    }

    private void runGetMultiRowsTest(RecordValue key, MultiRowOptions mro,
                                     int limitNum, boolean keyOnly,
                                     int expCount, int rowKBSize) {
        runGetMultiRowsTest(key, mro, limitNum, 0, keyOnly, expCount, rowKBSize);
    }

    private void runGetMultiRowsTest(RecordValue key, MultiRowOptions mro,
                                     int limitNum, int limitKB, boolean keyOnly,
                                     int expCount, int rowKBSize) {

        TableIteratorOptions tio = new TableIteratorOptions(Direction.UNORDERED,
                                                            limitKB, null, 0,
                                                            null, 0, limitNum);
        byte[] contdKey = null;
        List<Object> rows = new ArrayList<Object>();
        int batchSize = limitNum > 0 ? limitNum :
            (limitKB == 0) ? KVStoreImpl.DEFAULT_ITERATOR_BATCH_SIZE : 0;

        int readKB = 0;
        int lastNumRead = 0;
        while (true) {
            MultiGetResult<?> result;
            if (key.isPrimaryKey()) {
                PrimaryKey pKey = key.asPrimaryKey();
                if (keyOnly) {
                    result =
                    	tableImpl.multiGetKeys(pKey, contdKey, mro, tio);
                } else {
                    result = tableImpl.multiGet(pKey, contdKey, mro, tio);
                }
            } else {
                IndexKey iKey = key.asIndexKey();
                if (keyOnly) {
                    result =
                    	tableImpl.multiGetKeys(iKey, contdKey, mro, tio);
                } else {
                    result = tableImpl.multiGet(iKey, contdKey, mro, tio);
                }
            }
            readKB += result.getReadKB();

            if (!result.getResult().isEmpty()) {
                rows.addAll(result.getResult());
            }

            if (batchSize > 0) {
                assertTrue(result.getResult().size() <= batchSize + 1);
            }
            assertTrue(result.getReadKB() >= 0);
            assertTrue(result.getWriteKB() == 0);
            contdKey = result.getContinuationKey();

            if (limitKB > 0) {
                int maxActualKB = 0;
                if (key.isPrimaryKey()) {
                    maxActualKB = keyOnly ? limitKB + 1 : limitKB;
                } else {
                    maxActualKB = limitKB + (keyOnly ? 1 : rowKBSize);
                }
                assertTrue("Actual readKB " + result.getReadKB() +
                           ", expect <= " + maxActualKB,
                           result.getReadKB() <= maxActualKB);
            }

            if (contdKey == null) {
                lastNumRead = result.getResult().size();
                break;
            }
        }

        assertEquals("Unexpected number of records", expCount, rows.size());

        int expReadKB = expCount * (keyOnly ? 1 : rowKBSize);
        if (lastNumRead == 0) {
            expReadKB++;
        }
        assertEquals("Unexpected readKB", expReadKB, readKB);
    }

    private TableImpl buildShardedTable()
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("Sharded")
            .addInteger("sid1")
            .addInteger("sid2")
            .addInteger("id3")
            .addInteger("int1")
            .addInteger("int2")
            .addInteger("int3")
            .addBinary("binary")
            .primaryKey("sid1", "sid2", "id3")
            .shardKey("sid1", "sid2")
            .buildTable();

        return addTable(table);
    }

    private void addShardTableRows(TableImpl table,
                                   int numMajor1,
                                   int numPerMajor1,
                                   int numPerMajor2,
                                   int rowKBSize) {
        for (int i = 0; i < numMajor1; i++) {
            for (int j = 0; j < numPerMajor1; j++) {
                Row row = table.createRow();
                row.put("sid1", i);
                row.put("sid2", j);
                for (int k = 0; k < numPerMajor2; k++) {
                    row.put("id3", k);
                    row.put("int1", i);
                    row.put("int2", j);
                    row.put("int3", k);
                    row.put("binary", new byte[1024 * (rowKBSize - 1)]);
                    tableImpl.put(row, null, null);
                }
            }
        }
    }

    private TableImpl buildSizeLimitTable()
        throws Exception {

        TableImpl table =
                TableBuilder.createTableBuilder("testSizeLimitTable")
                .addInteger("id")
                .addString("string")
                .addBinary("binary")
                .primaryKey("id")
                .buildTable();
        table = addTable(table);
        addIndex(table, "idx1", new String[]{"string"}, true);

        return getTable(table.getFullName());
    }

    private void addSizeLimitRows(TableImpl table, int num, int nkb) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("string", "string " + i);
            row.put("binary", new byte[1024 * (nkb - 1)]);
            Version ver = tableImpl.put(row, null, null);
            assertTrue(ver != null);
        }
    }

    private TableImpl buildComplexTable()
        throws Exception {

        TableImpl table =
                TableBuilder.createTableBuilder("complexTable")
                .addInteger("id")
                .addField("array", TableBuilder.createArrayBuilder()
                                               .addString().build())

                .addField("map", TableBuilder.createMapBuilder()
                                               .addString().build())
                .addBinary("binary")
                .primaryKey("id")
                .buildTable();

        table = addTable(table);
        addIndex(table, "idxArray", new String[]{"array[]"}, true);
        addIndex(table, "idxMap", new String[]{"map.keys()", "map.values()"},
                 true);
        return getTable(table.getFullName());
    }

    private void addComplexRows(TableImpl table, int num,
                                int sizeOfArray, int sizeOfMap,
                                int nkb) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            ArrayValue av = row.putArray("array");
            for (int j = 0; j < sizeOfArray; j++) {
                av.add("a" + j);
            }
            MapValue mv = row.putMap("map");
            for (int j = 0; j < sizeOfMap; j++) {
                mv.put("k"+j, "v" + j);
            }
            row.put("binary", new byte[1024 * (nkb - 1)]);
            assertTrue(tableImpl.put(row, null, null) != null);
        }
    }
}
