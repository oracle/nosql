/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableLimits.NO_CHANGE;
import static org.junit.Assert.assertTrue;

import oracle.kv.Version;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.table.FieldRange;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.WriteOptions;

import org.junit.BeforeClass;
import org.junit.Test;

public class TableMultiDelTest extends TableTestBase {

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        staticSetUp(true/*excludeTombstones*/);
    }

    /**
     * Test table MultiDelete operation with write size limit.
     */
    @Test
    public void testMultiDeleteWithSizeLimit()
        throws Exception {

        final int numSids = 5;
        final int numIdsPerSid = 100;
        final int numKBPerRow = 2;

        TableImpl table = buildTestTable();
        setTableLimits(table, new TableLimits(40000, 10000, NO_CHANGE));
        loadRows(table, numSids, numIdsPerSid, numKBPerRow);

        /* Deletes rows with shard key {"sid": 0}, maxWriteKB = 0 */
        int maxWriteKB = 0;
        PrimaryKey key = createShardKey(table, 0);
        runMultiDelete(key, maxWriteKB, null, numIdsPerSid);

        /* Deletes rows with shard key {"sid": 1}, maxWriteKB = 10 */
        maxWriteKB = 10;
        key = createShardKey(table, 1);
        runMultiDelete(key, maxWriteKB, null, numIdsPerSid);

        /* Deletes rows with shard key {"sid": 2}, maxWriteKB = 51 */
        maxWriteKB = 51;
        key = createShardKey(table, 2);
        runMultiDelete(key, maxWriteKB, null, numIdsPerSid);

        /*
         * Deletes rows with shard key {"sid": 3} and "id" < 10,
         * maxWriteKB = 8
         */
        maxWriteKB = 8;
        FieldRange range = table.createFieldRange("id").setEnd(10, false);
        key = createShardKey(table, 3);
        runMultiDelete(key, maxWriteKB, range, 10);

        /*
         * Deletes rows with shard key {"sid": 3} and 10 <= "id" <= 19,
         * maxWriteKB = 18
         */
        maxWriteKB = 18;
        range = table.createFieldRange("id").setStart(10, true).setEnd(19, true);
        runMultiDelete(key, maxWriteKB, range, 10);

        /*
         * Deletes rows with shard key {"sid": 3} and 20 <= "id" < 31,
         * maxWriteKB = 20
         */
        maxWriteKB = 20;
        range = table.createFieldRange("id").setStart(20, true).setEnd(31, false);
        runMultiDelete(key, maxWriteKB, range, 11);

        /*
         * Deletes rows with shard key {"sid": 3} and "id" >= 31,
         * maxWriteKB = 25
         */
        maxWriteKB = 25;
        range = table.createFieldRange("id").setStart(31, true);
        runMultiDelete(key, maxWriteKB, range, 69);
        runMultiDelete(key, maxWriteKB, range, 0);

        /*
         * Deletes rows with shard key {"sid": 4} and 10 <= "id" <= 19,
         * maxWriteKB = 0
         */
        maxWriteKB = 0;
        key = createShardKey(table, 4);
        range = table.createFieldRange("id").setStart(10, true).setEnd(19, true);
        runMultiDelete(key, maxWriteKB, range, 10);
    }

    /**
     * Runs multiDelete operation and verifies its result.
     */
    private void runMultiDelete(PrimaryKey key,
                                int maxWriteKB,
                                FieldRange range,
                                int expNumDeleted) {

        MultiRowOptions mro =
            (range != null) ? range.createMultiRowOptions() : null;
        WriteOptions wo = new WriteOptions().setMaxWriteKB(maxWriteKB);

        int nBatches = 0;
        int nDeleted = 0;
        int totalWriteKB = 0;
        byte[] continuationKey = null;

        while(true) {
            Result ret = tableImpl.multiDeleteInternal((PrimaryKeyImpl)key,
                continuationKey, mro, wo);
            if (!ret.getSuccess()) {
                assertTrue(ret.getWriteKB() == 0);
                assertTrue(ret.getPrimaryResumeKey() == null);
                break;
            }

            assertTrue(ret.getWriteKB() > 0 && ret.getReadKB() > 0);
            if (ret.getPrimaryResumeKey() != null) {
                assertTrue(maxWriteKB > 0 && ret.getWriteKB() >= maxWriteKB);
            }

            nDeleted += ret.getNDeletions();
            totalWriteKB += ret.getWriteKB();
            nBatches++;
            if (ret.getPrimaryResumeKey() == null) {
                break;
            }
        }

        if (maxWriteKB == 0) {
            assertTrue(nBatches == 1);
        } else {
            assertTrue(nBatches <= ((totalWriteKB + (maxWriteKB -1)) / maxWriteKB));
        }
        assertTrue(nDeleted == expNumDeleted);
    }

    private TableImpl buildTestTable()
        throws Exception {

        TableImpl table =
                TableBuilder.createTableBuilder("testTable")
                .addInteger("sid")
                .addInteger("id")
                .addString("string")
                .addBinary("binary")
                .shardKey("sid")
                .primaryKey("sid", "id")
                .buildTable();
        return addTable(table, true);
    }

    private PrimaryKey createShardKey(TableImpl table, int sid) {
        PrimaryKey key = table.createPrimaryKey();
        key.put("sid", sid);
        return key;
    }

    private void loadRows(TableImpl table, int num, int numIdsPerSid, int nkb) {
        for (int i = 0; i < num; i++) {
            for (int j = 0; j < numIdsPerSid; j++) {
                Row row = table.createRow();
                row.put("sid", i);
                row.put("id", j);
                row.put("string", "string_" + i + "_" + j);
                row.put("binary", new byte[1024 * (nkb - 1)]);
                Version ver = tableImpl.put(row, null, null);
                assertTrue(ver != null);
            }
        }
    }
}
