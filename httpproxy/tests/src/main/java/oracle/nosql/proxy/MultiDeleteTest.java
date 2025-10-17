/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.nosql.driver.FieldRange;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

/**
 * Test on MultiDelete operation.
 */
public class MultiDeleteTest extends ProxyTestBase {
    final static int WRITE_KB_LIMIT = rlimits.getRequestWriteKBLimit();

    final static String tableName = "multiDeleteTable";

    /* Create a table */
    final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS multiDeleteTable(" +
            "sid INTEGER, id INTEGER, name STRING, longString STRING, " +
            "PRIMARY KEY(SHARD(sid), id))";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tableOperation(handle, createTableDDL,
                       new TableLimits(10000, 10000, 50),
                       TableResult.State.ACTIVE, 10000);
    }

    @Override
    public void tearDown() throws Exception {
        deleteTable(tableName);
        super.tearDown();
    }

    /**
     * Test on success cases
     */
    @Test
    public void testMultiDelete() {
        final int numMajor = 5;
        final int numPerMajor = 100;
        final int recordKB = 2;

        loadRows(numMajor, numPerMajor, recordKB);

        /* Deletes rows with the shard key {"sid":0}, maxWriteKB = 0 */
        int maxWriteKB = 0;
        MapValue pKey = new MapValue().put("sid", 0);
        runMultiDelete(pKey, null, maxWriteKB, numPerMajor, recordKB);

        /* Deletes rows with shard key {"sid":1}, maxWriteKB = 10 */
        maxWriteKB = 10;
        pKey.put("sid", 1);
        runMultiDelete(pKey, null, maxWriteKB, numPerMajor, recordKB);

        /* Deletes rows with shard key {"sid":3}, maxWriteKB = 51 */
        maxWriteKB = 51;
        pKey.put("sid", 2);
        runMultiDelete(pKey, null, maxWriteKB, numPerMajor, recordKB);

        /*
         * Deletes rows with shard key {"sid":3} and "id" < 10,
         * maxWriteKB = 8.
         */
        FieldRange range;
        maxWriteKB = 8;
        range = new FieldRange("id").setEnd(new IntegerValue(10), false);
        pKey.put("sid", 3);
        runMultiDelete(pKey, range, maxWriteKB, 10, recordKB);

        /*
         * Deletes rows with shard key {"sid":3} and 10 <= "id" <= 19,
         * maxWriteKB = 18
         */
        maxWriteKB = 18;
        range = new FieldRange("id")
            .setStart(new IntegerValue(10), true)
            .setEnd(new IntegerValue(19), true);
        runMultiDelete(pKey, range, maxWriteKB, 10, recordKB);

        /*
         * Deletes rows with shard key {"sid":3} and 20 <= "id" < 31,
         * maxWriteKB = 20
         */
        maxWriteKB = 20;
        range = new FieldRange("id")
            .setStart(new IntegerValue(20), true)
            .setEnd(new IntegerValue(31), false);
        runMultiDelete(pKey, range, maxWriteKB, 11, recordKB);

        /*
         * Deletes rows with shard key {"sid":3} and "id" >= 31,
         * maxWriteKB = 25
         */
        maxWriteKB = 25;
        range = new FieldRange("id").setStart(new IntegerValue(31), true);
        runMultiDelete(pKey, range, maxWriteKB, numPerMajor - 31, recordKB);
        runMultiDelete(pKey, range, maxWriteKB, 0, recordKB);

        /*
         * Deletes rows with shard key {"sid":4} and 10 <= "id" <= 19,
         * maxWriteKB = 0
         */
        maxWriteKB = 0;
        pKey.put("sid", 4);
        range = new FieldRange("id").setStart(new IntegerValue(10), true)
            .setEnd(new IntegerValue(19), true);
        runMultiDelete(pKey, range, maxWriteKB, 10, recordKB);
    }

    /* Test MultiDelete failed due to invalid argument */
    @Test
    public void testInvalidArgument() {

        MultiDeleteRequest req = new MultiDeleteRequest();

        /* Missing tableName */
        execMultiDeleteExpIAE(req);

        /* Missing a key */
        req.setTableName(tableName);
        execMultiDeleteExpIAE(req);

        /* Invalid primary key */
        req.setKey(new MapValue().put("name", 0));
        execMultiDeleteExpIAE(req);

        /* Missing shard field from the primary key */
        req.setKey(new MapValue().put("id", 0));
        execMultiDeleteExpIAE(req);

        /* Invalid FieldRange */
        req.setKey(new MapValue().put("sid", 0));
        FieldRange range = new FieldRange("name")
            .setStart(new IntegerValue(1), false);
        req.setRange(range);
        execMultiDeleteExpIAE(req);

        /* Invalid FieldRange */
        range = new FieldRange("id")
            .setStart(new IntegerValue(1), false)
            .setEnd(new IntegerValue(0), true);
        req.setRange(range);
        execMultiDeleteExpIAE(req);

        /* maxWriteKB should be >= 0 */
        try {
            req.setMaxWriteKB(-1);
            fail("Expect to catch IAE but not");
        } catch (IllegalArgumentException ignored) {
        }

        /* maxWriteKB can not exceed WRITE_KB_LIMIT */
        req.setMaxWriteKB(WRITE_KB_LIMIT + 1);
        execMultiDeleteExpIAE(req);

        /* Table not found */
        req.setTableName("InvalidTable");
        try {
            execMultiDeleteExpIAE(req);
        } catch (TableNotFoundException ignored) {
        }
    }

    /**
     * Runs MultiDelete request and verify its result.
     */
    private void runMultiDelete(MapValue pKey,
                                FieldRange range,
                                int maxWriteKB,
                                int expNumDeleted,
                                int recordKB) {

        int nDeleted = 0;
        int totalReadKB = 0;
        int totalWriteKB = 0;
        int totalReadUnits = 0;
        int totalWriteUnits = 0;

        byte[] continuationKey = null;

        final int minRead = getMinRead();
        int expWriteKB = 0;
        int expReadKB = 0;
        int writeKBLimit = (maxWriteKB != 0) ? maxWriteKB : WRITE_KB_LIMIT;

        while(true) {
            MultiDeleteResult ret = execMultiDelete(pKey, continuationKey,
                                                    range, maxWriteKB);
            nDeleted += ret.getNumDeletions();
            totalReadKB += ret.getReadKB();
            totalWriteKB += ret.getWriteKB();
            totalReadUnits += ret.getReadUnits();
            totalWriteUnits += ret.getWriteUnits();

            if (!onprem) {
                if (ret.getNumDeletions() > 0) {
                    assertTrue(ret.getWriteKB() > 0 && ret.getReadKB() > 0);
                } else {
                    expReadKB += minRead;
                }
            }

            if (ret.getContinuationKey() == null) {
                break;
            }

            if (!onprem) {
                assertTrue(ret.getWriteUnits() >= writeKBLimit &&
                           ret.getWriteUnits() < writeKBLimit + recordKB);
            }
        }

        assertTrue(nDeleted == expNumDeleted);

        if (onprem) {
            return;
        }

        expWriteKB += nDeleted * recordKB;
        expReadKB += nDeleted * minRead;

        assertReadKB(expReadKB, totalReadKB, totalReadUnits,
                     true /* isAbsolute */);

        assertWriteKB(expWriteKB, totalWriteKB, totalWriteUnits);
    }

    /**
     * Executes the MultiDelete request.
     */
    private MultiDeleteResult execMultiDelete(MapValue key,
                                              byte[] continuationKey,
                                              FieldRange range,
                                              int maxWriteKB) {

        MultiDeleteRequest mdReq = new MultiDeleteRequest()
            .setTableName(tableName)
            .setKey(key)
            .setContinuationKey(continuationKey)
            .setRange(range)
            .setMaxWriteKB(maxWriteKB);

        return handle.multiDelete(mdReq);
    }

    /**
     * Executes the MultiDelete request, it is expected to catch IAE.
     */
    private void execMultiDeleteExpIAE(MultiDeleteRequest mdReq) {
        try {
            handle.multiDelete(mdReq);
            fail("Expect to catch IAE but not");
        } catch (IllegalArgumentException ignored) {
        }
    }

    private void loadRows(int numMajor, int numPerMajor, int nKB) {

        MapValue value = new MapValue();
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);

        /* Load rows */
        final String longString = genString((nKB - 1) * 1024);
        for (int i = 0; i < numMajor; i++) {
            value.put("sid", i);
            for (int j = 0; j < numPerMajor; j++) {
                value.put("id", j);
                value.put("name", "name_" + i + "_" + j);
                value.put("longString", longString);
                PutResult res = handle.put(putRequest);
                assertNotNull("Put failed", res.getVersion());
            }
        }
    }

    private String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }
}
