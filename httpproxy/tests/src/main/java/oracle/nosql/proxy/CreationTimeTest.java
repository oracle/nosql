/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.Durability.ReplicaAckPolicy;
import oracle.nosql.driver.Durability.SyncPolicy;
import oracle.nosql.driver.SystemException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.ops.WriteRequest;
import oracle.nosql.driver.ops.WriteResult;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/*
 * The tests are ordered so that the zzz* test goes last so it picks up
 * DDL history reliably.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreationTimeTest extends ProxyTestBase {

    @Test
    public void smokeTest() {

        try {

            MapValue key = new MapValue().put("id", 10);

            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            /* drop a table */
            TableResult tres = tableOperation(handle,
                "drop table if exists testusers",
                null, TableResult.State.DROPPED,
                20000);
            assertNotNull(tres.getTableName());
            assertTrue(tres.getTableState() == TableResult.State.DROPPED);
            assertNull(tres.getTableLimits());

            /* Create a table */
            tres = tableOperation(
                handle,
                "create table if not exists testusers(id integer, " +
                    "name string, primary key(id))",
                new TableLimits(500, 500, 50),
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Create an index */
            tres = tableOperation(
                handle,
                "create index if not exists Name on testusers(name)",
                null,
                TableResult.State.ACTIVE,
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* PUT */
            PutRequest putRequest = new PutRequest()
                .setValue(value)                        // key is 10
                .setTableName("testusers");

            long startTime1 = System.currentTimeMillis();

            PutResult res = handle.put(putRequest);
            assertNotNull("Put failed", res.getVersion());
            long interval1 = System.currentTimeMillis() - startTime1;
            // no return row so creation time is 0
            checkCreationTime(res.getExistingCreationTime(), 0, 0);


            long startTime2 = System.currentTimeMillis();
            /* put another one. set TTL to test that path */
            putRequest.setTTL(TimeToLive.ofHours(2));
            value.put("id", 20);             // key is 20
            handle.put(putRequest);
            long interval2 = System.currentTimeMillis() - startTime2;
            // no return row so creation time is 0
            checkCreationTime(res.getExistingCreationTime(), 0, 0);

            /*
             * Test ReturnRow for simple put of a row that exists. 2 cases:
             * 1. unconditional (will return info)
             * 2. if absent (will return info)
             */
            value.put("id", 20);
            // turn on returning row
            putRequest.setReturnRow(true);

            PutResult pr = handle.put(putRequest);

            assertNotNull(pr.getVersion()); /* success */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingCreationTime()!=0);
            checkCreationTime(pr.getExistingCreationTime(), startTime2, interval2);
            assertTrue(pr.getExistingModificationTime() != 0);


            putRequest.setOption(Option.IfAbsent);
            pr = handle.put(putRequest);
            assertNull(pr.getVersion()); /* failure */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            checkCreationTime(pr.getExistingCreationTime(), startTime2, interval2);
            assertTrue(pr.getExistingModificationTime() != 0);

            /* clean up */
            putRequest.setReturnRow(false);
            putRequest.setOption(null);

            /* GET first row, id: 10 */
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName("testusers");

            GetResult res1 = handle.get(getRequest);
            assertNotNull("Get failed", res1.getJsonValue());
            assertReadKB(res1);

            assertTrue(res1.getCreationTime() > 0);
            assertTrue(res1.getCreationTime() - startTime1 <= interval1);
            checkCreationTime(res1.getCreationTime(), startTime1, interval1);


            /* DELETE same key, id: 10 */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName("testusers")
                .setReturnRow(true);

            DeleteResult del = handle.delete(delRequest);
            assertTrue("Delete failed", del.getSuccess());
            checkCreationTime(del.getExistingCreationTime(), startTime1, interval1);

            /* GET -- no row, it was removed above */
            getRequest.setTableName("testusers");
            res1 = handle.get(getRequest);
            assertNull(res1.getValue());
            // no row hence creationTime is 0
            assertEquals(0, res1.getCreationTime());
        } catch (Exception e) {
            checkErrorMessage(e);
            e.printStackTrace();
            fail("Exception in test");
        }
    }

    @Test
    public void testPutGetDelete() {

        final String tableName = "testusers";
        final int recordKB = 2;

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists testusers(id integer, " +
                "name string, primary key(id))",
            new TableLimits(500, 500, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        final String name = genString((recordKB - 1) * 1024);
        MapValue value = new MapValue().put("id", 10).put("name", name);
        MapValue newValue = new MapValue().put("id", 11).put("name", name);
        MapValue newValue1 = new MapValue().put("id", 12).put("name", name);
        MapValue newValue2 = new MapValue().put("id", 13).put("name", name);

        /* Durability will be ignored unless run with -Donprem=true */
        Durability dur = new Durability(SyncPolicy.WRITE_NO_SYNC,
            SyncPolicy.NO_SYNC,
            ReplicaAckPolicy.NONE);


        /* Put a row */
        long startTime = System.currentTimeMillis();
        PutRequest putReq = new PutRequest()
            .setValue(value)                        // key is 10
            .setDurability(dur)
            .setTableName(tableName)
            .setReturnRow(true);
        PutResult putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true  /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */ );
        long interval = System.currentTimeMillis() - startTime;
        // no return row hence creationTime is 0
        assertEquals(0, putRes.getExistingCreationTime());


        /* Put a row again with SetReturnRow(false).
         * expect no row returned
         */
        putReq.setReturnRow(false);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true  /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            true /* put over write */);
        Version oldVersion = putRes.getVersion();
        // no return row
        assertEquals(0, putRes.getExistingCreationTime());

        /*
         * Put row again with SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            true /* rowPresent */,
            value /* expPrevValue */,
            oldVersion /* expPrevVersion */,
            true, /* modtime should be zero */
            recordKB,
            true /* put overWrite */);
        oldVersion = putRes.getVersion();
        checkCreationTime(putRes.getExistingCreationTime(), startTime, interval);

        /*
         * Put a new row with SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq = new PutRequest()
            .setValue(newValue)
            .setDurability(dur)
            .setTableName(tableName)
            .setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /* PutIfAbsent an existing row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /*
         * PutIfAbsent fails + SetReturnRow(true),
         * return existing value and version
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            true  /* rowPresent */,
            value /* expPrevValue */,
            oldVersion /* expPrevVersion */,
            true, /* modtime should be recent */
            recordKB,
            false /* put overWrite */);
        checkCreationTime(putRes.getExistingCreationTime(), startTime, interval);

        /* PutIfPresent an existing row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);
        oldVersion = putRes.getVersion();

        /*
         * PutIfPresent succeed + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            true /* rowPresent */,
            value /* expPrevValue */,
            oldVersion /* expPrevVersion */,
            true, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        checkCreationTime(putRes.getExistingCreationTime(), startTime, interval);
        Version ifVersion = putRes.getVersion();

        /* PutIfPresent a new row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(newValue1)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // op didn't succeed
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /*
         * PutIfPresent fail + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // op didn't succeed, no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /* PutIfAbsent a new row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(newValue1)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true  /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        assertNull(putRes.getExistingRowMetadata());
        // no returnRow
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /* PutIfAbsent success + SetReturnRow(true) */
        putReq.setValue(newValue2).setReturnRow(true);
        putRes =  handle.put(putReq);
        checkPutResult(putReq, putRes,
            true  /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /*
         * PutIfVersion an existing row with unmatched version, it should fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(oldVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // op didn't succeed
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /*
         * PutIfVersion fails + SetReturnRow(true),
         * expect existing row returned.
         */
        putReq.setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            true  /* rowPresent */,
            value /* expPrevValue */,
            ifVersion /* expPrevVersion */,
            true, /* modtime should be recent */
            recordKB,
            false /* put overWrite */);
        checkCreationTime(putRes.getExistingCreationTime(), startTime, interval);


        /*
         * Put an existing row with matching version, it should succeed.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(ifVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);
        ifVersion = putRes.getVersion();

        /*
         * PutIfVersion succeed + SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq.setMatchVersion(ifVersion).setReturnRow(true);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);
        Version newVersion = putRes.getVersion();


        /*
         * Get
         */
        MapValue key = new MapValue().put("id", 10);

        /* Get a row */
        GetRequest getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            true /* rowPresent*/,
            value,
            null, /* Don't check version if Consistency.EVENTUAL */
            true, /* modtime should be recent */
            recordKB);
        checkCreationTime(getRes.getCreationTime(), startTime, interval);

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            true /* rowPresent*/,
            value,
            newVersion,
            true, /* modtime should be recent */
            recordKB);
        checkCreationTime(getRes.getCreationTime(), startTime, interval);

        getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            true /* rowPresent*/,
            value,
            null, /* Don't check version if Consistency.EVENTUAL */
            true, /* modtime should be recent */
            recordKB);
        checkCreationTime(getRes.getCreationTime(), startTime, interval);


        /* Get non-existing row */
        key = new MapValue().put("id", 100);
        getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            false /* rowPresent*/,
            null  /* expValue */,
            null  /* expVersion */,
            false, /* modtime should be zero */
            recordKB);
        // no row
        assertEquals(0, getRes.getCreationTime());

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            false /* rowPresent*/,
            null  /* expValue */,
            null  /* expVersion */,
            false, /* modtime should be zero */
            recordKB);
        // no row
        assertEquals(0, getRes.getCreationTime());

        /* Delete a row */
        key = new MapValue().put("id", 10);
        DeleteRequest delReq = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            true  /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        // no return row
        checkCreationTime(delRes.getExistingCreationTime(), 0, 0);

        /* Put the row back to store */
        startTime = System.currentTimeMillis();
        putReq = new PutRequest()
            .setValue(value)
            .setReturnRow(true)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        oldVersion = putRes.getVersion();
        assertNotNull(oldVersion);
        interval = System.currentTimeMillis() - startTime;
        // in NsonProtocol.writeReturnRow():1344 result contains creationTime
        // and modificationTime but version is null so they all get skipped.
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /* Delete succeed + setReturnRow(true), existing row returned. */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            true /* shouldSucceed */,
            true /* rowPresent */,
            value /* expPrevValue */,
            oldVersion /* expPrevVersion */,
            true, /* modtime should be zero */
            recordKB);
        checkCreationTime(delRes.getExistingCreationTime(), startTime, interval);

        /* Delete fail + setReturnRow(true), no existing row returned. */
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            false /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        assertEquals(0, delRes.getExistingCreationTime());

        /* Put the row back to store */
        startTime = System.currentTimeMillis();
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();
        interval = System.currentTimeMillis() - startTime;
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);


        /* DeleteIfVersion with unmatched version, it should fail */
        delReq = new DeleteRequest()
            .setMatchVersion(oldVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            false /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        // no return row
        assertEquals(0, delRes.getExistingCreationTime());

        /*
         * DeleteIfVersion with unmatched version + setReturnRow(true),
         * the existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            false /* shouldSucceed */,
            true  /* rowPresent */,
            value /* expPrevValue */,
            ifVersion /* expPrevVersion */,
            true, /* modtime should be recent */
            recordKB);
        checkCreationTime(delRes.getExistingCreationTime(), startTime, interval);

        /* DeleteIfVersion with matched version, it should succeed. */
        delReq = new DeleteRequest()
            .setMatchVersion(ifVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            true  /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        // no return row
        checkCreationTime(delRes.getExistingCreationTime(), 0, 0);

        /* Put the row back to store */
        startTime = System.currentTimeMillis();
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();
        interval = System.currentTimeMillis() - startTime;
        // no return row
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /*
         * DeleteIfVersion with matched version + setReturnRow(true),
         * it should succeed but no existing row returned.
         */
        delReq.setMatchVersion(ifVersion).setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            true  /* shouldSucceed */,
            false  /* returnRow */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        // no return row
        checkCreationTime(delRes.getExistingCreationTime(), 0, 0);

        /* DeleteIfVersion with a key not existing, it should fail. */
        delReq = new DeleteRequest()
            .setMatchVersion(ifVersion)
            .setKey(key)
            .setTableName(tableName);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            false /* shouldSucceed */,
            false /* returnRow */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        assertEquals(0, delRes.getExistingCreationTime());

        /*
         * DeleteIfVersion with a key not existing + setReturnRow(true),
         * it should fail and no existing row returned.
         */
        delReq.setReturnRow(true);
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            false /* shouldSucceed */,
            false /* returnRow */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        assertEquals(0, delRes.getExistingCreationTime());
    }

    private void checkCreationTime(long creationTime, long startTime, long interval) {
        assertTrue("creationTime should be >= than " + startTime + "  " +
            (creationTime - startTime),
            creationTime >= startTime);

        assertTrue("creationTime not in interval: " + interval,
            creationTime - startTime <= interval);

//        if (creationTime >= startTime &&
//            creationTime - startTime <= interval) {
//            System.out.println("  PASSED    ct: " + creationTime + "  i:" + interval);
//        } else {
//            System.out.println("  !!! FAILED    ct: " + creationTime + "  i:" + interval + "  !!!");
//        }
    }

    private void checkModTime(long modTime, boolean modTimeRecent) {
        if (modTimeRecent) {
            if (modTime < (System.currentTimeMillis() - 2000)) {
                fail("Expected modtime to be recent, got " + modTime);
            }
        } else {
            if (modTime != 0) {
                fail("Expected modtime to be zero, got " + modTime);
            }
        }
    }

    private void checkPutResult(PutRequest request,
        PutResult result,
        boolean shouldSucceed,
        boolean rowPresent,
        MapValue expPrevValue,
        Version expPrevVersion,
        boolean modTimeRecent,
        int recordKB,
        boolean putOverWrite) {
        if (shouldSucceed) {
            assertNotNull("Put should succeed", result.getVersion());
        } else {
            assertNull("Put should fail", result.getVersion());
        }
        checkExistingValueVersion(request, result, shouldSucceed, rowPresent,
            expPrevValue, expPrevVersion);

        checkModTime(result.getExistingModificationTime(), modTimeRecent);

        int[] expCosts = getPutReadWriteCost(request,
            shouldSucceed,
            rowPresent,
            recordKB,
            putOverWrite);

        if (onprem == false) {
            assertReadKB(result, expCosts[0], true /* isAbsolute */);
            assertWriteKB(result, expCosts[1]);
        }
    }

    private void checkDeleteResult(DeleteRequest request,
        DeleteResult result,
        boolean shouldSucceed,
        boolean rowPresent,
        MapValue expPrevValue,
        Version expPrevVersion,
        boolean modTimeRecent,
        int recordKB) {

        assertEquals("Delete should " + (shouldSucceed ? "succeed" : " fail"),
            shouldSucceed, result.getSuccess());
        checkExistingValueVersion(request, result, shouldSucceed, rowPresent,
            expPrevValue, expPrevVersion);

        checkModTime(result.getExistingModificationTime(), modTimeRecent);

        int[] expCosts = getDeleteReadWriteCost(request,
            shouldSucceed,
            rowPresent,
            recordKB);

        if (onprem == false) {
            assertReadKB(result, expCosts[0], true /* isAbsolute */);
            assertWriteKB(result, expCosts[1]);
        }
    }

    private void checkGetResult(GetRequest request,
        GetResult result,
        boolean rowPresent,
        MapValue expValue,
        Version expVersion,
        boolean modTimeRecent,
        int recordKB) {


        if (rowPresent) {
            if (expValue != null) {
                assertEquals("Unexpected value", expValue, result.getValue());
            } else {
                assertNotNull("Unexpected value", expValue);
            }
            if (expVersion != null) {
                assertArrayEquals("Unexpected version",
                    expVersion.getBytes(),
                    result.getVersion().getBytes());
            } else {
                assertNotNull("Unexpected version", result.getVersion());
            }
        } else {
            assertNull("Unexpected value", expValue);
            assertNull("Unexpected version", result.getVersion());
        }

        checkModTime(result.getModificationTime(), modTimeRecent);

        final int minRead = getMinRead();
        int expReadKB = rowPresent ? recordKB : minRead;

        if (onprem == false) {
            assertReadKB(result, expReadKB,
                (request.getConsistencyInternal() == Consistency.ABSOLUTE));
            assertWriteKB(result, 0);
        }
    }

    private void checkExistingValueVersion(WriteRequest request,
        WriteResult result,
        boolean shouldSucceed,
        boolean rowPresent,
        MapValue expPrevValue,
        Version expPrevVersion) {

        boolean hasReturnRow = rowPresent;
        if (hasReturnRow) {
            assertNotNull("PrevValue should be non-null",
                result.getExistingValueInternal());
            if (expPrevValue != null) {
                assertEquals("Unexpected PrevValue",
                    expPrevValue, result.getExistingValueInternal());
            }
            assertNotNull("PrevVersion should be non-null",
                result.getExistingVersionInternal());
            if (expPrevVersion != null) {
                assertNotNull(result.getExistingVersionInternal());
                assertArrayEquals("Unexpected PrevVersion",
                    expPrevVersion.getBytes(),
                    result.getExistingVersionInternal().getBytes());
            }
        } else {
            assertNull("PrevValue should be null",
                result.getExistingValueInternal());
            assertNull("PrevVersion should be null",
                result.getExistingVersionInternal());
        }
    }

    @Test
    public void testReadQuery() throws InterruptedException {
        final String createTable1 =
            "create table tjson(id integer, info json, primary key(id))";
        final String createTable2 =
            "create table trecord(id integer, " +
                "info record(name string, age integer), " +
                "primary key(id))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);
        tableOperation(handle, createTable2, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);

        MapValue rowNull = new MapValue()
            .put("id", 0)
            .put("info",
                new MapValue()
                    .put("name", NullValue.getInstance())
                    .put("age", 20));
        MapValue rowJsonNull = new MapValue()
            .put("id", 0)
            .put("info",
                new MapValue()
                    .put("name", JsonNullValue.getInstance())
                    .put("age", 20));

        MapValue[] rows = new MapValue[] {rowNull, rowJsonNull};
        Map<String, MapValue> tableExpRows = new HashMap<String, MapValue>();
        tableExpRows.put("tjson", rowJsonNull);
        tableExpRows.put("trecord", rowNull);

        long startTime = System.currentTimeMillis();
        /*
         * Put rows with NullValue or JsonNullValue, they should be converted
         * to the right value for the target type.
         */
        for (Map.Entry<String, MapValue> e : tableExpRows.entrySet()) {
            String table = e.getKey();
            MapValue expRow = e.getValue();

            for (MapValue row : rows) {
                PutRequest putReq = new PutRequest()
                    .setTableName(table)
                    .setValue(row);

                PutResult putRet = handle.put(putReq);
                Version pVersion = putRet.getVersion();
                assertNotNull(pVersion);
                long interval = System.currentTimeMillis() - startTime;

                MapValue key = new MapValue().put("id", row.get("id"));
                GetRequest getReq = new GetRequest()
                    .setTableName(table)
                    .setConsistency(Consistency.ABSOLUTE)
                    .setKey(key);
                GetResult getRet = handle.get(getReq);
                assertEquals(expRow, getRet.getValue());
                assertNotNull(getRet.getVersion());
                assertTrue(Arrays.equals(pVersion.getBytes(),
                    getRet.getVersion().getBytes()));
                checkCreationTime(getRet.getCreationTime(), startTime, interval);
                assertTrue(getRet.getModificationTime() > 0);
            }
        }
        long interval = System.currentTimeMillis() - startTime;

        /*
         * Query with variable for json field and set NullValue or
         * JsonNullValue to variable, the NullValue is expected to be converted
         * to JsonNullValue.
         */
        String query =
            "select id, info, creation_time($t) as ct, " +
            "creation_time_millis($t) as ctm, " +
            "modification_time($t) as mt " +
            "from tjson $t ";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        boolean shouldRetry = false;
        do {
            try {
                QueryResult queryRet = handle.query(queryReq);
                assertEquals(1, queryRet.getResults().size());

                for (MapValue v : queryRet.getResults()) {
                    assertTrue(v.get("ct").isTimestamp());
                    checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
                    assertTrue(v.get("ctm").isLong());
                    checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
                    assertTrue(v.get("mt").isTimestamp());
                    assertTrue(v.get("mt").asTimestamp().getValue().getTime() > 0);
                }

            } catch (SystemException e) {
                shouldRetry = e.okToRetry();
                System.out.println("Caught " + (e.okToRetry() ? "retryable" :
                    "") + " ex: " + e.getMessage());
                System.out.println(
                    "Retrying query: " + queryReq.getStatement());
                e.printStackTrace();
                Thread.sleep(300);
            }
        } while (shouldRetry);
    }

    @Test
    public void testTableMultiWrite() {
        final String createTable =
            "create table tMW(s integer, id integer, info json, primary key(shard(s), id))";

        tableOperation(handle, createTable, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);

        /* multi write */
        WriteMultipleRequest wmReq = new WriteMultipleRequest();
        String tableName = "tMW";

        for (int i = 0; i < 10; i++) {
            PutRequest pr = new PutRequest()
                .setTableName(tableName)
                .setRowMetadata("{\"n\":" + i + "}")
                .setValue(new MapValue()
                    .put("s", 1)
                    .put("id", i)
                    .put("info", new MapValue().put("name", "John")));
            wmReq.add(pr, true);
        }

        long startTime = System.currentTimeMillis();
        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());
        long interval = System.currentTimeMillis() - startTime;


        QueryRequest queryReq = new QueryRequest()
            .setStatement("select s, id, $t.info.name as name, " +
                "creation_time($t) as ct, creation_time_millis($t) as ctm " +
                "from " + tableName + " $t ORDER BY id ASC");
        QueryResult qRes = handle.query(queryReq);

        int i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(1, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertEquals("John", v.get("name").asString().getString());
            assertTrue(v.get("ct").isTimestamp());
            checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
            assertTrue(v.get("ctm").isLong());
            checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            i++;
        }
        assertEquals(10, qRes.getResults().size());
        assertEquals(10, i);

        wmReq.getOperations().forEach((req) -> {
            PutRequest put = (PutRequest)req.getRequest();
            put.setReturnRow(true);
        });

        wmRes = handle.writeMultiple(wmReq);
        wmRes.getResults().forEach((res) -> {
            checkCreationTime(res.getExistingCreationTime(), startTime, interval);
        });
    }

    @Test
    public void testNullJsonNull() throws InterruptedException {
        final String createTable1 =
            "create table tjson(id integer, info json, primary key(id))";
        final String createTable2 =
            "create table trecord(id integer, " +
                "info record(name string, age integer), " +
                "primary key(id))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);
        tableOperation(handle, createTable2, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);

        MapValue rowNull = new MapValue()
            .put("id", 0)
            .put("info",
                new MapValue()
                    .put("name", NullValue.getInstance())
                    .put("age", 20));
        MapValue rowJsonNull = new MapValue()
            .put("id", 0)
            .put("info",
                new MapValue()
                    .put("name", JsonNullValue.getInstance())
                    .put("age", 20));

        MapValue[] rows = new MapValue[] {rowNull, rowJsonNull};
        Map<String, MapValue> tableExpRows = new HashMap<String, MapValue>();
        tableExpRows.put("tjson", rowJsonNull);
        tableExpRows.put("trecord", rowNull);

        long startTime = System.currentTimeMillis();
        /*
         * Put rows with NullValue or JsonNullValue, they should be converted
         * to the right value for the target type.
         */
        for (Map.Entry<String, MapValue> e : tableExpRows.entrySet()) {
            String table = e.getKey();
            MapValue expRow = e.getValue();

            for (MapValue row : rows) {
                PutRequest putReq = new PutRequest()
                    .setTableName(table)
                    .setValue(row);

                PutResult putRet = handle.put(putReq);
                Version pVersion = putRet.getVersion();
                assertNotNull(pVersion);
                long interval = System.currentTimeMillis() - startTime;

                MapValue key = new MapValue().put("id", row.get("id"));
                GetRequest getReq = new GetRequest()
                    .setTableName(table)
                    .setConsistency(Consistency.ABSOLUTE)
                    .setKey(key);
                GetResult getRet = handle.get(getReq);
                assertEquals(expRow, getRet.getValue());
                assertNotNull(getRet.getVersion());
                assertTrue(Arrays.equals(pVersion.getBytes(),
                    getRet.getVersion().getBytes()));
                checkCreationTime(getRet.getCreationTime(), startTime, interval);
            }
        }
        long interval = System.currentTimeMillis() - startTime;

        String query =
            "select id, info, creation_time($t) as ct, " +
            "creation_time_millis($t) as ctm " +
            "from tjson $t";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        boolean shouldRetry = false;
        do {
            try {
                QueryResult queryRet = handle.query(queryReq);
                assertEquals(1, queryRet.getResults().size());

                MapValue v = queryRet.getResults().get(0);
                assertTrue(v.get("ct").isTimestamp());
                checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
                assertTrue(v.get("ctm").isLong());
                checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            } catch (SystemException e) {
                shouldRetry = e.okToRetry();
                System.out.println("Caught " + (e.okToRetry() ? "retryable" : "") + " ex: " + e.getMessage());
                System.out.println("Retrying query: " + queryReq.getStatement());
                e.printStackTrace();
                Thread.sleep(300);
            }
        } while (shouldRetry);


        query =
            "select id, info, creation_time($t) as ct, " +
            "creation_time_millis($t) as ctm " +
            "from trecord $t";
        prepReq = new PrepareRequest().setStatement(query);
        prepRet = handle.prepare(prepReq);
        prepStmt = prepRet.getPreparedStatement();

        queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        shouldRetry = false;
        do {
            try {
                QueryResult queryRet = handle.query(queryReq);
                assertEquals(1, queryRet.getResults().size());

                MapValue v = queryRet.getResults().get(0);
                assertTrue(v.get("ct").isTimestamp());
                checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
                assertTrue(v.get("ctm").isLong());
                checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            } catch (SystemException e) {
                shouldRetry = e.okToRetry();
                System.out.println("Caught " + (e.okToRetry() ? "retryable" : "") + " ex: " + e.getMessage());
                System.out.println("Retrying query: " + queryReq.getStatement());
                e.printStackTrace();
                Thread.sleep(300);
            }
        } while (shouldRetry);
    }

    private String genString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }

    @Test
    public void testCollection() {
        final String tableName = "testusersColl";

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists " + tableName +
                "(id integer, primary key(id)) as json collection",
            new TableLimits(500, 500, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        MapValue value = new MapValue().put("id", 10).put("name", "John");

        /* Put row */
        long startTime = System.currentTimeMillis();
        PutRequest putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putRes = handle.put(putReq);
        long interval = System.currentTimeMillis() - startTime;
        // no return value
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /* Get the row back */
        GetRequest getReq = new GetRequest()
            .setTableName(tableName)
            .setKey(value);
        GetResult getRet = handle.get(getReq);
        checkCreationTime(getRet.getCreationTime(), startTime, interval);

        /* Delete row check prev/existing is still null */
        DeleteRequest delReq = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        DeleteResult delRes = handle.delete(delReq);
        checkCreationTime(delRes.getExistingCreationTime(), startTime, interval);


        /* Put again */
        startTime = System.currentTimeMillis();
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        putRes = handle.put(putReq);
        interval = System.currentTimeMillis() - startTime;
        // no return
        checkCreationTime(putRes.getExistingCreationTime(), 0, 0);

        /* Query */
        QueryRequest queryReq = new QueryRequest()
            .setStatement("select id, name, creation_time($t) as ct," +
                "creation_time_millis($t) as ctm from " +
                tableName + " $t");
        QueryResult qRes = handle.query(queryReq);

        assertEquals(1, qRes.getResults().size());
        assertEquals(10, qRes.getResults().get(0).get("id").getInt());
        assertEquals("John", qRes.getResults().get(0).get("name").getString());
        assertTrue(qRes.getResults().get(0).get("ct").isTimestamp());
        checkCreationTime(qRes.getResults().get(0).get("ct").asTimestamp().getValue().getTime(), startTime, interval);
        assertTrue(qRes.getResults().get(0).get("ctm").isLong());
        checkCreationTime(qRes.getResults().get(0).get("ctm").asLong().getValue(), startTime, interval);
    }

    @Test
    public void testCollectionMultiWrite() {
        final String tableName = "testusersCollMWrite";

        /* Create a table */
        TableResult tres = tableOperation(
            handle,
            "create table if not exists " + tableName +
                "(s integer, id integer, primary key(shard(s), id)) as json collection",
            new TableLimits(500, 500, 50),
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* multi write */
        WriteMultipleRequest wmReq = new WriteMultipleRequest();

        for (int i = 0; i < 10; i++) {
            PutRequest pr = new PutRequest()
                .setTableName(tableName)
                .setValue(new MapValue()
                    .put("s", 1)
                    .put("id", i)
                    .put("name", "John"));
            wmReq.add(pr, true);
        }
        long startTime = System.currentTimeMillis();
        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());
        long interval = System.currentTimeMillis() - startTime;

        /* query read metadata */
        QueryRequest queryReq = new QueryRequest()
            .setStatement("select s, id, name, creation_time($t) as ct," +
                "creation_time_millis($t) as ctm from " +
                tableName + " $t ORDER BY id ASC");
        QueryResult qRes = handle.query(queryReq);

        int i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(1, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertEquals("John", v.get("name").asString().getString());
            assertTrue(v.get("ct").isTimestamp());
            checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
            assertTrue(v.get("ctm").isLong());
            checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            i++;
        }
        assertEquals(10, qRes.getResults().size());
        assertEquals(10, i);
    }

    @Test
    public void testWriteQuery() throws InterruptedException {
        final String tableName = "t";
        final String createTable1 =
            "create table "+ tableName +" (s integer, id integer, info json, primary key(shard(s), id))";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);

        // do a few inserts
        long startTime = System.currentTimeMillis();
        String query = "declare $id integer; insert into " + tableName + " values( 0, $id, {})";
        for (int i = 0; i < 10; i++) {
            PrepareRequest prepReq = new PrepareRequest().setStatement(query);
            PrepareResult prepRet = handle.prepare(prepReq);

            PreparedStatement prepStmt = prepRet.getPreparedStatement();
            prepStmt.setVariable("$id", new IntegerValue(i));

            QueryRequest queryReq = new QueryRequest()
                .setPreparedStatement(prepStmt);

            QueryResult queryRes = handle.query(queryReq);
            assertNotNull(queryRes);
            assertEquals(1, queryRes.getResults().get(0).asMap().get("NumRowsInserted").asInteger().getInt());
        }
        long interval = System.currentTimeMillis() - startTime;

        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, creation_time($t) as ct, " +
                "creation_time_millis($t) as ctm from " +
                tableName + " $t order by $t.id";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        QueryResult qRes = handle.query(queryReq);
        int i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(0, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertTrue(v.get("ct").isTimestamp());
            checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
            assertTrue(v.get("ctm").isLong());
            checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            i++;
        }
        assertEquals(10, i);


        // update many
        query = "update " + tableName + " t SET t.info = t.info where t.s = 0";

        queryReq = new QueryRequest()
            .setStatement(query);

        qRes = handle.query(queryReq);
        assertEquals(1, qRes.getResults().size());
        assertEquals(10, qRes.getResults().get(0).asMap().get("NumRowsUpdated").asInteger().getInt());


        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, creation_time($t) as ct, " +
                "creation_time_millis($t) as ctm from " +
                tableName + " $t order by $t.id";
        prepReq = new PrepareRequest().setStatement(query);
        prepRet = handle.prepare(prepReq);
        prepStmt = prepRet.getPreparedStatement();

        queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        qRes = handle.query(queryReq);
        i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(0, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertTrue(v.get("ct").isTimestamp());
            checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
            assertTrue(v.get("ctm").isLong());
            checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            i++;
        }
        assertEquals(10, i);
    }

    @Test
    public void testWriteQueryCollection() throws InterruptedException {
        final String tableName = "t";
        final String createTable1 =
            "create table "+ tableName +" (s integer, id integer, primary key(shard(s), id)) as json collection";

        tableOperation(handle, createTable1, new TableLimits(10, 10, 1),
            null, TableResult.State.ACTIVE, null);

        // do a few inserts
        long startTime = System.currentTimeMillis();
        String query = "declare $id integer; insert into " + tableName + " values( 0, $id, {\"info\":1})";
        for (int i = 0; i < 10; i++) {
            PrepareRequest prepReq = new PrepareRequest().setStatement(query);
            PrepareResult prepRet = handle.prepare(prepReq);

            PreparedStatement prepStmt = prepRet.getPreparedStatement();
            prepStmt.setVariable("$id", new IntegerValue(i));

            QueryRequest queryReq = new QueryRequest()
                .setPreparedStatement(prepStmt);

            QueryResult queryRes = handle.query(queryReq);
            assertNotNull(queryRes);
            assertEquals(1, queryRes.getResults().get(0).asMap().get("NumRowsInserted").asInteger().getInt());
        }
        long interval = System.currentTimeMillis() - startTime;

        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, creation_time($t) as ct, " +
                "creation_time_millis($t) as ctm," +
                "modification_time($t) as mt " +
                "from " + tableName + " $t order by $t.id";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        QueryResult qRes = handle.query(queryReq);
        int i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(0, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertTrue(v.get("ct").isTimestamp());
            checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
            assertTrue(v.get("ctm").isLong());
            checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            assertTrue(v.get("mt").isTimestamp());
            assertTrue(v.get("mt").asTimestamp().getValue().getTime() > 0);
            i++;
        }
        assertEquals(10, i);


        // update many
        query = "update " + tableName + " t SET t.info=3 where t.s = 0";

        queryReq = new QueryRequest()
            .setStatement(query);

        qRes = handle.query(queryReq);
        assertEquals(1, qRes.getResults().size());
        assertEquals(10, qRes.getResults().get(0).asMap().get("NumRowsUpdated").asInteger().getInt());


        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info,creation_time($t) as ct, " +
            "creation_time_millis($t) as ctm, " +
            "modification_time($t) as mt " +
            "from " + tableName + " $t order by $t.id";
        prepReq = new PrepareRequest().setStatement(query);
        prepRet = handle.prepare(prepReq);
        prepStmt = prepRet.getPreparedStatement();

        queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        qRes = handle.query(queryReq);
        i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(0, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertTrue(v.get("ct").isTimestamp());
            checkCreationTime(v.get("ct").asTimestamp().getValue().getTime(), startTime, interval);
            assertTrue(v.get("ctm").isLong());
            checkCreationTime(v.get("ctm").asLong().getValue(), startTime, interval);
            assertTrue(v.get("mt").isTimestamp());
            assertTrue(v.get("mt").asTimestamp().getValue().getTime() > 0);
            i++;
        }
        assertEquals(10, i);
    }
}
