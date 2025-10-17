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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.Durability.SyncPolicy;
import oracle.nosql.driver.Durability.ReplicaAckPolicy;
import oracle.nosql.driver.FieldRange;
import oracle.nosql.driver.SystemException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
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
import oracle.nosql.driver.values.JsonUtils;
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
public class RowMetadataTest extends ProxyTestBase {

    private final static String RM1 = "{\"n\":1}";
    private final static String RM2 = "{\"n\":2}";
    private final static String RM3 = "{\"n\":3}";
    private final static String RM4 = "{\"n\":4}";
    private final static String RM5 = "{\"n\":5}";

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
                .setValue(value)
                .setTableName("testusers")
                .setRowMetadata(RM1);

            assertEquals(RM1, putRequest.getRowMetadata());

            PutResult res = handle.put(putRequest);
            assertNotNull("Put failed", res.getVersion());
            assertWriteKB(res);

            /* put another one. set TTL to test that path */
            putRequest.setTTL(TimeToLive.ofHours(2));
            putRequest.setRowMetadata(RM2);
            value.put("id", 20);
            handle.put(putRequest);

            /*
             * Test ReturnRow for simple put of a row that exists. 2 cases:
             * 1. unconditional (will return info)
             * 2. if absent (will return info)
             */
            value.put("id", 20);
            putRequest.setReturnRow(true);
            putRequest.setRowMetadata(RM3);

            PutResult pr = handle.put(putRequest);
            assertNotNull(pr.getVersion()); /* success */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertEquals(RM2, pr.getExistingRowMetadata());
            assertReadKB(pr);
            assertWriteKB(pr);

            putRequest.setOption(Option.IfAbsent);
            putRequest.setRowMetadata(RM4);
            pr = handle.put(putRequest);
            assertNull(pr.getVersion()); /* failure */
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertEquals(RM3, pr.getExistingRowMetadata());
            assertReadKB(pr);

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

            assertNotNull(res1.getRowMetadata());
            assertEquals(RM1, res1.getRowMetadata());

            /* DELETE same key, id: 10 */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName("testusers")
                .setReturnRow(true)
                .setRowMetadata(RM2);

            DeleteResult del = handle.delete(delRequest);
            assertTrue("Delete failed", del.getSuccess());
            assertWriteKB(del);
            assertEquals( RM1, del.getExistingRowMetadata());

            /* GET -- no row, it was removed above */
            getRequest.setTableName("testusers");
            res1 = handle.get(getRequest);
            assertNull(res1.getValue());
            assertReadKB(res1);

            /* GET -- no table */
            try {
                getRequest.setTableName("not_a_table");
                res1 = handle.get(getRequest);
                fail("Attempt to access missing table should have thrown");
            } catch (TableNotFoundException nse) {
                /* success */
            }

            /* PUT -- invalid row -- this will throw */
            try {
                value.remove("id");
                value.put("not_a_field", 1);
                res = handle.put(putRequest);
                fail("Attempt to put invalid row should have thrown");
            } catch (IllegalArgumentException iae) {
                /* success */
            }
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

        /* Put a row with empty table name: should get illegal argument */
        PutRequest putReq = new PutRequest()
            .setValue(value)
            .setDurability(dur)
            .setTableName("")
            .setRowMetadata(RM1);
        try {
            handle.put(putReq);
            fail("expected illegal argument exception on empty table name");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Put a row */
        putReq = new PutRequest()
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM2);
        PutResult putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true  /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */ );

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
        assertNull(putRes.getExistingRowMetadata());

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
        assertEquals(RM2, putRes.getExistingRowMetadata());

        /*
         * Put a new row with SetReturnRow(true),
         * expect no existing row returned.
         */
        putReq = new PutRequest()
            .setValue(newValue)
            .setDurability(dur)
            .setTableName(tableName)
            .setReturnRow(true)
            .setRowMetadata(RM3);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        assertNull(putRes.getExistingRowMetadata());

        /* PutIfAbsent an existing row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM4);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        assertNull(putRes.getExistingRowMetadata());

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
        assertEquals(RM2, putRes.getExistingRowMetadata());

        /* PutIfPresent an existing row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM5);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        oldVersion = putRes.getVersion();
        assertNull(putRes.getExistingRowMetadata());  // no expPrevValue

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
        Version ifVersion = putRes.getVersion();
        assertEquals(RM5, putRes.getExistingRowMetadata());

        /* PutIfPresent an new row, it should fail */
        putReq = new PutRequest()
            .setOption(Option.IfPresent)
            .setValue(newValue1)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM1);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        assertNull(putRes.getExistingRowMetadata());

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
        assertNull(putRes.getExistingRowMetadata());

        /* PutIfAbsent an new row, it should succeed */
        putReq = new PutRequest()
            .setOption(Option.IfAbsent)
            .setValue(newValue1)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM2);
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
        assertNull(putRes.getExistingRowMetadata());

        /*
         * PutIfVersion an existing row with unmatched version, it should fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(oldVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM3);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            false /* shouldSucceed */,
            false  /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        assertNull(putRes.getExistingRowMetadata());

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
        assertEquals(RM5, putRes.getExistingRowMetadata());

        /*
         * Put an existing row with matching version, it should succeed.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setMatchVersion(ifVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM4);
        putRes = handle.put(putReq);
        checkPutResult(putReq, putRes,
            true /* shouldSucceed */,
            false /* rowPresent */,
            null /* expPrevValue */,
            null /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB,
            false /* put overWrite */);
        ifVersion = putRes.getVersion();
        assertNull(putRes.getExistingRowMetadata());

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
        Version newVersion = putRes.getVersion();
        assertNull(putRes.getExistingRowMetadata());

        /*
         * Put with IfVersion but no matched version is specified, put should
         * fail.
         */
        putReq = new PutRequest()
            .setOption(Option.IfVersion)
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(RM5);
        try {
            putRes = handle.put(putReq);
            fail("Put with IfVersion should fail");
        } catch (IllegalArgumentException iae) {
            checkErrorMessage(iae);
        }

        /*
         * Get
         */

        /* Get a row with empty table name: should get illegal argument */
        MapValue key = new MapValue().put("id", 10);
        GetRequest getReq = new GetRequest()
            .setKey(key)
            .setTableName("");
        try {
            handle.get(getReq);
            fail("expected illegal argument exception on empty table name");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Get a row */
        getReq = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            true /* rowPresent*/,
            value,
            null, /* Don't check version if Consistency.EVENTUAL */
            true, /* modtime should be recent */
            recordKB);
        assertEquals(RM4, getRes.getRowMetadata());

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            true /* rowPresent*/,
            value,
            newVersion,
            true, /* modtime should be recent */
            recordKB);
        assertEquals(RM4, getRes.getRowMetadata());

        /* Put row with null row metadata, ie remove */
        putReq = new PutRequest()
            .setValue(value)
            .setDurability(dur)
            .setTableName(tableName)
            .setRowMetadata(null);
        handle.put(putReq);

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
        assertNull(getRes.getRowMetadata());


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
        assertNull(getRes.getRowMetadata());

        /* Get a row with ABSOLUTE consistency */
        getReq.setConsistency(Consistency.ABSOLUTE);
        getRes = handle.get(getReq);
        checkGetResult(getReq, getRes,
            false /* rowPresent*/,
            null  /* expValue */,
            null  /* expVersion */,
            false, /* modtime should be zero */
            recordKB);
        assertNull(getRes.getRowMetadata());

        /* Delete a row with empty table name: should get illegal argument */
        key = new MapValue().put("id", 10);
        DeleteRequest delReq = new DeleteRequest()
            .setKey(key)
            .setTableName("");
        try {
            handle.delete(delReq);
            fail("expected illegal argument exception on empty table name");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Delete a row */
        delReq = new DeleteRequest()
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
        assertNull(delRes.getExistingRowMetadata());

        /* Put the row back to store */
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setRowMetadata(RM5);
        putRes = handle.put(putReq);
        oldVersion = putRes.getVersion();
        assertNotNull(oldVersion);
        assertNull(putRes.getExistingRowMetadata());

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
        assertEquals(RM5, delRes.getExistingRowMetadata());

        /* Delete fail + setReturnRow(true), no existing row returned. */
        delRes = handle.delete(delReq);
        checkDeleteResult(delReq, delRes,
            false /* shouldSucceed */,
            false /* rowPresent */,
            null  /* expPrevValue */,
            null  /* expPrevVersion */,
            false, /* modtime should be zero */
            recordKB);
        assertNull(delRes.getExistingRowMetadata());

        /* Put the row back to store */
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setRowMetadata(RM1);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();
        assertNull(putRes.getExistingRowMetadata());

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
        assertNull(delRes.getExistingRowMetadata());

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
        assertEquals(RM1, delRes.getExistingRowMetadata());

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
        assertNull(delRes.getExistingRowMetadata());

        /* Put the row back to store */
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setRowMetadata(RM2);
        putRes = handle.put(putReq);
        ifVersion = putRes.getVersion();
        assertNull(putRes.getExistingRowMetadata());

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
        assertNull(delRes.getExistingRowMetadata());

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
        assertNull(delRes.getExistingRowMetadata());

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
        assertNull(delRes.getExistingRowMetadata());
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
                    .setValue(row)
                    .setRowMetadata(RM1);
                ;
                PutResult putRet = handle.put(putReq);
                Version pVersion = putRet.getVersion();
                assertNotNull(pVersion);
                assertNull(putRet.getExistingRowMetadata());

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
                assertEquals(RM1, getRet.getRowMetadata());
            }
        }

        // add rmt field for checking the return of the query
        rowNull.put("rmt", 1);
        rowJsonNull.put("rmt", 1);

        /*
         * Query with variable for json field and set NullValue or
         * JsonNullValue to variable, the NullValue is expected to be converted
         * to JsonNullValue.
         */
        String query = "declare $name json;" +
            "select id, info, row_metadata($t).n as rmt from tjson $t " +
            "where $t.info.name = $name";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        prepStmt.setVariable("$name", JsonNullValue.getInstance());
        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        boolean shouldRetry = false;
        do {
            try {
                QueryResult queryRet = handle.query(queryReq);
                assertEquals(1, queryRet.getResults().size());
                assertEquals(rowJsonNull, queryRet.getResults().get(0));

                prepStmt.setVariable("$name", NullValue.getInstance());
                queryRet = handle.query(queryReq);
                assertEquals(0, queryRet.getResults().size());
            } catch (SystemException e) {
                shouldRetry = e.okToRetry();
                System.out.println("Caught " + (e.okToRetry() ? "retryable" :
                    "") + " ex: " + e.getMessage());
                System.out.println(
                    "Retrying query: " + queryReq.getStatement());
                e.printStackTrace();
                Thread.sleep(500);
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

        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());


        QueryRequest queryReq = new QueryRequest()
            .setStatement("select s, id, $t.info.name as name, row_metadata($t) as rmt from " +
                tableName + " $t ORDER BY id ASC");
        QueryResult qRes = handle.query(queryReq);

        int i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(1, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertEquals("John", v.get("name").asString().getString());
            assertTrue(v.get("rmt").isMap());
            assertTrue(v.get("rmt").asMap().get("n").isInteger());
            assertEquals(i, v.get("rmt").asMap().get("n").asInteger().getInt());
            i++;
        }
        assertEquals(10, qRes.getResults().size());
        assertEquals(10, i);

        // do a multi delete operation
        MultiDeleteRequest multiDeleteReq = new MultiDeleteRequest()
            .setTableName(tableName)
            .setKey(new MapValue().put("s", 1))
            .setRange(new FieldRange("id")
                .setStart(new IntegerValue(0), true)
                .setEnd(new IntegerValue(5), false))
            .setRowMetadata(RM1);
        MultiDeleteResult multiDeleteRes = handle.multiDelete(multiDeleteReq);

        assertEquals(5, multiDeleteRes.getNumDeletions());

        qRes = handle.query(queryReq);
        assertEquals(5, qRes.getResults().size());
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
                ;
                PutResult putRet = handle.put(putReq);
                Version pVersion = putRet.getVersion();
                assertNotNull(pVersion);

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
                assertNull(getRet.getRowMetadata());
            }
        }

        /*
         * Query with variable for json field and set NullValue or
         * JsonNullValue to variable, the NullValue is expected to be converted
         * to JsonNullValue.
         */
        String query = "declare $name json;" +
            "select id, info, row_metadata($t) as rmt from tjson $t " +
            "where $t.info.name = $name";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRet.getPreparedStatement();

        prepStmt.setVariable("$name", JsonNullValue.getInstance());
        QueryRequest queryReq = new QueryRequest()
            .setPreparedStatement(prepStmt);

        rowNull.put("rmt", JsonNullValue.getInstance());
        rowJsonNull.put("rmt", JsonNullValue.getInstance());

        boolean shouldRetry = false;
        do {
            try {
                QueryResult queryRet = handle.query(queryReq);
                assertEquals(1, queryRet.getResults().size());
                assertEquals(rowJsonNull, queryRet.getResults().get(0));

                prepStmt.setVariable("$name", NullValue.getInstance());
                queryRet = handle.query(queryReq);
                assertEquals(0, queryRet.getResults().size());
            } catch (SystemException e) {
                shouldRetry = e.okToRetry();
                System.out.println("Caught " + (e.okToRetry() ? "retryable" : "") + " ex: " + e.getMessage());
                System.out.println("Retrying query: " + queryReq.getStatement());
                e.printStackTrace();
                Thread.sleep(500);
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

        /* Put row without mentioning row metadata */
        PutRequest putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putRes = handle.put(putReq);
        assertNull(putRes.getExistingRowMetadata());

        /* Get the row back no row metadata expected */
        GetRequest getReq = new GetRequest()
            .setTableName(tableName)
            .setKey(value);
        GetResult getRet = handle.get(getReq);
        assertNull(getRet.getRowMetadata());


        /* Put row with row metadata */
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setRowMetadata(RM1);
        putRes = handle.put(putReq);
        assertNull(putRes.getExistingRowMetadata());

        /* Get the row back check there is the expected row metadata */
        getReq = new GetRequest()
            .setTableName(tableName)
            .setKey(value);
        getRet = handle.get(getReq);
        assertEquals(RM1, getRet.getRowMetadata());


        /* Put row without row metadata */
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setReturnRow(true)
            .setRowMetadata(null);
        putRes = handle.put(putReq);
        assertEquals(RM1, putRes.getExistingRowMetadata());

        /* Get the row back check row metadata is null */
        getReq = new GetRequest()
            .setTableName(tableName)
            .setKey(value);
        getRet = handle.get(getReq);
        assertNull(getRet.getRowMetadata());


        /* Delete row check prev/existing is still null */
        DeleteRequest delReq = new DeleteRequest()
            .setKey(value)
            .setTableName(tableName)
            .setReturnRow(true);
        DeleteResult delRes = handle.delete(delReq);
        assertNull(delRes.getExistingRowMetadata());


        /* Query */
        putReq = new PutRequest()
            .setValue(value)
            .setTableName(tableName)
            .setRowMetadata(RM1);
        putRes = handle.put(putReq);
        assertNull(putRes.getExistingRowMetadata());

        QueryRequest queryReq = new QueryRequest()
            .setStatement("select id, name, row_metadata($t) as rmt from " +
                tableName + " $t");
        QueryResult qRes = handle.query(queryReq);

        assertEquals(1, qRes.getResults().size());
        assertEquals(10, qRes.getResults().get(0).get("id").getInt());
        assertEquals("John", qRes.getResults().get(0).get("name").getString());
        assertEquals(RM1, qRes.getResults().get(0).get("rmt").toJson());
        assertTrue(qRes.getResults().get(0).get("rmt").isMap());
        assertTrue(qRes.getResults().get(0).get("rmt").asMap().get("n").isInteger());
        assertEquals(1, qRes.getResults().get(0).get("rmt").asMap().get("n").asInteger().getInt());
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
                .setRowMetadata("{\"n\":" + i + "}")
                .setValue(new MapValue()
                    .put("s", 1)
                    .put("id", i)
                    .put("name", "John"));
            wmReq.add(pr, true);
        }
        WriteMultipleResult wmRes = handle.writeMultiple(wmReq);
        assertEquals(10, wmRes.getResults().size());

        /* query read metadata */
        QueryRequest queryReq = new QueryRequest()
            .setStatement("select s, id, name, row_metadata($t) as rmt from " +
                tableName + " $t ORDER BY id ASC");
        QueryResult qRes = handle.query(queryReq);

        int i = 0;
        for (MapValue v : qRes.getResults()) {
            assertEquals(1, v.get("s").asInteger().getInt());
            assertEquals(i, v.get("id").asInteger().getInt());
            assertEquals("John", v.get("name").asString().getString());
            assertTrue(v.get("rmt").isMap());
            assertTrue(v.get("rmt").asMap().get("n").isInteger());
            assertEquals(i, v.get("rmt").asMap().get("n").asInteger().getInt());
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
        String query = "declare $id integer; insert into " + tableName + " values( 0, $id, {})";
        for (int i = 0; i < 10; i++) {
            PrepareRequest prepReq = new PrepareRequest().setStatement(query);
            PrepareResult prepRet = handle.prepare(prepReq);

            PreparedStatement prepStmt = prepRet.getPreparedStatement();
            prepStmt.setVariable("$id", new IntegerValue(i));

            QueryRequest queryReq = new QueryRequest()
                .setPreparedStatement(prepStmt)
                .setRowMetadata(RM1);

            QueryResult queryRes = handle.query(queryReq);
            assertNotNull(queryRes);
            assertEquals(1, queryRes.getResults().get(0).asMap().get("NumRowsInserted").asInteger().getInt());
        }

        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, row_metadata($t) as rmt from " +
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
            assertEquals(RM1, v.get("rmt").toJson());
            i++;
        }
        assertEquals(10, i);


        // update many
        query = "update " + tableName + " t SET t.info = t.info where t.s = 0";

        queryReq = new QueryRequest()
            .setStatement(query)
            .setRowMetadata(RM2);

        qRes = handle.query(queryReq);
        assertEquals(1, qRes.getResults().size());
        assertEquals(10, qRes.getResults().get(0).asMap().get("NumRowsUpdated").asInteger().getInt());


        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, row_metadata($t) as rmt from " +
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
            assertEquals(RM2, v.get("rmt").toJson());
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
        String query = "declare $id integer; insert into " + tableName + " values( 0, $id, {\"info\":1})";
        for (int i = 0; i < 10; i++) {
            PrepareRequest prepReq = new PrepareRequest().setStatement(query);
            PrepareResult prepRet = handle.prepare(prepReq);

            PreparedStatement prepStmt = prepRet.getPreparedStatement();
            prepStmt.setVariable("$id", new IntegerValue(i));

            QueryRequest queryReq = new QueryRequest()
                .setPreparedStatement(prepStmt)
                .setRowMetadata(RM1);

            QueryResult queryRes = handle.query(queryReq);
            assertNotNull(queryRes);
            assertEquals(1, queryRes.getResults().get(0).asMap().get("NumRowsInserted").asInteger().getInt());
        }

        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, row_metadata($t) as rmt from " +
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
            assertEquals(RM1, v.get("rmt").toJson());
            i++;
        }
        assertEquals(10, i);


        // update many
        query = "update " + tableName + " t SET t.info=3 where t.s = 0";

        queryReq = new QueryRequest()
            .setStatement(query)
            .setRowMetadata(RM2);

        qRes = handle.query(queryReq);
        assertEquals(1, qRes.getResults().size());
        assertEquals(10, qRes.getResults().get(0).asMap().get("NumRowsUpdated").asInteger().getInt());


        // check they have the correct row metadata
        query =
            "select $t.s, $t.id, $t.info, row_metadata($t) as rmt from " +
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
            assertEquals(RM2, v.get("rmt").toJson());
            i++;
        }
        assertEquals(10, i);
    }

    @Test
    public void testValidJSON() {
        // invalid setRowMetadata values
        String[] invalidJsons = new String[] {
            "custom metadata",
            "",
            " ",
            "\n",
            "\t",
            "'abc'",
            "{{}}",
            "{}{}",
            "{}\n{}",
            "{}   {}",
            "{},{}",
            "{\"a\":'c'}",   // single quoted string
            "{'a':1}",       // single quoted string
            "NULL",
            "Null",
            "True",
            "FALSE",
            "{\"a\":\"Invalid string \u0000\"",
            "\"abc\"\"def\"",
            "1true2null",
            "1,2,3",
            "[][]",
            "INF",  // ??? since -INF is allowed
            "Inf",
            "-Inf",
            "NAN",
            "Not-A-Number",
        };

        for (String invalidJson : invalidJsons) {
            assertThrows(IllegalArgumentException.class, () ->
                JsonUtils.validateJsonConstruct(invalidJson));
            assertThrows(IllegalArgumentException.class, () ->
                new PutRequest().setRowMetadata(invalidJson));
            assertThrows(IllegalArgumentException.class, () ->
                new DeleteRequest().setRowMetadata(invalidJson));
            assertThrows(IllegalArgumentException.class, () ->
                new QueryRequest().setRowMetadata(invalidJson));
            assertThrows(IllegalArgumentException.class, () ->
                new MultiDeleteRequest().setRowMetadata(invalidJson));
        }

        // valid values for setRowMetadata
        String[] validValues = new String[] {
            null,
            "{}",
            "{\"a\":1}",
            "{\"a\":2, \"b\":\"a\"}",
            "{\"a\":[]}",
            "{\"a\":[1, 2, 3]}",
            " { } ",
            "\n{\n}\n",
            " \t\n{ \n \t } ",
            "{\"a\": { \"b\":\"a\"}}",
            "{\"a\":{ \"b\":{}}}",
            "{\"a\":1}",
            "{\"a\":true}",
            "{\"a\":[null,1,\"c\", true, [[], {}, null]]}",
            "\"abc\"",
            "\"\"",
            "123",
            "123.456",
            "null",
            "true",
            "false",
            "[]",
            "[1, \"s\", true]",

            // Non-numerical numbers are allowed
            "NaN",
            "{\"a\":NaN}",
            "-INF",
            "Infinity",
            "-Infinity"
        };

        for (String v : validValues) {
            new PutRequest().setRowMetadata(v);
            new DeleteRequest().setRowMetadata(v);
            new QueryRequest().setRowMetadata(v);
            new MultiDeleteRequest().setRowMetadata(v);
        }
    }
}
