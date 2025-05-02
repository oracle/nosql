/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TTLExpirationTest extends TableTestBase {

    private static final String TABLE_NAME       = "ttltesttable";
    private static final String CREATE_TABLE_DDL = "create table " + TABLE_NAME +
                                                   "(" +
                                                   "id integer, " +
                                                   "data string, " +
                                                   "primary key (id)" +
                                                   ")";
    private static final long   ROW_COUNT        = 1000L;

    private Table tableH;
    private long  startTimeOfTest;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        //TODO: remove this after MRTable supports TTL.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp();
    }

    public static void setRNTime(long ttlTime) {
        try {
            for (RemoteTestAPI rta : createStore.getRepNodeTestAPI()) {
                rta.setTTLTime(ttlTime);
            }
        } catch (java.rmi.RemoteException re) {
            throw new RuntimeException("Couldn't set RN time: " + re);
        }
    }

    @Before
    public void setupTestcase() {
        startTimeOfTest = System.currentTimeMillis();
        setRNTime(startTimeOfTest);

        executeDdl(CREATE_TABLE_DDL);
        tableH = getTable(TABLE_NAME);
        assertTrue(tableH != null);
    }

    private long getRowCount() {
        PrimaryKey           pkey      = null;
        long                 trowCnt   = 0L;
        TableIterator<Row>   titer     = null;
        TableIteratorOptions tio       = null;

        pkey  = tableH.createPrimaryKey();
        tio   = new TableIteratorOptions(Direction.FORWARD,
                                         Consistency.ABSOLUTE,
                                         30L,
                                         TimeUnit.SECONDS);
        titer = tableImpl.tableIterator(pkey, null, tio);

        while (titer.hasNext()) {
            titer.next();
            trowCnt++;
        }
        titer.close();

        return trowCnt;
    }

    @Test
    public void testExpirationOfHours() {
        Row        row = null;
        TimeToLive ttl = TimeToLive.ofHours(10);

        row = tableH.createRow();
        row.setTTL(ttl);

        for(int i=1; i<=ROW_COUNT; i++) {
            row.put("id", i);
            tableImpl.put(row, null, null);
        }

        assertEquals(ROW_COUNT, getRowCount());
        setRNTime(startTimeOfTest + 9L*60L*60L*1000L);
        assertEquals(ROW_COUNT, getRowCount());
        setRNTime(startTimeOfTest + 11L*60L*60L*1000L);
        assertEquals(0L, getRowCount());
    }

    @Test
    public void testExpirationOfDays() {
        Row        row = null;
        TimeToLive ttl = TimeToLive.ofDays(30);

        row = tableH.createRow();
        row.setTTL(ttl);

        for(int i=1; i<=ROW_COUNT; i++) {
            row.put("id", i);
            tableImpl.put(row, null, null);
        }

        assertEquals(ROW_COUNT, getRowCount());
        setRNTime(startTimeOfTest + 29L*24L*60L*60L*1000L);
        assertEquals(ROW_COUNT, getRowCount());
        setRNTime(startTimeOfTest + 31L*24L*60L*60L*1000L);
        assertEquals(0L, getRowCount());
    }

    @Test
    public void testKeyOnlySecondaryIndexIntegrity() {
        final String indexDdl = "create index idxId on " + TABLE_NAME + "(id)";
        executeDdl(indexDdl);
        tableH = tableImpl.getTable(TABLE_NAME);

        final TimeToLive ttl = TimeToLive.ofHours(1);
        final WriteOptions options = new WriteOptions().setUpdateTTL(true);

        /* insert row */
        Row row = tableH.createRow();
        row.put("id", 1).put("data", "a");
        tableImpl.put(row, null, null);
        assertEquals(1, getRowCount());

        /* update row with TTL */
        row.setTTL(ttl);
        tableImpl.put(row, null, options);

        /* set RepNode's TTL time to force expiration, row should be expired. */
        setRNTime(startTimeOfTest + 2L*60L*60L*1000L);
        assertEquals(0, getRowCount());

        /* insert row again */
        row.setTTL(null);
        tableImpl.putIfAbsent(row, null, null);
        assertEquals(1, getRowCount());
    }
}
