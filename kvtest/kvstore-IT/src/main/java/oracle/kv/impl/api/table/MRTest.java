/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.util.NioByteOutputStream;

import oracle.kv.BulkWriteOptions;
import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVStoreConfig;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.XRegionService;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.WriteOptions;

import oracle.kv.util.CreateStore;
import oracle.kv.util.TableTestUtils;
import oracle.nosql.common.contextlogger.LogFormatter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Multi-region test using CreateStore
 * Steps to set up MR tables:
 * 1. create local/remote stores
 * 2. set local region names for each
 * 3. configure the XRegion service
 *  a. create config for each region
 *  b. start service for each region (service == agent)
 * 4. create remote regions (e.g. local needs to know about remote, vice versa)
 */
public class MRTest extends TestBase {

    private static final boolean verbose = false;

    private static final int startPort = 13250;
    private static final String localStoreName = "JsonMR-local";
    private static final String remoteStoreName = "JsonMR-remote";
    private static final String localRegion = "LOC";
    private static final String remoteRegion = "REM";

    private static CreateStore localStore;
    private static CreateStore remoteStore;

    private static KVStore localKV;
    private static KVStore remoteKV;

    private static TableAPIImpl localTableImpl;
    private static TableAPIImpl remoteTableImpl;

    private static HashSet<String> localHelpers;
    private static HashSet<String> remoteHelpers;
    private static RegionInfo localInfo;
    private static XRegionService remoteService;

    private static int localRegionId;

    private static Logger remoteTestLogger;

    private static int waitTime = 13; /* replication wait in seconds */

    private static Logger setupLogger(String fileName) {
        final String testPath = TestUtils.getTestDir().getAbsolutePath();
        Logger logger = ClientLoggerUtils.getLogger(MRTest.class, "MRTest");
        try {
            final File loggerFile = new File(new File(testPath), fileName);
            final FileHandler handler =
                new FileHandler(loggerFile.getAbsolutePath(), false);
            handler.setFormatter(new LogFormatter(null));
            logger.removeHandler(handler);
            logger.addHandler(handler);
            logger.info("Add test log file handler: path=" + testPath +
                        ", log file name=" + fileName +
                        ", file exits?=" + loggerFile.exists());
        } catch (Exception e) {}
        return logger;
    }

    @BeforeClass
    public static void staticSetUp() throws Exception {
        assumeTrue("All cross-region tests require async",
                   AsyncControl.serverUseAsync);

        TestUtils.clearTestDirectory();
        /* do this after clearing test directory or it gets removed */
        remoteTestLogger = setupLogger("remote");

        localStore = createStore(localStoreName, startPort);
        localKV = KVStoreFactory.getStore(createKVConfig(localStore));
        localTableImpl = (TableAPIImpl)localKV.getTableAPI();
        executeDdl(localKV, "set local region " + localRegion);
        String helperString = localStore.getHostname() + ":" +
            localStore.getRegistryPort();
        localHelpers = new HashSet<String>();
        localHelpers.add(helperString);
        localInfo = new RegionInfo(localRegion, localStoreName,
                                   new String[]{helperString});

        remoteStore = createStore(remoteStoreName, startPort);
        remoteKV = KVStoreFactory.getStore(createKVConfig(remoteStore));
        remoteTableImpl = (TableAPIImpl)remoteKV.getTableAPI();
        executeDdl(remoteKV, "set local region " + remoteRegion);
        helperString = remoteStore.getHostname() + ":" +
            remoteStore.getRegistryPort();
        remoteHelpers = new HashSet<String>();
        remoteHelpers.add(helperString);

        /*
         * Wait for tables to be initialized. This has to happen after the
         * DDL operations above, for now
         */
        TableTestBase.waitForTableMDSysTable(localTableImpl);
        TableTestBase.waitForTableMDSysTable(remoteTableImpl);

        JsonConfig remoteConfig =
            createXRConfig(TestUtils.getTestDir().toString(),
                           remoteRegion,
                           remoteStoreName,
                           remoteHelpers);
        remoteConfig.addRegion(localInfo);
        remoteService = createXRService(remoteConfig, remoteTestLogger);
        Thread remoteServiceThread = new Thread(remoteService);
        remoteServiceThread.start();

        while (!remoteService.isRunning()) {
            Thread.sleep(1000);
        }
        verbose("XR service running : " + remoteService.isRunning());

        /* add "other" regions */
        executeDdl(localKV, "create region " + remoteRegion);
        executeDdl(remoteKV, "create region " + localRegion);
	localRegionId =
	    localTableImpl.getRegionMapper().getRegionId(localRegion);
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        /* shutdown agents first */
        if (remoteService != null) {
            remoteService.shutdown();
        }
        if (localStore != null) {
            localKV.close();
            localStore.shutdown();
        }
        if (remoteStore != null) {
            remoteKV.close();
            remoteStore.shutdown();
        }
    }

    @Before
    @Override
    public void setUp()
        throws Exception {
    }

    @After
    @Override
    public void tearDown()
        throws Exception {
        TableTestUtils.removeTables(localTableImpl.getTables(),
                                    localStore.getAdmin(),
                                    localKV);
        TableTestUtils.removeTables(remoteTableImpl.getTables(),
                                    remoteStore.getAdmin(),
                                    remoteKV);
        waitForQueue();
    }

    @Test
    public void testMRCounter() throws Exception {
        final int fakeRegionId = 5;
        /*
         * create non-MR table with counter to observe behavior
         */
        executeDdl(localKV,
                   "create table foo_not_mr(id integer, primary key(id), " +
                   "json json(count as integer mr_counter))");
        TableImpl notmrTable =
            (TableImpl) localTableImpl.getTable("foo_not_mr");
        /* put row in local */
        Row trow = notmrTable.createRow();
        trow.put("id", 1);
        /* requires a path to the MR counter */
        trow.putJson("json", "{\"count\": 1, \"a\": 1}");
        localTableImpl.put(trow, null, null);
        /* local get */
        PrimaryKey tpkey = notmrTable.createPrimaryKey();
        tpkey.put("id", 1);
        trow = localTableImpl.get(tpkey, null);
        assertTrue(
            trow.get("json").asMap().get("count").asInteger().get() == 0);

        /*
         * Requires region id or will throw
         */
        ExecuteOptions options = new ExecuteOptions().setRegionId(fakeRegionId);
        executeDml(localKV,
                   "update foo_not_mr $f set $f.json.count = $ + 5 where " +
                   "id = 1", options);
        trow = localTableImpl.get(tpkey, null);
        assertTrue(
            trow.get("json").asMap().get("count").asInteger().get() == 5);

        /* NSON */
        NsonTest nsonTest = new NsonTest();
        byte[] nval = nsonTest.createNsonFromValue((FieldValueImpl)trow);
        RowImpl nRow = notmrTable.createRow();
        assertTrue(nsonTest.readNsonValue(nRow, nval, 0));
        assertTrue(
            nRow.get("json").asMap().get("count").asInteger().get() == 5);


        /* another put with modified val -- will be ignored */
        trow.putJson("json", "{\"count\": 6, \"a\": 3}");
        localTableImpl.put(trow, null, null);
        trow = localTableImpl.get(tpkey, null);
        /* counter value should still be 5 */
        assertTrue(
            trow.get("json").asMap().get("count").asInteger().get() == 5);

        /*
         * create a fake JSON mr counter and insert it using putResolve
         */
        TablePath path = notmrTable.getSchemaMRCounterPaths(1).get(0);
        FieldDefImpl counterDef = new IntegerDefImpl(true);
        IntegerValueImpl counterVal =
            (IntegerValueImpl) counterDef.createCRDTValue();
        IntegerValueImpl tval = (IntegerValueImpl) counterDef.createInteger(6);
        counterVal.incrementMRCounter(tval, fakeRegionId);
        JsonDefImpl.insertMRCounterField((FieldValueImpl) trow, path,
                                         counterVal, true);

        /* use putResolve to put counter value */
        trow.put("id", 2);
        tpkey.put("id", 2);
        WriteOptions wo = new WriteOptions();
        /*
         * region id needs to be both in WriteOptions and the row to convince
         * putResolve to work
         */
        wo.setRegionId(fakeRegionId);
        ((RowImpl)trow).setRegionId(fakeRegionId);
        boolean prRes = localTableImpl.putResolve(trow, null, wo);
        assertTrue(prRes);
        trow = localTableImpl.get(tpkey, null);
        assertEquals(6, trow.get("json").asMap().get("count").asInteger().get());
        localTableImpl.delete(tpkey, null, null);



        /*
         * Create real MR table in both regions, and both "normal" and
         * JSON MR Counter
         */
        final String mrName = "bar";
        executeDdl(remoteKV, "create table " + mrName +
                   "(id integer, primary key(id), " +
                   "counter integer as mr_counter, " +
                   "json json(count as integer mr_counter)) " +
                   "in regions LOC, REM");
        executeDdl(localKV, "create table " + mrName +
                   "(id integer, primary key(id), " +
                   "counter integer as mr_counter, " +
                   "json json(count as integer mr_counter)) " +
                   "in regions REM, LOC");

        /*
         * attempts to create indexes on MR counters (either normal or
         * JSON) should fail
         */
        try {
            executeDdl(remoteKV, "create index counterIdx on " + mrName +
                       "(json.count as integer)");
            fail("Indexes are not allowed on MR Counters");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }

        try {
            executeDdl(remoteKV, "create index counterIdx on " + mrName +
                       "(counter as integer)");
            fail("Indexes are not allowed on MR Counters");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }

        TableImpl localTable = (TableImpl) localTableImpl.getTable(mrName);
        TableImpl remoteTable = (TableImpl) remoteTableImpl.getTable(mrName);

        /* put row in local */
        Row row = localTable.createRow();
        row.put("id", 1);
        row.putJson("json", "{\"count\": 1, \"a\": 1}");
        localTableImpl.put(row, null, null);

        /* update local->remote */
        executeDml(localKV, "update " + mrName +
                   " $f set $f.json.count = $ + 5 where id = 1");

        wait(waitTime);

        tpkey = localTable.createPrimaryKey();
        tpkey.put("id", 1);
        trow = localTableImpl.get(tpkey, null);
        assertTrue(
            trow.get("json").asMap().get("count").asInteger().get() == 5);

        tpkey = remoteTable.createPrimaryKey();
        tpkey.put("id", 1);
        trow = remoteTableImpl.get(tpkey, null);
        assertTrue(
            trow.get("json").asMap().get("count").asInteger().get() == 5);

        /* update local->remote, 3x */
        for (int i = 0; i < 3; i++) {
            executeDml(localKV,
                       "update " + mrName +
                       " $f set $f.json.count = $ + 5 where id = 1");
        }
        wait(5);

        /* do a simple get */
        PrimaryKey pkey = localTable.createPrimaryKey();
        pkey.put("id", 1);
        row = localTableImpl.get(pkey, null);
        assertTrue(
            row.get("json").asMap().get("count").asInteger().get() == 20);

        /* verify that nested MR counters are not allowed */
        try {
            executeDdl(localKV,
                   "create table blah(id integer, primary key(id), " +
                   "rec record(counter integer as mr_counter))");
            fail("Nested MR counters are not supported");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /*
         * Test for bug where "returning" clause returns a counter
         * value provided with an insert instead of 0
         * KVSTORE-2039
         */
        @SuppressWarnings("unused")
        List<RecordValue> res = executeDml(
            localKV, "insert into " + mrName +
            " values(10, DEFAULT, {\"count\": 10}) returning *");
        for (@SuppressWarnings("unused") RecordValue r : res) {
            /* TODO: when fixed, assert that count is 0 */
        }
    }

    @Test
    public void testJCUpdateOnLongNumberTypeMRCounter () throws Exception{
        executeDdl(remoteKV,
                   "create table jsoncol(id integer, primary key(id), " +
                   "intCounter as integer mr_counter, " +
                   "longCounter as long mr_counter, " +
                   "numberCounter as number mr_counter) " +
                   "in regions LOC, REM as json collection");
        executeDdl(localKV,
                   "create table jsoncol(id integer, primary key(id), " +
                   "intCounter as integer mr_counter, " +
                   "longCounter as long mr_counter, " +
                   "numberCounter as number mr_counter) " +
                   "in regions REM, LOC as json collection");
        /* verify the fix for KVSTORE-2087 */
        TableImpl localTable = (TableImpl) localTableImpl.getTable("jsoncol");
        @SuppressWarnings("unused")
        TableImpl remoteTable = (TableImpl) remoteTableImpl.getTable("jsoncol");

        /* put row in local */
        Row row = localTable.createRow();
        row.put("id", 1);
        row.put("intCounter", 0);
        row.put("longCounter", 0);
        row.put("numberCounter", 0);
        localTableImpl.put(row, null, null);

        executeDml(localKV,
                "update jsoncol $f set $f.intCounter = $ + 5 where " +
                        "id = 1");
        executeDml(localKV,
                "update jsoncol $f set $f.longCounter = $ + 5 where " +
                        "id = 1");
        executeDml(localKV,
                "update jsoncol $f set $f.numberCounter = $ + 5 where " +
                        "id = 1");
        /* check the update happen in local store table mr counters */
        PrimaryKey lpkey = localTable.createPrimaryKey();
        lpkey.put("id", 1);
        Row getRow = localTableImpl.get(lpkey, null);
        assertTrue(((FieldValueImpl)getRow.get("intCounter")).getInt() == 5);
        assertTrue(((FieldValueImpl)getRow.get("longCounter")).getLong() == 5);
        assertTrue(((FieldValueImpl)getRow.get("numberCounter")).
                   castAsInt() == 5);
    }

    @Test
    public void testJsonCollection() throws Exception {
        executeDdl(remoteKV, "create table foo(id integer, primary key(id), " +
                   "counter as long mr_counter) " +
                   "in regions LOC, REM as json collection");
        executeDdl(localKV, "create table foo(id integer, primary key(id), " +
                   "counter as long mr_counter) " +
                   "in regions REM, LOC as json collection");
        /* verify that indexes on MR counters are not supported */
        try {
            executeDdl(localKV, "create index idx1 on foo(counter as long)");
            fail("Indexes are not allowed on MR Counters");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        TableImpl localTable = (TableImpl) localTableImpl.getTable("foo");
        TableImpl remoteTable = (TableImpl) remoteTableImpl.getTable("foo");

        /* put row in local */
        Row row = localTable.createRow();
        row.put("id", 1);
        row.putJson("some_json", "{\"a\": {\"b\": {\"c\":1}}}");
        row.put("counter", 1);
        localTableImpl.put(row, null, null);

        wait(waitTime);

        PrimaryKey lpkey = localTable.createPrimaryKey();
        lpkey.put("id", 1);
        Row getRow = localTableImpl.get(lpkey, null);
        assertTrue(((FieldValueImpl)getRow.get("counter")).getLong() == 0);

        PrimaryKey rpkey = remoteTable.createPrimaryKey();
        rpkey.put("id", 1);
        getRow = remoteTableImpl.get(rpkey, null);
        assertTrue(((FieldValueImpl)getRow.get("counter")).getLong() == 0);

        executeDml(localKV, "update foo $f set $f.counter = $ + 5 where " +
                   "id = 1");

        getRow = localTableImpl.get(lpkey, null);
        assertTrue(((FieldValueImpl)getRow.get("counter")).getLong() == 5);

        /* Look at NSON formats for correctness
         * o serialized row will have MR counter map
         */
        Value v =
            localTable.createValueInternal((RowImpl)getRow,
                                           Value.Format.MULTI_REGION_TABLE,
                                           localRegionId,
                                           (KVStoreImpl)localKV, null, false);
        byte[] vbytes = v.getValue();
        /*
         * does not include format and region id. MR counter should be in
         * map format
         */
        String nString = Nson.toJsonString(vbytes, null);
        assertTrue(nString.contains("\"counter\":{\"1\":5}"));

        /*
         * serialized using createNsonFromValueBytes. This is what is used by
         * GAT. Must include format and region id in byte[]
         */
        NioByteOutputStream bos = new NioByteOutputStream(100, false);
        assertTrue(NsonUtil.createNsonFromValueBytes(localTable,
                                                     v.toByteArray(),
                                                     /* collapse MR counters */
                                                     false,
                                                     bos));
        /* counter should be the the value, not in map format */
        byte[] nsonValue = bos.array();
        nString = Nson.toJsonString(nsonValue);
        assertTrue(nString.contains("\"counter\":5"));

        /*
         * turn NSON into Value and make sure that the counter
         * map is created
         */
        v = NsonUtil.createValueFromNsonBytes(localTable,
                                              nsonValue,
                                              localRegionId,
                                              true);

        TableTestBase.createValueFromNson(localTable, nsonValue, "xyz");

        /* query based on counter value and make sure index is used */
        final String qry = "select * from foo where counter = 5";
        PreparedStatement ps = localKV.prepare(qry, null);
        /*
         * assert that the plan is an all partition (no index) query and returns
         * a single row
         */
        assertTrue(ps.toString().contains("ALL_PARTITIONS"));
        assertTrue(executeDml(localKV, qry).size() == 1);

        /* let the update replicate */
        wait(waitTime);

        getRow = remoteTableImpl.get(rpkey, null);
        assertTrue(((FieldValueImpl)getRow.get("counter")).getLong() == 5);

        /* put again, test update of non-counter fields */
        row.putJson("some_json", "{\"a\": 25}");
        localTableImpl.put(row, null, null);
        getRow = localTableImpl.get(lpkey, null);
        assertTrue(((FieldValueImpl)getRow.get("counter")).getLong() == 5);
        assertTrue(((FieldValueImpl)getRow.get("some_json")).
                   getMap().get("a").asInteger().get() == 25);

        /*
         * create table with MR counter but not in region, then try to update
         * the counter
         */
        executeDdl(localKV, "create table notmr(id integer, primary key(id), " +
                   "counter as integer mr_counter) as json collection");
        TableImpl notmrTable = (TableImpl) localTableImpl.getTable("notmr");

        /* put row in local */
        row = notmrTable.createRow();
        row.put("id", 1);
        try {
            localTableImpl.put(row, null, null);
            fail("Put without counter value should fail");
        } catch (IllegalArgumentException iae) {
            // success
        }
        row.put("counter", 1);
        localTableImpl.put(row, null, null);

        /* do a simple get */
        PrimaryKey pkey = notmrTable.createPrimaryKey();
        pkey.put("id", 1);
        row = localTableImpl.get(pkey, null);
        assertTrue(((FieldValueImpl)row.get("counter")).getLong() == 0);

        /* this should fail because it's not in a region or MR as yet */
        try {
            executeDml(localKV,
                       "update notmr $f set $f.counter = $ + 5 where " +
                       "id = 1");
            fail("Update counter when not MR should fail");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* this should fail because alter is not supported on json collection table */
        try {
            executeDdl(localKV,"alter table notmr (DROP mr_counter)");
            fail("Field does not exist");
        } catch(IllegalArgumentException iae){

        }

        /*
         * Test for bug where "returning" clause returns a counter
         * value provided with an insert instead of 0
         * KVSTORE-2039.
         * Not yet fixed
        List<RecordValue> res = executeDml(
            localKV,
            "insert into foo values(10, {\"counter\": 10}) returning *");
        for (RecordValue r : res) {
            System.out.println(r);
        }
         */
    }

    @Test
    public void testJsonCollectionMR() throws Exception {
        executeDdl(remoteKV, "create table bar(id integer, primary key(id), " +
                "counter1 as long mr_counter, counter2 as integer mr_counter)" +
                " in regions LOC, REM as json collection");
        executeDdl(localKV, "create table bar(id integer, primary key(id), " +
                "counter1 as long mr_counter, counter2 as integer mr_counter)" +
                " in regions REM, LOC as json collection");

        /* verify alteration fails for mr enabled json collection table. */
        try {
            executeDdl(localKV, "alter table bar (drop counter2)");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    /*
     * Use cases:
     * 1. restore. Needs to maintain mod time and handle resolution based on
     * mod time (region is not relevant). Note: table migration will be similar
     * to restore. If target is MR make sure modifications make it into
     * replication stream. Make sure that tombstones work (delete target or not
     * based on modification time).
     * 2. On-premises MR service. Similar to GAT receiver except that the
     * region comes from the row. Test key-only tables
     * 3. GAT receiver. Use an external region id. Ensure that resolve happens
     * as expected. Modifications should not get to replication stream. Note
     * that GAT tables are not "MR" from a KV perspective and that the region
     * ids are only used by the receiver when writing replicated events. In
     * other words, locally written rows are *not* Format.MULTI_REGION_TABLE
     * as is the case on-prem. They become that format only when written by
     * the GAT receiver.
     *
     * Use tombstones in all cases. Verify that they exist
     */
    @Test
    public void testUsePutResolve() throws Exception {
        final int rowCount = 1000;
        final String tableName = "bulkTable";
        final String mrTableName = "bulkMRtable";

        /*
         * 3 tables, same schema, one purely local (not MR) -- "bulkTable"
         * and 2 for MR -- "bulkMRtable. Local table has an MR counter so
         * that case 3 (GAT) can be tested
         */
        final String ddlNotMR = "create table " + tableName +
            "(id integer, name string, counter integer as mr_counter, " +
            " primary key(id))";
        final String ddlLocal = "create table " + mrTableName +
            "(id integer, name string, counter integer as mr_counter, " +
            " primary key(id)) in regions LOC, REM";
        final String ddlRemote = "create table " + mrTableName +
            "(id integer, name string, counter integer as mr_counter, " +
            " primary key(id)) in regions REM, LOC";

        executeDdl(localKV, ddlNotMR);
        executeDdl(remoteKV, ddlLocal);
        executeDdl(localKV, ddlRemote);
        Table notMRtable = localTableImpl.getTable(tableName);
        Table localTable = localTableImpl.getTable(mrTableName);
        Table remoteTable = remoteTableImpl.getTable(mrTableName);

        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setUsePutResolve(true);
        List<Row> rows = new ArrayList<Row>();

        /* save current time for later */
        long t0 = System.currentTimeMillis();

        /*
         * Case 1: restore or table migration. Use a local, not-replicated
         * table and verify that modification time works and conflicts are
         * resolved correctly
         */
        bulkLoadRows(localTableImpl, notMRtable, t0, rowCount, "name",
                     writeOptions);
        /*
         * BulkPut a couple of tombstones with higher modification time, verify
         * that the rows disappear (use of expected number does this). When
         * tableIterator is enhanced to include tombstones, use that feature
         */
        rows.clear();
        addTombstones(notMRtable, rows, 0, rowCount/2, t0 + 1, -1);
        runBulkPut(localTableImpl, rows, writeOptions);
        rows = getAllRows(notMRtable, localTableImpl, rowCount/2);
        assertEquals(rowCount/2, rows.size());

        /* try to use putResolve to put counter value as 4 */
        Row trow = notMRtable.createRow();
        trow.put("id", rowCount * 2);
        putMRCounter(trow, "counter", 4, 5);
        PrimaryKey tkey = notMRtable.createPrimaryKey();
        tkey.put("id", rowCount * 2);
        WriteOptions wo = new WriteOptions();
        /*
         * region id needs to be both in WriteOptions and the row to convince
         * putResolve to work
         */
        wo.setRegionId(5);
        ((RowImpl)trow).setRegionId(5);
        assertTrue(localTableImpl.putResolve(trow, null, wo));
        trow = localTableImpl.get(tkey, null);
        assertEquals(4, trow.get("counter").asInteger().get());
        localTableImpl.delete(tkey, null, null);


        /*
         * Case 2: onprem MR tables
         *
         * Use the MR tables, load "local" using normal bulk put
         */
        writeOptions.setUsePutResolve(false);
        bulkLoadRows(localTableImpl, localTable, 0, rowCount, "name",
                     writeOptions);

        /* update MR counter */
        for (int i = 0; i < rowCount; i++) {
            executeDml(localKV,
                       ("update " + mrTableName +
                        " $t set $t.counter = $ + 5 where $t.id = " + i));
        }

        /* write some tombstones locally */
        t0 = System.currentTimeMillis() + 1;
        writeOptions.setUsePutResolve(true);
        rows.clear();
        addTombstones(localTable, rows, 0, rowCount/2, t0, localRegionId);
        runBulkPut(localTableImpl, rows, writeOptions);
        /* verify that the tombstones caused deletion locally */
        rows = getAllRows(localTable, localTableImpl, rowCount/2);
        assertEquals(rowCount/2, rows.size());

        /* replicate and check remote region */
        wait(waitTime);

        rows = getAllRows(remoteTable, remoteTableImpl, rowCount/2);
        assertEquals(rowCount/2, rows.size());

        /*
         * Case 3: cloud GAT receiver. In this case the receiver is using
         * rows from intermediate storage and applying a region id to them.
         * Use the table that is local only to test this because in the cloud
         * GAT tables "look" normal to kv itself.
         */

        /*
         * the table has rowCount/2 rows in it, along with the same number of
         * tombstones (TBD, validate tombstones when possible)
         */
        /* fake region ids */
        final int gatRegionId1 = 10;
        final int gatRegionId2 = 11;
        final int counterVal1 = 7;
        final int counterVal2 = 4;
        t0 = System.currentTimeMillis();
        writeOptions.setRegionId(gatRegionId1);
        rows.clear();
        rows = getAllRows(notMRtable, localTableImpl, rowCount/2);
        for (Row row : rows) {
            putMRCounter(row, "counter", counterVal1, gatRegionId1);
            ((RowImpl)row).setModificationTime(t0);
        }
        runBulkPut(localTableImpl, rows, writeOptions);
        rows = getAllRows(notMRtable, localTableImpl, rowCount/2);
        assertEquals(rowCount/2, rows.size());
        /* assert counter was updated */
        for (Row row : rows) {
            assertEquals(counterVal1, row.get("counter").asInteger().get());
            /* set up to merge new counters from different region */
            putMRCounter(row, "counter", counterVal2, gatRegionId2);
            /* back up mod time to before t0 */
            ((RowImpl)row).setModificationTime(t0 -1);
            /* reset "name" to verify that the new rows "lose" resolution */
            row.put("name", "foo");
        }
        writeOptions.setRegionId(gatRegionId2);
        runBulkPut(localTableImpl, rows, writeOptions);
        rows = getAllRows(notMRtable, localTableImpl, rowCount/2);
        assertEquals(rowCount/2, rows.size());
        /* assert counter was updated */
        for (Row row : rows) {
            assertEquals(counterVal1+counterVal2,
                         row.get("counter").asInteger().get());
            /* name should not have been updated */
            assertNotEquals("foo", row.get("name").asString().get());
        }

        /* try to write some tombstones that lose the race, so a no-op */
        rows.clear();
        addTombstones(notMRtable, rows, 0, rowCount, t0-1, gatRegionId2);
        runBulkPut(localTableImpl, rows, writeOptions);
        rows = getAllRows(notMRtable, localTableImpl, rowCount/2);
        assertEquals(rowCount/2, rows.size());
        /* assert nothing was changed */
        for (Row row : rows) {
            assertEquals(counterVal1+counterVal2,
                         row.get("counter").asInteger().get());
        }
    }

    /**
     * 0. Use artificial region id to handle counters
     * 1. Create a table with MR counter, populated it and incr the counter.
     * 2. Convert the rows to NsonRow with modified value to check
     * 3. bulk load the NsonRows along with the flag to use the region id to
     * create a serialized value that includes the counter
     * 4. read the new rows -- verify that the counter is correct
     *
     * There are 2 scenarios (MR and restore) for each of 3 MR counter
     * types (schema, JSON, JSON collection). In the MR case the counters
     * are replicated in their map form (map of region id to value). In the
     * restore case MR counters are single-valued fields of the type defined,
     * without any region information. The latter is because NSON-based
     * backups are not region specific and do not contain region information.
     *
     * Use all 3 types of counters (schema, JSON, JSON collection)
     *  for each create 2 bulk put scenarios:
     *   o create the NSON with map value of counter (MR case)
     *   o create the NSON with single value, provide region id on bulk put
     *  validate values at the end (restore case)
     */
    @Test
    public void testNsonMRCounter() throws Exception {
        final int numRows = 20;
        final String tableName = "nsonMRCounter";
        final String jcollTableName = "jcollNsonMRCounter";
        final String ddl = "create table " + tableName +
            "(id integer, name string, counter integer as mr_counter, " +
            "json json(jcount as integer mr_counter), " +
            " primary key(id))";
        final String jcollDdl = "create table " + jcollTableName +
            "(id integer, jcollcount as long mr_counter, " +
            " primary key(id)) as json collection";

        /* true is MR case, false is restore case */
        Boolean counterAsMapValues[] = new Boolean[]{true, false};

        for (boolean counterAsMap : counterAsMapValues) {
            executeDdl(localKV, ddl);
            executeDdl(localKV, jcollDdl);
            TableImpl table = (TableImpl) localTableImpl.getTable(tableName);
            TableImpl jcollTable =
                (TableImpl) localTableImpl.getTable(jcollTableName);

            /*
             * Insert numRows rows into both tables, all 3 counters
             */
            final int regionId = 5;
            for (int i = 0; i < numRows; i++) {
                Row row = table.createRow();
                Row jcrow = jcollTable.createRow();
                row.put("id", i);
                row.put("name", ("name" + i));
                /* JSON MR counters need path to exist on put */
                row.putJson("json", "{\"jcount\": null}");
                assertNotNull(localTableImpl.put(row, null, null));

                /* JSON collection table */
                jcrow.put("id", i);
                jcrow.put("name", ("name" + i));
                /* JSON collection counters need path to exist on put */
                jcrow.put("jcollcount", 0L);
                assertNotNull(localTableImpl.put(jcrow, null, null));

                ExecuteOptions options =
                    new ExecuteOptions().setRegionId(regionId);
                executeDml(localKV,
                           ("update " + tableName +
                            " $t set $t.counter= $ + 5 where $t.id=" + i), options);
                executeDml(localKV,
                           ("update " + tableName +
                            " $t set $t.json.jcount= $ + 7 where $t.id=" + i),
                           options);
                executeDml(localKV,
                           ("update " + jcollTableName +
                            " $t set $t.jcollcount= $ + 9 where $t.id=" + i), options);
            }

            /*
             * This first section does the schema-based table with its
             * 2 MR counters (schema and JSON)
             */
            List<NsonRow> nsonRows = new ArrayList<NsonRow>();
            List<Row> rows = getAllRows(table, localTableImpl, numRows);
            int i = 0;
            final String counterMap = "\"counter\":{\"5\":5}";
            final String jcountMap = "\"jcount\":{\"5\":7}";
            final String counterVal= "\"counter\":5";
            final String jcountVal = "\"jcount\":7";
            for (Row r : rows) {
                /* leave counter intact but modify id and name */
                r.put("id", i + numRows);
                r.put("name", ("name" + (i + numRows)));


                NsonRow nr = createNsonRow((RowImpl)r, regionId,
                                           table, counterAsMap);
                String jsonVal = Nson.toJsonString(nr.getNsonValue());
                /*
                 * assert that the JSON representation contains expected string
                 */
                if (counterAsMap) {
                    assertTrue(jsonVal.contains(counterMap));
                    assertTrue(jsonVal.contains(jcountMap));
                } else {
                    assertTrue(jsonVal.contains(counterVal));
                    assertTrue(jsonVal.contains(jcountVal));
                }

                /* write counters as map or not? */
                nsonRows.add(nr);
                i++;
            }
            final BulkWriteOptions writeOptions =
                new BulkWriteOptions(null, 0, null);
            writeOptions.setUsePutResolve(true);
            bulkLoadNsonRows(localTableImpl, writeOptions,
                             nsonRows, counterAsMap);
            rows = getAllRows(table, localTableImpl, numRows * 2);
            for (Row r : rows) {
                int id = r.get("id").asInteger().get();
                assertEquals(("name" + id), r.get("name").asString().get());
                assertEquals(5, r.get("counter").asInteger().get());
                assertEquals(7,
                             r.get("json").asMap().get("jcount").asInteger().get());
            }

            /*
             * Second part -- JSON collection table. This is a lot of cut/paste
             * from above
             */
            nsonRows = new ArrayList<NsonRow>();
            rows = getAllRows(jcollTable, localTableImpl, numRows);
            i = 0;
            final String jcollCounterMap = "\"jcollcount\":{\"5\":9}";
            final String jcollCounterVal = "\"jcollcount\":9";
            for (Row r : rows) {
                /* leave counter intact but modify id and name */
                r.put("id", i + numRows);
                r.put("name", ("name" + (i + numRows)));


                NsonRow nr = createNsonRow((RowImpl)r, regionId,
                                           jcollTable, counterAsMap);
                String jsonVal = Nson.toJsonString(nr.getNsonValue());
                /*
                 * assert that the JSON representation contains expected string
                 */
                if (counterAsMap) {
                    assertTrue(jsonVal.contains(jcollCounterMap));
                } else {
                    assertTrue(jsonVal.contains(jcollCounterVal));
                }
                /* write counters as map or not? */
                nsonRows.add(nr);
                i++;
            }
            bulkLoadNsonRows(localTableImpl, writeOptions,
                             nsonRows, counterAsMap);
            rows = getAllRows(jcollTable, localTableImpl, numRows * 2);
            for (Row r : rows) {
                int id = r.get("id").asInteger().get();
                assertEquals(("name" + id), r.get("name").asString().get());
                assertEquals(9L, r.get("jcollcount").asLong().get());
            }

            tearDown();
        }
    }

    /*
     * Test Bulk Load of NsonRows created from a JSON collection table
     */
    @Test
    public void testNsonJsonCollection() throws Exception {
        final int numRows = 20;
        final String tableName = "jsonCollBulk";
        final String ddl = "create table " + tableName +
            "(id integer, primary key(id)) as json collection";

        executeDdl(localKV, ddl);
        TableImpl table = (TableImpl) localTableImpl.getTable(tableName);
        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("name", ("name" + i));
            row.put("age", i);
            assertNotNull(localTableImpl.put(row, null, null));
        }
        List<NsonRow> nsonRows = new ArrayList<NsonRow>();
        List<Row> rows = getAllRows(table, localTableImpl, numRows);
        /* update name, age, leave id intact */
        for (Row r : rows) {
            int id = r.get("id").asInteger().get();
            r.put("name", ("name" + (id + numRows)));
            r.put("age", id + numRows);
            nsonRows.add(createNsonRow((RowImpl)r, 0, table));
        }
        final BulkWriteOptions writeOptions =
            new BulkWriteOptions(null, 0, null);
        writeOptions.setUsePutResolve(true); /* not really used here */

        bulkLoadNsonRows(localTableImpl, writeOptions, nsonRows, false);
        rows = getAllRows(table, localTableImpl, numRows);
        for (Row r : rows) {
            int id = r.get("id").asInteger().get();
            assertEquals(r.get("age").asInteger().get(), (id + numRows));
        }
    }

    private static NsonRow createNsonRow(RowImpl row, int regionId,
                                         TableImpl table) {
        return createNsonRow(row, regionId, table, false);
    }

    private static NsonRow createNsonRow(RowImpl row, int regionId,
                                         TableImpl table,
                                         boolean writeMRCountersAsMap) {
        NsonTest nsonTest =
            new NsonTest().setWriteMRCountersAsMap(writeMRCountersAsMap);
        PrimaryKeyImpl pkey = (PrimaryKeyImpl)row.createPrimaryKey();
        return new NsonRow(row.getLastModificationTime(),
                           row.getExpirationTime(),
                           regionId,
                           nsonTest.createNsonFromValue(pkey),
                           nsonTest.createNsonFromRow(row),
                           false, table);


    }

    /*
     * There are internal ways to convince the system to serialize an MR
     * counter. These are used internally by xregion and query code
     *
     * Issue: the same does not easily apply to JSON mr counters
     */
    private static void putMRCounter(RecordValue row,
                                     String counterName,
                                     int value,
                                     int regionId) {
        int pos = row.getFieldPos(counterName);
        IntegerDefImpl def =
            (IntegerDefImpl)row.getDefinition().getFieldDef(pos);
        FieldValueImpl counterVal = def.createCRDTValue();
        FieldValueImpl val = def.createInteger(value);
        counterVal.incrementMRCounter(val, regionId);
        ((RecordValueImpl)row).putInternal(counterName, counterVal, false);
    }

    private void bulkLoadRows(TableAPIImpl tapi, Table table, long modTime,
                              int rowCount, String namePrefix,
                              BulkWriteOptions writeOptions) {

        List<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < rowCount; i++) {
            Row row = table.createRow();
            row.put("id", i).put("name", (namePrefix+i));
            ((RowImpl)row).setModificationTime(modTime);
            rows.add(row);
        }

        runBulkPut(tapi, rows, writeOptions);
        rows = getAllRows(table, tapi, rowCount);
        /* if modification is specified, validate it */
        if (modTime != 0L) {
            for (Row row :rows) {
                assertEquals(modTime, row.getLastModificationTime());
            }
        }
    }

    /*
     * Return all rows in a single list for easy use
     */
    private static List<Row> getAllRows(Table table, TableAPIImpl tapi,
                                        int expected) {
        ArrayList<Row> rows = new ArrayList<Row>();
        int loopCount = 0;
        while (rows.size() < expected) {
            rows.clear();
            PrimaryKey key = table.createPrimaryKey();
            TableIterator<Row> iter = tapi.tableIterator(key, null, null);
            while (iter.hasNext()) {
                rows.add(iter.next());
            }
            if (rows.size() >= expected) {
                break;
            }
            if (++loopCount > 10) {
                /* 10 times (200s) is excessive */
                fail("Looped 10 times in getAllRows, expected " +
                     expected + " rows, got this time: " + rows.size());
            }
            wait(2);
        }
        return rows;
    }

    private static void addTombstones(Table table, List<Row> rows,
                                      int startIndex, int num,
                                      long modTime, int regionId) {
        for (int i = startIndex; i < (startIndex + num); i++) {
            PrimaryKeyImpl key = (PrimaryKeyImpl) table.createPrimaryKey();
            key.put("id", i);
            key.setIsTombstone(true);
            key.setModificationTime(modTime);
            if (regionId > 0) {
                key.setRegionId(regionId);
            }
            rows.add(key);
        }
    }

    private static CreateStore createStore(String storeName,
                                           int port) throws Exception {

        CreateStore cs = new CreateStore(storeName,
                                         port,
                                         1, /* n SNs */
                                         1, /* rf */
                                         10, /* n partitions */
                                         1, /* capacity per SN */
                                         CreateStore.MB_PER_SN,
                                         false, /* use threads is false */
                                         null);
        /* setPolicies(localStore); */
        final File rootDir = new File(cs.getRootDir(), storeName);
        rootDir.mkdir();
        cs.setRootDir(rootDir.toString());
        cs.start();
        return cs;
    }

    private static KVStoreConfig createKVConfig(CreateStore cs) {
        final KVStoreConfig config = cs.createKVConfig();
        return config;
    }

    private static void executeDdl(KVStore store, String statement)
        throws Exception {
        /* allow CRDT on non-MR tables */
        ExecuteOptions options = new ExecuteOptions().setAllowCRDT(true);
        final StatementResult res = store.executeSync(statement, options);
        assertTrue(res.isSuccessful());
    }

    private static List<RecordValue> executeDml(KVStore store, String statement)
        throws Exception {
        return executeDml(store, statement, null);
    }

    private static List<RecordValue> executeDml(KVStore store,
                                                String statement,
                                                ExecuteOptions options)
        throws Exception {

        List<RecordValue> list = new ArrayList<RecordValue>();
        final StatementResult res = store.executeSync(statement, options);
        for (RecordValue val : res) {
            list.add(val);
        }
        return list;
    }

    private static JsonConfig createXRConfig(String testPath,
                                             String regionName,
                                             String storeName,
                                             HashSet<String> helpers) {
        JsonConfig cfg =
            new JsonConfig(testPath, 1, 0, regionName, storeName, helpers);
        cfg.setStatIntervalSecs(1);
        cfg.setRequestTablePollIntvSecs(1);
        return cfg;
    }

    private static XRegionService createXRService(JsonConfig conf,
                                                  Logger logger) {
        final XRegionService service = new XRegionService(conf, logger);
        return service;
    }

    private static void verbose(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    private static void wait(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (Exception e) {
            /* ignore */
        }
    }

    private static void waitForQueue() {
        while (remoteService.getReqQueue().size() > 0) {
            wait(2);
        }
    }

    private static void runBulkPut(TableAPIImpl tapi,
                                   List<Row> rows,
                                   final BulkWriteOptions writeOptions) {
        TestStream stream = new TestStream(rows);
        runBulkPut(tapi, writeOptions, stream);
    }

    private static void runBulkPut(TableAPIImpl tapi,
                                   final BulkWriteOptions writeOptions,
                                   TestStream... streams) {
        final List<EntryStream<Row>> list;
        if (streams.length == 1) {
            list = Collections.singletonList((EntryStream<Row>)streams[0]);
        } else {
            list = new ArrayList<EntryStream<Row>>();
            for (TestStream stream : streams) {
                list.add(stream);
            }
        }
        try {
            tapi.put(list, writeOptions);
        } catch (RuntimeException re) {
            fail("Failed to execute putBulk operation: " + re.getMessage());
        }
    }

    private static class TestStream implements EntryStream<Row> {
        private final Iterator<Row> iterator;

        TestStream(List<Row> rows) {
            iterator = rows.iterator();
        }

        @Override
        public String name() {
            return "TestMRRowStream";
        }

        @Override
        public Row getNext() {

            if (iterator != null) {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
            }
            return null;
        }

        @Override
        public void completed() {
        }

        @Override
        public void keyExists(Row entry) {
            throw new RuntimeException("keyExists call not expected");
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   Row entry) {
            throw runtimeException;
        }
    }

    /*
     * Simple EntryStream for NsonRow
     */
    private static class TestNsonStream implements EntryStream<NsonRow> {
        private final Iterator<NsonRow> iterator;

        TestNsonStream(List<NsonRow> rows) {
            iterator = rows.iterator();
        }

        @Override
        public String name() {
            return "TestMRNsonRowStream";
        }

        @Override
        public NsonRow getNext() {

            if (iterator != null) {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
            }
            return null;
        }

        @Override
        public void completed() {
        }

        @Override
        public void keyExists(NsonRow entry) {
            throw new RuntimeException("keyExists call not expected");
        }

        @Override
        public void catchException(RuntimeException runtimeException,
                                   NsonRow entry) {
            throw runtimeException;
        }
    }

    /*
     * Wraps TableAPIImpl.putNson(EntryStream...) where there's only a
     * single List<NsonRow>
     */
    private void bulkLoadNsonRows(TableAPIImpl tapi,
                                  BulkWriteOptions writeOptions,
                                  List<NsonRow> rows,
                                  boolean mrCountersAreMaps) {
        final List<EntryStream<NsonRow>> list =
            Collections.singletonList((EntryStream<NsonRow>)
                                      new TestNsonStream(rows));
        tapi.putNson(list, writeOptions, mrCountersAreMaps);
    }
}
