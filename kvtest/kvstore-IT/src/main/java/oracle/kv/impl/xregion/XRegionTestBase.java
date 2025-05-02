/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.xregion;


import static oracle.kv.impl.systables.MRTableInitCkptDesc.COL_NAME_CHECKPOINT;
import static oracle.kv.impl.systables.StreamServiceTableDesc.COL_REQUEST_ID;
import static oracle.kv.impl.xregion.service.ServiceMDMan.RL_LOG_PERIOD_MS;
import static oracle.kv.impl.xregion.service.ServiceMDMan.RL_MAX_NUM_OBJECTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.Version;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RegionMapper;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.streamservice.MRT.Manager;
import oracle.kv.impl.streamservice.MRT.Request;
import oracle.kv.impl.streamservice.MRT.Response;
import oracle.kv.impl.streamservice.ServiceMessage;
import oracle.kv.impl.systables.MRTableInitCkptDesc;
import oracle.kv.impl.systables.StreamRequestDesc;
import oracle.kv.impl.systables.StreamResponseDesc;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.Pair;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.xregion.agent.BaseTableTransferThread;
import oracle.kv.impl.xregion.agent.RegionAgentStatus;
import oracle.kv.impl.xregion.agent.RegionAgentThread;
import oracle.kv.impl.xregion.init.TableInitCheckpoint;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.MRTableMetrics;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ReqRespManager;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.service.XRegionRequest;
import oracle.kv.impl.xregion.service.XRegionService;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.PubSubTestBase;
import oracle.kv.stats.ServiceAgentMetrics;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.json.JsonUtils;

import com.sleepycat.je.utilint.StoppableThread;

import org.junit.BeforeClass;

/**
 * Base test case of all stream manager test.
 */
public abstract class XRegionTestBase extends PubSubTestBase {

    protected static final RegionInfo JFK =
        new RegionInfo("JFK", "jfk-store",
                       new String[]{"jfk1.us.oracle.com:5001",
                           "jfk2.us.oracle.com:5001"});
    protected static final RegionInfo SFO =
        new RegionInfo("SFO", "sfo-store",
                       new String[]{"sfo1.us.oracle.com:5001",
                           "sfo2.us.oracle.com:5001"});
    protected static final RegionInfo IAD =
        new RegionInfo("IAD", "iad-store",
                       new String[]{"iad1.us.oracle.com:5001"});
    protected static final RegionInfo BOS =
        new RegionInfo("BOS", "bos-store",
                       new String[]{"bos1.us.oracle.com:5001"});

    protected static final String TESTPATH = TestUtils.getTestDir().getPath();
    protected static int POLL_INTERVAL_MS = 1000;
    protected static int POLL_TIMEOUT_MS = 60 * 1000;

    /* # of init rows in table */
    protected static int NUM_INIT_ROWS = 1024;

    /* description col length */
    protected static final int DESC_COL_LENGTH = 64;

    /* parameter for a second store */
    protected static final int BACKUP_START_PORT = 8000;
    protected static final int HA_RANGE = 5;
    /* default group size */
    private static final int DEFAULT_AGENT_GROUP_SIZE = 1;

    protected static String remoteStorePath = "remotekv";
    protected static String remoteStoreName = "remoteKV";
    protected KVStoreImpl remoteStore = null;
    protected TableAPI remoteTableAPI = null;
    protected CreateStore remoteCreateStore = null;
    protected KVStoreConfig remoteKVSConfig = null;

    /* map from store -> set of MRT created in that store */
    protected final Map<String, Set<String>> mrtTableByStore = new HashMap<>();

    /*---------------------*/
    /* MRT and PITR tables */
    /*---------------------*/
    private static final String NS1 = "TestNameSpace1";
    private static final String NS2 = "TestNameSpace2";
    protected static final String MRT_1 =
        NameUtils.makeQualifiedName(NS1, "MRT1");
    protected static final String MRT_2 =
        NameUtils.makeQualifiedName(NS2, "MRT2");
    protected static final String PITR_1 =
        NameUtils.makeQualifiedName(NS1, "PITR1");
    /** table to advance table id */
    protected static final String RANDOM_TABLE = "MyFoo";
    /*---------------------*/
    /* Schema Evolution    */
    /*---------------------*/
    /* new col to add in schema evolution */
    protected static final String NEW_COL_NAME = "notes";
    /* new col default value */
    protected static final String NEW_COL_DEFAULT = "default_notes";
    private static final String NEW_COL_DEFAULT_QUOTE =
        "\"" + NEW_COL_DEFAULT + "\"";

    /* # of agent in group */
    private static final int GROUP_SIZE = 1;
    /* agent index in group */
    private static final int MEMBER_ID = 0;
    /* test nosql subscriber id */
    protected static final NoSQLSubscriberId DEFAULT_TEST_AGENT_ID =
        new NoSQLSubscriberId(GROUP_SIZE, MEMBER_ID);

    /* source and target store */
    protected RegionInfo srcRegion;
    protected RegionInfo tgtRegion;
    protected KVStoreImpl srcStore;
    protected KVStoreImpl tgtStore;

    /* override default stat collect interval */
    protected static DurationParameter STAT_INTV =
        new DurationParameter("StatReportInterval", TimeUnit.SECONDS, 5);

    protected RateLimitingLogger<String> rlLogger;

    @BeforeClass
    public static void setUpStatic() {

        /*
         * The multi-region agent calls putResolveAsync, so anything involving
         * multi-region tables requires the server to support async.
         */
        assumeTrue("All cross-region tests require async",
                   AsyncControl.serverUseAsync);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestBase.setTestStatusActive();
        /* override default */
        kvstoreName = "mystore";
        repFactor = 1;
        numStorageNodes = 1;
        numPartitions = 3;
        kvstoreName = "localKV";
        logger.setLevel(Level.FINE);
        trace("Set logging level=" + logger.getLevel());
    }

    @Override
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }

        if (createStore != null) {
            createStore.shutdown();
        }

        if (remoteStore != null) {
            remoteStore.close();
        }

        if (remoteCreateStore != null) {
            remoteCreateStore.shutdown();
        }
        super.tearDown();
    }

    protected JsonConfig createJsonConfig() {
        return createJsonConfig(Math.max(1, NUM_INIT_ROWS / 10), 0, 0, 0);
    }

    protected JsonConfig createJsonConfig(NoSQLSubscriberId sid) {
        return createJsonConfig(sid.getTotal(), sid.getIndex(),
                                Math.max(1, NUM_INIT_ROWS / 10), 0, 0, 0);
    }

    /**
     * Creates JSON config with parameters
     *
     * @param reportIntvSecs   table init report interval, 0 if use default
     * @param statCollIntvSecs stats collection interval, 0 if use default
     * @param ckptIntvOps      count-based checkpoint interval, 0 if use default
     * @param ckptIntvSecs     time-based checkpoint interval, 0 if use default
     */
    protected JsonConfig createJsonConfig(int reportIntvSecs,
                                          int statCollIntvSecs,
                                          int ckptIntvOps,
                                          int ckptIntvSecs) {
        return createJsonConfig(GROUP_SIZE, MEMBER_ID, reportIntvSecs,
                                statCollIntvSecs, ckptIntvOps, ckptIntvSecs);
    }

    protected JsonConfig createJsonConfig(int groupSize,
                                          int member_id) {
        return createJsonConfig(groupSize, member_id, 0, 0, 0, 0);
    }

    private JsonConfig createJsonConfig(int groupSize,
                                        int member_id,
                                        int reportIntvSecs,
                                        int statCollIntvSecs,
                                        int ckptIntvOps,
                                        int ckptIntvSecs) {
        final JsonConfig conf = new JsonConfig(TESTPATH,
                                               groupSize,
                                               member_id,
                                               tgtRegion.getName(),
                                               tgtRegion.getStore(),
                                               new HashSet<>(Arrays.asList(
                                                   tgtRegion
                                                       .getHelpers())));
        conf.addRegion(srcRegion);
        if (reportIntvSecs > 0) {
            conf.setTableReportIntv(reportIntvSecs);
        }
        if (statCollIntvSecs > 0) {
            conf.setStatIntervalSecs(statCollIntvSecs);
        }
        if (ckptIntvOps > 0) {
            conf.setCheckpointIntvOps(ckptIntvOps);
        }
        if (ckptIntvSecs > 0) {
            conf.setCheckpointIntvSecs(ckptIntvSecs);
        }
        final String fileName = "config.json";
        JsonConfig.writeBootstrapFile(conf, TESTPATH, fileName);
        trace("Json config file " + fileName + " created in " + TESTPATH);
        return conf;
    }

    protected void setupLocalStore() throws Exception {
        setupLocalStore(false);
    }

    protected void setupLocalStore(boolean useThread) throws Exception {
        startStore(useThread);
        kvStoreConfig = createKVConfig(createStore);
        store = KVStoreFactory.getStore(kvStoreConfig);
        tgtStore = (KVStoreImpl) store;
        createDefaultTestNameSpace();
        createTestNameSpaces(store);
        tableAPI = store.getTableAPI();
        rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS,
                                            RL_MAX_NUM_OBJECTS,
                                            logger);
        tgtRegion =
            new RegionInfo(LOCAL_REGION,
                           ((KVStoreImpl) store).getTopology().getKVStoreName(),
                           new String[]{
                               createStore.getHostname() + ":" +
                               createStore.getRegistryPort()});
        setLocalRegionName(store, LOCAL_REGION, logger, false);

        /*
         * create a random table to advance table id in local store such that
         * any MR table in local and remote stores would have different ids
         */
        final String ddl = "CREATE TABLE IF NOT EXISTS " + RANDOM_TABLE +
                           " (id Integer, primary key (id))";
        trace("DDL=" + ddl);
        final StatementResult stmt = store.executeSync(ddl);
        assertNotNull(stmt);
        assertTrue(stmt.isSuccessful());
        final Table tb = store.getTableAPI().getTable(RANDOM_TABLE);
        assertNotNull(tb);
        trace("Random table created in local store" +
              ", tb=" + tb.getFullNamespaceName() +
              ", id=" + ((TableImpl) tb).getId());
    }

    protected void setupRemoteStore() throws Exception {
        setupRemoteStore(false);
    }

    protected void setupRemoteStore(boolean useThread) throws Exception {
        startRemoteStore(useThread);
        trace("Remote store created " +
              remoteStore.getTopology().getKVStoreName());
        srcRegion = new RegionInfo(REMOTE_REGION,
                                   remoteStore.getTopology().getKVStoreName(),
                                   new String[]{
                                       remoteCreateStore.getHostname() + ":" +
                                       remoteCreateStore.getRegistryPort()});
        srcStore = remoteStore;
        createTestNameSpaces(remoteStore);
    }

    protected void setSourceRegionWithoutCreatingstore() {
        final int backupPort =
            new PortFinder(BACKUP_START_PORT, HA_RANGE).getRegistryPort();
        srcRegion = new RegionInfo(REMOTE_REGION, remoteStoreName,
                                   new String[]{"localhost:" + backupPort});
    }

    /**
     * Start backup store and open store.
     */
    private void startRemoteStore(boolean useThread)
        throws Exception {

        final File file = new File(TESTPATH, remoteStorePath);
        FileUtils.deleteDirectory(file);
        if (file.mkdirs()) {
            trace("created dir for remote " + file.getPath());
        } else {
            fail("Cannot create dir " + file.getPath());
        }
        final int backupPort =
            new PortFinder(BACKUP_START_PORT, HA_RANGE).getRegistryPort();
        remoteCreateStore = createBackupStore(file, backupPort, false,
                                              useThread);
        assertNotNull(remoteCreateStore);
        remoteStore = (KVStoreImpl) KVStoreFactory.getStore(
            createKVConfig(remoteCreateStore));
        assertNotNull(remoteStore);
        setLocalRegionName(remoteStore, REMOTE_REGION, logger, false);
    }

    protected void stopService(XRegionService service) {
        final XRegionRequest req = XRegionRequest.getShutdownReq();
        try {
            service.getReqRespMan().submit(req);
        } catch (InterruptedException ie) {
            fail("Interrupted in submitting request");
        }
        trace("Request shutdown");
        /* wait for response */
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return !service.isAlive();
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
        trace("Service thread is no longer alive");
    }

    /*
     * Create test table, start a mock service to allow the MRT DDL to finish
     * shut down the mock service after the table is created.
     */
    protected void createTestTableMockService(KVStoreImpl kvstore,
                                              String tableName,
                                              int numRows,
                                              String... regions) {
        createTestTableMockService(kvstore, tableName, numRows, null, regions);
    }

    protected void createTestTableMockService(KVStoreImpl kvstore,
                                              String tableName,
                                              int numRows,
                                              TimeToLive ttl,
                                              String... regions) {
        createTestTableMockService(kvstore, tableName, numRows, ttl,
                                   Type.STRING, false, regions);
    }

    protected void createTestTableMockService(KVStoreImpl kvstore,
                                              String tableName,
                                              int numRows,
                                              TimeToLive ttl,
                                              Type valueType,
                                              boolean createCRDT,
                                              String... regions) {
        String ddl = getCreateTableDDL(valueType, createCRDT, tableName);
        createTestTableMockService(kvstore, tableName, ddl, ttl,
                                   numRows, regions);

    }

    protected void createTestTableMockService(KVStoreImpl kvstore,
                                              String tableName,
                                              String ddl,
                                              TimeToLive ttl,
                                              int numRows,
                                              String... regions) {
        createTestTableMockService(kvstore,
                                   Collections.singletonList(tableName),
                                   Collections.singletonList(ddl),
                                   ttl, numRows, regions);
    }

    protected void createTestTableMockService(KVStoreImpl kvstore,
                                              List<String> tableNames,
                                              List<String> ddls,
                                              TimeToLive ttl,
                                              int numRows,
                                              String... regions) {
        /* start mock service so DDL can finish */
        reqRespThread = new ReqRespThread(kvstore,
                                          logger,
                                          traceOnScreen);
        reqRespThread.start();
        initTestTable(kvstore, tableNames, numRows, ttl, ddls, regions);
        reqRespThread.stopResResp();

        for (String tableName : tableNames) {
            final Table tbl = kvstore.getTableAPI().getTable(tableName);
            assertNotNull(tbl);
            final String storeName = kvstore.getTopology().getKVStoreName();
            mrtTableByStore.computeIfAbsent(storeName, u -> new HashSet<>())
                           .add(tbl.getFullNamespaceName());
            trace("Table " + tableName + " created at " +
                  kvstore.getTopology().getKVStoreName() +
                  " with # rows " + numRows +
                  ", regions: " + Arrays.toString(regions) +
                  ", ttl=" + ttl +
                  ", MRT " +
                  ((TableImpl) kvstore.getTableAPI().getTable(tableName))
                      .isMultiRegion());
        }
    }

    protected void startService(final XRegionService service,
                                final String tblToInit,
                                final RegionInfo srcRg,
                                final RegionInfo tgtRg,
                                final KVStoreImpl tgtSt) {
        startService(service, tblToInit, srcRg, tgtRg, tgtSt, NUM_INIT_ROWS);

    }

    protected void startService(final XRegionService service,
                                final String tblToInit,
                                final RegionInfo srcRg,
                                final RegionInfo tgtRg,
                                final KVStoreImpl tgtSt,
                                final long numInitRows) {
        final String tableName =
            NameUtils.getFullNameFromQualifiedName(tblToInit);
        final boolean testingMRT =
            (tblToInit != null) && tableName.startsWith("MRT");
        service.start();
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return service.getReqRespMan().isRunning();
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting for request generation thread");
        }
        trace("Request response thread is running");

        if (tblToInit == null) {
            /* no MRT to initialize */
            return;
        }

        if (numInitRows == 0) {
            /* nothing to stream, just return */
            return;
        }

        final ServiceAgentMetrics metrics = service.getAgentMetrics();
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    /* all init rows streamed from source */
                    final ServiceAgentMetrics m = service.getAgentMetrics();
                    trace("Received # ops: " + m.getRecvOps() +
                          ", expected=" + numInitRows);
                    return m.getRecvOps() == numInitRows;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting for " + MRT_1 + " initialized");
        }
        trace("Agent received #ops=" + metrics.getRecvOps() +
              ", expected=" + numInitRows);
        assertEquals(numInitRows, metrics.getRecvOps());
        if (testingMRT) {
            /*
             * due to concurrent table transfer, it is not necessarily true
             * that all streamed ops will win, thus we dump the # of wining
             * ops and # winning puts
             */
            trace("Out of total ops=" + numInitRows +
                  ", # of winning ops=" + metrics.getNumWins() +
                  ", # of winning puts=" + metrics.getWinPuts());
        }
        final RegionAgentThread agent;
        if (testingMRT) {
            agent = service.getRegionAgent(srcRg.getName());
        } else {
            /* PITR stream from tgt */
            agent = service.getRegionAgent(tgtRg.getName());
        }
        /* wait for agent state switched to streaming before verification */
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    trace("agent status=" + agent.getStatus());
                    return RegionAgentStatus.STREAMING.equals(
                        agent.getStatus());
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
        /* verify */
        final Set<String> tbls = agent.getTables();
        assertEquals(1, tbls.size());
        assertTrue(tbls.contains(tblToInit));
        assertFalse(agent.isCanceled());
        assertEquals(RegionAgentStatus.STREAMING, agent.getStatus());

        /* tgt region should have all rows from source */
        if (testingMRT) {
            final int count = countRows(tgtSt, tblToInit);
            assertEquals(numInitRows, count);
            trace("tgt table " + tblToInit + " has # rows " + count);
        }

        if (tableName.startsWith("PITR")) {
            assertEquals(1, service.getAllAgents().size());
            final RegionAgentThread ra =
                service.getRegionAgent(tgtRg.getName());
            assertNotNull(ra);
            assertTrue(ra.isAlive());
            assertFalse(ra.isShutdownRequested());
        }
    }

    protected int countRows(KVStoreImpl kvs, String tableName) {
        final Table table = kvs.getTableAPI().getTable(tableName);
        final TableIterator<Row> iter = kvs.getTableAPI().tableIterator(
            table.createPrimaryKey(), null, null);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        trace("Table=" + tableName + " has #rows=" + count +
              " in store=" + kvs.getTopology().getKVStoreName());
        return count;
    }

    /* create service with cumulative stats used in some unit tests */
    protected XRegionService createXRegionService(JsonConfig conf,
                                                  RegionInfo srcRg,
                                                  RegionInfo tgtRg,
                                                  boolean mrtTest) {
        return createXRegionService(conf, srcRg, tgtRg, mrtTest, false);
    }

    protected XRegionService createXRegionService(JsonConfig conf,
                                                  RegionInfo srcRg,
                                                  RegionInfo tgtRg,
                                                  boolean mrtTest,
                                                  boolean intervalStat) {
        final XRegionService service = new XRegionService(conf, logger);
        service.getStatMan().setReportIntv(STAT_INTV);
        service.getStatMan().setUseIntervalStats(intervalStat);

        /* verify */
        final NoSQLSubscriberId sid =
            new NoSQLSubscriberId(conf.getAgentGroupSize(), conf.getAgentId());
        assertEquals(sid, service.getSid());
        assertTrue(service.getAllAgents().isEmpty());
        assertFalse(service.isRunning());

        final ServiceAgentMetrics metrics = service.getAgentMetrics();
        assertEquals(0, metrics.getRecvOps());
        assertEquals(0, metrics.getStreamBytes());
        assertEquals(0, metrics.getPersistStreamBytes());
        assertTrue(service.getReqQueue().isEmpty());
        assertNotNull(service.getStatusUpdater());

        final ReqRespManager reqRespMan = service.getReqRespMan();
        assertTrue(reqRespMan.isRunning());
        assertEquals(sid, reqRespMan.getSid());

        service.getMdMan().initialize();
        service.getStatMan().initialize();
        verifyMdMan(service, sid, srcRg, tgtRg, mrtTest);

        trace("XRegion service verified");
        return service;
    }

    private void verifyMdMan(XRegionService service, NoSQLSubscriberId sid,
                             RegionInfo srcRg, RegionInfo tgtRg,
                             boolean mrtTest) {
        final ServiceMDMan mdMan = service.getMdMan();
        assertEquals(sid, mdMan.getSid());
        assertEquals(tgtRg, mdMan.getServRegion());

        if (mrtTest) {
            assertTrue(mdMan.getPITRTables().isEmpty());
            if (!mrtTableByStore.isEmpty()) {
                KVStoreImpl kvs = (KVStoreImpl) mdMan.getRegionKVS(tgtRg);
                assertEquals(tgtRg.getStore(),
                             kvs.getTopology().getKVStoreName());
                kvs = (KVStoreImpl) mdMan.getRegionKVS(srcRg);
                assertEquals(srcRg.getStore(),
                             kvs.getTopology().getKVStoreName());

                final Collection<RegionInfo> src = mdMan.getSrcRegionsForMRT();
                assertEquals(1, src.size());
                assertTrue(src.contains(srcRg));
            }
        } else {
            /* PITR test */
            assertTrue(mdMan.getMRTNames().isEmpty());
            assertEquals(1, mdMan.getPITRTables().size());
            assertTrue(mdMan.getPITRTables().stream()
                            .map(Table::getFullNamespaceName)
                            .collect(Collectors.toSet()).contains(PITR_1));
        }
    }

    protected static CreateStore createSecureBackupStore(File root, int port)
        throws Exception {
        return createBackupStore(root, port, true, false);
    }

    /**
     * Make a 1x1 store.
     */
    private static CreateStore createBackupStore(File root, int port,
                                                 boolean secure,
                                                 boolean useThread)
        throws Exception {
        final CreateStore cs =
            new CreateStore(remoteStoreName,
                            port,
                            1, /* n SNs */
                            1, /* rf */
                            3, /* n partitions */
                            1, /* capacity per SN */
                            3 * CreateStore.MB_PER_SN,
                            useThread, /* use threads is false */
                            null, /* mgmtImpl */
                            true, /* mgmtPortsShared */
                            secure);
        cs.setRootDir(root.toString());
        cs.start(false);
        return cs;
    }

    protected Pair<Long, Long> upsertTable(TableAPI api, String tableName,
                                           long rows) {
        return upsertTable(api, tableName, rows, null);
    }

    protected Pair<Long, Long> upsertTable(TableAPI api, String tableName,
                                           long rows, TimeToLive ttl) {
        int counts = 0;
        long low = Long.MAX_VALUE;
        long high = 0;
        while (counts < rows) {
            final Version ver = upsertRow(api, tableName, tableName, counts,
                                          ttl);
            if (ver != null) {
                low = Math.min(low, ver.getVLSN());
                high = Math.max(high, ver.getVLSN());
                counts++;
            }
        }
        return new Pair<>(low, high);
    }

    protected Version upsertRow(TableAPI api, String tableName,
                                String desc, int id) {
        return upsertRow(api, tableName, desc, id, null);
    }

    protected Version upsertRow(TableAPI api, String tableName,
                                String desc, int id, TimeToLive ttl) {
        final Row row = createRow(api.getTable(tableName), desc, id);
        if (ttl == null) {
            return api.put(row, null, null);
        }

        /* write and set TTL */
        final WriteOptions wo = new WriteOptions().setUpdateTTL(true);
        row.setTTL(ttl);
        return api.put(row, null, wo);
    }

    protected Row createRow(Table table, String desc, int id) {
        final Row row = table.createRow();
        row.put("id", id);
        if (table.getFullName().contains(".")) {
            /* child table row */
            row.put("id2", id);
            row.put("childDesc", getDesc(desc, DESC_COL_LENGTH));
            if (table.getParent().getParent() != null) {
                /* nested child row */
                row.put("id3", id);
            }
        } else {
            row.put("desc", getDesc(desc, DESC_COL_LENGTH));

            /* if table has more col due to schema evolution, put random
             * value */
            row.getFieldNames().stream().filter(r -> !r.equals("id") &&
                                                     !r.equals("desc"))
               .forEach(r -> row.put(r, r /* col name as prefix */ +
                                        UUID.randomUUID().toString().
                                            substring(0, 8)));
        }
        return row;
    }

    protected void deleteTable(TableAPI api, String tableName, long rows) {
        int counts = 0;
        while (counts < rows) {
            if (deleteRow(api, tableName, counts)) {
                counts++;
            }
        }
    }

    protected boolean deleteRow(TableAPI api, String tableName, int id) {
        final PrimaryKey pkey = api.getTable(tableName).createPrimaryKey();
        pkey.put("id", id);
        return api.delete(pkey, null, null);
    }

    protected void initTestTable(KVStore storeHandle,
                                 String tableName,
                                 int numRows,
                                 String... regs) {
        String ddl = getCreateTableDDL(Type.STRING, false, tableName);
        initTestTable(storeHandle, tableName, numRows, null, ddl, regs);
    }

    protected void initTestTable(KVStore storeHandle,
                                 String tableName,
                                 int numRows,
                                 TimeToLive ttl,
                                 String ddl,
                                 String... regs) {
        initTestTable(storeHandle, Collections.singletonList(tableName),
                      numRows, ttl, Collections.singletonList(ddl), regs);
    }

    protected void initTestTable(KVStore storeHandle,
                                 List<String> tableNames,
                                 int numRows,
                                 TimeToLive ttl,
                                 List<String> ddls,
                                 String... regs) {
        /* only create region if it is not existent */
        final TableAPIImpl tblAPI = (TableAPIImpl) storeHandle.getTableAPI();
        final RegionMapper rm = tblAPI.getRegionMapper();
        for (String r : regs) {
            if (rm.getRegionId(r) == Region.UNKNOWN_REGION_ID ||
                rm.getRegionId(r) == Region.NULL_REGION_ID) {
                createRegion(storeHandle, r, logger, traceOnScreen);
            }
        }
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            String ddl = ddls.get(i);
            /* create test table */
            storeHandle.executeSync(
                getCreateTableDDL(ttl,
                                  new HashSet<>(Arrays.asList(regs)), ddl,
                                  tableName.contains(".")));

            /* Ensure table created */
            assertNotNull("table " + tableName + " not created",
                          storeHandle.getTableAPI().getTable(tableName));
            /* load init rows */
            upsertTable(storeHandle.getTableAPI(), tableName, numRows);
        }
    }

    protected void dropTestTable(KVStore storeHandle, String tableName) {
        storeHandle.executeSync(getDropTableDDL(tableName));

        /* wait till table is gone */
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return storeHandle.getTableAPI()
                                      .getTable(tableName) == null;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting for " + tableName + " dropped");
        }
        trace("Table " + tableName + " dropped at store=" +
              ((KVStoreImpl) storeHandle).getTopology().getKVStoreName());
    }

    public static void setLocalRegionName(KVStore kvs,
                                          String regionName,
                                          Logger logger,
                                          boolean traceOnScreen) {
        final TableAPIImpl api = (TableAPIImpl) kvs.getTableAPI();
        waitForMRSystemTables(api);
        kvs.executeSync(setLocalRegionNameDDL(regionName, logger,
                                              traceOnScreen));

        /* wait till region id available */
        final boolean succ = new PollCondition(POLL_INTERVAL_MS,
                                               POLL_TIMEOUT_MS) {
            @Override
            protected boolean condition() {
                final int rid = api.getRegionMapper().getRegionId(regionName);
                return rid != -1;
            }
        }.await();
        if (!succ) {
            fail("Timeout in waiting");
        }

        final int rid = api.getRegionMapper().getRegionId(regionName);
        assertEquals("Failed to set local region to " + regionName,
                     Region.LOCAL_REGION_ID, rid);
        trace(Level.INFO,
              "Local region name set to " + regionName + " for store " +
              ((KVStoreImpl) kvs).getTopology().getKVStoreName(), logger,
              traceOnScreen);
    }

    public static void createRegion(KVStore kvs,
                                    String regionName,
                                    Logger logger,
                                    boolean traceOnScreen) {
        final TableAPIImpl api = (TableAPIImpl) kvs.getTableAPI();
        waitForMRSystemTables(api);
        kvs.executeSync(getCreateRegionDDL(regionName, logger, traceOnScreen));
        /*
         * TODO Problematic call. It may get the mapper from an RN which does
         * not yet have the MD. Needs a poll loop.
         */
        final int rid = api.getRegionMapper().getRegionId(regionName);
        assertNotEquals("Region " + regionName + " does not exist",
                        Region.UNKNOWN_REGION_ID, rid);
        trace(Level.INFO, "Region=" + regionName + " created in store " +
                          ((KVStoreImpl) kvs).getTopology().getKVStoreName(),
              logger,
              traceOnScreen);
    }

    protected void createTestNameSpaces(KVStore kvs) {
        createNameSpace(kvs, NS1);
        createNameSpace(kvs, NS2);
    }

    protected void addCol(KVStore kvs) {
        addCol(kvs, Type.STRING);
    }

    protected void addCol(KVStore kvs, Type type) {
        final String storeName =
            ((KVStoreImpl) kvs).getTopology().getKVStoreName();
        final String ddl = getAddColDDL(type);
        final StatementResult ret = kvs.executeSync(ddl);
        trace("in store=" + storeName + " add col to table=" + MRT_1 +
              ", ddl=" + ddl + ", plan id=" + ret.getPlanId());
    }

    protected void removeCol(KVStore kvs) {
        final String storeName =
            ((KVStoreImpl) kvs).getTopology().getKVStoreName();
        final String ddl = getRemoveColDDL();
        final StatementResult ret = kvs.executeSync(ddl);
        trace("in store=" + storeName + ", remove col from table=" + MRT_1 +
              ", ddl=" + ddl + ", plan id=" + ret.getPlanId());
    }

    protected void dumpRegions(KVStore kvs, String tbl) {
        final TableAPIImpl api = (TableAPIImpl) kvs.getTableAPI();
        final TableImpl table = (TableImpl) api.getTable(tbl);
        final Set<Integer> ids = table.getRemoteRegions();
        final Set<String> regionNames = translateRegionNames(api, ids);
        trace("region ids=" + ids + ", region names=" + regionNames);
    }

    protected Set<String> translateRegionNames(TableAPIImpl api,
                                               Set<Integer> ids) {
        final RegionMapper rm = api.getRegionMapper();
        return ids.stream().map(rm::getRegionName).collect(Collectors.toSet());
    }

    /**
     * Wait till the table is removed from region agent and MD manager.
     */
    protected void waitForTableRemoved(XRegionService service, String tbl) {
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final Set<String> inMD = service.getMdMan().getMRTNames();
                    trace("in MD man: " + inMD);
                    return !inMD.contains(tbl) &&
                           service.getTableMetrics(tbl) == null;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
    }

    protected RegionAgentThread getRemoteRegionAgent(XRegionService service) {
        return service.getRegionAgent(REMOTE_REGION);
    }

    protected String getAddColDDL(Type type) {
        return "ALTER TABLE " + MRT_1 + " " +
               "(ADD " + NEW_COL_NAME + " " + type +
               " DEFAULT " + NEW_COL_DEFAULT_QUOTE + ")";

    }

    protected String getRemoveColDDL() {
        return "ALTER TABLE " + MRT_1 + " " + "(DROP " + NEW_COL_NAME + ")";
    }

    private static void createNameSpace(KVStore kvs, String namespace) {
        if (kvs == null) {
            return;
        }
        if (namespace == null || namespace.isEmpty() ||
            namespace.equalsIgnoreCase(TableAPI.SYSDEFAULT_NAMESPACE_NAME)) {
            return;
        }
        final String ddl = "CREATE NAMESPACE IF NOT EXISTS " + namespace;
        kvs.executeSync(ddl);
    }

    protected void dropTestNameSpaces() {
        dropNameSpace(store, NS1);
        dropNameSpace(store, NS2);
        dropNameSpace(remoteStore, NS1);
        dropNameSpace(remoteStore, NS2);
    }

    /**
     * Wait for table transfer done
     */
    protected void waitForTableTransDone(final XRegionService service,
                                         final String tableName) {
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                   return transferComplete(service, REMOTE_REGION, tableName);
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting transfer complete");
        }
        trace("Table=" + tableName + " transfer complete");
    }

    protected boolean transferComplete(XRegionService service,
                                       String region,
                                       String tb) {
        final TableInitCheckpoint tic = readTICkpt(service,
                                                   region,
                                                   tb);
        if (tic == null) {
            trace("No table init checkpoint, table=" + tb +
                  ", region=" + region);
            return false;
        }
        final TableInitStat.TableInitState state = tic.getState();
        trace("table init ckpt=" + tic + ", state=" + state +
              ", expected =" + TableInitStat.TableInitState.COMPLETE);
        return TableInitStat.TableInitState.COMPLETE.equals(state);
    }

    public boolean transferComplete(MRTableMetrics tbm, String region) {
        try {
            final TableInitStat tis = tbm.getRegionInitStat(region);
            if (tis == null) {
                return false;
            }
            trace("Table init stat=" + tbm);
            return TableInitStat.TableInitState.COMPLETE.equals(tis.getState());
        } catch (IllegalArgumentException iae) {
            /* not available yet */
            return false;
        }
    }

    protected TableInitCheckpoint readTICkpt(XRegionService service,
                                             String region,
                                             String table) {

        final Optional<TableInitCheckpoint> opt =
            TableInitCheckpoint.read(service.getMdMan(),
                                     service.getSid().toString(),
                                     region,
                                     table);
        return opt.orElse(null);
    }

    private static void dropNameSpace(KVStore kvs, String namespace) {
        if (kvs == null) {
            return;
        }
        if (namespace == null || namespace.isEmpty() ||
            namespace.equalsIgnoreCase(TableAPI.SYSDEFAULT_NAMESPACE_NAME)) {
            return;
        }
        final String ddl = "DROP NAMESPACE IF EXISTS " + namespace + " CASCADE";
        kvs.executeSync(ddl);
    }

    protected static String setLocalRegionNameDDL(String region,
                                                  Logger logger,
                                                  boolean traceOnScreen) {
        String ddl = "SET LOCAL REGION " + region;
        trace(Level.INFO, "DDL: " + ddl, logger, traceOnScreen);
        return ddl;
    }

    protected static String getCreateRegionDDL(String region,
                                               Logger logger,
                                               boolean traceOnScreen) {
        String ddl = "CREATE REGION " + region;
        trace(Level.INFO, "DDL: " + ddl, logger, traceOnScreen);
        return ddl;
    }

    protected void dropRegion(KVStore kvs, String regionName) {
        kvs.executeSync(getDropRegionDDL(regionName));
    }

    private String getDropRegionDDL(String region) {
        String ddl = "DROP REGION " + region;
        trace("DDL: " + ddl);
        return ddl;
    }

    /* generate random characters up to a limit */
    protected static String getDesc(String tableName, int limit) {
        final StringBuilder sb = new StringBuilder(tableName);
        while (true) {
            final String desc = UUID.randomUUID().toString();
            if (sb.length() + desc.length() > limit) {
                break;
            }
            sb.append("-").append(desc);
        }
        return sb.toString();
    }

    protected String getCreateTableDDL(String tableName,
                                       TimeToLive ttl,
                                       Set<String> regions) {
        String ddl = getCreateTableDDL(Type.STRING, false, tableName);
        return getCreateTableDDL(ttl, regions, ddl);
    }

    private String getCreateTableDDL(TimeToLive ttl,
                                     Set<String> regions,
                                     String ddl) {
        return getCreateTableDDL(ttl, regions, ddl, false);
    }

    private String getCreateTableDDL(TimeToLive ttl,
                                     Set<String> regions,
                                     String ddl,
                                     boolean childTable) {
        if (ttl != null) {
            ddl += " USING TTL " + ttl;
        }
        if (regions != null && !regions.isEmpty() && !childTable) {
            ddl += " IN REGIONS ";
            ddl += String.join(", ", regions);
        }
        trace(Level.INFO, "DDL: " + ddl, logger, traceOnScreen);
        return ddl;
    }

    protected String getCreateTableDDL(Type valueType,
                                       boolean createCRDT,
                                       String tableName) {
        return "CREATE TABLE " + tableName +
               " (id Integer, " +
               "desc " + valueType +
               (createCRDT ? " AS MR_COUNTER" : "") +
               ", PRIMARY KEY (id))";
    }

    protected String getAlterTableDDL(String tableName, TimeToLive ttl) {
        String ddl = "ALTER TABLE " + tableName;
        if (ttl != null) {
            ddl += " USING TTL " + ttl;
        }
        trace(Level.INFO, "DDL: " + ddl, logger, traceOnScreen);
        return ddl;
    }

    protected String getCreateTableDDL(String tableName, Set<String> regions) {
        return getCreateTableDDL(tableName, null, regions);
    }

    private String getDropTableDDL(String tableName) {
        final String ddl = "DROP TABLE " + tableName;
        trace("DDL: " + ddl);
        return ddl;
    }

    protected Set<Row> getRows(String tablename) {
        final Table table = tgtStore.getTableAPI().getTable(tablename);
        final TableIterator<Row> iter = tgtStore.getTableAPI().tableIterator(
            table.createPrimaryKey(), null, null);
        final Set<Row> ret = new HashSet<>();
        while (iter.hasNext()) {
            final Row row = iter.next();
            trace(row.toJsonString(true));
            ret.add(row);
        }
        return ret;
    }

    protected Row waitForResponse(int reqId) {
        final AtomicReference<Row> ret = new AtomicReference<>();
        /* wait for response */
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    for (Row r : getRows(StreamResponseDesc.TABLE_NAME)) {
                        if (r.get(COL_REQUEST_ID).asInteger().get() == reqId) {
                            ret.set(r);
                            trace("Row=" + r.toJsonString(true) +
                                  ", exp req id=" + reqId);
                            return true;
                        }
                    }
                    trace("Cannot find resp with req id=" + reqId);
                    return false;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
        return ret.get();
    }

    /**
     * Waits for the MR request and response tables to be created.
     */
    public static void waitForMRSystemTables(TableAPI tableAPI) {
        waitForTable(tableAPI, StreamRequestDesc.TABLE_NAME);
        waitForTable(tableAPI, StreamResponseDesc.TABLE_NAME);
    }

    protected XRegionService restartAgent(JsonConfig conf, int total)
        throws InterruptedException {
        /* avoid using old publisher in crashed agent */
        NoSQLPublisher.clearPublisherInstMap();

        /* wait for some time to let remote feeder clear up */
        final int sleepMs = 10 * 1000;
        trace("Start sleeping in ms=" + sleepMs);
        Thread.sleep(sleepMs);

        /* restart the agent */
        clearShutdownHook();

        final XRegionService restart = restartService(conf, total);
        trace("Service restart started");
        return restart;
    }

    protected XRegionService getService(JsonConfig conf) {
        final XRegionService service = new XRegionService(conf, logger);
        service.getMdMan().initialize();
        service.getStatMan().initialize();
        service.getStatMan().setReportIntv(STAT_INTV);
        service.getStatMan().setUseIntervalStats(false);
        return service;
    }

    protected void clearShutdownHook() {
        BaseTableTransferThread.expHook = null;
    }

    private XRegionService restartService(JsonConfig conf, int total) {
        final XRegionService service = getService(conf);
        service.start();
        /* wait till all writes to MRT1 and MRT2 streamed */
        waitForStreamedOps(service, total);
        return service;
    }

    protected void waitForStreamedOps(XRegionService service, long total) {
        /* wait till all writes to MRT1 and MRT2 streamed */
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    /* all init rows streamed from source */
                    final ServiceAgentMetrics m = service.getAgentMetrics();
                    final long ops = m.getRecvOps();
                    trace("Received # ops=" + ops + ", exp=" + total);
                    return ops == total;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
    }

    protected void waitFor(Supplier<Boolean> s, int intvMs, long timeoutMs) {
        try {
            waitFor(new PollCondition(intvMs, timeoutMs) {
                @Override
                protected boolean condition() {
                    return s.get();
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting");
        }
    }

    protected void shutDownStore(CreateStore cs, KVStore kvs) {
        if (cs != null) {
            cs.shutdown();
        }
        if (kvs != null) {
            kvs.close();
        }
    }

    public static class ReqRespThread extends Thread {

        private final TestReqRespManager man;

        private volatile boolean shutDownRequested;

        private final Logger logger;

        private final boolean traceOnScreen;

        public ReqRespThread(KVStore storeHandle,
                             Logger logger,
                             boolean traceOnScreen) {
            super("ReqRespThreadMockService");
            shutDownRequested = false;
            this.logger = logger;
            man = new TestReqRespManager(this, storeHandle.getTableAPI(),
                                         logger, traceOnScreen);
            this.traceOnScreen = traceOnScreen;
        }

        protected Logger getLogger() {
            return logger;
        }

        boolean isShutDownRequested() {
            return shutDownRequested;
        }

        public void stopResResp() {
            if (shutDownRequested) {
                return;
            }
            shutDownRequested = true;
        }

        @Override
        public void run() {
            try {
                while (!shutDownRequested) {
                    man.postSuccForRequest();
                    try {
                        synchronized (this) {
                            wait(1000);
                        }
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            } catch (Exception exp) {
                if (shutDownRequested) {
                    /* ignore exception in shut down */
                    trace(Level.INFO, "In shutdown, ignore exception=" + exp,
                          logger, traceOnScreen);
                }
            } finally {
                trace(Level.INFO, "Mock service thread exits",
                      logger, traceOnScreen);
            }
        }
    }

    protected static class TestReqRespManager extends Manager {

        private final Set<Integer> postedReq;

        private final TableAPI tblAPI;

        private final boolean traceOnScreen;

        private final ReqRespThread parent;

        TestReqRespManager(ReqRespThread parent,
                           TableAPI tblAPI,
                           Logger logger,
                           boolean traceOnScreen) {
            super(logger);
            this.parent = parent;
            this.tblAPI = tblAPI;
            postedReq = new HashSet<>();
            this.traceOnScreen = traceOnScreen;
        }

        public TestReqRespManager(TableAPI tblAPI,
                                  Logger logger,
                                  boolean traceOnScreen) {
            this(null, tblAPI, logger, traceOnScreen);
        }

        @Override
        protected TableAPI getTableAPI() {
            return tblAPI;
        }

        @Override
        protected void handleIOE(String error, IOException ioe) {
            trace(Level.INFO, "IOE in post response: " + error,
                  logger, traceOnScreen);
        }

        private boolean isClosed() {
            return parent != null && parent.isShutDownRequested();
        }

        void postSuccForRequest() {
            final RequestIterator itr =
                getRequestIterator(10, TimeUnit.SECONDS);
            while (!isClosed() && itr.hasNext()) {
                final Request req = itr.next();
                final ServiceMessage.ServiceType st = req.getServiceType();
                if (postedReq.contains(req.getRequestId())) {
                    continue;
                }
                trace(Level.INFO, "Mock service see new request " + req,
                      logger, traceOnScreen);
                if (!st.equals(ServiceMessage.ServiceType.MRT)) {
                    continue;
                }

                if (postedReq.contains(req.getRequestId())) {
                    /* already posted */
                    continue;
                }

                final Response sm =
                    Response.createReqResp(req.getRequestId(),
                                           DEFAULT_AGENT_GROUP_SIZE);
                sm.addSuccResponse(DEFAULT_TEST_AGENT_ID);
                postResponse(sm, false /*overwrite*/);
                postedReq.add(req.getRequestId());
                trace(Level.INFO, "Mock service post succ response for" +
                                  " req id " + sm.getRequestId(),
                      logger, traceOnScreen);
            }
        }

        /**
         * Overridden to make public
         */
        @Override
        public void setStoreAndMinAgentVersions(KVVersion storeVersion,
                                                KVVersion minAgentVersion) {
            super.setStoreAndMinAgentVersions(storeVersion, minAgentVersion);
        }

        public void setAgentVersion(KVVersion version) {
            setAgentVersion(DEFAULT_TEST_AGENT_ID, version);
        }

        @Override
        protected short getMaxSerialVersion() {
            return SerialVersion.CURRENT;
        }
    }

    protected Set<Table> getTable(KVStore kvs, String table) {
        final Table tbl = kvs.getTableAPI().getTable(table);
        if (tbl == null) {
            return Collections.emptySet();
        }
        return Collections.singleton(tbl);
    }

    protected Set<Table> getTables(KVStore kvs, Set<String> tables) {
        final Set<Table> ret = new HashSet<>();
        tables.forEach(t -> ret.addAll(getTable(kvs, t)));
        return ret;
    }

    protected class TestRespManager extends Manager {

        private final TableAPI tblAPI;

        public TestRespManager(TableAPI tblAPI, Logger logger) {
            super(logger);
            this.tblAPI = tblAPI;
        }

        @Override
        protected TableAPI getTableAPI() {
            return tblAPI;
        }

        @Override
        protected void handleIOE(String error, IOException ioe) {
            fail("Fail: " + error + ", " + ioe.getMessage());
        }

        public void verifyResponse() {
            final ResponseIterator itr =
                getResponseIterator(0, 10, TimeUnit.SECONDS);
            while (itr.hasNext()) {
                final Response resp = itr.next();
                trace("resp: " + resp);
                verifyAgentResponse(resp, DEFAULT_TEST_AGENT_ID, true);
            }
        }

        @Override
        protected short getMaxSerialVersion() {
            return SerialVersion.CURRENT;
        }
    }

    protected void verifyAgentResponse(Response resp,
                                       NoSQLSubscriberId agentId,
                                       boolean succ) {
        assertEquals(Response.Type.REQUEST_RESPONSE, resp.getType());
        final Response.ReqResponse r =
            (Response.ReqResponse) resp.getAgentResponse(agentId);
        assertEquals(succ, r.isSucc());
    }

    protected void runArgs(String[] args, ByteArrayOutputStream buffer)
        throws Exception {
        PrintStream out = new PrintStream(buffer);
        CommandShell shell = connectToStore(out);
        shell.parseArgs(args);
        shell.start();
    }

    private CommandShell connectToStore(PrintStream out)
        throws Exception {

        final CommandShell shell = new CommandShell(System.in, out);
        shell.openStore(createStore.getHostname(),
                        createStore.getRegistryPort(),
                        createStore.getStoreName(),
                        createStore.getDefaultUserName(),
                        createStore.getDefaultUserLoginPath());
        return shell;
    }

    protected InProgressTestHook setTestHook(int count) {
        final InProgressTestHook hook = new InProgressTestHook(count);
        BaseTableTransferThread.transInProgressHook = hook;
        return hook;
    }

    protected void clearInProgressTestHook() {
        BaseTableTransferThread.transInProgressHook = null;
    }

    protected void waitForPause(InProgressTestHook hook) {
        final boolean succ =
            new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return hook.isWaiting();
                }
            }.await();

        if (!succ) {
            fail("Timeout in waiting");
        }
    }

    protected void checkMismatchResult(XRegionService service) {
        try {
            waitFor(new PollCondition(POLL_INTERVAL_MS, POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return service.getAgentMetrics().getIncompatibleRows() >= 1;
                }
            });
        } catch (TimeoutException e) {
            fail("timeout in waiting for the incompatible row.");
        }
    }

    protected void waitMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            fail("Interrupted");
        }
    }

    protected void dumpTableInitCkptTable(KVStore kvs) {
        final TableAPI tapi = kvs.getTableAPI();
        final Table ckptTable = tapi.getTable(MRTableInitCkptDesc.TABLE_NAME);
        assertNotNull("Not found table=" + MRTableInitCkptDesc.TABLE_NAME,
                      ckptTable);
        final TableIteratorOptions opt =
            new TableIteratorOptions(Direction.FORWARD, Consistency.ABSOLUTE, 0,
                                     null);
        final TableIterator<Row> iter =
            tapi.tableIterator(ckptTable.createPrimaryKey(), null, opt);
        trace("Dump MR table init checkpoint table=" +
              ckptTable.getFullNamespaceName());
        while (iter.hasNext()) {
            final Row row = iter.next();
            trace("Row=" + row.toJsonString(true));
            final long ts = row.getLastModificationTime();
            final TableInitCheckpoint tic = getCkpt(row);
            if (tic == null) {
                trace("Cannot get table init checkpoint from row=" +
                      row.toJsonString(true));
                continue;
            }
            String sb = "LastModTime=" +
                        FormatUtils.formatDateTimeMillis(ts) +
                        ", ckptTime=" +
                        FormatUtils.formatDateTimeMillis(tic.getTimestamp()) +
                        row.toJsonString(true) + "\n";
            trace(sb);
        }
        trace("End dump MR table init checkpoint table");
    }

    private TableInitCheckpoint getCkpt(Row row) {
        final FieldValue fv = row.get(COL_NAME_CHECKPOINT);
        if (fv instanceof MapValueImpl) {
            return null;
        }

        final String json = row.get(COL_NAME_CHECKPOINT).asString().get();
        return JsonUtils.readValue(json, TableInitCheckpoint.class);
    }

    /**
     * test hook to pause table copy in transfer
     */
    public class InProgressTestHook implements TestHook<Pair<String, Long>> {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final int count;
        private volatile boolean waiting = false;
        private volatile boolean awaken = false;

        InProgressTestHook(int count) {
            this.count = count;
        }

        @Override
        public void doHook(Pair<String, Long> pair) {
            final String tableName = pair.first();
            final long transferred = pair.second();
            if (transferred >= count && !awaken) {
                try {
                    trace("[InProgHook] table=" + tableName +
                          ", #transferred=" + transferred + ", waiting...");
                    waiting = true;
                    latch.await();
                    waiting = false;
                    awaken = true;
                    trace("awaken");
                } catch (InterruptedException e) {
                    fail("Interrupted");
                }
            } else if (transferred % 100 == 0) {
                trace("[InProgHook] table=" + tableName +
                      ", #transferred=" + transferred +
                      ", count=" + count + ", awaken=" + awaken);
            }
        }

        public boolean isWaiting() {
            return waiting;
        }

        public CountDownLatch getLatch() {
            return latch;
        }
    }

    public class StartServiceThread extends StoppableThread {

        private volatile XRegionService service;
        private volatile boolean created;

        public StartServiceThread(String threadName) {
            super(threadName);
            service = null;
            created = false;
        }

        @Override
        protected int initiateSoftShutdown() {
            return POLL_INTERVAL_MS;
        }

        @Override
        public void run() {
            createService();
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        public XRegionService getService() {
            return service;
        }

        public boolean isCreated() {
            return created;
        }

        private void createService() {
            final JsonConfig conf = createJsonConfig();
            trace("JSON config: " + conf);
            service = createXRegionService(conf, srcRegion, tgtRegion, true);
            trace("Service created");
            created = true;
        }
    }
}