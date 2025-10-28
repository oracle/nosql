/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.pubsub;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_JE_HA;
import static oracle.kv.util.DDLTestUtils.execStatement;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.pubsub.NoSQLStreamFeederFilter;
import oracle.kv.impl.pubsub.StreamPutEvent;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionMap;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.contextlogger.LogFormatter;

import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.impl.node.Feeder;

import org.reactivestreams.Subscription;

/**
 * Base for Publisher and Subscriber unit test
 */
public class PubSubTestBase extends TestBase {

    protected static final String USER_TABLE_NAME = "user";
    protected static final String CUSTOMER_TABLE_NAME = "customer";

    public static final String TEST_LOG_FILE = "testlog";
    private static final String keyPrefix = "PubSubTestBase_Key_";
    private static final String valPrefix = "PubSubTestBase_Value_";
    protected static final String TEST_ROW_MD = "{\"custom MD\":1}";

    /* no r2compat table */
    protected static final boolean r2Compat = false;

    protected static final int TEST_POLL_INTERVAL_MS = 1000;
    protected static final int TEST_POLL_TIMEOUT_MS = 60000;

    private static final int NUM_INIT_KV_ENTRIES = 10;

    /* rg1rn1 should be master at the beginning */
    protected static final RepNodeId RG1_RN1 = new RepNodeId(1, 1);

    protected static final NoSQLSubscriberId SID = new NoSQLSubscriberId(1, 0);

    /* default test path */
    protected final String testPath = TestUtils.getTestDir().getAbsolutePath();

    /* default test namespace */
    private static final String DEFAULT_TEST_NAME_SPACE = "TestNamespace";
    /* default ckpt table name */
    protected String ckptTableName =
        NameUtils.makeQualifiedName(DEFAULT_TEST_NAME_SPACE, "CheckpointTable");

    /* environment config parameters */
    protected int repFactor = 3;
    protected int numStorageNodes = 1;
    protected int numDataCenters = 1;
    protected int numPartitions = 32;
    protected int nSecondaryZones = 0;
    protected int nShards = 0;

    protected volatile boolean traceOnScreen = false;

    protected KVRepTestConfig config;
    protected KVStore store;
    protected CreateStore createStore;
    protected KVStoreConfig kvStoreConfig;
    protected TableAPI tableAPI;
    boolean useFeederFilter = false;

    private Map<Key, Value> testData;

    protected static final TableImpl userTable =
        TableBuilder.createTableBuilder("User")
                    .addInteger("id")
                    .addString("firstName")
                    .addString("lastName")
                    .addInteger("age")
                    .primaryKey("id")
                    .shardKey("id")
                    .setR2compat(r2Compat)
                    .buildTable();

    private static final TableImpl jokeTable =
        TableBuilder.createTableBuilder("Joke")
                    .addInteger("id")
                    .addString("category")
                    .addString("text")
                    .addFloat("humorQuotient")
                    .primaryKey("id")
                    .setR2compat(r2Compat)
                    .buildTable();

    protected static final TableImpl noiseTable =
        TableBuilder.createTableBuilder("Noise")
                    .addInteger("id")
                    .addString("text")
                    .primaryKey("id")
                    .setR2compat(r2Compat)
                    .buildTable();

    protected TableMetadata metadata;

    protected final Random random = new Random(System.currentTimeMillis());

    protected volatile boolean useRowMD = false;

    /**
     * Below are for secure store test only
     */
    protected static final String ADMIN_USER = "streamAdmin";
    protected static final String ADMIN_PASS = "AdminPassword2016!!";
    protected static final LoginCredentials adminCredentials =
        new PasswordCredentials(ADMIN_USER, ADMIN_PASS.toCharArray());
    protected static final String[] ADMIN_ROLES = new String[]{"dbadmin"};
    protected static final String USER = "streamUser";
    protected static final String PASS = "StrongPassword2016!!";
    protected static final LoginCredentials USER_LOGIN_CRED =
        new PasswordCredentials(USER, PASS.toCharArray());
    protected SecurityParams securityParams;
    protected KVStore adminStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        /* default */
        repFactor = 3;
        numStorageNodes = 1;
        numDataCenters = 1;
        numPartitions = 32;
        nSecondaryZones = 0;
        nShards = 0;
        traceOnScreen = false;
        useFeederFilter = false;
        kvstoreName = "mystore";
        TestStatus.setManyRNs(true);
        addLoggerFileHandler();
        metadata = null;
    }

    @Override
    public void tearDown() throws Exception {

        if (config != null) {
            config.stopRNs();
        }
        super.tearDown();
    }

    protected void setUseRowMD() {
        useRowMD = true;
        trace("Set use row metadata");
    }

    protected long getTableId(String tableName) {
        final TableAPI tapi = store.getTableAPI();
        final TableImpl tb = (TableImpl) tapi.getTable(tableName);
        if (tb == null) {
            return -1;
        }
        return tb.getId();
    }

    protected String logTimestamp(long ts) {
        return ts + "(" + FormatUtils.formatDateTime(ts) + ")";
    }

    void createNamespace(String ns) {
        String ddl = "CREATE NAMESPACE IF NOT EXISTS " + ns;
        store.executeSync(ddl);
    }

    protected Table createTable(KVStore kvs, String testTable) {
        final String ddl = "CREATE TABLE " + testTable + " " +
                           "(id INTEGER, name STRING, age INTEGER, " +
                           "PRIMARY KEY (id))";
        kvs.executeSync(ddl);
        /* Ensure table created */
        final Table ret = kvs.getTableAPI().getTable(testTable);
        assertNotNull("table " + testTable + " not created",
                      kvs.getTableAPI().getTable(testTable));
        trace("Table=" + testTable + " has been created at store=" +
              ((KVStoreImpl)kvs).getTopology().getKVStoreName());
        return ret;
    }

    protected Table createChildTable(KVStore kvs, String parent, String child) {
        final String tb = parent + "." + child;
        return createChildTable(kvs, tb);
    }

    protected Table createChildTable(KVStore kvs, String tb) {
        final String ddl = "CREATE TABLE " + tb +
                           " (state STRING, address STRING, " +
                           " PRIMARY KEY (state))";
        kvs.executeSync(ddl);
        /* Ensure table created */
        final Table ret =  kvs.getTableAPI().getTable(tb);
        assertNotNull("table " + tb + " not created", ret);
        trace("Table=" + tb + " has been created at store=" +
              ((KVStoreImpl)kvs).getTopology().getKVStoreName());
        return ret;
    }

    protected NoSQLPublisher createPublisher() {
        /* create a publisher */
        final NoSQLPublisherConfig conf =
            new NoSQLPublisherConfig.Builder(kvStoreConfig, testPath).build();
        NoSQLPublisher publisher = NoSQLPublisher.get(conf, logger);
        trace("Publisher created to store " +
              conf.getStoreName() + ", helper host:" +
              Arrays.toString(conf.getHelperHosts()));
        return publisher;
    }

    protected Map<Integer, Version> loadRows(TableAPI api,
                                             String tableName,
                                             int numRows) {

        final Map<Integer, Version> ret = new Hashtable<>();
        for (int i = 0; i < numRows; i++) {
            final Version ver = writeRow(api, tableName, i,
                                         "name-" + random.nextInt(
                                             Integer.MAX_VALUE),
                                         random.nextInt(100));
            ret.put(i, ver);
        }
        trace("#rows=" + numRows + " loaded to table=" + tableName + " at " +
              "store=" + ((TableAPIImpl)api).getStore().getTopology()
                                            .getKVStoreName());

        return ret;
    }

    protected Map<Integer, Boolean> deleteRows(TableAPI api,
                                               String tableName,
                                               int numRows) {

        final Map<Integer, Boolean> ret = new Hashtable<>();
        for (int i = 0; i < numRows; i++) {
            final Boolean succ = deleteRow(api, tableName, i);
            ret.put(i, succ);
        }
        trace("#rows=" + numRows + " deleted to table=" + tableName + " at " +
              "store=" + ((TableAPIImpl)api).getStore().getTopology()
                                            .getKVStoreName());
        return ret;
    }

    protected boolean deleteRow(TableAPI api, String table, int id) {
        final PrimaryKey pkey = api.getTable(table).createPrimaryKey();
        pkey.put("id", id);
        if (useRowMD) {
            pkey.setRowMetadata(TEST_ROW_MD);
        }
        return api.delete(pkey, null, null);
    }

    private Version writeRow(TableAPI api, String tableName,
                             int id, String name, int age) {
        final Row row = api.getTable(tableName).createRow();
        row.put("id", id);
        row.put("name", name);
        row.put("age", age);
        if (useRowMD) {
            row.setRowMetadata(TEST_ROW_MD);
        }
        return api.put(row, null, null);
    }

    /* wait for test done, by checking subscription callback */
    protected void waitForStreamDone(final TestNoSQLSubscriberBase sub,
                                     final int total)
        throws TimeoutException {

        boolean success =
            new PollCondition(TEST_POLL_INTERVAL_MS, TEST_POLL_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    final long act = sub.getNumPuts();
                    trace("expected=" + total + ", actual=" + act);
                    return act == total;
                }
            }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling.");
        }
    }

    protected void createDefaultTestNameSpace() {
        final String stmt =
            "CREATE NAMESPACE IF NOT EXISTS " + DEFAULT_TEST_NAME_SPACE;
        final StatementResult sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());
        trace("Default namespace created=" + DEFAULT_TEST_NAME_SPACE);
    }

    protected void waitFor(final PollCondition pollCondition)
        throws TimeoutException {

        boolean success = pollCondition.await();
        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }

    /* add tables, set r2compat to true to deserialize */
    protected void addTableToMetadata(TableImpl... tables) {
        metadata = new TableMetadata(true);
         for (TableImpl table : tables) {
             metadata.addTable(null, /* no table name space */
                               table.getFullName(),
                               table.getParentName(),
                               table.getPrimaryKey(),
                               table.getPrimaryKeySizes(),
                               table.getShardKey(),
                               table.getFieldMap(),
                               null, null, null, true, 0, null, null);
         }

        for (RepGroupId repGroupId : config.getTopology().getRepGroupIds()) {
            final RepNode masterRN = getMaster(config, repGroupId);
            assert masterRN != null;
            masterRN.updateMetadata(metadata);
        }
    }

    public void prepareTestEnv(boolean loadTestData) {

        config.startRepNodeServices();
        if (loadTestData) {
            createTestKVData();
            loadTestData();
            verifyTestData();
        }
        store = KVStoreFactory.getStore(config.getKVSConfig());
        kvStoreConfig = config.getKVSConfig();
        trace("Test environment created successfully," +
              "\ntopology:\n" + topo2String(config) +
              "\ntest data loaded:\n" + loadTestData);
    }

    protected Map<String, Row> insertRowsIntoTable(TableImpl table,
                                                   int numKeys) {
        final TableAPI apiImpl = store.getTableAPI();
        return loadTable(apiImpl, table, numKeys);
    }

    protected static KVStoreConfig createKVConfig(CreateStore cs) {
        final KVStoreConfig conf =
            new KVStoreConfig(cs.getStoreName(),
                              cs.getHostname() + ":" +
                              cs.getRegistryPort());
        conf.setDurability
            (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.ReplicaAckPolicy.ALL));
        return conf;
    }

    protected void setSecureProperty(KVStoreConfig kvConfig) {

        final Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  createStore.getTrustStore().getPath());
        kvConfig.setSecurityProperties(props);

        trace(dumpProperty(props));
    }

    /* write test data into store */
    private void loadTestData() {
        final KVStore s = KVStoreFactory.getStore(config.getKVSConfig());
        for (Map.Entry<Key, Value> entry : testData.entrySet()) {
            Key k = entry.getKey();
            Value v = entry.getValue();
            s.put(k, v, null, Durability.COMMIT_SYNC, 0, null);
        }
        logger.info("Test data with " + testData.size() + " keys loaded.");
    }

    protected void loadUserTable(Table table, int numRows) {
        for (int i = 0; i < numRows; i++) {
            final Row row = table.createRow();
            row.put("id", i);
            row.put("firstName",
                    "firstName-"+random.nextInt(Integer.MAX_VALUE));
            row.put("lastName",
                    "lastName-"+random.nextInt(Integer.MAX_VALUE));
            row.put("age", random.nextInt(100));
            tableAPI.put(row, null, null);
        }
    }

    protected void loadCustomerTable(Table table, int numRows) {
        for (int i = 0; i < numRows; i++) {
            final Row row = table.createRow();
            row.put("customerId", i);
            row.put("customerName",
                    "customerName-"+random.nextInt(Integer.MAX_VALUE));
            tableAPI.put(row, null, null);
        }
    }


    /* Verify each received stream operation is an expected row */
    protected void verifyRows(Map<String, Row> expectedRows,
                              List<StreamOperation> recvOperations) {

        for (StreamOperation operation : recvOperations) {

            trace(Level.FINE, operation.toString());

            final StreamPutEvent put = (StreamPutEvent) operation;
            final Row row = put.getRow();
            assertNotNull("Row in PUT cannot be null", row);

            final String key = row.get("id").toString();
            final Row expectedRow = expectedRows.get(key);
            assertNotNull("Received unexpected row with key " + key,
                          expectedRow);

            assertEquals("Mismatched row", expectedRow, row);
        }
    }

    protected void verifyRows(Map<String, Row> expRows,
                              Map<String, Row> recvRows) {

        assertEquals("Mismatched # rows", expRows.size(), recvRows.size());
        int i = 0;
        for (Map.Entry<String, Row> entry : expRows.entrySet()) {
            final Row exp = entry.getValue();
            final Row act = recvRows.get(entry.getKey());
            assertEquals("Mismatched row at " + i,
                         exp, act);
            i++;
        }
    }

    protected void threadTrace(String msg) {
        trace("[Thread=" + Thread.currentThread().getName()+ "] " + msg);
    }

    protected void trace(String message) {
       trace(INFO, message);
    }

    protected void trace(Level level,
                         String message) {
        trace(level, message, logger, traceOnScreen);
    }

    protected static void trace(Level level,
                                String message,
                                Logger logger,
                                boolean traceOnScreen) {
        if (traceOnScreen) {
            if (logger.isLoggable(level)) {
                System.err.println(message);
            }
        }
        if (logger != null) {
            message = "[TEST] " + message;
            if (level.equals(INFO)) {
                logger.info(message);
            } else if (level.equals(FINE)) {
                logger.fine(message);
            } else if (level.equals(FINER)) {
                logger.finer(message);
            } else if (level.equals(FINEST)) {
                logger.finest(message);
            } else if (level.equals(WARNING)) {
                logger.warning(message);
            } else if (level.equals(SEVERE)) {
                logger.severe(message);
            }
        }
    }

    /**
     * Verify high stream position is higher than low position by a minimal
     * of difference
     * @param low      low stream position
     * @param high     high stream position
     * @param minDiff  minimal difference
     */
    protected void verifyCkpt(StreamPosition low,
                              StreamPosition high,
                              int minDiff) {
        final int numShards = low.getAllShardPos().size();
        /* vlsn must be higher for each shard */
        IntStream.range(1, numShards + 1).forEach(shardId -> {
            final long lvlsn = low.getShardPosition(shardId).getVLSN();
            final long hvlsn = high.getShardPosition(shardId).getVLSN();
            trace("shard id=" + shardId + ", low vlsn=" + lvlsn + ", high " +
                  "vlsn=" + hvlsn + ", minimal diff=" + minDiff);
            assertTrue(hvlsn >= lvlsn + minDiff);
        });
    }

    protected void logoutStores() {
        if (store != null) {
            store.logout();
            store.close();
            store = null;
        }
        trace("store logout");
    }

    protected NoSQLStreamFeederFilter buildFilter(Set<TableImpl> tables) {
        /* get a feeder filter passing selected tables */
        final PartitionMap pmap = config.getTopology().getPartitionMap();
        final int nParts = pmap.getNPartitions();
        return NoSQLStreamFeederFilter.getFilter(tables, nParts, false);
    }

    protected NoSQLStreamFeederFilter getFeederFilter(RepNodeId master,
                                                      String feederId) {
        final RepNode rn = config.getRN(master);
        final ReplicatedEnvironment env = rn.getEnv(0);
        final Feeder feeder = RepInternal.getRepImpl(env)
                                         .getRepNode()
                                         .feederManager()
                                         .getFeeder(feederId);
        return (NoSQLStreamFeederFilter) feeder.getFeederFilter();
    }

    protected void addLoggerFileHandler() throws IOException {
        final String fileName = TEST_LOG_FILE;
        final File loggerFile = new File(new File(testPath), fileName);
        final FileHandler handler =
            new FileHandler(loggerFile.getAbsolutePath(), false);
        handler.setFormatter(new LogFormatter(null));
        tearDowns.add(() -> logger.removeHandler(handler));
        logger.addHandler(handler);
        logger.info("Add test log file handler: path=" + testPath +
                    ", log file name=" + fileName +
                    ", logging level=" + logger.getLevel() +
                    ", file exits?=" + loggerFile.exists());
    }

    /* load table into kv store */
    private Map<String, Row> loadTable(TableAPI apiImpl,
                                       TableImpl tableImpl,
                                       long num) {

        final Map<String, Row> rows = new HashMap<>();
        for (int i = 0; i < num; i++) {

            final RowImpl row = makeRandomRow(tableImpl, i);
            apiImpl.put(row, null,
                        new WriteOptions(Durability.COMMIT_NO_SYNC, 10000,
                                         MILLISECONDS));

            final String key = row.get("id").toString();
            rows.put(key, row);
        }

        return rows;
    }

    /* dump topology from a RN */
    protected static String topo2String(KVRepTestConfig config) {
        final RepNode rn = config.getRN(RG1_RN1);
        return dumpTopo(rn.getTopology());
    }

    public static String dumpTopo(Topology topology) {
        final StringBuilder builder = new StringBuilder();
        builder.append("====== start topology dump ======");
        for (RepGroupId id : topology.getRepGroupIds()) {
            builder.append("\nrep group: ").append(id.getGroupName())
                   .append("\n");

            final RepGroup group = topology.get(id);
            for (oracle.kv.impl.topo.RepNode node : group.getRepNodes()) {
                builder.append("\tRN: ").append(node.toString());
            }
        }
        builder.append("\n===== done topology dump ======\n");

        return builder.toString();
    }

    static String buildCkptTableName(NoSQLSubscriberId sid,
                                     String ckptTablePrefix) {
        return ckptTablePrefix + sid.toString();
    }

    protected static String dumpTopo2(KVRepTestConfig config,
                                      RepNodeId repNodeId) {

        final Topology topology =  config.getRN(repNodeId).getTopology();
        final StringBuilder builder = new StringBuilder();
        builder.append("====== start topology dump ======");
        for (RepGroupId id : topology.getRepGroupIds()) {
            builder.append("\nrep group: ").append(id.getGroupName())
                   .append("\n");

            final RepGroup group = topology.get(id);
            for (oracle.kv.impl.topo.RepNode node : group.getRepNodes()) {
                builder.append("\tRN: ").append(node.toString())
                       .append(" ");

                final ReplicatedEnvironment.State
                    state = config.getRN(node.getResourceId()).getEnvImpl(6000)
                                  .getState();
                if (state.isMaster()) {
                    builder.append("master");
                } else if (state.isReplica()) {
                    builder.append("replica");
                } else if (state.isDetached()) {
                    builder.append("detached");
                } else if (state.isUnknown()) {
                    builder.append("unknown");
                }
            }
        }
        builder.append("\n===== done topology dump ======\n");

        return builder.toString();
    }

    /* create a publisher as admin to create the ckpt table  */
    protected void createCkptTable(KVStore kvs, String tableName) {

        NoSQLPublisher.createCheckpointTable(kvs, tableName);

        final TableImpl t = (TableImpl) kvs.getTableAPI()
                                           .getTable(tableName);
        assertTrue("Table " + tableName + " should be ready",
                   t.getStatus().isReady());

        trace("Created checkpoint table " + tableName);
    }

    protected void setLoginCacheTimeout(long timeoutSecs)
        throws RemoteException {

        setAuthPolicy(GP_LOGIN_CACHE_TIMEOUT + "=" + timeoutSecs + " SECONDS",
                      createStore);
    }

    protected NoSQLPublisher getPublisherSecureStore(int numInitRows)
        throws Exception {

        /* create tables */
        createUserTable(adminStore, numInitRows);
        trace("User table created with rows=" + numInitRows);
        createCustomerTable(adminStore, numInitRows);
        trace("Customer table created with rows=" + numInitRows);

        /* grant all privileges to user needed to subscribe the table */
        grantCreateAnyTablePriv(adminStore, USER);
        final String role = createRole(adminStore);
        grantReadPrivTable(adminStore, role, USER_TABLE_NAME, USER);
        trace("User=" + USER + " granted privileges to subscribe user table" +
              ", role=" + role);

        /* create a publisher */
        final NoSQLPublisherConfig pubConf =
            new NoSQLPublisherConfig.Builder(kvStoreConfig, testPath)
                .setReauthHandler(kvstore -> kvstore.login(USER_LOGIN_CRED))
                .build();
        final NoSQLPublisher publisher = NoSQLPublisher.get(pubConf,
                                                            USER_LOGIN_CRED,
                                                            logger);
        trace("Publisher created with user=" + USER_LOGIN_CRED.getUsername());

        return publisher;
    }

    /**
     * Given a set of security parameter assignments, joined with a ';'
     * separator, configure the store to use the specified parameters.
     */
    private static void setAuthPolicy(String params, CreateStore createStore)
        throws RemoteException {

        final ParameterState.Info info = ParameterState.Info.GLOBAL;
        final ParameterState.Scope scope = ParameterState.Scope.STORE;
        final ParameterMap map = parseParams(params, info, scope);

        final CommandServiceAPI cs = createStore.getAdmin();

        final int planId =
            cs.createChangeGlobalSecurityParamsPlan("_SetPolicy", map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    private static ParameterMap parseParams(String params,
                                            ParameterState.Info info,
                                            ParameterState.Scope scope) {
        final ParameterMap map = new ParameterMap();
        final String type = ParameterState.GLOBAL_TYPE;
        map.setType(type);

        final String[] paramStrings = params.split(";");
        for (String paramString : paramStrings) {
            final String[] paramSplit = paramString.split("=");
            assertEquals(2, paramSplit.length);
            final String name = paramSplit[0].trim();
            final String value = paramSplit[1].trim();

            final ParameterState pstate = ParameterState.lookup(name);
            assertNotNull(pstate);
            assertFalse(pstate.getReadOnly());
            assertEquals(scope, pstate.getScope());
            assertTrue(pstate.appliesTo(info));

            /* This method will validate the value if necessary */
            map.setParameter(name, value);
        }
        return map;
    }

    private static int getUserTableKey(int which) {
        return which;
    }

    private RepNode getMaster(KVRepTestConfig conf, RepGroupId repGroupId) {

        for (RepNode rn : conf.getRNs()) {

            final RepNodeState state = rn.getMaster(repGroupId);
            if (state == null) {
                continue;
            }

            final RepNodeId repNodeId = state.getRepNodeId();
            if (repGroupId != null) {
                return conf.getRN(repNodeId);
            }
        }

        return null;
    }

    /* make a random row */
    protected RowImpl makeRandomRow(TableImpl table, int which) {

        RowImpl row = table.createRow();

        if (table.getFullName().equals(noiseTable.getFullName())) {
            row.put("id", getUserTableKey(which));
            row.put("text", UUID.randomUUID().toString());
        } else if (table.getFullName().equals(jokeTable.getFullName())) {
            row.put("id", which);
            row.put("category",
                    "type-" + ThreadLocalRandom.current().nextInt(1,10));
            row.put("text", "text" + UUID.randomUUID());
            row.put("humorQuotient",
                    "humor-" + ThreadLocalRandom.current().nextInt(1,100));
        } else if (table.getFullName().equals(userTable.getFullName())) {
            row.put("id", which);
            row.put("firstName",
                    "FirstName-" + ThreadLocalRandom.current().nextInt(1,1000));
            row.put("lastName",
                    "lastName-" + ThreadLocalRandom.current().nextInt(1,1000));
            row.put("age", ThreadLocalRandom.current().nextInt(20,80));
        }
        return row;
    }

    /* read test data from the store and verify */
    private void verifyTestData() {
        final KVStore s = KVStoreFactory.getStore(config.getKVSConfig());
        int counter = 0;

        Iterator<KeyValueVersion> iterator =
            s.storeIterator(Direction.UNORDERED, 1);
        while (iterator.hasNext()) {
            final KeyValueVersion kvv = iterator.next();
            Key k = kvv.getKey();
            Value v = kvv.getValue();
            verifyKV(k, v);
            counter++;
        }
        assertEquals("number of records mismatch!", NUM_INIT_KV_ENTRIES, counter);

        logger.info("All test data verified.");
    }

    /* verify k/v are in test data generated */
    private void verifyKV(Key k, Value v) {
        assertTrue("Unexpected key", testData.containsKey(k));
        Value expectedValue = testData.get(k);
        assertArrayEquals("Value mismatch!",
                          expectedValue.toByteArray(),
                          v.toByteArray());
    }

    /* create test data and store them by partition */
    private void createTestKVData() {
        testData = new HashMap<>(NUM_INIT_KV_ENTRIES);

        for (int i = 0; i < NUM_INIT_KV_ENTRIES; i++) {
            String keyStr = keyPrefix + i;
            String valueStr = valPrefix + (i * i);
            Key key = Key.createKey(keyStr);
            Value value = Value.createValue(valueStr.getBytes());
            testData.put(key, value);
        }

        logger.info("Test data with " + testData.size() + " partitions.");
    }

    protected void startStore() throws Exception {
        startStore(false);
    }

    protected void startStore(boolean useThread) throws Exception {

        createStore = new CreateStore(kvstoreName,
                                      13420, /* start port */
                                      numStorageNodes, /* n SNs */
                                      repFactor, /* rf */
                                      numPartitions, /* n partitions */
                                      1, /* capacity per SN */
                                      2 * CreateStore.MB_PER_SN,
                                      useThread, /* use threads is false */
                                      null);
        setPolicies(createStore);
        createStore.start();

        /*
         * Disable statistics gathering, so no need to remove all data of
         * its tables during cleanup.
         */
        changeRNParameter("rnStatisticsEnabled", "false");
    }

    protected Map<String, Row> createUserTable(KVStore storeHandle,
                                               int numRows) {
        final String ddl = "CREATE TABLE " + USER_TABLE_NAME +
                           "(id STRING, firstName STRING, lastName STRING, " +
                           "age INTEGER, PRIMARY KEY (id))";
        storeHandle.executeSync(ddl);

        final TableAPI api = storeHandle.getTableAPI();
        /* Ensure table created */
        assertNotNull("table=" + USER_TABLE_NAME + " not created",
                      api.getTable(USER_TABLE_NAME));

        /* load rows into table */
        final Map<String, Row> rows = new HashMap<>();
        final TableImpl table = (TableImpl) api.getTable(USER_TABLE_NAME);
        for (int i = 0; i < numRows; i++) {
            final RowImpl row = table.createRow();
            row.put("id", USER_TABLE_NAME + i);
            row.put("firstName", "user-first-" +
                    random.nextInt(1000));
            row.put("lastName", "user-last-" +
                                UUID.randomUUID().toString().substring(0, 4));
            row.put("age", random.nextInt(65));

            api.put(row, null,
                    new WriteOptions(Durability.COMMIT_NO_SYNC, 10000,
                                     MILLISECONDS));
            final String key = row.get("id").toString();
            rows.put(key, row);
        }
        return rows;
    }

    protected void createCustomerTable(KVStore storeHandle, int numRows) {
        final String ddl = "CREATE TABLE " + CUSTOMER_TABLE_NAME +
                           " (cid STRING, name STRING, PRIMARY KEY (cid))";
        storeHandle.executeSync(ddl);

        final TableAPI api = storeHandle.getTableAPI();
        /* Ensure table created */
        assertNotNull("table=" + CUSTOMER_TABLE_NAME +
                      " not created", api.getTable(CUSTOMER_TABLE_NAME));

        /* load rows into table */
        final TableImpl table = (TableImpl) api.getTable(CUSTOMER_TABLE_NAME);
        for (int i = 0; i < numRows; i++) {
            final RowImpl row = table.createRow();
            row.put("cid", CUSTOMER_TABLE_NAME + i);
            row.put("name", "customer-name-" + random.nextInt(1000));
            api.put(row, null,
                    new WriteOptions(Durability.COMMIT_NO_SYNC, 10000,
                                     MILLISECONDS));
        }
    }

    protected String dumpProperty(Properties prop) {
        StringBuilder sp = new StringBuilder();
        for (Object key : prop.keySet()) {
            sp.append("\n").append(key).append(":")
              .append(prop.getProperty((String) key));
        }
        return sp.toString();
    }

    /**
     * Builds security parameter from security config file
     *
     * @param disableClientAuth  true if disable client auth (normally used to
     *                           simulate an external client like publisher),
     *                           false to enable client auth, like RN in a
     *                           secure store.
     *
     * @return security parameters
     */
    protected SecurityParams getSecureParams(boolean disableClientAuth) {

        final File secDir = new File(TestUtils.getTestDir(),
                                     FileNames.SECURITY_CONFIG_DIR);
        final File securityConfigPath =
            new File(secDir, FileNames.SECURITY_CONFIG_FILE);

        if (!securityConfigPath.exists()) {
            throw new IllegalStateException(
                "The security configuration file " + securityConfigPath +
                " does not exist.");
        }

        final LoadParameters lp =
            LoadParameters.getParameters(securityConfigPath, logger);
        final SecurityParams sp = new SecurityParams(lp, securityConfigPath);


        if (disableClientAuth) {
            sp.setTransClientAuthRequired(
                sp.getTransportMap(SECURITY_TRANSPORT_JE_HA), false);
            sp.setKeystoreFile(null);
            sp.setKeystoreType(null);
            sp.setKeystoreSigPrivateKeyAlias(null);
            sp.setKeystorePasswordAlias(null);
        }


        trace("Dump JE ha security prop:");
        trace(dumpProperty(sp.getJEHAProperties()));

        return sp;
    }

    protected static String createRole(KVStore adminStore)
        throws Exception {
        final String role = "role" +
                            UUID.randomUUID().toString().substring(0, 4);
        execStatement(adminStore, "CREATE ROLE " + role);
        return role;
    }

    protected static void grantReadPrivTable(KVStore admin,
                                             String role,
                                             String table,
                                             String user) throws Exception {
        grantPrivToRole(admin, role, table, KVStorePrivilegeLabel.READ_TABLE);
        grantRoleToUser(admin, role, user);
    }

    protected static void grantWritePrivTable(KVStore admin,
                                              String role,
                                              String table,
                                              String user) throws Exception {
        grantPrivToRole(admin, role, table, KVStorePrivilegeLabel.INSERT_TABLE);
        grantRoleToUser(admin, role, user);

    }

    protected static String grantCreateAnyTablePriv(KVStore admin, String user)
        throws Exception {

        final String role = createRole(admin);
        grantPrivToRole(admin, role, KVStorePrivilegeLabel.CREATE_ANY_TABLE);
        grantRoleToUser(admin, role, user);
        return role;
    }

    protected static void grantWriteAnyTablePriv(KVStore admin, String user)
        throws Exception {

        final String role = createRole(admin);
        grantPrivToRole(admin, role, KVStorePrivilegeLabel.INSERT_ANY_TABLE);
        grantRoleToUser(admin, role, user);
    }

    protected static void grantReadAnyTablePriv(KVStore admin, String user)
        throws Exception {

        final String role = createRole(admin);
        grantPrivToRole(admin, role, KVStorePrivilegeLabel.READ_ANY_TABLE);
        grantRoleToUser(admin, role, user);
    }

    protected static void dropUser(KVStore admin, String user) throws Exception {
        execStatement(admin, "DROP USER " + user + " CASCADE");
    }

    protected static void grantPrivToRole(KVStore adminStore,
                                          String role,
                                          KVStorePrivilegeLabel label)
        throws Exception {
        execStatement(adminStore, "GRANT " + label + " TO " + role);
        assertRoleHasPriv(adminStore, role, label.toString());

    }

    private static void grantPrivToRole(KVStore adminStore,
                                        String role,
                                        String table,
                                        KVStorePrivilegeLabel label)
        throws Exception {

        execStatement(adminStore,
                      "grant " + label + " on " + table + " to " +
                      role);
        assertRoleHasPriv(adminStore, role, label.toString());
    }

    protected static String revokeWritePrivFromUser(KVStore adminStore,
                                                    String role,
                                                    String table)
        throws Exception {

        final KVStorePrivilegeLabel label = KVStorePrivilegeLabel.INSERT_TABLE;
        return revokePrivFromUser(adminStore, role, label, table);
    }

    private static String revokePrivFromUser(KVStore adminStore,
                                             String role,
                                             KVStorePrivilegeLabel label,
                                             String... tables)
        throws Exception {

        for (String t : tables) {
            execStatement(adminStore,
                          "revoke " + label + " on " + t + " from " + role);
            final String priv =  label.toString() + "(" + t + ")";
            assertRoleHasNoPriv(adminStore, role, priv);
        }

        grantRoleToUser(adminStore, role, USER);

        return role;
    }

    private static void assertRoleHasPriv(KVStore adminStore,
                                          String role,
                                          String privStr) {

        assertThat(showRole(adminStore, role), containsString(privStr));
    }

    protected static void assertRoleHasNoPriv(KVStore adminStore,
                                              String role,
                                              String privStr) {
        assertThat(showRole(adminStore, role), not(containsString(privStr)));
    }

    protected static String showRole(KVStore adminStore, String role) {
        final StatementResult result =
            adminStore.executeSync("show role " + role);
        return result.getResult();
    }

    protected static String showUsers(KVStore adminStore) {
        final StatementResult result = adminStore.executeSync("show users");
        return result.getResult();
    }

    protected static String showRoles(KVStore adminStore) {
        final StatementResult result =
            adminStore.executeSync("SHOW ROLES");
        return result.getResult();
    }

    protected static String showSingleUser(KVStore adminStore, String user) {
        final StatementResult result =
            adminStore.executeSync("SHOW USER " + user);
        return result.getResult();
    }

    protected static void grantRoleToUser(KVStore adminStore,
                                          String role,
                                          String usr) throws Exception {
        execStatement(adminStore, "grant " + role + " to user " + usr);
    }

    /**
     * Returns a parameter map with any non-default parameter that should be
     * set as the store policy.
     */
    protected ParameterMap getPolicyParameters() {
        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        return map;
    }

    private void setPolicies(CreateStore cstore) {
        cstore.setPolicyMap(getPolicyParameters());
    }

    private void changeRNParameter(String paramName, String paramValue)
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(paramName, paramValue);

        int p = createStore.getAdmin().
            createChangeAllParamsPlan("change rn parameter", null, map);
        createStore.getAdmin().approvePlan(p);
        createStore.getAdmin().executePlan(p, false);
        createStore.getAdmin().awaitPlan(p, 0, null);
    }

    /* Test subscriber which merely remembers anything it receives */
    protected abstract class TestNoSQLSubscriberBase
        implements NoSQLSubscriber {

        protected final NoSQLSubscriptionConfig conf;

        private NoSQLSubscription subscription;

        boolean isSubscribeSucc;

        protected List<StreamOperation> recvPutOps;
        protected List<StreamOperation> recvDelOps;
        protected final AtomicLong numDels;
        protected final AtomicLong numPuts;

        List<Throwable> recvErrors;
        List<Throwable> recvWarnings;

        Throwable causeOfFailure;

        protected TestNoSQLSubscriberBase(NoSQLSubscriptionConfig conf) {

            this.conf = conf;

            recvPutOps = new ArrayList<>();
            recvDelOps = new ArrayList<>();
            recvWarnings = new ArrayList<>();
            recvErrors = new ArrayList<>();

            numDels = new AtomicLong();
            numPuts = new AtomicLong();
            causeOfFailure = null;
            isSubscribeSucc = false;
        }

        @Override
        public NoSQLSubscriptionConfig getSubscriptionConfig() {
            return conf;
        }

        /**
         * Invoked by the NoSQL subscriber to get an instance of subscription
         * created by NoSQL Publisher.
         */
        public NoSQLSubscription getSubscription() {
            return subscription;
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = (NoSQLSubscription) s;
            isSubscribeSucc = true;
            trace("Publisher called onSubscribe for " + conf.getSubscriberId());
        }

        @Override
        public void onComplete() {

        }

        @Override
        public abstract void onNext(StreamOperation t);

        @Override
        public void onError(Throwable t) {
            recvErrors.add(t);

            if (t instanceof SubscriptionFailureException) {
                causeOfFailure = t.getCause();
            } else if (t instanceof PublisherFailureException) {
                causeOfFailure = t.getCause();
            } else {
                causeOfFailure = t;
            }

            isSubscribeSucc = false;
            trace(Level.INFO,
                  "Subscriber " + conf.getSubscriberId() +
                  " receives an error=" + t + "\n" +
                  ", cause=" + causeOfFailure +
                  "\nStack\n" +
                  LoggerUtils.getStackTrace(t));
        }

        @Override
        public void onWarn(Throwable t) {
            recvWarnings.add(t);
            trace(Level.INFO,
                  "Subscriber " + conf.getSubscriberId() +
                  " receives a warning=" + t);
        }

        @Override
        public void onCheckpointComplete(StreamPosition streamPosition,
                                         Throwable cause) {

        }

        public boolean isSubscriptionSucc() {
            return isSubscribeSucc;
        }

        public List<Throwable> getRecvErrors() {
            return recvErrors;
        }

        public Throwable getFailure() {
            return causeOfFailure;
        }

        public String getCauseOfFailure() {
            if (causeOfFailure == null) {
                return "no failure";
            }
            return causeOfFailure.getMessage() + "\n" +
                   LoggerUtils.getStackTrace(causeOfFailure);
        }

        public List<Throwable> getRecvWarnings() {
            return recvWarnings;
        }

        public long getNumDels() {
            return numDels.get();
        }

        public long getNumPuts() {
            return numPuts.get();
        }

        public List<StreamOperation> getRecvPutOps() {
            return recvPutOps;
        }

        public List<StreamOperation> getRecvDelOps() {
            return recvDelOps;
        }
    }
}
