/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.MDTableUtil;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.Statement;
import oracle.kv.stats.KVStats;
import oracle.kv.stats.OperationMetrics;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.util.CreateStore;
import oracle.kv.util.Load;
import oracle.kv.util.TableTestUtils;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.util.NioByteOutputStream;
import oracle.nosql.nson.values.JsonUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/*
 * Base class for table classes.  The @BeforeClass and @AfterClass methods
 * create and destroy a store respectively.  The store and utility methods
 * in this class can be used for test cases.
 *
 * The @Before and @After methods remove all tables and indexes to clean the
 * store.  That (1) exercises this code path and (2) leaves the store clean
 * for subsequent test cases.  These can be overridden if desired.
 */

public class TableTestBase extends TestBase {

    protected static CreateStore createStore;
    protected static TableAPIImpl tableImpl;
    protected static KVStore store;
    protected static ParameterMap policyMap;
    protected static final int SN_MEMORY_MB_DEFAULT = CreateStore.MB_PER_SN;
    protected static int snMemoryMB = SN_MEMORY_MB_DEFAULT;

    /*
     * Use this variable to avoid hangs in set up and tear down actions if
     * the store was not started successfully.
     */
    protected static boolean storeStarted;

    static int createStoreCount;
    static TableAPI restoreTableImpl;
    static KVStore rsStore;
    public static final int startPort = 13240;

    protected static final TableIteratorOptions countOpts =
        new TableIteratorOptions(Direction.UNORDERED,
                                 Consistency.ABSOLUTE,
                                 0, null, /* timeout, unit */
                                 0, 200); /* batch size 200 */

    CreateStore restoreStore;

    /*
     * Thread management for multi-threaded test cases.
     */
    List<Thread> threads;

    /*
     * Implement in extending classes to override the defaults.
     */
    protected String getNamespace() {
        return null;
    }

    int getNumStorageNodes() {
        return 3;
    }

    int getReplicationFactor() {
        return 3;
    }

    int getNumPartitions() {
        return 10;
    }

    int getCapacity() {
        return 1;
    }

    boolean getUseThreads() {
        return false;
    }

    /**
     * Default static setup. Creates a 3x3 store for testing. Subclasses can
     * define their own staticSetUp() method and call the protected
     * parameterized version to create different stores.
     */
    @BeforeClass
    public static void staticSetUp() throws Exception {
        staticSetUp(false /* excludeTombstone */);
    }

    public static void staticSetUp(boolean excludeTombstone) throws Exception {
        staticSetUp(excludeTombstone, false);
    }

    /**
     * Settings for multi-region table mode.
     *
     * @param excludeTombstone specifies whether to return tombstones for
     * methods in kvstore api.
     * @param separateMRStore specifies whether to use a separate KVStore to
     * mock stream agent.
     */
    public static void staticSetUp(boolean excludeTombstone,
                                   boolean separateMRStore) throws Exception {
        /*
         * Make a local 3x3 store.  This exercises the metadata
         * distribution code better than a 1x1 store.
         */
        staticSetUp(3, 3, 2, excludeTombstone, separateMRStore);
    }

    protected static void staticSetUp(int nSNs, int rf, int capacity)
        throws Exception {
        staticSetUp(nSNs, rf, capacity, false, false);
    }

    protected static void staticSetUp(int nSNs,
                                      int rf,
                                      int capacity,
                                      boolean excludeTombstone,
                                      boolean separateMRStore)
        throws Exception {
        staticSetUp(nSNs, rf, capacity, excludeTombstone, separateMRStore,
                    false);
    }

    /**
     * Sets up a store for testing.
     *
     * @param nSNs number of SNs
     * @param rf replication factor
     * @param capacity per SN capacity
     * @param excludeTombstone whether to return tombstones for methods in
     *        kvstore api. Set to true if KVstore api should not return the
     *        deleted records in multi-region table mode. For example, it should
     *        be set to true if a test needs to count and check the number
     *        of existing records in the store. In multi-region table
     *        mode, all table related unit tests will be run using multi-region
     *        tables, whose deleted records will become tombstones.
     *        The mode can be enabled by setting -Dtest.mrtable=true.
     * @param separateMRStore whether to use a separate KVStore to mock stream
     *        agent in the multi-region table mode. Set to true if tests need
     *        to check any status stored in the KVStore instance and the status
     *        can be affected by the extra TableAPI calls invoked by the mock
     *        agent.
     *
     */
    protected static void staticSetUp(int nSNs,
                                      int rf,
                                      int capacity,
                                      boolean excludeTombstone,
                                      boolean separateMRStore,
                                      boolean useThread)
            throws Exception {

        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        startStore(nSNs, rf, capacity, useThread);

        KVStoreConfig config = createKVConfig(createStore);
        config.setExcludeTombstones(excludeTombstone);
        store = KVStoreFactory.getStore(config);
        tableImpl = (TableAPIImpl) store.getTableAPI();
        if (separateMRStore) {
            TestBase.mrTableSetUp(KVStoreFactory.
                getStore(createKVConfig(createStore)));
        } else {
            TestBase.mrTableSetUp(store);
        }
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        if (store != null) {
            store.close();
        }

        if (createStore != null) {
            createStore.shutdown();
            storeStarted = false;
        }

        if (reqRespThread != null) {
            reqRespThread.stopResResp();
        }

        LoggerUtils.closeAllHandlers();
    }

    public static KVStore getStore() {
        return store;
    }

    @Override
    public void setUp()
        throws Exception {

        initThreadList();
        if (storeStarted) {
            removeAllTables();
            /*
             * Clear stats
             */
            store.getStats(true);
        }
    }

    @Override
    public void tearDown()
        throws Exception {

        cleanupThreads();
        if (storeStarted) {
            removeAllTables();
            /*
             * Clear stats
             */
            store.getStats(true);
        }
        if (restoreStore != null) {
            restoreStore.shutdown();
        }
    }

    /**
     * Restart the store if one is running.  This takes a few seconds, at least.
     */
    void restartStore()
        throws Exception {

        storeStarted = false;

        if (store != null) {
            store.close();
        }

        if (createStore == null) {
            fail("Cannot restart a store that isn't created");
        }
        createStore.shutdown();
        createStore.restart();

        if (restoreStore != null) {
            restoreStore.shutdown();
            restoreStore = null;
        }

        tableImpl = null;
        store = null;

        /*
         * A hack to deal with a Registry caching problem.
         */
        int nretries = 0;
        while (store == null) {
            try {
                store = KVStoreFactory.getStore(createKVConfig(createStore));
            } catch (Exception kve) {
                /* kve.getCause() == KVStoreException */
                if (++nretries == 10) {
                    throw kve;
                }
            }
        }
        tableImpl = (TableAPIImpl) store.getTableAPI();

        storeStarted = true;
    }

    protected static void startStore(int nSNs, int rf, int capacity)
        throws Exception {
        startStore(nSNs, rf, capacity, false);
    }

    protected static void startStore(int nSNs, int rf, int capacity,
                                     boolean useThread)
        throws Exception {

        createStoreCount++;
        createStore = new CreateStore("kvtest-" +
                                      testClassName +
                                      "-tablestore-" + createStoreCount,
                                      startPort,
                                      nSNs, rf,
                                      10, /* n partitions */
                                      capacity,
                                      capacity * snMemoryMB,
                                      useThread, /* use threads is false */
                                      null);
        setPolicies(createStore);
        createStore.start();

        /*
         * Disable statistics gathering, so no need to remove all data of
         * its tables during cleanup.
         */
        changeRNParameter("rnStatisticsEnabled", "false");

        storeStarted = true;
    }

    /*
     * Non-static version of startStore that uses subclass overrides. It uses
     * some static code for cleanup.  This method can be used by test cases
     * that want a non-default store.  It should not be used much as it adds
     * time to the tests.
     *
     * TODO: look at adding stop/start store capability without destroying data
     */
    void startStoreDynamic()
        throws Exception {

        staticTearDown(); /* clean up first */
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      getNumStorageNodes(),
                                      getReplicationFactor(),
                                      getNumPartitions(),
                                      getCapacity(),
                                      getCapacity() * snMemoryMB,
                                      getUseThreads(),
                                      null);
        setPolicies(createStore);
        createStore.start();

        store = KVStoreFactory.getStore(createKVConfig(createStore));
        tableImpl = (TableAPIImpl) store.getTableAPI();
        storeStarted = true;
    }

    /**
     * Creates a KVStoreConfig for use in tests.
     */
    public static KVStoreConfig createKVConfig(CreateStore cs) {
        final KVStoreConfig config = cs.createKVConfig();
        config.setDurability
            (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.ReplicaAckPolicy.ALL));
        return config;
    }

    Thread startThread(Runnable r) {
        final Thread thread = new Thread(r);
        threads.add(thread);
        thread.start();
        return thread;
    }

    void joinAllThreads() {
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (Exception e) {}
        }
    }

    private void initThreadList() {
        if (threads != null) {
            cleanupThreads();
        }
        threads = new ArrayList<>();
    }

    protected void cleanupThreads() {
        if (threads == null) {
            return;
        }
        for (Thread thread : threads) {
            thread.interrupt();
        }
        joinAllThreads();
        threads.clear();
    }

    protected TableImpl addTable(TableImpl table) throws Exception {
        return addTable(table, true);
    }

    protected TableImpl addTable(TableImpl table, boolean shouldSucceed)
        throws Exception {
        return addTable(table, shouldSucceed, false);
    }

    protected TableImpl addTable(TableImpl table,
                                 boolean shouldSucceed,
                                 boolean noMRTable)
        throws Exception {

        addTable(table, shouldSucceed, getNamespace(), noMRTable);
        if (shouldSucceed) {
            return getTable(table.getInternalNamespace(), table.getFullName());
        }
        return null;
    }

    protected void removeTable(TableImpl table, boolean shouldSucceed)
        throws Exception {

        removeTable(table.getInternalNamespace(), table.getFullName(),
            shouldSucceed);
    }

    void evolveTable(TableEvolver evolver,
                     boolean shouldSucceed)
        throws Exception {

        evolveTable(evolver, shouldSucceed, createStore.getAdmin());
    }

    protected void removeTable(String namespace,
                               String tableName,
                               boolean shouldSucceed)
        throws Exception {

        removeTable(namespace, tableName, shouldSucceed,
                    createStore.getAdmin());
    }

    private void removeAllTables()
        throws Exception {

        /* Cannot use tableImpl because it may be closed by the test */
        final TableAPI tableAPI = store.getTableAPI();
        TableTestUtils.removeTables(tableAPI.getTables(),
                                    createStore.getAdmin(),
                                    store);
        if (countStoreTables(tableAPI) != 0) {
            TableTestUtils.removeTables(tableAPI.getTables(),
                                        createStore.getAdmin(),
                                        store);
        }
        waitForAllTablesRemoved(tableAPI);
        assertEquals("Store tables count", 0, countStoreTables(tableAPI));
    }

    private void waitForAllTablesRemoved(TableAPI tableAPI) throws TimeoutException {
        final long timeoutMs = 60 * 1000;
        final boolean success = new PollCondition(1000, 60 * 1000) {
            @Override
            protected boolean condition() {
                return countStoreTables(tableAPI) == 0;
            }
        }.await();
        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in wait for table to " +
                                       "be removed in time(ms)=" + timeoutMs +
                                       ", expect 0 but get=" +
                                       countStoreTables(tableAPI));
        }
    }

    /**
     * Waits for the store to be ready. If forTables is true, also waits
     * for all system tables to be created and the MD system table populated.
     * See comments at top of file.
     */
    public static void waitForStoreReady(CommandServiceAPI cs,
                                         final boolean forTables) {
        assertTrue(
            "Waiting for store to be ready"
                + (forTables ? " for table operations" : ""),
            MDTableUtil.waitForStoreReady(cs, forTables, 1000, 60000));
    }

    TableImpl evolveAndGet(TableEvolver evolver) {
        try {
            evolveTable(evolver, true);
            final TableImpl table = evolver.getTable();
            return getTable(table.getFullName());
        } catch (Exception e) {
            throw new IllegalStateException("Evolve failed: " + e, e);
        }
    }

    /**
     * Gets the named table using the public API.
     */
    protected TableImpl getTable(String tableName) {
        return getTable(null, tableName);
    }

    /**
     * Gets a table from the specified namespace (which can be null)
     */
    TableImpl getTable(String namespace, String tableName) {
        return (TableImpl) tableImpl.getTable(namespace, tableName);
    }

    /**
     * Gets the specified index from the table using the public API.
     * Returns null if either the table or index does not exist.
     */
    IndexImpl getIndex(String tableName, String indexName) {
        return getIndex(null, tableName, indexName);
    }

    /**
     * Gets the specified index from the table using the specified
     * namespace.
     *
     * Returns null if either the table or index does not exist.
     */
    IndexImpl getIndex(String namespace,
                       String tableName,
                       String indexName) {
        final Table table = tableImpl.getTable(namespace, tableName);
        if (table != null) {
            return (IndexImpl) table.getIndex(indexName);
        }
        return null;
    }

    protected TableImpl addIndex(TableImpl table, String indexName,
                                 String[] indexFields, boolean shouldSucceed)
        throws Exception {

        addIndex(table, indexName, indexFields,
                 shouldSucceed, createStore.getAdmin());
        return getTable(table.getFullName());
    }

    void removeIndex(TableImpl table, String indexName,
                     boolean shouldSucceed)
        throws Exception {

        removeIndex(table.getInternalNamespace(),
                    table.getFullName(), indexName, shouldSucceed,
                    createStore.getAdmin());
    }

    void removeIndex(String namespace,
                     String tableName, String indexName,
                     boolean shouldSucceed)
        throws Exception {

        TableTestUtils.removeIndex(namespace, tableName,
                                   indexName, shouldSucceed,
                                   createStore.getAdmin(),
                                   store);
    }

    public PreparedStatement prepare(String statement) {
        return store.prepare(
            statement, new ExecuteOptions().setNamespace(getNamespace(), false));
    }

    public PreparedStatement prepare(String statement, ExecuteOptions options) {
        return store.prepare(statement, options);
    }

    public StatementResult executeDml(String statement) {
        return executeDml(statement, new ExecuteOptions());
    }

    public StatementResult executeDml(String statement,
                                      ExecuteOptions options) {
        return store.executeSync(
            statement, options.setNamespace(getNamespace(), false));
    }

    public StatementResult executeDml(String statement,
                                      PrepareCallback callback,
                                      String namespace) {
        return store.executeSync(
            statement,
            new ExecuteOptions()
            .setNamespace(namespace, false)
            .setPrepareCallback(callback));
    }

    public StatementResult executeDml(String statement,
                                      String namespace) {
        return store.executeSync(
            statement, new ExecuteOptions().setNamespace(namespace, false));
    }

    public StatementResult executeStatement(Statement statement) {
        return store.executeSync(
            statement, new ExecuteOptions().setNamespace(getNamespace(), false));
    }

    public void executeDdl(String statement) {
        executeDdl(statement, getNamespace());
    }

    void executeChildDdl(String statement) {
        ExecuteOptions options = null;
        if (getNamespace() != null) {
            options = new ExecuteOptions().setNamespace(getNamespace(), false);
        }
        executeDdl(statement, options,
                   true /* shouldSucceed */,
                   false/* noMRTable */,
                   true /* child table */,
                   store);
    }

    public void executeDdl(String statement, String namespace) {
        executeDdl(statement, namespace, true /* shouldSucceed */);
    }

    public void executeDdl(String statement,
                           String namespace,
                           boolean shouldSucceed) {
        executeDdl(statement, namespace, shouldSucceed, false /* noMRTable */);
    }

    public void executeDdl(String statement, boolean shouldSucceed) {
        executeDdl(statement, getNamespace(), shouldSucceed, false);
    }

    public void executeDdl(String statement,
                           boolean shouldSucceed,
                           boolean noMRTableMode) {
        executeDdl(statement, getNamespace(), shouldSucceed,
                   noMRTableMode);
    }

    public static void executeDdl(String statement,
                                  String namespace,
                                  boolean shouldSucceed,
                                  boolean noMRTableMode) {
        final ExecuteOptions options =
            (namespace != null ?
             new ExecuteOptions().setNamespace(namespace, false) : null);
        executeDdl(statement, options, shouldSucceed, noMRTableMode,
                   false/* parent table*/, store);
    }

    protected static void executeDdl(String statement,
                                     ExecuteOptions options,
                                     boolean shouldSucceed,
                                     boolean noMRTableMode,
                                     boolean childTable,
                                     KVStore kvStore) {

        try {
            if (!noMRTableMode && !childTable /* no regions for child MR */) {
                statement = TestBase.addRegionsForMRTable(statement);
            }
            final StatementResult res =
                ((KVStoreImpl)kvStore).executeSync(statement, options);
            if (shouldSucceed) {
                assertTrue("Statement should have succeeded: " + statement,
                           res.isSuccessful());
            } else {
                assertTrue("Statement should have failed: " + statement,
                           !res.isSuccessful());
            }
        } catch (IllegalArgumentException | FaultException iae) {
            if (shouldSucceed) {
                fail("Statement failed to compile or execute: " + statement +
                     "\n" + iae);
            }
        }
    }

    public void executeDdlErrContainsMsg(String statement,
                                         String msg) {
        try {
            store.executeSync(statement,
                              new ExecuteOptions().
                              setNamespace(getNamespace(), false));
            fail("Statement should have failed to compile or execute");
        } catch (IllegalArgumentException | FaultException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                       e.getMessage().contains(msg));
        }
    }

    /* Default policy map.
     * For now:
     * 1.  change admin index check time to reduce time to create an index
     * (default is 10 s, which slows things down a lot when there isn't any,
     * or much data).
     **/
    static ParameterMap makePolicyMap() {
        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");

        /*
         * Related to statistics gathering, to reduce duration when
         * scanning.
         */
        map.setParameter(ParameterState.RN_SG_ENABLED, "false");
        map.setParameter(ParameterState.RN_SG_INTERVAL, "1 s");
        map.setParameter(ParameterState.RN_SG_LEASE_DURATION, "1 s");
        map.setParameter(ParameterState.RN_SG_SLEEP_WAIT, "5 ms");
        map.setParameter(ParameterState.COMMON_HIDE_USERDATA, "false");
        /* set hiding state for current JVM */
        UserDataControl.setKeyHiding(false);
        UserDataControl.setValueHiding(false);

        return map;
    }

    // CRC: Rather than setting a protected field to override the default, it
    // would be better for subclasses to override the makePolicyMap method.
    /*
     * Set store policies via a ParameterMap.  Subclasses that wish to set
     * their own policies before the store is created may initialize policyMap
     * variable before staticSetUp() is called. This will allow to create
     * store with customized policies for the whole test class (not just one
     * test, as in startStoreDynamic()).
     * Note that overriding setPolicies() in subclass will not work since
     * static methods are resolved at compile time.
     */
    static void setPolicies(CreateStore cstore) {
        ParameterMap map = policyMap;
        if (map == null) {
            map = makePolicyMap();
        }
        cstore.setPolicyMap(map);
    }

    static void addTable(TableImpl table,
                         boolean shouldSucceed,
                         String namespace) throws Exception {
        addTable(table, shouldSucceed, namespace, false);
    }

    public static void addTable(TableImpl table,
                                boolean shouldSucceed,
                                String namespace,
                                boolean noMRTableMode) throws Exception {
        addTable(table, shouldSucceed, namespace, noMRTableMode, store);
    }

    public static void addTable(TableImpl table,
                                boolean shouldSucceed,
                                String namespace,
                                boolean noMRTableMode,
                                KVStore kvStore)
        throws Exception {
        final TableAPIImpl tblAPI = (TableAPIImpl) kvStore.getTableAPI();
        final RegionMapper rm = tblAPI.getRegionMapper();
        DDLGenerator ddlGen = new DDLGenerator(table, false, rm);
        ExecuteOptions options = new ExecuteOptions()
                .setNamespace(namespace, false);
        executeDdl(ddlGen.getDDL(), options, shouldSucceed,
                   noMRTableMode, false /* childTable */, kvStore);
    }

    static void setTableLimits(TableImpl table, TableLimits limits)
            throws Exception {
        setTableLimits(table, limits, true);
    }

    static void setTableLimits(TableImpl table, TableLimits limits,
                               boolean shouldSucceed)
            throws Exception {
        CommandServiceAPI cs = createStore.getAdmin();
        try {
            final int planId = cs.createTableLimitPlan("SetTableLimits",
                table.getInternalNamespace(),
                table.getName(),
                limits);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            State state = cs.awaitPlan(planId, 0, null);
            TableTestUtils.clearTableCache(store);
            if (shouldSucceed) {
                assertTrue(state == State.SUCCEEDED);
            } else {
                assertTrue(state == State.ERROR);
            }
        } catch (AdminFaultException afe) {
            if (shouldSucceed) {
                fail("setTableLimits failed: " + afe);
            }
        }
    }

    static TableMetadata getTableMetadata(CreateStore st)
        throws Exception {

        return getTableMetadata(st.getAdmin());
    }

    private static TableMetadata getTableMetadata(CommandServiceAPI cs)
        throws Exception {

        return cs.getMetadata(TableMetadata.class,
                              MetadataType.TABLE);
    }

    static void addTable(TableBuilder builder,
                         boolean shouldSucceed)
        throws Exception {
        addTable(builder, shouldSucceed, false);
    }

    static void addTable(TableBuilder builder,
                         boolean shouldSucceed,
                         boolean noMRTable)
        throws Exception {

        TableTestUtils.addTable(builder, shouldSucceed, noMRTable, store);
    }

    static void evolveTable(TableEvolver evolver,
                            boolean shouldSucceed,
                            CommandServiceAPI cs)
        throws Exception {

        TableTestUtils.evolveTable(evolver, shouldSucceed, cs, store);
    }

    static void removeTable(String namespace, String tableName,
                            boolean shouldSucceed,
                            CommandServiceAPI cs)
        throws Exception {

        TableTestUtils.removeTable(namespace, tableName, shouldSucceed, cs,
                                   store);
    }

    static void addIndex(TableImpl table, String indexName,
                         String[] indexFields, boolean shouldSucceed,
                         CommandServiceAPI cs)
        throws Exception {

        TableTestUtils.addIndex(table, indexName, indexFields,
                                shouldSucceed, cs, store);
    }

    static void removeIndex(String namespace, String tableName,
                            String indexName,
                            boolean shouldSucceed,
                            CommandServiceAPI cs)
        throws Exception {

        TableTestUtils.removeIndex(namespace, tableName,
                                   indexName, shouldSucceed, cs, store);
    }

    static ExecutionFuture addTableAsync(TableImpl table,
                                         boolean shouldSucceed)
        throws Exception {
        try {
            final TableAPIImpl tblAPI = (TableAPIImpl) store.getTableAPI();
            final RegionMapper rm = tblAPI.getRegionMapper();
            DDLGenerator ddlGen = new DDLGenerator(table, false, rm);
            return store.execute(
                addRegionsForMRTable(ddlGen.getDDL()));
        } catch (IllegalArgumentException ice) {
            if (shouldSucceed) {
                fail("addTableAsync failed: " + ice);
            }
        }
        return null;
    }

    static int removeTableAsync(final String tableName,
                                boolean shouldSucceed,
                                CommandServiceAPI cs)
        throws Exception {

        try {
            final int planId = cs.createRemoveTablePlan("RemoveTable",
                                                        null,
                                                        tableName);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            if (!shouldSucceed) {
                fail("removeTableAsync should have failed");
            }
            return planId;
        } catch (AdminFaultException ice) {
            if (shouldSucceed) {
                fail("removeTableAsync failed: " + ice);
            }
        }
        return 0;
    }

    static int addIndexAsync(TableImpl table, String indexName,
                             String[] indexFields, boolean shouldSucceed,
                             CommandServiceAPI cs)
        throws Exception {

        try {
            final int planId = cs.createAddIndexPlan("AddIndex",
                                                     null,
                                                     indexName,
                                                     table.getFullName(),
                                                     indexFields,
                                                     null, /* types */
                                                     true, /*indexNulls*/
                                                     null);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            if (!shouldSucceed) {
                fail("addIndexAsync should have failed");
            }
            return planId;
        } catch (AdminFaultException ice) {
            if (shouldSucceed) {
                ice.printStackTrace();
                fail("addIndexAsync failed");
            }
        }
        return 0;
    }

    static void waitForPlan(int planId,
                            boolean shouldSucceed,
                            CommandServiceAPI cs)
        throws Exception {

        try {
            final State state = cs.awaitPlan(planId, 50, TimeUnit.SECONDS);
            TableTestUtils.clearTableCache(store);
            if (shouldSucceed) {
                assertEquals("Plan " + planId, State.SUCCEEDED, state);
                /*
                 * Test metadata interface on CommandService.  This adds a
                 * little overhead.
                 */
                final TableMetadata meta = getTableMetadata(cs);
                assertNotNull(meta);
            } else {
                assertEquals("Plan " + planId, State.ERROR, state);
            }
        } catch (AdminFaultException ice) {
            if (shouldSucceed) {
                fail("waitForPlan failed: " + ice);
            }
        }
    }

    static int countRecords(StatementResult sr) {
        assertTrue(sr.isSuccessful());
        final TableIterator<RecordValue> iter = sr.iterator();
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        iter.close();
        return count;
    }

    /**
     * Count records -- keys only
     */
    static int countTableRecords(PrimaryKey key, Table target) {
        MultiRowOptions mro = null;
        if (target != null && !key.getTable().equals(target)) {
            mro = new MultiRowOptions(null, null, Arrays.asList(target));
        }
        return countTableRecords1(key, mro);
    }

    static int countTableRecords1(PrimaryKey key, MultiRowOptions mro) {
        final TableIterator<PrimaryKey> iter =
            tableImpl.tableKeysIterator(key, mro, countOpts);
        int count = 0;
        while (iter.hasNext()) {
            key = iter.next();
            ++count;
        }
        iter.close();
        return count;
    }

    /**
     * Count rows -- this reads values, not just keys.
     */
    static int countTableRows(PrimaryKey key, Table target) {
        MultiRowOptions mro = null;
        if (target != null && !key.getTable().equals(target)) {
            mro = new MultiRowOptions(null, null, Arrays.asList(target));
        }
        return countTableRows1(key, mro);
    }

    static int countTableRows1(PrimaryKey key, MultiRowOptions mro) {
        final TableIterator<Row> iter =
            tableImpl.tableIterator(key, mro, countOpts);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        iter.close();
        return count;
    }

    static int countIndexRecords(IndexKey key, Table target) {
        MultiRowOptions mro = null;
        if (target != null && !key.getIndex().getTable().equals(target)) {
            mro = new MultiRowOptions(null, null, Arrays.asList(target));
        }
        return countIndexRecords(key, mro, null);
    }

    static int countIndexRecords1(IndexKey key,
                                  MultiRowOptions mro) {
        return countIndexRecords(key, mro, null);
    }

    static int countIndexRecords(IndexKey key,
                                 MultiRowOptions mro,
                                 TableIteratorOptions iterateOptions) {
        final TableIterator<KeyPair> iter =
                        tableImpl.tableKeysIterator(key, mro, iterateOptions);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        iter.close();
        return count;
    }

    /**
     * Like countIndexRecords but materialize values too.
     */
    static int countIndexRows(IndexKey key, Table target) {

        MultiRowOptions mro = null;
        if (target != null && !key.getIndex().getTable().equals(target)) {
            mro = new MultiRowOptions(null, null, Arrays.asList(target));
        }
        return countIndexRows1(key, mro);
    }

    static int countIndexRows1(IndexKey key, MultiRowOptions mro) {
        final TableIterator<Row> iter = tableImpl.tableIterator(key, mro, null);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        iter.close();
        return count;
    }

    static int countStoreRecords() {
        return countStoreRecords(store);
    }

    static int countStoreRecords(KVStore st) {
        final Iterator<Key> iter =
            st.storeKeysIterator(Direction.UNORDERED, 500,
                                 null, null, null,
                                 Consistency.ABSOLUTE,
                                 0, null); /* default timeout */
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        return count;
    }

    /*
     * System tables cannot be removed externally, so eliminate them when
     * counting number of store tables.
     */
    static int countStoreTables(TableAPI tableAPI) {
        int total = 0;
        for (Table table : tableAPI.getTables().values()) {
            if (!((TableImpl)table).isSystemTable()) {
                total++;
            }
        }
        return total;
    }

    public static List<String> makeIndexList(String ... fields) {
        return new ArrayList<>(Arrays.asList(fields));
    }

    public static List<FieldDef.Type> makeIndexTypeList(
        FieldDef.Type ... types) {
        return new ArrayList<>(Arrays.asList(types));
    }

    public static List<AnnotatedField>
                    makeTextIndexList(AnnotatedField ... fields) {
        return new ArrayList<>(Arrays.asList(fields));
    }


    /**
     * Convenience utility to allow tests to create String keys from
     * fields.
     */
    static String formatForKey(String fieldName,
                               RecordValue record,
                               TableImpl table) {
        return ((FieldValueImpl)record.get(fieldName))
            .formatForKey(table.getField("fieldName"));
    }

    /**
     * Format KVStats to a String for display.  Used for debugging.
     */
    String metricsToString() {
        final KVStats stats = store.getStats(false);
        final StringBuilder sb = new StringBuilder();
        final List<OperationMetrics> ops = stats.getOpMetrics();
        sb.append("Operations:");
        for (OperationMetrics op : ops) {
            /*
             * Only display operations that happened.
             */
            if (op.getTotalOpsLong() > 0) {
                sb.append("\n\t");
                sb.append(op);
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    /*
     * Test utilities for handling snapshots and store load
     */
    String createSnapshot(String name)
        throws Exception {

        final Snapshot snapshot =
                new Snapshot(createStore.getAdmin(), false, null);
        return snapshot.createSnapshot(name);
    }

    /**
     * Returns a list of snapshot directories, one per RepGroup (shard).
     */
    List<File> getRepNodeSnapshotDirs(String snapshotName) {
        final ArrayList<File> list = new ArrayList<>();
        final Set<String> groups = new HashSet<>();

        for (StorageNodeId snid : createStore.getStorageNodeIds()) {
            final List<RepNodeId> rns = createStore.getRNs(snid);
            for (RepNodeId rnid : rns) {
                final String group = rnid.getGroupName();
                if (groups.contains(group)) {
                    continue;
                }
                groups.add(group);
                list.add(new File
                         (FileNames.getSnapshotDir(createStore.getRootDir(),
                                                   createStore.getStoreName(),
                                                   null,
                                                   snid,
                                                   rnid), snapshotName));
            }
        }
        return list;
    }

    /**
     * Returns the directory that contains admin snapshot for the first admin
     * found.  If that admin's id is not "Admin1" this will fail.  There is no
     * current mechanism to get the AdminId.  If this test starts failing
     * something should be added to CreateStore.
     */
    File getAdminSnapshotDir(String snapshotName) {
        StorageNodeId adminSnid = null;
        for (StorageNodeId snid : createStore.getStorageNodeIds()) {
            if (createStore.hasAdmin(snid)) {
                adminSnid = snid;
                break;
            }
        }
        assertNotNull(adminSnid);
        return new File
            (FileNames.getSnapshotDir(createStore.getRootDir(),
                                      createStore.getStoreName(),
                                      null,
                                      adminSnid,
                                      new AdminId(1)), snapshotName);
    }

    /**
     * Starts a second CreateStore instance to be used as a "restore"
     * store for backup testing.  It's a simple 1x1 vs 3x3.
     */
    CreateStore startRestoreStore()
        throws Exception {

        restoreStore = new CreateStore(kvstoreName + "-restorestore",
                                       startPort + 100,
                                       1, /* n SNs */
                                       1, /* rf */
                                       10, /* n partitions */
                                       1, /* capacity per SN */
                                       snMemoryMB,
                                       false, /* use threads is false */
                                       null);
        setPolicies(restoreStore);
        final File rootDir = new File(createStore.getRootDir(), "restore");
        assertTrue(rootDir.mkdir());
        restoreStore.setRootDir(rootDir.toString());
        restoreStore.start();
        rsStore = KVStoreFactory.getStore(createKVConfig(restoreStore));
        restoreTableImpl = rsStore.getTableAPI();

        return restoreStore;
    }

    void loadToRestoreStore(String snapshotName)
        throws Exception {

        final File adminEnv = getAdminSnapshotDir(snapshotName);
        Load.loadAdmin(adminEnv,
                       restoreStore.getHostname(),
                       restoreStore.getRegistryPort(),
                       restoreStore.getDefaultUserName(), /* user */
                       /* security file */
                       restoreStore.getDefaultUserLoginPath(),
                       false, /* verbose */
                       true, /* force */
                       TestUtils.NULL_PRINTSTREAM);
        final List<File> snapshortDirs = getRepNodeSnapshotDirs(snapshotName);
        final Load loadShard =
                new Load(snapshortDirs.toArray(new File[snapshortDirs.size()]),
                         restoreStore.getStoreName(),
                         restoreStore.getHostname(),
                         restoreStore.getRegistryPort(),
                         restoreStore.getDefaultUserName(), /* user */
                         /* security file */
                         restoreStore.getDefaultUserLoginPath(),
                         null, /* status file */
                         false, /* verbose */
                         TestUtils.NULL_PRINTSTREAM);
        loadShard.run();
    }

    static boolean compareStoreTableMetadata(CreateStore store1,
                                             CreateStore store2)
        throws Exception {

        final TableMetadata md1 = getTableMetadata(store1);
        final TableMetadata md2 = getTableMetadata(store2);
        return md1.compareMetadata(md2);
    }

    static boolean compareStoreKVData(KVStore store1, KVStore store2) {
        final int c1 = countStoreRecords(store1);
        final int c2 = countStoreRecords(store2);
        if (c1 == c2) {
            final Iterator<KeyValueVersion> iter =
                store1.storeIterator(Direction.UNORDERED, 500);
            while (iter.hasNext()) {
                final KeyValueVersion kvv = iter.next();
                final ValueVersion vv = store2.get(kvv.getKey());

                /* Note: you can't compare Version across stores */
                if (vv == null || !vv.getValue().equals(kvv.getValue())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static void changeRNParameter(String paramName, String paramValue)
        throws RemoteException {

        final ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(paramName, paramValue);

        final int p = createStore.getAdmin().
            createChangeAllParamsPlan("change rn parameter", null, map);
        createStore.getAdmin().approvePlan(p);
        createStore.getAdmin().executePlan(p, false);
        createStore.getAdmin().awaitPlan(p, 0, null);
    }

    /*
     * test round-trip to/from JSON. Do it twice to be sure
     */
    static void roundTripTable(TableImpl table) {
        final String json = table.toJsonString(false);
        final TableImpl newTable = TableJsonUtils.fromJsonString(json, null);
        final String newJson = newTable.toJsonString(false);
        final TableImpl newTT = TableJsonUtils.fromJsonString(newJson, null);
        assertTrue(table.equals(newTT));
    }

    /*
     * Useful for debugging
     */
    static void displayIndexKey(IndexKey ikey) {
        System.out.println("Field names in index key:");
        for (String s : ikey.getFieldNames()) {
            System.out.println("\t" + s);
        }
        System.out.println("IndexKey: " + ikey);
    }

    static void assertFieldAbsent(RecordValue row, String fieldName) {
        try {
            row.get(fieldName);
            fail("Field should not exist: " + fieldName);
        } catch (IllegalArgumentException iae) {
        }
    }

    static void displayIndexFields(Index index) {
        System.out.println("Field names in index " + index.getName());
        for (IndexImpl.IndexField field : ((IndexImpl)index).getIndexFields()) {
            System.out.println("\t" + field.getPathName());
        }
    }

    static String displayBytes(byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append("[");
            sb.append(Integer.toHexString(0xFF & b));
            sb.append("]");
        }
        return sb.toString();
    }

    static String createLongName(char ch, int length) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(ch);
        }
        return sb.toString();
    }

    /*
     * Compare the primary keys extracted from the rows and return the
     * equivalent of compareTo() based on those objects.
     */
    static int compareKeys(Row row1, Row row2) {
        return ((RowImpl)row1).compareKeys(row2);
    }

    public void createTable(String statement,
                            TableLimits limits) throws Exception {
        createTable(statement, limits, false);
    }

    /*
     * Executes a create table statement with limits. It waits
     * indefinitely and expects to succeed.
     */
    public void createTable(String statement,
                            TableLimits limits,
                            boolean noMRTable) throws Exception {
        if (!noMRTable) {
            statement = TestBase.addRegionsForMRTable(statement);
        }
        ExecutionFuture future = ((KVStoreImpl)store)
            .execute(statement.toCharArray(), null, limits);
        StatementResult res = future.get();
        assertTrue(res.isSuccessful());
    }

    /**
     * This method updates or adds the supplied config in je config properties.
     * If the config already exists, it is replaced. Otherwise the new config
     * is appended to existing config properties.
     *
     * @param regexForConfig regular expression for config param.
     * @param newConfigStr string representing new config
     */
    public static void modifyJEConfigProperties(String regexForConfig,
                                                String newConfigStr) {
        CommandServiceAPI cs = createStore.getAdmin();
        try {
            Topology t = cs.getTopology();

            /* Check the setting of the one RepNode in this kvstore */
            RepGroupMap groupMap = t.getRepGroupMap();

            RepNodeId repNodeId = null;
            for (RepGroup rg : groupMap.getAll()) {
                for (RepNode rn : rg.getRepNodes()) {
                    repNodeId = rn.getResourceId();
                    break;
                }
            }

            Parameters params = cs.getParameters();

            RepNodeParams repParams = params.get(repNodeId);

            String jeMiscVal = repParams.getConfigProperties();

            String jeMiscNewVal;

            if (jeMiscVal == null || jeMiscVal.isEmpty()) {
                jeMiscNewVal = newConfigStr + ";";
            } else if (jeMiscVal.matches("(.)*" + regexForConfig + "(.)*")) {
                jeMiscNewVal =
                    jeMiscVal.replaceAll(regexForConfig, newConfigStr);
            } else {
                jeMiscNewVal = jeMiscVal + newConfigStr + ";";
            }

            changeRNParameter(ParameterState.JE_MISC, jeMiscNewVal);

        } catch (RemoteException remoteEx) {
            fail("The command service should have stayed up and available " +
                " but got " + remoteEx);
        }
    }

    public static RecordValueImpl createValueFromNson(TableImpl table,
                                                      byte[] value,
                                                      String msg) {
        ValueReader<RowImpl> reader = table.initRowReader(null);
        RecordValueImpl val = null;
        try {
            Value v =
                NsonUtil.createValueFromNsonBytes(table, value, 0, false);
            byte[] vbytes = v.getValue();
            table.initRowFromByteValue(reader, vbytes, table,
                                       Value.Format.TABLE, 0);
            val = reader.getValue();
        } catch (Exception e) {
            fail("Failed to create record from NSON: " + msg);
        }
        return val;
    }

    public static byte[] createNsonFromJson(String json) throws Exception {
        oracle.nosql.nson.values.FieldValue val =
            JsonUtils.createValueFromJson(json, null);
        byte[] nval = null;
        try (NioByteOutputStream bos = new NioByteOutputStream(500, false)) {
            Nson.writeFieldValue(bos, val);
            nval = Arrays.copyOfRange(bos.array(), 0, bos.getOffset());
        }
        return nval;
    }

}
