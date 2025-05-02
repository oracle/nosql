/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.parallelscan;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.ParallelScanIterator;
import oracle.kv.PasswordCredentials;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.DeployUtils;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.util.CreateStore;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests Parallel Scan version of storeIterate and storeKeysIterator.
 */
public class ParallelScanTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private CreateStore createStore;
    private StoreUtils su;
    private Set<Key> keys;

    private KVStore store;
    private int hostPort;
    private final int expectedNumRecords = 500;
    private final int partitions = 100;
    private final String testUser = "root";
    private final String userPassword = "NoSql00__1234567";

    @Override
    public void setUp()
        throws Exception {

        /* Allow write-no-sync durability for faster test runs. */
        TestStatus.setWriteNoSyncAllowed(true);
        super.setUp();
        TestUtils.clearTestDirectory();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (su != null) {
            su.close();
        }

        if (store != null) {
            store.close();
        }

        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
            sysAdminInfo = null;
        }

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
        LoggerUtils.closeAllHandlers();
    }

    /* Put all tests under one unit test to avoid repeated setup and teardown */
    @Test
    public void testRunParallelScanTests()
        throws Exception {
        Assume.assumeFalse(
            "testSecureParallelScan already covered the secure parallel " +
            "scan cases", SECURITY_ENABLE);
        /*
         * There's no need to setup the shards for each test. Set it up once.
         * Deploy a brand new store with three shards, add 500 records.
         */
        keys = new HashSet<Key>();
        keys.addAll(setupShardsRF3());

        doTestArgEdgeCases();
        doTestCheckNThreadsHeuristic();
        doTestBasicParallelScan();
        doTestBasicParallelScanKeysOnly();
        doTestExceptionPassingAcrossResultsQueue();
        doTestCancelIteration();
        doTestSmallBatchSize();
    }

    @Test
    public void testSecureParallelScan()
        throws Exception{

        keys = new HashSet<Key>();
        hostPort = 5240;
        createStore = new CreateStore(kvstoreName,
                                      hostPort,
                                      1, /* Storage nodes */
                                      1, /* RF */
                                      partitions, /* Partitions */
                                      1, /* Capacity */
                                      MB_PER_SN, /* memory */
                                      false, /* useThreads */
                                      null, /* mgmtImpl */
                                      true, /* mgmtPortsShared */
                                      true); /* secure */
        createStore.addUser(testUser, userPassword, true /* admin */);
        createStore.start();
        try {
            createStore.grantRoles(testUser, "writeonly", "readonly");
            List<Key> loadKeys = loadData(
                createStore.getHostname(),
                createStore.getRegistryPort(),
                createStore.getAdmin(),
                new PasswordCredentials(testUser, userPassword.toCharArray()),
                createStore.getTrustStore().getPath());
            keys.addAll(loadKeys);
            loginKVStoreUser(testUser, userPassword);
            doTestBasicParallelScan();
        } finally {
            createStore.revokeRoles(testUser, "readonly");
            logoutStores();
        }

        try {
            loginKVStoreUser(testUser, userPassword);
            doTestDeniedParallelScan();
        } finally {
            logoutStores();
        }
    }

    private void loginKVStoreUser(String userName, String password) {
        LoginCredentials creds =
                new PasswordCredentials(userName, password.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              createStore.getHostname() + ":" + hostPort);
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  createStore.getTrustStore().getPath());
        kvConfig.setSecurityProperties(props);

        store = KVStoreFactory.getStore(kvConfig, creds, null);
    }

    private void logoutStores() {
        if (store != null) {
            store.logout();
            store.close();
            store = null;
        }
    }


    private void doTestArgEdgeCases()
        throws Exception {

        /* Used for debugging and future test cases */
        @SuppressWarnings("unused")
        final AtomicInteger hookCalls = new AtomicInteger();

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig();

        /*
         * Check that negative args for MaxConcurrentRequests cause IAE.
         */
        try {
            storeIteratorConfig.setMaxConcurrentRequests(-1);
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) {
        }

        /*
         * Check that negative args for MaxResultsBatches cause IAE.
         * setMaxResultsBatches() has been depracated and does not throw IAE
         * anymore.

        try {
            storeIteratorConfig.setMaxResultsBatches(-1);
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) {
        }
        */

        /*
         * Check that passing a null StoreIteratorConfig arg to storeIterator
         * and storeKeysIterator throw IAE.
         */
        try {
            runParallelScan(null, false, Direction.UNORDERED);
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) {
            // expect
        }

        try {
            runParallelScan(null, true, Direction.UNORDERED);
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) {
            // expect
        }

        /*
         * Check that passing a null direction throw IAE.
         */
        try {
            runParallelScan(storeIteratorConfig, false, null);
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) {
            // expect
        }

        try {
            runParallelScan(storeIteratorConfig, true, null);
            fail("expected IAE");
        } catch (IllegalArgumentException IAE) {
            // expect
        }
    }

    /**
     * Run a parallel and a non-parallel scan. Make sure the results counts
     * from each match and that the parallel scan was actually run (using the
     * ParallelScanHook facility).
     *
     * @throws Exception
     */
    private void doTestBasicParallelScan()
        throws Exception {

        final long psRecs = runParallelScan(Direction.UNORDERED);
        assertEquals(psRecs, runParallelScan(Direction.FORWARD));
        assertEquals(psRecs, runParallelScan(Direction.REVERSE));
        final long ssRecs = runSequentialScan();
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);
        /*
        StoreIteratorMetrics sim =
            store.getStats(false).getStoreIteratorMetrics();
        System.out.println("Basic Blocked Gets: " +
                           sim.getBlockedResultsQueueGets());
        System.out.println("Basic Blocked Get time(ns): " +
                           sim.getBlockedResultsQueueGetTime());
        System.out.println("Basic Blocked Puts: " +
                           sim.getBlockedResultsQueuePuts());
        System.out.println("Basic Blocked Put time: " +
                           sim.getBlockedResultsQueuePutTime());
        */
    }

    /* Same as doTestBasicParallelScan() except for storeKeysIterator. */
    private void doTestBasicParallelScanKeysOnly()
        throws Exception {

        final long psRecs = runParallelScanKeysOnly(Direction.UNORDERED);
        assertEquals(psRecs, runParallelScanKeysOnly(Direction.FORWARD));
        assertEquals(psRecs, runParallelScanKeysOnly(Direction.REVERSE));
        final long ssRecs = runSequentialScan();
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);
        /*
        StoreIteratorMetrics sim =
            store.getStats(false).getStoreIteratorMetrics();
        System.out.println("Basic Blocked Gets: " +
                           sim.getBlockedResultsQueueGets());
        System.out.println("Basic Blocked Get time(ns): " +
                           sim.getBlockedResultsQueueGetTime());
        System.out.println("Basic Blocked Puts: " +
                           sim.getBlockedResultsQueuePuts());
        System.out.println("Basic Blocked Put time: " +
                           sim.getBlockedResultsQueuePutTime());
        */
    }

    /**
     * Run parallel scan without write privilege. The scan is expected to be
     * denied and no data will return.
     *
     * @throws Exception
     */
    private void doTestDeniedParallelScan()
        throws Exception {
        final int nStoreIteratorThreads = 3;

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().
            setMaxConcurrentRequests(nStoreIteratorThreads);

        if (store == null) {
            loginKVStoreUser(testUser, userPassword);
        }

        /*
         * Test hook to count how many times PS call this hook.
         */
        final AtomicInteger hookCalls = new AtomicInteger();
        ((KVStoreImpl) store).setParallelScanHook
            (new ParallelScanHook() {
                    @Override
                    public boolean callback(Thread t,
                                            HookType hookType,
                                            String info) {
                        if (hookType == HookType.AFTER_PROCESSING_STREAM) {
                            hookCalls.incrementAndGet();
                        }
                        return true;
                    }
             });

        final ParallelScanIterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 0, null, null, null,
                                Consistency.NONE_REQUIRED, 0, null,
                                storeIteratorConfig);
        assertFalse(iter.hasNext());

        /*
         * The number of hook calls is equal to the number of partitions, even
         * if no data can be retrieved from the store.
         */
        assertEquals(hookCalls.get(), partitions);
    }

    /*
     * Test the heuristic for maxConcurrentRequests == 0.
     */
    private void doTestCheckNThreadsHeuristic()
        throws Exception {

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().
            setMaxConcurrentRequests(0);

        runParallelScan(storeIteratorConfig, false, Direction.UNORDERED);
        runParallelScan(storeIteratorConfig, false, Direction.FORWARD);
        runParallelScan(storeIteratorConfig, false, Direction.REVERSE);
    }

    /* Make sure that exceptions get passed across the Results Queue. */
    private void doTestExceptionPassingAcrossResultsQueue()
        throws Exception {

        assertTrue(runParallelScanWithExceptions());
    }

    /* Test cancelIteration(). */
    private void doTestCancelIteration()
        throws Exception {

        final AtomicLong totalStoreIteratorRecords = new AtomicLong();

        if (store == null) {
            store = KVStoreFactory.getStore
                (new KVStoreConfig(kvstoreName,
                                   AdminUtils.HOSTNAME + ":" + hostPort));
        }

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().setMaxConcurrentRequests(3);

        final ParallelScanIterator<KeyValueVersion> psIter =
            store.storeIterator(Direction.UNORDERED, 0, null, null, null,
                                null, 0, null, storeIteratorConfig);

        while (psIter.hasNext()) {
            psIter.next();
            if (totalStoreIteratorRecords.incrementAndGet() >= 10) {
                psIter.close();
                assertFalse(psIter.hasNext());
                try {
                    psIter.next();
                    fail("expected NoSuchElementException");
                } catch (NoSuchElementException NSEE) {
                    /* Expected. */
                }
                return;
            }
        }

        fail("never tried to terminate?");
    }

    /* Test when the batchSize is very small, quick reading may need wait stream
     * to put result and stall reading will block stream to put result. */
    private void doTestSmallBatchSize()
        throws Exception {

        final long psRecs = runParallelScan(new StoreIteratorConfig(),
                                            false /* keyOnly */,
                                            Direction.UNORDERED,
                                            1 /* batchSize */,
                                            false /* stallReading */);
        assertEquals(psRecs, runParallelScan(new StoreIteratorConfig(),
                                             false /* keyOnly */,
                                             Direction.REVERSE,
                                             1 /* batchSize */,
                                             false /* stallReading */));
        assertEquals(psRecs, runParallelScan(new StoreIteratorConfig(),
                                             false /* keyOnly */,
                                             Direction.UNORDERED,
                                             1 /* batchSize */,
                                             true /* stallReading */));
        assertEquals(psRecs, runParallelScan(new StoreIteratorConfig(),
                                             false /* keyOnly */,
                                             Direction.REVERSE,
                                             1 /* batchSize */,
                                             true /* stallReading */));

        final long ssRecs = runSequentialScan();
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);
    }

    /* Kept around in case we ever want to set up a RF=1 system in this test. */
    @SuppressWarnings("unused")
    private List<Key> setupShardsRF1(int expectedNumRecords)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      3 /* max of 3 SNs */, "DataCenterA",
                                      1 /* repFactor*/, true /* useThreads*/,
                                      MB_PER_SN_STRING);

        /* Deploy two more SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     MB_PER_SN_STRING, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1, 2);
        hostPort = snSet.getRegistryPort(0);

        /* Make an initial topology. */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1 /*dc*/, 3 /*nSNs*/, 3, 3, 100);

        /* Deploy initial topology */
        final int numPlanRepeats = 1; // figure out what this is
        runDeploy("initialDeploy", cs, first, numPlanRepeats, true);

        List<String> candList = cs.listTopologies();
        assertEquals(1, candList.size());
        assertEquals(first, candList.get(0));

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/, 3 /*numRNs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 1 /* rf */,
                                    5000,
                                    logger);
        /*
         * Check that the cache size has been set to a non-zero value. The test
         * infrastructure sets per-SN memory, so the RN/JE cache size should
         * be explicitly set.
         */
        for (RepNodeParams rnp : cs.getParameters().getRepNodeParams()) {
            assertTrue("cacheSize should not be zero and is: " +
                       rnp.getJECacheSize(), rnp.getJECacheSize() != 0);
        }

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keysList = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keysList, expectedNumRecords);
        return keysList;
    }

    /*
     * Create a 3x3 KV Store and put some data in it.
     */
    private List<Key> setupShardsRF3()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      3 /* max of 3 SNs */, "DataCenterA",
                                      3 /* repFactor*/, true /* useThreads*/,
                                      MB_PER_SN_STRING);

        /* Deploy two more SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     MB_PER_SN_STRING, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);
        hostPort = snSet.getRegistryPort(0);

        /* Make an initial topology. */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1 /*dc*/, 3 /*nSNs*/, 3, 9, 100);

        /* Deploy initial topology */
        final int numPlanRepeats = 1; // figure out what this is
        runDeploy("initialDeploy", cs, first, numPlanRepeats, true);

        List<String> candList = cs.listTopologies();
        assertEquals(1, candList.size());
        assertEquals(first, candList.get(0));

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MultipleRNsInRoot.class,
                                    3, MissingRootDirectorySize.class);

        return loadData(AdminUtils.HOSTNAME, snSet.getRegistryPort(0),
                        cs, null, null);
    }

    /* Load some data. Ensure that each partition has at least 2 records */
    private List<Key> loadData(String hostName,
                               int registryPort,
                               CommandServiceAPI cs,
                               LoginCredentials cred,
                               String trustStorePath)
        throws Exception {

        su = new StoreUtils(kvstoreName,
                            hostName,
                            registryPort,
                            StoreUtils.RecordType.INT,
                            1 /* seed */,
                            cred,
                            trustStorePath);
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keysList = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keysList, expectedNumRecords);
        return keysList;
    }

    private long runParallelScan(Direction direction)
        throws Exception {

        final int nStoreIteratorThreads = 3;

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().
            setMaxConcurrentRequests(nStoreIteratorThreads);

        return runParallelScan(storeIteratorConfig, false, direction);
    }

    private long runParallelScanKeysOnly(Direction direction)
        throws Exception {

        final int nStoreIteratorThreads = 3;

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().
            setMaxConcurrentRequests(nStoreIteratorThreads);

        return runParallelScan(storeIteratorConfig, true, direction);
    }

    /* Run a parallel scan and return the record count. */
    private long runParallelScan(StoreIteratorConfig storeIteratorConfig,
                                 boolean keysOnly,
                                 Direction direction)
        throws Exception {

        return runParallelScan(storeIteratorConfig, keysOnly, direction, 0,
                               false);
    }

    /* Run a parallel scan and return the record count. */
    private long runParallelScan(StoreIteratorConfig storeIteratorConfig,
                                 boolean keysOnly,
                                 Direction direction,
                                 int batchSize,
                                 boolean stallReading)
        throws Exception {

        /*
         * If we're running batchSize == 1
         * indicates that we should stall reading the results a little so that
         * streams will be blocked putting results.
         */

        final AtomicLong totalStoreIteratorRecords = new AtomicLong();

        if (store == null) {
            store = KVStoreFactory.getStore
                (new KVStoreConfig(kvstoreName,
                                   AdminUtils.HOSTNAME + ":" + hostPort));
        }

        List<DetailedMetrics> partitionMetrics;
        List<DetailedMetrics> shardMetrics;
        if (keysOnly) {
            final ParallelScanIterator<Key> iter =
                store.storeKeysIterator(direction, batchSize,
                                        null, null, null,
                                        null, 0, null, storeIteratorConfig);

            Key lastEntry = null;
            while (iter.hasNext()) {
                if (stallReading) {
                    try {
                        Thread.sleep(10);
                    } catch (Exception E) {
                    }
                }
                Key key = iter.next();
                assertTrue(keys.contains(key));
                if(lastEntry != null) {
                    if(Direction.FORWARD.equals(direction)) {
                        assertTrue(lastEntry.compareTo(key) <= 0);
                    } else if(Direction.REVERSE.equals(direction)) {
                        assertTrue(lastEntry.compareTo(key) >= 0);
                    }
                }
                lastEntry = key;
                totalStoreIteratorRecords.incrementAndGet();
            }

            /* wait a while to update iterator metrics. */
            Thread.sleep(10);
            partitionMetrics = iter.getPartitionMetrics();
            shardMetrics = iter.getShardMetrics();
        } else {
            final ParallelScanIterator<KeyValueVersion> iter =
                store.storeIterator(direction, batchSize, null, null, null,
                                    Consistency.NONE_REQUIRED, 0, null,
                                    storeIteratorConfig);

            Key lastEntry = null;
            while (iter.hasNext()) {
                if (stallReading) {
                    try {
                        Thread.sleep(10);
                    } catch (Exception E) {
                    }
                }
                KeyValueVersion kvv = iter.next();
                final Key key = kvv.getKey();
                assertTrue(keys.contains(key));
                if(lastEntry != null) {
                    if(Direction.FORWARD.equals(direction)) {
                        assertTrue(lastEntry.compareTo(key) <= 0);
                    } else if(Direction.REVERSE.equals(direction)) {
                        assertTrue(lastEntry.compareTo(key) >= 0);
                    }
                }
                lastEntry = key;
                totalStoreIteratorRecords.incrementAndGet();
            }

            /* wait a while to update iterator metrics. */
            Thread.sleep(10);
            partitionMetrics = iter.getPartitionMetrics();
            shardMetrics = iter.getShardMetrics();
        }

        assertEquals(totalStoreIteratorRecords.get(),
                     tallyDetailedMetrics(partitionMetrics));
        assertEquals(totalStoreIteratorRecords.get(),
                     tallyDetailedMetrics(shardMetrics));

        return totalStoreIteratorRecords.get();
    }

    private long tallyDetailedMetrics(List<DetailedMetrics> partitionMetrics) {
        long totalRecords = 0;
        for (DetailedMetrics dm : partitionMetrics) {
            if (dm != null) {
                totalRecords += dm.getScanRecordCount();
            }
        }
        return totalRecords;
    }

    private boolean runParallelScanWithExceptions()
        throws Exception {

        final AtomicLong totalStoreIteratorRecords = new AtomicLong();
        final AtomicInteger hookCalls = new AtomicInteger();

        if (store == null) {
            store = KVStoreFactory.getStore
                (new KVStoreConfig(kvstoreName,
                                   AdminUtils.HOSTNAME + ":" + hostPort));
        }

        ((KVStoreImpl) store).setParallelScanHook
            (new ParallelScanHook() {
                    @Override
                    public boolean callback(Thread t,
                                            HookType hookType,
                                            String info) {
                        if (hookType == HookType.BEFORE_EXECUTE_REQUEST) {
                            /*
                             * The kvstore has 100 partitions, we will create at
                             * least 100 read requests. Hook one of them to
                             * throw Exception.
                             */
                            if (hookCalls.incrementAndGet() == 54) {
                                throw new FaultException("test", false);
                            }
                        }
                        return true;
                    }
                });

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().setMaxConcurrentRequests(2);

        final ParallelScanIterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 5, null, null, null,
                                null, 0, null, storeIteratorConfig);

        boolean ret = false;
        try {
            while (iter.hasNext()) {
                /*KeyValueVersion kvv = */iter.next();
                totalStoreIteratorRecords.incrementAndGet();
            }
        } catch (StoreIteratorException SIE) {
            ret = true;
        }
        return ret;
    }

    /* Run a sequential scan and return the record count. */
    private long runSequentialScan()
        throws Exception {

        long totalStoreIteratorRecords = 0;

        if (store == null) {
            store = KVStoreFactory.getStore
                (new KVStoreConfig(kvstoreName,
                                   AdminUtils.HOSTNAME + ":" + hostPort));
        }

        final Iterator<KeyValueVersion> iter =
            store.storeIterator(Direction.UNORDERED, 0, null, null, null,
                                null, 0, null);

        while (iter.hasNext()) {
            /*KeyValueVersion kvv = */iter.next();
            /*
            final Key key = kvv.getKey();
            final Value value = kvv.getValue();
            */
            totalStoreIteratorRecords++;
        }

        return totalStoreIteratorRecords;
    }

    private void runDeploy(String planName,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport)
        throws RemoteException {

        runDeploy(true, planName, cs, candidateName, numPlanRepeats,
                  generateStatusReport);
    }

    /**
     * @param generateStatusReport if true, create a thread that will generate
     * a status report while the plan is running, hoping to exercise the
     * reporting module in a realistic way.
     */
    private void runDeploy(boolean expectChange,
                           String planName,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport)
        throws RemoteException {

        try {
            for (int i = 0; i < numPlanRepeats; i++) {
                DeployUtils.printCandidate(cs, candidateName, logger);
                boolean noChange =  (expectChange && (i == 0)) ? false : true;
                DeployUtils.printPreview(candidateName, noChange, cs, logger);
                if (i > 0) {
                    logger.info(i + "th repeat of " + planName );
                }

                int planNum = cs.createDeployTopologyPlan(planName,
                                                          candidateName, null);
                cs.approvePlan(planNum);
                Timer statusThread = null;
                if (generateStatusReport) {
                    statusThread =
                        DeployUtils.spawnStatusThread
                        (cs, planNum,
                         (StatusReport.SHOW_FINISHED_BIT |
                          StatusReport.VERBOSE_BIT), logger, 1000);
                }

                cs.executePlan(planNum, false);
                cs.awaitPlan(planNum, 0, null);
                if (statusThread != null) {
                    statusThread.cancel();
                }
                logger.info
                    ("Plan status report \n" +
                     cs.getPlanStatus(planNum, (StatusReport.SHOW_FINISHED_BIT |
                                                StatusReport.VERBOSE_BIT),
                                      false /* json */));
                cs.assertSuccess(planNum);
                DeployUtils.printCurrentTopo(cs, i + "th iteration of " +
                                             planName, logger);
            }
        } catch (RuntimeException e) {
            logger.severe(LoggerUtils.getStackTrace(e));
            logger.severe(cs.validateTopology(null));
            logger.severe(cs.validateTopology(candidateName));
            throw e;
        }
    }
}
