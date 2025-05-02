/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.bulk;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.ParallelScanIterator;
import oracle.kv.PasswordCredentials;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.admin.AdminUtils;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.DeployUtils;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.parallelscan.ParallelScanHook;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.stats.DetailedMetrics;
import oracle.kv.util.CreateStore;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests bulk get KV APIs:
 * 	 public ParallelScanIterator<KeyValueVersion>
 *       storeIterator(final Iterator<Key> parentKeyiterator,
 *                     final int batchSize,
 *                     final KeyRange subRange,
 *                     final Depth depth,
 *                     final Consistency consistency,
 *                     final long timeout,
 *                     final TimeUnit timeoutUnit,
 *                     final StoreIteratorConfig storeIteratorConfig)
 *       throws ConsistencyException, RequestTimeoutException, FaultException;
 *
 *   public ParallelScanIterator<Key>
 *       storeKeysIterator(final Iterator<Key> parentKeyiterator,
 *                         final int batchSize,
 *                         final KeyRange subRange,
 *                         final Depth depth,
 *                         final Consistency consistency,
 *                         final long timeout,
 *                         final TimeUnit timeoutUnit,
 *                         final StoreIteratorConfig storeIteratorConfig)
 *       throws ConsistencyException, RequestTimeoutException, FaultException;
 *
 *   public ParallelScanIterator<KeyValueVersion>
 *       storeIterator(final List<Iterator<Key>> parentKeyiterators,
 *                     final int batchSize,
 *                     final KeyRange subRange,
 *                     final Depth depth,
 *                     final Consistency consistency,
 *                     final long timeout,
 *                     final TimeUnit timeoutUnit,
 *                     final StoreIteratorConfig storeIteratorConfig)
 *       throws ConsistencyException, RequestTimeoutException, FaultException;
 *
 *   public ParallelScanIterator<Key>
 *       storeKeysIterator(final List<Iterator<Key>> parentKeyiterators,
 *                         final int batchSize,
 *                         final KeyRange subRange,
 *                         final Depth depth,
 *                         final Consistency consistency,
 *                         final long timeout,
 *                         final TimeUnit timeoutUnit,
 *                         final StoreIteratorConfig storeIteratorConfig)
 *       throws ConsistencyException, RequestTimeoutException, FaultException;
 */
public class BulkGetTest extends TestBase {

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

    /* Parameters used for Depth and KeyRange related tests */
    private List<Key> grpKeys;
    private List<Key> subGrpKeys;
    private final int nGroups = 10;
    private final int nSubGroupsPerGroup = 10;
    private final int nUsersPerSubGroup = 10;

    /* The number of parent key iterators */
    private int numKeyIterators = 1;

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
        throws Exception{

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

    private void setNumParentKeyIterators(int num) {
        numKeyIterators = num;
    }

    private int getNumParentKeyIterators() {
        return numKeyIterators;
    }

    /* Put all tests under one unit test to avoid repeated setup and teardown */
    @Test
    public void testRunBulkGetTests()
        throws Exception {
        Assume.assumeFalse(
            "testSecureBulkGet already covered the secure BulkGet cases",
            SECURITY_ENABLE);
        /*
         * There's no need to setup the shards for each test. Set it up once.
         * Deploy a brand new store with three shards, add 500 records.
         */
        keys = new HashSet<Key>();
        keys.addAll(setupShardsRF3());

        openStore();

        doTestArgEdgeCases(true);
        doTestArgEdgeCases(false);

        final int[] nKeyIterators = {1, 3};
        for (int num : nKeyIterators) {
            setNumParentKeyIterators(num);
            doTestCheckNThreadsHeuristic(true);
            doTestCheckNThreadsHeuristic(false);

            doTestBasicBulkGet(true);
            doTestBasicBulkGet(false);

            doTestExceptionPassingAcrossResultsQueue(true);
            doTestExceptionPassingAcrossResultsQueue(false);

            doTestCancelIteration(true);
            doTestCancelIteration(false);

            doTestSmallBatchSize(true);
            doTestSmallBatchSize(false);
        }

        loadDataForRangeDepthTest();
        for (int num : nKeyIterators) {
            setNumParentKeyIterators(num);
            doTestKeyRangeDepth(true);
            doTestKeyRangeDepth(false);
        }
    }

    @Test
    public void testSecureBulkGet()
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
            doTestBasicBulkGet(true);
            doTestBasicBulkGet(false);
        } finally {
            createStore.revokeRoles(testUser, "readonly");
            logoutStores();
        }

        try {
            loginKVStoreUser(testUser, userPassword);
            doTestDeniedBulkGet(true);
            doTestDeniedBulkGet(false);
        } finally {
            logoutStores();
        }
    }

    private void openStore() {
        store = KVStoreFactory.getStore
            (new KVStoreConfig(kvstoreName,
                               AdminUtils.HOSTNAME + ":" + hostPort));
    }

    private void doTestArgEdgeCases(boolean keyOnly)
        throws Exception {

        final StoreIteratorConfig config =
            new StoreIteratorConfig().setMaxConcurrentRequests(0);
        /*
         * Check that passing a null Iterator<Key> arg to
         * storeIterator(Iterator<Key>, ...) throw IAE.
         */
        try {
            final Iterator<Key> iter = null;
            if (keyOnly) {
                store.storeKeysIterator(iter, 0, null, null, null,
                                        0, null, config);
            } else {
                store.storeIterator(iter, 0, null, null, null,
                                    0, null, config);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Check that passing a null List<Iterator<Key>> arg to
         * storeIterator(List<Iterator<Key>>, ...) throw IAE.
         */
        try {
            final List<Iterator<Key>> iters = null;
            if (keyOnly) {
                store.storeKeysIterator(iters, 0, null, null, null,
                                        0, null, config);
            } else {
                store.storeIterator(iters, 0, null, null, null,
                                    0, null, config);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Check that passing an empty List<Iterator<Key>> arg to
         * storeIterator(List<Iterator<Key>>, ...) throw IAE.
         */
        try {
            final List<Iterator<Key>> iters = new ArrayList<Iterator<Key>>();
            if (keyOnly) {
                store.storeKeysIterator(iters, 0, null, null, null,
                                        0, null, config);
            } else {
                store.storeIterator(iters, 0, null, null, null,
                                    0, null, config);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }

        /*
         * Check that passing a List<Iterator<Key>> that contains null element
         * to storeIterator(List<Iterator<Key>>, ...) throw IAE.
         */
        try {
            final List<Iterator<Key>> iters = new ArrayList<Iterator<Key>>();
            iters.add(null);
            if (keyOnly) {
                store.storeKeysIterator(iters, 0, null, null, null,
                                        0, null, config);
            } else {
                store.storeIterator(iters, 0, null, null, null,
                                    0, null, config);
            }
            fail("expected IAE");
        } catch (IllegalArgumentException expect) {
        }
    }

    /**
     * Test the heuristic for maxConcurrentRequests == 0.
     */
    private void doTestCheckNThreadsHeuristic(boolean keyOnly)
        throws Exception {

         runBulkGet(keys, 0, keyOnly);
    }

    /**
     * Basic bulk get test
     */
    private void doTestBasicBulkGet(boolean keyOnly)
        throws Exception {

        long psRecs = runBulkGet(keys, 0, keyOnly);
        final long ssRecs = runSequentialScan();
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);

        psRecs = runBulkGet(keys, 1, keyOnly);
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);

        psRecs = runBulkGet(keys, 9, keyOnly);
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);
    }

    /**
     * Make sure that exceptions get passed across the Results Queue.
     */
    private void doTestExceptionPassingAcrossResultsQueue(boolean keyOnly)
        throws Exception {

        assertTrue(runBulkGetWithExceptions(keys, keyOnly));
    }

    /**
     * Test cancel iteration.
     */
    private void doTestCancelIteration(boolean keyOnly)
        throws Exception {

        final AtomicLong totalStoreIteratorRecords = new AtomicLong();

        if (store == null) {
            store = KVStoreFactory.getStore
                (new KVStoreConfig(kvstoreName,
                                   AdminUtils.HOSTNAME + ":" + hostPort));
        }

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().setMaxConcurrentRequests(3);

        final ParallelScanIterator<?> psIter;
        if (keyOnly) {
            psIter = store.storeKeysIterator(keys.iterator(), 0, null, null,
                                             null, 0, null,
                                             storeIteratorConfig);
        } else {
            psIter = store.storeIterator(keys.iterator(), 0, null, null,
                                         null, 0, null, storeIteratorConfig);
        }

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

    /*
     * Test when the batchSize is very small, quick reading may need wait stream
     * to put result and stall reading will block stream to put result.
     */
    private void doTestSmallBatchSize(boolean keyOnly)
        throws Exception {

        final StoreIteratorConfig config =
            new StoreIteratorConfig().setMaxConcurrentRequests(9);
        final long psRecs = runBulkGet(keys,
                                       config,
                                       null /* keyRange */,
                                       null /* Depth */,
                                       1 /* batchSize */,
                                       false /* stallReading */,
                                       keys,
                                       keyOnly);

        assertEquals(psRecs, runBulkGet(keys,
                                        config,
                                        null /* keyRange */,
                                        null /* Depth */,
                                        1 /* batchSize */,
                                        true /* stallReading */,
                                        keys,
                                        keyOnly));
        final long ssRecs = runSequentialScan();
        assertEquals(ssRecs, psRecs);
        assertEquals(expectedNumRecords, psRecs);
    }

    /*
     * Test Depth and KeyRange.
     */
    private void doTestKeyRangeDepth(boolean keyOnly)
        throws Exception {

        runTestKeyRangeDepth(keyOnly);
        runTestKeyRangeDepthKeyWithMinorPath(keyOnly);
    }

    private void runTestKeyRangeDepth(boolean keyOnly)
        throws Exception{

        final int nSubGrp = nSubGroupsPerGroup;
        final Integer[][] ranges = new Integer[][] {
            {null, null},
            {nSubGrp/4, nSubGrp/2},
            {3/4*nSubGrp, null},
            {null, nSubGrp/4},
            {nSubGrp-1, null},
            {null, 0},
        };
        final boolean[][] inclusiveFlags = new boolean[][]{
            {true, true}, {false, true}, {true, false}, {false, false}
        };
        final StoreIteratorConfig config =
            new StoreIteratorConfig().setMaxConcurrentRequests(9);

        for (final Depth dep : EnumSet.allOf(Depth.class)) {
            for (final Integer[] range: ranges) {
                for (final boolean[] flag: inclusiveFlags) {
                    final KeyRange useRange =
                        createRange(range, SUB_GROUP_PREFIX, flag);
                    final long nRows = runBulkGet(grpKeys, config, useRange,
                                                  dep, 0, false, null, keyOnly);
                    final int nSubGrpsInRange =
                        getSubGroupNumInRange(useRange, nSubGrp);
                    final long exp =
                        getExpectedRowsNumByGroupKeys(dep, nGroups,
                                                      nSubGrpsInRange,
                                                      nUsersPerSubGroup);
                    assertEquals(exp, nRows);
                }
            }
        }

        /* SubRange: E/users/ */
        String fmt = "E/%s/";
        KeyRange useRange =
            KeyRange.fromString(String.format(fmt, USERS_PREFIX));
        final long nRows = runBulkGet(grpKeys, config, useRange,
                                      Depth.PARENT_AND_DESCENDANTS, 0, false,
                                      null, keyOnly);
        assertEquals(nGroups, nRows);
    }

    private void runTestKeyRangeDepthKeyWithMinorPath(boolean keyOnly)
        throws Exception{

        final int nUsers = nUsersPerSubGroup;
        final Integer[][] ranges = new Integer[][] {
            {null, null},
            {nUsers/4, nUsers/2},
            {3/4*nUsers, null},
            {null, nUsers/4},
            {nUsers-1, null},
            {null, 0},
        };
        final boolean[][] inclusiveFlags = new boolean[][]{
            {true, true}, {false, true}, {true, false}, {false, false}
        };
        final StoreIteratorConfig config =
            new StoreIteratorConfig().setMaxConcurrentRequests(9);

        for (final Depth dep : EnumSet.allOf(Depth.class)) {
            for (final Integer[] range: ranges) {
                for (final boolean[] flag: inclusiveFlags) {
                    final KeyRange useRange = createRange(range, null, flag);
                    final long nRows = runBulkGet(subGrpKeys, config, useRange,
                                                  dep, 0, false, null, keyOnly);
                    final int num = getUsersNumInRange(useRange, nUsers);
                    final long exp =
                        getExpectedRowsNumBySubGroupKeys(dep, nGroups,
                                                         nSubGroupsPerGroup,
                                                         num);
                    assertEquals(exp, nRows);
                }
            }
        }

        /* SubRange: E/users/ */
        String fmt = "E/%s/";
        KeyRange useRange =
            KeyRange.fromString(String.format(fmt, USERS_PREFIX));
        final long nRows = runBulkGet(subGrpKeys, config, useRange,
                                      Depth.PARENT_AND_DESCENDANTS,
                                      0, false, null, keyOnly);
        assertEquals(nGroups * nSubGroupsPerGroup, nRows);
    }

    private KeyRange createRange(Integer[] range,
                                 String prefix,
                                 boolean[] flags) {
        if (range == null) {
            return null;
        }

        assert(range.length == 2 && flags.length == 2);
        if (range[0] == null && range[1] == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        if (range[0] != null) {
            sb.append((flags[0] ? "I" : "E"));
            sb.append("/");
            if (prefix != null) {
                sb.append(prefix);
            }
            sb.append(formatInt10Digits(range[0]));
        }

        if (range[1] != null) {
            sb.append("/");
            if (prefix != null) {
                sb.append(prefix);
            }
            sb.append(formatInt10Digits(range[1]));
            sb.append((flags[1] ? "/I" : "/E"));
        } else {
            sb.append("/");
        }

        final String rangeStr = sb.toString();
        return KeyRange.fromString(rangeStr);
    }

    private int getSubGroupNumInRange(KeyRange range, int nSubGrps) {
        if (range == null) {
            return nSubGrps;
        }
        int num = nSubGrps;
        final String start = range.getStart();
        final int prefixLen = SUB_GROUP_PREFIX.length();
        if (start != null) {
            int from = Integer.valueOf(start.substring(prefixLen));
            num -= from;
            if (!range.getStartInclusive()) {
                num--;
            }
        }
        final String end = range.getEnd();
        if (end != null) {
            int to = Integer.valueOf(end.substring(prefixLen));
            num -= (nSubGrps - to);
            if (range.getEndInclusive()) {
                num++;
            }
        }
        return num;
    }

    private int getUsersNumInRange(KeyRange range, int nUsers) {
        if (range == null) {
            return nUsers;
        }
        int num = nUsers;
        final String start = range.getStart();
        if (start != null) {
            int from = Integer.valueOf(start);
            num -= from;
            if (!range.getStartInclusive()) {
                num--;
            }
        }
        final String end = range.getEnd();
        if (end != null) {
            int to = Integer.valueOf(end);
            num -= (nUsers - to);
            if (range.getEndInclusive()) {
                num++;
            }
        }
        return num;
    }

    /**
     * Run bulk get without write privilege. The scan is expected to be
     * denied and no data will return.
     *
     * @throws Exception
     */
    private void doTestDeniedBulkGet(boolean keyOnly)
        throws Exception {

        final int nStoreIteratorThreads = 10;

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

        final ParallelScanIterator<?> iter;
        if (keyOnly) {
            iter = store.storeKeysIterator(keys.iterator(), 0, null, null,
                                           Consistency.NONE_REQUIRED,
                                           10000, TimeUnit.MILLISECONDS,
                                           storeIteratorConfig);
        } else {
            iter = store.storeIterator(keys.iterator(), 0, null, null,
                                       Consistency.NONE_REQUIRED, 0, null,
                                       storeIteratorConfig);
        }
        assertFalse(iter.hasNext());
        /*
         * The number of hook calls is equal to the number of partitions, even
         * if no data can be retrieved from the store.
         */
        assertEquals(hookCalls.get(), partitions);

        iter.close();
        ((KVStoreImpl) store).setParallelScanHook(null);
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
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
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

    private final String USERS_PREFIX = "users";
    private final String GROUP_PREFIX = "group";
    private final String SUB_GROUP_PREFIX = "subgp";

    private void loadDataForRangeDepthTest() {

        long num = 0;

        if (store == null) {
            store = KVStoreFactory.getStore
                (new KVStoreConfig(kvstoreName,
                                   AdminUtils.HOSTNAME + ":" + hostPort));
        }
        grpKeys = new ArrayList<Key>();
        subGrpKeys = new ArrayList<Key>();
        for (int g = 0; g < nGroups; g++) {
            Key key = Key.fromString(getGroupKey(g));
            Version version = store.put(key, Value.EMPTY_VALUE);
            assertNotNull(version);
            num++;
            grpKeys.add(key);
            for (int sg = 0; sg < nSubGroupsPerGroup; sg++) {
                key = Key.fromString(getSubGroupKey(g, sg));
                version = store.put(key, Value.EMPTY_VALUE);
                assertNotNull(version);
                num++;
                subGrpKeys.add(key);
                for (int u = 0; u < nUsersPerSubGroup; u++) {
                    key = Key.fromString(getUserKey(g, sg, u));
                    final Value value = Value.createValue(key.toByteArray());
                    version = store.put(key, value);
                    assertNotNull(version);
                    num++;
                }
            }
        }

        final int total = nGroups *
            (1 + nSubGroupsPerGroup + nSubGroupsPerGroup * nUsersPerSubGroup);
        assertEquals(total, num);
    }

    private String getGroupKey(int g) {
        final String fmt = "/%s/%s%s";
        return String.format(fmt, USERS_PREFIX, GROUP_PREFIX,
            formatInt10Digits(g));
    }

    private String getSubGroupKey(int g, int sg) {
        final String fmt = "%s/-/%s%s";
        return String.format(fmt, getGroupKey(g), SUB_GROUP_PREFIX,
            formatInt10Digits(sg));
    }

    private String getUserKey(int g, int sg, int u) {
        final String fmt = "%s/%s";
        return String.format(fmt, getSubGroupKey(g, sg),
            formatInt10Digits(u));
    }

    private static String formatInt10Digits(int i) {
        final String fmt = "%010d";
        return String.format(fmt, i);
    }

    private long getExpectedRowsNumByGroupKeys(Depth dep, int nGrps,
                                               int nSubGrps, int nUsers) {
        long nRows = 0;

        switch (dep.ordinal()) {
        case 0: /* Depth.CHILD_ONLY */
            nRows = nGrps * nSubGrps;
            break;
        case 1: /* Depth.PARENT_AND_CHILDREN */
            nRows = nGrps + nGrps * nSubGrps ;
            break;
        case 2: /* Depth.DESCENDANTS_ONLY */
            nRows = nGrps * nSubGrps + nGrps * nSubGrps * nUsers;
            break;
        case 3: /* Depth.PARENT_AND_DESCENDANTS */
            nRows = nGrps + nGrps * nSubGrps + nGrps * nSubGrps * nUsers;
            break;
        default:
            break;
        }
        return nRows;
    }

    private long getExpectedRowsNumBySubGroupKeys(Depth dep, int nGrps,
                                                  int nSubGrps, int nUsers) {
        long nRows = 0;

        switch (dep.ordinal()) {
        case 0: /* Depth.CHILD_ONLY */
        case 2: /* Depth.DESCENDANTS_ONLY */
            nRows = nGrps * nSubGrps * nUsers;
            break;
        case 1: /* Depth.PARENT_AND_CHILDREN */
        case 3: /* Depth.PARENT_AND_DESCENDANTS */
            nRows = nGrps * nSubGrps + nGrps * nSubGrps * nUsers;
            break;
        default:
            break;
        }
        return nRows;
    }

    /* Run a bulk get and return the record count. */
    private long runBulkGet(Collection<Key> parentKeys,
                            int nStoreIteratorThreads,
                            boolean keyOnly)
        throws Exception {

        final StoreIteratorConfig storeIteratorConfig =
            new StoreIteratorConfig().
                setMaxConcurrentRequests(nStoreIteratorThreads);
        return runBulkGet(parentKeys, storeIteratorConfig, keyOnly);
    }

    /* Run a bulk get and return the record count. */
    private long runBulkGet(Collection<Key> parentKeys,
                            StoreIteratorConfig storeIteratorConfig,
                            boolean keyOnly)
        throws Exception {

        return runBulkGet(parentKeys, storeIteratorConfig, null, null,
                          0, false, parentKeys, keyOnly);
    }

    /* Run a bulk get and return the record count. */
    private long runBulkGet(Collection<Key> parentKeys,
                            StoreIteratorConfig storeIteratorConfig,
                            KeyRange range,
                            Depth dep,
                            int batchSize,
                            boolean stallReading,
                            Collection<Key> expectedKeys,
                            boolean keyOnly)
        throws Exception {

        /*
         * If we're running batchSize == 1
         * indicates that we should stall reading the results a little so that
         * streams will be blocked putting results.
         */

        final AtomicLong totalStoreIteratorRecords = new AtomicLong();

        final ParallelScanIterator<?> iter =
            createBulkGetIterator(parentKeys, keyOnly, batchSize,
                                  range, dep, Consistency.NONE_REQUIRED,
                                  storeIteratorConfig);
        while (iter.hasNext()) {
            if (stallReading) {
                try {
                    Thread.sleep(10);
                } catch (Exception E) {
                }
            }
            final Key key;
            if (keyOnly) {
                key = (Key)iter.next();
            } else {
                KeyValueVersion kvv = (KeyValueVersion)iter.next();
                key = kvv.getKey();
            }
            if (expectedKeys != null) {
                assertTrue(expectedKeys.contains(key));
            }
            totalStoreIteratorRecords.incrementAndGet();
        }

        /* wait a while to update iterator metrics. */
        final List<DetailedMetrics> shardMetrics = iter.getShardMetrics();
        final long recordCount = totalStoreIteratorRecords.get();
        boolean result =  new PollCondition(10, 500) {
            @Override
            protected boolean condition() {
                return recordCount == tallyDetailedMetrics(shardMetrics);
            }
        }.await();

        assertTrue(result);

        iter.close();
        return totalStoreIteratorRecords.get();
    }

    /*
     * This is static and public so it can be used from other test classes
     */
    public static List<Iterator<Key>> createKeyIterators(
        Collection<Key> parentKeys,
        int num) {
        final int nKeys = parentKeys.size();
        final int nKeyIterators = (nKeys < num) ? nKeys : num;
        final int perIteratorKeys = (nKeys + nKeyIterators - 1)/nKeyIterators;
        final List<Key> allKeys =
            Arrays.asList(parentKeys.toArray(new Key[nKeys]));
        final List<Iterator<Key>> keyIterators =
            new ArrayList<Iterator<Key>>(nKeyIterators);
        for (int i = 0; i < nKeyIterators; i++) {
            final int from = i * perIteratorKeys;
            final int to = Math.min((i + 1) * perIteratorKeys, nKeys);
            final List<Key> subKeys = allKeys.subList(from, to);
            keyIterators.add(subKeys.iterator());
        }
        return keyIterators;
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
            iter.next();
            totalStoreIteratorRecords++;
        }

        return totalStoreIteratorRecords;
    }

    private boolean runBulkGetWithExceptions(Collection<Key> parentKeys,
                                             boolean keyOnly)
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

        final StoreIteratorConfig config =
            new StoreIteratorConfig().setMaxConcurrentRequests(3);
        final ParallelScanIterator<?> iter =
            createBulkGetIterator(parentKeys, keyOnly, 5,
                                  null, null, null, config);
        boolean ret = false;
        try {
            while (iter.hasNext()) {
                iter.next();
                totalStoreIteratorRecords.incrementAndGet();
            }
        } catch (StoreIteratorException SIE) {
            ret = true;
        } finally {
            if (iter != null) {
                iter.close();
            }
            ((KVStoreImpl) store).setParallelScanHook(null);
        }
        return ret;
    }

    private ParallelScanIterator<?>
        createBulkGetIterator(Collection<Key> parentKeys,
                              boolean keyOnly,
                              int batchSize,
                              KeyRange range,
                              Depth depth,
                              Consistency consistency,
                              StoreIteratorConfig config) {

        final ParallelScanIterator<?> iter;
        final int nParentKeyIterators = getNumParentKeyIterators();
        if ( nParentKeyIterators== 1) {
            final Iterator<Key> keyIterator = parentKeys.iterator();
            if (keyOnly) {
                iter = store.storeKeysIterator(keyIterator, batchSize,
                                               range, depth, consistency,
                                               0, null, config);
            } else {
                iter = store.storeIterator(keyIterator, batchSize,
                                           range, depth, consistency,
                                           0, null, config);
            }
        } else {
            assert nParentKeyIterators > 1;
            final List<Iterator<Key>> keyIterators =
                createKeyIterators(parentKeys, nParentKeyIterators);
            if (keyOnly) {
                iter = store.storeKeysIterator(keyIterators, batchSize,
                                               range, depth, consistency,
                                               0, null, config);
            } else {
                iter = store.storeIterator(keyIterators, batchSize,
                                           range, depth, consistency,
                                           0, null, config);
            }
        }
        return iter;
    }

    private long tallyDetailedMetrics(List<DetailedMetrics> metrics) {
        long totalRecords = 0;
        for (DetailedMetrics dm : metrics) {
            if (dm != null) {
                totalRecords += dm.getScanRecordCount();
            }
        }
        return totalRecords;
    }
}
