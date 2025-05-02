/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.monitor;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.management.MBeanServer;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Operation;
import oracle.kv.OperationFactory;
import oracle.kv.OperationResult;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.mgmt.jmx.RepNode;
import oracle.kv.impl.mgmt.jmx.StorageNode;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.NonBlockingTrackerListener;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.kvlite.KVLite;

import org.junit.Test;


/**
 * Test no-restart parameter changes to Admin and RepNode.
 */
public class PerfStatsTest extends TestBase {

    private static final String testdir = TestUtils.getTestDir().toString();

    private static final String storeName = "cpstore";
    private static final String testhost = "localhost";
    private static final int startPort = 6000;
    private static final int haRange = 2;
    private static final String pollInterval = "2 SECONDS";
    private static final String statsInterval = "3 SECONDS";

    private PortFinder portFinder;
    private KVLite kvlite;

    @Override
    public void setUp() throws Exception {

        portFinder = new PortFinder(startPort, haRange);
        super.setUp();
        /* use threads to avoid process cleanup */
        kvlite = new KVLite(testdir,
                            storeName,
                            portFinder.getRegistryPort(),
                            true,
                            testhost,
                            portFinder.getHaRange(),
                            null,
                            0,
                            null,
                            true,
                            false,
                            null,
                            -1);
    }

    private void finishSetup(ParameterMap map) throws Exception {
        kvlite.setPolicyMap(mergeParameterMapDefaults(map));
        kvlite.setVerbose(false);
        kvlite.start();
        /*
         * Need to run the change plan since set policy does not work on global
         * parameter.
         */
        final CommandServiceAPI cs =
            RegistryUtils.getAdmin(testhost,
                                   portFinder.getRegistryPort(),
                                   null /* loginMgr */, logger);
        final int planId = cs.createChangeGlobalComponentsParamsPlan(
            "shorten collector", map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (kvlite != null) {
            kvlite.stop(false);
            kvlite = null;
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Exercise the basic interfaces, setting defaults, globals, and
     * per-node parameters.
     *
     * Note that this test counts RN events and is sensitive to changes by
     * other components that may also be performing operations to the store.
     * For example operations to system tables during startup will affect
     * the counts, causing random failures after an unrelated change.
     */
    @Test
    public void testBasic()
        throws Exception {
        finishSetup(createPolicyMap());

        PerfListener perfListener = new PerfListener();

        KVStoreConfig config = new KVStoreConfig(storeName,
                                                 testhost+":"+
                                                 portFinder.getRegistryPort());
        KVStore store = KVStoreFactory.getStore(config);
        perfListener.catchup(5000);

        final Key key1 = Key.createKey("one");
        final Key key12 = Key.createKey("one", "two");
        final Key key13 = Key.createKey("one", "three");
        final Key key14 = Key.createKey("one", "four");
        final Key key34 = Key.createKey("three", "four");
        final Value val1 = Value.createValue(new byte[1]);
        final Value val2 = Value.createValue(new byte[2]);

        /* first collection period, only single ops */
        perfListener.setupWait(3, 0);
        assertNotNull(store.put(key12, val1));
        assertNotNull(store.put(key34, val2));
        assertNotNull(store.get(key12));
        perfListener.await();

        /*
         * second collection period, issue command that are entirely different
         * from the first interval.
         */
        perfListener.setupWait(2, 0);
        assertTrue(store.delete(key12));
        assertTrue(store.delete(key34));
        perfListener.await();

        /* third collection, single and multi ops */
        perfListener.setupWait(3, 8);
        assertNotNull(store.put(key12, val1));
        assertNotNull(store.put(key13, val2));
        assertNotNull(store.put(key14, val2));
        Map<Key,ValueVersion> result = store.multiGet(key1, null, null);
        assertEquals(3, result.size());
        Iterator<KeyValueVersion> iter =
            store.storeIterator(
                Direction.UNORDERED, 10, null, new KeyRange("one"), null);
        assertTrue(iter.hasNext());

        final OperationFactory factory = store.getOperationFactory();
        final List<Operation> ops = new ArrayList<Operation>();
        ops.add(factory.createPutIfAbsent(Key.createKey("a", "a"), val1));
        ops.add(factory.createPutIfAbsent(Key.createKey("a", "b"), val2));
        List<OperationResult> listResults = store.execute(ops);
        assertEquals(2, listResults.size());

        perfListener.await();

        /* fourth collection, only multi */
        perfListener.setupWait(0, 3);
        int numDeleted = store.multiDelete(key1, null, null);
        assertEquals(3, numDeleted);
        perfListener.await();
        checkStatFile(true);

        store.close();
    }

    /**
     * Test the generation of the .stat file.
     */
    @Test
    public void testStatFile()
        throws Exception {

        /* Set the flag for the env stats. */
        ParameterMap map = createPolicyMap();
        map.setParameter(ParameterState.SP_COLLECT_ENV_STATS, "true");
        finishSetup(map);

        PerfListener perfListener = new PerfListener();

        KVStoreConfig config = new KVStoreConfig(storeName,
                                                 testhost+":"+
                                                 portFinder.getRegistryPort());
        KVStore store = KVStoreFactory.getStore(config);
        perfListener.catchup(5000);

        final Key key12 = Key.createKey("one", "two");
        final Key key34 = Key.createKey("three", "four");
        final Value val1 = Value.createValue(new byte[1]);
        final Value val2 = Value.createValue(new byte[2]);

        /* generate some activity */
        perfListener.setupWait(3, 0);
        assertNotNull(store.put(key12, val1));
        assertNotNull(store.put(key34, val2));
        assertNotNull(store.get(key12));

        perfListener.await();
        checkStatFile(true);

        store.close();
    }

    /**
     * Test formatting with long values operation and request counts. [#27517]
     */
    @Test
    public void testFormatLongTotalStats() {
        PerfEvent event = new PerfEvent(
            new RepNodeId(10, 2),
            new LatencyInfo(PerfStatType.USER_SINGLE_OP_INT,
                            100000000000L,
                            200000000000L,
                            new LatencyResult(30_000_000_000L,
                                              40_000_000_000L,
                                              0, 100, 1,
                                              2, 10, 0)),
            new LatencyInfo(PerfStatType.USER_SINGLE_OP_CUM,
                            100000000000L,
                            200000000000L,
                            new LatencyResult(300_000_000_000L,
                                              400_000_000_000L,
                                              0, 100, 1,
                                              2, 10, 0)),
            1, 2,
            new LatencyInfo(PerfStatType.USER_MULTI_OP_INT,
                            100000000000L,
                            200000000000L,
                            new LatencyResult(50_000_000_000L,
                                              60_000_000_000L,
                                              0, 100, 1,
                                              2, 10, 0)),
            new LatencyInfo(PerfStatType.USER_MULTI_OP_CUM,
                            100000000000L,
                            200000000000L,
                            new LatencyResult(500_000_000_000L,
                                              600_000_000_000L,
                                              0, 100, 1,
                                              2, 10, 0)));
        assertEquals("Header and row should have same length:\n" +
                     PerfEvent.HEADER + '\n' + event.getColumnFormatted(),
                     /* HEADER starts with '\n' and has two lines */
                     PerfEvent.HEADER.length(),
                     /* Event has two lines, need to add initial '\n' */
                     ('\n' + event.getColumnFormatted()).length());
    }

    /**
     * Check that the various RepNode.getXXXTotalOpsLong methods return longs
     * properly and that the associated int ones max out the value if it is too
     * large.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testRepNodeTotalOps() {

        /* Use mocks for RepNode constructor parameters */
        RepNodeParams repNodeParams = createMock(RepNodeParams.class);
        expect(repNodeParams.getRepNodeId()).andReturn(new RepNodeId(1, 1));
        expect(repNodeParams.getLatencyCeiling()).andReturn(1000);
        expect(repNodeParams.getThroughputFloor()).andReturn(0);
        replay(repNodeParams);
        RepNode repNode = new RepNode(repNodeParams,
                                      createMock(MBeanServer.class),
                                      createMock(StorageNode.class));

        StatsPacket stats = new StatsPacket(0, 0, "rg1-rn1", "rg1");
        stats.add(createLatencyInfo(PerfStatType.USER_SINGLE_OP_INT, 1, 9));
        stats.add(createLatencyInfo(PerfStatType.USER_SINGLE_OP_CUM, 2, 9));
        stats.add(createLatencyInfo(PerfStatType.USER_MULTI_OP_INT, 3, 9));
        stats.add(createLatencyInfo(PerfStatType.USER_MULTI_OP_CUM, 4, 9));
        repNode.setStats(stats);

        assertEquals(1, repNode.getIntervalTotalOps());
        assertEquals(1, repNode.getIntervalTotalOpsLong());
        assertEquals(2, repNode.getCumulativeTotalOps());
        assertEquals(2, repNode.getCumulativeTotalOpsLong());
        assertEquals(3, repNode.getMultiIntervalTotalOps());
        assertEquals(3, repNode.getMultiIntervalTotalOpsLong());
        assertEquals(4, repNode.getMultiCumulativeTotalOps());
        assertEquals(4, repNode.getMultiCumulativeTotalOpsLong());

        repNodeParams = createMock(RepNodeParams.class);
        expect(repNodeParams.getRepNodeId()).andReturn(new RepNodeId(1, 1));
        expect(repNodeParams.getLatencyCeiling()).andReturn(1000);
        expect(repNodeParams.getThroughputFloor()).andReturn(0);
        replay(repNodeParams);
        repNode = new RepNode(repNodeParams,
                              createMock(MBeanServer.class),
                              createMock(StorageNode.class));

        stats = new StatsPacket(0, 0, "rg1-rn1", "rg1");
        stats.add(createLatencyInfo(PerfStatType.USER_SINGLE_OP_INT,
                                    1_000_000_000_000L, 9));
        stats.add(createLatencyInfo(PerfStatType.USER_SINGLE_OP_CUM,
                                    2_000_000_000_000L, 9));
        stats.add(createLatencyInfo(PerfStatType.USER_MULTI_OP_INT,
                                    3_000_000_000_000L, 9));
        stats.add(createLatencyInfo(PerfStatType.USER_MULTI_OP_CUM,
                                    4_000_000_000_000L, 9));
        repNode.setStats(stats);

        assertEquals(Integer.MAX_VALUE, repNode.getIntervalTotalOps());
        assertEquals(1_000_000_000_000L, repNode.getIntervalTotalOpsLong());
        assertEquals(Integer.MAX_VALUE, repNode.getCumulativeTotalOps());
        assertEquals(2_000_000_000_000L, repNode.getCumulativeTotalOpsLong());
        assertEquals(Integer.MAX_VALUE, repNode.getMultiIntervalTotalOps());
        assertEquals(3_000_000_000_000L,
                     repNode.getMultiIntervalTotalOpsLong());
        assertEquals(Integer.MAX_VALUE, repNode.getMultiCumulativeTotalOps());
        assertEquals(4_000_000_000_000L,
                     repNode.getMultiCumulativeTotalOpsLong());
    }

    private LatencyInfo createLatencyInfo(PerfStatType perfStatType,
                                          long totalOps,
                                          long totalRequests) {
        return new LatencyInfo(perfStatType, 1000, 2000,
                               new LatencyResult(totalRequests,
                                                 totalOps,
                                                 0, 100, 1,
                                                 2, 5, 0));
    }

    /**
     * Create some default policy parameters to help test the change mechanism.
     * NOTE: parameters are merged vs replaced so this can be a partial map.
     */
    private ParameterMap createPolicyMap() {
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.MP_POLL_PERIOD, pollInterval);
        map.setParameter(ParameterState.GP_COLLECTOR_INTERVAL, statsInterval);
        map.setParameter(ParameterState.SN_SERVICE_STOP_WAIT, "10000 ms");

        /*
         * Disable RN key distribution stats because they produce other store
         * traffic and throw off the checks made in this test.
         */
        map.setParameter(ParameterState.RN_SG_ENABLED, "false");
        return map;
    }

    /**
     * Check the state of the .stat file. If {@code populated} is {@code true}
     * then the file length must be non-zero, otherwise the file must be empty.
     *
     * @param populated true if the .stat file should be non-empty
     */
    private void checkStatFile(final boolean populated) throws Exception {
        AdminTestConfig testConfig =
            new AdminTestConfig(kvstoreName, portFinder);
        final File statFile = new File(testConfig.getTestDir(),
                                       File.separator +
                                       "cpstore" +
                                       File.separator +
                                       "log" +
                                       File.separator +
                                       "cpstore_0.stat");

        /* The stat file is always created, so check if not */
        assertTrue(statFile.exists());

        /* Check if the file is populated only if it is supposed to be */
        boolean success = new PollCondition(1000, 10000) {

            @Override
            protected boolean condition() {
                return (populated ^ (statFile.length() == 0));
            }
        }.await();
        assertTrue("Expect there to be " + (populated ? " a " : " no ") +
                   "perf file", success);
    }

    /**
     * A listener that expects to see performance events.
     * Note that this class polls the Admin via the command service API to
     * get and count events.
     */
    private class PerfListener extends NonBlockingTrackerListener {
        private final CommandServiceAPI cs;
        private CountDownLatch waitForEvents;

        private int expectedSingleIntervalOps;
        private int expectedSingleCumulativeOps;
        private int expectedMultiIntervalOps;
        private int expectedMultiCumulativeOps;

        private long seenSingleIntervalOps;
        private long seenSingleCumulativeOps;
        private long seenMultiIntervalOps;
        private long seenMultiCumulativeOps;

        private RemoteException remoteException = null;

        PerfListener() throws RemoteException, NotBoundException {
            super();
            cs = RegistryUtils.getAdmin(testhost,
                                        portFinder.getRegistryPort(),
                                        null /* loginMgr */, logger);
            setInterestingTime(System.currentTimeMillis());
        }

        /**
         * Catches up for the perf stats.
         * Starts the polling thread, waits for the specified time and set the
         * expected values to the seen values.
         */
        void catchup(long timeoutMillis) throws Exception {
            waitForEvents = new CountDownLatch(0);
            start();
            Thread.sleep(timeoutMillis);
            stop();
            expectedSingleIntervalOps = (int) seenSingleIntervalOps;
            expectedSingleCumulativeOps = (int) seenSingleCumulativeOps;
            expectedMultiIntervalOps = (int) seenMultiIntervalOps;
            expectedMultiCumulativeOps = (int) seenMultiCumulativeOps;
            logger.fine(String.format(
                "PerfListener#catchup done, "
                + "singleInterval=%s, singleCumulative=%s, "
                + "multiInterval=%s, multiCumulative=%s",
                expectedSingleIntervalOps, expectedSingleCumulativeOps,
                expectedMultiIntervalOps, expectedMultiCumulativeOps));
        }

        /**
         * Starts a polling thread that will wait for a total of
         * numSingleEvents + numMultiEvents events.
         */
        void setupWait(int numSingleEvents, int numMultiEvents) {
            expectedSingleIntervalOps = numSingleEvents;
            expectedSingleCumulativeOps += numSingleEvents;
            expectedMultiIntervalOps = numMultiEvents;
            expectedMultiCumulativeOps += numMultiEvents;

            waitForEvents = new CountDownLatch(numSingleEvents +
                                               numMultiEvents);

            seenSingleIntervalOps = 0;
            seenMultiIntervalOps = 0;
            start();
        }

        /**
         * Waits for number of events specified in setupWait() and checks for
         * remote exception as well as the expected number of events.
         */
        void await() throws InterruptedException, RemoteException {
            waitForEvents.await();

            assertEquals("singleCumulative",
                         expectedSingleCumulativeOps, seenSingleCumulativeOps);
            assertEquals("singleInterval",
                         expectedSingleIntervalOps, seenSingleIntervalOps);
            assertEquals("multiCumlative",
                         expectedMultiCumulativeOps, seenMultiCumulativeOps);
            assertEquals("multiInterval",
                         expectedMultiIntervalOps, seenMultiIntervalOps);
            stop();

            if (remoteException != null) {
                throw remoteException;
            }
        }

        @Override
        public void newEvents() {
            try {
                Tracker.RetrievedEvents<PerfEvent> perfEvents =
                    cs.getPerfSince(getInterestingTime());
                long newTime = perfEvents.getLastSyntheticTimestamp();
                setInterestingTime(newTime);

                for (PerfEvent pe : perfEvents.getEvents()) {
                    if (pe.getResourceId().getType().isRepNode()) {
                        final LatencyInfo singleInt = pe.getSingleInt();
                        final LatencyInfo multiInt = pe.getMultiInt();

                        /* Check that we see the expected number of operations. */
                        long numOps = singleInt.getLatency().getOperationCount();
                        seenSingleIntervalOps += numOps;
                        seenSingleCumulativeOps += numOps;

                        numOps = multiInt.getLatency().getOperationCount();
                        seenMultiIntervalOps += numOps;
                        seenMultiCumulativeOps += numOps;
                    }
                }

                for (int i = 0;
                     i < (seenSingleIntervalOps + seenMultiIntervalOps);
                     i++) {
                    waitForEvents.countDown();
                }
            } catch (RemoteException re) {
                remoteException = re;
            }
        }
    }
}
