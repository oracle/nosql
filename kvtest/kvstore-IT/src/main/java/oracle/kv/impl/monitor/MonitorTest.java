/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.monitor;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.LoggerMessage;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.measurement.ProxiedServiceStatusChange;
import oracle.kv.impl.measurement.Pruned;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.measurement.StackTrace;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.monitor.views.ServiceStatusTracker;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.server.LoggerUtils;


import org.junit.Test;


/**
 *
 */
public class MonitorTest extends TestBase {

    private long lastStatusTime = 0;

    private final LatencyInfo EMPTY_MULTI_INT =
        new LatencyInfo(
            PerfStatType.USER_MULTI_OP_INT, 0, 0, new LatencyResult());

    private final LatencyInfo EMPTY_MULTI_CUM =
        new LatencyInfo(
            PerfStatType.USER_MULTI_OP_CUM, 0, 0, new LatencyResult());

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Test the publishing of monitoring data from the Admin process.
     */
    @Test
    public void testLocal()
        throws IOException {

        /* Set up Monitor and mock agents. */
        AdminTestConfig testConfig = new AdminTestConfig(kvstoreName);
        AdminServiceParams params = testConfig.getParams();
        Monitor monitor = new FakeAdmin(params).getMonitor();
        monitor.setupExistingAgents(new Topology(kvstoreName));

        /*
         * Status changes never really happen locally, but we're using them to
         * test the local monitoring path. Add two service status change
         * listeners that are awaiting particular status changes.
         */
        StatusChangeListener
            rep1Running = new StatusChangeListener(new RepNodeId(1, 1),
                                                   ServiceStatus.RUNNING);
        StatusChangeListener
            rep2Error =
            new StatusChangeListener(new RepNodeId(1, 2),
                                     ServiceStatus.ERROR_NO_RESTART);
        monitor.trackStatusChange(rep1Running);
        monitor.trackStatusChange(rep2Error);

        /*
         * Make sure that their countdown latches are at 1, and did not yet
         * trigger.
         */
        assertEquals(1, rep1Running.getCount());
        assertEquals(1, rep2Error.getCount());

        /*
         * Mimic service status changes. They should not trigger the listeners
         * because they are not for the right resource/status combination.
         */
        List<Measurement> mList = new ArrayList<Measurement>();
        mList.add(new ServiceStatusChange(ServiceStatus.RUNNING));
        ResourceId rId = new RepNodeId(1,3);
        monitor.publish(rId, mList);
        assertEquals(1, rep1Running.getCount());
        assertEquals(1, rep2Error.getCount());
        ServiceStatusTracker health = monitor.getServiceChangeTracker();
        checkQueue(rId, mList, health);

        mList.clear();
        mList.add(new ServiceStatusChange(ServiceStatus.ERROR_NO_RESTART));
        rId = new RepNodeId(1,3);
        monitor.publish(rId, mList);
        assertEquals(1, rep1Running.getCount());
        assertEquals(1, rep2Error.getCount());
        checkQueue(rId, mList, health);

        /* This next service status should trigger one listener. */
        rId = new RepNodeId(1,2);
        monitor.publish(rId, mList);
        assertEquals(1, rep1Running.getCount());
        assertEquals(0, rep2Error.getCount());
        checkQueue(rId, mList, health);

        /* Again, this next service status should trigger one listener. */
        mList.clear();
        mList.add(new ServiceStatusChange(ServiceStatus.RUNNING));
        rId = new RepNodeId(1,1);
        monitor.publish(new RepNodeId(1, 1), mList);
        assertEquals(0, rep1Running.getCount());
        assertEquals(0, rep2Error.getCount());
        checkQueue(rId, mList, health);
        monitor.shutdown();
    }

    private void checkQueue(ResourceId expectedId,
                            List<Measurement> expectedStatus,
                            ServiceStatusTracker health) {

        /* Compare what is on the queue with what is expected by the test. */
        RetrievedEvents<ServiceChange> info =
            health.retrieveNewEvents(lastStatusTime);
        lastStatusTime = info.getLastSyntheticTimestamp();
        Iterator<Measurement> expectedVals = expectedStatus.iterator();
        for (ServiceChange m : info.getEvents()) {
            Measurement expected = expectedVals.next();
            ServiceStatusChange expectedChange = (ServiceStatusChange) expected;
            assertEquals(expectedChange.getStatus(), m.getStatus());
            assertEquals(expectedId, m.getTarget());
        }
        /* We should have gone through everything from the queue. */
        assertFalse(expectedVals.hasNext());
    }

    /**
     * Test the generation of direct monitoring information from the
     * RepNode or SNA.
     */
    @Test
    public void testRemote()
        throws IOException, InterruptedException {

        /* Set up Monitor and mock agents. */
        AdminTestConfig testConfig = new AdminTestConfig(kvstoreName);
        AdminServiceParams params = testConfig.getParams();
        KVRepTestConfig repTestConfig =
            new KVRepTestConfig(this, 1, 2, 1, 10);
        Monitor monitor = new FakeAdmin(params).getMonitor();
        monitor.setupExistingAgents(repTestConfig.getTopology());
        MockMonitorAgent [] testAgents = createAgents(repTestConfig);

        int numTestAgents = testAgents.length;
        StatusChangeListener[] listenersForRunning =
            new StatusChangeListener[numTestAgents];
        StatusChangeListener[] listenersForStopping =
            new StatusChangeListener[numTestAgents];

        for (int i = 0; i < numTestAgents; i++) {
            MockMonitorAgent agent = testAgents[i];
            monitor.registerAgent("localhost",
                                  agent.getRegistryPort(),
                                  agent.getResourceId());
            listenersForRunning[i] =
                new StatusChangeListener(agent.getResourceId(),
                                         ServiceStatus.RUNNING);
            monitor.trackStatusChange(listenersForRunning[i]);
            listenersForStopping[i] =
                new StatusChangeListener(agent.getResourceId(),
                                         ServiceStatus.STOPPING);
            monitor.trackStatusChange(listenersForStopping[i]);
            assertEquals(1, listenersForStopping[i].getCount());
        }

        logger.fine("-- collect now --");
        monitor.collectNow();
        logger.fine("-- set to unknown --");
        for (int i = 0; i < numTestAgents; i++) {
            assertEquals(1, listenersForRunning[i].getCount());
            testAgents[i].getStatusTracker().update(ServiceStatus.UNREACHABLE);
        }

        logger.fine("-- collect now --");
        monitor.collectNow();

        logger.fine("-- set to running --");
        for (int i = 0; i < numTestAgents; i++) {
            assertEquals(1, listenersForRunning[i].getCount());
            testAgents[i].getStatusTracker().update(ServiceStatus.RUNNING);
        }

        logger.fine("-- collect now --");
        monitor.collectNow();

        for (StatusChangeListener l : listenersForRunning) {
            assertTrue(l.await(10, TimeUnit.SECONDS));
        }

        /*
         * Issue Stopping statuses for each test agent, but have them
         * reported through a third party.
         */
        logger.fine("--Set to stopping through proxied statuses--");
        StorageNodeId sna100 = new StorageNodeId(100);
        MockMonitorAgent reportingAgent =
                new MockMonitorAgent(repTestConfig.getGlobalParams(),
                                     repTestConfig.getAnyStorageNodeParams(),
                                     sna100);
        monitor.registerAgent("localhost",
                              reportingAgent.getRegistryPort(),
                              reportingAgent.getResourceId());
        for (int i = 0; i < numTestAgents; i++) {
            assertEquals(1, listenersForStopping[i].getCount());
            ServiceStatusChange changeForAgent =
                (new ProxiedServiceStatusChange(testAgents[i].getResourceId(),
                                                ServiceStatus.STOPPING));
            reportingAgent.add(changeForAgent);
        }

        monitor.collectNow();
        for (StatusChangeListener l : listenersForStopping) {
            assertTrue(l.await(10, TimeUnit.SECONDS));
        }

        /**
         * Issue some performance stats.
         */
        logger.fine("generating performance stats");
        PerfStatType[] summaryCriteria = new PerfStatType[] {
            PerfStatType.USER_SINGLE_OP_INT,
            PerfStatType.USER_SINGLE_OP_CUM};

        PerfWatcher perfWatcher = new PerfWatcher();
        monitor.trackPerfChange(perfWatcher);
        for (int n = 0; n< 10; n++) {
            for (int i = 0; i < numTestAgents; i++) {
                StatsPacket packet = makeStatsPacket(i, 0, 10000*(n+1));
                Map<PerfStatType, LatencyInfo> summary =
                    packet.summarizeLatencies(summaryCriteria);
                for (LatencyInfo l : summary.values()) {
                    packet.add(l);
                }
                packet.add(EMPTY_MULTI_INT);
                packet.add(EMPTY_MULTI_CUM);

                perfWatcher.addExpected(testAgents[i].getResourceId(), summary);
                testAgents[i].add(packet);
            }
        }

        monitor.collectNow();

        /* Check for perf alerts. */
        perfWatcher.waitForPerf();

        monitor.shutdown();
        shutdownAgents(testAgents);
    }

    /**
     * Check that we support the repeatd addition of monitor agents, and that
     * the collector pool stays the same size. Supports task retry.
     * @throws IOException
     */
    @Test
    public void testRepeatAdds() throws IOException {

        /* Set up Monitor and mock agents. */
        AdminTestConfig testConfig = new AdminTestConfig(kvstoreName);
        AdminServiceParams params = testConfig.getParams();
        Monitor monitor = new FakeAdmin(params).getMonitor();

        KVRepTestConfig repTestConfig =
            new KVRepTestConfig(this, 1, 2, 1, 10);
        MockMonitorAgent [] testAgents = createAgents(repTestConfig);
        int numTestAgents = testAgents.length;

        for (int i = 0; i < numTestAgents; i++) {
            MockMonitorAgent agent = testAgents[i];
            monitor.registerAgent("localhost",
                                  agent.getRegistryPort(),
                                  agent.getResourceId());
        }
        assertEquals(numTestAgents, monitor.getNumCollectorAgents());

        /* Add them again, should have no ill effect. */
        for (int i = 0; i < numTestAgents; i++) {
            MockMonitorAgent agent = testAgents[i];
            monitor.registerAgent("localhost",
                                  agent.getRegistryPort(),
                                  agent.getResourceId());
        }
        assertEquals(numTestAgents, monitor.getNumCollectorAgents());

        monitor.shutdown();
        shutdownAgents(testAgents);
    }

    private StatsPacket makeStatsPacket(int numAgent, long start, long end) {
        StatsPacket packet = new StatsPacket(start, end, "rg1-rn1", "rg1");

        packet.add(new LatencyInfo
                   (PerfStatType.PUT_IF_ABSENT_INT, start, end,
                    new LatencyResult(20, 20,
                                      1, 100, 10 * numAgent,
                                      95, 99, 0)));
        packet.add(new LatencyInfo
                   (PerfStatType.PUT_IF_VERSION_INT, start, end,
                    new LatencyResult(20, 20,
                                      2, 200, 20,
                                      195, 199, 0)));
        packet.add(new LatencyInfo
                   (PerfStatType.PUT_IF_ABSENT_CUM, start, end,
                    new LatencyResult(3000, 3000,
                                      1, 200, 10 * numAgent,
                                      195, 199, 0)));
        packet.add(new LatencyInfo
                   (PerfStatType.PUT_IF_VERSION_CUM, start, end,
                    new LatencyResult(5000, 5000,
                                      2, 200, 50,
                                      195, 199, 0)));
        return packet;
    }

    /**
     * Very localized test of ServiceStatusTracker tracker view.
     */
    @Test
    public void testServiceChangeTracker() {

        ServiceStatusTracker tracker = new ServiceStatusTracker();

        RepNodeId rg10n10 = new RepNodeId(10, 10);
        RepNodeId rg9n9 = new RepNodeId(9, 9);
        RepNodeId rg8n8 = new RepNodeId(8, 8);
        StorageNodeId sna1 = new StorageNodeId(1);

        /* Nothing in the tracker. */
        Map<ResourceId, ServiceChange> allStatus = tracker.getStatus();
        assertTrue(allStatus.size() == 0);

        RetrievedEvents<ServiceChange> statusContainer =
            tracker.retrieveNewEvents(lastStatusTime);
        lastStatusTime = statusContainer.getLastSyntheticTimestamp();
        List<ServiceChange> statusQueue = statusContainer.getEvents();
        assertTrue(statusQueue.size() == 0);

        /* Insert one status change. */
        ServiceStatusChange starting =
            new ServiceStatusChange(ServiceStatus.STARTING);

        tracker.newInfo(rg10n10, starting);
        allStatus = tracker.getStatus();
        statusContainer = tracker.retrieveNewEvents(lastStatusTime);
        lastStatusTime = statusContainer.getLastSyntheticTimestamp();
        statusQueue = statusContainer.getEvents();

        assertEquals(1, allStatus.size());
        assertEquals(ServiceStatus.STARTING,
                     allStatus.get(rg10n10).getStatus());
        assertEquals(1, statusQueue.size());
        assertEquals(ServiceStatus.STARTING, statusQueue.get(0).getStatus());

        /*
         * Insert another status change, and a proxied change. Make sure the
         * proxied change is attributed to the right resource, and that the
         * other status change supercedes the previous status.
         */
        ServiceStatusChange running =
            new ServiceStatusChange(ServiceStatus.RUNNING);
        ServiceStatusChange stopping =
            new ProxiedServiceStatusChange(rg9n9, ServiceStatus.STOPPING);
        ServiceStatusChange stopped =
            new ProxiedServiceStatusChange(rg8n8, ServiceStatus.STOPPED);
        tracker.newInfo(rg10n10, running);
        tracker.newInfo(sna1, stopping);
        tracker.newInfo(sna1, stopped);
        allStatus = tracker.getStatus();
        statusContainer = tracker.retrieveNewEvents(lastStatusTime);
        lastStatusTime = statusContainer.getLastSyntheticTimestamp();
        statusQueue = statusContainer.getEvents();
        assertEquals(3, allStatus.size());
        assertEquals(ServiceStatus.RUNNING,
                     allStatus.get(rg10n10).getStatus());
        assertEquals(ServiceStatus.STOPPING,
                     allStatus.get(rg9n9).getStatus());
        assertEquals(ServiceStatus.STOPPED,
                     allStatus.get(rg8n8).getStatus());
        assertEquals(3, statusQueue.size());
        assertEquals(ServiceStatus.RUNNING, statusQueue.get(0).getStatus());
        assertEquals(rg10n10, statusQueue.get(0).getTarget());
        assertEquals(ServiceStatus.STOPPING, statusQueue.get(1).getStatus());
        assertEquals(rg9n9, statusQueue.get(1).getTarget());
        assertEquals(ServiceStatus.STOPPED, statusQueue.get(2).getStatus());
        assertEquals(rg8n8, statusQueue.get(2).getTarget());

        /*
         * Insert a status that has come out of order. This status should not
         * take effect.
         */
        ServiceStatusChange error =
            new ServiceStatusChange(ServiceStatus.ERROR_NO_RESTART,
                                    running.getTimeStamp() - 10);
        tracker.newInfo(rg10n10, error);
        allStatus = tracker.getStatus();
        statusContainer = tracker.retrieveNewEvents(lastStatusTime);
        lastStatusTime = statusContainer.getLastSyntheticTimestamp();
        statusQueue = statusContainer.getEvents();
        assertEquals(3, allStatus.size());
        assertEquals(ServiceStatus.RUNNING,
                     allStatus.get(rg10n10).getStatus());
        assertEquals(ServiceStatus.STOPPING,
                     allStatus.get(rg9n9).getStatus());
        assertEquals(ServiceStatus.STOPPED,
                     allStatus.get(rg8n8).getStatus());
        assertEquals(0, statusQueue.size());

        /*
         * Make a proxied status change, that says that the service is
         * unreachable. If the previous status is terminal, the UNREACHABLE
         * should not be applied, because it will cause information to be lost.
         * But make an exception for the STOPPING state so it doesn't seem that
         * the SN is stuck forever in STOPPING mode.
         */
        ServiceStatusChange unreachable =
            new ServiceStatusChange(ServiceStatus.UNREACHABLE);

        tracker.newInfo(rg10n10, unreachable);
        tracker.newInfo(rg9n9, unreachable);
        tracker.newInfo(rg8n8, unreachable);
        allStatus = tracker.getStatus();
        statusContainer = tracker.retrieveNewEvents(lastStatusTime);
        lastStatusTime = statusContainer.getLastSyntheticTimestamp();
        statusQueue = statusContainer.getEvents();
        assertEquals(3, allStatus.size());

        /*
         * rg10n10 and rg9n9 should become unreachable, but rg8n8 should still
         * be STOPPED.
         */
        assertEquals(ServiceStatus.UNREACHABLE,
                     allStatus.get(rg10n10).getStatus());
        assertEquals(ServiceStatus.UNREACHABLE,
                     allStatus.get(rg9n9).getStatus());
        assertEquals(ServiceStatus.STOPPED,
                     allStatus.get(rg8n8).getStatus());
        assertEquals(2, statusQueue.size());
        assertEquals(ServiceStatus.UNREACHABLE, statusQueue.get(0).getStatus());
        assertEquals(rg10n10, statusQueue.get(0).getTarget());
        assertEquals(ServiceStatus.UNREACHABLE, statusQueue.get(1).getStatus());
        assertEquals(rg9n9, statusQueue.get(1).getTarget());

        /*
         * The ServiceStatusTracker should filter out duplicate service
         * statuses.
         */
        ServiceStatusChange duplicate =
            new ServiceStatusChange(ServiceStatus.UNREACHABLE,
                                    unreachable.getTimeStamp() + 100);
        tracker.newInfo(rg10n10, duplicate);
        tracker.newInfo(rg9n9, duplicate);
        statusContainer = tracker.retrieveNewEvents(lastStatusTime);
        lastStatusTime = statusContainer.getLastSyntheticTimestamp();
        statusQueue = statusContainer.getEvents();
        assertEquals(0, statusQueue.size());
    }

    /**
     * Test that logging output from a remote service is funneled into the
     * Monitor.
     */
    @Test
    public void testRemoteLogging()
        throws IOException, InterruptedException {

        /* Set up Monitor and mock agents. */
        logger.fine("testRemoteLogging ----------------");
        AdminTestConfig adminTestConfig =
            new AdminTestConfig(kvstoreName);
        AdminServiceParams adminParams = adminTestConfig.getParams();
        KVRepTestConfig repTestConfig =
            new KVRepTestConfig(this, 1, 2, 1, 10);
        Monitor monitor = new FakeAdmin(adminParams).getMonitor();

        monitor.setupExistingAgents(repTestConfig.getTopology());

        MockMonitorAgent [] testAgents = createAgents(repTestConfig);
        int numTestAgents = testAgents.length;

        /*
         * Generate and save test messages.  The Storewide view is going to
         * prepend the resource id, so do that for the test messages.
         */
        MsgCounter msgCounter = new MsgCounter();
        for (MockMonitorAgent agent : testAgents) {
            ResourceId id = agent.getResourceId();
            msgCounter.add(Level.INFO, "[" + id + "] MsgA:" + id);
            msgCounter.add(Level.WARNING, "[" + id + "] MsgB:" + id);

            /*
             * Test that LogRecord parameters are transmitted correctly
             * [KVSTORE-115]
             */
            msgCounter.add(Level.INFO, "[" + id + "] MsgC null:" + id);
            msgCounter.add(Level.INFO, "[" + id + "] MsgD obj0 4:" + id);
        }
        monitor.setLoggerHook(msgCounter);

        /* Tell the monitor about the mock agents */
        for (int i = 0; i < numTestAgents; i++) {
            MockMonitorAgent agent = testAgents[i];
            monitor.registerAgent("localhost",
                                  agent.getRegistryPort(),
                                  agent.getResourceId());
        }

        /* Issue the first set of expected messages. */
        for (MockMonitorAgent agent : testAgents) {
            agent.getLogger().info("MsgA:" + agent.getResourceId());
        }

        logger.fine("-- collect now --");
        monitor.collectNow();

        /* Issue the second set of expected messages. */
        for (MockMonitorAgent agent : testAgents) {
            agent.getLogger().warning("MsgB:" + agent.getResourceId());
            agent.getLogger().log(Level.INFO,
                                  "MsgC {0}:" + agent.getResourceId(),
                                  new Object[] { null });
            agent.getLogger().log(Level.INFO,
                                  "MsgD {0} {1}:" + agent.getResourceId(),
                                  new Object[] {
                                      new Object() {
                                          @Override
                                          public String toString() {
                                              return "obj0"; }
                                      },
                                      4
                                  });
        }

        monitor.collectNow();
        msgCounter.waitForMsgs();
        monitor.shutdown();
        shutdownAgents(testAgents);

    }

    /**
     * Test Pruned indirectly adding more Measurements than the AgentRepository
     * has defined as its maxSize.
     * AgentRepository with maxSize == 10.
     */
    @Test
    public void testAgentRepositoryFull()
            throws IOException, InterruptedException {

        /* Set up agents. */
        final int maxSize = 10;
        final AgentRepository agentRepository = new AgentRepository("kvTest",
                null,
                maxSize);
        KVRepTestConfig repTestConfig =
                new KVRepTestConfig(this, 1, 2, 1, 10);
        StorageNodeId sna100 = new StorageNodeId(100);
        MockMonitorAgent reportingAgent =
                new MockMonitorAgent(repTestConfig.getGlobalParams(),
                repTestConfig.getAnyStorageNodeParams(),
                sna100);

        StackTrace stackTrace = new StackTrace(20, "Trace: " + 20);
        agentRepository.add(stackTrace);

        ServiceStatusChange changeForAgent = null;
        for (int i = 0; i < maxSize; i++) {
            changeForAgent = 
                    (new ProxiedServiceStatusChange(reportingAgent.getResourceId(),
                    ServiceStatus.RUNNING));
            agentRepository.add(changeForAgent);
            TimeUnit.SECONDS.sleep(1);
        }
        /*
         * Add 2 repeated measurements to exceed the agentRepository maxSize property.
         */
        agentRepository.add(changeForAgent);
        agentRepository.add(changeForAgent);

        /* assertions block */
        List<Measurement> measurements = agentRepository.getAndReset().measurements;
        assertEquals(maxSize + 1, measurements.size());
        for (Measurement m : measurements) {
            if (m instanceof Pruned) {
                Pruned pruned = ((Pruned) m);
                 /* we are 3 over the maxSize (10) */
                assertEquals(3, pruned.getNumRemoved());
                assertNotNull(pruned.toString());
            }
        }
        measurements = agentRepository.getAndReset().measurements;
        assertEquals(0, measurements.size());

        reportingAgent.shutdown();
    }

    /**
     * Create a monitor agent for each sn in the group.
     */
    private MockMonitorAgent[] createAgents(KVRepTestConfig repTestConfig)
        throws IOException {

        StorageNodeMap snMap = repTestConfig.getTopology().getStorageNodeMap();
        MockMonitorAgent[] testAgents = new MockMonitorAgent[snMap.size()];
        StorageNode sns[] = new StorageNode[snMap.size()];
        sns = snMap.getAll().toArray(sns);

        for (int i = 0; i < testAgents.length; i++) {

            StorageNodeId snId = sns[i].getStorageNodeId();
            testAgents[i] = new MockMonitorAgent
                (repTestConfig.getGlobalParams(),
                 repTestConfig.getStorageNodeParams(snId), snId);

        }
        return testAgents;
    }

    private void shutdownAgents(MockMonitorAgent[] testAgents)
        throws RemoteException {
        for (MockMonitorAgent agent : testAgents) {
            agent.shutdown();
        }
    }

    /**
     * Used to count how many logging messages are received at the Monitor's
     * logging view.
     */
    private class MsgCounter implements TestHook<LoggerMessage> {

        /**
         * Use Strings instead of LoggerMessages for the expected set, because
         * the LogRecord held within the LoggerMessage will have a different
         * timestamp.
         */
        private final Set<String> expected;
        private final CountDownLatch isEmpty;

        MsgCounter() {
            expected = new HashSet<String>();
            isEmpty = new CountDownLatch(1);
        }

        void add(Level level, String msg) {
             expected.add(msg + level);
        }

        /**
         * Check that the test messages arrived at the view, and that they are
         * the expected ones. Ignores extra messages, since other logging may
         * be going on.
         */
        @Override
        public synchronized void doHook(LoggerMessage arrived) {
            String arrivedMsg = arrived.toString() +
                                arrived.getLogRecord().getLevel();
            expected.remove(arrivedMsg);
            if (expected.size() == 0) {
                isEmpty.countDown();
            }
        }

        void waitForMsgs()
            throws InterruptedException {

            assertTrue(isEmpty.await(30, SECONDS));
        }
    }

    /**
     * Used to detect that performance stats have arrived at the monitor.
     */
    private class PerfWatcher implements ViewListener<PerfEvent> {

        private final Set<Bundle> expected;
        private final CountDownLatch isEmpty;

        PerfWatcher() {
            isEmpty = new CountDownLatch(1);
            expected = new HashSet<Bundle>();
        }

        private void addExpected(ResourceId resourceId,
                                 Map<PerfStatType, LatencyInfo> vals) {
            expected.add
                (new Bundle
                 (new PerfEvent(resourceId,
                                vals.get(PerfStatType.USER_SINGLE_OP_INT),
                                vals.get(PerfStatType.USER_SINGLE_OP_CUM),
                                0, 0,
                                /* this test has no multis. */
                                EMPTY_MULTI_INT,
                                EMPTY_MULTI_CUM)));
        }

        void waitForPerf()
            throws InterruptedException {
            isEmpty.await();
        }

        @Override
        public synchronized void newInfo(ResourceId resourceId,
                                         PerfEvent newData) {
            Bundle arrived = new Bundle(newData);
            boolean removed = expected.remove(arrived);
            assertTrue("Couldn't find " + arrived, removed);
            if (expected.size() == 0) {
                isEmpty.countDown();
            }
        }

        /*
         * Just a wrapper so we can provide a suitable equals and hashCode for
         * test use.
         */
        private class Bundle {
            final PerfEvent event;

            public Bundle(PerfEvent event) {
                this.event = event;
            }

            @Override
            public int hashCode() {
                return (toString().hashCode());
            }

            @Override
            public boolean equals(Object obj) {

                logger.fine("Comparing " + this + " to " + obj);

                if (obj == null) {
                    return false;
                }
                if (!(obj instanceof Bundle)) {
                    return false;
                }
                Bundle other = (Bundle) obj;
                return this.toString().equals(other.toString());
            }

            @Override
            public String toString() {
                return event.toString();
            }
        }
    }

    /*
     * The StatusChangeListener listens and waits for service state changes
     */
    private class StatusChangeListener
        implements ViewListener<ServiceStatusChange>{

        private final CountDownLatch sawChange;
        private final ResourceId targetResource;
        private final ServiceStatus targetStatus;

        public StatusChangeListener(ResourceId targetResource,
                                    ServiceStatus targetStatus) {
            this.targetResource = targetResource;
            this.targetStatus = targetStatus;
            sawChange = new CountDownLatch(1);
        }

        @Override
        public void newInfo(ResourceId resourceId,
                            ServiceStatusChange change) {

            ResourceId changeTarget = change.getTarget(resourceId);

            if (changeTarget.equals(targetResource) &&
                change.getStatus().equals(targetStatus)) {
                logger.finest("Applied status change for " +
                              changeTarget + " " +   change);
                sawChange.countDown();
            }
        }

        public boolean await(long timeout, TimeUnit unit)
            throws InterruptedException {

            return sawChange.await(timeout, unit);
        }

        public long getCount() {
            return sawChange.getCount();
        }
    }

    private class FakeAdmin implements MonitorKeeper {

    	private final Monitor monitor;

    	FakeAdmin(AdminServiceParams params) {
            monitor = new Monitor(params, this, null /* loginMgr */);
    	}

        @Override
        public Monitor getMonitor() {
            return monitor;
        }

        @Override
        public int getLatencyCeiling(ResourceId rnid) {
            return 0;
        }

        @Override
        public int getThroughputFloor(ResourceId rnid) {
            return 0;
        }
    }
}
