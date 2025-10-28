/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.util.KVRepTestConfig.RG1_RN1_ID;
import static oracle.kv.impl.util.KVRepTestConfig.RG1_RN2_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.FaultException;
import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Durability;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.monitor.MonitorAgentAPI;
import oracle.kv.impl.rep.RepNodeService.KVStoreCreator;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.ServiceStatusTracker.Listener;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.impl.RepImpl;

import org.junit.Test;

/**
 * Run tests at the service level.
 */
public class RepNodeServiceTest extends UncaughtExceptionTestBase {

    final RepNodeId rg1n1Id = new RepNodeId(1, 1);
    final LoginManager LOGIN_MGR = null;
    private KVRepTestConfig config;
    private volatile boolean ignoreUncaughtException;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (config != null) {
            config.stopRepNodeServices();
        }
        LoggerUtils.closeAllHandlers();
        RepNodeService.startTestHook = null;
        RepNodeService.stopRequestedTestHook = null;
    }

    /** Ignore uncaught exceptions if ignoreUncaughtException is true. */
    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
        if (!ignoreUncaughtException) {
            super.uncaughtException(thread, exception);
        }
    }

    /**
     * Verify startup where a Topology is transmitted via a push from another
     * RN in the RG, rather than through the SNA's configure call.
     */
    @Test
    public void testStartupTopologyFromRNInRG()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 3, 10);

        final RepNodeId noTopologyRN = new RepNodeId(1, 3);
        config.startRepNodeServices(Collections.singleton(noTopologyRN));

        final RepNodeService noTopoService =
            config.getRepNodeService(noTopologyRN);

        final boolean hasTopology = new PollCondition(10, 60000) {

            @Override
            protected boolean condition() {
                return (noTopoService.getRepNode().getTopology() != null);
            }

        }.await();

        assertTrue(hasTopology);

        final int seqNumber =
            noTopoService.getRepNode().getTopology().getSequenceNumber();

        assertEquals(config.getTopology().getSequenceNumber(), seqNumber);

        config.stopRepNodeServices();
    }

    /**
     * Simulate startup failure case when the SNA fails to supply the initial
     * topology via the configure call and a client supplies it instead.
     *
     * Startup a KVS with two shards. rg1 gets topology as usual from the SNA.
     * rg2 does not and must acquire it from the client's dispatcher.
     */
    @Test
    public void testStartupTopologyFromClient()
        throws IOException, KVStoreException {

        /* A 2 RG, 3 RF config */
        config = new KVRepTestConfig(this, 1, 2, 3, 10);

        /* The RNs in RG2 that do not get topology from the SNA */
        final HashSet<RepNodeId> noTopologyRNset =
            new HashSet<RepNodeId>(Arrays.asList(new RepNodeId(2, 1),
                                                 new RepNodeId(2, 2),
                                                 new RepNodeId(2, 3)));
        config.startRepNodeServices(noTopologyRNset);

        boolean hasTopology = new PollCondition(10, 5000) {

            @Override
            protected boolean condition() {
                for (RepNodeService rns : config.getRepNodeServices()) {
                    if (rns.getRepNode().getTopology() == null) {
                        return false;
                    }
                }
                return true;
            }
        }.await();

        /* Verify that no topology was provided */
        assertTrue(!hasTopology);

        /* Start up the client dispatcher, so it can provide topology. */
        final RequestDispatcherImpl dispatcher = createDispatcher();

        /* Wait for the topology to be propagated. */
        hasTopology = new PollCondition(10, 60000) {

            @Override
            protected boolean condition() {
                for (RepNodeService rns : config.getRepNodeServices()) {
                    if (rns.getRepNode().getTopology() == null) {
                        return false;
                    }
                }
                return true;
            }
        }.await();

        /* Verify that no topology was provided  to rg2 */
        assertTrue(hasTopology);

        /* Verify that all RNs are at the same topo sequence number. */
        for (RepNodeService rns : config.getRepNodeServices()) {
            final int seqNumber =
                rns.getRepNode().getTopology().getSequenceNumber();

            assertEquals(config.getTopology().getSequenceNumber(), seqNumber);
        }

        /* cleanup */
        dispatcher.shutdown(null);
        config.stopRepNodeServices();
    }

    /**
     * Verify that the service fault handler can be invoked and would have
     * caused the process to exit due to the unhandled fault.
     *
     * @throws IOException
     */
    @Test
    public void testFaultHandler()
        throws IOException {

        /* Create the simplest KVS */
        config = new KVRepTestConfig(this, 1, 1, 1, 10);
        config.startRepNodeServices();
        final RepNodeService rns =
            config.getRepNodeServices().iterator().next();
        final RepNodeStateUpdateThread updateThread =
            ((RequestDispatcherImpl) rns.getReqHandler()
            .getRequestDispatcher()).getStateUpdateThread();
        final NullPointerException npe = new NullPointerException();

        /* Ignore the expected uncaught exception */
        ignoreUncaughtException = true;
        try {
            updateThread.getUncaughtExceptionHandler().
                uncaughtException(updateThread, npe);

            final ServiceFaultHandler faultHandler =
                (ServiceFaultHandler) rns.getFaultHandler();

            assertEquals(npe, faultHandler.getShutdownFault());
        } finally {
            ignoreUncaughtException = false;
        }

        /*
         * Stop the components individually due to the nature of the
         * test which would have resulted in a process exit outside the test
         * environment.
         */
        rns.getReqHandler().stop();
        rns.getRepNode().stop(false);

        config.stopRepNodeServices();
    }

    /**
     * Tests the handling of a fault exception by service fault handler.
     * The test below simulates this condition by creating an fault exception 
     * after the RN is up and running.
     */
    @Test
    public void testFaultHandlerFE()
        throws IOException {

        /* Create the simplest KVS */
        config = new KVRepTestConfig(this, 1, 1, 1, 10);
        config.startRepNodeServices();
        final RepNodeService rns =
            config.getRepNodeServices().iterator().next();
        final RepNodeStateUpdateThread updateThread =
            ((RequestDispatcherImpl) rns.getReqHandler()
            .getRequestDispatcher()).getStateUpdateThread();

        final FaultException npe = new FaultException("test", true);
              
        /* Ignore the expected uncaught exception */
        ignoreUncaughtException = true;
        try {
            updateThread.getUncaughtExceptionHandler().
                uncaughtException(updateThread, npe);

            final ServiceFaultHandler faultHandler =
                (ServiceFaultHandler) rns.getFaultHandler();
            assertEquals(npe, faultHandler.getShutdownFault());
            faultHandler.rethrow(npe);
        } catch(RuntimeException rte) {
            assertTrue(rte.toString().contains("Resource id is: rg1-rn1"));
        } finally {
            ignoreUncaughtException = false;
        }

        /*
         * Stop the components individually due to the nature of the
         * test which would have resulted in a process exit outside the test
         * environment.
         */
        rns.getReqHandler().stop();
        rns.getRepNode().stop(false);

        config.stopRepNodeServices();
    }


    /**
     * Tests the handling of an async ILE.
     *
     * ILEs are typically encountered when the ReplicationNodeService is first
     * started. However, they can also be encountered after the service has
     * been started if the RN starts out in the UNKNOWN state, and subsequently
     * joins the group only to discover that it needs a network restore.
     *
     * The test below simulates this condition by creating an ILE after the
     * RN is up and running.
     */
    @Test
    public void testAsyncILE()
        throws IOException {

        /* Create the simplest KVS */
        config = new KVRepTestConfig(this, 1, 2, 2, 10);
        config.startRepNodeServices();


        final oracle.kv.impl.rep.RepNode rg1rn1 = config.getRN(RG1_RN1_ID);
        final ReplicatedEnvironment env1 = rg1rn1.getEnv(1000);
        assertEquals(ReplicatedEnvironment.State.MASTER, env1.getState());
        RepInternal.getRepImpl(env1);


        final oracle.kv.impl.rep.RepNode  rg1rn2 = config.getRN(RG1_RN2_ID);
        final ReplicatedEnvironment e2prev = rg1rn2.getEnv(1000);
        final RepImpl rimpl2 = RepInternal.getRepImpl(e2prev);
        final ReplicationNode node1 =
            rimpl2.getRepNode().getGroup().getNode(env1.getNodeName());

        /* Simulate an ILE at replica(rg1-rn2), to force a network restore. */
        final Set<ReplicationNode> logProviders = new HashSet<>();
        logProviders.add(node1);
        @SuppressWarnings("unused")
        final InsufficientLogException ile =
            new InsufficientLogException(rimpl2.getRepNode(), logProviders);

        final boolean restarted = new PollCondition(10, 60000) {

            @Override
            protected boolean condition() {
                /*
                 * Verify that a new environment handle was established.
                 */
                ReplicatedEnvironment e2 = rg1rn2.getEnv(0);
                return e2 != null && (e2 != e2prev) &&
                       e2.getState().isReplica();

            }
        }.await();

        assertTrue(restarted);

        config.stopRepNodeServices();
    }

    /**
     * Test to go through the motions of starting up, stopping and re-starting
     * an RN through the service. It simulates interactions with the SNA.
     */
    @Test
    public void testStartup()
        throws IOException, InterruptedException, KVStoreException,
               NotBoundException {

        /* Create the simplest KVS */
        config = new KVRepTestConfig(this, 1, 1, 1, 10);
        config.startupSNs();

        /* The service will be configured start up this rn. */
        RepNodeService rg1n1Service = config.createRNService(rg1n1Id);

        final SNASimulatorThread sna =
            new SNASimulatorThread(config.getTopology(), rg1n1Id);
        sna.start();

        final CountDownLatch runningLatch = new CountDownLatch(1);
        final ServiceStatusTracker tracker = rg1n1Service.getStatusTracker();
        final Listener listener = new ServiceStatusTracker.Listener() {

            @Override
            public void update(ServiceStatusChange prevStatus,
                               ServiceStatusChange newStatus) {
                if (ServiceStatus.RUNNING.equals(newStatus.getStatus())) {
                    assertEquals(ServiceStatus.STARTING,
                                 prevStatus.getStatus());
                    runningLatch.countDown();
                }
            }
        };
        tracker.addListener(listener);
        rg1n1Service.start();

        /* Join the simulator thread. */
        sna.join(60000); /* Wait up to 1 minute for it to make progress. */
        assertTrue(!sna.isAlive());
        /* Ensure there were no exceptions in that thread. */
        assertNull(sna.failException);

        /*
         * Wait up to 1 min for the service to transition to the running state.
         */
        assert (runningLatch.await(1, TimeUnit.MINUTES));
        assertEquals(ServiceStatus.RUNNING, tracker.getServiceStatus());

        final MonitorAgentAPI ma =
            getMonitorHandle(config.getTopology(), rg1n1Id);
        final List<Measurement> info = ma.getMeasurements();
        ServiceStatusChange ssc = null;
        for (Measurement m : info) {
            if (Metrics.SERVICE_STATUS.getId() == m.getId()) {
              ssc = (ServiceStatusChange) m;
            }
        }
        assertNotNull(ssc);
        assertEquals(ServiceStatus.RUNNING, ssc.getStatus());

        /* Simulate a client that uses the created service */
        simulateClient();

        /* Shut it down via the admin request. */
        sna.adminHandle.shutdown(false);

        /* Go through one more cycle, no sna this time. */
        rg1n1Service = config.createRNService(rg1n1Id);
        rg1n1Service.start();
        simulateClient();
        rg1n1Service.stop(false, "test");

        assertEquals(ServiceStatus.STOPPED, tracker.getServiceStatus());
        config.stopSNs();
    }


    /**
     * Test Absolute Consistency configurations
     */
    @Test
    public void testAbsConsistencyParamFalse()
        throws IOException, EnvironmentFailureException {
        testAuthMasterParamInit(false);
    }

    @Test
    public void testAbsConsistencyParamTrue()
        throws IOException, EnvironmentFailureException {
        testAuthMasterParamInit(true);
    }

    private void testAuthMasterParamInit(boolean disableAuthMaster)
        throws IOException, RemoteException {
        config = new KVRepTestConfig(this, 1, 1, 1, 2) {
            @Override
            public RepNodeParams createRepNodeParams
            (final StorageNode sn,
             final oracle.kv.impl.topo.RepNode rn,
             final int haPort) {
                RepNodeParams rnParams =
                    super.createRepNodeParams(sn, rn, haPort);
                rnParams.setDisableAuthMaster(disableAuthMaster);
                return rnParams;
            }
        };
        oracle.kv.impl.rep.RepNode rg1n1 = config.getRN(rg1n1Id);
        RequestHandlerImpl handler = config.getRH(rg1n1Id);
        Params params = config.getParams(rg1n1.getRepNodeId());
        handler.initialize(params, rg1n1, new OperationsStatsTracker());
        config.startupRHs();
        RepEnvHandleManager envManager = rg1n1.getRepEnvManager();
        ReplicatedEnvironment env = envManager.getEnv(1_000);
        /* Verify that JE environment reflects KV parameter. */
        assertEquals(disableAuthMaster,
                     env.getRepConfig().getDisableAuthoritativeMaster());
        config.stopRHs();
        config.stopRepNodeServices();
        config = null;
    }

    /**
     * Verify availability of a node in the unknown state for requests.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testUnknownState()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 3, 10);
        config.startRepNodeServices();
        config.stopRepNodeServices();

        /* Start just one RN wait for it to come up in the unknown state */
        config.startupSNs();
        config.startRepNodeSubset(rg1n1Id);

        final RepNode rn11 = config.getRepNodeService(rg1n1Id).getRepNode();

        assertEquals(State.UNKNOWN, rn11.getEnv(1).getState());

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());

        /* Verify that sufficiently weak consistency reads succeed. */
        kvs.get(Key.createKey("one"),
                Consistency.NONE_REQUIRED,
                1, TimeUnit.SECONDS);
        kvs.get(Key.createKey("one"),
                Consistency.NONE_REQUIRED_NO_MASTER,
                1, TimeUnit.SECONDS);

        /* Verify that stronger consistency reads fail. */
        long startMs = System.currentTimeMillis();
        try {
            /*
             * Use consistency timeout less than request timeout and verify
             * we retry during the entire request timeout period.
             */
            kvs.get(Key.createKey("one"),
                    new Consistency.Time(5, TimeUnit.SECONDS,
                                         2, TimeUnit.SECONDS),
                    3, TimeUnit.SECONDS);
            fail("expected consistency exception");
        } catch (ConsistencyException ce) {
            final long elapsedMs = System.currentTimeMillis() - startMs;
            /*
             * Check that the request timeout period was exceeded due to
             * retries, but also that it was timely.
             */
            assertTrue("Elapsed time:" + elapsedMs,
                       (elapsedMs >= 3000) && (elapsedMs < 4000));
        }

        startMs = System.currentTimeMillis();
        try {
            kvs.get(Key.createKey("one"),
                    new Consistency.Time(5, TimeUnit.SECONDS,
                                         2, TimeUnit.SECONDS),
                    2, TimeUnit.SECONDS);
            fail("expected consistency exception");
        } catch (ConsistencyException ce) {
            final long elapsedMs = System.currentTimeMillis() - startMs;
            assertTrue("Elapsed time:" + elapsedMs,
                       (elapsedMs >= 2000) && (elapsedMs < 3000));
        }

        /* Writes must fail. */
        startMs = System.currentTimeMillis();
        try {
            kvs.put(Key.createKey("one"),
                    Value.createValue(new byte[0]));
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException ce) {
            final long elapsedTime =
                System.currentTimeMillis() - startMs;
            final long requestTimeout =
                config.getKVSConfig().getRequestTimeout(
                    TimeUnit.MILLISECONDS);
            assertTrue("Expected elapsed time " + elapsedTime +
                       " to be somewhat greater than request timeout " +
                       requestTimeout,
                       (elapsedTime >= requestTimeout) &&
                       (elapsedTime < (requestTimeout + 1000)));
        }

        /*
         * Make sure the request dispatcher also eventually sees the UNKNOWN
         * state
         */
        final RequestDispatcher dispatcher =
            ((KVStoreImpl) kvs).getDispatcher();
        final RepGroupStateTable rgst = dispatcher.getRepGroupStateTable();

        final State state = rgst.getRepState(rg1n1Id);
        assertEquals(State.UNKNOWN, state);

        kvs.close();

        config.stopRepNodeServices();
    }

    /**
     * Verifies that when <code>Consistency.NONE_REQUIRED_NO_MASTER</code> is
     * specified, a read operation will be successful as long as there is at
     * least one node available that is not the Master.
     *
     * <p>To verify the expected behavior, this test case creates a store with
     * 3 nodes and a replication factor of 3. After populating the store with a
     * pre-defined key/value pair, the master and all replicas but 1 are
     * disabled (stopped); leaving only 1 replica node to service any read
     * requests. A read operation is then initiated and this method verifies
     * that the operation successfully returns the expected results.</p>
     */
    @Test
    public void testNoneRequiredNoMasterOnlyReplicas() throws IOException {

        /* Set up the store */
        final int nZones = 1;
        final int nStorageNodes = 1;
        final int repFactor = 3;
        final int nPartitions = 10;
        config = new KVRepTestConfig(
            this, nZones, nStorageNodes, repFactor, nPartitions);
        config.startRepNodeServices();
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final ArrayList<RepNode> rnList = config.getRNs();

        /* Populate the store */
        final Key key = Key.createKey("one");
        final Value value = Value.createValue(new byte[] {'a', 'b', 'c', 'd'});
        final Version putVersion = kvs.put(key, value);
        final ValueVersion expectedVal = new ValueVersion(value, putVersion);

        /*
         * Stop all REPLICAs except 1 and then stop the MASTER. The REPLICAs
         * and MASTER are stopped separately and in that order for the
         * following reason. When looping through the RepNodes, the MASTER may
         * be the first RN that is stopped. If that MASTER is stopped first,
         * and the timing is right, a new MASTER might be elected from the
         * remaining REPLICAs before the loop is exited. In this case, it is
         * possible then that the newly elected MASTER, along with the
         * remaining REPLICA, could both be stopped after the first MASTER was
         * stopped; in which case, all nodes would be stopped and a
         * RequestTimeoutException would occur when the call to get is
         * attempted.
         */
        final int nReplicasToStop = rnList.size() - 2;
        int nReplicasStopped = 0;

        /* Stop all but 1 REPLICA, but do not stop the MASTER (yet). */
        for (RepNode rn : rnList) {
            State repState = rn.getEnv(1).getState();
            if (State.MASTER.equals(repState)) {
                continue;
            }
            if (nReplicasStopped < nReplicasToStop) {
                rn.stop(false);
                try {
                    final ReplicatedEnvironment env = rn.getEnv(1);
                    if (env != null) {
                        repState = env.getState();
                        fail("failed to stop RepNod [" + rn + "]");
                    }
                } catch (IllegalStateException e) {
                }
                nReplicasStopped = nReplicasStopped + 1;
            }
        }

        /* Stop all REPLICAs, but not the MASTER. */
        for (RepNode rn : rnList) {
            ReplicatedEnvironment env = rn.getEnv(1);
            if (env == null) {
                continue;
            }
            State repState = env.getState();
            if (State.MASTER.equals(repState)) {
                rn.stop(false);
                try {
                    env = rn.getEnv(1);
                    if (env != null) {
                        repState = env.getState();
                        fail("failed to stop MASTER [" + rn + "]");
                    }
                } catch (IllegalStateException e) {
                    break;
                }
            }
        }

        /* Verify results */
        try {
            @SuppressWarnings("deprecation")
            final ValueVersion getVal =
                kvs.get(key, Consistency.NONE_REQUIRED_NO_MASTER,
                        1, TimeUnit.SECONDS);
            assertTrue((expectedVal.getVersion()).equals(getVal.getVersion()));
            assertTrue((expectedVal.getValue()).equals(getVal.getValue()));
        } catch (Exception e) {
            fail("unexpected Exception: " + e);
        }

        /* Cleanup */
        kvs.close();
        config.stopRepNodeServices();
    }

    /**
     * Verifies that when <code>Consistency.NONE_REQUIRED_NO_MASTER</code> is
     * specified, the Master is never selected when performing a read
     * operation.
     *
     * <p>To verify the expected behavior, this test case creates a store with
     * 3 nodes and a replication factor of 3. After populating the store with a
     * pre-defined key/value pair, all replicas are disabled (stopped); leaving
     * only the master node to service any read requests. A read operation is
     * then initiated and this method verifies that a
     * <code>RequestTimeoutException</code> occurs, as expected.</p>
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testNoneRequiredNoMasterRequestTimeoutException()
        throws IOException {

        /* Set up the store */
        final int nZones = 1;
        final int nStorageNodes = 1;
        final int repFactor = 3;
        final int nPartitions = 10;
        config = new KVRepTestConfig(
            this, nZones, nStorageNodes, repFactor, nPartitions);
        config.startRepNodeServices();
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final ArrayList<RepNode> rnList = config.getRNs();

        /* Populate the store */
        final Key key = Key.createKey("one");
        final Value value = Value.createValue(new byte[] {'a', 'b', 'c', 'd'});
        kvs.put(key, value);

        /* Stop all REPLICAs, but not the MASTER */
        for (RepNode rn : rnList) {
            State repState = rn.getEnv(1).getState();
            if (State.MASTER.equals(repState)) {
                continue;
            }
            rn.stop(false);
            try {
                final ReplicatedEnvironment env = rn.getEnv(1);
                if (env != null) {
                    repState = env.getState();
                    fail("failed to stop RepNod [" + rn + "]");
                }
            } catch (IllegalStateException e) /* CHECKSTYLE:OFF */ {
            }/* CHECKSTYLE:ON */
        }

        /*
         * We currently fail here, so ensure that we clean up.  Otherwise,
         * the following test times out. When SR 23197 is fixed and we no
         * longer fail here, the try/finally construct can be removed, if
         * desired.
         */
        try {
            /* Verify results */
            long startMs = System.currentTimeMillis();
            try {
                kvs.get(key, Consistency.NONE_REQUIRED_NO_MASTER,
                        1, TimeUnit.SECONDS);
                fail("expected RequestTimeoutException");
            } catch (RequestTimeoutException e) /* CHECKSTYLE:OFF */ {
                final long elapsedTime = System.currentTimeMillis() - startMs;
                assertTrue("Elapsed time: " + elapsedTime,
                           (elapsedTime >= 1000) && (elapsedTime < 2000));
            }/* CHECKSTYLE:ON */
        } finally {
            /* Cleanup */
            kvs.close();
            config.stopRepNodeServices();
        }
    }

    /**
     * Test to verify that requests resulting in EnvironmentFailureExceptions
     * at an RN, are retried.
     */
    @Test
    public void testEnvFailureRetry()
        throws IOException {

        /* Single RG to simplify the test. */
        config = new KVRepTestConfig(this, 1, 1, 3, 10);
        config.startRepNodeServices();
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());

        kvs.put(Key.createKey("one"), Value.createValue(new byte[0]));

        /* Set up the hook to cause a env failure on the next request. */
        final RequestHandlerImpl rh1 = config.getRH(rg1n1Id);
        final EnvFailureExceptionHook hook = new EnvFailureExceptionHook();
        rh1.setPreCommitTestHook(hook);
        /*
         * Direct the request to the current master (rg1rn1), to provoke an
         * env failure and a retry at a different newly elected master.
         */
        kvs.get(Key.createKey("one"), Consistency.ABSOLUTE,
                20, TimeUnit.SECONDS);

        /* Verify that the hook was invoked at least once. */
        assertTrue(hook.count > 0);

        kvs.close();
        config.stopRepNodeServices();
    }

    /**
     * Test to verify KVStoreCreator to create and get KVStore handle.
     */
    @Test
    public void testKVStoreCreator() throws IOException {
        config = new KVRepTestConfig(this, 1, 1, 3, 10);
        config.startRepNodeServices();

        final RepNodeService rnService =
                config.getRepNodeServices().iterator().next();

        final KVStoreCreator creator1 =
                rnService.new KVStoreCreator(rnService, rnService.logger);

        final boolean hasKVStore = new PollCondition(10, 60000) {

            @Override
            protected boolean condition() {
                return (creator1.getKVStore() != null);
            }

        }.await();

        assertTrue(hasKVStore);

        config.stopRepNodeServices();
    }

    /**
     * Test session access after initialization, before session manager
     * started up. Should avoid the NPE for the KVStoreCreator.
     * [#25092]
     */
    @Test
    public void testSessionAccessDuringRestart()
        throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 1, 10);

        /* Setting the security parameters required for RN start up */
        SecurityParams sp = config.getSecurityParams(rg1n1Id);
        final File sslDir = SSLTestUtils.getTestSSLDir();
        sp.setSecurityEnabled(true);
        sp.setKeystoreFile(new File(SSLTestUtils.SSL_KS_NAME).getPath());
        sp.setTruststoreFile(new File(SSLTestUtils.SSL_TS_NAME).getPath());
        sp.setPasswordFile(new File(SSLTestUtils.SSL_PW_NAME).getPath());
        sp.addTransportMap("trans");
        sp.setTransServerKeyAlias("trans", SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransFactory("trans", SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setConfigDir(new File(sslDir.getPath()));

        RepNodeService rg1n1Service = config.createRNService(rg1n1Id);

        /*
         * After initialization, before start up, session manager is not ready.
         * Avoid NullPointerException at this point.
         */
        boolean result =
            rg1n1Service.getRepNodeSecurity().getKVSessionManager().isReady();

        assertFalse(result);

        config.startRepNodeServices();

        rg1n1Service = config.getRepNodeServices().iterator().next();

        /*
         * After started up, the session manager should be ready.
         */
        result =
            rg1n1Service.getRepNodeSecurity().getKVSessionManager().isReady();

        assertTrue(result);

        rg1n1Service.stop(true, "test");

        config.stopRepNodeServices();
    }

    /**
     * Test that an RN aborts its startup if a stop request comes in while
     * start is underway.
     */
    @Test
    public void testStopDuringStart()
        throws Exception {

        config = new KVRepTestConfig(this, 1, 1, 1, 10);

        final Semaphore start = new Semaphore(0);
        final Semaphore cont = new Semaphore(0);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        /* Start the RepNodeService, and wait for start to be called */
        RepNodeService.startTestHook = (a) -> {
            start.release();
            try {
                assertTrue(cont.tryAcquire(10, SECONDS));
            } catch (Throwable e) {
                failure.set(e);
            }
        };
        final Thread startThread = new Thread(
            () -> {
                try {
                    config.startRepNodeServices();
                    fail("Expected IllegalStateException");
                } catch (IllegalStateException e) {
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
        startThread.start();
        assertTrue(start.tryAcquire(10, SECONDS));

        /* Stop the service and wait for the stop request to be noticed */
        final Semaphore stopRequested = new Semaphore(0);
        RepNodeService.stopRequestedTestHook =
            (a) -> { stopRequested.release(); };
        final Thread stopThread = new Thread(
            () -> {
                try {
                    config.getRepNodeService(rg1n1Id).stop(false, "test");
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
        stopThread.start();
        assertTrue(stopRequested.tryAcquire(10, SECONDS));

        /* Let the start continue and check that everything completes */
        cont.release();
        startThread.join(10 * 1000);
        assertFalse(startThread.isAlive());
        stopThread.join(10 * 1000);
        assertFalse(stopThread.isAlive());
        assertNull(failure.get());
    }

    /**
     * Test that a write attempt on the server times out quickly enough in
     * cases where the dispatcher doesn't know that master quorum has been
     * lost.
     */
    @Test
    public void testMasterQuorumLost() throws IOException {

        config = new KVRepTestConfig(this, 1, 1, 3, 10);
        config.startRepNodeServices();

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());

        /* Make sure writes work and that the dispatcher knows the state */
        kvs.put(Key.createKey("one"), Value.createValue(new byte[0]));

        /* Stop other two nodes */
        config.stopRepNodeServicesSubset(false, /* force */
            new RepNodeId(1, 2), new RepNodeId(1, 3));

        /* Make sure that the write fails within the request timeout */
        final long start = System.currentTimeMillis();
        try {
            kvs.put(Key.createKey("one"), Value.createValue(new byte[0]),
                    null, Durability.COMMIT_NO_SYNC,
                    500, TimeUnit.MILLISECONDS);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rte) {
            final long time = System.currentTimeMillis() - start;
            assertTrue("Time is too short: " + time, time >= 500);
            assertTrue("Time is too long: " + time, time < 1500);
        }
    }

    /**
     * Simulates a client that uses the RepNodeService.
     */
    private void simulateClient()
        throws KVStoreException {

        final RequestDispatcherImpl dispatcher = createDispatcher();
        final Key key = Key.createKey("k1");
        final byte[] keyBytes = key.toByteArray();
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);

        final Durability durability = new Durability(SyncPolicy.NO_SYNC,
                        SyncPolicy.NO_SYNC,
                        Durability.ReplicaAckPolicy.ALL);
        final Put op = new Put(keyBytes,
                               Value.createValue(new byte[0]),
                               ReturnValueVersion.Choice.NONE);
        final int seqNumber = config.getTopology().getSequenceNumber();
        Request request = new Request(op, partitionId, true, durability, null,
                                      3, seqNumber,
                                      dispatcher.getDispatcherId(), 5000,
                                      null);
        dispatcher.execute(request, LOGIN_MGR);

        /* Verify the put. Retrieve the key from a master or replica */
        final Get gop = new Get(keyBytes);
        request = new Request(gop, partitionId, false, null,
                              Consistency.NONE_REQUIRED, 3, seqNumber,
                              dispatcher.getDispatcherId(), 5000, null);

        final Value v2 =
            dispatcher.execute(request, LOGIN_MGR).getResult().
            getPreviousValue();
        assertTrue(Arrays.equals(new byte[0], v2.getValue()));
        dispatcher.shutdown(null);
    }

    private RequestDispatcherImpl createDispatcher()
        throws KVStoreException {
        final ClientId clientId = new ClientId(1000);
        final StorageNode sn1 = config.getTopology().get(new StorageNodeId(1));
        final String sn1RegistryHostPort = sn1.getHostname() +
                    TopologyLocator.HOST_PORT_SEPARATOR +
                    sn1.getRegistryPort();

        /* Simulate a client remote dispatcher. */
        final KVStoreConfig kvsConfig =
            new KVStoreConfig(kvstoreName, sn1RegistryHostPort);
        final RequestDispatcherImpl dispatcher =
            RequestDispatcherImpl.createForClient(
                kvsConfig, clientId, null, this,
                ClientLoggerUtils.getLogger(KVStore.class,
                                            clientId.toString()));
        return dispatcher;
    }

    private MonitorAgentAPI getMonitorHandle(Topology topology, RepNodeId rnId)
        throws RemoteException, NotBoundException {

        /* Get a handle to the SNA */
        final StorageNode sn =
            topology.get(rnId.getComponent(topology).getStorageNodeId());
        return RegistryUtils.getMonitor(topology.getKVStoreName(),
                                        sn.getHostname(),
                                        sn.getRegistryPort(),
                                        rnId,
                                        LOGIN_MGR,
                                        logger);
    }

    /**
     * Simulates the SNA, by invoking the configure operation remotely through
     * the admin interface.
     */
    private class SNASimulatorThread extends Thread {
        private final Topology topology;
        private final RepNodeId rnId;
        RepNodeAdminAPI adminHandle = null;

        /**
         *  Set to a non null value if the thread exits with an exception.
         */
        Exception failException;

        public SNASimulatorThread(Topology topology,
                                  RepNodeId rnId) {
            super();
            this.topology = topology;
            this.rnId = rnId;
        }

        @Override
        public void run() {
            final RegistryUtils ru =
                new RegistryUtils(topology, LOGIN_MGR, logger);

            while (true) {
                try {
                    adminHandle = ru.getRepNodeAdmin(rnId);
                    final ServiceStatus status =
                        adminHandle.ping().getServiceStatus();
                    if (ServiceStatus.WAITING_FOR_DEPLOY.equals(status) ||
                        ServiceStatus.RUNNING.equals(status)) {
                        break;
                    }
                    /* Retry waiting for a state transition */
                    continue;

                } catch (RemoteException e) {
                    failException = e;
                    throw new RuntimeException(e);
                } catch (NotBoundException e) /* CHECKSTYLE:OFF */ {
                    /* Retry waiting for it to be bound */
                }/* CHECKSTYLE:ON */
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    failException = e1;
                    throw new RuntimeException(e1);
                }
            }

            try {
                adminHandle.updateMetadata(topology);
            } catch (RemoteException e) {
                failException = e;
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Test hook to generate an environment failure exception during a request.
     */
    static class EnvFailureExceptionHook implements
        TestHook<RequestHandlerImpl.ExecuteRequest> {

        int count = 0;

        EnvFailureExceptionHook() {
            super();
        }

        @Override
        public void doHook(RequestHandlerImpl.ExecuteRequest arg) {
            count++;

            throw EnvironmentFailureException.unexpectedException
                    (new ChecksumException("test"));
        }
    }
}
