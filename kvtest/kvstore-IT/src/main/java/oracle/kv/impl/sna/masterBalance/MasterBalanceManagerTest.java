/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna.masterBalance;

import static java.util.concurrent.ForkJoinPool.commonPool;
import static oracle.kv.impl.async.StandardDialogTypeFamily.STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.kv.TestBase;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.rep.admin.RepNodeAdminResponder;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentInterfaceResponder;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MDInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MasterLeaseInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.RepNodeAdminNOP;
import oracle.kv.impl.util.StorageNodeAgentNOP;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import org.junit.Test;

/**
 * Tests the SNA side of the master balancing service using mock RN objects.
 *
 * TODO:
 *  1) Simulate BDA-specific configuration:
 * http://www.oracle.com/ocom/groups/public/@otn/documents/webcontent/1453665.pdf
 *  2) Simulate rebalance retry because SN was down and replicas emerge on it
 *  3) Add tests to include secondaray data centers and ensure that SNs
 *  hosted in them do not participate in master balancing.
 */
public class MasterBalanceManagerTest extends TestBase {

    private final static LoginManager NULL_LOGIN_MGR = null;

    final StorageNodeId sn1Id = new StorageNodeId(1);
    final RepNodeId rn11Id = new RepNodeId(1, 1);
    final RepNodeId rn21Id = new RepNodeId(2, 1);
    final RepNodeId rn31Id = new RepNodeId(3, 1);
    final RepNodeId rn41Id = new RepNodeId(4, 1);

    final StorageNodeId sn2Id = new StorageNodeId(2);
    final RepNodeId rn12Id = new RepNodeId(1, 2);
    final RepNodeId rn22Id = new RepNodeId(2, 2);
    final RepNodeId rn32Id = new RepNodeId(3, 2);
    final RepNodeId rn42Id = new RepNodeId(4, 2);

    final StorageNodeId sn3Id = new StorageNodeId(3);
    final RepNodeId rn13Id = new RepNodeId(1, 3);
    final RepNodeId rn23Id = new RepNodeId(2, 3);
    final RepNodeId rn33Id = new RepNodeId(3, 3);
    final RepNodeId rn43Id = new RepNodeId(4, 3);

    final StorageNodeId sn4Id = new StorageNodeId(4);
    final RepNodeId rn14Id = new RepNodeId(1, 4);
    final RepNodeId rn24Id = new RepNodeId(2, 4);
    final RepNodeId rn34Id = new RepNodeId(3, 4);

    final StorageNodeId sn5Id = new StorageNodeId(5);
    final RepNodeId rn15Id = new RepNodeId(1, 5);
    final RepNodeId rn25Id = new RepNodeId(2, 5);
    final RepNodeId rn35Id = new RepNodeId(3, 5);

    final StorageNodeId sn6Id = new StorageNodeId(6);
    final RepNodeId rn16Id = new RepNodeId(1, 6);
    final RepNodeId rn26Id = new RepNodeId(2, 6);
    final RepNodeId rn36Id = new RepNodeId(3, 6);

    final short serVersion = 1;

    private FreePortLocator portLocator;
    private Topology topo;
    private RegistryUtils regUtils;
    private RepNode rn11;
    private final List<Registry> registries = new LinkedList<>();
    private final List<ListenHandle> registryHandles = new LinkedList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        portLocator = TopoUtils.makeFreePortLocator();
        topo = null;
    }

    @Override
    public void tearDown() throws Exception {
        for (Registry registry : registries) {
            TestUtils.destroyRegistry(registry);
        }
        registryHandles.forEach(lh -> lh.shutdown(true));

        /* Clear the hook */
        ContextProxy.beforeInvokeNoAuthRetry = null;
        super.tearDown();
    }

    private void createTopo(final int nDC,
                            final int nSN,
                            final int repFactor,
                            final int nSecondaryZones,
                            final int nShards) {
        topo = TopoUtils.create("burl", nDC, nSN, repFactor, 100,
                                nSecondaryZones, nShards, portLocator);

        /* Set up registry on sn1, and bind the rn11 RepNodeAdmin interface. */
        regUtils = new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        if (logger.isLoggable(Level.INFO)) {
            logger.info(TopologyPrinter.printTopology(topo));
        }
        rn11 = topo.get(rn11Id);
    }

    private List<MasterBalanceManager> createMBMs(List<StorageNodeId> snIds)
        throws IOException {

        List<MasterBalanceManager> mbms = new LinkedList<>();
        for (StorageNodeId  snId : snIds) {
            MasterBalanceManager mbm = createMBM(snId);

            mbms.add(mbm);
        }

        return mbms;
    }

    private MasterBalanceManager createMBM(StorageNodeId snId)
        throws IOException {

        StorageNode sn = topo.get(snId);

        SNInfo snInfo =
                new MasterBalanceManager.SNInfo(topo.getKVStoreName(),
                                                snId,
                                                sn.getHostname(),
                                                sn.getRegistryPort());
        final MasterBalanceManager mbm =
            (MasterBalanceManager)MasterBalanceManager.create(true, snInfo,
                                                              logger,
                                                              NULL_LOGIN_MGR);
        registries.add(TestUtils.createRegistry(sn.getRegistryPort()));
        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandles.add(
                TestUtils.createServiceRegistry(sn.getRegistryPort()));
        }

        StorageNodeAgentNOP snai = new SNAMasterBalance(mbm);

        /* Create the SNA registry entry. */
        tearDowns.add(() -> safeUnexport(snai));
        tearDownListenHandle(
            RegistryUtils.rebind(
                sn.getHostname(),
                sn.getRegistryPort(),
                topo.getKVStoreName(),
                snId,
                RegistryUtils.InterfaceType.MAIN,
                snai,
                DEFAULT_CSF,
                DEFAULT_SSF,
                STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY,
                () -> new StorageNodeAgentInterfaceResponder(
                    snai, commonPool(), logger),
                logger));

        /* Register the RepNodeAdmin interfaces. */
        for (final RepNodeId rnId : topo.getRepNodeIds()) {
            if (!topo.get(rnId).getStorageNodeId().equals(snId)) {
                continue;
            }

            /* Bind mock object to return topology. */
            final MasterTransferRNA masterTransferRNA =
                new MasterTransferRNA(rnId);
            tearDowns.add(() -> safeUnexport(masterTransferRNA));
            tearDownListenHandle(
                regUtils.rebind(
                    rnId, masterTransferRNA, DEFAULT_CSF, DEFAULT_SSF,
                    () -> new RepNodeAdminResponder(masterTransferRNA,
                                                    commonPool(), logger)));
        }
        return mbm;
    }

    /**
     * Test for master balance stasis on 3 SNs, capacity = 4, with 4 shards, so
     * that stasis can be reached with master distributions:
     * (2,1,1), (1,2,1) or (1,1,2)
     */
    @Test
    public void testUnevenMasters()
        throws Exception {

        final int pollPeriodMs = 1000;

        RebalanceThread.setPollPeriodMs(pollPeriodMs);
        createTopo(1 /* nDC */, 3 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 4 /* nShards */);
        createMBMs(Arrays.asList(sn1Id, sn2Id, sn3Id));

        noteState(rn11Id, State.MASTER);
        noteState(rn12Id, State.REPLICA);
        noteState(rn13Id, State.REPLICA);

        noteState(rn21Id, State.REPLICA);
        noteState(rn22Id, State.MASTER);
        noteState(rn23Id, State.REPLICA);

        noteState(rn31Id, State.REPLICA);
        noteState(rn32Id, State.REPLICA);
        noteState(rn33Id, State.MASTER);

        noteState(rn41Id, State.REPLICA);
        noteState(rn42Id, State.REPLICA);
        noteState(rn43Id, State.MASTER); /* Two masters on sn3 */

        awaitBalancedKVS(4, 50 /* Allow up to two RNs/SN to become masters */);
    }

    /**
     * Start 3 SNs each with capacity 3. Each SN has 1 Master.
     * 1) Shut down SN1 first. This causes RN Master transfer from SN1 to one of
     * SN2 or SN3.
     * 2) Shut down the SN with 2 Masters in it. This cause 2 Master transfer
     * from this SN to the only SN that is up.
     * 3) From SN2 or SN3 restart that SN that is down. Master Balancing takes
     * place. SN2 and SN3 have master distribution of 66% and 33% now.
     * 4) Restart SN1. Equal master distribution takes place. Each of the SNs
     * have 1 Master.
     */
    @Test
    public void testMasterTransfersOnSNShutdown()
        throws Exception {
        final int pollPeriodMs = 1000;

        RebalanceThread.setPollPeriodMs(pollPeriodMs);
        createTopo(1 /* nDC */, 3 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms =
                createMBMs(Arrays.asList(sn1Id, sn2Id, sn3Id));
        Set<Integer> targetMDs = new HashSet<>();

        /*
         * SCENARIO 1: SN with 1 master will be shutting down.
         *             Transfer the lone master.
         */
        noteState(rn11Id, State.MASTER);
        noteState(rn21Id, State.REPLICA);
        noteState(rn31Id, State.REPLICA);

        noteState(rn12Id, State.REPLICA);
        noteState(rn22Id, State.MASTER);
        noteState(rn32Id, State.REPLICA);

        noteState(rn13Id, State.REPLICA);
        noteState(rn23Id, State.REPLICA);
        noteState(rn33Id, State.MASTER);

        awaitBalancedKVS(3, 33);

        /*
         * SN with one master shuts down. Before shutdown, the lone Master gets
         * transferred.
         */
        mbms.get(0).transferMastersForShutdown();

        /*
         * SN1 has 0% Masters, SN2 has 66% Masters, SN3 has 33% Masters
         */
        targetMDs.add(0); targetMDs.add(66); targetMDs.add(33);
        awaitDefinedKVSState(targetMDs);

        /*
         * SCENARIO 2: SN with 2 masters will be shutting down.
         *             Transfer both the masters.
         */
        MasterBalanceManager mbm = null;

        /*
         * From SN2 and SN3 get the handle on the Master Balance Manager
         * for that SN which has 2 Masters in it.
         */
        if (mbms.get(1).getRebalanceThread().getMasterCount() == 2) {
            mbm = mbms.get(1);
        } else {
            mbm = mbms.get(2);
        }

        /*
         * SN with two masters shuts down. Before shutdown, both Masters gets
         * transferred.
         */
        mbm.transferMastersForShutdown();

        targetMDs.clear();
        /*
         * SN1 has 0% Masters, SN2 has 0% Masters, SN3 has 100% Masters
         */
        targetMDs.add(0); targetMDs.add(0); targetMDs.add(100);
        awaitDefinedKVSState(targetMDs);

        /*
         * From SN2 and SN3 get the handle on the Master Balance Manager
         * for that SN which is down (has all its masters transferred)
         */
        if (mbms.get(1).getRebalanceThread().getMasterCount() == 0) {
            mbm = mbms.get(1);
        } else {
            mbm = mbms.get(2);
        }

        /*
         * In actual deployment scenarios, setWillRNsShutDown(false) called
         * after the SN has transfered its masters and shut down all RNs in
         * this SN. If RNs are not shut down and setWillRNsShutDown(false)
         * is called, other SNs can now transfer masters to this SN in case they
         * are overloaded (which is the case in this test) and cause overall
         * balancing done by the Master Balancing Service.
         */
        mbm.setWillRNsShutDown(false);

        /*
         * 1 SN down 2 SN up. 3 Masters shared between 2 SNs.
         * 1 SN has 66% share and another SN has 33% share.
         */
        targetMDs.clear();
        targetMDs.add(0); targetMDs.add(33); targetMDs.add(66);
        awaitDefinedKVSState(targetMDs);

        mbms.get(0).setWillRNsShutDown(false);
        /*
         * All 3 SNs are up now. 3 Masters will be shared across all 3
         * Masters.
         */
        awaitBalancedKVS(3, 33);
    }

    /*
     * test rebalancing resulting from an SN being down and coming back up.
     *
     * 1) Start just sn1 and sn2
     * 2) Create 2 masters on sn1 and 1 on sn2, with MD of 66 and 33 resp.
     * 3) Verify that unbalanced state on sn1 persists
     * 4) Bring up sn3 and transition RNs on it to replicas
     * 5) Verify that SNs are balanced again at MD=33
     */
    @Test
    public void testSNDown()
        throws Exception {

        final int pollPeriodMs = 1000;
        RebalanceThread.setPollPeriodMs(pollPeriodMs);
        createTopo(1 /* nDC */, 3 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms =
                createMBMs(Arrays.asList(sn1Id, sn2Id));
        /* sn3 is down. */

        noteState(rn11Id, State.MASTER);
        noteState(rn21Id, State.REPLICA);
        noteState(rn31Id, State.MASTER);

        noteState(rn12Id, State.REPLICA);
        noteState(rn22Id, State.MASTER);
        noteState(rn32Id, State.REPLICA);

        /* Check for absence of any master transfer operations.*/

        Thread.sleep(5 * pollPeriodMs);

        /* master densities unchanged. */
        assertEquals(66, regUtils.getStorageNodeAgent(sn1Id).getMDInfo().getMD());
        assertEquals(33, regUtils.getStorageNodeAgent(sn2Id).getMDInfo().getMD());

        /* Simulate the startup of sn3. */
        createMBMs(Arrays.asList(sn3Id));
        Thread.sleep(5 * pollPeriodMs);

        /* master densities unchanged -- no RNs as yet in replica state */
        assertEquals(66, regUtils.getStorageNodeAgent(sn1Id).getMDInfo().getMD());
        assertEquals(33, regUtils.getStorageNodeAgent(sn2Id).getMDInfo().getMD());

        noteState(rn13Id, State.REPLICA);
        noteState(rn23Id, State.REPLICA);
        noteState(rn33Id, State.REPLICA);

        /* Wait for the SNs to become balanced. */
        awaitBalancedKVS(3, 33);

        shutdownMBMs(mbms);
    }

    /* An Sn with one surplus master, that is, MD = 66 */
    @Test
    public void testMD66()
        throws Exception {
        createTopo(1 /* nDC */, 3 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms = createMBMs(topo.getStorageNodeIds());

        noteState(rn11Id, State.MASTER);
        noteState(rn22Id, State.MASTER);
        noteState(rn33Id, State.MASTER);

        /* Simulate a master transition with a master moving from sn1 to sn2 */
        noteState(rn11Id, State.REPLICA);
        noteState(rn12Id, State.MASTER);

        awaitBalancedKVS(3, 33);

        shutdownMBMs(mbms);
    }

    private void shutdownMBMs(List<MasterBalanceManager> mbms) {
        for ( MasterBalanceManager  mbm : mbms) {
            mbm.shutdown();
        }
    }

    /* Rebalance an sn with two surplus masters, that is MD = 100 */
    @Test
    public void testMD100() throws Exception {
        createTopo(1 /* nDC */, 3 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms = createMBMs(topo.getStorageNodeIds());

        /* All masters on sn1. */
        noteState(rn12Id, State.REPLICA);
        noteState(rn22Id, State.REPLICA);
        noteState(rn13Id, State.REPLICA);
        noteState(rn23Id, State.REPLICA);

        noteState(rn11Id, State.MASTER);
        noteState(rn21Id, State.MASTER);
        noteState(rn31Id, State.MASTER);

        awaitBalancedKVS(3, 33);

        shutdownMBMs(mbms);
    }

    @Test
    public void testMDThreeZones() throws Exception {
        testMDThreeZonesInternal();
    }

    @Test
    public void testMDThreeZonesWithSAE()
        throws Exception {

        ContextProxy.beforeInvokeNoAuthRetry = new TestUtils
            .CountDownFaultHook(10 /* fault count */,
            RebalanceThread.THREAD_NAME,
            new SessionAccessException("sae"));
        testMDThreeZonesInternal();
    }

    @Test
    public void testMDThreeZonesFaultException()
        throws Exception {

        ContextProxy.beforeInvokeNoAuthRetry = new TestUtils
            .CountDownFaultHook(10 /* fault count */,
            RebalanceThread.THREAD_NAME,
            new SNAFaultException(new SessionAccessException("sae")));
        testMDThreeZonesInternal();
    }

    private void testMDThreeZonesInternal()
        throws Exception {

        createTopo(3 /* nDC */, 3 /* nSN */, 1 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms = createMBMs(topo.getStorageNodeIds());

        /* All masters on sn1. */
        noteState(rn11Id, State.MASTER);
        noteState(rn21Id, State.MASTER);
        noteState(rn31Id, State.MASTER);

        noteState(rn12Id, State.REPLICA);
        noteState(rn22Id, State.REPLICA);
        noteState(rn32Id, State.REPLICA);
        noteState(rn13Id, State.REPLICA);
        noteState(rn23Id, State.REPLICA);
        noteState(rn33Id, State.REPLICA);

        awaitBalancedKVS(3, 33);

        shutdownMBMs(mbms);
    }

    /* Rebalance with some nodes in a secondary zone */
    @Test
    public void testMDTwoZones()
        throws Exception {

        createTopo(2 /* nDC */, 6 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms = createMBMs(topo.getStorageNodeIds());

        /* All masters on sn1. */
        noteState(rn11Id, State.MASTER);
        noteState(rn21Id, State.MASTER);
        noteState(rn31Id, State.MASTER);

        noteState(rn12Id, State.REPLICA);
        noteState(rn22Id, State.REPLICA);
        noteState(rn32Id, State.REPLICA);
        noteState(rn13Id, State.REPLICA);
        noteState(rn23Id, State.REPLICA);
        noteState(rn33Id, State.REPLICA);
        noteState(rn14Id, State.REPLICA);
        noteState(rn24Id, State.REPLICA);
        noteState(rn34Id, State.REPLICA);
        noteState(rn15Id, State.REPLICA);
        noteState(rn25Id, State.REPLICA);
        noteState(rn35Id, State.REPLICA);
        noteState(rn16Id, State.REPLICA);
        noteState(rn26Id, State.REPLICA);
        noteState(rn36Id, State.REPLICA);

        awaitBalancedKVS(3, 33);

        shutdownMBMs(mbms);
    }

    /* Rebalance with some nodes in a secondary zone */
    @Test
    public void testMDTwoZonesOneSecondary()
        throws Exception {

        createTopo(2 /* nDC */, 6 /* nSN */, 3 /* RF */,
                   1 /* nSecondaryZones */, 3 /* nShards */);
        List<MasterBalanceManager> mbms = createMBMs(topo.getStorageNodeIds());

        /* All masters on sn1. */
        noteState(rn11Id, State.MASTER);
        noteState(rn21Id, State.MASTER);
        noteState(rn31Id, State.MASTER);

        noteState(rn12Id, State.REPLICA);
        noteState(rn22Id, State.REPLICA);
        noteState(rn32Id, State.REPLICA);
        noteState(rn13Id, State.REPLICA);
        noteState(rn23Id, State.REPLICA);
        noteState(rn33Id, State.REPLICA);
        noteState(rn14Id, State.REPLICA);
        noteState(rn24Id, State.REPLICA);
        noteState(rn34Id, State.REPLICA);
        noteState(rn15Id, State.REPLICA);
        noteState(rn25Id, State.REPLICA);
        noteState(rn35Id, State.REPLICA);
        noteState(rn16Id, State.REPLICA);
        noteState(rn26Id, State.REPLICA);
        noteState(rn36Id, State.REPLICA);

        awaitBalancedKVS(3, 33);

        shutdownMBMs(mbms);
    }

    private void awaitBalancedKVS(final int expectedMasters, int maxBMD) {

        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            StorageNodeAgentAPI sna;
            try {
                sna = regUtils.getStorageNodeAgent(snId);
                final MDInfo mdInfo = sna.getMDInfo();
                logger.info("SN:" + snId +
                            " " + mdInfo);
            } catch (RemoteException | NotBoundException e) {
                throw new RuntimeException(e);
            }
        }

        assertTrue("Awaiting balanced", new PollCondition(1000, 300000) {

            @Override
            protected boolean condition() {
                boolean balanced = true;
                int actualMasters = 0;
                try {
                    for (StorageNodeId snId : topo.getStorageNodeIds()) {
                        final StorageNodeAgentAPI sna =
                            regUtils.getStorageNodeAgent(snId);
                        final MDInfo mdInfo = sna.getMDInfo();
                        if (mdInfo == null) {
                            logger.info("Master density for " + snId +
                                        ": unavailable");
                            balanced = false;
                            continue;
                        }
                        assertTrue(mdInfo.getBMD() <= maxBMD);
                        actualMasters += mdInfo.getMasterCount();
                        logger.info("Master density for " + snId + ": " +
                                     mdInfo.getMD() +
                                    " balanced md:" + mdInfo.getBMD());
                        if (!mdInfo.isMasterPhobic() &&
                            mdInfo.hasExcessMasters()) {
                            balanced = false;
                        }
                    }
                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return balanced && (actualMasters == expectedMasters);
            }

        }.await());
    }

    private void awaitDefinedKVSState(final Set<Integer> targetMDs) {
        assertTrue("Awaiting DefinedKVSState", new PollCondition(1000, 300000) {
            Set<Integer> checkTargetMDs = new HashSet<>();

            @Override
            protected boolean condition() {
                checkTargetMDs.addAll(targetMDs);
                try {
                    for (StorageNodeId snId : topo.getStorageNodeIds()) {
                        final StorageNodeAgentAPI sna =
                            regUtils.getStorageNodeAgent(snId);
                        final MDInfo mdInfo = sna.getMDInfo();
                        logger.info("Master density for " + snId + ": " +
                                    ((mdInfo == null) ?
                                     null :
                                     mdInfo.getMD()));
                        if (mdInfo == null) {
                            return false;
                        }
                        int masterDensity = mdInfo.getMD();
                        if (checkTargetMDs.contains(masterDensity)) {
                            checkTargetMDs.remove(masterDensity);
                        }
                    }
                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return checkTargetMDs.isEmpty();
            }

        }.await());
    }

    private void noteState(RepNodeId rnId, State state)
      throws RemoteException, NotBoundException {
       StorageNode sn = topo.get(topo.get(rnId).getStorageNodeId());

       final StateInfo stateInfo =
               new StateInfo(rnId, state, topo.getSequenceNumber());

       regUtils.getStorageNodeAgent(sn.getResourceId()).noteState(stateInfo);
    }

    private void awaitPTMD(final MasterBalanceManager mbm, final int ptmd) {
        assertTrue(new PollCondition(1000, 10000) {

            @Override
            protected boolean condition() {
                return ptmd == mbm.getMDInfo(null, serVersion).getPTMD();
            }
        }.await());
    }

    /**
     * Verify calculation of PMD
     */
    @Test
    public void testPMD() throws Exception {

        createTopo(1 /* nDC */, 3 /* nSN */, 3 /* RF */,
                   0 /* nSecondaryZones */, 3 /* nShards */);

        /* Should result in 9 RNs with 3 RNs/SN */
        assertEquals(9, topo.getRepNodeIds().size());

        MasterBalanceManager mbm = createMBM(sn1Id);

        final TopoCache topoCache = mbm.getTopoCache();

        /* PTMD before cache initialization. */
        assertEquals(mbm.getMDInfo(null, serVersion), null);

        StateInfo stateInfo =
                new StateInfo(rn11.getResourceId(), State.MASTER,
                              topo.getSequenceNumber());

        /* Trigger topo cache initialization. */
        mbm.noteState(stateInfo, null, serVersion);

        assertTrue(new PollCondition(100, 10000) {

            @Override
            protected boolean condition() {
                return topoCache.isInitialized() &&
                       (topoCache.getTopology().getSequenceNumber() ==
                        topo.getSequenceNumber());
            }
        }.await());

        /* 1 current master + 1 post. */
        awaitPTMD(mbm, 66);

        mbm.noteState(new StateInfo(rn21Id, State.MASTER,
                                    topo.getSequenceNumber()),
                                    null, serVersion);
        awaitPTMD(mbm, 100);

        mbm.noteState(new StateInfo(rn21Id, State.REPLICA,
                                    topo.getSequenceNumber()),
                                    null, serVersion);
        mbm.noteState(new StateInfo(rn31Id, State.REPLICA,
                                    topo.getSequenceNumber()),
                                    null, serVersion);
        awaitPTMD(mbm, 66);

        final StorageNode sn2 = topo.get(sn2Id);
        final StorageNode sn3 = topo.get(sn3Id);

        final int leaseDurationMs = 2000;
        final RepNode rn31 = topo.get(rn31Id);


        long endTime = System.currentTimeMillis() + leaseDurationMs;

        /* Wait for lease to become available after the queued transition to
         * a replica is processed.
         */
        assertTrue(new PollCondition(100, 10000) {
            MasterLeaseInfo ml =
                new MasterLeaseInfo(sn2, rn31,
                                    mbm.getMDInfo(null, serVersion).getPTMD(),
                                    leaseDurationMs);
            @Override
            protected boolean condition() {
                return mbm.getMasterLease(ml, null, serVersion);
            }
        }.await());

        /* Fail a second lease request from sn3. */
        MasterLeaseInfo masterLease =
            new MasterLeaseInfo(sn3, rn31,
                                mbm.getMDInfo(null, serVersion).getPTMD(),
                                leaseDurationMs);
        final boolean leaseResult = mbm.getMasterLease(masterLease,
                                                       null, serVersion);

        /* Avoid spurious failures on slow systems. */
        assertTrue(System.currentTimeMillis() > endTime || !leaseResult);
        /* The PTMD should have increased as a result for the lease. */
        assertEquals(100, mbm.getMDInfo(null, serVersion).getPTMD());

        /* Wait for the lease to expire. */
        Thread.sleep(2 * leaseDurationMs);
        assertEquals(66, mbm.getMDInfo(null, serVersion).getPTMD());

        /* Can acquire lease after established lease has expired. */
        masterLease =
                new MasterLeaseInfo(sn3, rn31,
                                    mbm.getMDInfo(null, serVersion).getPTMD(),
                                    100000 /* ms */);
        assertTrue(mbm.getMasterLease(masterLease, null, serVersion));
        mbm.cancelMasterLease(sn3, rn31, null, serVersion);

        mbm.shutdown();
    }

    /**
     * A Mock RNA implementation that supplies a mock transferMaster service.
     */
    private final class MasterTransferRNA extends RepNodeAdminNOP {

        private final RepNodeId rnId;

        /**
         * @param rnId
         */
        private MasterTransferRNA(RepNodeId rnId) {
            this.rnId = rnId;
        }

        @Override
        public Topology getTopology(AuthContext ac, short sv) {
            return topo;
        }

        @Override
        public int getTopoSeqNum(AuthContext ac, short sv) {
            return topo.getSequenceNumber();
        }

        @Override
        public boolean initiateMasterTransfer(RepNodeId replicaId,
                                      int timeout,
                                      TimeUnit timeUnit,
                                      AuthContext ac, short sv) {
            logger.info("transfer master request received at master:" +
                        rnId + " target replica:" + replicaId);
            RepNode masterRN = topo.get(rnId);
            RepNode replicaRN = topo.get(replicaId);
            try {
                final StorageNodeAgentAPI sourceSNA = regUtils.
                    getStorageNodeAgent(masterRN.getStorageNodeId());
                StateInfo sinfo = new StateInfo(rnId, State.REPLICA,
                                                topo.getSequenceNumber());
                sourceSNA.noteState(sinfo);

                final StorageNodeAgentAPI targetSNA = regUtils.
                    getStorageNodeAgent(replicaRN.getStorageNodeId());
                sinfo = new StateInfo(replicaId, State.MASTER,
                                      topo.getSequenceNumber());
                targetSNA.noteState(sinfo);

            } catch (Exception e) {
               logger.log(Level.SEVERE, "unexpected exception", e);
               fail(e.getMessage());
            }
           return true;
        }
    }

    private void tearDownListenHandle(ListenHandle listenHandle) {
        if (listenHandle != null) {
            tearDowns.add(() -> listenHandle.shutdown(true));
        }
    }
}
