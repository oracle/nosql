/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.util.TestUtils.set;
import static oracle.kv.util.TestUtils.checkException;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.Value;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminNotReadyException;
import oracle.kv.impl.admin.AdminUtils;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.VerifyConfiguration.ParamMismatch;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.admin.topo.Validations.WrongNodeType;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepEnvHandleManager;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.admin.IllegalRepNodeServiceStateException;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.ZoneInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Miscelleneous failover and switchover tests. */
/* Increase test timeout to 90 minutes -- test can take 45 minutes */
@TestClassTimeoutMillis(90*60*1000)
public class FailoverSwitchoverMiscTest extends TestBase {

    private static final String JE_MISC_PARAMS = "je.cleaner.minUtilization 30";
    protected static final int startPort = 5000;

    protected CreateStore createStore;
    protected int numSNs;
    protected int snCapacity = 1;
    protected List<ZoneInfo> zones;
    protected int[] adminSNs;
    private ByteArrayOutputStream shellOutput;
    protected CommandShell shell;
    protected CommandServiceAPI cs;

    /** Whether to use threads to create services */
    protected boolean useThreads = false;

    @Before
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @After
    @Override
    public void tearDown() {
        if (createStore != null) {
            try {
                createStore.shutdown();
            } finally {
                createStore = null;
            }
        }
        logger.info("Shell output for " + testName.getMethodName() + ": " +
                    shellOutput);
        RepEnvHandleManager.openEnvTestHook = null;
    }

    /** Create the store. */
    protected void createStore()
        throws Exception {

        createStore = new CreateStore(
            kvstoreName,
            startPort,
            numSNs, /* Storage Nodes */
            zones,
            numSNs * 10, /* Partitions */
            snCapacity /* capacity */,
            snCapacity * CreateStore.MB_PER_SN,
            useThreads,
            null /* mgmtImpl */,
            true /* mgmtPortsShared */,
            SECURITY_ENABLE);
    }

    /** Initialize after creating store */
    protected void init()
        throws Exception {

        if (createStore == null) {
            createStore();
        }
        adminSNs = createStore.computeAdminsForZones();
        createStore.setAdminLocations(adminSNs);
        createStore.start();
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        shell.connectAdmin("localhost", createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        cs = shell.getAdmin();
    }

    private void populateStore() {
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        byte[] bytes = new byte[10000];
        try {
            for (int i = 0; i < 10000; i++) {
                Arrays.fill(bytes, (byte) i);
                store.put(Key.createKey("key" + i), Value.createValue(bytes));
            }
        } catch (Exception e) {
        }
    }

    /* Tests */

    @Test
    public void testFailoverSwitchoverTwoPrimaries()
        throws Exception {

        numSNs = 4;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1));
        init();

        /* Take zone 1 offline */
        shutdownSNs(0, 1);

        /*
         * Test that getMasterHttpAddress and getMasterRmiAddress just return
         * null if there is no master, rather than failing
         */
        cs = createStore.getAdmin(2);
        assertTrue("Master RMI address",
                   new PollCondition(1000, 10000) {
                       @Override
                       protected boolean condition() {
                           try {
                               return cs.getMasterRmiAddress() == null;
                           } catch (Exception e) {
                               return false;
                           }
                       }
                   }.await());

        /* Repair admin quorum using zone 2 */
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(2)),
                             Collections.<AdminId>emptySet());

        /* Failover to zone 2 */
        int planId =
            cs.createFailoverPlan("failover-plan",
                                  Collections.<DatacenterId>emptySet(),
                                  Collections.singleton(new DatacenterId(1)));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(5, RMIFailed.class);      /* admin1, 2 RNs, 2 SNs */

        /* Bring zone 1 online */
        createStore.startSNA(0, true /* disableServices */);
        createStore.startSNA(1, true /* disableServices */);

        /* Repair */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyTopology(DatacenterType.SECONDARY, DatacenterType.PRIMARY);

        /* Return zone 1 to primary type */
        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.PRIMARY);
        deployTopo("switchover-plan", topo, false /* cancelRetry */);

        verifyTopology(DatacenterType.PRIMARY, DatacenterType.PRIMARY);
    }

    @Test
    public void testSwitchoverTwoPrimariesReexecute()
        throws Exception {

        testSwitchoverTwoPrimaries(false /* cancelRetry */);
    }

    @Test
    public void testSwitchoverTwoPrimariesCancelRetry()
        throws Exception {

        testSwitchoverTwoPrimaries(true /* cancelRetry */);
    }

    private void testSwitchoverTwoPrimaries(boolean cancelRetry)
        throws Exception {

        numSNs = 5;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        init();

        /* Convert zones 1 and 2 to secondary zones */
        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(2), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(4), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(5), DatacenterType.PRIMARY);

        deployTopo("switchover-plan", topo, cancelRetry);

        verifyTopology(DatacenterType.SECONDARY, DatacenterType.SECONDARY,
                       DatacenterType.PRIMARY, DatacenterType.PRIMARY,
                       DatacenterType.PRIMARY);
    }

    @Test
    public void testSwitchoverTwoPrimariesSecondaryReexecute()
        throws Exception {

        testSwitchoverTwoPrimariesSecondary(false /* cancelRetry */);
    }

    @Test
    public void testSwitchoverTwoPrimariesSecondaryCancelRetry()
        throws Exception {

        testSwitchoverTwoPrimariesSecondary(true /* cancelRetry */);
    }

    private void testSwitchoverTwoPrimariesSecondary(boolean cancelRetry)
        throws Exception {

        numSNs = 5;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        init();

        /*
         * Convert zones 1 and 2 to secondary zones, and zones 4 and 5 to
         * primary zones
         */
        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(2), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(4), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(5), DatacenterType.PRIMARY);

        deployTopo("switchover-plan", topo, cancelRetry);

        verifyTopology(DatacenterType.SECONDARY, DatacenterType.SECONDARY,
                       DatacenterType.PRIMARY, DatacenterType.PRIMARY,
                       DatacenterType.PRIMARY);
    }

    @Test
    public void testSwitchoverSecondaryReexecute()
        throws Exception {

        testSwitchoverSecondary(false /* cancelRetry */);
    }

    @Test
    public void testSwitchoverSecondaryCancelRetry()
        throws Exception {

        testSwitchoverSecondary(true /* cancelRetry */);
    }

    private void testSwitchoverSecondary(boolean cancelRetry)
        throws Exception {

        numSNs = 6;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY),
                       new ZoneInfo(1, DatacenterType.SECONDARY),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        init();

        /*
         * Convert zones 1, 2, and 3 to secondary zones, and zones 4, 5, and 6
         * to primary zones
         */
        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(2), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(3), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(4), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(5), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(6), DatacenterType.PRIMARY);

        deployTopo("switchover-plan", topo, cancelRetry);

        verifyTopology(DatacenterType.SECONDARY, DatacenterType.SECONDARY,
                       DatacenterType.SECONDARY, DatacenterType.PRIMARY,
                       DatacenterType.PRIMARY, DatacenterType.PRIMARY);
    }

    @Test
    public void testSwitchoverSwitchbackPrimarySecondary()
        throws Exception {

        numSNs = 6;

        /*
         * TODO: Fails with capacity 3 due to: [#28133]
         * ReplicationGroupAdmin.deleteMember fails on master transfer. Revert
         * to capacity=3 when that problem is fixed.
         */
        /* snCapacity = 3; */
        snCapacity = 1;

        zones = asList(new ZoneInfo(3),
                       new ZoneInfo(3, DatacenterType.SECONDARY));
        init();

        /*
         * Convert zones 1 to a secondary zone, and zone 2 to a primary zone
         */
        final String switchOver = "switchover-topo";
        cs.copyCurrentTopology(switchOver);
        cs.changeZoneType(switchOver,
                          new DatacenterId(1), DatacenterType.SECONDARY);
        cs.changeZoneType(switchOver,
                          new DatacenterId(2), DatacenterType.PRIMARY);

        deployTopo("switchover-plan", switchOver, false);

        verifyTopology(DatacenterType.SECONDARY, DatacenterType.PRIMARY);

        /*
         * Convert zones 1 back to a primary zone, and zone 2 to a secondary
         */
        final String switchBack = "switchback-topo";
        cs.copyCurrentTopology(switchBack);
        cs.changeZoneType(switchBack,
                          new DatacenterId(1), DatacenterType.PRIMARY);
        cs.changeZoneType(switchBack,
                          new DatacenterId(2), DatacenterType.SECONDARY);

        deployTopo("switchback-plan", switchBack, false);

        verifyTopology(DatacenterType.PRIMARY, DatacenterType.SECONDARY);
    }

    /**
     * Test failover of a primary zone with a topology that includes
     * arbiters. Two primary zones with two SNs in each zone. RF==2.
     * Arbiters allowed in each zone.
     */
    @Test
    public void testFailoverSwitchoverTwoPrimariesArbiters()
        throws Exception {

        numSNs = 4;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1));
        for (ZoneInfo zi : zones) {
            zi.setAllowArbiters(true);
        }
        init();

        /* Take zone 1 offline */
        shutdownSNs(0, 1);

        /* Repair admin quorum using zone 2 */
        cs = createStore.getAdmin(2);
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(2)),
                             Collections.<AdminId>emptySet());

        /* Failover to zone 2 */
        int planId =
            cs.createFailoverPlan("failover-plan",
                                  Collections.<DatacenterId>emptySet(),
                                  Collections.singleton(new DatacenterId(1)));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(5, RMIFailed.class);  /* admin1, 2 RNs, 2 SNs */

        /* Bring zone 1 online */
        createStore.startSNA(0, true /* disableServices */);
        createStore.startSNA(1, true /* disableServices */);

        /* Repair */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyTopology(DatacenterType.SECONDARY, DatacenterType.PRIMARY);

        /* Return zone 1 to primary type */
        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        DatacenterId dc1Id = new DatacenterId(1);
        cs.changeZoneType(topo, dc1Id, DatacenterType.PRIMARY);
        cs.changeZoneArbiters(topo, dc1Id, true);

        /*
         * Rebalance topoology to add arbiters that
         * were removed from topo as part of failover
         * that changed topo from rf2 to rf1.
         */
        cs.rebalanceTopology(topo,
                             CreateStore.STORAGE_NODE_POOL_NAME,
                             null);
        deployTopo("switchover-plan", topo, false /* cancelRetry */);
        verifyTopology(DatacenterType.PRIMARY, DatacenterType.PRIMARY);
    }

    @Test
    public void testSwitchoverPrimaryToSecondary()
        throws Exception {

        numSNs = 3;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1));
        init();

        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.SECONDARY);
        final AdminFaultException afe = checkException(
            () -> deployTopo("switchover-plan", topo, false /* cancelRetry */),
            AdminFaultException.class,
            "Attempt to reduce the overall primary replication factor by 1" +
            " from 3 to 2.");
        assertEquals(IllegalCommandException.class.getName(),
                     afe.getFaultClassName());
    }

    /**
     * Test a previous bug case where attempting to perform a switchover with a
     * down RN that prevents the switchover from completing caused a subsequent
     * repair to go ahead despite a lack of quorum.
     */
    @Test
    public void testSwitchoverRepairWithNodeDown()
        throws Exception {

        /* 5 RF=1 zones, 3 shards */
        numSNs = 15;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        init();

        /*
         * Reduce the timeout when waiting for an admin operation to complete
         * so that the test passes more quickly. But make sure that the timeout
         * is long enough that the admin operations performed by the test can
         * complete in practice on possibly slow test machines.
         */
        AdminUtils.changeAllAdminParams(
                cs, ParameterState.AP_WAIT_TIMEOUT, "20 s");

        /* Find the last primary SN that holds a non-master RN and no admin */
        int snDown = -1;
        for (int i = 0; i < 9; i++) {
            boolean hasAdmin = false;
            for (int adminId : adminSNs) {
                if (i + 1 == adminId) {
                    hasAdmin = true;
                    break;
                }
            }
            if (hasAdmin) {
                continue;
            }
            final RepNodeAdminAPI rnapi = createStore.getRepNodeAdmin(i);
            if (!rnapi.ping().getReplicationState().isReplica()) {
                continue;
            }
            snDown = i;
        }
        assertTrue("Finding SN with non-master RN and no admin",
                   snDown != -1);

        /*
         * Attempt to convert zones 1 and 2 to secondaries while the RN is
         * down, which should fail
         */
        createStore.shutdownSNA(snDown, false);
        String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(2), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(4), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(5), DatacenterType.PRIMARY);
        deployTopo("switchover-plan", topo, false, Plan.State.ERROR);

        /* Perform a repair */
        int planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        verifyViolations(2, RMIFailed.class,      /* offline SN and RN */
                         1, WrongNodeType.class); /* RN type is wrong */

        /* Bring RN back online, repair again, and retry the switchover */
        createStore.startSNA(snDown);
        planId = cs.createRepairPlan("repair-topo-plan-2");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        deployTopo("switchover-plan-2", topo, false);
        verifyViolations();
    }

    @Test
    public void testFailoverRepairPrimary()
        throws Exception {

        /* 2 RF=1 zones, 1 shard */
        numSNs = 2;
        zones = asList(new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        init();

        /* Fail zone 1 */
        shutdownSNs(0);

        /* Repair admin quorum and failover using zone 2 */
        cs = createStore.getAdmin(1);
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(2)),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();
        int planId = cs.createFailoverPlan(
            "failover-plan",
            Collections.singleton(new DatacenterId(2)),  /* newPrimaryZones */
            Collections.singleton(new DatacenterId(1))); /* offlineZones */
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(3, RMIFailed.class);      /* 1 admin, 1 RN, 1 SN */

        /* Start zone 1 with services disabled */
        createStore.startSNA(0, true /* disableServices */);

        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();
    }

    /**
     * Test failover of a zone in a topology that
     * contains arbiters. Swap a rf=1 primary for a
     * rf=1 secondary.
     */
    @Test
    public void testFailoverRepairPrimaryArbiters()
        throws Exception {

        final DatacenterId dcId1 = new DatacenterId(1);
        final DatacenterId dcId2 = new DatacenterId(2);
        final DatacenterId dcId4 = new DatacenterId(4);

        /*
         * 2 RF=1 zones,
         * 1 RF=0 zone arbiters,
         * one RF=1 secondary zone,1 shard
         */
        numSNs = 4;
        zones = asList(new ZoneInfo(1),
                       new ZoneInfo(1),
                       new ZoneInfo(0),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        zones.get(2).setAllowArbiters(true);
        init();

        /* Fail zone 1 */
        shutdownSNs(0);

        /* Repair admin quorum and failover using zone 2 */
        /* get admin from sn1 */
        cs = createStore.getAdmin(1);
        cs.repairAdminQuorum(Collections.singleton(dcId2),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();
        int planId =
            cs.createFailoverPlan("failover-plan",
                                   Collections.singleton(dcId4),
                                   Collections.singleton(dcId1));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(3, RMIFailed.class);  /* 1 admin, 1 RN, 1 SN */

        /* Start zone 1 with services disabled */
        createStore.startSNA(0, true /* disableServices */);

        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();
    }

    /**
     * Test failover of a zone that contains Arbiters.
     * The AN's are moved to an offline secondary zone.
     * Repair topology using repair-plan and rebalance.
     */
    @Test
    public void testFailoverRepairPrimaryWithArbiters()
        throws Exception {

        final DatacenterId dcId1 = new DatacenterId(1);
        final DatacenterId dcId2 = new DatacenterId(2);
        final DatacenterId dcId3 = new DatacenterId(3);
        /*
         * 1 RF=1 zone with arbiters,
         * 1 RF=1 zone with arbiters,
         * one RF=1 secondary zone,1 shard
         */
        numSNs = 6;
        zones = asList(new ZoneInfo(1),
                       new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        zones.get(0).setAllowArbiters(true);
        zones.get(1).setAllowArbiters(true);
        init();

        /* Fail zone 1 */
        shutdownSNs(0, 1);

        /* Repair admin quorum and failover using zone 2 */
        /* get admin from sn1 */
        cs = createStore.getAdmin(2);
        cs.repairAdminQuorum(Collections.singleton(dcId2),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();

        /*
         * Do a failover offline zone 1, online secondary zone 3
         */
        int planId =
            cs.createFailoverPlan("failover-plan",
                                   Collections.singleton(dcId3),
                                   Collections.singleton(dcId1));
        cs.approvePlan(planId);

        /*
         * Note that you just execute the plan with force since
         * the new topology will have AN's in the secondary (offline)
         * zone.
         */
        cs.executePlan(planId, true);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(5, RMIFailed.class); /* 1 admin, 2 RN, 2 SN */

        /* Start zone 1 with services disabled */
        createStore.startSNA(0, true /* disableServices */);
        createStore.startSNA(1, true /* disableServices */);

        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        String candName = "candi2";
        cs.copyCurrentTopology(candName);
        cs.rebalanceTopology(candName,
                             CreateStore.STORAGE_NODE_POOL_NAME,
                             null);
        planId = cs.createDeployTopologyPlan("last-deploy-topo-plan",
                                             candName, null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();
    }

    /**
     * Test fail over that changes the total primary
     * RF from two(with arbiters) to three. Use rebalance
     * to correct the violations since there should not be
     * arbiters with RF=3;
     */
    @Test
    public void testFailoverChangeRFWithArbiters()
        throws Exception {

        final DatacenterId dcId1 = new DatacenterId(1);
        final DatacenterId dcId2 = new DatacenterId(2);
        final DatacenterId dcId3 = new DatacenterId(3);
        /*
         * 1 RF=1 zone with arbiters,
         * 1 RF=1 zone with arbiters,
         * one RF=2 secondary zone,1 shard
         */
        numSNs = 8;
        zones = asList(new ZoneInfo(1),
                       new ZoneInfo(1),
                       new ZoneInfo(2, DatacenterType.SECONDARY));
        zones.get(0).setAllowArbiters(true);
        zones.get(1).setAllowArbiters(true);
        init();

        /* Fail zone 1 */
        shutdownSNs(0, 1);

        /* Repair admin quorum and failover using zone 2 */
        /* get admin from sn1 */
        cs = createStore.getAdmin(2);
        cs.repairAdminQuorum(Collections.singleton(dcId2),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();

        /*
         * Do a failover offline zone 1, online secondary zone 3
         */
        int planId =
            cs.createFailoverPlan("failover-plan",
                                   Collections.singleton(dcId3),
                                   Collections.singleton(dcId1));
        cs.approvePlan(planId);

        /*
         * Note that you just execute the plan with force since
         * the new topology will have AN's in the secondary (offline)
         * zone.
         */
        cs.executePlan(planId, true);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(5, RMIFailed.class); /* 1 admin, 2 RN, 2 SN */

        /* Start zone 1 with services disabled */
        createStore.startSNA(0, true /* disableServices */);
        createStore.startSNA(1, true /* disableServices */);

        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();  /* wrong dc since it is 2ndary */
        verifyWarnings(8, MissingRootDirectorySize.class);

        String candName = "candi2";
        cs.copyCurrentTopology(candName);
        cs.rebalanceTopology(candName,
                             CreateStore.STORAGE_NODE_POOL_NAME,
                             null);
        planId = cs.createDeployTopologyPlan("last-deploy-topo-plan",
                                             candName, null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();
        verifyWarnings(8, MissingRootDirectorySize.class);
    }


    /**
     * Test fail over that changes the total primary
     * RF from three to two(with arbiters).
     */
    @Test
    public void testFailoverChangeRFFrom3To2WithArbiters()
        throws Exception {

        final DatacenterId dcId1 = new DatacenterId(1);
        /*
         * Zone 1: RF=1, allow arbiters
         * Zone 2: RF=1, allow arbiters
         * Zone 3: RF=1
         */
        numSNs = 6;
        zones = asList(new ZoneInfo(1),
                       new ZoneInfo(1),
                       new ZoneInfo(1));
        zones.get(0).setAllowArbiters(true);
        zones.get(1).setAllowArbiters(true);
        init();

        /* Fail zone 1 */
        shutdownSNs(0, 1);

        /* get admin from sn3 */
        cs = createStore.getAdmin(2);
        cs = createStore.getAdminMaster();

        /* Update admin group membership */
        cs.repairAdminQuorum(new HashSet<>(
                                 Arrays.asList(new DatacenterId(2),
                                               new DatacenterId(3))),
                             Collections.<AdminId>emptySet());

        /*
         * Do a failover with offline zone 1
         */
        int planId =
            cs.createFailoverPlan("failover-plan",
                                   Collections.emptySet(),
                                   Collections.singleton(dcId1));
        cs.approvePlan(planId);

        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(5, RMIFailed.class); /* 1 admin, 2 RN, 2 SN */

        /* Start zone 1 with services disabled */
        createStore.startSNA(0, true /* disableServices */);
        createStore.startSNA(1, true /* disableServices */);

        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();
        verifyWarnings(6, MissingRootDirectorySize.class);
    }

    /**
     * Test failover of zero rf zone hosting arbiters.
     * Failover will move the arbiters.
     */
    @Test
    public void testFailoverRepairRFZeroArbiters()
        throws Exception {

        final DatacenterId dcId3 = new DatacenterId(3);

        /*
         * 1 RF=1 zone allow arbiters
         * 1 RF=1 zone allow arbiters
         * 1 RF=0 zone arbiters,
         */
        numSNs = 5;
        zones = asList(new ZoneInfo(1),
                       new ZoneInfo(1),
                       new ZoneInfo(0));
        zones.get(0).setAllowArbiters(true);
        zones.get(1).setAllowArbiters(true);
        zones.get(2).setAllowArbiters(true);
        init();

        /* Fail zone 3 */
        shutdownSNs(4);

        cs = createStore.getAdminMaster();
        int planId =
            cs.createFailoverPlan("failover-plan",
                                  Collections.<DatacenterId>emptySet(),
                                  Collections.singleton(dcId3));
        cs.approvePlan(planId);
        cs.executePlan(planId, true);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(1, RMIFailed.class); /*  1 SN */

        /* Start zone 3 with services disabled */
        createStore.startSNA(4, true /* disableServices */);

        verifyViolations(2, ParamMismatch.class);
        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();
        verifyWarnings(1, UnderCapacity.class,
                       5, MissingRootDirectorySize.class);
    }

    /** This test reproduces problems found in Failover/Switchover test 12. */
    @Test
    public void testFailoverRepairPrimariesInStages()
        throws Exception {

        /* 4 RF=1 zones, 1 shards */
        numSNs = 4;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY));
        init();

        /* Fail zones 1, 2, and 3 */
        shutdownSNs(0, 1, 2);

        /* Repair admin quorum and failover using zone 4 */
        cs = createStore.getAdmin(3);
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(4)),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();
        int planId = cs.createFailoverPlan(
            "failover-plan",
            Collections.singleton(new DatacenterId(4)),   /* newPrimaryZones */
            set(new DatacenterId(1), new DatacenterId(2), /* offlineZones */
                new DatacenterId(3)));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(9, RMIFailed.class);      /* 3 admins, 3 RNs, 3 SNs */

        /* Start zone 3 with services disabled */
        createStore.startSNA(2, true /* disableServices */);

        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(6, RMIFailed.class);   /* 2 admins, 2 RNs, 2 SNs */

        /* Switchover to zone 3 as a primary and zone 4 as a secondary */
        String topo = "switchover-topo-1";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(3), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(4), DatacenterType.SECONDARY);
        deployTopo("switchover-plan", topo, false /* cancelRetry */);

        /* Start zones 1 and 2 with services disabled */
        createStore.startSNA(0, true /* disableServices */);
        createStore.startSNA(1, true /* disableServices */);

        /* Repair topo */
        planId = cs.createRepairPlan("repair-topo-plan-2");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations();

        /* Switchover to convert zones 1 and 2 back to primaries */
        topo = "switchover-topo-2";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.PRIMARY);
        cs.changeZoneType(topo, new DatacenterId(2), DatacenterType.PRIMARY);
        deployTopo("switchover-plan", topo, false /* cancelRetry */);

        verifyViolations();
    }

    /**
     * Test that notifying an RN that it has new parameters while the RN is
     * restarting throws an exception.  Previously, the implementation of
     * RepNodeAdmin.newParameters assumed that the environment had not been
     * created if it was null.  But if the environment is in the process of
     * being created, then the new parameters would be missed, which caused
     * switchovers to fail when electable group override changes were ignored.
     */
    @Test
    public void testNewParametersDuringRestart()
        throws Exception {

        numSNs = 1;
        zones = asList(new ZoneInfo(1));
        useThreads = true;
        init();

        /* Set a hook that will wait during JE environment creation */
        final Semaphore startHook = new Semaphore(0);
        final Semaphore finishHook = new Semaphore(0);
        try {
            RepEnvHandleManager.openEnvTestHook =
                new TestHook<RepNodeService.Params>() {
                    @Override
                    public void doHook(RepNodeService.Params params) {
                        logger.info("Entered openEnvTestHook");
                        startHook.release();
                        try {
                            finishHook.tryAcquire(1, 60, SECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(
                                "Unexpected interrupt", e);
                        }
                        logger.info("Exiting openEnvTestHook");
                    }
                };

            /* Restart the RN by restarting its SN and wait for the hook */
            shutdownSNs(0);
            createStore.startSNA(0, false, false);
            assertTrue("Wait for hook", startHook.tryAcquire(1, 60, SECONDS));

            /* Get the RN admin API */
            cs = createStore.getAdminMaster();
            final RepNodeAdminAPI rnApi =
                new RegistryUtils(cs.getTopology(),
                                  createStore.getSNALoginManager(0), logger)
                .getRepNodeAdmin(new RepNodeId(1, 1));

            /* Make sure newParameters throws the right exception */
            try {
                rnApi.newParameters();
                fail("Expected RepNodeAdminFaultException");
            } catch (RepNodeAdminFaultException e) {
                logger.info("Got exception: " + e);
                assertEquals(
                    IllegalRepNodeServiceStateException.class.getName(),
                    e.getFaultClassName());
            }

        } finally {
            finishHook.release();
        }
    }

    @Test
    public void testFailoverRepairPrimaryWithSecondaryAhead()
        throws Exception {

        /* 4 RF=1 zones, 1 shards */
        numSNs = 4;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY));

        init();

        /* Change JE parameters of zone 4 */
        RepNodeId rnId = createStore.getRNs(new StorageNodeId(4)).get(0);
        Parameters parms = cs.getParameters();
        RepNodeParams rnParams = new RepNodeParams(parms.get(rnId));
        rnParams.getMap().setParameter(ParameterState.JE_MISC, JE_MISC_PARAMS);

        int planId = cs.createChangeParamsPlan(
            "change-params", rnId, rnParams.getMap());
        cs.approvePlan(planId);
        cs.executePlan(planId, false /* force*/);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Populate store */
        populateStore();

        /* Fail zone 3 */
        shutdownSNs(2);

        /* Populate store again, so zone 3 doesn't have recent data */
        populateStore();

        /* Fail zones 1 and 2 */
        shutdownSNs(0, 1);

        /* Start zone 3 */
        createStore.startSNA(2, false, false);

        /* Repair admin quorum and failover using zone 3 */
        cs = createStore.getAdmin(3);
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(3)),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();
        planId = cs.createFailoverPlan(
            "failover-plan",
            Collections.<DatacenterId>emptySet(), /* newPrimaryZones */
            set(new DatacenterId(1), new DatacenterId(2) /* offlineZones */));
        cs.approvePlan(planId);

        try {
            cs.executePlan(planId, false);
            fail("Expected fail");
        } catch (AdminFaultException afe) {
            assertThat("data loss", afe.getMessage(),
                       containsString("may result in data loss"));
        }
        cs.executePlan(planId, true /* force*/);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        verifyViolations(6, RMIFailed.class); /* 2 admins, 2 RNs, 2 SNs */

        /* Verify if previous JE parameters has been reverted */
        parms = cs.getParameters();
        rnParams = new RepNodeParams(parms.get(rnId));
        assertEquals(JE_MISC_PARAMS,
                     rnParams.getMap().get(ParameterState.JE_MISC).asString());
    }

    @Test
    public void testFailoverRepairWithPrimaryDown()
        throws Exception {

        testFailoverRepairWithNodeDown(2);
    }

    @Test
    public void testFailoverRepairWithSecondaryDown()
        throws Exception {

        testFailoverRepairWithNodeDown(3);
    }

    private void testFailoverRepairWithNodeDown(int snaIndex)
        throws Exception {

        /* 4 RF=1 zones, 1 shards */
        numSNs = 4;
        zones = asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1),
                       new ZoneInfo(1, DatacenterType.SECONDARY));

        init();

        /* Fail zones 1 and 2 */
        shutdownSNs(0, 1);

        /* Repair admin quorum and failover using zone 3 */
        cs = createStore.getAdmin(3);
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(3)),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();

        /* Shutdown the primary RN */

        StorageNodeAgent sna = createStore.getStorageNodeAgent(snaIndex);
        RepNodeId rnId = createStore.getRNs(sna.getStorageNodeId()).get(0);
        assertTrue(sna.stopRepNode(rnId, true /* force*/));

        int planId = cs.createFailoverPlan(
            "failover-plan",
            Collections.<DatacenterId>emptySet(), /* newPrimaryZones */
            set(new DatacenterId(1), new DatacenterId(2) /* offlineZones */));
        cs.approvePlan(planId);

        try {
            cs.executePlan(planId, false);
            fail("Expected fail");
        } catch (AdminFaultException afe) {
            assertThat("vlsn unavailable", afe.getMessage(),
                       containsString("Unable to find sequence number"));
        }
    }

    /**
     * Test replacing SNs from a failed zone with new SNs. [#27598]
     */
    @Test
    public void testFailoverSwitchoverReplaceSNs()
        throws Exception {

        /*
         * Create an RF=3 store with a secondary zone and with extra SNs for
         * the primary zone:
         *
         * Zone 1 (secondary): sn1, sn2, sn3
         * Zone 2 (primary): sn4, sn5, sn6
         * Extra SNs for Zone 2: sn7, sn8, sn9
         */
        numSNs = 9;

        /*
         * Put the secondary zone first because CreateStore will assign the
         * extra SNs to the last listed zone, and the zone used needs to be the
         * original primary zone.
         */
        zones = asList(new ZoneInfo(3, DatacenterType.SECONDARY),
                       new ZoneInfo(3));

        /*
         * Create the store explicitly so we can set the pool to just use the 6
         * initial SNs
         */
        createStore();
        createStore.setPoolSize(6);
        init();

        /*
         * Failover to Zone 1 after Zone 2 fails:
         *
         * Zone 1 (primary): sn1, sn2, sn3
         * Zone 2 (secondary/offline): sn4, sn5, sn6
         * Extra SNs for Zone 2: sn7, sn8, sn9
         */

        /* Fail zone 2 */
        shutdownSNs(3, 4, 5);

        /* Repair admin quorum using zone 1 */
        cs = createStore.getAdmin(0);
        cs.repairAdminQuorum(Collections.singleton(new DatacenterId(1)),
                             Collections.<AdminId>emptySet());
        cs = createStore.getAdminMaster();

        /* Failover to zone 1 */
        int planId =
            cs.createFailoverPlan("failover",
                                  Collections.singleton(new DatacenterId(1)),
                                  Collections.singleton(new DatacenterId(2)));
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verifyViolations(
            /* Failed Zone 2: sn{4,5,6}, admin{4,5,6}, rg1-rn{4,5,6} */
            9, RMIFailed.class);

        /*
         * Move SNs from the failed zone to new SNs:
         *
         * Zone 1 (primary): sn1, sn2, sn3
         * Zone 2 (secondary): sn7, sn8, sn9
         */

        final StorageNodeId[] snIds = createStore.getStorageNodeIds();

        /*
         * Remove admins in failed zone 2. The admins need to be removed before
         * we can remove the SNs.
         *
         * It would be nice if we could specify failedSN=true so that the plans
         * would succeed, but that causes trouble in a unit test because the
         * plan needs to check the minimum store version in that case, which
         * has probably not been computed yet. So specify failedSN=false and
         * expect the plan to fail.
         *
         * See: [#27624] 'plan remove-admin -failed-sn -force' should ignore
         * upgrade issues
         */
        for (int i = 4; i <= 6; i++) {
            planId = cs.createRemoveAdminPlan("remove-admins-zone2",
                                              null,
                                              createStore.getAdminId(i - 1),
                                              false);
            cs.approvePlan(planId);
            cs.executePlan(planId, true);
            Plan.State state = cs.awaitPlan(planId, 0, null);
            assertEquals("Expecting 'plan remove-admin' to fail",
                         Plan.State.ERROR, state);
            // For debugging:
            // String status = cs.getPlanStatus(
            //     planId,
            //     StatusReport.VERBOSE_BIT | StatusReport.SHOW_FINISHED_BIT,
            //     true /* json */);
            // System.out.println("Status: " + status);
            cs.cancelPlan(planId);
        }

        verifyViolations(
            /* Failed zone 2: sn{4,5,6}, rg1-rn{4,5,6} */
            6, RMIFailed.class,
            /* Need admins in zone 2, but can't add until SNs are migrated */
            1, InsufficientAdmins.class);

        /*
         * Migrate SNs in zone 2 to new SNs.
         *
         * Call the plans with -force because they will fail due to a couple
         * problems, but will still have the desired effect. One issue is that
         * the initial verification will fail because of an inability to look
         * at the SN's configuration or to find the RNs in the JE HA group
         * (because they are secondaries). The issue is that attempts to update
         * the RN parameter state for the other failed RNs fails because the
         * associated SNs are unavailable.
         *
         * See: [#27623] Add -failed-zone flag to 'plan migrate-sn' command
         */
        for (int i = 4; i <= 6; i++) {
            planId = cs.createMigrateSNPlan("migrate-sn" + i,
                                            snIds[i - 1],
                                            snIds[i + 2]);
            cs.approvePlan(planId);
            cs.executePlan(planId, true);
            Plan.State state = cs.awaitPlan(planId, 0, null);
            assertEquals("Expecting 'plan migrate-sn' to fail",
                         Plan.State.ERROR, state);
            // For debugging:
            // String status = cs.getPlanStatus(
            //     planId,
            //     StatusReport.VERBOSE_BIT | StatusReport.SHOW_FINISHED_BIT,
            //     true /* json */);
            // System.out.println("Status: " + status);
            cs.cancelPlan(planId);
        }

        verifyViolations(
            /* Failed zone 2: sn{4,5,6} */
            3, RMIFailed.class,
            /* Add new admins to zone 2 after SNs are migrated */
            1, InsufficientAdmins.class);

        /* Add new admins */
        for (int i = 7; i <= 9; i++) {
            planId = cs.createDeployAdminPlan(
                "deploy-admin-" + i, snIds[i - 1],
                /*
                 * Specify null admin type to get zone type, to mirror what the
                 * CLI does and create secondary admins to match the zone type
                 */
                null);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

        /* Remove old SNs */
        for (int i = 4; i <= 6; i++) {
            planId = cs.createRemoveSNPlan("remove-sn" + i,
                                           snIds[i - 1]);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
        }

        verifyViolations();

        /*
         * Switchover to Zone 2:
         *
         * Zone 1 (secondary): sn1, sn2, sn3
         * Zone 2 (primary): sn7, sn8, sn9
         */
        final String topo = "switchover-topo";
        cs.copyCurrentTopology(topo);
        cs.changeZoneType(topo, new DatacenterId(1), DatacenterType.SECONDARY);
        cs.changeZoneType(topo, new DatacenterId(2), DatacenterType.PRIMARY);
        deployTopo("switchover", topo, false /* cancelRetry */);

        verifyViolations();
    }

    /* Utilities */

    /**
     * Wait for verifying the configuration to match the specified problems.
     */
    private void verifyViolations(Object... countsAndProblemClasses)
        throws RemoteException {

        AdminUtils.verifyViolations(cs, countsAndProblemClasses);
    }

    private void verifyWarnings(Object... countsAndProblemClasses)
        throws RemoteException {

        AdminUtils.verifyWarnings(cs, countsAndProblemClasses);
    }

    private void verifyTopology(DatacenterType... zoneTypes)
        throws Exception {

        shell.connectAdmin("localhost", createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        cs = shell.getAdmin();

        /* Wait for any violations to be repaired */
        verifyViolations();

        final Topology topo = cs.getTopology();
        int zn = 1;
        for (DatacenterType zoneType : zoneTypes) {
            assertEquals("Zone " + zn, zoneType,
                         topo.get(new DatacenterId(zn)).getDatacenterType());
            zn++;
        }
    }

    /**
     * Execute plan, allowing it to be interrupted for each admin, and either
     * canceling and starting a new one on interrupts, or reexecuting the
     * interrupted plan, and verifying that the plan succeeds.
     */
    private void deployTopo(String planName,
                            String candidate,
                            boolean cancelRetry)
        throws Exception {

        deployTopo(planName, candidate, cancelRetry, Plan.State.SUCCEEDED);
    }

    /**
     * Execute plan, allowing it to be interrupted for each admin, and either
     * canceling and starting a new one on interrupts, or reexecuting the
     * interrupted plan, and verifying that the plan otherwise reaches the
     * specified state.
     */
    private void deployTopo(String planName,
                            String candidate,
                            boolean cancelRetry,
                            Plan.State finalState)
        throws Exception {

        int planId = -1;
        Plan.State state = null;
        for (int i = 0; i < adminSNs.length; i++) {
            if ((i == 0) || cancelRetry) {
                planId = cs.createDeployTopologyPlan(
                    planName + "-" + i, candidate, null);
                cs.approvePlan(planId);
                cs.executePlan(planId, false);
            }

            /* Wait for the plan to end */
            while (true) {
                try {
                    state = cs.awaitPlan(planId, 0, null);
                    break;
                } catch (AdminFaultException afe) {
                    /*
                     * If the Admin is not available, retry, possibly at some
                     * other Admin
                     */
                    if (!afe.getFaultClassName().equals(
                                      AdminNotReadyException.class.getName())) {
                        throw afe;
                    }
                    final int masterAdminIndex = createStore.getAdminIndex();

                    final boolean ok = new PollCondition(1000, 60000) {
                        @Override
                        protected boolean condition() {
                            try {
                                if (!createStore.getAdmin(masterAdminIndex)
                                    .getAdminStatus()
                                    .getReplicationState()
                                    .isMaster()) {
                                    return true;
                                }
                            } catch (Exception e) {
                            }
                            return false;
                        }
                    }.await();
                    assertTrue("Waiting for admin" + (masterAdminIndex+1) +
                               " to restart as replica",
                               ok);

                    cs = createStore.getAdminMaster();
                }
            }
            if (state == Plan.State.SUCCEEDED) {
                break;
            }
            if (cancelRetry) {

                /*
                 * Cancel the plan, repair any problems, and run a new one.
                 * Retry canceling if the admin isn't fully the master yet.
                 */
                final int finalPlanId = planId;
                final AtomicReference<Exception> exception =
                    new AtomicReference<>();
                final boolean result = new PollCondition(1000, 20000) {
                    @Override
                    protected boolean condition() {
                        try {
                            cs.cancelPlan(finalPlanId);
                            return true;
                        } catch (Exception e) {
                            if ((e.getCause() instanceof
                                 IllegalCommandException) &&
                                e.getCause().getMessage().contains(
                                    "not the master")) {
                                return false;
                            }
                            exception.set(e);
                            return true;
                        }
                    }
                }.await();
                if (exception.get() != null) {
                    throw exception.get();
                }
                assertTrue("cancel plan", result);
                planId = cs.createRepairPlan(planName + "-repair-" + i);
                cs.approvePlan(planId);
                cs.executePlan(planId, false);
                cs.awaitPlan(planId, 0, null);
                cs.assertSuccess(planId);
            }
        }
        assertEquals("Final plan state", finalState, state);
    }

    private void shutdownSNs(final int... sns) {
        for (int sn : sns) {
            createStore.shutdownSNA(sn, true);
        }
        final boolean ok = new PollCondition(1000, 30000) {
            @Override
            protected boolean condition() {
                for (int sn : sns) {
                    try {
                        RegistryUtils.getAdmin(
                            "localhost", createStore.getRegistryPort(sn),
                            null /* loginMgr */, logger).ping();
                        logger.info("Admin " + sn + " is still active");
                        return false;
                    } catch (RemoteException | NotBoundException e) {
                        continue;
                    } catch (Exception e) {
                        logger.info("Problem with Admin " + sn + ": " + e);
                        return false;
                    }
                }
                return true;
            }
        }.await();
        assertTrue("Wait for admins to stop", ok);
    }
}
