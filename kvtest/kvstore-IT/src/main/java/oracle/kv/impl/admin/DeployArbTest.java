/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.KVVersion;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.Validations.EmptyZone;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.Ping;

import org.junit.Assert;
import org.junit.Test;

/**
 * Basic testing of the deployment of topology candidates with Arbiters.
 * made with create, topology redistribute, topology rebalance commands.
 */
@TestClassTimeoutMillis(40*60*1000)
public class DeployArbTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    private final String dcName = "DataCenterA";
    private final String dcName2 = "DataCenterAva";
    private final String candName = "topo";
    private final String candName2 = "topo2";
    private final String poolName = Parameters.DEFAULT_POOL_NAME;
    private final String planName = "plan1";
    private final String planName2 = "plan2";
    private final boolean useThreads = false;

    private final int nSNSet = 6;

    @Override
    public void setUp()
        throws Exception {

        /* Allow write-no-sync durability for faster test runs. */
        TestStatus.setWriteNoSyncAllowed(true);
        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (su != null) {
            su.close();
        }

        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testArbDeploy()
        throws Exception {
        doArbDeploy();

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * Test upgrade order on topology with Arbiters [#27808]
         */
        KVVersion c = KVVersion.CURRENT_VERSION;
        KVVersion upVer =
            new KVVersion(c.getMajor(), c.getMinor() + 1, 0, null);
        List <Set <StorageNodeId>> upgradeList =
            cs.getUpgradeOrderList(upVer, KVVersion.PREREQUISITE_VERSION);

        assertTrue("Upgrade order in invalid.", upgradeList.size() == 3);
        for (Set <StorageNodeId> s : upgradeList) {
            assertTrue("Upgrade order is invalid.",
                       s.size() == 1);
        }
        /* The ping output of ArbNode should display serviceStartTime. */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        Topology topo = cs.getTopology();
        Ping.pingTopology(topo, null, false, 0, ps, null, null, logger);
        String textRep = baos.toString();
        Pattern p = Pattern.compile(".*Arb Node.*serviceStartTime.*",
                                    Pattern.DOTALL);
        Matcher m = p.matcher(textRep);
        assertTrue(textRep, m.matches());

        /*
         * Test fix for KVSTORE-1692: kv_admin ping -shard is not showing
         * arbiters nodes
         */
        ps.close();
        baos.close();
        baos = new ByteArrayOutputStream();
        ps = new PrintStream(baos);
        Ping.pingTopology(topo, null, false, 0, ps, null,
                          new RepGroupId(1), logger);
        final String pingStat = baos.toString();
        p = Pattern.compile(".*Arb Node.*rg1-an1.*", Pattern.DOTALL);
        m = p.matcher(pingStat);
        assertTrue(pingStat, m.matches());
    }


    @Test
    public void testArbSNMigration()
        throws Exception {
        doArbDeploy();

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /* Add admin to correct violation */
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);


        StorageNodeAgent sna = snSet.getSNA(2);
        StorageNodeId origId = snSet.getId(2);
        sna.shutdown(true, true);

        /* Add new SN */
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName),
                     String.valueOf(2 * MB_PER_SN), 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 3);
        StorageNodeId newId = snSet.getId(3);



        int planId =
            cs.createMigrateSNPlan("Migrate sn2 to sn3", origId, newId);
        runPlan(cs, planId);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              4 /*numSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                              3/*nARBs*/, 100 /*numParts*/);

        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }


    /**
     * Test changes store from RF 1 to 2 and checks that Arbiters are created.
     * @throws Exception
     */
    @Test
    public void testChangeToARB() throws Exception {
        doDeployRF1();
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 );
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        doModifyRF2();

        snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        allPIds = cs.getTopology().getPartitionMap().getAllIds();
        keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    @Test
    public void testChangeRF()
        throws Exception {
        doArbDeploy();
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Change RF */
        doModifyRF3();

        snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        allPIds = cs.getTopology().getPartitionMap().getAllIds();
        keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    @Test
    public void testArbRelocate()
        throws Exception {
        doArbDeploy();

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /* Add admin to correct violation */
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);

        /* Add new SN */
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName),
                     String.valueOf(2 * MB_PER_SN), 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 3);


        cs.copyCurrentTopology(candName2);

        Topology topo = cs.getTopology();
        Set<ArbNodeId> anIds = topo.getArbNodeIds();
        ArbNodeId movedANId = null;
        StorageNodeId oldSNId = null;
        for (ArbNodeId anId : anIds) {
            ArbNode an = topo.get(anId);
            oldSNId = an.getStorageNodeId();
            movedANId = anId;
            for (int i = 0; i < nSNSet; i++) {
                StorageNodeId curSNId = snSet.getId(i);
                if (oldSNId.equals(curSNId)) {
                    snSet.changeParams(cs, ParameterState.SN_ALLOW_ARBITERS,
                                       "false", i);
                    break;
                }
            }
            break;
        }
        cs.rebalanceTopology(candName2, poolName, null);
        /* Deploy initial topology */
        runDeploy(planName2, cs, candName2, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                4 /*numSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                3/*nARBs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, UnderCapacity.class,
                                    3, MultipleRNsInRoot.class,
                                    4, MissingRootDirectorySize.class);

        /* Check AN was moved */
        topo = cs.getTopology();
        ArbNode movedAN = topo.get(movedANId);

        Assert.assertFalse("AN was not moved when it should have been. snId" +
                            oldSNId,
                            movedAN.getStorageNodeId().equals(oldSNId));
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                AdminUtils.HOSTNAME,
                snSet.getRegistryPort(0),
                StoreUtils.RecordType.INT,
                1 );
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
                cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    @Test
    public void testArbDeploy2DC()
        throws Exception {
        doArbDeploy2DCRF1();

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    @Test
    public void testArbDeploy2DCChangeZoneType()
        throws Exception {
        doArbDeploy2DCRF1();

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
        DatacenterId dc1Id = new DatacenterId(2);
        cs.copyCurrentTopology("TopoB");
        cs.changeZoneType("TopoB", dc1Id, DatacenterType.SECONDARY);
        final AdminFaultException afe = checkException(
            () -> cs.createDeployTopologyPlan(
                "Change Zone1 Type", "TopoB", null),
            AdminFaultException.class,
            "Attempt to reduce the overall primary replication factor by" +
            " 1 from 2 to 1.");
        assertEquals(IllegalCommandException.class.getName(),
                     afe.getFaultClassName());
    }

    /*
     * Create dc1 rf=1 -no-arbiters 3 SN(cap=1), dc2 rf=0 -arbiters 0 SN
     * Add 3 SN (cap=1) to dc2, change dc2 repfactor to 1
     * Check that topo contains expected components including arbiters.
     */
    @Test
    public void testArbTopoEvolution() throws Exception
    {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      nSNSet /* SNs */,
                                      dcName,
                                      1 /* repFactor*/,
                                      useThreads /* useThreads */,
                                      MB_PER_SN_STRING /* memory */,
                                      false /* allowArbiters */);
        /*
         * Deploy two more SNs on DataCenterA, for a total of 2 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING,
                     0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1, 2);

        AdminUtils.deployDatacenter(cs,
                                    dcName2,
                                    0 /* RF */,
                                    DatacenterType.PRIMARY,
                                    sysAdminInfo,
                                    true);

        /* make an initial topology */
        String first = candName;
        cs.createTopology(first, poolName, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              2 /*dc*/, 3 /*nSNs*/, 3 /*nRG*/,
                              3/*nRN*/, 0/*nARBs*/,100);

        /*
         * Deploy initial topology -- use force since it will contain an
         * EmptyZone violation for Zone 2
         */
        runDeploy(planName, cs, first, 1, false /* generateStatusReport */,
                  true /* force */);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 2 /* numDCs */,
                              3 /*numSNs*/, 3 /*numRGs*/, 3 /*numRNs*/,
                              0/*nARBs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 1 /* rf */,
                                    5000,
                                    logger,
                                    1, EmptyZone.class,
                                    3, MissingRootDirectorySize.class);

        snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING,
                     3, 4, 5);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 3, 4, 5);

        /* make a check that you may not add an admin in zero rf zone */
        boolean gotException = false;
        try {
            AdminUtils.deployAdmin(cs, snSet, 3, AdminType.PRIMARY);
        } catch (Exception e) {
            gotException = true;
        }
        assertTrue(gotException);

        cs.copyCurrentTopology(candName2);

        cs.changeRepFactor(candName2, poolName,
                           sysAdminInfo.getDCId(dcName2), 1);
        runDeploy(planName2, cs, candName2, 1, true);
        AdminUtils.deployAdmin(cs, snSet, 3, AdminType.PRIMARY);

        DeployUtils.checkTopo(cs.getTopology(), 2 /* numDCs */,
                              6 /*numSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                              3/*nARBs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    6, MissingRootDirectorySize.class);
    }

    private void runDeploy(String planname,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport)
        throws RemoteException {

        runDeploy(planname, cs, candidateName, numPlanRepeats,
                  generateStatusReport, false /* force */);
    }

    private void runDeploy(String planname,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport,
                           boolean force)
        throws RemoteException {

        runDeploy(true /* expectChange */, planname, cs, candidateName,
                  numPlanRepeats, generateStatusReport, force);
    }

    /**
     * @param generateStatusReport if true, create a thread that will generate
     * a status report while the plan is running, hoping to exercise the
     * reporting module in a realistic way.
     */
    private void runDeploy(boolean expectChange,
                           String planname,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport,
                           boolean force)
        throws RemoteException {

        try {
            for (int i = 0; i < numPlanRepeats; i++) {
                DeployUtils.printCandidate(cs, candidateName, logger);
                boolean noChange =  (expectChange && (i == 0)) ? false : true;
                DeployUtils.printPreview(candidateName, noChange, cs, logger);
                if (i > 0) {
                    logger.info(i + "th repeat of " + planname );
                }

                int planNum = cs.createDeployTopologyPlan(planname,
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

                cs.executePlan(planNum, force);
                cs.awaitPlan(planNum, 0, null);
                if (statusThread != null) {
                    statusThread.cancel();
                }

                logger.info
                    ("Plan status report \n" +
                     cs.getPlanStatus(planNum, (StatusReport.SHOW_FINISHED_BIT |
                                                StatusReport.VERBOSE_BIT),
                                                false));
                cs.assertSuccess(planNum);
                DeployUtils.printCurrentTopo(cs, i + "th iteration of " +
                                             planname, logger);
            }
        } catch (RuntimeException e) {
            logger.severe(LoggerUtils.getStackTrace(e));
            logger.severe(cs.validateTopology(null));
            logger.severe(cs.validateTopology(candidateName));
            throw e;
        }
    }

    private void doArbDeploy() throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      nSNSet /* SNs */,
                                      dcName,
                                      2 /* repFactor*/,
                                      useThreads /* useThreads */,
                                      String.valueOf(
                                          2 * MB_PER_SN) /* memory */,
                                      true /* allowArbiters */);
        /*
         * Deploy two more SNs on DataCenterA, for a total of 2 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName),
                     String.valueOf(2 * MB_PER_SN), 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /* make an initial topology */
        String first = candName;
        cs.createTopology(first, poolName, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 3 /*nSNs*/, 3 /*nRG*/,
                              6/*nRN*/, 3/*nARBs*/,100);

        /* Deploy initial topology */
        runDeploy(planName, cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              3 /*numSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                              3/*nARBs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingRootDirectorySize.class,
                                    3, MultipleRNsInRoot.class);

    }

    private void doDeployRF1()
            throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      nSNSet /* SNs */,
                                      dcName,
                                      1 /* repFactor*/,
                                      useThreads /* useThreads */,
                                      MB_PER_SN_STRING /* memory */,
                                      true /* allowArbiters */);
        /*
         * Deploy two more SNs on DataCenterA, for a total of 2 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING, 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1);

        /* make an initial topology */
        String first = candName;
        cs.createTopology(first, poolName, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 2 /*nSNs*/, 2 /*nRG*/,
                              2/*nRN*/, 0/*nARBs*/,100);

        /* Deploy initial topology */
        runDeploy(planName, cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              2 /*numSNs*/, 2 /*numRGs*/, 2 /*numRNs*/,
                              0/*nARBs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 1 /* rf */,
                                    5000,
                                    logger,
                                    2, MissingRootDirectorySize.class);

    }

    private void doModifyRF2() throws Exception {
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING, 2, 3);

        /*
         * Add  SNs
         */
        cs.copyCurrentTopology(candName2);

        cs.changeRepFactor(candName2, poolName,
                           sysAdminInfo.getDCId(dcName), 2);
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);
        /* Deploy initial topology */
        runDeploy(planName2, cs, candName2, 1, false);
        /*
         * Assert that the deployed topology has the number of
         * shards, ANs and RNs that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              4 /*numSNs*/, 2 /*numRGs*/, 4 /*numRNs*/,
                              2/*nARBs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 2 /* rf */, 5000, logger,
                                    4, MissingRootDirectorySize.class);
    }

    private void doModifyRF3() throws Exception {
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING,
                     3, 4, 5);

        /*
         * Add  SNs
         */
        cs.copyCurrentTopology(candName2);
        cs.changeRepFactor(candName2, poolName,
                           sysAdminInfo.getDCId(dcName), 3);
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);
        AdminUtils.deployAdmin(cs, snSet, 2, AdminType.PRIMARY);
        /* Deploy initial topology */
        runDeploy(planName2, cs, candName2, 1, false);
        /*
         * Assert that the deployed topology has the number of
         * shards, ANs and RNs that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              6 /*numSNs*/, 3 /*numRGs*/, 9 /*numRNs*/,
                              0/*nARBs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    3, MultipleRNsInRoot.class,
                                    6, MissingRootDirectorySize.class);

    }

    private void doArbDeploy2DCRF1()
            throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      nSNSet /* SNs */,
                                      dcName,
                                      1 /* repFactor*/,
                                      useThreads /* useThreads */,
                                      MB_PER_SN_STRING /* memory */,
                                      false /* allowArbiters */);
        /*
         * Deploy two more SNs on DataCenterA, for a total of 2 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING,
                     0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1, 2);

        AdminUtils.deployDatacenter(cs,
                dcName2,
                1 /* RF */,
                DatacenterType.PRIMARY,
                sysAdminInfo,
                true);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING,
                     3, 4, 5);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 3, 4, 5);
        AdminUtils.deployAdmin(cs, snSet, 3, AdminType.PRIMARY);
        /* make an initial topology */
        String first = candName;
        cs.createTopology(first, poolName, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              2 /*dc*/, 6 /*nSNs*/, 3 /*nRG*/,
                              6/*nRN*/, 3/*nARBs*/,100);

        /* Deploy initial topology */
        runDeploy(planName, cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 2 /* numDCs */,
                              6 /*numSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                              3/*nARBs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    6, MissingRootDirectorySize.class);

        DeployUtils.checkArbBalance(cs.getTopology());
    }

    private void runPlan(CommandServiceAPI cs, int planId)
        throws RemoteException {
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }
}
