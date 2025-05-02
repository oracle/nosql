/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.admin.AdminUtils.awaitPlanSuccess;
import static oracle.kv.impl.admin.AdminUtils.bootstrapStore;
import static oracle.kv.impl.admin.AdminUtils.deployDatacenter;
import static oracle.kv.impl.admin.DeployUtils.checkDeployment;
import static oracle.kv.impl.admin.DeployUtils.checkTopo;
import static oracle.kv.impl.admin.param.Parameters.DEFAULT_POOL_NAME;
import static oracle.kv.impl.param.ParameterState.COMMON_CAPACITY;
import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.regex.Pattern;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.AvailableStorageExceeded;
import oracle.kv.impl.admin.VerifyConfiguration.AvailableStorageLow;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.Validations;
import oracle.kv.impl.admin.topo.Validations.EmptyZone;
import oracle.kv.impl.admin.topo.Validations.ExcessAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MissingStorageDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.StorageDirectorySizeImbalance;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Basic testing of the deployment of topology candidates made with the
 * R2 topology create, topology redistribute, topology rebalance commands.
 */
/* Increase test timeout to 140 minutes -- test can take 70 minutes */
@TestClassTimeoutMillis(140*60*1000)
public class DeployTopoTest extends TestBase {

    private static final int AWAIT_DEPLOYMENT_MS = 10000;

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

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

        if (sysAdminInfo != null) {

            /*
             * Disable stats collection since it can interfere with cleanly
             * shutting down services
             */
            final CommandServiceAPI cs = sysAdminInfo.getCommandService();
            final ParameterMap map = new ParameterMap();
            map.setType(ParameterState.REPNODE_TYPE);
            map.setParameter(ParameterState.RN_SG_ENABLED, "false");
            final int pid =
                cs.createChangeAllParamsPlan("Disable stats collection",
                                             null /* zoneId */, map);
            cs.approvePlan(pid);
            cs.executePlan(pid, false);
            cs.awaitPlan(pid, 0, null);
            cs.assertSuccess(pid);

            /* Then shutdown all SNs */
            sysAdminInfo.getSNSet().shutdown();
        }

        if (su != null) {
            su.close();
        }

        LoggerUtils.closeAllHandlers();

        // oracle.kv.impl.async.AsyncTestUtils.checkActiveDialogTypes();
    }

    /**
     * Deploy a brand new store with two shards, then rebalance it.
     * @throws Exception
     */
    @Test
    public void testRebalance()
        throws Exception {

        doRebalance(1);
    }

    /**
     * The same as testRebalance, but repeat each plan twice to check for
     * idempotency.
     */
    @Test
    public void testRebalanceRepeat()
        throws Exception {

        doRebalance(2);
    }

    private void doRebalance(int numPlanRepeats)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads*/,
                                                 String.valueOf(
                                                     2 * MB_PER_SN));

        /*
         * Enable statistics gathering to work around a problem involving
         * waiting for an up-to-date shard [#26468]
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_SG_ENABLED, "true");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /* Deploy two more SNs on DataCenterA */

        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(2 * MB_PER_SN), 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1 /*dc*/, 3 /*nSNs*/, 2, 6, 100);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, numPlanRepeats, true);

        List<String> candList = cs.listTopologies();
        assertEquals(1, candList.size());
        assertEquals(first, candList.get(0));

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingRootDirectorySize.class,
                                    3, MultipleRNsInRoot.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Change the capacity so the store is out of compliance */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 1, 2);

        /*
         * Check deployment again: We expect two complaints that SNs are over
         * capacity and 3 that there are multiple RNs in the root directory
         */
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    3, MissingRootDirectorySize.class,
                                    2, OverCapacity.class,
                                    1, InsufficientAdmins.class,
                                    3, MultipleRNsInRoot.class);

        /*
         * Try a rebalance and deploy it, it will not be able to do anything
         * because there arent enough SNs.
         */
        String firstAttempt = "rebalanceAttempt_1";
        cs.copyCurrentTopology(firstAttempt);
        cs.rebalanceTopology(firstAttempt, Parameters.DEFAULT_POOL_NAME,
                             null);

        /*
         * Specify force=true to ignore violations for sn2 and sn3 being over
         * capacity
         */
        runDeploy(false, firstAttempt, cs, firstAttempt, numPlanRepeats, false,
                  null, true /* force */);
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              2 /*numRGs*/,  6 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    2, OverCapacity.class,
                                    3, MultipleRNsInRoot.class,
                                    3, MissingRootDirectorySize.class,
                                    1, InsufficientAdmins.class);

        /*
         * Add more SNs,and try a new rebalance.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3, 4);
        AdminUtils.deployAdmin(cs, snSet, 3);
        AdminUtils.deployAdmin(cs, snSet, 4);
        String secondAttempt = "rebalanceAttempt_2";
        DeployUtils.printCurrentTopo(cs, secondAttempt, logger);
        cs.copyCurrentTopology(secondAttempt);
        cs.rebalanceTopology(secondAttempt, Parameters.DEFAULT_POOL_NAME, null);

        runDeploy(secondAttempt, cs, secondAttempt, numPlanRepeats, false);
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 5 /*numSNs*/,
                              2 /*numRGs*/,  6 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, MultipleRNsInRoot.class,
                                    5, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * Change the repfactor, add more SNs, rebalance.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(2 * MB_PER_SN), 5, 6);
        AdminUtils.deployAdmin(cs, snSet, 5);
        AdminUtils.deployAdmin(cs, snSet, 6);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 5,6);
        String changeRFAttempt = "ChangeRepFactor";
        DeployUtils.printCurrentTopo(cs, changeRFAttempt, logger);
        cs.copyCurrentTopology(changeRFAttempt);
        cs.changeRepFactor(changeRFAttempt, Parameters.DEFAULT_POOL_NAME,
                           new DatacenterId(1), 5);

        runDeploy(changeRFAttempt, cs, changeRFAttempt, numPlanRepeats, false);
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 7 /*numSNs*/,
                              2 /*numRGs*/,  10 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 5 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    3, MultipleRNsInRoot.class,
                                    7, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * Add a SN, and move RN1 around. It will go to SN7, the only one
         * available.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     7);

        String moveAttempt = "moveRN";
        RepNodeId rnId = new RepNodeId(1,1);
        StorageNodeId oldHome = cs.getTopology().get(rnId).getStorageNodeId();
        DeployUtils.printCurrentTopo(cs, moveAttempt, logger);
        VerifyResults before = cs.verifyConfiguration(false, true, false);

        cs.copyCurrentTopology(moveAttempt);
        cs.moveRN(moveAttempt, rnId, null);

        runDeploy(moveAttempt, cs, moveAttempt, numPlanRepeats, false);
        StorageNodeId newHome = cs.getTopology().get(rnId).getStorageNodeId();
        assertTrue(newHome.toString(), !newHome.equals(oldHome));
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 8 /*numSNs*/,
                              2 /*numRGs*/,  10 /*numRNs*/, 100 /*numParts*/);
        Set<Class<?>> warnings = new HashSet<Class<?>>();
        Set<Problem> start = new HashSet<Problem>();
        start.addAll(before.getViolations());
        start.addAll(before.getWarnings());
        warnings.add(MultipleRNsInRoot.class);
        warnings.add(UnderCapacity.class);
        DeployUtils.checkDeployment(cs, 5, start, warnings, logger);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    @Test
    public void testContract()
        throws Exception {

        doContract(1);
    }

    @Test
    public void testContractRepeat()
        throws Exception {

        doContract(2);
    }

    @Test
    public void testContractWithArbter()
        throws Exception {

        doContractWithArbter(1);
    }

    @Test
    public void testContractWithArbterRepeat()
        throws Exception {

        doContractWithArbter(2);
    }

    private void doContractWithArbter(int numPlanRepeats) throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* maxSNs */,
                                                 "DataCenterA",
                                                 2 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING,
                                                 true);

        /* Deploy a total of six SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 6 SNs and 3 shards
         */
        String first6SNs = "first6SNs";
        AdminUtils.makeSNPool(cs, first6SNs, snSet.getIds(0, 1, 2, 3, 4, 5));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first6SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 6, 3, 6, 3, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 6 /*numSNs*/,
                              3 /*numRGs*/,  6 /*numRNs*/, 3/*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 2, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);


        /* Remove four SNs, contract, remove 1 shard. */
        logger.info("testContract: Make a new SNPool for contract");
        /* First make a pool of 10 SNs */
        AdminUtils.makeSNPool(cs, "first4SNs", snSet.getIds(0, 1, 2, 3));
        cs.copyCurrentTopology("contract");
        cs.contractTopology("contract", "first4SNs");

        candidateTopo = cs.getTopologyCandidate("contract").getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 6, 2, 4, 2, numPartitions);

        runDeploy("planA", cs, "contract", numPlanRepeats, false);
        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));
        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/, 4 /*numRNs*/, 2/*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    2, Validations.UnderCapacity.class,
                                    6, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Remove contracted storage nodes */
        final StorageNodeId sn5Id = new StorageNodeId(5);
        final StorageNodeId sn6Id = new StorageNodeId(6);

        removeStorageNode(sn5Id, cs);
        removeStorageNode(sn6Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 4 /*numSNs*/,
                              2 /*numRGs*/, 4 /*numRNs*/, 2/*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    4, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    private void doContract(int numPlanRepeats) throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of six SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5, 6, 7, 8);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 9 SNs and 3 shards
         */
        String first9SNs = "first9SNs";
        AdminUtils.makeSNPool(cs, first9SNs,
                              snSet.getIds(0, 1, 2, 3, 4, 5, 6, 7, 8));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first9SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 9, 3, 9, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/, 0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Remove four SNs, contract, remove 1 shard. */
        logger.info("testContract: Make a new SNPool for contract");
        /* First make a pool of 10 SNs */
        AdminUtils.makeSNPool(cs, "first6SNs", snSet.getIds(0, 1, 2, 3, 4, 5));
        cs.copyCurrentTopology("contract");
        cs.contractTopology("contract", "first6SNs");

        candidateTopo = cs.getTopologyCandidate("contract").getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 9, 2, 6, numPartitions);

        runDeploy("planA", cs, "contract", numPlanRepeats, false);
        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 9 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, UnderCapacity.class,
                                    9, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Remove contracted storage nodes */
        final StorageNodeId sn7Id = new StorageNodeId(7);
        final StorageNodeId sn8Id = new StorageNodeId(8);
        final StorageNodeId sn9Id = new StorageNodeId(9);

        removeStorageNode(sn7Id, cs);
        removeStorageNode(sn8Id, cs);
        removeStorageNode(sn9Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    /* Remove a storage node from topology */
    private void removeStorageNode(StorageNodeId snId, CommandServiceAPI cs)
        throws Exception {
        awaitPlanSuccess(
            cs, cs.createRemoveSNPlan("remove storage node", snId));
    }

    @Test
    public void testRemoveShard()
        throws Exception {

        doRemoveShard(1);
    }

    @Test
    public void testRemoveShardWithArbiter()
        throws Exception {

        /* TODO: Currently support for failed shard removal with arbiter
         * has limitation in RemoveAN where we need a active master
         * in shard for Arbiter removal. SR[#26553]
         */
        /* doRemoveShardWithArbiter(1); */
    }

    @Test
    public void testRemoveShardCapacity()
            throws Exception {

        doRemoveShardWithCapacity(1);
    }

    @Test
    public void testRemoveTwoShards()
        throws Exception {

        doRemoveTwoShards(1);
    }

    @Test
    public void testContractRemoveShard()
        throws Exception {

        doContractRemoveShard(1);
    }

    @Test
    public void testDeployWithoutFailedShard()
        throws Exception {

        doDeployWithoutFailedShard(1);
    }

    @SuppressWarnings("unused")
    private void doRemoveShardWithArbiter(int numPlanRepeats)
        throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* maxSNs */,
                                                 "DataCenterA",
                                                 2 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING,
                                                 true);

        /* Deploy a total of six SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 6 SNs and 3 shards
         */
        String first6SNs = "first6SNs";
        AdminUtils.makeSNPool(cs, first6SNs, snSet.getIds(0, 1, 2, 3, 4, 5));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first6SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 6, 3, 6, 3, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 6 /*numSNs*/,
                              3 /*numRGs*/,  6 /*numRNs*/, 3/*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 2, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Stop RN's corresponding to one shard */
        RepNodeId rg2_rn1 = new RepNodeId(2, 1);
        RepNodeId rg2_rn2 = new RepNodeId(2, 2);
        Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
        rnIds.add(rg2_rn1);
        rnIds.add(rg2_rn2);
        awaitPlanSuccess(cs, cs.createStopServicesPlan(null, rnIds),
                         /* Need force=true to stop the whole shard */
                         true /* force */);
        /* Remove 1 shard. */
        logger.info("testRemoveShard: New topology name and FailedShard object");
        RepGroupId failedShard = new RepGroupId(2);
        cs.removeFailedShard(failedShard, "removeShardTopo");

        candidateTopo = cs.getTopologyCandidate("removeShardTopo").getTopology();

        runDeployRemoveFailedShard("planA", cs, "removeShardTopo", numPlanRepeats,
                                   failedShard, false);
        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Remove contracted storage nodes */
        final StorageNodeId sn3Id = new StorageNodeId(3);
        final StorageNodeId sn4Id = new StorageNodeId(4);

        removeStorageNode(sn3Id, cs);
        removeStorageNode(sn4Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 4 /*numSNs*/,
                              2 /*numRGs*/, 4 /*numRNs*/, 2/*numANs*/,
                              numPartitions);

        DeployUtils.checkDeployment(cs, 2, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    4, MissingRootDirectorySize.class);
    }

    private void doRemoveShard(int numPlanRepeats) throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 12 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of nine SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5, 6, 7, 8);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 9 SNs and 3 shards
         */
        String first9SNs = "first9SNs";
        AdminUtils.makeSNPool(cs, first9SNs,
                              snSet.getIds(0, 1, 2, 3, 4, 5, 6, 7, 8));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first9SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 9, 3, 9, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/, 0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Stop RN's corresponding to one shard */
        RepNodeId rg2_rn1 = new RepNodeId(2,1);
        RepNodeId rg2_rn2 = new RepNodeId(2,2);
        RepNodeId rg2_rn3 = new RepNodeId(2,3);
        Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
        rnIds.add(rg2_rn1);
        rnIds.add(rg2_rn2);
        rnIds.add(rg2_rn3);
        awaitPlanSuccess(cs, cs.createStopServicesPlan(null, rnIds),
                         /* Need force=true to stop the whole shard */
                         true /* force */);

        /* Remove 1 shard i.e. rg2 */
        logger.info("testRemoveShard: New topology name and FailedShard object");
        RepGroupId failedShard = new RepGroupId(2);
        cs.removeFailedShard(failedShard, "removeShardTopo");

        candidateTopo = cs.getTopologyCandidate("removeShardTopo").getTopology();

        runDeployRemoveFailedShard("planA", cs, "removeShardTopo", numPlanRepeats,
                                   failedShard, false);
        logger.info("SecondDeploy: " +
                TopologyPrinter.printTopology(cs.getTopology(),
                                              cs.getParameters(),
                                              true));

        /* Remove contracted storage nodes */
        final StorageNodeId sn4Id = new StorageNodeId(4);
        final StorageNodeId sn5Id = new StorageNodeId(5);
        final StorageNodeId sn6Id = new StorageNodeId(6);

        removeStorageNode(sn4Id, cs);
        removeStorageNode(sn5Id, cs);
        removeStorageNode(sn6Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);

        /*
         * Check re-distribute operation on the topology having gaps in
         * shard numbering. Deploy three more SNs, redistribute,
         * expect 1 more shard.
         */
        logger.info("testRedistribute: Make a new SNPool for redistribution");
        /* First make a pool of 9 SNs */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     9, 10, 11);
        AdminUtils.makeSNPool(cs, "firstredistributeSNs",
                              snSet.getIds(0, 1, 2, 6, 7, 8, 9, 10, 11));
        cs.copyCurrentTopology("redistribute");
        cs.redistributeTopology("redistribute", "firstredistributeSNs");

        candidateTopo = cs.getTopologyCandidate("redistribute").getTopology();

        runDeploy("planA", cs, "redistribute", numPlanRepeats, false);
        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, numPartitions);

        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);
    }

    private void doRemoveShardWithCapacity(int numPlanRepeats)
        throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of three SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 3 SNs and 3 shards
         */
        String first3SNs = "first3SNs";
        AdminUtils.makeSNPool(cs, first3SNs, snSet.getIds(0, 1, 2));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first3SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 3, 3, 9, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/, 0 /*numANs*/,
                              numPartitions);

        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    3, MultipleRNsInRoot.class,
                                    1, InsufficientAdmins.class,
                                    3, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Stop RN's corresponding to one shard */
        RepNodeId rg2_rn1 = new RepNodeId(2,1);
        RepNodeId rg2_rn2 = new RepNodeId(2,2);
        RepNodeId rg2_rn3 = new RepNodeId(2,3);
        Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
        rnIds.add(rg2_rn1);
        rnIds.add(rg2_rn2);
        rnIds.add(rg2_rn3);
        awaitPlanSuccess(cs, cs.createStopServicesPlan(null, rnIds),
                         /* Need force=true to stop the whole shard */
                         true /* force */);

        /* Remove 1 shard i.e. rg2 */
        logger.info("testRemoveShard: New topology name and FailedShard object");
        RepGroupId failedShard = new RepGroupId(2);
        cs.removeFailedShard(failedShard, "removeShardTopo");

        candidateTopo = cs.getTopologyCandidate("removeShardTopo").getTopology();

        runDeployRemoveFailedShard("planA", cs, "removeShardTopo", numPlanRepeats,
                                   failedShard, false);
        logger.info("SecondDeploy: " +
                TopologyPrinter.printTopology(cs.getTopology(),
                                              cs.getParameters(),
                                              true));

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);

        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    3, UnderCapacity.class,
                                    3, MultipleRNsInRoot.class,
                                    1, InsufficientAdmins.class,
                                    3, MissingRootDirectorySize.class);
    }

    private void doRemoveTwoShards(int numPlanRepeats) throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of nine SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5, 6, 7, 8);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 9 SNs and 3 shards
         */
        String first9SNs = "first9SNs";
        AdminUtils.makeSNPool(cs, first9SNs,
                              snSet.getIds(0, 1, 2, 3, 4, 5, 6, 7, 8));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first9SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 9, 3, 9, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/, 0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Stop RN's corresponding to one shard */
        RepNodeId rg2_rn1 = new RepNodeId(2,1);
        RepNodeId rg2_rn2 = new RepNodeId(2,2);
        RepNodeId rg2_rn3 = new RepNodeId(2,3);
        Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
        rnIds.add(rg2_rn1);
        rnIds.add(rg2_rn2);
        rnIds.add(rg2_rn3);
        awaitPlanSuccess(cs, cs.createStopServicesPlan(null, rnIds),
                         /* Need force=true to stop the whole shard */
                         true /* force */);

        /* Remove 1 shard i.e. rg2 */
        logger.info("testRemoveShard: New topology name and FailedShard object");
        RepGroupId failedShard = new RepGroupId(2);
        cs.removeFailedShard(failedShard, "removeShardTopo");

        candidateTopo = cs.getTopologyCandidate("removeShardTopo").getTopology();

        runDeployRemoveFailedShard("planA", cs, "removeShardTopo", numPlanRepeats,
                                   failedShard, false);
        logger.info("SecondDeploy: " +
                TopologyPrinter.printTopology(cs.getTopology(),
                                              cs.getParameters(),
                                              true));

        /* Remove contracted storage nodes */
        final StorageNodeId sn4Id = new StorageNodeId(4);
        final StorageNodeId sn5Id = new StorageNodeId(5);
        final StorageNodeId sn6Id = new StorageNodeId(6);

        removeStorageNode(sn4Id, cs);
        removeStorageNode(sn5Id, cs);
        removeStorageNode(sn6Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);

        /* Stop RN's corresponding to another shard */
        RepNodeId rg3_rn1 = new RepNodeId(3,1);
        RepNodeId rg3_rn2 = new RepNodeId(3,2);
        RepNodeId rg3_rn3 = new RepNodeId(3,3);
        rnIds = new HashSet<RepNodeId>();
        rnIds.add(rg3_rn1);
        rnIds.add(rg3_rn2);
        rnIds.add(rg3_rn3);
        awaitPlanSuccess(cs, cs.createStopServicesPlan(null, rnIds),
                         /* Need force=true to stop the whole shard */
                         true /* force */);

        /* Remove 1 shard i.e. rg3 */
        logger.info("testRemoveShard: New topology name and FailedShard object");
        failedShard = new RepGroupId(3);
        cs.removeFailedShard(failedShard, "removeShardTopo1");

        candidateTopo = cs.getTopologyCandidate("removeShardTopo1").getTopology();

        runDeployRemoveFailedShard("planA", cs, "removeShardTopo1", numPlanRepeats,
                                   failedShard, false);
        logger.info("SecondDeploy: " +
                TopologyPrinter.printTopology(cs.getTopology(),
                                              cs.getParameters(),
                                              true));

        /* Remove contracted storage nodes */
        final StorageNodeId sn7Id = new StorageNodeId(7);
        final StorageNodeId sn8Id = new StorageNodeId(8);
        final StorageNodeId sn9Id = new StorageNodeId(9);

        removeStorageNode(sn7Id, cs);
        removeStorageNode(sn8Id, cs);
        removeStorageNode(sn9Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              1 /*numRGs*/, 3 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);

        /*
         * Use a longer timeout when checking the deployment here because
         * removing a failed shard can result in IllegalStateExceptions that
         * cause RNs to restart. This behavior is ugly, but ultimately the
         * store comes back up without warnings. It just might take longer.
         */
        DeployUtils.checkDeployment(cs, 3,
                                    3 * AWAIT_DEPLOYMENT_MS, /* long timeout */
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingRootDirectorySize.class);
    }

    private void doContractRemoveShard(int numPlanRepeats) throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of nine SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5, 6, 7, 8);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 9 SNs and 3 shards
         */
        String first9SNs = "first9SNs";
        AdminUtils.makeSNPool(cs, first9SNs,
                              snSet.getIds(0, 1, 2, 3, 4, 5, 6, 7, 8));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first9SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 9, 3, 9, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/, 0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Stop RN's corresponding to highest shard */
        RepNodeId rg3_rn1 = new RepNodeId(3,1);
        RepNodeId rg3_rn2 = new RepNodeId(3,2);
        RepNodeId rg3_rn3 = new RepNodeId(3,3);
        Set<RepNodeId> rnIds = new HashSet<RepNodeId>();
        rnIds.add(rg3_rn1);
        rnIds.add(rg3_rn2);
        rnIds.add(rg3_rn3);
        awaitPlanSuccess(cs, cs.createStopServicesPlan(null, rnIds),
                         /* Need force=true to stop the whole shard */
                         true /* force */);

        /* Remove three SNs, contract, remove 1 shard. */
        logger.info("testContract: Make a new SNPool for contract");
        /* First make a pool of 10 SNs */
        AdminUtils.makeSNPool(cs, "first6SNs", snSet.getIds(0, 1, 2, 3, 4, 5));
        cs.copyCurrentTopology("contract");
        cs.contractTopology("contract", "first6SNs");

        candidateTopo = cs.getTopologyCandidate("contract").getTopology();
        RepGroupId failedShard = new RepGroupId(3);
        runDeployRemoveFailedShard("planA", cs, "contract", numPlanRepeats,
                                   failedShard, false);

        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Remove contracted storage nodes */
        final StorageNodeId sn7Id = new StorageNodeId(7);
        final StorageNodeId sn8Id = new StorageNodeId(8);
        final StorageNodeId sn9Id = new StorageNodeId(9);

        removeStorageNode(sn7Id, cs);
        removeStorageNode(sn8Id, cs);
        removeStorageNode(sn9Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);
    }

    private void doDeployWithoutFailedShard(int numPlanRepeats) throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of nine SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1, 2, 3, 4, 5, 6, 7, 8);

        /*
         * Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 9 SNs and 3 shards
         */
        String first9SNs = "first9SNs";
        AdminUtils.makeSNPool(cs, first9SNs,
                              snSet.getIds(0, 1, 2, 3, 4, 5, 6, 7, 8));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, first9SNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 9, 3, 9, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 9 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/, 0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    9, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Remove 1 shard i.e. rg2 */
        logger.info("testRemoveShard: New topology name and FailedShard object");
        RepGroupId failedShard = new RepGroupId(2);
        cs.removeFailedShard(failedShard, "removeShardTopo");

        candidateTopo = cs.getTopologyCandidate("removeShardTopo").getTopology();
        runDeploy("planA", cs, "removeShardTopo", numPlanRepeats, false);
        logger.info("SecondDeploy: " +
                TopologyPrinter.printTopology(cs.getTopology(),
                                              cs.getParameters(),
                                              true));

        /* Remove contracted storage nodes */
        final StorageNodeId sn4Id = new StorageNodeId(4);
        final StorageNodeId sn5Id = new StorageNodeId(5);
        final StorageNodeId sn6Id = new StorageNodeId(6);

        removeStorageNode(sn4Id, cs);
        removeStorageNode(sn5Id, cs);
        removeStorageNode(sn6Id, cs);

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,  0 /*numANs*/,
                              numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);

        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    /**
     * Deploy a store with 3 shards, then add enough SNs to add another shard.
     * and then yet another.
     */
    @Test
    public void testRedistribute()
        throws Exception {

        doRedistribute(1);
    }

    @Test
    public void testRedistributeRepeat()
        throws Exception {
        doRedistribute(2);
    }

    private void doRedistribute(int numPlanRepeats)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 12 /* maxSNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);

        /* Deploy a total of six SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3, 4, 5);

        /* Set the period that an RN will wait between partition migrations
         * way down, to make the test run faster. This test will be invoking
         * parallel migrations.
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /*
         * The initial topology uses 6 SNs and 2 shards
         */
        String firstSixSNs = "firstSixSNs";
        AdminUtils.makeSNPool(cs, firstSixSNs, snSet.getIds(0, 1, 2, 3, 4, 5));
        String firstCand = "firstCandidate";
        int numPartitions = 50;
        cs.createTopology(firstCand, firstSixSNs, numPartitions, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(firstCand).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 6, 2, 6, numPartitions);
        runDeploy("first", cs, firstCand, numPlanRepeats, false);
        logger.info("FirstDeploy:" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));


        /* Check the number of shards and RNs on the deployed topology */
        Topology realizedTopo = cs.getTopology();
        DeployUtils.checkTopo(realizedTopo, 1 /* numDCs */, 6 /*numSNs*/,
                              2 /*numRGs*/,  6 /*numRNs*/, numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);


        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 300;
        Collection<PartitionId> allPIds =
            realizedTopo.getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * Deploy four more SNs, redistribute, expect 1 more shard.
         */
        logger.info("testRedistribute: Make a new SNPool for redistribution");
        /* First make a pool of 10 SNs */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     6, 7, 8, 9);
        AdminUtils.makeSNPool(cs, "firstTenSNs",
                              snSet.getIds(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        cs.copyCurrentTopology("redistribute");
        cs.redistributeTopology("redistribute", "firstTenSNs");

        candidateTopo = cs.getTopologyCandidate("redistribute").getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 10, 3, 9, numPartitions);

        runDeploy("planA", cs, "redistribute", numPlanRepeats, false);
        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Check the number of shards and RNs on the deployed topology */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 10 /*numSNs*/,
                  3 /*numRGs*/, 9 /*numRNs*/, numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    1, Validations.UnderCapacity.class,
                                    10, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * Add 2 more SNs, redistribute, expect 1 more shard, for a total of 4.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     10, 11);
        cs.copyCurrentTopology("redistributeAgain");
        cs.redistributeTopology("redistributeAgain",
                                Parameters.DEFAULT_POOL_NAME);

        candidateTopo =
            cs.getTopologyCandidate("redistributeAgain").getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 12, 4, 12, numPartitions);
        // TODO: reenable report generation when the GC issues are resolved.
        runDeploy("planB", cs, "redistributeAgain", numPlanRepeats, false);
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 12 /*numSNs*/,
                  4 /*numRGs*/, 12 /*numRNs*/, numPartitions);
        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    12, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    private void runDeploy(String planName,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport)
        throws RemoteException {

        runDeploy(planName, cs, candidateName, numPlanRepeats,
                  generateStatusReport, false /* force */);
    }

    private void runDeploy(String planName,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport,
                           boolean force)
        throws RemoteException {

        runDeploy(true, planName, cs, candidateName, numPlanRepeats,
                  generateStatusReport, null /* failedShard */, force);
    }

    private void runDeployRemoveFailedShard(String planName,
                                            CommandServiceAPI cs,
                                            String candidateName,
                                            int numPlanRepeats,
                                            RepGroupId failedShard,
                                            boolean generateStatusReport)
        throws RemoteException {

        runDeploy(true, planName, cs, candidateName, numPlanRepeats,
                  generateStatusReport, failedShard);
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
                           boolean generateStatusReport,
                           RepGroupId failedShard)
        throws RemoteException {

        runDeploy(expectChange, planName, cs, candidateName, numPlanRepeats,
                  generateStatusReport, failedShard, false /* force */);
    }

    private void runDeploy(boolean expectChange,
                           String planName,
                           CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport,
                           RepGroupId failedShard,
                           boolean force)
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
                                                          candidateName,
                                                          failedShard);
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
                String planStatus = cs.getPlanStatus(planNum,
                                        (StatusReport.SHOW_FINISHED_BIT |
                                         StatusReport.VERBOSE_BIT),
                                        false /* json */);
                //checking progress percent is in status report
                assertTrue(planStatus.contains("Progress percent:"));
                logger.info
                    ("Plan status report \n" + planStatus);
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

    /**
     * Do an initial deploy, a rebalance, and a redistribute with mount points.
     */
    @Test
    public void testMountPoints()
        throws Exception {

        doMountPoints(1);
    }

    @Test
    public void testMountPointsRepeat()
        throws Exception {

        doMountPoints(2);
    }

    private void doMountPoints(int numRepeats)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 String.valueOf(
                                                     3 * MB_PER_SN));
        /*
         * Deploy two more SNs on DataCenterA, for a total of 3 SNs with
         * capacity of 3 each.
         * On sn1 - no mount points, so 3 RNs in root
         * On sn2 - 3 mount points
         * On sn3 - 1 mount point, so 2 RNs in root
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_C", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", null, 2);

        /*
         * Make partition migrations run more frequently, for the sake of the
         * test.  Also enable statistics gathering to work around a problem
         * involving waiting for an up-to-date shard [#26468]
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        map.setParameter(ParameterState.RN_SG_ENABLED, "true");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 3 /*nSNs*/, 3, 9, 100);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, numRepeats, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    2, MultipleRNsInRoot.class,
                                    2, MissingStorageDirectorySize.class,
                                    1, MissingRootDirectorySize.class);

        /* Check that the mount points were created. */
        Parameters params = cs.getParameters();
        int numMPUsed = 0;
        for (RepNodeId rnId : cs.getTopology().getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            final File file = rnp.getStorageDirectoryFile();
            if (file != null) {
                File envHome = new File(file, rnId.toString());
                assertTrue(envHome.toString(), envHome.exists());
                logger.info("Checking that " + rnId + " exists on " + envHome);
                numMPUsed++;
            }
        }
        assertEquals(4, numMPUsed);

        /*
         * Load some data. Ensure that each partition has at least 2 records
         * Using data will serve to ensure that the RN and its notion of its
         * mount point match.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Change the capacity so the store is out of compliance */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);


        /*
         * We expect 3 complaints that SNs are over capacity and 2 warnings
         * that the SNs have more than 1 RN in the root dir.
         */
        DeployUtils.checkDeployment(cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    3, OverCapacity.class,
                                    2, MultipleRNsInRoot.class,
                                    2, MissingStorageDirectorySize.class,
                                    1, InsufficientAdmins.class,
                                    1, MissingRootDirectorySize.class);

        /*
         * Add 3 more SNs, 1 with a mount point,and try a rebalance.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(2 * MB_PER_SN), 3, 4, 5);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_A", null, 4);
        String rebalanceAttempt = "rebalance";
        cs.copyCurrentTopology(rebalanceAttempt);
        cs.rebalanceTopology(rebalanceAttempt, Parameters.DEFAULT_POOL_NAME,
                             null);

        runDeploy(rebalanceAttempt, cs, rebalanceAttempt, numRepeats, false);

        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    2, MultipleRNsInRoot.class,
                                    3, MissingStorageDirectorySize.class,
                                    3, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Run a redistribution, should add another shard */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 3, 4, 5);
        cs.copyCurrentTopology("redistribute");
        cs.redistributeTopology("redistribute", Parameters.DEFAULT_POOL_NAME);

        runDeploy("redistribute", cs, "redistribute", numRepeats, false);

        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 6 /*numSNs*/,
                              4 /*numRGs*/, 12 /*numRNs*/, 100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 3, AWAIT_DEPLOYMENT_MS, logger,
                                    1, InsufficientAdmins.class,
                                    4, MultipleRNsInRoot.class,
                                    3, MissingStorageDirectorySize.class,
                                    3, MissingRootDirectorySize.class);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    /**
     * Do an initial deploy, with mount point, rn log mount points.
     */
    @Test
    public void testRNLogMountPoints()
        throws Exception {

        doRNLogMountPoints(1);
    }

    private void doRNLogMountPoints(int numRepeats)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 String.valueOf(
                                                     3 * MB_PER_SN));
        /*
         * Deploy a total of 3 SNs with capacity of 3 each.
         * On sn1, sn2, sn3 - 3 mount points, 3 RN Log mount points.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_B", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_C", null, 0);

        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir1_A", null, 0);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir1_B", null, 0);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnlogDir1_C", null, 0);

        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_C", null, 1);

        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir2_A", null, 1);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir2_B", null, 1);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnlogDir2_C", null, 1);

        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", null, 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_B", null, 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_C", null, 2);

        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir3_A", null, 2);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir3_B", null, 2);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnlogDir3_C", null, 2);

        /*
         * Make partition migrations run more frequently, for the sake of the
         * test.  Also enable statistics gathering to work around a problem
         * involving waiting for an up-to-date shard [#26468]
         */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        map.setParameter(ParameterState.RN_SG_ENABLED, "true");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 3 /*nSNs*/, 3, 9, 100);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, numRepeats, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingStorageDirectorySize.class);

        /* Check that the mount points were created. */
        Parameters params = cs.getParameters();
        int numMPUsed = 0;
        for (RepNodeId rnId : cs.getTopology().getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            final File file = rnp.getStorageDirectoryFile();
            if (file != null) {
                File envHome = new File(file, rnId.toString());
                assertTrue(envHome.toString(), envHome.exists());
                logger.info("Checking that " + rnId + " exists on " + envHome);
                numMPUsed++;
            }
        }
        assertEquals(9, numMPUsed);

        /* Check that the rn log mount points were created. */
        params = cs.getParameters();
        numMPUsed = 0;
        for (RepNodeId rnId : cs.getTopology().getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            final File file = rnp.getLogDirectoryFile();
            if (file != null) {
                assertTrue(file.toString(), file.exists());
                logger.info("Checking that " + rnId + " has logs mounted on " + file);
                numMPUsed++;
            }
        }
        assertEquals(9, numMPUsed);
    }

    /**
     * Do an initial deploy, with rn log mount points.
     * Check if admin and rn je logs files are part of
     * kvroot log and rn log directory respectively.
     */
    @Test
    public void testJELogFilesWithMountPoints()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 String.valueOf(
                                                     3 * MB_PER_SN));
        /*
         * Deploy a total of 3 SNs with capacity of 3 each.
         * On sn1, sn2, sn3 - 3 mount points, 3 RN Log mount points.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_B", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_C", null, 0);

        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir1_A", null, 0);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir1_B", null, 0);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnlogDir1_C", null, 0);

        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_C", null, 1);

        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir2_A", null, 1);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir2_B", null, 1);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnlogDir2_C", null, 1);

        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", null, 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_B", null, 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_C", null, 2);

        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir3_A", null, 2);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnLogDir3_B", null, 2);
        snSet.changeRNLogMountPoint(cs, true, testdir + "rnlogDir3_C", null, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingStorageDirectorySize.class);

        /* Check that the rn log mount points has je log files. */
        Parameters params = cs.getParameters();
        int numMPUsed = 0;
        for (RepNodeId rnId : cs.getTopology().getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            final File file = rnp.getLogDirectoryFile();
            if (file != null) {
                /*
                 * Check rgx-rny.je prefix in rn log mount point
                 */
                String filePrefix = rnId.toString() + ".je";
                FileFilter prefixFilter = new PrefixFilter(filePrefix);
                File[] files = file.listFiles(prefixFilter);
                if(files.length > 0) {
                    logger.info("rn log mount point for " +
                        rnId + " has je logs mounted on " + file);
                    numMPUsed++;
                }
            }
        }
        assertEquals(9, numMPUsed);

        /* Check that the kvroot log directory has admin je log files. */
        int numAdminUsed = 0;
        for (StorageNodeId snId : cs.getTopology().getStorageNodeIds()) {
            StorageNodeParams snp = params.get(snId);
            String fileName = snp.getRootDirPath() + "/" + kvstoreName +
                              "/log";
            final File file = new File(fileName);
            if (file != null) {
                /*
                 * Check adminX.je prefix in kvroot log directory
                 */
                 String filePrefix = "admin" +
                     String.valueOf(snId.getStorageNodeId()) + ".je";
                 FileFilter prefixFilter = new PrefixFilter(filePrefix);
                 File[] files = file.listFiles(prefixFilter);
                 if(files.length > 0) {
                     logger.info("kvroot log directory for " +
                                 " has je logs mounted for admin" +
                                 String.valueOf(snId.getStorageNodeId()));
                     numAdminUsed++;
                 }
            }
        }
        assertEquals(1, numAdminUsed);
    }

    /**
     * Do an initial deploy, with-out rn log mount points.
     * Check if admin and rn je logs files are part of
     * kvroot log directory.
     */
    @Test
    public void testJELogWithoutMountPoints()
            throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 String.valueOf(
                                                     3 * MB_PER_SN));
        /*
         * Deploy a total of 3 SNs with capacity of 3 each.
         * On sn1, sn2, sn3 - 3 mount points, 3 RN Log mount points.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_B", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_C", null, 0);

        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_C", null, 1);

        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", null, 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_B", null, 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_C", null, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              3 /*numRGs*/, 9 /*numRNs*/, 100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingStorageDirectorySize.class);

        /* Check that the je log files are stored in kvroot log dir
         * for admin and rns */
        Parameters params = cs.getParameters();
        int numUsed = 0;
        int numAdminUsed = 0;
        for (StorageNodeId snId : cs.getTopology().getStorageNodeIds()) {
            StorageNodeParams snp = params.get(snId);
            String fileName = snp.getRootDirPath() + "/" + kvstoreName +
                              "/log";
            final File file = new File(fileName);
            if (file != null) {
                for (RepNodeId rnId :
                         cs.getTopology().getHostedRepNodeIds(snId)) {
                    /*
                     * Check rgx-rny.je prefix in kvroot log directory
                     */
                    String filePrefix = rnId.toString() + ".je";
                    FileFilter prefixFilter = new PrefixFilter(filePrefix);
                    File[] files = file.listFiles(prefixFilter);
                    if(files.length > 0) {
                        logger.info("kvroot log directory for " +
                                    " has je logs mounted for " + rnId);
                        numUsed++;
                    }
                }

                /*
                 * Check adminX.je prefix in kvroot log directory
                 */
                 String filePrefix = "admin" +
                     String.valueOf(snId.getStorageNodeId()) + ".je";
                 FileFilter prefixFilter = new PrefixFilter(filePrefix);
                 File[] files = file.listFiles(prefixFilter);
                 if(files.length > 0) {
                     logger.info("kvroot log directory for " +
                                 " has je logs mounted for admin" +
                                 String.valueOf(snId.getStorageNodeId()));
                     numAdminUsed++;
                 }
            }
        }
        assertEquals(9, numUsed);
        assertEquals(1, numAdminUsed);
    }

    class PrefixFilter implements FileFilter {
        String fileprefix;

        PrefixFilter(String fileprefix) {
            this.fileprefix = fileprefix;
        }

        @Override
        public boolean accept(File f) {
             return f.getName().startsWith(fileprefix);
        }
    }

    @Test
    public void testRebalanceSecondaryDC()
        throws Exception {

        doRebalanceSecondaryDC(1);
    }

    @Test
    public void testRebalanceSecondaryDCRepeat()
        throws Exception {

        doRebalanceSecondaryDC(2);
    }

    /** Test rebalancing a secondary data center. */
    private void doRebalanceSecondaryDC(final int numRepeats)
        throws Exception {

        /* Start admin, create primary DC with RF=1 */
        sysAdminInfo = bootstrapStore(kvstoreName, 9 /* maxSNs */,
                                      "dc1", 1 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final DatacenterId dc1 = sysAdminInfo.getDCId("dc1");
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet sns = sysAdminInfo.getSNSet();

        /* Create secondary DC with RF=1 */
        deployDatacenter(cs, "dc2", 1 /* RF */, DatacenterType.SECONDARY,
                         sysAdminInfo);
        final DatacenterId dc2 = sysAdminInfo.getDCId("dc2");

        /* Deploy 1 SN in each DC, each with capacity 2 */
        sns.deploy(cs, dc1, String.valueOf(2 * MB_PER_SN), 0);
        sns.deploy(cs, dc2, String.valueOf(2 * MB_PER_SN), 1);
        sns.changeParams(cs, COMMON_CAPACITY, "2", 0, 1);

        /* Deploy topology */
        cs.createTopology("topo1", DEFAULT_POOL_NAME, 100 /* partitions */,
                          false);
        runDeploy("deploy1", cs, "topo1", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 2 /* SNs */, 2 /* shards */,
                    4 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 2 /* RF */, AWAIT_DEPLOYMENT_MS, logger,
                        2, MultipleRNsInRoot.class,
                        1, InsufficientAdmins.class,
                        2, MissingRootDirectorySize.class);

        /*
         * Add an SN to each DC, reduce capacity of both DC's first SNs to 1,
         * and rebalance just the secondary DC.
         */
        sns.deploy(cs, dc1, MB_PER_SN_STRING, 2);
        sns.deploy(cs, dc2, MB_PER_SN_STRING, 3);
        sns.changeParams(cs, COMMON_CAPACITY, "1", 0, 1);
        cs.copyCurrentTopology("topo2");
        cs.rebalanceTopology("topo2", DEFAULT_POOL_NAME, dc2);
        /*
         * Specify force=true since we're not eliminating the OverCapacity
         * violation for sn1
         */
        runDeploy("deploy2", cs, "topo2", numRepeats,
                  true /* generateStatusReport */, true /* force */);
        checkTopo(cs.getTopology(), 2 /* DCs */, 4 /* SNs */, 2 /* shards */,
                   4 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 2 /* RF */, AWAIT_DEPLOYMENT_MS, logger,
                        1, MultipleRNsInRoot.class,
                        1, InsufficientAdmins.class,
                        /* From DC1, which was not rebalanced */
                        1, OverCapacity.class,
                        1, UnderCapacity.class,
                        4, MissingRootDirectorySize.class);
    }

    @Test
    public void testChangeRepFactorSecondaryDC()
        throws Exception {

        doChangeRepFactorSecondaryDC(1);
    }

    @Test
    public void testChangeRepFactorSecondaryDCRepeat()
        throws Exception {

        doChangeRepFactorSecondaryDC(2);
    }

    /** Test changing the replication factor of a secondary data center. */
    private void doChangeRepFactorSecondaryDC(final int numRepeats)
        throws Exception {

        /* Start admin, create primary DC with RF=1 */
        sysAdminInfo = bootstrapStore(kvstoreName, 9 /* maxSNs */,
                                      "dc1", 1 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final DatacenterId dc1 = sysAdminInfo.getDCId("dc1");
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet sns = sysAdminInfo.getSNSet();

        /* Create secondary DC with RF=1 */
        deployDatacenter(cs, "dc2", 1 /* RF */, DatacenterType.SECONDARY,
                         sysAdminInfo);
        final DatacenterId dc2 = sysAdminInfo.getDCId("dc2");

        /* Deploy 1 SN in each DC */
        sns.deploy(cs, dc1, MB_PER_SN_STRING, 0);
        sns.deploy(cs, dc2, MB_PER_SN_STRING, 1);
        AdminUtils.deployAdmin(cs, sns, 1);

        /* Deploy topology */
        cs.createTopology("topo1", DEFAULT_POOL_NAME, 100 /* partitions */,
                          false);
        runDeploy("deploy1", cs, "topo1", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 2 /* SNs */, 1 /* shards */,
                    2 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 2 /* RF */, 60000, logger,
                        2, MissingRootDirectorySize.class);

        /* Add an SN to the secondary DC, and change the DC's RF to 2 */
        sns.deploy(cs, dc2, MB_PER_SN_STRING, 2);
        AdminUtils.deployAdmin(cs, sns, 2);
        cs.copyCurrentTopology("topo2");
        cs.changeRepFactor("topo2", DEFAULT_POOL_NAME, dc2, 2);
        runDeploy("deploy2", cs, "topo2", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 3 /* SNs */, 1 /* shards */,
                   3 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 3 /* RF */, 60000, logger,
                        3, MissingRootDirectorySize.class);
    }

    @Test
    public void testReduceSecondaryRF()
        throws Exception {

        doReduceSecondaryRF(1);
    }

    @Test
    public void testReduceSecondaryRFRepeat()
        throws Exception {

        doReduceSecondaryRF(2);
    }

    /**
     * Test reducing the RF of a secondary zone, setting it to 0, and then
     * removing the zone. [#22995]
     */
    private void doReduceSecondaryRF(final int numRepeats)
        throws Exception {

        /* Start admin, create primary zone with RF=3 */
        sysAdminInfo = bootstrapStore(kvstoreName, 6 /* maxSNs */,
                                      "dc1", 3 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final DatacenterId dc1 = sysAdminInfo.getDCId("dc1");
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet sns = sysAdminInfo.getSNSet();

        /* Create secondary zone with RF=3 */
        deployDatacenter(cs, "dc2", 3 /* RF */, DatacenterType.SECONDARY,
                         sysAdminInfo);
        final DatacenterId dc2 = sysAdminInfo.getDCId("dc2");

        /* Deploy 3 SNs in each DC */
        sns.deploy(cs, dc1, MB_PER_SN_STRING, 0, 1, 2);
        sns.deploy(cs, dc2, MB_PER_SN_STRING, 3, 4, 5);

        /* Deploy an admin on each SN */
        for (int i = 1; i < 6; i++) {
            AdminUtils.deployAdmin(cs, sns, i);
        }

        /* Deploy topology */
        cs.createTopology("topo1", DEFAULT_POOL_NAME, 100 /* partitions */,
                          false);
        runDeploy("deploy1", cs, "topo1", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 6 /* SNs */, 1 /* shards */,
                    6 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 6 /* RF */, 60000, logger,
                        6, MissingRootDirectorySize.class);

        /* Change secondary zone's RF to 2 */
        cs.copyCurrentTopology("topo2");
        cs.changeRepFactor("topo2", DEFAULT_POOL_NAME, dc2, 2);
        runDeploy("deploy2", cs, "topo2", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 6 /* SNs */, 1 /* shards */,
                   5 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 5 /* RF */, 60000, logger,
                        6, MissingRootDirectorySize.class,
                        1, UnderCapacity.class,
                        1, ExcessAdmins.class);

        /* Change secondary zone's RF to 0 */
        cs.copyCurrentTopology("topo3");
        cs.changeRepFactor("topo3", DEFAULT_POOL_NAME, dc2, 0);
        runDeploy("deploy2", cs, "topo3", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 6 /* SNs */, 1 /* shards */,
                  3 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 3 /* RF */, 60000, logger,
                        6, MissingRootDirectorySize.class,
                        3, UnderCapacity.class,
                        1, ExcessAdmins.class);

        /* Remove admins in secondary zone */
        awaitPlanSuccess(cs, cs.createRemoveAdminPlan(null, dc2, null, false));
        checkTopo(cs.getTopology(), 2 /* DCs */, 6 /* SNs */, 1 /* shards */,
                   3 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 3 /* RF */, 60000, logger,
                        6, MissingRootDirectorySize.class,
                        3, UnderCapacity.class);

        /* Remove storage nodes in secondary zone */
        removeStorageNode(new StorageNodeId(4), cs);
        removeStorageNode(new StorageNodeId(5), cs);
        removeStorageNode(new StorageNodeId(6), cs);
        checkTopo(cs.getTopology(), 2 /* DCs */, 3 /* SNs */, 1 /* shards */,
                   3 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 3 /* RF */, 60000, logger,
                        3, MissingRootDirectorySize.class,
                        1, EmptyZone.class);

        /* Remove zone */
        AdminUtils.removeDatacenter(cs, "dc2", dc2, sysAdminInfo);
        checkTopo(cs.getTopology(), 1 /* DCs */, 3 /* SNs */, 1 /* shards */,
                   3 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 3 /* RF */, 60000, logger,
                        3, MissingRootDirectorySize.class);
    }

    /**
     * Tests to check whether the appropriate warning/notes message is
     * displayed when adding new SN with less storage directory sizes than
     * the current RN sizes.The reason is that when we generate a new
     * topology, all the storage directories that have a smaller size than
     * the shard minimum will be skipped when allocating new RNs. This occurs
     * in case of topology change repfactor, topology rebalance and topology
     * redistribute.
     */
    @Test
    public void testChangeRFWithAddingSNWithDiffDirectorySize()
        throws Exception {

        /* Deploy a store with 3 SN(capacity=1, Storage dirsize = 2gb) and RF=3.
         * When we change the RF to 5 after adding 2 SN with storage directory
         * size less than the initial size(say 1gb), error is thrown as the
         * newly added SN will be skipped because their size will be less than
         * the minimum size of the RN in the shard.
         */
        sysAdminInfo = bootstrapStore(kvstoreName, 5 /* maxSNs */,
                                      "dc1", 3 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final DatacenterId dc1 = sysAdminInfo.getDCId("dc1");
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("dc1"),
                     String.valueOf(MB_PER_SN), 0, 1, 2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
                         File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", "2_gb", 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", "2_gb", 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", "2_gb", 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, 1, false);

        checkTopo(cs.getTopology(), 1 /* DCs */, 3 /* SNs */, 1 /* shards */,
                  3 /* RNs */, 100 /* partitions */);
        snSet.deploy(cs, sysAdminInfo.getDCId("dc1"),
                     String.valueOf(MB_PER_SN), 3,4);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_A", "1_gb", 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir5_A", "1_gb", 4);

        String second = "secondCandidate";
        cs.createTopology(second, Parameters.DEFAULT_POOL_NAME, 100, false);
        String retMsg = cs.changeRepFactor(second, DEFAULT_POOL_NAME,
                                           dc1, 5);
        String msg = "Some new SN storage directory sizes are smaller" +
            " than the smallest existing storage directory size, which" +
            " may prevent new SNs from being used in the new topology.";
        assertTrue(retMsg.contains(msg));

        checkTopo(cs.getTopology(), 1 /* DCs */, 5 /* SNs */,
                  1 /* shards */, 3 /* RNs */, 100 /* partitions */);
    }

    @Test
    public void testRebalanceWithAddingSNWithDiffDirectorySize()
        throws Exception {

        /* Deploy a store with 3 SN(capacity=2, Storage dirsize = 2gb) and RF=3.
         * When we change the capacity of 1 SN(say SN1) to 1, after adding 1 SN
         * with storage directory size less than the initial size(say 1gb). We
         * perform a rebalance, error is thrown as the newly added SN will be
         * skipped in replacing the old RN because its size will be less than
         * the minimum size of the RN in the shard.
         */
        sysAdminInfo = bootstrapStore(kvstoreName, 4 /* maxSNs */,
                                      "dc1", 3 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final DatacenterId dc1 = sysAdminInfo.getDCId("dc1");
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("dc1"),
                     String.valueOf(MB_PER_SN), 0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1,2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
                         File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", "2_gb", 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_B", "2_gb", 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", "2_gb", 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", "2_gb", 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", "2_gb", 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_B", "2_gb", 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, 1, false);

        checkTopo(cs.getTopology(), 1 /* DCs */, 3 /* SNs */, 2 /* shards */,
                  6 /* RNs */, 100 /* partitions */);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0);
        snSet.deploy(cs, sysAdminInfo.getDCId("dc1"),
                     String.valueOf(MB_PER_SN), 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_A", "1_gb", 3);

        String second = "secondCandidate";
        cs.createTopology(second, Parameters.DEFAULT_POOL_NAME, 100, false);
        String retMsg = cs.rebalanceTopology(second,
                                             Parameters.DEFAULT_POOL_NAME, dc1);
        String msg = "Some new SN storage directory sizes are smaller" +
            " than the smallest existing storage directory size, which" +
            " may prevent new SNs from being used in the new topology.";
        assertTrue(retMsg.contains(msg));

        checkTopo(cs.getTopology(), 1 /* DCs */, 4 /* SNs */,
                  2 /* shards */, 6 /* RNs */, 100 /* partitions */);

    }

    @Test
    public void testRedistributeWithAddingSNWithDiffDirectorySize()
        throws Exception {

        /* Deploy a store with 1 DC and 2SN(capacity=1, Storage dirsize = 2gb)
         * and RF=2.
         * When we deploy another DC with 2 SN(cap =1 and size =1gb) and
         * perform a redistribute error is thrown as the newly added SN will
         * be skipped because its size will be less than the minimum size of
         * the RN in the shard.
         */
        sysAdminInfo = bootstrapStore(kvstoreName, 4 /* maxSNs */,
                                      "dc1", 2 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("dc1"),
                     String.valueOf(MB_PER_SN), 0, 1);

        String testdir = System.getProperty(TestUtils.DEST_DIR) +
                         File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", "2_gb", 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", "2_gb", 1);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, 1, false);

        checkTopo(cs.getTopology(), 1 /* DCs */, 2 /* SNs */, 1 /* shards */,
                  2 /* RNs */, 100 /* partitions */);

        deployDatacenter(cs, "dc2", 2 /* RF */, DatacenterType.PRIMARY,
                         sysAdminInfo);
        snSet.deploy(cs, sysAdminInfo.getDCId("dc2"),
                     String.valueOf(MB_PER_SN), 2, 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", "1_gb", 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_A", "1_gb", 3);

        String second = "secondCandidate";
        cs.createTopology(second, Parameters.DEFAULT_POOL_NAME, 100, false);
        String retMsg = cs.redistributeTopology(second,
                                                Parameters.DEFAULT_POOL_NAME);
        String msg = "Some new SN storage directory sizes are smaller" +
            " than the smallest existing storage directory size, which" +
            " may prevent new SNs from being used in the new topology.";
        assertTrue(retMsg.contains(msg));

        checkTopo(cs.getTopology(), 2 /* DCs */, 4 /* SNs */,
                  1 /* shards */, 2 /* RNs */, 100 /* partitions */);

    }

    @Test(timeout=1200000)
    public void testRedistributeSecondaryDC()
        throws Exception {

        doRedistributeSecondaryDC(1);
    }

    @Test
    public void testRedistributeSecondaryDCRepeat()
        throws Exception {

        doRedistributeSecondaryDC(2);
    }

    /** Test redistribute with a secondary data center. */
    private void doRedistributeSecondaryDC(final int numRepeats)
        throws Exception {

        /* Start admin, create primary DC with RF=1 */
        sysAdminInfo = bootstrapStore(kvstoreName, 9 /* maxSNs */, "dc1",
                                      1 /* RF */, true /* threads */,
                                      MB_PER_SN_STRING);
        final DatacenterId dc1 = sysAdminInfo.getDCId("dc1");
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();

        final SNSet sns = sysAdminInfo.getSNSet();

        /* Create secondary DC with RF=1 */
        deployDatacenter(cs, "dc2", 1 /* RF */, DatacenterType.SECONDARY,
                         sysAdminInfo);
        final DatacenterId dc2 = sysAdminInfo.getDCId("dc2");

        /* Deploy 1 SN in each DC */
        sns.deploy(cs, dc1, MB_PER_SN_STRING, 0);
        sns.deploy(cs, dc2, MB_PER_SN_STRING, 1);

        /* Deploy topology */
        cs.createTopology("topo1", DEFAULT_POOL_NAME, 100 /* partitions */,
                          false);
        runDeploy("deploy1", cs, "topo1", numRepeats, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 2 /* SNs */, 1 /* shards */,
                    2 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 2 /* RF */, AWAIT_DEPLOYMENT_MS, logger,
                        1, InsufficientAdmins.class,
                        2, MissingRootDirectorySize.class);

        /* Add an SN to each DC and redistribute */
        sns.deploy(cs, dc1, MB_PER_SN_STRING, 2);
        sns.deploy(cs, dc2, MB_PER_SN_STRING, 3);
        cs.copyCurrentTopology("topo2");
        cs.redistributeTopology("topo2", DEFAULT_POOL_NAME);
        runDeploy("deploy2", cs, "topo2", 1, true);
        checkTopo(cs.getTopology(), 2 /* DCs */, 4 /* SNs */, 2 /* shards */,
                   4 /* RNs */, 100 /* partitions */);
        checkDeployment(cs, 4 /* RF */, AWAIT_DEPLOYMENT_MS, logger,
                        1, InsufficientAdmins.class,
                        4, MissingRootDirectorySize.class);
    }

    @Test
    public void testStorageDirectorySize() throws Exception {
        final int NUM_PARTITIONS = 120;

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* max of 3 SNs */,
                                                 "DataCenterA",
                                                 2 /* repFactor*/,
                                                 false /* useThreads */,
                                                 String.valueOf(3 * MB_PER_SN),
                                                 true);

        /* Start with 2 SNs */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1);
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3, 4, 5);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 3, 4, 5);
        final String testdir = System.getProperty(TestUtils.DEST_DIR) +
                                                                File.separator;
        /* Set mount points with no sizes */
        /* SN1 will only have 2 mount points so one RN will go in root */
        snSet.changeMountPoint(cs, true, testdir + "storageDir0_A", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir0_B", null, 0);
        /* SN2 has mount points defined to meet capacity */
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_C", null, 1);

        snSet.changeMountPoint(cs, true, testdir + "storageDirA_A", null, 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDirA_B", null, 4);
        snSet.changeMountPoint(cs, true, testdir + "storageDirA_C", null, 5);

        /* Make partition migrations run more frequently. */
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_PM_WAIT_AFTER_BUSY, "1 s");
        cs.setPolicies(mergeParameterMapDefaults(map));

        /* Create and deploy initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME,
                          NUM_PARTITIONS, false);

        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 5 /*nSNs*/, 3 /*numRGs*/, 6 /*numRNs*/,
                              3 /*numANs*/, NUM_PARTITIONS);

        runDeploy("initialDeploy", cs, first, 1 /*numRepeats*/, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 5 /*numSNs*/,
                              3 /*numRGs*/, 6 /*numRNs*/,
                              3 /*numANs*/, NUM_PARTITIONS);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    5, MissingStorageDirectorySize.class);

        /* Add another SN with mount point sizes */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(3 * MB_PER_SN), 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", "10 MB", 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", "10 MB", 2);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_C", "10 MB", 2);

        snSet.changeMountPoint(cs, true, testdir + "storageDir2A_A", "5 MB", 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2A_B", "5 MB", 4);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2A_C", "5 MB", 5);

        cs.copyCurrentTopology("topo2");
        try {
            /* Should fail because not all mount points have sizes */
            cs.redistributeTopology("topo2", DEFAULT_POOL_NAME);
            assert(false);
        } catch (AdminFaultException afe) {
            /* Expected */
        }

        /* Add sizes to the rest of the mount points */
        snSet.changeMountPointSize(cs, testdir + "storageDir0_A", "20 MB", 0);
        snSet.changeMountPointSize(cs, testdir + "storageDir0_B", "20 MB", 0);
        snSet.changeMountPointSize(cs, testdir + "storageDir1_A", "30 MB", 1);
        snSet.changeMountPointSize(cs, testdir + "storageDir1_B", "30 MB", 1);
        snSet.changeMountPointSize(cs, testdir + "storageDir1_C", "30 MB", 1);
        snSet.changeMountPointSize(cs, testdir + "storageDirA_A", "5 MB", 3);
        snSet.changeMountPointSize(cs, testdir + "storageDirA_B", "5 MB", 4);
        snSet.changeMountPointSize(cs, testdir + "storageDirA_C", "5 MB", 5);

        cs.copyCurrentTopology("topo3");
        try {
            /* Should fail because RNs are in the root dir which doesn't have size */
            cs.redistributeTopology("topo3", DEFAULT_POOL_NAME);
            assert(false);
        } catch (AdminFaultException afe) {
            /* Expected */
        }

        /* Add sizes to root directories */
        snSet.changeParams(cs, ParameterState.SN_ROOT_DIR_SIZE, "31000000", 0);
        snSet.changeParams(cs, ParameterState.SN_ROOT_DIR_SIZE, "40000000", 1);
        cs.copyCurrentTopology("topo4");

        final int originalShards =
         cs.getTopologyCandidate("topo4").getTopology().getRepGroupIds().size();

        /*
         * This should not fail, but not change the topo because RNs can't be
         * moved to the new SN.
         */
        cs.redistributeTopology("topo4", DEFAULT_POOL_NAME);

        int newShards =
         cs.getTopologyCandidate("topo4").getTopology().getRepGroupIds().size();

        assertEquals(originalShards, newShards);

        runDeploy("deploy4", cs, "topo4", 1, true);

        /*
         * There should be two info warnings regarding size imbalance on
         * the shards. (The shards on sn1 root should be OK)
         */
        VerifyResults results = cs.verifyConfiguration(false, true, false);
        int imbalances = 0;
        for (Problem p : results.getWarnings()) {
            if (p instanceof StorageDirectorySizeImbalance) {
                imbalances++;
            }
        }
        assertEquals(results.display(), 2, imbalances);

        /* Up the sizes so that we can do a node swap */
        snSet.changeMountPointSize(cs, testdir + "storageDir2_A", "50 MB", 2);
        snSet.changeMountPointSize(cs, testdir + "storageDir2_B", "50 MB", 2);
        /*
         * Note that the swap should be made to this MP as it is the smallest
         * that will host the swapped node.
         */
        snSet.changeMountPointSize(cs, testdir + "storageDir2_C", "40 MB", 2);

        cs.copyCurrentTopology("topo5");
        cs.redistributeTopology("topo5", DEFAULT_POOL_NAME);

        /* If it worked we have a new shard */
        newShards =
         cs.getTopologyCandidate("topo5").getTopology().getRepGroupIds().size();

        assertEquals(originalShards + 1, newShards);
    }

    /**
     * Do an initial deploy with storagedir size less than 5 GB i.e 10 MB.
     * Check for verify configuration warnings notes and available Storage
     * information. This will suffice ping output check as well.
     */
    @Test
    public void testStoragedirPingVerifyOutput()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* max of 3 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING);
        /*
         * Deploy three SNs on DataCenterA, for a total of 3 SNs with
         * capacity of 1 each.
         * On sn1, sn2 and sn3 - 1 mount point each
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1, 2);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", "10_MB", 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", "10_MB", 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir3_A", "10_MB", 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);

        /*
         * Test that we can print a topology candidate with mount points. Since
         * this is actually testing the print implementation, make sure that
         * the call to print is outside the logger call.
         */
        DeployUtils.printCandidate(cs, first, logger);
        DeployUtils.checkTopo(cs.getTopologyCandidate(first).getTopology(),
                              1 /*dc*/, 3 /*nSNs*/, 1, 3, 100);

        /* Deploy initial topology */
        runDeploy("initialDeploy", cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              1 /*numRGs*/, 3 /*numRNs*/, 100 /*numParts*/);

        /*
         * check Deployment for AvailableStorageDirProblem warnings
         */
        DeployUtils.checkDeployment(cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    3, AvailableStorageLow.class);

        /* Verify available Storage through verify configuration output */
        VerifyResults results = cs.verifyConfiguration(true, true, true);
        logger.fine("Verify available Storage results:\n" + results.display());
        DeployUtils.checkVerifyAvailableStorageOutput(results,
                                                      cs.getTopology(),
                                                      cs.getParameters(),
                                                      0 /* violations*/,
                                                      3 /* warnings */);

        /* Check that the mount points were created. */
        Parameters params = cs.getParameters();
        int numMPUsed = 0;
        for (RepNodeId rnId : cs.getTopology().getRepNodeIds()) {
            RepNodeParams rnp = params.get(rnId);
            final File file = rnp.getStorageDirectoryFile();
            if (file != null) {
                File envHome = new File(file, rnId.toString());
                assertTrue(envHome.toString(), envHome.exists());
                logger.info("Checking that " + rnId + " exists on " + envHome);
                numMPUsed++;
            }
        }
        assertEquals(3, numMPUsed);

        /*
         * Load some data. Ensure that each partition has at least 2 records
         * Using data will serve to ensure that the RN and its notion of its
         * mount point match.
         *
         *
         * Insert records so that we hit out of disk limit exception and
         * then do check deployment for AvailableStorageDirProblem
         * violations.
         *
         * When started hitting out of disk limit, Master
         * will hit it first before replicas start hitting. Currently in
         * tests, we are getting violations on two nodes and warnings on one.
         * This is pretty much timing dependent. So currently 2 RN will
         * give violations and other 1 will give warnings.
         */
        try {
            su = new StoreUtils(kvstoreName,
                                AdminUtils.HOSTNAME,
                                snSet.getRegistryPort(0),
                                StoreUtils.RecordType.INT,
                                1 /* seed */);
            int expectedNumRecords = 50000000;
            Collection<PartitionId> allPIds =
                cs.getTopology().getPartitionMap().getAllIds();
            List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
            DeployUtils.checkContents(su, keys, expectedNumRecords);
        } catch (FaultException e) {
            /*
             * Ignore the out of disk limit fault exceptions caught
             * during insert
             */
        }

        /*
         * Check Deployment for AvailableStorageDirProblem violations
         *
         * We expect all three shards to have low storage or to exceed the
         * storage limit, but the number exceeding the limit may be 1, 2, or 3
         * shards depending on the timing. Accept any of these conditions.
         */
        DeployUtils.checkDeploymentWithRanges(
            cs, 3 /* rf */, AWAIT_DEPLOYMENT_MS, logger,
            4 /* minProblems */,
            4 /* maxProblems */,
            /* min, max, class */
            1, 1, InsufficientAdmins.class,
            1, 3, AvailableStorageExceeded.class,
            0, 2, AvailableStorageLow.class);

        /*
         * Verify available Storage through verify configuration output,
         * Value for available Storage will be -ve for one RN.
         */
        results = cs.verifyConfiguration(true, true, true);
        logger.fine("Verify available Storage results:\n" + results.display());
        DeployUtils.checkVerifyAvailableStorageOutput(results,
                                                      cs.getTopology(),
                                                      cs.getParameters(),
                                                      3 /* total */,
                                                      0 /* violationsMin */,
                                                      3 /* violationsMax */,
                                                      0 /* warningsMin */,
                                                      3 /* warningsMax */);

        /* Verify available storage warnings (KVSTORE-399) */
        assertTrue(results.display(),
                   Pattern.compile("Storage directory on " +
                                   "rg[0-9]*-rn[0-9]* " +
                                   "is running low .* size: " +
                                   "10 MB available: .*")
                          .matcher(results.display())
                          .find());
    }

    /**
     * Tests moving a single partition.
     */
    @Test
    public void testMovOnePartition()
        throws Exception {

        final int NUM_PARTITIONS = 10;

        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 2 /* max of 2 SNs */,
                                                 "DataCenterA",
                                                 1 /* repFactor*/,
                                                 false /* useThreads*/,
                                                 String.valueOf(
                                                     2 * MB_PER_SN));

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0, 1);

        /*
         * The initial topology uses 2 SNs and 2 shards
         */
        final String FIRST = "first";
        AdminUtils.makeSNPool(cs, FIRST, snSet.getIds(0, 1));
        final String FIRST_CAND = "firstCandidate";
        cs.createTopology(FIRST_CAND, FIRST, NUM_PARTITIONS, false);

        Topology candidateTopo =
            cs.getTopologyCandidate(FIRST_CAND).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 2, 2, 2, NUM_PARTITIONS);
        runDeploy(FIRST, cs, FIRST_CAND, 1, false);
        logger.info("Deploy " + FIRST_CAND + ":" +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));
        /* Verify initial partition layout */
        Topology topo = cs.getTopology();
        Set<Integer> partitions = topo.getPartitionsInShards(1, 1);
        assertTrue("Expected partition 1 to be in shard 1: " + partitions,
                   partitions.contains(1));
        assertEquals("Unexpected partition distribution in shard 1: " +
                     partitions, 5, partitions.size());
        partitions = topo.getPartitionsInShards(2, 2);
        assertEquals("Unexpected partition distribution in shard 2: " +
                     partitions, 5, partitions.size());

        final String MOVE_ONE = "moveOne";
        cs.copyCurrentTopology(MOVE_ONE);

        /* Try some incorrect noves first */
        try {
            /* Bad partition */
            cs.movePartition(MOVE_ONE, new PartitionId(100), new RepGroupId(2));
            fail("Expected IllegalCommandException");
        } catch (AdminFaultException afe) {
            /* Expected */
        }

        try {
            /* Bad group */
            cs.movePartition(MOVE_ONE, new PartitionId(1), new RepGroupId(200));
            fail("Expected IllegalCommandException");
        } catch (AdminFaultException afe) {
            /* Expected */
        }

        cs.movePartition(MOVE_ONE, new PartitionId(1), new RepGroupId(2));
        candidateTopo = cs.getTopologyCandidate(MOVE_ONE).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1, 2, 2, 2, NUM_PARTITIONS);

        runDeploy("planA", cs, "moveOne", 1, false);
        logger.info("SecondDeploy: " +
                    TopologyPrinter.printTopology(cs.getTopology(),
                                                  cs.getParameters(),
                                                  true));

        /* Verify that the move was made */
        topo = cs.getTopology();
        partitions = topo.getPartitionsInShards(1, 1);
        assertEquals("Wrong number of partitions in shard 1: " + partitions,
                     4, partitions.size());
        partitions = topo.getPartitionsInShards(2, 2);
        assertTrue("Expected partition 1 to be in shard 2: " + partitions,
                   partitions.contains(1));
        assertEquals("Wrong number of partitions in shard 2: " + partitions,
                     6, partitions.size());

    }

    /**
     * Tests that a client operation before the store is fully deployed throws
     * FaultException.
     *
     * [KVSTORE-2319].
     */
    @Test
    public void testClientOperationBeforeDeployment() throws Exception {
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 true /* useThreads*/,
                                                 String.valueOf(
                                                     2 * MB_PER_SN));
        /* Deploy two more SNs on DataCenterA */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"),
                     String.valueOf(2 * MB_PER_SN), 1, 2);
        /* Try connecting to kvstore */
        final KVStoreConfig config = new KVStoreConfig(kvstoreName,
            snSet.getHostname(0) + ":" + snSet.getRegistryPort(0));
        final KVStore kvstore = KVStoreFactory.getStore(config);
        try {
            kvstore.executeSync("CREATE TABLE Users" +
                    " (id INTEGER, firstName STRING, lastName STRING," +
                    " PRIMARY KEY (id))");
            fail("Client operation should fail");
        } catch (FaultException e) {
            logger.info("got exception: " + e);
            final String message = e.getMessage();
            assertTrue("Wrong message: " + message,
                message.contains("is not yet configured and deployed"));
        }
    }
}
