/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;

import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.topo.Validations.ANNotAllowedOnSN;
import oracle.kv.impl.admin.topo.Validations.ANWrongDC;
import oracle.kv.impl.admin.topo.Validations.ExcessANs;
import oracle.kv.impl.admin.topo.Validations.InsufficientANs;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.admin.topo.Validations.UnevenANDistribution;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Assert;
import org.junit.Test;

/* Increase test timeout to 30 minutes -- test can take 15 minutes */
@TestClassTimeoutMillis(30*60*1000)
public class ArbRebalanceAndRedistributeTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    private final String dcName1 = "DataCenterA";
    private final String dcName2 = "DatacenterB";
    private final String candName = "topo";
    private final String candName2 = "topo2";
    private final String poolName = Parameters.DEFAULT_POOL_NAME;
    private final String planName = "plan1";

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

    /**
     * Tests rebalance to add Arbiters to a topology
     * if an Arbiter only (Datacenter RF=0) zone is added.
     *
     * Deploy topo with total primary RF=2 but no AN
     * hosting DC. Add a RF=0 zone and SNs. Check for
     * InsufficientAN violation before rebalance
     * and insure that rebalance corrects this violation.
     */
    @Test
    public void testRebalanceNewANZone() throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 3 SNs (Capacity 1), 1 SN (Capacity 0) and Zone RF=2.
         * Primary Zone. Does not support Arbiters.
         * Initial topology created with primary RF=2 and no arbiters.
         * Deploy a new RF=0 zone with capacity 0 SN4 .
         * Rebalance the topology. On deploying the topology
         * the store is created with arbiter in SN4.
         */

        doDeployRF2(false /*Zone does not allow arbiters */);
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

        AdminUtils.deployDatacenter(cs,
                                    dcName2,
                                    0 /* RF */,
                                    DatacenterType.PRIMARY,
                                    sysAdminInfo,
                                    true);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 3);

        /*
         * Check for InsufficientAN violation before topology
         * deployment.
         */
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientANs.class,
                                    4, MissingRootDirectorySize.class);

        /* Rebalance the topology */
        doRebalance(2 /*numRNs*/,
                    1 /*numArbs*/,
                    4 /*numSNs*/,
                    2 /*numDCs*/,
                    1 /*numRGs*/,
                    2 /*rf*/);

        Topology topo = cs.getTopology();
        final int targetArbSNId = 4;

        /*
         * Check arbiter rg1-an1 has moved to SN4
         */
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            assertEquals(topo.get(arbNodeId).
                         getStorageNodeId().getStorageNodeId(),
                         targetArbSNId);
        }

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

    /**
     * Tests rebalance to delete Arbiters from a topology
     * if (Datacenter RF>0) zone is added.
     *
     * Deploy topo with total primary RF=2 hosting arbiters.
     * Add a RF>0 primary zone. Check for ExcessAN violation
     * before rebalance and insure that rebalance corrects
     * this violation. See [#24545]
     */
    @Test
    public void testRebalanceDeleteArbsRFNotTwo()
        throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs Capacity 1. 1 SN Capacity 0. Zone RF=2.
         * Primary Zone. Allows arbiters.
         * Initial topology created with primary RF=2 and has 1 arbiter.
         * Deploy a new RF=1 zone with capacity 1 SN (SN3).
         * Rebalance the topology. On deploying the topology
         * the PrimaryRF is now 3. Arbiter is removed from the shard.
         */

        doDeployRF2(true /*Zone allows arbiters */);
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

        AdminUtils.deployDatacenter(cs,
                                    dcName2,
                                    1 /* RF */,
                                    DatacenterType.PRIMARY,
                                    sysAdminInfo,
                                    true);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 3);
        AdminUtils.deployAdmin(cs, snSet, 3, AdminType.PRIMARY);
        /*
         * Check for ExcessAN violation before topology
         * deployment.
         */
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    1, ExcessANs.class,
                                    1, InsufficientRNs.class,
                                    1, UnderCapacity.class,
                                    4, MissingRootDirectorySize.class);

        /* Rebalance the topology */
        doRebalance(3 /*numRNs*/,
                    0 /*numArbs*/,
                    4 /*numSNs*/,
                    2 /*numDCs*/,
                    1 /*numRGs*/,
                    3 /*rf*/);

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

    /**
     * Tests rebalance to move arbiters from existing (RF>0)
     * primary zone to a new (RF=0) primary arbiter zone
     * that allows arbiters.
     *
     * Deploy topo with total primary RF=2 hosting arbiters.
     * Add a RF=0 zone. Check for ANDCHost violation
     * before rebalance and insure that rebalance corrects
     * this violation.
     */
    @Test
    public void testRebalanceMoveArbToNewANZone()
        throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs Capacity 1. 1 SN Capacity 0. Zone RF=2.
         * Primary Zone. Allows arbiters.
         * Initial topology created with primary RF=2 and has 1 arbiter.
         * Deploy a new RF=0 zone with capacity 0 SN (SN3).
         * Rebalance the topology. On deploying the topology
         * the arbiter is moved from exiting zone to RF=0 zone.
         */

        doDeployRF2(true /*Zone allows arbiters */);
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

        AdminUtils.deployDatacenter(cs,
                                    dcName2,
                                    0 /* RF */,
                                    DatacenterType.PRIMARY,
                                    sysAdminInfo,
                                    true);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 3);

        /*
         * Check for ANDCHost violation before topology deployment
         */
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, ANWrongDC.class,
                                    4, MissingRootDirectorySize.class);

        /* Rebalance the topology */
        doRebalance(2 /*numRNs*/,
                    1 /*numArbs*/,
                    4 /*numSNs*/,
                    2 /*numDCs*/,
                    1 /*numRGs*/,
                    2 /*rf*/);

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


    /**
     * Tests rebalance to remove arbiters from existing
     * primary zone when the zone has been altered to
     * not allow arbiters.
     *
     * Deploy topo with total primary RF=2 hosting arbiters.
     * Alter zone to not allow arbiters.
     * rebalance the topology
     * deploy new topology and check the AN was removed.
     */
    @Test
    public void testRebalanceChangeArbitersRemoveANs()
        throws Exception {

        doDeployRF2(true /*Zone allows arbiters */);
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

        doChangeArbitersAndRebalance(2 /*numRNs*/,
                                     0 /*numArbs*/,
                                     3 /*numSNs*/,
                                     1 /*numDCs*/,
                                     1 /*numRGs*/,
                                     2 /*rf*/);
    }

    /**
     * Tests rebalance with RN being moved from one SN to another. Topo
     * has Arbiters.
     */
    @Test
    public void testRebalanceMoveRNAN()
        throws Exception {
        doDeployIt();

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();

        Topology topo = cs.getTopology();
        Set<ArbNodeId> anIds = topo.getArbNodeIds();
        HashMap<StorageNodeId, Integer> countAnPerSn =
            new HashMap<StorageNodeId, Integer>();
        for (ArbNodeId anId : anIds) {
            StorageNodeId snId = topo.get(anId).getStorageNodeId();
            if (countAnPerSn.get(snId) == null) {
                countAnPerSn.put(snId,  1);
            } else {
                countAnPerSn.put(snId, countAnPerSn.get(snId) + 1);
            }
        }

        /*
         * This check is made to insure that the initial AN layout has an uneven
         * distribution. Later in the test, rebalance is used to
         * even the distribution out. This is a test sanity check to insure
         * that if in the future, we change things and the distribution is even,
         *  then this test no longer tests rebalance doing AN redistribution.
         */
        HashMap<Integer, Integer> anCounts = new HashMap<Integer, Integer>();
        anCounts.put(1, 1);
        anCounts.put(2, 2);
        for (Entry<StorageNodeId, Integer> entry : countAnPerSn.entrySet()) {
            anCounts.remove(entry.getValue());
        }
        assertTrue(anCounts.isEmpty());

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

        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING, 4);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 4);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING, 5);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 5);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 2);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    2, UnderCapacity.class,
                                    2, MultipleRNsInRoot.class,
                                    2, OverCapacity.class,
                                    1, UnevenANDistribution.class,
                                    6, MissingRootDirectorySize.class);

        /* Rebalance the topology */
        doRebalance(6 /*numRNs*/,
                    3 /*numArbs*/,
                    6 /*numSNs*/,
                    2 /*numDCs*/,
                    3 /*numRGs*/,
                    2 /*rf*/);

        /*
         * Check that rebalance redistributes the ANs.
         */
        topo = cs.getTopology();
        anIds = topo.getArbNodeIds();
        countAnPerSn = new HashMap<StorageNodeId, Integer>();
        for (ArbNodeId anId : anIds) {
            StorageNodeId snId = topo.get(anId).getStorageNodeId();
            if (countAnPerSn.get(snId) == null) {
                countAnPerSn.put(snId,  1);
            } else {
                countAnPerSn.put(snId, countAnPerSn.get(snId) + 1);
            }
        }
        for (Entry<StorageNodeId, Integer> entry : countAnPerSn.entrySet()) {
           assertTrue(entry.getValue() == 1);
        }

        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        allPIds = cs.getTopology().getPartitionMap().getAllIds();
        keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    /**
     * Tests rebalance to move arbiters from SN that does not
     * allow arbiters to another SN that allows arbiters.
     *
     * Deploy topo with total primary RF=2 hosting arbiters.
     * Change the arbiter hosting SNs parameter to not allow
     * arbiters. Check for ANSNHost violation
     * before rebalance and insure that rebalance corrects
     * this violation.
     */
    @Test
    public void testRebalanceMoveArbToANSN()
        throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs Capacity 1. 1 SN Capacity 0. Zone RF=2.
         * Primary Zone. Allows arbiters.
         * Initial topology created with primary RF=2 and has 1 arbiter
         * hosted in SN3. Change SN3 parameter to not allow arbiters.
         * Deploy a new SN (SN4) with capacity 0 and allows arbiters.
         * Rebalance the topology. On deploying the topology
         * the arbiter is moved from SN3 to SN4.
         */

        doDeployRF2(true /*Zone allows arbiters */);
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

        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 3);
        snSet.changeParams(cs, ParameterState.SN_ALLOW_ARBITERS, "false", 2);

        /*
         * Check for ANSNHost violation before topology deployment.
         */
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, ANNotAllowedOnSN.class,
                                    4, MissingRootDirectorySize.class);

        /* Rebalance the topology */
        doRebalance(2 /*numRNs*/,
                    1 /*numArbs*/,
                    4 /*numSNs*/,
                    1 /*numDCs*/,
                    1 /*numRGs*/,
                    2 /*rf*/);

        Topology topo = cs.getTopology();
        final int targetArbSNId = 4;

        /*
         * Check arbiter rg1-an1 has moved to SN4
         */
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            assertEquals(topo.get(arbNodeId).
                         getStorageNodeId().getStorageNodeId(),
                         targetArbSNId);
        }
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

    /**
     * Tests redistribute to create new Arbiters after redistributing
     * the topology.
     *
     * Deploy topo with primary RF=2 hosting arbiters.
     * Add more capacity to the store and redistribute
     * the topology. A new shard is created with arbiter.
     */
    @Test
    public void testRedistributeCreateNewArbs()
        throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs Capacity 1. 1 SN Capacity 0. Zone RF=2.
         * Primary Zone. Allows arbiters.
         * Topology deployed with 1 shard. RNs in SN1 and SN2.
         * Arbiter in SN3.
         * Deploy one more SN (SN4 Capacity 1).
         * Change the capacity of SN3 to 1. Redistribute the topology.
         * A new shard is created with RNs in SN3 and SN4.
         * New arbiter for second shard created in SN1.
         */

        doDeployRF2(true /*Zone allows arbiters */);
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

        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 3);

        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    2, UnderCapacity.class,
                                    4, MissingRootDirectorySize.class);

        /* Redistribute the topology */
        doRedistribute(4 /*numRNs*/,
                       2 /*numArbs*/,
                       4 /*numSNs*/,
                       1 /*numDCs*/,
                       2 /*numRGs*/);
        DeployUtils.checkDeployment(cs, 2 /* rf */, 5000, logger,
                                    4, MissingRootDirectorySize.class);

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
    public void testRebalanceMoveANSN()
        throws Exception {
        doDeployOneDCRF2(true);
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

        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1, 3);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    2, MultipleRNsInRoot.class,
                                    2, UnderCapacity.class,
                                    2, OverCapacity.class,
                                    1, UnevenANDistribution.class,
                                    4, MissingRootDirectorySize.class);

        /* Rebalance the topology */
        doRebalance(4 /*numRNs*/,
                    2 /*numArbs*/,
                    4 /*numSNs*/,
                    1 /*numDCs*/,
                    2 /*numRGs*/,
                    2);
        DeployUtils.checkDeployment(cs, 2 /* rf */, 5000, logger,
                                    4, MissingRootDirectorySize.class);

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

    /**
     * Tests redistribute to correct arbiter and RN
     * proximity problems.
     *
     * Deploy topo with primary RF=2 hosting arbiters.
     * Increase the capacity of the SN currently hosting
     * arbiter. Redistribute the topology to create a new
     * shard and also correct the arbiter and RN proximity.
     */
    @Test
    public void testRedistributeResolveANProximity()
        throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs Capacity 1. 1 SN Capacity 0. Zone RF=2.
         * Primary Zone. Allows arbiters.
         * Topology deployed with 1 shard. RNs in SN1 and SN2.
         * Arbiter in SN3.
         * Change the capacity of SN3 to 2 and redistribute the
         * topology.
         *
         * Resultant topology:
         * Shard1: rg1-rn1 in SN3, rg1-rn2 in SN2, rg1-an1 in SN1.
         * Shard2: rg2-rn1 in SN3, rg2-rn2 in SN1, rg2-an1 in SN2.
         */

        doDeployRF2(true /*Zone allows arbiters */);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();

        /*
         * Do sanity check there is only one AN.
         */
        Topology topo = cs.getTopology();
        Set<ArbNodeId> anIds = topo.getArbNodeIds();
        Assert.assertTrue(anIds.size() == 1);

        /*
         * Find index of SN that hosts the original AN.
         */
        int targSNOffset = -1;
        for (ArbNodeId anId : anIds) {
            ArbNode an = topo.get(anId);
            targSNOffset = snSet.getIndex(an.getStorageNodeId());
        }

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

        snSet.changeParams(cs, ParameterState.COMMON_MEMORY_MB,
                           String.valueOf(2 * MB_PER_SN),
                           targSNOffset);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY,
                           "2", targSNOffset);

        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, UnderCapacity.class,
                                    3, MissingRootDirectorySize.class);

        /* Redistribute the topology */
        doRedistribute(4 /*numRNs*/,
                       2 /*numArbs*/,
                       3 /*numSNs*/,
                       1 /*numDCs*/,
                       2 /*numRGs*/);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    1, MultipleRNsInRoot.class,
                                    3, MissingRootDirectorySize.class);

        snSet = sysAdminInfo.getSNSet();

        /* Make sure AN was moved off original SN */
        topo = cs.getTopology();
        StorageNode sn = topo.get(snSet.getId(targSNOffset));
        StorageNodeId snId = sn.getResourceId();
        anIds = topo.getArbNodeIds();
        for (ArbNodeId anId : anIds) {
            ArbNode an = topo.get(anId);
            Assert.assertFalse(snId.equals(an.getStorageNodeId()));
        }

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


    /**
     * Tests rebalance after capacity changes.
     *
     * Deploy topo with primary RF=2 hosting arbiters.
     * Decrease the capacity of the SNs.
     * Rebalance topology
     * deploy rebalanced topology.
     * This test created for [#26917]
     */
    @Test
    public void testRebalanceChangeCapacity()
        throws Exception {

        doDeploy2();
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();

        for (int i = 0 ; i < 4; i++)
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY,
                           "1", i);

        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    2, OverCapacity.class,
                                    4, MissingRootDirectorySize.class,
                                    2, UnderCapacity.class,
                                    2, MultipleRNsInRoot.class);


        /* Rebalance the topology */
        doRebalance(4 /*numRNs*/,
                    2 /*numArbs*/,
                    4 /*numSNs*/,
                    1 /*numDCs*/,
                    2 /*numRGs*/,
                    2 /*rf*/);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    5000,
                                    logger,
                                    4, MissingRootDirectorySize.class);
    }

    private void doRedistribute(int numRNs,
                                int numArbs,
                                int numSNs,
                                int numDCs,
                                int numRGs)
        throws Exception{

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        cs.copyCurrentTopology(candName2);
        cs.redistributeTopology(candName2, poolName);
        /* Deploy initial topology */
        runDeploy(cs, candName2, 1, false);
        /*
         * Assert that the deployed topology has the number of
         * shards, ANs and RNs that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(),
                              numDCs,
                              numSNs,
                              numRGs,
                              numRNs,
                              numArbs,
                              100 /*numParts*/);
    }

    private void doRebalance(int numRNs,
                             int numArbs,
                             int numSNs,
                             int numDCs,
                             int numRGs,
                             int rf)
        throws Exception{

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        cs.copyCurrentTopology(candName2);
        cs.rebalanceTopology(candName2, poolName, null);
        /* Deploy initial topology */
        runDeploy(cs, candName2, 1, false);
        /*
         * Assert that the deployed topology has the number of
         * shards, ANs and RNs that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(),
                              numDCs,
                              numSNs,
                              numRGs,
                              numRNs,
                              numArbs,
                              100 /*numParts*/);
        DeployUtils.checkDeployment(cs, rf, 5000, logger,
                                    numSNs, MissingRootDirectorySize.class);
    }


    private void doChangeArbitersAndRebalance(int numRNs,
                                              int numArbs,
                                              int numSNs,
                                              int numDCs,
                                              int numRGs,
                                              int rf)
        throws Exception{

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        cs.copyCurrentTopology(candName2);
        Topology topo = cs.getTopologyCandidate(candName2).getTopology();
        cs.changeZoneArbiters(candName2,
                              topo.getDatacenter(dcName1).getResourceId(),
                              false /* allowArbiters */);

        cs.rebalanceTopology(candName2, poolName, null);
        /* Deploy initial topology */
        runDeploy(cs, candName2, 1, false);

        /*
         * Assert that the deployed topology has the number of
         * shards, ANs and RNs that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(),
                              numDCs,
                              numSNs,
                              numRGs,
                              numRNs,
                              numArbs,
                              100 /*numParts*/);
        DeployUtils.checkDeployment(cs, rf, 5000, logger,
                                    numSNs, MissingRootDirectorySize.class);
    }

    /**
     *  Creates and deploys a topology.
     *  Datacenter <dcname1> RF=2
     *  SN1 capacity 1
     *  SN2 capacity 1
     *  SN3 capacity 0
     *
     *  Admin on SN1
     */
    private void doDeployRF2(boolean allowsArbiter)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      6 /* SNs */,
                                      dcName1,
                                      2 /* repFactor*/,
                                      false /* useThreads */,
                                      MB_PER_SN_STRING /* memory */,
                                      allowsArbiter);
        /*
         * Deploy one more SN on DataCenterA, for a total of 2 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        /*
         * Wait for the store version to be updated to avoid problems
         * with partition migration.
         */
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 2);
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);

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
                              1 /*dc*/,
                              3 /*nSNs*/,
                              1 /*nRG*/,
                              2/*nRN*/,
                              allowsArbiter ? 1 : 0/*nARBBs*/,
                              100);

        /* Deploy initial topology */
        runDeploy(cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              3 /*numSNs*/,
                              1 /*numRGs*/,
                              2 /*numRNs*/,
                              allowsArbiter ? 1 : 0/*nARBs*/,
                              100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 2 /* rf */, 5000, logger,
                                    3, MissingRootDirectorySize.class);
    }

    /*
     * This sets up and deploys a topology.
     * Datacenter <dcname1> RF=1
     * SN1 capacity 2
     * SN2 capacity 1
     * Datacenter <dcname2> RF=1
     * SN3 capacity 2
     * SN4 capacity 1
     */
    private void doDeployIt()
            throws Exception {

            /* Bootstrap the Admin, the first DC, and the first SN */
            sysAdminInfo =
                AdminUtils.bootstrapStore(kvstoreName,
                                          6 /* SNs */,
                                          dcName1,
                                          1 /* repFactor*/,
                                          false /* useThreads */,
                                          String.valueOf(
                                              2 * MB_PER_SN) /* memory */,
                                          true);
            CommandServiceAPI cs = sysAdminInfo.getCommandService();
            SNSet snSet = sysAdminInfo.getSNSet();
            snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0);
            snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING,
                         1);
            snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 1);


            AdminUtils.deployDatacenter(cs,
                    dcName2,
                    1 /* RF */,
                    DatacenterType.PRIMARY,
                    sysAdminInfo,
                    true);
            snSet.deploy(cs, sysAdminInfo.getDCId(dcName2),
                         String.valueOf(2 * MB_PER_SN), 2);
            snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 2);
            snSet.deploy(cs, sysAdminInfo.getDCId(dcName2), MB_PER_SN_STRING,
                         3);
            snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 3);
            AdminUtils.deployAdmin(cs, snSet, 2, AdminType.PRIMARY);

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
                                  2 /*dc*/,
                                  4 /*nSNs*/,
                                  3 /*nRG*/,
                                  6/*nRN*/,
                                  3 /*nARBBs*/,
                                  100);

            /* Deploy initial topology */
            runDeploy(cs, first, 1, false);

            /*
             * Assert that the deployed topology has the number of shards, RNs
             * and ANs that are expected.
             */
            DeployUtils.checkTopo(cs.getTopology(),
                                  2 /* numDCs */,
                                  4 /*numSNs*/,
                                  3 /*numRGs*/,
                                  6 /*numRNs*/,
                                  3 /*nARBs*/,
                                  100 /*numParts*/);

            DeployUtils.checkDeployment(cs, 2 /* rf */,
                                        5000,
                                        logger,
                                        2, MultipleRNsInRoot.class,
                                        4, MissingRootDirectorySize.class);
    }

    private void runDeploy(CommandServiceAPI cs,
                           String candidateName,
                           int numPlanRepeats,
                           boolean generateStatusReport)
        throws RemoteException {

        runDeploy(true, cs, candidateName, numPlanRepeats,
                  generateStatusReport);
    }

    /**
     *  Creates and deploys a topology.
     *  Datacenter <dcname1> RF=2
     *  SN1 capacity 2
     *  SN2 capacity 2
     *  SN3 capacity 1
     *  Admin on SN1
     *  Admin on SN2
     *
     */
    private void doDeployOneDCRF2(boolean allowsArbiter)
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      6 /* SNs */,
                                      dcName1,
                                      2 /* repFactor*/,
                                      false /* useThreads */,
                                      String.valueOf(
                                          2 * MB_PER_SN) /* memory */,
                                      allowsArbiter);
        /*
         * Deploy one more SN on DataCenterA, for a total of 2 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1),
                     String.valueOf(2 * MB_PER_SN), 1);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING, 2);

        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 2);
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);

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
                              1 /*dc*/,
                              3 /*nSNs*/,
                              2 /*nRG*/,
                              4/*nRN*/,
                              allowsArbiter ? 2 : 0/*nARBBs*/,
                              100);

        /* Deploy initial topology */
        runDeploy(cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              3 /*numSNs*/,
                              2 /*numRGs*/,
                              4 /*numRNs*/,
                              allowsArbiter ? 2 : 0/*nARBs*/,
                              100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 2 /* rf */,
                5000,
                logger,
                2, MultipleRNsInRoot.class,
                1, UnderCapacity.class,
                1, UnevenANDistribution.class,
                3, MissingRootDirectorySize.class);
    }

    /**
     * @param generateStatusReport if true, create a thread that will generate
     * a status report while the plan is running, hoping to exercise the
     * reporting module in a realistic way.
     */
    private void runDeploy(boolean expectChange,
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
                                                false));
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
     *  Creates and deploys a topology.
     *  Datacenter <dcname1> RF=2
     *  SN1 capacity 2
     *  SN2 capacity 2
     *  SN3 capacity 0
     *  SN4 capacity 0
     *
     *  Admin on SN1
     */
    private void doDeploy2()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      4 /* SNs */,
                                      dcName1,
                                      2 /* repFactor*/,
                                      false /* useThreads */,
                                      String.valueOf(
                                          2 * MB_PER_SN) /* memory */,
                                      true /* allowsArbiter */);
        /*
         * Deploy one more SN on DataCenterA, for a total of 4 SNs with
         * capacity of 2 each.
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1),
                     String.valueOf(2 * MB_PER_SN), 1);
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName1), MB_PER_SN_STRING,
                     2, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1 );
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 2, 3 );
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);

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
                              1 /*dc*/,
                              4 /*nSNs*/,
                              2 /*nRG*/,
                              4/*nRN*/,
                              2 /*nARBBs*/,
                              100);

        /* Deploy initial topology */
        runDeploy(cs, first, 1, false);

        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              4 /*numSNs*/,
                              2 /*numRGs*/,
                              4 /*numRNs*/,
                              2/*nARBs*/,
                              100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 2 /* rf */, 5000, logger,
                                    4, MissingRootDirectorySize.class,
                                    2, MultipleRNsInRoot.class);
    }
}
