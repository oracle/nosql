/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.topo;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.topo.Rules.CapacityProfile;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.Validations.BadAdmin;
import oracle.kv.impl.admin.topo.Validations.ExcessAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MissingStorageDirectorySize;
import oracle.kv.impl.admin.topo.Validations.NoPrimaryDC;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.RNProximity;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.admin.topo.Validations.StorageNodeMissing;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.admin.topo.Validations.WrongNodeType;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.NodeType;

import org.junit.Test;

/**
 * Test topology creation, redistribution and rebalancing. This test is focused
 * solely on the TopologyBuilder, and topologies are created and verified
 * without being deployed.
 * TODO: add specific test cases for BDA and NTT
 * TODO: add specific test case for redistribution where no shards are added,
 * but partitions are migrated
 */
public class TopologyBuilderTest extends TestBase {

    private static boolean VERBOSE = Boolean.getBoolean("verbose");
    private static final String CANDIDATE_NAME = "candidate";
    private static final int SN_PORT = 5001;

    private AtomicInteger snCounter;
    private AdminTestConfig atc;
    private Parameters params;
    private Topology sourceTopo;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        snCounter = new AtomicInteger(0);
        atc = new AdminTestConfig(kvstoreName);
        params = new Parameters(kvstoreName);
        sourceTopo = new Topology(kvstoreName);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testContractRemoveSNInEachZoneWithArbiter() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 1,
                                     DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
        CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.PRIMARY, false);
        addSNs(dc2, sourceTopo, params, snCounter, 3,
        CapGenType.SAME.newInstance(1));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 3001, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("ANs", 3, topo.getArbNodeIds().size());
        assertEquals("Partitions", 3001, topo.getPartitionMap().size());

        TopologyPrinter.printTopology(topo);

        pool = makePool(topo);
        pool.remove(new StorageNodeId(2));
        pool.remove(new StorageNodeId(4));

        candidate = new TopologyCandidate("contract", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());

        TopologyCandidate contracted = tb.contract();

        Topology contractedTopo = contracted.getTopology();

        assertEquals("Shards", 2, contractedTopo.getRepGroupMap().size());
        assertEquals("SNs", 6, contractedTopo.getStorageNodeMap().size());
        assertEquals("RNs", 4, contractedTopo.getRepNodeIds().size());
        assertEquals("RNs", 3001, contractedTopo.getPartitionMap().size());
        assertEquals("ANs", 2, contractedTopo.getArbNodeIds().size());
    }

    /**
     * Make a handcrafted, noncompliant topology, and contract it.
     */
    @Test
    public void testContractRemoveShard() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       9, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 9, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());
        assertEquals("RNs", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(7));
        pool.remove(new StorageNodeId(8));
        pool.remove(new StorageNodeId(9));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        TopologyCandidate contracted = tb.contract();

        Topology contractedTopo = contracted.getTopology();

        assertEquals("Shards", 2, contractedTopo.getRepGroupMap().size());
        assertEquals("SNs", 9, contractedTopo.getStorageNodeMap().size());
        assertEquals("RNs", 6, contractedTopo.getRepNodeIds().size());
        assertEquals("RNs", 1000, contractedTopo.getPartitionMap().size());
        assertEquals("ANs", 0, contractedTopo.getArbNodeIds().size());
    }

    @Test
    public void testContractRemoveRandomSNs() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       9, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 9, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());
        assertEquals("RNs", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(6));
        pool.remove(new StorageNodeId(9));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        TopologyCandidate contracted = tb.contract();

        Topology contractedTopo = contracted.getTopology();

        assertEquals("Shards", 2, contractedTopo.getRepGroupMap().size());
        assertEquals("SNs", 9, contractedTopo.getStorageNodeMap().size());
        assertEquals("RNs", 6, contractedTopo.getRepNodeIds().size());
        assertEquals("RNs", 1000, contractedTopo.getPartitionMap().size());
        assertEquals("ANs", 0, contractedTopo.getArbNodeIds().size());
    }

    @Test
    public void testContractRemoveShardWithArbiter() {
        int repFactor = 2;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor,
                              DatacenterType.PRIMARY, true);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("RNs", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 3, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(5));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        TopologyCandidate contracted = tb.contract();

        Topology contractedTopo = contracted.getTopology();

        assertEquals("Shards", 2, contractedTopo.getRepGroupMap().size());
        assertEquals("SNs", 6, contractedTopo.getStorageNodeMap().size());
        assertEquals("RNs", 4, contractedTopo.getRepNodeIds().size());
        assertEquals("RNs", 1000, contractedTopo.getPartitionMap().size());
        assertEquals("ANs", 2, contractedTopo.getArbNodeIds().size());
    }

    @Test
    public void testContractWithAddedExtraSNs() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with two shards, 6 RNs, 6 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        addSNs(dc, topo, params, snCounter, 3, CapGenType.SAME.newInstance(1));

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(5));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        Exception cause = null;
        try {
            tb.contract();
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("The storage pool provided for topology candidate " +
                     "contract should not contain the following SNs which " +
                     "are not in the current topology: [sn7, sn8, sn9]",
                     cause.getMessage());
    }

    @Test
    public void testContractRemoveRandomSNsWithArbiter() {
        int repFactor = 2;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor,
                              DatacenterType.PRIMARY, true);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("RNs", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 3, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        TopologyCandidate contracted = tb.contract();

        Topology contractedTopo = contracted.getTopology();

        assertEquals("Shards", 2, contractedTopo.getRepGroupMap().size());
        assertEquals("SNs", 6, contractedTopo.getStorageNodeMap().size());
        assertEquals("RNs", 4, contractedTopo.getRepNodeIds().size());
        assertEquals("RNs", 1000, contractedTopo.getPartitionMap().size());
        assertEquals("ANs", 2, contractedTopo.getArbNodeIds().size());
    }

    @Test
    public void testContractRemoveRandomSNsRF2() {
        int repFactor = 2;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 6 RNs, 6 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());

        Exception cause = null;
        try {
            tb.contract();
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Cannot relocate RN when the repfactor is 2 and arbiter" +
                     " is disabled", cause.getMessage());
    }

    @Test
    public void testContractRemoveRandomSNsPrimaryRF2() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 2,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 4,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        TopologyPrinter.printTopology(topo);

        pool = makePool(topo);
        pool.remove(new StorageNodeId(2));
        pool.remove(new StorageNodeId(4));

        candidate = new TopologyCandidate("contract", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());

        Exception cause = null;
        try {
            tb.contract();
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Cannot relocate RN when the repfactor is 2 and arbiter" +
                     " is disabled", cause.getMessage());
    }

    @Test
    public void testContractRemoveRandomSNsRF1() {
        int repFactor = 1;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 6 RNs, 6 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 6, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("contract", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());

        Exception cause = null;
        try {
            tb.contract();
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Cannot relocate RN when the repfactor is 1",
                     cause.getMessage());
    }

    @Test
    public void testContractRemoveZone() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 2,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 4,
        CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
        CapGenType.SAME.newInstance(1));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        TopologyPrinter.printTopology(topo);

        pool = makePool(topo);
        pool.remove(new StorageNodeId(5));
        pool.remove(new StorageNodeId(6));

        candidate = new TopologyCandidate("contract", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());

        Exception cause = null;
        try {
            tb.contract();
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Insufficient storage nodes to support current zones.",
                     cause.getMessage());
    }

    /**
     * Make a handcrafted topology, and remove shard from it.
     */
    @Test
    public void testRemoveFailedShard() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       9, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 9, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(5));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("remove", topo);
        TopologyBuilder tb = new TopologyBuilder(candidate, pool, params,
                                                 atc.getParams());
        RepGroupId failedShard = RepGroupId.parse("rg2");
        TopologyCandidate removedShardcand = tb.removeFailedShard(failedShard);
        Topology removedShardTopo = removedShardcand.getTopology();

        assertEquals("Shards", 2, removedShardTopo.getRepGroupMap().size());
        assertEquals("SNs", 9, removedShardTopo.getStorageNodeMap().size());
        assertEquals("RNs", 6, removedShardTopo.getRepNodeIds().size());
        assertEquals("Partitions", 1000,
                     removedShardTopo.getPartitionMap().size());
        assertEquals("ANs", 0, removedShardTopo.getArbNodeIds().size());
    }

    @Test
    public void testRemoveHighestFailedShard() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       9, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 9, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(7));
        pool.remove(new StorageNodeId(8));
        pool.remove(new StorageNodeId(9));

        TopologyCandidate candidate = new TopologyCandidate("remove", topo);
        TopologyBuilder tb = new TopologyBuilder(candidate, pool, params,
                                                 atc.getParams());
        RepGroupId failedShard = RepGroupId.parse("rg3");
        TopologyCandidate removedShardcand = tb.removeFailedShard(failedShard);
        Topology removedShardTopo = removedShardcand.getTopology();

        assertEquals("Shards", 2, removedShardTopo.getRepGroupMap().size());
        assertEquals("SNs", 9, removedShardTopo.getStorageNodeMap().size());
        assertEquals("RNs", 6, removedShardTopo.getRepNodeIds().size());
        assertEquals("Partitions", 1000,
                     removedShardTopo.getPartitionMap().size());
        assertEquals("ANs", 0, removedShardTopo.getArbNodeIds().size());
    }

    @Test
    public void testRemoveFailedShardWithAdmin() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       9, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 9, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(1));
        pool.remove(new StorageNodeId(2));
        pool.remove(new StorageNodeId(3));

        TopologyCandidate candidate = new TopologyCandidate("remove", topo);
        TopologyBuilder tb = new TopologyBuilder(candidate, pool, params,
                                                 atc.getParams());
        RepGroupId failedShard = RepGroupId.parse("rg1");
        TopologyCandidate removedShardcand = tb.removeFailedShard(failedShard);
        Topology removedShardTopo = removedShardcand.getTopology();

        assertEquals("Shards", 2, removedShardTopo.getRepGroupMap().size());
        assertEquals("SNs", 9, removedShardTopo.getStorageNodeMap().size());
        assertEquals("RNs", 6, removedShardTopo.getRepNodeIds().size());
        assertEquals("Partitions", 1000,
                     removedShardTopo.getPartitionMap().size());
        assertEquals("ANs", 0, removedShardTopo.getArbNodeIds().size());
    }

    @Test
    public void testRemoveFailedShardEmptyTopology() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 1, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 3, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);
        TopologyCandidate candidate = new TopologyCandidate("remove", topo);
        TopologyBuilder tb = new TopologyBuilder(candidate, pool, params,
                                                 atc.getParams());
        RepGroupId failedShard = RepGroupId.parse("rg1");
        pool.remove(new StorageNodeId(1));
        pool.remove(new StorageNodeId(2));
        pool.remove(new StorageNodeId(3));
        Exception cause = null;
        try {
            tb.removeFailedShard(failedShard);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Removing shard operation would result in a store with " +
                     "no shards.",
                     cause.getMessage());
    }

    @Test
    public void testRemoveFailedShardCapacity() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(3),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 0, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);

        TopologyCandidate candidate = new TopologyCandidate("remove", topo);
        TopologyBuilder tb = new TopologyBuilder(candidate, pool, params,
                                                 atc.getParams());
        RepGroupId failedShard = RepGroupId.parse("rg2");
        TopologyCandidate removedShardcand = tb.removeFailedShard(failedShard);
        Topology removedShardTopo = removedShardcand.getTopology();

        assertEquals("Shards", 2, removedShardTopo.getRepGroupMap().size());
        assertEquals("SNs", 3, removedShardTopo.getStorageNodeMap().size());
        assertEquals("RNs", 6, removedShardTopo.getRepNodeIds().size());
        assertEquals("Partitions", 1000,
                     removedShardTopo.getPartitionMap().size());
        assertEquals("ANs", 0, removedShardTopo.getArbNodeIds().size());
    }

    @Test
    public void testRemoveFailedShardWithArbiter() {
        int repFactor = 2;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor,
                              DatacenterType.PRIMARY, true);

        /* Simple topology with three shards, 9 RNs, 9 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());
        assertEquals("ANs", 3, topo.getArbNodeIds().size());

        StorageNodePool pool = makePool(topo);


        TopologyCandidate candidate = new TopologyCandidate("remove", topo);
        TopologyBuilder tb = new TopologyBuilder(candidate, pool, params,
                                                 atc.getParams());
        RepGroupId failedShard = RepGroupId.parse("rg2");
        TopologyCandidate removedShardcand = tb.removeFailedShard(failedShard);

        pool.remove(new StorageNodeId(3));
        pool.remove(new StorageNodeId(4));
        Topology removedShardTopo = removedShardcand.getTopology();

        assertEquals("Shards", 2, removedShardTopo.getRepGroupMap().size());
        assertEquals("SNs", 6, removedShardTopo.getStorageNodeMap().size());
        assertEquals("RNs", 4, removedShardTopo.getRepNodeIds().size());
        assertEquals("Partitions", 1000,
                     removedShardTopo.getPartitionMap().size());
        assertEquals("ANs", 2, removedShardTopo.getArbNodeIds().size());
    }

    /**
     * Make a handcrafted, noncompliant topology, and rebalance it.
     */
    @Test
    public void testRebalance() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 3 RNs, 5 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       5, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 5, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());

        /*
         * Now add a fourth shard with two RNs so that the
         *  - consanguinity rules are violated
         *  - SN1 is over capacity
         *  - the fourth shard has insufficient rep factor
         */
        RepGroup newRG = new RepGroup();
        topo.add(newRG);
        newRG.add(new RepNode(new StorageNodeId(1)));
        newRG.add(new RepNode(new StorageNodeId(1)));

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(9, validation, topo);
        assertNumProblems(1, OverCapacity.class, validation, topo);
        assertNumProblems(1, RNProximity.class, validation, topo);
        assertNumProblems(1, UnderCapacity.class, validation, topo);
        assertNumProblems(1, InsufficientRNs.class, validation, topo);
        assertNumProblems(5, MissingRootDirectorySize.class, validation, topo);

        /*
         * Rebalance, will use up the one extra capacity slot, to get rid of
         * the RN proximity problem, but won't be able to fix the OverCapacity
         * and InsufficientRf.
         */
        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());
        TopologyCandidate fixed = tb.rebalance(null);
        validation = Rules.validate(fixed.getTopology(), params, false);
        assertNumProblems(7, validation, fixed.getTopology());
        assertNumProblems(1, OverCapacity.class, validation,
                          fixed.getTopology());
        assertNumProblems(1, InsufficientRNs.class, validation,
                          fixed.getTopology());
        assertNumProblems(5, MissingRootDirectorySize.class, validation,
                          fixed.getTopology());

        /*
         * Add one more capacity slot, rebalance and check that one more
         * problem is gone.
         */
        StorageNodeParams snp = params.get(new StorageNodeId(2));
        snp.setCapacity(snp.getCapacity() + 1);

        tb = new TopologyBuilder(fixed, makePool(topo), params,
                                 atc.getParams());
        fixed = tb.rebalance(null);
        validation = Rules.validate(fixed.getTopology(), params, false);
        assertNumProblems(6, validation, fixed.getTopology());

        /*
         * Add one more capacity slot to a SN that already hosts shard 4,
         * rebalance and check that one more problem is gone. This will require
         * moving an existing RN.
         */
        snp = params.get(new StorageNodeId(5));
        snp.setCapacity(snp.getCapacity() + 1);

        tb = new TopologyBuilder(fixed, makePool(topo), params,
                                 atc.getParams());
        fixed = tb.rebalance(null);
        validation = Rules.validate(fixed.getTopology(), params, false);
        assertNumProblems(5, validation, fixed.getTopology());

        /*
         * Change the replication factor. At first the topology will have more
         * violations, because too few SNs have been supplied. Add more and
         * more SNs until the new RF requirements are fully satisfied.
         */
        int attempt = 0;
        Topology startTopo = fixed.getTopology();
        TopologyCandidate result = null;
        do {
            if (++attempt > 1000) {
                fail("Too many attempts");
            }
            addSNs(dc, startTopo, params, snCounter, 1,
                   CapGenType.SAME.newInstance(2));
            logger.info("change repfactor attempt " + attempt);
            TopologyCandidate trial =
                new TopologyCandidate("repFactorChangeAttempt" + attempt,
                                      startTopo);
            tb = new TopologyBuilder(trial, makePool(startTopo), params,
                                     atc.getParams());
            result = tb.changeRepfactor(7, dc.getResourceId());
            /* Set topoIsDeployed=true so that all rules are exercised */
            validation = Rules.validate(result.getTopology(), params, true);

            logger.info(TopologyPrinter.printTopology
                        (result.getTopology(), params, true));

        } while (getNonMissingRootDirectorySizeNum(validation.
                                                   getProblems()) > 1);

        assertNumProblems(14, validation, result.getTopology());
        assertNumProblems(1, InsufficientAdmins.class, validation,
                          result.getTopology());

        /* Should take at a maximum 12 attempts to add 12 RNs */
        assertTrue("numAttempts for changing RF=" + attempt, (attempt <= 12));
        assertEquals(4, result.getTopology().getRepGroupMap().size());
        /* RF of 7, for 4 shards, mean 28 RNs */
        assertEquals(28, result.getTopology().getRepNodeIds().size());

        /* Redistribute in order to fix the number of partitions warnings */
        TopologyCandidate partitionFix =
                new TopologyCandidate("partFix", result.getTopology());
        tb = new TopologyBuilder(partitionFix, makePool(result.getTopology()),
                                 params, atc.getParams());
        fixed = tb.build();
        /* Set topoIsDeployed=true so that all rules are exercised */
        validation = Rules.validate(fixed.getTopology(), params, true);
        assertNumProblems(14, validation, fixed.getTopology());
        assertNumProblems(1, InsufficientAdmins.class, validation,
                          result.getTopology());
    }

    /**
     * Return the number of non missing root directory size problems.
     */
    private int getNonMissingRootDirectorySizeNum(List<RulesProblem> list) {
        int size = list.size();
        for (RulesProblem problem : list) {
            if (MissingRootDirectorySize.class.equals(problem.getClass())) {
                size--;
            }
        }
        return size;
    }

    @Test
    public void testRebalanceWithMissingSNs() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 3 RNs, 5 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       5, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(5));

        /*
         * Rebalance when missing SNs .
         */
        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
            (candidate, pool, params, atc.getParams());
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("The storage pool provided for topology candidate " +
                     "start must contain the following SNs which are " +
                     "already in use in the current topology: [sn4, sn5]",
                     cause.getMessage());
    }

    /**
     * Rebalance 2*1 topology and arbiter is not enabled
     */
    @Test
    public void testRebalanceForRF1() {
        int repFactor = 1;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with two shards, 2 RNs, 1 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       1, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 1, topo.getStorageNodeMap().size());
        assertEquals("RNs", 2, topo.getRepNodeIds().size());


        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(1, validation, topo);
        assertNumProblems(1, MissingRootDirectorySize.class, validation, topo);

        /* Add an additional SN */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 2, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(1)).setCapacity(1);

        validation = Rules.validate(topo, params, false);
        assertNumProblems(4, validation, topo);
        assertNumProblems(1, OverCapacity.class, validation, topo);
        assertNumProblems(1, UnderCapacity.class, validation, topo);
        assertNumProblems(2, MissingRootDirectorySize.class, validation, topo);

        /* Rebalance*/
        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Cannot relocate RN when the repfactor is 1",
                     cause.getMessage());
    }

    /**
     * Rebalance 3*2 topology and arbiter is not enabled
     */
    @Test
    public void testRebalanceForRF2() {
        int repFactor = 2;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 6 RNs, 3 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());


        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(3, validation, topo);
        assertNumProblems(3, MissingRootDirectorySize.class, validation, topo);

        /* Add three additional SNs */
        addSNs(dc, topo, params, snCounter, 3, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(1)).setCapacity(1);
        params.get(new StorageNodeId(2)).setCapacity(1);
        params.get(new StorageNodeId(3)).setCapacity(1);

        validation = Rules.validate(topo, params, false);
        assertNumProblems(12, validation, topo);
        assertNumProblems(3, OverCapacity.class, validation, topo);
        assertNumProblems(3, UnderCapacity.class, validation, topo);
        assertNumProblems(6, MissingRootDirectorySize.class, validation, topo);

        /* Rebalance*/
        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("Cannot relocate RN when the repfactor is 2 and arbiter" +
                     " is disabled", cause.getMessage());
    }

    /**
     * Rebalance 3*2 topology and arbiter is enabled
     */
    @Test
    public void testRebalanceForRF2WithArbiter() {
        int repFactor = 2;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor,
                              DatacenterType.PRIMARY, true);

        /* Simple topology with three shards, 6 RNs, 3 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());


        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(3, validation, topo);
        assertNumProblems(3, MissingRootDirectorySize.class, validation, topo);

        /* Add three additional SNs */
        addSNs(dc, topo, params, snCounter, 3, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(1)).setCapacity(1);
        params.get(new StorageNodeId(2)).setCapacity(1);
        params.get(new StorageNodeId(3)).setCapacity(1);

        validation = Rules.validate(topo, params, false);
        assertNumProblems(12, validation, topo);
        assertNumProblems(3, OverCapacity.class, validation, topo);
        assertNumProblems(3, UnderCapacity.class, validation, topo);
        assertNumProblems(6, MissingRootDirectorySize.class, validation, topo);

        /* Rebalance*/
        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause == null);
    }

    /**
     * Redistribute 4*2 topology with Arbiters. The redistribute
     * will move an existing RN in order to generate more shards.
     */
    @Test
    public void testRedistibuteForRF2Arbiters() {
        redistibuteForRF2Internal(true);
    }

    /**
     * Redistribute 4*2 topology without Arbiters. The redistribute will
     * not relocate existing RNs.
     */
    @Test
    public void testRedistibuteForRF2NoArbiters() {
        redistibuteForRF2Internal(false);
    }

    /**
     *  Create a 4x2 topo.
     *  Add SN with capacity 3.
     *  do a redistribute.
     *  If arbiters are used, you can relocate an RN and get more shards,
     *  otherwise relocation is prevented and the number of shards is less.
     */
    private void redistibuteForRF2Internal(boolean useArbiters) {
        final int repFactor = 2;
        final String DC_NAME = "FIRST_DC";
        final Datacenter dc =
            addDC(sourceTopo, params, DC_NAME, repFactor,
                  DatacenterType.PRIMARY, useArbiters);

        /* Simple topology with three shards, 6 RNs, 3 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(3),
                                       1000,
                                       repFactor);

        assertEquals("Shards", 4, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 8, topo.getRepNodeIds().size());


        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(4, validation, topo);
        assertNumProblems(3, MissingRootDirectorySize.class, validation, topo);

        /* Add additional SN */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(3));
        assertEquals("SNs", 4, topo.getStorageNodeMap().size());

        /* Redistribute*/
        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());

        Exception cause = null;
        try {
            candidate = tb.build();
        } catch (Exception e) {
            cause = e;
        }
        assertTrue(cause == null);

        int nShards = useArbiters ? 6 : 5;
        int nANs = useArbiters ? 6 : 0;
        int nRNs = useArbiters ? 12 : 10;
        topo = candidate.getTopology();
        assertEquals("Shards", nShards, topo.getRepGroupMap().size());
        assertEquals("SNs", 4, topo.getStorageNodeMap().size());
        assertEquals("RNs", nRNs, topo.getRepNodeIds().size());
        assertEquals("ANs", nANs, topo.getArbNodeIds().size());
    }

    @Test
    public void testRebalanceSecondaryRNsPRF2SRF2() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 2,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 4,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 2,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(2));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());
        assertEquals("RNs", 8, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(6, validation, topo);
        assertNumProblems(6, MissingRootDirectorySize.class, validation, topo);

        /* Add two additional SNs */
        addSNs(dc2, topo, params, snCounter, 2, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 8, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(5)).setCapacity(1);
        params.get(new StorageNodeId(6)).setCapacity(1);

        candidate = new TopologyCandidate("rebalanced", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());

        validation = Rules.validate(topo, params, false);
        assertNumProblems(12, validation, topo);
        assertNumProblems(2, OverCapacity.class, validation, topo);
        assertNumProblems(2, UnderCapacity.class, validation, topo);
        assertNumProblems(8, MissingRootDirectorySize.class, validation , topo);

        /* Rebalance*/
        candidate = new TopologyCandidate("start", topo);
        tb = new TopologyBuilder(candidate, makePool(topo), params,
                                 atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause == null);
    }

    @Test
    public void testRebalanceSecondaryRNsPRF2SRF1() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 2,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 4,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(2));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 5, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(5, validation, topo);
        assertNumProblems(5, MissingRootDirectorySize.class, validation, topo);

        /* Add an additional SN */
        addSNs(dc2, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(5)).setCapacity(1);

        candidate = new TopologyCandidate("rebalanced", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());

        validation = Rules.validate(topo, params, false);
        assertNumProblems(8, validation, topo);
        assertNumProblems(1, OverCapacity.class, validation, topo);
        assertNumProblems(1, UnderCapacity.class, validation, topo);
        assertNumProblems(6, MissingRootDirectorySize.class, validation, topo);

        /* Rebalance*/
        candidate = new TopologyCandidate("start", topo);
        tb = new TopologyBuilder(candidate, makePool(topo), params,
                                 atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause == null);
    }

    @Test
    public void testRebalanceSecondaryRNsPRF1SRF2() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 1,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 2,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(2));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 4, topo.getStorageNodeMap().size());
        assertEquals("RNs", 6, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(4, validation, topo);
        assertNumProblems(4, MissingRootDirectorySize.class, validation, topo);

        /* Add two additional SNs */
        addSNs(dc2, topo, params, snCounter, 2, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 6, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(3)).setCapacity(1);
        params.get(new StorageNodeId(4)).setCapacity(1);

        validation = Rules.validate(topo, params, false);
        assertNumProblems(10, validation, topo);
        assertNumProblems(2, OverCapacity.class, validation, topo);
        assertNumProblems(2, UnderCapacity.class, validation, topo);
        assertNumProblems(6, MissingRootDirectorySize.class, validation, topo);

        /* Rebalance*/
        candidate = new TopologyCandidate("start", topo);
        tb = new TopologyBuilder(candidate, makePool(topo), params,
                                 atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause == null);
    }

    @Test
    public void testRebalanceSecondaryRNsPRF1SRF1() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 1,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(2));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 4, topo.getRepNodeIds().size());
        assertEquals("Partitions", 1000, topo.getPartitionMap().size());

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(3, validation, topo);
        assertNumProblems(3, MissingRootDirectorySize.class, validation, topo);

        /* Add an additional SN */
        addSNs(dc2, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1));
        assertEquals("SNs", 4, topo.getStorageNodeMap().size());

        params.get(new StorageNodeId(3)).setCapacity(1);

        candidate = new TopologyCandidate("rebalanced", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());

        validation = Rules.validate(topo, params, false);
        assertNumProblems(6, validation, topo);
        assertNumProblems(1, OverCapacity.class, validation, topo);
        assertNumProblems(1, UnderCapacity.class, validation, topo);
        assertNumProblems(4, MissingRootDirectorySize.class, validation, topo);

        /* Rebalance*/
        candidate = new TopologyCandidate("start", topo);
        tb = new TopologyBuilder(candidate, makePool(topo), params,
                                 atc.getParams());

        /* Precheck whether elasticity operation could be executed or not */
        Exception cause = null;
        try {
            tb.rebalance(null);
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause == null);
    }

    /**
     * Walk though these common steps:
     *   - Make a topology of 3 SNs, capacity of 1, and let it house 1 shard.
     *   - Add a single SN with capacity of 3, no shards are added
     *   - Add a second SN, make sure an extra shard is added.
     */
    @Test
    public void testIncremental() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with 1 shard */
        Topology first =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       repFactor);

        assertEquals(1, first.getRepGroupMap().size());
        assertEquals(3, first.getStorageNodeMap().size());
        assertEquals(3, first.getRepNodeIds().size());
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(TopologyPrinter.printTopology(first));
        }
        Topology second = redistributeAndValidateTopo
            (new TopologyCandidate("second try", first),
             dc,
             snCounter,
             params,
             1, // additional SNS
             3, // initial total capacity
             CapGenType.SAME.newInstance(3),
             4, // total SNs
             repFactor,
             false);

        assertEquals(1, second.getRepGroupMap().size());
        assertEquals(4, second.getStorageNodeMap().size());
        assertEquals(3, second.getRepNodeIds().size());

        Topology third = redistributeAndValidateTopo
            (new TopologyCandidate("third try", second),
             dc,
             snCounter,
             params,
             1, // additional SNS
             6, // initial total capacity
             CapGenType.SAME.newInstance(1),
             5, // total SNs
             repFactor,
             true);

        assertEquals(2, third.getRepGroupMap().size());
        assertEquals(5, third.getStorageNodeMap().size());
        assertEquals(6, third.getRepNodeIds().size());

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(TopologyPrinter.printTopology(third));
        }
    }

    /**
     * Like testRebalance, but create two data centers and limit rebalancing to
     * a single data center.
     */
    @Test
    public void testRebalanceOneDatacenter() {
        final CapacityGenerator capGen = CapGenType.SAME.newInstance(2);

        /*
         * Create two data centers with RF=3, and add 5 SNs with capacity two
         * to each, to create three shards.
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3);
        addSNs(dc1, sourceTopo, params, snCounter, 5, capGen, 3);
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 3);
        addSNs(dc2, sourceTopo, params, snCounter, 5, capGen, 3);
        final StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, CANDIDATE_NAME, pool, 1000, params, atc.getParams());
        TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(TopologyPrinter.printTopology(topo));
        }

        /* Check configuration */
        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 10, topo.getStorageNodeMap().size());
        assertEquals("RNs", 18, topo.getRepNodeIds().size());

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(12, validation, topo);
        assertNumProblems(2, UnderCapacity.class, validation, topo);

        /*
         * Now add a fourth shard with two RNs to data center 1 so that the
         *  - consanguinity rules are violated
         *  - SN1 is over capacity
         *  - the fourth shard has insufficient rep factor
         */
        RepGroup newRG = new RepGroup();
        topo.add(newRG);
        newRG.add(new RepNode(new StorageNodeId(1)));
        newRG.add(new RepNode(new StorageNodeId(1)));

        /* Check problems */
        validation = Rules.validate(topo, params, false);
        assertNumProblems(16, validation, topo);
        assertNumProblems(1, OverCapacity.class, validation, topo);
        assertNumProblems(1, RNProximity.class, validation, topo);
        assertNumProblems(2, UnderCapacity.class, validation, topo);
        assertNumProblems(2, InsufficientRNs.class, validation, topo);
        assertNumProblems(10, MissingRootDirectorySize.class, validation, topo);

        /*
         * Rebalance data center 1, which eliminates 1 RNProximity and 1
         * UnderCapacity problem.
         */
        candidate = new TopologyCandidate("start", topo);
        tb = new TopologyBuilder(candidate, makePool(topo), params,
                                 atc.getParams());
        TopologyCandidate fixed = tb.rebalance(dc1.getResourceId());

        validation = Rules.validate(fixed.getTopology(), params, false);
        assertNumProblems(14, validation, fixed.getTopology());
        assertNumProblems(1, OverCapacity.class, validation,
                          fixed.getTopology());
        assertNumProblems(1, UnderCapacity.class, validation,
                          fixed.getTopology());
        assertNumProblems(2, InsufficientRNs.class, validation,
                          fixed.getTopology());
        assertNumProblems(10, MissingRootDirectorySize.class, validation,
                          fixed.getTopology());

        /*
         * Add one more capacity slot to data center 1, rebalance, and check
         * that 1 InsufficientRNs problem is gone.
         */
        params.get(new StorageNodeId(2)).setCapacity(3);

        tb = new TopologyBuilder(fixed, makePool(topo), params,
                                 atc.getParams());
        fixed = tb.rebalance(dc1.getResourceId());

        validation = Rules.validate(fixed.getTopology(), params, false);
        assertNumProblems(13, validation, fixed.getTopology());
        assertNumProblems(1, UnderCapacity.class, validation,
                          fixed.getTopology());
        assertNumProblems(2, InsufficientRNs.class, validation,
                          fixed.getTopology());
        assertNumProblems(10, MissingRootDirectorySize.class, validation,
                          fixed.getTopology());

        /*
         * Add one more capacity slot to an SN in data center 1 that already
         * hosts shard 4, rebalance, and check that the InsufficientRNs problem
         * is gone. This will require moving an existing RN.
         */
        params.get(new StorageNodeId(5)).setCapacity(3);

        tb = new TopologyBuilder(fixed, makePool(topo), params,
                                 atc.getParams());
        fixed = tb.rebalance(dc1.getResourceId());

        validation = Rules.validate(fixed.getTopology(), params, false);
        assertNumProblems(12, validation, fixed.getTopology());
        assertNumProblems(1, UnderCapacity.class, validation,
                          fixed.getTopology());
        assertNumProblems(1, InsufficientRNs.class, validation,
                          fixed.getTopology());
        assertNumProblems(10, MissingRootDirectorySize.class, validation,
                          fixed.getTopology());

        /*
         * Change the replication factor. At first the topology will have more
         * violations, because too few SNs have been supplied. Add more and
         * more SNs until the new RF requirements are fully satisfied.
         * Change-repfactor doesn't redistribute partitions, so we will
         * continue to see 4 NonOptimalNumPartitions warnings.
         */
        int attempt = 0;
        Topology startTopo = fixed.getTopology();
        TopologyCandidate result = null;
        int problemSize = 0;
        do {
            problemSize = 0;
            if (++attempt > 1000) {
                fail("Too many attempts");
            }

            /*
             * Specify storage directories for the new SNs so we can test that
             * the change repfactor operation notices them.  [#23281]
             */
            addSNs(dc1, startTopo, params, snCounter, 1, capGen,
                   0, new StorageDirectoryGenerator());
            logger.info("change repfactor attempt " + attempt);
            TopologyCandidate trial =
                new TopologyCandidate("repFactorChangeAttempt" + attempt,
                                      startTopo);
            tb = new TopologyBuilder(trial, makePool(startTopo), params,
                                     atc.getParams());
            result = tb.changeRepfactor(7, dc1.getResourceId());
            /* Set topoIsDeployed=true so that all rules are exercised */
            validation = Rules.validate(result.getTopology(), params, true);
            assertEquals("Storage directory count",
                         result.getTopology().getRepNodeIds().size(),
                         result.getDirectoryAssignments().size());

            logger.info(TopologyPrinter.printTopology
                        (result.getTopology(), params, true));

            /* Filter out InsufficientStorageDirectorySize */
            List<RulesProblem> list = validation.getProblems();
            for (RulesProblem problem : list) {
                if (!problem.getClass().
                        equals(MissingStorageDirectorySize.class)) {
                    problemSize++;
                }
            }
        } while (problemSize > 13);

        assertNumProblems(21, validation, result.getTopology());
        assertNumProblems(1, UnderCapacity.class, validation,
                          result.getTopology());
        assertNumProblems(1, InsufficientRNs.class, validation,
                          result.getTopology());
        assertNumProblems(1, InsufficientAdmins.class, validation,
                          result.getTopology());
        assertNumProblems(8, MissingStorageDirectorySize.class, validation,
                          result.getTopology());
        assertNumProblems(10, MissingRootDirectorySize.class, validation,
                          result.getTopology());

        /* Should take at a maximum 12 attempts to add 12 RNs */
        assertTrue("numAttempts for changing RF=" + attempt, (attempt <= 12));
        assertEquals("Shards", 4, result.getTopology().getRepGroupMap().size());
        /* RFs of 7 for 4 shards, mean 28 RNs from DC1.  DC2 has 9. */
        assertEquals("RNs", 37, result.getTopology().getRepNodeIds().size());

        /* Redistribute in order to fix the number of partitions warnings, and
         * add another SN in DC2.
         */
        addSNs(dc2, result.getTopology(), params, snCounter, 1, capGen);
        final TopologyCandidate partitionFix =
                new TopologyCandidate("partFix", result.getTopology());
        tb = new TopologyBuilder(partitionFix, makePool(result.getTopology()),
                                 params, atc.getParams());
        fixed = tb.build();
        /* Set topoIsDeployed=true so that all rules are exercised */
        validation = Rules.validate(fixed.getTopology(), params, true);
        assertNumProblems(20, validation, fixed.getTopology());
        assertNumProblems(1, InsufficientAdmins.class, validation,
                          result.getTopology());
        assertNumProblems(8, MissingStorageDirectorySize.class, validation,
                          result.getTopology());
        assertNumProblems(11, MissingRootDirectorySize.class, validation,
                          result.getTopology());
    }

    @Test
    public void testChangeRFWithMissingSNs() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 3 RNs, 5 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        addSNs(dc, topo, params, snCounter, 3, CapGenType.SAME.newInstance(1));

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(5));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        Exception cause = null;
        try {
            tb.changeRepfactor(4, dc.getResourceId());
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("The storage pool provided for topology candidate " +
                     "start must contain the following SNs which are " +
                     "already in use in the current topology: [sn4, sn5, sn6]",
                     cause.getMessage());
    }

    /**
     * Do an initial topology, and then redistribute. Vary the number of
     * SNs and the capacities.
     * TODO: use new flavor of test suite annotation
     */
    @Test
    public void testRedistributeSameCapacitySNs() {
        makeInitialAndRedistribute(20,    // numStartingSNs
                                   1000,  // numParititons
                                   3,     //repFactor,
                                   CapGenType.SAME);
    }

    @Test
    public void testRedistributeAscendingCapacitySNs() {
        makeInitialAndRedistribute(20,    // numStartingSNs
                                   1000,  // numParititons
                                   3,     //repFactor,
                                   CapGenType.ASCENDING);
    }

    @Test
    public void testRedistributeRandomSNs() {
        makeInitialAndRedistribute(20,    // numStartingSNs
                                   1000,  // numParititons
                                   5,     //repFactor,
                                   CapGenType.RANDOM);
    }

    @Test
    public void testRedistributeWithMissingSNs() {
        int repFactor = 3;
        String DC_NAME = "FIRST_DC";
        Datacenter dc = addDC(sourceTopo, params, DC_NAME, repFactor);

        /* Simple topology with three shards, 3 RNs, 5 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       6, // numStartingSNs
                                       CapGenType.SAME.newInstance(2),
                                       1000,
                                       repFactor);

        addSNs(dc, topo, params, snCounter, 3, CapGenType.SAME.newInstance(1));

        StorageNodePool pool = makePool(topo);
        pool.remove(new StorageNodeId(4));
        pool.remove(new StorageNodeId(5));
        pool.remove(new StorageNodeId(6));

        TopologyCandidate candidate = new TopologyCandidate("start", topo);
        TopologyBuilder tb = new TopologyBuilder
                (candidate, pool, params, atc.getParams());
        Exception cause = null;
        try {
            tb.build();
        } catch (Exception e) {
            cause = e;
        }

        assertTrue(cause != null);
        assertEquals("The storage pool provided for topology candidate " +
                     "start must contain the following SNs which are " +
                     "already in use in the current topology: [sn4, sn5, sn6]",
                     cause.getMessage());
    }

    /**
     * Create a topology with two data centers, both with the exact capacity to
     * support 3 shards with RF=3 in DC1 and RF=2 in DC2.
     */
    @Test
    public void testTwoDatacenters() {
        final CapacityGenerator capGen = CapGenType.SAME.newInstance(1);
        final int numPartitions = 1000;

        /* Create data center 1 with RF=3 and 9 SNs for 3 shards */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3);
        addSNs(dc1, sourceTopo, params, snCounter, 9, capGen, 3);

        /* Create data center 2 with RF=2 and 6 SNs for 3 shards */
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 2);
        addSNs(dc2, sourceTopo, params, snCounter, 6, capGen, 2);

        final StorageNodePool pool = makePool(sourceTopo);

        assertEquals("Pool capacity",
                     capGen.getTotalCapacity(),
                     Rules.calculateMaximumCapacity(pool, params));

        final TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, CANDIDATE_NAME, pool, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(TopologyPrinter.printTopology(topo));
        }
        validate(topo, params, 2 /*DCs*/, 15 /*SNs*/, 15 /*capacity*/,
                 5 /*RF*/);
        assertEquals("Shards", 3, topo.getRepGroupMap().size());
    }

    /**
     * Create a topology with three data centers, with RF=3 for 1 and RF=2 for
     * the others.  DC2 has space for 2 shards, and the others have space for 3
     * shards, so make sure only 2 shards are created.
     */
    @Test
    public void testThreeDatacenters() {
        final CapacityGenerator capGen1 = CapGenType.SAME.newInstance(1);
        final CapacityGenerator capGen2 = CapGenType.SAME.newInstance(2);
        final int numPartitions = 1000;

        /* Create data center 1 with RF=3 and 9 SNs for 3 shards */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3);
        addSNs(dc1, sourceTopo, params, snCounter, 9, capGen1, 3);

        /* Create data center 2 with RF=2 and 2 capacity=2 SNs for 2 shards */
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 2);
        addSNs(dc2, sourceTopo, params, snCounter, 2, capGen2, 2);

        /* Create data center 3 with RF=2 and 3 capacity=2 SNs for 3 shards */
        final Datacenter dc3 = addDC(sourceTopo, params, "DC3", 2);
        addSNs(dc3, sourceTopo, params, snCounter, 3, capGen2, 2);

        final StorageNodePool pool = makePool(sourceTopo);

        assertEquals("Pool capacity",
                     capGen1.getTotalCapacity() + capGen2.getTotalCapacity(),
                     Rules.calculateMaximumCapacity(pool, params));

        final TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, CANDIDATE_NAME, pool, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(TopologyPrinter.printTopology(topo));
        }
        validate(topo, params, 3 /*DCs*/, 14 /*SNs*/, 19 /*capacity*/,
                 7 /*RF*/);
        assertEquals("Shards", 2, topo.getRepGroupMap().size());
    }

    /**
     * Create a topology with one data center at RF=3 and 3 shards, and then
     * build two more topologies by each time adding another data center, first
     * with RF=2 and then RF=1.
     */
    @Test
    public void testAddDatacenters() {
        final CapacityGenerator capGen = CapGenType.SAME.newInstance(1);
        final int numPartitions = 1000;

        /* Create data center 1 with RF=3 and 9 SNs for 3 shards */
        final Topology startTopo1 = new Topology(kvstoreName);
        final Datacenter dc1 = addDC(startTopo1, params, "DC1", 3);
        addSNs(dc1, startTopo1, params, snCounter, 9, capGen, 3);
        final StorageNodePool pool1 = makePool(startTopo1);
        final TopologyBuilder tb1 = new TopologyBuilder(
            startTopo1, CANDIDATE_NAME, pool1, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate1 = tb1.build();
        final Topology topo1 = candidate1.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("topo1: " + TopologyPrinter.printTopology(topo1) +
                    "\n" + candidate1.showAudit());
        }
        validate(topo1, params, 1 /*DCs*/, 9 /*SNs*/, 9 /*capacity*/,
                 3 /*RF*/);
        assertEquals("Shards", 3, topo1.getRepGroupMap().size());

        /*
         * Add data center 2 with RF=2, and first try building a topology
         * without adding any new SNs
         */
        final Topology startTopo2 = topo1;
        final Datacenter dc2 = addDC(startTopo2, params, "DC2", 2);
        final TopologyBuilder tb2a = new TopologyBuilder(
            startTopo2, CANDIDATE_NAME + "2a", pool1, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate2a = tb2a.build();
        final Topology topo2a = candidate2a.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("topo2a: " + TopologyPrinter.printTopology(topo2a) +
                    "\n" + candidate2a.showAudit());
        }
        /* Set topoIsDeployed=true so that all rules are exercised */
        final Results results2a = Rules.validate(topo2a, params, true);
        logger.log(Level.FINE, "validation: {0}", results2a);
        assertNumProblems(14, results2a, topo2a);  /* 4 RN and 1 Admin probs. */
        assertNumProblems(3, InsufficientRNs.class, results2a, topo2a);

        /*
         * Next add 6 SNs for 3 shards, but don't add them to the SN pool.
         *
         * Specify storage directories for the new SNs so we can test that the
         * redistribute operation performed by TopologyBuilder.build takes them
         * into account [#23161].
         */
        addSNs(dc2, startTopo2, params, snCounter, 6, capGen, 2,
               new StorageDirectoryGenerator());
        final TopologyBuilder tb2b = new TopologyBuilder(
            startTopo2, CANDIDATE_NAME + "2b", pool1, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate2b = tb2b.build();
        final Topology topo2b = candidate2b.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("topo2b: " + TopologyPrinter.printTopology(topo2b) +
                        "\n" + candidate2b.showAudit());
        }
        final Results results2b = Rules.validate(topo2b, params, false);
        logger.log(Level.FINE, "validation: {0}", results2b);
        assertNumProblems(24, results2b, topo2b);
        assertNumProblems(3, InsufficientRNs.class, results2b, topo2b);
        assertNumProblems(6, UnderCapacity.class, results2b, topo2b);
        assertNumProblems(6, MissingStorageDirectorySize.class,
                          results2b, topo2b);
        assertNumProblems(9, MissingRootDirectorySize.class, results2b, topo2b);

        /* Finally build with the new SNs */
        final StorageNodePool pool2 = makePool(startTopo2);
        final TopologyBuilder tb2c = new TopologyBuilder(
            startTopo2, CANDIDATE_NAME + "2c", pool2, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate2c = tb2c.build();
        final Topology topo2c = candidate2c.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("topo2c: " + TopologyPrinter.printTopology(topo2c) +
                        "\n" + candidate2c.showAudit());
        }
        validate(topo2c, params, 2 /*DCs*/, 15 /*SNs*/, 15 /*capacity*/,
                 5 /*RF*/);
        assertEquals("Shards", 3, topo2c.getRepGroupMap().size());
        /* should be one storage dir for each RN */
        assertEquals("Storage directory count",
                     topo2c.getRepNodeIds().size(),
                     candidate2c.getDirectoryAssignments().size());

        /* Add data center 3 with RF=1 and 3 SNs for 3 shards */
        final Topology startTopo3 = topo2c;
        final Datacenter dc3 = addDC(startTopo3, params, "DC3", 1);
        addSNs(dc3, startTopo3, params, snCounter, 3, capGen, 1);
        final StorageNodePool pool3 = makePool(startTopo3);
        final TopologyBuilder tb3 = new TopologyBuilder(
            startTopo3, CANDIDATE_NAME, pool3, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate3 = tb3.build();
        final Topology topo3 = candidate3.getTopology();
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "topo3: {0}\n{1}",
                   new Object[]{TopologyPrinter.printTopology(topo3),
                                candidate3.showAudit()});
        }
        validate(topo3, params, 3 /*DCs*/, 18 /*SNs*/, 18 /*capacity*/,
                 6 /*RF*/);
        assertEquals("Shards", 3, topo3.getRepGroupMap().size());
    }

    /** Test creating a topology with a data center with RF=-1. */
    @Test
    public void testDatacenterRFLessThanZero() {
        try {
            addDC(sourceTopo, params, "DC1", -1);
            fail("Datacenter should have RF >= 0.");
        } catch (IllegalArgumentException e) {
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void testCreateNoSNs() {
        final StorageNodePool pool = makePool(sourceTopo);
        try {
            new TopologyBuilder(
                sourceTopo, "candidate", pool, 10, params, atc.getParams());
            fail("Expected exception");
        } catch (IllegalCommandException e) {
            logger.fine(e.toString());
        }
    }

    /**
     * Exercise the situation where the target topology candidate is missing
     * SNs that are in the source topology.
     * - Create a simple topology with one data center at RF=1 and 1 shard
     * - then create a copy of current topology
     * - add one more storage node right after topology clone.
     * Building the topology should fail.
     */
    @SuppressWarnings("unused")
    @Test
    public void testCreateWithOutDatedTopo() {
        final Datacenter dc = addDC(sourceTopo, params, "DC", 1);

        /* Simple topology with single shards, 1 RNs, 1 SNs */
        Topology topo =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       1, // numStartingSNs
                                       CapGenType.SAME.newInstance(1),
                                       1000,
                                       1 /* repfactor */);
        assertEquals("Shards", 1, topo.getRepGroupMap().size());
        assertEquals("SNs", 1, topo.getStorageNodeMap().size());
        assertEquals("RNs", 1, topo.getRepNodeIds().size());
        TopologyCandidate oodCandidate = new TopologyCandidate("outdate", topo);
        addSNs(dc, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1));
        try {
            TopologyBuilder tb = new TopologyBuilder(oodCandidate,
                                                     makePool(sourceTopo),
                                                     params, atc.getParams());
            fail("Expected exception");
        } catch (IllegalCommandException e) {
            logger.fine(e.toString());
        }
    }

    /**
     * Number of partitions must be at least the SN capacity (5) for 1 DC with
     * RF=1.
     */
    @SuppressWarnings("unused")
    @Test
    public void testNumPartitionsOneDatacenterRF1() {
        final Datacenter dc = addDC(sourceTopo, params, "DC", 1);
        addSNs(dc, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(1));
        final StorageNodePool pool = makePool(sourceTopo);
        try {
            new TopologyBuilder(
                sourceTopo, "candidate", pool, 4, params, atc.getParams());
            fail("Expected exception");
        } catch (IllegalCommandException e) {
            logger.fine(e.toString());
        }
        new TopologyBuilder(
            sourceTopo, "candidate", pool, 5, params, atc.getParams());
        new TopologyBuilder(
            sourceTopo, "candidate", pool, 10, params, atc.getParams());
    }

    /**
     * Number of partitions must be at least the SN capacity (10) divided by
     * the RF (3) (equals 3) for 1 DC with RF > 1.
     */
    @SuppressWarnings("unused")
    @Test
    public void testNumPartitionsOneDatacenterRF3() {
        final Datacenter dc = addDC(sourceTopo, params, "DC", 3);
        addSNs(dc, sourceTopo, params, snCounter, 10,
               CapGenType.SAME.newInstance(1));
        final StorageNodePool pool = makePool(sourceTopo);
        try {
            new TopologyBuilder(
                sourceTopo, "candidate", pool, 2, params, atc.getParams());
            fail("Expected exception");
        } catch (IllegalCommandException e) {
            logger.fine(e.toString());
        }
        new TopologyBuilder(
            sourceTopo, "candidate", pool, 3, params, atc.getParams());
        new TopologyBuilder(
            sourceTopo, "candidate", pool, 9, params, atc.getParams());
    }

    /**
     * Number of paratitions must be at least the SN capacity (30) divided by
     * the total RF (5) (equals 6) for multiple DCs with RF > 1.
     */
    @SuppressWarnings("unused")
    @Test
    public void testNumPartitionsMultipleDatacenters() {
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3);
        addSNs(dc1, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(3));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 2);
        addSNs(dc2, sourceTopo, params, snCounter, 15,
               CapGenType.SAME.newInstance(1));
        final StorageNodePool pool = makePool(sourceTopo);
        try {
            new TopologyBuilder(
                sourceTopo, "candidate", pool, 5, params, atc.getParams());
            fail("Expected exception");
        } catch (IllegalCommandException e) {
            logger.fine(e.toString());
        }
        new TopologyBuilder(
            sourceTopo, "candidate", pool, 6, params, atc.getParams());
    }

    /**
     * Test that the topology builder requires that the topology contain SNs
     * from a primary datacenter when there is a secondary datacenter.
     */
    @Test
    public void testBuilderNoPrimaries() {
        final Datacenter dc1 =
            addDC(sourceTopo, params, "DC1", 1, DatacenterType.SECONDARY);
        addSNs(dc1, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(3));
        final StorageNodePool pool = makePool(sourceTopo);
        try {
            @SuppressWarnings("unused")
            TopologyBuilder topologyBuilder = new TopologyBuilder(
                sourceTopo, "candidate", pool, 5, params, atc.getParams());
            fail("Expected exception");
        } catch (IllegalCommandException e) {
            logger.fine(e.toString());
        }
    }

    /**
     * Test modifying and validating a topology that has shards without RNs in
     * a primary datacenter.
     */
    @Test
    public void testValidateNoPrimaries() {

        /*
         * Create a store with a primary and a secondary datacenter, each with
         * two SNs, to produce two shards.
         */
        final Datacenter dc1 =
            addDC(sourceTopo, params, "DC1", 1, DatacenterType.PRIMARY);
        final Datacenter dc2 =
            addDC(sourceTopo, params, "DC2", 1, DatacenterType.SECONDARY);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        final TopologyBuilder builder = new TopologyBuilder(
            sourceTopo, "candidate", makePool(sourceTopo), 5, params,
            atc.getParams());
        final TopologyCandidate candidate = builder.build();
        final Topology topo = candidate.getTopology();
        validate(topo, params, 2 /*dcs*/, 4 /*sns*/, 4 /*capacity*/, 2 /*rf*/);

        /*
         * Remove SN 1 from the primary datacenter, leaving its shard without
         * an RN in a primary datacenter.  Don't use TopologyBuilder to make
         * these changes because it's constructor makes a similar check.
         *
         * We will leave the Admin, so we check the BadAdmin path.
         */
        final StorageNodeId snId = new StorageNodeId(1);
        for (final RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (final Iterator<RepNode> iter = rg.getRepNodes().iterator();
                 iter.hasNext(); ) {
                final RepNode rn = iter.next();
                if (snId.equals(rn.getStorageNodeId())) {
                    iter.remove();
                }
            }
        }
        topo.remove(snId);
        params.remove(snId);
        /* Set topoIsDeployed=true so that all rules are exercised */
        final Results validation = Rules.validate(topo, params, true);
        logger.log(Level.FINE, "validation: {0}", validation);
        assertNumProblems(7, validation, topo);
        assertNumProblems(1, NoPrimaryDC.class, validation, topo);
        assertNumProblems(1, InsufficientRNs.class, validation, topo);
        assertNumProblems(1, BadAdmin.class, validation, topo);
        assertNumProblems(1, InsufficientAdmins.class, validation, topo);
        assertNumProblems(3, MissingRootDirectorySize.class, validation, topo);
    }

    /**
     * Test that the topology builder does not produce NoPrimaryDC violations
     * for a topology that has shards without RNs but that only has primary
     * data centers.
     */
    @Test
    public void testNoNoPrimaryDCWithOnlyPrimaries() {

        /*
         * Create a store with two primary datacenters, each with two SNs, to
         * produce two shards.
         */
        final Datacenter dc1 =
            addDC(sourceTopo, params, "DC1", 1, DatacenterType.PRIMARY);
        final Datacenter dc2 =
            addDC(sourceTopo, params, "DC2", 1, DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        final TopologyBuilder builder = new TopologyBuilder(
            sourceTopo, "candidate", makePool(sourceTopo), 5, params,
            atc.getParams());
        final Topology topo = builder.build().getTopology();
        validate(topo, params, 2 /*dcs*/, 4 /*sns*/, 4 /*capacity*/, 2 /*rf*/);

        /*
         * Remove SN 1 from the primary datacenter, leaving its shard without
         * an RN in a primary datacenter.
         */
        final StorageNodeId snId = new StorageNodeId(1);
        for (final RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (final Iterator<RepNode> iter = rg.getRepNodes().iterator();
                 iter.hasNext(); ) {
                final RepNode rn = iter.next();
                if (snId.equals(rn.getStorageNodeId())) {
                    iter.remove();
                }
            }
        }
        topo.remove(snId);
        params.remove(snId);

        /* Remove the admin on that SN */
        for (AdminParams ap : params.getAdminParams()) {
            if (ap.getStorageNodeId().equals(snId)) {
                params.remove(ap.getAdminId());
                break;
            }
        }

        /* No secondary datacenters, so no NoPrimaryDC */
        /* Set topoIsDeployed=true so that all rules are exercised */
        final Results validation = Rules.validate(topo, params, true);
        logger.log(Level.FINE, "validation: {0}", validation);
        assertNumProblems(5, validation, topo);
        assertNumProblems(1, InsufficientRNs.class, validation, topo);
        assertNumProblems(1, InsufficientAdmins.class, validation, topo);
        assertNumProblems(3, MissingRootDirectorySize.class, validation, topo);
    }

    /**
     * Test detecting mismatches between RN node types and the RN's zone type.
     */
    @Test
    public void testWrongNodeType()
        throws Exception {

        /* Create primary and secondary zones */
        final Datacenter zone1 =
            addDC(sourceTopo, params, "Zone1", 1, DatacenterType.PRIMARY);
        final Datacenter zone2 =
            addDC(sourceTopo, params, "Zone2", 1, DatacenterType.SECONDARY);
        addSNs(zone1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        addSNs(zone2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);

        /* Create topology */
        final TopologyBuilder builder = new TopologyBuilder(
            sourceTopo, "candidate", makePool(sourceTopo), 5, params,
            atc.getParams());
        final Topology topo = builder.build().getTopology();

        /* Change node types for one RN in each zone */
        boolean foundPrimary = false;
        boolean foundSecondary = false;
        for (final RepNode rn : topo.getSortedRepNodes()) {
            final Datacenter zone = topo.getDatacenter(rn.getStorageNodeId());
            if (zone.equals(zone1) && !foundPrimary) {
                params.add(new RepNodeParams(rn.getStorageNodeId(),
                                             rn.getResourceId(),
                                             false /* disabled */,
                                             "localhost", 1000,
                                             "localhost:1000",
                                             null /* storageDirctory */,
                                             NodeType.SECONDARY));
                foundPrimary = true;
            } else if (!foundSecondary) {
                params.add(new RepNodeParams(rn.getStorageNodeId(),
                                             rn.getResourceId(),
                                             false /* disabled */,
                                             "localhost", 1010,
                                             "localhost:1000",
                                             null /* storageDirctory */,
                                             NodeType.ELECTABLE));
                foundSecondary = true;
            }
        }

        /* Check for two WrongNodeType problems */
        /* Set topoIsDeployed=true so that all rules are exercised */
        final Results validation = Rules.validate(topo, params, true);
        logger.log(Level.FINE, "validation: {0}", validation);
        assertNumProblems(6, validation, topo);
        assertNumProblems(2, WrongNodeType.class, validation, topo);
    }

    @Test
    public void testAdminCheck() {
        final CapacityGenerator capGen = CapGenType.SAME.newInstance(1);
        final int numPartitions = 1000;

        /* Create data center 1 with RF=3 and 2 Admins */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3);
        addSNs(dc1, sourceTopo, params, snCounter, 9, capGen, 2);

        /* Create data center 2 with RF=3 and 4 Admins */
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 3);
        addSNs(dc2, sourceTopo, params, snCounter, 9, capGen, 4);

        final StorageNodePool pool = makePool(sourceTopo);
        final TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, CANDIDATE_NAME, pool, numPartitions, params,
            atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();

        /* Set topoIsDeployed=true so that all rules are exercised */
        final Results validation = Rules.validate(topo, params, true);
        logger.log(Level.FINE, "validation: {0}", validation);
        assertNumProblems(20, validation, topo);
        assertNumProblems(1, InsufficientAdmins.class, validation, topo);
        assertNumProblems(1, ExcessAdmins.class, validation, topo);
        assertNumProblems(18, MissingRootDirectorySize.class, validation, topo);
    }

    /**
     * Test the builder's handling of a rebalance when directory sizes are
     * specified. This test starts with a 3x3 on three SNs. SNs are added
     * (with varying sizes) while reducing the initial set's capacity.
     */
    @Test
    public void testRebalanceWithDirSize() {
        final int RF = 3;
        final String DC_NAME = "FIRST_DC";
        final Datacenter dc = addDC(sourceTopo, params, DC_NAME, RF);

        /* Should end in 200, 200, 600 */
        final int N_PART = 1000;
        final long INITIAL_SIZE = 20000;

        final TopologyCandidate candidate =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(3),
                                       N_PART,
                                       RF,
                                   new StorageDirectoryGenerator(INITIAL_SIZE));

        Topology topo = candidate.getTopology();
        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());

        /*
         * Add three new SNs with larger directories. The over capacity
         * RNs. Hopefullly from the same shard.
         */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*2));

        /* Reduce the capacity of the original nodes */
        StorageNodeParams snp = params.get(new StorageNodeId(1));
        snp.setCapacity(snp.getCapacity() -1 );
        snp = params.get(new StorageNodeId(2));
        snp.setCapacity(snp.getCapacity() -1 );
        snp = params.get(new StorageNodeId(3));
        snp.setCapacity(snp.getCapacity() -1 );

        /* Expand to use the new SNs */
        TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());
        TopologyCandidate expanded = tb.rebalance(null);
        topo = expanded.getTopology();

        /* Should be no over/under issues */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(0, validation, topo);

        checkPartitionDistribution(topo, N_PART);

        /*
         * Add three new SNs with smaller directories.
         */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));

        /* Again reduce the capacity of the original nodes */
        snp = params.get(new StorageNodeId(1));
        snp.setCapacity(snp.getCapacity() - 1 );
        snp = params.get(new StorageNodeId(2));
        snp.setCapacity(snp.getCapacity() - 1 );
        snp = params.get(new StorageNodeId(3));
        snp.setCapacity(snp.getCapacity() - 1 );

        /*
         * Attempt to expand to use the new SNs. This should not work because
         * we cannot move RNs with larger minimum directory sizes to smaller
         * ones.
         */
        tb = new TopologyBuilder(expanded, makePool(expanded.getTopology()),
                                 params, atc.getParams());
        expanded = tb.rebalance(null);
        topo = expanded.getTopology();

        /* No change, old nodes over, while new nodes under */
        validation = Rules.validate(topo, params, false);
        assertNumProblems(6, validation, topo);
        assertNumProblems(3, UnderCapacity.class, validation, topo);
        assertNumProblems(3, OverCapacity.class, validation, topo);

        /*
         * Add three new SNs, this time with the same size directories as the
         * initial set.
         */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE));

        /* Try again to expand to use the new SNs */
        tb = new TopologyBuilder(expanded, makePool(expanded.getTopology()),
                                 params, atc.getParams());
        expanded = tb.rebalance(null);
        topo = expanded.getTopology();

        /*
         * This should work, leaving the first set added still under capacity.
         */
        validation = Rules.validate(topo, params, false);
        assertNumProblems(3, validation, topo);
        assertNumProblems(3, UnderCapacity.class, validation, topo);

        checkPartitionDistribution(topo, N_PART);
    }

    public void testRedistributeWithDirSize1() {
        final int RF = 3;
        final String DC_NAME = "FIRST_DC";
        final Datacenter dc = addDC(sourceTopo, params, DC_NAME, RF);

        /* The end total size will be 8,000 so use an multiple */
        final int N_PART = 1600;
        final long INITIAL_SIZE = 10000;

        /* Initial topology with three shards, 9 RNs, 3 SNs */
        final TopologyCandidate candidate =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       3, // numStartingSNs
                                       CapGenType.SAME.newInstance(3),
                                       N_PART,
                                       RF,
                                   new StorageDirectoryGenerator(INITIAL_SIZE));

        Topology topo = candidate.getTopology();
        assertEquals("Shards", 3, topo.getRepGroupMap().size());
        assertEquals("SNs", 3, topo.getStorageNodeMap().size());
        assertEquals("RNs", 9, topo.getRepNodeIds().size());

        /*
         * Add six new SNs with larger directories. The new shards should be
         * placed there and have more partitions allocated to them.
         */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*3));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*3));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(1),
               0, new StorageDirectoryGenerator(INITIAL_SIZE*3));

        /* Check problems */
        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(6, validation, topo);
        assertNumProblems(6, UnderCapacity.class, validation, topo);

        /* Expand to use the new SNs */
        final TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());
        TopologyCandidate expanded = tb.build();

        topo = expanded.getTopology();
        assertEquals("Shards", 5, topo.getRepGroupMap().size());

        final Map<RepGroupId, Integer> m =
                                       checkPartitionDistribution(topo, N_PART);
        /*
         * We cheated and know which groups end up where, any code changes
         * may cause these checks to fail.
        */
        assertEquals("Group 1 size", N_PART/8, (int)m.get(new RepGroupId(1)));
        assertEquals("Group 2 size", N_PART/8, (int)m.get(new RepGroupId(2)));
        assertEquals("Group 3 size", N_PART/8, (int)m.get(new RepGroupId(3)));
        assertEquals("Group 4 size", (N_PART/8)*3,
                     (int)m.get(new RepGroupId(4)));
        assertEquals("Group 5 size", (N_PART/8)*2,
                     (int)m.get(new RepGroupId(5)));
    }

    @Test
    public void testRedistributeWithDirSize2() {
        final int RF = 3;
        final String DC_NAME = "FIRST_DC";
        final Datacenter dc = addDC(sourceTopo, params, DC_NAME, RF);

        final int N_PART = (80 + 24) * 12;
        final long INITIAL_SIZE = 80000;

        /* Initial topology with three shards, 9 RNs, 3 SNs */
        final TopologyCandidate candidate =
            makeAndValidateInitialTopo(sourceTopo,
                                       dc,
                                       snCounter,
                                       params,
                                       10, // numStartingSNs
                                       CapGenType.SAME.newInstance(12),
                                       N_PART,
                                       RF,
                                   new StorageDirectoryGenerator(INITIAL_SIZE));

        Topology topo = candidate.getTopology();
        assertEquals("Shards", 40, topo.getRepGroupMap().size());
        assertEquals("SNs", 10, topo.getStorageNodeMap().size());
        assertEquals("RNs", 40*3, topo.getRepNodeIds().size());

        /*
         * Add six new SNs with smaller directories. The new shards should be
         * placed there and have less partitions allocated to them.
         */
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(12),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(12),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(12),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(12),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(12),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));
        addSNs(dc, topo, params, snCounter, 1, CapGenType.SAME.newInstance(12),
               0, new StorageDirectoryGenerator(INITIAL_SIZE/2));

        /* Expand to use the new SNs */
        final TopologyBuilder tb = new TopologyBuilder
            (candidate, makePool(topo), params, atc.getParams());
        TopologyCandidate expanded = tb.build();

        topo = expanded.getTopology();
        assertEquals("Shards", 64, topo.getRepGroupMap().size());

        final Map<RepGroupId, Integer> m =
                                      checkPartitionDistribution(topo, N_PART);

        /*
         * There should be two sets of shards with either a large or small
         * number of partitions.
         */
        int large = 0;
        int small = 0;
        for (Integer i : m.values()) {
            if (i == 12) {
                small++;
            } else if (i == 24) {
                large++;
            }
        }
        assertEquals("Large groups", 40, large);
        assertEquals("Small groups", 24, small);
    }

    @Test
    public void testContractionWithDirSize() {
        final int RF = 1;
        final String DC_NAME = "FIRST_DC";
        final Datacenter dc = addDC(sourceTopo, params, DC_NAME, RF);

        final int N_PART = 100;

        final TopologyCandidate candidate =
                    makeAndValidateInitialTopo(sourceTopo,
                                               dc,
                                               snCounter,
                                               params,
                                               3, // numStartingSNs
                                               CapGenType.SAME.newInstance(1),
                                               N_PART,
                                               RF,
                                               new StorageDirectoryGenerator());

        setSize(params, new StorageNodeId(1), "950 MB");
        setSize(params, new StorageNodeId(2), "250 MB");
        setSize(params, new StorageNodeId(3), "200 MB");

        Topology topo = candidate.getTopology();
        final StorageNodePool pool = makePool(topo);
        TopologyBuilder tb = new TopologyBuilder
                            (candidate, pool, params, atc.getParams());
        final TopologyCandidate expanded = tb.build();
        topo = expanded.getTopology();
        assertEquals("Shards", 3, topo.getRepGroupMap().size());

        pool.remove(new StorageNodeId(3));
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());
        final TopologyCandidate contract = tb.contract();
        topo = contract.getTopology();
        assertEquals("Shards", 2, topo.getRepGroupMap().size());
    }

    /**
     * Test whether there is a waring to remind the user
     * that we cannot find the SN in the current store
     * when the user wants to validate a topology candidate that contains
     * SN(s) does not exist in the current store.
     */
    @Test
    public void testValidateStorageNodesRemoved() {
        /*
         * Create a store with a datacenter, with
         * three SNs, capacities are all 1
         * Produce one shard.
         */

        final Datacenter dc1 =
                addDC(sourceTopo, params, "DC1", 3, DatacenterType.PRIMARY);

        addSNs(dc1, sourceTopo, params, snCounter, 3,
                CapGenType.SAME.newInstance(1), 3,
                new StorageDirectoryGenerator(100));

        final TopologyBuilder builder = new TopologyBuilder(
                sourceTopo, "candidate", makePool(sourceTopo), 30, params,
                atc.getParams());

        final TopologyCandidate candidate = builder.build();
        final Topology topo = candidate.getTopology();

        /*
         * Add a new Storage Node to the topology so that we can
         * pretend the topology we are going to validate has a Storage Node
         * that does not exist in the current store.
         */
        topo.add(new StorageNode(dc1, "host4", SN_PORT));

        /* Set topoIsDeployed=true so that all rules are exercised */
        final Results validation = Rules.validate(topo, params, true);
        logger.log(Level.FINE, "validation: {0}", validation);
        assertNumProblems(1, validation, topo);
        assertNumProblems(1, StorageNodeMissing.class, validation, topo);
    }

    @Test
    public void testChangeRFMiscArgs() {

        /* Create topology: DC1 primary RF=3, DC2 secondary RF=1 */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());

        checkException(() -> tb.changeRepfactor(1, new DatacenterId(999)),
                       IllegalCommandException.class, "not a valid zone");
        checkException(() -> tb.changeRepfactor(2, dc1.getResourceId()),
                       IllegalCommandException.class, "primary zone");
        checkException(() -> tb.changeRepfactor(-1, dc2.getResourceId()),
                       IllegalCommandException.class, "valid range");
    }

    @Test
    public void testChangeRFSecondaryZoneToZero() {

        /* Create topology: DC1 primary RF=3, DC2 secondary RF=1 */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 1,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 1, topo.getRepGroupMap().size());
        assertEquals("SNs", 4, topo.getStorageNodeMap().size());
        assertEquals("RNs", 4, topo.getRepNodeIds().size());

        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(4, validation, topo);
        assertNumProblems(4, MissingRootDirectorySize.class, validation, topo);

        /* Change DC2 to RF=0 */
        candidate = new TopologyCandidate("DC2-at-rf-0", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());
        candidate = tb.changeRepfactor(0, dc2.getResourceId());
        topo = candidate.getTopology();

        assertEquals("Shards", 1, topo.getRepGroupMap().size());
        assertEquals("SNs", 4, topo.getStorageNodeMap().size());
        assertEquals("RNs", 3, topo.getRepNodeIds().size());

        validation = Rules.validate(topo, params, false);
        assertNumProblems(5, validation, topo);
        assertNumProblems(4, MissingRootDirectorySize.class, validation, topo);
        assertNumProblems(1, UnderCapacity.class, validation, topo);
    }

    @Test
    public void testReduceRFSecondaryZone() {

        /*
         * Create topology: DC1 primary RF=3, DC2 secondary RF=3, DC3 secondary
         * RF=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "DC1", 3,
                                     DatacenterType.PRIMARY);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2));
        final Datacenter dc2 = addDC(sourceTopo, params, "DC2", 3,
                                     DatacenterType.SECONDARY);
        addSNs(dc2, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2));
        final Datacenter dc3 = addDC(sourceTopo, params, "DC3", 2,
                                     DatacenterType.SECONDARY);
        addSNs(dc3, sourceTopo, params, snCounter, 4,
               CapGenType.SAME.newInstance(1));
        StorageNodePool pool = makePool(sourceTopo);
        TopologyBuilder tb = new TopologyBuilder(sourceTopo, "candidate",
                                                 pool, 1000, params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 10, topo.getStorageNodeMap().size());
        assertEquals("RNs", 16, topo.getRepNodeIds().size());

        Results validation = Rules.validate(topo, params, false);
        assertNumProblems(10, validation, topo);
        assertNumProblems(10, MissingRootDirectorySize.class, validation, topo);

        /* Change DC2 to RF=1 */
        candidate = new TopologyCandidate("DC2-rf1", topo);
        tb = new TopologyBuilder(candidate, pool, params, atc.getParams());
        candidate = tb.changeRepfactor(1, dc2.getResourceId());
        topo = candidate.getTopology();

        assertEquals("Shards", 2, topo.getRepGroupMap().size());
        assertEquals("SNs", 10, topo.getStorageNodeMap().size());
        assertEquals("RNs", 12, topo.getRepNodeIds().size());

        validation = Rules.validate(topo, params, false);
        assertNumProblems(12, validation, topo);
        assertNumProblems(10, MissingRootDirectorySize.class, validation, topo);
        assertNumProblems(2, UnderCapacity.class, validation, topo);
    }

    @Test
    public void testValidateTransitionReduceTotalPrimaryRF() {

        /* Create topology: Zone 1 Primary RF=2, Zone 2 Primary RF=1 */
        final Datacenter zn1 =
            addDC(sourceTopo, params, "zn1", 2, DatacenterType.PRIMARY);
        addSNs(zn1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1));
        final Datacenter zn2 =
            addDC(sourceTopo, params, "zn2", 1, DatacenterType.PRIMARY);
        addSNs(zn2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1));
        final StorageNodePool pool = makePool(sourceTopo);
        final TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, "topo1", pool, 1000, params, atc.getParams());
        final Topology topo1 = tb.build().getTopology();

        /* Change zn1 to secondary */
        final Topology topo2 = topo1.getCopy();
        topo2.update(zn1.getResourceId(),
                     Datacenter.newInstance(
                         zn1.getName(), zn1.getRepFactor(),
                         DatacenterType.SECONDARY,
                         false /* allowArbiters */,
                         false /* masterAffinity */));
        final TopologyCandidate candidate =
            new TopologyCandidate("topo2", topo2);
        checkException(
            () -> Rules.validateTransition(topo1, candidate, params,
                                           false /* forFailover */),
            IllegalCommandException.class,
            "Attempt to reduce the overall primary replication factor by" +
            " 2 from 3 to 1.");
        Rules.validateTransition(topo1, candidate, params,
                                 true /* forFailover */);
    }

    @Test
    public void testValidateTransitionReduceSinglePrimaryRF() {

        /* Create topology: Zone 1 Primary RF=3 */
        final Datacenter zn1 =
            addDC(sourceTopo, params, "zn1", 3, DatacenterType.PRIMARY);
        addSNs(zn1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1));
        final StorageNodePool pool = makePool(sourceTopo);
        final TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, "topo1", pool, 1000, params, atc.getParams());
        final Topology topo1 = tb.build().getTopology();

        /* Reduce zn1 RF */
        final Topology topo2 = topo1.getCopy();
        topo2.update(zn1.getResourceId(),
                     Datacenter.newInstance(
                         zn1.getName(), 2,
                         DatacenterType.PRIMARY,
                         false /* allowArbiters */,
                         false /* masterAffinity */));
        final TopologyCandidate candidate =
            new TopologyCandidate("topo2", topo2);
        checkException(
            () -> Rules.validateTransition(topo1, candidate, params,
                                           false /* forFailover */),
            IllegalCommandException.class,
            "Attempt to reduce the replication factor of primary zone zn1" +
            " from 3 to 2");
        Rules.validateTransition(topo1, candidate, params,
                                 true /* forFailover */);

    }

    @Test
    public void testTopoPreviewZoneChanges() {

        /*
         * Create topology:
         * Zone 1 Primary RF=2 allow-arbiters
         * Zone 2 Primary RF=1 master-affinity
         * Zone 3 Secondary RF=1
         */
        final Datacenter zn1 =
            addDC(sourceTopo, params, "zn1", 2, DatacenterType.PRIMARY,
                  true /* allowArbiters */);
        addSNs(zn1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1));
        final Datacenter zn2 =
            addDC(sourceTopo, params, "zn2", 1, DatacenterType.PRIMARY,
                  false /* allowArbiters */, true /* masterAffinity */);
        addSNs(zn2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1));
        final Datacenter zn3 =
            addDC(sourceTopo, params, "zn3", 1, DatacenterType.SECONDARY);
        addSNs(zn3, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1));

        final StorageNodePool pool = makePool(sourceTopo);
        final TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, "topo1", pool, 1000, params, atc.getParams());
        final Topology topo1 = tb.build().getTopology();

        /*
         * Zone 1 increase RF from 2 to 3
         * Zone 1 remove allow-arbiters
         * Zone 2 add allow-arbiters
         */
        Topology topo2 = topo1.getCopy();
        topo2.update(zn1.getResourceId(),
                     Datacenter.newInstance(
                         zn1.getName(), zn1.getRepFactor(),
                         DatacenterType.PRIMARY,
                         false /* allowArbiters */,
                         false /* masterAffinity */));
        topo2.update(zn2.getResourceId(),
                     Datacenter.newInstance(
                         zn2.getName(), zn2.getRepFactor(),
                         DatacenterType.PRIMARY,
                         true /* allowArbiters */,
                         true /* masterAffinity */));
        TopologyCandidate candidate2 =
            new TopologyBuilder(topo2, "topo2", pool, 1000, params,
                                atc.getParams())
            .changeRepfactor(3, zn1.getResourceId());
        final TopologyDiff diff2 = new TopologyDiff(
            topo1, "topo1", candidate2, params, true /* validate */);
        assertEquals("Topology transformation from topo1 to topo2:\n" +
                     "Change 1 zone replication factor \n" +
                     "Change 2 zone allow arbiter properties\n" +
                     "Create 1 RN \n" +
                     "\n" +
                     "change zn1 replication factor by 1\n" +
                     "change zn1 allow arbiters to false\n" +
                     "change zn2 allow arbiters to true\n" +
                     "shard rg1\n" +
                     "  1 new RN : rg1-rn5 \n",
                     diff2.display(false /* verbose */));

        /*
         * Zone 1 add master affinity
         * Zone 2 remove master affinity
         * Zone 3 decrease RF from 1 to 0
         */
        Topology topo3 = topo1.getCopy();
        topo3.update(zn1.getResourceId(),
                     Datacenter.newInstance(
                         zn1.getName(), zn1.getRepFactor(),
                         DatacenterType.PRIMARY,
                         true /* allowArbiters */,
                         true /* masterAffinity */));
        topo3.update(zn2.getResourceId(),
                     Datacenter.newInstance(
                         zn2.getName(), zn2.getRepFactor(),
                         DatacenterType.PRIMARY,
                         false /* allowArbiters */,
                         false /* masterAffinity */));
        TopologyCandidate candidate3 =
            new TopologyBuilder(topo3, "topo3", pool, 1000, params,
                                atc.getParams())
            .changeRepfactor(0, zn3.getResourceId());
        final TopologyDiff diff3 = new TopologyDiff(
            topo1, "topo1", candidate3, params, true /* validate */);
        assertEquals("Topology transformation from topo1 to topo3:\n" +
                     "Change 1 zone replication factor \n" +
                     "Change 2 zone master affinities\n" +
                     "Remove 1 RN \n" +
                     "\n" +
                     "change zn3 replication factor by -1\n" +
                     "change zn1 master affinity to true\n" +
                     "change zn2 master affinity to false\n" +
                     "shard rg1\n" +
                     "  1 to be removed RN : rg1-rn4 \n",
                     diff3.display(false /* verbose */));
    }

    /**
     * Creates a map of partition distribution across shards. If numPartitions
     * is non-zero, the total number of partitions must match numPartitions
     * and no shard can have 0 partitions.
     */
    private Map<RepGroupId, Integer>
                                 checkPartitionDistribution(Topology topo,
                                                            int numPartitions) {
        final Map<RepGroupId, Integer> m =
                            new HashMap<>(topo.getRepGroupMap().size());
        for (Partition p : topo.getPartitionMap().getAll()) {
            Integer counter = m.get(p.getRepGroupId());
            if (counter == null) {
                counter = 0;
            }
            counter++;
            m.put(p.getRepGroupId(), counter);
        }

        if (numPartitions == 0) {
            return m;
        }

        int count = 0;
        for (Entry<RepGroupId, Integer> e : m.entrySet()) {
            int size = e.getValue();
            assertNotEquals("Shard " + e.getKey() + " partitions", 0, size);
            count += size;
        }
        assertEquals("Num partitions", numPartitions, count);
        return m;
    }

    /** Make a topology, and then try different redistributes */
    @SuppressWarnings("hiding")
    private void makeInitialAndRedistribute(int numStartingSNs,
                                            int numPartitions,
                                            int repFactor,
                                            CapGenType capGenType) {
        /**
         * Try initial deployments with a total capacity of 20 - 200,
         * then redistribute.
         */
        for (int startingCapacity = 1;
             startingCapacity < 10;
             startingCapacity++) {

            logger.info("Initial deploy, numSNs = " + numStartingSNs +
                        " starting capacity = " +  startingCapacity);
            AtomicInteger snCounter = new AtomicInteger(0);
            Parameters params = new Parameters(kvstoreName);
            Topology sourceTopo = new Topology(kvstoreName);
            Datacenter dc = addDC(sourceTopo, params, "Datacenter_East",
                                  repFactor);

            /* Make and validate the first topology. */
            CapacityGenerator initialCapGen =
                capGenType.newInstance(startingCapacity);
            Topology first = makeAndValidateInitialTopo(sourceTopo,
                                                        dc,
                                                        snCounter,
                                                        params,
                                                        numStartingSNs,
                                                        initialCapGen,
                                                        numPartitions,
                                                        repFactor);

            /*
             * Deploy additional SNs, causing a redistribute
             */
            int originalNumSNs = snCounter.get();
            int redistributeAttempt = 0;
            for (int additionalSNs = 1; additionalSNs < 30; additionalSNs++) {

                int totalSNs = numStartingSNs + additionalSNs;

                /* Vary the capacity of the additional SNs. */
                for (int additionalCapacity = 1; additionalCapacity < 10;
                     additionalCapacity++) {

                    redistributeAttempt++;
                    logger.log
                        (Level.INFO,
                         "Redistribute attempt [{0}]: " +
                         "Initial: {1} SNs, {2} shards, {3} total capacity " +
                         "redistribute: adding {4} SNs with starting " +
                         "capacity {5}",
                         new Object[]{redistributeAttempt,
                                      numStartingSNs,
                                      first.getRepGroupMap().size(),
                                      initialCapGen.getTotalCapacity(),
                                      additionalSNs,
                                      additionalCapacity});

                    TopologyCandidate startingTopo =
                        new TopologyCandidate("Redistribute" +
                                              redistributeAttempt,
                                              first.getCopy());
                    AtomicInteger newSNCounter =
                        new AtomicInteger(originalNumSNs);

                    Parameters useParams = new Parameters(params);

                    redistributeAndValidateTopo
                        (startingTopo,
                         dc,
                         newSNCounter,
                         useParams,
                         additionalSNs,
                         initialCapGen.getTotalCapacity(),
                         capGenType.newInstance(additionalCapacity),
                         totalSNs,
                         repFactor,
                         null);
                }
            }
        }
    }

    /**
     * Assume that all the SNs are in the same datacenter
     * @param numSNs the number of SNS to use in the deployment
     * @param capacity the capacity value for each SN
     * @param numPartitions specified in an initial deployment
     * @param repFactor specified in an initial deployment
     */
    @SuppressWarnings("hiding")
    private Topology makeAndValidateInitialTopo(Topology sourceTopo,
                                                Datacenter dc,
                                                AtomicInteger snCounter,
                                                Parameters params,
                                                int numSNs,
                                                CapacityGenerator capGen,
                                                int numPartitions,
                                                int repFactor) {
        return makeAndValidateInitialTopo(sourceTopo,
                                         dc,
                                         snCounter,
                                         params,
                                         numSNs,
                                         capGen,
                                         numPartitions,
                                         repFactor,
                                         null).getTopology();
    }

    @SuppressWarnings("hiding")
    private TopologyCandidate makeAndValidateInitialTopo(Topology sourceTopo,
                                                Datacenter dc,
                                                AtomicInteger snCounter,
                                                Parameters params,
                                                int numSNs,
                                                CapacityGenerator capGen,
                                                int numPartitions,
                                                int repFactor,
                                                StorageDirectoryGenerator storageDirGen) {

        addSNs(dc, sourceTopo, params, snCounter, numSNs,
               capGen, repFactor, storageDirGen);
        StorageNodePool pool = makePool(sourceTopo);

        /*
         * Check that the Rules module calculates maximum capacity correctly.
         * That calculation will be used during topology building, and this is
         * a handy place to verify it.
         */
        assertEquals("Capacity",
                     capGen.getTotalCapacity(),
                     Rules.calculateMaximumCapacity(pool, params));

        TopologyBuilder tb = new TopologyBuilder(sourceTopo,
                                                 CANDIDATE_NAME,
                                                 pool,
                                                 numPartitions,
                                                 params,
                                                 atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology result = candidate.getTopology();
        validate(result, params, 1 /*DCs*/, numSNs, capGen.getTotalCapacity(),
                 repFactor);
        return candidate;
    }

    /**
     * Add a Datacenter to the topology and parameter set.
     */
    private Datacenter addDC(Topology topo,
                             Parameters parameters,
                             String dcName,
                             int repFactor) {

        return addDC(topo, parameters, dcName, repFactor,
                     DatacenterType.PRIMARY);
    }

    /**
     * Add a Datacenter to the topology and parameter set, and specify the
     * type.
     */
    private Datacenter addDC(
        final Topology topo,
        final Parameters parameters,
        final String dcName,
        final int repFactor,
        final DatacenterType datacenterType) {

        return addDC(topo, parameters, dcName, repFactor, datacenterType,
                     false);
    }

    private Datacenter addDC(
            final Topology topo,
            final Parameters parameters,
            final String dcName,
            final int repFactor,
            final DatacenterType datacenterType,
            final boolean allowArbiter) {

        return addDC(topo, parameters, dcName, repFactor, datacenterType,
                     allowArbiter, false /* masterAffinity */);
    }

    private Datacenter addDC(
            final Topology topo,
            final Parameters parameters,
            final String dcName,
            final int repFactor,
            final DatacenterType datacenterType,
            final boolean allowArbiter,
            final boolean masterAffinity) {

        final Datacenter dc = topo.add(
            Datacenter.newInstance(dcName, repFactor, datacenterType,
                                   allowArbiter, masterAffinity));
        parameters.add(new DatacenterParams(dc.getResourceId(),
                                            "Lexington"));
        return dc;
    }

    /**
     * Add SNs to the topology and parameter set.
     */
    @SuppressWarnings("hiding")
    private void addSNs(Datacenter dc,
                        Topology topo,
                        Parameters params,
                        AtomicInteger snCounter,
                        int numSNs,
                        CapacityGenerator capGen) {
        addSNs(dc, topo, params, snCounter, numSNs, capGen, 0, null);
    }

    /**
     * Add SNs and Admins to the topology and parameter set.
     */
    @SuppressWarnings("hiding")
    private void addSNs(Datacenter dc,
                        Topology topo,
                        Parameters params,
                        AtomicInteger snCounter,
                        int numSNs,
                        CapacityGenerator capGen,
                        int numAdmins) {
        addSNs(dc, topo, params, snCounter, numSNs, capGen, numAdmins, null);
    }

    /**
     * Add SNs and Admins to the topology and parameter set, and optionally
     * specify storage directories.
     */
    @SuppressWarnings("hiding")
    private void addSNs(Datacenter dc,
                        Topology topo,
                        Parameters params,
                        AtomicInteger snCounter,
                        int numSNs,
                        CapacityGenerator capGen,
                        int numAdmins,
                        StorageDirectoryGenerator storageDirGen) {
        if (numAdmins > numSNs) {
            numAdmins = numSNs;
        }

        for (int i = 0; i < numSNs; i++) {
            int snCount = snCounter.incrementAndGet();
            String hostName = "host" + snCount;
            StorageNode sn =
                topo.add(new StorageNode(dc, hostName, SN_PORT));
            StorageNodeParams snp =
                new StorageNodeParams(sn.getResourceId(), hostName,
                                      SN_PORT, null);
            final int snId = snp.getStorageNodeId().getStorageNodeId();
            final int capacity = capGen.getCapacity(snId);
            snp.setCapacity(capacity);

            final StorageDirectory[] storageDirs = (storageDirGen != null) ?
                storageDirGen.getStorageDirectories(capacity) :
                null;
            if (storageDirs != null) {
                for (final StorageDirectory storageDir : storageDirs) {
                    /* If we are setting the root directory there is only one */
                    if (storageDir.isRoot()) {
                        snp.setRootDirSize(storageDir.getSize());
                        break;
                    }
                    final String size = (storageDir.getSize() == 0) ? null :
                            Long.toString(storageDir.getSize());
                    snp.setStorageDirMap(StorageNodeParams.
                                       changeStorageDirMap(params,
                                                           snp, true,
                                                           storageDir.getPath(),
                                                           size));
                }
            }
            params.add(snp);

            if (numAdmins > 0) {
                numAdmins--;
                String adminStorageDir = snp.getRootDirPath();
                if (!snp.getAdminDirPaths().isEmpty()) {
                    adminStorageDir = snp.getAdminDirPaths().get(0);
                }
                params.add(new AdminParams(
                               new AdminId(params.getAdminCount() + 1),
                               sn.getResourceId(),
                               getAdminType(dc),
                               adminStorageDir));
            }
        }
    }

    private void setSize(Parameters params, StorageNodeId snId, String size) {
        final StorageNodeParams snp = params.get(snId);
        final ParameterMap newMap = snp.getStorageDirMap().copy();
        for (Parameter p : snp.getStorageDirMap()) {
            newMap.put(new SizeParameter(p.getName(), size));
        }
        snp.setStorageDirMap(newMap);
        snp.setRootDirSize(new SizeParameter(null, size).asLong());
    }

    private AdminType getAdminType(Datacenter dc) {
        switch (dc.getDatacenterType()) {
            case PRIMARY:
                return AdminType.PRIMARY;
            case SECONDARY:
                return AdminType.SECONDARY;
        }
        throw new IllegalStateException("Datacenter does not have a type "+ dc);
    }

    /**
     * If the redistribute could expand the store, return the resulting
     * topology. If it's expected that the redistribute failed, return the
     * starting topology so that the test can chain more attempts.
     * @param shouldExpandOverride is used if the test case knows that
     * expansion is not possible. It's hard to statically compute whether
     * a topology can expand, given the consanguinity rules, so this is
     * just a way the calling test can override the capacity based
     * assessment
     */
    @SuppressWarnings("hiding")
    private Topology
        redistributeAndValidateTopo(TopologyCandidate startCandidate,
                                    Datacenter dc,
                                    AtomicInteger snCounter,
                                    Parameters params,
                                    int numAdditionalSNs,
                                    int initialCapacity,
                                    CapacityGenerator capGen,
                                    int numTotalSNs,
                                    int repFactor,
                                    Boolean shouldExpandOverride) {

        addSNs(dc, startCandidate.getTopology(), params, snCounter,
               numAdditionalSNs, capGen);

        Topology startTopo = startCandidate.getTopology();
        StorageNodePool pool = makePool(startTopo);
        TopologyBuilder tb = new TopologyBuilder(startCandidate,
                                                 pool,
                                                 params,
                                                 atc.getParams());

        boolean shouldExpand;
        if (shouldExpandOverride == null) {
            shouldExpand = expansionPossible(startTopo, params, repFactor);
        } else {
            shouldExpand = shouldExpandOverride;
        }

        Topology result = null;
        try {
            TopologyCandidate candidate = tb.build();
            result = candidate.getTopology();

            /*
             * Be sure to generate the diff, even if logging is off, to
             * exercise the preview implementation.
             */
            String preview = new TopologyDiff(startCandidate.getTopology(),
                                              null,
                                              candidate,
                                              params).display(VERBOSE);
            logger.info(preview);
            if (shouldExpand) {
                validate(result, params, 1 /*DCs*/, numTotalSNs,
                         initialCapacity + capGen.getTotalCapacity(),
                         repFactor);
            } else {
                assertEquals(0, TopologyDiff.shardDelta
                             (startCandidate.getTopology(), result));
            }
        } catch (IllegalCommandException e) {
            fail("Redistribution should have succeeded, totalCapacity=" +
                 capGen.getTotalCapacity() + " " + e);
        }
        return result;
    }

    /**
     * Make a storage node pool with all the SNs in the topology.
     */
    StorageNodePool makePool(Topology topo) {
        StorageNodePool pool = new StorageNodePool("trial");
        for (StorageNodeId snId : topo.getStorageNodeIds()) {
            pool.add(snId);
        }
        return pool;
    }

    /**
     * Check if the resulting topology is legal
     */
    @SuppressWarnings("hiding")
    private void validate(Topology result,
                          Parameters params,
                          int totalDCs,
                          int totalSNs,
                          int totalCapacity,
                          int repFactor) {

        /* Validate */
        Results rulesResult = Rules.validate(result, params, false);
        assertNumViolations(0, rulesResult, result);

        /*
         * The only kind of warnings we expect are UnderCapacity and
         * InsufficientStorageDirectorySize ones, which would come about
         * because the topology didn't use all the available SN slots. We also
         * only expect to see repFactor-1 underCapacity warnings; otherwise,
         * you'd think we could fit another shard in. And because not all
         * storage directories are storage sizes, then the exception
         * InsufficientStorageDirectorySize comes about.
         */
        final List<RulesProblem> warnings = rulesResult.getWarnings();
        for (final RulesProblem rp : warnings) {
            if (!UnderCapacity.class.equals(rp.getClass()) &&
                !MissingStorageDirectorySize.class.equals(rp.getClass()) &&
                !MissingRootDirectorySize.class.equals(rp.getClass())) {
                logger.info("===>Validation failure on \n" +
                            TopologyPrinter.printTopology(result, params,
                                                          true));
                fail("Only expected UnderCapacity warnings, but got " + rp);
            }
        }

        if (warnings.size() >= 2*totalSNs + 1) {
            fail("Expected less than " + repFactor + " warnings, found " +
                 warnings.size() +
                 "\nWarnings: " + warnings +
                 "\nTopology: " + TopologyPrinter.printTopology(result));
        }

        /* Check assumptions */
        assertEquals("Data centers",
                     totalDCs, result.getDatacenterMap().size());
        assertEquals("SNs", totalSNs, result.getStorageNodeMap().size());

        int numRGs = totalCapacity / repFactor;

        logger.log(Level.INFO,
                   "Validating with total SNs={0} total capacity={1}",
                   new Object[]{totalSNs, totalCapacity});
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(TopologyPrinter.printTopology(result));
        }

        int numActualRGs = result.getRepGroupMap().size();
        if (numRGs != numActualRGs) {
            logger.severe(TopologyPrinter.printTopology(result, params, false));
            fail("Expected " + numRGs + " shards, got " + numActualRGs);
        }

        int numRNs = numActualRGs * repFactor;
        assertEquals("RNs", numRNs, result.getSortedRepNodes().size());
    }

    /**
     * Return true if this topology can hold additional shards. This is
     * tricky to calculate given the consanguinity rules, and this
     * method only looks at capacity right now.
     */
    @SuppressWarnings("hiding")
    private boolean expansionPossible(Topology topo,
                                      Parameters params,
                                      int repFactor) {

        /* Is there enough capacity to add a new one? */
        CapacityProfile profile = Rules.getCapacityProfile(topo, params);
        return  (profile.getExcessCapacity()/repFactor) > 0;
    }

    /**
     * Assert that the specified number of rule problems occurred of the
     * specified type.
     */
    private static void assertNumProblems(
        final int expectedNumProblems,
        final Class<? extends RulesProblem> problemClass,
        final Results validation,
        final Topology topo) {

        final List<? extends RulesProblem> problems =
            validation.find(problemClass);
        if (expectedNumProblems != problems.size()) {
            fail("Expected " + expectedNumProblems +
                 " problems of class " + problemClass.getSimpleName() +
                 ", found " + problems.size() + ":\n" + problems + "\n" +
                 TopologyPrinter.printTopology(topo) +
                 "\nAll problems: " + validation);
        }
    }

    /**
     * Assert that the specified number of rule problems occurred.
     */
    private static void assertNumProblems(final int expectedNumProblems,
                                          final Results validation,
                                          final Topology topo ) {

        if (expectedNumProblems != validation.numProblems()) {
            fail("Expected " + expectedNumProblems +
                 " problems, found " + validation.numProblems() + ":\n" +
                 validation + "\n" + TopologyPrinter.printTopology(topo));
        }
    }

    /**
     * Assert that the specified number of rule violations occurred.
     */
    private static void assertNumViolations(final int expectedNumViolations,
                                            final Results validation,
                                            final Topology topo) {

        if (expectedNumViolations != validation.numViolations()) {
            fail("Expected " + expectedNumViolations +
                 " violations, found " + validation.numViolations() + ":\n" +
                 validation + "\n" +
                 TopologyPrinter.printTopology(topo));
        }
    }

    /**
     * The capacity generator interface and classes are used to implement
     * different policies for generating per-sn capacity values when deploying
     * SNs. Different capacity values can vary how the topology layout logic
     * works.
     */
    interface CapacityGenerator {

        /**
         * Get a capacity value to use for this SN, and add to the
         * totalCapacity count.
         */
        abstract int getCapacity(int snNum);

        /**
         * @return the sum of the capacity values that have been
         * handed out via getCapacity()
         */
        abstract int getTotalCapacity();
    }

    enum CapGenType {
        SAME, ASCENDING, RANDOM;

        CapacityGenerator newInstance(int startCapVal) {
            switch (this) {
            case SAME:
                return new SameCapacity(startCapVal);
            case ASCENDING:
                return new AscendingCapacity(startCapVal);
            case RANDOM:
                return new RandomCapacity(startCapVal);
            }
            return null;
        }
    }

    /**
     * Return the same capacity value for each SN.
     */
    private static class SameCapacity implements CapacityGenerator {
        private final int capVal;
        private int totalCapacity = 0;

        SameCapacity(int capVal) {
            this.capVal = capVal;
        }

        @Override
        public int getCapacity(int snNum) {
            totalCapacity += capVal;
            return capVal;
        }

        @Override
        public int getTotalCapacity() {
            return totalCapacity;
        }
    }

    /**
     * The capacity value increases for each SN.
     */
    private static class AscendingCapacity implements CapacityGenerator {
        private int capVal;
        private int totalCapacity;

        /**
         * Start capacity value generation at this value.
         */
        AscendingCapacity(int capVal) {
            this.capVal = capVal;
        }

        /**
         * The provided capacity value increases monotonically.
         */
        @Override
        public int getCapacity(int snNum) {
            capVal++;
            totalCapacity += capVal;
            return capVal;
        }

        @Override
        public int getTotalCapacity() {
            return totalCapacity;
        }
    }

    /**
     * The capacity value is random from 1-10
     */
    private static class RandomCapacity implements CapacityGenerator {
        private final Random rand;
        private int totalCapacity;

        /**
         * Start capacity value generation at this value.
         */
        RandomCapacity(int seed) {
            rand = new Random(seed);
        }

        /**
         * The provided capacity value increases monotonically.
         */
        @Override
        public int getCapacity(int snNum) {
            /*
             * Random will return a value between 0 and 9l, so increment to
             * get it to 1-10
             */
            int capVal = rand.nextInt(9);
            capVal ++;
            totalCapacity += capVal;
            return capVal;
        }

        @Override
        public int getTotalCapacity() {
            return totalCapacity;
        }
    }

    /**
     * Return the storage directory for the specified SN with the given
     * capacity. Can return null if no storage directories are desired.
     */
    private static class StorageDirectoryGenerator {
        final long size;
        private int next = 0;

        StorageDirectoryGenerator() {
            this(0L);
        }

        StorageDirectoryGenerator(long size) {
            this.size = size;
        }

        StorageDirectory[] getStorageDirectories(int capacity) {
            final StorageDirectory[] result = new StorageDirectory[capacity];
            for (int i = 0; i < capacity; i++) {
                File path =
                    new File(File.separator + "storage-dir-" + next++);
                result[i] = new StorageDirectory(path.getAbsolutePath(), size);
            }
            return result;
        }
    }
}
