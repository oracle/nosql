/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.topo;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.TopologyDiff.ShardChange;
import oracle.kv.impl.admin.topo.Validations.UnevenANDistribution;
import oracle.kv.impl.admin.topo.Validations.ANWrongDC;
import oracle.kv.impl.admin.topo.Validations.ANNotAllowedOnSN;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.admin.topo.Validations.InsufficientANs;
import oracle.kv.impl.admin.topo.Validations.ExcessANs;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;
/**
 * Test topology creation, redistribution and rebalancing. This test is focused
 * solely on the TopologyBuilder, Rules and topologies are created and verified
 * without being deployed.
 */
public class TopoBuilderArbiterTest extends TestBase {

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

    /**
     * Test that arbiters are created in the topo.
     */
    @Test
    public void testARBs() {

        /*
         * Create a topology with 3 shards, a primary data center with RF=2 and
         * 3 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                                     2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 3);
        TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, CANDIDATE_NAME, makePool(sourceTopo), 1000, params,
            atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        assertEquals(3, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(6, topo.getRepNodeIds().size());
        assertEquals(3,topo.getArbNodeIds().size());
        Results validation = Rules.validate(topo, params, false);
        assertNumViolations(0, validation, topo);

        /*
         * check to insure the ARBs are on separate SN's.
         */
        HashMap<StorageNodeId, StorageNodeId> SNs =
            new HashMap<StorageNodeId, StorageNodeId>();

        for (Iterator<ArbNodeId> arbIds =
            topo.getArbNodeIds().iterator(); arbIds.hasNext();) {
            ArbNode arbNode = topo.get(arbIds.next());
            StorageNodeId snid = arbNode.getStorageNodeId();
            assertTrue(SNs.put(snid, snid) == null);
         }
    }

    /*
     * Test that a topo with total primary RF topology without an
     * arbiter hosting DC will not have arbiters
     * or violations.
     */
    @Test
    public void testNoARBsNoARBZone() {

        /*
         * Create a topology with 3 shards, a primary data center with RF=2 and
         * 3 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC", 2);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 3);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final Topology topo = tb.build().getTopology();
        assertEquals(3, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(6, topo.getRepNodeIds().size());
        assertEquals(0,topo.getArbNodeIds().size());
        Results validation = Rules.validate(topo, params, false);
        assertNumViolations(0, validation, topo);
    }

    /**
     * Test that a topo with total primary RF equal to three
     * will not have arbiters or violations.
     */
    @Test
    public void testNoARBsRF3() {

        /*
         * Create a topology with 2 shards, a primary data center with RF=3 and
         * 4 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                                     3, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 3);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final Topology topo = tb.build().getTopology();
        assertEquals(2, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(6, topo.getRepNodeIds().size());
        assertEquals(0,topo.getArbNodeIds().size());
        Results validation = Rules.validate(topo, params, false);
        assertNumViolations(0, validation, topo);
    }

    /**
     * Test that an existing topo that is modified by adding more
     * SN will produce more shards with ANs.
     */
    @Test
    public void testIncreaseShardsWithArbs() {

        /*
         * Create a topology with 3 shards, a primary data center with RF=2 and
         * 3 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                                     2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 3);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        assertEquals(3, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(6, topo.getRepNodeIds().size());
        assertEquals(3,topo.getArbNodeIds().size());
        Results validation = Rules.validate(topo, params, false);
        assertNumViolations(0, validation, topo);

        TopologyCandidate startingTopoC =
                new TopologyCandidate("Redistribute",
                                      topo.getCopy());
        Topology startingTopo = startingTopoC.getTopology();
        addSNs(dc1, startingTopo, params, snCounter, 2,
                CapGenType.SAME.newInstance(2));

        StorageNodePool pool1 = makePool(startingTopo);


        TopologyBuilder tb1 =
            new TopologyBuilder(startingTopoC, pool1, params, atc.getParams());

        TopologyCandidate candidate1 = tb1.build();
        Topology topo1 = candidate1.getTopology();

        validation = Rules.validate(topo1, params, false);
        assertNumViolations(0, validation, topo1);
        assertEquals(5, topo1.getRepGroupMap().size());
        assertEquals(5, topo1.getStorageNodeMap().size());
        assertEquals(10, topo1.getRepNodeIds().size());
        assertEquals(5,topo1.getArbNodeIds().size());

        /*
         * Be sure to generate the diff, even if logging is off, to
         * exercise the preview implementation.
         */
        TopologyDiff td =
            new TopologyDiff(topo, null, candidate1, params);

        int numShardChanges = 0;
        int numArbsAdded = 0;
        for (Map.Entry<RepGroupId, ShardChange> change :
            td.getChangedShards().entrySet()) {
            ShardChange sc = change.getValue();
            numShardChanges++;
            numArbsAdded = numArbsAdded + sc.getNewANs().size();
        }
        assertEquals(4, numShardChanges);
        assertEquals(2, numArbsAdded);
    }


    /**
     * Test that an existing topo that is modified by adding more
     * SN will produce more shards with ANs.
     */
    @Test
    public void testIncreaseShard() {

        /*
         * Create a topology with 3 shards, a primary data center with RF=2 and
         * 3 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                                     2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
                CapGenType.SAME.newInstance(0), 0);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        assertEquals(1, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(2, topo.getRepNodeIds().size());
        assertEquals(1,topo.getArbNodeIds().size());
        Results validation = Rules.validate(topo, params, false);
        assertNumViolations(0, validation, topo);

        TopologyCandidate startingTopoC =
            new TopologyCandidate("Redistribute", topo.getCopy());
        Topology startingTopo = startingTopoC.getTopology();
        Set<ArbNodeId> anIds = startingTopo.getArbNodeIds();
        ArbNodeId anId = null;
        for (ArbNodeId tanid : anIds) {
            anId = tanid;
        }
        ArbNode an = startingTopo.get(anId);

        StorageNodeParams snp = params.get(an.getStorageNodeId());
        snp.setCapacity(2);

        StorageNodePool pool1 = makePool(startingTopo);


        TopologyBuilder tb1 =
            new TopologyBuilder(startingTopoC, pool1, params, atc.getParams());

        TopologyCandidate candidate1 = tb1.build();
        Topology topo1 = candidate1.getTopology();

        validation = Rules.validate(topo1, params, false);
        assertNumViolations(0, validation, topo1);
        assertEquals(2, topo1.getRepGroupMap().size());
        assertEquals(3, topo1.getStorageNodeMap().size());
        assertEquals(4, topo1.getRepNodeIds().size());
        assertEquals(2,topo1.getArbNodeIds().size());

        TopologyDiff td = new TopologyDiff(topo, null, candidate1, params);

        /*
        String preview = td.display(true);
        System.out.println(preview);
        */

        int numShardChanges = 0;
        int numANsAdded = 0;
        int numANsRelocated = 0;
        int numRNsRelocated = 0;
        int numRNsAdded = 0;

        for (Map.Entry<RepGroupId, ShardChange> change :
            td.getChangedShards().entrySet()) {
            ShardChange sc = change.getValue();
            numShardChanges++;
            numANsAdded = numANsAdded + sc.getNewANs().size();
            numRNsAdded = numRNsAdded + sc.getNewRNs().size();
            numANsRelocated = numANsRelocated + sc.getRelocatedANs().size();
            numRNsRelocated = numRNsRelocated + sc.getRelocatedRNs().size();
        }
        assertEquals(2, numShardChanges);
        assertEquals(1, numANsAdded);
        assertEquals(1, numANsRelocated);
        assertEquals(1, numRNsRelocated);
        assertEquals(2, numRNsAdded);

    }


    /**
     * Test that ANs are allocated on SN that does
     * not contain members of its shard.
     */
    @Test
    public void testArbitersNotInShardSN() {
        /*
         * Create a topology with 2 shards, a primary DC with RF=2.
         * 2 SNs (SN1 and SN2) each with capacity 2 and 1 SN (SN3) with
         * capacity 0.
         * 2 Arbiters will get deployed in SN3.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
            2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
                CapGenType.SAME.newInstance(2), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
                CapGenType.SAME.newInstance(0), 1);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(anId);
            assertEquals(3, arbNode.getStorageNodeId().getStorageNodeId());
        }
    }

    /**
     * Test that ANs are distributed evenly.
     */
    @Test
    public void testArbiterDistribution() {
        /*
         * Create a topology with 5 shards, a primary datacenter with RF=2.
         * 2 SNs (SN1 and SN2) each with capacity 5, 1 SN (SN3) with
         * capacity 1 and 1 SN (SN4) with capacity 0.
         * 5 Arbiters will get deployed. 3 arbiters in SN4 and 2 arbiters
         * in SN3.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
            2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
            CapGenType.SAME.newInstance(5), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
            CapGenType.SAME.newInstance(1), 1);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
            CapGenType.SAME.newInstance(0), 1);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        List<Integer> expectedSNIDs = new ArrayList<Integer>();
        expectedSNIDs.add(4); expectedSNIDs.add(4);
        expectedSNIDs.add(4); expectedSNIDs.add(3);
        expectedSNIDs.add(3);
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(anId);
            Integer snId =
                Integer.valueOf(arbNode.getStorageNodeId().getStorageNodeId());
            if (expectedSNIDs.contains(snId)) {
                expectedSNIDs.remove(snId);
            }
        }
        assertTrue(expectedSNIDs.isEmpty());
    }

    /**
     * Test that ANs are distibuted evenly with priority
     * given to zero capacity SNs.
     */
    @Test
    public void testArbitersZeroCapacitySNMorePriority() {
        /*
         * Create a topology with 5 shards, a primary datacenter with RF=2.
         * 2 SNs (SN1 and SN2) each with capacity 5, 1 SN (SN3) with
         * capacity 1 and 2 SNs (SN4 and SN5) with capacity 0.
         * 5 Arbiters will get deployed. 2 arbiters in SN5, 2 arbiters
         * in SN4 and 1 arbiter in SN3. SN4 and SN5 being zero capacity has
         * higher priority to place an arbiter than SN3.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(5), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1), 1);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(0), 2);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        List<Integer> expectedSNIDs = new ArrayList<Integer>();
        expectedSNIDs.add(5);
        expectedSNIDs.add(4);
        expectedSNIDs.add(3);
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(anId);
            Integer snId =
                Integer.valueOf(arbNode.getStorageNodeId().getStorageNodeId());
            if (expectedSNIDs.contains(snId)) {
                expectedSNIDs.remove(snId);
            }
        }
        assertTrue(expectedSNIDs.isEmpty());
    }

    /**
     * Test AN SN placement. Arbiter hosting SN have zero and non-zero capacity.
     */
    @Test
    public void testArbitersSNPlacement() {
        /*
         * Create a topology with 6 shards, a primary datacenter with RF=2.
         * 2 SNs (SN1 and SN2) each with capacity 5, 2 SNs (SN3 and SN4) with
         * capacity 1 and 2 SNs (SN5 and SN6) with capacity 0.
         * The topo creates 6 shards.
         * The RN layout places shards 1-5 on SN1 and SN2. The AN for shard 6
         * may be allocated on either SN1 or SN2. However, the other SN of the
         * two cannot host an AN since it hosts all remaining shards.
         * Note that the layout is not perfectly balanced.
         * The current method of not moving RN's for AN layout will force
         * the layout of AN's to be unbalanced.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
            2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
            CapGenType.SAME.newInstance(5), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
            CapGenType.SAME.newInstance(1), 1);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
            CapGenType.SAME.newInstance(0), 2);
        TopologyBuilder tb = new TopologyBuilder(
            sourceTopo, CANDIDATE_NAME, makePool(sourceTopo), 1000, params,
                    atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        List<Integer> expectedSNIDs = new ArrayList<Integer>();
        expectedSNIDs.add(6);
        expectedSNIDs.add(6);
        expectedSNIDs.add(5);
        expectedSNIDs.add(4);
        expectedSNIDs.add(3);
        expectedSNIDs.add(1);
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(anId);
            Integer snId =
                Integer.valueOf(arbNode.getStorageNodeId().getStorageNodeId());
            if (expectedSNIDs.contains(snId)) {
                expectedSNIDs.remove(snId);
            }
        }
        assertTrue(expectedSNIDs.isEmpty());
    }

    /**
     * Test AN SN placement.
     */
    @Test
    public void testOneArbiterInEachSN() {
        /*
         * Create a topology with 5 shards, a primary datacenter with RF=2.
         * 10 SNs (SN1 - SN10) each with capacity 1, 5 SNs (SN11 - SN15) with
         * capacity 0.
         * 5 Arbiters get deployed each in SN11 - SN15.
         * SN1 - SN10 have 1 RN each.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
            2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 10,
            CapGenType.SAME.newInstance(1), 10);
        addSNs(dc1, sourceTopo, params, snCounter, 5,
            CapGenType.SAME.newInstance(0), 1);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        List<Integer> expectedSNIDs = new ArrayList<Integer>();
        expectedSNIDs.add(11); expectedSNIDs.add(12);
        expectedSNIDs.add(13); expectedSNIDs.add(14);
        expectedSNIDs.add(15);
        for (ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(anId);
            Integer snId =
                Integer.valueOf(arbNode.getStorageNodeId().getStorageNodeId());
            if (expectedSNIDs.contains(snId)) {
                expectedSNIDs.remove(snId);
            }
        }
        assertTrue(expectedSNIDs.isEmpty());
    }

    /**
     * Test AN placement in RF=0 zone.
     */
    @Test
    public void testArbitersInZoneRFZero() {
        /*
         * Create a topology with 3 shards.
         * 2 Primary Datacenters:
         *
         * DC1: RF=2. Does not allow arbiters.
         * 3 SNs (SN1-SN3) each with capacity 2.
         * 3 SNs (SN4-SN6) each with capacity 0.
         *
         * DC2: RF=0. Allows arbiters.
         * 1 SN (SN7) with capacity 0.
         * 1 SN (SN8) with capacity 1.
         *
         * 3 Arbiters created all in DC2. 2 Arbiters in SN8 and 1 arbiter
         * in SN7.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
            2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
            CapGenType.SAME.newInstance(2), 3);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
            CapGenType.SAME.newInstance(0), 3);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC1",
            0, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
            CapGenType.SAME.newInstance(1), 3);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
            CapGenType.SAME.newInstance(0), 1);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        List<DatacenterId> expectedRNDCIds = new ArrayList<DatacenterId>();
        expectedRNDCIds.add(dc1.getResourceId());
        expectedRNDCIds.add(dc1.getResourceId());
        expectedRNDCIds.add(dc1.getResourceId());
        expectedRNDCIds.add(dc1.getResourceId());
        expectedRNDCIds.add(dc1.getResourceId());
        expectedRNDCIds.add(dc1.getResourceId());

        List<DatacenterId> expectedArbDCIds = new ArrayList<DatacenterId>();
        expectedArbDCIds.add(dc2.getResourceId());
        expectedArbDCIds.add(dc2.getResourceId());
        expectedArbDCIds.add(dc2.getResourceId());

        List<Integer> expectedArbSNs = new ArrayList<Integer>();
        expectedArbSNs.add(8); expectedArbSNs.add(8); expectedArbSNs.add(7);
        for (RepGroupId rgId : topo.getRepGroupIds()) {
            RepGroup rg = topo.get(rgId);
            for (RepNode rn : rg.getRepNodes()) {
                StorageNodeId snId = rn.getStorageNodeId();
                DatacenterId dcId = topo.get(snId).getDatacenterId();
                expectedRNDCIds.remove(dcId);
            }
            for (ArbNode an : rg.getArbNodes()) {
                StorageNodeId snId = an.getStorageNodeId();
                DatacenterId dcId = topo.get(snId).getDatacenterId();
                expectedArbDCIds.remove(dcId);
                expectedArbSNs.remove(
                    Integer.valueOf(snId.getStorageNodeId()));
            }
        }
        assertTrue(expectedRNDCIds.isEmpty());
        assertTrue(expectedArbDCIds.isEmpty());
        assertTrue(expectedArbSNs.isEmpty());
    }

    /**
     * Test AN placement in zone with more SNs.
     */
    @Test
    public void testSelectZoneWithMoreSNsForArbiters() {
        /*
         * Create a topology with 2 Zones.
         * ZN1: 3 SNs and RF=1.
         * ZN2: 4 SNs and RF=1.
         * ZN2 selected to host arbiters since it has more SNs than ZN1
         */
        Datacenter dc1 =
            addDC(sourceTopo, params, "PrimaryDC",
                  1, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 3);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC1",
                               1, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 4,
               CapGenType.SAME.newInstance(2), 3);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Set<ArbNodeId> dc1Arbs = topo.getArbNodeIds(dc1.getResourceId());
        Set<ArbNodeId> dc2Arbs = topo.getArbNodeIds(dc2.getResourceId());

        assertTrue(dc1Arbs.isEmpty());
        assertFalse(dc2Arbs.isEmpty());
    }

    /**
     * Test AN zone selection when there are two
     * zero RF zones.
     */
    @Test
    public void testZeroRFZoneWithMoreSNsHigherPriority() {
        /*
         * Create a topology with 3 Zones.
         * ZN1: 10 SNs and Zone RF=2.
         * ZN2: 3 SNs and Zone RF=0.
         * ZN3: 5 SNs and Zone RF=0
         * All arbiters placed in zone ZN3.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 10,
               CapGenType.SAME.newInstance(3), 10);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC1",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(0), 3);
        Datacenter dc3 = addDC(sourceTopo, params, "PrimaryDC2",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc3, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(0), 5);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Set<ArbNodeId> dc1Arbs = topo.getArbNodeIds(dc1.getResourceId());
        Set<ArbNodeId> dc2Arbs = topo.getArbNodeIds(dc2.getResourceId());
        Set<ArbNodeId> dc3Arbs = topo.getArbNodeIds(dc3.getResourceId());

        assertTrue(dc1Arbs.isEmpty());
        assertTrue(dc2Arbs.isEmpty());
        assertFalse(dc3Arbs.isEmpty());
    }

    @Test
    public void testArbitersInPrimaryZone() {
        /*
         * Create a topology with 3 Zones.
         * ZN1: 10 SNs and Zone RF=2. Primary Zone.
         * ZN2: 3 SNs and Zone RF=0. Primary Zone.
         * ZN3: 5 SNs and Zone RF=0. Secondary Zone.
         * All arbiters placed in zone ZN2.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 10,
               CapGenType.SAME.newInstance(3), 10);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(0), 3);
        Datacenter dc3 = addDC(sourceTopo, params, "PrimaryDC",
                               1, DatacenterType.SECONDARY, false);
        addSNs(dc3, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(1), 5);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Set<ArbNodeId> dc1Arbs = topo.getArbNodeIds(dc1.getResourceId());
        Set<ArbNodeId> dc2Arbs = topo.getArbNodeIds(dc2.getResourceId());
        Set<ArbNodeId> dc3Arbs = topo.getArbNodeIds(dc3.getResourceId());

        assertTrue(dc1Arbs.isEmpty());
        assertFalse(dc2Arbs.isEmpty());
        assertTrue(dc3Arbs.isEmpty());
    }

    /**
     * Test AN zone placement. Insure that a zone with RF>0 is
     * selected over a zone with RF=0 but does not allow ANs.
     */
    @Test
    public void testSelectZoneThatAllowsArbiters() {
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 10,
               CapGenType.SAME.newInstance(3), 10);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC1",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(0), 3);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Set<ArbNodeId> dc1Arbs = topo.getArbNodeIds(dc1.getResourceId());
        Set<ArbNodeId> dc2Arbs = topo.getArbNodeIds(dc2.getResourceId());

        assertTrue(dc1Arbs.isEmpty());
        assertFalse(dc2Arbs.isEmpty());
    }

    /**
     * Negative test to insure that an Exception is
     * thrown if there are not enough SN to support
     * arbiters in the topo.
     */
    @Test
    public void testSufficientSNsNotAvailableForArbiters() {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs and Zone RF=2. Primary Zone. Allows Arbiters.
         * Initial topology creation failed. IllegalCommandException thrown.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(3), 2);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        try {
            tb.build();
            fail("Expected IllegalCommandException");
        } catch (IllegalCommandException e) {
            logger.info("Expected exception: " + e);
        }
    }

    /**
     * Negative test checking that a single primary
     * RF=0 errors since there is not a primary zone
     * with a non-zero RF.
     */
    @Test
    public void testPrimaryNonZeroRFZone() {
        /*
         * Create a topology with 3 Zones.
         * ZN1: 3 SNs and Zone RF=2. Secondary Zone.
         * ZN2: 3 SNs and Zone RF=2. Secondary Zone.
         * ZN3: 5 SNs and Zone RF=0. Primary Zone.
         *
         * IllegalCommandException thrown. Need atleast one non zero
         * primary RF zone.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.SECONDARY, false);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 2);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.SECONDARY, false);
        addSNs(dc2, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 2);
        Datacenter dc3 = addDC(sourceTopo, params, "PrimaryDC",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc3, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(0), 0);
        try {
            @SuppressWarnings("unused")
            TopologyBuilder tb =
                new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                    makePool(sourceTopo), 1000, params,
                                    atc.getParams());
            fail("Expected IllegalCommandException");
        } catch (IllegalCommandException e) {
            logger.info("Expected exception: " + e);
        }
    }

    /**
     *  Deploy topo with total primary RF=2 but no AN
     *  hosting DC. Add a RF=0 zone and SNs. Insure that
     *  validate exposes the ISF AN violations and that
     *  rebalance corrects the violations.
     */
    @Test
    public void testRebalanceInsuffientAN() {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 1 SN and Zone RF=2. Primary Zone. Does not Arbiters.
         * Initial topology created with primary RF=2 and no arbiters.
         * Deploy a new RF=0 zone with capacity 0 SN.
         * Check for Insufficient Arbiters rules problem.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, false);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(3), 2);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Datacenter dc2 = addDC(topo, params, "newDC",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc2, topo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);

        Results results = Rules.validate(topo, params, false);

        assertNumProblems(3, results, topo);
        assertNumProblems(3, InsufficientANs.class, results, topo);

        /*
         * Do a rebalance and check that AN were created
         */
        TopologyCandidate candidate1 = new TopologyCandidate("second", topo);
        TopologyBuilder tb1 =
            new TopologyBuilder(candidate1, makePool(topo),
                                params, atc.getParams());
        TopologyCandidate fixed = tb1.rebalance(null);
        Topology topo2 = fixed.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(0, results, topo2);
        assertEquals(6, topo2.getRepNodeIds().size());
        assertEquals(3,topo2.getArbNodeIds().size());
        /* Check the AN in now in new DC */
        DatacenterId newDCId = dc2.getResourceId();
        for (ArbNodeId anId : topo2.getArbNodeIds()) {
            ArbNode an = topo2.get(anId);
            StorageNode sn = topo2.get(an.getStorageNodeId());
            assertTrue(newDCId.equals(sn.getDatacenterId()));
        }
    }

    /**
     * Test that increases the total primary RF from
     * two to three by adding a new zone. This should
     * raise violations including ExcessANs.
     */
    @Test
    public void testExcessArbitersRules() {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs and Zone RF=2. Primary Zone. Allows Arbiters.
         * Initial topology created with primary RF=2 and has arbiters.
         * Deploy a new RF=1 zone with 3 SN.
         * Check for excess arbiters rules problem.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(3), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        Results results = Rules.validate(topo, params, false);
        assertNumProblems(1, UnevenANDistribution.class, results, topo);


        Datacenter dc2 = addDC(topo, params, "PrimaryDC2",
                               1, DatacenterType.PRIMARY, true);
        addSNs(dc2, topo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1), 1);

        results = Rules.validate(topo, params, false);
        assertNumProblems(9, results, topo);
        assertNumProblems(3, ExcessANs.class, results, topo);
        assertNumProblems(3, UnderCapacity.class, results, topo);
        assertNumProblems(3, InsufficientRNs.class, results, topo);
    }

    /**
     * Test that a Topology is built using ANs. When a new
     * DC with zero capacity is added, rebalance will
     * move the AN to that DC.
     * The second part of the test adds SN to the zero capacity DC
     * and changes the RF of the DC to one.
     */
    @Test
    public void testRebToZeroRFDCAndChangeRF() {
        /*
         * Create a topology with 1 DC.
         * DC1: 2 SNs(capacity 3) and RF=2. Primary. Allows Arbiters.
         * Initial topology created with primary RF=2 and has arbiters.
         * Deploy a new RF=0 zone with 1 SN with capacity 0.
         * Check that rules now has violations.
         * Rebalance the topo and check that the violations are gone and
         * that the AN were move to the new DC.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(3), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        Results results = Rules.validate(topo, params, false);
        assertNumProblems(1, UnevenANDistribution.class, results, topo);
        assertEquals(6, topo.getRepNodeIds().size());
        assertEquals(3,topo.getArbNodeIds().size());

        /*
         * Add new RF zero DC and SN to it.
         */
        Datacenter dc2 = addDC(topo, params, "AVADC2",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc2, topo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);

        /* Check for new violation. */
        results = Rules.validate(topo, params, false);
        assertNumProblems(3, results, topo);
        assertNumProblems(3, ANWrongDC.class, results, topo);

        /*
         * Do a rebalance and check that AN were moved.
         */
        TopologyCandidate candidate2 = new TopologyCandidate("second", topo);
        TopologyBuilder tb2 =
            new TopologyBuilder(candidate2, makePool(topo),
                                params, atc.getParams());
        TopologyCandidate fixed = tb2.rebalance(null);
        Topology topo2 = fixed.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(0, results, topo2);
        assertEquals(6, topo2.getRepNodeIds().size());
        assertEquals(3,topo2.getArbNodeIds().size());
        /* Check the AN in now in new DC */
        DatacenterId newDCId = dc2.getResourceId();
        for (ArbNodeId anId : topo2.getArbNodeIds()) {
            ArbNode an = topo2.get(anId);
            StorageNode sn = topo2.get(an.getStorageNodeId());
            assertTrue(newDCId.equals(sn.getDatacenterId()));
        }

        /* now test changing RF */
        addSNs(dc2, topo2, params, snCounter, 3,
               CapGenType.SAME.newInstance(1), 1);

        TopologyCandidate candidate3 = new TopologyCandidate("second", topo2);
        TopologyBuilder tb3 =
            new TopologyBuilder(candidate3, makePool(topo2),
                                params, atc.getParams());
        candidate3 = tb3.changeRepfactor(1, dc2.getResourceId());
        Topology topo3 = candidate3.getTopology();
        results = Rules.validate(topo3, params, false);
        assertNumProblems(0, results, topo3);
        assertEquals(9, topo3.getRepNodeIds().size());
        assertEquals(0,topo3.getArbNodeIds().size());

        TopologyDiff td = new TopologyDiff(topo2, null, candidate3, params);

        int numShardChanges = 0;
        int numArbsAdded = 0;
        int numArbsRemoved = 0;
        for (Map.Entry<RepGroupId, ShardChange> change :
            td.getChangedShards().entrySet()) {
            ShardChange sc = change.getValue();
             numShardChanges++;
            numArbsAdded = numArbsAdded + sc.getNewANs().size();
            numArbsRemoved = numArbsRemoved + sc.getRemovedANs().size();
        }
        assertEquals(3, numShardChanges);
        assertEquals(0, numArbsAdded);
        assertEquals(3, numArbsRemoved);
    }

    /**
     * Test changeRepFactor from one to two and make sure ANs are created.
     * Second part of the test changes the RF from two to three and checks
     * that there are no ANs in the topo.
     */
    @Test
    public void testChangeRFOneTwoThree() {

        /*
         * Create a topology with 3 shards, a primary data center with RF=2 and
         * 3 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                                     1, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        assertEquals(2, topo.getRepGroupMap().size());
        assertEquals(2, topo.getStorageNodeMap().size());
        assertEquals(2, topo.getRepNodeIds().size());
        assertEquals(0,topo.getArbNodeIds().size());
        Results results = Rules.validate(topo, params, false);
        assertNumViolations(0, results, topo);

        addSNs(dc1, topo, params, snCounter, 2,
               CapGenType.SAME.newInstance(0), 0);
        addSNs(dc1, topo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        TopologyCandidate candidate2 = new TopologyCandidate("second", topo);
        TopologyBuilder tb2 =
            new TopologyBuilder(candidate2, makePool(topo), params,
                                atc.getParams());
        candidate2 = tb2.changeRepfactor(2, dc1.getResourceId());
        Topology topo2 = candidate2.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(0, results, topo2);
        assertEquals(4, topo2.getRepNodeIds().size());
        assertEquals(2,topo2.getArbNodeIds().size());

        addSNs(dc1, topo2, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        TopologyCandidate candidate3 = new TopologyCandidate("second", topo2);
        TopologyBuilder tb3 =
            new TopologyBuilder(candidate3, makePool(topo2), params,
                                atc.getParams());
        candidate3 = tb3.changeRepfactor(3, dc1.getResourceId());
        Topology topo3 = candidate3.getTopology();
        results = Rules.validate(topo3, params, false);
        assertNumProblems(0, results, topo3);
        assertEquals(6, topo3.getRepNodeIds().size());
        assertEquals(0,topo3.getArbNodeIds().size());
    }


    /**
     * Test that alters the allowArbiterHost on SN that
     * hosts an Arbiter. Checks that Rules.validate() generates the
     * correct violation and that rebalance corrects violation.
     */
    @Test
    public void testRebalanceANSNHost() {
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1), 2);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Results results = Rules.validate(topo, params, false);
        assertNumProblems(1, results, topo);
        assertNumProblems(1, UnderCapacity.class, results, topo);
        Set<ArbNodeId> anIds = topo.getArbNodeIds();
        assertTrue(anIds.size() == 1);

        StorageNodeId anSNId = null;
        for (ArbNodeId anId : anIds) {
            ArbNode an = topo.get(anId);
            anSNId = an.getStorageNodeId();
            assertTrue(anSNId.equals(new StorageNodeId(4)));
        }

        /* Set SN param to not host AN */
        StorageNodeParams snp = params.get(anSNId);
        snp.setAllowArbiters(false);
        results = Rules.validate(topo, params, false);
        assertNumProblems(2, results, topo);
        assertNumProblems(1, ANNotAllowedOnSN.class, results, topo);
        assertNumProblems(1, UnderCapacity.class, results, topo);

        /*
         * Do a rebalance and check that the AN moved.
         */
        TopologyCandidate candidate1 = new TopologyCandidate("second", topo);
        TopologyBuilder tb1 =
            new TopologyBuilder(candidate1, makePool(topo), params,
                                atc.getParams());
        TopologyCandidate fixed = tb1.rebalance(null);
        Topology topo2 = fixed.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(1, results, topo);
        assertNumProblems(1, UnderCapacity.class, results, topo);
    }

    /*
     * Tests AN zone selection if there are two zero RF zones that
     * can host ANs.
     */
    @Test
    public void testSelectRFZeroZoneWithMoreArbHostingSns() {
        /*
         * Create a topology with 3 Zones.
         * ZN1: 2 SNs (SN1-SN2) each capacity 1. Primary zone and allows
         *      arbiters. Zone RF = 2.
         * ZN2: 7 SNs (SN3-SN9) each capacity 0. Primary zone and allows
         *      arbiters. Zone RF = 0.
         * ZN3: 5 SNs (SN10-SN14) each capacity 0. Primary zone and allows
         *      arbiters. Zone RF = 0.
         * Change SN7, SN8, SN9 params to not host arbiters.
         * Build the topology.
         * Result: Arbiters hosted in RF = 0 zone ZN3 since it has more number
         *         of SNs that can host arbiters.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 2);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 7,
               CapGenType.SAME.newInstance(0), 0);
        Datacenter dc3 = addDC(sourceTopo, params, "PrimaryDC",
                               0, DatacenterType.PRIMARY, true);
        addSNs(dc3, sourceTopo, params, snCounter, 5,
               CapGenType.SAME.newInstance(0), 0);
        StorageNodeParams snp = params.get(new StorageNodeId(7));
        snp.setAllowArbiters(false);
        snp = params.get(new StorageNodeId(8));
        snp.setAllowArbiters(false);
        snp = params.get(new StorageNodeId(9));
        snp.setAllowArbiters(false);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(arbNodeId);
            StorageNodeId snId = arbNode.getStorageNodeId();
            assertEquals(3, topo.get(snId).getDatacenterId().getDatacenterId());
        }
    }

    /*
     * Test to insure that Topology has no violations
     * with two zones and all but one SN is configured
     * to host ANs.
     */
    @Test
    public void testArbZoneAtleastOneArbHostingSN() {
        /*
         * Create a topology with 2 Zones.
         * ZN1: 2 SNs (SN1-SN2) each capacity 1.
         *      1 SN (SN3) capacity 0.
         *      Primary zone and allows arbiters.
         *      Zone RF = 1.
         * ZN2: 2 SNs (SN4-SN5) each capacity 1.
         *      1 SN (SN6) capacity 0.
         *      Primary zone and allows arbiters.
         *      Zone RF = 1.
         * Change all sn params to not host arbiters
         * except for the ZN2 SN6.
         * Build the topology.
         * Insure that there are no violations.
         */
        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               1, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);
        addSNs(dc1, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);
        Datacenter dc2 = addDC(sourceTopo, params, "PrimaryDC1",
                               1, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(1), 1);
        addSNs(dc2, sourceTopo, params, snCounter, 1,
               CapGenType.SAME.newInstance(0), 0);
        StorageNodeParams snp = params.get(new StorageNodeId(1));
        snp.setAllowArbiters(false);
        snp = params.get(new StorageNodeId(2));
        snp.setAllowArbiters(false);
        snp = params.get(new StorageNodeId(3));
        snp.setAllowArbiters(false);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();
        for (ArbNodeId arbNodeId : topo.getArbNodeIds()) {
            ArbNode arbNode = topo.get(arbNodeId);
            StorageNodeId snId = arbNode.getStorageNodeId();
            assertEquals(2, topo.get(snId).getDatacenterId().getDatacenterId());
        }
    }

    /**
     * Test redistribution with arbiters. Create topo, add SNs in order
     * to support more shards. Call topo.build() with existing topo
     * (redistribute). Check that new shards with ANs are created.
     */
    @Test
    public void testRedistribute() {
        /*
         * Create a topology with 3 shards, a primary data center with RF=2 and
         * 3 SNs with capacity=2
         */
        final Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                                     2, DatacenterType.PRIMARY, true);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(2), 2);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        assertEquals(3, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(6, topo.getRepNodeIds().size());
        assertEquals(3,topo.getArbNodeIds().size());
        Results validation = Rules.validate(topo, params, false);
        assertNumViolations(0, validation, topo);

        addSNs(dc1, topo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 0);

        Results results = Rules.validate(topo, params, false);

        assertNumProblems(2, results, topo);
        assertNumProblems(2, UnderCapacity.class, results, topo);

        TopologyCandidate candidate1 = new TopologyCandidate("start", topo);
        TopologyBuilder tb1 =
            new TopologyBuilder(candidate1, makePool(topo), params,
                                atc.getParams());
        TopologyCandidate fixed = tb1.build();
        Topology topo2 = fixed.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(0, results, topo2);
        assertEquals(8, topo2.getRepNodeIds().size());
        assertEquals(4,topo2.getArbNodeIds().size());
    }

    /**
     * Test that exercises changing an Arbiter configured topology to
     * one that does not contain Arbiters by increasing the rep factor. This
     * test uses two datacenter. The DC that does not host AN's rep factor
     * is increased. This causes the topology to have ExcessAN warnings. The
     * DC hosting the AN's is then rebalanced to eliminate the warnings.
     */
    @Test
    public void testChangeRFRemoveAN() {

        /*
         * Create a topology with 1 shards, a primary data center with RF=1 and
         * 3 SNs with capacity=1
         */
        final Datacenter dc1 =
            addDC(sourceTopo, params, "PrimaryDC",
                  1, DatacenterType.PRIMARY, false);
        addSNs(dc1, sourceTopo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1), 1);
        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        final TopologyCandidate candidate = tb.build();
        final Topology topo = candidate.getTopology();
        assertEquals(3, topo.getRepGroupMap().size());
        assertEquals(3, topo.getStorageNodeMap().size());
        assertEquals(3, topo.getRepNodeIds().size());
        assertEquals(0,topo.getArbNodeIds().size());
        Results results = Rules.validate(topo, params, false);
        assertNumViolations(0, results, topo);

        /* Add another datacenter that supports arbiters and 3 SNs */
        final Datacenter dc2 =
            addDC(topo, params, "PrimaryDC2",
                  1, DatacenterType.PRIMARY, true);

        addSNs(dc2, topo, params, snCounter, 3,
               CapGenType.SAME.newInstance(1), 0);

        TopologyCandidate candidate2 = new TopologyCandidate("second", topo);
        TopologyBuilder tb2 =
            new TopologyBuilder(candidate2, makePool(topo),
                                params, atc.getParams());
        TopologyCandidate candidate3 =  tb2.build();
        Topology topo2 = candidate3.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(0, results, topo2);
        assertEquals(6, topo2.getRepNodeIds().size());
        assertEquals(3,topo2.getArbNodeIds().size());

        addSNs(dc1, topo2, params, snCounter, 3,
               CapGenType.SAME.newInstance(1), 1);
        TopologyCandidate candidate4 = new TopologyCandidate("third", topo2);
        TopologyBuilder tb3 =
            new TopologyBuilder(candidate4, makePool(topo2),
                                params, atc.getParams());
        TopologyCandidate candidate5 =
            tb3.changeRepfactor(2, dc1.getResourceId());
        Topology topo3 = candidate5.getTopology();
        results = Rules.validate(topo3, params, false);
        assertNumProblems(3, results, topo3);
        assertEquals(9, topo3.getRepNodeIds().size());
        assertEquals(3,topo3.getArbNodeIds().size());

        TopologyBuilder tb4 =
            new TopologyBuilder(candidate5, makePool(topo3),
                                params, atc.getParams());
        TopologyCandidate candidate6 = tb4.rebalance(dc2.getResourceId());
        Topology topo4 = candidate6.getTopology();
        results = Rules.validate(topo4, params, false);
        assertNumProblems(0, results, topo4);
        assertEquals(9, topo4.getRepNodeIds().size());
        assertEquals(0,topo4.getArbNodeIds().size());
    }


    /**
     * Tests redistribute that requires an AN to
     * be moved in order for the AN layout to be uniform.
     * Initially
     *  DC1(no arbiters) SN1(rg1-rn1) SN2(rg2-rn1)
     *  DC2(arbiters) SN3(rg1-rn2)(rg2-an1) SN4(rg2-rn2)(rg1-an1)
     *
     *  Add 1 SN to DC1 and DC2.
     *  After redistribute:
     *  DC1(no arbiters) SN1(rg1-rn1) SN2(rg2-rn1), SN5(rg3-rn1)
     *  DC2(arbiters) SN3(rg1-rn2)(rg3-an1) SN4(rg2-rn2)(rg1-an1)
     *                SN6(rg3-rn2)(rg2-an1)
     *
     *  Notice that an an (in this case rg2-an1) was moved from SN3 to
     *  SN6.
     */
    @Test
    public void testRebalanceMoveAN() {

        Datacenter dc1 = addDC(sourceTopo, params, "PrimaryDC",
                               1, DatacenterType.PRIMARY, false);
        addSNs(dc1, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);

        Datacenter dc2 = addDC(sourceTopo, params, "newDC",
                               1, DatacenterType.PRIMARY, true);
        addSNs(dc2, sourceTopo, params, snCounter, 2,
               CapGenType.SAME.newInstance(1), 1);

        TopologyBuilder tb =
            new TopologyBuilder(sourceTopo, CANDIDATE_NAME,
                                makePool(sourceTopo), 1000, params,
                                atc.getParams());
        TopologyCandidate candidate = tb.build();
        Topology topo = candidate.getTopology();

        Results results = Rules.validate(topo, params, false);

        assertNumProblems(0, results, topo);

        addSNs(dc1, topo, params, snCounter, 1,
                CapGenType.SAME.newInstance(1), 0);
        addSNs(dc2, topo, params, snCounter, 1,
                CapGenType.SAME.newInstance(1), 0);

        /*
         * Do a redistribute and check that AN were created
         */
        TopologyCandidate candidate1 = new TopologyCandidate("second", topo);
        TopologyBuilder tb1 =
            new TopologyBuilder(candidate1, makePool(topo),
                                params, atc.getParams());
        TopologyCandidate fixed = tb1.build();
        Topology topo2 = fixed.getTopology();
        results = Rules.validate(topo2, params, false);
        assertNumProblems(0, results, topo2);
        assertEquals(6, topo2.getRepNodeIds().size());
        assertEquals(3,topo2.getArbNodeIds().size());

        List<Integer> expectedSNIDs = new ArrayList<Integer>();
        expectedSNIDs.add(3);
        expectedSNIDs.add(4);
        expectedSNIDs.add(6);
        for (ArbNodeId anId : topo2.getArbNodeIds()) {
            ArbNode arbNode = topo2.get(anId);
            Integer snId =
                Integer.valueOf(arbNode.getStorageNodeId().getStorageNodeId());
            if (expectedSNIDs.contains(snId)) {
                expectedSNIDs.remove(snId);
            }
        }
        assertTrue(expectedSNIDs.isEmpty());
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
        return addDC(topo, parameters, dcName, repFactor,
                     datacenterType, false /* allowArbiters */);

    }

    private Datacenter addDC(
        final Topology topo,
        final Parameters parameters,
        final String dcName,
        final int repFactor,
        final DatacenterType datacenterType,
        final boolean allowArbiters) {

        final Datacenter dc = topo.add(
            Datacenter.newInstance(dcName, repFactor,
                                   datacenterType, allowArbiters, false));
        parameters.add(new DatacenterParams(dc.getResourceId(), "Lexington"));
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
     * specify mount points.
     */
    @SuppressWarnings("hiding")
    private void addSNs(Datacenter dc,
                        Topology topo,
                        Parameters params,
                        AtomicInteger snCounter,
                        int numSNs,
                        CapacityGenerator capGen,
                        int numAdmins,
                        MountPointGenerator mountPointGenerator) {
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

            final String[] mountPoints = (mountPointGenerator != null) ?
                mountPointGenerator.getMountPoints(capacity) :
                null;
            if (mountPoints != null) {
                for (final String mountPoint : mountPoints) {
                    snp.setStorageDirMap(
                       StorageNodeParams.changeStorageDirMap(params,
                                                             snp, true,
                                                             mountPoint, null));
                }
            }
            params.add(snp);

            if (numAdmins > 0) {
                numAdmins--;
                params.add(new AdminParams(
                               new AdminId(params.getAdminCount() + 1),
                               sn.getResourceId(),
                               getAdminType(dc),
                               snp.getRootDirPath()));
            }
        }
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
                 TopologyPrinter.printTopology(topo));
        }
    }

    /**
     * Assert that the specified number of rule problems occurred.
     */
    private static void assertNumProblems(final int expectedNumProblems,
                                          final Results validation,
                                          final Topology topo ) {
        final Iterator<RulesProblem> iter = validation.getWarnings().iterator();
        while (iter.hasNext()) {
            if (iter.next() instanceof MissingRootDirectorySize) {
                iter.remove();
            }
        }

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
     * Return the mount points for the specified SN with the given capacity.
     * Can return null if no mount points are desired.
     */
    static class MountPointGenerator {
        private int next = 0;
        String[] getMountPoints(int capacity) {
            final String[] result = new String[capacity];
            for (int i = 0; i < capacity; i++) {
                File path =
                    new File(File.separator + "mount-point-" + next++);
                result[i] = path.getAbsolutePath();
            }
            return result;
        }
    }
}
