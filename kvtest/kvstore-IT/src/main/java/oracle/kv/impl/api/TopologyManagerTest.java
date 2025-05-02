/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.ref.WeakReference;

import oracle.kv.StaleStoreHandleException;
import oracle.kv.TestBase;
import oracle.kv.impl.api.TopologyManager.PostUpdateListener;
import oracle.kv.impl.api.TopologyManager.PreUpdateListener;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.TopologyTest;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;

import org.junit.Test;

/**
 * Topology manager tests.
 */
public class TopologyManagerTest extends TestBase {

    static final PartitionId p1Id = new PartitionId(1);
    static final PartitionId p2Id = new PartitionId(2);
    static final PartitionId p6Id = new PartitionId(6);
    static final PartitionId p7Id = new PartitionId(6);

    static final RepGroupId rg1Id = new RepGroupId(1);
    static final RepGroupId rg2Id = new RepGroupId(2);
    static final RepGroupId rg3Id = new RepGroupId(3);

    /**
     * Verify that topology mismatches are detected and the correct exceptions
     * are thrown
     */
    @Test
    public void testTopoIdMismatch() {
        Topology t1 = populateTopology();
        int startSeqNum = t1.getSequenceNumber();

        final TopologyManager tm =
            new TopologyManager(t1.getKVStoreName(), 100, logger);

        assertTrue(tm.update(t1.getCopy()));

        t1.add(Datacenter.newInstance("EC-datacenter", 2,
                                      DatacenterType.PRIMARY, false,
                                      false));

        /*
         * Change t1's topo id to be different. This might happen in real
         * life if a client that was talking to a now-defunct store attempts
         * to contact the a new store that's been constructed in its place.
         * If this happens, we should refuse to let this old client update
         * this store's topology, but we should only issue an
         * OperationFaultException
         */
        t1.setId(t1.getId()+1);

        try {
            tm.update(new TopologyInfo(t1.getId(),
                                       t1.getSequenceNumber(),
                                       t1.getChanges(startSeqNum),
                                       t1.getSignature()));
            fail("expected exception");
        } catch (WrappedClientException expected) {
            /* Expected. */
            assertEquals(expected.getCause().getClass(),
                         StaleStoreHandleException.class);
        }

        try {
            tm.update(t1);
            fail("expected exception");
        } catch (WrappedClientException expected) {
            /* Expected. */
            assertEquals(expected.getCause().getClass(),
                         StaleStoreHandleException.class);
        }

        t1.setId(Topology.NOCHECK_TOPOLOGY_ID);
        t1.add(Datacenter.newInstance("EC-datacenter", 2,
                                      DatacenterType.PRIMARY, false, false));

        /* No exceptions with special topology id */
        tm.update(new TopologyInfo(t1.getId(),
                                   t1.getSequenceNumber(),
                                   t1.getChanges(startSeqNum),
                                   t1.getSignature()));
        TopologyTest.assertTopoEquals(t1, tm.getTopology());

        tm.update(t1.getCopy());
        TopologyTest.assertTopoEquals(t1, tm.getTopology());
    }

    /**
     * Verify that changes below the configured limit are maintained and
     * pruning started and is maintained after the threshold has been passed.
     */
    @Test
    public void testPruning() {

        Topology t1 = populateTopology();

        final int maxTopoChanges = 10;
        final TopologyManager tm =
            new TopologyManager("kvs1", maxTopoChanges, logger);
        int unrestrictedSize = t1.getChangeTracker().getChanges().size();
        assertTrue(t1.getChangeTracker().getChanges().size() < maxTopoChanges);

        /* Verify no pruning during wholesale topology update. */
        assertTrue(tm.update(t1)); t1 = tm.getTopology();
        assertEquals(unrestrictedSize,
                     t1.getChangeTracker().getChanges().size());

        final Topology t2 = t1.getCopy();


        /* Verify no pruning during incremental topology updates. */
        for (int i=0; i < 10; i++) {

            final int currSeqNum = t1.getChangeTracker().getSeqNum();
            assertEquals(t1.getSequenceNumber(), t2.getSequenceNumber());

            t2.add(Datacenter.newInstance("EC-datacenter" + i, 2,
                                          DatacenterType.PRIMARY, false,
                                          false));
            assertTrue(t1.getSequenceNumber() != t2.getSequenceNumber());

            tm.update(new TopologyInfo(t2.getId(),
                                       t2.getSequenceNumber(),
                                       t2.getChangeTracker().getChanges
                                       (currSeqNum+1),
                                       t2.getSignature()));
            t1 = tm.getTopology();

            assertEquals(t1.getSequenceNumber(), t2.getSequenceNumber());

            /* Verify no pruning */
            assertEquals(t2.getChangeTracker().getChanges().size(),
                         t1.getChangeTracker().getChanges().size());

            if (t2.getChangeTracker().getChanges().size() == maxTopoChanges) {
                break;
            }
        }

        /* Verify pruning during incremental topology updates. */
        for (int i=0; i < 10; i++) {

            final int currSeqNum = t1.getChangeTracker().getSeqNum();
            assertEquals(t1.getSequenceNumber(), t2.getSequenceNumber());

            t2.add(Datacenter.newInstance("EC-datacenter" + i, 2,
                                          DatacenterType.PRIMARY, false,
                                          false));
            assertTrue(t1.getSequenceNumber() != t2.getSequenceNumber());

            tm.update(t2.getId(),
                      t2.getChangeTracker().getChanges(currSeqNum+1),
                      t2.getSignature());
            t1 = tm.getTopology();

            assertEquals(t1.getSequenceNumber(), t2.getSequenceNumber());

            assertEquals(maxTopoChanges,
                         t1.getChangeTracker().getChanges().size());
        }

        /**
         * Verify that the last change is always in its entirety, even if it
         * the configured topo retention.
         */

        int currSeqNum = t1.getChangeTracker().getSeqNum();
        for (int i=0; i < maxTopoChanges*2; i++) {
            t2.add(new RepGroup());
        }
        tm.update(t2.getId(), t2.getChangeTracker().getChanges(currSeqNum+1),
                  t2.getSignature());
        t1 = tm.getTopology();
        assertEquals(maxTopoChanges * 2,
                     t1.getChangeTracker().getChanges().size());

        /* Verify that it's back to normal after the next change. */
        currSeqNum = t1.getChangeTracker().getSeqNum();
        t2.add(new RepGroup());
        tm.update(t2.getId(), t2.getChangeTracker().getChanges(currSeqNum+1),
                  t2.getSignature());
        t1 = tm.getTopology();
        assertEquals(maxTopoChanges,
                     t1.getChangeTracker().getChanges().size());
    }


    /**
     * Verify that changes above the configured limit are pruned when a
     * an entire topology is added.
     */
    @Test
    public void testInitialWholesalePruning() {

        Topology t1 = populateTopology();

        final int maxTopoChanges = 4;
        final TopologyManager tm =
            new TopologyManager("kvs1", maxTopoChanges, logger);

        assertTrue(t1.getChangeTracker().getChanges().size() > maxTopoChanges);
        /* Verify pruning during wholesale topology update. */
        assertTrue(tm.update(t1)); t1 = tm.getTopology();
        assertEquals(maxTopoChanges,
                     t1.getChangeTracker().getChanges().size());
    }

    /* Populate a Topology with some topo elements. */
    private Topology populateTopology() {
        Topology t1 = new Topology("t1");
        Datacenter dc1 = t1.add(
            Datacenter.newInstance("EC-datacenter", 2,
                                   DatacenterType.PRIMARY, false, false));

        StorageNode sn1 =
            t1.add(new StorageNode(dc1,
                                   "sn1-hostname",
                                   TestUtils.DUMMY_REGISTRY_PORT));
        StorageNode sn2 = t1.add
            (new StorageNode(dc1,
                             "sn2-hostname",
                             TestUtils.DUMMY_REGISTRY_PORT));
        RepGroup rg1 = t1.add(new RepGroup());

        rg1.add(new RepNode(sn1.getResourceId()));
        rg1.add(new RepNode(sn2.getResourceId()));

        t1.add(new Partition(rg1));
        t1.add(new Partition(rg1));
        return t1;
    }

    /**
     * Verify that invalid partition changes are detected by the TM and that
     * partition changes that are consistent with migrations are allowed. The
     * test simulates migrations by setting a "local topology" explicitly via
     * the setLocalTopology below.
     */
    @Test
    public void testPartitionUpdateChanges() {
        final Topology t = new Topology("t1");
        final int repFactor = 2;
        Datacenter dc1 = t.add(
            Datacenter.newInstance("EC-datacenter", repFactor,
                                   DatacenterType.PRIMARY, false, false));

        t.add(new StorageNode(dc1,"sn1-hostname",
                               TestUtils.DUMMY_REGISTRY_PORT));
        addRG(t);
        addRG(t);

        /* Add 10 partitions, 5/RG */
        final int nPartitions = 10;
        for (int i=1; i < nPartitions; i++) {
            t.add(new Partition(new RepGroupId((i/5) + 1)));
        }

        final TopologyManager tmrg1 =
            new TopologyManager("kvs1", 10000, logger);

        tmrg1.addPreUpdateListener(new MockUpdateListener(rg1Id, tmrg1));
        tmrg1.update(t.getCopy());

        int seqNum = t.getSequenceNumber();
        addRG(t);

        /* redistribute 3 partitions to the new rg 3 */

        t.update(p1Id, new Partition(rg3Id));
        t.update(p2Id, new Partition(rg3Id));
        t.update(p6Id, new Partition(rg3Id));

        /* Removal of partitions from rg1 and rg2 */
        try {
            tmrg1.update(t.getId(), t.getChanges(seqNum + 1), t.getSignature());
            fail("expected ISE");
        } catch (IllegalStateException ise) {
            /* Expected, partition movement in the absence of elasticity. */
            // System.err.println(ise.getMessage());
        }

        Topology tbad = t.getCopy();
        tbad.update(p2Id, new Partition(rg1Id));
        tmrg1.setLocalTopology(tbad);
        try {
            tmrg1.update(t.getId(), t.getChanges(seqNum + 1), t.getSignature());
            fail("expected ISE");
        } catch (IllegalStateException ise) {
            /* Current and new Topology inconsistent with local topo */
            // System.err.println(ise.getMessage());
        }

        /* Simulate partition migration, allowing change to go through */
        tmrg1.setLocalTopology(t);
        tmrg1.update(t.getId(), t.getChanges(seqNum + 1), t.getSignature());
        tmrg1.setLocalTopology(null); /* End partition migration. */

        /* Move p2 back to rg1. */
        seqNum = t.getSequenceNumber();
        t.update(p2Id, new Partition(rg1Id));

        /* Check that gain in partition, without elasticity is rejected. */
        try {
            tmrg1.update(t.getId(), t.getChanges(seqNum + 1), t.getSignature());
            fail("expected ISE");
        } catch (IllegalStateException ise) {
            /* Expected, partition movement in the absence of elasticity. */
            // System.err.println(ise.getMessage());
        }

        tbad = t.getCopy();
        tbad.update(p2Id, new Partition(rg2Id));
        tmrg1.setLocalTopology(tbad);

        try {
            tmrg1.update(t.getId(), t.getChanges(seqNum + 1), t.getSignature());
            fail("expected ISE");
        } catch (IllegalStateException ise) {
            /* Local topology disagrees with new official topology. */
            // System.err.println(ise.getMessage());
        }

        /* Simulate partition migration, allowing change to go through */
        tmrg1.setLocalTopology(t);
        tmrg1.update(t.getId(), t.getChanges(seqNum + 1), t.getSignature());
        tmrg1.setLocalTopology(null); /* End partition migration. */
    }

    /**
     * Verify that a weak listener is cleaned out when the caller removes its
     * reference to it.
     */
    @Test
    public void testPostListenerCleanup() {
        final Topology t = new Topology("kvs1");
        final Datacenter dc1 = t.add(
                 Datacenter.newInstance("EC-datacenter", 2,
                                        DatacenterType.PRIMARY, false, false));
        final int maxTopoChanges = 4;
        final TopologyManager tm =
            new TopologyManager("kvs1", maxTopoChanges, logger);

        PostUpdateListener listener = new MockUpdateListener(null, null);
        tm.addPostUpdateListener(listener, true);

        try {
            tm.update(t);
            fail("expected UOE");
        } catch (UpdateListenerException ule) {
            /* mock listner postUpdate() throws ule */
        }

        /*
         * Keep a weak reference to the listner, clear our reference, then gc.
         */
        final WeakReference<PostUpdateListener> listenerRef =
                                new WeakReference<PostUpdateListener>(listener);
        listener = null;

        /*
         * The System.gc method is not guaranteed to perform a full GC or to
         * clear out weak references, so loop a few times hoping to clear it.
         */
        boolean collected = new PollCondition(10, 6000) {
            @Override
            protected boolean condition() {
                System.gc();
                return listenerRef.get() == null;
            }
        }.await();

        assertTrue("This failure may be due to Java GC not clearing a weak " +
                   "reference. Rerun to see if the problem persist", collected);

        t.add(new StorageNode(dc1,
                              "sn1-hostname",
                              TestUtils.DUMMY_REGISTRY_PORT));

        /* Update should work if the listener has been GCed */
        tm.update(t);
    }

    private RepGroup addRG(Topology t1) {
        final StorageNode sn1 = t1.get(new StorageNodeId(1));
        final RepGroup rg = t1.add(new RepGroup());

        rg.add(new RepNode(sn1.getResourceId()));
        rg.add(new RepNode(sn1.getResourceId()));
        return rg;
    }

    private static class MockUpdateListener implements PreUpdateListener,
                                                PostUpdateListener {
        final RepGroupId rgId;
        final TopologyManager tm;

        MockUpdateListener(RepGroupId rgId, TopologyManager tm) {
            super();
            this.rgId = rgId;
            this.tm = tm;
        }

        @Override
        public void preUpdate(Topology topology) {
            tm.checkPartitionChanges(rgId, topology);
        }

        @Override
        public boolean postUpdate(Topology topology) {
            throw new UpdateListenerException();
        }
    }

    /**
     * Test specific exception.
     */
    private static class UpdateListenerException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
