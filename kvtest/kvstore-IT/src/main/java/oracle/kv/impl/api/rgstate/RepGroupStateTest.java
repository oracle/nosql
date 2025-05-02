/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.rgstate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.Version;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.FilterableParameterized;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(FilterableParameterized.class)
public class RepGroupStateTest extends UncaughtExceptionTestBase {

    int repFactor = 3;
    private Topology topology;
    private RepGroupState rg1s;
    private RepGroup rg1 ;

    private final RepGroupId rg1Id = new RepGroupId(1);
    private final RepNodeId rg1n1Id = new RepNodeId(1,1);
    private final RepNodeId rg1n2Id = new RepNodeId(1,2);
    private final boolean async;

    public RepGroupStateTest(boolean async) {
        this.async = async;
    }

    @Parameters(name="async={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Override
    public void setUp()
        throws Exception {

        RepNodeState.RATE_INTERVAL_MS = 2000;
        rg1s = new RepGroupState(rg1Id, null /* trackerId */, async, logger);
        topology = TopoUtils.create("test", 1, 1, repFactor, 10);
        rg1 = topology.get(rg1s.getResourceId());

        assertEquals(0, rg1s.getRepNodeStates().size());

        /* Should now have three rn status entries. */
        rg1s.update(rg1, topology);
        assertEquals(repFactor, rg1s.getRepNodeStates().size());
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Test basic operations on a rep group
     */
    @Test
    public void testInitialState() {

        /* Initial state should be Replica. */
        for (RepNodeState rns : rg1s.getRepNodeStates()) {
            assertEquals(State.REPLICA, rns.getRepState());
        }
        assertNull(rg1s.getMaster());
    }

    @Test
    public void testStateUpdate() {

        assertNull(rg1s.getMaster());
        rg1s.update(rg1n1Id, rg1n1Id, State.MASTER, 2);

        RepNodeId mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n1Id, mId);

        /* Obsolete change event, ignore it. */
        rg1s.update(rg1n2Id, rg1n2Id, State.MASTER, 1);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n1Id, mId);

        /* Master change event to update master state */
        rg1s.update(rg1n2Id, rg1n2Id, State.MASTER, 3);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n2Id, mId);

        /* Replica change event to update master state. */
        RepNodeId rg1n3Id = new RepNodeId(1,3);
        rg1s.update(rg1n2Id, rg1n3Id, State.REPLICA, 4);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n3Id, mId);

        /*
         * Local state change event to DETACHED or UNKNOWN, should not impact
         * group master state.
         */
        rg1s.update(rg1n2Id, null, State.UNKNOWN, 5);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n3Id, mId);
    }

    @Test
    public void testGetLoadBalancedRN() {

        /* Assign increasing activity counts. */
        int activityCount = 0;
        for (RepNodeState rns : rg1s.getRepNodeStates()) {
            activityCount++;
            for (int i=0; i < activityCount; i++) {
                rns.requestStart();
            }
        }

        Request dummyRequest = new Request();

        /* Exclude them one at a time. */
        Set<RepNodeId> excludeRNs = new HashSet<RepNodeId>();
        for (RepNodeState rns : rg1s.getRepNodeStates()) {

            RepNodeState lbrn =
                rg1s.getLoadBalancedRN(dummyRequest, excludeRNs);

            /*
             * RNs are ordered by increasing activity, so we should always get
             * the current entry in the list, since it is the least busy of the
             * remaining RNs.
             */
            assertSame(rns, lbrn);
            excludeRNs.add(rns.getRepNodeId());
        }

        /* all rns excluded. */
        RepNodeState lbrn = rg1s.getLoadBalancedRN(dummyRequest, excludeRNs);
        assertNull(lbrn);
    }

    /**
     * Test for: [#27490] Dispatch to random RN with same active counts &
     * response times
     */
    @Test
    public void testGetLoadBalancedRNSameCounts() {

        /* Leave activity and response times at zero so all nodes are equal */

        /* Collect all RN IDs */
        final Set<RepNodeId> randomRNs = new HashSet<RepNodeId>();
        for (final RepNodeState rns : rg1s.getRepNodeStates()) {
            randomRNs.add(rns.getRepNodeId());
        }

        /* Make load-balanced requests and make sure all RNs are returned */
        final Request dummyRequest = new Request();
        for (int i = 0;
             !randomRNs.isEmpty() && (i < (1000 * repFactor));
             i++)
        {
            final RepNodeState loadBalanced =
                rg1s.getLoadBalancedRN(dummyRequest, null);
            if (loadBalanced != null) {
                randomRNs.remove(loadBalanced.getRepNodeId());
            }
        }

        assertEquals(0, randomRNs.size());
    }

    @Test
    public void testConsistencyScreening() throws InterruptedException {

        for (RepNodeState rns: rg1s.getRepNodeStates()) {
            /* Update with vlsns -- higher node nums get higher vlsns. */
            rns.updateVLSN(rns.getRepNodeId().getNodeNum());
        }
        int vlsnAndRepNodeId = 3;
        Version version = new Version(new UUID(32, 32), vlsnAndRepNodeId);

        Consistency.Version consistency =
            new Consistency.Version(version, 5, TimeUnit.SECONDS);

        Request request = new Request(new Get(new byte[0]),
                                      new PartitionId(1),
                                      false,
                                      null,
                                      consistency,
                                      1,
                                      100,
                                      new ClientId(1),
                                      6000,
                                      null);

        RepNodeState lbrn = rg1s.getLoadBalancedRN(request, null);

        /*
         * Verify that we pick the only node that can satisfy the consistency
         * requirement.
         */
        assert ((lbrn != null) &&
                (lbrn.getRepNodeId().getNodeNum() == vlsnAndRepNodeId));

        version = new Version(new UUID(32, 32), vlsnAndRepNodeId + 2);
        consistency =
            new Consistency.Version(version, 5, TimeUnit.SECONDS);

        request = new Request(new Get(new byte[0]),
                              new PartitionId(1),
                              false,
                              null,
                              consistency,
                              1,
                              100,
                              new ClientId(1),
                              6000,
                              null);
        RepNodeState nullrn = rg1s.getLoadBalancedRN(request, null);

        /* All nodes disqualified based upon VLSN consistency requirements. */
        assert(nullrn == null);

        /* Sleep long enough for rate calculation to be updated. */
        Thread.sleep(2 * RepNodeState.RATE_INTERVAL_MS);
        lbrn.updateVLSN(vlsnAndRepNodeId + RepNodeState.RATE_INTERVAL_MS * 10);

        version = new Version(new UUID(32, 32), vlsnAndRepNodeId + 2);
        consistency =
            new Consistency.Version(version, 5, TimeUnit.SECONDS);

        request = new Request(new Get(new byte[0]),
                              new PartitionId(1),
                              false,
                              null,
                              consistency,
                              1,
                              100,
                              new ClientId(1),
                              6000,
                              null);
        lbrn = rg1s.getLoadBalancedRN(request, null);
        assert(lbrn != null);
    }

    @Test
    public void testGetRandomRN() {

        Set<RepNodeId> randomRNs = new HashSet<RepNodeId>();

        for (RepNodeState rns : rg1s.getRepNodeStates()) {
            randomRNs.add(rns.getRepNodeId());
        }

        for (int i=0;
             (randomRNs.size() > 0) && (i < (1000 * repFactor)); i++) {
            randomRNs.remove(
                rg1s.getRandomRN(
                    null, new HashSet<RepNodeId>()).getRepNodeId());
        }

        assertEquals(0,randomRNs.size());
    }

    /**
     * Simulates the update sequence during a network partition.
     */
    @Test
    public void testNetworkPartition() {
        rg1s.update(rg1n1Id, rg1n1Id, State.MASTER, 1);
        RepNodeId mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n1Id, mId);

        /* A network partition. */
        rg1s.update(rg1n2Id, rg1n2Id, State.MASTER, 2);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n2Id, mId);

        /* verify that it resulted in rg1n1 transitioning to a replica */
        assertTrue(rg1s.get(rg1n1Id).getRepState().isReplica());

        /* Verify that an older upgrade is ignored. */
        rg1s.update(rg1n1Id, rg1n1Id, State.MASTER, 1);
        assertEquals(rg1n2Id, mId);
        assertEquals(State.REPLICA, rg1s.get(rg1n1Id).getRepState());

    }

    /**
     * Verifies that RepGroupState.inConsistencyRange handles the basic
     * consistency types: NONE_REQUIRED, ABSOLUTE, and NONE_REQUIRED_NO_MASTER.
     * Additionally, acts as a regression test for a logic issue introduced
     * when the NONE_REQUIRED_NO_MASTER consistency policy was added.
     */
    @Test
    public void testBasicConsistencyTypes() {

        /* Establish rg1-rn1 as MASTER */
        rg1s.update(rg1n1Id, rg1n1Id, State.MASTER, 2);

        final Set<RepNodeId> allRNs = new HashSet<RepNodeId>();
        final Set<RepNodeId> excludeRNs = new HashSet<RepNodeId>();

        RepNodeId masterRnId = null;
        for (RepNodeState rns : rg1s.getRepNodeStates()) {
            allRNs.add(rns.getRepNodeId());
            if (State.MASTER.equals(rns.getRepState())) {
                masterRnId = rns.getRepNodeId();
            }
        }

        /* Exclude all but the master to verify that NONE_REQUIRED_NO_MASTER
         * consistency does not return the master.
         */
        for (RepNodeId rnId : allRNs) {
            if (!rnId.equals(masterRnId)) {
                excludeRNs.add(rnId);
            }
        }

        @SuppressWarnings("deprecation")
        Request request = new Request(new Get(new byte[0]),
                                      new PartitionId(1),
                                      false,
                                      null,
                                      Consistency.NONE_REQUIRED_NO_MASTER,
                                      1,
                                      100,
                                      new ClientId(1),
                                      6000,
                                      null);

        RepNodeState targetRn = rg1s.getLoadBalancedRN(request, excludeRNs);
        String targetState = null;
        String targetId = null;
        if (targetRn != null) {
            targetState = targetRn.getRepState().toString();
            targetId = targetRn.getRepNodeId().toString();
        }
        assertNull("NONE_REQUIRED_NO_MASTER consistency requested" +
                   "with all but master excluded, so null return value " +
                   "expected. But non-null " + targetState + " node " +
                   "returned instead [id=" + targetId + "].", targetRn);

        /* Exclude the master but not the replicas to verify that ABSOLUTE
         * consistency does not return a replica.
         */
        excludeRNs.clear();
        for (RepNodeId rnId : allRNs) {
            if (rnId.equals(masterRnId)) {
                excludeRNs.add(rnId);
            }
        }

        request = new Request(new Get(new byte[0]),
                              new PartitionId(1),
                              false,
                              null,
                              Consistency.ABSOLUTE,
                              1,
                              100,
                              new ClientId(1),
                              6000,
                              null);
        targetRn = rg1s.getLoadBalancedRN(request, excludeRNs);
        if (targetRn != null) {
            targetState = targetRn.getRepState().toString();
            targetId = targetRn.getRepNodeId().toString();
        }
        assertNull("ABSOLUTE consistency requested with the master excluded " +
                   "and only replicas available, so null return value " +
                   "expected. But non-null " + targetState + " node " +
                   "returned instead [id=" + targetId + "].", targetRn);

        /* No nodes are excluded when verifying that NONE_REQUIRED consistency
         * returns either a replica or the master.
         */
        request = new Request(new Get(new byte[0]),
                              new PartitionId(1),
                              false,
                              null,
                              Consistency.NONE_REQUIRED,
                              1,
                              100,
                              new ClientId(1),
                              6000,
                              null);
        targetRn = rg1s.getLoadBalancedRN(request, null);
        assertNotNull("NONE_REQUIRED consistency requested, but null " +
                      "returned when either master or replica expected.",
                      targetRn);
    }
}
