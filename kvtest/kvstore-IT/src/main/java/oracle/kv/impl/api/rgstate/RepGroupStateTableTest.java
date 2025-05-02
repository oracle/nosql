/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.rgstate;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import oracle.kv.Consistency;
import oracle.kv.UncaughtExceptionTestBase;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.Response;
import oracle.kv.impl.api.StatusChanges;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.SerialVersion;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.impl.node.NameIdPair;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test state maintenance at the table level. There are dedicated group and
 * node level unit tests as well.
 */
@RunWith(FilterableParameterized.class)
public class RepGroupStateTableTest extends UncaughtExceptionTestBase {

    int repFactor = 3;
    private Topology topology;
    private RepGroupState rg1s;
    private final RepGroupId rg1Id = new RepGroupId(1);
    private final RepNodeId rg1n1Id = new RepNodeId(1,1);
    private final RepNodeId rg1n2Id = new RepNodeId(1,2);
    private RepGroupStateTable rgst;
    private final boolean async;

    public RepGroupStateTableTest(boolean async) {
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

        super.setUp();
        topology = TopoUtils.create("test", 1, 1, repFactor, 10);
        rgst = new RepGroupStateTable(rg1n1Id, async, logger);
        rgst.postUpdate(topology);
        rg1s = rgst.getGroupState(rg1Id);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Test updates through state change events, that is, simulate updates
     * through a replicated environment Listener.
     */
    @Test
    public void testUpdateStateChangeEvent() {
        StateChangeEvent event =
            new StateChangeEvent(State.MASTER,
                                 new NameIdPair(rg1n1Id.getFullName(), 1));
        rgst.update(event);

        RepNodeId mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n1Id, mId);

        /* Obsolete change event, ignore it. */
        event = new StateChangeEvent(State.REPLICA,
                                     new NameIdPair(rg1n2Id.getFullName(), 0));
        rgst.update(event);
        assertEquals(rg1n1Id, mId);

        /*
         * The granularity for discriminating between state changes is a ms,
         * sleep 1 ms to enforce the discrimination 
         */
        try {
            Thread.sleep(1);
        } catch (InterruptedException ex) {
        }

        /* replica change event to update master state */
        event = new StateChangeEvent(State.REPLICA,
                                     new NameIdPair(rg1n2Id.getFullName(), 2));
        rgst.update(event);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n2Id, mId);
    }

    /**
     * Test status updates via response(
     */
    @Test
    public void testUpdateResponse() {
        StatusChanges statusChanges =
            new StatusChanges(State.MASTER, rg1n1Id, 1);
        Get gop = new Get(new byte[0]);
        Request request = new Request(gop,
                                      new PartitionId(1),
                                      false, null,
                                      Consistency.NONE_REQUIRED, 3, 1,
                                      new RepNodeId(1,1), 100, null);
        long respVLSN = 1;
        Response response = new Response(rg1n1Id,
                                         respVLSN,
                                         null, null, statusChanges,
                                         SerialVersion.CURRENT);
        rgst.update(request, response, 0);
        RepNodeId mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n1Id, mId);
        assertEquals(respVLSN, rg1s.get(rg1n1Id).getVLSN());

        /* Obsolete status change, ignore it. */
        statusChanges =  new StatusChanges(State.REPLICA, rg1n2Id, 0);
        response = new Response(rg1n1Id,
                                INVALID_VLSN, null, null, statusChanges,
                                SerialVersion.CURRENT);
        rgst.update(request, response, 0);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n1Id, mId);

        statusChanges =  new StatusChanges(State.REPLICA, rg1n2Id, 2);
        response = new Response(rg1n1Id,
                                INVALID_VLSN, null, null, statusChanges,
                                SerialVersion.CURRENT);
        rgst.update(request, response, 0);
        mId = rg1s.getMaster().getRepNodeId();
        assertEquals(rg1n2Id, mId);
    }

    /**
     * Test that the RepGroupStateTable updates RN information correctly.
     */
    @Test
    public void testUpdateTopo() {
        topology = TopoUtils.create("test", 2, 2, repFactor, 10);
        rgst = new RepGroupStateTable(rg1n1Id, async, logger);

        for (int i = 0; i < 3; i++) {
            rgst.postUpdate(topology);

            /* Check state table for all RNs in the topology */
            RepNodeId lastRnId = null;
            for (final RepGroup rg : topology.getRepGroupMap().getAll()) {
                final RepGroupId rgId = rg.getResourceId();
                final RepGroupState rgState = rgst.getGroupState(rgId);
                assertNotNull("Get RepGroupState for " + rgId, rgState);
                for (final RepNode rn : rg.getRepNodes()) {
                    final RepNodeId rnId = rn.getResourceId();
                    final RepNodeState rnState = rgState.get(rnId);
                    assertNotNull("Get RepNode for " + rnId, rnState);
                    final Datacenter dc = topology.getDatacenter(rnId);
                    assertEquals("Get RepNodeState zone for " + rnId,
                                 dc.getResourceId(), rnState.getZoneId());
                    lastRnId = rnId;
                }
            }

            assertEquals("Check number of RNs in state table",
                         topology.getRepNodeIds().size(),
                         rgst.getRepNodeStates().size());

            /*
             * Remove an RN to make sure the change is reflected in the
             * RepGroupStateTable
             */
            topology.remove(lastRnId);
        }
    }
}
