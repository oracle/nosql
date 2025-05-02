/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;


// TODO: test full Topology pull/push amongst RNs
/**
 * Tests to ensure that Topology information is communicated correctly in
 * responses.
 */
public class TopologyInfoTest  extends RepNodeTestBase {

    private static final LoginManager NULL_LOGIN_MGR = null;

    final ClientId clientId = new ClientId(1);
    static {
        RepNodeState.RATE_INTERVAL_MS = 2000;
    }

    /**
     * Verify that a topology with discarded changes (and therefore requiring a
     * full topo pull) seeded at an RN is propagated via the client to all the
     * other nodes via a full pull to the client followed by a push to all the
     * other RNs.
     *
     * So a sample path is:
     * rg1-rn1 --full pull--> c1 --full push--> rg1-rn2,rg1-rn3,...
     */
    @Test
    public void testFullTopoPropagationViaClient()
        throws IOException, NotBoundException {

        final KVRepTestConfig config = new KVRepTestConfig(
            this, 1 /*nDCs*/, 1 /*nSNs*/, 3 /*rf*/, 2 /*nParts*/);

        config.startRepNodeServices();

        shutdownRNStateUpdateThread(config);

        final KVStoreConfig kvConfig = config.getKVSConfig();

        /* Test specifying read zones */
        kvConfig.setReadZones("DC0");

        final KVStoreImpl kvs = (KVStoreImpl)KVStoreFactory.getStore(kvConfig);
        final RequestDispatcherImpl rd =
            (RequestDispatcherImpl)kvs.getDispatcher();

        waitForTopology(config, config.getTopology().getSequenceNumber());

        assertTrue(new PollCondition(10, 60000) {
            @Override
            protected boolean condition() {

                return (rd.getTopologyManager().getTopology() != null) &&
                       (config.getTopology().getSequenceNumber() ==
                        rd.getTopologyManager().getTopology().
                        getSequenceNumber());
            }

        }.await());

        assertArrayEquals("Read zone IDs before new zones added",
                          new int[] { 1 }, rd.getReadZoneIds());

        Topology newTopo = makeAndVerifyTopoChanges(config, true);

        assertEquals(newTopo.getSequenceNumber(),
                     rd.getTopologyManager().getTopology().getSequenceNumber());

        assertTrue(rd.getStateUpdateThread().getPullFullTopologyCount() > 0);

        assertArrayEquals("Read zone IDs after new zones added",
                          new int[] { 1 }, rd.getReadZoneIds());

        kvs.close();
        config.stopRepNodeServices();
    }

    /**
     * Verify that a topology with discarded changes (and therefore requiring a
     * full topo pull/push) seeded at an RN is propagated with its RG.
     */
    @Test
    public void testFullTopoPropagationViaRN()
        throws IOException, NotBoundException {

        final KVRepTestConfig config = new KVRepTestConfig(
            this, 1 /*nDCs*/, 1 /*nSNs*/, 3 /*rf*/, 2 /*nParts*/);

        config.startRepNodeServices();

        /* Verify propagation via RNs no client involved. */
        makeAndVerifyTopoChanges(config, true);
        int pushPullCount = 0;
        for (RequestHandlerImpl rh : config.getRHs()) {
            RepNodeStateUpdateThread sut =
                ((RequestDispatcherImpl)rh.getRequestDispatcher()).
                getStateUpdateThread();
           pushPullCount += sut.getPullFullTopologyCount();
           pushPullCount += sut.getPushFullTopologyCount();
        }

        assertTrue(pushPullCount > 0);
        config.stopRepNodeServices();
    }

    private void shutdownRNStateUpdateThread(final KVRepTestConfig config) {
        for (RepNodeService rns : config.getRepNodeServices()) {
            RequestDispatcherImpl reqDispatcher = (RequestDispatcherImpl)
                rns.getReqHandler().getRequestDispatcher();
            /*
             * Shut down to make sure no topology updates are propagated
             * through RNs.
             */
            reqDispatcher.getStateUpdateThread().shutdown();
        }
    }

    /* Tests that responses contain Topology information as expected. */
    @Test
    public void testBasic () throws IOException {
        final KVRepTestConfig config =
            new KVRepTestConfig(this, 1, 1, 3, 2);

        config.startupRHs();
        final RequestHandlerImpl rg1n1 = config.getRHs().get(0);
        final int topoSeqNumber = config.getTopology().getSequenceNumber();

        /* Matching topology */
        Request request = Request.createNOP(topoSeqNumber, clientId, 10000);
        Response response = rg1n1.execute(request);
        assertNull(response.getTopoInfo());

        /* Simulate a more recent client topology. */
        request = Request.createNOP(topoSeqNumber + 1, clientId, 10000);
        response = rg1n1.execute(request);
        TopologyInfo topoInfo = response.getTopoInfo();
        assertNotNull(topoInfo);
        assertEquals(topoSeqNumber, topoInfo.getSequenceNumber());
        List<TopologyChange> changes = topoInfo.getChanges();
        assertNull(changes);

        /* Simulate a more recent responder topology. */
        request = Request.createNOP(topoSeqNumber - 1, clientId, 10000);
        response = rg1n1.execute(request);
        topoInfo = response.getTopoInfo();
        assertNotNull(topoInfo);
        assertEquals(topoSeqNumber, topoInfo.getSequenceNumber());
        changes = topoInfo.getChanges();
        assertNotNull(changes);
        assertEquals(1, changes.size());
        assertEquals(changes.get(changes.size() - 1).getSequenceNumber(),
                     topoSeqNumber);
        assertEquals(changes.get(0).getSequenceNumber(), topoSeqNumber);
        config.stopRHs();
    }

    /*
     * Verify that a topology update at one node is propagated to all the
     * other nodes in the KVS as well, via the periodic NOP requests sent
     * by the RepNodeStatusUpdateThread in the client.
     */
    @Test
    public void testTopoPropagationViaClient()
        throws IOException, NotBoundException {

        final KVRepTestConfig config = new KVRepTestConfig(
            this, 1 /*nDCs*/, 1 /*nSNs*/, 3 /*rf*/, 2 /*nParts*/);

        config.startRepNodeServices();

        shutdownRNStateUpdateThread(config);

        final KVStoreConfig kvConfig = config.getKVSConfig();
        final KVStoreImpl kvs = (KVStoreImpl)KVStoreFactory.getStore(kvConfig);

        makeAndVerifyTopoChanges(config, false);

        kvs.close();
        config.stopRepNodeServices();
    }

    /*
     * Verify that a topology update at one node in a RG is propagated to the
     * other nodes in the RG via the periodic NOP requests sent by the
     * RepNodeStatusUpdateThread in the RN.
     */
    @Test
    public void testTopoPropagationViaRN()
        throws IOException, NotBoundException {

        final KVRepTestConfig config =
            new KVRepTestConfig(this, 1, 1, 3, 2);

        assertEquals(1, config.getTopology().getRepGroupMap().getAll().size());

        config.startRepNodeServices();

        /*
         * Note that there is no client to ensure that RNs provide the sole
         * propagation mechanism.
         */
        makeAndVerifyTopoChanges(config, false);

        config.stopRepNodeServices();
    }

    private Topology makeAndVerifyTopoChanges(final KVRepTestConfig config,
                                              boolean fullTopology)
        throws RemoteException, NotBoundException {

        final Topology topo = config.getTopology().getCopy();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, NULL_LOGIN_MGR, logger);

        /*
         * Try each type of node: master and replica, as the initial recipient
         * of the Topology change.
         */
        int newZoneCount = 0;
        for (RequestHandlerImpl rh : config.getRHs()) {
            final int seq1 = topo.getSequenceNumber();

            topo.add(Datacenter.newInstance("New Zone " + (++newZoneCount),
                                            3 /*rf*/,
                                            DatacenterType.PRIMARY, false,
                                            false));
            topo.add(Datacenter.newInstance("New Zone " + (++newZoneCount),
                                            3 /*rf*/,
                                            DatacenterType.PRIMARY, false,
                                            false));
            final int seq2 = topo.getSequenceNumber();

            assertTrue(seq2 > seq1);
            final RepNodeAdminAPI repNodeAdmin =
                regUtils.getRepNodeAdmin(rh.getRepNode().getRepNodeId());

            if (fullTopology) {
                /* Discard the last change. */
               topo.discardChanges(seq2 - 1);
               repNodeAdmin.updateMetadata(topo);
            } else {
                final List<TopologyChange> changes = topo.getChanges(seq1 + 1);
                repNodeAdmin.updateMetadata(new TopologyInfo(topo, changes));
            }

            waitForTopology(config, seq2);
        }
        return topo;
    }

    private void waitForTopology(final KVRepTestConfig config, final int seq2) {
        /*
         * Wait for the periodic NOP messages generated by the state update
         * thread to detect the topology change and propagate it to all the
         * nodes.
         */
        for (final RepNodeService rns : config.getRepNodeServices()) {
            final RepNode repNode = rns.getReqHandler().getRepNode();

            new PollCondition(10, 60000) {
                @Override
                protected boolean condition() {
                    final Topology rtopo = repNode.getTopology();
                    if (rtopo == null) {
                        return false;
                    }
                    int seqNum = rtopo.getSequenceNumber();
                    return seqNum == seq2;
                }

            }.await();

            assertNotNull("RepNode:" + repNode.getRepNodeId(),
                          repNode.getTopology());
            assertEquals("RepNode:" + repNode.getRepNodeId(),
                         seq2, repNode.getTopology().getSequenceNumber());
        }
    }
}
