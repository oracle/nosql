/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.Key;
import oracle.kv.RequestLimitConfig;
import oracle.kv.RequestLimitException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KeyGenerator;

import org.junit.Test;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

/**
 * Test the forwarding behavior of KVS from the RequestDispatcher's frame of
 * reference.
 */
public class RequestForwardingTest
    extends RequestDispatcherTestBase {

    private final LoginManager LOGIN_MGR = null;

    /**
     * The KV pair used during the forwarding tests. They are associated with
     * the group rg1.
     */
    private Key k1;
    private Value v1;
    private byte[] k1Bytes;
    private PartitionId k1PartitionId;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        RepNode rg1n1 = config.getRNs().get(0);

        assertEquals(rg1n1Id, rg1n1.getRepNodeId());
        KeyGenerator kgenRg1 =  new KeyGenerator(rg1n1);

        k1 = kgenRg1.getKeys(1)[0];
        v1 = Value.createValue(getKeyBytes(k1));

        k1Bytes = k1.toByteArray();
        k1PartitionId = dispatcher.getPartitionId(k1Bytes);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Verify that requests are forwarded, when they are misdirected. Tests the
     * following cases of misdirection:
     *
     * 1) A write misdirected to a replica.
     * 2) A write misdirected to a different group than the one holding the key
     * for the partition.
     * 3) A write misdirected to a node that was once the master but isn't
     * anymore.
     */
    @Test
    public void testForward() {

        Put op = new Put(k1Bytes, v1, ReturnValueVersion.Choice.NONE);
        RepGroupState rg1s =
            dispatcher.getRepGroupStateTable().getGroupState(rg1Id);
        assertNull(rg1s.getMaster());

        /* Forward misdirected write to a replica (node num 2 in group)*/
        Request req = new Request(op, k1PartitionId, true, allDurability, null,
                                  3, seqNum, clientId, timeoutMs, null);
        Response resp = dispatcher.execute(req, rg1n2Id, LOGIN_MGR);
        assertEquals(rg1n1Id, resp.getRespondingRN());
        /* It's the first request, so status must be returned. */

        final StatusChanges statusChanges = resp.getStatusChanges();
        assertNotNull(statusChanges);
        /* The responding id must be that of the master */
        assertEquals(rg1n1Id, resp.getRespondingRN());
        assertTrue(statusChanges.getState().isMaster());
        assertEquals(rg1n1Id, statusChanges.getCurrentMaster());

        /*
         * The sequence numbers match up, so no topology changes should be
         * returned.
         */
        assertNull(resp.getTopoInfo());
        /*
         * The group's master must become known as a result of the non null
         * status in the response.
         */
        assertEquals(rg1n1Id, rg1s.getMaster().getRepNodeId());

        /* Forward because write was directed to group not holding partition. */
        req = new Request(op, k1PartitionId, true, allDurability, null, 4,
                          seqNum, clientId, timeoutMs, null);
        resp = dispatcher.execute(req, new RepNodeId(2,1), LOGIN_MGR);
        assertEquals(rg1n1Id, resp.getRespondingRN());
        /*
         * The dispatcher id has become known to the client and there are no
         * intervening mastership changes, so changes must be null.
         */
        assertNull(resp.getStatusChanges());
    }

    /**
     * Ensures that writes are forwarded correctly after a change of master
     * due to a node failure.
     */
    @Test
    public void testFailoverForward()
        throws InterruptedException {

        Put op = new Put(k1Bytes, v1, ReturnValueVersion.Choice.NONE);
        RepGroupState rg1s =
            dispatcher.getRepGroupStateTable().getGroupState(rg1Id);
        assertNull(rg1s.getMaster());

        Request req = new Request(op, k1PartitionId, true, allDurability, null,
                                  3, seqNum, clientId, timeoutMs, null);
        Response resp = dispatcher.execute(req, LOGIN_MGR);
        assertEquals(rg1n1Id, resp.getRespondingRN());

        /* Shutdown r1n1, the current master. */
        RequestHandlerImpl rg1n1h = config.getRHs().get(0);
        RepNode rg1n1 = rg1n1h.getRepNode();
        assertEquals(rg1n1Id, rg1n1.getRepNodeId());

        CountDownLatch masterLatch = new CountDownLatch(1);

        for (RepNodeState rns : rg1s.getRepNodeStates()) {
            RepNode rn = config.getRN(rns.getRepNodeId());
            if (rg1n1Id.equals(rn.getRepNodeId())) {
                continue;
            }
            ReplicatedEnvironment env = rn.getEnv(10000);
            MCListener listener =
                new MCListener(env.getStateChangeListener(), masterLatch);
            rn.getEnv(1000).setStateChangeListener(listener);
        }
        rg1n1h.stop(); rg1n1.stop(false);

        /* This should result in an election and a new master. */
        assertTrue(masterLatch.await(1, TimeUnit.MINUTES));

        /* Use simple majority, instead of all, since a node is down. */
        final Durability simpleMajorityDurability =
            new Durability(SyncPolicy.NO_SYNC,
                           SyncPolicy.NO_SYNC,
                           Durability.ReplicaAckPolicy.SIMPLE_MAJORITY);
        op = new Put(k1Bytes, v1,
                     ReturnValueVersion.Choice.NONE);
        req = new Request(op, k1PartitionId, true, simpleMajorityDurability,
                          null, 2, seqNum, clientId, 10000, null);

        /* Request should be re-directed to the right master. */
        resp = dispatcher.execute(req, LOGIN_MGR);
        assertFalse(rg1n1Id.equals(resp.getRespondingRN()));
    }

    /**
     * Regression test for the forwardIfRequired method of the
     * RequestHandlerImpl class.
     *
     * Prior to modifying forwardIfRequired, when the NONE_REQUIRED_NO_MASTER
     * consistency policy was used during a GET request, it was possible for
     * the request to be serviced by the master node; violating the guarantee
     * made by NONE_REQUIRED_NO_MASTER. If this test is run against a
     * version of RequestHandlerImpl that does not include the modification
     * to forwardIfRequired (a new if block: else if(request.needsReplica())),
     * the test will fail; as the GET request that is made will always be
     * serviced by the master (rg1-rn1). On the other hand, if this test
     * is run against a version which includes the modification, the test
     * should pass; as the GET request should always be handled by a replica.
     */
    @Test
    public void testNoneRequiredNoMaster() {

        RepGroupState rg1s =
            dispatcher.getRepGroupStateTable().getGroupState(rg1Id);
        assertNull(rg1s.getMaster());

        final Get getOp = new Get(new byte[0]);
        final boolean isWriteRqst = false;
        final Durability durability = null;
        @SuppressWarnings("deprecation")
        final Consistency consistency = Consistency.NONE_REQUIRED_NO_MASTER;
        final int rqstTtl = 3;

        Request getRqst = new Request(getOp, k1PartitionId, isWriteRqst,
                                      durability, consistency,
                                      rqstTtl, seqNum, clientId, timeoutMs,
                                      null);
        Response getResp = dispatcher.execute(getRqst, rg1n1Id, LOGIN_MGR);

        assertEquals(ReplicatedEnvironment.State.REPLICA,
                     rg1s.get(getResp.getRespondingRN()).getRepState());
    }

    /**
     * Test that RequestLimitExceptions thrown when an RN forwards a request
     * get logged as rate-limited warnings [#27826]
     */
    @Test
    public void testForwardingRequestLimit() throws Exception {

        /*
         * Count the number of RequestLimitException warnings logged by
         * rg1-rn2's request handler
         */
        final AtomicInteger warningCount = new AtomicInteger();
        config.getRH(rg1n2Id).getLogger().addHandler(new StreamHandler() {
            @Override
            public synchronized void publish(final LogRecord record) {
                if ((record.getLevel() == Level.WARNING) &&
                    (record.getThrown() instanceof RequestLimitException) &&
                    record.getMessage().contains(
                        "Request limit exception when forwarding")) {
                    warningCount.getAndIncrement();
                }
            }
        });

        /*
         * Set a hook on rg1-rn2's request dispatcher so that it throws a
         * RequestLimitException when it attempts to forward a request
         */
        ((RequestDispatcherImpl) config.getRN(rg1n2Id).getRequestDispatcher())
            .setPreExecuteHook(request -> {
                    throw RequestLimitException.create(
                        new RequestLimitConfig(1, 90, 80), rg1n1Id, 0, 1,
                        true);
                });

        /* Arrange for a misdirected write to a replica to be forwarded */
        final Put op = new Put(k1Bytes, v1, ReturnValueVersion.Choice.NONE);
        final RepGroupState rg1s =
            dispatcher.getRepGroupStateTable().getGroupState(rg1Id);
        assertNull(rg1s.getMaster());
        final Request req =
            new Request(op, k1PartitionId, true, allDurability, null, 3,
                        seqNum, clientId, 10000, null);

        /* Repeat the request so we can check rate limiting of warnings */
        for (int i = 0; i < 2; i++) {
            try {
                dispatcher.execute(req, rg1n2Id, LOGIN_MGR);
                fail("Expected RequestLimitException");
            } catch (RequestLimitException e) {
            }
        }

        /* Check for a single warning */
        assertEquals(1, warningCount.get());
    }

    /**
     * Listener used to trip a countdown latch as soon as a Master becomes
     * available.
     */
    class MCListener implements StateChangeListener {

        /* The latch that's trigered when a new master makes an appearance. */
        private final CountDownLatch masterLatch;

        /*
         * The existing listener being wrapped -- an environment can have at
         * most one listener.
         */
        private final StateChangeListener wrappedListener;

        MCListener(StateChangeListener wrappedListener,
                   CountDownLatch masterLatch) {
            super();
            this.wrappedListener = wrappedListener;
            this.masterLatch = masterLatch;
        }

        @Override
        public void stateChange(StateChangeEvent sce) throws RuntimeException {
            if (wrappedListener != null) {
                wrappedListener.stateChange(sce);
            }
            if (sce.getState().isMaster()) {
                masterLatch.countDown();
            }
        }
    }
}
