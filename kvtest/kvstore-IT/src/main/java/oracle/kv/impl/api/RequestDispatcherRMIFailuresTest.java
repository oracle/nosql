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
import static org.junit.Assume.assumeFalse;

import java.rmi.RemoteException;

import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.registry.AsyncControl;

import org.junit.Test;

/**
 * Tests the handling of RM failures during message dispatch
 */
public class RequestDispatcherRMIFailuresTest extends
    RequestDispatcherTestBase {

    private final LoginManager LOGIN_MGR = null;

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
        KeyGenerator kgenRg1 = new KeyGenerator(rg1n1);

        k1 = kgenRg1.getKeys(1)[0];
        v1 = Value.createValue(getKeyBytes(k1));

        k1Bytes = k1.toByteArray();
        k1PartitionId = dispatcher.getPartitionId(k1Bytes);

        /* Place a key in the KVS */
        Put pop = new Put(k1Bytes, v1, ReturnValueVersion.Choice.NONE);
        Request req = new Request(pop, k1PartitionId, true, allDurability,
                                  null, 2, seqNum, clientId, timeoutMs, null);
        Response resp = dispatcher.execute(req, LOGIN_MGR);
        /* It's the master. */
        assertEquals(rg1n1Id, resp.getRespondingRN());
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    /** Test using a stale reference */
    @Test
    public void testNoSuchObjectException()
        throws RemoteException {

        /*
         * This test no longer works in async mode because object IDs depend on
         * service IDs and so can be reused after service restart
         */
        assumeFalse(AsyncControl.serverUseAsync);

        Get gop = new Get(k1Bytes);
        Request req = new Request(gop, k1PartitionId, false, null,
                                  consistencyNone, 2, seqNum, clientId,
                                  timeoutMs, null);
        /* Direct the read request to n2, a replica. */
        Response resp = dispatcher.execute(req, rg1n2Id, LOGIN_MGR);
        assertEquals(rg1n2Id, resp.getRespondingRN());
        config.restartRH(rg1n2Id);
        biasDispatch(rg1n2Id);
        resp = dispatcher.execute(req, LOGIN_MGR);
        assertTrue(!rg1n2Id.equals(resp.getRespondingRN()));
    }

    /** Test using a reference to a shutdown service */
    @Test
    public void testNoSuchObjectExceptionShutdown() {
        Get gop = new Get(k1Bytes);
        Request req = new Request(gop, k1PartitionId, false, null,
                                  consistencyNone, 2, seqNum, clientId,
                                  timeoutMs, null);
        /* Direct the read request to n2, a replica. */
        Response resp = dispatcher.execute(req, rg1n2Id, LOGIN_MGR);
        assertEquals(rg1n2Id, resp.getRespondingRN());
        config.getRH(rg1n2Id).stop();
        biasDispatch(rg1n2Id);
        resp = dispatcher.execute(req, LOGIN_MGR);
        assertTrue(!rg1n2Id.equals(resp.getRespondingRN()));
    }

    /**
     * Exercises the failure path resulting in an RMI UnknownHostException.
     * The failure must mark the RN as being in need of repair.
     */
    @Test
    public void testRMIUnknownHostFailure() {
        checkRequestDispatchRepairHandling
            (new java.rmi.UnknownHostException("test"));
    }

    /**
     * Exercises the failure path resulting in an RMI ConnectException.
     * The failure must mark the RN as being in need of repair.
     */
    @Test
    public void testRMIConnectException() {
        checkRequestDispatchRepairHandling
            (new java.rmi.ConnectException("test"));
    }

    /**
     * Exercises the failure path resulting in an RMI ConnectIOException
     * The failure must mark the RN as being in need of repair.
     */
    @Test
    public void testRMIConnectIOException() {
        checkRequestDispatchRepairHandling
            (new java.rmi.ConnectIOException("test"));
    }

    @Test
    public void testRMIUnmarshalException() {
        checkRequestDispatchExceptionConversion
            (new java.rmi.UnmarshalException
             ("test", new java.net.SocketTimeoutException("test")));
    }

    /**
     * Verifies that the RN is marked as being in need of repair as a
     * result of the exception argument.
     */
    private void checkRequestDispatchRepairHandling(final Exception e) {
        Get gop = new Get(k1Bytes);
        Request req = new Request(gop, k1PartitionId, false, null,
                                  consistencyNone, 2, seqNum, clientId, 60000,
                                  null);
        /* Direct the read request to n2, a replica. */
        Response resp = dispatcher.execute(req, rg1n2Id, LOGIN_MGR);
        assertEquals(rg1n2Id, resp.getRespondingRN());

        /* Bias the dispatch to the rn resident on the bad sn */
        biasDispatch(rg1n2Id);

        /*
         * Don't try to repair the state of broken connections in the
         * background, so we can verify that they are down.
         */
        RepNodeStateUpdateThread.setUpdateState(false);

        final RepNodeState rg1n2s =
            dispatcher.getRepGroupStateTable().getNodeState(rg1n2Id);

        /*
         * Set up for exception
         */
        ExceptionTestHook<Request, Exception> exceptionHook =
            new ExceptionTestHook<Request, Exception>() {

            boolean done = false;

            @Override
            public void doHook(Request r)
                throws Exception {

                if (!done) {
                    done = true;
                    throw e;
                }
            }
        };

        dispatcher.setPreExecuteHook(exceptionHook);

        /* Provoke the exception. */
        resp = dispatcher.execute(req, LOGIN_MGR);

        /* Some other node must respond. */
        assertTrue(!rg1n2Id.equals(resp.getRespondingRN()));

        /*
         * The node that resulted in the exception, must be marked as
         * being in need of repair, so it can be fixed in the background.
         */
        assertTrue(rg1n2s.reqHandlerNeedsRepair());
    }

    /**
     * Biases the dispatch towards biasedRNId by raising the average response
     * times associated with all the other nodes in the group.
     */
    private void biasDispatch(RepNodeId biasedRNId) {
        RepGroupState rg1s = dispatcher.getRepGroupStateTable().
            getGroupState(new RepGroupId(rg1n1Id.getGroupId()));
        for (RepNodeState  rns : rg1s.getRepNodeStates()) {
            if (biasedRNId.equals(rns.getRepNodeId())) {
                continue;
            }

            /* Push up response times of the others */
            rns.accumRespTime(false, 200);
        }
    }

    private void checkRequestDispatchExceptionConversion(final Exception e) {
        Put pop = new Put(k1Bytes, v1, ReturnValueVersion.Choice.NONE);
        Request req = new Request(pop, k1PartitionId, true, allDurability,
                                  null, 2, seqNum, clientId, timeoutMs,
                                  null);

        /*
         * Set up for exception
         */
        ExceptionTestHook<Request, Exception> exceptionHook =
            new ExceptionTestHook<Request, Exception>() {

            boolean done = false;

            @Override
            public void doHook(Request r)
                throws Exception {

                if (!done) {
                    done = true;
                    throw e;
                }
            }
        };

        dispatcher.setPreExecuteHook(exceptionHook);

        try {
            /* Provoke the exception. */
            dispatcher.execute(req, LOGIN_MGR);
            fail("didn't catch exception");
        } catch (RequestTimeoutException RTE) {
            // expected
        } catch (Exception E) {
            fail("didn't catch RequestTimeoutException");
        }
    }
}
