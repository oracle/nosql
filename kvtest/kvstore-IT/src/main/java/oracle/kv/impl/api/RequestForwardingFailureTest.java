/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;

import org.junit.Test;

/**
 * Test to ensure that requests fail when a Master is not reachable and
 * that the request dispatcher waits and retries for the timeout period.
 */
public class RequestForwardingFailureTest extends RequestDispatcherTestBase {

    private final LoginManager LOGIN_MGR = null;
    private Exception exception;

    /* (non-Javadoc)
     * @see oracle.kv.impl.api.RequestDispatcherTestBase#setUp()
     */
    @Override
    public void setUp()
        throws Exception {

        repFactor = 5;
        nSN = 1;
        super.setUp();
        final RepNodeStateUpdateThread thread =
            dispatcher.getStateUpdateThread();
        thread.start();
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.api.RequestDispatcherTestBase#tearDown()
     */
    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testMasterAvailability()
        throws RemoteException, InterruptedException {

        final Put op = new Put(new byte[1], Value.createValue(new byte[0]),
                               ReturnValueVersion.Choice.NONE);
        Request req = new Request(op, new PartitionId(1), true, allDurability,
                                  null, 5, seqNum, clientId, timeoutMs, null);
        Response resp = dispatcher.execute(req, LOGIN_MGR);
        assertEquals(rg1n1Id, resp.getRespondingRN());

        /* Render the master unreachable by removing its registry entry. */
        config.getRHs().get(0).stop();

        req = new Request(op, new PartitionId(1), true, allDurability,
                          null, 5, seqNum, clientId, timeoutMs, null);
        long start = System.nanoTime();
        try {
            resp = dispatcher.execute(req, LOGIN_MGR);
            fail("Expected Operation failure exception");
        } catch (RequestTimeoutException rte) {
            /* Expected to timeout. */
            long timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            assertTrue((System.nanoTime() >= (start + timeoutNs)));

        }

        /* Set up for an async put operation. */
        Thread putThread = new Thread() {

            @Override
            public void run() {
                Request preq = new Request(op, new PartitionId(1), true,
                                           allDurability, null, 5, seqNum,
                                           clientId, 30000, null);
                try {
                    dispatcher.execute(preq, LOGIN_MGR);
                } catch (Exception rte) {
                    /* Failure. The main thread will check for it. */
                    exception = rte;
                    rte.printStackTrace();
                }
            }
        };

        /* Start the async put. */
        putThread.start();

        /* Let the dispatch be retried a few times. */
        Thread.sleep(1000);

        /* Make the master visible again. */
        RequestHandlerImpl rgnh = config.getRHs().get(0);
        rgnh.startup();

        putThread.join(30000);

        assertNull(exception);
    }

    /**
     * Test that a TTL failure after a second (or subsequent) forwarding is
     * handled correctly. Previously, the TTLFaultException was being
     * transmitted back to the caller even though that class said that it was
     * only used locally. Instead, the TTLFaultException should be handled by
     * the request dispatcher and result in a RequestTimeoutException.
     */
    @Test
    public void testTTLFailureAfterForwarding()
        throws Exception {

        final Put op = new Put(new byte[1], Value.createValue(new byte[0]),
                               ReturnValueVersion.Choice.NONE);
        Request req = new Request(op, new PartitionId(1), true, allDurability,
                                  null, 1, seqNum, clientId, timeoutMs, null);
        Response resp = dispatcher.execute(req, LOGIN_MGR);
        assertEquals(rg1n1Id, resp.getRespondingRN());

        /* Disable master */
        config.getRHs().get(0).stop();

        /* Set TTL to 1 so request fails when forwarded a second time */
        req = new Request(op, new PartitionId(1), true, allDurability,
                          null, 1, seqNum, clientId, timeoutMs, null);
        long start = System.nanoTime();
        try {
            resp = dispatcher.execute(req, LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rte) {
            long timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            long timeSpentNs = System.nanoTime() - start;
            assertTrue("Time spent (" + timeSpentNs + ")" +
                       " should be greater than timeout (" + timeoutNs + ")",
                       timeSpentNs > timeoutNs);
        }
    }
}
