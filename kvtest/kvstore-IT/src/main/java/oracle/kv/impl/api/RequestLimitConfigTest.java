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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.RequestLimitConfig;
import oracle.kv.RequestLimitException;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Tests that the RequestLimitConfiguration is enforced.
 */
public class RequestLimitConfigTest extends TestBase {

    final Key key = Key.createKey("one", "two");
    final Value value = Value.createValue(new byte[1]);

    private KVRepTestConfig config;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        /*
         * Turn off requests made from status threads, so they don't interfere
         * with request count thresholds.
         */
        RepNodeStateUpdateThread.setUpdateState(false);

        /* Create a simple 1RG X 3 KVS */
        config = new KVRepTestConfig(this, 1, 1, 3, 10);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Tests that configuration limits are enforced.
     */
    @Test
    public void testRequestLimitConfig()
        throws InterruptedException {

        config.startRepNodeServices();

        final int requestTimeoutMs = 10000;

        final KVStoreConfig kvsConfig = config.getKVSConfig();
        RequestLimitConfig clientLimitConfig =
            new RequestLimitConfig(10, 50, 40);

        kvsConfig.setRequestLimit(clientLimitConfig);
        kvsConfig.setRequestTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS);
        KVStore kvs = KVStoreFactory.getStore(kvsConfig);

        /* Should go through, since it's below the threshold. */
        kvs.put(key, value);

        /*
         * Start up as many concurrency requests as are necessary to exceed the
         * thread threshold.
         */
        final int threshold = clientLimitConfig.getRequestThreshold();
        final CountDownLatch requestLatch =  new CountDownLatch(threshold);
        final CountDownLatch waitLatch = new CountDownLatch(1);

        setMasterRHLatency(requestLatch, waitLatch);
        List<ClientRequest> requestThreads = new LinkedList<ClientRequest>();
        for (int i=0; i < threshold; i++) {
            final ClientRequest clientRequest = new ClientRequest(kvs);
            clientRequest.start();
            requestThreads.add(clientRequest);
        }

        /*
         * Ensure all the above threads have requests waiting in the request
         * handlers.
         */
        if (!requestLatch.await(requestTimeoutMs,
                                TimeUnit.MILLISECONDS)) {
            fail("Expected requestLatch to reach zero, found: " +
                 requestLatch);
        }

        try {
            kvs.put(key, value);
            fail("Expected RequestLimitException");
        } catch (RequestLimitException rle) {
            // Expected exception
        }

        /*
         * Requests to other nodes should get through. Relying on the
         * dispatcher to go to a different node for the read operation, since
         * rg1-rn1 already has a large number of outstanding requests.
         */
        kvs.get(key);

        /* Send a read request to the master, it should block. */
        try {
            kvs.get(key, Consistency.ABSOLUTE,
                    requestTimeoutMs, TimeUnit.MILLISECONDS);
            fail("Expected RequestLimitException");
        } catch (RequestLimitException rle) {
            // Expected exception
        }

        /* Release the waiting request threads. */
        waitLatch.countDown();

        /* Verify that there are no hidden exceptions. */
        for (ClientRequest cr : requestThreads) {
            cr.join(requestTimeoutMs);
            assertTrue(!cr.isAlive());
            assertTrue(cr.failException == null);
        }

        kvs.close();
        config.stopRepNodeServices();
    }

    /**
     * Test boundary conditions for the constructor parameters.
     */
    @SuppressWarnings("unused")
    @Test
    public void testConstructorParams() {

        try {
            new RequestLimitConfig(0, 1, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("maxActiveRequests: 0 must be positive",
                         e.getMessage());
        }

        try {
            new RequestLimitConfig(1, 0, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("requestThresholdPercent: 0 must be positive",
                         e.getMessage());
        }

        try {
            new RequestLimitConfig(1, 101, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("requestThresholdPercent: 101 cannot exceed 100",
                         e.getMessage());
        }

        try {
            new RequestLimitConfig(1, 2, 0);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("nodeLimitPercent: 0 must be positive",
                         e.getMessage());
        }

        try {
            new RequestLimitConfig(1, 2, 3);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("nodeLimitPercent: 3 cannot exceed" +
                         " requestThresholdPercent: 2",
                         e.getMessage());
        }

        /* Test possible integer overflow conditions */
        RequestLimitConfig config1 =
            new RequestLimitConfig(Integer.MAX_VALUE, 10, 5);
        assertEquals("maxActiveRequests",
                     Integer.MAX_VALUE, config1.getMaxActiveRequests());
        assertEquals("requestThresholdPercent",
                     Integer.MAX_VALUE / 10, config1.getRequestThreshold());
        assertEquals("nodeLimit",
                     Integer.MAX_VALUE / 20, config1.getNodeLimit());
    }

    /**
     * Set up the test hooks for waiting in the request handlers.
     */
    private void setMasterRHLatency(final CountDownLatch latch,
                                    final CountDownLatch waitLatch) {
        for (final RepNodeService  rns : config.getRepNodeServices()) {
            final RequestHandlerImpl rh = rns.getReqHandler();
            rh.setTestHook(new TestHook<Request>() {

                @Override
                public void doHook(Request arg) {
                    if (rh.getRepNode().getRepNodeId().getNodeNum() == 1) {
                        try {
                            latch.countDown();
                            waitLatch.await();
                        } catch (InterruptedException e) {
                            fail("Unexpected interrupt");
                        }
                    }
                }
            });
        }
    }

    /*
     * Simulates a KVS client thread(one of many)`, making put requests.
     */
    class ClientRequest extends Thread {

        private final KVStore kvs;
        Exception failException;

        ClientRequest(KVStore kvs) {
            super();
            this.kvs = kvs;
            this.setName("ClientRequest");
        }

        @Override
        public void run() {
            try {
                kvs.put(key, value);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                failException = e;
            }
        }
    }
}
