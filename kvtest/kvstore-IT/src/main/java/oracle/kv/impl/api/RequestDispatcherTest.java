/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.async.AsyncOption.DLG_HEARTBEAT_TIMEOUT;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.util.registry.AsyncRegistryUtils.createInitiatorProxyEndpointConfigBuilderHook;
import static oracle.kv.util.TestUtils.checkCause;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ReturnValueVersion;
import oracle.kv.ServerResourceLimitException;
import oracle.kv.Value;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.dialog.nio.NioEndpointHandler;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.TemporaryDialogException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.KeyGenerator;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.OutputWireRecord;

import org.junit.Test;

/**
 * Tests the RequestDispatcher
 */
public class RequestDispatcherTest extends RequestDispatcherTestBase {

    private final LoginManager LOGIN_MGR = null;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        NioEndpointHandler.readHook = null;
    }

    /* Tests */

    /**
     * Test dispatch of request operations over multiple nodes and groups. Note
     * that in this case, the dispatcher starts up, just as it would in a KV
     * client, with no knowledge of the state of any of the nodes and acquires
     * the state information via responses.
     */
    @Test
    public void testExecute() {

        KeyGenerator kgen = new KeyGenerator(null);

        /* Permit forwarding. Should get it right in at most two steps. */
        final int ttl = 2;
        final Key[] testKeys = kgen.getKeys(1000);

        for (Key key : testKeys) {

            /* Key shouldn't be there. */
            Request request = createGetRequest(key, ttl, 5000);
            Value v1 =
                dispatcher.execute(request, LOGIN_MGR).getResult().
                getPreviousValue();
            assertNull(v1);

            /* Insert the key */
            v1 = Value.createValue(getKeyBytes(key));
            request = createPutRequest(key, v1, ttl, 150);
            final Response resp = dispatcher.execute(request, LOGIN_MGR);
            assertNotNull(resp.getResult().getNewVersion());

            /* The first node is the master for this configuration. */
            assertEquals(1, resp.getRespondingRN().getNodeNum());

            /* Retrieve the key from a master or replica */
            request = createGetRequest(key, ttl, 100);
            final Value v2 =
                dispatcher.execute(request, LOGIN_MGR).getResult().
                getPreviousValue();
            assertNotNull(v2);
            assertArrayEquals(v1.getValue(), v2.getValue());
        }
        // dispatcher.logRequestStats();
    }

    /**
     * Ensures that the minimum of the service and client serial version is
     * used as the effective version.
     */
    @Test
    public void testEffectiveSerialVersion()
        throws Exception {

        final List<RequestHandlerImpl> rhs = config.getRHs();
        assertTrue(String.valueOf(rhs.size()), rhs.size() >= 3);
        final RequestHandlerImpl rh0 = rhs.get(0);
        final RequestHandlerImpl rh1 = rhs.get(1);
        final RequestHandlerImpl rh2 = rhs.get(2);

        assertEquals(SerialVersion.CURRENT, rh0.getSerialVersion());
        assertEquals(SerialVersion.CURRENT, rh1.getSerialVersion());
        assertEquals(SerialVersion.CURRENT, rh2.getSerialVersion());

        final short sv0 = (short) (SerialVersion.CURRENT - 1);
        final short sv1 = (short) (SerialVersion.CURRENT + 0);
        final short sv2 = (short) (SerialVersion.CURRENT + 1);

        rh0.setTestSerialVersion(sv0);
        rh1.setTestSerialVersion(sv1);
        rh2.setTestSerialVersion(sv2);

        assertEquals(sv0, rh0.getSerialVersion());
        assertEquals(sv1, rh1.getSerialVersion());
        assertEquals(sv2, rh2.getSerialVersion());

        final Collection<RepNodeState> rns =
            dispatcher.getRepGroupStateTable().getRepNodeStates();

        checkEffectiveSerialversion(rh0, rns,
                                    Math.min(sv0, SerialVersion.CURRENT));
        checkEffectiveSerialversion(rh1, rns,
                                    Math.min(sv1, SerialVersion.CURRENT));
        checkEffectiveSerialversion(rh2, rns,
                                    Math.min(sv2, SerialVersion.CURRENT));
    }

    private void checkEffectiveSerialversion(RequestHandlerImpl rh,
                                             Collection<RepNodeState> rns,
                                             int expectSerialVersion)
        throws Exception {

        RepNodeState state = null;
        for (RepNodeState s : rns) {
            if (s.getRepNodeId().equals(rh.getRepNode().getRepNodeId())) {
                state = s;
            }
        }
        assertNotNull(state);
        final Response resp = dispatcher.executeNOP(state, 1000 /*timeout*/,
                                                    LOGIN_MGR);
        assertNotNull(resp);
        assertEquals(expectSerialVersion, resp.getSerialVersion());
    }

    /**
     * Test that the server detects that a request was sent from a client
     * running at below the minimum supported serial version.
     */
    @Test
    public void testClientVersionBelowMinimum()
        throws Exception {

        /*
         * Disable this test in async mode. The test causes a server-side
         * exception to be thrown, which gets delivered asynchronously and was
         * causing following test methods in this class to fail sometimes.
         *
         * TODO: Try to find a way to handle these asynchronous failures so
         * that the test can be reenabled.
         */
         assumeFalse("Disable test in async mode",
                     AsyncControl.serverUseAsync);

        final byte[] keyBytes = { 42 };
        final int ttl = 1;

        try {
            Request.setTestSerialVersion((short) (SerialVersion.CURRENT - 1));
            Request.testServerMinimumSerialVersion = SerialVersion.CURRENT;
            final Request request = createGetRequest(keyBytes, ttl, 5000);
            try {
                dispatcher.execute(request, LOGIN_MGR);
                fail("Expected UnsupportedOperationException");
            } catch (UnsupportedOperationException e) {
                assertTrue(e.getMessage(),
                           e.getMessage().contains("client is incompatible"));
            }
        } finally {
            Request.setTestSerialVersion((short) 0);
            Request.testServerMinimumSerialVersion = (short) 0;
        }
    }

    /**
     * The request timeout on a forward should contain the request level
     * timeout and not the reduced timeout resulting from the forwarded
     * request.
     */
    @Test
    public void testRequestTimeoutOnForward() {

        final RepNodeId rg1rn1Id = new RepNodeId(1,1);
        final RequestHandlerImpl handler = config.getRH(rg1rn1Id);
        final String testString = "$TEST$";

        /* Check with remote RN originated timeout. */
        /* Hook to fake the remote RTE with distinctive timeout value. */
        handler.setTestHook(new TestHook<Request>() {
            @Override
            public void doHook(Request r) {
                throw new RequestTimeoutException(Integer.MAX_VALUE,
                                                  testString,
                                                  null,
                                                  true);
            }
        });

        final InternalOperation op = new Get(new byte[0]);
        Request request = new Request(op, new PartitionId(1), false, null,
                                      Consistency.NONE_REQUIRED, 1, 0,
                                      clientId, timeoutMs, null);
        try {
            dispatcher.execute(request, rg1rn1Id, LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rue) {
            /* In some async cases, the original exception will be the cause */
            assertTrue(rue.getMessage().contains(testString) ||
                       ((rue.getCause() != null) &&
                        rue.getCause().getMessage().contains(testString)));
            assertEquals(timeoutMs, rue.getTimeoutMs());
        }

        /* Check with local client originated timeout. */
        handler.stop(); /* Make the RN unreachable. */
        request = new Request(op, new PartitionId(1), false, null,
                              Consistency.NONE_REQUIRED, 1, 0,
                              clientId, 1, null);
        try {
            dispatcher.execute(request, rg1rn1Id, LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rue) {
            assertEquals(1, rue.getTimeoutMs());
        }
    }

    /**
     * Test that a forwarded request that gets a MetadataNotFoundException at
     * the final target reports that exception back to the caller. Note that
     * this case only happens if the original RN has the table metadata and the
     * RN the request is forwarded to does not. [KVSTORE-1385]
     */
    @Test
    public void testMetadataNotFoundExceptionOnForward() {

        /* Create a Put request that will require a master */
        final byte[] bytes = { 42 };
        final Value value = Value.createValue(bytes);
        final Request request =
            createPutRequest(bytes, value, 4 /* ttl */, 5000 /* timeout */);

        /* Make the request and get RN ID of master from the response */
        final Response response = dispatcher.execute(request, LOGIN_MGR);
        final RepNodeId masterId = response.getRespondingRN();

        /*
         * Make the master throw MetadataNotFoundException. The hook causes
         * RequestHandlerImpl.executeInternal to throw the exception in a
         * somewhat different place than it would be during the real check, so
         * wrap the exception with WrappedClientException because that is what
         * would happen under normal circumstances.
         */
        final RequestHandlerImpl masterHandler = config.getRH(masterId);
        masterHandler.setTestHook(r -> {
                throw new WrappedClientException(
                    new MetadataNotFoundException(
                        "Dummy table not found exception", 33));
            });

        /* Get the RN ID for another node in the shard */
        RepNodeId anotherId = null;
        for (final RepNodeState state :
                 dispatcher.getRepGroupStateTable().getRepNodeStates()) {
            final RepNodeId id = state.getRepNodeId();
            if ((masterId.getGroupId() == id.getGroupId()) &&
                !masterId.equals(id)) {
                anotherId = id;
                break;
            }
        }
        assertNotNull(anotherId);

        /*
         * Make another request, which should be forwarded, but should still
         * throw MetadataNotFoundException
         */
        final RepNodeId anotherIdFinal = anotherId;
        checkException(() ->
                       dispatcher.execute(
                           createPutRequest(bytes, value, 4 /* ttl */,
                                            5000 /* timeout */),
                           anotherIdFinal, LOGIN_MGR),
                       MetadataNotFoundException.class,
                       "Dummy table not found exception");
    }

    /**
     * Regression test that causes the selectDispatchRN method of
     * RequestDispatcherImpl to throw a NoSuitableRNException when called
     * by the dispatcher's execute method. This behavior is used to
     * verify that the dispatcher's execute method correctly handles
     * a null excludeRNs parameter; where previous to the fix, a
     * NullPointerException was encountered.
     */
    @Test
    public void testExecuteNoSuitableRNExceptionNullExcludeRNs() {

        final KeyGenerator kgen = new KeyGenerator(null);
        final int ttl = 1;
        final Key[] testKeys = kgen.getKeys(1);
        final byte[] keyBytes = testKeys[0].toByteArray();
        final Request request = createGetRequest(keyBytes, ttl, 5000);
        dispatcher.setTestHook(new TestHook<Request>() {
            @Override
            public void doHook(Request arg) {
                throw new RuntimeException
                    ("from testExecuteNoSuitableRNExceptionNullExcludeRNs");
            }
        });

        try {
            dispatcher.execute(request, LOGIN_MGR);
        } catch (NullPointerException e) {
            fail("Unexpected NullPointerException");
        } catch (RequestTimeoutException e) {
        }
    }

    /**
     * Test that the server can obtain the client host that initiated a
     * request.
     */
    @Test
    public void testExecuteClientHost() {

        assertNull("Should be no client host outside of a request",
                   AsyncVersionedRemoteDialogResponder.getClientHost());

        final List<RequestHandlerImpl> rhs = config.getRHs();
        final byte[] keyBytes = { 42 };
        final int ttl = 1;
        final AtomicReference<String> clientHost = new AtomicReference<>();
        try {
            for (RequestHandlerImpl rh : rhs) {
                rh.setTestHook(
                    r ->
                    clientHost.set(
                        AsyncVersionedRemoteDialogResponder.getClientHost()));
            }
            final Request request = createGetRequest(keyBytes, ttl, 5000);
            dispatcher.execute(request, LOGIN_MGR);
            assertNotNull(clientHost.get());
        } finally {
            for (RequestHandlerImpl rh : rhs) {
                rh.setTestHook(null);
            }
        }
    }

    /**
     * Check that the FaultException thrown by an async call that gets an
     * unexpected EOF when reading a response has a IOException as its cause
     * rather than an internal exception. [#27473]
     */
    @Test
    public void testFaultExceptionCause() throws Exception {
        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);

        final byte[] bytes = { 42 };
        final PartitionId partitionId = dispatcher.getPartitionId(bytes);
        final Value v1 = Value.createValue(bytes);
        final InternalOperation op =
            new Put(bytes, v1, ReturnValueVersion.Choice.NONE);
        final int ttl = 10;
        Request request = new Request(op, partitionId, true, allDurability,
                                      null, ttl, seqNum, clientId, 100, null);
        dispatcher.execute(request, LOGIN_MGR);

        final RepNodeId rg1rn1Id = new RepNodeId(1,1);
        request = new Request(op, partitionId, true, allDurability,
                              null, ttl, seqNum, clientId, 100, null);

        /*
         * Simulate an EOF when reading the response on the sender. Check for
         * a port greater than 10000 so that we know we are talking to a request
         * handler over an anonymous port rather than a service registry on an
         * explicit port.
         */
        NioEndpointHandler.readHook = r -> {
            if (r.isCreator() && (r.getRemoteAddress().getPort() > 10000)) {
                throw new ConnectionEndpointShutdownException(
                    true, true, r.getChannelDescription(), "Simulating EOF");
            }
        };

        try {
            dispatcher.execute(request, rg1rn1Id, LOGIN_MGR);
            fail("Expected FaultException");
        } catch (FaultException e) {
            assertTrue("Expected IOException cause, got: " + e.getCause(),
                       e.getCause() instanceof IOException);
        }
    }

    /**
     * Test that requests are rejected if the number of concurrent active
     * requests exceeds the limits specified by the queue size
     * (rnRHAsyncExecQueueSize) and thread count (rnRHAsyncExecMaxThreads), and
     * that the requests work again after the queue is flushed out.
     */
    @Test
    public void testAsyncRejectRequests() throws Exception {
        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);

        /* Set limits */
        final int smallQueueSize = 4;
        final int smallPoolSize = 2;
        final int maxRHRequests = smallQueueSize + smallPoolSize;

        /*
         * Stop the existing configuration and clear out the directory so we
         * can start another one configured with the right parameter values.
         */
        config.stopRHs();
        clearTestDirectory();
        config = new KVRepTestConfig(this, nDC, nSN, repFactor, nPartitions,
                                     nSecondaryZones, nShards) {
            @Override
            public RepNodeParams
                createRepNodeParams(StorageNode sn,
                                    oracle.kv.impl.topo.RepNode rn,
                                    int haPort) {
                final RepNodeParams rnParams =
                    super.createRepNodeParams(sn, rn, haPort);
                rnParams.setAsyncExecMaxThreads(smallPoolSize);
                rnParams.setAsyncMaxConcurrentRequests(maxRHRequests);
                return rnParams;
            }
        };
        config.startupRHs();

        final int numRHs = config.getRHs().size();

        /* Create keys and insert entries */
        final Map<RepNodeId,Key[]> allKeys = new HashMap<>();
        for (final RepNode rn : config.getRNs()) {
            final Key[] keys = new KeyGenerator(rn).getKeys(maxRHRequests);
            allKeys.put(rn.getRepNodeId(), keys);
            for (final Key key : keys) {
                final Value value = Value.createValue(getKeyBytes(key));
                final Request request = createPutRequest(key, value, 1, 5000);
                final Response resp = dispatcher.execute(request, LOGIN_MGR);
                assertNotNull(resp.getResult().getNewVersion());
            }
        }

        /*
         * Block request completion and count down until all request execution
         * slots are blocked
         */
        final int maxRequests = numRHs * smallPoolSize;
        final CountDownLatch blockedRequests = new CountDownLatch(maxRequests);
        final CompletableFuture<Void> resumeRn1Requests =
            new CompletableFuture<>();
        tearDowns.add(() -> resumeRn1Requests.complete(null));
        final CountDownLatch rn1RequestsResumed = new CountDownLatch(1);
        final CompletableFuture<Void> resumeRequests =
            new CompletableFuture<>();
        tearDowns.add(() -> resumeRequests.complete(null));
        for (final RequestHandlerImpl rh : config.getRHs()) {
            boolean isRn1 = rg1n1Id.equals(rh.getRepNode().getRepNodeId());
            rh.setTestHook(
                request -> {
                    blockedRequests.countDown();
                    try {
                        if (isRn1) {
                            resumeRn1Requests.get(30, SECONDS);
                        } else {
                            resumeRequests.get(30, SECONDS);
                        }
                    } catch (Exception e) {
                    }
                    if (isRn1) {
                        rn1RequestsResumed.countDown();
                    }
                });
        }

        /* Fill up executors with blocked get requests */
        final AsyncRequestDispatcherImpl asyncDispatcher =
            (AsyncRequestDispatcherImpl) dispatcher;
        for (final Key[] keys : allKeys.values()) {
            for (final Key key : keys) {
                final Request request = createGetRequest(key, 1, 30000);
                asyncDispatcher.executeAsync(request, null, LOGIN_MGR);
            }
        }
        assertTrue("Wait for get requests to block during execution",
                   blockedRequests.await(5, SECONDS));

        /*
         * Next request to each request handler should fail due to resource
         * limits
         */
        final AtomicInteger resourceExcepts = new AtomicInteger(numRHs);
        final CompletableFuture<Boolean> gotResourceExcepts =
            new CompletableFuture<>();
        for (final Entry<RepNodeId,Key[]> entry : allKeys.entrySet()) {
            final RepNodeId rnId = entry.getKey();
            final Key[] keys = entry.getValue();
            asyncDispatcher.executeAsync(createGetRequest(keys[0], 1, 100),
                                         null, LOGIN_MGR)
                .whenComplete(
                    unwrapExceptionVoid(
                        (r, e) -> {
                            if (e == null) {
                                gotResourceExcepts.completeExceptionally(
                                    new RuntimeException(
                                        "Missing exception for " + rnId));
                            } else if (!(e instanceof
                                         ServerResourceLimitException)) {
                                gotResourceExcepts.completeExceptionally(
                                    new RuntimeException(
                                        "Unexpected exception: " + e, e));
                            } else if (resourceExcepts.decrementAndGet()
                                       == 0) {
                                gotResourceExcepts.complete(true);
                            }
                        }));
        }
        gotResourceExcepts.get(10, SECONDS);

        /*
         * Release blocked requests for rg1-rn1, disable test hooks for new
         * requests, and make sure a new request completes
         */
        config.getRHs().stream().forEach(rh -> rh.setTestHook(null));
        resumeRn1Requests.complete(null);
        assertTrue("Waiting for RN1 requests to resume: " + rn1RequestsResumed,
                   rn1RequestsResumed.await(10, SECONDS));
        asyncDispatcher.executeAsync(createGetRequest(allKeys.get(rg1n1Id)[0],
                                                      2, 5000),
                                     null, LOGIN_MGR)
            .get(10, SECONDS);
        resumeRequests.complete(null);
    }

    /**
     * Test that an async dialog layer heartbeat timeout produces a
     * FaultException with an IOException as the cause, since this is how the
     * Datacheck tests detect remote communication problems. [KVSTORE-688]
     */
    @Test
    public void testAsyncHeartbeatTimeout() throws Exception {
        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);
        final boolean runningAlone =
            isRunningAlone("testAsyncHeartbeatTimeout");

        final Semaphore wait = setupHeartbeatTimeout();

        /* Make a put request so that it is not retried on failure */
        final byte[] keyBytes = { 42 };
        final Request request =
            createPutRequest(keyBytes, Value.createValue(keyBytes), 1, 5000);
        final CompletableFuture<Response> future =
            dispatcher.executeAsync(request, null, LOGIN_MGR);

        /* Check that the exception thrown is right */
        try {
            future.handle((r, e) -> {
                if (runningAlone) {
                    System.out.println(CommonLoggerUtils.getStackTrace(e));
                }
                checkCause(
                    checkException(e, FaultException.class,
                                   "Communication problem with rg1-rn"),
                    IOException.class,
                    "Problem with channel.*NBL");
                return null;
            })
            .get();
        } finally {
            /* Unblock the server so the cleanup works quickly */
            wait.release(10);
        }
    }

    private Semaphore setupHeartbeatTimeout() throws Exception {
        /*
         * Reduce the dialog layer heartbeat timeout for client side (creator)
         * endpoints so we get a heartbeat timeout more quickly. Note that we
         * can't easily change the interval -- the time period rather than the
         * missing heartbeat count -- because it is determined by the max of
         * the local and remote values.
         */
        tearDowns.add(
            () -> createInitiatorProxyEndpointConfigBuilderHook = null);
        createInitiatorProxyEndpointConfigBuilderHook =
            builder -> builder.option(DLG_HEARTBEAT_TIMEOUT, 1);

        /* Recreate the dispatcher so it gets the new heartbeat settings */
        dispatcher = createRequestDispatcher();

        /* Disable sending heartbeats to provoke the heartbeat timeout */
        tearDowns.add(
            () -> AbstractDialogEndpointHandler.testDisableHeartbeat = false);
        AbstractDialogEndpointHandler.testDisableHeartbeat = true;

        /*
         * Make the request wait on the server side so we get a client-side
         * timeout
         */
        final List<RequestHandlerImpl> rhs = config.getRHs();
        final Semaphore wait = new Semaphore(0);
        tearDowns.add(() ->
                      rhs.forEach(h -> {
                              h.setTestHook(null);
                              wait.release();
                          }));
        rhs.forEach(h -> h.setTestHook(
                        request -> {
                            try {
                                wait.acquire();
                            } catch (InterruptedException e) {
                            }
                        }));
        return wait;
    }

    /**
     * Test that requests are dispatched and handled on the proper RNs given
     * replication lags and time consistency constraints. [KVSTORE-1802]
     */
    @Test
    public void testTimeConsistency() throws Exception {

        /* Generate a key in rg1 */
        final KeyGenerator rg1Keys =
            new KeyGenerator(config.getRN(new RepNodeId(1, 1)));
        final Key key = rg1Keys.getKeys(1)[0];
        final byte[] keyBytes = key.toByteArray();

        /* Get master and replicas for rg1 */
        RepNode master = null;
        final List<RepNode> replicas = new ArrayList<>();
        for (final RepNode rn : config.getRNs()) {
            if (rn.getRepNodeId().getGroupId() != 1) {
                continue;
            }
            if (rn.getIsAuthoritativeMaster()) {
                master = rn;
            } else {
                replicas.add(rn);
            }
        }
        assertTrue(master != null);
        final RepNode rn1 = replicas.get(0);
        final RepNode rn2 = replicas.get(1);

        /* Install feeder filters that delay the replication stream */
        installFeederDelay(master, rn1, 4000);
        installFeederDelay(master, rn2, 8000);

        final int ttl = 3;

        /* Read from all RNs to establish baseline for computing VLSN rate */
        for (int id = 1; id <= 3; id++) {
            dispatcher.execute(
                createGetRequest(keyBytes, ttl, timeoutMs, consistencyNone),
                new RepNodeId(1, id), LOGIN_MGR);
        }

        /*
         * Run a thread that exercises the store and run it for longer than
         * RepNodeState.RATE_INTERVAL_MS to make sure VLSN rate information is
         * cached in the request dispatcher. Use no durability because the
         * replicas are lagging and we don't want (or need) to wait.
         */
        final Durability noDurability = new Durability(
            SyncPolicy.NO_SYNC, SyncPolicy.NO_SYNC, ReplicaAckPolicy.NONE);
        final Value value = Value.createValue(new byte[] { 77 });
        final AtomicBoolean stopThread = new AtomicBoolean();
        tearDowns.add(() -> stopThread.set(true));
        final CompletableFuture<Void> threadDone = CompletableFuture.runAsync(
            () -> {
                while (!stopThread.get()) {
                    dispatcher.execute(
                        createPutRequest(keyBytes, value, ttl, timeoutMs,
                                         noDurability),
                        LOGIN_MGR);
                    /* Throttle writes -- no need to write quickly */
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            });
        Thread.sleep(RepNodeState.RATE_INTERVAL_MS + 100);

        /*
         * A 20 second time consistency should be satisfied by all RNS. Exclude
         * the RNs already tried one by one so we can check all of them.
         */
        Consistency.Time timeConsistency =
            new Consistency.Time(20, SECONDS, timeoutMs, MILLISECONDS);
        Set<RepNodeId> rnsTried = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            final Request request =
                createGetRequest(keyBytes, ttl, timeoutMs, timeConsistency);
            final Response response = dispatcher.execute(
                request, new HashSet<>(rnsTried), LOGIN_MGR);
            final RepNodeId respondingRN = response.getRespondingRN();
            assertTrue("Responding RN (" + respondingRN +
                       ") should not be present in " + rnsTried,
                       rnsTried.add(respondingRN));
        }
        assertEquals("RNs tried: " + rnsTried, 3, rnsTried.size());

        /*
         * A 6 second time consistency is only satisfied by master and rn1, not
         * rn2. The time consistency timeout is the same as the request
         * timeout, so after the request handler waits that amount of time for
         * the RN to catch up, there is no time to retry the request on another
         * RN, and so it just fails.
         */
        timeConsistency = new Consistency.Time(6, SECONDS,
                                               timeoutMs, MILLISECONDS);
        rnsTried.clear();
        final RepNodeId rn2Id = rn2.getRepNodeId();
        for (int i = 0; i < 2; i++) {
            final Request request =
                createGetRequest(keyBytes, ttl, timeoutMs, timeConsistency);
            final Response response = dispatcher.execute(
                request, new HashSet<>(rnsTried), LOGIN_MGR);
            final RepNodeId respondingRN = response.getRespondingRN();
            assertTrue("Response from " + respondingRN +
                       ", present in RNs tried: " + rnsTried,
                       rnsTried.add(respondingRN));
        }
        assertEquals("RNs tried: " + rnsTried, 2, rnsTried.size());
        assertFalse(rnsTried.contains(rn2Id));
        try {
            final Request request =
                createGetRequest(keyBytes, ttl, timeoutMs, timeConsistency);
            final Response response =
                dispatcher.execute(request, rnsTried, LOGIN_MGR);
            fail("Got response from " + response.getRespondingRN() +
                 ", expected ConsistencyException");
        } catch (ConsistencyException e) {
            assertTrue("Message should contain " + rn2Id + ": " +
                       e.getMessage(),
                       e.getMessage().contains(rn2Id.toString()));
        }

        /*
         * Try replica 2 again, but increase the request timeout so that there
         * is time to retry the request on another RN. Note that the excludeRNs
         * argument to execute specifies the initial set of RNs to exclude, but
         * those RNs will be tried anyway if other RNs fail, which is what
         * should happen here.
         */
        {
            final Request request =
                createGetRequest(keyBytes, ttl, 2 * timeoutMs,
                                 timeConsistency);
            final Response response =
                dispatcher.execute(request, rnsTried, LOGIN_MGR);
            final RepNodeId respondingRN = response.getRespondingRN();
            assertFalse(rn2Id.equals(respondingRN));
        }

        /*
         * Try sending the request explicitly to the lagging RN using the same
         * consistency timeout as the request timeout so that the request will
         * fail when it can't be handled on the RN.
         */
        try {
            final Request request =
                createGetRequest(keyBytes, ttl, timeoutMs, timeConsistency);
            final Response response =
                dispatcher.execute(request, rn2Id, LOGIN_MGR);
            fail("Got response from " + response.getRespondingRN() +
                 ", expected ConsistencyException");
        } catch (ConsistencyException e) {
            assertTrue("Message should contain " + rn2Id + ": " +
                       e.getMessage(),
                       e.getMessage().contains(rn2Id.toString()));
        }

        /* Check for any exceptions in the exercise thread */
        stopThread.set(true);
        threadDone.get(5, SECONDS);
    }

    Request createPutRequest(Key key, Value value, int ttl, int timeoutMillis)
    {
        return createPutRequest(key.toByteArray(), value, ttl, timeoutMillis);
    }

    Request createPutRequest(byte[] keyBytes,
                             Value value,
                             int ttl,
                             int timeoutMillis) {
        return createPutRequest(keyBytes, value, ttl, timeoutMillis,
                                allDurability);
    }

    Request createPutRequest(byte[] keyBytes,
                             Value value,
                             int ttl,
                             int timeoutMillis,
                             Durability durability) {
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final InternalOperation op =
            new Put(keyBytes, value, ReturnValueVersion.Choice.NONE);
        return new Request(op, partitionId, true, durability, null, ttl,
                           seqNum, clientId, timeoutMillis, null);
    }

    Request createGetRequest(Key key, int ttl, int timeoutMillis) {
        return createGetRequest(key.toByteArray(), ttl, timeoutMillis);
    }

    Request createGetRequest(byte[] keyBytes, int ttl, int timeoutMillis) {
        return createGetRequest(keyBytes, ttl, timeoutMillis, consistencyNone);
    }

    Request createGetRequest(byte[] keyBytes,
                             int ttl,
                             int timeoutMillis,
                             Consistency consistency) {
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final InternalOperation op = new Get(keyBytes);
        return new Request(op, partitionId, false, null, consistency,
                           ttl, seqNum, clientId, timeoutMillis, null);
    }

    Response createResponse(RepNodeId rnId, long vlsn, State state) {
        return new Response(
            rnId, vlsn, null /* result */, null /* topoInfo */,
            new StatusChanges(state, rnId, 1 /* sequenceNum */),
            SerialVersion.CURRENT);
    }

    /**
     * Install a feeder filter for the specified replica that will delay
     * replication by the specified amount of time.
     */
    void installFeederDelay(RepNode master, RepNode replica, long delayMs) {
        /*
         * Check for commit records and wait for the current time to exceed the
         * commit time by delayMs.
         */
        final FeederFilter delayFilter = new FeederFilter() {
            @Override
            public OutputWireRecord execute(OutputWireRecord record,
                                            RepImpl repImpl) {
                final long commitTimestamp = record.getCommitTimeStamp();
                if (commitTimestamp > 0) {
                    while (true) {
                        final long waitMs = commitTimestamp + delayMs -
                            System.currentTimeMillis();
                        if (waitMs <= 0) {
                            break;
                        }
                        try {
                            Thread.sleep(waitMs);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
                return record;
            }
            @Override
            public String[] getTableIds() { return null; }
            @Override
            public void setLogger(Logger logger) { }
        };
        RepInternal.getRepImpl(master.getEnv(100))
            .getRepNode()
            .feederManager()
            .getFeeder(replica.getRepNodeId().getFullName())
            .setFeederFilter(delayFilter);
    }

    /**
     * Tests the tracing for exceptions during handler resolution and
     * request execution and prints out the exception message.
     */
    @Test
    public void testExceptionTracingAsync() {
        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);
        final boolean runningAlone =
            isRunningAlone("testExceptionTracingAsync");
        final Request request = setupEventTracingTest();
        dispatcher = createRequestDispatcher(true);
        /*
         * Starts the update thread so that it could recover from previous
         * exceptions.
         */
        dispatcher.startStateUpdateThread();
        try {
            dispatcher.execute(request, LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rue) {
            final String s = rue.toString();
            assertTrue(s, s.contains("request handler error"));
            assertTrue(s, s.contains("RNUnavailableException"));
            if (runningAlone) {
                rue.printStackTrace();
            }
        }
    }

    private Request setupEventTracingTest() {
        tearDowns.add(() ->  { AsyncRegistryUtils.getRequestHandlerHook = null; });
        tearDowns.add(() ->  { RegistryUtils.getRequestHandlerHook = null; });
        /* Make all resolution fail until after 100ms the request is created. */
        final AtomicLong t0 = new AtomicLong(Long.MAX_VALUE);
        AsyncRegistryUtils.getRequestHandlerHook = (rnId) -> {
            if (System.currentTimeMillis() - t0.get() < 100) {
                throw new TemporaryDialogException(
                    false, false, "injected",
                    new IOException("request handler error"));
            }
        };
        RegistryUtils.getRequestHandlerHook = (rnId) -> {
            if (System.currentTimeMillis() - t0.get() < 100) {
                throw new ServerResourceLimitException(
                    "request handler error");
            }
        };
        /*
         * Make the first request handler execution fail with a retryable
         * exception, but second with timeout which will be delivered to the
         * user.
         */
        final List<RequestHandlerImpl> rhs = config.getRHs();
        final AtomicInteger numExecutions = new AtomicInteger(0);
        tearDowns.add(
            () ->
            rhs.forEach(h -> {
                h.setTestHook(null);
            }));
        rhs.forEach(h -> {
            h.setTestHook((r) -> {
                if (numExecutions.getAndIncrement() == 0) {
                    throw new ThreadInterruptedException(
                        DbInternal.getEnvironmentImpl(
                            config.getRN(new RepNodeId(1, 1))
                            .getEnv(1000)),
                         "test", new InterruptedException("test"));
                }
                if (numExecutions.getAndIncrement() >= 1) {
                    throw new RequestTimeoutException(
                        Integer.MAX_VALUE, "test", null, true);
                }
            });
        });
        /* Make the request. */
        final byte[] keyBytes = { 42 };
        final int ttl = 2;
        final Request request = createGetRequest(keyBytes, ttl, 5000);
        t0.set(System.currentTimeMillis());
        return request;
    }

    /**
     * Tests the tracing for exceptions during handler resolution and request
     * execution and prints out the exception message with sync dispatcher.
     */
    @Test
    public void testExceptionTracingSync() {
        assumeFalse("Only when testing sync", AsyncControl.serverUseAsync);
        final boolean runningAlone =
            "testExceptionTracingSync".equals(
                System.getProperty("testcase.methods", null));
        final Request request = setupEventTracingTest();
        dispatcher = createRequestDispatcher(false);
        dispatcher.startStateUpdateThread();
        try {
            dispatcher.execute(request, LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rue) {
            final String s = rue.toString();
            assertTrue(s, s.contains("request handler error"));
            assertTrue(s, s.contains("RNUnavailableException"));
            if (runningAlone) {
                rue.printStackTrace();
            }
        }
    }

    /**
     * Tests the tracing for exceptions is disabled and prints out the
     * exception message.
     */
    @Test
    public void testExceptionTracingDisabled() {
        final boolean runningAlone =
            isRunningAlone("testExceptionTracingDisabled");
        final Request request = setupEventTracingTest();
        final int defaultValue =
            RequestDispatcherImpl.maxExecuteRequestEvents;
        tearDowns.add(() ->  {
            RequestDispatcherImpl.maxExecuteRequestEvents = defaultValue;
        });
        RequestDispatcherImpl.maxExecuteRequestEvents = -1;
        /*
         * Create the dispatcher again to adopt the maxExecuteRequestEvents
         * value.
         */
        dispatcher = createRequestDispatcher();
        try {
            dispatcher.execute(request, LOGIN_MGR);
            fail("Expected RequestTimeoutException");
        } catch (RequestTimeoutException rue) {
            final String msg = rue.getMessage();
            assertTrue(msg, !msg.contains("Dispatch event trace:"));
            if (runningAlone) {
                rue.printStackTrace();
            }
        }
    }
}
