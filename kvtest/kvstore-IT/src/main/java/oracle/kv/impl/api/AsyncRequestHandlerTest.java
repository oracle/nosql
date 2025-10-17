/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.async.FutureUtils.complete;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import oracle.kv.Consistency;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultValueVersion;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.AsyncVersionedRemoteTestBase;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.InetNetworkAddress;
import oracle.kv.impl.async.dialog.AbstractDialogEndpointHandler;
import oracle.kv.impl.async.exception.ConnectionTimeoutException;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.DialogLimitExceededException;
import oracle.kv.impl.async.exception.InitialHandshakeIOException;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Simple test of the asynchronous request handler defined by {@link
 * AsyncRequestHandlerAPI}.
 */
@RunWith(FilterableParameterized.class)
public class AsyncRequestHandlerTest extends AsyncVersionedRemoteTestBase {

    private static final DialogType requestDialogType =
        new DialogType(REQUEST_HANDLER_TYPE_FAMILY, 1);

    public AsyncRequestHandlerTest(boolean secure) {
        super(secure);
    }

    /* -- Tests -- */

    @Test
    public void testSimple() throws Exception {
        final CreatorEndpoint creatorEndpoint = createEndpoint(
            requestDialogType,
            new AsyncRequestHandlerResponder(
                new AsyncRequestHandlerImpl(), logger));
        assertNotNull(execute(creatorEndpoint, createRequest()));
    }

    @Test
    public void testServerThrowsException() throws Exception {
        final String msg = "Test server exception";
        final CreatorEndpoint creatorEndpoint = createEndpoint(
            requestDialogType,
            new AsyncRequestHandlerResponder(
                new AsyncRequestHandlerImpl() {
                    @Override
                    public CompletableFuture<Response>
                        execute(Request request, long timeout)
                    {
                        return failedFuture(new LocalException(msg));
                    }
                },
                logger));
        try {
            execute(creatorEndpoint, createRequest());
            fail("Expected LocalException");
        } catch (LocalException e) {
            assertEquals(msg, e.getMessage());
        }
    }

    @Test
    public void testConnectionRejected() throws Exception {

        /* Only permit a single connection */
        listenerConfig = createListenerConfigBuilder()
            .option(AsyncOption.DLG_ACCEPT_MAX_ACTIVE_CONNS, 1)
            .build();

        final CreatorEndpoint creatorEndpoint1 =
            createEndpoint(requestDialogType,
                           new AsyncRequestHandlerResponder(
                               new AsyncRequestHandlerImpl(), logger));

        AsyncRequestHandlerAPI.wrap(new AsyncRequestHandlerInitiator(
                                        creatorEndpoint1, requestDialogType,
                                        logger),
                                    TIMEOUT)
            .thenCompose(
                api -> {
                    if (api == null) {
                        throw new IllegalArgumentException("API is null");
                    }
                    return api.execute(createRequest(), TIMEOUT);
                })
            .get(10, SECONDS);

        final CreatorEndpoint creatorEndpoint2 =
            getEndpointGroup().getCreatorEndpoint(
                "perfName", creatorEndpoint1.getRemoteAddress(),
                InetNetworkAddress.ANY_LOCAL_ADDRESS,
                createEndpointConfigBuilder(false)

                /*
                 * Set a non-default option value to make sure we get second
                 * endpoint and trigger the maximum connections limit
                 */
                .option(AsyncOption.DLG_CONNECT_TIMEOUT,
                        EndpointConfigBuilder.getOptionDefault(
                            AsyncOption.DLG_CONNECT_TIMEOUT) + 1000)
                .build());
        try {
            execute(creatorEndpoint2, createRequest());
            fail("Expected PersistentDialogException");
        } catch (PersistentDialogException e) {
            assertTrue("Cause should be InitialHandshakeIOException: " +
                       e.getCause(),
                       e.getCause() instanceof InitialHandshakeIOException);
        }
    }

    @Test
    public void testDialogRejected() throws Exception {

        /* Only permit a single dialog on the server side*/
        listenerConfig = createListenerConfigBuilder()
            .endpointConfigBuilder(
                createEndpointConfigBuilder(true)
                .option(AsyncOption.DLG_REMOTE_MAXDLGS, 1))
            .build();

        /*
         * Set up server to hang on first call and fail on second, which isn't
         * expected
         */
        final CountDownLatch firstCallWaiting = new CountDownLatch(1);
        final CountDownLatch finishWaiting = new CountDownLatch(1);
        final AsyncRequestHandlerImpl requestHandler =
            new AsyncRequestHandlerImpl() {
                volatile int count;
                @Override
                public CompletableFuture<Response> execute(Request request,
                                                           long timeout) {
                    final CompletableFuture<Response> future =
                        new CompletableFuture<>();
                    try {
                        count++;
                        if (count < 2) {
                            firstCallWaiting.countDown();
                            /* Wait to respond, to keep the dialog active */
                            CompletableFuture.runAsync(() -> {
                                    try {
                                        assertTrue(
                                            "Not done",
                                            finishWaiting.await(30, SECONDS));
                                        super.execute(request, timeout)
                                            .whenComplete(
                                                (r, e) ->
                                                complete(future, r, e));
                                    } catch (Throwable t) {
                                        future.completeExceptionally(t);
                                    }
                                });
                        } else {
                            throw new RuntimeException(
                                "Unexpected second call to execute");
                        }
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                    return future;
                }
            };
        final CreatorEndpoint creatorEndpoint = createEndpoint(
            requestDialogType,
            new AsyncRequestHandlerResponder(requestHandler, logger));

        /* Make the first call */
        final CountDownLatch firstCallDone = new CountDownLatch(1);
        AsyncRequestHandlerAPI.wrap(new AsyncRequestHandlerInitiator(
                                        creatorEndpoint, requestDialogType,
                                        logger),
                                    TIMEOUT)
            .thenCompose(
                api -> {
                    if (api == null) {
                        throw new IllegalArgumentException("API is null");
                    }
                    return api.execute(createRequest(), TIMEOUT);
                })
            .thenRun(() -> firstCallDone.countDown());
        assertTrue("Waiting for first call to block",
                   firstCallWaiting.await(10, SECONDS));

        /*
         * Make a second call, which should get an exception.
         *
         * For now, the DialogLimitExceededException is an unexpected
         * exception, so we won't translate it to an expected one, like
         * ServerResourceLimitException. Note that the request dispatcher will
         * wrap this exception in a FaultException.
         */
        try {
            assertNotNull(execute(creatorEndpoint, createRequest()));
            fail("Expected DialogLimitExceededException");
        } catch (DialogLimitExceededException e) {
        }
    }

    @Test
    public void testMissingHeartbeatException() throws Exception {
        AbstractDialogEndpointHandler.handshakeDoneTestHook = (h) -> {
            h.cancelHeartbeatTask();
        };
        listenerConfig = createListenerConfigBuilder().endpointConfigBuilder(
            createEndpointConfigBuilder(false)
                .option(AsyncOption.DLG_HEARTBEAT_TIMEOUT, 1))
            .build();
        final CreatorEndpoint creatorEndpoint =
            createEndpoint(requestDialogType,
                new AsyncRequestHandlerResponder(new AsyncRequestHandler() {
                    @Override
                    public CompletableFuture<Short>
                        getSerialVersion(short serialVersion,
                                         long timeoutMillis) {
                        return completedFuture(SerialVersion.CURRENT);
                    }

                    @Override
                    public CompletableFuture<Response> execute(Request request,
                                                               long timeout) {
                        /* do nothing and wait for timeout */
                        return new CompletableFuture<>();
                    }
                }, logger));
        try {
            execute(creatorEndpoint, createRequest());
            fail("Execution should fail due to missing heartbeat");
        } catch (DialogException e) {
            if (e.getCause() instanceof ConnectionTimeoutException) {
                /* ignore */
            } else {
                throw e;
            }
        }
    }

    /* -- Other classes and methods -- */

    Request createRequest() {
        final Request request = new Request(
            new Get(new byte[] { 1 }, 2, false), new PartitionId(3), false, null,
            Consistency.NONE_REQUIRED, 4, 5, new ClientId(6), 7, null);
        request.setSerialVersion(SerialVersion.CURRENT);
        return request;
    }

    /** Dummy server implementation */
    class AsyncRequestHandlerImpl implements AsyncRequestHandler {
        @Override
        public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                         long timeoutMillis) {
            return completedFuture(SerialVersion.CURRENT);
        }
        @Override
        public CompletableFuture<Response> execute(Request request,
                                                   long timeout) {
            try {
                final RepNodeId rnId = new RepNodeId(1, 2);
                final ResultValueVersion rvv =
                    new ResultValueVersion(new byte[] { 0, 3 },
                                           new Version(new UUID(4, 5), 6, rnId, 7),
                                           8, 0, 0,-1);
                final Result result =
                    new Result.GetResult(OpCode.GET, 9, 10, rvv);
                final Response response = new Response(
                    rnId, 11, result, null, null, SerialVersion.CURRENT);
                return completedFuture(response);
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }
    }

    private Response execute(CreatorEndpoint creatorEndpoint,
                             Request request) throws Exception {
        try {
            final AsyncRequestHandlerInitiator initiator =
                new AsyncRequestHandlerInitiator(creatorEndpoint,
                                                 requestDialogType,
                                                 logger);
            return AsyncRequestHandlerAPI.wrap(initiator, TIMEOUT)
                .thenCompose(
                    api -> {
                        if (api == null) {
                            throw new IllegalArgumentException("API is null");
                        }
                        return api.execute(request, TIMEOUT);
                    })
                .get(TIMEOUT, MILLISECONDS);
        } catch (Throwable t) {
            throw handleFutureGetException(t);
        }
    }
}
