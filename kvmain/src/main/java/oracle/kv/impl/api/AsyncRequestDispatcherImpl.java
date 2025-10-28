/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.api;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static oracle.kv.impl.async.FutureUtils.complete;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.async.FutureUtils.whenComplete;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVStoreConfig;
import oracle.kv.RequestLimitConfig;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ServerResourceLimitException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.DialogUnknownException;
import oracle.kv.impl.async.exception.GetUserException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.Protocols;

/**
 * An implementation of RequestDispatcher that supports asynchronous
 * operations.
 */
public class AsyncRequestDispatcherImpl extends RequestDispatcherImpl {

    /**
     * The amount of time in milliseconds to allow for a single roundtrip
     * network communication with the server.
     */
    private final long networkRoundtripTimeoutMs;

    /**
     * Whether we are already delivering a result in the current thread. If the
     * user calls a synchronous method on the future that makes another API
     * call, and the dialog layer handles the operation in the current thread,
     * then we need to use a different thread to deliver the result to avoid
     * recursion which might exceed the supported stack depth.
     */
    private final ThreadLocal<Boolean> deliveringResult =
        ThreadLocal.withInitial(() -> false);

    /**
     * The endpoint group to use to get an executor service.
     */
    private final EndpointGroup endpointGroup =
        AsyncRegistryUtils.getEndpointGroup();

    /**
     * A separate thread factory to use in case the executor service rejects a
     * request.
     */
    private final KVThreadFactory backupThreadFactory =
        new KVThreadFactory(" backup async response delivery", logger);

    /**
     * Creates RequestDispatcher for a KVStore client. As part of the creation
     * of the client side RequestDispatcher, it contacts one or more SNs from
     * the list of SNs identified by their <code>registryHostport</code>.
     *
     * @param kvsName the store name
     *
     * @param clientId the unique clientId associated with the KVS client
     *
     * @param topology the store topology
     *
     * @param regUtilsloginMgr a login manager used to authenticate metadata
     * access
     *
     * @param requestLimitConfig specifies limit of the max requests to a node
     *
     * @param exceptionHandler the handler to be associated with the state
     * update thread
     *
     * @param protocols the protocols to use when calling the store
     *
     * @param logger a Logger
     *
     * @param readZones zone names which can be used for read requests
     */
    AsyncRequestDispatcherImpl(String kvsName,
                               ClientId clientId,
                               Topology topology,
                               LoginManager regUtilsLoginMgr,
                               RequestLimitConfig requestLimitConfig,
                               UncaughtExceptionHandler exceptionHandler,
                               Protocols protocols,
                               Logger logger,
                               String[] readZones) {
        super(kvsName, clientId, topology, regUtilsLoginMgr,
              requestLimitConfig, exceptionHandler, protocols, logger,
              readZones);
        this.networkRoundtripTimeoutMs =
            KVStoreConfig.DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;
    }

    /** Internal constructor used with a configuration and a topology. */
    AsyncRequestDispatcherImpl(KVStoreConfig config,
                               ClientId clientId,
                               Topology topology,
                               LoginManager loginMgr,
                               UncaughtExceptionHandler exceptionHandler,
                               Logger logger) {
        super(config, clientId, topology, loginMgr, exceptionHandler, logger);
        networkRoundtripTimeoutMs =
            config.getNetworkRoundtripTimeout(MILLISECONDS);
    }

    /** Creates a dispatcher for a rep node. */
    AsyncRequestDispatcherImpl(String kvsName,
                               RepNodeParams repNodeParams,
                               LoginManager internalLoginMgr,
                               UncaughtExceptionHandler exceptionHandler,
                               Protocols protocols,
                               Logger logger) {
        super(kvsName, repNodeParams, internalLoginMgr, exceptionHandler,
              protocols, logger);
        this.networkRoundtripTimeoutMs =
            KVStoreConfig.DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    /**
     * Executes a synchronous request by performing the request asynchronously
     * and waiting for the result.
     */
    @Override
    public Response execute(Request request,
                            RepNodeId targetId,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr)
        throws FaultException {

        /* Grab the timeout value, since it can change during execution */
        final int timeoutMs = request.getTimeout();
        final AsyncExecuteRequest asyncRequest =
            new AsyncExecuteRequest(request, targetId, excludeRNs, loginMgr);
        final Supplier<String> operation =
            () -> String.format("AsyncRequest(%s)", asyncRequest);
        if (timeoutMs <= 0) {
            throw new RequestTimeoutException(timeoutMs,
                                              operation.get() + " timed out",
                                              null, isRemote);
        }
        return AsyncRegistryUtils.getWithTimeout(
            asyncRequest.execute(), operation, timeoutMs,
            getAsyncTimeout(timeoutMs),
            ignore -> getTimeoutException(asyncRequest.request,
                                          asyncRequest.exception,
                                          timeoutMs,
                                          asyncRequest.retryCount,
                                          asyncRequest.target,
                                          asyncRequest.events));
    }

    /**
     * Returns the timeout in milliseconds that should be used for the async
     * dialog based on the request timeout.  The amount of time returned is
     * larger than the request timeout so that exceptions detected on the
     * server side can be propagated back to the client over the network.
     */
    long getAsyncTimeout(long requestTimeoutMs) {
        assert networkRoundtripTimeoutMs >= 0;
        long timeoutMs = requestTimeoutMs + networkRoundtripTimeoutMs;

        /* Correct for overflow */
        if ((requestTimeoutMs > 0) && (timeoutMs <= 0)) {
            timeoutMs = Long.MAX_VALUE;
        }
        return timeoutMs;
    }

    /**
     * Dispatches a request asynchronously to a suitable RN.
     *
     * <p> This implementation supports asynchronous operations.
     */
    @Override
    public CompletableFuture<Response> executeAsync(Request request,
                                                    Set<RepNodeId> excludeRNs,
                                                    LoginManager loginMgr) {
        return executeAsync(request, null, excludeRNs, loginMgr);
    }

    /**
     * Dispatches a request asynchronously, and also provides a parameter for
     * specifying a preferred target node, for testing.
     *
     * <p> This implementation supports asynchronous operations.
     */
    @Override
    public CompletableFuture<Response> executeAsync(Request request,
                                                    RepNodeId targetId,
                                                    Set<RepNodeId> excludeRNs,
                                                    LoginManager loginMgr) {
        try {
            return new AsyncExecuteRequest(request, targetId, excludeRNs,
                                           loginMgr)
                .execute();
        } catch (Throwable e) {
            return failedFuture(convertException(e, targetId));
        }
    }

    /**
     * Convert an exception to one that should be supplied to users. The issue
     * is to convert dialog exceptions to FaultExceptions as needed, which
     * doesn't seem to have been done systematically otherwise.
     */
    private Throwable convertException(Throwable e, RepNodeId targetId) {
        if (!(e instanceof DialogException)) {
            return e;
        }
        final DialogException dialogException = (DialogException) e;
        final Throwable userThrowable = dialogException.getUserException();
        if (!(userThrowable instanceof Exception)) {
            return userThrowable;
        }
        final Exception userException = (Exception) userThrowable;
        return convertToFaultException(
            String.format("Communication problem with %s", targetId),
            userException);
    }

    /**
     * The execution environment for a single asynchronous request.
     */
    private class AsyncExecuteRequest
            implements Runnable, DispatchExceptionHandler {

        private final Request request;
        private final RepNodeId targetId;
        private volatile Set<RepNodeId> excludeRNs;
        private final LoginManager loginMgr;
        private final CompletableFuture<Response> future =
            new CompletableFuture<>();

        private volatile RepGroupStateInfo rgStateInfo = null;
        private final int initialTimeoutMs;
        private final long limitNs;
        volatile int retryCount;

        private volatile Exception exception;
        volatile RepNodeState target;
        private volatile long retrySleepNs;
        private volatile LoginHandle loginHandle;

        private volatile long startNs;

        private volatile LinkedList<ExecuteRequestEvent> events = null;

        AsyncExecuteRequest(Request request,
                            RepNodeId targetId,
                            Set<RepNodeId> excludeRNs,
                            LoginManager loginMgr) {
            this.request = request;
            this.targetId = targetId;
            this.excludeRNs = excludeRNs;
            this.loginMgr = loginMgr;


            initialTimeoutMs = request.getTimeout();
            limitNs = System.nanoTime() +
                MILLISECONDS.toNanos(initialTimeoutMs);
            retryCount = 0;

            exception = null;
            target = null;
            retrySleepNs = 10000000; /* 10 milliseconds */
            loginHandle = null;
        }

        @Override
        public String toString() {
            return String.format(
                "request=%s, rgId=%s, retryCount=%s, lastException=%s, "
                    + "lastTarget=%s",
                request,
                (rgStateInfo != null)
                    ? rgStateInfo.repGroupState.getResourceId()
                    : "(not yet assigned)",
                retryCount, exception,
                (target != null) ? target.getRepNodeId() : null);
        }

        CompletableFuture<Response> execute() {
            try {
                logger.finest(
                    () -> String.format(
                        "AsyncRequestDispatcherImpl.execute start" +
                        " request=%s" +
                        " targetId=%s" +
                        " stacktrace=%s",
                        request,
                        targetId,
                        CommonLoggerUtils.getStackTrace(new Throwable())));
                run();
            } catch (Throwable e) {
                onResult(null, e);
            }
            if (logger.isLoggable(Level.FINE)) {
                future.whenComplete(
                    unwrapExceptionVoid(
                        (response, e) ->
                        logger.log(
                            (e == null) ? Level.FINEST : Level.FINE,
                            () -> String.format(
                                "AsyncRequestDispatcherImpl.execute end" +
                                " request=%s" +
                                " targetId=%s" +
                                " response=%s" +
                                " exception=%s",
                                request,
                                targetId,
                                response,
                                ((e == null) ?
                                 "null" :
                                 logger.isLoggable(Level.FINEST) ?
                                 CommonLoggerUtils.getStackTrace(e) :
                                 e.toString())))));
            }
            return future;
        }

        /**
         * Make requests, finishing if it can obtain a result or failure
         * without blocking, and otherwise scheduling a new call when called
         * back after waiting for an intermediate result.
         */
        @Override
        public void run() {

            /* Retry until timeout or async handoff */
            while ((limitNs - System.nanoTime()) > 0) {
                if (rgStateInfo == null) {
                    /* First time dispatch. */
                    rgStateInfo = startExecuteRequest(request);
                } else {
                    rgStateInfo = refreshRepGroupState(request, rgStateInfo);
                }
                try {
                    target = selectTarget(request, targetId,
                        rgStateInfo.repGroupState, excludeRNs);
                } catch (RNUnavailableException e) {
                    onResult(null, e);
                    return;
                } catch (NoSuitableRNException e) {
                    /*
                     * NSRN exception is thrown once the dispatcher has tried
                     * all candidate RNs in the group. There are potentially
                     * exceptions that should be thrown immediately vs
                     * retrying. This method does that.
                     */
                    RuntimeException re = checkThrowNoSuitableRN(exception);
                    if (re != null) {
                        onResult(null, re);
                        return;
                    }

                    if (preferNewException(e, exception)) {
                        exception = e;
                    }
                    retrySleepNs = computeWaitBeforeRetry(limitNs,
                                                          retrySleepNs);
                    if (retrySleepNs > 0) {
                        /*
                         * TODO: Need a better way to use the endpoint group's
                         * executor service
                         */
                        while (true) {
                            try {
                                endpointGroup.getSchedExecService()
                                    .schedule(this, retrySleepNs, NANOSECONDS);
                                break;
                            } catch (RejectedExecutionException ree) {
                                if (endpointGroup.getIsShutdown()) {
                                    throw ree;
                                }
                                /* Try another executor */
                                continue;
                            }
                        }
                        return;
                    }
                    continue;
                }

                /* Have a target RN in hand */
                startNs = 0;
                try {
                    activeRequestCount.incrementAndGet();
                    final int targetRequestCount = target.requestStart();
                    startNs = latencyTracker.markStart();
                    checkStartDispatchRequest(target, targetRequestCount);
                } catch (Exception dispatchException) {
                    handleResponse(null, dispatchException);
                    return;
                }
                addEvent(ExecuteRequestEventType.OBTAIN_REQUEST_HANDLER);
                target.getReqHandlerRefAsync(
                    regUtils, NANOSECONDS.toMillis(limitNs - startNs))
                    .whenComplete(
                        unwrapExceptionVoid(this::handleRequestHandler));
                return;
            }
            onResult(null,
                     getTimeoutException(request, exception, initialTimeoutMs,
                                         retryCount, target, events));
        }

        /**
         * Record the exception and complete the dispatch operation, requesting
         * it be retried.
         */
        @Override
        public void retryDispatch(Exception dispatchException) {
            if (preferNewException(dispatchException, exception)) {
                exception = dispatchException;
            }
            completeDispatch(null /* response */, dispatchException,
                             false /* done */);
        }

        /** Complete the dispatch operation as completed with an exception */
        @Override
        public void failDispatch(Throwable dispatchException) {
            /* Was the dispatch exception for a timeout? */
            Exception timeoutException =
                dispatchException instanceof RequestTimeoutException ?
                (RequestTimeoutException) dispatchException :
                null;
            /* Maybe prefer saved exception */
            if ((dispatchException instanceof Exception) &&
                !preferNewException((Exception) dispatchException,
                                    exception)) {
                dispatchException = exception;
                if (timeoutException != null) {
                    timeoutException = exception;
                }
            }
            /* Use the correct exception on a timeout */
            if (timeoutException != null) {
                dispatchException = getTimeoutException(
                    request, timeoutException, initialTimeoutMs, retryCount,
                    target, events);
            }
            completeDispatch(null /* response */, dispatchException,
                             true /* done */);
        }

        /**
         * Deliver a result to the future, using a separate thread if needed to
         * avoid recursion.
         */
        void onResult(Response response, Throwable e) {
            if (deliveringResult.get()) {
                logger.finest("Detected recursion");
                doAsync(() -> onResult(response, e));
                return;
            }
            logger.log((e == null) ? Level.FINEST : Level.FINE,
                       () -> String.format(
                           "Done executing async" +
                           " request=%s" +
                           " targetId=%s" +
                           " response=%s" +
                           " e=%s",
                           request,
                           targetId,
                           response,
                           ((e == null) ?
                            "null" :
                            logger.isLoggable(Level.FINEST) ?
                            CommonLoggerUtils.getStackTrace(e) :
                            e.toString())));
            deliveringResult.set(true);
            try {
                complete(future, response,
                         convertException(e, targetId));
                return;
            } finally {
                deliveringResult.set(false);
            }
        }

        private void doAsync(Runnable action) {

            /*
             * TODO: Need a better way to use the endpoint group's executor
             * service
             */
            while (true) {
                try {
                    endpointGroup.getSchedExecService().execute(action);
                    return;
                } catch (RejectedExecutionException ree) {
                    if (endpointGroup.getIsShutdown()) {
                        break;
                    }
                    /* Try another executor */
                    continue;
                }
            }

            /*
             * There must have been a race condition during store shutdown.
             * Create a new thread that just delivers the result, and don't
             * worry about the inefficiency of that approach because it should
             * happen only at shutdown.
             */
            backupThreadFactory.newThread(action);
        }

        /** Handle the result from attempt to obtain the request handler. */
        void handleRequestHandler(AsyncRequestHandlerAPI requestHandler,
                                  Throwable dispatchException) {
            addEvent(ExecuteRequestEventType
                     .OBTAIN_REQUEST_HANDLER_RESPONDED,
                     dispatchException);
            if (requestHandler == null) {
                /*
                 * Save this exception unless the current one is more
                 * interesting, but don't set dispatchException, because we
                 * want to try again.
                 */
                final IllegalStateException newException =
                    new IllegalStateException(
                        "Could not establish handle to " +
                        target.getRepNodeId());
                if (preferNewException(newException, exception)) {
                    exception = newException;
                }
            } else {
                addEvent(ExecuteRequestEventType.DISPATCH_REQUEST);
                prepareRequest(request, limitNs, retryCount++, target,
                               loginMgr)
                    .thenCompose(lh -> {
                            loginHandle = lh;
                            return requestHandler.execute(
                                request,
                                getAsyncTimeout(request.getTimeout()));
                        })
                    .whenComplete(
                        unwrapExceptionVoid(this::handleResponse));
                return;
            }
            handleResponse(null, dispatchException);
        }

        /** Handles a response, retrying asynchronously if needed */
        private void handleResponse(Response response, Throwable t) {
            logger.finest(() -> "AsyncRequestDispatcherImpl.handleResponse" +
                          " response=" + response +
                          " t=" + t);
            addEvent(
                ExecuteRequestEventType.DISPATCH_REQUEST_RESPONDED, t);
            try {

                /* No exception, request is done if response is non-null */
                if (t == null) {
                    completeDispatch(response, null /* t */,
                                     response != null /* done */);
                    return;
                }

                /* A non-Exception means request failed */
                if (!(t instanceof Exception)) {
                    failDispatch(t);
                    return;
                }
                final Exception e = (Exception) t;

                /* Handle dialog exceptions */
                if (e instanceof DialogException) {
                    retryDispatch(
                        handleDialogException(request, target,
                                              (DialogException) e));
                    return;
                }

                /* Handle other dispatch exceptions */
                handleDispatchException(request,
                                        initialTimeoutMs,
                                        target, e,
                                        loginHandle,
                                        this /* handler */);
            } catch (Throwable t2) {

                /* Throwing exceptions during handling means request failed */
                failDispatch(t2);
            }
        }

        /*
         * Record the completion of a request attempt, ending the request as
         * done if done is true, and otherwise retrying.
         */
        private void completeDispatch(Response response,
                                      Throwable t,
                                      boolean done) {
            /*
             * Run in a separate thread if the response contains topo info,
             * because the topo processing can block.
             */
            if ((response == null) || (response.getTopoInfo() == null)) {
                completeDispatchInternal(response, t, done);
            } else {
                CompletableFuture.runAsync(
                    () -> completeDispatchInternal(response, t, done));
            }
        }

        /* This method may block if the response includes topo info */
        private void completeDispatchInternal(Response response,
                                              Throwable t,
                                              boolean done) {
            excludeRNs = dispatchCompleted(startNs, request, response, target,
                                           t, excludeRNs);
            if (done) {
                onResult(response, t);
            } else {

                /*
                 * Retry asynchronously to keep from recursing, which could
                 * exceed the stack depth
                 */
                doAsync(this::run);
            }
        }

        private void addEvent(ExecuteRequestEventType eventType) {
            addEvent(eventType, null /* throwable */);
        }

        private void addEvent(ExecuteRequestEventType eventType,
                              Throwable throwable)
        {
            final RepNodeState rn = target;
            final RepNodeId rnId = (rn == null) ? null : rn.getRepNodeId();
            events = addExecuteRequestEvent(
                events, eventType, rnId, throwable);
        }
    }

    @Override
    protected
        CompletableFuture<LoginHandle> withLoginHandle(LoginManager loginMgr,
                                                       RepNodeId repNodeId) {
        try {
            final LoginHandle loginHandle =
                loginMgr.getHandle(repNodeId, true /* cachedOnly */);
            if (loginHandle != null) {
                return completedFuture(loginHandle);
            }

            /*
             * Make the call in another thread if the handle isn't cached since
             * the call might block
             */
            return CompletableFuture.supplyAsync(
                () -> loginMgr.getHandle(repNodeId));
        } catch (Throwable e) {
            return failedFuture(convertException(e, repNodeId));
        }
    }

    /** Perform login renewal in a separate thread since it can block. */
    @Override
    protected void
        handleRenewalRequiredException(LoginHandle loginHandle,
                                       LoginToken currToken,
                                       AuthenticationRequiredException are,
                                       DispatchExceptionHandler handler) {
        try {
            CompletableFuture.runAsync(
                () -> super.handleRenewalRequiredException(
                    loginHandle, currToken, are, handler));
        } catch (Throwable t) {
            handler.failDispatch(t);
        }
    }

    @Override
    protected int rankException(Exception e) {
        final int superResult = super.rankException(e);
        final int localResult =

            /*
             * Next after the #1 ranked ConsistencyException from the super
             * class, prefer ServerResourceLimitException since it provides
             * more specific information than remaining exceptions
             */
            (e instanceof ServerResourceLimitException) ? 2 :

            /*
             * After that, prefer PersistentDialogException so that the
             * caller knows to back off
             */
            (e instanceof PersistentDialogException) ? 3 :
            10;

        /* Return the best (lowest) ranking of the two */
        return Math.min(superResult, localResult);
    }

    /**
     * Provide handling for dialog exceptions, checking for side effects on
     * writes, and unwrapping dialog and connection exceptions to obtain the
     * underlying exception.  Returns the exception if the request should be
     * retried, and throws an exception if it should not be retried.
     */
    Exception handleDialogException(Request request,
                                    RepNodeState target,
                                    DialogException dialogException) {
        logger.finest(
            () -> "AsyncRequestDispatcherImpl.handleDialogException" +
            " request=" + request +
            " target=" + target +
            " dialogException=" +
            CommonLoggerUtils.getStackTrace(dialogException));
        if (dialogException instanceof DialogUnknownException) {
            throwAsFaultException("Internal error", dialogException);
        }

        /*
         * Fail if there could be side effects and this is a write operation,
         * because the side effects mean it isn't safe to retry automatically.
         */
        if (dialogException.hasSideEffect()) {
            faultIfWrite(
                request,
                String.format(
                    "Communication problem with %s", target.getRepNodeId()),
                dialogException);
        }

        /*
         * Note the exception, which removes this RN from consideration for
         * subsequent dispatches until the state update thread re-established
         * the handle.
         */
        if (dialogException.isPersistent()) {
            target.noteReqHandlerException(dialogException);
        }
        return dialogException;
    }

    /**
     * Add handling for exceptions that need to be translated to a user
     * exception.
     */
    @Override
    void throwAsFaultException(String faultMessage, Exception exception)
        throws FaultException {

        if (exception instanceof GetUserException) {
            final Throwable t =
                ((GetUserException) exception).getUserException();
            if (t instanceof Error) {
                throw (Error) t;
            }
            exception = (t instanceof Exception) ?
                (Exception) t :
                new IllegalStateException("Unexpected exception: " + t, t);
        }
        super.throwAsFaultException(faultMessage, exception);
    }

    /**
     * Implement synchronous version using an asynchronous request.
     */
    @Override
    public Response executeNOP(RepNodeState rns,
                               int timeoutMs,
                               LoginManager loginMgr) {
        return AsyncRegistryUtils.getWithTimeout(
            executeNOPAsync(rns, timeoutMs, loginMgr), "NOP request",
            timeoutMs);
    }

    /**
     * Asynchronous version to dispatch the special NOP request.  Keep this
     * method up-to-date with the sync version in RequestDispatcherImpl.
     */
    public CompletableFuture<Response> executeNOPAsync(RepNodeState rns,
                                                       int timeoutMs,
                                                       LoginManager loginMgr) {
        try {
            return rns.getReqHandlerRefAsync(getRegUtils(), timeoutMs)
                .thenCompose(
                    api -> executeNOPAsync(api, rns, timeoutMs, loginMgr));
        } catch (Throwable e) {
            return failedFuture(
                convertException(e, rns.getRepNodeId()));
        }
    }

    private CompletableFuture<Response>
        executeNOPAsync(AsyncRequestHandlerAPI ref,
                        RepNodeState rns,
                        int timeoutMs,
                        LoginManager loginMgr) {
        if (ref == null) {
            /* needs to be resolved. */
            return completedFuture(null);
        }

        long startTimeNs = System.nanoTime();
        Request nop = null;
        CompletableFuture<Response> future;
        try {
            rns.requestStart();
            activeRequestCount.incrementAndGet();
            startTimeNs = latencyTracker.markStart();
            final int topoSeqNumber =
                getTopologyManager().getTopology().getSequenceNumber();
            nop = Request.createNOP(topoSeqNumber,
                                    getDispatcherId(),
                                    timeoutMs);
            nop.setSerialVersion(rns.getRequestHandlerSerialVersion());
            if (loginMgr != null) {
                final Request nopFinal = nop;
                future = withLoginHandle(loginMgr, rns.getRepNodeId())
                    .thenCompose(loginHandle -> {
                            nopFinal.setAuthContext(
                                new AuthContext(loginHandle.getLoginToken()));
                            return ref.execute(
                                nopFinal,
                                getAsyncTimeout(nopFinal.getTimeout()));
                        });

            } else {
                future = ref.execute(nop, getAsyncTimeout(nop.getTimeout()));
            }
        } catch (Throwable e) {
            future = failedFuture(
                convertException(e, rns.getRepNodeId()));
        }
        final long startTimeNsFinal = startTimeNs;
        final Request nopFinal = nop;
        return whenComplete(
            future.thenCompose(
                response -> {
                    if (response == null) {
                        return completedFuture(null);
                    }
                    if (response.getTopoInfo() == null) {
                        processResponse(startTimeNsFinal, nopFinal, response);
                        return completedFuture(response);
                    }
                    /*
                     * If the topo info is not null, then processResponse might
                     * block when handling it, so run it asynchronously
                     */
                    logger.finest("Processing response asynchronously" +
                                  " because it contains topology information");
                    return CompletableFuture.supplyAsync(
                        () -> {
                            processResponse(startTimeNsFinal, nopFinal,
                                            response);
                            return response;
                        },
                        endpointGroup.getBackupExecService());
                }),
            (response, exception) -> {
                /*
                 * If it is a communications problem (a DialogException) that
                 * asks that the caller back off, then note the exception so we
                 * back off on this node until the state update thread tries
                 * again.
                 */
                if (exception instanceof DialogException) {
                    final DialogException de = (DialogException) exception;
                    if (de.isPersistent()) {
                        rns.noteReqHandlerException(de);
                    }
                }

                rns.requestEnd();
                activeRequestCount.decrementAndGet();
                latencyTracker.markFinish(OpCode.NOP, startTimeNsFinal);
            });
    }

    /**
     * This implementation returns the default local dialog layer limit.
     */
    @Override
    protected int getMaxActiveRequests() {
        return EndpointConfigBuilder.getOptionDefault(
            AsyncOption.DLG_LOCAL_MAXDLGS);
    }

    @Override
    public long getNetworkRoundtripTimeoutMs() {
        return networkRoundtripTimeoutMs;
    }
}
