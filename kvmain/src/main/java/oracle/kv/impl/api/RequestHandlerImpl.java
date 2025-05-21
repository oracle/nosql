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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder.withThreadDialogContext;
import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.FutureUtils.complete;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapException;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.async.FutureUtils.whenComplete;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY;
import static oracle.kv.impl.util.registry.AsyncRegistryUtils.getWithTimeout;
import static oracle.nosql.common.contextlogger.ContextUtils.finestWithCtx;
import static oracle.nosql.common.contextlogger.ContextUtils.isLoggableWithCtx;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.Consistency;
import oracle.kv.ConsistencyException;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.RequestLimitException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ServerResourceLimitException;
import oracle.kv.UnauthorizedException;
import oracle.kv.Version;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.InternalOperation.OpCode;
import oracle.kv.impl.api.ops.OperationHandler;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableVersionException;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.query.QueryRuntimeException;
import oracle.kv.impl.rep.EnvironmentFailureRetryException;
import oracle.kv.impl.rep.IncorrectRoutingException;
import oracle.kv.impl.rep.OperationsStatsTracker;
import oracle.kv.impl.rep.RepEnvHandleManager.StateChangeListenerFactory;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RequestTypeUpdater.RequestType;
import oracle.kv.impl.rep.migration.MigrationService;
import oracle.kv.impl.rep.migration.MigrationStreamHandle;
import oracle.kv.impl.rep.migration.generation.PartitionGenNum;
import oracle.kv.impl.rep.migration.generation.PartitionGeneration;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.rep.table.ResourceCollector;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.ConsistencyTranslator;
import oracle.kv.impl.util.DurabilityTranslator;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ReusingThreadPoolExecutor;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.WaitableCounter;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.nosql.common.sklogger.measure.LatencyElement;

import com.sleepycat.je.AsyncAckHandler;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.DatabasePreemptedException;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.LockPreemptedException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.RollbackProhibitedException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DoubleExpMovingAvg;
import com.sleepycat.je.utilint.FIOStatsCollectingThread;

/**
 * @see RequestHandler
 */
public class RequestHandlerImpl extends VersionedRemoteImpl
        implements AsyncRequestHandler, RequestHandler {

    /**
     * Amount of time to wait after a lock conflict. Since we order all
     * multi-key access in the KV Store by key, the only source of lock
     * conflicts should be on the Replica as {@link LockPreemptedException}s.
     */
    private static final int LOCK_CONFLICT_RETRY_NS = 100000000;

    /**
     * Amount of time to wait after an environment failure. The wait allows
     * some time for the environment to restart, partition databases to be
     * re-opened, etc.  This is also the amount of time used to wait when
     * getting the environment, so the effective wait for the environment is
     * twice this value.
     */
    private static final int ENV_RESTART_RETRY_NS = 100000000;

    /**
     * The max number of faults to track for the purposes of log rate limiting.
     */
    private static final int LIMIT_FAULTS = 20;

    /**
     * 1 min in millis
     */
    private static final int ONE_MINUTE_MS = 60 * 1000;

    /**
     * 10 mins in millis
     */
    private static final int TEN_MINUTES_MS = 10 * 60 * 1000;

    /**
     * An empty, immutable privilege list, used when an operation is requested
     * that does not require authentication.
     */
    private static final List<KVStorePrivilege> emptyPrivilegeList =
        Collections.emptyList();

    /**
     * Provide a way to disable async acknowledgements, just for in case there
     * is a problem with using them.
     */
    private static final boolean disableAsyncAckHandler =
        Boolean.getBoolean("oracle.kv.disableAsyncAckHandler");

    /**
     * The parameters used to configure the request handler.
     */
    private RepNodeService.Params params;

    /**
     * The rep node that will be used to satisfy all operations.
     */
    private RepNode repNode;

    /**
     *  Identifies the rep node servicing local requests.
     */
    private RepNodeId repNodeId;

    /**
     * Tracks the number of requests that are currently active, that is,
     * are being handled.
     */
    private final WaitableCounter activeRequests = new WaitableCounter();

    /**
     * Tracks the total requests received in this interval.
     */
    private final WaitableCounter totalRequests = new WaitableCounter();

    /**
     * Tracks the number of asynchronous requests in the queue waiting to be
     * executed.
     */
    private final AtomicInteger queuedAsyncRequestsCount = new AtomicInteger();

    /**
     * The count of push and pop operations on the async request queue, for use
     * in computing the moving average.
     */
    private final AtomicLong asyncRequestQueueOpsCount = new AtomicLong();

    /**
     * Tracks an exponential moving average over the last 1 million
     * asynchronous requests of the number of requests waiting in the queue to
     * be executed. Specify the "time interval" as 2 million, where the time
     * actually represents the push and pop operations.
     */
    private final DoubleExpMovingAvg avgQueuedAsyncRequests =
        new DoubleExpMovingAvg("avgQueuedAsyncRequests", 2000000);

    /**
     * Tracks the maximum number of items in the queue asynchronous requests
     * waiting to be executed.
     */
    private final AtomicInteger maxQueuedAsyncRequests = new AtomicInteger();

    /**
     * Tracks the amount of time in nanoseconds that requests remain in the
     * async request queue.
     */
    private final LatencyElement asyncRequestQueueTimeNanos =
        new LatencyElement();

    /**
     * Table operation counters. These counters keep track of active table
     * operations associated with a specific version of table metadata.
     * The sequence number of the table metadata at the time the operation
     * starts is used to index into the array. The counter at that location
     * is incremented when the operation starts and decremented at the end.
     * The same counter is decremented even if the metadata changes during
     * the operation.
     *
     */
    /* Number of counters must be power of 2 */
    private static final int N_COUNTERS = 8;

    /*
     * The mask is used to create an index into the tableOpCounter array
     * from a metadata seq num.
    */
    private static final int INDEX_MASK = N_COUNTERS-1;

    /* Counters keeping track of active table operations by metadata version */
    final WaitableCounter[] tableOpCounters = new WaitableCounter[N_COUNTERS];

    /* Keeps track of the aggregate "raw" RW throughput. */
    private final AggregateThroughputTracker aggregateTracker =
                                            new AggregateThroughputTracker();

    /**
     * A Map to track exception requests count at this node in this interval.
     * Key is fully qualified exception name, value is count.
     */
    private final AtomicReference<Map<String, AtomicInteger>> exceptionCounts;

    /**
     * All user operations directed to the requestHandler are implemented by
     * the OperationHandler.
     */
    private OperationHandler operationHandler;

    /**
     * The requestDispatcher used to forward requests that cannot be handled
     * locally.
     */
    private final RequestDispatcher requestDispatcher;

    /**
     * Mediates access to the shared topology.
     */
    private final TopologyManager topoManager;

    /**
     *  The table used to track the rep state of all rep nodes in the KVS
     */
    private final RepGroupStateTable stateTable;

    /**
     *  The last state change event communicated via the Listener.
     *
     * Initialize the value to the unknown state so that we can send that value
     * if, for some reason, the RN fails to switch from it's initial unknown
     * state before it starts handling requests. [KVSTORE-684]
     */
    private volatile StateChangeEvent stateChangeEvent =
        new StateChangeEvent(State.UNKNOWN, NameIdPair.NULL);

    /**
     * The set of requesters that have been informed of the latest state change
     * event.
     */
    private final Map<ResourceId, RequesterMapValue> requesterMap;

    /**
     * A timer that schedules requesterMap cleanup work, to be run periodically.
     */
    private volatile Timer reqMapCleanupTimer;

    /*
     * requesterMap cleanup period.
     */
    private volatile long reqMapCleanupPeriodMs = ONE_MINUTE_MS * 60;

    /*
     * requesterMap entry lifetime.
     */
    private volatile long reqMapEntryLifetimeNs = TimeUnit.MILLISECONDS.toNanos(
        ONE_MINUTE_MS * 60 * 24);

    /**
     * opTracker encapsulates all repNode stat recording.
     */
    private OperationsStatsTracker opTracker;

    /**
     * The amount of time to wait for the active requests to quiesce, when
     * stopping the request handler.
     */
    private int requestQuiesceMs;

    /**
     * The poll period used when quiescing requests.
     */
    private static final int REQUEST_QUIESCE_POLL_MS = 100;

    private final ProcessFaultHandler faultHandler;

    /**
     * Test hook used to during request handling to introduce request-specific
     * behavior.
     */
    private TestHook<Request> requestExecute;

    /**
     * Test hook used to during NOP handling.
     */
    private TestHook<Request> requestNOPExecute;

    /**
     * The access checking implementation.  This will be null if security is
     * not enabled.
     */
    private final AccessChecker accessChecker;

    /**
     * Test hook to be invoked immediately before initiating a request
     * transaction commit.
     */
    private TestHook<RepImpl> preCommitTestHook;

    /**
     * Test hook to be invoked before the response is returned.
     */
    private TestHook<Result> preResponseTestHook;

    private Logger logger = null;

    private ListenHandle asyncServerHandle = null;

    /**
     * Thread pool for executing async requests, or null if async is
     * disabled. Set to null on shutdown.
     */
    private volatile ReusingThreadPoolExecutor asyncThreadPool;

    /*
     * Encapsulates the above logger to limit the rate of log messages
     * associated with a specific fault.
     */
    private RateLimitingLogger<String> rateLimitingLogger;

    /**
     * Rate-limiting logger with longer sampling interval
     */
    private RateLimitingLogger<String> rateLimitingLoggerLong;

    /**
     * Flags indicate request type enabled on this node.
     */
    private volatile RequestType enabledRequestsType = RequestType.ALL;

    /**
     * Disable new (strict) auth master semantics and revert to old semantics.
     */
    private boolean disableAuthMaster = false;

    public RequestHandlerImpl(RequestDispatcher requestDispatcher,
                              ProcessFaultHandler faultHandler,
                              AccessChecker accessChecker) {
        super();

        this.requestDispatcher = requestDispatcher;
        this.faultHandler = faultHandler;
        this.topoManager = requestDispatcher.getTopologyManager();
        this.accessChecker = accessChecker;
        stateTable = requestDispatcher.getRepGroupStateTable();
        requesterMap = new ConcurrentHashMap<>();
        this.exceptionCounts =
           new AtomicReference<>(new ConcurrentHashMap<String,
                                                       AtomicInteger>());
        for (int i = 0; i < N_COUNTERS; i++) {
            tableOpCounters[i] = new WaitableCounter();
        }
    }

    @SuppressWarnings("hiding")
    public void initialize(RepNodeService.Params params,
                           RepNode repNode,
                           OperationsStatsTracker opStatsTracker) {
        this.params = params;

        /* Get the rep node that we'll use for handling our requests. */
        this.repNode = repNode;
        opTracker = opStatsTracker;
        repNodeId = repNode.getRepNodeId();
        operationHandler = new OperationHandler(repNode, params);
        final RepNodeParams rnParams = params.getRepNodeParams();
        requestQuiesceMs = rnParams.getRequestQuiesceMs();
        logger = LoggerUtils.getLogger(this.getClass(), params);
        rateLimitingLogger = new RateLimitingLogger<>
                                        (ONE_MINUTE_MS, LIMIT_FAULTS, logger);
        rateLimitingLoggerLong = new RateLimitingLogger<>
                                        (TEN_MINUTES_MS, LIMIT_FAULTS, logger);
        enableRequestType(rnParams.getEnabledRequestType());
        disableAuthMaster = rnParams.getDisableAuthMaster();
    }

    /**
     * Computes the async request queue size based on the difference between
     * max concurrent requests and max threads.
     */
    private static int getAsyncExecQueueSize(RepNodeParams rnParams) {
        final int maxRequests = rnParams.getAsyncMaxConcurrentRequests();
        final int maxThreads = rnParams.getAsyncExecMaxThreads();
        /*
         * The queue needs to hold at least one item although, in practice, we
         * expect it to be quite a bit larger.
         */
        return Math.max(1, maxRequests - maxThreads);
    }

    /**
     * Update the async thread pool for a change in parameters.
     */
    public void updateAsyncThreadPoolParams(RepNodeParams oldParams,
                                            RepNodeParams newParams) {
        final ReusingThreadPoolExecutor threadPool = asyncThreadPool;
        if (threadPool == null) {
            return;
        }

        if (oldParams.getAsyncExecThreadKeepAliveMs() !=
            newParams.getAsyncExecThreadKeepAliveMs()) {
            threadPool.setKeepAliveTime(
                newParams.getAsyncExecThreadKeepAliveMs(), MILLISECONDS);
        }
    }

    /**
     * Returns the number of requests currently being processed at this node.
     */
    public int getActiveRequests() {
        return activeRequests.get();
    }

    /**
     * Returns the aggregate read/write KB resulting from direct application
     * requests. RWKB from housekeeping tasks is not included.
     */
    public AggregateThroughputTracker getAggregateThroughputTracker() {
        return aggregateTracker;
    }

    /**
     * Returns total requests received at this node and then reset it to 0.
     */
    public int getAndResetTotalRequests() {
        return totalRequests.getAndSet(0);
    }

    /**
     * Returns a rolling average of the number of asynchronous requests open on
     * this node.
     */
    public int getAverageQueuedAsyncRequests() {
        return (int) Math.min(Integer.MAX_VALUE,
                              Math.round(avgQueuedAsyncRequests.get()));
    }

    /**
     * Returns the maximum number of asynchronous requests that were open at
     * the same time and then resets the value to 0.
     */
    public int getAndResetMaxQueuedAsyncRequests() {
        return maxQueuedAsyncRequests.getAndSet(0);
    }

    /**
     * Returns statistics about the amount of time in nanoseconds that
     * requests have remained in the async request queue.
     */
    public LatencyElement.Result getAsyncRequestQueueTimeStatsNanos(
        String watcherName) {

        return asyncRequestQueueTimeNanos.obtain(watcherName);
    }

    /**
     * Returns the exception count map at this node and reset counts to 0.  The
     * returned exception count map will no longer be modified, and can be used
     * by the caller without fear of ConcurrentModificationeExceptions.
     */
    public Map<String, AtomicInteger> getAndResetExceptionCounts() {

        Map<String, AtomicInteger> nextCounts =
            new ConcurrentHashMap<>();
        Map<String, AtomicInteger> oldCounts =
            exceptionCounts.getAndSet(nextCounts);
        return oldCounts;
    }

    /**
     * Returns the RepNode associated with this request handler.
     */
    public RepNode getRepNode() {
       return repNode;
    }

    /**
     * Returns the request dispatcher used to forward requests.
     */
    public RequestDispatcher getRequestDispatcher() {
        return requestDispatcher;
    }

    public StateChangeListenerFactory getListenerFactory() {
        return new StateChangeListenerFactory() {

            @Override
            public StateChangeListener create(ReplicatedEnvironment repEnv) {
                return new Listener(repEnv);
            }
        };
    }

    public void setTestHook(TestHook<Request> hook) {
        requestExecute = hook;
    }

    public void setTestNOPHook(TestHook<Request> hook) {
        requestNOPExecute = hook;
    }

    public void setPreCommitTestHook(TestHook<RepImpl> hook) {
        preCommitTestHook = hook;
    }

    public void setPreResponseTestHook(TestHook<Result> hook) {
        preResponseTestHook = hook;
    }

    @Override
    public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                     long timeoutMillis) {
        try {
            return completedFuture(getSerialVersion());
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    /**
     * Apply security checks and execute the request
     */
    @Override
    public Response execute(final Request request)
        throws FaultException, RemoteException {

        final long timeoutMs = request.getTimeout();
        if (timeoutMs <= 0) {
            throw new RequestTimeoutException(
                (int) timeoutMs, request + " timed out", null /* cause */,
                true /* isRemote */);
        }
        return getWithTimeout(executeFuture(request), "execute", timeoutMs);
    }

    /**
     * Execute the request, returning a future that supplies the result or any
     * exception thrown.
     */
    private CompletableFuture<Response> executeFuture(Request request) {
        return faultHandler.executeFuture(
            (Supplier<CompletableFuture<Response>>)
            () -> {
                final ExecutionContext execCtx = checkSecurity(request);
                return (execCtx == null) ?
                    trackExecuteRequest(request) :
                    ExecutionContext.runWithContext(
                        (Supplier<CompletableFuture<Response>>)
                        () -> trackExecuteRequest(request),
                        execCtx);
            });
    }

    @Override
    public CompletableFuture<Response> execute(final Request request,
                                               long timeoutMillis) {
        final ReusingThreadPoolExecutor threadPool = asyncThreadPool;
        try {
            if (threadPool == null) {
                throw new IllegalStateException(
                    "Async is disabled or shutdown");
            }
            final DialogContext context =
                AsyncVersionedRemoteDialogResponder.getThreadDialogContext();
            final long startTimeNanos = System.nanoTime();
            asyncQueueDelta(1);
            final CompletableFuture<Response> future =
                new CompletableFuture<>();
            try {
                threadPool.execute(() -> {
                        asyncQueueDelta(-1);
                        asyncRequestQueueTimeNanos.observe(
                            System.nanoTime() - startTimeNanos);
                        executeAsyncRequest(context, request)
                            .whenComplete(
                                unwrapExceptionVoid(
                                    (response, exception) -> {
                                        complete(future, response, exception);
                                    }));
                    });
                return future;
            } catch (RejectedExecutionException e) {
                asyncQueueDelta(-1);

                /*
                 * We expect the dialog layer to enforce limits so that
                 * requests can be rejected without needing to deserialize
                 * them, but handle the rejection exception here anyway just in
                 * case, since there may be edge cases where not all of the
                 * executor's capacity is used.
                 */
                final String msg =
                    "Number of concurrent requests received by " +
                    repNodeId + " exceeded the capacity of the execution" +
                    " thread pool:" +
                    " queueCapacity=" + threadPool.getQueueCapacity() +
                    " maxThreads=" + threadPool.getMaximumPoolSize();
                rateLimitingLogger.log(
                    "Async thread pool rejected execution exception",
                    Level.WARNING, e, () -> msg);
                throw new ServerResourceLimitException(msg);
            }
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    /** Update stats for a change in the number of queued async requests. */
    private void asyncQueueDelta(int delta) {
        final int current = queuedAsyncRequestsCount.addAndGet(delta);
        maxQueuedAsyncRequests.getAndUpdate(max -> Math.max(max, current));
        avgQueuedAsyncRequests.add(
            current, asyncRequestQueueOpsCount.incrementAndGet());
    }

    /**
     * Execute a request received via an async call within the specified dialog
     * context and delivering the result to the handler.
     */
    private CompletableFuture<Response>
        executeAsyncRequest(DialogContext context, Request request)
    {
        try {
            return withThreadDialogContext(
                context, checked(() -> executeFuture(request)));
        } catch (Throwable e) {
            return failedFuture(unwrapException(e));
        }
    }

    /** Returns the async thread pool -- for testing. */
    public ReusingThreadPoolExecutor getAsyncThreadPool() {
        return asyncThreadPool;
    }

    /**
     * Verify that the request is annotated with an appropriate access
     * token if security is enabled.  Only basic access checking is performed
     * at this level.  Table/column level access checks are implemented
     * at a deeper level.
     *
     * @throws SessionAccessException if there is an internal security error
     * @throws KVSecurityException if a security exception is
     * generated by the requesting client
     * @throws WrappedClientException if there is a
     * AuthenticationRequiredException, wrap the exception and return to client.
     */
    private ExecutionContext checkSecurity(Request request)
        throws SessionAccessException, KVSecurityException,
               WrappedClientException {
        if (accessChecker == null) {
            return null;
        }

        try {
            final RequestContext reqCtx = new RequestContext(request);
            return ExecutionContext.create(
                accessChecker, request.getAuthContext(), reqCtx);
        } catch (AuthenticationRequiredException are) {
            /* Return the exception to the client */
            throw new WrappedClientException(are);
        }
    }

    /**
     * Add tracking request count and exception count to execute the request.
     */
    private
        CompletableFuture<Response> trackExecuteRequest(final Request request)
    {
        activeRequests.incrementAndGet();
        totalRequests.incrementAndGet();

        /*
         * The table operation counters keep track of active table
         * operations associated with a specific version of table metadata.
         * The sequence number of the table metadata at the time the operation
         * starts is used to index into the array. The counter at that location
         * is incremented and then decremented when the operation completes.
         * The same counter is decremented even if the metadata changes during
         * the operation.
         */
        final boolean isTableOp = request.getOperation().isTableOp();
        final int index;
        if (isTableOp) {
            /* During unit tests the MD sequence number returned may be 0 */
            index = repNode.getMetadataSeqNum(MetadataType.TABLE) & INDEX_MASK;
            tableOpCounters[index].incrementAndGet();
        } else {
            index = 0;
        }
        CompletableFuture<Response> future;
        try {
            future = executeRequest(request);
        } catch (Throwable e) {
            future = failedFuture(unwrapException(e));
        }
        return whenComplete(
            future,
            (response, e) -> {
                try {
                    if (e instanceof Exception) {
                        /*
                         * To collect the root exception of
                         * WrappedClientExceptions in the collector, for better
                         * analyzing. Requirement comes from [ANDC-309]
                         */
                        Throwable cause = e;
                        if (e instanceof WrappedClientException &&
                            (e.getCause() != null)) {
                            cause = e.getCause();
                        }
                        final String exception = cause.getClass().getName();
                        Map<String, AtomicInteger> counts =
                            exceptionCounts.get();
                        counts.putIfAbsent(exception, new AtomicInteger(0));
                        counts.get(exception).incrementAndGet();
                        exceptionCounts.compareAndSet(counts, counts);
                    }
                } finally {
                    activeRequests.decrementAndGet();

                    if (isTableOp) {
                        tableOpCounters[index].decrementAndGet();
                    }
                }
            });
    }

    /**
     * Executes the operation associated with the request.
     * <p>
     * All recoverable JE operations are handled at this level and retried
     * after appropriate corrective action. The implementation body comments
     * describe the correct action.
     * <p>
     */
    private CompletableFuture<Response> executeRequest(final Request request) {

        try {
            final OpCode opCode = request.getOperation().getOpCode();

            if (OpCode.NOP.equals(opCode)) {
                return completedFuture(executeNOPInternal(request));
            } else if (topoManager.getTopology() != null) {
                return executeInternal(request);
            } else {

                /*
                 * The Topology is null. The node is waiting for one
                 * of the members in the rep group to push topology
                 * to it. Have the dispatcher re-direct the request to
                 * another node, or keep retrying until the topology
                 * is available.
                 */
                final String message = "awaiting topology push";
                throw new RNUnavailableException(message);
            }
        }  catch (ThreadInterruptedException  tie) {
            /* This node may be going down. */
            final String message =
                "RN: " + repNodeId + " was interrupted.";
            logger.info(message);
            /* Have the request dispatcher retry at a different node.*/
            throw new RNUnavailableException(message);
        }
    }

    /**
     * Wraps the execution of the request in a transaction and handles
     * any exceptions that might result from the execution of the request.
     */
    private CompletableFuture<Response> executeInternal(Request request) {

        assert TestHookExecute.doHookIfSet(requestExecute, request);
        Response response = forwardIfRequired(request);
        if (response != null) {
            return completedFuture(response);
        }
        return new ExecuteRequest(request).execute();
    }

    /**
     * Holds the context needed to execute a request, particularly for when the
     * request is completed by a JE async acknowledgements handler.
     */
    private class ExecuteRequest {
        private final Request request;
        private final InternalOperation internalOp;

        /*
         * Non-final fields accessed by AckHandler either directly, or
         * indirectly via cleanup() or getResponse(), so volatile for thread
         * safety.
         */
        private volatile Transaction txn;
        private volatile MigrationStreamHandle streamHandle;
        private volatile ReplicatedEnvironment repEnv;
        private volatile long startNs;
        private volatile Result result;
        private volatile Response response;

        /* Fields only accessed from the main execute thread */
        private long limitNs;
        private OperationFailureException exception;

        ExecuteRequest(Request request) {
            this.request = request;
            internalOp = request.getOperation();
        }

        CompletableFuture<Response> execute() {
            final LogContext lc = request.getLogContext();
            if (isLoggableWithCtx(logger, Level.FINEST, lc)) {
                finestWithCtx(logger, "executing " + request.toString(), lc);
            }
            /* Can be processed locally. */

            /*
             * Track resource usage. If tc is null the operation is not
             * a table operation or tracking for the table is not enabled.
             */
            internalOp.setTimeout(request.getTimeout());

            /*
             * Check whether this is a no-charge operation. In that case ignore
             * table resource collection (if any).
             */
            final ResourceCollector rc = request.isNoCharge() ? null :
                repNode.getTableManager().
                getResourceCollector(internalOp.getTableId());
            if (rc != null) {
                /*
                 * If there is resource tracking first check to see if the
                 * operation is permitted. checkOperation() will thrown an
                 * exception if a limit has been exceeded.
                 */
                rc.checkOperation(internalOp, request.getPartitionId());
                final Consistency consistency =
                    request.isWrite() ? Consistency.ABSOLUTE :
                    request.getConsistency();
                internalOp.setResourceTracker(rc, consistency);
            } else {
                /* Tracking not enabled for table, or non-table (KV) request. */
                internalOp.setResourceTracker(aggregateTracker,
                                              request.getConsistency());
            }

            exception = null;
            limitNs = System.nanoTime() +
                MILLISECONDS.toNanos(request.getTimeout());

            do {
                final CompletableFuture<Response> future = executeOnce();
                if (future != null) {
                    return future;
                }
            } while (true);
        }

        /**
         * Make a single attempt to execute the request. Returns null if
         * execution should be retried, returns a future that will supply the
         * response or an exception for the execution attempt, or throws an
         * exception if a failure occurred in the main thread.
         */
        CompletableFuture<Response> executeOnce() {
            final TransactionConfig txnConfig =
                setupTxnConfig(request, internalOp, limitNs);
            final PartitionId pid = request.getPartitionId();
            final AtomicBoolean pendingCounterIncr = new AtomicBoolean(false);
            long sleepNs = 0;
            txn = null;
            streamHandle = null;
            repEnv = null;
            QueryRuntimeException queryRuntimeException = null;
            startNs = opTracker.getLatencyTracker().markStart();
            result = null;

            /*
             * The finally should call cleanup unless we have arranged for the
             * callback to do so
             */
            boolean callCleanup = true;
            try {

                checkEnabledRequestType(request);
                final long getEnvTimeoutNs =
                    Math.min(limitNs - startNs, ENV_RESTART_RETRY_NS);
                repEnv = repNode.getEnv(NANOSECONDS.toMillis(getEnvTimeoutNs));
                if (repEnv == null) {

                    if (startNs + getEnvTimeoutNs < limitNs) {

                        /*
                         * It took too long to get the environment, but there
                         * is still time remaining before the request times
                         * out.  Chances are that another RN will be able to
                         * service the request before this environment is
                         * ready, because environment restarts are easily
                         * longer (30 seconds) than typical request timeouts (5
                         * seconds).  Report this RN as unavailable and let the
                         * client try another one.  [#22661]
                         */
                        throw new RNUnavailableException(
                            "Environment for RN: " + repNodeId +
                            " was unavailable after waiting for " +
                            (getEnvTimeoutNs/1000) + "ms");
                    }

                    /*
                     * Request timed out -- sleepBeforeRetry will throw the
                     * right exception.
                     */
                    sleepBeforeRetry(request, null,
                                     ENV_RESTART_RETRY_NS, limitNs);
                    return null;
                }

                /*
                 * Install JE async ack handler if appropriate. Note that we
                 * don't attempt to detect RF=1 here -- needsAsyncAcks handles
                 * that.
                 */
                final AckHandler ackHandler;
                if (request.isWrite() &&
                    (request.getDurability().getReplicaAck() !=
                     ReplicaAckPolicy.NONE) &&
                    !disableAsyncAckHandler) {
                    ackHandler = new AckHandler();
                    txnConfig.setAsyncAckHandler(ackHandler);
                } else {
                    ackHandler = null;
                }

                checkVersionConsistency(request, repEnv);
                try {
                    txn = repEnv.beginTransaction(null, txnConfig);
                } catch (IllegalArgumentException iae) {
                    /*
                     * Note that beginTransaction declares to throw
                     * IllegalArgumentException if an invalid parameter is
                     * specified, for example, version consistency with a
                     * version from a different RepGroup. The version in
                     * consistency is checked ahead but the exception may still
                     * be thrown if there is a state change. Other invalid
                     * parameter should be avoided as well, but catching the
                     * exception just in case, otherwise it may bring down RN.
                     */
                    final String msg = iae.getMessage();
                    rateLimitingLogger.log(msg, Level.WARNING, msg);
                    throw new WrappedClientException(iae);
                }

                final MigrationService service =
                    repNode.getMigrationManager().getMigrationService();
                if (!request.isWrite()) {
                    /* no migration handler for reads */
                    streamHandle = MigrationStreamHandle.NOOP_HANDLER;
                } else {
                    assert MigrationStreamHandle.checkForStaleHandle();
                    if (service == null) {
                        /*
                         * no migration service, use noop handler and no
                         * change to the counter
                         */
                        streamHandle = MigrationStreamHandle.NOOP_HANDLER;
                    } else {
                        /*
                         * for write and migration service available, initialize
                         * the handler, increment counter if necessary
                         */
                        streamHandle = service.getMigrationHandle(
                            pid, txn, pendingCounterIncr);
                    }
                    /* set thread-local migration handler */
                    MigrationStreamHandle.setHandle(streamHandle);
                }
                try {
                    result = operationHandler.execute(
                        internalOp, txn, request.getPartitionId());
                } catch (QueryRuntimeException qre) {
                    /*
                     * Throw the cause, which is always a non-null
                     * RuntimeException, but keep track of the original
                     * QueryRuntimeException since we will rethrow that if the
                     * cause is not handled.
                     */
                    queryRuntimeException = qre;
                    throw (RuntimeException) qre.getCause();
                }

                /*
                 * If this is a table operation return the sequence number of
                 * the table in the response.
                 */
                if (internalOp.isTableOp()) {
                    int seqNum = 0;
                    /*
                     * If there is no table ID return the overall metadata seq#
                     */
                    if (internalOp.getTableId() == 0) {
                        seqNum = repNode.getMetadataSeqNum(MetadataType.TABLE);
                    } else {
                        final TableImpl table =
                                repNode.getTable(internalOp.getTableId());
                        if (table != null) {
                            seqNum = table.getSequenceNumber();
                        }
                    }
                    result.setMetadataSeqNum(seqNum);
                }

                boolean needsAsyncAcks = false;
                if (txn.isValid()) {
                    streamHandle.prepare();
                    /* If testing SR21210, throw InsufficientAcksException. */
                    assert TestHookExecute.doHookIfSet
                        (preCommitTestHook,
                         RepInternal.getRepImpl(repEnv));
                    /*
                     * Grab the internal txn before the commit since the commit
                     * clears that field but we need to check for async acks
                     * after the commit.
                     */
                    final Txn internalTxn = DbInternal.getTxn(txn);
                    txn.commit();
                    needsAsyncAcks = needsAsyncAcks(internalTxn);
                } else {
                    /*
                     * The transaction could have been invalidated in the
                     * an unsuccessful Execute.execute operation, or
                     * asynchronously due to a master->replica transition.
                     *
                     * Note that single operation (non Execute requests)
                     * never invalidate transactions explicitly, so they are
                     * always forwarded.
                     */
                    if (!internalOp.getOpCode().equals(OpCode.EXECUTE) ||
                        result.getSuccess()) {
                        /* Async invalidation independent of the request. */
                        throw new ForwardException();
                    }
                    /*
                     * An execute operation failure, allow a response
                     * generation which contains the reason for the failure.
                     */
                }

                /*
                 * Return a future that waits for the JE async ack handler, if
                 * appropriate
                 */
                if ((ackHandler != null) && needsAsyncAcks) {

                    /* Ack handler will do cleanup */
                    callCleanup = false;
                    return ackHandler.handle(
                        withContext(
                            checked((value, ex) -> {
                                    try {
                                        if (ex != null) {
                                            throw ex;
                                        }
                                        return getResponse();
                                    } finally {
                                        cleanup(pendingCounterIncr.get());
                                    }
                                })));
                }
                assert TestHookExecute.doHookIfSet(preResponseTestHook, result);
                postTimeConsistencyCheck(request);
                return completedFuture(getResponse());
            } catch (InsufficientAcksException iae) {
                /* Propagate RequestTimeoutException back to the client */
                throw new RequestTimeoutException
                    (request.getTimeout(),
                     "Timed out due to InsufficientAcksException", iae, true);
            } catch (ReplicaConsistencyException rce) {
                /* Propagate it back to the client */
                throw new ConsistencyException
                    (rce, ConsistencyTranslator.translate(
                      rce.getConsistencyPolicy(),
                      request.getConsistency()));
            } catch (InsufficientReplicasException ire) {
                /* Propagate it back to the client */
                throw new DurabilityException
                    (ire,
                     DurabilityTranslator.translate(ire.getCommitPolicy()),
                     ire.getRequiredNodeCount(), ire.getAvailableReplicas());
            } catch (DiskLimitException dle) {
                /* Propagate it back to the client */
                throw new FaultException(dle, true);
            } catch (ReplicaWriteException | ReplicaStateException rwe) {
                /* Misdirected message, forward to the master. */
                return completedFuture(
                    forward(request, repNodeId.getGroupId()));
            } catch (UnknownMasterException rwe) {
                /* Misdirected message, forward to the master. */
                return completedFuture(
                    forward(request, repNodeId.getGroupId()));
            } catch (ForwardException rwe) {
                /* Misdirected message, forward to the master. */
                return completedFuture(
                    forward(request, repNodeId.getGroupId()));
            } catch (LockConflictException lockConflict) {
                /*
                 * Retry the transaction until the timeout associated with the
                 * request is exceeded. Note that LockConflictException covers
                 * the HA LockPreemptedException.
                 */
                exception = lockConflict;
                sleepNs = LOCK_CONFLICT_RETRY_NS;
            } catch (RollbackException rre) {
                /* Re-establish handles. */
                repNode.asyncEnvRestart(repEnv, rre);
                sleepNs = ENV_RESTART_RETRY_NS;
            } catch (RollbackProhibitedException pe) {
                /*
                 * Include hashcode of repEnv so that each new environment has
                 * its own rate limiting logger instance.
                 */
                rateLimitingLogger.log("Rollback prohibited" +
                    Objects.hashCode(repEnv), Level.SEVERE, pe,
                    () -> "Rollback prohibited admin intervention required");

                /*
                 * Rollback prohibited, the process fault handler will ensure
                 * that the process exits and that the SNA does no try to
                 * restart.
                 */
                throw pe;
            } catch (IncorrectRoutingException ire) {

                /*
                 * An IncorrectRoutingException can occur at the end of a
                 * partition migration, where the local topology has been
                 * updated with the partition's new location (here) but the
                 * parition DB has not yet been opened (see
                 * RepNode.updateLocalTopology).
                 */
                return completedFuture(handleException(request, ire));
            } catch (DatabasePreemptedException dpe) {

                /*
                 * A DatabasePreemptedException can occur when the partition
                 * DB has been removed due to partition migration activity
                 * during the request.
                 */
                return completedFuture(handleException(request, dpe));
            } catch (SecondaryIntegrityException sie) {
                /*
                 * JE has detected constraint or integrity problem with the
                 * secondary database (index). Throw an exception back to the
                 * client.
                 */
                final String secondaryDBName = sie.getSecondaryDatabaseName();
                rateLimitingLogger.log(secondaryDBName, Level.SEVERE,
                                       sie.getMessage());
                repNode.getTableManager().invalidateSecondary(sie);
                throw new FaultException("Integrity problem " +
                                         "with secondary index: " +
                                         sie.getMessage(),
                                         sie,
                                         true);
            } catch (EnvironmentFailureException efe) {
                /*
                 * All subclasses of EFE that needed explicit handling were
                 * handled above. Throw out, so the process fault handler can
                 * restart the RN.
                 */
                  if (!request.isWrite() || notCommitted(txn)) {
                      throw new EnvironmentFailureRetryException(efe);
                  }
                  throw efe;
            } catch (UnauthorizedException ue) {
                /*
                 * Data access violation in operations, log it and throw out.
                 */
                AccessCheckUtils.logSecurityError(
                    ue, "API request: " + internalOp,
                    rateLimitingLogger);
                throw ue;
            } catch (WrappedClientException wce) {
                /*
                 * These are returned to the client.
                 */
                throw wce;
            } catch (KVSecurityException kvse) {
                /*
                 * These are returned to the client.
                 */
                throw new WrappedClientException(kvse);
            } catch (MetadataNotFoundException mnfe) {
                throw new WrappedClientException(mnfe);
            } catch (TableVersionException tve) {
                /*
                 * Operation caught window during a table metadata update,
                 * throw it out.
                 */
                throw tve;
            } catch (RNUnavailableException rnue) {
                /*
                 * Some initialization has not finished (topology,
                 * table/security metadata, secondary DBs, etc). Propagate
                 * back to the client, which will retry.
                 */
                throw rnue;
            } catch (FaultException fe) {

                /*
                 * All FEs coming from the server should be remote. Since FEs
                 * are usually caused by bad client parameters, invalid
                 * requests, or out-of-sync metadata, we limit the logging to
                 * avoid flooding the log files. The exception message is used
                 * as the key because, in general, the FE message does not vary
                 * given the same cause.
                 */
                if (fe.wasLoggedRemotely()) {
                    final String msg = fe.getMessage();
                    rateLimitingLogger.log(msg, Level.INFO, msg);
                } else {
                    logger.log(Level.SEVERE, "unexpected fault", fe);
                }
                /* Propagate it back to the client. */
                throw fe;
            } catch (RuntimeException re) {
                final Response resp  =
                    handleRuntimeException(repEnv, txn, request, re,
                                           queryRuntimeException);

                if (resp != null) {
                    return completedFuture(resp);
                }
                sleepNs = ENV_RESTART_RETRY_NS;
            } finally {
                if (callCleanup) {
                    cleanup(pendingCounterIncr.get());
                }
            }
            sleepBeforeRetry(request, exception, sleepNs, limitNs);
            return null;
        }

        /** Perform clean up needed at the end of an execution attempt */
        private void cleanup(boolean pendingCounterIncr) {
            if (response == null) {
                TxnUtil.abort(txn);
                /* Clear the failed operation's activity */
                opTracker.getLatencyTracker().markFinish(null, 0L);
            }
            if (streamHandle != null) {
                final PartitionId pid = request.getPartitionId();
                streamHandle.done(pid, repNode, txn, pendingCounterIncr);
            }
        }

        /**
         * Create a response for the request and result, and track that the
         * operation is finished.
         */
        private Response getResponse() {
            response = createResponse(repEnv, request, result, startNs);
            opTracker.getLatencyTracker().markFinish(
                internalOp.getOpCode(), startNs, result.getNumRecords());
            return response;
        }

        /**
         * Returns a function that will call the specified function after
         * reestablishing the current dialog and execution contexts.
         */
        private <T, U> BiFunction<T, Throwable, U>
            withContext(BiFunction<T, Throwable, U> function)
        {
            final DialogContext dialogContext =
                AsyncVersionedRemoteDialogResponder.getThreadDialogContext();
            final ExecutionContext executionContext =
                ExecutionContext.getCurrent();
            return (value, ex) -> ExecutionContext.runWithContext(
                (Supplier<U>) () -> withThreadDialogContext(
                    dialogContext, () -> function.apply(value, ex)),
                executionContext);
        }

        /**
         * A future implementing a JE async acknowledgements handler that
         * completes successfully when onQuorumAcks is called and exceptionally
         * when onException is called.
         */
        private class AckHandler extends CompletableFuture<Void>
                implements AsyncAckHandler {

            @Override
            public void onQuorumAcks(MasterTxn txnIgnore) {
                complete(null);
            }

            @Override
            public void onException(MasterTxn txnIgnore, Exception ex) {
                if (ex instanceof InsufficientAcksException) {
                    completeExceptionally(
                        new RequestTimeoutException(
                            request.getTimeout(),
                            "Timed out due to InsufficientAcksException",
                            ex, true));
                } else {
                    /*
                     * Currently only InsufficientAcksException is an expected
                     * exception passed to the AsyncAckHandler. Add additional
                     * handling here if new expected exceptions are introduced.
                     */
                    completeExceptionally(
                        new IllegalStateException(
                            "Unexpected exception passed to AckHandler: " + ex,
                            ex));
                }
            }
        }
    }

    /**
     * Checks if the transaction needs asynchronous acknowledgements. This
     * method should only be called after the transaction has already either
     * committed or aborted.
     */
    private static boolean needsAsyncAcks(Txn txn) {
        return (txn instanceof MasterTxn) &&
            ((MasterTxn) txn).needsAsyncAcks();
    }

    /**
     * The method makes provisions for special handling of RunTimeException.
     * The default action on encountering a RuntimeException is to exit the
     * process and have it restarted by the SNA, since we do not understand the
     * nature of the problem and restarting, while high overhead, is safe.
     * However there are some cases where we would like to avoid RN process
     * restarts and this method makes provisions for such specialized handling.
     *
     * RuntimeExceptions can arise because JE does not make provisions for
     * asynchronous close of the environment. This can result in NPEs or ISEs
     * from JE when trying to perform JE operations, using database handles
     * belonging to the closed environments.
     *
     * In addition, if queryRuntimeException is non-null, then the exception
     * occurred during a query evaluation. In that case, throw the
     * QueryRuntimeException, which will cause the ServiceFaultHandler to
     * propagate the cause back to the client rather than causing the RN to
     * exit.
     *
     * TODO: The scope of this handler is rather large and this check could be
     * narrowed down to enclose only the JE operations.
     *
     * @return a Response, if the request could be handled by forwarding it
     * or null for a retry at this same RN.
     *
     * @throws RNUnavailableException if the request can be safely retried
     * by the requestor at some other RN.
     */
    private Response handleRuntimeException(ReplicatedEnvironment repEnv,
                                            Transaction txn,
                                            Request request,
                                            RuntimeException re,
                                            QueryRuntimeException qre)
        throws RNUnavailableException {

        if ((repEnv == null) || repEnv.isValid()) {

            /*
             * If the environment is OK (or has not been established) and the
             * exception is an IllegalStateException, it may be a case that
             *
             * - the database has been closed due to partition migration.
             *
             * - the transaction commit threw an ISE because the node
             * transitioned from master to replica, and the transaction was
             * asynchronously aborted by the JE RepNode thread.
             *
             * If so, try forward the request.
             */
            if (re instanceof IllegalStateException) {
                final Response resp = forwardIfRequired(request);
                if (resp != null) {
                    rateLimitingLogger.log(
                        "Request forward due to ISE",
                        Level.INFO,
                        re,
                        () ->
                        String.format(
                            "Request %s forwarded due to ISE: %s",
                            request,
                            re.getMessage()));
                    return resp;
                }
                /*
                 * Response could have been processed at this node but wasn't.
                 *
                 * The operation may have used an earlier environment handle,
                 * acquired via the database returned by PartitionManager,
                 * which resulted in the ISE. [#23114]
                 *
                 * Have the caller retry the operation at some other RN if we
                 * can ensure that the environment was not modified by the
                 * transaction.
                 */
                if (notCommitted(txn)) {
                    final String msg =
                        String.format("ISE for request %s, "
                                      + "retry at some different RN.\n%s",
                                      request,
                                      CommonLoggerUtils.getStackTrace(re));

                    rateLimitingLogger.log(
                        "Request needs retry due to ISE",
                        Level.INFO, re, () -> msg);
                    throw new RNUnavailableException(msg);
                }
            }

            if (qre != null) {
                /*
                 * This is to log QueryRuntimeException that wraps the
                 * unexpected failure during query execution with INFO level.
                 *
                 * The QueryRuntimeExeption will not cause the RN exit, it
                 * will be rethrown as its cause if it is a known exception or
                 * FaultException that wraps its cause, so log it as INFO
                 * level.
                 */
                rateLimitingLogger.log(re.getMessage(), Level.INFO, re,
                                       () -> "Query execution failed");
                throw qre;
            }

            /* Unknown failure, propagate the RE */
            logger.log(Level.SEVERE, "unexpected exception", re);
            throw re;
        }
        /*
         * Include hashcode of repEnv so that each new environment has
         * its own rate limiting logger instance.
         */
        rateLimitingLoggerLong.log("Invalid env" + Objects.hashCode(repEnv),
            Level.INFO, re, () -> "Ignoring exception and retrying at this " +
            "RN, environment has been closed or invalidated");

        /* Retry at this RN. */
        return null;
    }

    private boolean notCommitted(Transaction txn) {
        return (txn == null) ||
               ((txn.getState() != Transaction.State.COMMITTED) &&
                (txn.getState() !=
                 Transaction.State.POSSIBLY_COMMITTED));
    }

    /**
     * Implements the execution of lightweight NOPs which is done without
     * the need for a transaction.
     */
    private Response executeNOPInternal(final Request request)
        throws RequestTimeoutException {

        assert TestHookExecute.doHookIfSet(requestNOPExecute, request);

        final long startNs = System.nanoTime();

        final ReplicatedEnvironment repEnv =
            repNode.getEnv(request.getTimeout());

        if (repEnv == null) {
            throw new RequestTimeoutException
                (request.getTimeout(),
                 "Timed out trying to obtain environment handle.",
                 null,
                 true /*isRemote*/);
        }
        final Result result =
            operationHandler.execute(request.getOperation(), null, null);
        return createResponse(repEnv, request, result, startNs);
    }

    /**
     * Handles a request that has been incorrectly directed at this node or
     * there is some internal inconsistency. If this node's topology has more
     * up-to-date information, forward it, otherwise throws a
     * RNUnavailableException which will cause the client to retry.
     *
     * The specified RuntimeException must an instance of either a
     * DatabasePreemptedException or an IncorrectRoutingException.
     */
    private Response handleException(Request request, RuntimeException re) {
        assert (re instanceof DatabasePreemptedException) ||
               (re instanceof IncorrectRoutingException);

        /* If this is a non-partition request, then rethrow */
        /* TODO - Need to check on how these exceptions are handled for group
         * directed dispatching.
         */
        if (request.getPartitionId().isNull()) {
            throw re;
        }
        final Topology topology = topoManager.getLocalTopology();
        final Partition partition = topology.get(request.getPartitionId());

        /*
         * If the local topology is newer and the partition has moved, forward
         * the request.
         *
         * We are comparing the official topology of the client to the local
         * one of either an official topology or a localized one. In either
         * case, The local topology is newer if the sequence number is larger.
         * In the case when the sequence number is equal, then the local
         * topology is newer if it is localized (with newer localization
         * information).
         */
        final int topoSeqNum = topology.getSequenceNumber();
        final int reqSeqNum = request.getTopoSeqNumber();
        final boolean topoLocalized =
            (topology.getLocalizationNumber() != Topology.NULL_LOCALIZATION);
        if (((topoSeqNum > reqSeqNum)
             || ((topoSeqNum == reqSeqNum) && topoLocalized)) &&
            !partition.getRepGroupId().sameGroup(repNode.getRepNodeId())) {
            request.clearForwardingRNs();
            return forward(request, partition.getRepGroupId().getGroupId());
        }
        throw new RNUnavailableException
                             ("Partition database is missing for partition: " +
                              partition.getResourceId());
    }

    /**
     * Forward the request, if the RG does not own the key, if the request
     * needs a master and this node is not currently the master, or if
     * the request can only be serviced by a node that is neither the
     * master nor detached and this node is currently either the master
     * or detached.
     *
     * @param request the request that may need to be forwarded
     *
     * @return the response if the request was processed after forwarding it,
     * or null if the request can be processed at this node.
     *
     * @throws KVStoreException
     */
    private Response forwardIfRequired(Request request) {

        /*
         * If the request has a group ID, use that, otherwise, use the partition
         * iID. If that is null, the request is simply directed to this node,
         * e.g. a NOP request.
         */
        RepGroupId repGroupId = request.getRepGroupId();

        if (repGroupId.isNull()) {
            final PartitionId partitionId = request.getPartitionId();
            repGroupId = partitionId.isNull() ?
                      new RepGroupId(repNodeId.getGroupId()) :
                      topoManager.getLocalTopology().getRepGroupId(partitionId);
        }
        if (repGroupId == null) {
            throw new RNUnavailableException("RepNode not yet initialized");
        }

        if (repGroupId.getGroupId() != repNodeId.getGroupId()) {
            /* Forward the request to the appropriate RG */
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("RN does not contain group: " +
                            repGroupId + ", forwarding request.");
            }
            request.clearForwardingRNs();
            return forward(request, repGroupId.getGroupId());
        }

        final RepGroupState rgState = stateTable.getGroupState(repGroupId);
        final RepNodeState master = rgState.getMaster();

        if (request.needsMaster()) {
            /* Check whether this node is the master. */
            if ((master != null) &&
                 repNodeId.equals(master.getRepNodeId())) {
                /* All's well, it can be processed here. */
                return null;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("RN is not master, forwarding request. " +
                            "Last known master is: " +
                            ((master != null) ?
                             master.getRepNodeId() :
                             "unknown"));
            }
            return forward(request, repNodeId.getGroupId());

        } else if (request.needsReplica()) {

            ReplicatedEnvironment.State rnState =
                rgState.get(repNodeId).getRepState();

            /* If the RepNode is the MASTER or DETACHED, forward the request;
             * otherwise, service the request.
             */
            if ((master != null) && repNodeId.equals(master.getRepNodeId())) {
                rnState = ReplicatedEnvironment.State.MASTER;
            } else if (rnState.isReplica() || rnState.isUnknown()) {
                return null;
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("With requested consistency policy, RepNode " +
                            "cannot be MASTER or DETACHED, but RepNode [" +
                            repNodeId + "] is " + rnState + ". Forward the " +
                            "request.");
            }
            return forward(request, repNodeId.getGroupId());
        }

        if (!request.isWrite() &&
            (request.getConsistency() instanceof Consistency.Time)) {
             //(request.getConsistency() instanceof Consistency.NoneRequired))) {

            final PartitionId pid = request.getPartitionId();
            if (pid == null || pid.isNull()) {
                /*
                 * Partition id is not available, this was a shard based
                 * requests, such as index scan, their check will be done
                 * in other place.
                 */
                return null;
            }

            /*
             * During the partition migration, a time-based consistency check
             * could be incorrect, because replicas in new shard may consider
             * they have up-to-date data and can meet the permissible time lag,
             * even though they haven't received the migrated data yet.
             *
             * Check partition generation table here to ensure the partition
             * generation is opened, which means that it has all of the
             * migrated data, otherwise forward the request.
             * [KVSTORE-981]
             */
            final PartitionGenerationTable pgt = repNode.getPartGenTable();
            if (pgt == null) {
                /*
                 * The node is waiting for the in-memory partition generation
                 * table to be initialized from the generation database, which
                 * is done at the first time of RepNode attempts to open
                 * database handles, see RepNode.updateDbHandles().
                 */
                throw new RNUnavailableException(
                    "Partition information is unavailable for " +
                    "time consistency requests");
            }

            /*
             * Partition generation table is initialized at the beginning of the
             * first partition migration starts, it always happens before open
             * of any partition database to be migrated. The initialization of
             * generation table writes new records to the generation database.
             * When generation table is not initialized, not ready, getOpenGen
             * will throw IllegalStateException, meaning there isn't migration
             * happens before, time consistency reads are allowed without
             * checking if generation is open. There might be a case where
             * partition migration has started and partition generation table is
             * initialized on master, but the replica hasn't received the change
             * made by generation table initialization from replication stream,
             * so the replica thinks the generation table is not initialized at
             * this point and allows the read request to proceed. However, it's
             * possible that after this check, the replica receives the change
             * made by generation table initialization and the change to open
             * the migrating partition database, and the replica opens the
             * database after topology update, but this replica hasn't received
             * all data from this migrating partition database yet. In this
             * case, this read shouldn't be allowed to proceed on this replica
             * but forward to the master. Check if the partition database is
             * open first here can ensure the replica won't allow the read to
             * proceed in this case, which is based on the fact described before
             * that the change made by initialization of generation table is
             * before open of any partition database to be migrated.
             */
            try {
                repNode.getPartitionDB(pid);
            } catch (IncorrectRoutingException ire) {
                return handleException(request, ire);
            }

            PartitionGeneration pg = null;
            try {
                pg = pgt.getOpenGen(pid);
            } catch (IllegalStateException ise) {
                /*
                 * getOpenGen throws ISE when PGT is not initialized, not ready,
                 * no migration happens before, return without forward
                 */
                logger.fine(() -> "Partition generation table is not ready " +
                    "to get open generation in time consistency pre-check");
                return null;
            }
            if (pg == null) {
                return forward(request, repNodeId.getGroupId());
            }

            /* Set the current generation number to request for post-scheck */
            request.setPartitionGenNum(pg.getGenNum());
        }
        return null;
    }

    /**
     * Returns topology information to be returned as part of the response. If
     * the Topology numbers match up there's nothing to be done.
     *
     * If the handler has a new Topology return the changes needed to bring the
     * requester up to date wrt the handler.
     *
     * If the handler has a Topology that's obsolete vis a vis the requester,
     * return the topology sequence number, so that the requester can push the
     * changes to it at some future time.
     *
     * @param reqTopoSeqNum the topology associated with the request
     *
     * @return null if the topologies match, or the information needed to
     * update either end.
     */
    private TopologyInfo getTopologyInfo(int reqTopoSeqNum) {

        final Topology topology = topoManager.getTopology();

        if (topology == null) {

            /*
             * Indicate that this node does not have topology so that the
             * request invoker can push Topology to it.
             */
            return TopologyInfo.EMPTY_TOPO_INFO;
        }

        final int topoSeqNum = topology.getSequenceNumber();

        if (topoSeqNum == reqTopoSeqNum) {
            /* Short circuit. */
            return null;
        }

        /* Topology mismatch, send changes, if this node has them. */
        final TopologyInfo topoInfo = topology.getChangeInfo(reqTopoSeqNum + 1);

        /*
         * For secure store, there is a window between topology and its
         * signature updates. Don't send changes if signature is not updated.
         */
        if (ExecutionContext.getCurrent() != null &&
            (topoInfo.getTopoSignature() == null ||
             topoInfo.getTopoSignature().length == 0)) {
            return null;
        }
        return topoInfo;
    }

    /**
     * Packages up the result of the operation into a Response, including all
     * additional status information about topology changes, etc.
     *
     * @param result the result of the operation
     *
     * @return the response
     */
    private Response createResponse(ReplicatedEnvironment repEnv,
                                    Request request,
                                    Result result,
                                    long startNs) {

        final StatusChanges statusChanges = getStatusChanges(
            request.getInitialDispatcherId(), startNs);

        long currentVLSN = NULL_VLSN;
        if (repEnv.isValid()) {
            final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
            if (repImpl != null) {
                currentVLSN = repImpl.getVLSNIndex().getRange().getLast();
            }
        }

        return new Response(repNodeId,
                            currentVLSN,
                            result,
                            getTopologyInfo(request.getTopoSeqNumber()),
                            statusChanges,
                            request.getSerialVersion());
    }

    /**
     * Sleep before retrying the operation locally.
     *
     * @param request the request to be retried
     * @param exception the exception from the last preceding retry
     * @param sleepNs the amount of time to sleep before the retry
     * @param limitNs the limiting time beyond which the operation is timed out
     * @throws RequestTimeoutException if the operation is timed out
     */
    private void sleepBeforeRetry(Request request,
                                  OperationFailureException exception,
                                  long sleepNs,
                                  final long limitNs)
        throws RequestTimeoutException {

        if ((System.nanoTime() + sleepNs) >  limitNs) {

            final String message =  "Request handler: " + repNodeId +
             " Request: " + request.getOperation() + " current timeout: " +
              request.getTimeout() + " ms exceeded." +
              ((exception != null) ?
               (" Last retried exception: " + exception.getClass().getName() +
                " Message: " + exception.getMessage()) :
               "");

            throw new RequestTimeoutException
                (request.getTimeout(), message, exception, true /*isRemote*/);
        }

        if (sleepNs == 0) {
            return;
        }

        try {
            Thread.sleep(NANOSECONDS.toMillis(sleepNs));
        } catch (InterruptedException ie) {
            throw new IllegalStateException("unexpected interrupt", ie);
        }
    }

    /**
     * Forwards the request, modifying request and response attributes.
     * <p>
     * The request is modified to denote the topology sequence number at this
     * node. Note that the dispatcher id is retained and continues to be that
     * of the node originating the request.
     *
     * @param request the request to be forwarded
     *
     * @param repGroupId the group to which the request is being forwarded
     *
     * @return the modified response
     *
     * @throws KVStoreException
     */
    private Response forward(Request request, int repGroupId) {

        Set<RepNodeId> excludeRN = null;
        if (repNodeId.getGroupId() == repGroupId) {
            /* forwarding within the group */
            excludeRN = request.getForwardingRNs(repGroupId);
            excludeRN.add(repNodeId);
        }
        final short requestSerialVersion = request.getSerialVersion();
        final int topoSeqNumber = request.getTopoSeqNumber();
        request.setTopoSeqNumber
            (topoManager.getTopology().getSequenceNumber());

        final Response response;
        try {
            response = requestDispatcher.execute(request, excludeRN,
                                                 (LoginManager) null);
        } catch (RequestLimitException rle) {
            /*
             * A RequestLimitException when doing forwarding suggests a problem
             * with resources on the RN -- log a rate-limited warning for each
             * group
             */
            final String summary = "Request limit exception" +
                " when forwarding for group " + repGroupId;
            rateLimitingLogger.log(summary, Level.WARNING, rle,
                                   () -> summary + ": " + rle.getMessage());
            throw rle;
        }
        return updateForwardedResponse(requestSerialVersion, topoSeqNumber,
                                       response);

    }

    /**
     * Updates the response from a forwarded request with the changes needed
     * by the initiator of the request.
     *
     * @param requestSerialVersion the serial version of the original request.
     * @param reqTopoSeqNum of the topo seq number associated with the request
     * @param response the response to be updated
     *
     * @return the updated response
     */
    private Response updateForwardedResponse(short requestSerialVersion,
                                             int reqTopoSeqNum,
                                             Response response) {

        /*
         * Before returning the response to the client we must set its serial
         * version to match the version of the client's request, so that the
         * response is serialized with the version requested by the client.
         * This version may be different than the version used for forwarding.
         */
        response.setSerialVersion(requestSerialVersion);

        /*
         * Potential future optimization, use the request handler id to avoid
         * returning the changes multiple times.
         */
        response.setTopoInfo(getTopologyInfo(reqTopoSeqNum));
        return response;
    }

    /**
     * Create a transaction configuration. Note that the transaction is marked
     * as being readOnly if the request is readOnly and no writes are allowed
     * within the transaction.
     *
     * The transaction may be for a DML update statement, which is both a read
     * and a write operation. Such a request cannot be a read request and will
     * therefore be routed to the master, where any reads that are done in the
     * transaction will be done as usual with absolute consistency.
     *
     * The limitNs parameter specifies the end time for the transaction and
     * consistency timeouts.
     */
    private TransactionConfig setupTxnConfig(Request request,
                                             InternalOperation internalOp,
                                             long limitNs) {
        final TransactionConfig txnConfig = new TransactionConfig();

        final long timeoutMs = Math.max(
            0, NANOSECONDS.toMillis(limitNs - System.nanoTime()));
        txnConfig.setTxnTimeout(timeoutMs, MILLISECONDS);

        txnConfig.setReadCommitted(operationHandler
            .getHandler(internalOp.getOpCode())
            .getReadCommitted());

        if (request.isWrite()) {
            final com.sleepycat.je.Durability haDurability =
                DurabilityTranslator.translate(request.getDurability());
            txnConfig.setDurability(haDurability);

            final ReplicaConsistencyPolicy haConsistency =
                NoConsistencyRequiredPolicy.NO_CONSISTENCY;

            return txnConfig.setConsistencyPolicy(haConsistency);
        }

        /* A read request, configure to use a readonly transaction. */
        txnConfig.setReadOnly(true);

        final Consistency reqConsistency = request.getConsistency();
        final ReplicaConsistencyPolicy haConsistency =
            ConsistencyTranslator.translate(disableAuthMaster,
                                            reqConsistency, timeoutMs);
        return txnConfig.setConsistencyPolicy(haConsistency);
    }

    /**
     * If request is a read and has version consistency specified, check
     * whether the requested version in consistency is from the same
     * shard as current node to catch the invalid version before passing
     * it down to JE.
     *
     * TODO: handle the case where there is a partition migration
     */
    private void checkVersionConsistency(Request request,
                                         ReplicatedEnvironment repEnv) {
        if (request.isWrite() ||
            !(request.getConsistency() instanceof Consistency.Version)) {
            return;
        }

        final Version requestedVersion = ((Consistency.Version)
            request.getConsistency()).getVersion();
        final State state = repEnv.getState();

        if ((State.DETACHED.equals(state) || State.MASTER.equals(state))) {
            /*
             * No need to check if current node is MASTER or DETACHED,
             * see com.sleepycat.je.rep.txn.ReadOnlyTxn.checkConsistency
             */
            return;
        }
        if (requestedVersion == null ||
            requestedVersion.getRepGroupUUID() == null) {
            throw new WrappedClientException(
                new IllegalArgumentException(
                    "Invalid version " + requestedVersion +
                    " specified in consistency "));
        }
        if (!requestedVersion.getRepGroupUUID().equals(
             operationHandler.getRepNodeUUID())) {

            throw new WrappedClientException(
                new IllegalArgumentException(
                    "Unable to ensure consistency on " +
                    repNode.getRepNodeId() +
                    ", the version specified in consistency wasn't " +
                    "returned by this shard " +
                    repNode.getRepNodeId().getGroupName()));
        }
    }

    /**
     * If request is a read and has time consistency specified, check whether
     * the generation of requesting partition has been changed over the actual
     * request execution using the generation number associated with the request
     * added by pre-check. If generation has changed, throw a ForwardException.
     */
    private void postTimeConsistencyCheck(Request request)
        throws ForwardException {

        if (request.isWrite() ||
            !(request.getConsistency() instanceof Consistency.Time)) {
            return;
        }
        final PartitionId pid = request.getPartitionId();
        if (pid == null || pid.isNull()) {
            return;
        }
        final PartitionGenerationTable pgt = repNode.getPartGenTable();
        if (pgt == null) {
            throw new RNUnavailableException(
                "Partition information is unavailable for " +
                "time consistency requests");
        }

        final PartitionGenNum reqGenNum = request.getPartitionGenNum();
        if (!pgt.isReady()) {
            if (reqGenNum != null) {
                final String msg = "Partition generation table is no longer " +
                                   "ready after time consistency pre-check";
                rateLimitingLogger.log(msg, Level.WARNING, msg);
                throw new ForwardException();
            }

            /*
             * Partition generation table still hasn't initialized, no need
             * to check further, no partition migration yet.
             */
            return;
        }

        PartitionGeneration pg = null;
        try {
            pg = pgt.getOpenGen(pid);
        } catch (IllegalStateException ise) {
            /*
             * getOpenGen throws ISE when PGT is not ready, seems unlikely to
             * happen, catch and forward the request just in case.
             */
            rateLimitingLogger.log("PGT no longer ready in post-check",
                Level.WARNING, ise, () -> ise.getMessage());
            throw new ForwardException();
        }

        if (pg == null) {
            logger.fine(() -> "Generation " + reqGenNum + " of partition " +
                              pid + " has closed after time consistency " +
                              "pre-check");
            throw new ForwardException();
        }

        final PartitionGenNum currentGenNum = pg.getGenNum();
        if (reqGenNum == null &&
            currentGenNum.equals(PartitionGenNum.generationZero())) {
            /*
             * Partition generation table wasn't initialized at the point of
             * time consistency pre-check, but initialized now after the request
             * execution. The generation zero indicates this partition is
             * originally owned by this shard, shouldn't have migrated data,
             * no need to forward to master.
             */
            return;
        }

        if (!currentGenNum.equals(reqGenNum)) {
            logger.fine(() -> "Generation " + reqGenNum + " of partition " +
                              pid + " has changed to " + currentGenNum +
                              " after time consistency pre-check");
            throw new ForwardException();
        }
    }

    /**
     * Bind the request handler in the registry so that it can start servicing
     * requests.
     */
    public void startup()
        throws RemoteException {

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final GlobalParams globalParams = params.getGlobalParams();
        final RepNodeParams repNodeParams = params.getRepNodeParams();

        if (AsyncRegistryUtils.getEndpointGroupOrNull() == null) {
            asyncThreadPool = null;
        } else {
            final int maxThreads = repNodeParams.getAsyncExecMaxThreads();
            asyncThreadPool =
                new ReusingThreadPoolExecutor(
                    0, maxThreads,
                    repNodeParams.getAsyncExecThreadKeepAliveMs(),
                    MILLISECONDS, getAsyncExecQueueSize(repNodeParams),
                    new KVThreadFactory("RequestHandlerImpl(Async)", logger) {
                        @Override
                        public RequestHandlerThread
                            newThreadInternal(Runnable r, String name)
                        {
                            return new RequestHandlerThread(r, name);
                        }
                    });
        }

        final String kvStoreName = globalParams.getKVStoreName();
        final String csfName = ClientSocketFactory.
                factoryName(kvStoreName,
                            RepNodeId.getPrefix(),
                            InterfaceType.MAIN.interfaceName());
        RMISocketPolicy rmiPolicy =
            params.getSecurityParams().getRMISocketPolicy();
        SocketFactoryPair sfp =
            repNodeParams.getRHSFP(rmiPolicy,
                                   snParams.getServicePortRange(),
                                   csfName,
                                   kvStoreName);
        String serviceName = RegistryUtils.bindingName(
            kvStoreName, repNode.getRepNodeId().getFullName(),
            RegistryUtils.InterfaceType.MAIN);

        if (sfp.getServerFactory() != null) {
            sfp.getServerFactory().setConnectionLogger(logger);
        }

        asyncServerHandle =
            RegistryUtils.rebind(snParams.getHostname(),
                                 snParams.getRegistryPort(),
                                 serviceName,
                                 repNodeId,
                                 this,
                                 sfp.getClientFactory(),
                                 sfp.getServerFactory(),
                                 REQUEST_HANDLER_TYPE_FAMILY,
                                 new RequestHandlerDialogHandlerFactory(),
                                 logger);
        scheduleReqMapCleanup();
    }

    private class RequestHandlerDialogHandlerFactory
            implements DialogHandlerFactory {
        @Override
        public DialogHandler create() {
            return new AsyncRequestHandlerResponder(
                RequestHandlerImpl.this, logger);
        }
    }

    /**
     * Unbind registry entry so that no new requests are accepted. The method
     * waits for requests to quiesce so as to minimize any exceptions on the
     * client side.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {

        final StorageNodeParams snParams = params.getStorageNodeParams();
        final GlobalParams globalParams = params.getGlobalParams();
        final String serviceName = RegistryUtils.bindingName(
            globalParams.getKVStoreName(),
            repNode.getRepNodeId().getFullName(),
            RegistryUtils.InterfaceType.MAIN);

        /* Stop accepting new requests. */
        try {
            RegistryUtils.unbind(snParams.getHostname(),
                                 snParams.getRegistryPort(),
                                 serviceName,
                                 this,
                                 asyncServerHandle,
                                 logger);
        } catch (RemoteException e) {
            /* Ignore */
        }

        /*
         * Wait for the requests to quiesce within the requestQuiesceMs
         * period.
         */
        activeRequests.awaitZero(REQUEST_QUIESCE_POLL_MS, requestQuiesceMs);

        /* Log requests that haven't quiesced. */
        final int activeRequestCount = activeRequests.get();
        if (activeRequestCount > 0) {
            logger.info("Requested quiesce period: " + requestQuiesceMs +
                        "ms was insufficient to quiesce all active " +
                        "requests for soft shutdown. " +
                        "Pending active requests: " + activeRequestCount);
        }

        requestDispatcher.shutdown(null);

        final ReusingThreadPoolExecutor threadPool = asyncThreadPool;
        if (threadPool != null) {
            threadPool.shutdown();
            asyncThreadPool = null;
        }

        if (reqMapCleanupTimer != null) {
            reqMapCleanupTimer.cancel();
        }
    }

    /**
     * Waits for table operations to complete. Operations which are using
     * table metadata with the sequence number equal to seqNum are excluded.
     * This method will timeout if operations do not complete.
     */
    public int awaitTableOps(int seqNum) {
        int totalOutstanding = 0;
        final int skip = seqNum & INDEX_MASK;
        for (int i = 0; i < N_COUNTERS; i++) {
            if (i != skip) {
                final int count = tableOpCounters[i].get();
                if (count > 0) {
                    totalOutstanding += count;
                    tableOpCounters[i].awaitZero(100, 500);
                }
            }
        }
        return totalOutstanding;
    }

    /**
     * Returns the status changes that the requester identified by
     * <code>remoteRequestHandlerId</code> is not aware of.
     *
     * @param resourceId the id of the remote requester
     *
     * @param startNs request start time
     *
     * @return the StatusChanges if there are any to be sent back
     */
    public StatusChanges getStatusChanges(ResourceId resourceId,
                                          long startNs) {

        if (stateChangeEvent == null) {

            /*
             * Nothing of interest to communicate back. This is unexpected, we
             * should not be processing request before the node is initialized.
             */
            return null;
        }
        final RequesterMapValue mapValue = requesterMap.get(resourceId);
        if (mapValue != null &&
            mapValue.getStateChangeEvent() == stateChangeEvent) {

            /*
             * The current SCE is already known to the requester, ignore it and
             * reset startTime to extend the entry lifetime.
             */
            mapValue.setStartTime(startNs);
            return null;
        }

        try {
            final State state = stateChangeEvent.getState();

            if (state.isMaster() || state.isReplica()) {
                final String masterName = stateChangeEvent.getMasterNodeName();
                return new StatusChanges(state,
                                         RepNodeId.parse(masterName),
                                         stateChangeEvent.getEventTime());
            }

            /* Not a master or a replica. */
            return new StatusChanges(state, null, 0);
        } finally {
            requesterMap.put(resourceId,
                new RequesterMapValue(stateChangeEvent, startNs));
        }
    }

    /*
     * For testing purpose
     */
    public void setReqMapEntryLifetimeNs(long reqMapEntryLifetimeNs) {
        this.reqMapEntryLifetimeNs = reqMapEntryLifetimeNs;
    }

    /*
     * For testing purpose
     */
    public void setReqMapCleanupPeriodMs(long reqMapCleanupPeriodMs) {
        this.reqMapCleanupPeriodMs = reqMapCleanupPeriodMs;
    }

    /**
     * Enable given request type served on this node. The allowed types are
     * all, none, readonly.
     *
     * @param requestType request type is being enabled
     */

    public void enableRequestType(RequestType requestType) {
        enabledRequestsType = requestType;
        logger.info("Request type " + enabledRequestsType + " is enabled");
    }

    /**
     * Check if type of client request is enabled.
     *
     * @throws RNUnavailableException if type of given request is not enabled
     */
    private void checkEnabledRequestType(Request request) {
        if (enabledRequestsType == RequestType.ALL) {
            return;
        }

        if (!request.getInitialDispatcherId().getType().isClient()) {
            return;
        }

        if (enabledRequestsType == RequestType.READONLY) {
            /*
             * If permits read request, only throw exception for write request.
             */
            if (request.isWrite()) {
                throw new RNUnavailableException(
                    "RN: " + repNodeId + " was unavailable because " +
                    "write requests are disabled");
            }
            return;
        }

        throw new RNUnavailableException(
            "RN: " + repNodeId + " was unavailable because " +
            "all requests have been disabled");
    }

    /**
     * For testing only.
     */
    RateLimitingLogger<String> getRateLimitingLoggerLong() {
        return rateLimitingLoggerLong;
    }

    /** For testing */
    Logger getLogger() {
        return logger;
    }

    /**
     * Listener for local state changes at this node.
     */
    private class Listener implements StateChangeListener {

        final ReplicatedEnvironment repEnv;

        public Listener(ReplicatedEnvironment repEnv) {
            this.repEnv = repEnv;
        }

        /**
         * Takes appropriate action based upon the state change. The actions
         * must be simple and fast since they are performed in JE's thread of
         * control.
         */
        @Override
        public void stateChange(StateChangeEvent sce)
            throws RuntimeException {

            stateChangeEvent = sce;
            final NodeType nodeType = (params != null) ?
                params.getRepNodeParams().getNodeType() : null;
            LoggerUtils.logStateChangeEvent(logger, sce, nodeType);
            /* Ensure that state changes are sent out again to requesters */
            requesterMap.clear();
            stateTable.update(sce);
            if (repNode != null) {
                repNode.noteStateChange(repEnv, sce);
            }
        }
    }

    /**
     * A utility exception used to indicate that the request must be forwarded
     * to some other node because the transaction used to implement the
     * request has been invalidated, typically because of a master-&gt;replica
     * transition.
     */
    @SuppressWarnings("serial")
    private class ForwardException extends Exception {}

    /**
     * Provides an implementation of OperationContext for access checking.
     */
    public class RequestContext implements OperationContext {
        private final Request request;

        private RequestContext(Request request) {
            this.request = request;
        }

        public Request getRequest() {
            return request;
        }

        @Override
        public String describe() {
            return "API request: " + request.getOperation().toString();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            final OpCode opCode = request.getOperation().getOpCode();

            /* NOP does not require authentication */
            if (OpCode.NOP.equals(opCode)) {
                return emptyPrivilegeList;
            }

            return operationHandler.getRequiredPrivileges(
                request.getOperation());
        }
    }

    /**
     * The FIOStatasCollectingThread, allocated by the async thread pool used to process requests; it permits the
     * tracking of file i/o statistics.
     */
    private final class RequestHandlerThread extends Thread
        implements FIOStatsCollectingThread {

        RequestHandlerThread(Runnable r, String name) {
            super(r, name);
        }

        @Override
        public void collect(boolean read, long bytes) {
            /* The environment can change over the lifetime of the RepNode, so
             * get it dynamically.
             */
            final RepImpl env = repNode.getEnvImpl(0 /* Don't wait. */);
            if (env != null) {
                env.getFileManager().getAppStatsCollector().
                    collect(read, bytes);
            }
        }
    }

    /**
     * This class reperesents the value type of requesterMap and stores a
     * StateChangeEvent object and request start time.
     */
    static class RequesterMapValue {

        private final StateChangeEvent sce;
        /*
         * The time at which the request execution started. This is used to
         * decide if the requesterMap entry is old enough to be removed.
         * May be set to extend the expiration time.
         */
        private volatile long startNs;

        public RequesterMapValue(StateChangeEvent sce, long startNs) {
            this.sce = sce;
            this.startNs = startNs;
        }

        public void setStartTime(long startNs) {
            this.startNs = startNs;
        }

        public long getStartNs() {
            return startNs;
        }

        public StateChangeEvent getStateChangeEvent() {
            return sce;
        }
    }

    /**
     * Schedules a requesterMap cleanup task.
     */
    private void scheduleReqMapCleanup() {
        /* Cancel any outstanding timer */
        if (reqMapCleanupTimer != null) {
            reqMapCleanupTimer.cancel();
            reqMapCleanupTimer = null;
        }
        this.reqMapCleanupTimer = new Timer(true /* isDaemon */);
        final TimerTask cleanupTask =
            new TimerTask() {
                @Override
                public void run() {
                    final Iterator<Map.Entry<ResourceId, RequesterMapValue>>
                        iter = requesterMap.entrySet().iterator();
                    while (iter.hasNext()) {
                        final long startTime =
                            iter.next().getValue().getStartNs();
                        if (System.nanoTime() - startTime >
                            reqMapEntryLifetimeNs) {
                            iter.remove();
                        }
                    }
                }
            };

        this.reqMapCleanupTimer.schedule(cleanupTask, 0, reqMapCleanupPeriodMs);

        logger.fine("requesterMap cleanup task scheduled to run with a " +
                    "period of " + (reqMapCleanupPeriodMs / 1000L) +
                    " seconds");
    }
}
