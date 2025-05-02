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

package oracle.kv.impl.api.rgstate;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.Consistency;
import oracle.kv.impl.api.AsyncRequestHandlerAPI;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.api.Response;
import oracle.kv.impl.api.ops.NOP;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.WatcherNames;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.stats.KVStats;
import oracle.kv.stats.NodeMetrics;
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.LongMinMaxElement;
import oracle.nosql.common.sklogger.measure.LongStateProbe;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.utilint.VLSN;

/**
 * The operational state associated with the RN or KV client. This state is
 * dynamic and is used primarily to dispatch a request to a specific RN within
 * a replication group.
 * <p>
 * Note that the methods used to modify state are specified as
 * <code>update</code> rather that <code>set</code> methods. Due to the
 * distributed and concurrent nature of the processing it's entirely possible
 * that state update requests can be received out of order, so each update
 * method must be prepared to ignore obsolete update requests. The return value
 * from the <code>update</code> methods indicates whether an update was
 * actually made, or whether the request was obsolete and therefore ignored.
 */
public class RepNodeState {

    private final RepNodeId rnId;

    private final ResourceId trackerId;

    final Logger logger;

    /** The ID of the zone containing the RN, or null if not known. */
    private volatile DatacenterId znId = null;

    /**
     * The remote request handler for the RN, supporting either the sync or
     * async API.
     */
    private final ReqHandlerRef reqHandlerRef;

    /**
     * A future completed to null indicating no resolution was made. Used to
     * initialize/reset the AsyncReqHandlerRef.
     */
    private static final CompletableFuture<AsyncRequestHandlerAPI>
        NULL_REQUEST_HANDLE_FUTURE =
        CompletableFuture.completedFuture(null);

    /**
     * The last known haState associated with the RN. We simply want the latest
     * state so access to it is unsynchronized.
     */
    private volatile State repState = null;

    /**
     * The VLSN state associated with this RN
     */
    private final VLSNState vlsnState;

    private int topoSeqNum = Metadata.UNKNOWN_SEQ_NUM;

    /**
     * The size of the sample used for the trailing average. The sample needs
     * to be large enough so that it represents the different types of
     * operations, but not too large so that it can't react rapidly enough to
     * changes in the response times.
     */
    public static final int SAMPLE_SIZE = 8;

    /**
     * Accumulates the response time from read requests, so they can be load
     * balanced.
     */
    private final ResponseTimeAccumulator readAccumulator;

    /**
     * The response times across both read and write operations. Note that the
     * LatencyElement object observe values in nano-seconds.
     */
    private final LatencyElement responseTimeElement =
        new LatencyElement();

    /*
     * A watcher name used for collecting a resonse time stats that is never
     * cleared
     */
    private static final String NO_CLEAR_WATCHER_NAME =
        WatcherNames.getWatcherName(RepNodeState.class, "noClearWatcherName");


    /**
     * The number of request actively being processed by this node.
     */
    private final LongStateProbe<LongStateProbe.Result>
        activeRequestCountElement =
        new LongStateProbe<>((v) -> new LongStateProbe.Result(v));

    /* The max active request count. Just a rough metric. */
    private final LongMinMaxElement maxActiveRequestCountElement =
        new LongMinMaxElement();

    /**
     * The total requests dispatched to this node.
     */
    private final ThroughputElement requestCountElement =
        new ThroughputElement();

    /**
     * The total number of requests that resulted in exceptions.
     */
    private final ThroughputElement errorCountElement =
        new ThroughputElement();

    /**
     * The approx interval over which the VLSN rate is updated. We may want
     * to have a different rate for a client hosted dispatcher versus a RN
     * hosted dispatcher to minimize the number of open tcp connections. RNS
     * will use these connections only when forwarding requests, which should
     * be a relatively uncommon event.
     */
    public static int RATE_INTERVAL_MS = 10000;

    RepNodeState(RepNodeId rnId,
                 ResourceId trackerId,
                 boolean async,
                 Logger logger) {
        this(rnId, trackerId, async, logger, RATE_INTERVAL_MS);
    }

    RepNodeState(RepNodeId rnId,
                 ResourceId trackerId,
                 boolean async,
                 Logger logger,
                 int rateIntervalMs) {
        this.rnId = rnId;
        this.trackerId = trackerId;
        this.logger = checkNull("logger", logger);
        readAccumulator = new ResponseTimeAccumulator();
        reqHandlerRef =
            async ? new AsyncReqHandlerRef() : new SyncReqHandlerRef();
        vlsnState = new VLSNState(rateIntervalMs);

        /*
         * Start it out as being in the replica state, so an attempt is made to
         * contact it by sending it a request. At this point the state can be
         * adjusted appropriately.
         */
        repState = State.REPLICA;
    }

    /**
     * Returns the unique resourceId associated with the RepNode
     */
    public RepNodeId getRepNodeId() {
        return rnId;
    }

    /**
     * Returns the ID of the zone containing the RepNode, or {@code null} if
     * not known.
     *
     * @return the zone ID or {@code null}
     */
    public DatacenterId getZoneId() {
        return znId;
    }

    /**
     * Sets the ID of the zone containing the RepNode.
     *
     * @param znId the zone ID
     */
    public void setZoneId(final DatacenterId znId) {
        this.znId = znId;
    }

    /**
     * See ReqHandlerRef.getSync
     */
    public RequestHandlerAPI getReqHandlerRef(RegistryUtils registryUtils,
                                              long timeoutMs) {
        return reqHandlerRef.getSync(registryUtils, timeoutMs);
    }

    /**
     * Returns the completable future of resolving the request handler.
     *
     * The future completes to a non-null request handler if resolved
     * sucessfully, the resolving exception if failed with an exception or
     * null. If the future completes to null, it simply means the request
     * handler is not obtained but the exception is not provided an exception:
     * for example, the request handler ref has just been initialized/reset, or
     * the rep node is not in the topology yet or the resolution is timedout.
     */
    public CompletableFuture<AsyncRequestHandlerAPI>
        getReqHandlerRefAsync(RegistryUtils registryUtils, long timeoutMs)
    {
        return reqHandlerRef.getAsync(registryUtils, timeoutMs);
    }

    /**
     * See ReqHandlerRef.needsResolution()
     */
    public boolean reqHandlerNeedsResolution() {
        return reqHandlerRef.needsResolution();
    }

    /**
     * See ReqHandlerRef.needsRepair()
     */
    public boolean reqHandlerNeedsRepair() {
        return reqHandlerRef.needsRepair();
    }

    /**
     * This method returns its results via a future even when the
     * implementation is synchronous. Callers should not assume that results
     * will be returned immediately, either due to blocking or a delay until
     * the future is complete.
     *
     * See ReqHandlerRef.resolve
     */
    public CompletableFuture<Boolean>
        resolveReqHandlerRef(RegistryUtils registryUtils, long timeoutMs)
    {
        return reqHandlerRef.resolve(registryUtils, timeoutMs);
    }

    /**
     * See ReqHandlerRef.reset
     */
    public void resetReqHandlerRef() {
        reqHandlerRef.reset();
    }

    /**
     * See ReqHandlerRef.noteException(Exception)
     */
    public void noteReqHandlerException(Exception e) {
        reqHandlerRef.noteException(e);
    }

    /**
     * Returns the SerialVersion that should be used with the request handler.
     */
    public short getRequestHandlerSerialVersion() {
        return reqHandlerRef.getSerialVersion();
    }

    /**
     * Returns the current known replication state associated with the
     * node.
     */
    public ReplicatedEnvironment.State getRepState() {
        return repState;
    }

    /**
     * Updates the rep state.
     *
     * @param state the new rep state
     */
    void updateRepState(State state) {
        repState = state;
    }

    /**
     * Returns the current VLSN associated with the RN.
     * <p>
     * The VLSN can be used to determine how current a replica is with respect
     * to the Master. It may return a null VLSN if the RN has never been
     * contacted.
     */
    long getVLSN() {
        return vlsnState.getVLSN();
    }

    boolean isObsoleteVLSNState() {
        return vlsnState.isObsolete();
    }

    /**
     * Updates the VLSN associated with the node.
     *
     * @param newVLSN the new VLSN
     */
    void updateVLSN(long newVLSN) {
       vlsnState.updateVLSN(newVLSN);
    }

    /**
     * Returns the current topo seq number associated with the node. A negative
     * return value means that the sequence number is unknown.
     */
    int getTopoSeqNum() {
        return topoSeqNum;
    }

    /**
     * Updates the topo sequence number, moving it forward. If it's less than
     * the current sequence number it's ignored.
     *
     * @param newTopoSeqNum the new sequence number
     */
    synchronized void updateTopoSeqNum(int newTopoSeqNum) {
        if (topoSeqNum < newTopoSeqNum) {
            topoSeqNum = newTopoSeqNum;
        }
    }

    /**
     * Resets the topo sequence number as specified.
     */
    synchronized void resetTopoSeqNum(int endSeqNum) {
        topoSeqNum = endSeqNum;
    }

    /**
     * Increments the error count associated with the RN.
     */
    public void incErrorCount() {
        errorCountElement.observe(1);
    }

    /**
     * Returns the average trailing read response time associated with the RN
     * in milliseconds.
     * <p>
     * It's used primarily for load balancing purposes.
     */
    int getAvReadRespTimeMs() {
        return readAccumulator.getAverage();
    }

    /**
     * Accumulates the average response time by folding in this contribution
     * from a successful request to the accumulated response times. This method
     * is only invoked upon successful completion of a read request.
     * <p>
     * <code>lastAccessTime</code> is also updated each time this method is
     * invoked.
     *
     * @param forWrite determines if the time is being accumulated for a write
     * operation
     * @param responseTimeMs the response time associated with this successful
     * request.
     */
    public void accumRespTime(boolean forWrite, int responseTimeMs) {
        if (!forWrite) {
            readAccumulator.update(responseTimeMs);
        }
        responseTimeElement.observe(responseTimeMs * 1_000_000L);
    }

    /**
     * Returns the number of outstanding remote requests to the RN.
     * <p>
     * It's used to ensure that all the outgoing connections are not used
     * up because the node is slow in responding to requests.
     */
    int getActiveRequestCount() {
        return (int) activeRequestCountElement.obtain().getValue();
    }

    /**
     * Invoked before each remote request is issued to maintain the outstanding
     * request count. It must be paired with a requestEnd in a finally block.
     *
     * @return the number of requests active at this node
     */
    public int requestStart() {
        requestCountElement.observe(1);
        final long count = activeRequestCountElement.incrValue();
        maxActiveRequestCountElement.observe(count);
        return (int) count;
    }

    /**
     * Invoked after each remote request completes. It, along with the
     * requestStart method, is used to maintain the outstanding request count.
     */
    public void requestEnd() {
        final long newValue = activeRequestCountElement.decrValue();
        if (newValue < 0) {
            throw new IllegalStateException(
                "The active request count should not become negative");
        }
    }

    /**
     * Returns the node metrics with respect to a watcher.
     */
    public NodeMetrics getNodeMetrics(Topology topology,
                                      String watcherName,
                                      boolean clear) {
        final RepNodeId repNodeId = getRepNodeId();
        final String datacenterName =
            topology.getDatacenter(repNodeId).getName();
        final boolean isActive = !reqHandlerNeedsResolution();
        final boolean isMaster = getRepState().isMaster();
        final long maxActiveRequestCount =
            maxActiveRequestCountElement.obtain(watcherName, clear).getMax();
        final long requestCount =
            requestCountElement.obtain(watcherName, clear).getCount();
        final long errorCount =
            errorCountElement.obtain(watcherName, clear).getCount();
        final long averageTrailingResponseTimeNanos =
            responseTimeElement.obtain(NO_CLEAR_WATCHER_NAME, false).
            getAverage();
        final long averageResponseTimeNanos =
            responseTimeElement.obtain(watcherName, clear).getAverage();

        return new KVStats.NodeMetricsImpl(
            repNodeId, datacenterName, isActive, isMaster,
            maxActiveRequestCount, requestCount, errorCount,
            averageTrailingResponseTimeNanos, averageResponseTimeNanos);
    }

    /**
     * Returns a descriptive string for the rep node state
     */
    public String printString() {
        final String watcherName = WatcherNames.getWatcherName(
            RepNodeState.class, "printString");
        return String.format(
            "node: %s state: %s errors: %,d av resp time %,d ns " +
            "total requests: %,d",
            getRepNodeId().toString(),
            getRepState().toString(),
            errorCountElement.obtain(watcherName).getCount(),
            responseTimeElement.obtain(NO_CLEAR_WATCHER_NAME, false)
                .getAverage(),
            requestCountElement.obtain(watcherName).getCount());
    }

    @Override
    public String toString() {
        return "RepNodeState[" + getRepNodeId() + ", " + getRepState() + "]";
    }

    /**
     * AttributeValue encapsulates a RN state value along with a sequence
     * number used to compare attribute values for recency.
     *
     * @param <V> the type associate with the attribute value.
     */
    @SuppressWarnings("unused")
    private static class AttributeValue<V> {
        final V value;
        final long sequence;

        private AttributeValue(V value, long sequence) {
            super();
            this.value = value;
            this.sequence = sequence;
        }

        /** Returns the value associated with the attribute. */
        private V getValue() {
            return value;
        }

        /**
         * Returns a sequence that can be used to order two attribute
         * values in time. Given two values v1 and v2, if v2.getSequence()
         * > v1.getSequence() implies that v2 is the more recent value.
         */
        private long getSequence() {
            return sequence;
        }
    }

    /**
     * Encapsulates the computation of average response times.
     */
    private static class ResponseTimeAccumulator {

        /*
         * The samples in ms.
         */
        final short[] samples;
        int sumMs = 0;
        int index = 0;

        private ResponseTimeAccumulator() {
            samples = new short[SAMPLE_SIZE];
            sumMs = 0;
        }

        private synchronized void update(int sampleMs) {
            if (sampleMs > Short.MAX_VALUE) {
                sampleMs = Short.MAX_VALUE;
            }

            index = (++index >= SAMPLE_SIZE) ? 0 : index;
            sumMs += (sampleMs - samples[index]);
            samples[index] = (short)sampleMs;
        }

        /*
         * Unsynchronized to minimize contention, a slightly stale value is
         * good enough.
         */
        private int getAverage() {
            return (sumMs / SAMPLE_SIZE);
        }
    }

    /**
     * Wraps a remote RequestHandlerAPI reference along with its accompanying
     * state. It also encapsulates the operations used to maintain the remote
     * reference. <p>
     *
     * If the reference does not have a cached remote reference, it is
     * designated as "needs resolution". A reference that needs resolution can
     * get and cache a new remote reference (or be "resolved") via a call to
     * getSync() or getAsync(). If request dispatcher's attempt to resolve the
     * reference returns null, then this reference may be resolved again,
     * perhaps immediately, when the request dispatcher makes another attempt
     * to find a handler for the current request. The reference will also be
     * available to be resolved, if needed, for future requests. <p>
     *
     * If an exception occurs either when resolving the reference or when using
     * the reference to make a remote call, then any cached remote reference is
     * cleared (marked "needs resolution"). If the exception is thrown during
     * the request dispatcher's attempt to resolve the reference, then the
     * request handler will be considered again when attempting to find a
     * request handler for the current request only after all other RNs are
     * tried and fail to be usable. The reference is still available to be
     * resolved, if needed, for future requests. <p>
     *
     * When a reference is marked as needing resolution because an attempt to
     * use it or resolve it throws an exception, the exception can also be
     * stored in the exceptionSummary to record that resolving the reference is
     * not expected to work if retried immediately. In this case the reference
     * is designated as "needs repair", which prevents it from being considered
     * again until it is "repaired". For example, a reference is marked as
     * needing repair if the exception thrown suggests that the host's network
     * address is not accessible. <p>
     *
     * The RepNodeStateUpdateThread attempts to resolve all references that
     * need resolution on a periodic basis, whether or not they need repair.
     * This resolution is done out of the request's thread of control to
     * minimize the timeout latencies associated with resolving references. In
     * particular, this is the way requests that need repair are repaired, with
     * the repair always performed in the upgrade thread, not as part of a
     * request.
     */
    private abstract class ReqHandlerRef {

        volatile short serialVersion;

        ReqHandlerRef() {
            /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
            serialVersion = SerialVersion.CURRENT;
        }

        /**
         * Resets the state of the reference back to its initial state.
         */
        abstract void reset();

        /**
         * Returns the serial version number used to serialize requests to this
         * request handler.
         */
        short getSerialVersion() {
            return serialVersion;
        }

        /**
         * Returns true if the ref is not currently resolved. This may be
         * because the reference has an exception associated with it or simply
         * because it was never referenced before.
         */
        abstract boolean needsResolution();

        /**
         * Returns true if the ref has a pending exception associated with it
         * and the reference needs to be resolved before before it can be used.
         */
        abstract boolean needsRepair();

        /**
         * Attempts to resolve the request handler, returning a future that
         * supplies a boolean value to say whether the resolution succeeded,
         * and which throws an exception if there was a specific cause for a
         * failure.
         */
        abstract CompletableFuture<Boolean>
            resolve(RegistryUtils registryUtils, long timeoutMs);

        /**
         * Returns a remote reference to the RN that can be used to make remote
         * method calls. It will try resolve the reference if it has never been
         * resolved before.  This method is synchronous.
         *
         * @param registryUtils is used to resolve "never been resolved before"
         * references
         * @param timeoutMs the amount of time to be spent trying to resolve
         * the handler
         *
         * @return a remote reference to the RN's request handler, or null.
         */
        RequestHandlerAPI getSync(RegistryUtils registryUtils,
                                  long timeoutMs) {
            throw new UnsupportedOperationException(
                "The sync operation is not supported");
        }

        /**
         * Like the previous get overloading, but asynchronous.
         */
        @SuppressWarnings("unused")
        CompletableFuture<AsyncRequestHandlerAPI>
            getAsync(RegistryUtils registryUtils, long timeoutMs)
        {
            return failedFuture(
                new UnsupportedOperationException(
                    "The async operation is not supported"));
        }

        /**
         * Makes note of the exception encountered during the execution of a
         * request.
         *
         * This RN is removed from consideration until the background
         * thread resolves it once again.
         *
         * This method is called by an external caller (e.g., {@link
         * AsyncRequestDispatcherImpl}) which noticed a problem with this
         * handler.
         */
        abstract void noteException(Exception e);
    }

    /**
     * Synchronous implementation based on RMI.
     */
    private class SyncReqHandlerRef extends ReqHandlerRef {

        /* used to coordinate updates to the handler. */
        private final Semaphore semaphore = new Semaphore(1,true);

        /**
         * The remote request handler (an RMI reference) for the RN. It's
         * volatile, since any request thread can invalidate it upon
         * encountering an exception and is read asynchronously by needs
         * resolution.
         */
        private volatile RequestHandlerAPI requestHandler;

        /**
         * It's non-null if there is an error that resulted in the
         * requestHandler iv above being nulled. It's volatile because it's
         * read without acquiring the semaphore by needsRepair() as part of a
         * request dispatch.
         */
        private volatile ExceptionSummary exceptionSummary;

        private SyncReqHandlerRef() {
            requestHandler = null;
            exceptionSummary = null;
        }

        @Override
        void reset() {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new OperationFaultException("Unexpected interrupt", e);
            }
            try {
                requestHandler = null;
                /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
                serialVersion = SerialVersion.CURRENT;
                exceptionSummary = null;
            } finally {
                semaphore.release();
            }
        }

        @Override
        boolean needsResolution() {
            return (requestHandler == null);
        }

        /**
         * Note that, in this implementation, the exception summary is read
         * outside the semaphore and that's ok, we just need an approximate
         * answer.
         */
        @Override
        boolean needsRepair() {
            return (exceptionSummary != null);
        }

        @Override
        CompletableFuture<Boolean> resolve(RegistryUtils registryUtils,
                                           long timeoutMs) {
            try {
                return completedFuture(
                    resolveSync(registryUtils, timeoutMs) != null);
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }

        /**
         * Resolves the remote reference based upon its Topology in
         * RegistryUtils.
         *
         * This method tries to "fail fast" so that it does not run down the
         * request timeout limit, when it's invoked in a request context.
         *
         * It implements this "fail fast" strategy by noting whether it
         * acquired the lock with contention. If it acquired the lock with
         * contention and the state is still unresolved, it means that the
         * immediately preceding attempt to repair it failed and there is no
         * reason to retry resolving it immediately, particularly since
         * resolving it may involved getting stuck in a connect open timeout
         * which would eat into the request timeout limit and prevent retries
         * at other nodes which may be available. The first thread that
         * acquires the lock uncontended will be the one that attempts to
         * resolve the state. If it fails, a retry at a higher level, either by
         * the RepNodeStateUpdateThread, or RequestDispatcherImpl, depending on
         * which thread gets there first, will resolve it.
         *
         * @param registryUtils used to resolve the remote reference
         *
         * @param timeoutMs the amount of time to be spent trying to resolve
         * the handler
         *
         * @return the handle or null if one could not be established due to a
         * timeout, a remote exception, a recently failed attempt by a
         * different thread, etc.
         */
        private RequestHandlerAPI resolveSync(RegistryUtils registryUtils,
                                              long timeoutMs) {

            {   /* Try getting it contention-free first. */
                RequestHandlerAPI refNonVolatile = requestHandler;
                if (refNonVolatile != null) {
                    return refNonVolatile;
                }
            }

            /**
             * First try to acquire an uncontended lock
             */
            boolean waited = false;
            try {
                if (!semaphore.tryAcquire()) {
                    /*
                     * If not, try waiting. The wait could be as long as the SO
                     * connect timeout for a node that was down, if some other
                     * thread was trying to resolve the reference and had the
                     * lock.
                     */
                    if (!semaphore.tryAcquire(timeoutMs,
                                              TimeUnit.MILLISECONDS)) {
                        /* Could not acquire after waiting. */
                        return null;
                    }
                    waited = true;
                }
            } catch (InterruptedException e) {
                throw new OperationFaultException("Unexpected interrupt", e);
            }

            if (waited) {
                /* Acquired semaphore after waiting. */
                boolean needsRepair = true;
                try {
                    needsRepair = needsRepair();
                    if (needsRepair) {
                        /*
                         * Don't bother retrying immediately, since another
                         * thread just failed. Retrying in this case may have
                         * an adverse impact in that it could run a request out
                         * of time, which could otherwise be used to retry a
                         * request at some other node.
                         */
                        return null;
                    }
                } finally {
                    if (needsRepair) {
                        /* Exiting method via return or exception. */
                        semaphore.release();
                    }
                }

                /* Semaphore acquired after waiting. */
            }

            /* Semaphore acquired by this thread. */
           try {
                if (requestHandler != null) {
                    return requestHandler;
                }

                try {
                    /*
                     * registryUtils can be null if the RN has not finished
                     * initialization.
                     */
                    if (registryUtils == null) {
                        return null;
                    }
                    requestHandler = registryUtils.getRequestHandler(rnId);

                    /*
                     * There is a possibility that the requested RN is not yet
                     * in the topology. In this case requestHandler will be
                     * null. This can happen during some elasticity operations.
                     */
                    if (requestHandler == null) {
                        noteExceptionInternal(
                              new IllegalArgumentException(rnId +
                                                           " not in topology"));
                        return null;
                    }
                    serialVersion = requestHandler.getSerialVersion();
                    exceptionSummary = null;
                    return requestHandler;
                } catch (RemoteException e) {
                    noteExceptionInternal(e);
                } catch (NotBoundException e) {
                    /*
                     * The service has not yet appeared in the registry, it may
                     * be down, or is in the process of coming up.
                     */
                    noteExceptionInternal(e);
                } catch (AuthenticationFailureException e) {
                    noteExceptionInternal(e);
                }

                return null;
            } finally {
                semaphore.release();
            }
        }

        @Override
        RequestHandlerAPI getSync(RegistryUtils registryUtils,
                                  long timeoutMs) {
            final RequestHandlerAPI refNonVolatile = requestHandler;

            if (refNonVolatile != null) {
                /* The fast path. NO synchronization. */
                return refNonVolatile;
            }

            /*
             * Needs resolution and does not need repair: resolve it now.
             */
            if (exceptionSummary == null) {
                return resolveSync(registryUtils, timeoutMs);
            }

            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new OperationFaultException("Unexpected interrupt", e);
            }
            try {
                /*
                 * If the exception summary was cleared while we were acquiring
                 * the semaphore, then another thread tried resolving it:
                 * return that it was not found. TODO: Maybe we should
                 * return the reference if there was one?
                 */
                if (exceptionSummary == null) {
                    return null;
                }

                /*
                 * If last resolve failed because of
                 * AuthenticationFailureException, throw the exception here
                 * since it cannot be fixed automatically by the next
                 * resolution.
                 */
                final Exception lastException = exceptionSummary.getException();
                if (lastException instanceof AuthenticationFailureException) {
                    exceptionSummary = null;
                    throw (AuthenticationFailureException) lastException;
                }
            } finally {
                semaphore.release();
            }

            /* Otherwise just return null to say it was not found */
            return null;
        }

        @Override
        void noteException(Exception e) {
            try {
                semaphore.acquire();
            } catch (InterruptedException ie) {
                throw new OperationFaultException("Unexpected interrupt", ie);
            }
            try {
                noteExceptionInternal(e);
            } finally {
                semaphore.release();
            }
        }

        /**
         * Assumes that the caller has already set the semaphore
         */
        private void noteExceptionInternal(Exception e) {
            assert semaphore.availablePermits() == 0;

            requestHandler = null;
            if (exceptionSummary == null) {
                exceptionSummary = new ExceptionSummary();
            }
            exceptionSummary.noteException(e);
        }
    }

    /**
     * Asynchronous implementation based on the dialog layer using
     * AsyncRequestHandlerAPI.
     */
    private class AsyncReqHandlerRef extends ReqHandlerRef {

        /*
         * Access to all fields must be protected by synchronization on this
         * instance.
         */

        /**
         * A future holding the most recently obtained request handler for the
         * RN. The value of this field is always non-null.
         *
         * The future can complete to a non-null request handler, an exception
         * or null. See {@link #getReqHandlerRefAsync}.
         *
         * The variable {@code haveHandler} is set to {@code true} only if the
         * future completes to a non-null request handler.
         *
         * The variable {@code resolving} is set to {@code true} only if the
         * future is not done.
         *
         * The variable {@code exceptionSummary} is set to a non-null exception
         * only if the future completes to {@code null} or an exception.
         */
        private CompletableFuture<AsyncRequestHandlerAPI> future =
            NULL_REQUEST_HANDLE_FUTURE;

        /**
         * Whether the {@code future} completes to a request handler for the RN.
         */
        private boolean haveHandler;

        /**
         * Non-null if resolution failed with an exception because of a problem
         * we do not expect to be fixed immediately.
         *
         * A non-null value in this field tells the needsRepair method that the
         * request handler is not ready and that the problem is not expected to
         * be corrected soon, so we should not try another resolution
         * immediately. The exceptionSummary field is set to true based on the
         * resolution exception type. TODO: This wording and usage of this
         * field is a bit obscure, perhaps should give better names.
         */
        private ExceptionSummary exceptionSummary;

        /**
         * Whether a resolve operation is underway, that is, the {@code future}
         * is not done.
         */
        private boolean resolving;

        @Override
        synchronized void reset() {
            future.complete(null);
            future = NULL_REQUEST_HANDLE_FUTURE;
            haveHandler = false;
            resolving = false;
            /* TODO: Set to SerialVersion.UNKNOWN to catch errors. */
            serialVersion = SerialVersion.CURRENT;
            exceptionSummary = null;
        }

        @Override
        synchronized boolean needsResolution() {
            return !haveHandler;
        }

        @Override
        synchronized boolean needsRepair() {
            return (exceptionSummary != null);
        }

        @Override
        CompletableFuture<Boolean> resolve(RegistryUtils registryUtils,
                                           long timeoutMs) {
            return resolveInternal(registryUtils, timeoutMs, true)
                .thenApply(ref -> ref != null);
        }

        @Override
        CompletableFuture<AsyncRequestHandlerAPI>
            getAsync(RegistryUtils registryUtils, long timeoutMs)
        {
            return resolveInternal(registryUtils, timeoutMs, false);
        }

        /**
         * Implement resolution.  If tryResolve is false, then only attempt to
         * resolve the reference if we don't already have information about a
         * previous failure.
         */
        private synchronized CompletableFuture<AsyncRequestHandlerAPI>
            resolveInternal(RegistryUtils registryUtils,
                            long timeoutMs,
                            boolean tryResolve) {
            try {

                /* Already have the request handler or already resolving */
                if (haveHandler || resolving) {
                    return future;
                }

                /*
                 * If there is no request handler here or on the way,
                 * registryUtils are available (they might not be if the RN has
                 * not finished initialization), and either we are trying to
                 * resolve or there is no exception recorded for a failed
                 * resolve, then lookup request handler in the registry.
                 */
                if ((registryUtils != null) &&
                    (tryResolve || !needsRepair())) {
                    resolving = true;
                    future = new CompletableFuture<>();
                    final Topology topo = registryUtils.getTopology();
                    AsyncRegistryUtils.getRequestHandler(topo, rnId,
                                                         trackerId,
                                                         timeoutMs,
                                                         logger)
                        .whenComplete(
                            unwrapExceptionVoid(this::deliverResult));
                    return future;
                }

                /*
                 * At this point, the future is completed to an exception or
                 * null, and we do not want to try another resolution
                 * immediately. Simply return the future.
                 */
                return future;
            } catch (Throwable e) {
                future.completeExceptionally(e);
                return future;
            }
        }

        /**
         * Deliver the results of a remote call to obtain the request handler.
         * If a request handler was obtained successfully, supply it to the
         * future. If the request handler was not available, note the exception
         * and deliver the failure results immediately. Retrying in that case
         * might have an adverse impact because it could run a request out of
         * time which could otherwise be used to retry the request on another
         * node.
         */
        private synchronized void deliverResult(AsyncRequestHandlerAPI rh,
                                                Throwable e) {
            try {
                if (!resolving) {
                    throw new IllegalStateException("Not resolving");
                }
                resolving = false;
                if (rh != null) {
                    haveHandler = true;
                    serialVersion = rh.getSerialVersion();
                    exceptionSummary = null;
                    future.complete(rh);
                    return;
                }
                haveHandler = false;
                if (e == null) {

                    /*
                     * There is a possibility that the requested RN is not yet
                     * in the topology. In this case requestHandler will be
                     * null. This can happen during some elasticity operations.
                     * Note the exception but return null so that the caller
                     * can retry.
                     */
                    /*
                     * TODO: A null result could also mean that the remote call
                     * timed out. Maybe that case should be noting a
                     * RequestTimeoutException instead?
                     */
                    noteExceptionInternal(
                        new IllegalArgumentException(
                            rnId + " not in topology"));
                    future.complete(null);
                    return;
                }
                if (e instanceof Exception) {
                    noteExceptionInternal((Exception) e);
                }

                if (exceptionSummary != null) {

                    /*
                     * If last resolve failed because of
                     * AuthenticationFailureException, throw that exception,
                     * rather than just returning null, so that the caller
                     * knows to address the problem, since it won't be fixed
                     * automatically by the next resolution. But set
                     * exceptionSummary to null since we have reason to think
                     * that the next resolution will succeed if the caller
                     * fixes the authentication issue.
                     */
                    final Exception lastEx =
                        exceptionSummary.getException();
                    if (lastEx instanceof AuthenticationFailureException) {
                        e = lastEx;
                        exceptionSummary = null;
                    }

                    /*
                     * Throw PersistentDialogException rather than returning
                     * null so that the caller knows it should back off. Save
                     * the exception so that other calls will know that the
                     * problem is likely persistent and not use this RN.
                     * [KVSTORE-1423]
                     */
                    if (lastEx instanceof PersistentDialogException) {
                        e = lastEx;
                        noteExceptionInternal((PersistentDialogException) e);
                    }
                }
                future.completeExceptionally(e);
            } catch (Throwable t) {
                resolving = false;
                haveHandler = false;
                future.completeExceptionally(t);
            }
        }

        /**
         * Note the exception and update haveHandler and the future to return
         * this exception.
         */
        @Override
        synchronized void noteException(Exception e) {
            if (resolving) {
                /*
                 * Someone has already noticed an exception and a resolution is
                 * underway. This method should not be called by the resolution
                 * procedure, i.e., the #deliverResult method.
                 */
                return;
            }
            /*
             * Simply sets the {@code haveHandler} and completes the {@code
             * future}. Do not attempt to resolve immediately since that
             * decision involves needsRepair.
             */
            haveHandler = false;
            future = failedFuture(e);
            noteExceptionInternal(e);
        }

        /**
         * Note the exception but depend on the caller to update haveHandler
         * and the future as appropriate.
         */
        private void noteExceptionInternal(Exception e) {
            if (exceptionSummary == null) {
                exceptionSummary = new ExceptionSummary();
            }
            exceptionSummary.noteException(e);
        }
    }

    /**
     * Tracks errors encountered when trying to create the reference.
     */
    private static class ExceptionSummary {

        /**
         * Track errors resulting from attempts to create a remote reference.
         * If the value is > 0 then exception must be non null and denotes the
         * last exception that resulted in the reference being unresolved.
         */
        private int errorCount = 0;
        private Exception exception = null;
        private long exceptionTimeMs = 0;

        private void noteException(Exception e) {
            exception = e;
            errorCount++;
            exceptionTimeMs = System.currentTimeMillis();
        }

        private Exception getException() {
            return exception;
        }

        @SuppressWarnings("unused")
        private long getExceptionTimeMs() {
            return exceptionTimeMs;
        }

        @SuppressWarnings("unused")
        private int getErrorCount() {
            return errorCount;
        }
    }

    /**
     * Returns true if the version based consistency requirements can be
     * satisfied at time <code>timeMs</code> based upon the historical rate of
     * progress associated with the node.
     *
     * @param timeMs the time at which the consistency requirement is to be
     * satisfied
     *
     * @param consistency the version consistency requirement
     */
    boolean inConsistencyRange(long timeMs, Consistency.Version consistency) {
        assert consistency != null;

        final long consistencyVLSN = consistency.getVersion().getVLSN();

        return vlsnState.vlsnAt(timeMs) >= consistencyVLSN;
    }

    /**
     * Returns true if the time based consistency requirements can be satisfied
     * at time <code>timeMs</code> based upon the historical rate of progress
     * associated with the node.
     *
     * @param timeMs the time at which the consistency requirement is to be
     * satisfied
     *
     * @param consistency the time consistency requirement
     */
    boolean inConsistencyRange(long timeMs,
                               Consistency.Time consistency,
                               RepNodeState master) {
        assert consistency != null;

        if (master == null) {
            /*
             * No master information, assume it is. The information will get
             * updated as a result of the request directed to it.
             */
            return true;
        }

        final long lagMs =
                consistency.getPermissibleLag(TimeUnit.MILLISECONDS);
        final long consistencyVLSN = master.vlsnState.vlsnAt(timeMs - lagMs);
        return vlsnState.vlsnAt(timeMs) >= consistencyVLSN;
    }

    /**
     * VLSNState encapsulates the handling of all VLSN state associated with
     * this node.
     * <p>
     * The VLSN state is typically updated based upon status
     * information returned in a {@link Response}. It's read during a request
     * dispatch for requests that specify a consistency requirement to help
     * direct the request to a node that's in a good position to satisfy the
     * constraint.
     * <p>
     * The state is also updated by {@link RepNodeStateUpdateThread} on a
     * periodic basis through the use of {@link NOP} requests.
     */
    private static class VLSNState {

        /**
         *  The last known vlsn
         */
        private volatile long vlsn = INVALID_VLSN;

        /**
         * The time at which the vlsn above was last updated.
         */
        private long lastUpdateMs;

        /**
         * The starting vlsn used to compute the next iteration of the
         * vlsnsPerSec.
         *
         * Invariant: intervalStart <= vlsn
         */
        private long intervalStart = INVALID_VLSN;

        /**
         *  The time at which the intervalStart was updated.
         *
         *  Invariant: intervalStartMs <= lastUpdateMs
         */
        private long intervalStartMs;

        /* The vlsn rate: vlsns/sec */
        private long vlsnsPerSec;

        private final int rateIntervalMs;

        private VLSNState(int rateIntervalMs) {
            vlsn = NULL_VLSN;
            intervalStart = NULL_VLSN;
            intervalStartMs = 0l;
            vlsnsPerSec = 0;
            this.rateIntervalMs = rateIntervalMs;
        }

        /**
         * Returns the current VLSN associated with this RN
         */
        private long getVLSN() {
           return vlsn;
        }

        /**
         * Returns true if the state information has not been updated over an
         * interval exceeding MAX_RATE_INTERVAL_MS
         */
        private synchronized boolean isObsolete() {
            return (lastUpdateMs + rateIntervalMs)  <
                    System.currentTimeMillis();
        }

        /**
         * Updates the VLSN and maintains the vlsn rate associated with it.
         *
         * @param newVLSN the new VLSN
         */
        private synchronized void updateVLSN(long newVLSN) {

            if ((newVLSN == INVALID_VLSN) || VLSN.isNull(newVLSN)) {
                return;
            }

            lastUpdateMs = System.currentTimeMillis();
            if  (newVLSN > vlsn) {
                vlsn = newVLSN;
            }

            final long intervalMs = (lastUpdateMs - intervalStartMs);
            if (intervalMs <= rateIntervalMs) {
                return;
            }

            if (intervalStartMs == 0) {
                resetRate(lastUpdateMs);
                return;
            }

            final long vlsnDelta = vlsn - intervalStart;
            if (vlsnDelta < 0) {
                /* Possible hard recovery */
                resetRate(lastUpdateMs);
            } else {
                intervalStart = vlsn;
                intervalStartMs = lastUpdateMs;
                vlsnsPerSec = (vlsnDelta * 1000) / intervalMs;
            }
        }

        /**
         * Returns the VLSN that the RN will have progressed to at
         * <code>timeMs</code> based upon its current known state and its rate
         * of progress.
         *
         * @param timeMs the time in ms
         *
         * @return the predicted VLSN at timeMs or NULL_VLSN if the state has
         * not yet been initialized.
         */
        private synchronized long vlsnAt(long timeMs) {
            if (VLSN.isNull(vlsn)) {
                return NULL_VLSN;
            }

            final long deltaMs = (timeMs - lastUpdateMs);
            /* deltaMs can be negative. */

            final long vlsnAt = vlsn +
                            ((deltaMs * vlsnsPerSec) / 1000);

            /*
             * Return the zero VLSN if the extrapolation yields a negative
             * value since vlsns cannot be negative.
             */
            return vlsnAt < 0 ? 0 : vlsnAt;
        }

        private void resetRate(long now) {
            intervalStart = vlsn;
            intervalStartMs = now;
            vlsnsPerSec = 0;
        }
    }
}
