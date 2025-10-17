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

package oracle.kv.impl.pubsub;

import static oracle.kv.pubsub.SubscriptionChangeNotAppliedException.Reason.CHANGE_TIMEOUT;
import static oracle.kv.pubsub.SubscriptionChangeNotAppliedException.Reason.SUBSCRIPTION_ALL_TABLES;
import static oracle.kv.pubsub.SubscriptionChangeNotAppliedException.Reason.SUBSCRIPTION_CANCELED;
import static oracle.kv.pubsub.SubscriptionChangeNotAppliedException.Reason.TABLE_ALREADY_SUBSCRIBED;
import static oracle.kv.pubsub.SubscriptionChangeNotAppliedException.Reason.TABLE_NOT_SUBSCRIBED;
import static oracle.kv.pubsub.SubscriptionChangeNotAppliedException.Reason.TOO_MANY_PENDING_CHANGES;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.OpenTransactionBuffer.ClosedPartGenStreamOp;
import oracle.kv.impl.rep.migration.generation.PartitionGeneration;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.ThreadUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.CheckpointFailureException;
import oracle.kv.pubsub.NoSQLSubscriber;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionChangeNotAppliedException;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.pubsub.SubscriptionTableNotFoundException;
import oracle.kv.stats.SubscriptionMetrics;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * Object represents an implementation of NoSQLSubscription interface. After
 * a subscription is created by publisher, the publisher will return a handle
 * to the instance of this object, which can be subsequently retrieved by user
 * to manipulate the subscription, e.g., start and cancel streaming, do
 * checkpoint, etc.
 */
public class NoSQLSubscriptionImpl implements NoSQLSubscription {

    /**
     * the default number of max worker thread is one because the requests to
     * change stream have to be serial. Currently, it is 1 because we support
     * serial feeder filter update only. In the future, if the protocol
     * supports concurrent updates of feeder filter, we might consider
     * increase the limit for performance purpose.
     */
    private final static int DEFAULT_MAX_WORKER_THREAD = 1;
    /**
     * rate limiting log period in ms
     */
    private static final int RL_LOG_PERIOD_MS = 10 * 60 * 1000;
    /**
     * rate limiting log max number of objects
     */
    private static final int RL_LOG_MAX_OBJ = 1024;
    /**
     * subscription worker thread prefix
     */
    private final static String workerThreadPrefix = "SubscriptionWorkerThread";

    /**
     * parent publishing unit creating this subscription
     */
    private final PublishingUnit parentPU;
    /**
     * subscriber that uses this subscription
     */
    private final NoSQLSubscriber subscriber;
    /**
     * private logger
     */
    private final Logger logger;
    /**
     * a FIFO queue from which to request stream operations
     */
    private final PublishingUnit.BoundedOutputQueue queue;
    /**
     * init stream position
     */
    private final StreamPosition initPos;
    /**
     * true if subscription cancelled, access needs be synchronized
     */
    private volatile boolean canceled;
    /**
     * Cause of cancellation, null if not cancelled or normal cancellation.
     * Must be set with {@link #canceled} atomically.
     */
    private volatile Throwable causeOfCancel;

    /**
     * to sync worker thread
     */
    private final Object workerLock = new Object();
    /**
     * Worker thread to dequeue the stream operations.  Synchronized on
     * workerLock when accessing this field.
     */
    private SubscriptionWorkerThread subscriptionWorkerThread;
    /**
     * # of ops to stream. Per Rule Subscription.17, a demand equal to
     * java.lang.Long.MAX_VALUE is considered as "effectively unbounded".
     * Synchronized on workerLock when accessing this field.
     */
    private long numToStreamInThread;

    /**
     * current stream position
     */
    private final StreamPosition currStreamPos;
    /**
     * worker thread to checkpoint
     */
    private volatile StreamCkptWorkerThread streamCkptWorkerThread;

    /**
     * # of ops consumed by subscriber in the life time of this subscription,
     * operations may be streamed by multiple, sequentially running worker
     * threads.
     */
    private volatile long numStreamedOps;

    /**
     * number of concurrent change worker threads
     */
    private final AtomicInteger pendingRequests;

    /**
     * max pending requests
     */
    private volatile long maxPendingReqs;

    /**
     * for change request task
     */
    private final ExecutorService executor;

    /*-- For test and internal use only. ---*/
    /**
     * true if enable checkpoint in subscription, default is true
     */
    private final boolean enableCheckpoint;

    /**
     * Stream stats report interval in ms
     */
    private final long statReportIntvMs;
    /**
     * Stream stats report interval in number of ops
     */
    private final long statReportIntvOps;
    /**
     * rate limiting logger
     */
    private final RateLimitingLogger<String> rlLogger;

    /**
     * Creates and initiates a subscription instance
     */
    private NoSQLSubscriptionImpl(PublishingUnit parentPU,
                                  NoSQLSubscriber subscriber,
                                  StreamPosition initPos,
                                  Logger logger)
        throws SubscriptionFailureException {

        /* cannot be null, null position shall have been already converted */
        if (initPos == null) {
            throw new SubscriptionFailureException(subscriber
                                                       .getSubscriptionConfig()
                                                       .getSubscriberId(),
                                                   "Cannot create " +
                                                   "subscription instance " +
                                                   "with null initial " +
                                                   "position");
        }

        this.parentPU = parentPU;
        this.subscriber = subscriber;
        this.initPos = initPos;
        this.logger = logger;

        final NoSQLSubscriptionConfig config =
            subscriber.getSubscriptionConfig();
        enableCheckpoint = config.isCkptEnabled();
        statReportIntvMs = config.getStatReportIntervalMs();
        statReportIntvOps = config.getStatReportIntervalOps();

        queue = parentPU.getOutputQueue();
        canceled = false;
        causeOfCancel = null;
        currStreamPos = new StreamPosition(initPos);
        subscriptionWorkerThread = null;
        streamCkptWorkerThread = null;
        numStreamedOps = 0;
        pendingRequests = new AtomicInteger(0);
        maxPendingReqs =
            SubscriptionChangeNotAppliedException.MAX_NUM_PENDING_CHANGES;
        executor = Executors.newFixedThreadPool(
            DEFAULT_MAX_WORKER_THREAD, new NoSQLSubscriptionThreadFactory());
        rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS,
                                            RL_LOG_MAX_OBJ,
                                            logger);
    }

    /**
     * Returns a handle to the NoSQLSubscription
     *
     * @param parentPU   parent publishing unit
     * @param subscriber NoSQL subscriber that uses the subscription
     * @param startPos   start stream position
     * @param logger     private logger
     * @return an instance of the NoSQL subscription
     * @throws SubscriptionFailureException if fail to create a subscription
     */
    static NoSQLSubscriptionImpl get(PublishingUnit parentPU,
                                     NoSQLSubscriber subscriber,
                                     StreamPosition startPos,
                                     Logger logger)
        throws SubscriptionFailureException {

        return new NoSQLSubscriptionImpl(parentPU, subscriber, startPos,
                                         logger);
    }

    /**
     * Returns the index associated with the subscriber
     *
     * @return the index associated with the subscriber
     */
    @Override
    public NoSQLSubscriberId getSubscriberId() {
        return subscriber.getSubscriptionConfig().getSubscriberId();
    }

    /**
     * Returns the instantaneous position in the stream. All elements up to
     * and including this position have been delivered to the Subscriber via
     * Subscriber.onNext().
     * <p>
     * This position can be used by a subsequent subscriber to resume the
     * stream from this point forwards, effectively resuming an earlier
     * subscription.
     * <p>
     * To make it efficient, current stream position will be updated at a fixed
     * time interval, instead of for each incoming operation.
     */
    @Override
    public StreamPosition getCurrentPosition() {
        /*
         * Because this could be called concurrently with the thread that
         * process stream operations and update the current stream position,
         * the currStreamPos could higher, equal, or lower than the min
         * position from open transaction buffer, therefore, we should
         * compare and return minimal of either position for each shard.
         */
        final String store = currStreamPos.getStoreName();
        final long sid = currStreamPos.getStoreId();
        final StreamPosition ret = new StreamPosition(store, sid);
        final StreamPosition otbMinPos = parentPU.getMinPosFromOTB(store, sid);
        currStreamPos.getAllShardPos().forEach(s -> {
            final int gid = s.getRepGroupId();
            final StreamPosition.ShardPosition sp =
                otbMinPos.getShardPosition(gid);
            if (sp == null) {
                ret.setShardPosition(gid, s.getVLSN());
            } else {
                ret.setShardPosition(gid, Math.min(s.getVLSN(), sp.getVLSN()));
            }
        });
        rlLogger.log(store + sid, Level.INFO,
                     lm("OTB minimal position=" + otbMinPos +
                       ", delivered position=" + currStreamPos +
                       ", return current stream position=" + ret));
        return ret;
    }

    /**
     * Does an exact subscription checkpoint. The checkpoint will be made to
     * the source NoSQL DB. Each subscription has its own checkpoints stored
     * in a particular table. The table is created and populated the first
     * time the checkpoint is made, and updated when subscription makes each
     * subsequent checkpoints.
     *
     * @param streamPosition the stream position to checkpoint
     */
    @Override
    public synchronized void doCheckpoint(StreamPosition streamPosition) {
        if (!enableCheckpoint) {
            /* unit test only */
            logger.warning(lm("Checkpoint disabled"));
            return;
        }

        if (isCanceled()) {
            logger.warning(lm("Subscription already canceled, no more " +
                              "checkpoint."));
            return;
        }

        /* no concurrent checkpoint threads for a subscription at any time */
        if (streamCkptWorkerThread != null &&
            streamCkptWorkerThread.isAlive()) {
            final NoSQLSubscriberId sid = subscriber.getSubscriptionConfig()
                                                    .getSubscriberId();
            final String err = "Cannot do checkpoint because there " +
                               "is a concurrently running checkpoint " +
                               "for subscriber " + sid;
            final Throwable cause =
                new CheckpointFailureException(sid, null, err, null);
            /* warning msg in throwable and callback decides if it be logged */
            logger.fine(() -> lm(err));
            subscriber.onCheckpointComplete(streamPosition, cause);
            return;
        }

        streamCkptWorkerThread = new StreamCkptWorkerThread(streamPosition);
        streamCkptWorkerThread.start();
    }

    /**
     * Makes a checkpoint with given stream position or optimized position
     * @param streamPosition the stream position to checkpoint
     * @param exact true if checkpoint at exact given position, false if use
     *              an optimized stream position to checkpoint.
     */
    @Override
    public synchronized void doCheckpoint(StreamPosition streamPosition,
                                          boolean exact) {
        final StreamPosition ckpt = exact ? streamPosition :
            getOptimizedPosition(streamPosition);
        doCheckpoint(ckpt);
    }

    /**
     * Gets optimized position with respect to the given one
     * @param streamPosition the stream position to checkpoint
     * @return an optimized stream position
     */
    @Override
    public StreamPosition getOptimizedPosition(StreamPosition streamPosition) {
        return parentPU.getPositionForCheckpoint(streamPosition);
    }

    /**
     * Gets the per-shard filter metrics
     */
    public Map<Integer, FeederFilterStat> getFilterMetrics() {
        return parentPU.getFilterMetrics();
    }

    /**
     * Starts stream a given number of operations from publisher. It spawns a
     * thread running to dequeue the operations from publisher, and apply the
     * subscriber-defined callbacks on each operation.
     * <p>
     * Per Rule Subscription.16, Subscription.request MUST return normally.
     * The only legal way to signal failure to a Subscriber is via the
     * onError method.
     *
     * @param n number of operations to stream.
     */
    @Override
    public synchronized void request(long n) {

        if (isCanceled()) {
            /*
             * Per Rule Subscription.6, after the Subscription is canceled,
             * additional Subscription.request(long n) MUST be NOPs.
             */
            logger.info(lm("Subscription has already been canceled, NOP."));
            return;
        }

        if (n <= 0) {
            /*
             * Per Rule Subscription.9, while the Subscription is not
             * canceled, Subscription.request(long n) MUST signal onError
             * with a java.lang.IllegalArgumentException if the argument is <=
             * 0. The cause message MUST include a reference to this rule
             * and/or quote the full rule.
             */
            final IllegalArgumentException err =
                new IllegalArgumentException("Per Rule Subscription.9 in " +
                                             "reactive stream spec, the " +
                                             "argument of Subscription" +
                                             ".request cannot be less than or" +
                                             " equal to 0.");
            subscriberOnError(err);
            return;
        }

        /*
         * About recursive request call
         *
         * Per Rule Subscription.3 in reactive streams spec, Subscription
         * .request MUST place an upper bound on possible synchronous
         * recursion between Publisher and Subscriber. But recursion can only
         * happen if a call to request can produce a synchronous call to
         * onNext, which doesn't apply in our implementation because an async
         * subscriptionWorkerThread is created in which all onNext will be
         * called from there. Therefore this rule does not apply to us and we
         * don't need the recursion check at all.
         */

        /*
         * Synchronized with existing worker thread clean up if any
         */
        synchronized (workerLock) {
            /* no running worker, safe to create a new one */
            if (subscriptionWorkerThread == null) {
                numToStreamInThread = n;
                subscriptionWorkerThread = new SubscriptionWorkerThread();
                subscriptionWorkerThread.start();
                logger.fine(() -> lm("A new worker thread=" +
                                     subscriptionWorkerThread.getName() +
                                     " has been created to stream " +
                                     (n == Long.MAX_VALUE ? "infinite" : n) +
                                     " ops," +
                                     " # ops already streamed in " +
                                     "subscription=" + numStreamedOps));
            } else {
                /* there is a running worker */
                numToStreamInThread += n;
                if (numToStreamInThread < 0) {
                    /* handle overflow */
                    numToStreamInThread = Long.MAX_VALUE;
                }
                /* wake up worker if in sleep */
                workerLock.notifyAll();
                logger.finest(() -> lm("Worker adds " +
                                       (n == Long.MAX_VALUE ? "infinite" : n) +
                                       " ops to stream, new " +
                                       "total=" + numToStreamInThread +
                                       ", # ops already streamed in " +
                                       "subscription=" + numStreamedOps));
            }
        }
    }

    /**
     * User requires to cancel an ongoing subscription. The subscription is
     * canceled and publisher will clean up and free resources. Terminates
     * shard streams and checkpoint threads, clear queues, and close handle
     * to source kvstore etc. The publisher will also signal onComplete to
     * subscriber after the subscription is canceled.
     * <p>
     * Reactive stream spec requires Subscription.cancel return in a timely
     * manner, and be idempotent and be thread-safe. Also, per Rule
     * Subscription.15, Subscription.cancel MUST return normally. The only
     * legal way to signal failure to a Subscriber is via the onError method.
     */
    @Override
    public void cancel() {

        /* user requires to cancel the subscription */
        cancel(null);
    }

    /**
     * Returns true if the subscription has been shut down.
     *
     * @return true if the subscription has been shut down, false otherwise.
     */
    @Override
    public synchronized boolean isCanceled() {
        return canceled;
    }

    /**
     * Returns the cause of cancellation, or null
     */
    public synchronized Throwable getCauseOfCancel() {
        return causeOfCancel;
    }

    /**
     * Gets the last checkpoint stored in kv store for the given subscription
     *
     * @return the last checkpoint associated with that subscription, or null
     * if this subscription does not have any persisted checkpoint in kv store.
     */
    @Override
    public StreamPosition getLastCheckpoint() {
        return parentPU.getLastCheckpoint();
    }

    /**
     * Returns the subscription metrics
     *
     * @return the subscription metrics
     */
    @Override
    public SubscriptionMetrics getSubscriptionMetrics() {
        return parentPU.getStatistics();
    }

    /**
     * For test use only
     *
     * @return id of shards this subscription covers
     */
    public Set<RepGroupId> getCoveredShards() {
        return parentPU.getCoveredShards();
    }

    /**
     * For test use only
     */
    public void removeTableFromParentPU(TableImpl table) {
        parentPU.removeTable(table);
    }

    /**
     * Gets init stream position from where streaming begins.
     *
     * @return init stream position
     */
    public StreamPosition getInitPos() {
        return initPos;
    }

    /**
     * For test use only
     *
     * @return subscriber
     */
    public NoSQLSubscriber getSubscriber() {
        return subscriber;
    }

    /**
     * For test use only
     */
    public void setMaxPendingReqs(int maxReqs) {
        maxPendingReqs = maxReqs;
    }

    /**
     * Gets the number of pending requests that have been submitted to the
     * scheduler
     */
    public int getPendingRequests() {
        return pendingRequests.get();
    }

    /**
     * Adds a subscribed table to the running subscription.
     *
     * @param tableName name of table
     */
    @Override
    public void subscribeTable(String tableName) {
        subscribeTable(tableName, false);
    }

    /**
     * Adds a subscribed table to the running subscription, and specify if to
     * stream transaction.
     * @param tableName name of the table
     * @param streamTxn true if to stream transactions, false to stream write
     *                 operations in {@link StreamOperation}.
     */
    @Override
    public void subscribeTable(String tableName, boolean streamTxn) {
        startChangeWorker(StreamChangeReq.Type.ADD, tableName, streamTxn);
    }

    /**
     * Removes a subscribe table from the running subscription.
     *
     * @param tableName name of table
     */
    @Override
    public void unsubscribeTable(String tableName) {
        startChangeWorker(StreamChangeReq.Type.REMOVE, tableName, false);
    }

    /**
     * Returns the set of currently subscribed tables
     *
     * @return the set of currently subscribed tables
     * @throws SubscriptionFailureException if the subscription already
     *                                      cancelled or has shut down.
     */
    @Override
    public Set<String> getSubscribedTables() {
        if (isCanceled() || parentPU.isClosed()) {
            throw new SubscriptionFailureException(
                getSubscriberId(), "Subscription is cancelled.");
        }

        return parentPU.getSubscribedTables();
    }

    /**
     * Clean up and free resources. Terminates shard streams and checkpoint
     * threads, clear queues, and close handle to source kvstore etc.
     *
     * @param cause cause of subscription is canceled, null if shutdown
     *              normally without error.
     */
    public synchronized void cancel(Throwable cause) {

        /*
         * Cancel subscription needs be sync with request and checkpoint since
         * they both check if subscription has been cancelled
         */

        /* avoid multiple simultaneous cancels */
        if (canceled) {
            /*
             * Per Rule Subscription.7, after the Subscription is canceled,
             * additional Subscription.cancel() MUST be NOPs.
             */
            return;
        }
        canceled = true;
        causeOfCancel = cause;
        logger.info(lm("Subscription starts to shut down, cause=" + cause));

        executor.shutdown();
        logger.info(lm("Executor shuts down"));

        /*
         * first try soft shut down and wait till worker thread exit, if fail
         * to soft shutdown, interruption will be signaled to make thread
         * exit. In either case, subscriptionWorkerThread wont be modified at
         * the same time by caller and thread itself, hence no need to put in
         * lock.
         */
        shutDownWorkerThread(subscriptionWorkerThread);
        subscriptionWorkerThread = null;
        logger.info(lm("Subscription worker thread shuts down"));

        shutDownWorkerThread(streamCkptWorkerThread);
        streamCkptWorkerThread = null;
        logger.info(lm("Subscription checkpoint thread shuts down"));

        /*
         * Close parent PU because per Rule Subscription.12, Subscription
         * .cancel() MUST request Publisher to eventually stop signaling its
         * Subscriber.
         */
        parentPU.close(cause);

        logger.info(lm("Subscription canceled at stream position=" +
                       currStreamPos));
    }

    long getNumStreamedOps() {
        return numStreamedOps;
    }

    /**
     * Gets current time stamp in ms
     *
     * @return current time stamp in ms
     */
    static long getCurrTimeMs() {
        // TODO: consider using nanoTime if it becomes a performance issue
        return System.currentTimeMillis();
    }

    /**
     * Unit test only.
     *
     * @return checkpoint manager
     */
    public CheckpointTableManager getCKptManager() {
        if (parentPU == null) {
            return null;
        }
        return parentPU.getCkptTableManager();
    }

    /**
     * Unit test only
     */
    public PublishingUnit getParentPU() {
        return parentPU;
    }

    /**
     * Unit test only
     */
    long getStatReportIntvMs() {
        return statReportIntvMs;
    }

    /**
     * Unit test only
     */
    long getStatReportIntvOps() {
        return statReportIntvOps;
    }

    /*-----------------------------------*/
    /*-       PRIVATE FUNCTIONS         -*/
    /*-----------------------------------*/
    private String lm(String msg) {
        return "[SI-" +
               (parentPU == null || parentPU.getCkptTableManager() == null ?
                   "<na>" : parentPU.getCkptTableManager().getCkptTableName()) +
               "-" + subscriber.getSubscriptionConfig().getSubscriberId()
               + "] " + msg;
    }

    /* shut down worker thread */
    private void shutDownWorkerThread(StoppableThread thread) {
        if (thread == null || !thread.isAlive() || thread.isShutdown()) {
            return;
        }
        /* wake it up if in sleep */
        synchronized (workerLock) {
            workerLock.notifyAll();
        }
        logger.fine(() -> lm("Start shuts down thread=" + thread.getName()));
        thread.shutdownThread(logger);
        logger.fine(() -> lm("Thread=" + thread.getName() +
                             "(id=" + ThreadUtils.threadId(thread) + ")" +
                             " shut down."));
    }

    /* update the current stream position */
    private synchronized void updateStreamPos(StreamOperation op) {
        final int shardId = op.getRepGroupId();
        final StreamSequenceId seq = (StreamSequenceId) op.getSequenceId();
        final long vlsn = seq.getSequence();
        final StreamPosition.ShardPosition currPos =
            currStreamPos.getShardPosition(shardId);
        if (currPos == null) {
            /* a new shard (e.g., store expansion), add to stream position */
            currStreamPos.addShardPosition(shardId, vlsn);
            logger.fine(() -> lm("A new shard=" + shardId + " added to stream" +
                                 " position at=" + vlsn));
        } else {
            currPos.setVlsn(vlsn);
        }
    }

    /* rebalance multiple subscribers if necessary */
    private void rebalance() {
        /*
         * TODO: check ckpt rows of other shards to see any elastic
         * operations happened in kvstore, rebalance if necessary
         */
    }

    /* private worker thread to dequeue and process messages */
    private class SubscriptionWorkerThread extends StoppableThread {

        private final static int SOFT_SHUTDOWN_TIMEOUT_MS =
            PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS +
            60 * 1000 /* hopefully enough for onNext() */;
        private final static int LOCK_WAIT_TIMEOUT_MS =
            PublishingUnit.OUTPUT_QUEUE_TIMEOUT_MS;
        /** # ops streamed in this worker thread */
        private volatile long numStreamedInThread;

        SubscriptionWorkerThread() {
            super(workerThreadPrefix + "-" +
                  parentPU.getCkptTableManager().getCkptTableName() + "-" +
                  subscriber.getSubscriptionConfig().getSubscriberId());
            numStreamedInThread = 0;
        }

        @Override
        public void run() {
            logger.info(lm("Subscription worker thread=" + getName() +
                           " starts dequeue " +
                           (numToStreamInThread == Long.MAX_VALUE ?
                               "infinite" : numToStreamInThread) + " ops."));
            /* # ops last reported */
            long lastReported = 0;
            /* last time the stat reported */
            long lastReportedMs = System.currentTimeMillis();
            try {
                while (!isShutdown()) {
                    /*
                     * Loop exits normally. Protected in lock to prevent user
                     * from adding more entries to stream after worker leaves
                     * the loop.
                     */
                    synchronized (workerLock) {
                        /*
                         * If streamed enough data, put the worker in sleep.
                         * The worker will be waken up when 1) request() is
                         * called to stream more data, or 2) the steam is
                         * canceled and need shut down the worker
                         */
                        while ((numStreamedInThread >= numToStreamInThread) &&
                               !isShutdown()) {
                            logger.finest(() -> lm("# ops streamed=" +
                                                   numStreamedInThread +
                                                   ", # ops to stream=" +
                                                   numToStreamInThread +
                                                   ", put in waitMs=" +
                                                   LOCK_WAIT_TIMEOUT_MS));
                            workerLock.wait(LOCK_WAIT_TIMEOUT_MS);
                        }

                        if (isShutdown()) {
                            logger.fine(() -> lm("Start exit worker thread,  " +
                                                 ", # ops streamed " +
                                                 numStreamedInThread +
                                                 ", # ops to stream " +
                                                 (numToStreamInThread ==
                                                  Long.MAX_VALUE ?
                                                     " infinite " :
                                                     numToStreamInThread)));
                            break;
                        }
                    }

                    final StreamOperation op = queue.dequeue(this::isShutdown);
                    if (op == null) {
                        /* in shutdown */
                        return;
                    }

                    /* get an error from Publisher */
                    if (op instanceof Throwable) {
                        subscriberOnError((Throwable) op);
                        break;
                    }

                    /*
                     * process dummy stream operations. Dummy operation would
                     * not be delivered to subscriber.
                     */
                    if (op instanceof ClosedPartGenStreamOp) {
                        final ClosedPartGenStreamOp closeOp =
                            (ClosedPartGenStreamOp) op;
                        processClosedGen(closeOp);
                        continue;
                    }

                    /*
                     * User may cancel the subscription in onNext, thus we
                     * need to update the stats before signal subscriber.
                     */

                    /* must be a regular stream operation, update stats */
                    updateStreamPos(op);

                    /*
                     * update local and global counter
                     */
                    synchronized (this) {
                        numStreamedInThread++;
                        numStreamedOps++;
                    }

                    /* ask pu to collect stats periodically */
                    final long now = System.currentTimeMillis();
                    if (parentPU != null &&
                        needCollectStat(lastReported, lastReportedMs, now)) {
                        parentPU.doStatCollection();
                        lastReported = numStreamedInThread;
                        lastReportedMs = now;
                    }

                    /* finally, signal subscriber */
                    if (!isShutdown()) {
                        subscriberOnNext(subscriber, op);
                    }
                }
            } catch (InterruptedException ie) {
                final String err = "Unable to dequeue due to interruption";
                logger.warning(err);
                final SubscriptionFailureException sfe =
                    new SubscriptionFailureException(
                        subscriber.getSubscriptionConfig().getSubscriberId(),
                        err);

                /* notify subscriber */
                subscriberOnError(sfe);
            } catch (Throwable err) {
                if (!isShutdown()) {
                    /* signal subscriber for any other exception or error */
                    final String msg = "Subscription worker thread to exit, " +
                                       "error=" + err;
                    logger.warning(msg);
                    /* notify subscriber */
                    subscriberOnError(err);
                }
            } finally {
                exitThread();
            }
        }

        /**
         * Processes the closed partition generation. It updates the checkpoint
         * table with the given vlsn in the dummy stream operation
         * @param closed dummy stream operation for closed partition generation
         */
        void processClosedGen(ClosedPartGenStreamOp closed) {

            final PartitionGeneration gen = closed.getCloseGen();
            final RepGroupId shardId = new RepGroupId(closed.getRepGroupId());
            final long ckptVLSN =
                ((StreamSequenceId) closed.getSequenceId()).getSequence();
            final ReplicationStreamConsumer rsc =
                parentPU.getConsumer(shardId);
            final OpenTransactionBuffer otb = rsc.getTxnBuffer();
            final PartitionGenMarkerProcessor pm =
                otb.getPartGenMarkProcessor();

            logger.info(lm("Dequeued a stream operation on a" +
                           " closed generation, shard=" + shardId +
                           ", closed generation=" + gen +
                           ", will checkpoint vlsn=" + ckptVLSN));
            try {
                pm.processClosedGen(gen, shardId, ckptVLSN);
            } catch (CheckpointFailureException cfe) {
                /*
                 * Cannot continue, otherwise other subscriber might be
                 * blocked forever, have to fail the stream to signal
                 * subscriber.
                 */
                final String err = "Unable to checkpoint for closed " +
                                   "generation, shard=" + shardId +
                                   ", ckpt vlsn=" + ckptVLSN +
                                   ", closed generation=" + gen +
                                   ", error=" + cfe;
                final SubscriptionFailureException sfe =
                    new SubscriptionFailureException(
                        subscriber.getSubscriptionConfig().getSubscriberId(),
                        err,  cfe);
                subscriberOnError(sfe);
                logger.warning(lm("Cancel subscription stream because of " +
                                  " failure to checkpoint for closed " +
                                  "generation, shard=" + shardId +
                                  ", ckpt vlsn=" + ckptVLSN +
                                  ", closed generation=" + gen +
                                  ", error=" + cfe));
            }
        }

        private boolean needCollectStat(long lastReported,
                                        long lastReportMs,
                                        long now) {
            if (numStreamedInThread - lastReported >= statReportIntvOps) {
                return true;
            }
            return now - lastReportMs >= statReportIntvMs;
        }

        private void exitThread() {

            /* ask pu to collect stats before exits */
            if (parentPU != null) {
                parentPU.doStatCollection();
            }

            if (numStreamedInThread < numToStreamInThread) {

                /*
                 * unable to stream all required entries because it is
                 * cancelled in the middle of subscription
                 */
                logger.info(lm("Worker thread exits due to cancellation, " +
                               "ops streamed by worker: " +
                               numStreamedInThread +
                               " while requested: " +
                               (numToStreamInThread == Long.MAX_VALUE ?
                                   "infinite" : numToStreamInThread)));

                if (queue != null && !queue.isEmpty()) {
                    logger.fine(() -> lm("# unconsumed messages in queue=" +
                                         queue.getCurrNumQueuedOps() +
                                         ", current queue size in bytes=" +
                                         queue.getCurrSizeBytes() +
                                         ", remaining messages: \n" +
                                         Arrays.toString(
                                             queue.getQueuedOps())));
                }
            } else {
                logger.fine(() -> lm("Worker thread exits after streaming all" +
                                     " requested " +
                                     (numToStreamInThread == Long.MAX_VALUE ?
                                         "infinite" : numToStreamInThread) +
                                     " ops, "));
            }

            logger.info(lm("Worker thread has done streaming with final" +
                           " position=" + currStreamPos +
                           ", total # streamed ops in the " +
                           "subscription=" + numStreamedOps +
                           ", subscription cancelled=" + canceled +
                           ", worker thread shutdown=" + isShutdown()));
        }

        @Override
        protected int initiateSoftShutdown() {
            /*
             * when shutdown by StoppableThread.shutdownThread(), wait a bit to
             * let thread itself detect shutdown flag and exit neatly in soft
             * shutdown.
             */
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(() -> lm("Signal worker thread to shutdown, " +
                                 "already signalled?=" + alreadySet +
                                 ", wait for time(ms)=" +
                                 SOFT_SHUTDOWN_TIMEOUT_MS + " to let it exit"));
            return SOFT_SHUTDOWN_TIMEOUT_MS;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /* private worker thread to checkpoint */
    private class StreamCkptWorkerThread extends StoppableThread {

        private final StreamPosition pos;

        StreamCkptWorkerThread(StreamPosition pos) {
            super("StreamCkptWorkerThread-" +
                  subscriber.getSubscriptionConfig().getSubscriberId());

            this.pos = pos;
        }

        @Override
        public void run() {

            try {
                parentPU.getCkptTableManager().updateCkptTableInTxn(pos);

                /* rebalance subscribers if necessary */
                rebalance();

                /* finally let subscriber know the result */
                subscriber.onCheckpointComplete(pos, null);
            } catch (CheckpointFailureException cfe) {
                logger.fine(() -> lm("Fail to checkpoint at=" + pos +
                                     ", exception=" + cfe));
                subscriber.onCheckpointComplete(pos, cfe);
            } catch (Throwable thr) {
                logger.warning(lm("Unexpected error to checkpoint at=" + pos +
                                  ", will shut down, error=" + thr));
                subscriber.onCheckpointComplete(pos, thr);
                subscriberOnError(thr);
            }
        }

        @Override
        protected int initiateSoftShutdown() {
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(() -> lm("Signal checkpoint worker to shutdown, " +
                                 "already signalled? " + alreadySet +
                                 ", wait for time(ms)=" +
                                 CheckpointTableManager.CKPT_TIMEOUT_MS +
                                 " to let it exit"));
            return CheckpointTableManager.CKPT_TIMEOUT_MS;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /* utility function call subscriber onChangeResult */
    private void subscriberOnChangeResult(NoSQLSubscriber s,
                                          StreamPosition position,
                                          Throwable e) {
        try {
            s.onChangeResult(position, e);
        } catch (Throwable exp) {
            final String err = "Exception in onChangeResult to process " +
                               "change result, position=" + position + ", " +
                               "result=" + e + ", exception=" + exp;
            final SubscriptionFailureException sfe =
                new SubscriptionFailureException(
                    subscriber.getSubscriptionConfig().getSubscriberId(),
                    err, exp);

            subscriberOnError(sfe);
            logger.warning(lm("Cancel subscription stream because of " +
                              "exception in executing subscriber " +
                              "onChangeResult to process position=" +
                              position + ", error=" + exp +
                              "\n" + LoggerUtils.getStackTrace(exp)));
        }
    }

    /* utility function call subscriber onError */
    private void subscriberOnError(Throwable err) {

        /*
         * Per Rule Subscription.6, If a Publisher signals either onError
         * or onComplete on a Subscriber, that Subscriber's Subscription
         * MUST be considered canceled.
         */
        /* final stat collection */
        if (parentPU != null) {
            parentPU.doStatCollection();
        }

        /* pu will signal onError to subscriber */
        cancel(err);
    }

    /* utility function call subscriber onNext */
    private void subscriberOnNext(NoSQLSubscriber s, StreamOperation op) {
        try {
            s.onNext(op);
        } catch (Throwable thr) {

            final String opStr = op.toJsonString();

            /*
             * Looks there is no rule in reactive stream spec that when
             * non-normal returns from onNext, we shall cancel the
             * subscription or not. For safety, we notify subscriber and
             * cancel the subscription, and user need to fix her onNext in
             * order to subscribe streams.
             */
            final String err = "in onNext for op=" + opStr + ", error=" + thr;
            final SubscriptionFailureException sfe =
                new SubscriptionFailureException(
                    subscriber.getSubscriptionConfig().getSubscriberId(),
                    err, thr);
            subscriberOnError(sfe);
            logger.warning(lm("Subscription canceled, reason=" + err +
                              "\n" + LoggerUtils.getStackTrace(thr)));
        }
    }

    /* Starts change worker thread */
    private synchronized void startChangeWorker(StreamChangeReq.Type type,
                                                String table,
                                                boolean streamTxn) {
        if (!isCanceled() && !parentPU.isClosed()) {
            /* check if too many requests before submit to executor */
            if (!isTooManyRequests()) {
                executor.submit(
                    new StreamChangeWorkerThread(type, table, streamTxn));
                logger.info(lm("Submitted worker thread for request=" + type +
                               " table=" + table + ", #pending requests=" +
                               pendingRequests.incrementAndGet()));
                return;
            }

            final String err = "Concurrent number of change " +
                               "requests has reached maximum=" +
                               maxPendingReqs + ", please try later" +
                               ", request=" + type + ", table=" + table;
            logger.warning(lm(err));
            return;
        }

        final String err = "Subscription is already canceled " +
                           "or shutdown.";
        logger.info(lm(err));
        final Exception exp = new SubscriptionChangeNotAppliedException(
            getSubscriberId(), SUBSCRIPTION_CANCELED, err);

        /*
         * {@link NoSQLSubscription#subscribeTable(String)} and
         * {@link NoSQLSubscription#unsubscribeTable(String)} are async, thus
         * signal subscriber from another thread to avoid blocking
         */
        new Thread(() -> subscriberOnChangeResult(subscriber, null, exp))
            .start();
    }

    /**
     * Checks if too many concurrent pending requests. If yes, signal the
     * subscriber from another thread to avoid blocking
     * @return true if there are too many concurrent requests, false otherwise.
     */
    private boolean isTooManyRequests() {
        final int count = pendingRequests.get();
        if (count < maxPendingReqs) {
            return false;
        }

        final String err = "Concurrent number of change requests=" + count +
                           " has reached maximum=" + maxPendingReqs;
        final NoSQLSubscriberId sid = getSubscriberId();
        final Throwable exp = new SubscriptionChangeNotAppliedException(
            sid, TOO_MANY_PENDING_CHANGES, err);
        /* create a new thread signal subscriber to avoid blocking */
        new Thread(() -> subscriberOnChangeResult(subscriber, null, exp))
            .start();
        return true;
    }

    /* private worker thread to checkpoint */
    private class StreamChangeWorkerThread extends StoppableThread {

        private final StreamChangeReq.Type type;
        private final String tableName;
        private final boolean streamTxn;
        StreamChangeWorkerThread(StreamChangeReq.Type type,
                                 String tableName,
                                 boolean streamTxn) {
            super("StreamChangeWorkerThread" + "-" +
                  UUID.randomUUID().toString().subSequence(0, 8) +
                  "-" + subscriber.getSubscriptionConfig().getSubscriberId());
            this.type = type;
            this.tableName = tableName;
            this.streamTxn = streamTxn;
        }

        @Override
        public void run() {

            logger.info(lm("Change worker thread" +
                           "(id=" +
                           ThreadUtils.threadId(Thread.currentThread()) + ")" +
                           " starts, type=" + type + ", table=" + tableName));
            try {
                final NoSQLSubscriberId sid = getSubscriberId();
                if (getSubscriber().getSubscriptionConfig()
                                   .isWildCardStream()) {
                    final String err =
                        "Cannot change a subscription that is configured to " +
                        "stream from all user tables.";
                    logger.fine(() -> lm(err));
                    throw new SubscriptionChangeNotAppliedException(
                        sid, SUBSCRIPTION_ALL_TABLES, err);
                }

                if (type.equals(StreamChangeReq.Type.ADD) &&
                    parentPU.isTableSubscribed(tableName)) {
                    final String err = "Table=" + tableName + " is already in" +
                                       " subscription.";
                    logger.info(lm(err));
                    /* nothing changed */
                    throw new SubscriptionChangeNotAppliedException(
                        sid, TABLE_ALREADY_SUBSCRIBED, err);
                }

                if (type.equals(StreamChangeReq.Type.REMOVE) &&
                    !parentPU.isTableSubscribed(tableName)) {
                    final String err = "Table=" + tableName +
                                       " is not subscribed in subscription.";
                    logger.info(lm(err));
                    /* nothing changed */
                    throw new SubscriptionChangeNotAppliedException(
                        sid, TABLE_NOT_SUBSCRIBED, err);
                }

                final TableImpl tableImpl =
                    parentPU.getTable(getSubscriberId(), tableName);
                if (tableImpl == null) {
                    final String err = "Table=" + tableName + " does not " +
                                       "exist.";
                    logger.info(lm(err));
                    throw new SubscriptionTableNotFoundException(tableName);
                }

                /*
                 * Close the gate and no writes will be streamed from the
                 * table to remove. Removing table from feeder is a background
                 * clean-house process after signaling the user.
                 */
                if (type.equals(StreamChangeReq.Type.REMOVE)) {
                    processRemove(tableImpl);
                }

                /* ask PU send change to feeder and collect result */
                final StreamChangeStatus status =
                    parentPU.applyChange(this, type, tableImpl);

                /* process result */
                final String err;
                switch (status) {
                    case NOT_APPLICABLE:
                        /* nothing changed */
                        err = "Change not applicable to filter";
                        logger.fine(() -> lm(err));
                        if (type.equals(StreamChangeReq.Type.ADD)) {
                            /*
                             * When adding table and status is
                             * NOT_APPLICABLE, that means the id of the table
                             * is already at the feeder filter. This could
                             * happen when the previous remove table request
                             * of the same table does not complete. Since the
                             * table id is in the source feeder filter, we
                             * add table into stream and consider the change
                             * success
                             */
                            logger.info(lm("Id of table=" + tableName +
                                           " already exists in the feeder " +
                                           "filter, type=" + type));
                            processAdd(tableImpl);
                        } else {
                            /*
                             * When removing table and status is
                             * that means the table id to remove does not exist
                             * in the feeder filter. In this case, it is
                             * considered as a success. At this time subscriber
                             * has been signalled in
                             * {@link #processRemove(TableImpl)}
                             */
                            logger.info(lm("Id of table=" + tableName +
                                           " does not in the feeder filter, " +
                                           "type=" + type));
                        }
                        break;
                    case OK:
                        err = "Change successfully applied to filter" +
                              " (type=" + type + ", table=" + tableName +
                              ", stream txn=" + streamTxn + ")";
                        logger.fine(() -> lm(err));
                        if (type.equals(StreamChangeReq.Type.ADD)) {
                            processAdd(tableImpl);
                        }
                        break;
                    case TIMEOUT:
                        err = "Timeout in applying the change filter" +
                              " (type=" + type + ", table=" + tableName + ")";
                        logger.info(lm(err));
                        if (type.equals(StreamChangeReq.Type.ADD)) {
                            throw new SubscriptionChangeNotAppliedException(
                                sid, CHANGE_TIMEOUT, err);
                        }
                        break;
                    case FAIL:
                        err = "Fail to change the stream," +
                              " (type=" + type + ", table=" + tableName + ")";
                        logger.info(lm(err));
                        if (type.equals(StreamChangeReq.Type.ADD)) {
                            throw new SubscriptionFailureException(
                                getSubscriberId(), err);
                        }
                }
            } catch (SubscriptionChangeNotAppliedException |
                SubscriptionTableNotFoundException exp) {
                subscriberOnChangeResult(subscriber, null, exp);
            } catch (Throwable thr) {
                /* all other exceptions that need terminate the stream */
                subscriberOnError(thr);
                /* notify subscriber that the change has failed */
                subscriberOnChangeResult(subscriber, null, thr);
            } finally {
                final int remaining = pendingRequests.decrementAndGet();
                logger.info(lm("Change worker thread=" + "(id=" +
                               ThreadUtils.threadId(Thread.currentThread()) +
                               ") exits, # remaining pending requests=" +
                               remaining));
            }
        }

        /* add table */
        private void processAdd(TableImpl tableImpl) {
            synchronized (NoSQLSubscriptionImpl.this) {

                /* Check if subscription is canceled */
                checkCanceled();

                /* add the table to the subscribed table list */
                parentPU.addTable(tableImpl, streamTxn);

                /* unset expiration time if stream is not empty */
                parentPU.unsetExpireTimeMs();

                /* signal user */
                subscriberOnChangeResult(subscriber,
                                         getCurrentPosition(),
                                         null);

                /* start stream writes from new table */
                parentPU.updateCachedTable(StreamChangeReq.Type.ADD, tableImpl);
            }
        }

        /* remove table */
        private void processRemove(TableImpl tableImpl) {
            synchronized (NoSQLSubscriptionImpl.this) {

                /* Check if subscription is canceled */
                checkCanceled();

                /* stop stream writes from the table */
                parentPU.updateCachedTable(StreamChangeReq.Type.REMOVE,
                                           tableImpl);

                /* remove the table from the subscribed table list */
                parentPU.removeTable(tableImpl);

                /* set expiration time if stream is empty */
                parentPU.setExpireTimeMs();

                /* remove the table from the table metadata manager */
                parentPU.getTableMDManager().removeTable(tableImpl);

                logger.info(lm("Removed table=" + tableName +
                               "(id=" + tableImpl.getId() + ") from " +
                               "parent pu"));

                /* signal user */
                subscriberOnChangeResult(subscriber, getCurrentPosition(),
                                         null);
            }
        }

        private void checkCanceled() {
            if (isCanceled() || parentPU.isClosed()) {
                final String err = "Subscription is already canceled " +
                                   "or shutdown.";
                logger.info(lm(err));
                throw new SubscriptionChangeNotAppliedException(
                    getSubscriberId(), SUBSCRIPTION_CANCELED, err);
            }
        }

        @Override
        protected int initiateSoftShutdown() {
            final boolean alreadySet = shutdownDone(logger);
            logger.fine(() -> lm("Signal change worker to shutdown, " +
                                 "already signalled? " + alreadySet +
                                 ", wait for time(ms)=" +
                                 KVStoreConfig.DEFAULT_REQUEST_TIMEOUT +
                                 " to let it exit"));
            return KVStoreConfig.DEFAULT_REQUEST_TIMEOUT;
        }

        /**
         * @return a logger to use when logging uncaught exceptions.
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    public enum StreamChangeStatus {

        /* stream change applied successfully */
        OK,

        /* stream change not applicable, no effect */
        NOT_APPLICABLE,

        /*
         * stream change timeout in one or more shards, may want to retry
         */
        TIMEOUT,

        /*
         * Fail to stream change timeout in one or more shards, stream will
         * terminate because the feeder state is inconsistent
         */
        FAIL
    }

    /**
     * A KV thread factory that logs if a thread exits on an unexpected
     * exception.
     */
    private class NoSQLSubscriptionThreadFactory extends KVThreadFactory {
        NoSQLSubscriptionThreadFactory() {
            super("NoSQLSubscriptionThreadFactory", logger);
        }

        @Override
        public Thread.UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return (thread, ex) ->
                logger.warning(lm("NoSQL Subscription thread=" +
                                  thread.getName() +
                                  "(id=" + ThreadUtils.threadId(thread) + ")" +
                                  " exits unexpectedly, error=" + ex +
                                  ", subscriber id=" + getSubscriberId() +
                                  ", tables=" + getSubscribedTables() +
                                  ", current position=" + getCurrentPosition() +
                                  ", last checkpoint" + getLastCheckpoint() +
                                  ", metrics=" + getSubscriptionMetrics() +
                                  "\nstack=" + LoggerUtils.getStackTrace(ex)));
        }
    }
}
