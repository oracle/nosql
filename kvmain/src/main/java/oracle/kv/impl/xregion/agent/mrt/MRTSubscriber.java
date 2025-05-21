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

package oracle.kv.impl.xregion.agent.mrt;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.JsonDefImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.impl.api.table.TablePath.StepInfo;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.pubsub.PublishingUnit;
import oracle.kv.impl.pubsub.StreamSequenceId;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.agent.BaseRegionAgentSubscriber;
import oracle.kv.impl.xregion.agent.RegionAgentThread;
import oracle.kv.impl.xregion.agent.TargetTableEvolveException;
import oracle.kv.impl.xregion.service.JsonConfig;
import oracle.kv.impl.xregion.service.MRTableMetrics;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.ShardTimeoutException;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.pubsub.SubscriptionInsufficientLogException;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.WriteOptions;

import com.sleepycat.je.dbi.TTL;

import org.reactivestreams.Subscription;

/**
 * Object that represents the subscriber of region agent for multi-region
 * table.
 */
public class MRTSubscriber extends BaseRegionAgentSubscriber {

    /**
     * A test hook that can be used to inject exceptions after async put or
     * delete calls. Called with the stream operation that produced the call.
     * For testing.
     */
    public static volatile ExceptionTestHook<StreamOperation, Exception>
        asyncExceptionHook;

    /**
     * A test hook that can be used by test to make expired put operations
     */
    public static volatile TestHook<Row> expiredPutHook = null;

    /** wait before retry in ms */
    private static final long WAIT_BEFORE_RETRY_MS = 1000;

    /** rate limit logger sampling interval */
    private static final int RL_INTV_MS = 10 * 60 * 1000;

    /** rate limit logger max number of objects */
    private static final int RL_MAX_OBJ = 1024;

    /** metadata management */
    private final ServiceMDMan mdMan;

    /** instance of a table api to target store */
    private final TableAPIImpl tgtTableAPI;

    /** The name of the target store. */
    private final String tgtStoreName;

    /** source region name */
    private final String srcRegion;

    /*
     * TODO: Provide a way to make sure that operations have been sync'ed to
     * disk before creating a checkpoint when the operation durability is less
     * than COMMIT_SYNC.
     */
    /** Write options to persist writes to target region. */
    private final WriteOptions writeOptions;

    /**
     * A scheduled executor service for retrying operations after fault
     * exceptions and to do checkpoints as needed when there is no activity.
     */
    private final ScheduledExecutorService executor =
        Executors.newScheduledThreadPool(1, new MRTSubscriberThreadFactory());

    /**
     * An array of queues of StreamOperations, with array elements indexed by
     * the hash of the operation's primary key modulo the number of queues.
     * These queues are used to make sure that operations on the same key are
     * performed in order.
     *
     * Since Java guarantees that access to an immutable object stored in a
     * final field is thread safe after construction, access to the array
     * elements is safe because they are only set in the constructor. But that
     * guarantee is not extended to the contents of the array elements, so
     * synchronize on each element when accessing it. All elements are of type
     * {@code Queue<StreamOperations>}, but Java doesn't allow parameterized
     * types for array elements.
     */
    final Queue<?>[] pendingOperations;

    /** Whether the subscriber has been shutdown. */
    private volatile boolean shutdown;

    /** # of concurrent stream ops */
    private final int numConcurrentStreamOps;

    /** rate limiting logger */
    private final RateLimitingLogger<String> rlLogger;

    /** Subscriber checkpoint manager */
    private final MRTSubscriberCkptMan ckptMan;

    /** Testhook for CRDT initialization test. The argument is the Row ID. */
    public static com.sleepycat.je.utilint.TestHook<Integer>
        crdtInitializationHook = null;

    public MRTSubscriber(RegionAgentThread agentThread,
                         NoSQLSubscriptionConfig config,
                         KVStore tgtKVS,
                         Logger logger) {
        super(agentThread, config, logger);

        srcRegion = agentThread.getSourceRegion().getName();
        mdMan = agentThread.getMdMan();
        writeOptions =
            new WriteOptions(mdMan.getJsonConf().durabilitySetting(), 0,
                             MILLISECONDS)
                .setUpdateTTL(true); /* allow update TTL */
        tgtTableAPI = ((KVStoreImpl) tgtKVS).getTableAPIImpl();
        tgtStoreName = ((KVStoreImpl) tgtKVS).getTopology().getKVStoreName();
        numConcurrentStreamOps = agentThread.getNumConcurrentStreamOps();
        pendingOperations = new Queue<?>[numConcurrentStreamOps];
        Arrays.setAll(pendingOperations, i -> new ArrayDeque<>());
        rlLogger = new RateLimitingLogger<>(RL_INTV_MS, RL_MAX_OBJ, logger);
        ckptMan = new MRTSubscriberCkptMan(this,
                                           config.getSubscriberId(),
                                           parent.getSourceRegion(),
                                           agentThread.getCkptIntvNumOps(),
                                           agentThread.getCkptIntvSecs(),
                                           parent.getMdMan(),
                                           logger);
        logger.info(lm("Subscriber created with config=" + config));
    }

    boolean shutdownRequested() {
        return shutdown;
    }

    /**
     * Unit test only
     * @return internal stream checkpoint manager
     */
    public MRTSubscriberCkptMan getCkptMan() {
        return ckptMan;
    }

    @Override
    public void onSubscribe(Subscription s) {
        final NoSQLSubscriptionImpl active = getActiveSubscription();
        if (active != null) {
            /*
             * There is an active subscription within this subscriber.
             *
             * Per Rule Subscriber Rule 5 in reactive streams spec 1.0.3,
             * a Subscriber must call Subscription.cancel() on the given
             * Subscription after an onSubscribe signal if it already has an
             * active Subscription.
             *
             * https://github.com/reactive-streams/reactive-streams-jvm/blob/master/RELEASE-NOTES.md
             *
             * Note MR table implementation does not allow re-subscribe an
             * existing subscriber, therefore normally we should not reach
             * here. It is a protection of future code change.
             */
            if (!active.isCanceled()) {
                logger.info(lm(
                    "Has an active subscription of tables=" +
                    active.getSubscribedTables() +
                    ", cancel the new subscription of tables=" +
                    ((NoSQLSubscriptionImpl) s).getSubscribedTables()));
                s.cancel();
                return;
            }
        }

        /* activate stream */
        super.onSubscribe(s);
        ckptMan.start();
        final NoSQLSubscriptionImpl subImpl =
            (NoSQLSubscriptionImpl) getSubscription();
        logger.fine(() -> lm("Streaming tables=" + config.getTables() +
                             " from shards=" + subImpl.getCoveredShards() +
                             " from position=" + subImpl.getInitPos()));
    }

    /** Schedule an operation, handling exceptions and shutdowns */
    private void schedule(Runnable task) {
        try {
            executor.schedule(
                () -> {
                    if (shutdown) {
                        return;
                    }
                    try {
                        task.run();
                    } catch (Throwable t) {
                        /* Handle unexpected exceptions */
                        operationFailed(t);
                    }
                },
                WAIT_BEFORE_RETRY_MS, MILLISECONDS);
        } catch (RejectedExecutionException e) {
            if (!shutdown) {
                throw e;
            }
        }
    }

    @Override
    public void onNext(StreamOperation source) {

        /* add table metrics if not existing */
        final Row row = getRowFromStreamOp(source);
        final String tableName = row.getTable().getFullNamespaceName();
        final Table tb = getTargetTable(source);
        if (tb == null) {
            final String err = "Table=" + tableName + " is removed from stream";
            rlLogger.log(err, Level.INFO, lm(err));
            return;
        }
        /* update last message time */
        getMetrics().setLastMsgTime(source.getRepGroupId(),
                                    System.currentTimeMillis());

        /* generate checkpoint candidate if needed */
        if (parent.isStreamCkptEnabled()) {
            ckptMan.queueCkptIfNeeded();
        }

        /*
         * Record the operation in the queue, checking first to see if the
         * queue was empty
         */
        final Queue<StreamOperation> queue = getQueue(source);
        final boolean wasEmpty;
        synchronized (queue) {
            wasEmpty = queue.isEmpty();
            queue.add(source);
        }

        /* Wait if the operation is not at the head of the queue */
        if (!wasEmpty) {
            getMetrics().incrNumOpsQueued();
            logger.finest(() -> lm("Queued operation=" + source));
            return;
        }

        processOperation(source, tb, 1);
    }

    /** Process a stream operation that is at the head of the queue. */
    private void processOperation(StreamOperation source,
                                  Table tgtTable,
                                  int attempts) {

        final StreamOperation.Type type = source.getType();
        final Row srcRow = getRowFromStreamOp(source);
        final Row tgtRow;
        try {
            tgtRow = transformRow(srcRow, type, tgtTable, false);
            if (tgtRow == null) {
                /* Couldn't process operation, so operation is done */
                operationCompleted(source);
                return;
            }
        } catch (FaultException fe) {
            /* store may be down, retry, wait */
            final String tableName = getTableName(srcRow);
            final String msg = "Cannot write to table=" + tableName +
                               " at target store=" + tgtStoreName +
                               " after # of attempts=" + attempts +
                               ", " + fe;
            /* avoid repeated warnings for same table and exception class */
            rlLogger.log(tableName + fe.getFaultClassName(),
                         Level.WARNING, lm(msg));

            scheduleRetry(source, tgtTable, attempts);
            /* Operation is not done, so don't request another operation */
            return;
        }

        /* persistence */
        writeTarget(source, tgtRow, attempts);
    }

    /**
     * Schedule a retry, but skipping the operation if it is associated with a
     * pending checkpoint and is already active.
     */
    private void scheduleRetry(StreamOperation source,
                               Table tgtTb,
                               int attempts) {
        /*
         * TODO: processOperation will also transform the row again. We might
         * want to optimize that out later in cases where it isn't needed.
         */
        schedule(() -> processOperation(source, tgtTb, attempts + 1));
    }

    private String getTableName(Row row) {
        if (row == null) {
            return "";
        }
        return row.getTable().getFullNamespaceName();
    }

    /**
     * Return the queue for the specified stream operation. Use the same queue
     * for operations associated with the same key to insure the proper
     * ordering of operations.
     */
    private Queue<StreamOperation> getQueue(StreamOperation op) {
        return getQueue(getKeyQueueIndex(getPKey(op), numConcurrentStreamOps));
    }

    /**
     * Gets primary key from stream operation
     * @param op stream operation
     * @return primary key
     */
    private static PrimaryKey getPKey(StreamOperation op) {
        return (op.getType() == StreamOperation.Type.DELETE) ?
            op.asDelete().getPrimaryKey() :
            op.asPut().getRow().createPrimaryKey();
    }

    /** Return the queue for the specified index. */
    private Queue<StreamOperation> getQueue(int index) {
        @SuppressWarnings("unchecked")
        final Queue<StreamOperation> queue =
            (Queue<StreamOperation>) pendingOperations[index];
        return queue;
    }

    /**
     * Computes the index of the queue used for the specified key. This method
     * is public for use in tests.
     *
     * @param key the key
     * @param numConcurrentStreamOps number of concurrent stream operations
     * @return the index of the queue for the key
     */
    private static int getKeyQueueIndex(PrimaryKey key,
                                        int numConcurrentStreamOps) {

        /*
         * Compute the index of the queue associated with the key by hashing
         * the key and taking the value modulo the number of queues. Note that
         * Math.floorMod is like the '%' modulus operator, but, for a positive
         * modulus, always returns a value between 0 and mod-1 inclusive.
         */
        final int hash = key.hashCode();
        return Math.floorMod(hash, numConcurrentStreamOps);
    }

    /**
     * Unit test only
     */
    public static int getKeyQueueIndex(PrimaryKey key) {
        return getKeyQueueIndex(key,
                                JsonConfig.DEFAULT_MAX_CONCURRENT_STREAM_OPS);
    }

    /**
     * Log an unexpected exception that occurs during an operation and that
     * means that the subscription should be canceled.
     */
    private void operationFailed(Throwable t) {
        logger.warning(lm("Cannot process operation, error=" + t +
                          ", call stack\n" +
                          LoggerUtils.getStackTrace(t)));
        final Subscription subscription = getSubscription();
        if (subscription != null) {
            subscription.cancel();
        }
    }

    /**
     * Call this method when a stream operation has been completed, so we can
     * request more and possibly run another queued operation.
     */
    private void operationCompleted(StreamOperation op) {
        logger.finest(() -> lm("Operation completed=" + op));

        /* update the high water mark */
        final int qid = getKeyQueueIndex(getPKey(op), numConcurrentStreamOps);
        final int shardId = op.getRepGroupId();
        final long vlsn = ((StreamSequenceId) op.getSequenceId()).getSequence();
        ckptMan.updateHWM(qid, shardId, vlsn);

        /* process the next operation with available target table */
        StreamOperation next = op;
        Table tgtTable = null;
        while (true) {
            /* peek the next op */
            next = popAndPeekNext(next, qid);
            if (next == null) {
                /* queue is empty */
                break;
            }

            /* get target table for next operation */
            tgtTable = getTargetTable(next);
            if (tgtTable != null) {
                /* target table is available, break and process the op */
                break;
            }
            /* continue if tgtTable is null, meaning the table was dropped */
        }

        /* Process next operation in the queue, if any. */
        if (next != null) {
            processOperation(next, tgtTable, 1);
        }

        /* Regardless, request a new operation to replace the completed one */
        getSubscription().request(1);
    }

    private void processSILE() {
        parent.initFromRegion();
    }

    @Override
    public void onError(Throwable t) {

        /* remember the cause */
        cause = t;
        if (t instanceof SubscriptionInsufficientLogException) {
            /*
             * Stream failed or cannot be created because of insufficient log,
             * agent to initialize all tables from that region
             */
            logger.warning(lm("Agent to initialize region=" + srcRegion +
                              " because of error=" + t));
            processSILE();
            return;
        }
        if (t instanceof PublishingUnit.EmptyStreamExpireException) {
            final PublishingUnit.EmptyStreamExpireException ese =
                (PublishingUnit.EmptyStreamExpireException) t;
            logger.info(lm("stream (sid=" + ese.getSid() + ") expires " +
                           "because it is empty for time(secs)=" +
                           getSubscriptionConfig().getEmptyStreamSecs() +
                           " no restart."));
            return;
        }

        /* all subscribed MR tables */
        final Set<String> tbs = parent.getTgtTableNames();
        final NoSQLSubscription subscription = getSubscription();
        if (subscription == null) {
            logger.log(Level.WARNING,
                       lm("Stream is not created, agent to create " +
                          "a new one for table=" + tbs + ", " + getError(t)),
                       logger.isLoggable(Level.FINE) ? t : null);
        } else {
            logger.log(Level.WARNING,
                       lm("Running stream shut down due to error, " +
                          "agent to create a new one for tables " + tbs +
                          ", " + getError(t)),
                       logger.isLoggable(Level.FINE) ? t : null);
        }

        try {
            /* retry with the configured stream mode */
            final NoSQLStreamMode mode = config.getStreamMode();
            if (RegionAgentThread.getInitTableStreamMode().equals(mode)) {
                logger.info(lm("To re-initialize from region=" + srcRegion));
                parent.initFromRegion();
                return;
            }
            logger.info(lm("To resume the stream from checkpoint"));
            parent.createStream(tbs.toArray(new String[]{}));
        } catch (InterruptedException e) {
            if (!parent.isShutdownRequested()) {
                logger.warning(lm("interrupted in queuing a msg to agent to " +
                                  "create a new stream"));
            }
        }
    }

    @Override
    public void onWarn(Throwable t) {
        String warning = t.getMessage();
        if (t instanceof ShardTimeoutException) {
            warning += ", last message time=" +
                       formatTime(((ShardTimeoutException) t).getLastMsgTime());
        } else {
            warning += ", stack:\n" + LoggerUtils.getStackTrace(t);
        }
        logger.warning(lm(warning));
    }

    @Override
    public void onChangeResult(StreamPosition pos, Throwable exp) {
        super.onChangeResult(pos, exp);
        if (exp != null) {
            logger.warning(lm("Fail to change the stream, error=" + exp +
                              ", cause=" + exp.getCause() +
                              (logger.isLoggable(Level.FINE) ?
                                  LoggerUtils.getStackTrace(exp) : "")));
            return;
        }
        logger.fine(() -> "Succeed to change the stream at position=" + pos);
    }

    @Override
    public void onCheckpointComplete(StreamPosition sp, Throwable failure) {

        /*
         * TODO: Consider storing the time and number of operations at the
         * start of the checkpoint, not the end, so that the time and number of
         * operations between checkpoints isn't extended by what happens while
         * the checkpoint is underway.
         */
        getMetrics().incrNumCheckpoints();
        if (failure == null) {
            ckptPos = sp;
            logger.info(lm("Done checkpoint at position=" + sp));
            /* remove stale checkpoint if necessary */
            ckptMan.dropStaleCkptTableIfPossible();
            return;
        }
        final Throwable exp = failure.getCause();
        logger.fine(() -> lm("Skip checkpoint at position=" + sp +
                             ", error=" + failure + ", cause=" + exp + "\n" +
                             LoggerUtils.getStackTrace(exp)));
    }

    /**
     * Shut down the retry executor.
     */
    @Override
    public void shutdown() {
        shutdown = true;
        final long ts = System.currentTimeMillis();
        executor.shutdown();
        ckptMan.shutdown();
        logger.info(lm("MRTable subscriber has shut down in time(ms)=" +
                       (System.currentTimeMillis() - ts)));
    }

    @Override
    public long getInitialRequestSize() {
        return numConcurrentStreamOps;
    }

    /**
     * Transforms the row from source region to a row in the target region.
     * The transformation includes schema reconciliation and region id
     * translation.
     *
     * @param srcRow row from source region
     * @param type   operation type
     * @param initialization set to true if it is for table transfer
     *        during initialization.
     * @return row of target region or null if the row cannot be transformed
     */
    Row transformRow(Row srcRow,
                     StreamOperation.Type type,
                     Table target,
                     boolean initialization) {

        /* get target table */
        if (tgtTableAPI == null) {
            throw new IllegalStateException("For MRT subscriber, the target " +
                                            "table API cannot be null");
        }

        final String tableName = getTableName(srcRow);
        if (target == null) {
            final String err = "Cannot convert row from source=" + srcRegion +
                               " because table=" + tableName +
                               " not found in target, in state=" +
                               (initialization ? "initialization" : "stream");
            rlLogger.log(err, Level.WARNING, lm(err));
            return null;
        }
        final long tid = ((TableImpl) target).getId();
        if (mdMan.isFromDroppedTable(tid)) {
            final String err = "No need to convert row from source=" +
                               srcRegion + " because table=" + tableName +
                               "(tid=" + tid + ")" +
                               " has dropped in local region";
            rlLogger.log(err, Level.INFO, lm(err));
            return null;
        }

        /* translate the region id */
        int srcRid;
        srcRid = ((RowImpl) srcRow).getRegionId();
        if (srcRid == Region.NULL_REGION_ID) {
            final String err = "Row may not be from a multi-region table, " +
                " from region=" + parent.getSourceRegion().getName();
            rlLogger.log(tableName + err, Level.WARNING, lm(err));
            logger.warning(lm(err));
            return null;
        }

        if (srcRid == Region.UNKNOWN_REGION_ID) {
            final String err = "Invalid region id =" + srcRid +
                               " from region=" +
                               parent.getSourceRegion().getName();
            rlLogger.log(tableName + err, Level.WARNING, lm(err));
            srcRid = Region.LOCAL_REGION_ID;
        }

        final Integer tgtRid = translate(parent.getSourceRegion(), srcRid,
                                         tableName, null);
        if (tgtRid == null) {
            /* Not valid region ID. */
            return null;
        }

        /*
         * JSON Collection tables need no translation. Translation is
         * to handle schema differences and JSON collections can have
         * no differences other than primary keys and if there's a key
         * mismatch the operation will fail. The region id must be set
         * as that is a side effect of convertRow() that is required.
         * The table also needs to be set to the target table because it
         * is used to set the table id sent to the server. Because there is
         * no schema, just keys, this is safe to do for JSON collection
         * tables
         */
        if (((TableImpl)target).isJsonCollection()) {
            if (!((TableImpl) srcRow.getTable()).isJsonCollection()) {
                throw new UnsupportedOperationException(
                    "Source table must be a JSON collection if the " +
                    "target is a JSON collection");
            }
            ((RowImpl)srcRow).setRegionId(tgtRid);
            ((RowImpl)srcRow).setTable((TableImpl)target);
            return srcRow;
        }

        /* convert to target row */
        final RowImpl tgtRow;
        try {
            tgtRow = (RowImpl) convertRow(srcRow, target, type, initialization,
                                          tgtRid);
            if (tgtRow == null) {
                return null;
            }
        } catch (IllegalArgumentException exp) {
            final String tbName = getTableName(srcRow);
            final String err =
                "Cannot convert row from table=" + tbName +
                ", source region=" + srcRegion + ", " +
                "skip writing to target, error=" + exp;
            rlLogger.log(err, Level.WARNING, lm(err));
            getMetrics().getTableMetrics(tbName).incrIncompatibleRows(1);
            return null;
        }

        /* set modification time */
        final long ts = srcRow.getLastModificationTime();
        if (ts == 0L) {
            /*
             * This can only happen if creating an MR table from an
             * existing table with a row that was last updated using
             * a release prior to 19.5, when modification time was
             * introduced.
             * TODO: maybe let this happen and set the mod time
             */
            final String msg =lm("Row from table=" + tableName +
                                 ", remote region=" +
                                 parent.getSourceRegion().getName() +
                                 " has no modification time" +
                                 ", key hash=" +
                                 srcRow.createPrimaryKey().hashCode() +
                                 ", version=" + srcRow.getVersion());
            logger.warning(msg);
            /* let it surface after warning */
            throw new UnsupportedOperationException(msg);
        }
        tgtRow.setModificationTime(ts);

        /*
         * Set expiration time. Expired put will be put in the same way that
         * non-expired key is put, and will expire instantly
         */
        if (StreamOperation.Type.PUT.equals(type)) {
            /* unit test only: expire puts if set */
            assert TestHookExecute.doHookIfSet(expiredPutHook, srcRow);
            tgtRow.setExpirationTime(srcRow.getExpirationTime());
        }

        return tgtRow;
    }

    /**
     * Returns the configured write options for write operations on the local
     * store.
     */
    WriteOptions getWriteOptions() {
        return writeOptions;
    }

    /* private functions */
    private String lm(String msg) {
        return "[MRTSubscriber-" + srcRegion + "-" +
               config.getSubscriberId() + "] " + msg;
    }

    /**
     * Translates the region id from streamed operation to localized region id.
     *
     * @param region region the op streamed from
     * @param srcRid    region in streamed op
     * @param tableName name of table
     * @param CRDTColName CRDT column name
     * @return localized region id, null for invalid region id.=
     */
    private Integer translate(RegionInfo region,
                              int srcRid,
                              String tableName,
                              String CRDTColName) {
        /* Testhook for crdt initialization test. */
        if (crdtInitializationHook != null) {
            crdtInitializationHook.doHook(srcRid);
            return crdtInitializationHook.getHookValue();
        }

        if (mdMan == null) {
            throw new IllegalStateException("Metadata man is not ready");
        }
        int tgtRid = mdMan.translate(region.getName(), srcRid);

        if (rlLogger.getInternalLogger().getLevel() == Level.FINE) {
            final String msg = "table=" + tableName +
                (CRDTColName == null ? "" : (" CRDT column = " + CRDTColName)) +
                ", src=" + parent.getSourceRegion().getName() + ", src id=" +
                srcRid + " -> tgt id=" + tgtRid;
            rlLogger.log(msg, Level.FINE, () -> lm(msg));
        }

        if (tgtRid == Region.UNKNOWN_REGION_ID ||
            tgtRid == Region.NULL_REGION_ID) {
            final String err = "Cannot translate source region id=" + srcRid +
                               " from source region=" +
                               parent.getSourceRegion().getName();
            rlLogger.log(tableName + err, Level.WARNING, lm(err));
            return null;
        }
        return tgtRid;
    }

    /**
     * Convert the row to target table and reconciles schema of source table to
     * that of target table. Also, set the translated region id.
     *
     * @param srcRow row from source region
     * @param type   operation type
     * @param initialization if transferring a table during initializing
     * @return the row of target region or null if there was a problem
     * converting a CRDT field
     * @throws IllegalArgumentException if there was another mismatch
     */
    private Row convertRow(Row srcRow,
                           Table tgtTable,
                           StreamOperation.Type type,
                           boolean initialization,
                           int regionId) {

        if (mdMan.isPKeyMismatch(srcRow.getTable(), tgtTable)) {
            final String err =
                "Mismatch primary keys," +
                "src table=" + srcRow.getTable().getFullNamespaceName() +
                ", while tgt table=" + tgtTable.getFullNamespaceName();
            rlLogger.log(err, Level.WARNING, lm(err));
            throw new IllegalArgumentException("Primary key does not match");
        }

        Row tgtRow = null;

        if (StreamOperation.Type.PUT.equals(type)) {
            /* convert source row to json */
            final String json = srcRow.toJsonString(false);
            /* convert json to target table row and reconcile the difference */
            tgtRow = tgtTable.createRowFromJson(json, false);
            /* if there is CRDT, extra conversion */
            if (convertCRDT((RowImpl)srcRow, (RowImpl)tgtRow) == null) {
                return null;
            }
        } else if (StreamOperation.Type.DELETE.equals(type)) {
            final PrimaryKey pkey;
            final boolean isTombstone = ((RowImpl) srcRow).isTombstone();
            if (isTombstone) {
                /* a tombstone row from table iterator "*/
                pkey = srcRow.createPrimaryKey();
            } else {
                /* a deletion from stream */
                pkey = (PrimaryKey) srcRow;
            }
            /* convert source pkey to json */
            final String json = pkey.toJsonString(false);
            /* convert json to key and reconcile the difference */
            tgtRow = tgtTable.createPrimaryKeyFromJson(
                json, false/* no exact match */);
        }

        /* Set region ID. */
        if (tgtRow != null) {
            /*
             * Turn off ignoring local originated row because when the
             * local table is recreated, we cannot tell a row from remote
             * region is originated from the recreated table (in which case
             * it should be ignored), or from the previously created table
             * with the same name (in which case it should not be ignored),
             * merely rom the region id in the streamed row. Therefore, we
             * would just have to push all such streamed rows to local table.
             */

            /* set new region id */
           ((RowImpl) tgtRow).setRegionId(regionId);
           /* log loopback rows, may need a separate stat */
           if (regionId == Region.LOCAL_REGION_ID) {
               final String tb = tgtRow.getTable().getFullNamespaceName();
               final String msg = "Loopback rows from table=" + tb + " in " +
                                  (initialization ? "transfer" :  "streaming");
               rlLogger.log(tb, Level.INFO, lm(msg));
           }

           return tgtRow;
        }

        throw new IllegalStateException("Unsupported type=" + type);
    }

    /**
     * Check if the row contains a CRDT column and convert it if it does. For
     * each CRDT, copy each counter from source and translate its region id to
     * localized region id.
     *
     * @param srcRow source row
     * @param tgtRow row in target region format before CRDT conversion
     * @return true if column contains a CRDT, false if it does not, and null
     * if there was a problem converting a CRDT field
     * @throws IllegalArgumentException if there is a disagreement between the
     * source and target about whether a field is a CRDT, or if there is some
     * other mismatch between source and target
     */
    private Boolean convertCRDT(RowImpl srcRow, RowImpl tgtRow) {
        final String table = tgtRow.getTable().getFullNamespaceName();
        boolean hasCRDT = false;
        int numFields = tgtRow.getNumFields();
        for (int fpos = 0; fpos < numFields; ++fpos) {

            String fname = tgtRow.getFieldName(fpos);
            FieldDefImpl fdef = tgtRow.getFieldDef(fpos);
            FieldValueImpl fval = tgtRow.get(fpos);

            if (fval == null || fval.isNull()) {
                continue;
            }

            /* check if source row has a column with the field name */
            final FieldValueImpl srcVal;
            try {
                srcVal = srcRow.get(fname);
                if (srcVal == null) {
                    /* no column with this name in source row, ignore */
                    continue;
                }
            } catch (IllegalArgumentException e) {
                /* the column does not exist in the source row, ignore */
                continue;
            }


            FieldDefImpl srcDef = srcRow.getFieldDef(fname);
            if (!fdef.isMRCounter() && !fdef.hasJsonMRCounter()) {
                /* This column is not a CRDT in the target row.*/
                if (srcVal.isMRCounter()) {
                    /*
                     * If the column of the source row is a CRDT,
                     * the row is incompatible.
                     */
                    final String err =
                        "Incompatible rows in table " + table + ": Column " +
                        fname + " is an MR_Counter in the source row but not " +
                        "in the target row";
                    rlLogger.log(err, Level.WARNING, lm(err));
                    throw new IllegalArgumentException(err);
                }

                if (srcDef.hasJsonMRCounter()) {
                    final String err =
                        "Incompatible rows in table " + table + ": Column " +
                        fname + " contains MR_Counters in the source row but " +
                        "not in the target row";
                    rlLogger.log(err, Level.WARNING, lm(err));
                    throw new IllegalArgumentException(err);
                }
                continue;
            }
            FieldValueImpl newVal;
            hasCRDT = true;

            if (fdef.isMRCounter()) {
                if (!srcVal.isMRCounter()) {
                    String err =
                        "Incompatible rows in table " + table + ": Column " +
                            fname + " has type " + srcDef.getDDLString() +
                            " in the source row, but in the target row its type is " +
                            fdef.getDDLString();
                    rlLogger.log(err, Level.WARNING, lm(err));
                    throw new IllegalArgumentException(err);
                }
                newVal = translateCRDT(srcVal, fdef, tgtRow.getTable(), fname);
            } else {
                JsonDefImpl jdef = (JsonDefImpl)fdef;
                Set<String> tgtMrcounterFields =
                    jdef.allMRCounterFieldsInternal().keySet();

                if (!srcDef.hasJsonMRCounter() ||
                    !((JsonDefImpl)srcDef).allMRCounterFieldsInternal().
                    keySet().equals(tgtMrcounterFields)) {

                    String err =
                        "Incompatible rows in table " + table + ": Column " +
                            fname + " has type " + srcDef.getDDLString() +
                            " in the source row, but in the target row its type is " +
                            fdef.getDDLString();
                    rlLogger.log(err, Level.WARNING, lm(err));
                    throw new IllegalArgumentException(err);
                }

                newVal = translateJsonCRDTList(fval.asMap(),
                                               fdef,
                                               srcVal.asMap(),
                                               srcDef,
                                               tgtRow.getTable(),
                                               fname);
            }
            if (newVal == null) {
                return null;
            }
            /* finish copy, add to target row */
            tgtRow.putInternal(fname, newVal, false /*fromUser*/);
        }

        return hasCRDT;
    }

    private FieldValueImpl translateJsonCRDTList(MapValue tgtMap,
                                                 FieldDef tgtDef,
                                                 MapValue srcMap,
                                                 FieldDef srcDef,
                                                 TableImpl table,
                                                 String fieldName) {
        /*
         * Clone the map value with the right element field def for
         * Json with counter CRDTs.
         */
        MapValueImpl newVal = (MapValueImpl)tgtDef.createMap();
        for (Map.Entry<String, FieldValue> entry :
            tgtMap.getFields().entrySet()) {
            newVal.put(entry.getKey(), entry.getValue().clone());
        }

        /* Iterate over all json CRDT fields in the source Json. */
        for (String s : ((JsonDefImpl)srcDef).allMRCounterFields().keySet()) {
            List<StepInfo> list = TablePath.parsePathName(table, s);
            FieldValueImpl srcVal = ((FieldValueImpl)srcMap).
                evaluateScalarPath(list, 0);
            if (!srcVal.isEMPTY()) {
                /*
                 * This json CRDT field exists in the source json,
                 * so translate it.
                 */
                Type tgtType = ((JsonDefImpl)tgtDef).allMRCounterFields().get(s);
                FieldValueImpl newCounter =
                    translateCRDT(srcVal,
                                  FieldDefImpl.getCRDTDef(tgtType),
                                  table, fieldName);
                if (newCounter == null) {
                    return null;
                }
                JsonDefImpl.insertMRCounterField(newVal, list, newCounter);
            }
        }
        return newVal;

    }

    private FieldValueImpl translateCRDT(FieldValueImpl srcVal,
                                         FieldDefImpl tgtDef,
                                         TableImpl table,
                                         String fieldName) {
        /* copy and translate CRDT */
        srcVal = srcVal.castToOtherMRCounter(tgtDef);

        /* copy counter and translate each region id in map */
        final FieldValueImpl newVal = tgtDef.createCRDTValue();
        for (Map.Entry<Integer, FieldValueImpl> entry :
            srcVal.getMRCounterMap().entrySet()) {

            Integer regionId = entry.getKey();
            FieldValueImpl count = entry.getValue();
            /* ensure the region is valid. */
            if (regionId == Region.NULL_REGION_ID) {
                final String err =
                    "CRDT from table " + table.getFullNamespaceName() +
                    " has an entry with regionId=" + Region.NULL_REGION_ID;
                logger.warning(lm(err));
                throw new IllegalStateException(err);
            }
            /* negative region id represents the decrement counter */
            final int sign = regionId >= 0 ? 1 : -1;
            final Integer newId = translate(parent.getSourceRegion(),
                                            Math.abs(regionId),
                                            table.getFullNamespaceName(),
                                            fieldName);
            if (newId == null) {
                /* unable to translate a region id, incompatible row */
                final String err =
                    "Cannot translate regionId " + regionId +
                    " in CRDT from table " + table.getFullNamespaceName() +
                    ", incompatible row";
                logger.warning(lm(err));
                return null;
            }

            newVal.putMRCounterEntry(sign * newId, count);
        }
        return newVal;
    }

    private static String formatTime(long time) {
        return FormatUtils.formatDateTime(time);
    }

    private void writeTarget(StreamOperation source,
                             Row tgtRow,
                             int attempts) {

        logger.finest(() -> lm("Write target=" + source + " #attempts=" +
                               attempts));

        final CompletableFuture<Boolean> future;
        if (source.getType().equals(StreamOperation.Type.PUT)) {
            /* a put with optional expiration time */
            future = tgtTableAPI.putResolveAsync(tgtRow, null, writeOptions);
        } else {
            /* a delete */
            future = tgtTableAPI.deleteResolveAsync(
                (PrimaryKey) tgtRow, null, writeOptions);
        }

        /* TODO: Use dialog layer executor */
        future.whenCompleteAsync(
            unwrapExceptionVoid(
                (srcWin, ex) -> {
                    try {
                        putDelResolveComplete(
                            source, tgtRow, (srcWin != null) && srcWin,
                            ex, attempts);
                    } catch (Throwable t) {
                        /* Handle unexpected exceptions */
                        operationFailed(t);
                    }
                }));
    }

    private void putDelResolveComplete(StreamOperation source,
                                       Row tgtRow,
                                       boolean srcWin,
                                       Throwable exception,
                                       int attempts) {
        try {
            assert ExceptionTestHookExecute.doHookIfSet(asyncExceptionHook,
                                                        source);
        } catch (Exception e) {
            exception = e;
        }
        final Throwable ex = exception;
        logger.finest(() -> lm("putDelResolveComplete" +
                               " source=" + source +
                               " srcWin=" + srcWin +
                               " exception=" + ex));
        final Row srcRow = getRowFromStreamOp(source);
        final String tbName = getTableName(srcRow);
        if (ex instanceof FaultException) {
            final String fault = ((FaultException) ex).getFaultClassName();
            if (TargetTableEvolveException.class.getName().equals(fault)) {
                /* local table has evolved, refresh table */
                final String tableName = getTableName(tgtRow);
                final TableImpl tb = (TableImpl) tgtRow.getTable();
                final int oldVer = tb.getTableVersion();
                final Table refresh = waitForRefreshStream(tb);
                if (refresh == null) {
                    final String err =
                        "Table=" + ServiceMDMan.getTrace(tb) + " is removed " +
                        "from target table list, no retry";
                    logger.info(lm(err));
                    return;
                }
                final int newVer = refresh.getTableVersion();
                final String err = "On exception=" + fault +
                                   ", local table=" + tableName +
                                   " has refreshed from ver=" + oldVer +
                                   " to ver=" + newVer + ", will retry using" +
                                   " refreshed table";
                rlLogger.log(err, Level.INFO, lm(err));
                /*
                 * Try again immediately, which includes transforming the row
                 * again
                 */
                processOperation(source, refresh, attempts + 1);
                return;
            }

            /* store may be down, retry, wait */
            final FaultException fe = (FaultException) ex;
            final String msg = "Cannot write to table=" + tbName +
                               " at target store=" + tgtStoreName +
                               " after # of attempts=" + attempts +
                               ", will retry, error=" + fe;
            rlLogger.log(tbName + fe.getFaultClassName(), Level.WARNING,
                         lm(msg));
            scheduleRetry(source, tgtRow.getTable(), attempts);
            /* This operation is not done, so don't request another operation */
            return;
        }

        if (ex instanceof MetadataNotFoundException) {
            /*
             * TODO: What if the table was newly added and we've gotten this
             * exception from an RN that hasn't heard about it yet?
             */
            /* table dropped, shall not retry  */
            final MetadataNotFoundException mnfe =
                (MetadataNotFoundException) ex;
            final TableImpl tb = (TableImpl) tgtRow.getTable();
            mnfeHandler(source, tb, mnfe);
            return;
        }

        if (ex instanceof WrappedClientException) {
            final Throwable reason = ex.getCause();
            if (reason instanceof MetadataNotFoundException) {
                final MetadataNotFoundException mnfe =
                    (MetadataNotFoundException) reason;
                final TableImpl tb = (TableImpl) tgtRow.getTable();
                mnfeHandler(source, tb, mnfe);
                return;
            }
        }

        if (ex != null) {
            final String msg = "Cannot write to table=" + tbName +
                               " at target store=" + tgtStoreName +
                               " after # of attempts=" + attempts +
                               ", will cancel stream, error=" + ex +
                               ", stack:" + LoggerUtils.getStackTrace(ex);
            rlLogger.log(tbName + ex.getClass().getName(), Level.WARNING,
                         lm(msg));
            ((NoSQLSubscriptionImpl) getSubscription()).cancel(ex);
            return;
        }
        logger.finest(() -> lm("Write to table=" + tbName +
                               " at target store=" + tgtStoreName +
                               " after # of attempts=" + attempts));

        final StreamOperation.Type type = source.getType();
        if (type.equals(StreamOperation.Type.PUT)) {
            /* for debugging only, shall be the lowest level */
            logger.finest(() -> lm("Pushed PUT, key hash=" +
                                   tgtRow.createPrimaryKey()
                                         .toJsonString(false).hashCode() +
                                   ", src region id=" +
                                   ((RowImpl) srcRow).getRegionId() +
                                   ", translated tgt region id=" +
                                   ((RowImpl) tgtRow).getRegionId() +
                                   ", source win=" + srcWin));
        } else {
            /* for debugging only, shall be the lowest level */
            logger.finest(() -> lm("Pushed DEL key hash=" +
                                   tgtRow.toJsonString(false).hashCode() +
                                   ", region id=" +
                                   ((RowImpl) tgtRow).getRegionId() +
                                   ", source win=" + srcWin));
        }

        /* stats update */
        /* storageSize is available from streaming */
        final long sz = ((RowImpl) srcRow).getStorageSize();
        final MRTableMetrics tbm = getMetrics().getTableMetrics(tbName);
        tbm.incrStreamBytes(sz);
        if (type.equals(StreamOperation.Type.PUT)) {
            tbm.incrPuts(1);
            if (TTL.isExpired(srcRow.getExpirationTime())) {
                tbm.incrExpiredPuts(1);
            }
        } else {
            tbm.incrDels(1);
        }

        if (srcWin) {
            //TODO: Using source row size as estimate for target row for now.
            // Use RowImpl.storageSize when it is available
            tbm.incrPersistStreamBytes(sz);
            if (type.equals(StreamOperation.Type.PUT)) {
                tbm.incrWinPuts(1);
            } else {
                tbm.incrWinDels(1);
            }
        }

        /* update stat */
        final int shardId = source.getRepGroupId();
        final long ts = srcRow.getLastModificationTime();
        getMetrics().setLastModTime(shardId, ts);
        final long latency = Math.max(0, System.currentTimeMillis() - ts);
        getMetrics().addLatency(shardId, latency);
        operationCompleted(source);
    }

    /**
     * A KV thread factory that logs and cancels the subscription if a thread
     * gets an unexpected exception.
     */
    private class MRTSubscriberThreadFactory extends KVThreadFactory {
        MRTSubscriberThreadFactory() {
            super("MRTSubscriberThread", logger);
        }
        @Override
        public UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return (thread, ex) -> operationFailed(ex);
        }
    }

    /**
     * Gets a rate limiting logger
     */
    RateLimitingLogger<String> getRlLogger() {
        return rlLogger;
    }

    private String getError(Throwable t) {
        if (t == null) {
            return "NA";
        }
        return "error=" + exceptionString(t);
    }

    /*
     * Stat reference may change if interval stat is collected, thus get
     * the reference from parent instead of keeping a cached reference
     */
    MRTAgentMetrics getMetrics() {
        return (MRTAgentMetrics) parent.getMetrics();
    }

    /**
     * Deal with table not found exception
     */
    private void mnfeHandler(StreamOperation source,
                             TableImpl tb,
                             MetadataNotFoundException mnfe) {
        /* table not found, complete the op */
        operationCompleted(source);

        final long tid = tb.getId();
        final String tbName = tb.getFullNamespaceName();
        final String msg = "Table=" + ServiceMDMan.getTrace(tb) +
                           " dropped, error=" + mnfe;
        rlLogger.log(msg, Level.INFO, lm(msg));
        mdMan.addDroppedTable(tbName, tid);
    }

    /**
     * Wait for target table list sees a table higher than current version
     * @param curr  current table
     * @return table instance of new table version
     */
    private Table waitForRefreshStream(TableImpl curr) {
        final String tableName = curr.getFullNamespaceName();
        final long oldVer = curr.getTableVersion();
        while (!shutdownRequested()) {
            final Table refresh = parent.getTgtTable(tableName);
            if (refresh == null) {
                /* table has been removed from target table list */
                return null;
            }
            if (refresh.getTableVersion() > oldVer) {
                /* table has refreshed */
                return refresh;
            }
            synchronized (this) {
                try {
                    final String err =
                        "Streaming waiting for table=" + tableName +
                        " to refresh from version=" + oldVer;
                    rlLogger.log(err, Level.INFO, lm(err));
                    wait(WAIT_BEFORE_RETRY_MS);
                } catch (InterruptedException e) {
                    logger.fine(() -> "Interrupted in waiting for refresh " +
                                      "table=" + tableName +
                                      ", old version=" + oldVer);
                    return null;
                }
            }
        }
        logger.info(lm("Shutdown requested, stop waiting for refreshed " +
                       "table, current=" + ServiceMDMan.getTrace(curr) +
                       " for streaming, old version=" + oldVer));
        return null;
    }

    /**
     * Returns the corresponding target table from stream operation from
     * source region. Returns null if the op is null, or if the table has been
     * removed or dropped.
     */
    private Table getTargetTable(StreamOperation op) {
        final Row srcRow = getRowFromStreamOp(op);
        final String tb = srcRow.getTable().getFullNamespaceName();
        final Table tgtTable = parent.getTgtTable(tb);
        if (tgtTable == null) {
            /* the table has been removed from the region or dropped */
            return null;
        }

        if (!tb.equals(tgtTable.getFullNamespaceName())) {
            throw new IllegalArgumentException(
                "Source table name=" + tb + " does not match target table " +
                "name=" + tgtTable.getFullNamespaceName());
        }
        return tgtTable;
    }

    /**
     * Gets the row or primary key from stream operation
     */
    private Row getRowFromStreamOp(StreamOperation op) {
        final StreamOperation.Type type = op.getType();
        if (type.equals(StreamOperation.Type.PUT)) {
            return op.asPut().getRow();
        } else if (type.equals(StreamOperation.Type.DELETE)){
            return op.asDelete().getPrimaryKey();
        }
        throw new IllegalArgumentException(
            "Only PUT or DELETE is supported, invalid type " + type);
    }

    /**
     * Pop out the current op and peek the next one in the queue
     * @param curr    current operation
     * @param qid     id of queue
     * @return next operation in the queue, or null if queue is empty after
     * popping out the current one
     */
    private StreamOperation popAndPeekNext(StreamOperation curr, int qid) {
        final Queue<StreamOperation> queue = getQueue(qid);
        final StreamOperation next;
        synchronized (queue) {
            final StreamOperation top = queue.poll();
            if (!curr.equals(top)) {
                throw new IllegalStateException(
                    "Expected top of queue=" + curr + ", found=" + top);
            }
            next = queue.peek();
        }
        return next;
    }
}
