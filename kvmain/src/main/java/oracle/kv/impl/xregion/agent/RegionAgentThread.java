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

package oracle.kv.impl.xregion.agent;

import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;
import static oracle.kv.impl.xregion.service.XRegionRequest.NULL_REQUEST_ID;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.COMPLETE;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.NOT_START;
import static oracle.kv.pubsub.NoSQLStreamMode.FROM_NOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.pubsub.CheckpointTableManager;
import oracle.kv.impl.pubsub.NoSQLSubscriptionImpl;
import oracle.kv.impl.pubsub.PublishingUnit;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.Pair;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.agent.mrt.MRTAgentMetrics;
import oracle.kv.impl.xregion.agent.mrt.MRTSubscriber;
import oracle.kv.impl.xregion.agent.mrt.MRTSubscriberCkptMan;
import oracle.kv.impl.xregion.agent.mrt.MRTSubscriberCkptMan.StreamCheckpointName;
import oracle.kv.impl.xregion.agent.mrt.MRTTableTransferThread;
import oracle.kv.impl.xregion.agent.pitr.PITRAgentMetrics;
import oracle.kv.impl.xregion.agent.pitr.PITRSubscriber;
import oracle.kv.impl.xregion.init.TableInitCheckpoint;
import oracle.kv.impl.xregion.service.MRTableMetrics;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ReqRespManager;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.service.StatusUpdater;
import oracle.kv.impl.xregion.service.XRegionRespHandlerThread;
import oracle.kv.impl.xregion.service.XRegionService;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.kv.pubsub.NoSQLPublisher;
import oracle.kv.pubsub.NoSQLPublisherConfig;
import oracle.kv.pubsub.NoSQLStreamMode;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.NoSQLSubscription;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.PublisherFailureException;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.utilint.StoppableThread;


/**
 * Object serves as the agent for a source region and a target region. It is
 * responsible for
 * - streaming subscribed tables from the source region;
 * - writing streamed data to target region;
 * - initializing tables in the target region from the source region;
 * - managing underlying stream from the source region;
 * - changing parameters of subscribed tables;
 * - statistics collection, etc
 */
public class RegionAgentThread extends StoppableThread {

    /**
     * A test hook to compute index for table initialization, If non-null, the
     * value of the init index supplied by computeTableInitIndex.
     */
    public static volatile Integer testTableInitIndex = null;

    /**
     * A test hook to pause the execution
     */
    public static volatile TestHook<RegionAgentReq> pauseHook = null;

    /**
     * hook to simulate stream change failure w/ parameter 1) number of times
     * the agent has retried, and 2) name of the table being processed
     */
    public static volatile
        ExceptionTestHook<Pair<Long, String>, RuntimeException>
        changeStreamHook = null;

    /**
     * Used in test only. True if to simulate a timeout exception in table
     * transfer, false otherwise
     */
    public static volatile boolean throwSimulatedTimeoutException = false;
    /**
     * Used in test only. True if a simulated timeout exception is thrown,
     * false otherwise;
     */
    public static volatile boolean simulatedExceptionThrown = false;

    /** default wait time to check table to accommodate stale table md */
    private static final int DEFAULT_REMOTE_TABLE_TIMEOUT_MS = 10 * 1000;

    /** rate limiting log period in ms */
    private static final int RL_LOG_PERIOD_MS = 30 * 1000;

    /** empty stream expires after all tables removed from the stream */
    private static final int DEFAULT_EMPTY_STREAM_TIMEOUT_SECS = 60 * 60;

    /** default stream change timeout in ms */
    private static final int DEFAULT_STREAM_CHANGE_TIMEOUT_MS = 10 * 60 * 1000;

    /** sleep in ms before reconnect if remote store is unreachable  */
    private static final int SLEEP_MS_BEFORE_RECONNECT = 10 * 1000;

    /** polling internal in ms */
    private static final int POLL_INTERVAL_MS = 1000;

    /** timeout in last ckpt */
    //TODO: configuring this value
    private static final int LAST_CKPT_TIMEOUT_MS = 1000;

    /** default collector */
    private static final Collector<CharSequence, ?, String> DEFAULT_COLL =
        Collectors.joining(",", "[", "]");

    /** max number of message in the queue */
    private static final int MAX_MSG_QUEUE_SIZE = 1024;

    /** timeout in ms to poll message queue */
    private static final int MSG_QUEUE_POLL_TIMEOUT_MS = 1000;

    /** timeout in ms to put message queue */
    private static final int MSG_QUEUE_PUT_TIMEOUT_MS = 1000;

    /** prefix of region id */
    private static final String ID_PREFIX = "RA";

    /**
     * wait time in ms during soft shutdown, given enough time for
     * checkpointing, read/write message queue, and read/write stream ops,
     * with the minimal 60 seconds
     */
    private static final int MIN_SOFT_SHUTDOWN_WAIT_MS = 60 * 1000;
    private static final int SOFT_SHUTDOWN_WAIT_MS =
        Math.max(MIN_SOFT_SHUTDOWN_WAIT_MS,
                 10 * LAST_CKPT_TIMEOUT_MS + DEFAULT_REMOTE_TABLE_TIMEOUT_MS +
                 SLEEP_MS_BEFORE_RECONNECT + MSG_QUEUE_POLL_TIMEOUT_MS +
                 MSG_QUEUE_PUT_TIMEOUT_MS);

    /** default stream mode to start from checkpoint */
    private final static NoSQLStreamMode DEFAULT_STREAM_MODE =
        NoSQLStreamMode.FROM_EXACT_CHECKPOINT;

    /** special request to notify the agent to shut down */
    private static final RegionAgentReq SHUTDOWN_REQ = new ShutDownRequest();

    /** indefinite retry to execute a request */
    private static final long MAX_REQUEST_RETRY = Long.MAX_VALUE;

    /** stream mode if init is needed to start from now */
    private static final NoSQLStreamMode INIT_TABLE_STREAM_MODE = FROM_NOW;

    /**
     * Extra timeout percentage. This adds extra room in computing change
     * timeout in waiting for a stream change request, in order to avoid
     * timing out prematurely
     */
    private static final int EXTRA_CHANGE_TIMEOUT_PERCENT = 10;

    /** id of the region agent */
    private final String agentId;

    /** private logger */
    private final Logger logger;

    /** metadata manager */
    private final ServiceMDMan mdMan;

    /** FIFO queue of messages of actions */
    private final BlockingQueue<RegionAgentReq> msgQueue;

    /** agent configuration */
    private final RegionAgentConfig config;

    /** agent statistics */
    private final AtomicReference<BaseRegionAgentMetrics> metrics;

    /** executor for scheduled task */
    private final ScheduledExecutorService ec;

    /** internal publisher in the agent */
    private NoSQLPublisher publisher;

    /** internal subscriber */
    private MRTSubscriber subscriber;

    /** status updater */
    private final StatusUpdater statusUpd;

    /** true if shut down the agent */
    private volatile boolean shutdownRequested;

    /** status of the agent */
    private volatile RegionAgentStatus status;

    /** stored cause of failure */
    private volatile Throwable storedCause;

    /** rate limiting logger */
    private final RateLimitingLogger<String> rlLogger;

    /** table polling thread from source region */
    private final TablePollingThread tablePollingThread;

    /** get remote table timeout */
    private volatile int remoteTableTimeoutMs;

    /** currently processed request from kvstore */
    private volatile RegionAgentReq currentReq;

    /** true if the agent needs to initialize all tables from the region */
    private final AtomicBoolean initRegion;

    /** true if retry currently running request */
    private final AtomicBoolean retryRequest;

    /** number of attempts to execute a request */
    private final AtomicLong numRetry;

    /**
     * Target tables used to convert source row to target row. Indexed by
     * table name because from the source row, we can only get the table name
     * associated with the row.
     * <p>
     * The map is created when the subscriber is created, and updated table
     * requests are queued
     */
    private final ConcurrentMap<String, Table> tgtTables;

    /**
     * Map indexed by table id to store the table in pending table evolve
     * request. A table in evolve request will be stored when the request is
     * queued, and will be removed from map when the evolve request completes
     * execution. This is to prevent the deadlock between the table transfer
     * thread and agent thread when a table evolves at target store when the
     * agent is doing table initialization.
     */
    private final ConcurrentMap<Long, Table> pendingEvolveTable;

    /**
     * Creates an instance of change subscriber
     *
     * @param config    stream configuration
     * @param mdMan     metadata manager
     * @param statusUpd status updater
     * @param logger    logger
     * @throws PublisherFailureException if fail to create the publisher
     */
    public RegionAgentThread(RegionAgentConfig config,
                             ServiceMDMan mdMan,
                             StatusUpdater statusUpd,
                             Logger logger)
        throws PublisherFailureException {

        super("RAThread" + config.getSource().getName());
        this.mdMan = mdMan;
        this.config = config;
        this.statusUpd = statusUpd;
        this.logger = logger;
        rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS, 1024, logger);

        storedCause = null;
        final RegionAgentThreadFactory factory = new RegionAgentThreadFactory();
        ec = Executors.newSingleThreadScheduledExecutor(factory);
        shutdownRequested = false;

        final String src = config.getSource().getName();
        agentId = ID_PREFIX + config.getSubscriberId() + "." +
                  config.getType() + "." + src;

        final BaseRegionAgentMetrics st =
            createMetrics(config.getType(),
                          config.getSource().getName(),
                          config.getTarget().getName());
        metrics = new AtomicReference<>(st);
        subscriber = null;
        msgQueue = new ArrayBlockingQueue<>(MAX_MSG_QUEUE_SIZE);
        tablePollingThread = new TablePollingThread(this, logger);
        remoteTableTimeoutMs = DEFAULT_REMOTE_TABLE_TIMEOUT_MS;
        status = RegionAgentStatus.IDLE;
        currentReq = null;
        initRegion = new AtomicBoolean(false);
        retryRequest = new AtomicBoolean(false);
        tgtTables = new ConcurrentHashMap<>();
        pendingEvolveTable = new ConcurrentHashMap<>();
        numRetry = new AtomicLong(0);
        logger.fine(() -> lm("Start agent=" + agentId +
                             " for table=" +
                             Arrays.toString(config.getTables()) +
                             " with mode=" + config.getStartMode()));
    }

    /* Public APIs */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected int initiateSoftShutdown() {
        logger.info(lm("Signal thread " + getName() + " to shutdown" +
                       ", wait up to time(ms)=" + SOFT_SHUTDOWN_WAIT_MS +
                       " to exit"));
        return SOFT_SHUTDOWN_WAIT_MS;
    }

    @Override
    public void run() {

        logger.info(lm("Region agent starts, mode=" + config.getStartMode() +
                       ", local writes only=" + config.isLocalWritesOnly() +
                       ", tables=" + Arrays.toString(config.getTables())));

        try {
            initializeAgent();

            while (!isShutdownRequested()) {

                /* get next request to process */
                final RegionAgentReq req = getNextRequest();
                if (req == null) {
                    logger.finest(() -> lm("Unable to dequeue request for " +
                                           "time(ms)=" +
                                           MSG_QUEUE_POLL_TIMEOUT_MS));
                    continue;
                }

                if (req instanceof ShutDownRequest || isShutdownRequested()) {
                    logger.info(lm("Shutdown requested, will close the agent"));
                    break;
                }

                /* unit test: pause execution if set */
                assert TestHookExecute.doHookIfSet(pauseHook, req);

                final long ts = System.currentTimeMillis();
                logger.info(lm(reqPrefix(req) +
                               "Processing request, type=" + req.getReqType() +
                               ", tables=" +
                               ServiceMDMan.getTrace(req.getTableMD()) +
                               ", #numRetry=" + numRetry +
                               ", agent status=" + status));
                /* record with local table id before processing request */
                recordLocalTableIdIfNeeded(req);
                /* update stats entries */
                updateStatsEntries(req);
                /* update target table list */
                updateTargetTable(req);
                switch (req.reqType) {

                    case STREAM:
                        createStreamFromSource(req);
                        break;

                    case INITIALIZE_FROM_REGION:
                        initializeRegion(req);
                        break;

                    case INITIALIZE_TABLES:
                        initializeTables(req);
                        break;

                    case ADD:
                        addTablesToStream(req);
                        break;

                    case REMOVE:
                        removeTablesFromStream(req);
                        break;

                    case EVOLVE:
                        updateTables(req);
                        break;

                    default:
                        final String err = "Unsupported request:" +
                                           ", type=" + req.reqType +
                                           ", tables=" + req.getTables();
                        logger.warning(lm(reqPrefix(req) + err));
                        throw new IllegalStateException(err);
                }
                logger.info(lm(reqPrefix(req) + "Done processing request" +
                               ", will retry=" + retryRequest.get() +
                               ", elapsed(ms)=" +
                               (System.currentTimeMillis() - ts) +
                               ", agent status=" + status));
            }
        } catch (TransferTableException tte) {
            storedCause = tte;
            String error = "Shut down agent because fail to transfer table=" +
                           tte.getFailed() + ", completed=" +
                           tte.getTransferred() + ", remaining=" +
                           tte.getRemaining();
            if (tte.getCause() != null) {
                final Throwable cause = tte.getCause();
                error += ", cause=" + cause +
                         (logger.isLoggable(Level.FINE) ?
                             LoggerUtils.getStackTrace(cause) : "");
            }
            logger.warning(lm(error));
        } catch (InterruptedException ie) {
            if (!isShutdownRequested()) {
                storedCause = ie;
                logger.warning(lm("Interrupted with status=" + status +
                                  ", agent exits"));
            }
        } catch (Exception exp) {
            storedCause = exp;
            logger.warning(lm("Shut down agent due to error=" + exp +
                              ", status=" + status +
                              ", call stack\n" +
                              LoggerUtils.getStackTrace(exp)));
        } finally {
            close();
            logger.info(lm("Agent exits"));
        }
    }

    public static NoSQLStreamMode getInitTableStreamMode() {
        return INIT_TABLE_STREAM_MODE;
    }

    /**
     * Returns true if shut down is requested, false otherwise
     */
    public boolean isShutdownRequested() {
        return shutdownRequested;
    }

    /**
     * Returns metadata manager
     *
     * @return metadata manager
     */
    public ServiceMDMan getMdMan() {
        return mdMan;
    }

    /**
     * Stops the agent thread from another thread without error
     */
    public void shutDown() {

        if (shutdownRequested) {
            logger.fine(() -> lm("Agent is in shutdown or already shut " +
                                 "down, current stream status=" + status));
            return;
        }
        shutdownRequested = true;

        final long ts = System.currentTimeMillis();
        closePendingRequests();
        logger.info(lm("All pending requests are closed in time(ms)=" +
                       (System.currentTimeMillis() - ts)));

        if (subscriber != null) {
            subscriber.shutdown();
        }

        /* shutdown table polling threads */
        if (tablePollingThread != null) {
            tablePollingThread.shutdown();
        }

        /* notify main loop to shut down */
        final boolean succ = msgQueue.offer(SHUTDOWN_REQ);
        if (!succ) {
            logger.info(lm("Fail to enqueue the shut down request"));
        }
        synchronized (msgQueue) {
            msgQueue.notifyAll();
        }

        shutdownThread(logger);
        logger.info(lm("Agent of region=" + getSourceRegion().getName() +
                       " has shut down in time(ms)=" +
                       (System.currentTimeMillis() - ts)));
    }

    /**
     * Returns region agent id
     *
     * @return region agent id
     */
    public String getAgentId() {
        return agentId;
    }

    /**
     * Returns the agent subscriber id
     * @return the agent subscriber id
     */
    public NoSQLSubscriberId getAgentSubscriberId() {
        return config.getSubscriberId();
    }

    /**
     * Returns agent status
     *
     * @return agent status
     */
    public RegionAgentStatus getStatus() {
        return status;
    }

    /**
     * Returns the subscriber
     *
     * @return the subscriber
     */
    public MRTSubscriber getSubscriber() {
        return subscriber;
    }

    /**
     * Gets the metrics of underlying stream
     *
     * @return the metrics of underlying stream
     */
    public BaseRegionAgentMetrics getMetrics() {
        return metrics.get();
    }

    /**
     * Gets the metrics of underlying stream, and refresh if required.
     * @return the metrics of underlying stream
     */
    public BaseRegionAgentMetrics getMetricsRefresh() {
        synchronized (metrics) {
            /* refresh the stat and return the old one */
            final MRTAgentMetrics stat = getRefresh(metrics.get());
            /* plug in refreshed stat atomically */
            return metrics.getAndSet(stat);
        }
    }

    /**
     * Returns a refreshed metrics built from this one. This is used in
     * reporting interval stat. After report, the region stat will be
     * refreshed in new cycle. This method is only public for use in unit
     * testing. Otherwise, this method should only be called if the
     * RegionAgentThread lock is held.
     *
     * @return a refreshed metrics
     */
    public static MRTAgentMetrics getRefresh(BaseRegionAgentMetrics agentSt) {
        final String sourceRegion = agentSt.getSourceRegion();
        final String targetRegion = agentSt.getTargetRegion();
        final MRTAgentMetrics refreshed =
            new MRTAgentMetrics(sourceRegion, targetRegion);
        for (String s : agentSt.getTables()) {
            /* create a new table in new stats */
            final MRTableMetrics oldTM = agentSt.getTableMetrics(s);
            final MRTableMetrics newTM = new MRTableMetrics(targetRegion, s,
                                                            oldTM.getTableId());
            final Map<String, TableInitStat> stat = oldTM.getInitialization();
            for (String region : stat.keySet()) {
                /* create init statistics */
                final TableInitStat tis = newTM.getRegionInitStat(region);

                /* carry the transfer start,  end time, and state */
                final TableInitStat oldTis = stat.get(region);
                tis.setTransferStartMs(oldTis.getTransferStartMs());
                tis.setTransferCompleteMs(oldTis.getTransferCompleteMs());
                tis.setState(oldTis.getState());
            }

            /* add it to refreshed metrics */
            refreshed.addTable(newTM);
        }
        return refreshed;
    }

    /**
     * Returns table metrics, create one if not exist. Synchronized with the
     * stats refresh in {@link #getMetricsRefresh()}
     * @param tbl table name
     * @param tid local table id
     * @return table metrics
     */
    private MRTableMetrics getAddTable(String tbl, long tid) {
        final String region = config.getTarget().getName();
        synchronized (metrics) {
            final MRTableMetrics tm = metrics.get().getTableMetrics(tbl);
            /* verify by name and id */
            if (tm != null && tm.getTableName().equals(tbl) &&
                tm.getTableId() == tid) {
                return tm;
            }
            /* a new table */
            final MRTableMetrics ret = new MRTableMetrics(region, tbl, tid);
            metrics.get().addTable(ret);
            return ret;
        }
    }

    /**
     * Returns the cause of region agent failure
     *
     * @return the cause of region agent failure
     */
    public Throwable getCauseOfFaillure() {
        return storedCause;
    }

    /**
     * Returns true if the agent has been canceled, false otherwise.
     *
     * @return true if the agent has been canceled, false otherwise.
     */
    public boolean isCanceled() {
        return RegionAgentStatus.CANCELED.equals(status);
    }

    /**
     * Gets set of tables which are currently in streaming, or null if the
     * agent has been canceled.
     *
     * @return tables in streaming, or null if agent is canceled.
     */
    public Set<String> getTables() {
        if (isCanceled()) {
            return null;
        }
        if (subscriber == null) {
            return null;
        }
        final NoSQLSubscriptionImpl subImpl =
            (NoSQLSubscriptionImpl) subscriber.getSubscription();
        if (subImpl == null) {
            return null;
        }
        return subImpl.getSubscribedTables();
    }

    /**
     * Adds a table to the internal stream and wait for result
     *
     * @param resp      response handler
     * @param tables    tables to add
     * @throws InterruptedException if interrupted during enqueue the message
     */
    public void addTable(XRegionRespHandlerThread resp, Set<Table> tables)
        throws InterruptedException {
        changeStream(ReqType.ADD, resp, tables);
    }

    /**
     * Removes a table from the internal stream and wait for result
     *
     * @param resp      response handler
     * @param tables    table to remove
     * @throws InterruptedException if interrupted during enqueue the message
     */
    public void removeTable(XRegionRespHandlerThread resp, Set<Table> tables)
        throws InterruptedException {
        changeStream(ReqType.REMOVE, resp, tables);
    }

    /**
     * Evolves tables. The table version bumps up without region changes
     *
     * @param resp      response handler
     * @param tables    tables to evolve
     * @throws InterruptedException if interrupted during enqueue the message
     */
    public void evolveTable(XRegionRespHandlerThread resp, Set<Table> tables)
        throws InterruptedException {
        enqueue(new EvolveTableRegionReq(resp, tables));
    }

    /**
     * Initializes given tables from the source region
     *
     * @throws InterruptedException if fail to enqueue the message
     */
    void initTables(String[] tables) throws InterruptedException {
        if (tables == null || tables.length == 0) {
            logger.info(lm("No table to initialize"));
            return;
        }
        enqueue(new RegionAgentReq(ReqType.INITIALIZE_TABLES,
                                   mdMan.fetchTables(tables)));
    }

    /**
     * Initializes all MR tables from the source region
     */
    public void initFromRegion() {
        initRegion.set(true);
    }

    /**
     * Creates a stream with given tables
     *
     * @param tables tables to stream
     * @throws InterruptedException if interrupted during enqueue
     */
    public void createStream(String[] tables) throws InterruptedException {
        enqueue(new RegionAgentReq(ReqType.STREAM, mdMan.fetchTables(tables)));
    }

    /**
     * Returns the source region from which to stream
     *
     * @return source region
     */
    public RegionInfo getSourceRegion() {
        return config.getSource();
    }

    /**
     * Returns the host region
     *
     * @return host region
     */
    public RegionInfo getHost() {
        return config.getHost();
    }

    /**
     * Unit test only
     * @return if cascading replication is turned on
     */
    public boolean isCascadingReplication() {
        return !config.isLocalWritesOnly();
    }

    /**
     * Returns the target region
     *
     * @return target region
     */
    RegionInfo getTargetRegion() {
        return config.getTarget();
    }

    /**
     * Gets the checkpoint interval in seconds
     *
     * @return the checkpoint interval in seconds
     */
    public long getCkptIntvSecs() {
        return config.getCkptIntvSecs();
    }

    /**
     * Gets the checkpoint interval in terms of number of streamed ops
     *
     * @return the checkpoint interval in terms of number of streamed ops
     */
    public long getCkptIntvNumOps() {
        return config.getCkptIntvNumOps();
    }

    /**
     * Gets number of concurrent stream ops
     * @return number of concurrent stream ops
     */
    public int getNumConcurrentStreamOps() {
        return config.getNumConcurrentStreamOps();
    }

    /**
     * Returns the set of shards in source store streamed by the region agent
     */
    public Set<RepGroupId> getCoveredShards() {
        final NoSQLSubscriptionImpl handle =
            ((NoSQLSubscriptionImpl) subscriber.getSubscription());
        return handle.getCoveredShards();
    }

    /**
     * Returns subscriber id of the agent
     * @return subscriber id of the agent
     */
    NoSQLSubscriberId getSid() {
        return config.getSubscriberId();
    }

    /**
     * Returns true if enable stream checkpoint, false otherwise
     *
     * @return true if enable stream checkpoint, false otherwise
     */
    public boolean isStreamCkptEnabled() {
        return config.isEnableCkptTable();
    }

    /**
     * Recreates a remote table, internally used when remote table is dropped
     * or recreated
     *
     * @param tableName name of recreated remote table
     * @param from component requesting to recreate table
     * @throws InterruptedException if interrupted during enqueue
     */
    synchronized void recreateRemoteTable(String tableName, String from)
        throws InterruptedException {
        /*
         * The remote table has gone, have to remove and re-add the
         * table. If the remote is recreated, the new table will eventually
         * be added back to the stream with a complete table copy. If the
         * table is not recreated at the mean time, the table will be added
         * to the polling thread. The agent will initialize it when it is
         * created at remote, till the remote region is removed from the MR
         * table.
         */
        try {
            final Table existTable = getTgtTable(tableName);
            /*
             * both polling thread and table copy thread can detect the remote
             * table is gone and submit recreate requests. Check to avoid
             * duplicate request.
             */
            if (existInternalReq(tableName, ReqType.REMOVE)) {
                logger.fine(() -> "Already exists a request to remove " +
                                  "table=" + tableName);
            } else {
                if (existTable == null) {
                    logger.warning(lm("Cannot find table md to remove " +
                                      "table=" + tableName + ", table might " +
                                      "be dropped at local region" +
                                       ", request from=" + from));
                } else {
                    logger.info(lm("Requested to remove table=" + tableName +
                                   " from=" + from));
                    removeTable(null, Collections.singleton(existTable));
                }
            }

            if (existInternalReq(tableName, ReqType.ADD)) {
                logger.fine(() -> "Already exists a request to add " +
                                  "table=" + tableName);
            } else {
                if (existTable == null) {
                    logger.warning(lm("Cannot find table md to add " +
                                      "table=" + tableName + ", table might " +
                                      "be dropped at local region" +
                                      ", request from=" + from));
                } else {
                    /* adds back the table to stream */
                    addTable(null/* no response*/,
                             Collections.singleton(existTable));
                    logger.info(lm("Requested to add table=" + tableName +
                                   " from=" + from));
                }
            }
        } catch (InterruptedException ie) {
            logger.warning(lm("Interrupted in re-adding table=" + tableName +
                              " from region=" + config.getSource().getName() +
                              " from=" + from));
            throw ie;
        }
    }

    /*--------------------*
     * Private functions  *
     *--------------------*/
    private String lm(String msg) {
        return "[RA-MRT-from-" + config.getSource().getName() + "-" +
               config.getSubscriberId() + "] " + msg;
    }

    /**
     * Creates a stream from source with the table specified in the request,
     * with the default stream mode. If the stream cannot be created, it
     * would turn to initialization.
     *
     * @param req request body
     */
    private void createStreamFromSource(RegionAgentReq req)
        throws InterruptedException {
        final Set<Table> tables = req.getTableMD();
        final XRegionRespHandlerThread resp = req.getResp();
        final String region = config.getSource().getName();
        final Pair<Set<Table>, Set<Table>> result = ensureTable(req, tables);
        final Set<Table> found = result.first();
        final Set<Table> inPolling = result.second();
        if (found.isEmpty()) {
            /* no table found at remote region, skip creating stream */
            handleNoTableFound(req, tables, inPolling);
            logger.info(lm(reqPrefix(req) + "No table found," +
                           "complete create stream request, " +
                           "type=" + req.getReqType()));
            return;
        }

        if (ServiceMDMan.getTbNames(tables)
                        .equals(ServiceMDMan.getTbNames(found))) {
            logger.info(lm(reqPrefix(req) +
                           "All tables found, will create streams for " +
                           "tables=" + ServiceMDMan.getTrace(found) +
                           " from region=" + region));
        } else {
            logger.info(lm(reqPrefix(req) + "Create stream from region=" +
                           config.getSource().getName() +
                           ", not all tables found in time(ms)=" +
                           remoteTableTimeoutMs +
                           ", will create streams for found tables=" +
                           ServiceMDMan.getTrace(found) +
                           ". For tables=" + ServiceMDMan.getTrace(inPolling) +
                           ", the agent will continue polling and start " +
                           "initialization once the table is found."));
        }

        /* start stream with default mode */
        statusUpd.post(getSourceRegion(), status,
                       ServiceMDMan.getTbNames(found));
        final NoSQLStreamMode mode = DEFAULT_STREAM_MODE;
        if (startStream(mode, found, req)) {
            status = RegionAgentStatus.STREAMING;
            /* resume initialization */
            final Set<String> tbs = resumeTableInitialization(req);
            /* successfully created the stream, no table in polling */
            if (inPolling.isEmpty()) {
                postSucc(req.getReqId(), resp);
            }
            /* dump trace */
            logger.fine(() -> lm(getStartStreamTrace(
                mode, ServiceMDMan.getTrace(found), tbs)));
            return;
        }

        /*
         * If we fail to create a stream from remote region, the error will be
         * processed in
         * {@link oracle.kv.pubsub.NoSQLSubscriber#onError(Throwable)}.
         */

        /* fail because of other reasons */
        postFail(req.getReqId(), resp, subscriber.getFailure());
    }

    private String getStartStreamTrace(NoSQLStreamMode mode,
                                       Set<String> found,
                                       Set<String> tbs) {
        final NoSQLSubscriptionImpl handle =
            ((NoSQLSubscriptionImpl) subscriber.getSubscription());
        final String startPos = getStartPos(handle);
        return "Stream started from region=" +
               config.getSource().getName() +
               ", shards=" + getCoveredShards() +
               ", tables=" + found + ", mode=" + mode +
               ", from position=" + startPos +
               ", resuming initialization for tables=" + tbs;
    }

    /**
     * Initializes all MR tables from source region. This usually happens when
     * the agent encounter insufficient log entry exception that the stream
     * cannot resume because the log entry has been cleaned at source. The
     * initialization will pull all MR tables in table metadata from the
     * source after reestablishes the stream with mode "now".
     * <p>
     * Note that this call may take long to return. After it returns, all MR
     * tables are initialized and the stream is recovered.
     *
     * @param req request body
     * @throws InterruptedException if interrupted in waiting
     */
    private void initializeRegion(RegionAgentReq req)
        throws InterruptedException {
        /* initialize all MR tables */
        logger.info(lm(reqPrefix(req) + "Start region initialization" +
                       ", tables=" + req.getTables()));
        initializeTablesFromSource(req);
        logger.info(lm(reqPrefix(req) + "Done region initialization"));
    }

    /**
     * Initializes the tables in request from the source region
     *
     * @param req request body
     * @throws InterruptedException if interrupted in waiting
     */
    private void initializeTables(RegionAgentReq req)
        throws InterruptedException {
        /* initialize MR tables in request */
        initializeTablesFromSource(req);
    }

    /**
     * Initializes MR tables from source.
     *
     * @param req    request body
     * @throws InterruptedException if interrupted in waiting
     */
    private synchronized void initializeTablesFromSource(RegionAgentReq req)
        throws InterruptedException {
        final Set<Table> tbs = req.getTableMD();
        statusUpd.post(getSourceRegion(), status, ServiceMDMan.getTbNames(tbs));

        if (tbs == null || tbs.isEmpty()) {
            logger.info(lm(reqPrefix(req) +
                           "No table to initialize, post succ"));
            postSucc(req.getReqId(), req.getResp());
            return;
        }

        /* reset table init checkpoints */
        final Set<Table> tables = resetInitCkpt(tbs);
        if (tables.isEmpty()) {
            logger.info(lm(reqPrefix(req) +
                           "Cannot find any table=" +
                           ServiceMDMan.getTrace(tbs) +
                           " in local region, skip initialization"));
            return;
        }
        logger.info(lm(reqPrefix(req) + "Reset init checkpoint for tables=" +
                       ServiceMDMan.getTrace(tables)));

        /* ensure all tables are existing at source */
        final String region = config.getSource().getName();
        final Pair<Set<Table>, Set<Table>> result = ensureTable(req, tables);
        final Set<Table> found = result.first();
        final Set<Table> inPolling = result.second();
        if (found.isEmpty()) {
            handleNoTableFound(req, tables, inPolling);
            logger.info(lm(reqPrefix(req) + "No table found," +
                           "complete init table request, " +
                           "type=" + req.getReqType()));
            return;
        }

        if (ServiceMDMan.getTbNames(tables).equals(
            ServiceMDMan.getTbNames(found))) {
            logger.info(lm(reqPrefix(req) +
                           "All tables found, will start initialization " +
                           "for tables=" + ServiceMDMan.getTrace(found) +
                           " from region=" + region));
        } else {
            logger.info(lm(reqPrefix(req) + "Init tables from region=" +
                           config.getSource().getName() +
                           ", not all tables found in ms=" +
                           remoteTableTimeoutMs +
                           ", will start initializing tables=" +
                           ServiceMDMan.getTrace(found) +
                           ", to initialize by polling thread=" +
                           ServiceMDMan.getTrace(inPolling)));
        }

        /* cancel existing stream if any */
        cancelRunningStream(req);

        logger.info(lm(reqPrefix(req) + "Start initializing tables=" +
                       ServiceMDMan.getTrace(found)));

        /* create a new stream from now */
        final NoSQLStreamMode mode = INIT_TABLE_STREAM_MODE;
        if (!startStream(mode, found, req)) {
            final String err = "Initialization failed because it cannot " +
                               "create stream from " +
                               "region=" + config.getSource().getName() +
                               ", mode=" + mode +
                               ", tables=" + ServiceMDMan.getTrace(found);
            logger.warning(lm(reqPrefix(req) + err));
            postFail(req.getReqId(), req.getResp(),
                     new IllegalStateException(err));
            return;
        }

        logger.info(lm(reqPrefix(req) +
                       "Done creating stream for tables=" +
                       ServiceMDMan.getTrace(found)));
        /* start transfer tables */
        status = RegionAgentStatus.INITIALIZING_TABLES;
        try {
            transferTables(found, req);
            status = RegionAgentStatus.STREAMING;
            statusUpd.post(getSourceRegion(), status,
                           ServiceMDMan.getTbNames(found));
            if (tables.equals(found)) {
                postSucc(req.getReqId(), req.getResp());
            }
        } catch (TransferTableException tte) {
            processTransTableExp(req, tte);
        }
    }

    private void processTransTableExp(RegionAgentReq req,
                                      TransferTableException tte) {
        if (isShutdownRequested()) {
            logger.fine(() -> reqPrefix(req) +
                              "Table transfer aborted in shutdown, " +
                              ", failed=" + tte.getFailed() +
                              ", completed=" + tte.getTransferred() +
                              ", remaining=" + tte.getRemaining());
            return;
        }

        final Throwable exp = tte.getCause();
        final String err = reqPrefix(req) +
                           "Fail to transfer table=" + tte.getFailed() +
                           ", completed=" + tte.getTransferred() +
                           ", remaining=" + tte.getRemaining() +
                           ", error=" + exp;
        logger.warning(lm(err) + "\n" +
                       /* stack might already be dumped in transfer thread */
                       (logger.isLoggable(Level.FINE) ?
                           LoggerUtils.getStackTrace(exp) : ""));
        cancelRunningStream(req);
        postFail(req.getReqId(), req.getResp(), new IllegalStateException(err));

        /* let main loop capture it */
        throw tte;
    }

    /**
     * Cancels the current stream if it is running
     */
    private void cancelRunningStream(RegionAgentReq req) {
        final NoSQLSubscription current =
            (subscriber == null) ? null : subscriber.getSubscription();
        if (current != null && !current.isCanceled()) {
            current.cancel();
            logger.fine(() -> lm(reqPrefix(req) + "Current stream canceled"));
        }
    }

    /**
     * Adds a table to an existing stream if exists, or create a new stream
     * if not. Steps to add a new table to existing stream:
     * <p>
     * 1. wait till the table exists at source and ensure compatible schema;
     * 2. if the stream already exists, add the table into the stream;
     * 3. if the stream does not exist, create a new stream with mode "now";
     * 4. start transfer the table from source;
     * 5. wait till the transfer complete
     *
     * @param req request body
     * @throws InterruptedException if interrupted in waiting
     */
    private void addTablesToStream(RegionAgentReq req)
        throws InterruptedException {

        /* ensure table exist at source */
        final Set<Table> tables = req.getTableMD();
        final Pair<Set<Table>, Set<Table>> result = ensureTable(req, tables);
        final Set<Table> found = result.first();
        final Set<Table> inPolling = result.second();
        if (found.isEmpty()) {
            handleNoTableFound(req, tables, inPolling);
            logger.info(lm(reqPrefix(req) + "No table found," +
                           "complete adding table request, " +
                           "type=" + req.getReqType()));
            return;
        }

        /* table exists, adding table */
        status = RegionAgentStatus.ADDING_TABLES;
        statusUpd.post(getSourceRegion(), status,
                       ServiceMDMan.getTbNames(found));

        /* remote table id must have been recorded earlier */
        final String srcRegion = getSourceRegion().getName();
        if (!isStreamOn(req.getReqId())) {
            /*
             * Check if the stream will be restarted. If it is normal
             * cancellation or {@link MRTSubscriber#onError(Throwable)} is
             * not signalled, we can create a new stream here. If the stream is
             * canceled due to error, the
             * {@link MRTSubscriber#onError(Throwable)} would
             * recover from it by either initializing the region or restarting
             * the stream from the checkpoint, in this case, just fail the
             * request to change the stream since a new stream will be
             * created later.
             * */
            if (willStreamRestart()) {
                final String tbs = req.getTables().toString();
                final String msg =
                    "Cannot create new stream for tables=" + tbs +
                    ", previous stream will restart from checkpoint";
                final ChangeStreamException cse =
                    new ChangeStreamException(req.getReqId(), tbs, msg, null);
                logger.warning(lm(reqPrefix(req) + msg));
                postFail(req.getReqId(), req.getResp(), cse);
                return;
            }

            /* start a new stream */
            if (!startStream(INIT_TABLE_STREAM_MODE, found, req)) {
                final String err =
                    reqPrefix(req) +
                    "Cannot create stream with mode=" + INIT_TABLE_STREAM_MODE +
                    " from region=" + srcRegion +
                    ", tables=" + ServiceMDMan.getTrace(found);
                logger.warning(lm(err));
                postFail(req.getReqId(), req.getResp(),
                         new IllegalStateException(err));
                return;
            }
            logger.info(lm(reqPrefix(req) +
                           "Stream created for tables=" +
                           ServiceMDMan.getTrace(found) +
                           " with mode=" + INIT_TABLE_STREAM_MODE));
        } else {
            /* stream is on, just add each table to existing stream */
            try {
                for (Table t : found) {
                    final long ts = System.currentTimeMillis();
                    logger.info(lm(reqPrefix(req) +
                                   "Start adding table=" +
                                   ServiceMDMan.getTrace(t) + " to stream"));
                    final StreamPosition sp =
                        changeStream(req.getResp(), t, ReqType.ADD);
                    logger.info(lm(reqPrefix(req) +
                                   "Table=" +
                                   ServiceMDMan.getTrace(t) +
                                   " added to stream at pos=" + sp +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - ts)));
                }
            } catch (ChangeStreamException cse) {
                final String err = reqPrefix(req) +
                                   "Fail to add table=" + cse.getTable() +
                                   ", error=" + cse.getError();
                logger.warning(lm(err));
                if (!isStreamOn(req.getReqId())) {
                    logger.warning(lm(reqPrefix(req) + "Cannot change " +
                                      "stream that has shut down, err="+ cse));
                    postFail(req.getReqId(), req.getResp(), cse);
                    return;
                }
                if (needRetry()) {
                    retryCurrRequest();
                    logger.info(lm(reqPrefix(req) + "Set retry current " +
                                   "request, #numRetried=" + numRetry));
                } else {
                    /* have retried enough, give up */
                    logger.warning(lm(reqPrefix(req) + "Request has reached " +
                                      "max retry=" + MAX_REQUEST_RETRY));
                    postFail(req.getReqId(), req.getResp(), cse);
                }
                return;
            }
        }

        /*
         * Report succ after table are added to stream
         *
         * why post region success here instead of after transfer is done?
         * Today the main XRegionService serializes each request. It blocks
         * till adding table request is complete. If we post region succ after
         * table transfer is done. There are at least two problems, 1)
         * XRegionService wont be able to process any request to update the
         * pending evolve table list if the table evolves at local region,
         * and this causes deadlock if transfer sees table version mismatch
         * and waits for table instance to be refreshed; 2) Initialization
         * may take a very long time if the table is huge, posting region
         * succ after transfer of all tables are done will block the main
         * XRegionService for a long time, which seems unnecessary.
         */
        status = RegionAgentStatus.STREAMING;
        statusUpd.post(getSourceRegion(), status,
                       ServiceMDMan.getTbNames(found));
        postSucc(req.getReqId(), req.getResp());
        recordStreamTable(req.getReqId());
        logger.info(lm((reqPrefix(req) +
                        "Region succ, tables added to stream=" +
                        ServiceMDMan.getTrace(found))));

        /* skip the table already in polling thread or already complete */
        final long reqId = req.getReqId();
        final Set<Table> tbls = found.stream()
                                     .filter(t -> !transComplete(reqId, t))
                                     .collect(Collectors.toSet());
        if (tbls.isEmpty()) {
            return;
        }

        /* start transferring table */
        final Set<Table> toTrans = resetInitCkpt(tbls, false);
        logger.info(lm(reqPrefix(req) +
                       "To initialize=" + ServiceMDMan.getTrace(toTrans) +
                       " after reset checkpoint"));
        /* start transfer tables */
        try {
            transferTables(toTrans, req);
            status = RegionAgentStatus.STREAMING;
            statusUpd.post(getSourceRegion(), status,
                           ServiceMDMan.getTbNames(toTrans));
            logger.fine(() -> lm(reqPrefix(req) +
                                 "Transfer complete of tables=" +
                                 ServiceMDMan.getTrace(toTrans)));
        } catch (TransferTableException tte) {
            processTransTableExp(req, tte);
        }
    }

    /**
     * Removes tables from existing stream. If the table exists in stream,
     * remove it from the stream and the agent will no longer see the writes
     * from the table.
     *
     * @param req request body
     * @throws InterruptedException if interrupted in waiting
     */
    private void removeTablesFromStream(RegionAgentReq req)
        throws InterruptedException {

        final Set<String> tables = req.getTables();
        tables.forEach(t -> removeTableFromTgtList(req.getReqId(), t));
        status = RegionAgentStatus.REMOVING_TABLES;
        statusUpd.post(getSourceRegion(), status, tables);

        final String region = getSourceRegion().getName();
        for (String table : tables) {
            final long remoteTblId = mdMan.getRecordedTableId(table, region);
            if (remoteTblId == 0) {
                logger.info(lm(logPrefix(req.getReqId()) +
                               "No recorded remote id for table=" + table +
                               ", region=" + region));
                continue;
            }

            if (mdMan.removeRemoteTableId(table, region)) {
                logger.info(lm(logPrefix(req.getReqId()) +
                               "Deleted remote id=" + remoteTblId +
                               " for table=" + table +
                               " at region=" + region));
            } else {
                logger.warning(lm(logPrefix(req.getReqId()) +
                                  "Fail to delete remote id=" + remoteTblId +
                                  " for table=" + table +
                                  " at region=" + region));
            }
        }

        /* remove table in polling thread */
        final Set<Table> tblsMD = req.getTableMD();
        final Set<Table> inPolling =
            tblsMD.stream()
                  .filter(t -> {
                      final long tid = ((TableImpl) t).getId();
                      return tablePollingThread.inPolling(tid);
                  })
                  .collect(Collectors.toSet());
        if (!inPolling.isEmpty()) {
            tablePollingThread.removeTableFromCheckList(inPolling);
            logger.info(lm(reqPrefix(req) +
                           "Tables=" + ServiceMDMan.getTrace(inPolling) +
                           " removed from the polling thread"));
        }
        /* deal with tables in stream */
        if (!isStreamOn(req.getReqId())) {
            logger.fine(() -> lm(reqPrefix(req) +
                                 "Stream not on, no need to remove " +
                                 "tables from the stream, tables=" +
                                 ServiceMDMan.getTrace(tblsMD)));
            delInitCkpt(tables, req);
            postSucc(req.getReqId(), req.getResp());
            /* remove tables from the service metadata */
            tables.forEach(t -> mdMan.removeRemoteTables(region, t));
            return;
        }

        /* stream is on, just remove table from existing stream */
        try {
            for (Table t : tblsMD) {
                final long ts = System.currentTimeMillis();
                logger.info(lm(reqPrefix(req) +
                               "Start removing table=" +
                               ServiceMDMan.getTrace(t) + " to stream"));
                final StreamPosition sp =
                    changeStream(req.getResp(), t, ReqType.REMOVE);
                final MRTableMetrics tm =
                    getMetrics().getTableMetrics(t.getFullNamespaceName());
                logger.info(lm(reqPrefix(req) +
                               "Table=" + ServiceMDMan.getTrace(t) +
                               " removed from stream at " +
                               "pos=" + sp +
                               ", elapsedMs=" +
                               (System.currentTimeMillis() - ts) +
                               (logger.isLoggable(Level.FINE) ?
                                   ", final metrics=" + tm : "")));
            }
            status = RegionAgentStatus.STREAMING;
            statusUpd.post(getSourceRegion(), status, tables);
            postSucc(req.getReqId(), req.getResp());
            recordStreamTable(req.getReqId());
            logger.info(lm(reqPrefix(req) +
                           "Tables=" + ServiceMDMan.getTrace(tblsMD) +
                           " removed from stream" +
                           (inPolling.isEmpty() ? "" :
                               ", tables=" + ServiceMDMan.getTrace(inPolling) +
                               " removed from polling thread, ") +
                           ", post success"));
        } catch (ChangeStreamException cse) {
            final String err = reqPrefix(req) +
                               "Post failure to remove table=" +
                               cse.getTable() + ", reqId=" + cse.getReqId() +
                               ", error=" + cse.getError();
            logger.warning(lm(err));
            postFail(req.getReqId(), req.getResp(), cse);
        } finally {
            delInitCkpt(tables, req);
            tables.forEach(t -> mdMan.removeRemoteTables(region, t));
        }
    }

    /**
     * Update tables instances in target table list
     */
    private void updateTables(RegionAgentReq req) {
        removePendingEvolveTable(req);
        logger.info(lm(logPrefix(req.getReqId()) + "Region succ, tables=" +
                       ServiceMDMan.getTrace(req.getTableMD()) + " updated"));
        postSucc(req.getReqId(), req.getResp());
    }

    /**
     * Update the table instances and its child tables recursively
     * @param table table instance
     */
    void updateTableRecursive(long reqId, Table table) {
        addTableToTgtList(reqId, table);
        logger.fine(() -> lm(logPrefix(reqId) +
                             "Table=" + table.getFullNamespaceName() +
                             " updated in target table list"));
        if (table.getChildTables().isEmpty()) {
            return;
        }
        table.getChildTables().values()
             .forEach(v -> updateTableRecursive(reqId, v));
    }

    /**
     * Changes the stream by adding or removing tables
     *
     * @param reqType   request type
     * @param resp      response handler
     * @param tables    tables to add
     * @throws InterruptedException if interrupted in enqueueing the request
     */
    private void changeStream(ReqType reqType,
                              XRegionRespHandlerThread resp,
                              Set<Table> tables)
        throws InterruptedException {

        enqueue(new RegionAgentReq(reqType, resp, tables));
    }

    /**
     * Creates agent metrics based on the type
     *
     * @param type type of agent
     * @param srcRegion source region name
     * @param tgtRegion target region name
     * @return agent metrics
     */
    private BaseRegionAgentMetrics createMetrics(RegionAgentConfig.Type type,
                                                 String srcRegion,
                                                 String tgtRegion) {

        final BaseRegionAgentMetrics ret;
        switch (type) {
            case MRT:
                ret = new MRTAgentMetrics(srcRegion, tgtRegion);
                break;
            case PITR:
                ret = new PITRAgentMetrics(srcRegion);
                break;
            default:
                throw new IllegalStateException("Unsupported agent type " +
                                                type);
        }
        return ret;
    }

    /**
     * Does the last checkpoint and wait before cancel
     */
    private void doLastCkptWait() {
        final StreamPosition sp = getLastPersistedPosition();
        subscriber.getSubscription().doCheckpoint(sp);
        final boolean succ =
            new PollCondition(POLL_INTERVAL_MS, LAST_CKPT_TIMEOUT_MS) {
                @Override
                protected boolean condition() {
                    return sp.equals(subscriber.getLastCkpt());
                }
            }.await();

        if (!succ) {
            /* cannot do the last ckpt, log it and exit */
            final String err = "Timeout (" + LAST_CKPT_TIMEOUT_MS +
                               " ms) in last checkpoint=" + sp;
            logger.info(lm(err));
        }
    }

    /**
     * Gets the position of last persisted to target store
     *
     * @return the position of last persisted to target store
     */
    private StreamPosition getLastPersistedPosition() {
        //TODO: in V1 we use the current stream position from stream because
        //it is updated after onNext() and each call of onNext() would persist
        // one stream operation to local store. In post-V1, if we optimize the
        // onNext() to use the bulkPut, we should not use the current stream
        // position, instead, we should maintain a stream position of last
        // persisted operation and use that as checkpoint.

        final NoSQLSubscription stream = subscriber.getSubscription();
        return stream.getCurrentPosition();
    }

    /**
     * Shuts down the agent completely
     */
    private void close() {

        logger.fine(() -> lm("Start shutting down the agent"));
        status = RegionAgentStatus.CANCELED;
        statusUpd.post(getSourceRegion(), status, getTables());
        /* shutdown auxiliary threads */
        if (tablePollingThread != null) {
            tablePollingThread.shutdown();
        }
        if (msgQueue != null && !msgQueue.isEmpty()) {
            logger.fine(() -> lm(
                "Clear the message queue, remaining messages=" +
                msgQueue.stream().map(Object::toString)
                        .collect(DEFAULT_COLL)));
            msgQueue.clear();
        }

        if (subscriber != null &&
            subscriber.getSubscription() != null &&
            !subscriber.getSubscription().isCanceled()) {
            if (subscriber.getSubscriptionConfig().isCkptEnabled()) {
                doLastCkptWait();
            }
            subscriber.getSubscription().cancel();
            logger.fine(() -> lm("Stream from region=" +
                                 config.getSource().getName() + " canceled."));
        }

        if (publisher != null && !publisher.isClosed()) {
            publisher.close(true);
            publisher = null;
            logger.fine(() -> lm("Publisher in agent closed."));
        }

        if (ec != null) {
            ec.shutdownNow();
            logger.fine(() -> lm("Executor shutdown"));
        }

        if (storedCause != null) {
            logger.warning(lm("Agent has been shut down with error=" +
                              storedCause));
            logger.fine(() -> lm("Call stack\n" +
                                 LoggerUtils.getStackTrace(storedCause)));
        }
    }

    void postSucc(long reqId, XRegionRespHandlerThread resp) {
        if (resp == null) {
            logger.info(lm(logPrefix(reqId) +
                           "Success, response handler unavailable"));
            return;
        }

        final String msg;
        if (subscriber == null) {
            msg = "subscriber undefined";
        } else {
            final NoSQLSubscription stream = subscriber.getSubscription();
            if (stream == null) {
                msg = "stream not yet created";
            } else {
                msg = "position=" + stream.getCurrentPosition() +
                      "(canceled=" + stream.isCanceled() + ")";
            }
        }
        logger.info(lm(logPrefix(reqId) + "Post success, " +
                       "source region=" + config.getSource().getName() +
                       ", msg=" + msg));
        resp.regionSucc(config.getSource(), msg);
    }

    private void postFail(long reqId,
                          XRegionRespHandlerThread resp,
                          Throwable cause) {
        if (resp == null) {
            logger.info(lm(logPrefix(reqId) +
                           "Fail, response handler unavailable"));
            return;
        }

        resp.regionFail(config.getSource(), cause);
    }

    /**
     * Starts the inbound stream
     *
     * @param streamMode stream mode
     * @param tables     tables to stream
     * @param req        region agent request, or null if no request is
     *                   available, e.g., called by polling thread
     * @return true if stream starts up successfully, false otherwise
     * @throws InterruptedException if interrupted in creating stream
     */
    private synchronized boolean startStream(NoSQLStreamMode streamMode,
                                             Set<Table> tables,
                                             RegionAgentReq req)
        throws InterruptedException {

        /* if shutdown, simply returns */
        if (isShutdownRequested()) {
            return false;
        }

        /* ensure publisher */
        if (publisher == null || publisher.isClosed()) {
            final String err = reqPrefix(req) + "Publisher from region=" +
                               config.getSource().getName() +
                               " has closed.";
            logger.warning(lm(err));
            throw new IllegalArgumentException(err);
        }

        /* cancel current stream if exists */
        final NoSQLSubscription curr =
            subscriber == null ? null : subscriber.getSubscription();
        if (curr != null && !curr.isCanceled()) {
            final String err = reqPrefix(req) + "Cancel existing stream from " +
                               "region=" + config.getSource().getName();
            logger.info(lm(err));
            curr.cancel();
        }

        /* create subscriber instance */
        subscriber = (MRTSubscriber) createSubscriber(config.getType(),
                                                      streamMode, tables);

        publisher.subscribe(subscriber);

        final boolean succ =
            new PollCondition(POLL_INTERVAL_MS, Long.MAX_VALUE) {
                @Override
                protected boolean condition() {
                    return subscriber.isSubscriptionSucc() ||
                           subscriber.getFailure() != null ||
                           isShutdownRequested();
                }
            }.await();

        if (isShutdownRequested()) {
            throw new InterruptedException(
                "in shutdown, give up starting stream");
        }

        if (!succ) {
            /* since wait forever, false means it is interrupted */
            final String err = reqPrefix(req) +
                               "Interrupted in waiting for stream to start up" +
                               " from region=" + config.getSource().getName() +
                               ", tables=" + ServiceMDMan.getTrace(tables) +
                               ", mode=" + streamMode;
            logger.fine(() -> lm(err));
            throw new InterruptedException(err);
        }

        /* poll returns true, either success or failure to start stream */
        if (subscriber.isSubscriptionSucc()) {
            final long reqId = (req == null) ? 0 : req.getReqId();
            recordStreamTable(reqId);
            logger.info(lm(reqPrefix(req) +
                           "Stream created from region=" +
                           config.getSource().getName() +
                           ", mode=" + streamMode +
                           ", tables=" + ServiceMDMan.getTrace(tables)));
            return true;
        }

        /* failure to start a stream is serious enough to qualify a warning */
        logger.warning(lm(
            reqPrefix(req) + "Stream cannot start up from region=" +
            config.getSource().getName() +
            ", tables=" + ServiceMDMan.getTrace(tables) +
            ", mode=" + streamMode +
            ", error=" + subscriber.getFailure()));
        return false;
    }

    private String getStartPos(NoSQLSubscriptionImpl subscription) {
        if (subscription == null) {
            return "N/A";
        }
        return subscription.getSubscriptionMetrics().getAckStartVLSN()
                           .entrySet()
                           .stream()
                           .map(entry -> "shard " + entry.getKey() +
                                         ": " + entry.getValue())
                           .collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * Creates subscriber based on the type specified
     *
     * @param type       type of subscriber
     * @param streamMode stream mode
     * @param tables     tables to subscribe
     * @return subscriber based on the type specified
     */
    private BaseRegionAgentSubscriber createSubscriber(
        RegionAgentConfig.Type type,
        NoSQLStreamMode streamMode,
        Set<Table> tables) {

        final NoSQLSubscriptionConfig.Builder builder =
            new NoSQLSubscriptionConfig.Builder(this::buildCkptTableName)
                .setSubscriberId(config.getSubscriberId())
                .setStreamMode(streamMode)
                .setCreateNewCheckpointTable(true)
                .setSubscribedTables(ServiceMDMan.getTbNames(tables))
                .setLocalWritesOnly(config.isLocalWritesOnly())
                .setChangeTimeoutMs(DEFAULT_STREAM_CHANGE_TIMEOUT_MS)
                .setEmptyStreamDuration(DEFAULT_EMPTY_STREAM_TIMEOUT_SECS);
        /* build start position */
        final StreamPosition pos = buildStartPos(streamMode);
        if (pos != null) {
            /*
             * the stream position is constructed from checkpoints made by
             * other stream agents, need to change the stream mode to from_
             * stream_position and start the stream with constructed start
             * position
             */
            final NoSQLStreamMode mode =
                NoSQLStreamMode.FROM_EXACT_STREAM_POSITION;
            builder.setStreamMode(mode);
            builder.setStartStreamPosition(pos);
            logger.info(lm("Start stream from a start position constructed " +
                           "from checkpoints made by other agents, " +
                           "stream mode changes from=" + streamMode +
                           " to=" + mode + ", start pos=" + pos));
        }

        final NoSQLSubscriptionConfig conf = builder.build();
        if (!config.isEnableCkptTable()) {
            /* test only */
            conf.disableCheckpoint();
            logger.warning(lm("Stream checkpoint disabled."));
        }

        /* set test hook */
        conf.setILETestHook(config.getTestHook());

        BaseRegionAgentSubscriber ret;
        switch (type) {
            case MRT:
                ret = new MRTSubscriber(this, conf,
                                        mdMan.getRegionKVS(config.getTarget()),
                                        logger);
                break;
            case PITR:
                ret = new PITRSubscriber(this, conf, logger);
                break;
            default:
                throw new IllegalStateException("Unsupported agent type=" +
                                                type);
        }
        return ret;
    }

    /**
     * Builds stream checkpoint table from given agent id
     * @param sid agent id
     * @return stream checkpoint table name for that agent
     */
    private String buildCkptTableName(NoSQLSubscriberId sid) {
        return StreamCheckpointName.buildCkptTableName(
            config.getSource().getName(), config.getHost().getName(), sid);
    }

    /**
     * Transfers a set of tables from source region to target region.
     *
     * @param tables   name of tables to transfer
     * @param req      region agent request
     */
    private void transferTables(Set<Table> tables, RegionAgentReq req) {
        final long reqId = req.getReqId();
        if (tables == null || tables.isEmpty()) {
            logger.info(lm(logPrefix(reqId) + "No table to " +
                           "initialize"));
            return;
        }

        logger.fine(() -> lm("To transfer table=" +
                             ServiceMDMan.getTrace(tables) + " from=" +
                             config.getSource().getName()));
        final Set<String> complete = new HashSet<>();
        final Set<String> dropped = new HashSet<>();
        /* create table copy threads */
        boolean succ = false;
        for (Table tb : tables) {
            if (isShutdownRequested()) {
                logger.info(lm("Region agent shutdown requested, stop " +
                               "transferring tables=" +
                               ServiceMDMan.getTrace(tables) +
                               ", completed=" + complete));
                return;
            }
            final String table = tb.getFullNamespaceName();
            final long tid = ((TableImpl) tb).getId();
            if (tablePollingThread.inPolling(tid)) {
                logger.info(lm(logPrefix(reqId) +
                               "Table=" + ServiceMDMan.getTrace(tb) +
                               " already in polling thread check list"));
                continue;
            }
            if (tablePollingThread.inTrans(tid)) {
                logger.info(lm(logPrefix(reqId) +
                               "Table=" + ServiceMDMan.getTrace(tb) +
                               " already submitted to transfer in polling " +
                               "thread"));
                continue;
            }

            if (!readyToSnapshot(tb)) {
                tablePollingThread.addTables(req, Collections.singleton(tb));
                logger.info(lm(logPrefix(reqId) +
                               "Table=" + ServiceMDMan.getTrace(tb) +
                               " not ready for transfer, moved to" +
                               " polling thread"));
                continue;
            }

            if (!config.getSubscriberId().isSingleAgentGroup()) {
                logger.info(lm("Table=" + ServiceMDMan.getTrace(tb) +
                               " is ready for transfer in this agent=" +
                               config.getSubscriberId()));
            }
            try {
                succ = transferTable(tb, config.getSource(),
                                     config.getTarget(), req);
                /* transfer is complete */
                complete.add(table);
                final MRTableMetrics tm =
                    metrics.get().getTableMetrics(table);
                final TableInitStat st =
                    tm.getRegionInitStat(getSourceRegion().getName());
                final Set<Table> remain =
                    tables.stream()
                          .filter(t -> !complete.contains(
                              t.getFullNamespaceName()))
                          .collect(Collectors.toSet());
                logger.info(lm(
                    logPrefix(reqId) +
                    (succ ? "Done" : "Timeout") +
                    " transfer table=" + ServiceMDMan.getTrace(tb) +
                    ", #transferred=" +
                    st.getTransferRows() +
                    ", #persisted=" +
                    st.getPersistRows() +
                    ", transferred=" + complete +
                    ", remaining=" + ServiceMDMan.getTrace(remain)));
            } catch (StoreIteratorException sie) {
                /*
                 * If unable to read the source table, log error and move to
                 * the next table
                 */
                logger.warning(lm(logPrefix(reqId) +
                                  "Unable to read" +
                                  " table=" + ServiceMDMan.getTrace(tb) +
                                  " at region=" + config.getSource().getName() +
                                  ", error=" + sie +
                                  ", cause=" + sie.getCause()));
            } catch (MetadataNotFoundException mnfe) {
                dropped.add(table);
                /*
                 * when the multi-region table can be dropped at local
                 * regions, the agent would encounter MNFE during
                 * table scan. This is an error which should be logged,
                 * but it should not bring down the agent.
                 */
                logger.warning(lm(logPrefix(reqId) +
                                  "Local table=" + ServiceMDMan.getTrace(tb) +
                                  " is dropped, error=" + mnfe +
                                  ", cause=" + mnfe.getCause()));
            } catch (InterruptedException ie) {
                logger.warning(lm(logPrefix(reqId) +
                                  "Interrupted, out of all tables=" +
                                  ServiceMDMan.getTrace(tables) +
                                  ", fail to transfer table=" +
                                  ServiceMDMan.getTrace(tb) +
                                  ", reason=" + ie));
                throw new TransferTableException(
                    ServiceMDMan.getTbNames(tables), complete, table, ie);
            } catch (Exception exp) {
                /* surface all other hard failures */
                logger.warning(lm(logPrefix(reqId) +
                                  "Out of all tables=" +
                                  ServiceMDMan.getTrace(tables) +
                                  ", fail to transfer table=" +
                                  ServiceMDMan.getTrace(tb) +
                                  ", error=" + exp + "\ndump stack=" +
                                  LoggerUtils.getStackTrace(exp)));
                throw new TransferTableException(
                    ServiceMDMan.getTbNames(tables), complete, table, exp);
            }
        }
        logger.info(lm(logPrefix(reqId) +
                       (succ ? "Done" : "Timeout") +
                       " initializing tables=" + complete +
                       (dropped.isEmpty() ? "" :
                           ", dropped local tables=" + dropped)));
    }

    /**
     * Initialize the agent thread. Create the publisher for remote region,
     * will retry endlessly if fail to create the publisher. It also starts
     * all auxiliary threads.
     *
     * @throws InterruptedException if interrupted
     */
    private void initializeAgent() throws InterruptedException {
        /* create publisher instance */
        int attempts = 0;
        while (!isShutdownRequested()) {
            try {
                attempts++;
                publisher = createAgentPublisher(config);
                final int attemptsFinal = attempts;
                logger.fine(() -> lm("Publisher (id=" +
                                     publisher.getPublisherId() + ")" +
                                     " has been created in # of attempts=" +
                                     attemptsFinal + ", from region=" +
                                     config.getSource().getName()));
                break;
            } catch (PublisherFailureException pfe) {
                if (shouldRetry(pfe)) {
                    /*
                     * cannot reach remote store which can be down, sleep and
                     * retry
                     *
                     * xregion service will endlessly retry, the admin
                     * should monitor the log to ensure the remote the region
                     * is up.
                     */
                    final String msg = "Cannot create publisher for region=" +
                                       config.getSource().getName() +
                                       ", will retry after time(ms)=" +
                                       SLEEP_MS_BEFORE_RECONNECT +
                                       ", error=" + pfe +
                                       ", cause=" + pfe.getCause() +
                                       (logger.isLoggable(Level.FINE) ?
                                           LoggerUtils.getStackTrace(pfe) : "");
                    logger.warning(lm(msg));
                    /* dump detailed msg of cause */
                    final String sid = (pfe.getSubscriberId() == null) ? "NA" :
                        pfe.getSubscriberId().toString();
                    rlLogger.log(sid, Level.WARNING, lm(getPFECauseMsg(pfe)));

                    synchronized (msgQueue) {
                        /* may miss the notification, check shutdown */
                        if (isShutdownRequested()) {
                            break;
                        }
                        msgQueue.wait(SLEEP_MS_BEFORE_RECONNECT);
                    }
                    continue;
                }

                /* surface failure without retry */
                String err = "Surface failure to create publisher, error=" +
                             pfe + ", " + getPFECauseMsg(pfe);
                logger.warning(lm(err));
                throw pfe;
            }
        }

        if (isShutdownRequested()) {
            throw new InterruptedException("shut down requested");
        }

        /* depending on the start mode, put a msg in the queue */
        switch (config.getStartMode()) {
            case STREAM:
                createStream(config.getTables());
                break;
            case INIT_TABLE:
                initTables(config.getTables());
                break;
            case IDLE:
                break;
            default:
                logger.fine(() -> lm("No msg enqueued, agent idle"));
        }

        /* start auxiliary threads after agent initialized */
        tablePollingThread.start();

        logger.info(lm("Agent of region=" + config.getSource().getName() +
                       " initialized, mode=" + config.getStartMode() +
                       ", status=" + status +
                       ", tables=" + Arrays.toString(config.getTables())));
    }

    /**
     * Builds an instance of publisher
     *
     * @return publisher instance
     * @throws PublisherFailureException if fail to create the publisher
     */
    private NoSQLPublisher createAgentPublisher(RegionAgentConfig conf)
        throws PublisherFailureException {

        /* create publisher instance */
        final RegionInfo region = conf.getSource();
        final String store = region.getStore();
        final String[] helper = region.getHelpers();
        /* create publisher root directory for region */
        final Path dir = Paths.get(conf.getLocalPath(), region.getName());
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException exp) {
                final String err = "Cannot create publisher directory " +
                                   dir.getFileName() +
                                   " region=" + region.getName() +
                                   ", error=" + exceptionString(exp);
                throw new PublisherFailureException(err, false, exp);
            }
        }
        final String path = dir.toString();
        final KVStoreConfig kvStoreConfig = new KVStoreConfig(store, helper);
        if (conf.isSecureStore()) {
            final Properties sp = XRegionService.setSecureProperty(
                kvStoreConfig, new File(conf.getSecurityConfig()));
            logger.info(lm("For region=" + region.getName() +
                           ", secure store=" + store +
                           ", security property=" +
                           ServiceMDMan.dumpProperty(sp)));
        }
        final NoSQLPublisherConfig cf;
        try {
            cf = new NoSQLPublisherConfig.Builder(kvStoreConfig, path)
                /* allow preexisting directory */
                .setAllowPreexistDir(true)
                .setPublisherId(conf.getSource().getName() + "." +
                                conf.getSource().getStore())
                .build();
        } catch (IllegalArgumentException iae) {
            final String err = "Cannot build publisher configuration" +
                               ", error=" + iae;
            logger.warning(lm(err));
            throw new PublisherFailureException(err, false, iae);
        }

        /*
         * In the case when a region agent is recreated but the previous one
         * did not shut down cleanly, the old publisher may still exist and
         * did not close, NoSQLPublisher.get() may return the old publisher
         * that will fail creating any stream from the remote region because
         * of the duplicate subscription. Remove the old instance will force
         * to create a new publisher instance.
         */
        final NoSQLPublisher old = NoSQLPublisher.removePublisherInst(cf);
        if (old != null) {
            logger.info(lm("Existing publisher deleted, id=" +
                           old.getPublisherId()));
        }
        return NoSQLPublisher.get(cf, logger);
    }

    /**
     * Returns true if the stream is on, false otherwise
     */
    private boolean isStreamOn(long reqId) {

        if (publisher == null || publisher.isClosed()) {
            logger.info(lm(logPrefix(reqId) +
                           "Publisher does not exist or has shut down."));
            return false;
        }
        if (subscriber == null) {
            logger.info(lm(logPrefix(reqId) + "Subscriber does not exist"));
            return false;
        }
        final NoSQLSubscription subscription = subscriber.getSubscription();
        if (subscription == null) {
            logger.info(lm(logPrefix(reqId) + "Subscription is not created"));
            return false;
        }
        if (subscription.isCanceled()) {
            final Throwable failure = subscriber.getFailure();
            logger.info(lm(logPrefix(reqId) +
                           "Subscription has been canceled, error=" + failure));
            return false;
        }
        return true;
    }

    private boolean willStreamRestart() {
        if (subscriber == null) {
            return false;
        }
        final NoSQLSubscriptionImpl stream =
            (NoSQLSubscriptionImpl) subscriber.getSubscription();
        if (stream == null) {
            return false;
        }
        return isStreamTobeRestarted(stream.getCauseOfCancel());
    }

    private boolean isStreamTobeRestarted(Throwable reasonCancel) {
        if (reasonCancel == null) {
            /* onError not signalled */
            return false;
        }
        if (reasonCancel instanceof PublishingUnit.EmptyStreamExpireException) {
            /* shutdown stream due to expiration, stream won't restart */
            return false;
        }

        /* stream canceled due to error and will restart */
        logger.info(lm("Stream will restart, cancellation reason=" +
                       reasonCancel));
        return true;
    }

    /**
     * Changes the stream by adding or removing a table and returns the
     * stream position where the table is added or removed. If the table
     * has been already added or removed, returns the current stream position.
     *
     * @param resp  response handler
     * @param tbl   table to add or remove
     * @param type  type of request
     * @return effective position if change is successful
     * @throws InterruptedException  if interrupted in changing stream
     * @throws ChangeStreamException if fail to change the stream
     */
    private synchronized StreamPosition
    changeStream(XRegionRespHandlerThread resp, Table tbl, ReqType type)
        throws InterruptedException, ChangeStreamException {

        final long reqId = (resp == null ? NULL_REQUEST_ID : resp.getReqId());
        final String table = tbl.getFullNamespaceName();

        /* test hook to simulate stream change result */
        assert ExceptionTestHookExecute.doHookIfSet(
            changeStreamHook, new Pair<>(numRetry.get(), table));

        if (!isStreamOn(reqId)) {
            final String err = "Cannot change stream because it is not on";
            logger.fine(() -> lm(logPrefix(reqId) + err));
            throw new ChangeStreamException(reqId, table, err,
                                            new IllegalStateException(err));
        }

        final NoSQLSubscription subscription = subscriber.getSubscription();
        if (subscription == null) {
            final String err = "Cannot change stream because it is not created";
            logger.fine(() -> lm(logPrefix(reqId) + err));
            throw new ChangeStreamException(reqId, table, err,
                                            new IllegalStateException(err));
        }

        /*
         * For MR table, adding or removing tables from streams must be serial.
         * However, because in Streams API, removing table would return
         * early and let the filter change as a background process, it is
         * possible that when we reach here, the thread
         * {@link NoSQLSubscriptionImpl#StreamChangeWorkerThread} processing
         * the previous remove table request is still running. It is incorrect
         * that feeder filter is updated concurrently in multiple threads,
         * therefore we should wait till the running change worker thread is
         * gone. The wait will either return if the previous thread is gone,
         * or throw exception if timeout, and in this case, the change cannot
         * be made.
         */
        final long timeoutMs = getChangeTimeoutMs(subscription);
        if (!waitPreviousChangeDone(table, reqId, timeoutMs, subscription)) {
            /* timeout, throw exception */
            final String err = "Timeout in waiting for previously running " +
                               "thread to complete, reqId=" + reqId +
                               ", table=" + table +
                               " timeoutMs=" + timeoutMs;
            throw new ChangeStreamException(reqId, table, err, null);
        }

        logger.fine(() -> lm(logPrefix(reqId) +
                             "Change thread of previous request is gone."));

        subscriber.clearChangeResult();
        switch (type) {
            case ADD:
                if (subscription.getSubscribedTables().contains(table)) {
                    logger.info(lm(logPrefix(reqId) +
                                   "Table=" + ServiceMDMan.getTrace(tbl) +
                                   " already exists in stream"));
                    return subscription.getCurrentPosition();
                }
                logger.fine(() -> lm(logPrefix(reqId) + "Adding table=" +
                                     ServiceMDMan.getTrace(tbl) +
                                     " to existing stream"));
                subscription.subscribeTable(table);
                break;

            case REMOVE:
                if (!subscription.getSubscribedTables().contains(table)) {
                    logger.fine(() -> lm(logPrefix(reqId) +
                                         "Table=" + ServiceMDMan.getTrace(tbl) +
                                         " already removed from stream"));
                    return subscription.getCurrentPosition();
                }
                logger.fine(() -> lm(logPrefix(reqId) + "Removing table=" +
                                     ServiceMDMan.getTrace(tbl) +
                                     " from existing stream"));
                subscription.unsubscribeTable(table);
                break;

            default:
                throw new IllegalArgumentException("Unsupported type=" + type);
        }

        /* wait for change result */
        final StreamPosition sp = waitForChangeResult(reqId, type, tbl);
        logger.fine(() -> lm(logPrefix(reqId) + "Done request reqId=" + reqId +
                             ": type=" + type +
                             " table=" + ServiceMDMan.getTrace(tbl) +
                             " at position=" + sp));
        return sp;
    }

    /**
     * Waits for the running change-stream is done.
     *
     * @param tableName name of table
     * @param reqId id of request
     * @param timeoutMs timeout in ms
     * @param subscription subscription stream
     * @return true if the previous change stream request is done, false if
     * timeout
     */
    private boolean waitPreviousChangeDone(String tableName,
                                           long reqId,
                                           long timeoutMs,
                                           NoSQLSubscription subscription) {
        final NoSQLSubscriptionImpl impl = (NoSQLSubscriptionImpl) subscription;
        if (impl.getPendingRequests() == 0) {
            /* no pending request */
            return true;
        }

        final long ts = System.currentTimeMillis();
        return new PollCondition(POLL_INTERVAL_MS, timeoutMs) {
            @Override
            protected boolean condition() {
                final String msg = logPrefix(reqId) +
                                   "Waiting for #pending requests=" +
                                   impl.getPendingRequests() +
                                   " to complete, table=" + tableName +
                                   ", timeoutMs=" + timeoutMs +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - ts);
                rlLogger.log(tableName, Level.INFO, lm(msg));
                /* wait till all pending requests are gone */
                return impl.getPendingRequests() == 0;
            }
        }.await();
    }

    /**
     * Returns the change timeout for given subscription
     */
    private long getChangeTimeoutMs(NoSQLSubscription subscription) {
        final long nShards =
            ((NoSQLSubscriptionImpl) subscription).getCoveredShards().size();

        /*
         * As of today the stream change is made serially one shard a
         * time, thus the total timeout is the sum of change timeout per
         * shard across all shards.
         *
         * An extra room is added to the sum timeout to account for the
         * overhead such that we do not time out prematurely
         */
        final long perShardTimeoutMs = subscriber.getSubscriptionConfig()
                                                 .getChangeTimeoutMs();
        return (long) (perShardTimeoutMs * nShards *
                       /* extra room of timeout */
                       (1 + 1.0 * EXTRA_CHANGE_TIMEOUT_PERCENT / 100.0));
    }

    /**
     * Transfers a table from source region to target region
     *
     * @param table table to transfer
     * @param src   source region
     * @param tgt   target region
     * @param req   region agent request
     * @throws Exception if fail to transfer
     * @return true if transfer is complete, false if timeout
     */
    private boolean transferTable(Table table, RegionInfo src, RegionInfo tgt,
                                  RegionAgentReq req) throws Exception {

        boolean complete = false;
        final long reqId = req.getReqId();
        /* create table initialization thread */
        final KVStore srcKVS = mdMan.getRegionKVS(src);
        final KVStore tgtKVS = mdMan.getRegionKVS(tgt);
        final MRTTableTransferThread
            tc = new MRTTableTransferThread(this,
                                            getSubscriber(),
                                            table,
                                            config.getSource(),
                                            config.getTarget(),
                                            (TableAPIImpl) srcKVS.getTableAPI(),
                                            (TableAPIImpl) tgtKVS.getTableAPI(),
                                            getTimeoutMs(req),
                                            logger);
        ec.submit(tc);
        logger.fine(() -> lm(logPrefix(reqId) + "Thread to transfer table=" +
                             ServiceMDMan.getTrace(table) + " submitted"));
        /* wait for table transfer done */
        try {
            waitForTableTransferDone(reqId, tc);
            complete = true;
        } catch (TimeoutException toe) {
            /* timeout in transfer, add to polling thread */
            logger.info(lm(logPrefix(reqId) +
                           "TimeoutMs=" + tc.getTimeoutMs() +
                           " in transfer of table=" +
                           table.getFullNamespaceName() +
                           ", add to polling thread"));
            tablePollingThread.addTables(req, Collections.singleton(table));
        }

        return complete;
    }

    /**
     * Returns the response handler timeout, or default timeout if null
     * request or no response handler in the request
     * @param req region agent request
     */
    public long getTimeoutMs(RegionAgentReq req) {
        if (req == null) {
            return XRegionRespHandlerThread.DEFAULT_TIMEOUT_MS;
        }

        final XRegionRespHandlerThread resp = req.getResp();
        if (resp == null) {
            return XRegionRespHandlerThread.DEFAULT_TIMEOUT_MS;
        }

        return resp.getTimeoutMs();
    }

    /***
     * Waits for the table transfer to complete. Throw exception if transfer
     * is interrupted, times out or fails due to error
     *
     * @param reqId request id
     * @param tc  table transfer thread
     * @throws InterruptedException if interrupted in waiting
     * @throws TimeoutException if timeout in transfer
     * @throws Exception if fail to transfer table due to errors
     */
    private void waitForTableTransferDone(long reqId, MRTTableTransferThread tc)
        throws Exception {

        final Table table = tc.getTable();
        final long ts = System.currentTimeMillis();
        final boolean succ;
        if (throwSimulatedTimeoutException) {
            /* test only: no need to wait, throw simulated timeout exception */
            succ = false;
            logger.info(lm("Will throw simulated timeout exception after shut" +
                           " down transfer of table=" +
                           table.getFullNamespaceName()));
        } else {
            /* wait for table transfer done */
            succ = waitForTransferToComplete(reqId, ts, tc, rlLogger,
                                             this::isShutdownRequested);
        }

        if (!succ) {
            /*
             * table transfer timeout, shutdown current transfer, make table
             * transfer checkpoint, and move the table to the polling thread.
             * The polling thread will continue initializing the table in next
             * turn;
             */
            final String msg = logPrefix(reqId) +
                               "Table=" + tc.getTable().getFullNamespaceName() +
                               " transfer timeoutMs=" + tc.getTimeoutMs() +
                               ", shutdown transfer after checkpointing";
            stopTableTransfer(tc);
            logger.info(lm(msg));
            /* if simulated exception, mark as thrown */
            simulatedExceptionThrown = throwSimulatedTimeoutException;
            throw new TimeoutException(msg);
        }

        if (isShutdownRequested()) {
            logger.info(lm("Shutdown requested, stop running thread to " +
                           "transfer table=" + ServiceMDMan.getTrace(table)));
            /* stop the running thread */
            tc.shutdown();
            final String err = logPrefix(reqId) + "In shutdown, stop " +
                               "transferring table=" +
                               ServiceMDMan.getTrace(table);
            throw new InterruptedException(err);
        }

        if (tc.hasFailed()) {
            /* fail to transfer the table, throw failure cause to caller */
            throw tc.getCause();
        }

        logger.fine(() -> lm(logPrefix(reqId) + "Complete transferring " +
                             "table=" + ServiceMDMan.getTrace(table)));
    }

    /**
     * Clears up if a table transfer times out
     * @param tc table transfer
     */
    void stopTableTransfer(MRTTableTransferThread tc) {
        tc.doCheckpoint();
        tc.shutdown();
    }

    /**
     * Waits for the table transfer to complete
     * @param reqId request id
     * @param ts   start timestamp
     * @param tc   table transfer thread
     * @param rl   rate limiting logger supplier
     * @param shutdownReq  shutdown request supplier
     * @return true table transfer completes, or fails due to errors, or
     * shutdown, false if timeout.
     */
    boolean waitForTransferToComplete(long reqId,
                                      long ts,
                                      MRTTableTransferThread tc,
                                      RateLimitingLogger<String> rl,
                                      Supplier<Boolean> shutdownReq) {
        /* wait for table transfer done */
        final long timeoutMs = tc.getTimeoutMs();
        final String tableName = tc.getTable().getFullNamespaceName();
        return new PollCondition(POLL_INTERVAL_MS, timeoutMs) {
            @Override
            protected boolean condition() {
                final String msg = logPrefix(reqId) +
                                   "Waiting for table=" + tableName +
                                   " transfer to complete" +
                                   ", timeoutMs=" + timeoutMs +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - ts);
                rl.log(tableName, Level.INFO, lm(msg));
                return tc.isComplete() || tc.hasFailed() || shutdownReq.get();
            }
        }.await();
    }

    /**
     * Waits for change result, return effective stream position if change is
     * successful, throw exception otherwise
     *
     * @param reqId   request id
     * @param reqType type of request
     * @param table   table in request
     * @return effective stream position if change is successful
     * @throws ChangeStreamException if fail to change the stream
     * @throws InterruptedException  if interrupted
     */
    private StreamPosition waitForChangeResult(long reqId,
                                               ReqType reqType,
                                               Table table)
        throws ChangeStreamException, InterruptedException {

        final boolean succ =
            new PollCondition(POLL_INTERVAL_MS, Long.MAX_VALUE) {
                @Override
                protected boolean condition() {
                    return subscriber.isChangeResultReady() ||
                           isShutdownRequested();
                }
            }.await();

        if (!succ) {
            final String err = logPrefix(reqId) +
                               "Timeout in waiting for change result" +
                               ", reqId=" + reqId + ", type=" + reqType +
                               ", table=" + ServiceMDMan.getTrace(table);
            logger.fine(() -> lm(err));
            throw new ChangeStreamException(reqId, table.getFullNamespaceName(),
                                            err, null);
        }

        if (isShutdownRequested()) {
            throw new InterruptedException(
                "in shutdown, give up change result");
        }

        /* change result is ready */
        final StreamPosition sp = subscriber.getEffectivePos();
        if (sp != null) {
            /* change is successful */
            return sp;
        }

        /* fail to change the stream */
        final Throwable cause = subscriber.getChangeResultExp();
        if (cause == null) {
            throw new IllegalStateException(
                logPrefix(reqId) + "Missing change result exception " +
                "when fail to type=" + reqType +
                " table=" + ServiceMDMan.getTrace(table));
        }

        final String err = logPrefix(reqId) + "cannot change the stream, " +
                           "error=" + exceptionString(cause);
        throw new ChangeStreamException(reqId, table.getFullNamespaceName(),
                                        err, cause);
    }

    /**
     * Enqueues a msg for the agent to execute
     *
     * @param msg message to enqueue
     * @throws InterruptedException if interrupted in enqueue
     */
    private synchronized void enqueue(RegionAgentReq msg)
        throws InterruptedException {

        if (msg == null) {
            return;
        }

        /*
         * Update target table list if not remove the table before enqueue.
         * Reason to update target before executing the request is to avoid a
         * possible deadlock that the current request is to initialize the
         * table while the table has been evolved in later requests. The
         * request to initialize the table will be blocked waiting for the
         * target table to evolve, while the evolve request is being waiting
         * for the initialization to complete
         */
        updatePendingEvolveTable(msg);

        while (!shutdownRequested) {
            try {
                if (msgQueue.offer(msg, MSG_QUEUE_PUT_TIMEOUT_MS,
                                   TimeUnit.MILLISECONDS)) {
                    logger.finest(() -> lm("Message=" + msg + " enqueued"));
                    break;
                }

                logger.finest(() -> lm("Unable enqueue message=" + msg +
                                       " for time(ms)=" +
                                       MSG_QUEUE_PUT_TIMEOUT_MS +
                                       ", keep trying..."));
            } catch (InterruptedException e) {
                /* This might have to get smarter. */
                logger.warning(lm("Interrupted offering message queue, " +
                                  "message=" + msg));
                throw e;
            }
        }
    }

    private void updateTargetTable(RegionAgentReq req) {
        if (!req.getReqType().equals(ReqType.REMOVE)) {
            final Set<Table> tbs = req.getTableMD();
            if (tbs != null && !tbs.isEmpty()) {
                final long reqId = req.getReqId();
                tbs.forEach(t -> updateTableRecursive(reqId, t));
            }
        }
    }

    private void updateStatsEntries(RegionAgentReq req) {
        final ReqType type = req.getReqType();
        switch (type) {
            case ADD:
            case STREAM:
            case INITIALIZE_TABLES:
            case INITIALIZE_FROM_REGION:
                addStatsEntries(req.getReqId(), req.getTableMD());
                break;
            default:
                logger.fine(() -> "No need to add stats entry, type=" + type);
        }
    }

    /**
     * Add stats entries for the table and its child tables if exists
     * @param reqId request id
     * @param tbs   set of tables
     */
    private void addStatsEntries(long reqId, Set<Table> tbs) {
        for (Table tb : tbs) {
            final Set<Table> allTbs = new HashSet<>();
            mdMan.getAllChildTables(tb, allTbs);
            allTbs.forEach(t -> {
                final long tableId = ((TableImpl) t).getId();
                final String tableName = t.getFullNamespaceName();
                final MRTableMetrics tbm = getAddTable(tableName, tableId);
                tbm.getRegionInitStat(config.getSource().getName());
                logger.info(lm(logPrefix(reqId) +
                               "Region stats added for table=" +
                               ServiceMDMan.getTrace(t)));
            });
        }
    }

    /**
     * Internal exception raised when fail to change the stream
     */
    public static class ChangeStreamException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /* request id */
        private final long reqId;

        /* name of table to change the stream */
        private final String table;

        /* summary of error message */
        private final String error;

        public ChangeStreamException(long reqId,
                                     String table,
                                     String error,
                                     Throwable cause) {
            super(cause);
            this.reqId = reqId;
            this.table = table;
            this.error = error;
        }

        long getReqId() {
            return reqId;
        }

        String getTable() {
            return table;
        }

        String getError() {
            return error;
        }
    }

    /**
     * Internal exception raised when fail to transfer tables
     */
    private static class TransferTableException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /* all tables */
        private final Set<String> tables;

        /* already transferred tables */
        private final Set<String> transferred;

        /* table that fail to transfer */
        private final String failed;

        TransferTableException(Set<String> tables, Set<String> transferred,
                               String failed, Throwable cause) {
            super(cause);
            this.tables = tables;
            this.transferred = transferred;
            this.failed = failed;
        }

        Set<String> getTransferred() {
            return transferred;
        }

        String getFailed() {
            return failed;
        }

        Set<String> getRemaining() {
            return tables.stream().filter(t -> !transferred.contains(t))
                         .collect(Collectors.toSet());
        }
    }

    /**
     * Checks for the specified tables in remote (source) and local regions,
     * and returns two values: tables in the specified set found in
     * the remote region, and ones only found in the local region and added to
     * the polling thread.
     * <p>
     * Each of the tables in parameter falls into one of the following cases:
     * <p>
     * 1) Table is found in remote region, and may or may not be present in the
     * local region. This table will be included in the first return value of
     * tables found in the source region.
     * <p>
     * 2) Table is only found in the local region, not the remote region. This
     * table will be added to the polling thread, and included in the second
     * return value of tables that were added to the polling thread.
     * <p>
     * 3) Table is not found in the remote region or the local region. This
     * table has been dropped and is not included in either return value.
     *
     * @param req    request
     * @param tblsMD given tables
     * @return set of tables found at source region, and set of tables in
     * polling thread
     */
    private Pair<Set<Table>, Set<Table>> ensureTable(RegionAgentReq req,
                                                     Set<Table> tblsMD) {
        /* ensure table exists at source, otherwise stream cannot be created */
        final Set<String> tables = ServiceMDMan.getTbNames(tblsMD);
        final Set<String> found = mdMan.tableExists(
            config.getSource(), tables, remoteTableTimeoutMs);

        Set<Table> inPolling;
        if (tables.equals(found)) {
            /* all tables found at remote region, nothing to poll */
            inPolling = Collections.emptySet();
        } else {
            /* not all tables found */
            Set<String> notFound =
                tables.stream().filter(t -> !found.contains(t))
                      .collect(Collectors.toSet());
            final Set<Table> notFoundMD =
                tblsMD.stream()
                      .filter(t -> notFound.contains(t.getFullNamespaceName()))
                      .collect(Collectors.toSet());
            inPolling = resetInitCkpt(notFoundMD);
            if (inPolling.isEmpty()) {
                logger.info(lm(reqPrefix(req) + "Tables not found at remote " +
                               "region=" + config.getSource().getName() +
                               " have all been dropped in local region"));
            } else {
                /*
                 * for tables not found at remote, but found at local region,
                 * add to polling thread
                 */
                logger.info(lm(reqPrefix(req) +
                               "Create checkpoint and add to polling " +
                               "thread for missing tables=" +
                               ServiceMDMan.getTrace(inPolling)));
                tablePollingThread.addTables(req, inPolling);
            }
        }

        /*
         * check schema of tables at source, if incompatible schema is
         * found or the table is missing, log the warning. Stream will start
         * and writes to incompatible tables will not be persisted till the
         * schema is fixed.
         */
        final RegionInfo source = config.getSource();
        final Set<Table> foundMD =
            tblsMD.stream()
                  .filter(t -> found.contains(t.getFullNamespaceName()))
                  .collect(Collectors.toSet());
        final Set<String> result = mdMan.matchingSchema(source, foundMD);
        if (!result.isEmpty()) {
            final String sb = reqPrefix(req) +
                              "Tables=" + result + " with incompatible" +
                              " schema or missing at remote region=" +
                              source.getName() + ", will be put to polling";
            logger.warning(lm(sb));
            tablePollingThread.addTables(req, inPolling);
        }

        logger.info(lm(reqPrefix(req) + "Tables found=" + found +
                       " at region=" + config.getSource().getName() +
                       (inPolling.isEmpty() ? "" :
                           ", tables in polling=" +
                           ServiceMDMan.getTrace(inPolling))));
        return new Pair<>(foundMD, inPolling);
    }

    /**
     * Closes all pending requests in shutdown
     */
    private void closePendingRequests() {

        /* drain the pending requests in the queue */
        final Set<RegionAgentReq> drainReqs = new HashSet<>();
        synchronized (msgQueue) {
            msgQueue.drainTo(drainReqs);
            /* no queue request will be processed */
            msgQueue.clear();
        }

        /* request ids of success and failure response */
        final Set<Long> succ = new HashSet<>();
        final Set<Long> fail = new HashSet<>();
        drainReqs.forEach(r -> {
            /* post success for remove table, otherwise post failure */
            if (r.getReqType().equals(ReqType.REMOVE)) {
                postSucc(r.getReqId(), r.getResp());
                succ.add(r.getReqId());
                return;
            }
            final IllegalStateException ise =
                new IllegalStateException("agent shutdown");
           postFail(r.getReqId(), r.getResp(), ise);
           fail.add(r.getReqId());
        });

        /* post success or failure for each request in the queue */
        final String msg =
            "Will close agent to region=" + getSourceRegion().getName() +
            (succ.isEmpty() ? ", no pending request to post succ" :
                ", post success to queued request #=" + succ.size() +
                ", ids=" + succ) +
            (fail.isEmpty() ? ", no pending request to post fail" :
                ", post failure to queued request #=" + fail.size() +
                ", ids=" + fail);
        logger.info(lm(msg));

        /* post failure msg for currently processing request */
        if (currentReq != null) {
            final long reqId = currentReq.getReqId();
            final XRegionRespHandlerThread resp = currentReq.getResp();
            /* post succ for remove table, otherwise post failure */
            if (currentReq.getReqType().equals(ReqType.REMOVE)) {
                postSucc(currentReq.getReqId(), resp);
                logger.info(lm(reqPrefix(currentReq) +
                               "Will close region to region=" +
                               getSourceRegion().getName() +
                               ", post succ for current reqId=" + reqId));
                return;
            }
            final String err =
                "Will close agent to region=" + getSourceRegion().getName() +
                ", post failure to current request id=" + reqId;
            logger.info(lm(reqPrefix(currentReq) + err));
            final IllegalStateException ise = new IllegalStateException(err);
            postFail(currentReq.getReqId(), resp, ise);
        }
    }

    /**
     * Returns table polling thread
     */
    public TablePollingThread getTablePollingThread() {
        return tablePollingThread;
    }

    /**
     * Unit test only
     */
    void setRemoteTableTimeoutMs(int val) {
        remoteTableTimeoutMs = val;
    }

    /**
     * RegionAgentReq type of region agent
     */
    public enum ReqType {

        /* stream a table */
        STREAM {
            @Override
            public String toString() {
                return "stream";
            }
        },

        /* initialize given tables */
        INITIALIZE_TABLES {
            @Override
            public String toString() {
                return "initialize tables";
            }
        },

        /* initialize all tables from a region */
        INITIALIZE_FROM_REGION {
            @Override
            public String toString() {
                return "initialize region";
            }
        },

        /* add a table */
        ADD {
            @Override
            public String toString() {
                return "add";
            }
        },

        /* remove a table */
        REMOVE {
            @Override
            public String toString() {
                return "remove";
            }
        },

        /* evolve a table */
        EVOLVE {
            @Override
            public String toString() {
                return "evolve";
            }
        }
    }

    /**
     * RegionAgentReq of region agent to execute
     */
    public static class RegionAgentReq {

        /* message type to represent the action to take */
        private final ReqType reqType;

        /* name of table in action */
        private final Set<Table> tables;

        /* response handler */
        private final XRegionRespHandlerThread resp;

        RegionAgentReq(ReqType reqType, Table... tables) {
            this.reqType = reqType;
            this.tables = new HashSet<>(Arrays.asList(tables));

            /* internal generated request, no need to post to response table */
            this.resp = null;
        }

        RegionAgentReq(ReqType reqType,
                       XRegionRespHandlerThread resp,
                       Set<Table> tables) {
            this.reqType = reqType;
            this.resp = resp;
            this.tables = tables;
        }

        @Override
        public String toString() {
            return "(id=" + getReqId() + ", type=" + reqType +
                   ", tables=" + tables + ")";
        }

        Set<Table> getTableMD() {
            return tables;
        }

        public Set<String> getTables() {
            if (tables == null) {
                return null;
            }
            return tables.stream().map(Table::getFullNamespaceName)
                         .collect(Collectors.toSet());
        }
        public XRegionRespHandlerThread getResp() {
            return resp;
        }

        public ReqType getReqType() {
            return reqType;
        }

        public long getReqId() {
            if (resp == null) {
                return NULL_REQUEST_ID;
            }
            return resp.getReqId();
        }
    }

    /**
     * RegionAgentReq of change parameters
     */
    private static class EvolveTableRegionReq extends RegionAgentReq {

        EvolveTableRegionReq(XRegionRespHandlerThread resp, Set<Table> tables) {
            super(ReqType.EVOLVE, resp, tables);
        }

        @Override
        public String toString() {
            return super.toString();
        }
    }

    private static class ShutDownRequest extends RegionAgentReq {
        ShutDownRequest() {
            super(null);
        }
    }


    /**
     * A KV thread factory that logs if a thread exits on an unexpected
     * exception.
     */
    private class RegionAgentThreadFactory extends KVThreadFactory {
        RegionAgentThreadFactory() {
            super("RegionAgentThreadFactory", logger);
        }
        @Override
        public UncaughtExceptionHandler makeUncaughtExceptionHandler() {
            return (thread, ex) ->
                logger.warning(lm("Thread=" + thread.getName() +
                                  " exit unexpectedly" +
                                  ", error=" + ex +
                                  ", stack=" + LoggerUtils.getStackTrace(ex)));
        }
    }

    /**
     * Lets polling thread to resume table transfer in separate threads from
     * the checkpoint
     */
    private Set<String> resumeTableInitialization(RegionAgentReq req) {
        /* check if any unfinished table initialization */
        final String src = getSourceRegion().getName();
        final Set<Table> tblsResume = mdMan.getTablesResumeInit(src);
        if (tblsResume.isEmpty()) {
            logger.info(lm(reqPrefix(req) +
                           "No table to resume initialization"));
            return Collections.emptySet();
        }
        logger.info(lm(reqPrefix(req) + "To resume initialization for " +
                       "tables=" + ServiceMDMan.getTrace(tblsResume)));

        /* delegate to polling thread to initialize to avoid block agent */
        tablePollingThread.addTables(req, tblsResume);
        return ServiceMDMan.getTbNames(tblsResume);
    }

    /**
     * Resets init checkpoint for given tables. Delete checkpoint if tables
     * have been dropped.
     *
     * @param tables tables to reset the checkpoint
     * @return set of tables for which the checkpoint are reset.
     */
    Set<Table> resetInitCkpt(Set<Table> tables) {
        return resetInitCkpt(tables, true/* overwrite*/);
    }

    /**
     * Resets init checkpoint for given tables. Delete checkpoint if tables
     * have been dropped.
     *
     * @param tables tables to reset the checkpoint
     * @param overwrite true if overwrite existing checkpoint
     *
     * @return set of tables for which the checkpoint are reset.
     */
    private Set<Table> resetInitCkpt(Set<Table> tables, boolean overwrite) {
        if (tables == null || tables.isEmpty()) {
            return tables;
        }
        final Set<Table> ret = new HashSet<>();
        final String src = getSourceRegion().getName();
        for (Table t : tables) {
            if (belongsToMe(t.getFullNamespaceName(), src)) {
                /* tables belongs to me */
                if (!overwrite && tableInitCkptExists(t, src)) {
                    /* no overwrite, find existing ckpt, will use it later */
                    ret.add(t);
                    continue;
                }
                final long tid = ((TableImpl) t).getId();
                mdMan.writeCkptRetry(src, t.getFullNamespaceName(), tid, null,
                                     NOT_START);
                ret.add(t);
            }
        }
        return ret;
    }

    /**
     * Returns true if the table initialization checkpoint exists for given
     * table and region name
     *
     * @param tb table instance. Check the table id in the checkpoint.
     * @param region region name
     * @return true if checkpoint exists, false otherwise
     */
    private boolean tableInitCkptExists(Table tb, String region) {
        TableInitCheckpoint ckpt =
            mdMan.readTableInitCkpt(region, tb.getFullNamespaceName());
        if (ckpt == null) {
            /* try read checkpoint from peer agent */
            ckpt = mdMan.readCkptAnyAgent(tb.getFullNamespaceName(), region);
        }
        if (ckpt == null) {
            logger.fine(() -> lm("No checkpoint found for table=" +
                                 ServiceMDMan.getTrace(tb) +
                                 ", source region=" + region));
            return false;
        }

        /* check if table id match */
        final long tid = ((TableImpl) tb).getId();
        if (ckpt.getTableId() == tid) {
            final boolean fromMe =
                ckpt.getAgentId().equals(config.getSubscriberId().toString());
            logger.info(lm("Will reuse checkpoint for table=" +
                           ServiceMDMan.getTrace(tb) +
                           ", source region=" + region +
                           (fromMe ? "" :
                               " from another agent=" + ckpt.getAgentId()) +
                           ", checkpoint=" + ckpt));
            return true;
        }
        return false;
    }

    /**
     * Drops init checkpoint for given tables
     */
    private void delInitCkpt(Set<String> tables, RegionAgentReq req) {
        final String src = getSourceRegion().getName();
        tables.forEach(t -> mdMan.delTableInitCkpt(src, t));
        logger.fine(() -> lm(reqPrefix(req) +
                             "Delete init checkpoint for tables=" + tables));
    }

    /**
     * Returns true if the table already in polling thread
     */
    boolean inPolling(Table table) {
        final long tid = ((TableImpl) table).getId();
        if (tablePollingThread.inPolling(tid)) {
            logger.info(lm("Table=" + ServiceMDMan.getTrace(table) +
                           " in polling thread check list"));
            return true;
        }
        if (tablePollingThread.inTrans(tid)) {
            logger.info(lm("Table=" + ServiceMDMan.getTrace(table) +
                           " submitted to transfer in polling thread"));
            return true;
        }
        return false;
    }

    /**
     * Returns true if the table transfer is already complete
     */
    private boolean transComplete(long reqId, Table tb) {
        final String table = tb.getFullNamespaceName();
        try {
            final TableInitCheckpoint ckpt =
                getMdMan().readTableInitCkpt(config.getSource().getName(),
                                             table);
            if (ckpt == null) {
                return false;
            }
            /* verify table id */
            final long tid = ((TableImpl) tb).getId();
            if (tid <= 0) {
                logger.info(lm(logPrefix(reqId) +
                               "Table=" + ServiceMDMan.getTrace(tb) +
                               ", table id in checkpoint=" + ckpt.getTableId() +
                               ", has been dropped in local region, delete " +
                               "checkpoint"));
                mdMan.delTableInitCkpt(getSourceRegion().getName(), table);
                return false;
            }
            if (tid != ckpt.getTableId()) {
                logger.info(lm(logPrefix(reqId) +
                               "Mismatch table id for table=" +
                               ServiceMDMan.getTrace(tb) +
                               ", in checkpoint table id=" + ckpt.getTableId() +
                               ", reset checkpoint"));
                resetInitCkpt(Collections.singleton(tb));
                return false;
            }
            if (COMPLETE.equals(ckpt.getState())) {
                logger.info(lm(logPrefix(reqId) +
                               "Transfer already complete for table=" +
                               ServiceMDMan.getTrace(tb)));
                return true;
            }
        } catch (IllegalStateException ise) {
            logger.warning(lm(logPrefix(reqId) +
                              "Cannot read the checkpoint for table=" +
                              ServiceMDMan.getTrace(tb) +
                              ", error=" + ise));
        }
        return false;
    }

    /**
     * Returns log prefix using request id
     */
    private String reqPrefix(RegionAgentReq req) {
        if (req == null) {
            return "";
        }
        return logPrefix(req.getReqId());
    }

    private String logPrefix(long requestId) {
        return "[reqId=" + requestId + "] ";
    }

    /** Returns true if agent should retry on the given exception */
    private boolean shouldRetry(PublisherFailureException pfe) {
        /* non-null cause */
        if (pfe.getCause() instanceof FaultException ||
            pfe.getCause() instanceof KVStoreException ) {
            return true;
        }
        /* exception may have cause=null if remote, get fault class name */
        final String faultClass = pfe.getFaultClassName();
        /* if cause is null and is fault or kvstore exception */
        return faultClass != null &&
               (faultClass.equals(FaultException.class.getName()) ||
                faultClass.equals(KVStoreException.class.getName()));
    }

    private String getPFECauseMsg(PublisherFailureException pfe) {
        final Throwable cause = pfe.getCause();
        if (cause != null) {
            return "cause=" + cause + "\n" +
                   LoggerUtils.getStackTrace(cause);
        }
        return pfe.toString();
    }

    /* handle the situation that no table is found at remote region */
    private void handleNoTableFound(RegionAgentReq req,
                                    Set<Table> tables,
                                    Set<Table> inPolling) {
        /* no table is found, skip creating stream */
        String err = reqPrefix(req) +
                     "Cannot find any tables=" + ServiceMDMan.getTrace(tables) +
                     " at region=" + config.getSource().getName();
        if (!inPolling.isEmpty()) {
            status = RegionAgentStatus.INITIALIZING_TABLES;
            err += ", to initialize by polling thread for tables=" +
                   ServiceMDMan.getTrace(inPolling) +
                   " in local region, agent status=" +
                   status;
            logger.info(lm(err));
            /*
             * report success since the tables not found at remote regions
             * are in polling thread and will be initialized later
             */
            postSucc(req.getReqId(), req.getResp());
        } else {
            err += ", and all tables dropped in local region," +
                   " agent status unchanged=" + status;
            logger.info(lm(err));
            /* no table found in both remote and local regions */
            postFail(req.getReqId(), req.getResp(),
                     new IllegalStateException(err));
        }
    }

    private RegionAgentReq getNextRequest() throws InterruptedException {

        /* First check if region init is needed */
        if (initRegion.compareAndSet(true, false)) {
            logger.info(lm("Region initialization is needed and should " +
                           "precede any other request."));
            /*
             * This is an internally generated request that the agent has to
             * complete before it is able to service  requests from kvstore.
             * The current request field is unchanged.
             */
            return new RegionAgentReq(ReqType.INITIALIZE_FROM_REGION,
                                      tgtTables.values().toArray(new Table[0]));
        }

        /* Next check if it needs to retry current request */
        if (retryRequest.compareAndSet(true, false)) {
            numRetry.incrementAndGet();
            logger.info(lm("Retry current request=" + currentReq +
                           ", numRetry=" + numRetry +
                           ", maxRetry=" + MAX_REQUEST_RETRY));
            return currentReq;
        }

        /* Finally, dequeue the next request */
        numRetry.set(0);
        currentReq = msgQueue.poll(MSG_QUEUE_POLL_TIMEOUT_MS,
                                   TimeUnit.MILLISECONDS);
        return currentReq;
    }

    private boolean needRetry() {
        return numRetry.get() < MAX_REQUEST_RETRY;
    }

    private void retryCurrRequest() {
        retryRequest.set(true);
    }

    /**
     * Adds a table to the stream. Create a stream if the stream is not on.
     * This is usually called from another thread like
     * {@link TablePollingThread}, and it needs to be synchronized with
     * {@link #startStream(NoSQLStreamMode, Set, RegionAgentReq)} and
     * {@link #changeStream(XRegionRespHandlerThread, Table, ReqType)}
     * @param table name of table to be added
     * @return the stream position where the table is added to the stream
     */
    synchronized StreamPosition addTableHelper(Table table) {
        try {
            if (isStreamOn(NULL_REQUEST_ID)) {
                return changeStream(null/* no response needed */, table,
                                    RegionAgentThread.ReqType.ADD);
            }

            /* stream already closed, need to create a new one */
            final NoSQLStreamMode mode =
                RegionAgentThread.INIT_TABLE_STREAM_MODE;
            if(startStream(mode, Collections.singleton(table), null)) {
                logger.info(lm("Create a new stream from=" +
                               getSourceRegion().getName() +
                               " for table=" + table.getFullNamespaceName() +
                               " with mode=" + mode));
                return getSubscriber().getSubscription().getCurrentPosition();
            }
        } catch (ChangeStreamException cse) {
            final String err = "Fail to add table=" + cse.getTable() +
                               ", reqId=" + cse.getReqId() +
                               ", error=" + cse.getError();
            logger.warning(lm(err));
        } catch (InterruptedException e) {
            final String err = "Interrupted in adding table=" +
                               table.getFullNamespaceName();
            logger.warning(lm(err));
        }
        return null;
    }

    /**
     * Returns true if there is no duplicate internal request for given table
     * and type
     */
    private boolean existInternalReq(String tableName, ReqType type) {
        final boolean existInQueue =
            msgQueue.stream().anyMatch(req -> matchReq(req, tableName, type));
        if (existInQueue) {
            /* found duplicate */
            return true;
        }
        /* not found in the queue, check current request */
        return matchReq(currentReq, tableName, type);
    }

    private boolean matchReq(RegionAgentReq req, String table, ReqType type) {
        if (req == null) {
            return false;
        }
        return req.getReqId() == 0 &&
               req.getResp() == null &&
               req.getReqType().equals(type) &&
               req.getTables().contains(table);
    }

    private void recordLocalTableIdIfNeeded(RegionAgentReq req) {
        final ReqType type = req.getReqType();
        final long reqId = req.getReqId();
        final Set<Table> tables = req.getTableMD();
        switch (type) {
            /* need to record local table id */
            case ADD:
            case STREAM:
            case INITIALIZE_TABLES:
            case INITIALIZE_FROM_REGION:
                tables.forEach(t -> recordLocalTableId(reqId, t));
                break;

            /* no need to change table id */
            case REMOVE:
            case EVOLVE:
            default:
                break;
        }
    }

    private void recordLocalTableId(long reqId, Table tb) {
        final long tid = ((TableImpl) tb).getId();
        final String table = tb.getFullNamespaceName();
        final String localRegion = mdMan.getServRegion().getName();
        if (mdMan.getRecordedTableId(table, localRegion) == tid) {
            /* id can be recorded by other region agent */
            logger.fine(() -> lm(logPrefix(reqId) + "Table=" + table +
                              " id=" + tid + " already recorded"));
            return;
        }
        if (mdMan.recordLocalTableId(table, tid)) {
            logger.info(lm(logPrefix(reqId) +
                           "Recorded id=" + tid + " for table=" + table +
                           " at local region"));
        } else {
            logger.warning(lm(logPrefix(reqId)  +
                              "Failed to record id=" + tid +
                              " for table=" + table + " at local region"));
        }
    }

    /**
     * Gets table from target table list, or null if not exist
     * @param tb name of table
     * @return table instance
     */
    public Table getTgtTable(String tb) {
        return tgtTables.get(tb);
    }

    /**
     * Adds table to target table list if table is newer
     * @param reqId request id
     * @param table table instance
     */
    private void addTableToTgtList(long reqId, Table table) {
        final String tbName = table.getFullNamespaceName();
        final TableImpl curr = (TableImpl) tgtTables.get(tbName);
        if (curr == null ||
            curr.getTableVersion() < table.getTableVersion()) {
            tgtTables.put(tbName, table);
            logger.info(lm(logPrefix(reqId) +
                           "Add or update table=" +
                           ServiceMDMan.getTrace(table) +
                           " to target table list," +
                           (curr == null ? "" : ", curr table=" +
                                                ServiceMDMan.getTrace(curr))));
        }
    }

    private void removeTableFromTgtList(long reqId, String tbName) {
        if (tgtTables.remove(tbName) != null) {
            logger.info(lm(logPrefix(reqId) + "Table=" + tbName + " removed " +
                           "from target table list"));
        }
    }

    public Set<String> getTgtTableNames() {
        return tgtTables.keySet();
    }

    public Collection<Table> getTgtTables() {
        return tgtTables.values();
    }

    private void updatePendingEvolveTable(RegionAgentReq req) {
        if (!req.getReqType().equals(ReqType.EVOLVE)) {
            return;
        }

        final long reqId = req.getReqId();
        final Set<Table> tbs = req.getTableMD();
        tbs.forEach(t -> {
            final long tableId = ((TableImpl) t).getId();
            final Table exist = pendingEvolveTable.get(tableId);
            if (exist == null) {
                pendingEvolveTable.put(tableId, t);
                logger.info(lm(logPrefix(reqId) +
                               "Add a pending evolve table=" +
                               ServiceMDMan.getTrace(t)));
                return;
            }

            /* only update the table md if newer than existent */
            if (t.getTableVersion() > exist.getTableVersion()) {
                pendingEvolveTable.put(tableId, t);
                logger.info(lm(logPrefix(reqId) +
                               "Update a pending evolve table " +
                               "from =" + ServiceMDMan.getTrace(exist) +
                               " to=" + ServiceMDMan.getTrace(t)));
            }
        });
    }

    private void removePendingEvolveTable(RegionAgentReq req) {
        if (!req.getReqType().equals(ReqType.EVOLVE)) {
            return;
        }

        final long reqId = req.getReqId();
        final Set<Table> tbs = req.getTableMD();
        tbs.forEach(t -> {
            final long tableId = ((TableImpl) t).getId();
            final Table exist = pendingEvolveTable.get(tableId);
            if (exist == null) {
                logger.fine(() -> lm(logPrefix(reqId) +
                                     "Pending evolve table=" +
                                     ServiceMDMan.getTrace(t) +
                                     " has been removed"));
                return;
            }

            /*
             * only remove the table md if stored table version is equal
             * or lower than that in request. If store table version is
             * higher, that means the stored table is added a later table
             * evolve request
             */
            if (exist.getTableVersion() <= t.getTableVersion()) {
                pendingEvolveTable.remove(tableId);
                logger.info(lm(logPrefix(reqId) +
                               "Remove a pending evolve table=" +
                               ServiceMDMan.getTrace(exist) +
                               ", table in request=" +
                               ServiceMDMan.getTrace(t)));
            }
        });
    }

    public Table getPendingEvolveTable(long tid) {
        return pendingEvolveTable.get(tid);
    }

    /**
     * Returns true if the table from given region should be initialized by
     * this agent, false if by another agent
     */
    boolean belongsToMe(String table, String region) {
        final NoSQLSubscriberId sid = config.getSubscriberId();
        final int group = sid.getTotal();
        final int index = computeTableInitIndex(table, region, group);
        if (index != sid.getIndex()) {
            logger.info(lm("Table=" + table + " from region=" + region +
                           " will be initialized in a different agent " +
                           "index=" + index));
            return false;
        }
        return true;
    }

    /**
     * Computes the index of subscriber from table name and source region name
     * @param tableName table name
     * @param region    source region
     * @param groupSize group size
     * @return index of subscriber
     */
    private int computeTableInitIndex(String tableName,
                                      String region,
                                      int groupSize) {

        if (testTableInitIndex != null) {
            /* unit test only */
            logger.info(lm("Return index set in unit test, index=" +
                           testTableInitIndex));
            return testTableInitIndex;
        }

        final String str = tableName + region;
        return Math.abs(str.toLowerCase().hashCode()) % groupSize;
    }

    /**
     * Builds start stream position from checkpoints made by other agents. If
     * the stream mode does not require a checkpoint, or a checkpoint table
     * from the same agent is found, returns null. Otherwise, returns a stream
     * position constructed from existing checkpoint tables by other agents
     *
     * @param mode stream mode
     * @return a start stream position built from other agents, or null
     */
    private StreamPosition buildStartPos(NoSQLStreamMode mode) {
        if (!mode.needCheckpoint()) {
            logger.info(lm("No checkpoint is required with mode=" + mode));
            return null;
        }

        /* see if my checkpoint table exists */
        final RegionInfo srcRegion = config.getSource();
        final String region = config.getSource().getName();
        final String hostName = config.getHost().getName();
        final NoSQLSubscriberId sid = config.getSubscriberId();
        final String ckptName =
            StreamCheckpointName.buildCkptTableName(region, hostName, sid);
        final KVStore kvs = mdMan.getRegionKVS(srcRegion);
        final TableAPI tapi = kvs.getTableAPI();
        final Table tb = tapi.getTable(ckptName);
        if (tb != null) {
            logger.info(lm("Found checkpoint table from same agent id=" + sid +
                           " table=" + tb.getFullNamespaceName() +
                           ", stream mode=" + mode));
            return null;
        }

        logger.info(lm("Cannot find checkpoint table from same agent id=" +
                       sid + ", need construct stream start position from " +
                       "checkpoints by other agents"));
        /* read checkpoints from peer agents in local region */
        final Collection<Table> allTbs =
            MRTSubscriberCkptMan.getAllCkptTables(kvs);
        final Set<Table> ckptTables =
            allTbs.stream()
                  .filter(t -> peerCkptTable(t.getFullNamespaceName(),
                                             srcRegion.getName(), hostName))
                  .collect(Collectors.toSet());
        /* build checkpoint stream position from peer checkpoint tables */
        final StreamPosition pos = buildStartPosition(sid, ckptTables);
        logger.info(lm("Constructed stream start position=" + pos +
                       ", stream mode=" + mode +
                       " from peer checkpoint tables=" +
                       ckptTables.stream()
                                 .map(Table::getFullNamespaceName)
                                 .collect(Collectors.toSet())));
        return pos;
    }

    /**
     * Builds stream position for given subscriber from a set of checkpoint
     * tables, or null if the stream position is not available from the given
     * checkpoint tables, e.g., the checkpoint tables not exist, or do not
     * cover the shard of this subscriber.
     *
     * @param sid  agent subscriber id
     * @param tables set of checkpoint tables
     * @return constructed stream position, or null
     */
    private StreamPosition buildStartPosition(NoSQLSubscriberId sid,
                                              Set<Table> tables) {
        if (tables.isEmpty()) {
            logger.info(lm("No existing stream checkpoint table found " +
                           "from region=" + config.getSource().getName()));
            return null;
        }

        final KVStoreImpl kvs =
            (KVStoreImpl) mdMan.getRegionKVS(config.getSource());
        final Topology topo = kvs.getTopology();
        final Set<RepGroupId> all = topo.getRepGroupIds();
        final Set<RepGroupId> shards =
            NoSQLSubscriptionConfig.computeShards(sid, all);
        if (shards.isEmpty()) {
            throw new IllegalStateException("Covered shards cannot be empty, " +
                                            "id=" + sid);
        }
        return buildPos(shards, tables);
    }

    /**
     * Builds a stream position from a set of checkpoint tables. Use the most
     * recent checkpoint if there are multiple checkpoint tables with the
     * checkpoint row for s shard, or returns null if cannot build the
     * checkpoint from the checkpoint tables.
     *
     * @param shards set of covered shard ids
     * @param tbs set of checkpoint tables
     * @return constructed stream position, or null.
     */
    private StreamPosition buildPos(Set<RepGroupId> shards, Set<Table> tbs) {

        final KVStore kvs = mdMan.getRegionKVS(config.getSource());

        /* a map from shard id to a pair of vlsn and timestamp */
        final Map<RepGroupId, Pair<Long, Long>> pos = new HashMap<>();
        tbs.forEach(t -> {
            final List<Row> rows =
                CheckpointTableManager.getRowsFromCkptTable(kvs, t);
            for (Row row : rows) {
                if (CheckpointTableManager.isRowFromElasticOps(row)) {
                    /* ignore internal checkpoint rows from elastic ops */
                    continue;
                }
                final RepGroupId gid =
                    CheckpointTableManager.getRepGroupIdFromRow(row, false);
                if (!shards.contains(gid)) {
                    /* not my shard */
                    continue;
                }

                final long vlsn =
                    CheckpointTableManager.getVLSNFromRow(row);
                final long timestamp =
                    CheckpointTableManager.getTimestampFromRow(row);
                final Pair<Long, Long> exist = pos.get(gid);
                if (exist == null ||
                    timestamp > exist.second() /* newer checkpoint */) {
                    final Pair<Long, Long> val = new Pair<>(vlsn, timestamp);
                    pos.put(gid, val);
                }
            }
        });

        if (pos.isEmpty()) {
            logger.info(lm("Peer checkpoint tables " +
                           tbs.stream().map(Table::getFullNamespaceName)
                              .collect(Collectors.toSet()) +
                           "do not have checkpoint for " +
                           "my shards=" + shards));
            return null;
        }

        final Topology topo = ((KVStoreImpl) kvs).getTopology();
        final String store = topo.getKVStoreName();
        final long id = topo.getId();
        final StreamPosition ret = new StreamPosition(store, id);
        pos.keySet().forEach(gid -> ret.setShardPosition(gid.getGroupId(),
                                                         pos.get(gid).first()));
        return ret;
    }

    /**
     * Returns true if the table is a checkpoint table made by an agent in my
     * region, false otherwise.
     */
    private boolean peerCkptTable(String table,
                                  String srcRegion,
                                  String hostName) {
        final MRTSubscriberCkptMan.StreamCheckpointName name;
        try {
            name = new MRTSubscriberCkptMan.StreamCheckpointName(table);
        } catch (IllegalArgumentException iae) {
            logger.info(lm("Cannot parse the table name=" + table +
                           ", error=" + iae));
            return false;
        }
        return name.peerCkpt(srcRegion, hostName);
    }

    /**
     * Returns true if the table is streaming in all other agents and ready
     * to initialize, or false otherwise
     * @param table table to initialize
     * @return true if ready to initialize, or false.
     */
    private boolean readyToSnapshot(Table table) {
        if (config.getSubscriberId().isSingleAgentGroup()) {
            /* single agent group, no need to check */
            return true;
        }
        final Set<Integer> agents = streamingAgents(table);
        final Set<Integer> remain = getComplementAgents(agents);
        if (!remain.isEmpty()) {
            logger.info(lm("Table=" + ServiceMDMan.getTrace(table) +
                           " not ready for transfer, streaming not " +
                           "started on agents=" + remain));
            return false;
        }
        return true;
    }

    /**
     * Records the tables in streaming from remote region
     * @param reqId request id
     */
    private void recordStreamTable(long reqId) {
        if (config.getSubscriberId().isSingleAgentGroup()) {
            /* no need to record table in streaming if single agent */
            return;
        }
        final NoSQLSubscriptionImpl stream =
            (NoSQLSubscriptionImpl) subscriber.getSubscription();
        final ReqRespManager reqMan = mdMan.getParent().getReqRespMan();
        final String src = getSourceRegion().getName();
        if (!isStreamOn(reqId)) {
            reqMan.updateStreamTables(src, Collections.emptySet());
            logger.info(lm(logPrefix(reqId) +
                           "Stream is not on, remove all stream tables from " +
                           "response table"));
            return;
        }
        final Set<Table> tbs = getStreamTgtTableId(stream);
        final Set<Long> tableIds = tbs.stream()
                                      .map(t -> ((TableImpl) t).getId())
                                      .collect(Collectors.toSet());
        reqMan.updateStreamTables(src, tableIds);
        logger.info(lm(logPrefix(reqId) +
                       "Update stream tables in response table" +
                       ", agent id=" + config.getSubscriberId() +
                       ", tables=" + ServiceMDMan.getTrace(tbs)));
    }

    /**
     * Returns a set of agent indices streaming the given table, or empty set
     * if no agent is streaming the table.
     * @param table table in streaming
     * @return a set of agent indices streaming the given table
     */
    Set<Integer> streamingAgents(Table table) {
        Set<Integer> ret = Collections.emptySet();
        final long tid = ((TableImpl) table).getId();
        try {
            final ReqRespManager reqMan = mdMan.getParent().getReqRespMan();
            final String region = getSourceRegion().getName();
            ret = reqMan.getStreamAgentIdx(tid, region);
        } catch (IOException ioe) {
            logger.info(lm("Error in checking streams for table=" +
                           ServiceMDMan.getTrace(table) +
                           ", err=" + ioe));
        }
        return ret;
    }

    Set<Integer> getComplementAgents(Set<Integer> agents) {
        return  IntStream.range(0, config.getSubscriberId().getTotal()).boxed()
                         .filter(idx -> !agents.contains(idx))
                         .collect(Collectors.toSet());

    }

    private  Set<Table> getStreamTgtTableId(NoSQLSubscriptionImpl stream) {
        final Set<Table> ret = new HashSet<>();
        final Set<String> tbNames = stream.getSubscribedTables();
        for (String tb : tbNames) {
            final Table tbl = getTgtTable(tb);
            if (tbl == null) {
                logger.info(lm("Table=" + tb + " has been removed from " +
                               "target table list, will remove from stream " +
                               "soon"));
                continue;
            }
            ret.add(tbl);
        }
        return ret;
    }
}