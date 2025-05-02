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


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.agent.mrt.MRTSubscriber;
import oracle.kv.impl.xregion.agent.mrt.MRTTableTransferThread;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.service.XRegionRequest;
import oracle.kv.table.Table;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * A child thread of {@link TablePollingThread} that monitors initialization
 * of tables in polling thread. The tables to initialize are queued and
 * processed in FIFO mode. Each table initialization has a time limit and
 * when timeout, it would be terminated and put back to the parent polling
 * thread to be processed in next turn.
 */
class TablePollingTableInitThread extends StoppableThread {

    /**
     * max number of table transfer in the queue
     */
    private static final int MAX_TRANS_QUEUE_SIZE = 1024 * 10;
    /**
     * waiting time in ms for dequeue when queue is empty
     */
    private static final int DEQUEUE_RETRY_WAIT_MS = 1000;
    /**
     * rate limiting log period in ms
     */
    private static final int RL_LOG_PERIOD_MS = 30 * 1000;
    /**
     * max objects in rate limiting logger
     */
    private static final int RL_LOG_MAX_OBJ = 1024;
    /**
     * Default request id
     */
    private static final long REQUEST_ID = XRegionRequest.NULL_REQUEST_ID;

    /**
     * Parent table polling thread
     */
    private final TablePollingThread parent;
    /**
     * Region agent the thread belongs to
     */
    private final RegionAgentThread regionAgent;
    /**
     * Source region
     */
    private final RegionInfo source;
    /**
     * private logger
     */
    private final Logger logger;
    /**
     * rate limiting logger
     */
    private final RateLimitingLogger<String> rlLogger;
    /**
     * true if shut down is requested
     */
    private final AtomicBoolean shutdownRequested;
    /**
     * executor for scheduled task
     */
    private final ScheduledExecutorService ec;
    /**
     * FIFO queue of id of the table waiting to be transferred
     */
    private final BlockingQueue<Long> transferQueue;
    /**
     * Map from table id to table instance for tables in the queue
     */
    private final Map<Long, Table> tables;

    /**
     * table being transferred
     */
    private volatile Table tablesInTrans;

    TablePollingTableInitThread(TablePollingThread parent, Logger logger) {
        super("TablePollingTableInitThread-" + parent.getAgentId());
        this.parent = parent;
        this.logger = logger;
        rlLogger = new RateLimitingLogger<>(RL_LOG_PERIOD_MS, RL_LOG_MAX_OBJ,
                                            logger);
        regionAgent = parent.getParent();
        source = parent.getSourceRegion();
        shutdownRequested = new AtomicBoolean(false);
        transferQueue = new ArrayBlockingQueue<>(MAX_TRANS_QUEUE_SIZE);
        tables = Collections.synchronizedMap(new HashMap<>());
        final TablePollingTableInitThreadFactory
            factory = new TablePollingTableInitThreadFactory();
        ec = Executors.newSingleThreadScheduledExecutor(factory);
        tablesInTrans = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected int initiateSoftShutdown() {
        logger.fine(() -> lm("Wait for thread " + getName() + " to shutdown" +
                             " in time(ms)=" +
                             TablePollingThread.SOFT_SHUTDOWN_WAIT_MS));
        return TablePollingThread.SOFT_SHUTDOWN_WAIT_MS;
    }

    @Override
    public void run() {

        logger.info(lm("Table initialization thread starts up" +
                       ", remote region=" + source.getName() +
                       ", agent id=" + regionAgent.getAgentId()));

        try {
            while (!isShutdownRequested()) {

                final Table table = dequeue();
                if (table == null) {
                    final String msg = "No table transfer in queue, retry " +
                                       "after waitMs=" + DEQUEUE_RETRY_WAIT_MS;
                    rlLogger.log(msg, Level.FINEST, () -> lm(msg));
                    synchronized (this) {
                        wait(DEQUEUE_RETRY_WAIT_MS);
                    }
                    continue;
                }

                /* create a table transfer thread */
                final MRTTableTransferThread transfer =
                    createTableTransfer(table);
                /* start transfer */
                final long startTs = System.currentTimeMillis();
                final long timeoutMs = transfer.getTimeoutMs();
                logger.info(lm("Start initializing table=" +
                               ServiceMDMan.getTrace(table) +
                               " from region=" + source.getName() +
                               ", timeoutMs=" + timeoutMs));
                ec.submit(transfer);
                tablesInTrans = table;

                /*
                 * Wait for the table transfer to 1) timeout, or 2) complete
                 * or 3) fail due to error or 4) shutdown requested;
                 */
                final boolean noTimeout =
                    regionAgent.waitForTransferToComplete(
                        REQUEST_ID, startTs, transfer, rlLogger,
                        this::isShutdownRequested);

                /* a quick check of shutdown */
                if (isShutdownRequested()) {
                    logger.info(lm("In shutdown, stop transfer table=" +
                                   ServiceMDMan.getTrace(table)));
                    break;
                }

                if (!noTimeout) {
                    /* timeout in transfer, add to polling thread */
                    logger.info(lm("TimeoutMs=" + transfer.getTimeoutMs() +
                                   " in transfer of table=" +
                                   ServiceMDMan.getTrace(table) +
                                   ", add back to the parent polling thread " +
                                   "for the next turn"));
                    regionAgent.stopTableTransfer(transfer);
                    parent.addTables(null, Collections.singleton(table));
                    continue;
                }

                if (transfer.hasFailed()) {
                    final String msg = "Fail to transfer table=" +
                                       ServiceMDMan.getTrace(table) +
                                       " from region=" + source.getName() +
                                       ", error=" + transfer.getCause();
                    logger.warning(lm(msg));
                    tablesInTrans = null;
                    continue;
                }

                /* transfer complete */
                final String msg = "Complete transfer table=" +
                                   ServiceMDMan.getTrace(table) +
                                   " from region=" + source.getName() +
                                   ", elapsedMs=" +
                                   (System.currentTimeMillis() - startTs);
                tablesInTrans = null;
                logger.info(lm(msg));
            }
        } catch (InterruptedException ie) {
            logger.warning(lm("Interrupted in sleeping, will exit"));
        } finally {
            close();
            logger.info(lm("Table initialization thread exits" +
                           ", remote region=" + source.getName() +
                           ", agent id=" + regionAgent.getAgentId()));
        }
    }

    Table getTablesInTrans() {
        return tablesInTrans;
    }

    boolean tableInQueue(long tid) {
        return transferQueue.contains(tid);
    }

    synchronized private Table dequeue() {
        final Long tid = transferQueue.poll();
        if (tid == null) {
            /* empty queue */
            return null;
        }

        final Table removed = tables.remove(tid);
        if (removed == null) {
            final String msg = "Table with id=" + tid + " does not exist " +
                               "in the map";
            logger.warning(lm(msg));
            throw new IllegalStateException(msg);
        }
        /* return the table and remove it from map */
        return removed;
    }

     synchronized boolean enqueue(Table table) {

         if (table == null) {
             return true;
         }
         final long tid = ((TableImpl) table).getId();
         if (transferQueue.offer(tid)) {
             /*
              * add table to the map; a table should not be queued more
              * than once, check to ensure the table does not exist in map
              */
             final Table previous = tables.put(tid, table);
             if (previous != null) {
                 final String msg = "Table with id=" + tid + " already exists" +
                                    " in the map";
                 logger.warning(lm(msg));
                 throw new IllegalStateException(msg);
             }

             logger.finest(() -> lm("Table=" + ServiceMDMan.getTrace(table) +
                                    " is enqueued to transfer"));
             return true;
         }

         /* queue is full */
         rlLogger.log(table.getFullNamespaceName(), Level.INFO,
                      lm("Unable enqueue table=" +
                         ServiceMDMan.getTrace(table)));
         return false;
     }

    /**
     * Stops the thread from another thread without error
     */
    void shutdown() {
        if (!shutdownRequested.compareAndSet(false, true)) {
            logger.fine(() -> lm("Thread already shut down"));
            return;
        }
        final long ts = System.currentTimeMillis();
        close();
        shutdownThread(logger);
        logger.info(lm("Table init thread shut down in time(ms)=" +
                       (System.currentTimeMillis() - ts)));
    }

    /*--------------------*
     * Private functions  *
     *--------------------*/
    private String lm(String msg) {
        /* a child thread of parent thread */
        return "[" + parent.getLogHeader() + ".TableInit] " + msg;
    }

    /***
     * Closes and free all resources
     */
    private void close() {
        if (ec != null && !ec.isShutdown()) {
            ec.shutdownNow();
            logger.fine(() -> lm("Executor shutdown"));
        }
    }

    private boolean isShutdownRequested() {
        return shutdownRequested.get();
    }

    /**
     * Creates a thread to transfer the table
     */
    private MRTTableTransferThread createTableTransfer(Table table) {
        final RegionInfo target = parent.getTarget();
        final RegionAgentThread ra = parent.getParent();
        final ServiceMDMan mdMan = ra.getMdMan();
        final TableAPIImpl srcAPI =
            (TableAPIImpl) mdMan.getRegionKVS(source).getTableAPI();
        final TableAPIImpl tgtAPI =
            (TableAPIImpl) mdMan.getRegionKVS(target).getTableAPI();
        final MRTSubscriber sub = ra.getSubscriber();
        final long timeoutMs = ra.getTimeoutMs(null);
        final MRTTableTransferThread transfer =
            new MRTTableTransferThread(
                ra, sub, table, source, target, srcAPI, tgtAPI,
                timeoutMs, logger);
        /* avoid confuse itself as duplicate */
        transfer.skipCheckRedundant();

        return transfer;
    }

    /**
     * A KV thread factory that logs if a thread exits on an unexpected
     * exception.
     */
    private class TablePollingTableInitThreadFactory extends KVThreadFactory {
        TablePollingTableInitThreadFactory() {
            super("TablePollingTableInitThreadFactory", logger);
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
}
