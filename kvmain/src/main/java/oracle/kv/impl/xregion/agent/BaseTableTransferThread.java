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
import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_BATCH_SIZE_PER_REQUEST;
import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_ROWS_REPORT_PROGRESS_INTV;
import static oracle.kv.impl.xregion.service.JsonConfig.DEFAULT_THREADS_TABLE_ITERATOR;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.COMPLETE;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.ERROR;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.IN_PROGRESS;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.NOT_START;
import static oracle.kv.impl.xregion.stat.TableInitStat.TableInitState.SHUTDOWN;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.test.ExceptionTestHookExecute;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.Pair;
import oracle.kv.impl.util.ThreadUtils;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.init.TableInitCheckpoint;
import oracle.kv.impl.xregion.service.MRTableMetrics;
import oracle.kv.impl.xregion.service.RegionInfo;
import oracle.kv.impl.xregion.service.ServiceMDMan;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * Object represents the base class of table transfer thread
 */
abstract public class BaseTableTransferThread extends StoppableThread {

    /**
     * A test hook that can be used by test during table transfer, the
     * parameter is pair of 1) the name of table being transferred and 2) the
     * number of transferred rows in that table.
     */
    public static volatile TestHook<Pair<String, Long>>
        transInProgressHook = null;

    /**
     * unit test only, test hook to generate failure. The hook will be called
     * after writing a transferred row from source to the target store. The
     * arguments pass the number of transferred rows and exception thrown if
     * the hook is fired.
     */
    public static volatile ExceptionTestHook<Long, Exception> expHook = null;

    /** soft shutdown waiting time in ms */
    private static final int SOFT_SHUTDOWN_WAIT_MS = 5000;

    /** time interval in ms to report table initialization progress */
    private static final long TABLE_INIT_REPORT_INTV_MS = 30 * 60 * 1000;

    /** private logger */
    protected final Logger logger;

    /** parent region agent */
    protected final RegionAgentThread parent;

    /** local table md to initialize, can be refreshed */
    protected volatile TableImpl table;

    /** source region */
    protected final RegionInfo srcRegion;

    /** cause of failure */
    protected volatile Exception cause;

    /** if the thread is requested to shut down */
    private final AtomicBoolean shutdownReq;

    /** true if transfer is complete */
    protected volatile boolean complete;

    /** source Table API */
    private final TableAPIImpl srcAPI;

    /**
     * table iterator option
     */
    private final TableIteratorOptions iter_opt;

    /**
     * checkpoint primary key to resume table scan, or an empty key
     * {@link Table#createPrimaryKey()} for a full table scan. If null,
     * either the checkpoint is corrupted or the table initialization is
     * done, in either case there is no need to resume the table copy.
     */
    private volatile PrimaryKey ckptKey;

    /**
     * the table to scan
     */
    private final Table srcTable;

    /**
     * unit test only
     */
    private volatile BaseRegionAgentMetrics metrics = null;

    /**
     * last primary persisted to target store
     */
    private volatile PrimaryKey lastPersistPkey;

    /**
     * True if check redundant table transfer
     */
    private volatile boolean checkRedundant;

    /**
     * Time to start table transfer
     */
    private volatile long startTime;

    /**
     * Constructs an instance of table transfer thread
     *
     * @param threadName thread name
     * @param parent     parent region agent
     * @param table      table to transfer
     * @param srcRegion  source region to transfer table from
     * @param srcAPI     source region table api
     * @param logger     private logger
     */
    public BaseTableTransferThread(String threadName,
                                   RegionAgentThread parent,
                                   Table table,
                                   RegionInfo srcRegion,
                                   TableAPIImpl srcAPI,
                                   Logger logger) {

        super(threadName);

        this.parent = parent;
        this.table = (TableImpl) table;
        this.srcRegion = srcRegion;
        this.srcAPI = srcAPI;
        this.logger = logger;

        final String tableName = table.getFullNamespaceName();
        shutdownReq = new AtomicBoolean(false);
        complete = false;
        cause = null;
        setUncaughtExceptionHandler(new ExceptionHandler());

        if (parent == null) {
            /* unit test only */
            srcTable = srcAPI.getTable(tableName);
        } else {
            /*
             * the region agent starts up a table transfer only after the
             * table has been ensured to exist at remote region, therefore,
             * the table instance must be cached.
             */
            srcTable = parent.getMdMan().getRemoteTable(srcRegion.getName(),
                                                        tableName);
        }

        if (srcTable == null) {
            /*
             * We have ensured existence of remote table, the source table
             * must have been cached and cannot be null. We leave detecting
             * the missing remote table to the time when the table is being
             * transferred. If the source table is dropped at that time, we
             * would encounter {@link oracle.kv.StoreIteratorException}
             */
            final String err = "Cached remote table not found" +
                               ", table=" + tableName +
                               ", region=" + srcRegion.getName();
            logger.warning(lm(err));
            throw new IllegalStateException(err);
        }
        /* verify that the table from remote region has the right name */
        if (!tableName.equals(srcTable.getFullNamespaceName())) {
            final String err =
                "Source table=" + srcTable.getFullNamespaceName() +
                " does not match the requested table=" + tableName;
            throw new IllegalStateException(err);
        }
        ckptKey = null;
        iter_opt = new TableIteratorOptions(
            Direction.FORWARD,
            Consistency.ABSOLUTE,
            /* iterator timeout upper bounded by store read timeout */
            srcAPI.getStore().getReadTimeoutMs(),
            TimeUnit.MILLISECONDS,
            getTableThreads(),
            getTableBatchSz());

        /* always include tombstones in MR table initialization */
        iter_opt.setIncludeTombstones();

        /* create initialization stats */
        final BaseRegionAgentMetrics currentMetrics = getMetrics();
        if (currentMetrics == null) {
            /* unit test only */
            return;
        }
        checkRedundant = true;
        startTime = 0;
    }

    /**
     * Returns table id to identify table initialized checkpoint (TIC). For
     * MR table, the table id is the target (local) table id. For PITR, table
     * id is the source (local) table id, there is no target table for PITR.
     * @return table id of local table.
     */
    abstract protected long getTICTableId();

    /**
     * Pushes the row from the source region
     *
     * @param srcRow row of source region
     */
    abstract protected void pushRow(Row srcRow);

    /**
     * Returns the stat summary of the thread
     *
     * @return the stat summary of the thread
     */
    abstract protected String dumpStat();

    /**
     * Adds prefix for log messages
     *
     * @param msg log message
     * @return log message with prefix
     */
    protected String lm(String msg) {
        /* use thread name */
        return "[" + getName() + "] " + msg;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void run() {

        logger.info(lm("Start transfer thread for table=" +
                       ServiceMDMan.getTrace(table) +
                       " from region=" + srcRegion.getName()));
        startTime = System.currentTimeMillis();
        final String region = srcRegion.getName();
        final String tableName = table.getFullNamespaceName();
        final long tableId = table.getId();

        /* first check if the table should be initialized by this agent */
        if (parent != null && !parent.belongsToMe(tableName, region)) {
            complete = true;
            /* table belongs to another agent */
            logger.info(lm("Transfer exits for table=" + tableName +
                           " region=" + region));
            return;
        }

        if (parent != null) {
            logger.info(lm("Table=" + tableName + " region=" + region +
                           " belongs to me=" + parent.getAgentSubscriberId()));
        }
        if (checkRedundant && isRedundantTransfer()) {
            logger.info(lm("Skip transfer because there is already an " +
                           "existing transfer thread for table=" +
                           tableName + " from region=" + region));
            return;
        }

        try {
            ckptKey = getCheckpointKey();
            if (ckptKey == null) {
                logger.info(lm("Skip transfer for table=" + tableName));
                return;
            }
        } catch (CheckpointKeyException exp) {
            cause = exp;
            final String msg =
                "Fail to get checkpoint key for table=" + tableName +
                " from region=" + srcRegion.getName() +
                ", error=" + exp + ", stack trace\n" +
                LoggerUtils.getStackTrace(exp);
            logger.warning(lm(msg));
            return;
        }

        /* set start time */
        getMetrics().getTableMetrics(tableName)
                    .getRegionInitStat(region)
                    .setTransferStartMs(startTime);
        int attempts = 0;

        /* remember the exception encountered */
        Exception error = null;
        /*
         * the thread will indefinitely retry until transfer is complete, or
         * service shuts down, or encounters unexpected failure
         */
        while (!complete && !shutdownRequested()) {
            final long tid = (parent == null) ? 0 /* unit test only*/ :
                tableId;
            /* start time of this attempt */
            final long ts = System.currentTimeMillis();
            logger.info(lm("Starting transfer table=" + tableName +
                           "(id=" +  tid +
                           ", remote id=" + ((TableImpl) srcTable).getId() +
                           ") from region=" + region +
                           (isFullScan(srcTable, ckptKey) ? ", full scan" :
                           ", from checkpoint key hash=" +
                           getKeyHash(ckptKey)) + ", attempts=" + attempts));
            try {
                transTable(++attempts, ts);
            } catch (StoreIteratorException sie) {
                final Throwable err = sie.getCause();
                if (err instanceof MetadataNotFoundException) {
                    procMissingRemoteTable();
                    error = sie;
                    break;
                }
                /* retry on other cases */
                retryLog(sie, attempts, ts);
            } catch (FaultException fe) {
                /* always retry */
                retryLog(fe, attempts, ts);
            } catch (MetadataNotFoundException exp) {
                logger.warning(lm("Fail to transfer table=" + tableName +
                                  " not found at local region, error=" + exp));
                error = exp;
                break;
            } catch (Exception exp) {
                /* no retry on hard failures */
                if (!shutdownRequested() &&
                    (parent != null && !parent.isShutdownRequested())) {
                    error = exp;
                }
                break;
            }
        }

        /* success */
        if (parent == null) {
            /*
             * in some unit test without parent region agent, no need to
             * update the stats and checkpoint below
             */
            return;
        }

        /* normal case */
        final ServiceMDMan mdMan = parent.getMdMan();
        if (complete) {
            /* update table init checkpoint to complete */
            mdMan.writeCkptRetry(region, tableName, getTICTableId(), null,
                                 COMPLETE);
            getMetrics().getTableMetrics(tableName).getRegionInitStat(region)
                        .setTransferCompleteMs(System.currentTimeMillis());
            logger.info(lm("Complete transferring table=" +
                           ServiceMDMan.getTrace(table) +
                           ", remote id=" +
                           mdMan.getRemoteTableId(region, tableName) +
                           " in attempts=" + attempts +
                           ", shutdown=" + shutdownRequested()));
            logger.fine(() -> lm(dumpStat()));
            return;
        }

        /* unexpected failure, no retry */
        if (error != null) {
            getMetrics().getTableMetrics(tableName).getRegionInitStat(region)
                        .setState(ERROR);
            mdMan.writeTableInitCkpt(region, tableName, getTICTableId(),
                                     lastPersistPkey, ERROR,
                                     error.getMessage());
            final String msg = "Fail to copy table=" + tableName +
                               " from region=" + srcRegion.getName() +
                               " in attempts=" + attempts +
                               ", shutdown=" + shutdownRequested() +
                               ", error=" + error +
                               ", " + dumpStat() +
                               (logger.isLoggable(Level.FINE) ? "\n" +
                                   LoggerUtils.getStackTrace(error) : "");
            logger.warning(lm(msg));
            /*
             * finally, we set cause to let RegionAgentThread to catch the
             * failure, note that the RegionAgentThread can only catch the
             * failure after the checkpoint with ERROR state is persistent.
             * Otherwise, we might lose the error checkpoint.
            */
            cause = error;
            return;
        }

        /* shutdown requested */
        getMetrics().getTableMetrics(tableName).getRegionInitStat(region)
                    .setState(SHUTDOWN);
        logger.info(lm("Shutdown requested before completing" +
                       " transfer table=" + tableName +
                       " in attempts=" + attempts));
        logger.fine(() -> lm(dumpStat()));
    }

    @Override
    protected int initiateSoftShutdown() {
        logger.fine(() -> lm("Signal thread=" + getName() + " to shutdown" +
                             ", wait for time(ms)=" + SOFT_SHUTDOWN_WAIT_MS +
                             " to exit"));
        return SOFT_SHUTDOWN_WAIT_MS;
    }

    /**
     * Shuts down the transfer thread
     */
    public void shutdown() {

        if (!shutdownReq.compareAndSet(false, true)) {
            logger.fine(() -> lm("Shutdown already signalled"));
            return;
        }

        final long ts = System.currentTimeMillis();
        shutdownThread(logger);
        logger.info(lm("Thread=" + getName() + " has shut down in time(ms)=" +
                       (System.currentTimeMillis() - ts)));
    }

    /**
     * Gets the start timestamp, or 0 if not started
     * @return the start timestamp, or 0 if not started
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Gets the cause of failure, or null if transfer is complete or shutdown
     * by the parent agent
     *
     * @return the cause of failure, or null.
     */
    public Exception getCause() {
        return cause;
    }

    /**
     * Returns the local table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Returns true if the transfer is complete, false otherwise
     *
     * @return true if the transfer is complete, false otherwise
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * Returns true if the transfer has failed, false otherwise
     *
     * @return true if the transfer has failed, false otherwise
     */
    public boolean hasFailed() {
        return cause != null;
    }

    /**
     * Returns true if enough rows have been transferred, or enough time has
     * elapsed since last report that we should log initialization progress,
     * otherwise false.
     *
     * @param rowsTrans # rows already transferred
     * @param lastRows # of rows transferred in last report
     * @param lastTs timestamp of last report
     *
     * @return true if it needs to report progress, false otherwise
     */
    private boolean reportProgress(long rowsTrans, long lastRows, long lastTs) {

        if (noProgress(rowsTrans, lastRows)) {
            return false;
        }

        if (System.currentTimeMillis() - lastTs > TABLE_INIT_REPORT_INTV_MS) {
            return true;
        }

        return (rowsTrans - lastRows) > getTableReportIntv();
    }

    /***
     * Returns true if no progress has been made since last report
     *
     * @param rowsTrans # rows already transferred
     * @param lastReported # of rows transferred in last report
     * @return true if no progress has been made since last report
     */
    private boolean noProgress(long rowsTrans, long lastReported) {
        return rowsTrans == 0 || rowsTrans == lastReported;
    }

    /**
     * Transfer table from source region. The transfer will start from a
     * given start key each time and update the start key during transfer.
     * It will return
     * - transfer is complete, or
     * - shutdown is requested, or
     * - transfer fails because an exception is thrown
     *
     * @param attempts # of attempts
     * @param ts       start time of this attempt
     */
    private void transTable(int attempts, long ts) {
        TableIterator<Row> iterator = null;
        long lastReportedRows = 0; /* # of transferred rows in last report */
        long lastReportTs = 0; /* timestamp of last report */
        final String tableName = table.getFullNamespaceName();
        try {
            complete = false;

            /* set the resume key if not full table scan */
            if (ckptKey != null && !isFullScan(srcTable, ckptKey)) {
                iter_opt.setResumePrimaryKey(ckptKey);
                logger.info(lm("Set resume key hash=" + getKeyHash(ckptKey) +
                               " for table=" + tableName +
                               "(id=" + ((TableImpl) srcTable).getId() + ")"));
            }
            /* create iterator from given start key */
            iterator = srcAPI.tableIterator(
                srcTable.createPrimaryKey(), null, iter_opt);
            /* rows transferred in this attempt */
            long rows = 0;
            while (!shutdownRequested() && iterator.hasNext()) {
                final Row srcRow = iterator.next();
                pushRow(srcRow);
                lastPersistPkey = srcRow.createPrimaryKey();
                rows++;

                /* report progress */
                if (reportProgress(rows, lastReportedRows, lastReportTs)) {
                    /* log table transfer stats */
                    lastReportedRows = rows;
                    lastReportTs = System.currentTimeMillis();
                    logTableInitStat(attempts, ts, rows);
                    if (parent != null) {
                        /* update checkpoint if not unit test  */
                        doCheckpoint();
                    }
                }

                /* unit test only: simulate failure */
                fireTestHookInTransfer(rows);
            }

            if (!shutdownRequested()) {
                complete = true;
            }
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    public void doCheckpoint() {
        if (lastPersistPkey == null) {
            /* did not transfer any row, no checkpoint */
            return;
        }
        parent.getMdMan().writeTableInitCkpt(srcRegion.getName(),
                                             table.getFullNamespaceName(),
                                             getTICTableId(),
                                             lastPersistPkey, IN_PROGRESS,
                                             null);
    }

    private int getTableThreads() {
        if (parent == null) {
            /* unit test */
            return DEFAULT_THREADS_TABLE_ITERATOR;
        }
        return parent.getMdMan().getJsonConf().getTableThreads();
    }

    private int getTableBatchSz() {
        if (parent == null) {
            /* unit test */
            return DEFAULT_BATCH_SIZE_PER_REQUEST;
        }
        return parent.getMdMan().getJsonConf().getTableBatchSz();
    }

    private int getTableReportIntv() {
        if (parent == null) {
            /* unit test */
            return DEFAULT_ROWS_REPORT_PROGRESS_INTV;
        }
        return parent.getMdMan().getJsonConf().getTableReportIntv();
    }

    /*
     * Stat reference may change if interval stat is collected, thus get
     * the reference from parent instead of keeping a constant reference
     */
    protected BaseRegionAgentMetrics getMetrics() {
        if (parent == null) {
            /* unit test only */
            return metrics;
        }
        return parent.getMetrics();
    }

    /**
     * unit test only
     */
    protected void setMetrics(BaseRegionAgentMetrics val) {
        metrics = val;
    }

    /**
     * Fires test hook, unit test only
     *
     * @param rows # rows transferred
     */
    private void fireTestHookInTransfer(long rows) {
        final String tableName = table.getFullNamespaceName();
        try {
            /* test hook to throw exception */
            assert ExceptionTestHookExecute.doHookIfSet(expHook, rows);
            /* test hook in transfer */
            assert TestHookExecute.doHookIfSet(transInProgressHook,
                                               new Pair<>(tableName, rows));
        } catch (Exception exp) {
            final String err = exp.getMessage();
            logger.warning(lm("TEST ONLY: cause=" + err));
            throw new IllegalStateException(err, exp);
        }
    }

    private String getKeyHash(PrimaryKey key) {
        if (key == null) {
            return "null";
        }
        return UserDataControl.getHash(key.toJsonString(false).getBytes());
    }

    private boolean isFullScan(Table tb, PrimaryKey primaryKey) {
        return primaryKey.equals(tb.createPrimaryKey());
    }

    /**
     * Builds primary key from checkpoint
     */
    private PrimaryKey buildPrimaryKey(TableInitCheckpoint ckpt) {
        PrimaryKey ret;
        if (NOT_START.equals(ckpt.getState())) {
            ret = srcTable.createPrimaryKey();
        } else {
            final String json = ckpt.getPrimaryKey();
            ret = srcTable.createPrimaryKeyFromJson(json, true);
        }
        final String tableName = table.getFullNamespaceName();
        logger.info(lm("Table=" + tableName +
                       (isFullScan(srcTable, ret) ?
                           " requires full scan" :
                           " resumes from key hash=" + getKeyHash(ret))));
        return ret;
    }

    /**
     * Reads the primary key from checkpoint table
     *
     * @return primary key to resume scan, or null if table initialization is
     * complete, or the checkpoint is gone in the checkpoint table.
     * @throws CheckpointKeyException if unable to read the checkpoint
     */
    private PrimaryKey getCheckpointKey() throws CheckpointKeyException {
        /* build primary key from checkpoint, or a full table scan */
        if (parent == null) {
            /* some unit test only, always full table scan */
            return srcTable.createPrimaryKey();
        }

        /* normal case */
        final String region = srcRegion.getName();
        final String tableName = table.getFullNamespaceName();
        final TableInitCheckpoint ckpt;
        try {
            ckpt = readCheckpoint(tableName, region);
        } catch (IllegalStateException exp) {
            final String err = "Cannot read checkpoint for table=" + tableName +
                               ", error=" + exceptionString(exp);
            throw new CheckpointKeyException(err, exp);
        }

        if (ckpt == null) {
            final String msg = "No checkpoint for table=" + tableName +
                               ", table might be dropped from region=" +
                               srcRegion.getName();
            logger.info(lm(msg));
            return null;
        }
        /* verify the checkpoint has matching table id */
        if (ckpt.getTableId() != getTICTableId()) {
            final String msg = "Table id=" + ckpt.getTableId() +
                               " in checkpoint for table=" + tableName +
                               " does not match expected id=" +
                               getTICTableId() +
                               ", reset table init checkpoint";
            parent.resetInitCkpt(Collections.singleton(table));
            logger.info(lm(msg));
            /* full scan */
            return srcTable.createPrimaryKey();
        }
        if (COMPLETE.equals(ckpt.getState())) {
            final String msg = "Skip transfer because initialization " +
                               "of table=" + tableName + " is already " +
                               "complete";
            logger.info(lm(msg));
            return null;
        }
        /* get the primary key from checkpoint */
        return buildPrimaryKey(ckpt);
    }

    /**
     * Reads the table initialization checkpoint. It would first read the
     * checkpoint made by itself. If not exist, it would read checkpoint from
     * other agent. If exists, a new checkpoint table would be persisted to
     * replace the checkpoint from other agent.
     * @param tableName table name
     * @param region region name
     * @return table initialization checkpoint
     */
    private TableInitCheckpoint readCheckpoint(String tableName,
                                               String region) {
        /* read checkpoint from myself */
        TableInitCheckpoint ckpt =
            parent.getMdMan().readTableInitCkpt(region, tableName);
        if (ckpt == null) {
            /* make another attempt to read checkpoint from other agents */
            ckpt = parent.getMdMan().readCkptAnyAgent(tableName, region);
        }

        if (ckpt == null) {
            logger.info(lm("Cannot find any checkpoint for table=" + tableName +
                           " from region=" + region));
            return null;
        }

        final boolean fromMe =
            ckpt.getAgentId().equals(parent.getAgentSubscriberId().toString());
        logger.info(lm("Found checkpoint made by " +
                       (fromMe ? "myself=" : "another agent=") +
                       ckpt.getAgentId() + ", ckpt="+ ckpt));
        /*
         * if the checkpoint from another agent, do clean up and persist a
         * new checkpoint with my agent id
         */
        if (!fromMe) {
            /* write a new checkpoint */
            final PrimaryKey pkey = buildPrimaryKey(ckpt);
            parent.getMdMan().writeTableInitCkpt(region,
                                                 tableName,
                                                 getTICTableId(),
                                                 pkey,
                                                 ckpt.getState(),
                                                 ckpt.getMessage());
            final TableInitCheckpoint tic =
                parent.getMdMan().readTableInitCkpt(region, tableName);
            logger.info(lm("Refresh existing checkpoint with my agentId=" +
                           parent.getAgentSubscriberId() + ", ckpt=" + tic));


            /* delete all stale checkpoints from other agents */
            parent.getMdMan().delInitCkpt(region, tableName,
                                          true/* keep my own checkpoint */);
        }

        return ckpt;
    }

    /**
     * Exception thrown when unable to get the primary key from the checkpoint
     */
    private static class CheckpointKeyException extends IllegalStateException {
        private static final long serialVersionUID = 1;
        CheckpointKeyException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /**
     * Uncaught exception handler
     */
    private class ExceptionHandler implements UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.warning(lm("Uncaught exception in transfer thread for" +
                              " table=" + ServiceMDMan.getTrace(table) +
                              " from region=" + srcRegion.getName() +
                              ", thread=" + t.getName() +
                              ", id=" + ThreadUtils.threadId(t) +
                              ", error=" + e +
                              "\n" + LoggerUtils.getStackTrace(e)));
        }
    }

    /**
     * Skips check redundant table transfer. The polling thread should call
     * it to disable the check, because transfer thread submitted by the
     * polling thread cannot be redundant.
     */
    protected void skipCheckRedundant() {
        checkRedundant = false;
    }

    private boolean isRedundantTransfer() {
        if (parent == null) {
            /* unit test only */
            return false;
        }
        /*
         * since the region agent only transfers one table at a time, when
         * this thread is scheduled to run, all previous transfers have
         * either been complete or terminated because of errors. In either
         * case we can look at the state of the checkpoint table to determine
         * if a table has been transferred. Here we only need to check the
         * polling thread to see if the table has been scheduled to transfer.
         */
        return parent.inPolling(table);
    }

    private void retryLog(Throwable exp, int attempts, long startTs) {
        final boolean fullScan = isFullScan(srcTable, ckptKey);
        /* retry transfer */
        final String msg =
            "Unable to copy table=" + ServiceMDMan.getTrace(table) +
            ", remote table=" + ServiceMDMan.getTrace(srcTable) +
            " from region=" + srcRegion.getName() +
            " in attempts=" + attempts +
            "(elapsedMs=" + (System.currentTimeMillis() - startTs) +
            "), error=" + exp +
            ", will retry " +
            (fullScan ? "full scan" :
                "from checkpoint key hash=" + ckptKey.hashCode()) +
            (logger.isLoggable(Level.FINE) ?
                LoggerUtils.getStackTrace(exp) : "");
        logger.info(lm(msg));
    }

    /**
     * Processes missing remote tables
     */
    private void procMissingRemoteTable() {
        final String tableName = table.getFullNamespaceName();
        final String region = srcRegion.getName();
        if (parent.getMdMan().verifyTableInfo(tableName, srcRegion)) {
            logger.fine(() -> lm("Verified table=" +
                                 ServiceMDMan.getTrace(table) +
                                 " at region=" + region));
        } else {
            logger.info(lm("Fail to verify table=" +
                           ServiceMDMan.getTrace(table) +
                           " at region=" + region));
        }

        try {
            parent.recreateRemoteTable(tableName, "table transfer");
        } catch (InterruptedException e) {
            logger.info(lm("Interrupted in adding table=" +
                           ServiceMDMan.getTrace(table) +
                           " initialization request, ignore"));
        }
    }

    protected boolean shutdownRequested() {
        return shutdownReq.get();
    }

    private TableInitStat getTableInitMetrics(String tableName) {
        final MRTableMetrics tm = getMetrics().getTableMetrics(tableName);
        if (tm == null) {
            return null;
        }
        return tm.getRegionInitStat(srcRegion.getName());
    }

    private void logTableInitStat(int attempt, long startTs, long rows) {
        final String tableName = table.getFullNamespaceName();
        final TableInitStat st = getTableInitMetrics(tableName);
        final long elapsedMs =  System.currentTimeMillis() - startTs;
        final double throughput = 1000.0 * rows / elapsedMs;
        final String msg =
            "In transferring table=" + ServiceMDMan.getTrace(table) +
            ", attempts=" + attempt +
            ", elapsedMs=" + elapsedMs +
            ", # rows transferred in this attempt=" + rows +
            ", throughput(rows/sec)=" + throughput +
            ", in current stat report interval: #rows transferred=" +
            (st != null ? st.getTransferRows() : "NA") +
            ", #rows persisted=" +
            (st != null ? st.getPersistRows() : "NA");
        logger.info(lm(msg));
    }
}
