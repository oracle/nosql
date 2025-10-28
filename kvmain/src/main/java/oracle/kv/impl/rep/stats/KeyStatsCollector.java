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

package oracle.kv.impl.rep.stats;

import static oracle.kv.impl.rep.RepNodeService.SHUTDOWN_TIMEOUT_MS;
import static oracle.nosql.common.sklogger.ScheduleStart.calculateDelay;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.ResourceTracker;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadata.TableMetadataIteratorCallback;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService.KVStoreCreator;
import oracle.kv.impl.rep.stats.IndexLeaseManager.IndexLeaseInfo;
import oracle.kv.impl.rep.stats.PartitionLeaseManager.PartitionLeaseInfo;
import oracle.kv.impl.rep.stats.StatsLeaseManager.LeaseInfo;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.ShutdownThread;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.Index;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * The class is to gather statistics information. A lease mechanism is within
 * its inner class ScanningThread. The lease mechanism is to deal with
 * coordinating scanning among RNs in a shard and deal with the failures of RNs.
 * The mechanism is as follows:
 *
 * 1. Get a partition(or secondary database), and update database to get or
 * create a lease(if a lease associated with the target partition or secondary
 * database exists, then get it, or create a new lease), and then start to scan
 * the partition(or secondary database).
 *
 * 2. During the scanning, check whether the lease expires or not. If the
 * lease is within it's within 10% of its expiry time, extend the expiry time.
 *
 * 3. Extend a lease to ensure the scanning can be completed by a same RN, and
 * also deal with failures. If the current ScanningThread is down during
 * scanning partition(or secondary database), because a lease time is short ,
 * and another ScanningThread within another RN will continue to scan the
 * partition(or secondary database) after the lease time expires.
 *
 * 4. Modify last updated time to ensure the frequency of scanning partitions
 * (or secondary database), and also coordinate scanning among in RNs.
 */
public class KeyStatsCollector implements ParameterListener {

    /**
     * If true, ignores the MIN_GATHER_INTERVAL_MS and MIN_SLEEP_WAIT_MS
     * values, for use in testing to get stats collection to happen quickly.
     */
    public static volatile boolean testIgnoreMinimumDurations;

    /*
     * Constraints on the key stats parameter values.
     */
    static final long MIN_GATHER_INTERVAL_MS = 60 * 1000;
    static final long MIN_SIZE_UPDATE_INTERVAL_MS = 10 * 1000;

    /* Lease duration must be <= 1 day. See StatsLeaseManager.LeaseInfo */
    static final long MAX_LEASE_DURATION_MS = (24 * 60 * 60 * 1000);
    static final long MIN_SLEEP_WAIT_MS = 10 * 1000;

    /* Name of scanning thread */
    private static final String THREAD_NAME = "Key Stats Gather Thread";

    /* Use for logging times */
    static final SimpleDateFormat UTC_DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    {
        UTC_DATE_FORMAT.setTimeZone(FormatUtils.getTimeZone());
    }

    /**
     * A test hook that is called when ScanningThread detects that there are
     * user tables.
     */
    static volatile TestHook<Void> foundUserTablesTestHook;

    private final RepNode repNode;
    private final Logger logger;

    private TableAPI tableAPI;
    private final KVStoreCreator creator;

    final ResourceTracker aggregateThroughputTracker;

    /* The map is to store the table name and table pairs */
    private Map<String, TableImpl> tableListMap;

    /* A flag to mark whether StatisticsGather is shutdown or not */
    private volatile boolean shutdown = false;

    /* Lease managers to control the lease of scan partitions and indices */
    private PartitionLeaseManager partitionLeaseManager;
    private IndexLeaseManager indexLeaseManager;

    /* Scanning thread handle */
    private ScanningThread scanningThread;

    /* Variables to control scanning which are loaded from parameters */

    /**
     * Whether the stats collection is enabld.
     */
    private volatile boolean statsEnabled;
    /**
     * The interval for stats scan. The next scan interval start is computed
     * with this parameter which is used to coordinate with the lease. See
     * StatsScan#checkLease.
     */
    private volatile long statsGatherInterval;
    /**
     * The duration of the lease. Only the RN holding an unexpired lease can do
     * the stats scan.
     */
    private volatile long statsLeaseDuration;
    /**
     * The duration for the next poll to check if we should start a scan.
     */
    private volatile long statsSleepWaitDuration;
    /**
     * Whether to update table size according to write delta. See
     * ResourceCollector.sizeDeltaMap and IntermediateTableSizeUpdate.
     */
    private volatile boolean updateTableSizes;
    /**
     * The duration to check whether to do a table size update with
     * IntermediateTableSizeUpdate.
     */
    private volatile long statsSizeUpdateInterval;
    /**
     * The duration to force a table size update with
     * IntermediateTableSizeUpdate. We set this value to 1/10 of the
     * statsGatherInterval. The heuristic is to make sure we do a forced update
     * pretty soon when the new results of the stats scan from this RN or other
     * RNs is available while on the other hand not doing too many forced
     * updates. See the comment on the IntermediateTableSizeUpdate class for
     * more information.
     */
    private volatile long statsSizeForceUpdateInterval;

    /* TTL for lease records. */
    private volatile TimeToLive leaseTTL;

    public KeyStatsCollector(RepNode repNode,
                             KVStoreCreator creator,
                             Logger logger) {
        this.repNode = repNode;
        this.creator = creator;
        this.logger = logger;
        aggregateThroughputTracker = repNode.getAggregateThroughputTracker();
    }

    /**
     * Attempt to start the ScanningThread based on collector state. If stats
     * are disabled or in shutdown this call is a noop.
     */
    private synchronized void startScanningThread() {

        /* If stats not enabled or in shutdown, skip */
        if (!statsEnabled || shutdown) {
            return;
        }

        if (scanningThread != null) {
            /* If there is a thread already running, skip */
            if (!scanningThread.isShutdown()) {
                return;
            }
            /* If the thread is shutdown, give it a bit of time to exit */
            try {
                scanningThread.join(SHUTDOWN_TIMEOUT_MS);
            } catch (InterruptedException ie) {
                if (!shutdown) {
                    logger.log(Level.WARNING,
                               "Unexpected interrupt waiting for" +
                               " statistics gathering thread to exit", ie);
                }
            }
        }
        scanningThread = new ScanningThread();
        logger.log(Level.INFO, "Start statistics gathering: {0}",
                   scanningThread);
        scanningThread.start();
    }

    /**
     * Stop ScanningThread without waiting for the thread to exit.
     */
    private synchronized void stopScanningThread() {

        if ((scanningThread != null) && !scanningThread.isShutdown()) {
            logger.log(Level.INFO, "Stop statistics gathering: {0}",
                       scanningThread );
            scanningThread.shutdown();
            /* Leave the reference, may have to wait for the thead to exit */
        }
    }

    /**
     * Load statistics parameters. Stops the scanning thread, if running,
     * (re)loads parameters and starts the scanning thread if needed.
     */
    private void loadStatsParametersAndStart(RepNodeParams repNodeParams) {
        assert MIN_SLEEP_WAIT_MS <= MIN_GATHER_INTERVAL_MS;

        stopScanningThread();

        statsEnabled = repNodeParams.getStatsEnabled();

        statsGatherInterval = repNodeParams.getStatsGatherInterval();
        if (!testIgnoreMinimumDurations &&
            (statsGatherInterval < MIN_GATHER_INTERVAL_MS)) {
            logger.warning("The key stats gathering interval: " +
                           statsGatherInterval +
                           "ms is less than the minimum, setting it to " +
                           MIN_GATHER_INTERVAL_MS + "ms");
            statsGatherInterval = MIN_GATHER_INTERVAL_MS;
        }

        statsLeaseDuration = repNodeParams.getStatsLeaseDuration();
        if (statsLeaseDuration > MAX_LEASE_DURATION_MS) {
            logger.warning("The key stats lease duration: " +
                           statsLeaseDuration +
                           "ms is greater than the maximum, setting it to " +
                           MAX_LEASE_DURATION_MS + "ms");
            statsLeaseDuration = MAX_LEASE_DURATION_MS;
        }

        statsSleepWaitDuration = repNodeParams.getStatsSleepWaitDuration();
        if (!testIgnoreMinimumDurations &&
            (statsSleepWaitDuration < MIN_SLEEP_WAIT_MS)) {
            logger.warning("The key stats sleep wait duration: " +
                           statsSleepWaitDuration +
                           "ms is less than the minimum, setting it to " +
                           MIN_SLEEP_WAIT_MS + "ms");
            statsSleepWaitDuration = MIN_SLEEP_WAIT_MS;
        } else if (statsSleepWaitDuration > statsGatherInterval) {
            logger.warning("The key stats sleep wait duration: " +
                           statsSleepWaitDuration +
                           "ms is greater than the gathering" +
                           " interval, setting it to " +
                           statsGatherInterval + "ms");
            statsSleepWaitDuration = statsGatherInterval;
        }

        updateTableSizes = repNodeParams.getStatsIncludeStorageSize();
        if (updateTableSizes) {
            statsSizeUpdateInterval =
                                repNodeParams.getStatsTableSizeUpdateInterval();
            if (statsSizeUpdateInterval == 0) {
                updateTableSizes = false;
            } else if (statsSizeUpdateInterval >= statsGatherInterval) {
                logger.warning("The table size update interval: " +
                               statsSizeUpdateInterval +
                               "ms is equal to or greater than the gathering" +
                               " interval, updates will not be done");
                updateTableSizes = false;
            } else if (!testIgnoreMinimumDurations &&
                statsSizeUpdateInterval < MIN_SIZE_UPDATE_INTERVAL_MS)
            {
                logger.warning("The table size update interval: " +
                               statsSizeUpdateInterval +
                               "ms is less than the minimum, setting it to " +
                               MIN_SIZE_UPDATE_INTERVAL_MS + "ms");
                statsSizeUpdateInterval = MIN_SIZE_UPDATE_INTERVAL_MS;
            }

            statsSizeForceUpdateInterval =
                Math.max(statsGatherInterval / 10, 1);
        }

        final long hours = TimeUnit.MILLISECONDS.toHours(statsGatherInterval);
        leaseTTL = TimeToLive.ofHours(hours < 1 ? 1 : hours);

        startScanningThread();
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        if (oldMap != null) {
            final ParameterMap filtered =
                oldMap.diff(newMap, true /* notReadOnly */).
                    filter(EnumSet.of(ParameterState.Info.POLICY));
            if (filtered.isEmpty()) {
                return;
            }
        }

        /* If parameters changed, re-load statistics parameters */
        loadStatsParametersAndStart(new RepNodeParams(newMap));
    }

    /**
     * Start scanning operation using the specified parameters.
     */
    public void startup(RepNodeParams repNodeParams) {
        loadStatsParametersAndStart(repNodeParams);
    }

    /**
     * Shuts down any scans and prevents any future scans. This method does not
     * wait for any active threads to exit.
     */
    public void shutdown() {
        shutdown = true;
        stopScanningThread();
    }

    /*
     * Returns true if the RN is active (either a master or replica)
     */
    private boolean isRNActive() {
        final ReplicatedEnvironment repEnv = repNode.getEnv(0L);
        try {
            return (repEnv == null) ? false : repEnv.getState().isActive();
        } catch (IllegalStateException ise) {
            /* Environment has been closed */
            return false;
        }
    }

    /**
     * This class is to do the real scanning work.
     */
    private class ScanningThread extends ShutdownThread {

        /*
         * A handler of StatsScan and used to stop scanning when thread is
         * stopped
         */
        private StatsScan<? extends LeaseInfo> statsScanHandler;

        private IntermediateTableSizeUpdate tableSizeUpdater = null;

        private ScanningThread() {
            super(THREAD_NAME);
        }

        /**
         * Initialize Table API
         */
        private boolean initializeTableAPI() {
            /*
             * Callers normally check that tableAPI is non-null before calling
             * this, but the tableAPI variable may become non-null by the time
             * they enter this method.
             */
            if (tableAPI != null) {
                return true;
            }

            final KVStore store = creator.getKVStore();
            /* Store is not ready */
            if (store == null) {
                return false;
            }

            try {
                tableAPI = store.getTableAPI();
            } catch (IllegalArgumentException iae) {
                throw new IllegalStateException("Unable to get Table API", iae);
            }

            return true;
        }

        private boolean foundUserTables() {
            try {
                return tableAPI.getTables().values().stream().anyMatch(
                    t -> !((TableImpl) t).isSystemTable());
            } catch (FaultException e) {
                return false;
            }
        }

        /**
         * Scan partition databases and secondary databases. And delete
         * obsolete statistics info from all tables
         */
        private long scan(long intervalStart) {
            /* Check whether can scan */
            if (isShutdown() || !isRNActive()) {
                return 0L;
            }

            long scannedRecords = 0L;
            try {

                /* Check whether statistics tables exist or not */
                if (!checkLeaseTable()) {
                    return 0L;
                }

                /* Scan partition databases */
                scannedRecords = scanPartitions(intervalStart);

                /* Scan secondary database */
                scannedRecords += scanTableIndexes(intervalStart);

            } catch (Exception ignore) {
                /*
                 * Ignore all exceptions, and statements in the loop
                 * continue running in the next time, it is to ensure the
                 * statistics gathering always works even though exceptions
                 * are thrown.
                 */

                /* Log the exception */
                logger.log(Level.FINE, "Stats scanning operation failed: {0}",
                           ignore);
            }

            try {
                /*
                 * Delete obsolete statistics from statistics tables,
                 * because of the changes for tables and indexes
                 */
                deleteObsoleteStats();

            } catch (Exception ignore) {
                /*
                 * Ignore all exceptions, and statements in the loop
                 * continue running in the next time, it is to ensure the
                 * deletion of obsolete statistic always works even though
                 * exceptions are thrown.
                 */

                /* Log the exception */
                logger.log(Level.FINE, "Obsolete statistics deleting " +
                                       "operation failed: {0}", ignore);
            }
            return scannedRecords;
        }

        /**
         * Check whether lease tables exist or not
         * @return true when all lease tables exist; Or return false
         */
        private boolean checkLeaseTable() {
            final TableMetadata metadata =
                    (TableMetadata)repNode.getMetadata(MetadataType.TABLE);
            /* No metadata exists means no table exists */
            if (metadata == null) {
                return false;
            }

            /* Four tables are need to check */
            final String[] tablesToCheck = new String[] {
                    PartitionStatsLeaseDesc.TABLE_NAME,
                    IndexStatsLeaseDesc.TABLE_NAME,
                    TableStatsPartitionDesc.TABLE_NAME,
                    TableStatsIndexDesc.TABLE_NAME };

            for (String table : tablesToCheck) {
                if (isShutdown() || !isRNActive()) {
                    return false;
                }
                if (getTable(metadata, table) == null) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public void run() {
            logger.log(Level.FINE, "{0} start", this);
            try {
                doRun();
            } catch (InterruptedException ie) {
                /* Should not happen. */
                logger.log(Level.WARNING,
                           "Unexpected interrupt in " +
                           this + ", exiting", ie);
            } finally {
                /* Mark this thread as shut down */
                shutdown();
                logger.log(Level.FINE, "{0} stop", this);
            }
        }

        private void doRun() throws InterruptedException {
            /* Must have table API */
            while (!isShutdown() && !initializeTableAPI()) {
                waitForMS(10*1000);
            }

            /* Wait for user tables before doing anything */
            boolean loggedNoUserTables = false;
            while (!isShutdown() && !foundUserTables()) {
                if (!loggedNoUserTables) {
                    logger.fine("Waiting for user table to start key stats");
                    loggedNoUserTables = true;
                }
                waitForMS(10 * 1000);
            }

            assert TestHookExecute.doHookIfSet(foundUserTablesTestHook, null);

            long now = System.currentTimeMillis();

            /*
             * nextInterval is the start time of the next scan interval,
             * initially calculated by ScheduleStart.calculateDelay. It is
             * incremented by statsGatherInterval on each pass.
             */
            long nextInterval = now + calculateDelay(statsGatherInterval, now);

            /*
             * nextUpdate is the start time of the next table size update (if
             * enabled). The time is offset to reduce the chances that it
             * coincides with scan. There isn't a real problem if it does, just
             * better use of resource if it doesn't.
             */
            long nextUpdate = now +
                              calculateDelay(statsSizeUpdateInterval, now) +
                              (statsSizeUpdateInterval/2);

            logger.log(Level.INFO,
                       "The next key stats scan interval begins at {0}",
                       UTC_DATE_FORMAT.format(new Date(nextInterval)));

            /*
             * intervalStart is the start time of the current scan interval,
             * which is intialized to be the start of the previous interval.
             */
            long intervalStart = nextInterval - statsGatherInterval;

            while (!isShutdown()) {
                /*
                 * nextPoll is the next time that we check whether to scan. We
                 * check more often than once per interval because there may
                 * have been errors on other nodes which have left some items
                 * unscaned. Polling should take care of these cases.
                 * TODO - checking is expensive, it would be nice to
                 * know whether a scan was complete, so we can skip further
                 * checking until the next interval.
                 */
                long nextPoll = now + statsSleepWaitDuration;

                /*
                 * If we are near, or past the nextInterval, adjust the next
                 * poll time, update the interval start, and calculate the next
                 * interval.
                 */
                if (nextPoll > nextInterval) {
                    nextPoll = nextInterval;

                    /* When we poll again, we will be in the next interval. */
                    intervalStart = nextInterval;

                    /*
                     * Advance to the next interval. If way behind, find the
                     * next scan interval that is actually in the future.
                     */
                    nextInterval = advanceInterval(nextInterval,
                                                   statsGatherInterval,
                                                   now);
                    logger.log(Level.FINE,
                               "The next key stats scan interval begins at {0}",
                               UTC_DATE_FORMAT.format(new Date(nextInterval)));
                }

                /*
                 * Wait until the next poll time. While waiting, do intermediate
                 * table size updates.
                 */
                while (!isShutdown() && (nextInterval > now)) {
                    long waitMS = nextPoll - now;
                    if (updateTableSizes) {
                        if (now >= nextUpdate) {
                            updateTableSizes();
                            now = System.currentTimeMillis();
                            nextUpdate = advanceInterval(nextUpdate,
                                                         statsSizeUpdateInterval,
                                                         now);
                        }
                        /*
                         * Adjust the wait time to get to the next poll or
                         * update, which ever is sooner.
                         */
                        waitMS = Math.min(nextUpdate - now, waitMS);
                    }
                    if (now >= nextPoll) {
                        break;
                    }
                    /*
                     * TODO - Possible optimization: if this wait is very long
                     * (hours) then it may be better to exit and reschedule.
                     */
                    if (waitMS > 0) {
                        waitForMS(waitMS);
                        now = System.currentTimeMillis();
                    }
                }

                if (isShutdown()) {
                    break;
                }

                logger.log(Level.FINE, "Starting key stats scan");
                now = System.currentTimeMillis();
                final long startTime = now;

                /* Start scanning all data stored on this shard. */
                final long scannedRecords = scan(intervalStart);

                now = System.currentTimeMillis();
                final long totalScanTime = now - startTime;
                if (totalScanTime > statsGatherInterval) {
                    logger.log(Level.WARNING,
                               "Key stats scan took {0}ms to scan {1}" +
                               " records, which is longer than the scan" +
                               " interval of {2}ms",
                               new Object[] { totalScanTime,
                                              scannedRecords,
                                              statsGatherInterval });
                } else if (scannedRecords > 0) {
                    logger.log(Level.INFO,
                               "Key stats scan took {0}ms to scan {1} records",
                               new Object[] { totalScanTime, scannedRecords });
                } else {
                    logger.log(Level.FINE,
                               "Key stats scan took {0}ms (no records scanned)",
                               totalScanTime);
                }
            }
        }

        /*
         * Returns the next interval. Incrementing as necessary so that
         * the returned interval is greater than now.
         */
        private long advanceInterval(long interval, long increment, long now) {
            interval += increment;
            while (interval < now) {
                interval += increment;
            }
            return interval;
        }

        private void updateTableSizes() {
            IntermediateTableSizeUpdate updater = tableSizeUpdater;
            if (updater == null) {
                synchronized (this) {
                    if (isShutdown()) {
                        return;
                    }
                    if (tableSizeUpdater == null) {
                        tableSizeUpdater =
                            new IntermediateTableSizeUpdate(repNode,
                                                            tableAPI, logger);
                    }
                    updater = tableSizeUpdater;
                }
            }
            updater.runUpdate(statsSizeForceUpdateInterval);
        }

        /**
         * Deletes all obsolete statistics information from statistics tables.
         */
        private void deleteObsoleteStats() {

            if (isShutdown() || !isRNActive()) {
                return;
            }

            final Set<PartitionId> partIdSet = repNode.getPartitions();
            if (partIdSet.isEmpty()) {
                return;
            }

            /*  Get all tables include top tables and inner tables */
            tableListMap = getAllTables();
            if (tableListMap == null) {
                return;
            }

            final TableMetadata metadata =
                    (TableMetadata) repNode.getMetadata(MetadataType.TABLE);

            if (metadata == null) {
                return;
            }

            /* It is possible that getTopology() returns null */
            final Topology topology = repNode.getTopology();
            if (topology == null) {
                return;
            }

            /* Find all existing partitions in the topology */
            final Set<PartitionId> allPartIdSet =
                    topology.getPartitionMap().getAllIds();
            /* Delete obsolete statistics from TableStatsTable */
            Table table = getTable(metadata, TableStatsPartitionDesc.TABLE_NAME);
            if (table != null) {
                /*
                 * Iterate all records of TableStatsPartition, get table name
                 * from the record
                 */
                Set<String> tableNameList = new TableScanner<String>() {
                    @Override
                    protected String covertResult(PrimaryKey pKey) {
                        return
                            pKey.get(TableStatsPartitionDesc.COL_NAME_TABLE_NAME).
                                                        asString().get();
                    }
                }.getAllPrimaryKeys((TableImpl)table);

                deleteStatsByTable(table,
                                   TableStatsPartitionDesc.COL_NAME_TABLE_NAME,
                                   TableStatsPartitionDesc.COL_NAME_PARTITION_ID,
                                   allPartIdSet,
                                   tableNameList);
            }

            /* Find all existing shards */
            Set<RepGroupId>  shardIds = topology.getRepGroupIds();

            /* Delete obsolete statistics from IndexLeaseTable */
            table = getTable(metadata, IndexStatsLeaseDesc.TABLE_NAME);
            if (table != null) {
                /*
                 * Iterate all records of IndexStatsLease, get table name,
                 * index name and shard id in the primary key
                 */
                Set<PrimaryKey> indexList = new TableScanner<PrimaryKey>() {
                    @Override
                    protected PrimaryKey covertResult(PrimaryKey pKey) {
                        return pKey;
                    }
                }.getAllPrimaryKeys((TableImpl)table);

                deleteStatsByIndex(table,
                                   IndexStatsLeaseDesc.COL_NAME_TABLE_NAME,
                                   IndexStatsLeaseDesc.COL_NAME_INDEX_NAME,
                                   IndexStatsLeaseDesc.COL_NAME_SHARD_ID,
                                   shardIds,
                                   indexList);
            }

            /* Delete obsolete statistics from IndexStatsTable */
            table = getTable(metadata, TableStatsIndexDesc.TABLE_NAME);
            if (table != null) {
                /*
                 * Iterate all records of TableStatsIndex, get table name,
                 * index name and shard id in the primary key
                 */
                Set<PrimaryKey> indexList = new TableScanner<PrimaryKey>() {
                    @Override
                    protected PrimaryKey covertResult(PrimaryKey pKey) {
                        return pKey;
                    }
                }.getAllPrimaryKeys((TableImpl)table);

                deleteStatsByIndex(table,
                                   TableStatsIndexDesc.COL_NAME_TABLE_NAME,
                                   TableStatsIndexDesc.COL_NAME_INDEX_NAME,
                                   TableStatsIndexDesc.COL_NAME_SHARD_ID,
                                   shardIds,
                                   indexList);
            }
        }

        /**
         * Delete statistics belongs to deleted tables from statistics tables.
         * A table is already deleted, all the statistics of the table should
         * be deleted. The method is to use the passed table name list
         * to check whether the table is deleted or not. If the table is
         * deleted, then remove its statistics from statistics or lease table.
         *
         * @param primaryKey is the primary key of the statistics/lease tables.
         * @param tableNameField is to indicate which column is to store table
         * name in statistics tables.
         * @param partitionIdField is to indicate which column is to store
         * partition id in statistics tables
         * @param partIdSet all partitions in the topology
         * @param all table names in stats tables.
         */
        private void deleteStatsByTable(Table target,
                                        String tableNameField,
                                        String partitionIdField,
                                        Set<PartitionId> partIdSet,
                                        Set<String> tableNameList) {
            /*
             * check the tables name and determine whether the associated
             * records should be deleted or not.
             */
            int numDeletedRecords = 0;
            for (String tableName : tableNameList) {
                try {
                    if (!tableListMap.containsKey(tableName) &&
                        !tableName.equals(PartitionScan.KV_STATS_TABLE_NAME)) {
                        /*
                         * Delete stats of the associated table for all
                         * partitions from stats table.
                         */
                        for (PartitionId partId : partIdSet) {
                            if (isShutdown() || !isRNActive()) {
                                return;
                            }
                            PrimaryKey pk = target.createPrimaryKey();
                            pk.put(tableNameField, tableName);
                            pk.put(partitionIdField, partId.getPartitionId());
                            if(tableAPI.delete(pk, null, null)) {
                                numDeletedRecords++;
                            }
                        }
                    }
                } catch (DurabilityException |
                         RequestTimeoutException ignore) {
                    /* Get it on the next pass. */
                }
            }

            if (numDeletedRecords > 0) {
                logger.log(Level.FINE,
                           "Deleted {0} record(s) of obsolete statistics " +
                           "from {1}",
                           new Object[]{numDeletedRecords,
                                        target.getFullName()});
            }
        }

        /**
         * Delete statistics belongs to deleted indices from statistics/lease
         * tables. An index is already deleted, all the statistics of the index
         * should be deleted. This method is to iterate all records of a
         * statistics or lease table, get table name and index name and then
         * use them to check whether the index is deleted or not. If the index
         * is deleted, then remove its statistics from statistics or lease
         * table.
         *
         * @param primaryKey is the primary key of the statistics/lease table.
         * @param tableNameField is to indicate which column is to store table
         * name in statistics tables.
         * @param indexNameField is to indicate which column is to store index
         * name in statistics tables.
         */
        private void deleteStatsByIndex(Table target,
                                        String tableNameField,
                                        String indexNameField,
                                        String shardIdField,
                                        Set<RepGroupId> shardIds,
                                        Set<PrimaryKey> indexList) {
            /*
             * check the passed primary key and determine whether the associated
             * records should be deleted or not.
             */
            int numDeletedRecords = 0;

            for (PrimaryKey pkey : indexList) {
                if (isShutdown() || !isRNActive()) {
                    return;
                }
                String tableName = pkey.get(tableNameField).asString().get();
                String indexName = pkey.get(indexNameField).asString().get();
                int shardId = pkey.get(shardIdField).asInteger().get();

                try {
                    /*
                     * Delete the record when the table is removed or the shard
                     * is removed
                     */
                    if (!tableListMap.containsKey(tableName) ||
                            !shardIds.contains(new RepGroupId(shardId))) {
                        PrimaryKey pk = target.createPrimaryKey();
                        pk.put(tableNameField, tableName);
                        pk.put(indexNameField, indexName);
                        pk.put(shardIdField, shardId);
                        if(tableAPI.delete(pk, null, null)) {
                            numDeletedRecords++;
                        }
                    } else if (tableListMap.get(tableName).
                            getIndex(indexName) == null) {
                        /* Delete the record when the index is removed */
                        PrimaryKey pk = target.createPrimaryKey();
                        pk.put(tableNameField, tableName);
                        pk.put(indexNameField, indexName);
                        pk.put(shardIdField, shardId);
                        if(tableAPI.delete(pk, null, null)) {
                            numDeletedRecords++;
                        }
                    }
                } catch (DurabilityException |
                         RequestTimeoutException ignore) {
                    /* Get it on the next pass. */
                }
            }

            if (numDeletedRecords > 0) {
                logger.log(Level.FINE,
                           "Deleted {0} record(s) of obsolete statistics " +
                           "from {1}",
                           new Object[]{numDeletedRecords,
                                        target.getFullName()});
            }
        }

        /* -- From ShutdownThread -- */

        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        protected int initiateSoftShutdown() {
            synchronized (this) {
                if (tableSizeUpdater != null) {
                    tableSizeUpdater.stop();
                }
                if (statsScanHandler != null) {
                    statsScanHandler.stop();
                }
            }
            return super.initiateSoftShutdown();
        }

        /**
         * The class is designed to scan a table in all partitions of a RepNode.
         */
        private abstract class TableScanner<T> {
            /* Covert the primary key */
            abstract protected T covertResult(PrimaryKey key);

            /* Scan the passed table in all partitions of a RepNode */
            Set<T> getAllPrimaryKeys(TableImpl target) {
                Set<T>  list = new HashSet<>();

                PrimaryKeyImpl pk = target.createPrimaryKey();
                byte[] keyByte = pk.createKeyBytes();
                final Set<PartitionId> partIdSet = repNode.getPartitions();

                for (PartitionId partId : partIdSet) {
                    Database db = repNode.getPartitionDB(partId);
                    list.addAll(scanDatabase(db, target, keyByte));
                }

                return list;
            }

            /* Scan the passed table in a partition */
            private Set<T> scanDatabase(Database db,
                                        TableImpl target,
                                        byte[] initialkeyBytes) {
                Set<T> list = new HashSet<>();
                Cursor cursor = null;
                Transaction txn = null;

                final ReplicatedEnvironment env =
                        (ReplicatedEnvironment)db.getEnvironment();

                try {
                    txn = env.beginTransaction(null, StatsScan.TXN_CONFIG);
                    txn.setTxnTimeout(
                        StatsScan.TXN_TIME_OUT, TimeUnit.MILLISECONDS);

                    cursor = db.openCursor(txn, StatsScan.CURSOR_CONFIG);
                    cursor.setCacheMode(CacheMode.UNCHANGED);

                    final DatabaseEntry keyEntry = new DatabaseEntry();
                    final DatabaseEntry dataEntry = new DatabaseEntry();
                    dataEntry.setPartial(0, 0, true);
                    OperationStatus status;

                    keyEntry.setData(initialkeyBytes);
                    status = cursor.getSearchKeyRange(keyEntry, dataEntry,
                                            LockMode.READ_UNCOMMITTED);

                    PrimaryKey pKey = null;
                    while ((status == OperationStatus.SUCCESS) && !isShutdown()) {

                        /* Track the cost of the read for load management */
                        final int storageSize =
                              DbInternal.getCursorImpl(cursor).getStorageSize();
                        aggregateThroughputTracker.addReadBytes(storageSize,
                                                                false);

                        byte[] keyData = keyEntry.getData();
                        if (target.findTargetTable(keyData) == null) {
                            break;
                        }

                        pKey = target.createPrimaryKeyFromKeyBytes(keyData);
                        list.add(covertResult(pKey));
                        dataEntry.setPartial(0, 0, true);
                        status = cursor.getNext(keyEntry, dataEntry,
                                          LockMode.READ_UNCOMMITTED);
                    }
                } catch (DatabaseException | IllegalArgumentException e) {
                    logger.log(Level.FINE,
                               "Exception encountered scanning " +
                               target.getFullName(), e);
                } finally {
                    if (cursor != null) {
                        TxnUtil.close(cursor);
                    }

                    /* We are just reading. Abort every transaction */
                    TxnUtil.abort(txn);
                }
                return list;
            }
        }

        /**
         * Scan partition database with the RN which KeyStatsCollector
         * resides and store the collected statistics information into
         * statistics tables.
         * @return the number of records scanned
         * @throws Exception is thrown when the statistics tables do not exist
         */
        private long scanPartitions(long intervalStart) throws Exception {

            /* Check whether can scan */
            if (isShutdown() || !isRNActive()) {
                return 0L;
            }

            /*
             * Initialize the partition lease manager. If it is set up before,
             * checks if partition lease table exist.
             */
            if (partitionLeaseManager == null) {
                partitionLeaseManager = new PartitionLeaseManager(tableAPI);
            } else if (!partitionLeaseManager.leaseTableExists()) {
                logger.log(Level.FINE, "Partition lease table {0} not found. " +
                           "Parition scan stops.",
                           partitionLeaseManager.getLeaseTableName());
                return 0L;
            }

            /* Fetch all partition databases with RN */
            final Set<PartitionId> partIdSet = repNode.getPartitions();
            if (partIdSet.isEmpty()) {
                return 0L;
            }

            /* Get RN name and group id where the RN is */
            final String rnName = repNode.getRepNodeId().getFullName();

            /*
            * Start gather the statistics information for the selected
            * partition
             */
            int scannedParts = 0;
            long scannedRecords = 0L;
            for (PartitionId partId : partIdSet) {
                /* Check whether can scanning */
                if (isShutdown() || !isRNActive()) {
                    break;
                }

                /* Create LeaseInfo for scanning of selected partition */
                final PartitionLeaseInfo leaseInfo =
                        new PartitionLeaseInfo(partId.getPartitionId(),
                                               rnName,
                                               statsLeaseDuration,
                                               leaseTTL);

                /* Create a lease manager to control the lease */
                statsScanHandler = new PartitionScan(tableAPI,
                                                     partId,
                                                     repNode,
                                                     partitionLeaseManager,
                                                     leaseInfo,
                                                     intervalStart,
                                                     logger);

                /*
                 * Start gather the statistics information for the selected
                 * partition. If scan successfully, to accumulate the scanned
                 * information: scanned partition number and scanned records.
                 */
                if (statsScanHandler.runScan()) {
                    scannedParts++;
                    scannedRecords += statsScanHandler.getTotalRecords();
                }
            }
            if (scannedParts > 0) {
                    logger.log(Level.FINE,
                               "Partition scanning completed: scan {0} " +
                               "partition(s) and {1} record(s).",
                               new Object[] {scannedParts, scannedRecords});
            }
            return scannedRecords;
        }


        /**
         * Get all tables store in KVStore
         * @return the map storing all tables and their names
         */
        private Map<String, TableImpl> getAllTables() {
            final TableMetadata metadata =
                    (TableMetadata)repNode.getMetadata(MetadataType.TABLE);
            if (metadata == null) {
                return null;
            }

            /* Use the iterateTables method to get all TableMetadata */
            final Map<String, TableImpl> map = new HashMap<>();
            final TableMetadataIteratorCallback callback =
                new TableMetadata.TableMetadataIteratorCallback() {

                @Override
                public boolean tableCallback(Table t) {
                    map.put(t.getFullNamespaceName(), (TableImpl)t);
                    return true;
                }
            };

            metadata.iterateTables(callback);
            return map;
        }

        /**
         * Scan index secondary database and store the results into statistics
         * tables
         * @return the number of records scanned
         * @throws Exception
         */
        private long scanTableIndexes(long intervalStart) throws Exception {

            /* Check whether can scan */
            if (isShutdown() || !isRNActive()) {
                return 0L;
            }

            /*
             * Initialize the index lease manager. If it is set up before,
             * checks if index lease table exist.
             */
            if (indexLeaseManager == null) {
                indexLeaseManager = new IndexLeaseManager(tableAPI);
            } else if (!indexLeaseManager.leaseTableExists()) {
                logger.log(Level.FINE, "Index lease table {0} not found. " +
                           "Index scan stops.",
                           indexLeaseManager.getLeaseTableName());
                return 0L;
            }

            /* Get RN name and group id where the RN is */
            final String rnName = repNode.getRepNodeId().getFullName();
            final int groupId = repNode.getRepNodeId().getGroupId();

            /* Get all tables include top tables and inner tables */
            tableListMap = getAllTables();
            if (tableListMap == null || (tableListMap.isEmpty())) {
                return 0L;
            }

            /*
             * Get table/index pairs and try to scan the mapped secondary
             * database
             */
            int scannedIndices = 0;
            long scannedRecords = 0L;
            for (final TableImpl table : tableListMap.values()) {

                for (final Map.Entry<String, Index> entry :
                    table.getIndexes().entrySet()) {

                    final Index index = entry.getValue();

                    /* skip all text indices */
                    if (index.getType().equals(Index.IndexType.TEXT)) {
                        continue;
                    }

                    /* Check whether can scanning */
                    if (isShutdown() || !isRNActive()) {
                        break;
                    }

                    final String indexName = index.getName();

                    /*
                     * Create LeaseInfo for scanning of selected index
                     * secondary database
                     */
                    final IndexLeaseInfo leaseInfo =
                        new IndexLeaseInfo(table,
                                           indexName, groupId,
                                           rnName,
                                           statsLeaseDuration,
                                           leaseTTL);

                    statsScanHandler = new TableIndexScan(tableAPI,
                                                          table,
                                                          indexName,
                                                          repNode,
                                                          indexLeaseManager,
                                                          leaseInfo,
                                                          intervalStart,
                                                          logger);

                    /*
                     * Start gather the statistics information for the selected
                     * index secondary database.  If scan successfully, to
                     * accumulate the scanned information: scanned indices
                     * number and scanned records.
                     */
                    if (statsScanHandler.runScan()) {
                        scannedIndices++;
                        scannedRecords += statsScanHandler.getTotalRecords();
                    }
                }
            }
            if (scannedIndices > 0) {
                if (scannedIndices == 1) {
                    logger.log(Level.FINE,
                               "Index scanning completed: scan {0} index and " +
                               "{1} record(s).",
                               new Object[] {scannedIndices, scannedRecords});
                } else {
                    logger.log(Level.FINE,
                               "Index scanning completed: scan {0} indices " +
                               "and {1} record(s).",
                               new Object[] {scannedIndices, scannedRecords});
                }
            }
            return scannedRecords;
        }
    }

    /*
     * This is only called for system tables, which have no namespace
     */
    private static TableImpl getTable(TableMetadata md,
                                      String tableName) {
        return md.getTable(null, /* namespace */
                           tableName);
    }
}
