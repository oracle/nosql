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

import static oracle.kv.impl.api.ops.ResourceTracker.RW_BLOCK_SIZE;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Version;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.ops.ResourceTracker;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.stats.StatsLeaseManager.LeaseInfo;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * the class collects statistics information into statistics tables. The
 * subclasses of StatsScan provide the partition or shard-index specific
 * behaviors.
 */
public abstract class StatsScan <T extends LeaseInfo> {

    /* The batch size to be used when iterating over keys. */
    public static final int BATCH_SIZE = 10000;

    /* The time out of transaction */
    static final int TXN_TIME_OUT = 5000;

    /*
     * Build transaction configuration and cursor configuration, which are
     * used to scan the target database
     */
    static final TransactionConfig TXN_CONFIG =
        new TransactionConfig()
        .setReadOnly(true)
        .setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

    /*
     * Set as READ_UNCOMMITTED, it is more efficient since the statistics
     * don't have to be 100% accurate.
     */
    static final CursorConfig CURSOR_CONFIG =
            new CursorConfig().setReadUncommitted(true);

    /*
     * The write option uses relaxed durability, since the data is low value
     * and it's ok, if the data for a specific interval is missing, it will
     * simply be recomputed onthe next pass.
     */
    static final WriteOptions NO_SYNC_NO_ACK_WRITE_OPTION =
            new WriteOptions(new Durability(SyncPolicy.NO_SYNC,
                                            SyncPolicy.NO_SYNC,
                                            ReplicaAckPolicy.NONE),
                             0, null);

    /** default read option that includes tombstone  */
    static final ReadOptions DEFAULT_READ_OPT =
        DbInternal.getReadOptions(LockMode.READ_UNCOMMITTED)
                  .clone()
                  .setExcludeTombstones(false);

    /* it is the RN which KeyStatsCollector resides */
    protected final RepNode repNode;

    /* Used to store the statistics results into statistics tables */
    private final TableAPI tableAPI;

    private final ResourceTracker aggregateThroughputTracker;

    /* leaseManager is to determine whether extend a lease */
    private final StatsLeaseManager<T> leaseManager;

    /* leaseInfo is to store the necessary info for the lease */
    private final T leaseInfo;

    /* The start time of the current scan. */
    private final long intervalStart;

    /* TTL for stats data records */
    protected final TimeToLive ttl;

    /* Logger handler */
    protected final Logger logger;

    /** Whether to include the storage size when collecting statistics. */
    protected final boolean includeStorageSize;

    /* Used to store wrapper table rows of statistics after a scan. */
    private final List<Row> statRows = new ArrayList<>();

    /* Flag to mark whether the scanning is stopped or not */
    private volatile boolean stop;

    /**
     * latestVersion is to store the latest database version of the lease, it
     * is used to determine whether a lease is modified by another gathering
     * thread
     */
    private Version latestVersion;

    /* the scanned total records in the tables or indices */
    protected long totalRecords = 0;

    /**
     * The number of bytes read but not been reported yet because the count has
     * not reached 1 KB.
     */
    private final AtomicInteger unreportedBytesRead = new AtomicInteger();

    protected StatsScan(RepNode repNode,
                        TableAPI tableAPI,
                        StatsLeaseManager<T> leaseManager,
                        T leaseInfo,
                        long intervalStart,
                        Logger logger) {
        this.repNode = repNode;
        this.tableAPI = tableAPI;
        this.leaseManager = leaseManager;
        this.leaseInfo = leaseInfo;
        this.intervalStart = intervalStart;
        this.logger = logger;

        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        ttl = repNodeParams.getStatsTTL();
        includeStorageSize = repNodeParams.getStatsIncludeStorageSize();
        aggregateThroughputTracker = repNode.getAggregateThroughputTracker();
    }

    void stop() {
        stop = true;
    }

    /*
     * Returns true if the scan should stop. This could be due to the
     * stop() method being called or the RN is not active.
     */
    protected boolean isStopped() {
        if (stop) {
            return stop;
        }
        final ReplicatedEnvironment repEnv = repNode.getEnv(0L);
        return (repEnv == null) ? true : !repEnv.getState().isActive();
    }

    /**
     * Check whether the lease scan meets the condition of lease time and
     * interval time, and whether it can be started or not.
     * @return true if the scan can be started; or return false
     */
    private boolean checkLease() {
        /* Get a existing lease from database */
        final StatsLeaseManager<T>.Lease lease =
                leaseManager.getStoredLease(leaseInfo);

        if (lease == null) {
            /* No lease exists in database, try to create a new lease */
            final Version version = leaseManager.createLease(leaseInfo);
            if (version == null) {
                logger.log(Level.FINE, "Get lease failed: another " +
                                       "ScanningThread already created " +
                                       "a lease for scanning the " +
                                       "selected partition");
                return false;
            }
            latestVersion = version;
            return true;
        }
        latestVersion = lease.getLatestVersion();

        /* When the lease exist, check whether the lease expires or not */
        final String expiryTimeStr = lease.getExpiryDate();
        long expiryTime;
        try {
            expiryTime = StatsLeaseManager.DATE_FORMAT.
                    parse(expiryTimeStr).getTime();
        } catch (ParseException e) {
            logger.log(Level.WARNING, "ScanningThread converts String into " +
                                      "Date failed: {0}", e);
            /*
             * Cannot convert String into Date, it is unexpected and cease
             * scanning
             */
            return false;
        }

        /* Lease is not expired and held by another StatsGather */
        if (System.currentTimeMillis() < expiryTime &&
            !lease.getLeaseRN().equals(leaseInfo.getLeaseRN())) {
            return false;
        }

        /* When the lease exists, check last updated date */
        final String lastUpdatedStr = lease.getLastUpdated();

        /*
         * Check lastUpdated when the lastUpdatedStr is not empty. If
         * lastUpdatedStr is empty, it means no previous scanning completes,
         * and the next scanning is the first scanning, so no need check the
         * lastUpdated
         */
        if (!lastUpdatedStr.isEmpty()) {
            long lastUpdated;
            try {
                lastUpdated = StatsLeaseManager.DATE_FORMAT.
                        parse(lastUpdatedStr).getTime();
            } catch (ParseException e) {
                logger.log(Level.WARNING, "ScanningThread converts String " +
                                          "into Date failed: {0}", e);

                /*
                 * Cannot convert String into Date, it is unexpected and cease
                 * scanning
                 */
                return false;
            }

            /*
             * Skip if the last update was after the start of the current
             * scan interval.
             */
            if (lastUpdated >= intervalStart) {
                return false;
            }
        }

        /* If the lease expired, renew the lease. */
        final Version version =
                leaseManager.renewLease(leaseInfo, latestVersion);
        if (version == null) {
            logger.log(Level.FINE, "Get lease failed: another " +
                                   "ScanningThread already renewed the lease");
            return false;
        }
        latestVersion = version;
        return true;
    }

    protected boolean runScan() throws Exception {
        try {

            /* Check whether we own the lease */
            if(isStopped() || !checkLease()) {
                return false;
            }

            /* Start the scanning thread */
            logger.log(Level.FINE, "Lease acquired, scanning {0} starts",
                       leaseInfo);

            /* Do the task in the beginning of scan */
            if (isStopped() || !preScan()) {
                return false;
            }

            boolean scanCompleted = true;
            try {
                /* Scan the database */
                scanCompleted = scan();
            } finally {

                /* Do the task in the end of scan */
                postScan(scanCompleted);
            }

            /*
             * When scan is stopped or scan is not completed, the following
             * statements are not executed
             */
            if (!scanCompleted || isStopped()) {
                return false;
            }

            Version version = leaseManager.extendLeaseIfNeeded(leaseInfo,
                                                               latestVersion);
            /*
             * Ensure that we have the lease before updating the stats. The
             * lease is owned by another thread, version is null and exit scan
             */
            if (version == null) {
                logger.log(Level.FINE,
                           "Failed to extend statistics lease before stats " +
                           "table update");
                return false;
            }

            /* Copy the latest version after extending lease */
            latestVersion = version;

            /* save the scanning result into statistics tables */
            saveResult();

            /*
             * Scan completed, modify the last updated time and terminate the
             * lease. Note that the lease must only be terminated after the
             * statistics have been updated by the runScan method.
             */
           version = leaseManager.terminateLease(leaseInfo, latestVersion);
           latestVersion = version;
           logger.log(Level.FINE, "Lease scanning {0} completed", leaseInfo);

           return !isStopped();
        } catch (Exception e) {
            if (repNode.isStopped()) {
                logger.log(Level.FINE, "RepNode is stopped, " +
                           "statistics scanning exists: {0}", e);
            } else {
                throw e;
            }
        }

        /* Return false encounter any exceptions */
        return false;
    }

    /**
     * Reads the next entry from given cursor include tombstone
     * @param cursor cursor of database
     * @param key    key entry
     * @param data   data entry
     * @return operation result if a record is returned, or null if not found
     */
    OperationResult getNextInternal(Cursor cursor,
                                    DatabaseEntry key,
                                    DatabaseEntry data) {
        return cursor.get(key, data, Get.NEXT, DEFAULT_READ_OPT);
    }

    /**
     * Add a scan result into a list.
     *
     * @param row a row of collected statistics result
     */
    protected void addRow(Row row) {
        statRows.add(row);
    }

    /**
     * Save all scan results into database.
     * @throws Exception
     */
    private void saveResult() throws Exception {
        logger.log(Level.FINE, "Store statistics information of into database");

        if (tableAPI == null) {
            logger.log(Level.FINE, "Table API is invalid, store " +
                    "statistics information into database failed.");
            throw new IllegalStateException("Table API is invalid," +
                    "store statistics information into database failed");
        }

        /* When scan is stopped, the following statements are not executed */
        if (isStopped()) {
            return;
        }

        for (final Row row : statRows) {
            try {
                tableAPI.put(row, null, NO_SYNC_NO_ACK_WRITE_OPTION);
            } catch (DurabilityException | RequestTimeoutException e) {
                logger.log(Level.FINE, "Exception found when put " +
                            UserDataControl.displayRow(row), e);
            } catch (FaultException e) {
                logger.log(Level.FINE, "FaultException found when put " +
                            UserDataControl.displayRow(row), e);
            }  catch (KVSecurityException kse) {
                logger.log(Level.FINE, "KVSecurityException found when put " +
                            UserDataControl.displayRow(row), kse);
            } catch (Exception e) {
                throw e;
            }
        }
        statRows.clear();
    }

    void accumulateResult(byte[] key, OperationResult result) {
        /*
         * getStorageSize returns the estimated disk storage size for the
         * record at the current position.
         */
        final int sz = result.getStorageSize();

        /*
         * Track the cost of the read for load management, regardless of
         * whether it is a tombstone or not. Call getReadSize to allow
         * different implementations to specify how many bytes were read since,
         * for example, they might not read value bytes.
         */
        unreportedBytesRead.addAndGet(getReadSize(key.length, sz));

        /*
         * Report reads in RW_BLOCK_SIZE chunks rather than allowing the
         * addReadBytes call to round up each read to the block size. The
         * rounding was overstating the read cost and causing throttling
         * incorrectly. The final read in the scan will probably not always be
         * reported this way, but that small inaccuracy seems pretty harmless.
         * Use a loop to handle concurrent races. [KVSTORE-1929]
         *
         * TODO: Maybe modify ResourceTracker to support tallying unrounded
         * values so we could that capability more generally.
         */
        while (true) {
            final int bytes = unreportedBytesRead.get();
            if (bytes < RW_BLOCK_SIZE) {
                break;
            }
            final int remainder = bytes % RW_BLOCK_SIZE;
            final int fullBlocks = bytes - remainder;
            if (unreportedBytesRead.compareAndSet(bytes, remainder)) {
                aggregateThroughputTracker.addReadBytes(fullBlocks, false);
            }
        }

        /* count regular record or tombstone size */
        accumulateResult(key, sz, result.isTombstone());
    }

    /**
     * Returns the number of bytes to count as read throughput for an entry
     * with the specified key size and total storage size. By default returns
     * the total storage size.
     */
    int getReadSize(@SuppressWarnings("unused") int keySize,
                    int totalStorageSize) {
        return totalStorageSize;
    }

    /**
     * Accumulate result of every iteration scan.
     *
     * @param key the record's key bytes
     * @param storageSize the disk storage size for non-tombstone record
     * @param isTombstone true if the storageSize is the size of a tombstone,
     *                   false otherwise.
     */
    abstract void accumulateResult(byte[] key,
                                   int storageSize,
                                   boolean isTombstone);

    /**
     * Check whether the statistics tables exist or not
     * @param md
     * @return true when all statistics tables exist; or return false.
     */
    abstract boolean checkStatsTable(TableMetadata md);

    /**
     * Wrap the result of statistics as table rows and store the rows into
     * cache list
     */
    protected abstract void wrapResult();

    /**
     * Get the target database whose statistics information is scanned.
     * @return target database, the return result is always non-null
     */
    abstract Database getDatabase();

    /**
     * Performs pre scan work. Subclasses can override to do work before the
     * scan starts. Returns true if this step was successful, or false if the
     * scan should be skipped.
     *
     * @return true if the scan should proceed, false if it should be skipped
     */
    protected abstract boolean preScan();

    /**
     * Performs post scan work. Subclasses can override to do work after the
     * scan completes. This method is always called if scan() is called, even
     * if scan() returns false, or throws an exception. If scan() throws an
     * exception, scanComplete will be true, otherwise it is the value
     * returned by scan().
     */
    protected abstract void postScan(boolean scanCompleted);

    /**
     * Scans the target database accumulating statistics. If scan() is
     * called, postScan() will be called with the value returned by scan()
     * if true if scan() throws an exception.
     *
     * @return true if the scan completed, false, if it was incomplete.
     * If exceptions are thrown, the scan is interrupted.
     */
    private boolean scan() throws InterruptedException {
        final Database db = getDatabase();
        if (db == null) {
            throw new IllegalStateException("Database is null, scanning for " +
                                            leaseInfo + " exits");
        }

        final TableMetadata md =
            ((TableMetadata) repNode.getMetadata(MetadataType.TABLE));

        if (md == null) {
            throw new IllegalStateException("TableMetadataHelper is null, " +
                                            "scanning for " + leaseInfo +
                                            " exits");
        }

        /* Stop scanning when statistics tables are missing */
        if (!checkStatsTable(md)) {
            throw new IllegalStateException("Statistics tables are missing, " +
                                            "scanning for " + leaseInfo +
                                            " exits");
        }

        if (!leaseManager.leaseTableExists()) {
            throw new IllegalStateException("Lease table not found");
        }

        final ReplicatedEnvironment repEnv =
                (ReplicatedEnvironment) db.getEnvironment();

        /* When scan is stopped, the following statements are not executed */
        if (isStopped()) {
            return false;
        }

        logger.log(Level.FINE,
                   "Start scanning database: {0} to gather statistics",
                   db.getDatabaseName());

        boolean hasMoreElements = true;

        /*
         * Scan the database as long as not stopped and there are more elements
         */
        while(!isStopped() && hasMoreElements) {

            /* scan the target database an iteration */
            final Version version =
                    leaseManager.extendLeaseIfNeeded(leaseInfo, latestVersion);
            /* Return false when the lease cannot be extended */
            if (version == null) {
                return false;
            }
            latestVersion = version;

            hasMoreElements = scanDatabase(repEnv, db);
        }

        /* When scan is stopped, the following statements are not executed */
        if (isStopped()) {
            return false;
        }

        /* Wrap results as table rows */
        wrapResult();
        return true;
    }

    protected long getTotalRecords() {
        return totalRecords;
    }

    /**
     * Scan at most BATCH_SIZE kv pairs in the target database and put the
     * scanned data into the results IV.
     */
    abstract boolean scanDatabase(Environment env, Database db)
        throws InterruptedException;
}
