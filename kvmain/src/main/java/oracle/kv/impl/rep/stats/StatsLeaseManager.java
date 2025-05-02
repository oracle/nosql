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

import java.text.ParseException;
import java.text.SimpleDateFormat;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Version;
import oracle.kv.impl.rep.stats.StatsLeaseManager.LeaseInfo;
import oracle.kv.impl.systables.StatsLeaseDesc;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

/**
 * This class manages the leases associated with the statistics tables.
 * An RN must hold a lease associated with partition, or a shard-index before
 * it can update the associated statistics.
 *
 * The lease mechanism is to deal with coordinating scanning among RNs
 * in a shard and deal with the failures of RNs. The mechanism is as follows:
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
abstract class StatsLeaseManager <L extends LeaseInfo> {
    /*
     * Currently, Date type is not supported for table in KVStore. To store
     * Date type, all Date type should be converted as String.
     */
    static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");

    private final static ReadOptions ABSOLUTE_READ_OPTION =
            new ReadOptions(Consistency.ABSOLUTE, 0, null);


    private final static WriteOptions NO_SYNC_WRITE_OPTION =
            new WriteOptions(Durability.COMMIT_NO_SYNC, 0, null);

    private final TableAPI tableAPI;
    protected final Table leaseTable;

    /* lastUpdatedDateStr should be empty when the lease is created firstly */
    protected String lastUpdatedDateStr = "";

    /* The expiry time for the lease */
    protected long leaseExpiryTime = 0;

    protected StatsLeaseManager(TableAPI tableAPI) {
        this.tableAPI = tableAPI;

        leaseTable = tableAPI.getTable(getLeaseTableName());
        if (leaseTable == null) {
            throw new IllegalArgumentException(
                "Lease table " + getLeaseTableName() + " not found");
        }
    }

    /**
     * Get the lease from the lease table in database, if there is one. If
     * this is the first time that statistics are being created for a
     * partition, or a shard-index, an entry for it may not exist in the lease
     * table.
     * The lease if it exists, may have expired or may be active but belong to
     * some other RN.
     *
     * @return the lease, or null
     */
    Lease getStoredLease(L leaseInfo) {
        final PrimaryKey pk = createPrimaryKey(leaseInfo);

        /* Get lease from lease table */
        final Row row = tableAPI.get(pk, ABSOLUTE_READ_OPTION);
        if (row == null) {
            /* lastUpdatedDateStr should be empty when no lease exists */
            lastUpdatedDateStr = "";
            return null;
        }

        lastUpdatedDateStr =
            row.get(StatsLeaseDesc.COL_NAME_LAST_UPDATE).asString().get();
        return new Lease(row.get(StatsLeaseDesc.COL_NAME_LEASE_RN).
                                                               asString().get(),
                         row.get(StatsLeaseDesc.COL_NAME_LEASE_DATE).
                                                               asString().get(),
                         lastUpdatedDateStr,
                         row.getVersion());
    }

    /**
     * Update the stored lease information to do one of:
     *
     *   1) Acquire the lease for this RN (latestVersion == null)
     *   2) Extend the acquired lease for this RN (leaseInfo.terminated is
     *   false)
     *   3) Terminate the lease acquired by this RN (leaseInfo.terminated is
     *   true)
     *
     * @return the version if the operation was successful, null otherwise.
     */
    private Version updateLease(L leaseInfo,
                                boolean isTerminated,
                                Version latestVersion) {
        final Row newRow = createRow(leaseInfo, isTerminated);

        final Version version;
        if (latestVersion == null) {
            /* Use putIfAbsent to ensure that the lease is a new one. */
            version = tableAPI.putIfAbsent(newRow, null, NO_SYNC_WRITE_OPTION);
        } else {
            /*
             * Use putIfVersion to ensure that we are updating the version we
             * have read or updated. Note that latestVersion may be null if we
             * are creating a new entry.
             */
            version = tableAPI.putIfVersion(newRow, latestVersion, null,
                    NO_SYNC_WRITE_OPTION);
        }

        return version;
    }

    /**
     * Create a new lease
     *
     * @return version if it was created, null if some other RN beat us to it
     */
    Version createLease(L leaseInfo) {
        return updateLease(leaseInfo, false, null);
    }

    /**
     * Renew a lease
     * @return version when renewing successfully; or return null
     */
    Version renewLease(L leaseInfo, Version latestVersion) {
        assert(latestVersion != null);
        return updateLease(leaseInfo, false, latestVersion);
    }

    /**
     * Terminate the currently help lease.
     * @return version when renewing successfully; or return null
     */
    Version terminateLease(L leaseInfo, Version latestVersion) {
        return updateLease(leaseInfo, true, latestVersion);
    }

    /**
     * Extend the lease, but only if it's within 10% of its expiry time.
     *
     * @return modified version when extend lease successfully; return the
     * passed latestVersion when no need extend lease; or return null when
     * extend lease failed or the lease does not exist.
     */
    Version extendLeaseIfNeeded(L leaseInfo, Version latestVersion) {

        /*
         * No need to check lease whether need to be extended when
         * current timestamp is not near the lease expiry time
         */
        long thresholdTime = leaseExpiryTime -
                (long)(leaseInfo.getLeaseTime() * 0.2);
        if (System.currentTimeMillis() < thresholdTime) {
            return latestVersion;
        }

        final PrimaryKey pk = createPrimaryKey(leaseInfo);

        /* Get lease from lease table */
        final Row row = tableAPI.get(pk, ABSOLUTE_READ_OPTION);
        if (row == null) {
            return null;
        }

        /*
         * The version is not equal as the latest version, it means the lease
         * is modified by another thread. The lease owned by the current thread
         * is invalid, return null.
         */
        if(!row.getVersion().equals(latestVersion)) {
            return null;
        }

        long leaseExpiryTimestamp = getExpiryTimestamp(row);

        /* try to extend the lease when 10% lease duration left. */
        long extendThreholdTimestamp = leaseExpiryTimestamp -
                (long)(leaseInfo.getLeaseTime() * 0.1);
        if (System.currentTimeMillis() >= extendThreholdTimestamp) {
            return updateLease(leaseInfo, false, latestVersion);
        }
        return latestVersion;
    }

    long getExpiryTimestamp(Row row) {
        final String expiryTimestampStr =
                row.get(StatsLeaseDesc.COL_NAME_LEASE_DATE).asString().get();
        long expiryTimestamp;

        try {
            expiryTimestamp = DATE_FORMAT.parse(expiryTimestampStr).getTime();
        } catch (ParseException e) {
            expiryTimestamp =  Long.MAX_VALUE;
        }
        return expiryTimestamp;
    }

    /**
     * Checks if the lease table exists. If not, an IllegalArgumentException
     * will be throw.
     */
    boolean leaseTableExists() {
        return tableAPI.getTable(getLeaseTableName()) != null;
    }

    /**
     * Returns the name of internal lease table
     */
    abstract String getLeaseTableName();

    /**
     * Create a primary key via the passed lease info
     * @param info is the lease info which contains the all values of creating
     * a primary key
     * @return a primary key
     */
    abstract PrimaryKey createPrimaryKey(L info);

    /**
     * Create a row via the passed lease info
     * @param info is the lease info which contains the all values of creating
     * a row
     * @param terminated is to indicate which the created row is the the
     * terminated lease row. If yes, expiry date = current date, or
     * expiry date = current date + lease time
     * @return a row
     */
    abstract Row createRow(L info, boolean terminated);

    /**
     * A convenience class class containing the expiry date of a lease, and the
     * time it was last updated.
     *
     */
    class Lease {
        private final String leaseRN;
        private final String expiryDate;
        private final String lastUpdated;
        private final Version latestVersion;

        private Lease(String leaseRN,
                      String expiryDate,
                      String lastUpdated,
                      Version latestVersion) {
            this.leaseRN = leaseRN;
            this.expiryDate = expiryDate;
            this.lastUpdated = lastUpdated;
            this.latestVersion = latestVersion;
        }

        /**
         * Return the owner RN of the lease
         */
        String getLeaseRN() {
            return leaseRN;
        }

        /**
         * Return the expiry date of the lease
         */
        String getExpiryDate() {
            return expiryDate;
        }

        /**
         * Return last updated date of the lease
         */
        String getLastUpdated() {
            return lastUpdated;
        }

        /**
         * Return latest version of this lease
         */
        Version getLatestVersion() {
            return latestVersion;
        }
    }

    /**
     * The base class for different types of leases, e.g. partition,
     * shard-index.
     */
    static abstract class LeaseInfo {
        /* The RN holding the lease. */
        protected final String leaseRN;

        /* The time by which the lease will be extended. */
        private final long leaseTime;

        /* TTL for lease records. */
        private final TimeToLive ttl;

        protected LeaseInfo(String leaseRN, long leaseTime, TimeToLive leaseTTL) {
            /* This assumes leaseTime is < days */
            assert leaseTime <= KeyStatsCollector.MAX_LEASE_DURATION_MS;
            this.leaseRN = leaseRN;
            this.leaseTime = leaseTime;
            ttl = leaseTTL;
        }

        protected String getLeaseRN() {
            return leaseRN;
        }

        protected long getLeaseTime() {
            return leaseTime;
        }
        
        /**
         * Gets the TTL to be applied to the lease record.
         * 
         * @return the lease record TTL
         */
        protected TimeToLive getTTL() {
            return ttl;
        }
    }
}
