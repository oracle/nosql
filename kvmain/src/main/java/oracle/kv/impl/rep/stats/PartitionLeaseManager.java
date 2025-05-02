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

import java.util.Date;

import oracle.kv.impl.rep.stats.PartitionLeaseManager.PartitionLeaseInfo;
import oracle.kv.impl.rep.stats.StatsLeaseManager.LeaseInfo;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

/**
 * PartitionLeaseTable is used to manage the partition-wide lease table
 * associated with the gathering of table statistics for a partition.
 *
 */
public class PartitionLeaseManager extends
                    StatsLeaseManager<PartitionLeaseInfo> {

    public PartitionLeaseManager(TableAPI tableAPI) {
        super(tableAPI);
    }

    @Override
    PrimaryKey createPrimaryKey(PartitionLeaseInfo info) {
        final PrimaryKey primaryKey = leaseTable.createPrimaryKey();
        primaryKey.put(PartitionStatsLeaseDesc.COL_NAME_PARTITION_ID,
                       info.getParitionId());
        return primaryKey;
    }

    @Override
    Row createRow(PartitionLeaseInfo info, boolean terminated) {
        final Row row = leaseTable.createRow();
        row.setTTL(info.getTTL());
        row.put(PartitionStatsLeaseDesc.COL_NAME_PARTITION_ID,
                info.getParitionId());
        row.put(PartitionStatsLeaseDesc.COL_NAME_LEASE_RN,
                info.getLeaseRN());

        final long currentTime = System.currentTimeMillis();
        final Date currentDate = new Date(currentTime);
        final String currentDateStr =
                StatsLeaseManager.DATE_FORMAT.format(currentDate);

        /*
         * Mark last updated data means we are done gathering statistics
         * for the index and no longer need to extend the lease.
         */
        if (terminated) {
            row.put(PartitionStatsLeaseDesc.COL_NAME_LEASE_DATE,
                    currentDateStr);

            /* Update the lastUpdated after the lease scanning completes */
            row.put(PartitionStatsLeaseDesc.COL_NAME_LAST_UPDATE,
                    currentDateStr);
        } else {
            final long nextTime = currentTime + info.getLeaseTime();
            leaseExpiryTime = nextTime;
            final Date nextDate = new Date(nextTime);
            final String nextDateStr =
                    StatsLeaseManager.DATE_FORMAT.format(nextDate);
            row.put(PartitionStatsLeaseDesc.COL_NAME_LEASE_DATE, nextDateStr);

            /*
             * Set the lastUpdated as the old one when the lease scanning is on
             * progress
             */
            row.put(PartitionStatsLeaseDesc.COL_NAME_LAST_UPDATE,
                    lastUpdatedDateStr);
        }

        return row;
    }

    @Override
    String getLeaseTableName() {
        return PartitionStatsLeaseDesc.TABLE_NAME;
    }

    /**
     * This class is to save a row of the table PartitionStatsLease, and it
     * also converts passed arguments into a table row.
     */
    static class PartitionLeaseInfo extends LeaseInfo {

        private final int partitionId;

        public PartitionLeaseInfo(int partitionId,
                                  String leaseRN,
                                  long leaseTime,
                                  TimeToLive leaseTTL) {
            super(leaseRN, leaseTime, leaseTTL);
            this.partitionId = partitionId;
        }

        public int getParitionId() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "partition-" + String.valueOf(partitionId) + " by " +
                    leaseRN;
        }
    }
}
