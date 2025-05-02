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
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.stats.IndexLeaseManager.IndexLeaseInfo;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

/**
 * StatsLeaseTable is used to manage the shard-wide lease associated with the
 * gathering of index statistics for a shard.
 */
class IndexLeaseManager extends StatsLeaseManager<IndexLeaseInfo> {

    IndexLeaseManager(TableAPI tableAPI) {
        super(tableAPI);
    }

    @Override
    PrimaryKey createPrimaryKey(IndexLeaseInfo info) {
        PrimaryKey primaryKey = leaseTable.createPrimaryKey();
        primaryKey.put(IndexStatsLeaseDesc.COL_NAME_TABLE_NAME,
                       info.getNamespaceName());
        primaryKey.put(IndexStatsLeaseDesc.COL_NAME_INDEX_NAME,
                       info.getIndexName());
        primaryKey.put(IndexStatsLeaseDesc.COL_NAME_SHARD_ID,
                       info.getShardId());
        return primaryKey;
    }

    @Override
    Row createRow(IndexLeaseInfo info, boolean terminated) {
        Row row = leaseTable.createRow();
        row.setTTL(info.getTTL());
        row.put(IndexStatsLeaseDesc.COL_NAME_TABLE_NAME,
                info.getNamespaceName());
        row.put(IndexStatsLeaseDesc.COL_NAME_INDEX_NAME, info.getIndexName());
        row.put(IndexStatsLeaseDesc.COL_NAME_SHARD_ID, info.getShardId());

        row.put(IndexStatsLeaseDesc.COL_NAME_LEASE_RN, info.getLeaseRN());

        final long currentTime = System.currentTimeMillis();
        final Date currentDate = new Date(currentTime);
        final String currentDateStr =
                StatsLeaseManager.DATE_FORMAT.format(currentDate);

        /*
         * Mark last updated data means we are done gathering statistics
         * for the index and no longer need to extend the lease.
         */
        if (terminated) {
            row.put(IndexStatsLeaseDesc.COL_NAME_LEASE_DATE, currentDateStr);

            /* Update the lastUpdated after the lease scanning completes */
            row.put(IndexStatsLeaseDesc.COL_NAME_LAST_UPDATE, currentDateStr);
        } else {
            final long nextTime = currentTime + info.getLeaseTime();
            leaseExpiryTime = nextTime;
            final Date nextDate = new Date(nextTime);
            final String nextDateStr =
                    StatsLeaseManager.DATE_FORMAT.format(nextDate);
            row.put(IndexStatsLeaseDesc.COL_NAME_LEASE_DATE, nextDateStr);

            /*
             * Set the lastUpdated as the old one when the lease scanning is on
             * progress
             */
            row.put(IndexStatsLeaseDesc.COL_NAME_LAST_UPDATE,
                    lastUpdatedDateStr);
        }


        return row;
    }

    @Override
    String getLeaseTableName() {
        return IndexStatsLeaseDesc.TABLE_NAME;
    }

    /**
     * This class is to save a row of the table IndexStatsLease, and it also
     * converts passed arguments into a table row.
     */
    static class IndexLeaseInfo extends StatsLeaseManager.LeaseInfo {

        private final String namespaceName;
        private final String indexName;
        private final int shardId;

        public IndexLeaseInfo(Table table,
                              String indexName,
                              int shardId,
                              String leaseRN,
                              long leaseTime,
                              TimeToLive leaseTTL) {
            super(leaseRN, leaseTime, leaseTTL);
            final String namespace = ((TableImpl)table).getInternalNamespace();
            final String tableName = table.getFullName();
            this.namespaceName =
                NameUtils.makeQualifiedName(namespace, tableName);
            this.indexName = indexName;
            this.shardId = shardId;
        }

        public String getNamespaceName() {
            return namespaceName;
        }

        public String getIndexName() {
            return indexName;
        }

        public int getShardId() {
            return shardId;
        }

        @Override
        public String toString() {
            return "index " + namespaceName + "." + indexName + " in shard-" +
                    shardId + " by " + leaseRN;
        }
    }
}
