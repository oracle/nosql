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

import static oracle.kv.impl.rep.stats.StatsScan.NO_SYNC_NO_ACK_WRITE_OPTION;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_PARTITION_ID;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_NAME;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_SIZE;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_SIZE_WITH_TOMBSTONES;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.TABLE_NAME;

import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import oracle.kv.Consistency;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.rep.table.ResourceCollector;
import oracle.kv.impl.rep.table.ResourceCollector.TopCollector;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * Updates the size field in table stats records from data collected during
 * operations by the resource collection framework.
 */
class IntermediateTableSizeUpdate {

    private final static ReadOptions ABSOLUTE_READ_OPTIONS =
                            new ReadOptions(Consistency.ABSOLUTE, 0, null);

    private final static long MB = 1024L * 1024L;
    
    /*
     * Thresholds for how much delta bytes need to be before we fo an update.
     * The thresholds are meant reduce the load of intermediate updates. The
     * positive (increase) and negative (decrease) thresholds are asymmetrical
     * since we are more concerned with early detection of a size overage.
     * Package access for unit tests.
     */
    final static long POSITIVE_THRESHOLD_BYTES = 1 * MB;
    final static long NEGATIVE_THRESHOLD_BYTES = 2 * -MB;
    
    private final RepNode repNode;
    private final TableAPI tableAPI;
    private final Logger logger;

    private volatile boolean stop = false;

    IntermediateTableSizeUpdate(RepNode repNode,
                                TableAPI tableAPI,
                                Logger logger) {
        this.repNode = repNode;
        this.tableAPI = tableAPI;
        this.logger = logger;
    }

    /*
     * Do an intermediate table size update.
     */
    void runUpdate() {
        final TableManager tm = repNode.getTableManager();
        final TableImpl tableStatsTable = tm.getTable(null, TABLE_NAME, 0);
        if (tableStatsTable == null) {
            /* Stats table does not exist */
            return;
        }

        final Map<Long, ResourceCollector> collectorMap = tm.getCollectorMap();
        if ((collectorMap == null) || (collectorMap.isEmpty())) {
            return;
        }
        logger.fine("Running IntermediateTableSizeUpdate");

        /*
         * For each table hierarchy that has a resource collector, update the
         * table size in each tables stats record.
         */
        for (Entry<Long, ResourceCollector> entry : collectorMap.entrySet()) {
            if (stop) {
                return;
            }
            final long tableId = entry.getKey();
            final TableImpl table = tm.getTable(tableId);
            if (table == null) {
                continue;
            }
            if (table.isTop()) {
                updateSize(table, (TopCollector)entry.getValue(),
                           collectorMap, tableStatsTable);
            }
        }
    }

    /**
     * Updates the stats records for the table hierarchy for all partitions
     * that have had activity.
     */
    private void updateSize(TableImpl topTable,
                            TopCollector tc,
                            Map<Long, ResourceCollector> collectorMap,
                            Table tableStatsTable) {
        /*
         * For each partition that had activity, update the stats record
         * and check for partition limits.
         */
        for (int pid : tc.getActivePartitions()) {
            final long totalDelta = tc.getTotalSizeDelta(pid);
            
            /*
             * Update only when the change is over a threshold.
             */
            if ((totalDelta < POSITIVE_THRESHOLD_BYTES) &&
                (totalDelta > NEGATIVE_THRESHOLD_BYTES)) {
                continue;
            }
            
            /*
             * Since we are doing an update we can reset the total delta. The
             * individual table deltas are reset in updateTable().
             */
            tc.resetTotalSizeDelta(pid);
            final TableSizeResult res =
                updateTable(topTable, pid, collectorMap, tableStatsTable);
            /* use the table size with tombstone to check the limit */
            tc.checkPartitionLimit(res.sizeTb, pid);
        }
    }
    
    /**
     * Updates the stats records for the specified table and its children
     * if there has been activity on the specified partition. Returns the
     * total partition size of the table and its children.
     *
     * @param table  table metadata
     * @param pid    partition id
     * @param collectorMap map of the resource of tables
     * @param tableStatsTable the system table stores the table stats
     * @return an object that contains the table size with and without
     * tombstones
     */
    private TableSizeResult updateTable(
        TableImpl table, int pid, Map<Long, ResourceCollector> collectorMap,
        Table tableStatsTable) {

        /*
         * Size of the partition based on the stats record adjusted by
         * a size delta from the resource collector.
         */   
        long size = 0L;
        long sizeTb;

        /*
         * Key into the stats table. The key is the table name and the
         * partition ID.
         */
        final PrimaryKey key = tableStatsTable.createPrimaryKey();
        key.put(COL_NAME_TABLE_NAME, table.getFullNamespaceName());
        key.put(COL_NAME_PARTITION_ID, pid);
        
        final ResourceCollector rc = collectorMap.get(table.getId());
        
        /*
         * The row could be null if this is a new table and a
         * partition scan hasn't run since creation.
         * It may be possible to write a record with just the size
         * information. However, we would need to verify that such
         * a record would not break anything since it would be
         * missing some fields. See PartitionScan.wrapResult()).
         */
        final Row row = getRow(key);
        if (row == null) {
            /*
             * Set the size to the current delta for this partition. We don't
             * reset the counter to let the deltas accumulate until the
             * row appears.
             */
            size = rc.getSizeDelta(pid);
            sizeTb = size;
            if (size < 0) {
                size = 0;
                sizeTb = 0;
            }
        } else {
            size = row.get(COL_NAME_TABLE_SIZE).asLong().get();
            sizeTb = TableStatsPartitionDesc.getSizeValue(row);

            /*
             * If there has been activity (delta != 0), update the size from
             * the stats record and reset the delta.
             */
            final long delta = rc.getAndResetSizeDelta(pid);
            if (delta != 0L) {
                /*
                 * Adjust the size. If the calculation goes negative, set
                 * to zero.
                 */
                size += delta;
                sizeTb += delta;
                if (size < 0) {
                    size = 0;
                }
                if (sizeTb < 0) {
                    sizeTb = 0;
                }
                row.put(COL_NAME_TABLE_SIZE, size);
                row.put(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES, sizeTb);
                updateRow(row);
            }
        }
        /* Decend to all child tables, updating and sum partition sizes */
        for (Table child : table.getChildTables().values()) {
            final TableSizeResult res =
                updateTable((TableImpl) child, pid,
                            collectorMap, tableStatsTable);
            size += res.size;
            sizeTb += res.sizeTb;
        }
        return new TableSizeResult(size, sizeTb);
    }

    private Row getRow(PrimaryKey key) {
        try {
            /*
             * Do an absolute read because we will likely turn around and
             * do a putIfVersion().
             */
            return tableAPI.get(key, ABSOLUTE_READ_OPTIONS);
        } catch (RuntimeException re) {
            /* Ignore exceptions since this is only best effort */
        }
        return null;
    }

    private void updateRow(Row row) {
        try {
            /*
             * The putIfVersion could fail if a partition scan was running on
             * some other node in the shard and just happen to update the
             * record between the getRow() above and here. Small window, and
             * likely very rare so don't bother trying to handle it.
             */
            tableAPI.putIfVersion(row, row.getVersion(),
                                  null,
                                  NO_SYNC_NO_ACK_WRITE_OPTION);
        } catch (RuntimeException re) {
            /* Ignore exceptions since this is only best effort */
        }
    }

    void stop() {
        stop = true;
     }

    private static class TableSizeResult {
        private final long size;
        private final long sizeTb;

        TableSizeResult(long size, long sizeTb) {
            this.size = size;
            this.sizeTb = sizeTb;
        }
    }
}
