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

package oracle.kv.impl.as;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.as.AggregationService.Status.Beacon;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

/**
 * Gathers table size information from the records written to system tables by
 * the key scans preformed on the RNs. The data is in two system tables, one for
 * tables (partition scan), and one for indexes (index scan). The records
 * include the table name and size. The partition scan data includes one
 * record per table per partition. The index scan data includes one record per
 * index per shard. The tables may include records for top level tables as well
 * as child tables. There may also be records for tables which do not have
 * limits set. These records are ignored.
 *
 * The  getTableSizes() returns a map of UsageRecord for each top level
 * table that has limits set. To create the map both system tables are read
 * and the sizes are aggregated per table hierarchy. A child table size is
 * added to the top level table's size. Once completed, the table sizes
 * are checked against the table's limits. If a size is over the limit, a
 * UsageRecord is created with a non-zero size. If the size is under,
 * the UsageRecord size is 0.
 *
 * All of the UsageRecord (0 and non-zero size) returned by getTableSizes()
 * must be sent to the RNs. This is so that previous overages can be reset
 * if the size was reduced, or the limit increased.
 */
class TableSizeAggregator {

    private final TableAPI tableAPI;
    private final Logger logger;
    private final PrimaryKey tableKey;
    private final PrimaryKey indexKey;
    
    TableSizeAggregator(TableAPI tableAPI, Logger logger) {
        this.tableAPI = tableAPI;
        this.logger = logger;

        /*
         * Get and cache primary keys for the tables used
         */
        Table table = tableAPI.getTable(TableStatsPartitionDesc.TABLE_NAME);
        
        /*
         * There is a startup race where the table may not yet exist. Caller
         * will retry.
         */
        if (table == null) {
            throw new IllegalStateException(
                "AggregationService: stats table not yet available: " +
                TableStatsPartitionDesc.TABLE_NAME);
        }

        tableKey = table.createPrimaryKey();

        /* now this table */
        table = tableAPI.getTable(TableStatsIndexDesc.TABLE_NAME);
        if (table == null) {
            throw new IllegalStateException(
                "AggregationService: stats table not yet available: " +
                TableStatsIndexDesc.TABLE_NAME);
        }
        indexKey = table.createPrimaryKey();
    }

    /**
     * Gets limit records for each top level table that has limits. If the
     * table size is over the limit the record size will be &gt; 0.
     */
    Collection<UsageRecord> getTableSizes(long periodMillis,
                                          AggregationService as) {
        final long startMillis = System.currentTimeMillis();

        final TableMetadata tableMD =
                                ((TableAPIImpl)tableAPI).getTableMetadata();
        if (tableMD == null) {
            throw new IllegalStateException(
                "AggregationService: Unable to get table metadata");
        }
        final Map<Long, SizeAccumulator> sizeMap = createSizeMap(tableMD);
        
        /* Collect the table sizes */
        TableIterator<Row> iter = tableAPI.tableIterator(tableKey, null, null);
        int nTableRecords = 0;
        try {
            while (iter.hasNext()) {
                final Row row = iter.next();
                /* table name is a qualified fullName */
                final FieldValue nameValue =
                    row.get(TableStatsPartitionDesc.COL_NAME_TABLE_NAME);
                accumulateSize(tableMD, sizeMap,
                               nameValue.asString().get(),
                               TableStatsPartitionDesc.getSizeValue(row));
                nTableRecords++;
            }
        } finally {
            iter.close();
        }
        /* Collect the index sizes */
        int nIndexRecords = 0;
        iter = tableAPI.tableIterator(indexKey, null, null);
        try {
            while (iter.hasNext()) {
                final Row row = iter.next();
                /* table name is a qualified fullName */
                final FieldValue nameValue =
                    row.get(TableStatsIndexDesc.COL_NAME_TABLE_NAME);
                final FieldValue sizeValue =
                    row.get(TableStatsIndexDesc.COL_NAME_INDEX_SIZE);
                accumulateSize(tableMD, sizeMap,
                               nameValue.asString().get(),
                               sizeValue.asLong().get());
                nIndexRecords++;
            }
        } finally {
            iter.close();
        }

        final long durationMillis = System.currentTimeMillis() - startMillis;
        final long healthyMillis = periodMillis / 2;
        if (periodMillis - durationMillis < 0) {
            as.recordHealth(Beacon.RED,
                            "Table size collection did not complete " +
                            "within polling period");
        } else if (durationMillis > healthyMillis) {
            as.recordHealth(Beacon.YELLOW,
                            "Table size collection did not complete " +
                            "within 50% polling period");
        }

        /*
         * Create the UsageRecord map from the size data.
         */
        final Map<Long, UsageRecord> usageRecords = new HashMap<>();
        for (Entry<Long, SizeAccumulator> entry : sizeMap.entrySet()) {
            final Long tableId = entry.getKey();
            final SizeAccumulator sr = entry.getValue();
            final UsageRecord ur = new UsageRecord(tableId, sr.getSize());
            logger.fine(() -> ur.toString());
            usageRecords.put(tableId, ur);
        }
        logger.log(Level.INFO,
                   "Collecting {0} table and {1} index size records" +
                   " took {2}ms, generated {3} usage records",
                   new Object[]{nTableRecords, nIndexRecords,
                                durationMillis, usageRecords.size()});
        return new ArrayList<>(usageRecords.values());
    }

    /*
     * Accumulates the size for the specified table into the sizeMap records.
     */
    private void accumulateSize(TableMetadata tableMD,
                                Map<Long, SizeAccumulator> sizeMap,
                                String qualifiedName, long size) {
        if (size <= 0) {
            return;
        }
        final String namespace =
                        NameUtils.getNamespaceFromQualifiedName(qualifiedName);
        final String tableName =
                        NameUtils.getFullNameFromQualifiedName(qualifiedName);
        final Table table = tableMD.getTable(namespace, tableName);
        if (table == null) {
            return;
        }
        final TableImpl top = ((TableImpl)table).getTopLevelTable();
        final SizeAccumulator sr = sizeMap.get(top.getId());
        if (sr == null) {
            /* If no SizeRecorder then the table didn't have a size limit */
            return;
        }
        sr.add(size);
    }

    /**
     * Creates the size map. The map is tableName -> SizeRecord. There is
     * one entry for each top level table which has a size limit.
     */
    private Map<Long, SizeAccumulator> createSizeMap(TableMetadata tableMD) {
        final Map<Long, SizeAccumulator> sizeMap = new HashMap<>();

        final Map<String, Table> tableMap = tableMD.getTables();
        for (Entry<String, Table> entry : tableMap.entrySet()) {
            final TableImpl table = (TableImpl)entry.getValue();
            if (table.isTop()) {
                /* Only create records for tables with size limits */
                final TableLimits limits = table.getTableLimits();
                if ((limits == null) || !limits.hasSizeLimit()) {
                    continue;
                }
                sizeMap.put(table.getId(), new SizeAccumulator());
            }
        }
        return sizeMap;
    }

    /**
     * Struct to record table size. Note that this class deals in bytes where
     * as the limit is specified in GB.
     */
    private class SizeAccumulator {

        /* These fields are in bytes */
        private long size = 0L;

        /**
         * Adds the specified size (in bytes) to is record.
         */
        private void add(long sizeToAdd) {
            this.size += sizeToAdd;
        }

        /**
         * Returns the recorded size.
         */
        long getSize() {
            return size;
        }

        @Override
        public String toString() {
            return "SizeAccumulator[" + size + "]";
        }
    }
}
