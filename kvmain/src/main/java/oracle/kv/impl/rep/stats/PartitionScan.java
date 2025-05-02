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

import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_SIZE;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_SIZE_WITH_TOMBSTONES;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Key;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.table.DroppedTableException;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.RNTaskCoordinator;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.stats.PartitionLeaseManager.PartitionLeaseInfo;
import oracle.kv.impl.rep.table.ResourceCollector;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.utilint.TaskCoordinator.Permit;

/**
 * The class scans the partition database to calculate primary key statistics
 * and stores the scanned results into statistics tables.
 */
public class PartitionScan extends StatsScan<PartitionLeaseInfo>
                           implements TopologyManager.PostUpdateListener {

    private final PartitionId partId;
    private final int groupId;
    private Table tableStatsTable;
    private final Set<String> emptyTableSet = new HashSet<>();

    /* The key to record the last read one. It is used as a resume key */
    private byte[] resumeKey = null;

    /*
     * The name of the fake internal table used to store statistics for
     * KV pairs.
     */
    static String KV_STATS_TABLE_NAME = "$KV$";

    private final Map<String, StatsAccumulator> tableAccMap = new HashMap<>();

    PartitionScan(TableAPI tableAPI,
                  PartitionId partId,
                  RepNode repNode,
                  StatsLeaseManager<PartitionLeaseInfo> leaseManager,
                  PartitionLeaseInfo leaseInfo,
                  long intervalStart,
                  Logger logger) {
        super(repNode, tableAPI, leaseManager, leaseInfo,
              intervalStart, logger);
        this.partId = partId;
        this.groupId = repNode.getRepNodeId().getGroupId();
    }

    @Override
    boolean checkStatsTable(TableMetadata md) {
        if (tableStatsTable != null) {
            return true;
        }

        tableStatsTable = md.getTable(null, TableStatsPartitionDesc.TABLE_NAME);
        if (tableStatsTable == null) {
            /* Table does not exist, stop gathering statistics info */
            return false;
        }

        return true;
    }

    @Override
    protected void accumulateResult(byte[] key,
                                    int storageSize,
                                    boolean isTombstone) {

        /*
         * Check whether a key is belong to a table one by one. If a key is
         * belong to a table, store it into a map with the table name; if
         * not, associate it with the fake internal table: KV_STATS_TABLE_NAME.
         */

        /* Filter out internal key space record */
        if (Key.keySpaceIsInternal(key)) {
            return;
        }

        final TableImpl table;
        try {
            table = repNode.getTableManager().getTable(key);
        } catch (DroppedTableException dte) {
            /* Do not accumulate records for dropped tables. */
            return;
        }

        final String tableName = (table == null) ? KV_STATS_TABLE_NAME :
            table.getFullNamespaceName();

        StatsAccumulator csa = tableAccMap.get(tableName);

        if (csa == null) {
            csa = new StatsAccumulator();
            tableAccMap.put(tableName, csa);

            /* Find data for the table and remove table name from set */
            emptyTableSet.remove(tableName);
        }
        if (!isTombstone) {
            /* only count key and size for live data (non-tombstone) */
            csa.addKeySize(key.length);
        }
        if (includeStorageSize) {
            if (isTombstone) {
                csa.addTombstoneSize(storageSize);
            } else {
                csa.addTableSize(storageSize);
            }
        }
    }

    /**
     * Since partition scans request that no value bytes be read when doing a
     * scan, return just the key size as the number of bytes for the read
     * throughput.
     */
    @Override
    int getReadSize(int keySize, int totalStorageSize) {
        return keySize;
    }

    @Override
    protected void wrapResult() {
        /* Deal with empty table */
        for (String tableName : emptyTableSet) {
            tableAccMap.put(tableName, new StatsAccumulator());
        }

        /*
         * Convert individual table statistics into rows that can be inserted
         * into table TableStatsPartition.
         */
        for (Map.Entry<String, StatsAccumulator> entry :
            tableAccMap.entrySet()) {

            final String tableName = entry.getKey();
            final StatsAccumulator sa = entry.getValue();
            final Row row = tableStatsTable.createRow();
            row.setTTL(ttl);
            row.put(TableStatsPartitionDesc.COL_NAME_TABLE_NAME,
                    tableName);
            row.put(TableStatsPartitionDesc.COL_NAME_PARTITION_ID,
                    partId.getPartitionId());
            row.put(TableStatsPartitionDesc.COL_NAME_SHARD_ID, groupId);
            row.put(TableStatsPartitionDesc.COL_NAME_COUNT,
                    sa.count);
            row.put(TableStatsPartitionDesc.COL_NAME_AVG_KEY_SIZE,
                    sa.getAvgKeySize());
            /* collect storage size excluding tombstone */
            final Table tb = row.getTable();
            if (tb.getField(COL_NAME_TABLE_SIZE) != null) {
                row.put(COL_NAME_TABLE_SIZE, sa.totalTableSize);
            }
            /* collect storage size including tombstone */
            if (tb.getField(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES) != null) {
                row.put(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES,
                        sa.totalTableSize + sa.totalTombstoneSizeBytes);
            }
            addRow(row);
            /*
             * If there are limits on the table, real-time size deltas are
             * being collected and the above record will be updated. The
             * delta for this partition needs to be reset so that it starts
             * from now.
             */
            resetSizeDelta(tableName, partId.getPartitionId());
        }
    }

    private void resetSizeDelta(String tableName, int pid) {
        final ResourceCollector rc = repNode.getTableManager()
                                            .getResourceCollector(tableName);
        if (rc != null) {
            rc.getAndResetSizeDelta(pid);
        }
    }

    @Override
    protected boolean preScan() {
        tableAccMap.clear();
        emptyTableSet.clear();
        resumeKey = null;

        final TableMetadata metadata =
            (TableMetadata) repNode.getMetadata(MetadataType.TABLE);
        if (metadata == null) {
            return false;
        }

        /* Initialize with fake table name. */
        emptyTableSet.add(KV_STATS_TABLE_NAME);
        for (String tableName : metadata.listTables(null, true)) {
            emptyTableSet.add(tableName);
        }

        repNode.getTopologyManager().addPostUpdateListener(this);
        return true;
    }

    @Override
    protected void postScan(boolean scanCompleted) {
        repNode.getTopologyManager().removePostUpdateListener(this);
    }

    @Override
    Database getDatabase() {
        return repNode.getPartitionDB(partId);
    }

    /* -- From TopologyManager.PostUpdateListener -- */

    @Override
    public boolean postUpdate(Topology topology) {
        final Partition p = topology.getPartitionMap().get(partId);
        if ((p == null) || (p.getRepGroupId().getGroupId() != groupId)) {
            logger.log(Level.INFO,
                       "Stopping partition scan, partition {0} has moved",
                       partId);
            stop();
            return true;
        }
        return false;
    }

    /**
     * A class to assist to record and accumulate the result of scanning.
     */
    private static class StatsAccumulator {
        /** Number of entries excluding tombstones */
        private long count;
        /** Total key size in bytes excluding tombstones */
        private long totalKeySize;
        /** Total storage size in bytes of the table, excluding tombstone */
        private long totalTableSize;
        /** Total storage size in bytes of tombstones, or 0 if no tombstone */
        private long totalTombstoneSizeBytes;

        private void addKeySize(long keySize) {
            count++;
            totalKeySize += keySize;
        }

        private int getAvgKeySize() {
            return count == 0 ? 0 : (int) (totalKeySize / count);
        }

        private void addTableSize(int size) {
            totalTableSize += size;
        }

        private void addTombstoneSize(int size) {
            totalTombstoneSizeBytes += size;
        }
    }

    @Override
    boolean scanDatabase(Environment env, Database db)
        throws InterruptedException {

        Cursor cursor = null;
        Transaction txn = null;
        /* Acquire a permit for each batch of keys. */
        final RepNodeParams repNodeParams = repNode.getRepNodeParams();
        final long permitTimeoutMs =
            repNodeParams.getPermitTimeoutMs(
                RNTaskCoordinator.KV_STORAGE_STATS_TASK);
        final long permitLeaseMs =
            repNodeParams.getPermitLeaseMs(
                RNTaskCoordinator.KV_STORAGE_STATS_TASK);

        /*
         * Acquire a permit before scanning each batch. If permits are in short
         * supply the permit may be a deficit permit, but we choose not to act
         * on it for now to keep things simple.
         */
        try (
            final Permit permit =
                repNode.getTaskCoordinator()
                       .acquirePermit(RNTaskCoordinator.KV_STORAGE_STATS_TASK,
                                      permitTimeoutMs,
                                      permitLeaseMs,
                                      TimeUnit.MILLISECONDS)) {
            txn = env.beginTransaction(null, TXN_CONFIG);
            txn.setTxnTimeout(TXN_TIME_OUT, TimeUnit.MILLISECONDS);

            int nRecords = 0;
            cursor = db.openCursor(txn, CURSOR_CONFIG);
            cursor.setCacheMode(CacheMode.UNCHANGED);

            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry dataEntry = new DatabaseEntry();
            dataEntry.setPartial(0, 0, true);

            OperationResult result;
            final LockMode lockMode = StatsScan.DEFAULT_READ_OPT.getLockMode();
            if (resumeKey == null) {
                result = getNextInternal(cursor, keyEntry, dataEntry);
            } else {
                keyEntry.setData(resumeKey);
                final OperationStatus st =
                    cursor.getSearchKeyRange(keyEntry, dataEntry, lockMode);
                if (st == OperationStatus.SUCCESS &&
                    Arrays.equals(resumeKey, keyEntry.getData())) {
                    result = getNextInternal(cursor, keyEntry, dataEntry);
                } else {
                    result = null;
                }
            }

            if (result == null) {
                return false;
            }

            boolean hasMoreElement = false;
            while (result != null && !isStopped()) {
                /* Record the latest key as a resume key */
                resumeKey = keyEntry.getData();

                /* Accumulate the key into results */
                accumulateResult(resumeKey, result);
                nRecords++;

                if (nRecords >= BATCH_SIZE) {
                    hasMoreElement = true;
                    break;
                }
                dataEntry.setPartial(0, 0, true);
                result = getNextInternal(cursor, keyEntry, dataEntry);
            }
            totalRecords += nRecords;
            return hasMoreElement;
        } catch (DatabaseException | IllegalArgumentException e) {
            logger.log(Level.FINE, "Scanning encounters exception: {0}, " +
                                   "iteration scanning exits", e);
        } finally {
            if (cursor != null) {
                TxnUtil.close(cursor);
            }

            /* We are just reading. Abort every transaction */
            TxnUtil.abort(txn);
        }
        return false;
    }
}
