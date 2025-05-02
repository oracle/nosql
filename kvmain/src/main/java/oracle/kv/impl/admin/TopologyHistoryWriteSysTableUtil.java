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

package oracle.kv.impl.admin;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.systables.TopologyHistoryDesc;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

/**
 * Utility methods for writing the topology history system table.
 */
public class TopologyHistoryWriteSysTableUtil {

    /** The constant shard key. */
    public static final String SHARD_KEY = "TopologyHistory";
    /**
     * The TTL for the topology history row is one week.
     */
    private static final TimeToLive TOPOLOGY_HISTORY_ROW_TTL =
        TimeToLive.ofDays(7);
    /** Delay between retries in milli-seconds. */
    private static final int RETRY_SLEEP_MS = 1000;

    /** The admin. */
    private Admin admin;
    /** The Logger. */
    private Logger logger;

    public TopologyHistoryWriteSysTableUtil(
        Admin admin,
        Logger logger)
    {
        this.admin = admin;
        this.logger = logger;
    }

    /**
     * Writes the topology into the store topology history system table, so
     * that the topology can be obtained given its sequence number.
     *
     * The record is written into the table {@link TopologyHistoryDesc}. Each
     * row has two columns: sequence number and topology bytes. The topology
     * bytes column is the Java serialization of the topology object. Each row
     * inside the history table has a TTL of TOPOLOGY_HISTORY_ROW_TTL.
     *
     * We make the best effort to write the record including retry in this
     * method {@code numInPlaceRetries} times.
     *
     * Note that we will not be able to guarantee that the topology is written
     * into the history table. The topology will be missing under the following
     * conditions:
     * (1) the rep group that hosts the partitions of the topology history
     * record is unavailable to the admin, and
     * (2) the unavailablity situation of (1) lasts long enough for retry to
     * fail, or the admin fails during that time.
     *
     * Use this method if writing the history record is optional and we cannot
     * fail the caller. The current known use case is to support query
     * operations under elasticity. Writing is optional since it is relatively
     * low cost to throw exception for those queries that find the record
     * missing. On the other hand, the parent migration plan cannot fail or
     * otherwise the rep groups participating the migration will have degraded
     * performance for the migrated partitions. We do not want the failure of
     * one rep group causing performance issue for two other rep groups.
     *
     * @param topology the topology to write
     * @param numInPlaceRetries the number of retries in the method for failed
     * attempts before we give up and return
     */
    public void writeWithRetry(Topology topology,
                               int numInPlaceRetries) {
        /*
         * Updates the request dispatcher with the new topology. We can update
         * the new topology of the request dispatcher because we know the
         * topology was already persisted as an official topology. We need to
         * do the update because it is possible that the new topology has taken
         * effect but the dispatcher does not know it and would otherwise
         * dispatch to the wrong RN (e.g. one that is already shutdown) causing
         * write failure.
         */
        final RequestDispatcherImpl dispatcher =
            (RequestDispatcherImpl)
            ((KVStoreImpl) admin.getInternalKVStore()).getDispatcher();
        dispatcher.getTopologyManager().update(topology);
        for (int i = 0; i < numInPlaceRetries; ++i) {
            if (writeAndLogError(topology)) {
                return;
            }
            try {
                Thread.sleep(RETRY_SLEEP_MS);
            } catch (InterruptedException ie) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }
        logger.log(
            Level.INFO,
            () -> String.format(
                "Topology (seqNo=%s) is not written into system table",
                topology.getSequenceNumber()));
    }

    /**
     * Writes the topology and returns {@code true} if successful, otherwise
     * logs the error and returns {@code false}.
     */
    private boolean writeAndLogError(Topology topology) {
        try {
            write(topology);
            return true;
        } catch (KVStoreException e) {
            logger.log(
                Level.FINE,
                () ->
                String.format(
                    "Error writing topo (seqNo=%s): %s",
                    topology.getSequenceNumber(),
                    CommonLoggerUtils.getStackTrace(e)));
        }
        return false;
    }

    /**
     * Writes the topology into the store topology history system table, so
     * that the topology can be obtained given its sequence number.
     *
     * The record is written into the table {@link TopologyHistoryDesc}. Each
     * row has a constant shard key and two columns: sequence number and
     * topology bytes. The topology bytes column is the Java serialization of
     * the topology object. Each row inside the history table has a TTL of
     * TOPOLOGY_HISTORY_ROW_TTL.
     *
     * @param topology the topology to write
     *
     * @throws KVStoreException if the write fails. The caller should handle
     * the exception carefully. In general, writing to the store rarely fails:
     * losing writability of a rep group is a drop-everything-and-fix-it event.
     * However, one important aspect to consider is that even under the rare
     * case of one rep group fail, the failure should not affect the operations
     * of other rep groups.
     */
    private void write(Topology topology) throws KVStoreException
    {
        final Topology pruned =
            topology.getCopy()
            .pruneChanges(Integer.MAX_VALUE, 0);
        try {
            final KVStore kvstore = admin.getInternalKVStore();
            if (kvstore == null) {
                throw new IllegalStateException(
                    "No kvstore available for read-only instance");
            }
            if (pruned.getNumPartitions() == 0) {
                /* The store is not ready yet, skip writing. */
                return;
            }
            final TableAPI api = kvstore.getTableAPI();
            if (api == null) {
                /*
                 * This means that store has not been initialized. Skip writing the
                 * topology.
                 */
                return;
            }
            final Table table = api.getTable(TopologyHistoryDesc.TABLE_NAME);
            if (table == null) {
                /*
                 * The topology history system table store has not been initialized.
                 * Skip writing the topology.
                 */
                return;
            }
            final Row row = createRow(table, topology);
            api.putIfAbsent(row, null, null);
            logger.log(
                Level.INFO,
                () -> String.format(
                    "Wrote topology (seqNo=%s, hostPartition=%s) into system table",
                    topology.getSequenceNumber(),
                    getPartitionId(api, row)));
        } catch (Exception t) {
            throw new KVStoreException("Error writing topology history", t);
        }
    }

    private Row createRow(Table table, Topology topology) {
        final Row row = table.createRow();
        row.put(TopologyHistoryDesc.COL_SHARD_KEY, SHARD_KEY);
        row.put(TopologyHistoryDesc.COL_NAME_TOPOLOGY_SEQUENCE_NUMBER,
                topology.getSequenceNumber());
        row.put(TopologyHistoryDesc.COL_NAME_SERIALIZED_TOPOLOGY,
                SerializationUtil.getBytes(topology));
        row.setTTL(TOPOLOGY_HISTORY_ROW_TTL);
        return row;
    }

    private PartitionId getPartitionId(TableAPI api, Row row) {
        return ((TableAPIImpl) api).getStore()
            .getPartitionId(((RowImpl) row).getPrimaryKey(false));
    }

}
