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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import oracle.kv.impl.xregion.service.MRTableMetrics;
import oracle.kv.impl.xregion.stat.ShardMetrics;
import oracle.nosql.common.json.JsonUtils;

/**
 * Object represents the base class of region agent statistics
 */
abstract public class BaseRegionAgentMetrics {

    /**
     * source region name
     */
    protected final String sourceRegion;
    /**
     * target region name
     */
    protected final String targetRegion;
    /**
     * per-table metrics
     */
    private final ConcurrentMap<String, MRTableMetrics> tableMetrics;
    /**
     * per-shard metrics
     */
    private final ConcurrentMap<Integer, ShardMetrics> shardMetrics;

    /* ------------------------ */
    /* statistics for streaming */
    /* ------------------------ */

    protected BaseRegionAgentMetrics(String sourceRegion, String targetRegion) {
        this.sourceRegion = sourceRegion;
        this.targetRegion = targetRegion;
        tableMetrics = new ConcurrentHashMap<>();
        shardMetrics = new ConcurrentHashMap<>();
    }

    protected BaseRegionAgentMetrics(String regionName) {
        this(regionName, regionName);
    }

    /* returns per-table stat */
    public String tableStat() {
        return "Table statistics=" + "\n" +
               tableMetrics.values().stream()
                           .map(m -> JsonUtils.print(m, true))
                           .collect(Collectors.joining("\n", "", ""));
    }

    /**
     * Sets the timestamp for when the last message was received from the
     * server
     */
    public void setLastMsgTime(int shardId, long val) {
        shardMetrics.computeIfAbsent(shardId, u -> new ShardMetrics())
                    .setLastMsgTime(val);

    }

    /**
     * Sets the timestamp of the original modification time of the last put or
     * delete operation completed on the local target store.
     */
    public void setLastModTime(int shardId, long val) {
        shardMetrics.computeIfAbsent(shardId, u -> new ShardMetrics())
                    .setLastModTime(val);

    }

    /**
     * Adds latency stat
     */
    public void addLatency(int shardId, long val) {
        shardMetrics.computeIfAbsent(shardId, u -> new ShardMetrics())
                    .addLatency(val);
    }

    public Map<Integer, ShardMetrics> getShardMetrics() {
        return shardMetrics;
    }

    /* -------------- */
    /* Public Getters */
    /* -------------- */

    /**
     * Returns the source region name
     *
     * @return the source region name
     */
    public String getSourceRegion() {
        return sourceRegion;
    }

    public String getTargetRegion() {
        return targetRegion;
    }

    /**
     * Returns all table names
     *
     * @return all table names
     */
    public Set<String> getTables() {
        return tableMetrics.keySet();
    }

    /**
     * Returns metrics for given table, or null if table does not exist
     *
     * @param table table name
     * @return metrics for given table
     */
    public MRTableMetrics getTableMetrics(String table) {
        return tableMetrics.get(table);
    }

    /**
     * Adds a table metrics, replace existing one if exists
     * @param tm table metrics
     */
    public void addTable(MRTableMetrics tm) {
        tableMetrics.put(tm.getTableName(), tm);
    }

    /* ------------------------------------ */
    /* Public Getters For Stream Statistics */
    /* ------------------------------------ */

    /**
     * Returns the total number of operations streamed from source region
     *
     * @return the total number of operations streamed from source region
     */
    public long getTotalStreamOps() {
        return getTotalStreamDels() + getTotalStreamPuts();
    }

    /**
     * Returns the total number of put operations streamed from source region
     *
     * @return the total number of put operations streamed from source region
     */
    public long getTotalStreamPuts() {
        return tableMetrics.values().stream()
                           .mapToLong(MRTableMetrics::getPuts)
                           .sum();
    }

    /**
     * Returns the total number of delete operations streamed from source region
     *
     * @return the total number of delete operations streamed from source region
     */
    public long getTotalStreamDels() {
        return tableMetrics.values().stream()
                           .mapToLong(MRTableMetrics::getDels)
                           .sum();
    }

    /**
     * Returns the total number of winning put operations streamed from source
     * region
     *
     * @return the total number of winning put operations streamed from source
     * region
     */
    public long getTotalWinPut() {
        return tableMetrics.values().stream()
                           .mapToLong(MRTableMetrics::getWinPuts)
                           .sum();
    }

    /**
     * Returns the total number of winning delete operations streamed from
     * source region
     *
     * @return the total number of winning delete operations streamed from
     * source region
     */
    public long getTotalWinDel() {
        return tableMetrics.values().stream()
                           .mapToLong(MRTableMetrics::getDels)
                           .sum();
    }

    /**
     * Returns the total number of winning operations streamed from source
     * region
     *
     * @return the total number of winning operations streamed from source
     * region
     */
    public long getTotalWinOp() {
        return getTotalWinDel() + getTotalWinPut();
    }

    @Override
    public String toString() {
        return tableStat();
    }
}
