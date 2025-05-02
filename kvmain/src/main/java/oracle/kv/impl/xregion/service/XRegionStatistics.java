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

package oracle.kv.impl.xregion.service;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import oracle.kv.impl.util.NonNullByDefault;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Object that represents a package of statistics in the service. It includes
 * - a collection of the agent statistics
 * - per-table statistics
 */
public class XRegionStatistics {

    /** agent metrics */
    private final XRegionServiceMetrics serviceMetrics;

    /** per-table metrics indexed by table name */
    private final ConcurrentMap<String, MRTableMetrics> tableMetrics;

    /** agent id */
    private final String agentId;

    @NonNullByDefault
    public XRegionStatistics(String agentId, String localRegion) {
        this.agentId = agentId;
        serviceMetrics = new XRegionServiceMetrics(agentId, localRegion);
        tableMetrics = new ConcurrentHashMap<>();
    }

    /** Returns agent id */
    public String getAgentId() {
        return agentId;
    }

    /** Returns agent metrics */
    public XRegionServiceMetrics getServiceMetrics() {
        return serviceMetrics;
    }

    /**
     * Returns set of table names
     *
     * @return set of table names
     */
    public Set<String> getTables() {
        return tableMetrics.keySet();
    }

    /**
     * Returns per-table metrics, or null if does not exist
     *
     * @param tbName  table name
     * @return per-table metrics
     */
    public MRTableMetrics getTableMetrics(@NonNull String tbName) {
        return tableMetrics.get(tbName);
    }

    /**
     * Removes table metrics
     * @param table table name
     */
    void removeTableMetrics(@NonNull String table) {
        tableMetrics.remove(table);
    }

    /**
     * Adds table metrics
     * @param metrics table metrics
     */
    void addTableMetrics(@NonNull MRTableMetrics metrics) {
        tableMetrics.put(metrics.getTableName(), metrics);
    }

    /**
     * Returns per-table metrics, or create a new metrics if does not exist
     * @param tbName table name
     * @return  per-table metrics
     */
    public MRTableMetrics getOrCreateTableMetrics(@NonNull String tbName,
                                                  long tid) {
        final String localRegion = serviceMetrics.getLocalRegion();
        return tableMetrics.computeIfAbsent(
            tbName, u -> new MRTableMetrics(localRegion, tbName, tid));
    }

    /**
     * Returns set of all table metrics
     * @return set of table metrics
     */
    Set<MRTableMetrics> getAllTableMetrics() {
        return new HashSet<>(tableMetrics.values());
    }

    @Override
    public String toString() {
        return "\n====== XRegionService Stats Dump Begin ======\n" +
               "XRegionService metrics (agentId=" + agentId + "):\n" +
               serviceMetrics + "\n" +
               tableMetrics.values().stream().map(MRTableMetrics::toString)
                           .collect(Collectors.joining("\n")) +
               "\n====== XRegionService Stats Dump End ======\n";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof XRegionStatistics)) {
            return false;
        }
        final XRegionStatistics other = (XRegionStatistics) obj;
        return serviceMetrics.equals(other.serviceMetrics) &&
               tableMetrics.equals(other.tableMetrics) &&
               agentId.equals(other.agentId);
    }

    @Override
    public int hashCode() {
        return serviceMetrics.hashCode() + tableMetrics.hashCode() +
               agentId.hashCode();
    }
}
