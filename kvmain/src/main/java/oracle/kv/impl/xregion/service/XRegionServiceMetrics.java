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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import oracle.kv.impl.xregion.stat.JsonMetricsHeader;
import oracle.kv.impl.xregion.stat.JsonRegionStat;
import oracle.kv.stats.ServiceAgentMetrics;
import oracle.nosql.common.json.JsonUtils;

/**
 * XRegion Service agent metrics. The class is serialized locally to create a
 * json field in the system table and the class instance itself is not
 * transmitted over network.
 */
public class XRegionServiceMetrics extends JsonMetricsHeader
    implements ServiceAgentMetrics {

    private static final long serialVersionUID = 1L;

    /**
     * # of requests
     */
    private volatile long requests = 0;
    /**
     * # of responses
     */
    private volatile long responses = 0;
    /**
     * # received puts from remote region
     */
    private volatile long puts = 0;
    /**
     * # received deletes from remote region
     */
    private volatile long dels = 0;
    /**
     * total streamed bytes
     */
    private volatile long streamBytes = 0;
    /**
     * total streamed bytes persisted to target
     */
    private volatile long persistStreamBytes = 0;
    /**
     * # remote puts that win conflict resolution
     */
    private volatile long winPuts = 0;
    /**
     * # remote deletes that win conflict resolution
     */
    private volatile long winDels = 0;
    /**
     * # of looped back put operations
     */
    private volatile long loopbackPuts = 0;
    /**
     * # of looped back delete operations
     */
    private volatile long loopbackDels = 0;
    /**
     * # incompatible rows from remote
     */
    private volatile long incompatibleRows = 0;
    /**
     * Per-region statistics
     */
    private final ConcurrentMap<String, JsonRegionStat> regionStat =
        new ConcurrentHashMap<>();

    /**
     * No args constructor for use in serialization,
     * used when constructing instance from JSON.
     */
    XRegionServiceMetrics() {
        super();
    }

    XRegionServiceMetrics(String agentId, String localRegion) {
        super(agentId, localRegion);
    }

    /* agent setters */
    public void setNRequest(long val) {
        requests = val;
    }

    public void setNResp(long val) {
        responses = val;
    }

    public void setPuts(long val) {
        puts = val;
    }

    public void setDels(long val) {
        dels = val;
    }

    public void setWinPuts(long val) {
        winPuts = val;
    }

    public void setLoopbackPuts(long val) {
        loopbackPuts = val;
    }

    public void setWinDels(long val) {
        winDels = val;
    }

    public void setLoopbackDels(long val) {
        loopbackDels = val;
    }

    public void setStreamBytes(long val) {
        streamBytes = val;
    }

    public void setPersistStreamBytes(long val) {
        persistStreamBytes = val;
    }

    public void setIncompatibleRows(long val) {
        incompatibleRows = val;
    }

    public void setRegionStat(String region, JsonRegionStat stat) {
        if (stat == null) {
            return;
        }
        regionStat.put(region, stat);
    }

    /* getters of counters */
    @Override
    public long getRequests() {
        return requests;
    }

    @Override
    public long getResponses() {
        return responses;
    }

    @Override
    public long getPuts() {
        return puts;
    }

    @Override
    public long getDels() {
        return dels;
    }

    @Override
    public long getWinPuts() {
        return winPuts;
    }
    @Override
    public long getLoopbackPuts() {
        return loopbackPuts;
    }
    @Override
    public long getLoopbackDels() {
        return loopbackDels;
    }
    @Override
    public long getWinDels() {
        return winDels;
    }

    @Override
    public long getStreamBytes() {
        return streamBytes;
    }

    @Override
    public long getPersistStreamBytes() {
        return persistStreamBytes;
    }

    @Override
    public long getIncompatibleRows() {
        return incompatibleRows;
    }

    /* public access for json getter */
    public Map<String, JsonRegionStat> getRegionStat() {
        return regionStat;
    }

    @Override
    public Map<String, Long> getLastMessageMs() {
        final Map<String, Long> ret = new HashMap<>();
        regionStat.forEach((k, v) -> ret.put(k, v.getLastMessageMs()));
        return ret;
    }

    @Override
    public Map<String, Long> getLastModificationMs() {
        final Map<String, Long> ret = new HashMap<>();
        regionStat.forEach((k, v) -> ret.put(k, v.getLastModificationMs()));
        return ret;
    }

    @Override
    public Map<String, Long> getCompleteWriteOps() {
        final Map<String, Long> ret = new HashMap<>();
        regionStat.forEach((k, v) -> ret.put(k, v.getCompleteWriteOps()));
        return ret;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<String, oracle.kv.stats.MetricStats> getLaggingMs() {
        final Map<String, oracle.kv.stats.MetricStats> ret = new HashMap<>();
        regionStat.forEach((k, v) -> ret.put(k, v.getLaggingMs()));
        return ret;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<String, oracle.kv.stats.MetricStats> getLatencyMs() {
        final Map<String, oracle.kv.stats.MetricStats> ret = new HashMap<>();
        regionStat.forEach((k, v) -> ret.put(k, v.getLatencyMs()));
        return ret;
    }

    @Override
    public long getRecvOps() {
        return dels + puts;
    }

    @Override
    public long getNumWins() {
        return getWinDels() + getWinPuts();
    }

    @Override
    public String toString() {
        return JsonUtils.print(this, true);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof XRegionServiceMetrics)) {
            return false;
        }
        final XRegionServiceMetrics other = (XRegionServiceMetrics) obj;
        return super.equals(obj) &&
               requests == other.requests &&
               responses == other.responses &&
               persistStreamBytes == other.persistStreamBytes &&
               streamBytes == other.streamBytes &&
               incompatibleRows == other.incompatibleRows &&
               puts == other.puts &&
               dels == other.dels &&
               winPuts == other.winPuts &&
               winDels == other.winDels &&
               loopbackPuts == other.loopbackPuts &&
               loopbackDels == other.loopbackDels &&
               regionStat.equals(other.regionStat);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
               Long.hashCode(requests) +
               Long.hashCode(responses) +
               Long.hashCode(persistStreamBytes) +
               Long.hashCode(streamBytes) +
               Long.hashCode(incompatibleRows) +
               Long.hashCode(puts) +
               Long.hashCode(dels) +
               Long.hashCode(winPuts) +
               Long.hashCode(winDels) +
               Long.hashCode(loopbackPuts) +
               Long.hashCode(loopbackDels) +
               regionStat.hashCode();
    }
}
