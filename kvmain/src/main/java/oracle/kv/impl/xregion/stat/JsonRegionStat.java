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

package oracle.kv.impl.xregion.stat;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import oracle.kv.impl.pubsub.FeederFilterStat;
import oracle.kv.impl.pubsub.StreamOpMetrics;
import oracle.nosql.common.json.JsonUtils;

/**
 * Object represents a collection of per-region stat, used to generate json
 * in stat report
 */
public class JsonRegionStat implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Fields in json
     */
    private final AtomicLong lastMessageMs = new AtomicLong();
    private final AtomicLong lastModificationMs = new AtomicLong();
    private final AtomicLong completeWriteOps = new AtomicLong();
    private volatile StreamOpMetrics latencyMs = new StreamOpMetrics();
    private volatile StreamOpMetrics laggingMs = new StreamOpMetrics();

    /**
     * No args constructor for use in serialization,
     * used when constructing instance from JSON.
     */
    public JsonRegionStat() {
    }

    /**
     * Builds region metrics from shard metrics
     */
    public JsonRegionStat(Map<Integer, FeederFilterStat> filterStat,
                          Map<Integer, ShardMetrics> shardMetrics) {
        lastMessageMs.set(
            shardMetrics.values().stream()
                        .mapToLong(ShardMetrics::getLastMsgTime)
                        .max().orElse(-1));

        lastModificationMs.set(
            shardMetrics.values().stream()
                        .mapToLong(ShardMetrics::getLastModTime)
                        .max().orElse(-1));

        latencyMs = StreamOpMetrics.merge(
            shardMetrics.values().stream()
                        .map(ShardMetrics::getLatencyMetrics)
                        .collect(Collectors.toSet()));

        /* build lagging stat from stream */
        laggingMs = StreamOpMetrics.merge(
            filterStat.values().stream()
                      .map(FeederFilterStat::getSnapShot)
                      .map(FeederFilterStat::getLaggingMs)
                      .collect(Collectors.toList()));

        /* total count from all shards */
        completeWriteOps.set(latencyMs.getCount());
    }

    /* public access required by json */
    public long getLastMessageMs() {
        return lastMessageMs.get();
    }

    public long getLastModificationMs() {
        return lastModificationMs.get();
    }

    public long getCompleteWriteOps() {
        return completeWriteOps.get();
    }

    public StreamOpMetrics getLatencyMs() {
        return latencyMs;
    }

    public StreamOpMetrics getLaggingMs() {
        return laggingMs;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof JsonRegionStat)) {
            return false;
        }
        final JsonRegionStat other = (JsonRegionStat) obj;
        return lastMessageMs.get() == other.lastMessageMs.get() &&
               lastModificationMs.get() == other.lastModificationMs.get() &&
               completeWriteOps.get() == other.completeWriteOps.get() &&
               latencyMs.equals(other.latencyMs) &&
               laggingMs.equals(other.laggingMs);
    }

    @Override
    public int hashCode() {
        return lastMessageMs.hashCode() +
               lastModificationMs.hashCode() +
               completeWriteOps.hashCode() +
               latencyMs.hashCode() +
               laggingMs.hashCode();
    }

    @Override
    public String toString() {
        return JsonUtils.prettyPrint(this);
    }
}
