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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import oracle.kv.impl.util.NonNullByDefault;
import oracle.kv.impl.xregion.stat.JsonMetricsHeader;
import oracle.kv.impl.xregion.stat.TableInitStat;
import oracle.nosql.common.json.JsonUtils;

import org.checkerframework.checker.nullness.qual.NonNull;


/**
 * Multi-region table metrics. The class is serialized locally in the agent to
 * create a json field in the system table and the class instance itself is
 * not transmitted over network.
 */
public class MRTableMetrics extends JsonMetricsHeader {
    private static final long serialVersionUID = 1L;

    /** name of the table, non-final because of JSON serialization */
    private final String tableName;
    /** table id of the table, non-final because of JSON serialization */
    private final long tableId;

    /* streaming statistics */
    /** # of streamed puts from remote */
    private volatile long puts = 0;
    /** # of streamed puts but have expired when writing to target */
    private volatile long expiredPuts = 0;
    /** # of streamed deletes from remote */
    private volatile long dels = 0;
    /** # of streamed puts winning conflict resolution */
    private volatile long winPuts = 0;
    /** # of streamed deletes winning conflict resolution */
    private volatile long winDels = 0;
    /** # of streamed incompatible rows from remote */
    private volatile long incompatibleRows = 0;
    /** # of streamed bytes from source */
    private volatile long streamBytes = 0;
    /** # of persisted streamed bytes to target */
    private volatile long persistStreamBytes = 0;
    /** # of looped back puts */
    private volatile long loopbackPuts = 0;
    /** # of looped back deletes */
    private volatile long loopbackDels = 0;

    /** per-region initialization statistics */
    private final ConcurrentMap<String, TableInitStat> initialization =
        new ConcurrentHashMap<>();

    @NonNullByDefault
    public MRTableMetrics(String localRegion, String tableName, long tableId) {
        super("RegionAgent", localRegion);
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("Null table name");
        }
        this.tableName = tableName;
        this.tableId = tableId;
    }

    /*
     * Getters
     */
    public String getTableName() {
        return tableName;
    }

    public long getTableId() {
        return tableId;
    }

    /**
     * Gets the per-region initialization stat. All read and write to
     * initialization stat should call this method to get the stat object
     * @param region remote region
     * @return per-region initialization stat, or null if not exists
     */
    public TableInitStat getRegionInitStat(@NonNull String region) {
        return initialization.computeIfAbsent(
            region, u -> new TableInitStat(region));
    }

    public long getPuts() {
        return puts;
    }

    public long getExpiredPuts() {
        return expiredPuts;
    }

    public long getDels() {
        return dels;
    }

    public long getWinPuts() {
        return winPuts;
    }

    public long getWinDels() {
        return winDels;
    }

    public long getIncompatibleRows() {
        return incompatibleRows;
    }

    public long getStreamBytes() {
        return streamBytes;
    }

    public long getPersistStreamBytes() {
        return persistStreamBytes;
    }

    public long getLoopbackPuts() {
        return loopbackPuts;
    }

    public long getLoopbackDels() {
        return loopbackDels;
    }

    /* used by json */
    public Map<String, TableInitStat> getInitialization() {
        return new ConcurrentSkipListMap<>(initialization);
    }

    /*
     * Setters
     */
    /* aggregate table streaming stat */
    public void aggregateStreamStat(@NonNull MRTableMetrics metrics) {
        incrPuts(metrics.getPuts());
        incrExpiredPuts(metrics.getExpiredPuts());
        incrDels(metrics.getDels());
        incrWinPuts(metrics.getWinPuts());
        incrWinDels(metrics.getWinDels());
        incrLoopbackPuts(metrics.getLoopbackPuts());
        incrLoopbackDels(metrics.getLoopbackDels());
        incrIncompatibleRows(metrics.getIncompatibleRows());
        incrStreamBytes(metrics.getStreamBytes());
        incrPersistStreamBytes(metrics.getPersistStreamBytes());
    }

    public void setRegionInitialization(@NonNull TableInitStat stat) {
        initialization.put(stat.getRegion(), stat);
    }

    public synchronized void incrPuts(long delta) {
        puts += delta;
    }

    public synchronized void incrExpiredPuts(long delta) {
        expiredPuts += delta;
    }

    public synchronized void incrDels(long delta) {
        dels += delta;
    }

    public synchronized void incrWinPuts(long delta) {
        winPuts += delta;
    }

    public synchronized void incrWinDels(long delta) {
        winDels += delta;
    }

    public synchronized void incrIncompatibleRows(long delta) {
        incompatibleRows += delta;
    }

    public synchronized void incrStreamBytes(long delta) {
        streamBytes += delta;
    }

    public synchronized void incrPersistStreamBytes(long delta) {
        persistStreamBytes += delta;
    }

    public synchronized void incrLoopbackPuts(long delta) {
        loopbackPuts += delta;
    }

    public synchronized void incrLoopbackDels(long delta) {
        loopbackDels += delta;
    }

    @Override
    public String toString() {
        return JsonUtils.print(this, true);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MRTableMetrics)) {
            return false;
        }
        final MRTableMetrics other = (MRTableMetrics) obj;
        return super.equals(obj) &&
               tableName.equals(other.tableName) &&
               puts == other.puts &&
               expiredPuts == other.expiredPuts &&
               dels == other.dels &&
               winPuts == other.winPuts &&
               winDels == other.winDels &&
               loopbackPuts == other.loopbackPuts &&
               loopbackDels == other.loopbackDels &&
               streamBytes == other.streamBytes &&
               persistStreamBytes == other.persistStreamBytes &&
               incompatibleRows == other.incompatibleRows &&
               initialization.equals(other.initialization);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
               tableName.hashCode() +
               Long.hashCode(puts) +
               Long.hashCode(expiredPuts) +
               Long.hashCode(dels) +
               Long.hashCode(winPuts) +
               Long.hashCode(winDels) +
               Long.hashCode(loopbackPuts) +
               Long.hashCode(loopbackDels) +
               Long.hashCode(streamBytes) +
               Long.hashCode(persistStreamBytes) +
               Long.hashCode(incompatibleRows) +
               initialization.hashCode();
    }
}
