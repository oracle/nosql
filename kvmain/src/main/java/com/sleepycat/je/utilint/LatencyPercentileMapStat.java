/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

import java.util.Iterator;
import java.util.Map.Entry;

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * A JE stat that maintains a map of individual {@link LatencyPercentile}
 * values which can be looked up with a String key, and that returns results as
 * a formatted string.
 */
public class LatencyPercentileMapStat
        extends MapStat<Long, LatencyPercentile> {

    private static final long serialVersionUID = 1;

    /**
     * The percentile latency to report as the value of this statistic,
     * represented as a fractional value between 0.0 and 1.0.
     */
    private final float percentile;

    /** Tracks non-negative millisecond latencies below this value. */
    private final int maxTrackedLatencyMillis;

    /**
     * Creates an instance of this class using the default maximum for tracking
     * latency values of {@value
     * LatencyPercentileStat#DEFAULT_MAX_TRACKED_LATENCY_MILLIS}.  The
     * definition type must be INCREMENTAL.
     *
     * @param group the owning group
     * @param definition the associated definition
     * @param percentile the percentile latency to report as a ratio between
     * 0.0 and 1.0
     * @throws IllegalArgumentException if the stat definition type is not
     * INCREMENTAL, the percentile is less than 0.0 or greater than 1.0 or
     * maxTrackedLatencyMillis is less than 0
     */
    public LatencyPercentileMapStat(StatGroup group,
                                    StatDefinition definition,
                                    float percentile) {
        this(group, definition, percentile,
             LatencyPercentileStat.DEFAULT_MAX_TRACKED_LATENCY_MILLIS);
    }

    /**
     * Creates an instance of this class.  The definition type must be
     * INCREMENTAL.
     *
     * @param group the owning group
     * @param definition the associated definition
     * @param percentile the percentile latency to report as a ratio between
     * 0.0 and 1.0
     * @param maxTrackedLatencyMillis the maximum for tracking latency values
     * @throws IllegalArgumentException if the stat definition type is not
     * INCREMENTAL, the percentile is less than 0.0 or greater than 1.0 or
     * maxTrackedLatencyMillis is less than 0
     */
    public LatencyPercentileMapStat(StatGroup group,
                                    StatDefinition definition,
                                    float percentile,
                                    int maxTrackedLatencyMillis) {
        super(group, definition);
        if (definition.getType() != StatType.INCREMENTAL) {
            throw new IllegalArgumentException(
                "The stat type must be INCREMENTAL, found: " +
                definition.getType());
        }
        if ((percentile < 0.0) || (percentile > 1.0)) {
            throw new IllegalArgumentException(
                "Percentile must not be less than 0.0 or greater than 1.0: " +
                percentile);
        }
        this.percentile = percentile;
        if (maxTrackedLatencyMillis < 0) {
            throw new IllegalArgumentException(
                "The maxTrackedLatencyMillis must not be negative: " +
                maxTrackedLatencyMillis);
        }
        this.maxTrackedLatencyMillis = maxTrackedLatencyMillis;
    }

    /** Constructor used for copying. */
    private LatencyPercentileMapStat(LatencyPercentileMapStat other) {
        super(other);
        percentile = other.percentile;
        maxTrackedLatencyMillis = other.maxTrackedLatencyMillis;
    }

    /**
     * Creates, stores, and returns a new stat for the specified key.
     *
     * @param key the key
     * @return the new stat
     */
    public synchronized LatencyPercentile createStat(String key) {
        assert key != null;
        final LatencyPercentile stat = new LatencyPercentile(
            definition.getName() + ":" + key, percentile,
            maxTrackedLatencyMillis);
        statMap.put(key, stat);
        return stat;
    }

    @Override
    public LatencyPercentileMapStat copy() {
        return new LatencyPercentileMapStat(this);
    }

    /**
     * Creates a new map that contains entries that represent the interval for
     * all keys that appear in both maps.  The base argument must be a
     * LatencyPercentileMapStat.
     */
    @Override
    public LatencyPercentileMapStat computeInterval(Stat<String> base) {
        if (!(base instanceof LatencyPercentileMapStat)) {
            throw new IllegalArgumentException(
                "Base stat must be a LatencyPercentileMapStat, found: " +
                base);
        }
        final LatencyPercentileMapStat other =
            (LatencyPercentileMapStat) base.copy();
        final LatencyPercentileMapStat result = copy();
        synchronized (result) {
            synchronized (other) {
                for (Iterator<Entry<String, LatencyPercentile>> i =
                         result.statMap.entrySet().iterator();
                     i.hasNext(); ) {
                    final Entry<String, LatencyPercentile> e = i.next();
                    final String key = e.getKey();
                    final LatencyPercentile stat = e.getValue();
                    final LatencyPercentile otherStat = other.statMap.get(key);
                    if (otherStat != null) {
                        stat.updateInterval(otherStat);
                    } else {
                        i.remove();
                    }
                }
            }
        }
        return result;
    }

    /* Stat methods */

    @Override
    public synchronized void negate() {
        for (final LatencyPercentile latency : statMap.values()) {
            latency.negate();
        }
    }
}
