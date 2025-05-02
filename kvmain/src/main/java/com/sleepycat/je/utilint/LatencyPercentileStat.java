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

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * An long JE stat that computes a percentile latency by tracking latency
 * values in milliseconds.  The percentile value represents the smallest
 * latency value for which no more than the specified percentage of the
 * observed values were strictly less than the value and at least the specified
 * percentage of the values were less than or equal to that value.  To save
 * space, specific values are recorded only below a specified value, so all
 * larger values are lumped together and reported as the maximum value.
 * Returns zero if no values are observed.
 */
public class LatencyPercentileStat extends StatWithValueType<Long> {

    /**
     * The default maximum value for tracking latency values.
     */
    public static final int DEFAULT_MAX_TRACKED_LATENCY_MILLIS = 1000;

    private static final long serialVersionUID = 1;

    private final LatencyPercentile latency;

    /**
     * Creates an instance of this class using the default maximum for tracking
     * latency values of {@value #DEFAULT_MAX_TRACKED_LATENCY_MILLIS}.  The
     * definition type must be INCREMENTAL.
     *
     * @param percentile the percentile latency to report as a ratio between
     * 0.0 and 1.0
     * @throws IllegalArgumentException if the stat definition type is not
     * INCREMENTAL, the percentile is less than 0.0 or greater than 1.0, or
     * maxTrackedLatencyMillis is less than 0
     */
    public LatencyPercentileStat(StatGroup group,
                                 StatDefinition definition,
                                 float percentile) {
        this(group, definition, percentile,
             DEFAULT_MAX_TRACKED_LATENCY_MILLIS);
    }

    /**
     * Creates an instance of this class.  The definition type must be
     * INCREMENTAL.
     *
     * @param percentile the percentile latency to report as a ratio between
     * 0.0 and 1.0
     * @param maxTrackedLatencyMillis the maximum for tracking latency values
     * @throws IllegalArgumentException if the stat definition type is not
     * INCREMENTAL, the percentile is less than 0.0 or greater than 1.0, or
     * maxTrackedLatencyMillis is less than 0
     */
    public LatencyPercentileStat(StatGroup group,
                                 StatDefinition definition,
                                 float percentile,
                                 int maxTrackedLatencyMillis) {
        super(group, definition);
        if (definition.getType() != StatType.INCREMENTAL) {
            throw new IllegalArgumentException(
                "The stat type must be INCREMENTAL, found: " +
                definition.getType());
        }
        latency = new LatencyPercentile(
            definition.getName(), percentile, maxTrackedLatencyMillis);
    }

    /** Constructor used for copying. */
    private LatencyPercentileStat(StatDefinition definition,
                                  LatencyPercentile latency) {
        super(definition);
        this.latency = latency.copy();
    }

    /**
     * Record a single operation that the specified amount of time in
     * milliseconds.
     */
    public void add(long latencyMillis) {
        latency.add(latencyMillis);
    }

    /* StatWithValueType methods */

    @Override
    public Class<Long> getValueType() {
        return Long.class;
    }

    /* Stat methods */

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void set(Long newValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if argument is not a
     * LatencyPercentileStat or if it has a different maximum latency
     */
    @Override
    public void add(Stat<Long> other) {
        if (!(other instanceof LatencyPercentileStat)) {
            throw new IllegalArgumentException(
                "Other stat must be a LatencyPercentileStat, found: " + other);
        }
        final LatencyPercentileStat otherPercentile =
            (LatencyPercentileStat) other;
        latency.add(otherPercentile.latency);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if argument is not a
     * LatencyPercentileStat or if it has a different maximum latency
     */
    @Override
    public LatencyPercentileStat computeInterval(Stat<Long> base) {
        if (!(base instanceof LatencyPercentileStat)) {
            throw new IllegalArgumentException(
                "Base stat must be a LatencyPercentileStat, found: " + base);
        }
        final LatencyPercentileStat baseStat = (LatencyPercentileStat) base;
        final LatencyPercentileStat result = copy();
        result.latency.updateInterval(baseStat.latency);
        return result;
    }

    @Override
    public void negate() {
        latency.negate();
    }

    /* BaseStat methods */

    @Override
    public Long get() {
        return latency.get();
    }

    @Override
    public void clear() {
        latency.clear();
    }

    @Override
    public LatencyPercentileStat copy() {
        return new LatencyPercentileStat(definition, latency);
    }

    @Override
    public String getFormattedValue() {
        return latency.getFormattedValue();
    }

    @Override
    public boolean isNotSet() {
        return latency.isNotSet();
    }
}
