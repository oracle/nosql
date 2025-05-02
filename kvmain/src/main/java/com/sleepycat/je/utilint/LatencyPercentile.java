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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import com.sleepycat.utilint.FormatUtil;

/**
 * A long JE stat component that computes a percentile latency by tracking
 * latency values in milliseconds.  The percentile value represents the
 * smallest latency value for which no more than the specified percentage of
 * the observed values were strictly less than the value and at least the
 * specified percentage of the values were less than or equal to that value.
 * To save space, specific values are recorded only below a specified value, so
 * all larger values are lumped together and reported as the maximum value.
 * Returns zero if no values are observed.
 *
 * <p>This class was inspired by the somewhat different {@link
 * com.sleepycat.utilint.LatencyStat} class.
 */
public class LatencyPercentile
        extends MapStatComponent<Long, LatencyPercentile> {

    private static final long serialVersionUID = 1;

    /**
     * The name of the stat, to help with debugging.
     */
    private final String name;

    /**
     * The percentile latency to compute, represented as a fractional value
     * between 0.0 and 1.0.
     */
    private final float percentile;

    /**
     * Separately tracks all non-negative millisecond latencies below this
     * value.
     */
    private final int maxTrackedLatencyMillis;

    /**
     * Buckets with counts for each tracked latency value plus an overflow
     * bucket.
     */
    private static class Values implements Serializable {
        private static final long serialVersionUID = 1;

        /** The total number operations. */
        final AtomicLong count = new AtomicLong();

        /**
         * Array is indexed by latency in milliseconds and elements contain the
         * number of ops for that latency.  The highest bucket includes all
         * values greater than or equal to the maximum.
         */
        final AtomicLongArray histogram;

        Values(int maxTrackedLatencyMillis) {
            histogram = new AtomicLongArray(maxTrackedLatencyMillis + 1);
        }

        Values(Values other) {
            count.set(other.count.get());
            final int max = other.histogram.length();
            histogram = new AtomicLongArray(max);
            for (int i = 0; i < max; i++) {
                histogram.set(i, other.histogram.get(i));
            }
        }

        /**
         * Return a new instance that represents adding the values collected in
         * the argument to the values stored in this instance.
         */
        Values add(Values other) {
            final int max = other.histogram.length();
            final Values result = new Values(max-1);
            result.count.set(count.get() + other.count.get());
            for (int i = 0; i < max; i++) {
                result.histogram.set(
                    i, histogram.get(i) + other.histogram.get(i));
            }
            return result;
        }

        /**
         * Return a new instance that represents the difference between the
         * values stored in this instance and the ones in the argument.
         */
        Values computeInterval(Values other) {
            final int max = histogram.length();
            final Values result = new Values(max-1);
            result.count.set(count.get() - other.count.get());
            for (int i = 0; i < max; i++) {
                result.histogram.set(
                    i, histogram.get(i) - other.histogram.get(i));
            }
            return result;
        }

        /**
         * Return a new instance that represents the negation all of the
         * values.
         */
        Values negate() {
            final int max = histogram.length();
            final Values result = new Values(max-1);
            result.count.set(-count.get());
            for (int i = 0; i < max; i++) {
                result.histogram.set(i, -histogram.get(i));
            }
            return result;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Values[");
            sb.append("count=").append(count);
            sb.append(" histogram={");
            boolean first = true;
            for (int i = 0; i < histogram.length(); i++) {
                final long val = histogram.get(i);
                if (val != 0) {
                    if (!first) {
                        sb.append(',');
                    } else {
                        first=false;
                    }
                    sb.append(i).append('=').append(val);
                }
            }
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Contains the values tracked by add() and reported by get().
     *
     * <p>To clear the values, this field is assigned a new instance.  This
     * prevents uninitialized values when set() and clear() run concurrently.
     * Methods that access the values (add and calculate) should assign
     * trackedValues to a local var and perform all access using the local var,
     * so that clear() will not impact the computation.
     *
     * <p>Concurrent access by add() and calculate() is handled differently.
     * The count field is incremented by add() last, and is checked first by
     * calculate().  If count is zero, calculate() will always return zero.  If
     * count is non-zero, calculate() may still return a latency value that is
     * inconsistent, when add() runs concurrently.  But at least calculate()
     * won't return uninitialized latency values.  Without synchronizing add(),
     * this is the best we can do.  Synchronizing add() might introduce
     * contention during operations.
     */
    private volatile Values trackedValues;

    /** The most recently computed percentile value. */
    private volatile int savedPercentileValue;

    /**
     * Creates an instance of this class
     *
     * @param name the name of the statistic
     * @param percentile the percentile latency to report as a ratio between
     * 0.0 and 1.0
     * @param maxTrackedLatencyMillis the maximum for tracking latency values
     * @throws IllegalArgumentException if the percentile is less than 0.0 or
     * greater than 1.0, or if maxTrackedLatencyMillis is less than 0
     */
    public LatencyPercentile(String name,
                             float percentile,
                             int maxTrackedLatencyMillis) {
        this.name = name;
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
        clear();
    }

    /** Constructor used for copying. */
    private LatencyPercentile(LatencyPercentile other) {
        this.name = other.name;
        this.percentile = other.percentile;
        this.maxTrackedLatencyMillis = other.maxTrackedLatencyMillis;
        trackedValues = new Values(other.trackedValues);
        this.savedPercentileValue = other.savedPercentileValue;
    }

    /* MapStatComponent methods */

    @Override
    protected String getFormattedValue(boolean useCommas) {
        if (isNotSet()) {
            return "unknown";
        }
        final long value = calculate(false);
        if (useCommas) {
            return FormatUtil.decimalScale0().format(value);
        }
        return Long.toString(value);
    }

    @Override
    public LatencyPercentile copy() {
        return new LatencyPercentile(this);
    }

    /* BaseStat methods */

    /**
     * Calculates and returns the current value without clearing the existing
     * statistics.
     */
    @Override
    public Long get() {
        return calculate(false);
    }

    @Override
    public void clear() {
        clearInternal();
    }

    @Override
    public boolean isNotSet() {
        return trackedValues.count.get() == 0;
    }

    /* Object methods */

    @Override
    public String toString() {
        return "LatencyPercentile[" +
            "name=" + name +
            " percent=" + percentile +
            " maxTracked=" + maxTrackedLatencyMillis +
            " value=" + savedPercentileValue +
            " trackedValues=" + trackedValues + "]";
    }

    /* Other methods */

    /**
     * Records an operation that used the specified amount of time in
     * milliseconds.
     */
    public void add(long latencyMillis) {

        /* Ignore negative values */
        if (latencyMillis < 0) {
            return;
        }

        /*
         * Use a local var to support concurrent access.  See {@link
         * #trackedValues}.
         */
        final Values values = trackedValues;

        /*
         * if latency is a bigger than maxTrackedLatencyMillis,
         * put it the last bucket indexed by maxTrackedLatencyMillis, which
         * accounts ops with latency maxTrackedLatencyMillis or beyond.
         */
        final int bucket = (latencyMillis > maxTrackedLatencyMillis) ?
            maxTrackedLatencyMillis : (int)latencyMillis;

        values.histogram.incrementAndGet(bucket);

        /* Update the count last */
        values.count.incrementAndGet();
    }

    public void add(LatencyPercentile other) {
        checkSameMax(other);
        trackedValues = trackedValues.add(other.trackedValues);
    }

    public void negate() {
        trackedValues = trackedValues.negate();
    }

    /**
     * Updates the operation counts in this instance to represent the
     * difference between the values stored in this instance and the ones in
     * the argument.
     */
    public void updateInterval(LatencyPercentile other) {
        checkSameMax(other);
        trackedValues = trackedValues.computeInterval(other.trackedValues);
    }

    /** Check for same max latency. */
    private void checkSameMax(LatencyPercentile other) {
        if (maxTrackedLatencyMillis != other.maxTrackedLatencyMillis) {
            throw new IllegalArgumentException(
                "Stats must have the same maximum.  This stat uses " +
                maxTrackedLatencyMillis + ", but other stat uses " +
                other.maxTrackedLatencyMillis);
        }
    }

    /** Returns and clears the current stats. */
    private Values clearInternal() {
        final Values values = trackedValues;

        /*
         * Create a new instance to support concurrent access.  See {@link
         * #trackedValues}.
         */
        trackedValues = new Values(maxTrackedLatencyMillis);

        return values;
    }

    /**
     * Calculate may be called on a stat that is concurrently updating, so
     * while it has to be thread safe, it's a bit inaccurate when there's
     * concurrent activity. That tradeoff is made in order to avoid the cost of
     * synchronization during the add() method.  See {@link #trackedValues}.
     */
    private synchronized long calculate(boolean clear) {

        /*
         * Use a local var to support concurrent access.  See {@link
         * #trackedValues}.
         */
        final Values values = clear ? clearInternal() : trackedValues;

        /*
         * Check count first and return zero if it is zero.  This ensures that
         * we don't report partially computed values when they are zero.  This
         * works because the other values are calculated first by add(), and
         * count is incremented last.
         */
        final long count = values.count.get();
        if (count == 0) {
            return 0;
        }

        /*
         * Compute the number of entries equal to the percentile by rounding up
         * -- see:
         * https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method
         */
        final long percentileCount = (long) Math.ceil(count * percentile);

        final int histogramLength = values.histogram.length();
        int percentileValue = 0;
        long numSeen = 0;
        for (int latency = 0; latency < histogramLength; latency++) {
            final long latencyCount = values.histogram.get(latency);

            if (latencyCount == 0) {
                continue;
            }

            percentileValue = latency;
            numSeen += latencyCount;

            if (numSeen >= percentileCount) {
                break;
            }
        }

        savedPercentileValue = percentileValue;

        return percentileValue;
    }
}
