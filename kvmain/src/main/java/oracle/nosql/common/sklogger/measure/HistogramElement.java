/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger.measure;

import java.util.concurrent.atomic.AtomicLongArray;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * An element that uses histogram to measures the min, max, average, 95th and
 * 99th of a long value metric over a period of time.
*/
public abstract class HistogramElement<
    R extends JsonSerializable,
    CH extends HistogramElement.Current<R, CH, SH>,
    SH extends HistogramElement.Start<R, CH, SH>>
    extends MemorylessHistoryMeasure<HistogramElement.Observation, R, CH, SH> {

    /* A thread-local value to avoid create observation objects */
    protected static final ThreadLocal<Observation> observationThreadLocal =
        ThreadLocal.withInitial(Observation::new);

    /**
     * An observation including a value and a count.
     */
    public static class Observation {
        public long value;
        public long count;
    }

    public static abstract class Current<
        R extends JsonSerializable,
        C extends Current<R, C, S>,
        S extends Start<R, C, S>>
        implements CurrentHistory<Observation, R, C, S> {

        /* The histogram */
        protected final AtomicLongArray histogram;
        /*
         * A snapshot of the histogram to avoid go through the histogram twice
         * during Current#compare and Start#set.
         */
        protected final long[] histogramSnapshot;

        protected Current(int numBuckets) {
            this.histogram = new AtomicLongArray(numBuckets);
            this.histogramSnapshot = new long[numBuckets];
        }

        @Override
        public void observe(Observation ob) {
            histogram.addAndGet(getIndex(ob.value), ob.count);
        }

        /**
         * Returns the number of buckets.
         */
        public int getNumBuckets() {
            return histogramSnapshot.length;
        }

        /**
         * Sets the {@code histogramSnapshot} and returns the diff array
         * between the snapshot and the start-time histogram.
         */
        protected long[] setSnapshotAndReturnDiff(S start) {
            final int numBuckets = histogramSnapshot.length;
            for (int i = 0; i < numBuckets; ++i) {
                histogramSnapshot[i] = histogram.get(i);
            }
            final long[] histogramDiff = new long[numBuckets];
            for (int i = 0; i < numBuckets; ++i) {
                histogramDiff[i] = histogramSnapshot[i] - start.histogram[i];
            }
            return histogramDiff;
        }

        /**
         * Returns the index of the bucket given a value.
         */
        protected abstract int getIndex(long val);
    }

    public static abstract class Start<
        R extends JsonSerializable,
        C extends Current<R, C, S>,
        S extends Start<R, C, S>>
        implements StartHistory<Observation, R, C, S> {

        /* The histogram */
        protected final long[] histogram;

        protected Start(int numBuckets) {
            this.histogram = new long[numBuckets];
        }

        @Override
        public void set(C current) {
            for (int i = 0; i < histogram.length; ++i) {
                histogram[i] = current.histogramSnapshot[i];
            }
        }
    }

    /* Utility methods. */

    /**
     * Computes the upper bound values with linear steps.
     */
    protected static void computeBucketBoundariesForRange(
        long lowerBound,
        long upperBound,
        int numBucketsBefore,
        int numBuckets,
        long[] bucketUpperBounds)
    {
        for (int i = 0; i < numBuckets; ++i) {
            bucketUpperBounds[i + numBucketsBefore] =
                (upperBound - lowerBound) / numBuckets *
                (i + 1) + lowerBound;
        }
    }

    /**
     * Computes the bucket index for a linear-step range.
     */
    protected static int getBucketIndexForRange(long value,
                                                long lowerBound,
                                                long upperBound,
                                                int numBucketsBefore,
                                                int numBuckets) {
        return numBucketsBefore +
            (int) ((value - lowerBound) /
                   ((upperBound - lowerBound) / numBuckets));
    }
}
