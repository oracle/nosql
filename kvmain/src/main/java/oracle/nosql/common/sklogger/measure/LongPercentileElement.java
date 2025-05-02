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

/**
 * An element that measures the min, max, average, 95th and 99th of a
 * non-negative long value over a period of time.
 *
 * <p>This implementation uses a fixed amount of buckets to cover the range
 * from 0 to Long.MAX_VALUE. The buckets are assigned as follows, K stands for
 * 1000, M stands for 1_000_000, etc:
 *
 * 0,99 : 100 buckets, precision 1, upper bounds [1, 2, ..., 100]
 * 100,999 : 90 buckets, precision 10, upper bounds [110, 120, ..., 1K]
 * 1K,1M-1 : 100 buckets, precision 10K, upper bounds [10K, 20K, ..., 1M]
 * 1M,1G-1 : 100 buckets, precision 10M, upper bounds [10M, 20M, ..., 1G]
 * ..., ...
 * 10^18,2^63 - 1 : 1 bucket for overflow, upper bound Long.MAX_VALUE
 *
 * In total, this scheme uses 691 buckets.
 *
 * <p>Average, max, 95th and 99th stats are computed excluding overflow data.
 * The overflow data is represented by an overflow count. See {@link
 * LongValueStats#computeStats}.
 *
 * @see LongCappedPercentileElement for a long element with known maximum and
 * fixed precision
 * @see LatencyElement for a long element for latency metrics
 */
public class LongPercentileElement
    extends HistogramElement<LongValueStats,
                             LongPercentileElement.Current,
                             LongPercentileElement.Start> {

    /* Histogram boundary constants. */
    private static final long K = 1_000L;
    private static final long M = K * K;
    private static final long G = M * K;
    private static final long T = G * K;
    private static final long P = T * K;
    private static final long E = P * K;
    /* Histogram bucket sizes */
    /* Below 100 */
    private static final int NUM_BUCKETS_BELOW_100 = 100;
    /* Below 1K */
    private static final int NUM_BUCKETS_100_TO_1K = 90;
    private static final int NUM_BUCKETS_BELOW_1K =
        NUM_BUCKETS_BELOW_100 + NUM_BUCKETS_100_TO_1K;
    /* Below 1M */
    private static final int NUM_BUCKETS_1K_TO_1M = 100;
    private static final int NUM_BUCKETS_BELOW_1M =
        NUM_BUCKETS_BELOW_1K + NUM_BUCKETS_1K_TO_1M;
    /* Below 1G */
    private static final int NUM_BUCKETS_1M_TO_1G = 100;
    private static final int NUM_BUCKETS_BELOW_1G =
        NUM_BUCKETS_BELOW_1M + NUM_BUCKETS_1M_TO_1G;
    /* Below 1T */
    private static final int NUM_BUCKETS_1G_TO_1T = 100;
    private static final int NUM_BUCKETS_BELOW_1T =
        NUM_BUCKETS_BELOW_1G + NUM_BUCKETS_1G_TO_1T;
    /* Below 1P */
    private static final int NUM_BUCKETS_1T_TO_1P = 100;
    private static final int NUM_BUCKETS_BELOW_1P =
        NUM_BUCKETS_BELOW_1T + NUM_BUCKETS_1T_TO_1P;
    /* Below 1E */
    private static final int NUM_BUCKETS_1P_TO_1E = 100;
    private static final int NUM_BUCKETS_BELOW_1E =
        NUM_BUCKETS_BELOW_1P + NUM_BUCKETS_1P_TO_1E;
    /* Above 1E */
    private static final int NUM_BUCKETS_ABOVE_1E = 1;
    /* num of buckets */
    private static final int NUM_BUCKETS =
        NUM_BUCKETS_BELOW_1E + NUM_BUCKETS_ABOVE_1E;
    /* Histogram bucket bounds */
    private static final long[] BUCKET_UPPERBOUNDS = new long[NUM_BUCKETS];

    static {
        computeBucketBoundaries();
    }


    /* The current history */
    private final Current current = new Current();

    private static void computeBucketBoundaries() {
        computeBucketBoundaries(
            1, 0, NUM_BUCKETS_BELOW_100);
        computeBucketBoundariesForRange(
            100, K, NUM_BUCKETS_BELOW_100, NUM_BUCKETS_100_TO_1K,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundaries(
            10 * K, NUM_BUCKETS_BELOW_1K, NUM_BUCKETS_1K_TO_1M);
        computeBucketBoundaries(
            10 * M, NUM_BUCKETS_BELOW_1M, NUM_BUCKETS_1M_TO_1G);
        computeBucketBoundaries(
            10 * G, NUM_BUCKETS_BELOW_1G, NUM_BUCKETS_1G_TO_1T);
        computeBucketBoundaries(
            10 * T, NUM_BUCKETS_BELOW_1T, NUM_BUCKETS_1T_TO_1P);
        computeBucketBoundaries(
            10 * P, NUM_BUCKETS_BELOW_1P, NUM_BUCKETS_1P_TO_1E);
        BUCKET_UPPERBOUNDS[NUM_BUCKETS - 1] = Long.MAX_VALUE;
    }

    private static void computeBucketBoundaries(long boundUnit,
                                                int numBucketsBefore,
                                                int numBuckets) {
        for (int i = 0; i < numBuckets; ++i) {
            BUCKET_UPPERBOUNDS[i + numBucketsBefore] = (i + 1) * boundUnit;
        }
    }

    /**
     * Returns the index for the given value.
     */
    private static int getBucketIndex(long value) {
        if (value < G) {
            if (value < K) {
                if (value < 100) {
                    return getBucketIndex(value, 1, 0);
                }
                return getBucketIndexForRange(
                    value, 100, K,
                    NUM_BUCKETS_BELOW_100, NUM_BUCKETS_100_TO_1K);
            } else if (value < M) {
                return getBucketIndex(
                    value, 10 * K, NUM_BUCKETS_BELOW_1K);
            }
            return getBucketIndex(
                value, 10 * M, NUM_BUCKETS_BELOW_1M);
        } else if (value < P) {
            if (value < T) {
                return getBucketIndex(
                    value, 10 * G, NUM_BUCKETS_BELOW_1G);
            }
            return getBucketIndex(
                value, 10 * T, NUM_BUCKETS_BELOW_1T);
        } else if (value < E) {
            return getBucketIndex(
                value, 10 * P, NUM_BUCKETS_BELOW_1P);
        }
        return NUM_BUCKETS - 1;
    }

    private static int getBucketIndex(long value,
                                      long boundUnit,
                                      int numBucketsBefore) {
        return numBucketsBefore + (int) (value / boundUnit);
    }

    @Override
    protected Current getCurrent() {
        return current;
    }

    @Override
    protected Start createStartHistory() {
        return new Start();
    }

    /**
     * Observes a value.
     */
    public void observe(long value) {
        final Observation ob = observationThreadLocal.get();
        ob.value = value;
        ob.count = 1;
        observe(ob);
    }

    protected static class Current
        extends HistogramElement.Current<LongValueStats, Current, Start> {

        private Current() {
            super(NUM_BUCKETS);
        }

        @Override
        public void observe(Observation ob) {
            if (ob.value < 0) {
                return;
            }
            super.observe(ob);
        }

        @Override
        public LongValueStats compare(Start start) {
            final long[] histogramDiff = setSnapshotAndReturnDiff(start);
            return new LongValueStats(
                histogramDiff, (i) -> BUCKET_UPPERBOUNDS[i]);
        }

        @Override
        protected int getIndex(long val) {
            return getBucketIndex(val);
        }
    }

    protected static class Start
        extends HistogramElement.Start<LongValueStats, Current, Start> {

        private Start() {
            super(NUM_BUCKETS);
        }
    }
}
