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

import java.util.function.Function;


/**
 * An element that measures the min, max, average, 95th and 99th of a long
 * value of a known maximum over a period of time.
 *
 * <p>This implementation assumes the value is non-negative and is capped under
 * a maximum. Values over the maximum are dropped and counted as overflow.
 * Values less than zero are simply dropped.
 *
 * <p>The implementation uses a histogram for computing the stats. The maximum
 * number of histogram bucket can be specified. The actual maximum of the
 * histogram is always a multiple of the number of buckets. Therefore, the
 * maximum value reported can be an overestimate. This does not seem to be a
 * problem. The value is capped and the overflow count is reported, so the
 * maximum value is of less interest. The caller can specify the number of
 * buckets to a proper value to avoid such inaccuracy as well.
 *
 * <p>Average, max, 95th and 99th stats are computed excluding overflow data.
 * The overflow data is represented by an overflow count. See {@link
 * LongValueStats#computeStats}.
 *
 * @see LongPercentileElement for a long element with unknown maximum
*/
public class LongCappedPercentileElement
    extends HistogramElement<
    LongValueStats,
    LongCappedPercentileElement.Current,
    LongCappedPercentileElement.Start> {

    private static final int DEFAULT_MAX_NUM_BUCKETS = 1000;

    private final Current current;
    private final int numBuckets;

    public LongCappedPercentileElement(long maximum) {
        this(maximum, (int) Math.min(maximum, DEFAULT_MAX_NUM_BUCKETS));
    }

    public LongCappedPercentileElement(long maximum,
                                       int numBuckets) {
        final long actualMax = adjustMaximum(maximum, numBuckets);
        final int actualNumBuckets = adjustNumBuckets(actualMax, numBuckets);
        this.numBuckets = actualNumBuckets;
        this.current = new Current(actualMax, actualNumBuckets);
    }

    /**
     * Rounds up the maximum according to the number of buckets.
     */
    private static long adjustMaximum(long maximum, int numBuckets) {
        return (maximum + numBuckets - 1) / numBuckets * numBuckets;
    }

    /**
     * Adds a overflow bucket if the maximum is not Long.MAX_VALUE.
     */
    private static int adjustNumBuckets(long maximum, int numBuckets) {
        return (maximum == Long.MAX_VALUE) ? numBuckets : numBuckets + 1;
    }

    @Override
    protected Current getCurrent() {
        return current;
    }

    @Override
    protected Start createStartHistory() {
        return new Start(numBuckets);
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

        private final long maximum;

        protected Current(long maximum, int numBuckets) {
            super(numBuckets);
            this.maximum = maximum;
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
                histogramDiff,
                getUpperBoundFunction(maximum, getNumBuckets()));
        }

        @Override
        protected int getIndex(long val) {
            return getBucketIndex(maximum, getNumBuckets(), val);
        }
    }

    /**
     * Given a maximum and the total number of buckets, returns the index
     * function.
     *
     * <p>Both the maximum and the number of buckets are adjusted. The number
     * of buckets is adjusted so that the last bucket is the overflow bucket.
     * The maximum is adjusted so that it is a multiple of the number of
     * non-overflow buckets.
     */
    private static int getBucketIndex(
        long maximum, int numBuckets, long val) {

        if (val >= maximum) {
            return numBuckets - 1;
        }

        if (maximum == Long.MAX_VALUE) {
            /*
             * The overflow bucket was not added, maximum is a multiple of
             * the number of buckets.
             */
            return (int) (val / (maximum / numBuckets));
        }

        return (int) (val / (maximum / (numBuckets - 1)));
    }

    /**
     * Given a maximum and the total number of buckets, returns the upper bound
     * function.
     */
    private static Function<Integer, Long> getUpperBoundFunction(
        long maximum,
        int numBuckets) {

        return (i) -> {
            if (i >= numBuckets) {
                throw new IllegalStateException(String.format(
                    "Index %s exceeds the number of buckets, " +
                    "numBuckets=%s, maximum=%s"));
            }
            if (i == numBuckets - 1) {
                return Long.MAX_VALUE;
            }
            if (maximum == Long.MAX_VALUE) {
                /* The overflow bucket was not added */
                return (i + 1) * (maximum / numBuckets);
            }
            return (i + 1) * (maximum / (numBuckets - 1));
        };
    }

    protected static class Start
        extends HistogramElement.Start<LongValueStats, Current, Start> {

        protected Start(int numBuckets) {
            super(numBuckets);
        }
    }
}
