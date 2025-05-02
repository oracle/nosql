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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.jss.ObjectNodeSerializable.FieldExtractor;

/**
 * An element that measures the min, max, average, 95th and 99th of a latency
 * metric over a period of time.
 *
 * <p>This implementation solves two problems of using histogram to compute
 * latency percentile stats: (1) a maximum value is needed in advance to
 * allocate enough buckets; (2) the result does not have enough precision if
 * values are small. Given the nature of latency, we resolve these two issues
 * by devide the set of positive integer into the following ranges:
 *
 *  0.00-0.99 ms - 100 buckets, 0.01 ms accuracy, upper bounds 0.01 - 1.0 ms
 *  1.0-9.9 ms - 90 buckets, 0.1 ms accuracy, upper bounds 1.1 - 10ms
 *  10-99 ms - 90 buckets, 1 ms accuracy, upper bounds 11 - 100ms
 *  100-990 ms - 90 buckets, 10 ms accuracy, upper bounds 110 - 1 ms
 *  1000-9900 ms - 90 buckets, 100 ms accuracy, upper bounds 1.1 - 10s
 *  10-99 sec - 90 buckets, 1 sec accuracy, upper bounds 11 - 100s
 *  100-990 sec - 90 buckets, 10 sec accuracy, upper bounds 110 - 1000s
 *  1000+ sec - 1 bucket for overflow, upper bound Long.MAX_VALUE
 *
 *  In total, this scheme uses 641 buckets.
 *
 * <p>Average, max, 95th and 99th stats are computed excluding overflow data.
 * The overflow data is represented by an overflow count. See {@link
 * LongValueStats#computeStats}.
 *
 * <p>Latency values are stored and computed with nanoseconds as the time unit.
 * The result reports with nanoseconds as well. The convenient {@link
 * LatencyElement.Result#toJson(TimeUnit)} can be used to report stats with
 * other time units with double-digits precision.
*/
public class LatencyElement
    extends HistogramElement<LatencyElement.Result,
                             LatencyElement.Current,
                             LatencyElement.Start> {

    /* Histogram boundary constants in nanoseconds */
    private static final long NANOS_1_MS = 1_000_000L;
    private static final long NANOS_10_MS = 10 * NANOS_1_MS;
    private static final long NANOS_100_MS = 100 * NANOS_1_MS;
    private static final long NANOS_1_S = 1000 * NANOS_1_MS;
    private static final long NANOS_10_S = 10 * NANOS_1_S;
    private static final long NANOS_100_S = 100 * NANOS_1_S;
    private static final long NANOS_1000_S = 1000 * NANOS_1_S;
    /* Histogram bucket sizes */
    /* Below 1ms */
    private static final int NUM_BUCKETS_BELOW_1_MS = 100;
    /* Below 10ms */
    private static final int NUM_BUCKETS_1_MS_TO_10_MS = 90;
    private static final int NUM_BUCKETS_BELOW_10_MS =
        NUM_BUCKETS_BELOW_1_MS + NUM_BUCKETS_1_MS_TO_10_MS;
    /* Below 100ms */
    private static final int NUM_BUCKETS_10_MS_TO_100_MS = 90;
    private static final int NUM_BUCKETS_BELOW_100_MS =
        NUM_BUCKETS_BELOW_10_MS + NUM_BUCKETS_10_MS_TO_100_MS;
    /* Below 1s */
    private static final int NUM_BUCKETS_100_MS_TO_1_S = 90;
    private static final int NUM_BUCKETS_BELOW_1_S =
        NUM_BUCKETS_BELOW_100_MS + NUM_BUCKETS_100_MS_TO_1_S;
    /* Below 10s */
    private static final int NUM_BUCKETS_1_S_TO_10_S = 90;
    private static final int NUM_BUCKETS_BELOW_10_S =
        NUM_BUCKETS_BELOW_1_S + NUM_BUCKETS_1_S_TO_10_S;
    /* Below 100s */
    private static final int NUM_BUCKETS_10_S_TO_100_S = 90;
    private static final int NUM_BUCKETS_BELOW_100_S =
        NUM_BUCKETS_BELOW_10_S + NUM_BUCKETS_10_S_TO_100_S;
    /* Below 1000s */
    private static final int NUM_BUCKETS_100_S_TO_1000_S = 90;
    private static final int NUM_BUCKETS_BELOW_1000_S =
        NUM_BUCKETS_BELOW_100_S + NUM_BUCKETS_100_S_TO_1000_S;
    /* Above 1000s */
    private static final int NUM_BUCKETS_ABOVE_1000_S = 1;
    /* num of buckets */
    private static final int NUM_BUCKETS =
        NUM_BUCKETS_BELOW_1000_S + NUM_BUCKETS_ABOVE_1000_S;
    /* Histogram bucket bounds */
    private static final long[] BUCKET_UPPERBOUNDS = new long[NUM_BUCKETS];

    static {
        computeBucketBoundaries();
    }

    /* The current history */
    private final Current current = new Current();

    private static void computeBucketBoundaries() {
        computeBucketBoundariesForRange(
            0, NANOS_1_MS, 0, NUM_BUCKETS_BELOW_1_MS,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundariesForRange(
            NANOS_1_MS, NANOS_10_MS,
            NUM_BUCKETS_BELOW_1_MS, NUM_BUCKETS_1_MS_TO_10_MS,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundariesForRange(
            NANOS_10_MS, NANOS_100_MS,
            NUM_BUCKETS_BELOW_10_MS, NUM_BUCKETS_10_MS_TO_100_MS,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundariesForRange(
            NANOS_100_MS, NANOS_1_S,
            NUM_BUCKETS_BELOW_100_MS, NUM_BUCKETS_100_MS_TO_1_S,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundariesForRange(
            NANOS_1_S, NANOS_10_S,
            NUM_BUCKETS_BELOW_1_S, NUM_BUCKETS_1_S_TO_10_S,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundariesForRange(
            NANOS_10_S, NANOS_100_S,
            NUM_BUCKETS_BELOW_10_S, NUM_BUCKETS_10_S_TO_100_S,
            BUCKET_UPPERBOUNDS);
        computeBucketBoundariesForRange(
            NANOS_100_S, NANOS_1000_S,
            NUM_BUCKETS_BELOW_100_S, NUM_BUCKETS_100_S_TO_1000_S,
            BUCKET_UPPERBOUNDS);
        BUCKET_UPPERBOUNDS[NUM_BUCKETS - 1] = Long.MAX_VALUE;
    }

    /**
     * Returns the index for the given latency.
     */
    private static int getBucketIndex(long latencyNanos) {
        if (latencyNanos < NANOS_1_S) {
            if (latencyNanos < NANOS_10_MS) {
                if (latencyNanos < NANOS_1_MS) {
                    return getBucketIndexForRange(latencyNanos, 0, NANOS_1_MS,
                                                  0, NUM_BUCKETS_BELOW_1_MS);
                }
                return getBucketIndexForRange(
                    latencyNanos, NANOS_1_MS, NANOS_10_MS,
                    NUM_BUCKETS_BELOW_1_MS, NUM_BUCKETS_1_MS_TO_10_MS);
            } else if (latencyNanos < NANOS_100_MS) {
                return getBucketIndexForRange(
                    latencyNanos, NANOS_10_MS, NANOS_100_MS,
                    NUM_BUCKETS_BELOW_10_MS, NUM_BUCKETS_10_MS_TO_100_MS);
            }
            return getBucketIndexForRange(
                latencyNanos, NANOS_100_MS, NANOS_1_S,
                NUM_BUCKETS_BELOW_100_MS, NUM_BUCKETS_100_MS_TO_1_S);
        } else if (latencyNanos < NANOS_100_S) {
            if (latencyNanos < NANOS_10_S) {
                return getBucketIndexForRange(
                    latencyNanos, NANOS_1_S, NANOS_10_S,
                    NUM_BUCKETS_BELOW_1_S, NUM_BUCKETS_1_S_TO_10_S);
            }
            return getBucketIndexForRange(
                latencyNanos, NANOS_10_S, NANOS_100_S,
                NUM_BUCKETS_BELOW_10_S, NUM_BUCKETS_10_S_TO_100_S);
        } else if (latencyNanos < NANOS_1000_S) {
            return getBucketIndexForRange(
                latencyNanos, NANOS_100_S, NANOS_1000_S,
                NUM_BUCKETS_BELOW_100_S, NUM_BUCKETS_100_S_TO_1000_S);
        }
        return NUM_BUCKETS - 1;
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
     * Observes an average operation latency for a number of operations
     * associated with a single request. The latency represents the latency of
     * a single operation computed as an average of the observation of the
     * specified number of operations.
     *
     * TODO: This method is added to support collecting latency value for the
     * multi-operation requests in KV. The current stats collection model is a
     * bit awkward as the operation latency is not directly observed. We
     * probably should treat single and multi-operations requests (as well as
     * query) differently. And thus not dealing with them at this layer.
     */
    public void observe(long latencyNanos, int numOperations) {
        if (latencyNanos < 0) {
            return;
        }
        final Observation ob = observationThreadLocal.get();
        ob.value = latencyNanos;
        ob.count = numOperations;
        observe(ob);
    }

    /**
     * Observes a latency value.
     */
    public void observe(long latencyNanos) {
        observe(latencyNanos, 1);
    }

    protected static class Current
        extends HistogramElement.Current<Result, Current, Start> {

        /*
         * The current request count. Each observation counts as one request.
         * Each request may have multiple operations. The summation of counts
         * over the histogram equals to the operation count.
         */
        private final LongAdder requestCount = new LongAdder();
        private long requestCountSnapshot;

        private Current() {
            super(NUM_BUCKETS);
        }

        @Override
        public void observe(Observation ob) {
            super.observe(ob);
            requestCount.increment();
        }


        @Override
        public Result compare(Start start) {
            final long[] histogramDiff = setSnapshotAndReturnDiff(start);
            requestCountSnapshot = requestCount.sum();
            final long requestCountDiff =
                requestCountSnapshot - start.requestCount;
            return new Result(requestCountDiff, histogramDiff);
        }

        @Override
        protected int getIndex(long val) {
            return getBucketIndex(val);
        }
    }

    protected static class Start
        extends HistogramElement.Start<Result, Current, Start> {

        /* The request count */
        private long requestCount;

        private Start() {
            super(NUM_BUCKETS);
        }

        @Override
        public void set(Current current) {
            super.set(current);
            requestCount = current.requestCountSnapshot;
        }
    }

    public static class Result extends LongValueStatsBase {

        /** The field map. */
        public static final Map<String, Field<?, Result>> FIELDS =
            new HashMap<>();
        /**
         * The total number of requests, including the overflow count. A
         * request may be of multiple operations. For each request, we compute
         * the latency and add that value to the histogram n times where n is
         * the number of operaitons.
         */
        public static final String REQUEST_COUNT =
            LongField.create(
                FIELDS, "requestCount", Result::getRequestCount);
        /**
         * The total number of operations, including the overflow count.
         */
        public static final String OPERATION_COUNT =
            LongField.create(
                FIELDS, "operationCount", Result::getOperationCount);

        /** The default latency result. */
        public static final Result DEFAULT =
            new Result(LongField.DEFAULT,
                       LongField.DEFAULT,
                       LongField.DEFAULT,
                       LongField.DEFAULT,
                       LongField.DEFAULT,
                       LongField.DEFAULT,
                       LongField.DEFAULT,
                       LongField.DEFAULT);

        private final long requestCount;
        private final long operationCount;

        public Result(long requestCount,
                      long operationCount,
                      long min,
                      long max,
                      long average,
                      long percent95,
                      long percent99,
                      long overflowCount) {
            super(min, max, average,
                  percent95, percent99, overflowCount);
            this.requestCount = requestCount;
            this.operationCount = operationCount;
        }

        private Result(long requestCount,
                       long[] histogram) {
            this(requestCount,
                 computeStats(histogram,
                              (i) -> BUCKET_UPPERBOUNDS[i]),
                 false /* place holder */);
        }

        private Result(long requestCount,
                       HistogramSummary histogramSummary,
                       boolean placeHolder) {
            super(histogramSummary);
            this.requestCount = requestCount;
            this.operationCount = histogramSummary.count;
        }

        public Result(JsonNode payload) {
            super(payload);
            this.requestCount = readField(FIELDS, payload, REQUEST_COUNT);
            this.operationCount = readField(FIELDS, payload, OPERATION_COUNT);
        }

        public long getRequestCount() {
            return requestCount;
        }

        public long getOperationCount() {
            return operationCount;
        }

        @Override
        public ObjectNode toJson() {
            return writeFields(FIELDS, super.toJson(), this);
        }

        @Override
        public boolean isDefault() {
            return super.isDefault() && (operationCount == LongField.DEFAULT);
        }
    }

    /**
     * A {@link Result} field in an {@link ObjectNodeSerializable}.
     */
    public static class ResultField<T extends ObjectNodeSerializable>
        extends ObjectNodeSerializable.JsonSerializableField<Result, T>
    {
        public static <T extends ObjectNodeSerializable>
            String create(
                Map<String, ObjectNodeSerializable.Field<?, T>> fields,
                String name,
                FieldExtractor<Result, T> fieldExtractor)
        {
            return new ResultField<>(fields, name, fieldExtractor).getName();
        }

        private ResultField(
            Map<String, ObjectNodeSerializable.Field<?, T>> fields,
            String name,
            FieldExtractor<Result, T> fieldExtractor)
        {
            super(fields, name, fieldExtractor,
                  (p, d) -> new Result(p),
                  Result.DEFAULT);
        }
    }
}
