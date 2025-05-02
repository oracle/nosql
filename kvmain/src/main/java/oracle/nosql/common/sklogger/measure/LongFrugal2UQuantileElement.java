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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.JsonSerializationUtils;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.jss.ObjectNodeSerializable.FieldExtractor;


/**
 * Implements the Frugal2U Algorithm for estimating quantiles.
 *
 * <p>Reference: Ma, Qiang, S. Muthukrishnan, and Mark Sandler. "Frugal
 * Streaming for Estimating Quantiles." Space-Efficient Data Structures,
 * Streams, and Algorithms. Springer Berlin Heidelberg, 2013. 77-96. Available
 * at: http://arxiv.org/abs/1407.1121.
 *
 * <p>The algorithm uses extremely low memory consumption at the cost of
 * accuracy (approaching speed) as well as computation resource (i.e., since
 * each observation requires a random number generation).
 *
 * <p>The original algorithm computes the quantile on a stream of history data.
 * To fit our optimized algorithm for observe calls with O(1) complexity, we
 * need to devise a way to combine two history data. We simply compute a
 * weighted (on data size) average between the quantiles of the two history.
 * Intuitively, we assume that the stream feeds the data with a uniform
 * distribution, and therefore, each history estimates the quantile with an
 * accuracy corresponding to the data size.
 */
public class LongFrugal2UQuantileElement
    extends HistoryMeasure<
    Long, LongFrugal2UQuantileElement.Result,
    LongFrugal2UQuantileElement.Recent,
    LongFrugal2UQuantileElement.Past> {

    public LongFrugal2UQuantileElement(double...quantiles) {
        super(new Recent(quantiles), () -> new Past(quantiles));
    }

    protected static class Recent
        implements HistoryMeasure.RecentHistory<Long, Result, Recent, Past> {

        private final double[] quantiles;
        private final Estimate[] estimates;

        private Recent(double[] quantiles) {
            this.quantiles = quantiles;
            this.estimates = Arrays.stream(quantiles).
                mapToObj(q -> new Estimate(q)).toArray(Estimate[]::new);
        }

        @Override
        public void observe(Long value) {
            for (Estimate e : estimates) {
                e.observe(value);
            }
        }

        @Override
        public void reset() {
            for (Estimate e : estimates) {
                e.reset();
            }
        }

        @Override
        public Result computeWith(Past past) {
            final long[] values = new long[estimates.length];
            for (int i = 0; i < estimates.length; ++i) {
                values[i] = estimates[i].computeWith(past.estimates[i]);
            }
            return new Result(
                quantiles, values,
                estimates[0].count + past.estimates[0].count,
                estimates[0].sum + past.estimates[0].sum);
        }
    }

    protected static class Past
        implements HistoryMeasure.PastHistory<Long, Result, Recent, Past> {

        private final Estimate[] estimates;

        private Past(double[] quantiles) {
            this.estimates = Arrays.stream(quantiles).
                mapToObj(q -> new Estimate(q)).toArray(Estimate[]::new);
        }

        @Override
        public void reset() {
            for (Estimate e : estimates) {
                e.reset();
            }
        }

        @Override
        public void combine(Recent recent) {
            for (int i = 0; i < estimates.length; ++i) {
                estimates[i].combine(recent.estimates[i]);
            }
        }

        @Override
        public void inherit(Past past) {
            for (int i = 0; i < estimates.length; ++i) {
                estimates[i].combine(past.estimates[i]);
            }
        }
    }

    private static class Estimate {

        /* estimated quantile value */
        private long count = 0;
        private long sum = 0;
        private long estimated = 0;
        /* target quantile */
        private final double quantile;
        private int step = 1;
        private int sign = 0;
        private Random rand = new Random();

        private Estimate(double quantile) {
            this.quantile = quantile;
        }

        private synchronized void observe(long item) {
            /*
             * TODO: How to deal with overflow is not clear to me, which
             * depends on the purpose of count and sum. Just reset them to 0
             * for now.
             */
            try {
                count = Math.addExact(count, 1);
                sum = Math.addExact(sum, item);
            } catch (ArithmeticException e) {
                count = 0;
                sum = 0;
            }
            /* first item */
            if (sign == 0) {
                estimated = item;
                sign = 1;
                return;
            }

            if (item > estimated && rand.nextDouble() > 1 - quantile) {
                /* Increase estimated direction */

                /*
                 * Increment the step size if and only if the estimate keeps
                 * moving in the same direction. Step size is incremented by
                 * the result of applying the specified step function to the
                 * previous step size.
                 */
                step += sign * f(step);
                /*
                 * Increment the estimate by step size if step is positive.
                 * Otherwise, increment the step size by one.
                 */
                if (step > 0) {
                    estimated += step;
                } else {
                    estimated += 1;
                }
                /*
                 * If the estimate overshot the item in the stream, pull the
                 * estimate back and re-adjust the step size.
                 */
                if (estimated > item) {
                    step += (item - estimated);
                    estimated = item;
                }
                /*
                 * Reset the step if direction is changed.
                 */
                if (sign < 0) {
                    step = 1;
                }
                /*
                 * Mark that the estimate as increased direction.
                 */
                sign = 1;
            } else if (item < estimated && rand.nextDouble() > quantile) {
                /*
                 * Opposite to above increase estimated direction, it is
                 * decreasing estimated direction.
                 */
                step += -sign * f(step);

                if (step > 0) {
                    estimated -= step;
                } else {
                    estimated--;
                }

                if (estimated < item) {
                    step += (estimated - item);
                    estimated = item;
                }

                if (sign > 0) {
                    step = 1;
                }
                sign = -1;
            }
        }

        private synchronized int f(int currentStep) {
            /*
             * Move one step constantly. Increase the step if we want to
             * adjust more quickly.
             */
            return 1;
        }

        /**
         * Reset the estimate.
         */
        private synchronized void reset() {
            count = 0;
            estimated = 0;
            sum = 0;
        }

        /**
         * Computes an weighted average among two histories of estimation.
         */
        private synchronized long computeWith(Estimate past) {
            return (long)
                ((((double) estimated) * count +
                  ((double) past.estimated) * past.count) /
                 (count + past.count));
        }

        /**
         * Combines the estimates and count.
         */
        private synchronized void combine(Estimate recent) {
            estimated = recent.computeWith(this);
            count += recent.count;
            sum += recent.sum;
        }
    }

    public static class Result extends ObjectNodeSerializable {

        /** The default long array. */
        public static final long[] DEFAULT_ARRAY = new long[0];
        /** The default. */
        public static final Result DEFAULT =
            new Result(DEFAULT_ARRAY, DEFAULT_ARRAY,
                       LongField.DEFAULT, LongField.DEFAULT);
        /** The field map. */
        public static final Map<String, Field<?, Result>> FIELDS =
            new HashMap<>();
        /** The percentiles array. */
        public static final String PERCENTILES =
            Field.create(
                FIELDS, "percentiles",
                wrapped(Result::getPercentiles,
                        Result::writeExtractedLongArray),
                Result::readLongArray,
                DEFAULT_ARRAY);
        /** The values array. */
        public static final String VALUES =
            Field.create(
                FIELDS, "values",
                wrapped(Result::getValues,
                        Result::writeExtractedLongArray),
                Result::readLongArray,
                DEFAULT_ARRAY);
        /** The total number of occurrences. */
        public static final String COUNT =
            LongField.create(FIELDS, "count", Result::getCount);
        /** The sum of all the occurrences. */
        public static final String SUM =
            LongField.create(FIELDS, "sum", Result::getSum);

        private static void writeExtractedLongArray(ObjectNode payload,
                                                    String name,
                                                    long[] array,
                                                    long[] defaultValue) {
            if (Arrays.equals(array, defaultValue)) {
                return;
            }
            payload.put(
                name,
                Arrays.stream(array)
                .mapToObj((v) -> JsonUtils.createJsonNode(v))
                .collect(JsonSerializationUtils.getArrayCollector()));
        }

        private static long[] readLongArray(JsonNode payload,
                                            long[] defaultValue) {
            if (!payload.isArray()) {
                return defaultValue;
            }
            final ArrayNode array = payload.asArray();
            final long[] result = new long[array.size()];
            for (int i = 0; i < array.size(); ++i) {
                final JsonNode val = array.get(i);
                if (!val.isNumber()) {
                    return defaultValue;
                }
                result[i] = val.asLong();
            }
            return result;
        }

        private final double[] quantiles;
        private final long[] values;
        private final long count;
        private final long sum;

        public Result(double[] quantiles,
                      long[] values,
                      long count,
                      long sum) {
            this.quantiles = quantiles;
            this.values = values;
            this.count = count;
            this.sum = sum;
        }

        public Result(JsonNode payload) {
            this((long[]) readField(FIELDS, payload, PERCENTILES),
                 readField(FIELDS, payload, VALUES),
                 readField(FIELDS, payload, COUNT),
                 readField(FIELDS, payload, SUM));
        }

        private Result(long[] percentiles,
                       long[] values,
                       long count,
                       long sum) {
            this(Arrays.stream(percentiles)
                 .mapToDouble((p) -> p / 100.0).toArray(),
                 values, count, sum);
        }

        public double[] getQuantiles() {
            return quantiles;
        }

        public long[] getPercentiles() {
            return Arrays.stream(quantiles).
                mapToLong((q) -> (long) (q * 100)).
                toArray();
        }

        public long[] getValues() {
            return values;
        }

        public long getCount() {
            return count;
        }

        public long getSum() {
            return sum;
        }

        @Override
        public ObjectNode toJson() {
            return writeFields(FIELDS, this);
        }

        @Override
        public boolean isDefault() {
            return count == LongField.DEFAULT;
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
