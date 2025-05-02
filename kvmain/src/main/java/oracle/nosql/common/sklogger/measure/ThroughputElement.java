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

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.jss.ObjectNodeSerializable.FieldExtractor;

/**
 * An element that measures the throughput over a period of time.
 *
 * <p>The throughput is the average per second of the sum of observed values.
 * Observed values are truncated to long values.
 *
 * <p>This class is expected to be used to measure the throughput of a computer
 * system that usually cannot overflow a {@code long} counter.
 *
 * @see LongCounterElement for a counter-oriented element.
 */
public class ThroughputElement
    extends MemorylessHistoryMeasure<
    Long, ThroughputElement.Result,
    ThroughputElement.Current, ThroughputElement.Start> {

    private final Current current = new Current();
    private final long startTimeNanos = System.nanoTime();

    @Override
    protected Current getCurrent() {
        return current;
    }

    @Override
    protected Start createStartHistory() {
        return new Start(startTimeNanos);
    }

    public void observe(int value) {
        observe((long) value);
    }

    protected static class Current
        implements MemorylessHistoryMeasure.CurrentHistory<
        Long, ThroughputElement.Result, Current, Start> {

        private final LongAdder count = new LongAdder();
        private long obtainTimeNanos = 0;

        @Override
        public void observe(Long value) {
            count.add(value);
        }

        @Override
        public Result compare(Start start) {
            obtainTimeNanos = System.nanoTime();
            final long duration = obtainTimeNanos - start.obtainTimeNanos;
            return new Result(count.longValue() - start.count, duration);
        }

        public long getCount() {
            return count.longValue();
        }
    }

    protected static class Start
        implements MemorylessHistoryMeasure.StartHistory<
        Long, ThroughputElement.Result, Current, Start> {

        private long count = 0;
        private long obtainTimeNanos;

        protected Start(long startTimeNanos) {
            this.obtainTimeNanos = startTimeNanos;
        }

        @Override
        public void set(Current current) {
            count = current.count.longValue();
            obtainTimeNanos = current.obtainTimeNanos;
        }
    }

    /**
     * The throughput result.
     */
    public static class Result extends ObjectNodeSerializable {

        private static final long ONE_MOUNTH_IN_NANOS = DAYS.toNanos(1) * 30;
        private static final long ONE_YEAR_IN_NANOS = DAYS.toNanos(1) * 365;
        private static final long[] SCALES = new long[] {
                0L, /* place holder, should not be used. */
                SECONDS.toNanos(1),
                MINUTES.toNanos(1),
                HOURS.toNanos(1),
                DAYS.toNanos(1),
                ONE_MOUNTH_IN_NANOS,
                ONE_YEAR_IN_NANOS,
        };
        private static final String[] UNITS = new String[] {
            StringField.DEFAULT,
            "/second",
            "/minute",
            "/hour",
            "/day",
            "/month",
            "/year",
        };

        /** The field map. */
        public static final Map<String, Field<?, Result>> FIELDS =
            new HashMap<>();
        /** The total count. */
        public static final String COUNT =
            LongField.create(FIELDS, "count", Result::getCount);
        /** The duration corresponding to the count in nano-seconds. */
        public static final String DURATION =
            LongField.create(
                FIELDS, "durationNanos", Result::getDurationNanos);
        /**
         * The throughput value scaled to the proper unit so that it is an
         * integer. This field is for readablity. We have seen throughput
         * values that are too small if the unit is per-second: understanding a
         * floating-point value with several leading zeros is difficult. With
         * this field, a low throughput will have this field to be one, while
         * the reader should pay attention to the {@code SCALED_UNIT} to
         * understand the throughput value.
         */
        public static final String SCALED_VALUE =
            LongField.create(FIELDS, "scaledValue", Result::getScaledValue);
        /**
         * The throughput unit scaled so that {@code SCALED_VALUE} is an
         * integer. This field is for readablity. We have seen throughput
         * values that are too small if the unit is per-second: understanding a
         * floating-point value with several leading zeros is difficult.
         */
        public static final String SCALED_UNIT =
            StringField.create(FIELDS, "scaledUnit", Result::getScaledUnit);

        /** The default throughput result. */
        public static final Result DEFAULT =
            new Result(LongField.DEFAULT, LongField.DEFAULT);


        private final long count;
        private final long durationNanos;
        private final long scaledValue;
        private final String scaledUnit;

        public Result(long count,
                      long durationNanos) {
            this(count, durationNanos, computeValue(count, durationNanos));
        }

        /*
         * Computes the scaled throughput and returns an array of two
         * representing the value and the unit index.
         */
        private static long[] computeValue(long count,
                                           long durationNanos) {
            if ((count == 0) || (durationNanos == 0)) {
                return new long[] { 0, 0 };
            }
            long v = 0;
            int uidx = 0;
            for (int i = 1; i < SCALES.length; ++i) {
                v = Math.round(
                    count * ((double) SCALES[i] / durationNanos));
                if (v >= 1) {
                    uidx = i;
                    break;
                }
            }
            if (v < 1) {
                v = Math.round(
                    count * (((double) SCALES[SCALES.length - 1])
                              / durationNanos));
                uidx = UNITS.length - 1;
            }
            final long[] result = new long[] { v, uidx };
            return result;
        }

        private Result(long count,
                       long durationNanos,
                       long[] scaled) {
            this(count, durationNanos, scaled[0],
                 UNITS[(int) scaled[1]]);
        }

        public Result(long count,
                      long durationNanos,
                      long scaledValue,
                      String scaledUnit) {
            this.count = count;
            this.durationNanos = durationNanos;
            this.scaledValue = scaledValue;
            this.scaledUnit = scaledUnit;
        }

        public Result(JsonNode payload) {
            this(readField(FIELDS, payload, COUNT),
                 readField(FIELDS, payload, DURATION),
                 readField(FIELDS, payload, SCALED_VALUE),
                 readField(FIELDS, payload, SCALED_UNIT));
        }

        /**
         * Returns the throughput per second.
         *
         * Use double for a better precision when throughput is low.
         */
        public double getThroughputPerSecond() {
            if (durationNanos == 0) {
                return 0;
            }
            return ((double) count)  * SECONDS.toNanos(1) / durationNanos;
        }

        /**
         * Returns the count (equivalently throughput multiplies duration) of
         * this element.
         */
        public long getCount() {
            return count;
        }

        /**
         * Returns the duration corresponding to the count.
         */
        public long getDurationNanos() {
            return durationNanos;
        }

        /**
         * Returns the scaled throughput value.
         */
        public long getScaledValue() {
            return scaledValue;
        }

        /**
         * Returns the scaled throughput unit.
         */
        public String getScaledUnit() {
            return scaledUnit;
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

    public long getCurrentCount() {
        return current.getCount();
    }
}
