
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
import java.util.concurrent.atomic.AtomicLong;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.jss.ObjectNodeSerializable.FieldExtractor;


/**
 * An element that measures the min/max of a long value observed over a period
 * of time.
 */
public class LongMinMaxElement
    extends HistoryMeasure<
    Long, LongMinMaxElement.Result,
    LongMinMaxElement.Recent, LongMinMaxElement.Past> {

    /**
     * Constructs the element. If no observation, Long.MAX_VALUE is returned
     * for min and Long.MIN_VALUE is returned for max.
     */
    public LongMinMaxElement() {
        super(new Recent(), () -> new Past());
    }

    protected static class Recent
        implements HistoryMeasure.RecentHistory<Long, Result, Recent, Past> {

        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        @Override
        public void observe(Long value) {
            min.getAndUpdate((prev) -> Math.min(prev, value));
            max.getAndUpdate((prev) -> Math.max(prev, value));
        }

        @Override
        public void reset() {
            min.set(Long.MAX_VALUE);
            max.set(Long.MIN_VALUE);
        }

        @Override
        public Result computeWith(Past past) {
            final long minValue = min.get();
            final long maxValue = max.get();
            return new Result(Math.min(minValue, past.min),
                              Math.max(maxValue, past.max));
        }
    }

    protected static class Past
        implements HistoryMeasure.PastHistory<Long, Result, Recent, Past> {

        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private boolean noValue = true;

        @Override
        public void reset() {
            min = Long.MAX_VALUE;
            max = Long.MIN_VALUE;
            noValue = true;
        }

        @Override
        public void combine(Recent recent) {
            final long minValue = recent.min.get();
            final long maxValue = recent.max.get();
            noValue = (minValue > maxValue);
            min = Math.min(min, minValue);
            max = Math.max(max, maxValue);
        }

        @Override
        public void inherit(Past past) {
            min = Math.min(min, past.min);
            max = Math.max(max, past.max);
            noValue = past.noValue;
        }
    }

    /**
     * The min/max result.
     */
    public static class Result extends ObjectNodeSerializable {

        /** The default min. */
        public static final long DEFAULT_MIN = Long.MAX_VALUE;
        /** The default max. */
        public static final long DEFAULT_MAX = Long.MIN_VALUE;
        /** The default. */
        public static final Result DEFAULT =
            new Result(DEFAULT_MIN, DEFAULT_MAX);

        /** The field map. */
        public static final Map<String, Field<?, Result>> FIELDS =
            new HashMap<>();
        /** The min value. */
        public static final String MIN =
            LongField.create(FIELDS, "min", Result::getMin, DEFAULT_MIN);
        /** The max value. */
        public static final String MAX =
            LongField.create(FIELDS, "max", Result::getMin, DEFAULT_MAX);

        private final long min;
        private final long max;

        public Result(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public Result(JsonNode payload) {
            this(readField(FIELDS, payload, MIN),
                 readField(FIELDS, payload, MAX));
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        @Override
        public ObjectNode toJson() {
            return writeFields(FIELDS, this);
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
