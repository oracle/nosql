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
import java.util.function.Function;
import java.util.stream.IntStream;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;

/**
 * The abstract base class for implementing statistics, i.e., min, max,
 * average, 95th and 99th of a long value.
 */
public abstract class LongValueStatsBase extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, LongValueStatsBase>> FIELDS =
        new HashMap<>();
    /** The min value, excluding the overflow occurrences. */
    public static final String MIN =
        LongField.create(FIELDS, "min", LongValueStatsBase::getMin);
    /** The max value, excluding the overflow occurrences. */
    public static final String MAX =
        LongField.create(FIELDS, "max", LongValueStatsBase::getMax);
    /** The average value, excluding the overflow occurrences. */
    public static final String AVG =
        LongField.create(FIELDS, "avg", LongValueStatsBase::getAverage);
    /** The 95th percentile value, excluding the overflow occurrences. */
    public static final String PERCENT95 =
        LongField.create(FIELDS, "95th", LongValueStatsBase::getPercent95);
    /** The 99th percentile value, excluding the overflow occurrences. */
    public static final String PERCENT99 =
        LongField.create(FIELDS, "99th", LongValueStatsBase::getPercent99);
    /** The overflow count. */
    public static final String OVERFLOW =
        LongField.create(
            FIELDS, "overflowCount", LongValueStatsBase::getOverflowCount);

    private final long min;
    private final long max;
    private final long average;
    private final long percent95;
    private final long percent99;
    private final long overflowCount;

    protected LongValueStatsBase(HistogramSummary histogramSummary) {
        this(histogramSummary.min,
             histogramSummary.max,
             histogramSummary.average,
             histogramSummary.percent95,
             histogramSummary.percent99,
             histogramSummary.overflowCount);
    }

    public LongValueStatsBase(LongValueStatsBase other) {
        this(other.getMin(),
             other.getMax(),
             other.getAverage(),
             other.getPercent95(),
             other.getPercent99(),
             other.getOverflowCount());
    }

    public LongValueStatsBase(
        long min,
        long max,
        long average,
        long percent95,
        long percent99,
        long overflowCount)
    {
        this.min = min;
        this.max = max;
        this.average = average;
        this.percent95 = percent95;
        this.percent99 = percent99;
        this.overflowCount = overflowCount;
    }

    public LongValueStatsBase(JsonNode payload) {
        this(readField(FIELDS, payload, MIN),
             readField(FIELDS, payload, MAX),
             readField(FIELDS, payload, AVG),
             readField(FIELDS, payload, PERCENT95),
             readField(FIELDS, payload, PERCENT99),
             readField(FIELDS, payload, OVERFLOW));
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public long getAverage() {
        return average;
    }

    public long getPercent95() {
        return percent95;
    }

    public long getPercent99() {
        return percent99;
    }

    public long getOverflowCount() {
        return overflowCount;
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

    /**
     * Computes the stats based on a histogram and returns an long array of the
     * value in following order: count, min, max, average, 95th, 99th,
     * overflowCount.
     *
     * <p>The histogram must have a overflow bucket.
     *
     * <p>The upper bound value of the last bucket must be Long.MAX_VALUE. The
     * count in that bucket is excluded when computing average/max/95th/99th.
     * The count is put as the overflowCount.
     *
     * @throws ArithmeticException if computation overflows even after
     * excluding the overflow bucket.
     */
    protected static HistogramSummary computeStats(
        long[] histogram,
        Function<Integer, Long> upperBoundFunction)
    {
        final long defaultValue = LongField.DEFAULT;
        final long count = Arrays.stream(histogram).reduce(0, Math::addExact);
        final long lastValue = upperBoundFunction.apply(histogram.length - 1);
        if (lastValue != Long.MAX_VALUE) {
            throw new IllegalStateException(String.format(
                "A histogram for computing %s must have a overflow bucket, " +
                "current last value=%s",
                LongValueStats.class.getSimpleName(), lastValue));
        }
        final long overflowCount = histogram[histogram.length - 1];
        final long normalCount = count - overflowCount;

        /* Deal with special cases first */
        if ((count == 0) || (count == overflowCount)) {
            return new HistogramSummary (
                0, defaultValue, defaultValue,
                defaultValue, defaultValue, defaultValue, overflowCount);
        }
        if (count == 1) {
            final long only = IntStream.range(0, histogram.length).
                filter((i) -> histogram[i] != 0).
                mapToLong((i) -> upperBoundFunction.apply(i)).
                findFirst().orElse(defaultValue);
            return new HistogramSummary(
                1, only, only, only, only, only, overflowCount);
        }

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        double sum = 0;
        long percent95 = 0;
        long percent99 = 0;

        final long percent95Count = Math.round(normalCount * .95);
        final long percent99Count = Math.round(normalCount * .99);
        long cnt = 0;
        for (int i = 0; i < histogram.length - 1; ++i) {
            if (histogram[i] == 0) {
                continue;
            }
            final long n = histogram[i];
            final long value = upperBoundFunction.apply(i);

            if ((cnt < percent95Count) && (cnt + n >= percent95Count)) {
                percent95 = value;
            }
            if ((cnt < percent99Count) && (cnt +n >= percent99Count)) {
                percent99 = value;
            }

            cnt += n;

            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += ((double) value) * n;
        }

        return new HistogramSummary(
            count, min, max, (long) (sum / normalCount),
            percent95, percent99, overflowCount);
    }

    /**
     * A convenient class of the result of computeStats.
     *
     * There is a bit duplicated code as this class looks very similar to the
     * parent class LongValueStatsBase itself. I need this convenient class,
     * however, because we want to give a different serialization name to the
     * count field in one of the subclass.
     */
    protected static class HistogramSummary {

        public long count;
        public long min;
        public long max;
        public long average;
        public long percent95;
        public long percent99;
        public long overflowCount;

        private HistogramSummary(long count,
                                 long min,
                                 long max,
                                 long average,
                                 long percent95,
                                 long percent99,
                                 long overflowCount) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.average = average;
            this.percent95 = percent95;
            this.percent99 = percent99;
            this.overflowCount = overflowCount;
        }
    }
}
