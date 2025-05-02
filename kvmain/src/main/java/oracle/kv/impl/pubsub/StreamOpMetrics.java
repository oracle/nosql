/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.pubsub;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.sklogger.measure.LongValueStats;

/**
 * Object represents stream operation metrics
 */
@SuppressWarnings("deprecation")
public class StreamOpMetrics
    implements oracle.kv.stats.MetricStats, Serializable
{
    private static final long serialVersionUID = 1L;

    private final long min;
    private final long max;
    private final long avg;
    private final long count;

    /** 95 and 99 percentile metrics */
    private final long percent95;
    private final long percent99;

    /**
     * Creates a stream metrics from latency statistics
     */
    public StreamOpMetrics(LongValueStats result) {
        this(result.getMin(), result.getMax(), result.getAverage(),
             result.getCount(), result.getPercent95(), result.getPercent99());
    }

    public StreamOpMetrics() {
        this(-1, -1, 0, 0, 0, 0);
    }

    private StreamOpMetrics(long min, long max, long avg, long count,
                            long percent95, long percent99) {
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.count = count;
        this.percent95 = percent95;
        this.percent99 = percent99;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public long getAverage() {
        return avg;
    }

    @Override
    public long getPercent95() {
        return percent95;
    }

    @Override
    public long getPercent99() {
        return percent99;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StreamOpMetrics)) {
            return false;
        }
        final StreamOpMetrics other = (StreamOpMetrics) obj;
        return min == other.min &&
               max == other.max &&
               avg == other.avg &&
               count == other.count &&
               getPercent95() == other.getPercent95() &&
               getPercent99() == other.getPercent99();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(min) +
               Long.hashCode(max) +
               Long.hashCode(avg) +
               Long.hashCode(count) +
               Long.hashCode(getPercent95()) +
               Long.hashCode(getPercent99());
    }

    @Override
    public String toString() {
        return "min=" + min + ", max=" + max + ", count=" + count +
               ", average=" + avg +
               ", 95percent=" + getPercent95() +
               ", 99percent=" + getPercent99();

    }

    public long getSum() {
        return avg * count;
    }

    public static StreamOpMetrics merge(Collection<StreamOpMetrics> oms) {
        if (oms == null || oms.isEmpty()) {
            return new StreamOpMetrics();
        }
        final long min = oms.stream().mapToLong(StreamOpMetrics::getMin)
                            .min().orElse(-1);
        final long max = oms.stream().mapToLong(StreamOpMetrics::getMax)
                            .max().orElse(-1);
        final long count = oms.stream().mapToLong(StreamOpMetrics::getCount).
                              sum();
        final long sum = oms.stream().mapToLong(StreamOpMetrics::getSum).sum();
        final long avg = (count == 0) ? -1 : (sum / count);
        /* build a map of value and weight */
        final Map<Long, Long> map95 = new HashMap<>();
        final Map<Long, Long> map99 = new HashMap<>();
        oms.forEach(o -> {
            final long weight = o.getCount();
            map95.put(o.getPercent95(), weight);
            map99.put(o.getPercent99(), weight);
        });
        /* set the merged percentile */
        final long percent95 = computeWeightedAvg(map95);
        final long percent99 = computeWeightedAvg(map99);
        return new StreamOpMetrics(min, max, avg, count, percent95, percent99);
    }

    /**
     * This is to approximately aggregate percentile statistics. It works
     * reasonable well when each percentile has similar distribution of values
     * and similar counts.
     */
    private static long computeWeightedAvg(Map<Long, Long> map) {
        long totalWt = 0;
        long total = 0;
        for (Map.Entry<Long, Long> e : map.entrySet()) {
            final long val = e.getKey();
            final long weight = e.getValue();
            total += val * weight;
            totalWt += weight;
        }
        if (totalWt == 0) {
            /* no count, no stat */
            return 0;
        }
        return total / totalWt;
    }
}
