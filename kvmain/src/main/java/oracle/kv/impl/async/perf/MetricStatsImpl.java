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

package oracle.kv.impl.async.perf;

import java.io.Serializable;

import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.LongValueStats;

import com.google.gson.JsonObject;

@SuppressWarnings("deprecation")
public class MetricStatsImpl
    implements oracle.kv.stats.MetricStats, Serializable {

    private static final long serialVersionUID = 1L;

    private final long count;
    private final long min;
    private final long max;
    private final long avg;
    private final long percent95;
    private final long percent99;

    public MetricStatsImpl(LongValueStats result) {
        this(result.getCount(),
             result.getMin(),
             result.getMax(),
             result.getAverage(),
             result.getPercent95(),
             result.getPercent99());
    }

    public MetricStatsImpl(LatencyElement.Result result) {
        this(result.getOperationCount(),
             result.getMin(),
             result.getMax(),
             result.getAverage(),
             result.getPercent95(),
             result.getPercent99());
    }

    public MetricStatsImpl(long count,
                           long min,
                           long max,
                           long average,
                           long percent95,
                           long percent99) {
        this.count = count;
        this.min = min;
        this.max = max;
        this.avg = average;
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
    public String toString() {
        return toJson().toString();
    }

    public JsonObject toJson() {
        final JsonObject object = new JsonObject();
        object.addProperty("count", count);
        object.addProperty("min", min);
        object.addProperty("max", max);
        object.addProperty("avg", avg);
        object.addProperty("95th", percent95);
        object.addProperty("99th", percent99);
        return object;
    }
}
