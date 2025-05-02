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

package oracle.kv.impl.measurement;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import oracle.kv.impl.async.perf.MetricStatsImpl;
import oracle.nosql.common.sklogger.measure.LatencyElement;

import com.google.gson.JsonObject;

/**
 * Represents a latency result.
 *
 * <p>This class is intended to replace the je Latency class for two reasons:
 * (1) the precision requirement changing from millis to nanos and (2) the data
 * type requirement the average value changing from float to long.
 *
 * <p>This class is also an adaptor to the LatencyElement.Result class. Ideally
 * we would have some kind of unified interface for
 * MetricStatsImpl/LatencyResult/LatencyElement.Result. To do that we need to
 * understand the desired relationship between serializable and json. TODO for
 * future.
 */
public class LatencyResult extends MetricStatsImpl {

    private static final long serialVersionUID = 1L;

    private final long requestCount;
    private final long overflowCount;
    /*
     * Indicates whether the percntile values are not valid. The percentile
     * values may not be valid if this is a combined result.
     */
    private final boolean hasValidPercentile;

    public LatencyResult(LatencyElement.Result other) {
        this(other.getRequestCount(),
             other.getOperationCount(),
             other.getMin(),
             other.getMax(),
             other.getAverage(),
             other.getPercent95(),
             other.getPercent99(),
             other.getOverflowCount());
    }

    public LatencyResult(long requestCount,
                         long operationCount,
                         long min,
                         long max,
                         long average,
                         long percent95,
                         long percent99,
                         long overflowCount) {
        this(requestCount, operationCount,
             min, max, average,
             percent95, percent99, overflowCount, true);
    }

    public LatencyResult(long requestCount,
                         long operationCount,
                         long min,
                         long max,
                         long average,
                         long overflowCount) {
        this(requestCount, operationCount,
             min, max, average, -1, -1, overflowCount, false);
    }

    public LatencyResult() {
        this(0, 0, 0, 0, 0, 0, 0, 0);
    }

    public LatencyResult(long requestCount,
                         long operationCount,
                         long min,
                         long max,
                         long average,
                         long percent95,
                         long percent99,
                         long overflowCount,
                         boolean hasValidPercentile) {
        super(operationCount, min, max, average,
              percent95, percent99);
        this.requestCount = requestCount;
        this.overflowCount = overflowCount;
        this.hasValidPercentile = hasValidPercentile;
    }

    public long getOperationCount() {
        return getCount();
    }

    public long getRequestCount() {
        return requestCount;
    }

    public long getOverflowCount() {
        return overflowCount;
    }

    public boolean hasValidPercentile() {
        return hasValidPercentile;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject result = super.toJson();
        if (!hasValidPercentile) {
            result.addProperty("95th", "NA");
            result.addProperty("99th", "NA");
        }
        return result;
    }

    public JsonObject toJsonMillis() {
        final JsonObject result = new JsonObject();
        result.addProperty("count", getCount());
        result.addProperty("min", NANOSECONDS.toMillis(getMin()));
        result.addProperty("max", NANOSECONDS.toMillis(getMax()));
        result.addProperty("avg", NANOSECONDS.toMicros(getAverage()) / 1000.0);
        if (!hasValidPercentile) {
            result.addProperty("95th", "NA");
            result.addProperty("99th", "NA");
        } else {
            result.addProperty("95th", NANOSECONDS.toMillis(getPercent95()));
            result.addProperty("99th", NANOSECONDS.toMillis(getPercent99()));
        }
        return result;
    }
}

