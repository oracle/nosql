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

package oracle.kv.impl.monitor.views;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.Serializable;

import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FormatUtils;

import com.google.gson.JsonObject;


/**
 * The aggregated interval and cumulative latencies for a rep node, for
 * all operations.
 */
public class PerfEvent implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String eol = System.getProperty("line.separator");

    private final ResourceId resourceId;

    /*
     * Interval and cumulative stats, aggregated for all user single-operation
     * kvstore operations. Note that these are never null.
     */
    private final LatencyInfo singleInt;
    private final LatencyInfo singleCum;

    /*
     * Interval and cumulative stats, aggregated for all user multi-operation
     * kvstore operations. Note that these are never null.
     */
    private final LatencyInfo multiInt;
    private final LatencyInfo multiCum;

    private final boolean needsAlert;

    private final boolean singleCeilingExceeded;
    private final boolean singleFloorExceeded;
    private final boolean multiCeilingExceeded;
    private final boolean multiFloorExceeded;

    public PerfEvent(ResourceId resourceId,
                     LatencyInfo singleInt,
                     LatencyInfo singleCum,
                     int latencyCeiling,
                     int throughputFloor,
                     LatencyInfo multiInt,
                     LatencyInfo multiCum) {
        this.singleInt = singleInt;
        this.singleCum = singleCum;
        this.multiInt = multiInt;
        this.multiCum = multiCum;
        this.resourceId = resourceId;

        assert singleInt != null;
        assert singleCum != null;
        assert multiInt != null;
        assert multiCum != null;

        this.singleCeilingExceeded =
            latencyCeilingExceeded(latencyCeiling, singleInt);
        this.singleFloorExceeded =
            throughputFloorExceeded(throughputFloor, singleInt);

        this.multiCeilingExceeded =
            latencyCeilingExceeded(latencyCeiling, multiInt);
        this.multiFloorExceeded =
            throughputFloorExceeded(throughputFloor, multiInt);

        if (singleCeilingExceeded) {
            needsAlert = true;
        } else if (singleFloorExceeded) {
            needsAlert = true;
        } else if (multiCeilingExceeded) {
            needsAlert = true;
        } else if (multiFloorExceeded) {
            needsAlert = true;
        } else {
            needsAlert = false;
        }
    }

    public static boolean latencyCeilingExceeded(int ceiling,
                                                 LatencyInfo stat) {
        return ((ceiling > 0) && (stat.getLatency().getAverage() > ceiling));
    }

    public static boolean throughputFloorExceeded(int floor,
                                                  LatencyInfo stat) {
        /*
         * If there are no multi (or single) ops, then the throughput will
         * default to 0. Because of this, unless a 0 throughput is excluded
         * from the floor test performed below (throughput > 0), an ALERT will
         * ALWAYS be recorded; even when an ALERT is not intended/expected.
         * Thus, only positive throughputs are used in the comparison below.
         */
        return ((floor > 0) && (stat.getThroughputPerSec() > 0) &&
                (stat.getThroughputPerSec() < floor));
    }

    public static final String HEADER = eol +
        "                                        -------------------------- Interval --------------------------        --------------------------- Cumulative ---------------------------" +
        eol +
        "Resource   Time yy-mm-dd UTC Op Type    TotalOps PerSec    TotalReq    Min    Max    Avg   95th   99th        TotalOps PerSec        TotalReq    Min    Max    Avg   95th   99th";
    //   *234567890 yy-mm-dd xx:xx:xx 1234567 12345678901 123456 12345678901 123456 123456 123456 123456 123456 123456789012345 123456 123456789012345 123456 123456 123456 123456 123456


    /**
     * Print the single/multi interval/cumulative stats in a way suitable for
     * the .perf file.
     */
    public String getColumnFormatted() {

        final StringBuilder sb = new StringBuilder();
        if (singleInt.getLatency().getOperationCount() > 0) {
            sb.append(getFormatted("single", singleInt, singleCum));

            /* Identify the type of ALERT if applicable */
            if (singleCeilingExceeded) {
                sb.append(" - latency");
                if (singleFloorExceeded) {
                    sb.append(",throughput");
                }
            } else if (singleFloorExceeded) {
                sb.append(" - throughput");
            }
        }

        if (multiInt.getLatency().getOperationCount() > 0) {
            if (sb.length() > 0) {
                sb.append(eol);
            }
            sb.append(getFormatted("multi", multiInt, multiCum));

            /* Identify the type of ALERT if applicable */
            if (multiCeilingExceeded) {
                sb.append(" - latency");
                if (multiFloorExceeded) {
                    sb.append(",throughput");
                }
            } else if (multiFloorExceeded) {
                sb.append(" - throughput");
            }
        }

        return sb.toString();
    }

    /**
     * Format one pair of interval/cumulative stats in a way suitable for
     * adding to the .perf file.
     */
    private String getFormatted(String label, // single or multi
                                LatencyInfo intInfo,
                                LatencyInfo cumInfo) {
        final LatencyResult intLat = intInfo.getLatency();
        final LatencyResult cumLat = cumInfo.getLatency();

        /*
         * Be sure to use UTC timezone, to match logging output and timestamps
         * in the .stat file.
         */
        String formatted =
            String.format
            ("%-10s %17s %7s %11d %6d %11d %6d %6d %6d %6d %6d %15d %6d %15d %6d %6d %6d %6d %6d",
             resourceId,
             FormatUtils.formatPerfTime(intInfo.getEnd()),
             label,
             intLat.getOperationCount(),
             intInfo.getThroughputPerSec(),
             intLat.getRequestCount(),
             NANOSECONDS.toMillis(intLat.getMin()),
             NANOSECONDS.toMillis(intLat.getMax()),
             NANOSECONDS.toMillis(intLat.getAverage()),
             NANOSECONDS.toMillis(intLat.getPercent95()),
             NANOSECONDS.toMillis(intLat.getPercent99()),
             cumLat.getOperationCount(),
             cumInfo.getThroughputPerSec(),
             cumLat.getRequestCount(),
             NANOSECONDS.toMillis(cumLat.getMin()),
             NANOSECONDS.toMillis(cumLat.getMax()),
             NANOSECONDS.toMillis(cumLat.getAverage()),
             NANOSECONDS.toMillis(cumLat.getPercent95()),
             NANOSECONDS.toMillis(cumLat.getPercent99()));

        if (needsAlert) {
            formatted += " ALERT";
        }

        return formatted;
    }

    @Override
    public String toString() {
        /* Single interval, non-cummulative metrics */
        String value = resourceId + " interval=" + singleInt;
        if (singleCeilingExceeded || singleFloorExceeded) {
            value += " ALERT";
        }

        /* Single interval, cummulative metrics */
        value += " cumulative=" + singleCum;

        /* Multi interval, non-cummulative metrics */
        value += " multiOpsInterval=" + multiInt;
        if (multiCeilingExceeded || multiFloorExceeded) {
            value += " ALERT";
        }

        /* Multi interval, cummulative metrics */
        value += " multiOpsCumulative=" + multiCum;

        return value;
    }

    public JsonObject toJson() {
        final JsonObject result = new JsonObject();
        result.addProperty("resourceId", resourceId.toString());

        final JsonObject singleIntResult = singleInt.toJson();
        singleIntResult.addProperty(
            "ALERT", singleCeilingExceeded || singleFloorExceeded);
        result.add("interval", singleIntResult);

        result.add("cumulative", singleCum.toJson());

        final JsonObject multiIntResult = multiInt.toJson();
        multiIntResult.addProperty(
            "ALERT", multiCeilingExceeded || multiFloorExceeded);
        result.add("multiOpsInterval", multiIntResult);

        result.add("multiOpsCumulative", multiCum.toJson());

        return result;
    }

    public ResourceId getResourceId() {
        return resourceId;
    }

    /** Never returns null. */
    public LatencyInfo getSingleInt() {
        return singleInt;
    }

    /** Never returns null. */
    public LatencyInfo getSingleCum() {
        return singleCum;
    }

    /** Never returns null. */
    public LatencyInfo getMultiInt() {
        return multiInt;
    }

    /** Never returns null. */
    public LatencyInfo getMultiCum() {
        return multiCum;
    }

    public boolean needsAlert() {
        return needsAlert;
    }

    public long getChangeTime() {
        if (singleInt.getLatency().getOperationCount() != 0) {
            return singleInt.getEnd();
        }

        if (multiInt.getLatency().getOperationCount() != 0) {
            return multiInt.getEnd();
        }

        return 0;
    }
}
