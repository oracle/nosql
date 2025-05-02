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


/**
 * Used to summarize the latency info.
 *
 * Operations have parent/child types. Summarize the latency stats of the child
 * types to obtain the parent one.
 */
public class LatencyInfoSummarizer {

    private final PerfStatType perfStatType;
    private final long startMillis;
    private final long endMillis;
    private long requestCount;
    private long operationCount;
    private long min = Long.MAX_VALUE;
    private long max = 0;
    private long average;
    private long overflowCount;

    public LatencyInfoSummarizer(PerfStatType perfStatType,
                                 long startMillis,
                                 long endMillis) {
        this.perfStatType = perfStatType;
        this.startMillis = startMillis;
        this.endMillis = endMillis;
    }

    public void rollup(LatencyInfo childInfo) {
        final LatencyResult childResult = childInfo.getLatency();
        final long opCnt = childResult.getOperationCount();
        min = Math.min(min, childResult.getMin());
        max = Math.max(max, childResult.getMax());
        /* Compute with double to avoid overflow */
        average = (long) ((((double) average) * operationCount
                           + ((double) childResult.getAverage()) * opCnt)
                          / (operationCount + opCnt));
        requestCount += childResult.getRequestCount();
        operationCount += opCnt;
        overflowCount += childResult.getOverflowCount();
    }

    public LatencyInfo build() {
        final LatencyResult latencyResult =
            (operationCount == 0) ?
            new LatencyResult() :
            new LatencyResult(requestCount, operationCount,
                              min, max, average, overflowCount);
        return new LatencyInfo(
            perfStatType, startMillis, endMillis, latencyResult);
    }
}
