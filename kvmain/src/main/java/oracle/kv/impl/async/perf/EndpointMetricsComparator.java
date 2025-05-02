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

import java.util.Comparator;
import java.util.List;

/**
 * The comparator to sort endpoint metrics.
 *
 * Note: this comparator imposes orderings that are inconsistent with equals.
 */
@SuppressWarnings("deprecation")
public class EndpointMetricsComparator
    implements Comparator<oracle.kv.stats.EndpointMetrics> {

    private final double maxAvgFinishedDialogLatency;

    public EndpointMetricsComparator(List<EndpointMetricsImpl> mlist) {
        this.maxAvgFinishedDialogLatency =
            Math.max(mlist.stream().
                     mapToDouble(
                         (m) ->
                         m.getFinishedDialogLatencyNanos().getAverage()).
                     max().orElse(1),
                     1 /* avoid devide by zero issue */);
    }

    @Override
    public int compare(oracle.kv.stats.EndpointMetrics m1,
                       oracle.kv.stats.EndpointMetrics m2) {
        final double delta = score(m1) - score(m2);
        return (delta < 0) ? -1 : ((delta == 0) ? 0 : 1);
    }

    /**
     * Computes the score of the endpoint metrics.
     *
     * <p>The score considers the abort rate and the latency of finished
     * dialogs.
     *
     * <p>Abnormal endpoints have lower scores, so that when sorted they are on
     * top.
     *
     * <p>Scaled the score so that it is always within the range [0, 1].
     *
     * <p>Adds more weight to the abort rate, so that endpoints with half of
     * the dialogs aborted has a score slightly lower than the one with the
     * highest latency.
     */
    public double score(oracle.kv.stats.EndpointMetrics m) {
        final long finishedCount =
            m.getFinishedDialogLatencyNanos().getCount();
        final long abortedCount =
            m.getAbortedDialogLatencyNanos().getCount();
        final double abortScore =
            (abortedCount + finishedCount == 0) ?
            0 : ((double) abortedCount) / (abortedCount + finishedCount);
        final double finishedLatencyScore =
            m.getFinishedDialogLatencyNanos().getAverage() /
            maxAvgFinishedDialogLatency;
        return 1 - (2.0f / 3 * abortScore + 1.0f / 3 * finishedLatencyScore);
    }
}
