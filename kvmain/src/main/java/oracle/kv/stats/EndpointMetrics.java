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

package oracle.kv.stats;

import java.util.Map;

/**
 * The metrics associated with an async endpoint.
 *
 * This interface is deprecated. Use Json instead.
 *
 * @hidden Until we make the async metrics public
 * @deprecated since 22.3
 */
@Deprecated
public interface EndpointMetrics {

    /**
     * Returns the name of the metrics.
     * @return the name
     */
    String getName();

    /**
     * Returns the average number of dialogs started per second.
     * @return the average number
     */
    double getDialogStartThroughput();

    /**
     * Returns the average number of dialogs dropped per second.
     * @return the average number
     */
    double getDialogDropThroughput();

    /**
     * Returns the average number of dialogs finished per second.
     * @return the average number
     */
    double getDialogFinishThroughput();

    /**
     * Returns the average number of dialogs aborted per second.
     * @return the average number
     */
    double getDialogAbortThroughput();

    /**
     * Returns the latency stats of finished dialogs.
     * @return the stats
     */
    MetricStats getFinishedDialogLatencyNanos();

    /**
     * Returns the latency stats of aborted dialogs.
     * @return the stats
     */
    MetricStats getAbortedDialogLatencyNanos();

    /**
     * Returns the stats of dialog concurrency.
     * @return the stats
     */
    MetricStats getDialogConcurrency();

    /**
     * Returns the map of a dialog latency breaking down into events latency.
     */
    Map<String, MetricStats> getEventLatencyNanosMap();
}
