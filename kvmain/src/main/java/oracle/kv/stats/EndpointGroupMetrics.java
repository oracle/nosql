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

import java.util.List;

/**
 * The metrics associated with the async endpoint group.
 *
 * This interface is deprecated. Use Json instead.
 *
 * @hidden
 * @deprecated since 22.3
 */
@Deprecated
public interface EndpointGroupMetrics {

    /**
     * Returns a list of metrics of endpoints connecting to nodes.
     *
     * @return the list
     */
    List<EndpointMetrics> getEndpointMetricsList();

    /**
     * Returns the metrics for the endpoint identified by the given string
     * (specifying the IP address).
     *
     * @param address the remote IP address identifing the endpoint
     * @return the endpoint metrics, {@code null} if no such endpoint
     */
    EndpointMetrics getEndpointMetrics(String address);

    /**
     * Returns a formatted stats string which list stats for all endpoints.
     *
     * @return a formatted stats string
     */
    String getFormattedStats();

    /**
     * Returns a summarized stats string of endpoint metrics which describes
     * the number of endpoints and median stats.
     *
     * @return a summarized stats string
     */
    String getSummarizedStats();
}

