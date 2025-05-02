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

/**
 * Aggregates the metrics associated with a KVS operation.
 */
public interface OperationMetrics {

    /**
     * Returns the name of the KVS operation associated with the metrics.
     */
    public String getOperationName();

    /**
     * Returns the number of operations that were executed as an int.
     *
     * @deprecated as of 19.2, see {@link #getTotalOpsLong}
     */
    @Deprecated
    public int getTotalOps();

    /**
     * Returns the number of operations that were executed.
     * <p>
     * For requests (API method calls) that involve a single operation (get,
     * put, etc.), one operation is counted per request.  For requests that
     * involve multiple operations (multiGet, multiDelete, execute), all
     * individual operations are counted.
     *
     * @since 19.2
     */
    public long getTotalOpsLong();

    /**
     * Returns the number of requests that were executed as an int.
     *
     * @deprecated as of 19.2, see {@link #getTotalRequestsLong}
     */
    @Deprecated
    public int getTotalRequests();

    /**
     * Returns the number of requests that were executed.
     * <p>
     * Only one request per API method call is counted, whether the request
     * involves a single operation (get, put, etc.) or multiple operations
     * (multiGet, multiDelete, execute).  For requests that involve a single
     * operation, this method returns the same value as {@link
     * #getTotalOpsLong}.
     *
     * @since 19.2
     */
    public long getTotalRequestsLong();

    /**
     * Returns the minimum request latency in milliseconds.
     */
    public int getMinLatencyMs();

    /**
     * Returns the maximum request latency in milliseconds.
     */
    public int getMaxLatencyMs();

    /**
     * Returns the average request latency in milliseconds.
     */
    public float getAverageLatencyMs();

    /**
     * @hidden
     * Returns the 95th percentile request latency in milliseconds.
     */
    public int get95thLatencyMs();

    /**
     * @hidden
     * Returns the 99th percentile request latency in milliseconds.
     */
    public int get99thLatencyMs();

    /**
     * Returns the request latency stats in nanoseconds.
     *
     * @hidden Until we make the async metrics public
     */
    @SuppressWarnings("deprecation")
    public MetricStats getLatencyStatsNanos();
}
