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
 * @hidden
 *
 * Object represents interface of a collection of service agent metrics
 */
public interface ServiceAgentMetrics {

    /**
     * Returns the total number of processed requests
     *
     * @return the total number of processed requests
     */
    long getRequests();

    /**
     * Returns the total number of post response
     *
     * @return the total number of post response
     */
    long getResponses();

    /**
     * Returns the total number of received operations
     *
     * @return the total number of received operations
     */
    long getRecvOps();

    /**
     * Returns the total number of received put operations
     *
     * @return the total number of received put operations
     */
    long getPuts();

    /**
     * Returns the total number of received delete operations
     *
     * @return the total number of received delete operations
     */
    long getDels();

    /**
     * Returns the total number of received operations from source that are
     * successfully pushed to the target by the agent
     *
     * @return the total number of received operations from source that are
     * successfully pushed to the target by the agent
     */
    long getNumWins();

    /**
     * Returns the total number of received put operations from source that
     * are successfully pushed to the target by the agent
     *
     * @return the total number of received put operations from source that
     * are successfully pushed to the target by the agent
     */
    long getWinPuts();

    /**
     * Returns the total number of received delete operations from source that
     * are successfully pushed to the target by the agent
     *
     * @return the total number of received delete operations from source that
     * are successfully pushed to the target by the agent
     */
    long getWinDels();

    /**
     * Returns the total number of looped back put operations originated at
     * the local region.
     * @return the total number of looped back put operations originated at
     * the local region.
     */
    long getLoopbackPuts();

    /**
     * Returns the total number of looped back delete operations originated at
     * the local region.
     * @return the total number of looped back delete operations originated at
     * the local region.
     */
    long getLoopbackDels();

    /**
     * Returns the total bytes received from source
     *
     * @return the total bytes received from source
     */
    long getStreamBytes();

    /**
     * Returns the total bytes persisted at the target
     *
     * @return the total bytes persisted at the target
     */
    long getPersistStreamBytes();

    /**
     * Returns the total number of incompatible rows
     *
     * @return the total number of incompatible rows
     */
    long getIncompatibleRows();

    /**
     * Returns a map from region name to the timestamp of when the last message
     * was received from that region.
     *
     * @return map of per-region last message timestamps
     */
    Map<String, Long> getLastMessageMs();

    /**
     * Returns a map from region name to the original modification timestamp of
     * the last stream operation stored locally. For each remote region, the
     * timestamp is the minimum of shard modification times of all shards in
     * that region. For each shard, the timestamp is the original modification
     * time for the last stream operation from that shard that was stored
     * locally.
     *
     * @return map of per-region modification timestamps
     */
    Map<String, Long> getLastModificationMs();

    /**
     * Returns a map from region name to the total number of operations that
     * have completed writing to the target region.
     * @return a map from region name to complete write ops
     */
    Map<String, Long> getCompleteWriteOps();

    /**
     * Returns a map from region name to the region lag time in milliseconds.
     * For each remote region, its lag is computed as the maximum shard lag of
     * all shards in that region. For each shard, the lag is computed as the
     * difference between the timestamp of the last commit in that shard, and
     * the original modification time of last operation that filter has
     * processed.
     * <p>
     * The lag tells how far the XRegion Service is behind a remote region. For
     * example, when the service restarts, likely it is quite behind the remote
     * region, and the lagging will be diminishing when the service is catching
     * up.
     * @return a map from region name to the region lag time in milliseconds
     */
    @SuppressWarnings("deprecation")
    Map<String, MetricStats> getLaggingMs();

    /**
     * Returns a map from region name to the latency time in milliseconds.
     * For each operation from the remote region, its latency is computed as
     * the difference between the timestamp when the operation is made to
     * the target store and the timestamp when the operation is made at
     * original store.
     * @return a map from region name to latency time in milliseconds
     */
    @SuppressWarnings("deprecation")
    Map<String, MetricStats> getLatencyMs();
}
