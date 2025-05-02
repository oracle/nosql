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

import java.io.Serializable;

import oracle.kv.KVStore;

/**
 * The metrics associated with a node in the KVS.
 */
public interface NodeMetrics extends Serializable {

    /**
     * Returns the internal name associated with the node. It's unique across
     * the KVStore.
     */
    public String getNodeName();

    /**
     * Returns the zone that hosts the node.
     *
     * @deprecated replaced by {@link #getZoneName}
     */
    @Deprecated
    public String getDataCenterName();

    /**
     * Returns the zone that hosts the node.
     */
    public String getZoneName();

    /**
     * Returns true is the node is currently active, that is, it's reachable
     * and can service requests.
     */
    public boolean isActive();

    /**
     * Returns true if the node is currently a master.
     */
    public boolean isMaster();

    /**
     * Returns the number of requests that were concurrently active for this
     * node at this KVS client.
     */
    public int getMaxActiveRequestCount();

    /**
     * Returns the total number of requests processed by the node.
     *
     * <p>The stats reflects the value computed since the last time it is
     * obtained and cleared with respect to a watcher name. See {@link
     * KVStore#getStats(String, boolean)}.
     */
    public long getRequestCount();

    /**
     * Returns the number of requests that were tried at this node but did not
     * result in a successful response.
     *
     * <p>The stats reflects the value computed since the last time it is
     * obtained and cleared with respect to a watcher name. See {@link
     * KVStore#getStats(String, boolean)}.
     */
    public long getFailedRequestCount();

    /**
     * Returns the trailing average latency (in ms) over all requests made to
     * this node.
     * <p>
     * Note that since this is a trailing average it's not cleared when the
     * statistics are cleared via the {@link KVStore#getStats(boolean)} method.
     */
    public int getAvLatencyMs();

    /**
     * Returns the average latency in nanoseconds over requests made to this
     * node.
     *
     * <p>The stats reflects the value computed since the last time it is
     * obtained and cleared with respect to a watcher name. See {@link
     * KVStore#getStats(String, boolean)}.
     */
    public long getAverageLatencyNanos();
}
