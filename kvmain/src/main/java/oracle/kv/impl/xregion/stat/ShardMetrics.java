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

package oracle.kv.impl.xregion.stat;

import oracle.kv.impl.pubsub.StreamOpMetrics;
import oracle.kv.impl.util.FormatUtils;
import oracle.nosql.common.sklogger.measure.LongPercentileElement;
import oracle.nosql.common.sklogger.measure.LongValueStats;

/**
 * Object represents the statistics of a shard from a remote region
 */
public class ShardMetrics {

    /** timestamp when last msg is received */
    private volatile long shardLastMsgTime = 0;
    /** modification time of the last received operation */
    private volatile long shardLastModTime = 0;
    /** latency metrics */
    private final LongPercentileElement latencyMs = new LongPercentileElement();

    public ShardMetrics() {
    }

    long getLastMsgTime() {
        return shardLastMsgTime;
    }

    long getLastModTime() {
        return shardLastModTime;
    }

    synchronized StreamOpMetrics getLatencyMetrics() {
        /* do not clear the stats in query */
        final LongValueStats result = latencyMs.obtain(false);
        return new StreamOpMetrics(result);
    }

    public synchronized void setLastMsgTime(long val) {
        shardLastMsgTime = Math.max(shardLastMsgTime, val);
    }

    public synchronized void setLastModTime(long val) {
        shardLastModTime = Math.max(shardLastModTime, val);
    }

    public synchronized void addLatency(long val) {
        latencyMs.observe(val);
    }

    @Override
    public String toString() {
        return "\narrival time of last msg=" +
               FormatUtils.formatTimeMillis(shardLastMsgTime) + "\n" +
               "mod. time of last msg=" +
               FormatUtils.formatTimeMillis(shardLastModTime) + "\n" +
               "latency=[" + latencyMs + "]\n";
    }
}
