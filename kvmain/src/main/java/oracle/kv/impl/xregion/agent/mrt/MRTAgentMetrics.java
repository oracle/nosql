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

package oracle.kv.impl.xregion.agent.mrt;

import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.xregion.agent.BaseRegionAgentMetrics;

/**
 * Object represents the metrics of region agent for multi-region table (MRT)
 */
public class MRTAgentMetrics extends BaseRegionAgentMetrics {

    /**
     * Total number of operations queued to keep operations on the same key
     * ordered properly.
     */
    private final AtomicLong nOpsQueued;

    /* TODO: Break out successful and failed checkpoints */
    /**
     * Number of checkpoints performed.
     */
    private final AtomicLong nCheckpoints;

    public MRTAgentMetrics(String sourceRegion, String targetRegion) {
        super(sourceRegion, targetRegion);
        nOpsQueued = new AtomicLong(0);
        nCheckpoints = new AtomicLong(0);
    }

    /* -------------- */
    /* Public Getters */
    /* -------------- */

    /**
     * Returns the total number of operations queued to keep operations on the
     * same key ordered properly.
     *
     * @return the total number of operations queued
     */
    public long getNumOpsQueued() {
        return nOpsQueued.get();
    }

    /**
     * Gets the number of checkpoints performed.
     *
     * @return the number of checkpoints performed
     */
    long getNumCheckpoints() {
        return nCheckpoints.get();
    }

    /* ------- */
    /* Setters */
    /* ------- */

    /**
     * Increments the total number of operations queued.
     */
    void incrNumOpsQueued() {
        nOpsQueued.incrementAndGet();
    }

    /**
     * Increments the number of checkpoints.
     */
    void incrNumCheckpoints() {
        nCheckpoints.incrementAndGet();
    }

    public String streamStat() {
        return "# ops=" + getTotalStreamOps() + "\n" +
               "# puts=" + getTotalStreamPuts() + "\n" +
               "# deletes=" + getTotalStreamDels() + "\n" +
               "# persisted ops=" + getTotalWinOp() + "\n" +
               "# persisted puts=" + getTotalWinPut() + "\n" +
               "# persisted deletes=" + getTotalWinDel() + "\n" +
               "# ops queued=" + getNumOpsQueued() + "\n" +
               "# checkpoints=" + getNumCheckpoints() + "\n";
    }
}
