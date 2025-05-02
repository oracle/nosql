/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.txn;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.utilint.StatGroup;

import java.io.IOException;

/**
 * The GroupCommit component in TxnManager is responsible for optimizing fsync
 * operations by batching multiple transactions.
 * It has two implementations: GroupCommitMaster and GroupCommitReplica.
 * As their names suggest, TxnManager dynamically selects the appropriate
 * implementation based on the node's current state.
 *
 * The primary function of GroupCommit is to group a set number of transactions
 * that require immediate fsync operations and consolidate them into a single fsync.
 * This approach minimizes the number of IO operations.
 *
 * GroupCommit determines when to trigger a fsync operation using both of the following
 * criteria, depends on which one is satisfied earlier:
 * 1. A time threshold.
 * 2. A size threshold.
 */
public interface GroupCommit {


    /**
     * Returns true if group commits are enabled at the node.
     */
    boolean isEnabled();

    /**
     * Buffers a transaction's commit entry that calls for a sync
     * into the GroupCommit mechanism, flushing pending commits if needed.
     * Thread-safe for concurrent commit operations.
     *
     * @param nowNs          the current time in nanoseconds, used for timing thresholds.
     * @param txn            the transaction to be committed.
     * @param commitVLSN     the commitVLSN of the txn
     */
     void bufferCommit(long nowNs,
                         Txn txn,
                         long commitVLSN)
        throws DatabaseException;


    /**
     * Flushes and synchronizes pending transaction commits in the GroupCommit
     * mechanism. Typically, invoked periodically or when thresholds are exceeded.
     * Thread-safe and designed for concurrent environments.
     *
     * @param nowNs the current time in nanoseconds, used to determine if pending commits
     * should be flushed based on configured thresholds.
     */
    void flushPendingAcks(long nowNs) throws IOException;

    StatGroup getStats(StatsConfig config);

    void clearStats();
}
