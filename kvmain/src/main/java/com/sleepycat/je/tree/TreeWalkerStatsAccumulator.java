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

package com.sleepycat.je.tree;

/**
 * Accumulates stats about a tree during tree walking.
 */
public interface TreeWalkerStatsAccumulator {
    public void processIN(IN node, Long nid, int level);

    public void processBIN(BIN node, Long nid, int level);

    public void incrementLNCount();

    public void incrementDeletedLNCount();
}
