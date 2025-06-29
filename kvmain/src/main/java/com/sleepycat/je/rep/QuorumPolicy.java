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

package com.sleepycat.je.rep;

import com.sleepycat.je.EnvironmentFailureException;

/**
 * The quorum policy determine the number of nodes that must participate to
 * pick the winner of an election, and therefore the master of the group.
 * The default quorum policy during the lifetime of the group is
 * QuorumPolicy.SIMPLE_MAJORITY. The only time that the application needs to
 * specify a specific quorum policy is at node startup time, by passing one
 * to the {@link ReplicatedEnvironment} constructor.
 *
 * <p>Note that {@link NodeType#SECONDARY} nodes are not counted as part of
 * master election quorums.
 */
public enum QuorumPolicy {

    /**
     * All participants are required to vote.
     */
    ALL,

     /**
      *  A simple majority of participants is required to vote.
      */
    SIMPLE_MAJORITY;

    /**
     * Returns the minimum number of nodes to needed meet the quorum policy.
     *
     * @param groupSize the number of election participants in the replication
     *        group
     *
     * @return the number of nodes that are needed for a quorum for a group
     *         with {@code groupSize} number of election participants
     */
    public int quorumSize(int groupSize) {
        switch (this) {
            case ALL:
                return groupSize;

            case SIMPLE_MAJORITY:
                return (groupSize / 2 + 1);

            default:
                throw EnvironmentFailureException.unexpectedState
                    ("Unknown quorum:" + this);
        }
    }
}
