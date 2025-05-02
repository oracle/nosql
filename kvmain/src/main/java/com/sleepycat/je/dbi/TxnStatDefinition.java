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

package com.sleepycat.je.dbi;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for JE transaction statistics.
 */
public class TxnStatDefinition {

    public static final StatDefinition TXN_ACTIVE =
        new StatDefinition("nActive", 
                           "Number of transactions that are currently " + 
                           "active.");

    public static final StatDefinition TXN_BEGINS =
        new StatDefinition("nBegins", 
                           "Number of transactions that have begun.");

    public static final StatDefinition TXN_ABORTS =
        new StatDefinition("nAborts",
                           "Number of transactions that have aborted.");

    public static final StatDefinition TXN_COMMITS =
        new StatDefinition("nCommits",
                           "Number of transactions that have committed.");

    public static final StatDefinition TXN_ACTIVE_TXNS =
        new StatDefinition("activeTxns", 
                           "Array of active transactions. Each element of " +
                           "the array is an object of type " +
                           "Transaction.Active.");

    public static final StatDefinition N_NON_REPLICATED_GROUP_COMMITS =
        new StatDefinition("nNonReplicatedGroupCommits",
                           "Number of group commit operations for local" +
                           "non replicated txns");

    public static final StatDefinition N_NON_REPLICATED_GROUP_COMMIT_TXNS =
        new StatDefinition("nNonReplicatedGroupCommitTxns",
                           "Number of transaction commits for local non " +
                           "replicated txns that were part of a group commit" +
                           " operation.");

    public static final StatDefinition N_NON_REPLICATED_GROUP_COMMIT_MAX_EXCEEDED =
        new StatDefinition("nNonReplicatedGroupCommitMaxExceeded",
                           "Number of group commits for local non replicated" +
                           " txns that were initiated due to the max group " +
                           "size(ReplicationConfig.MASTER_MAX_GROUP_COMMIT)" +
                           "  being exceeded.");

    public static final StatDefinition N_NON_REPLICATED_GROUP_COMMIT_TIMEOUTS =
        new StatDefinition("nNonReplicatedGroupCommitTimeouts",
                               "Number of group commits on the master side that were" +
                               " initiated due to the group timeout interval" +
                               "(ReplicationConfig.MASTER_GROUP_COMMIT_INTERVAL) being" +
                               " exceeded.");
}
