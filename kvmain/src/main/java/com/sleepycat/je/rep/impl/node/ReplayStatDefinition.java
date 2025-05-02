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

package com.sleepycat.je.rep.impl.node;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Replay statistics.
 */
public class ReplayStatDefinition {

    public static final String GROUP_NAME = "Replay";
    public static final String GROUP_DESC = "The Replay unit applies the " +
        "incoming replication stream at a Replica. These stats show the " +
        "load the Replica incurs when processing updates.";

    public static final String N_COMMITS_NAME =
        "nCommits";
    public static final String N_COMMITS_DESC =
        "Number of Commits replayed by the Replica.";
    public static final StatDefinition N_COMMITS =
        new StatDefinition(
            N_COMMITS_NAME,
            N_COMMITS_DESC);

    public static final String N_GROUP_COMMIT_TIMEOUTS_NAME =
        "nGroupCommitTimeouts";
    public static final String N_GROUP_COMMIT_TIMEOUTS_DESC =
        "Number of group commits on replica side that were initiated due to" +
        " the group timeout" +
        " interval(ReplicationConfig.REPLICA_GROUP_COMMIT_INTERVAL) being" +
        " exceeded.";
    public static final StatDefinition N_GROUP_COMMIT_TIMEOUTS =
        new StatDefinition(
            N_GROUP_COMMIT_TIMEOUTS_NAME,
            N_GROUP_COMMIT_TIMEOUTS_DESC);

    public static final String N_GROUP_COMMIT_MAX_EXCEEDED_NAME =
        "nGroupCommitMaxExceeded";
    public static final String N_GROUP_COMMIT_MAX_EXCEEDED_DESC =
        "Number of group commits on replica side that were initiated due to" +
        " the max group " +
        "size(ReplicationConfig.REPLICA_MAX_GROUP_COMMIT)  being exceeded.";
    public static final StatDefinition N_GROUP_COMMIT_MAX_EXCEEDED =
        new StatDefinition(
            N_GROUP_COMMIT_MAX_EXCEEDED_NAME,
            N_GROUP_COMMIT_MAX_EXCEEDED_DESC);

    public static final String N_GROUP_COMMIT_TXNS_NAME =
        "nGroupCommitTxns";
    public static final String N_GROUP_COMMIT_TXNS_DESC =
        "Number of replay transaction commits that were part of a group " +
            "commit operation on replica side.";
    public static final StatDefinition N_GROUP_COMMIT_TXNS =
        new StatDefinition(
            N_GROUP_COMMIT_TXNS_NAME,
            N_GROUP_COMMIT_TXNS_DESC);

    public static final String N_GROUP_COMMITS_NAME =
        "nGroupCommits";
    public static final String N_GROUP_COMMITS_DESC =
        "Number of group commit operations on replica side.";
    public static final StatDefinition N_GROUP_COMMITS =
        new StatDefinition(
            N_GROUP_COMMITS_NAME,
            N_GROUP_COMMITS_DESC);

    public static final String N_COMMIT_ACKS_NAME =
        "nCommitAcks";
    public static final String N_COMMIT_ACKS_DESC =
        "Number of commits for which the Master requested an ack.";
    public static final StatDefinition N_COMMIT_ACKS =
        new StatDefinition(
            N_COMMIT_ACKS_NAME,
            N_COMMIT_ACKS_DESC);

    public static final String N_COMMIT_SYNCS_NAME =
        "nCommitSyncs";
    public static final String N_COMMIT_SYNCS_DESC =
        "Number of CommitSyncs used to satisfy ack requests. Note that user " +
            "level commit sync requests may be optimized into CommitNoSync " +
            "requests as part of a group commit.";
    public static final StatDefinition N_COMMIT_SYNCS =
        new StatDefinition(
            N_COMMIT_SYNCS_NAME,
            N_COMMIT_SYNCS_DESC);

    public static final String N_COMMIT_NO_SYNCS_NAME =
        "nCommitNoSyncs";
    public static final String N_COMMIT_NO_SYNCS_DESC =
        "Number of CommitNoSyncs used to satisfy ack requests.";
    public static final StatDefinition N_COMMIT_NO_SYNCS =
        new StatDefinition(
            N_COMMIT_NO_SYNCS_NAME,
            N_COMMIT_NO_SYNCS_DESC);

    public static final String N_COMMIT_WRITE_NO_SYNCS_NAME =
        "nCommitWriteNoSyncs";
    public static final String N_COMMIT_WRITE_NO_SYNCS_DESC =
        "Number of CommitWriteNoSyncs used to satisfy ack requests.";
    public static final StatDefinition N_COMMIT_WRITE_NO_SYNCS =
        new StatDefinition(
            N_COMMIT_WRITE_NO_SYNCS_NAME,
            N_COMMIT_WRITE_NO_SYNCS_DESC);

    public static final String N_ABORTS_NAME =
        "nAborts";
    public static final String N_ABORTS_DESC =
        "Number of Aborts replayed by the Replica.";
    public static final StatDefinition N_ABORTS =
        new StatDefinition(
            N_ABORTS_NAME,
            N_ABORTS_DESC);

    public static final String N_LNS_NAME =
        "nLNs";
    public static final String N_LNS_DESC =
        "Number of LNs.";
    public static final StatDefinition N_LNS =
        new StatDefinition(
            N_LNS_NAME,
            N_LNS_DESC);

    public static final String N_NAME_LNS_NAME =
        "nNameLNs";
    public static final String N_NAME_LNS_DESC =
        "Number of Name LNs.";
    public static final StatDefinition N_NAME_LNS =
        new StatDefinition(
            N_NAME_LNS_NAME,
            N_NAME_LNS_DESC);

    public static final String N_REPLAY_QUEUE_OVERFLOWS_NAME =
        "nReplayQueueOverflows";
    public static final String N_REPLAY_QUEUE_OVERFLOWS_DESC =
        "Number of failed attempts to place an entry in the replica replay " +
            "queue due to the queue being full.";
    public static final StatDefinition N_REPLAY_QUEUE_OVERFLOWS =
        new StatDefinition(
            N_REPLAY_QUEUE_OVERFLOWS_NAME,
            N_REPLAY_QUEUE_OVERFLOWS_DESC);

    public static final String N_REPLAY_QUEUE_AVG_SIZE_NAME =
        "nReplayQueueAvgSize";
    public static final String N_REPLAY_QUEUE_AVG_SIZE_DESC =
        "Average size of the replay queue; it represents the replay backlog.";
    public static final StatDefinition N_REPLAY_QUEUE_AVG_SIZE =
        new StatDefinition(
            N_REPLAY_QUEUE_AVG_SIZE_NAME,
            N_REPLAY_QUEUE_AVG_SIZE_DESC);

    public static final String N_NOT_PREPROCESSED_NAME =
        "nNotPreprocessed";
    public static final String N_NOT_PREPROCESSED_DESC =
        "Number of times the replay preprocessor did not process an " +
            "operation before it was processed by the replayer.";
    public static final StatDefinition N_NOT_PREPROCESSED =
        new StatDefinition(
            N_NOT_PREPROCESSED_NAME,
            N_NOT_PREPROCESSED_DESC);

    public static final String N_PREPROCESS_MISS_NAME =
        "nPreprocessMiss";
    public static final String N_PREPROCESS_MISS_DESC =
        "Number of times the replay preprocessor selected a BIN that could " +
            "not be used by the operation because the key is located in " +
            "another BIN, will be inserted at the end of the BIN, or a " +
            "split is needed.";
    public static final StatDefinition N_PREPROCESS_MISS =
        new StatDefinition(
            N_PREPROCESS_MISS_NAME,
            N_PREPROCESS_MISS_DESC);

    public static final String LATEST_COMMIT_LAG_MS_NAME =
        "latestCommitLagMs";
    public static final String LATEST_COMMIT_LAG_MS_DESC =
        "Time in msec between when the latest update operation committed on " +
            "the master and then subsequently committed on the replica. This " +
            "value is affected by any clock skew between the master and the " +
            "replica.";
    public static final StatDefinition LATEST_COMMIT_LAG_MS =
        new StatDefinition(
            LATEST_COMMIT_LAG_MS_NAME,
            LATEST_COMMIT_LAG_MS_DESC);

    public static final String REPLAY_QUEUE_AVG_DELAY_NS_NAME =
        "replayQueueAvgDelayNs";
    public static final String REPLAY_QUEUE_AVG_DELAY_NS_DESC =
        "The average time in nanoseconds between when the replica receives " +
        "a replication entry and when the entry is read from the replay " +
        "queue by the replay thread, so that it can be replayed." +
        "The busier the replay thread, the longer the entry sits in the " +
        "queue before it can be serviced by the replay thread.";
    public static final StatDefinition REPLAY_QUEUE_AVG_DELAY_NS =
        new StatDefinition(REPLAY_QUEUE_AVG_DELAY_NS_NAME,
                           REPLAY_QUEUE_AVG_DELAY_NS_DESC);

    public static final String REPLAY_QUEUE_95_DELAY_MS_NAME =
        "replayQueue95DelayMs";
    public static final String REPLAY_QUEUE_95_DELAY_MS_DESC =
        "The 95th percentile value of the time in milliseconds between when " +
        "the replica receives a replication entry and when the entry is " +
        "read from the replay queue by the replay thread.";
    public static final StatDefinition REPLAY_QUEUE_95_DELAY_MS =
        new StatDefinition(REPLAY_QUEUE_95_DELAY_MS_NAME,
                           REPLAY_QUEUE_95_DELAY_MS_DESC);

    public static final String REPLAY_QUEUE_99_DELAY_MS_NAME =
        "replayQueue99DelayMs";
    public static final String REPLAY_QUEUE_99_DELAY_MS_DESC =
        "The 99th percentile value of the time in milliseconds between when " +
        "the replica receives a replication entry and when the entry is " +
        "read from the replay queue by the replay thread.";
    public static final StatDefinition REPLAY_QUEUE_99_DELAY_MS =
        new StatDefinition(REPLAY_QUEUE_99_DELAY_MS_NAME,
                           REPLAY_QUEUE_99_DELAY_MS_DESC);

    public static final String REPLAY_QUEUE_MAX_DELAY_NS_NAME =
        "replayQueueMaxDelayNs";
    public static final String REPLAY_QUEUE_MAX_DELAY_NS_DESC =
        "The maximum time in nanoseconds between when the replica " +
        "receives a replication entry and when the entry is read from the " +
        "replay queue by the replay thread.";
    public static final StatDefinition REPLAY_QUEUE_MAX_DELAY_NS =
        new StatDefinition(REPLAY_QUEUE_MAX_DELAY_NS_NAME,
                           REPLAY_QUEUE_MAX_DELAY_NS_DESC);

    public static final String OUTPUT_QUEUE_AVG_DELAY_NS_NAME =
        "outputQueueAvgDelayNs";
    public static final String OUTPUT_QUEUE_AVG_DELAY_NS_DESC =
        "The average time in nanoseconds between when the replay thread " +
        "places a response in the output queue and when the entry is read " +
        "from the queue in preparation for sending it to the feeder by the " +
        "output thread.";
    public static final StatDefinition OUTPUT_QUEUE_AVG_DELAY_NS =
        new StatDefinition(OUTPUT_QUEUE_AVG_DELAY_NS_NAME,
                           OUTPUT_QUEUE_AVG_DELAY_NS_DESC);

    public static final String OUTPUT_QUEUE_95_DELAY_MS_NAME =
        "outputQueue95DelayMs";
    public static final String OUTPUT_QUEUE_95_DELAY_MS_DESC =
        "The 95th percentile value of the time in milliseconds between when " +
        "the replay thread places a response in the output queue and when " +
        "the entry is read from the queue in preparation for sending it to " +
        "the feeder by the output thread.";
    public static final StatDefinition OUTPUT_QUEUE_95_DELAY_MS =
        new StatDefinition(OUTPUT_QUEUE_95_DELAY_MS_NAME,
                           OUTPUT_QUEUE_95_DELAY_MS_DESC);

    public static final String OUTPUT_QUEUE_99_DELAY_MS_NAME =
        "outputQueue99DelayMs";
    public static final String OUTPUT_QUEUE_99_DELAY_MS_DESC =
        "The 99th percentile value of the time in milliseconds between when " +
        "the replay thread places a response in the output queue and when " +
        "the entry is read from the queue in preparation for sending it to " +
        "the feeder by the output thread.";
    public static final StatDefinition OUTPUT_QUEUE_99_DELAY_MS =
        new StatDefinition(OUTPUT_QUEUE_99_DELAY_MS_NAME,
                           OUTPUT_QUEUE_99_DELAY_MS_DESC);

    public static final String OUTPUT_QUEUE_MAX_DELAY_NS_NAME =
        "outputQueueMaxDelayNs";
    public static final String OUTPUT_QUEUE_MAX_DELAY_NS_DESC =
        "The maximum time in nanoseconds between when the replay thread " +
        "places a response in the output queue and when the entry is read " +
        "from the queue in preparation for sending it to the feeder by the " +
        "output thread.";
    public static final StatDefinition OUTPUT_QUEUE_MAX_DELAY_NS =
        new StatDefinition(OUTPUT_QUEUE_MAX_DELAY_NS_NAME,
                           OUTPUT_QUEUE_MAX_DELAY_NS_DESC);

    public static final String TXN_AVG_NS_NAME = "txnAvgNs";
    public static final String TXN_AVG_NS_DESC =
        "The average time in nanos to process a transaction at the replica. " +
        "It spans the time period from when the first log entry for the txn " +
        "was recieved by the replica, to the time the transaction was " +
        "committed at the replica; it includes the time spent waiting in " +
        "the replay queue by the transaction's entries.";
    public static final StatDefinition TXN_AVG_NS =
        new StatDefinition(TXN_AVG_NS_NAME,
                           TXN_AVG_NS_DESC);

    public static final String TXN_95_MS_NAME = "txn95Ms";
    public static final String TXN_95_MS_DESC =
        "The 95th percentile value of the time in ms to process " +
        "a transaction.";
    public static final StatDefinition TXN_95_MS =
        new StatDefinition(TXN_95_MS_NAME,
                           TXN_95_MS_DESC);

    public static final String TXN_99_MS_NAME = "txn99Ms";
    public static final String TXN_99_MS_DESC =
        "The 99th percentile value of the time in ms to process " +
        "a transaction.";
    public static final StatDefinition TXN_99_MS =
        new StatDefinition(TXN_99_MS_NAME,
                           TXN_99_MS_DESC);

    public static final String TXN_MAX_NS_NAME = "txnMaxNs";
    public static final String TXN_MAX_NS_DESC =
        "The maximum time in nanos to process a transaction.";
    public static final StatDefinition TXN_MAX_NS =
        new StatDefinition(TXN_MAX_NS_NAME,
                           TXN_MAX_NS_DESC);
}
