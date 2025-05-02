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

package com.sleepycat.je.rep.utilint;

import java.util.SortedSet;
import java.util.TreeSet;

import com.sleepycat.je.rep.elections.AcceptorStatDefinition;
import com.sleepycat.je.rep.elections.ElectionStatDefinition;
import com.sleepycat.je.rep.elections.LearnerStatDefinition;
import com.sleepycat.je.rep.elections.ProposerStatDefinition;
import com.sleepycat.je.rep.impl.node.ChannelTimeoutStatDefinition;
import com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition;
import com.sleepycat.je.rep.impl.node.MasterTransferStatDefinition;
import com.sleepycat.je.rep.impl.node.ReplayStatDefinition;
import com.sleepycat.je.rep.impl.node.ReplicaStatDefinition;
import com.sleepycat.je.rep.stream.FeederTxnStatDefinition;
import com.sleepycat.je.rep.subscription.SubscriptionStatDefinition;
import com.sleepycat.je.rep.vlsn.VLSNIndexStatDefinition;
import com.sleepycat.je.statcap.StatCaptureDefinitions;
import com.sleepycat.je.statcap.StatManager;
import com.sleepycat.je.utilint.StatDefinition;

public class StatCaptureRepDefinitions extends StatCaptureDefinitions {

    private static StatDefinition[] channelTimeoutStats = {
        ChannelTimeoutStatDefinition.N_CHANNEL_TIMEOUT_MAP
    };

    private static StatDefinition[] feederStats = {
        FeederManagerStatDefinition.N_FEEDERS_CREATED,
        FeederManagerStatDefinition.N_FEEDERS_SHUTDOWN,
        FeederManagerStatDefinition.TXN_AUTH_AVG_WAIT,
        FeederManagerStatDefinition.N_TXN_AUTH_FAILED,
        FeederManagerStatDefinition.N_TXN_AUTH_SUCCEED,
        FeederManagerStatDefinition.N_TXN_AUTH_NO_WAIT,
        FeederManagerStatDefinition.N_TXN_AUTH_WAITED,
        FeederManagerStatDefinition.REPLICA_DELAY_MAP,
        FeederManagerStatDefinition.REPLICA_AVG_DELAY_MS_MAP,
        FeederManagerStatDefinition.REPLICA_95_DELAY_MS_MAP,
        FeederManagerStatDefinition.REPLICA_99_DELAY_MS_MAP,
        FeederManagerStatDefinition.REPLICA_MAX_DELAY_MS_MAP,
        FeederManagerStatDefinition.REPLICA_LAST_COMMIT_TIMESTAMP_MAP,
        FeederManagerStatDefinition.REPLICA_LAST_COMMIT_VLSN_MAP,
        FeederManagerStatDefinition.REPLICA_LOCAL_DURABLE_VLSN_MAP,
        FeederManagerStatDefinition.REPLICA_VLSN_LAG_MAP,
        FeederManagerStatDefinition.REPLICA_VLSN_RATE_MAP,
        FeederManagerStatDefinition.REPLICA_AVG_ACK_LATENCY_NS_MAP,
        FeederManagerStatDefinition.REPLICA_N_HEARTBEAT_SENT_MAP,
        FeederManagerStatDefinition.REPLICA_N_HEARTBEAT_RECEIVED_MAP
    };

    private static StatDefinition[] masterTransferStats = {
            MasterTransferStatDefinition.N_MASTER_TRANSFERS,
            MasterTransferStatDefinition.N_MASTER_TRANSFERS_FAILURE,
            MasterTransferStatDefinition.N_MASTER_TRANSFERS_SUCCESS
    };

    private static StatDefinition[] replayStats = {
        ReplayStatDefinition.N_COMMITS,
        ReplayStatDefinition.N_COMMIT_ACKS,
        ReplayStatDefinition.N_COMMIT_SYNCS,
        ReplayStatDefinition.N_COMMIT_NO_SYNCS,
        ReplayStatDefinition.N_COMMIT_WRITE_NO_SYNCS,
        ReplayStatDefinition.N_ABORTS,
        ReplayStatDefinition.N_LNS,
        ReplayStatDefinition.N_NAME_LNS,
        ReplayStatDefinition.N_REPLAY_QUEUE_OVERFLOWS,
        ReplayStatDefinition.N_REPLAY_QUEUE_AVG_SIZE,
        ReplayStatDefinition.N_NOT_PREPROCESSED,
        ReplayStatDefinition.N_PREPROCESS_MISS,
        ReplayStatDefinition.LATEST_COMMIT_LAG_MS,
        ReplayStatDefinition.N_GROUP_COMMIT_TIMEOUTS,
        ReplayStatDefinition.N_GROUP_COMMIT_MAX_EXCEEDED,
        ReplayStatDefinition.N_GROUP_COMMITS,
        ReplayStatDefinition.N_GROUP_COMMIT_TXNS,
        ReplayStatDefinition.REPLAY_QUEUE_AVG_DELAY_NS,
        ReplayStatDefinition.REPLAY_QUEUE_95_DELAY_MS,
        ReplayStatDefinition.REPLAY_QUEUE_99_DELAY_MS,
        ReplayStatDefinition.REPLAY_QUEUE_MAX_DELAY_NS,
        ReplayStatDefinition.OUTPUT_QUEUE_AVG_DELAY_NS,
        ReplayStatDefinition.OUTPUT_QUEUE_95_DELAY_MS,
        ReplayStatDefinition.OUTPUT_QUEUE_99_DELAY_MS,
        ReplayStatDefinition.OUTPUT_QUEUE_MAX_DELAY_NS,
        ReplayStatDefinition.TXN_AVG_NS,
        ReplayStatDefinition.TXN_95_MS,
        ReplayStatDefinition.TXN_99_MS,
        ReplayStatDefinition.TXN_MAX_NS
    };

    private static StatDefinition[] replicaStats = {
        ReplicaStatDefinition.N_LAG_CONSISTENCY_WAITS,
        ReplicaStatDefinition.N_LAG_CONSISTENCY_WAIT_MS,
        ReplicaStatDefinition.N_VLSN_CONSISTENCY_WAITS,
        ReplicaStatDefinition.N_VLSN_CONSISTENCY_WAIT_MS,
        ReplicaStatDefinition.REPLICA_LOCAL_VLSN_LAG,
        ReplicaStatDefinition.REPLICA_TXN_END_TIME_LAG
    };

    private static StatDefinition[] feedertxnStats = {
        FeederTxnStatDefinition.TXNS_ACKED,
        FeederTxnStatDefinition.TXNS_ASYNC_ACKED,
        FeederTxnStatDefinition.TXNS_NOT_ACKED,
        FeederTxnStatDefinition.ACK_TXN_AVG_NS,
        FeederTxnStatDefinition.LOCAL_TXN_AVG_NS,
        FeederTxnStatDefinition.LAST_COMMIT_VLSN,
        FeederTxnStatDefinition.LAST_COMMIT_TIMESTAMP,
        FeederTxnStatDefinition.VLSN_RATE,
        FeederTxnStatDefinition.N_MASTER_GROUP_COMMIT_TIMEOUTS,
        FeederTxnStatDefinition.N_MASTER_GROUP_COMMIT_MAX_EXCEEDED,
        FeederTxnStatDefinition.N_MASTER_GROUP_COMMITS,
        FeederTxnStatDefinition.N_MASTER_GROUP_COMMIT_TXNS,
    };

    private static StatDefinition[] binaryProtocolStats = {
        BinaryProtocolStatDefinition.N_READ_NANOS,
        BinaryProtocolStatDefinition.N_WRITE_NANOS,
        BinaryProtocolStatDefinition.N_MAX_WRITE_NANOS,
        BinaryProtocolStatDefinition.WRITE_AVG_NANOS,
        BinaryProtocolStatDefinition.WRITE_95_MS,
        BinaryProtocolStatDefinition.WRITE_99_MS,
        BinaryProtocolStatDefinition.N_BYTES_READ,
        BinaryProtocolStatDefinition.N_MESSAGES_READ,
        BinaryProtocolStatDefinition.N_BYTES_WRITTEN,
        BinaryProtocolStatDefinition.WRITE_AVG_BYTES,
        BinaryProtocolStatDefinition.N_MESSAGE_BATCHES,
        BinaryProtocolStatDefinition.N_MESSAGES_BATCHED,
        BinaryProtocolStatDefinition.N_MESSAGES_WRITTEN,
        BinaryProtocolStatDefinition.MESSAGE_READ_RATE,
        BinaryProtocolStatDefinition.MESSAGE_WRITE_RATE,
        BinaryProtocolStatDefinition.BYTES_READ_RATE,
        BinaryProtocolStatDefinition.BYTES_WRITE_RATE,
        BinaryProtocolStatDefinition.N_ACK_MESSAGES,
        BinaryProtocolStatDefinition.N_GROUP_ACK_MESSAGES,
        BinaryProtocolStatDefinition.N_MAX_GROUPED_ACKS,
        BinaryProtocolStatDefinition.N_GROUPED_ACKS,
        BinaryProtocolStatDefinition.N_ENTRIES_WRITTEN_OLD_VERSION
    };

    private static StatDefinition[] vlsnIndexStats = {
        VLSNIndexStatDefinition.N_HITS,
        VLSNIndexStatDefinition.N_MISSES,
        VLSNIndexStatDefinition.N_HEAD_BUCKETS_DELETED,
        VLSNIndexStatDefinition.N_TAIL_BUCKETS_DELETED,
        VLSNIndexStatDefinition.N_BUCKETS_CREATED
    };

    private static StatDefinition[] electionStats = {
        ElectionStatDefinition.ELECTIONS_INITIATED,
        ProposerStatDefinition.PHASE1_ARBITER,
        ProposerStatDefinition.PHASE1_NO_QUORUM,
        ProposerStatDefinition.PHASE1_HIGHER_PROPOSAL,
        ProposerStatDefinition.PHASE1_NO_NON_ZERO_PRIO,
        ProposerStatDefinition.PHASE2_NO_QUORUM,
        ProposerStatDefinition.PHASE2_HIGHER_PROPOSAL,
        ProposerStatDefinition.PROMISE_COUNT,
        ProposerStatDefinition.ELECTIONS_DELAYED_COUNT,
        AcceptorStatDefinition.PROPOSE_ACCEPTOR_ACCEPTED,
        AcceptorStatDefinition.PROPOSE_ACCEPTOR_IGNORED,
        AcceptorStatDefinition.PROPOSE_ACCEPTOR_REJECTED,
        AcceptorStatDefinition.ACCEPT_ACCEPTOR_ACCEPTED,
        AcceptorStatDefinition.ACCEPT_ACCEPTOR_REJECTED,
        LearnerStatDefinition.MASTER_LEARNED
    };

    public static StatManager.SDef[] maxStats = {
        new StatManager.SDef(ReplayStatDefinition.GROUP_NAME,
                             ReplayStatDefinition.TXN_MAX_NS),
        new StatManager.SDef(ReplayStatDefinition.GROUP_NAME,
                             ReplayStatDefinition.OUTPUT_QUEUE_MAX_DELAY_NS),
        new StatManager.SDef(ReplayStatDefinition.GROUP_NAME,
                             ReplayStatDefinition.REPLAY_QUEUE_MAX_DELAY_NS),
        new StatManager.SDef(BinaryProtocolStatDefinition.GROUP_NAME,
                             BinaryProtocolStatDefinition.N_MAX_GROUPED_ACKS),
        new StatManager.SDef(BinaryProtocolStatDefinition.GROUP_NAME,
                             BinaryProtocolStatDefinition.N_MAX_WRITE_NANOS)
    };
    
    private static StatDefinition[] subscriptionStats = { 
    		SubscriptionStatDefinition.SUB_N_REPLAY_QUEUE_OVERFLOW,
    		SubscriptionStatDefinition.SUB_MSG_RECEIVED,
    		SubscriptionStatDefinition.SUB_MSG_RESPONDED,
    		SubscriptionStatDefinition.SUB_MAX_PENDING_INPUT,
    		SubscriptionStatDefinition.SUB_OPS_PROCESSED,
    		SubscriptionStatDefinition.SUB_TXN_ABORTED,
    		SubscriptionStatDefinition.SUB_TXN_COMMITTED,
    		SubscriptionStatDefinition.SUB_HEARTBEAT_SENT,
    		SubscriptionStatDefinition.SUB_HEARTBEAT_RECEIVED
    };

    public StatCaptureRepDefinitions() {
        super();
        String groupname = ChannelTimeoutStatDefinition.GROUP_NAME;
        for (StatDefinition stat : channelTimeoutStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = FeederManagerStatDefinition.GROUP_NAME;
        for (StatDefinition stat : feederStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = FeederTxnStatDefinition.GROUP_NAME;
        for (StatDefinition stat : feedertxnStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = MasterTransferStatDefinition.GROUP_NAME;
        for (StatDefinition stat : masterTransferStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = ReplayStatDefinition.GROUP_NAME;
        for (StatDefinition stat : replayStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = ReplicaStatDefinition.GROUP_NAME;
        for (StatDefinition stat : replicaStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = BinaryProtocolStatDefinition.GROUP_NAME;
        for (StatDefinition stat : binaryProtocolStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = VLSNIndexStatDefinition.GROUP_NAME;
        for (StatDefinition stat : vlsnIndexStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = ElectionStatDefinition.GROUP_NAME;
        for (StatDefinition stat : electionStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
        groupname = SubscriptionStatDefinition.GROUP_NAME;
        for (StatDefinition stat : subscriptionStats) {
            nameToDef.put(groupname + ":" + stat.getName(), stat);
        }
    }

    @Override
    public SortedSet<String> getStatisticProjections() {
        SortedSet<String> retval = new TreeSet<>();
        super.getProjectionsInternal(retval);

        String groupname = FeederTxnStatDefinition.GROUP_NAME;
        for (StatDefinition stat : channelTimeoutStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = FeederManagerStatDefinition.GROUP_NAME;
        for (StatDefinition stat : feederStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = FeederTxnStatDefinition.GROUP_NAME;
        for (StatDefinition stat : feedertxnStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = MasterTransferStatDefinition.GROUP_NAME;
        for (StatDefinition stat : masterTransferStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = ReplayStatDefinition.GROUP_NAME;
        for (StatDefinition stat : replayStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = ReplicaStatDefinition.GROUP_NAME;
        for (StatDefinition stat : replicaStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = BinaryProtocolStatDefinition.GROUP_NAME;
        for (StatDefinition stat : binaryProtocolStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = VLSNIndexStatDefinition.GROUP_NAME;
        for (StatDefinition stat : vlsnIndexStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = ElectionStatDefinition.GROUP_NAME;
        for (StatDefinition stat : electionStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        groupname = SubscriptionStatDefinition.GROUP_NAME;
        for (StatDefinition stat : subscriptionStats) {
            retval.add(groupname + ":" + stat.getName());
        }
        return retval;
    }
}
