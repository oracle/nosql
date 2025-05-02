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

import static com.sleepycat.je.utilint.StatDefinition.StatType.CUMULATIVE;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Replay statistics.
 */
public class FeederManagerStatDefinition {

    public static final String GROUP_NAME = "FeederManager";
    public static final String GROUP_DESC =
        "A feeder is a replication stream connection between a master and " +
            "replica nodes.";

    public static final String N_FEEDERS_CREATED_NAME =
        "nFeedersCreated";
    public static final String N_FEEDERS_CREATED_DESC =
        "Number of Feeder threads since this node was started.";
    public static final StatDefinition N_FEEDERS_CREATED =
        new StatDefinition(
            N_FEEDERS_CREATED_NAME,
            N_FEEDERS_CREATED_DESC);

    public static final String N_FEEDERS_SHUTDOWN_NAME =
        "nFeedersShutdown";
    public static final String N_FEEDERS_SHUTDOWN_DESC =
        "Number of Feeder threads that were shut down, either because this " +
            "node, or the Replica terminated the connection.";
    public static final StatDefinition N_FEEDERS_SHUTDOWN =
        new StatDefinition(
            N_FEEDERS_SHUTDOWN_NAME,
            N_FEEDERS_SHUTDOWN_DESC);

    public static final String REPLICA_DELAY_MAP_NAME =
        "replicaDelayMap";
    public static final String REPLICA_DELAY_MAP_DESC =
        "A map from replica node name to the most recently known delay, in " +
            "milliseconds, between when a transaction was committed on the " +
            "master and when the master learned that the change was " +
            "processed on the replica, if known. Returns an empty map if " +
            "this node is not the master. The delay represents the time " +
            "that would be needed to receive a transaction acknowledgement.";
    public static final StatDefinition REPLICA_DELAY_MAP =
        new StatDefinition(
            REPLICA_DELAY_MAP_NAME,
            REPLICA_DELAY_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_AVG_DELAY_MS_MAP_NAME =
        "replicaAvgDelayMsMap";
    public static final String REPLICA_AVG_DELAY_MS_MAP_DESC =
        "A map from replica node name to the average for the current " +
        "period of the delay, in milliseconds, between when a transaction " +
        "was committed on the master and when the master learned that the " +
        "change was processed on the replica, if known. Returns an empty " +
        "map if this node is not the master.";
    public static final StatDefinition REPLICA_AVG_DELAY_MS_MAP =
        new StatDefinition(REPLICA_AVG_DELAY_MS_MAP_NAME,
                           REPLICA_AVG_DELAY_MS_MAP_DESC);

    public static final String REPLICA_AVG_ACK_LATENCY_NS_MAP_NAME =
        "replicaAvgAckLatencyNsMap";
    public static final String REPLICA_AVG_ACK_LATENCY_NS_MAP_DESC =
        "A map from replica node name to the average ack latency. " +
        "It represents the time interval starting with the time the commit " +
        "record was sent to the replica and ending when an ack for the " +
        "commit was recieved at the master. That is, it represents the " +
        "network roundtrip time plus the time it took for the replica to " +
        "process the commit record. " +
        "This metric only covers acks that were used to build a quorum. " +
        "Acks recieved after a quorum are not included in this metric. " +
        "In particular, a lagging replica may not show up at all in the metric" +
        "Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_AVG_ACK_LATENCY_NS_MAP =
        new StatDefinition(REPLICA_AVG_ACK_LATENCY_NS_MAP_NAME,
                           REPLICA_AVG_ACK_LATENCY_NS_MAP_DESC);

    public static final String REPLICA_95_DELAY_MS_MAP_NAME =
        "replica95DelayMsMap";
    public static final String REPLICA_95_DELAY_MS_MAP_DESC =
        "A map from replica node name to the 95th percentile value for the " +
        "current period of the delay, in milliseconds, between when a " +
        "transaction was committed on the master and when the master" +
        "learned that the change was processed on the replica, if " +
        "known. Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_95_DELAY_MS_MAP =
        new StatDefinition(REPLICA_95_DELAY_MS_MAP_NAME,
                           REPLICA_95_DELAY_MS_MAP_DESC);

    public static final String REPLICA_99_DELAY_MS_MAP_NAME =
        "replica99DelayMsMap";
    public static final String REPLICA_99_DELAY_MS_MAP_DESC =
        "A map from replica node name to the 99th percentile value for the " +
        "current period of the delay, in milliseconds, between when a " +
        "transaction was committed on the master and when the master" +
        "learned that the change was processed on the replica, if " +
        "known. Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_99_DELAY_MS_MAP =
        new StatDefinition(REPLICA_99_DELAY_MS_MAP_NAME,
                           REPLICA_99_DELAY_MS_MAP_DESC);

    public static final String REPLICA_MAX_DELAY_MS_MAP_NAME =
        "replicaMaxDelayMsMap";
    public static final String REPLICA_MAX_DELAY_MS_MAP_DESC =
        "A map from replica node name to the maximum value for the current " +
        "period of the delay, in milliseconds, between when a " +
        "transaction was committed on the master and when the master" +
        "learned that the change was processed on the replica, if " +
        "known. Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_MAX_DELAY_MS_MAP =
        new StatDefinition(REPLICA_MAX_DELAY_MS_MAP_NAME,
                           REPLICA_MAX_DELAY_MS_MAP_DESC);

    public static final String REPLICA_LAST_COMMIT_TIMESTAMP_MAP_NAME =
        "replicaLastCommitTimestampMap";
    public static final String REPLICA_LAST_COMMIT_TIMESTAMP_MAP_DESC =
        "A map from replica node name to the commit timestamp of the last " +
            "committed transaction that was processed on the replica, if " +
            "known. Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_LAST_COMMIT_TIMESTAMP_MAP =
        new StatDefinition(
            REPLICA_LAST_COMMIT_TIMESTAMP_MAP_NAME,
            REPLICA_LAST_COMMIT_TIMESTAMP_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_LAST_COMMIT_VLSN_MAP_NAME =
        "replicaLastCommitVLSNMap";
    public static final String REPLICA_LAST_COMMIT_VLSN_MAP_DESC =
        "A map from replica node name to the VLSN of the last committed " +
            "transaction that was processed on the replica, if known. Returns" +
            " an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_LAST_COMMIT_VLSN_MAP =
        new StatDefinition(
            REPLICA_LAST_COMMIT_VLSN_MAP_NAME,
            REPLICA_LAST_COMMIT_VLSN_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_LOCAL_DURABLE_VLSN_MAP_NAME =
            "replicaLocalDurableVLSNMap";
    public static final String REPLICA_LOCAL_DURABLE_VLSN_MAP_DESC =
            "A map from replica node name to the VLSN of the local durable " +
                "vlsn that resides on the replica's disk, if known. Returns" +
                " an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_LOCAL_DURABLE_VLSN_MAP =
            new StatDefinition(
                    REPLICA_LOCAL_DURABLE_VLSN_MAP_NAME,
                    REPLICA_LOCAL_DURABLE_VLSN_MAP_DESC,
                    CUMULATIVE);

    public static final String REPLICA_VLSN_LAG_MAP_NAME =
        "replicaVLSNLagMap";
    public static final String REPLICA_VLSN_LAG_MAP_DESC =
        "A map from replica node name to the lag, in VLSNs, between the " +
            "replication state of the replica and the master, if known. " +
            "Returns an empty map if this node is not the master.";
    public static final StatDefinition REPLICA_VLSN_LAG_MAP =
        new StatDefinition(
            REPLICA_VLSN_LAG_MAP_NAME,
            REPLICA_VLSN_LAG_MAP_DESC,
            CUMULATIVE);

    public static final String REPLICA_N_HEARTBEAT_SENT_MAP_NAME =
        "replicaNHeartbeatSent";
    public static final String REPLICA_N_HEARTBEAT_SENT_MAP_DESC =
        "A map from replica node name to the number of heartbeats sent to the replica.";
    public static final StatDefinition REPLICA_N_HEARTBEAT_SENT_MAP =
        new StatDefinition(
            REPLICA_N_HEARTBEAT_SENT_MAP_NAME,
            REPLICA_N_HEARTBEAT_SENT_MAP_DESC);
    
    public static final String REPLICA_N_HEARTBEAT_RECEIVED_MAP_NAME =
            "replicaNHeartbeatReceived";
        public static final String REPLICA_N_HEARTBEAT_RECEIVED_MAP_DESC =
            "A map from replica node name to the number of heartbeats received from the replica.";
        public static final StatDefinition REPLICA_N_HEARTBEAT_RECEIVED_MAP =
            new StatDefinition(
                REPLICA_N_HEARTBEAT_RECEIVED_MAP_NAME,
                REPLICA_N_HEARTBEAT_RECEIVED_MAP_DESC);

    public static final String REPLICA_VLSN_RATE_MAP_NAME =
            "replicaVLSNRateMap";
        public static final String REPLICA_VLSN_RATE_MAP_DESC =
            "A map from replica node name to a moving average of the rate, in " +
                "VLSNs per minute, that the replica is processing replication " +
                "data, if known. Returns an empty map if this node is not the " +
                "master.";
        public static final StatDefinition REPLICA_VLSN_RATE_MAP =
            new StatDefinition(
                REPLICA_VLSN_RATE_MAP_NAME,
                REPLICA_VLSN_RATE_MAP_DESC);

    public static final String N_TXN_AUTH_WAITED_NAME =
        "nTxnAuthWaited";
    public static final String N_TXN_AUTH_WAITED_DESC =
        "Number of times a transaction had to wait to authorize the master.";
    public static final StatDefinition N_TXN_AUTH_WAITED =
        new StatDefinition(N_TXN_AUTH_WAITED_NAME, N_TXN_AUTH_WAITED_DESC);

    public static final String N_TXN_AUTH_NO_WAIT_NAME =
        "nTxnAuthNoWait";
    public static final String N_TXN_AUTH_NO_WAIT_DESC =
        "Number of times a transaction was confirmed authorized without " +
        "having to wait.";
    public static final StatDefinition N_TXN_AUTH_NO_WAIT =
        new StatDefinition(N_TXN_AUTH_NO_WAIT_NAME, N_TXN_AUTH_NO_WAIT_DESC);

    public static final String N_TXN_AUTH_SUCCEED_NAME =
        "nTxnAuthSucceed";
    public static final String N_TXN_AUTH_SUCCEED_DESC =
        "Number of times a transaction succeeded in authorizing the master.";
    public static final StatDefinition N_TXN_AUTH_SUCCEED =
        new StatDefinition(N_TXN_AUTH_SUCCEED_NAME, N_TXN_AUTH_SUCCEED_DESC);

    public static final String N_TXN_AUTH_FAILED_NAME =
        "nTxnAuthFailed";
    public static final String N_TXN_AUTH_FAILED_DESC =
        "Number of times a transaction failed to authorize the master.";
    public static final StatDefinition N_TXN_AUTH_FAILED =
        new StatDefinition(N_TXN_AUTH_FAILED_NAME, N_TXN_AUTH_FAILED_DESC);

    public static final String TXN_AUTH_AVG_WAIT_NAME =
        "txnAuthAvgWait";
    public static final String TXN_AUTH_AVG_WAIT_DESC =
        "Average wait time in milliseconds to authorize the master when " +
        "forced to wait.";
    public static final StatDefinition TXN_AUTH_AVG_WAIT =
        new StatDefinition(TXN_AUTH_AVG_WAIT_NAME, TXN_AUTH_AVG_WAIT_DESC);
}

