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
 * Per-stat Metadata for HA Replica statistics.
 */
public class ReplicaStatDefinition {

    public static final String GROUP_NAME = "ConsistencyTracker";
    public static final String GROUP_DESC = "Statistics on the delays " +
        "experienced by read requests at the replica in order to conform " +
        "to the specified ReplicaConsistencyPolicy.";

    public static final String N_LAG_CONSISTENCY_WAITS_NAME =
        "nLagConsistencyWaits";
    public static final String N_LAG_CONSISTENCY_WAITS_DESC =
        "Number of Transaction waits while the replica catches up in order to" +
            " meet a transaction's consistency requirement.";
    public static final StatDefinition N_LAG_CONSISTENCY_WAITS =
        new StatDefinition(
            N_LAG_CONSISTENCY_WAITS_NAME,
            N_LAG_CONSISTENCY_WAITS_DESC);

    public static final String N_LAG_CONSISTENCY_WAIT_MS_NAME =
        "nLagConsistencyWaitMS";
    public static final String N_LAG_CONSISTENCY_WAIT_MS_DESC =
        "Number of msec waited while the replica catches up in order to meet " +
            "a transaction's consistency requirement.";
    public static final StatDefinition N_LAG_CONSISTENCY_WAIT_MS =
        new StatDefinition(
            N_LAG_CONSISTENCY_WAIT_MS_NAME,
            N_LAG_CONSISTENCY_WAIT_MS_DESC);

    public static final String N_VLSN_CONSISTENCY_WAITS_NAME =
        "nVLSNConsistencyWaits";
    public static final String N_VLSN_CONSISTENCY_WAITS_DESC =
        "Number of Transaction waits while the replica catches up in order to" +
            " receive a VLSN.";
    public static final StatDefinition N_VLSN_CONSISTENCY_WAITS =
        new StatDefinition(
            N_VLSN_CONSISTENCY_WAITS_NAME,
            N_VLSN_CONSISTENCY_WAITS_DESC);

    public static final String N_VLSN_CONSISTENCY_WAIT_MS_NAME =
        "nVLSNConsistencyWaitMS";
    public static final String N_VLSN_CONSISTENCY_WAIT_MS_DESC =
        "Number of msec waited while the replica catches up in order to " +
            "receive a VLSN.";
    public static final StatDefinition N_VLSN_CONSISTENCY_WAIT_MS =
        new StatDefinition(
            N_VLSN_CONSISTENCY_WAIT_MS_NAME,
            N_VLSN_CONSISTENCY_WAIT_MS_DESC);

    public static final String REPLICA_LOCAL_VLSN_LAG_NAME =
        "replicaLocalVLSNLag";
    public static final String REPLICA_LOCAL_VLSN_LAG_DESC =
        "The lag, in VLSNs, between the " +
            "replication state of the replica and the master, " +
            "maintained by the replica locally.";
    public static final StatDefinition REPLICA_LOCAL_VLSN_LAG =
        new StatDefinition(
            REPLICA_LOCAL_VLSN_LAG_NAME,
            REPLICA_LOCAL_VLSN_LAG_DESC,
            CUMULATIVE);

    public static final String REPLICA_TXN_END_TIME_LAG_NAME =
        "replicaTxnEndTimeLag";
    public static final String REPLICA_TXN_END_TIME_LAG_DESC =
        "An JSON object describing the txn-end time lag between the " +
        "the replica and the master. " +
        "The lag is a key indicator for replica health." +
        "The stats contains histograms and metrics for anomaly detection";
    public static final StatDefinition REPLICA_TXN_END_TIME_LAG =
        new StatDefinition(
            REPLICA_TXN_END_TIME_LAG_NAME,
            REPLICA_TXN_END_TIME_LAG_DESC,
            CUMULATIVE);
}
