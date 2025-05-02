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
 * Per-stat Metadata for Master Transfer statistics.
 */
public class MasterTransferStatDefinition {

    public static final String GROUP_NAME = "MasterTransfer";
    public static final String GROUP_DESC =
        "Master transfer is purposely switching mastership from the current " +
        "master to one of the replicas.";

    public static final String N_MASTER_TRANSFERS_NAME =
        "nMasterTransfers";
    public static final String N_MASTER_TRANSFERS_DESC =
        "Number of Master Transfers initiated in the last stat collection " +
        "period.";
    public static final StatDefinition N_MASTER_TRANSFERS =
        new StatDefinition(
            N_MASTER_TRANSFERS_NAME,
            N_MASTER_TRANSFERS_DESC);

    public static final String N_MASTER_TRANSFERS_SUCCESS_NAME =
        "nMasterTransfersSuccess";
    public static final String N_MASTER_TRANSFERS_SUCCESS_DESC =
        "Number of Master Transfers that succeeded in the last stat " +
        "collection period.";
    public static final StatDefinition N_MASTER_TRANSFERS_SUCCESS =
        new StatDefinition(
            N_MASTER_TRANSFERS_SUCCESS_NAME,
            N_MASTER_TRANSFERS_SUCCESS_DESC);

    public static final String N_MASTER_TRANSFERS_FAILURE_NAME =
        "nMasterTransfersFailure";
    public static final String N_MASTER_TRANSFERS_FAILURE_DESC =
        "Number of Master Transfers that faileded in the last stat " +
        "collection period.";
    public static final StatDefinition N_MASTER_TRANSFERS_FAILURE =
        new StatDefinition(
            N_MASTER_TRANSFERS_FAILURE_NAME,
            N_MASTER_TRANSFERS_FAILURE_DESC);
}
