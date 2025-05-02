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
import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * Per-stat Metadata for JE Btree statistics.
 */
public class BTreeStatDefinition {

    /*
     * The BT_COUNT group is used only by Database.stat. It is not used for
     * EnvironmentStats.
     */
    public static final String BT_COUNT_GROUP_NAME = "BTreeCount";
    public static final String BT_COUNT_GROUP_DESC = "Btree node counts.";

    public static final StatDefinition BT_COUNT_BINS =
        new StatDefinition("bins",
                           "Number of bottom internal nodes in " +
                           "the database btree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_DELETED_LNS =
        new StatDefinition("deletedLNs",
                           "Number of deleted data records in the " +
                               "database btree that are pending removal by " +
                               "the compressor.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_INS =
        new StatDefinition("ins",
                           "Number of internal nodes in database btree. " +
                           "BINs are not included.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_LNS =
        new StatDefinition("lns",
                           "Number of leaf nodes in the database btree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_MAINTREE_MAXDEPTH =
        new StatDefinition("mainTreeMaxDepth",
                           "Number of levels in the database btree.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_INS_BYLEVEL =
        new StatDefinition("insByLevel",
                            "Number of Internal Nodes indexed by level.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_BINS_BYLEVEL =
        new StatDefinition("binsByLevel",
                           "Number of Bottom Internal Nodes indexed by level.",
                           StatType.CUMULATIVE);

    public static final StatDefinition BT_COUNT_BIN_ENTRIES_HISTOGRAM =
        new StatDefinition("binsByFillPercent",
                           "Number of Bottom Internal Nodes indexed by 0-9 " +
                               "indicating fill percentage: [0-9% full, " +
                               "10-19% full, ..., 90-100% full].",
                           StatType.CUMULATIVE);

    /*
     * The BT_OP group is used for EnvironmentStats.
     */
    public static final String BT_OP_GROUP_NAME = "BtreeOp";
    public static final String BT_OP_GROUP_DESC =
        "Btree internal operation statistics.";

    public static final String BT_OP_RELATCHES_REQUIRED_NAME =
        "relatchesRequired";
    public static final String BT_OP_RELATCHES_REQUIRED_DESC =
        "Number of btree latch upgrades required while operating " +
            "on this Environment. A measurement of contention.";
    public static final StatDefinition BT_OP_RELATCHES_REQUIRED =
        new StatDefinition(
            BT_OP_RELATCHES_REQUIRED_NAME,
            BT_OP_RELATCHES_REQUIRED_DESC);

    public static final String BT_OP_ROOT_SPLITS_NAME =
        "nRootSplits";
    public static final String BT_OP_ROOT_SPLITS_DESC =
        "Number of times a database btree root was split.";
    public static final StatDefinition BT_OP_ROOT_SPLITS =
        new StatDefinition(
            BT_OP_ROOT_SPLITS_NAME ,
            BT_OP_ROOT_SPLITS_DESC);

    public static final String BT_OP_BIN_DELTA_GETS_NAME =
        "nBinDeltaGet";
    public static final String BT_OP_BIN_DELTA_GETS_DESC =
        "Number of gets performed in BIN-deltas";
    public static final StatDefinition BT_OP_BIN_DELTA_GETS =
        new StatDefinition(
            BT_OP_BIN_DELTA_GETS_NAME,
            BT_OP_BIN_DELTA_GETS_DESC);

    public static final String BT_OP_BIN_DELTA_INSERTS_NAME =
        "nBinDeltaInsert";
    public static final String BT_OP_BIN_DELTA_INSERTS_DESC =
        "Number of insertions performed in BIN-deltas";
    public static final StatDefinition BT_OP_BIN_DELTA_INSERTS =
        new StatDefinition(
            BT_OP_BIN_DELTA_INSERTS_NAME,
            BT_OP_BIN_DELTA_INSERTS_DESC);

    public static final String BT_OP_BIN_DELTA_UPDATES_NAME =
        "nBinDeltaUpdate";
    public static final String BT_OP_BIN_DELTA_UPDATES_DESC =
        "Number of updates performed in BIN-deltas";
    public static final StatDefinition BT_OP_BIN_DELTA_UPDATES =
        new StatDefinition(
            BT_OP_BIN_DELTA_UPDATES_NAME,
            BT_OP_BIN_DELTA_UPDATES_DESC);

    public static final String BT_OP_BIN_DELTA_DELETES_NAME =
        "nBinDeltaDelete";
    public static final String BT_OP_BIN_DELTA_DELETES_DESC =
        "Number of deletions performed in BIN-deltas";
    public static final StatDefinition BT_OP_BIN_DELTA_DELETES =
        new StatDefinition(
            BT_OP_BIN_DELTA_DELETES_NAME,
            BT_OP_BIN_DELTA_DELETES_DESC);
}
