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

package com.sleepycat.je.rep.elections;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for each Learner statistics.
 */
public class LearnerStatDefinition {

    public static final String GROUP_NAME = "Learner";
    public static final String GROUP_DESC =
        "stats related to the learner of the election";

    public static final String N_MASTER_LEARNED =
        "nMasterLearned";
    public static final String N_MASTER_LEARNED_DESC =
        "Number of times learner discovered the master";

    public static StatDefinition MASTER_LEARNED =
        new StatDefinition
        (N_MASTER_LEARNED,
         N_MASTER_LEARNED_DESC);
}
