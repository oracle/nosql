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

package com.sleepycat.je.util;

import com.sleepycat.je.utilint.StatDefinition;

public class DbVerifyStatDefinition {

    public static final String DB_VERIFY_GROUP_NAME = "DbVerify";
    public static final String DB_VERIFY_GROUP_DESC =
        "DbVerify running stats";

    public static final String DBVERIFY_RUNS_NAME =
            "nDbVerifyRuns";
    public static final String DBVERIFY_RUNS_DESC =
            "Number of times that DbVerify has been called.";
    public static final StatDefinition DBVERIFY_RUNS =
            new StatDefinition(
                DBVERIFY_RUNS_NAME,
                DBVERIFY_RUNS_DESC,
                StatDefinition.StatType.CUMULATIVE);

    public static final String DBVERIFY_RUN_TIME_NAME =
            "DbVerifyRunTime";
    public static final String DBVERIFY_RUN_TIME_DESC =
            "Total run time of DBVerify";
    public static final StatDefinition DBVERIFY_RUN_TIME =
            new StatDefinition(
                    DBVERIFY_RUN_TIME_NAME,
                    DBVERIFY_RUN_TIME_DESC,
                    StatDefinition.StatType.CUMULATIVE);

    public static final String DBVERIFY_PROBLEMS_FOUND_NAME =
            "nDbVerifyProblemsFound";
    public static final String DBVERIFY_PROBLEMS_FOUND_DESC =
            "Total number of problems that DbVerify found among all execution";
    public static final StatDefinition DBVERIFY_PROBLEMS_FOUND =
            new StatDefinition(
                    DBVERIFY_PROBLEMS_FOUND_NAME,
                    DBVERIFY_PROBLEMS_FOUND_DESC,
                    StatDefinition.StatType.CUMULATIVE);


    public static StatDefinition ALL[] = {
            DBVERIFY_RUNS,
            DBVERIFY_RUN_TIME,
            DBVERIFY_PROBLEMS_FOUND,
    };

}
