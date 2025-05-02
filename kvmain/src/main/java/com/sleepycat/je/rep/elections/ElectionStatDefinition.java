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
 * Per-stat Metadata for each Election statistics.
 */
public class ElectionStatDefinition {

    public static final String GROUP_NAME = "Elections";
    public static final String GROUP_DESC =
        "stats related to the node elections ";

    public static final String N_ELECTIONS_INITIATED =
        "nElectionsInitiated";
    public static final String N_ELECTIONS_INITIATED_DESC =
        "Number of elections initiated by this node";

    public static StatDefinition ELECTIONS_INITIATED =
        new StatDefinition
        (N_ELECTIONS_INITIATED,
         N_ELECTIONS_INITIATED_DESC);
}
