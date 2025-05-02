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

package com.sleepycat.je.rep.stream;

import com.sleepycat.je.utilint.StatDefinition;

/**
 * Per-stat Metadata for HA Arbiter feeder statistics.
 */
public class ArbiterFeederStatDefinition {

    public static final String GROUP_NAME = "ArbiterFeeder";
    public static final String GROUP_DESC = "ArbiterFeeder statistics";

    public static StatDefinition QUEUE_FULL =
        new StatDefinition("queueFull", "Number of times a item could " +
                           "not be queued because the queue was full.");
}
