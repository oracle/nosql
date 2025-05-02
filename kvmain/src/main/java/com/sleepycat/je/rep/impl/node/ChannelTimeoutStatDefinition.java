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
 * Statistics concerning NamedChannelWithTimeout connections.
 *
 */
public class ChannelTimeoutStatDefinition {

    public static final String GROUP_NAME = "ChannelTimeout";
    public static final String GROUP_DESC =
        "A channel is the network connection between a node and other nodes" +
        " in the replication group.";

    public static final String N_CHANNEL_TIMEOUT_MAP_NAME =
        "nChannelTimeoutMap";
    public static final String N_CHANNEL_TIMEOUT_MAP_DESC =
        "A map of all channels to other nodes that have timed out due to" +
        " inactivity and how many times it timed out.";
    public static final StatDefinition N_CHANNEL_TIMEOUT_MAP =
        new StatDefinition(N_CHANNEL_TIMEOUT_MAP_NAME,
        N_CHANNEL_TIMEOUT_MAP_DESC);
}
