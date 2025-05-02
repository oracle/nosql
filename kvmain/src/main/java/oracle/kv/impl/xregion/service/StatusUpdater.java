/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.xregion.service;

import java.util.Set;

import oracle.kv.impl.xregion.agent.RegionAgentStatus;

/**
 * Interface the post the service agent status.
 */
public interface StatusUpdater {

    /**
     * Posts the status of a region agent when it changes.
     *
     * @param region   region of the agent
     * @param status   status of the agent
     * @param tables   affected tables
     */
    void post(RegionInfo region, RegionAgentStatus status, Set<String> tables);
}
