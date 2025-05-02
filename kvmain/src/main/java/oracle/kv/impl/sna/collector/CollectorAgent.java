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

package oracle.kv.impl.sna.collector;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;

/**
 * This is the interface for agent in SNA collector service.
 */
public interface CollectorAgent {

    /**
     * Agent start to schedule its collector tasks. Throw Exception if agent
     * fail to schedule tasks. After agent is started, the task executor and
     * scheduled task must not throw any Exception. Agent should catch failure
     * and retry task instead of quit implicitly. 
     */
    public void start();

    /**
     * Agent stop scheduled collector tasks and quit.
     */
    public void stop();

    /**
     * notify new global parameters and SN parameters.
     */
    public void updateParams(GlobalParams newGlobalParams,
                             StorageNodeParams newSNParams);
}
