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

package oracle.kv.impl.admin.plan.task;

import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Check if a storage node is alive or stopped..
 */
public class ConfirmSNStatus extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final StorageNodeId snId;
    private final TopologyPlan plan;
    private final boolean shouldBeRunning;
    private final String infoMsg;

    public ConfirmSNStatus(TopologyPlan plan,
                           StorageNodeId snId,
                           boolean shouldBeRunning,
                           String infoMsg) {
        super();
        this.snId = snId;
        this.plan = plan;
        this.shouldBeRunning = shouldBeRunning;
        this.infoMsg = infoMsg;
    }

    @Override
    protected TopologyPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        Utils.confirmSNStatus(plan.getTopology(),
                              plan.getLoginManager(),
                              plan.getLogger(),
                              snId,
                              shouldBeRunning,
                              infoMsg);
        return Task.State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
