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
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Topology;

/**
 * Update the topology to remove the desired datacenter. This is purely a
 * change to the admin database. It is assumed that the target datacenter is
 * already empty, and that there is no need to issue remote requests to stop
 * storage nodes belonging to that datacenter.
 */
public class RemoveDatacenter extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final DatacenterId targetId;
    private final TopologyPlan plan;

    public RemoveDatacenter(TopologyPlan plan, DatacenterId targetId) {

        super();
        this.plan = plan;
        this.targetId = targetId;
    }

    @Override
    protected TopologyPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        /*
         * At this point, it has been determined that the datacenter is
         * empty. So the target datacenter can be removed from the topology.
         */

        final Topology currentTopo = plan.getTopology();
        currentTopo.remove(targetId);

        /*
         * Save the modified topology to the administrative db to preserve a
         * consistent view of the change. Note that if this plan has been
         * retried it's possible that the topology created by this task has
         * already been saved.
         */
        if (plan.isFirstExecutionAttempt()) {
            plan.getAdmin().saveTopoAndRemoveDatacenter(currentTopo,
                                                        plan.getDeployedInfo(),
                                                        targetId,
                                                        plan);
        }

        /* Send topology changes to all nodes.*/
        if (!Utils.broadcastTopoChangesToRNs
            (plan.getLogger(),
             currentTopo,
             "remove zone [id=" + targetId + "]",
             plan.getAdmin().getParams().getAdminParams(),
             plan)) {
            return Task.State.INTERRUPTED;
        }
        return Task.State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(targetId);
    }
}
