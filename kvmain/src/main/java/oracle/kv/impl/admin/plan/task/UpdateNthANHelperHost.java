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

import java.util.List;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

/**
 * A task for asking a ArbNode to update its helper hosts to include all its
 * peers.Because the topology is not written until task execution time, this
 * flavor of UpdateNthANHelperHost must wait until task run time to know the
 * actual RepGroupId and ArbNodeId to use.
 */
public class UpdateNthANHelperHost extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final DeployTopoPlan plan;
    private final int planShardIdx;
    private final int nthAN;

    public UpdateNthANHelperHost(DeployTopoPlan plan,
                                 int planShardIdx,
                                 int nthAN) {

        super();
        this.plan = plan;
        this.planShardIdx = planShardIdx;
        this.nthAN = nthAN;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        final RepGroupId rgId = plan.getShardId(planShardIdx);
        final List<ArbNodeId> anList = topo.getSortedArbNodeIds(rgId);
        final ArbNodeId anId = anList.get(nthAN);

        Utils.updateHelperHost(admin, topo, topo.get(rgId),
                               anId, plan.getLogger());
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
