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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;

/**
 * A task for asking a ArbNode to update its helper hosts to include all its
 * peers.
 */
public class UpdateHelperHostV2 extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final ResourceId resId;

    private final RepGroupId rgId;

    public UpdateHelperHostV2(AbstractPlan plan,
                              ResourceId resId,
                              RepGroupId rgId) {
        super();
        this.plan = plan;
        this.resId = resId;
        this.rgId = rgId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {
        RepGroup rg =  plan.getAdmin().getCurrentTopology().get(rgId);
        if (resId.getType().isRepNode()) {
            Utils.updateHelperHost(plan.getAdmin(),
                                   plan.getAdmin().getCurrentTopology(),
                                   rg,
                                   (RepNodeId)resId,
                                   plan.getLogger());
        } else {
            Utils.updateHelperHost(plan.getAdmin(),
                                   plan.getAdmin().getCurrentTopology(),
                                   rg,
                                   (ArbNodeId)resId,
                                   plan.getLogger());
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
