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

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * A task for stopping a given RepNode or ArbNode
 */
public class StopNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final StorageNodeId snId;
    private final ResourceId resId;
    private final boolean continuePastError;

    /**
     * We expect that the target Node exists before StopNode is
     * executed.
     * @param continuePastError if true, if this task fails, the plan
     * will stop.
     */
    public StopNode(AbstractPlan plan,
                    StorageNodeId snId,
                    ArbNodeId anId,
                    boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = snId;
        this.resId = anId;
        this.continuePastError = continuePastError;
    }

    public StopNode(AbstractPlan plan,
                    StorageNodeId snId,
                    RepNodeId rnId,
                    boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = snId;
        this.resId = rnId;
        this.continuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        if (resId.getType().isRepNode()) {
            /*
             * TODO - Survey usages of this task to see if it should wait for
             * nodes to be consistent, stopRN(..., true).
             *
             * Currently this task is used by the following plans to stop rns:
             * ChangeANParamsPlan, ChangeSNParamsPlan, ChangeParamsPlan,
             * StopRepNodesPlan, StopServicesPlan.
             *
             * With [#22425], StopRepNodesPlan and StopServicesPlan will do
             * health check before the execution of the plan. Therefore, a
             * check here only adds a slight safety for the period between the
             * health check and the time the node is stopped. Furthermore, to
             * add a check here, we need to pass in the force flag which
             * requires to take the upgrade precaution. For these two reasons,
             * we do not do health check for StopServicesPlan in this task.
             */
            Utils.stopRN(plan, snId, (RepNodeId)resId,
                    false, /* not await for healthy */
                    false /* not failure */);
        } else {
            Utils.disableAndStopAN(plan, snId, (ArbNodeId)resId);
        }

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(resId);
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockANRN(planner, plan, resId);
    }

    public ResourceId getResourceId() {
        return resId;
    }
}
