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


import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.topo.AdminId;

/**
 * Update the helper hosts on an Admin instance.
 */
public class UpdateAdminHelperHost extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    /*
     * aid is no longer used, but it is kept here
     * for serialization compatibility.
     */
    @Deprecated
    private final AdminId aid;

    public UpdateAdminHelperHost(AbstractPlan plan) {
        super();
        this.plan = plan;
        this.aid = null;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Parameters p = admin.getCurrentParameters();

        if (!Utils.updateAdminHelpersAndNotify(admin,p, plan)) {
            return State.ERROR;
        }

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(aid);
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        final Admin admin = plan.getAdmin();
        final Parameters p = admin.getCurrentParameters();
        for (AdminParams ap : p.getAdminParams()) {
            LockUtils.lockSN(planner, plan, ap.getStorageNodeId());
        }
    }
}
