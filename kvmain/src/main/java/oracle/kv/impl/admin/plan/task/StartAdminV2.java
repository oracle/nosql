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

import java.util.Set;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.ChangeAdminParamsPlan;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * A task for starting a given Admin. Assumes the node has already been created.
 */
public class StartAdminV2 extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final StorageNodeId snId;
    private final AdminId adminId;
    private final boolean continuePastError;

    public StartAdminV2(AbstractPlan plan,
                        StorageNodeId storageNodeId,
                        AdminId adminId,
                        boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = storageNodeId;
        this.adminId = adminId;
        this.continuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork() throws Exception {

        final Set<AdminId> needsAction =
                (plan instanceof ChangeAdminParamsPlan) ?
                       ((ChangeAdminParamsPlan)plan).getNeedsActionSet() : null;

        /*
         * If this is a ChangeAdminParamsPlan we won't perform
         * the action unless the aid is in the needsAction set.
         */
        if ((needsAction == null) || needsAction.contains(adminId)) {
            StartAdmin.start(plan, adminId, snId);
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(adminId);
    }
}
