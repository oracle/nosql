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
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;

public class VerifyAndRepair extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final boolean shouldContinuePastError;

    public VerifyAndRepair(AbstractPlan plan, boolean continuePastError) {
        this.plan = plan;
        this.shouldContinuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return shouldContinuePastError;
    }

    /**
     * Run a verify on the current configuration and then attempt to repair
     * any problems that are found.
     */
    @Override
    public State doWork() throws Exception {

        final Admin admin = plan.getAdmin();
        final VerifyConfiguration checker =
            new VerifyConfiguration(admin,
                                    false, /* showProgress */
                                    true, /* listAll */
                                    false, /* json */
                                    plan.getLogger());
        checker.verifyTopology();
        final VerifyResults results = checker.getResults();

        final TopologyCheck topoCheck = checker.getTopoChecker();
        final AdminId masterAdminId =
            admin.getParams().getAdminParams().getAdminId();
        final List<Remedy> remedies = checker.getRemedies(masterAdminId);
        plan.getLogger().log(Level.INFO, "{0} found repairs: {1}",
                             new Object[]{this, remedies});
        topoCheck.applyRemedies(remedies, plan);
        topoCheck.repairInitialEmptyShards(results, plan);

        return Task.State.SUCCEEDED;
    }
}
