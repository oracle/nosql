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

import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;

import com.sleepycat.je.rep.MemberNotFoundException;

/**
 * A task for removing references to an Admin that is being removed.
 * 1. Remove the Admin from its rep group.
 * 2. Remove the relevant Parameters entry from the Admin Database.
 */
public class RemoveAdminRefs extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final AdminId victim;

    public RemoveAdminRefs(AbstractPlan plan,
                           AdminId victim) {
        this.plan = plan;
        this.victim = victim;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        if (victim.equals(admin.getParams().getAdminParams().getAdminId())) {
            throw new IllegalStateException(
                "Should not be removing references to master admin");
        }

        /*
         * Shut down the admin before removing it from the replication group if
         * we can do so without losing quorum.  That approach avoids a timing
         * window when removing the previous master where the removal happens
         * before the node in question connects to the current master as a
         * replica, causing it to fail when it finds that it has been removed.
         * [#24571]
         */
        final Parameters dbParams = admin.getCurrentParameters();
        try {
            /*
             * Wait for the admin group to be robust. Use a non-default
             * requirement since the victim could be already removed, and
             * we want to check health no matter if it was a electable or
             * not.
             */
            HealthCheck healthCheck =
                HealthCheck.create(plan.getAdmin(), toString(), victim);
            healthCheck.await((new HealthCheck.Requirements()).
                        and(HealthCheck.NODE_TYPE_VIEW_CONSISTENT).
                        and(HealthCheck.SIMPMAJ_WRITE).
                        and(HealthCheck.PROMOTING_CAUGHT_UP));

            final int primaryRepFactor =
                healthCheck.ping().get(0).getNumDataElectables();

            if (primaryRepFactor > 2) {
                StopAdmin.stop(plan, victim,
                               dbParams.get(victim).getStorageNodeId());
            }
        } catch (Exception e) {

            /*
             * The stop attempt failed, either because quorum would be lost or
             * because we couldn't contact the admin, possibly because it was
             * already stopped -- continue.
             *
             * TODO: Consider setting the electable group size override as a
             * way to maintain quorum
             */
        }

        AdminType victimAdminType = null;
        for (AdminParams adminParams : dbParams.getAdminParams()) {
            if (adminParams.getAdminId().equals(victim)) {
                victimAdminType = adminParams.getType();
                break;
            }
        }

        /*
         * Remove a primary admin from the rep group before shutting it down.
         * This sequence is necessary if there is a two-node group, so that
         * majority can be maintained until the node is removed, from JE HA's
         * perspective.
         */
        if ((victimAdminType != null) && victimAdminType.isPrimary()) {
            try {
                admin.removeAdminFromRepGroup(victim);
            } catch (MemberNotFoundException mnfe) {
                /*
                 * This would happen if the plan was interrupted and
                 * re-executed.
                 */
                plan.getLogger().log(
                    Level.INFO,
                    "{0} the admin was not found in the repgroup", this);
            }
        }

        try {
            admin.removeAdminParams(victim);
        } catch (MemberNotFoundException mnfe) {
            /* This could happen if the plan was interrupted and re-executed. */
            plan.getLogger().log(Level.INFO,
                                 "{0} the admin was not found.", this);
        }

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(victim);
    }
}
