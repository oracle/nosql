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

package oracle.kv.impl.admin.plan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.DestroyAdmin;
import oracle.kv.impl.admin.plan.task.EnsureAdminNotMaster;
import oracle.kv.impl.admin.plan.task.RemoveAdminRefs;
import oracle.kv.impl.admin.plan.task.UpdateAdminHelperHost;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * A plan for removing an Admin replica.
 */
public class RemoveAdminPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    @Deprecated
    private AdminId victim;

    public RemoveAdminPlan(String name,
                           Planner planner,
                           DatacenterId dcid,
                           AdminId victim,
                           boolean failedSN) {
        super(name, planner);

        final Admin admin = planner.getAdmin();
        final Parameters parameters = admin.getCurrentParameters();

        Set<AdminId> victimIds = new HashSet<>();

        /*
         * Determine the Admins to remove based on the input parameters.
         * If both DatacenterId and AdminId are null, throw an
         * IllegalCommandException. If AdminId is not null (regardless of
         * DatacenterId), then remove only the Admin with the specified
         * AdminId. Otherwise, remove all (and only) the Admins running
         * in the datacenter with the specified DatacenterId.
         */
        if (victim == null) {
            if (dcid != null) {
                victimIds =
                    parameters.getAdminIds(dcid, admin.getCurrentTopology());
            } else {
                throw new IllegalCommandException(
                    "null specified for both DatacenterId and " +
                    "AdminId parameters");
            }
        } else {
            victimIds.add(victim);
        }
        for (AdminId victimId : victimIds) {

            final AdminParams ap = parameters.get(victimId);
            final StorageNodeId snid = ap.getStorageNodeId();

            /*
             * If running on the current victim, the first task will shut
             * down and restart that Admin, with the effect of transferring
             * mastership to a replica.  The plan will be interrupted, and
             * then must be re-executed on the new master.  On the second
             * execution, this task will be a no-op because we won't be
             * running on that victim, unless no replica was able to assume
             * mastership for some reason, and mastership has rebounded back
             * to the victim.
             */
            addTask(new EnsureAdminNotMaster(this, victimId));

            /*
             * Remove the Admin from all configurations.
             */
            addTask(new RemoveAdminRefs(this, victimId));

            /*
             * Update the helpers of the non-targeted Admins to exclude the
             * current Admin that was removed.
             */
            addTask(new UpdateAdminHelperHost(this));

            /*
             * Finally, tell the SNA to shut down the Admin and remove it from
             * its local configuration.  This step is done last so that if the
             * SNA is unavailable, the Admin can still be removed from the
             * store.  If this step fails because the SNA is unavailable, the
             * operator can choose either to re-execute the plan when the SNA
             * returns to service; or, if the SNA is expected never to return,
             * to cancel the plan.
             *
             * If operator is sure about failed SN and want to remove admins
             * hosted on failed SN then they could choose failedSN option then
             * step will not fail and they do not need to re-execute or cancel
             * the plan.
             *
             * Were such a plan to be canceled, and were the SNA then to
             * return, it would require manual editing to remove the Admin from
             * the SNA's local configuration.  No new plan to remove the Admin
             * can be created after the Admin's parameter record is removed
             * from the database, which happens in the RemoveAdminRefs task.
             *
             */
            addTask(new DestroyAdmin(this, snid, victimId, failedSN));
        }
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    @Override
    public String getDefaultName() {
        return "Remove Admin Replica";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
