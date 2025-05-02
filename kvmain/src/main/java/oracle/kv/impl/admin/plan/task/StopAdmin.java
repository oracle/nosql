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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.ChangeAdminParamsPlan;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * A task for stopping a given Admin
 */
public class StopAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final StorageNodeId snId;
    private final AdminId adminId;
    private final boolean continuePastError;

    /**
     * We expect that the target Admin exists before StopAdmin is
     * executed.
     * @param continuePastError if true, if this task fails, the plan
     * will stop.
     */
    public StopAdmin(AbstractPlan plan,
                     StorageNodeId snId,
                     AdminId adminId,
                     boolean continuePastError) {
        super();
        this.plan = plan;
        this.snId = snId;
        this.adminId = adminId;
        this.continuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        boolean needsAction = true;
        if (plan instanceof ChangeAdminParamsPlan) {
            needsAction = ((ChangeAdminParamsPlan) plan).
                getNeedsActionSet().contains(adminId);
        }

        /*
         * We won't perform the action unless the aid is set to NO needsAction
         * by a ChangeAdminParamsPlan.
         */
        if (needsAction) {
            return stop(plan, adminId, snId);
        }
        return State.SUCCEEDED;
    }

    /**
     * Stops the specified Admin. Returns SUCCESS if the Admin was remote
     * and successfully stopped. Returns INTERRUPTED if the Admin to be stopped
     * is the one running this task.
     */
    public static State stop(final AbstractPlan plan,
                             AdminId adminId,
                             StorageNodeId snId)
        throws RemoteException, NotBoundException {
        final Admin admin = plan.getAdmin();

        /*
         * If we are running on the admin to be shut down, just shut down
         * without directly involving the SNA.  The SNA will restart the
         * AdminService immediately, but the mastership will transfer to
         * a replica, if there are any.
         */
        if (adminId.equals
                    (admin.getParams().getAdminParams().getAdminId())) {

            new StoppableThread("StopAdminThread") {
                @Override
                public void run() {
                    admin.awaitPlan(plan.getId(), 10, TimeUnit.SECONDS);
                    admin.stopAdminService(false, plan.toString());
                }
                @Override
                protected Logger getLogger() {
                    return plan.getLogger();
                }
            }.start();

            return State.INTERRUPTED;
        }

        /*
         * Update the admin params to indicate that this node is now
         * disabled, and save the changes.
         */
        final Parameters parameters = admin.getCurrentParameters();
        final AdminParams ap = parameters.get(adminId);
        ap.setDisabled(true);
        admin.updateParams(ap);

        /* Tell the SNA to start the admin. */
        final StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent(parameters, snId,
                                              plan.getLoginManager(),
                                              plan.getLogger());

        sna.stopAdmin(false /* force */, plan.toString());
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return continuePastError;
    }

    @Override
    public boolean restartOnInterrupted() {
        return true;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(adminId);
    }

    public ResourceId getResourceId() {
        return adminId;
    }
}
