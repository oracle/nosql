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
import java.util.Set;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.ChangeAdminParamsPlan;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for starting a given Admin. Assumes the node has already been created.
 *
 * NOTE: This class is for backward compatibility only, it has been replaced
 * by StartAdminV2.
 */
public class StartAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private ChangeAdminParamsPlan plan;
    private StorageNodeId snId;
    private AdminId adminId;
    private boolean continuePastError;

    /*
     * Unused constructor to clarify that no instances should be created.
     * Callers should use StartAdminV2.
    */
    @SuppressWarnings("unused")
    private StartAdmin(ChangeAdminParamsPlan plan,
                       StorageNodeId storageNodeId,
                       AdminId adminId,
                       boolean continuePastError) {
        throw new AssertionError("Use StartAdminV2");
    }

    @Override
    protected ChangeAdminParamsPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Set<AdminId> needsAction = plan.getNeedsActionSet();

        /*
         * We won't perform the action unless the aid is in the needsAction set.
         */
        if (needsAction.contains(adminId)) {
            start(plan, adminId, snId);
        }
        return State.SUCCEEDED;
    }

    /*
     * Starts the specified Admin.
     */
    public static void start(AbstractPlan plan,
                             AdminId adminId,
                             StorageNodeId snId)
        throws RemoteException, NotBoundException {
        /*
         * Update params to indicate that this admin is now enabled,
         * and save the changes.
         */
        final Admin admin = plan.getAdmin();
        final Parameters parameters = admin.getCurrentParameters();
        final AdminParams ap = parameters.get(adminId);
        ap.setDisabled(false);
        admin.updateParams(ap);

        /* Tell the SNA to start the admin. */
        final StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent(parameters, snId,
                                              plan.getLoginManager(),
                                              plan.getLogger());

        sna.startAdmin();
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
