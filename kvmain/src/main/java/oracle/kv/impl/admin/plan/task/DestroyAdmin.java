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
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for asking a storage node to destroy an Admin that it hosts.
 */
public class DestroyAdmin extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final StorageNodeId snid;
    private final AdminId victim;
    private final boolean failedSN;

    public DestroyAdmin(AbstractPlan plan,
                        StorageNodeId snid,
                        AdminId victim,
                        boolean failedSN) {
        this.plan = plan;
        this.snid = snid;
        this.victim = victim;
        this.failedSN = failedSN;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Parameters parameters = admin.getCurrentParameters();
        StorageNodeAgentAPI sna = null;
        try {
            sna = RegistryUtils.getStorageNodeAgent(parameters, snid,
                                                    plan.getLoginManager(),
                                                    plan.getLogger());
        } catch (RemoteException | NotBoundException e) {
            /*
             * OK if the SNA isn't available in the failed SN case
             */
            if (!failedSN) {
                throw e;
            }
        }

        if (sna != null) {
            sna.destroyAdmin(victim, true /* deleteData */, plan.toString());
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
    
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, snid);
    }
}
