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

import java.rmi.RemoteException;
import java.util.logging.Level;

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Tell the SN to delete this RN from its configuration file, and from its set
 * of managed processes.
 */
public class DeleteRepNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final StorageNodeId snId;
    private final RepNodeId rnId;
    private final AbstractPlan plan;

    public DeleteRepNode(AbstractPlan plan,
                         StorageNodeId snId,
                         RepNodeId rnId) {
        this.plan = plan;
        this.snId = snId;
        this.rnId = rnId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        try {
            final RegistryUtils registry =
                new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                                  plan.getAdmin().getLoginManager(),
                                  plan.getLogger());
            final StorageNodeAgentAPI sna = registry.getStorageNodeAgent(snId);
            sna.destroyRepNode(rnId, true /* deleteData */, plan.toString());
            return State.SUCCEEDED;

        } catch (java.rmi.NotBoundException notbound) {
            plan.getLogger().log(Level.INFO,
                                 "{0} {1} cannot be contacted to " +
                                 "delete {2}:{3}",
                                 new Object[]{this, snId, rnId, notbound});
        } catch (RemoteException e) {
            plan.getLogger().log(Level.SEVERE,
                                 "{0} attempting to delete {1} from {2}: {3}",
                                 new Object[]{this, rnId, snId, e});
        }
        return State.ERROR;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        final StorageNodeParams snp =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(snId) : null);
        return super.getName(sb).append(" remove ").append(rnId)
                                .append(" from ")
                                .append(snp != null ? snp.displaySNIdAndHost() :
                                                      snId);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    /* Lock the target RN */
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockRN(planner, plan, rnId);
    }
}
