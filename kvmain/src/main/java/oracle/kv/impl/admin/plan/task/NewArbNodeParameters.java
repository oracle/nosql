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

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Send a simple newParameters call to the ArbNodeAdminAPI to refresh its
 * parameters without a restart.
 */
public class NewArbNodeParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final ArbNodeId targetNodeId;
    private final AbstractPlan plan;

    public NewArbNodeParameters(AbstractPlan plan,
                                ArbNodeId targetNodeId) {
        this.plan = plan;
        this.targetNodeId = targetNodeId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        plan.getLogger().log(Level.FINE,
                             "{0} sending newParameters", this);

        try {
            final RegistryUtils registry =
                new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                                  plan.getAdmin().getLoginManager(),
                                  plan.getLogger());
            final ArbNodeAdminAPI anAdmin =
                    registry.getArbNodeAdmin(targetNodeId);
            anAdmin.newParameters();
        } catch (java.rmi.NotBoundException notbound) {
            plan.getLogger().log(Level.INFO,
                                 "{0} {1} cannot be contacted when updating " +
                                 "its parameters: {2}",
                                 new Object[]{this, targetNodeId, notbound});
            throw notbound;
        } catch (RemoteException e) {
            plan.getLogger().log(Level.SEVERE,
                                 "{0} attempting to update parameters: {1}",
                                 new Object[]{this, e});
            throw e;
        }
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" refresh ").append(targetNodeId)
                               .append(" parameter state without restarting");
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
