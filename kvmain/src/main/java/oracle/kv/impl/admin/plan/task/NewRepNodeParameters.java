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
import java.util.logging.Level;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Send a simple newParameters call to the RepNodeAdminAPI to refresh its
 * parameters without a restart.
 */
public class NewRepNodeParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    @Deprecated
    private final StorageNodeId hostSNId = null;
    
    private final RepNodeId targetNodeId;
    private final AbstractPlan plan;

    public NewRepNodeParameters(AbstractPlan plan,
                                RepNodeId targetNodeId) {
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

        plan.getLogger().log(Level.FINE, "{0} sending newParameters", this);

        try {
            final RegistryUtils registry =
                new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                                  plan.getAdmin().getLoginManager(),
                                  plan.getLogger());
            final RepNodeAdminAPI rnAdmin =
                registry.getRepNodeAdmin(targetNodeId);
            rnAdmin.newParameters();
        } catch (RemoteException | NotBoundException e) {
            plan.getLogger().log(Level.WARNING,
                                 "{0} attempting to update parameters: {1}",
                                 new Object[]{this, e});
              return State.ERROR;
        }
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        final StorageNodeParams snp =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(hostSNId) : null);
        super.getName(sb).append(" refresh ").append(targetNodeId);
        if (snp != null) {
            sb.append(" on ").append(snp.displaySNIdAndHost());
        }
        return sb.append(" parameter state without restarting");
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
