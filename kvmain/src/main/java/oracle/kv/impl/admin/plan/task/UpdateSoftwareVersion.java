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
import java.util.HashMap;
import java.util.Map.Entry;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.UpdateSoftwareVersionPlan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

public class UpdateSoftwareVersion extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final HashMap<StorageNodeId, String> updates;

    /**
     * Constructor.
     * @param plan the plan
     */
    public UpdateSoftwareVersion(UpdateSoftwareVersionPlan plan,
                                 HashMap<StorageNodeId, String> updates) {
        this.plan = plan;
        this.updates = updates;
    }

    @Override
    public State doWork() throws Exception {
        update(plan.getAdmin());
        return State.SUCCEEDED;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        for (StorageNodeId snId : updates.keySet()) {
            LockUtils.lockSN(planner, plan, snId);
        }
    }

    private void update(Admin admin) {
        final Topology topo = admin.getCurrentTopology();
        final Parameters params = admin.getCurrentParameters();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginMgr, plan.getLogger());

        for (Entry<StorageNodeId, String>val : updates.entrySet()) {
            StorageNodeParams snp = params.get(val.getKey());
            ParameterMap snParams = snp.getMap();

            snParams.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                                  val.getValue());
            admin.updateParams(snp, null);

            ParameterMap pm = new ParameterMap();
            pm.setType(snParams.getType());
            pm.setName(snParams.getName());
            pm.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                            val.getValue());
            try {
                StorageNodeAgentAPI sna =
                    regUtils.getStorageNodeAgent(val.getKey());
                sna.newStorageNodeParameters(pm);
            }  catch (NotBoundException | RemoteException e) {
            }
        }
    }
}
