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
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for creating and starting an instance of an Admin service on a
 * StorageNode.
 */
public class UpdateESConnectionInfo extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<Integer> FAULT_HOOK;

    private final AbstractPlan plan;
    private final StorageNodeId snid;
    private final String clusterName;
    private final String allTransports;
    private final boolean secure;

    public UpdateESConnectionInfo(AbstractPlan plan,
                                  StorageNodeId snid,
                                  String clusterName,
                                  String allTransports,
                                  boolean secure) {
        super();
        this.plan = plan;
        this.snid = snid;
        this.clusterName = clusterName;
        this.allTransports = allTransports;
        this.secure = secure;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Parameters p = admin.getCurrentParameters();
        final StorageNodeParams snp = p.get(snid);
        snp.setSearchClusterMembers(allTransports);
        snp.setSearchClusterName(clusterName);
        snp.setSearchClusterSecure(secure);
        admin.updateParams(snp, null);

        plan.getLogger().log(Level.INFO,
                             "{0} changed searchClusterMembers for {1} to {2}",
                             new Object[]{this, snid, allTransports});

        /* Tell the SNA about it. */
        final StorageNodeAgentAPI sna =
            RegistryUtils.getStorageNodeAgent(p, snid,
                                              plan.getLoginManager(),
                                              plan.getLogger());
        sna.newStorageNodeParameters(snp.getMap());

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
    
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, snid);
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(snid);
    }
    
    public StorageNodeId getSNId() {
        return snid;
    }
}
