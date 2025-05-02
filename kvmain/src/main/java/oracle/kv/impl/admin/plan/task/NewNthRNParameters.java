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
import java.util.List;
import java.util.logging.Level;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Send a simple newParameters call to the RepNodeAdminAPI to refresh its
 * parameters without a restart. Because the topology is not written until task
 * execution time, this flavor of NewRepNodeParameters must wait until task run
 * time to know the actual RepNodeId to use.
 */
public class NewNthRNParameters extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final int nthRN;
    private final int planShardIdx;
    private final DeployTopoPlan plan;

    /**
     * We don't have an actual RepGroupId and RepNodeId to use at construction
     * time. Look those ids up in the plan at run time.
     */
    public NewNthRNParameters(DeployTopoPlan plan,
                              int planShardIdx,
                              int nthRN) {
        this.plan = plan;
        this.planShardIdx = planShardIdx;
        this.nthRN = nthRN;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Topology topo = plan.getAdmin().getCurrentTopology();
        final RepGroupId rgId = plan.getShardId(planShardIdx);
        final List<RepNodeId> rnList = topo.getSortedRepNodeIds(rgId);
        final RepNodeId targetRNId = rnList.get(nthRN);
        final StorageNodeId hostSNId = topo.get(targetRNId).getStorageNodeId();
        plan.getLogger().log(Level.FINE, "{0} sending newParameters", this);

        final GlobalParams gp = plan.getAdmin().getParams().getGlobalParams();
        final StorageNodeParams snp =
            plan.getAdmin().getStorageNodeParams(hostSNId);
        try {
            RepNodeAdminAPI rnai =
                RegistryUtils.getRepNodeAdmin(gp.getKVStoreName(),
                                              snp.getHostname(),
                                              snp.getRegistryPort(),
                                              targetRNId,
                                              plan.getLoginManager(),
                                              plan.getLogger());

            rnai.newParameters();
        } catch (RemoteException | NotBoundException e) {
            plan.getLogger().log(Level.WARNING,
                                 "Unable to contact Rep Node Admin for {0} "+
                                 "on {1}  to refresh parameters.",
                                 new Object[]{targetRNId, hostSNId});
            return State.ERROR;
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
