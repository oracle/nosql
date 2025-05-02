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

import java.util.List;

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.task.LockUtils;
import oracle.kv.impl.admin.plan.task.VerifyAndRepair;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;

/**
 * A plan which takes issues reported by VerifyConfiguration.verifyTopology and
 * attempts to fix any reported problems.
 */
public class RepairPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private transient DeploymentInfo deploymentInfo;

    public RepairPlan(String planName, Planner planner) {
        super(planName, planner);
        addTask(new VerifyAndRepair(this, false /* continuePastError */));
    }

    @Override
        public String getDefaultName() {
        return "Repair Topology";
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
        final Topology topo = planner.getAdmin().getCurrentTopology();
        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            LockUtils.lockSN(planner, this, sn.getStorageNodeId());
        }
    }

    @Override
    public DeploymentInfo getDeployedInfo() {
        return deploymentInfo;
    }

    @Override
    synchronized PlanRun startNewRun() {
        deploymentInfo = DeploymentInfo.makeDeploymentInfo
                (this, TopologyCandidate.NO_NAME);
        return super.startNewRun();
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}