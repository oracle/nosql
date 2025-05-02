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
import oracle.kv.impl.admin.SnConsistencyUtils;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.UpdateRepNodeParams;
import oracle.kv.impl.admin.plan.task.WriteNewANParams;
import oracle.kv.impl.admin.plan.task.WriteNewGlobalParams;
import oracle.kv.impl.admin.plan.task.WriteNewSNParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

public class SNParameterConsistencyPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;
    private final StorageNodeId snId;

   public
        SNParameterConsistencyPlan(String name,
                                   Planner planner,
                                   StorageNodeId snId,
                                   SnConsistencyUtils.ParamCheckResults pcr) {

        super(name, planner, true);
        this.snId = snId;

        /* generate tasks */
        if (pcr.getGlobalDiff()) {
            addTask(new WriteNewGlobalParams(this, null, snId, true));
        }

        for (ResourceId resId : pcr.getDiffs()) {
            if (resId instanceof StorageNodeId) {
                ParameterMap snMap =
                    getAdmin().getStorageNodeParams(snId).getMap();
                addTask(new WriteNewSNParams(this, snId, snMap, true));
            } else if (resId instanceof RepNodeId) {
                addTask(new UpdateRepNodeParams(this,
                                                (RepNodeId)resId, false,
                                                true, true));
                addTask(new NewRepNodeParameters(this, (RepNodeId)resId));
            } else if (resId instanceof ArbNodeId) {
                ArbNodeId anId = (ArbNodeId)resId;
                ParameterMap anMap =
                    getAdmin().getArbNodeParams(anId).getMap();
                addTask(new WriteNewANParams(this, anMap, anId, snId, true));
            }
        }
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        planner.lockElasticity(getId(), getName());
    }

    @Override
    public String getDefaultName() {
        return "SN parameter consistency plan for " + snId;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

}
