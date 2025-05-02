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

import java.util.HashMap;
import java.util.List;

import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.task.UpdateSoftwareVersion;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.StorageNodeId;

public class UpdateSoftwareVersionPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;

    public UpdateSoftwareVersionPlan(String name,
                                     Planner planner,
                                     HashMap<StorageNodeId, String> updates) {
        super(name, planner, true);
        addTask(new UpdateSoftwareVersion(this, updates));
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        /*
         * We take the elasticity lock because we don't want the topology to
         * change (specifically new SNs added) because we don't want to miss
         * updating anyone.
         */
        planner.lockElasticity(getId(), getName());
    }

    @Override
    public String getDefaultName() {
        return "Update Software Version plan";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }


}
