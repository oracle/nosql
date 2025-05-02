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
import java.util.Set;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.StartNode;
import oracle.kv.impl.admin.plan.task.WaitForNodeState;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Start the given set of RepNodes.
 */
public class StartRepNodesPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    private Set<RepNodeId> repNodeIds;

    protected StartRepNodesPlan(String name,
                                Planner planner,
                                Topology topology,
                                Set<RepNodeId> rnids) {
        super(name, planner);
        repNodeIds = rnids;

        /*
         * Add all the start tasks first. TODO: in the future, these could be
         * started in parallel.
         */
        for (RepNodeId rnid : rnids) {
            RepNode rn = topology.get(rnid);

            if (rn == null) {
                throw new IllegalCommandException
                    ("There is no RepNode with id " + rnid +
                     ". Please provide the id of an existing RepNode.");
            }

            addTask(new StartNode(this, rn.getStorageNodeId(), rnid, true));
        }

        /* Add the wait tasks in a second phase. */
        for (RepNodeId rnid : rnids) {
            addTask(new WaitForNodeState
                    (this, rnid, ServiceStatus.RUNNING));
        }
    }

    public Set<RepNodeId> getRepNodeIds() {
       return repNodeIds;
    }

    @Override
    public String getDefaultName() {
        return "Start RepNodes";
    }

    @Override
    public void stripForDisplay() {
        repNodeIds = null;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
