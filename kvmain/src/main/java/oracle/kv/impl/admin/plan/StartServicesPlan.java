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
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.StartAdminV2;
import oracle.kv.impl.admin.plan.task.StartNode;
import oracle.kv.impl.admin.plan.task.WaitForAdminState;
import oracle.kv.impl.admin.plan.task.WaitForNodeState;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * Start the specified services
 */
public class StartServicesPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;

    StartServicesPlan(String name,
                      Planner planner,
                      Topology topology,
                      Set<? extends ResourceId> serviceIds) {
        super(name, planner);

        final Parameters dbParams = getAdmin().getCurrentParameters();

        final ParallelBundle startTasks = new ParallelBundle();
        final ParallelBundle waitTasks = new ParallelBundle();

        for (ResourceId id : serviceIds) {

            if (id instanceof RepNodeId) {
                final RepNodeId rnId = (RepNodeId)id;
                final RepNode rn = topology.get(rnId);

                if (rn == null) {
                    throw new IllegalCommandException
                        ("There is no RepNode with id " + rnId +
                         ". Please provide the id of an existing RepNode.");
                }

                startTasks.addTask(new StartNode(this, rn.getStorageNodeId(),
                                                 rnId, true));
                waitTasks.addTask(new WaitForNodeState(this, rnId,
                                                       ServiceStatus.RUNNING));
            } else if (id instanceof AdminId) {
                final AdminId adminId = (AdminId)id;
                final AdminParams adminDbParams = dbParams.get(adminId);

                if (adminDbParams == null) {
                    throw new IllegalCommandException
                        ("There is no Admin with id " + adminId +
                         ". Please provide the id of an existing Admin.");
                }
                final StorageNodeId snId = adminDbParams.getStorageNodeId();
                if (snId == null) {
                    throw new IllegalCommandException
                        ("Storage node not found for Admin with id " + adminId);
                }
                startTasks.addTask(new StartAdminV2(this, snId, adminId, true));
                waitTasks.addTask(new WaitForAdminState(this, snId, adminId,
                                                        ServiceStatus.RUNNING));
            } else if (id instanceof ArbNodeId) {
                final ArbNodeId anId = (ArbNodeId)id;
                final ArbNode an = topology.get(anId);

                if (an == null) {
                    throw new IllegalCommandException
                        ("There is no ArbNode with id " + anId +
                         ". Please provide the id of an existing ArbNode.");
                }

                startTasks.addTask(new StartNode(this, an.getStorageNodeId(),
                                                 anId, true));
                waitTasks.addTask(new WaitForNodeState(this, anId,
                                                       ServiceStatus.RUNNING));
            } else {
                throw new IllegalCommandException
                        ("Command not supported for " + id);
            }
        }
        addTask(startTasks);
        addTask(waitTasks);
    }

    @Override
    public String getDefaultName() {
        return "Start Services";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }
}
