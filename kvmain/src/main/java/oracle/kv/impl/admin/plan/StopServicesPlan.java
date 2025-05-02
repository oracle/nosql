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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.HealthCheck;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.StopAdmin;
import oracle.kv.impl.admin.plan.task.StopNode;
import oracle.kv.impl.admin.plan.task.Task;
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

/**
 * Stop the given set of services.
 */
public class StopServicesPlan extends AbstractPlan {
    private static final long serialVersionUID = 1L;

    StopServicesPlan(String name,
                     Planner planner,
                     Topology topology,
                     Set<? extends ResourceId> serviceIds) {

        super(name, planner);

        final Parameters dbParams = getAdmin().getCurrentParameters();

        final ParallelBundle stopRNANTasks = new ParallelBundle();
        final ParallelBundle stopAdminTasks = new ParallelBundle();

        for (ResourceId id : serviceIds) {

            if (id instanceof RepNodeId) {
                final RepNodeId rnId = (RepNodeId)id;
                final RepNode rn = topology.get(rnId);

                if (rn == null) {
                    throw new IllegalCommandException
                        ("There is no RepNode with id " + rnId +
                         ". Please provide the id of an existing RepNode.");
                }
                stopRNANTasks.addTask(new StopNode(this, rn.getStorageNodeId(),
                                                   rnId, true));
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
                stopAdminTasks.addTask(new StopAdmin(this, snId,
                                                     adminId, true));
            }  else if (id instanceof ArbNodeId) {
                final ArbNodeId anId = (ArbNodeId)id;
                final ArbNode an = topology.get(anId);

                if (an == null) {
                    throw new IllegalCommandException
                        ("There is no ArbNode with id " + anId +
                         ". Please provide the id of an existing ArbNode.");
                }
                stopRNANTasks.addTask(new StopNode(this, an.getStorageNodeId(),
                                                    anId, true));
            } else {
                throw new IllegalCommandException
                        ("Command not supported for " + id);
            }
        }

        /*
         * Stop the RNs first, just in case stopping Admins
         * cause loss of quorum, stopping the plan.
         */
        addTask(stopRNANTasks);
        addTask(stopAdminTasks);
    }

    @Override
    public void preExecuteCheck(boolean force, Logger plannerLogger) {
        if (force) {
            return;
        }

        HealthCheck.
            create(getAdmin(), toString(), getServiceIdsFromTasks()).
            verify((new HealthCheck.Requirements()).
                    and(HealthCheck.SIMPMAJ_RUNNING));
    }

    @Override
    public String getDefaultName() {
        return "Stop Services";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    public Set<ResourceId> getServiceIdsFromTasks() {
        Set<ResourceId> result = new HashSet<ResourceId>();
        for (Task parentTask : getTaskList().getTasks()) {
            ParallelBundle bundle = (ParallelBundle) parentTask;
            for (Task task : bundle.getNestedTasks().getTasks()) {
                if (task instanceof StopNode) {
                    result.add(((StopNode) task).getResourceId());
                } else if (task instanceof StopAdmin) {
                    result.add(((StopAdmin) task).getResourceId());
                }
            }
        }
        return result;
    }

}
