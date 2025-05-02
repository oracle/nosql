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

import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.StartNetworkRestore;
import oracle.kv.impl.admin.plan.task.WaitForNetworkRestore;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.FormatUtils;

/**
 * Performing a plan to run network restore from source to target node.
 */
public class NetworkRestorePlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    /*
     * Using resource id instead of RepNode id for future extension for Admin,
     * though only RepNode is supported now.
     */
    private final ResourceId sourceId;
    private final ResourceId targetId;

    public NetworkRestorePlan(String planName,
                              Planner planner,
                              ResourceId sourceId,
                              ResourceId targetId,
                              boolean retainOriginalLog) {
        super(planName, planner);
        this.sourceId = sourceId;
        this.targetId = targetId;

        if (sourceId.getType() != targetId.getType()) {
            throw new IllegalCommandException(
                "Cannot restore a node from other node with different type. " +
                "Source node type is " + sourceId.getType() +
                " but target node type is " + targetId.getType());
        }
        if (!sourceId.getType().isRepNode()) {
            throw new IllegalCommandException("Only RepNode can be restored.");
        }
        if (sourceId.equals(targetId)) {
            throw new IllegalCommandException(
                "Source node " + sourceId + " and target node " + targetId +
                " is the same one.");
        }
        final RepNodeId sourceRN = (RepNodeId) sourceId;
        final RepNodeId targetRN = (RepNodeId) targetId;

        /* Verify if source and target are in the same replication group */
        if (sourceRN.getGroupId() != targetRN.getGroupId()) {
            throw new IllegalCommandException(
                "Source RepNode " + sourceRN +
                " and target RepNode " + targetRN +
                " are not in the same replication group.");
        }

        /* Verify if source and target restore nodes exist in the topology */
        StartNetworkRestore.verifyIfNodesExist(
            getAdmin().getCurrentTopology(), sourceId, targetId);

        addTask(new StartNetworkRestore(
            this, sourceId, targetId, retainOriginalLog));

        /* Parallel task runner to execute wait task until restore is done */
        final ParallelBundle bundle = new ParallelBundle();
        bundle.addTask(new WaitForNetworkRestore(this, sourceId, targetId));
        addTask(bundle);
    }

    /**
     * Add custom task status information to the TaskRun which records
     * information about each task execution. Must be synchronized on the
     * plan instance, to coordinate between different threads who are modifying
     * task state and persisting the plan instance.
     */
    public synchronized void addTaskDetails(Map<String, String> taskRunStatus,
                                            Map<String, String> info) {
        taskRunStatus.putAll(info);
    }

    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        final Topology topology = getAdmin().getCurrentTopology();

        /* Basic verification in case topology has been changed */
        StartNetworkRestore.verifyIfNodesExist(topology, sourceId, targetId);

        /* Verification before restore execution */
        StartNetworkRestore.verifyBeforeRestore((RepNodeId)sourceId,
                                                (RepNodeId)targetId,
                                                topology,
                                                getAdmin().getLoginManager(),
                                                executeLogger,
                                                force);
    }

    @Override
    public String getDefaultName() {
        return "NetworkRestore";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    /**
     * Describe running tasks, for a status report.
     */
    @Override
    public void describeRunning(Formatter fm,
                                final List<TaskRun> running,
                                boolean verbose) {

        TaskRun waitNetworkRestore = null;

        for (TaskRun tRun : running) {
            if (tRun.getTask() instanceof WaitForNetworkRestore) {
                waitNetworkRestore = tRun;
            } else {
                fm.format("   Task %d/%s started at %s\n",
                          tRun.getTaskNum(), tRun.getTask(),
                          FormatUtils.formatDateTime(tRun.getStartTime()));
            }
        }

        if (waitNetworkRestore != null) {
            final Map<String, String> details = waitNetworkRestore.getDetails();
            String stats =details.get(WaitForNetworkRestore.BACKUP_STATS_KEY);
            if (stats == null) {
                stats = "Network backup statistics unavailable";
            }

            fm.format("   Task %d/%s:\n     %s\n",
                      waitNetworkRestore.getTaskNum(),
                      waitNetworkRestore.getTask(),
                      stats);
        }
    }
}
