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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.SerialBundle;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.TaskList;
import oracle.kv.impl.admin.plan.task.VerifyAdminDataV2;
import oracle.kv.impl.admin.plan.task.VerifyData;
import oracle.kv.impl.admin.plan.task.VerifyRNDataV2;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * A plan that verifies either the btree, or log files, or both of databases.
 * For btree verification, it can verify primary tables and indexes. Also, it
 * can verify data records in disk. The verification can be ran on selected
 * admins or/and rns.
 */
public class VerifyDataPlanV2 extends AbstractPlan {
    private static final long serialVersionUID = 1L;
    private final boolean showMissingFiles;

    public VerifyDataPlanV2(String name,
                          Planner planner,
                          Set<AdminId> allAdmins,
                          Set<RepNodeId> allRns,
                          VerificationOptions verifyOptions) {
        super(name, planner);
        this.showMissingFiles = verifyOptions.showMissingFiles;
        ParallelBundle parallelTasks = new ParallelBundle();

        if (allAdmins != null) {
            /*
             * Run verifications on all admins serially in the same order
             * every time.
             */
            TreeSet<AdminId> allAdminsSorted = new TreeSet<>(allAdmins);
            SerialBundle adminBundle = new SerialBundle();

            for (AdminId aid : allAdminsSorted) {
                adminBundle.addTask(new VerifyAdminDataV2(this, aid, verifyOptions));
            }
            parallelTasks.addTask(adminBundle);
        }

        if (allRns != null) {
            /*
             * Run the verify plan on all RN's of a shard in the same order
             * every time.
             */
            TreeSet<RepNodeId> allRnsSorted = new TreeSet<>(allRns);
            /* Group rns by shard */
            Map<Integer, List<RepNodeId>> idsPerShard =
                new HashMap<Integer, List<RepNodeId>>();
            for (RepNodeId id : allRnsSorted) {
                int shardNum = id.getGroupId();
                if (idsPerShard.get(shardNum) == null) {
                    idsPerShard.put(shardNum, new ArrayList<RepNodeId>());
                }
                idsPerShard.get(shardNum).add(id);
            }

            for (List<RepNodeId> rnids : idsPerShard.values()) {
                if (rnids != null) {
                    /*
                     * run verifications on RNs within the same shard serially
                     */
                    SerialBundle rnBundle = new SerialBundle();
                    for (RepNodeId rnid : rnids) {
                        rnBundle.addTask(new VerifyRNDataV2(this, rnid, verifyOptions));
                    }
                    parallelTasks.addTask(rnBundle);

                }
            }
        }
        addTask(parallelTasks);
    }

    @Override
    public String getDefaultName() {
        return "Verify Data";
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    @Override
    public ObjectNode getPlanJson() {
        ObjectNode jsonTop = JsonUtils.createObjectNode();
        ObjectNode jsonReport = jsonTop.putObject("Verify Report");
        List<TaskRun> taskRuns =
            getExecutionState().getLatestPlanRun().getTaskRuns();
        for (TaskRun t : taskRuns) {
            Task task = t.getTask();
            /*
             * Fix for KVSTORE-1747
             * keep on recursively appending the rn/admin btree/log file
             * corruption details even if the task reports ERROR status
             */
            addJson(task, jsonReport);
            if (t.getState() != Task.State.SUCCEEDED) {
                String nodeId = "" + ((VerifyData<?>)task).getNodeId();
                /*
                 * Don't overwrite if node verification details already exists
                 * and if they don't then just add the node task status
                 */
                if (!jsonReport.has(nodeId)) {
                    jsonReport.put(nodeId, t.getState().toString());
                }
            }
        }

        return jsonTop;
    }

    private void addJson(Task task, ObjectNode jsonReport) {
        TaskList nestedTaskList = task.getNestedTasks();
        if (nestedTaskList == null) {
            /*
             * Avoid NPE if node verification status is not available which
             * might happen if the node was not available during the data
             * verification plan run time
             */
            VerificationInfo verificationInfo =
                ((VerifyData<?>)task).getVerifyResult();
            if (verificationInfo != null) {
                jsonReport.merge(verificationInfo.getJson(showMissingFiles));
            }
        } else {
            for (Task t : nestedTaskList.getTasks()) {
                addJson(t, jsonReport);
            }
        }
    }
}
