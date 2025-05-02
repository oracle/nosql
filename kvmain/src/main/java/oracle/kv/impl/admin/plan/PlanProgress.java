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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.util.FormatUtils;
import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.ObjectNode;

/**
 * Information about plan progress, for monitoring.
 */
public class PlanProgress implements Measurement, Serializable {
    private static final long serialVersionUID = 1L;

    private final int planId;
    private final String planName;
    private final Plan.State status;
    private final long time;
    private final int attemptNumber;
    private final Map<String, Integer> totalTasks;
    private final Map<String, Integer> successfulTasks;
    private final Map<String, Integer> errorTasks;
    private final Map<String, Integer> interruptedTasks;
    private final Map<String, Integer> runningTasks;
    private final Map<String, Integer> notStartedTasks;

    public PlanProgress(Plan plan) {
        this.planId = plan.getId();
        this.planName = plan.getName();
        this.status = plan.getState();
        this.time = System.currentTimeMillis();
        totalTasks = new HashMap<String, Integer>();
        successfulTasks = new HashMap<String, Integer>();
        errorTasks = new HashMap<String, Integer>();
        interruptedTasks = new HashMap<String, Integer>();
        runningTasks = new HashMap<String, Integer>();
        notStartedTasks = new HashMap<String, Integer>();
        PlanRun planRun = plan.getExecutionState().getLatestPlanRun();
        if (planRun == null) {
            this.attemptNumber = 0;
            return;
        }
        this.attemptNumber = planRun.getAttemptNumber();
        List<TaskRun> allStarted = planRun.getTaskRuns();
        int nStarted = allStarted.size();
        for (TaskRun tRun : allStarted) {
            incrementTaskCount(tRun.getTask(), totalTasks);
            if (tRun.getState().equals(Task.State.RUNNING)) {
                incrementTaskCount(tRun.getTask(), runningTasks);
            } else if (tRun.getState() == Task.State.SUCCEEDED) {
                incrementTaskCount(tRun.getTask(), successfulTasks);
            } else if (tRun.getState() == Task.State.ERROR) {
                incrementTaskCount(tRun.getTask(), errorTasks);
            } else if (tRun.getState() == Task.State.INTERRUPTED) {
                incrementTaskCount(tRun.getTask(), interruptedTasks);
            }
        }
        List<Task> pending = PlanExecutor.getFlatTaskList(plan, nStarted);
        for (Task task : pending) {
            incrementTaskCount(task, totalTasks);
            incrementTaskCount(task, notStartedTasks);
        }
    }

    private void incrementTaskCount(Task task, Map<String, Integer> taskCount) {
        final String taskType = task.getTaskProgressType();
        final Integer count = taskCount.get(taskType);
        if (count == null) {
            taskCount.put(taskType, 1);
        } else {
            taskCount.put(taskType, count + 1);
        }
    }

    @Override
    public long getStart() {
        return time;
    }

    @Override
    public long getEnd() {
        return time;
    }

    @Override
    public int getId() {
        return Metrics.PLAN_STATE.getId();
    }

    @Override
    public String toString() {
        String show = "PlanProgress [id=" + planId +
            " name=" + planName +
            " state=" + status +
            " reportTime=" + FormatUtils.formatDateTime(time) +
            " numAttempts=" + attemptNumber;

        for (Entry<String, Integer> entry : totalTasks.entrySet()) {
            show += " " + entry.getKey() + "_Total=" + entry.getValue();
        }
        for (Entry<String, Integer> entry : successfulTasks.entrySet()) {
            show += " " + entry.getKey() + "_Successful=" + entry.getValue();
        }
        for (Entry<String, Integer> entry : errorTasks.entrySet()) {
            show += " " + entry.getKey() + "_Failed=" + entry.getValue();
        }
        for (Entry<String, Integer> entry : interruptedTasks.entrySet()) {
            show += " " + entry.getKey() + "_Interrupted=" + entry.getValue();
        }
        for (Entry<String, Integer> entry : runningTasks.entrySet()) {
            show += " " + entry.getKey() + "_Running=" + entry.getValue();
        }
        for (Entry<String, Integer> entry : notStartedTasks.entrySet()) {
            show += " " + entry.getKey() + "_NotStarted=" + entry.getValue();
        }

        show +="]";
        return show;
    }

    public String toJsonString() {
        try {
            ObjectNode jsonRoot = JsonUtils.createObjectNode();
            jsonRoot.put("planId", planId);
            jsonRoot.put("planName", planName);
            jsonRoot.put("reportTime", time);
            jsonRoot.put("state", status.toString());
            jsonRoot.put("attemptNumber", attemptNumber);
            for (Entry<String, Integer> entry : totalTasks.entrySet()) {
                jsonRoot.put(entry.getKey() + "_Total", entry.getValue());
            }
            for (Entry<String, Integer> entry : successfulTasks.entrySet()) {
                jsonRoot.put(entry.getKey() + "_Successful", entry.getValue());
            }
            for (Entry<String, Integer> entry : errorTasks.entrySet()) {
                jsonRoot.put(entry.getKey() + "_Failed", entry.getValue());
            }
            for (Entry<String, Integer> entry : interruptedTasks.entrySet()) {
                jsonRoot.put(entry.getKey() + "_Interrupted", entry.getValue());
            }
            for (Entry<String, Integer> entry : runningTasks.entrySet()) {
                jsonRoot.put(entry.getKey() + "_Running", entry.getValue());
            }
            for (Entry<String, Integer> entry : notStartedTasks.entrySet()) {
                jsonRoot.put(entry.getKey() + "_NotStarted", entry.getValue());
            }
            return JsonUtils.toJsonString(jsonRoot, false);
        } catch (Exception e) {
            return "";
        }
    }
}
