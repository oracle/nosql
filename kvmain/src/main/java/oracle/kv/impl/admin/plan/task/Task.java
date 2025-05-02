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

import java.util.Map;
import java.util.concurrent.Callable;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.PlanProgress;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;

/**
 * A step in the process of executing a plan. Tasks are assembled (using {@link
 * TaskList}s) to carry out the changes in a plan.  Tasks are a unit of work
 * that can be repeated, canceled, and recovered during plan execution.
 *
 * All Tasks must define a method called getFirstJob that returns a Callable
 * which performs all or a portion of the task work. If getFirstJob only does a
 * portion of the work, the job will be responsible for scheduling follow on
 * work when the first job is finished.
 */
public interface Task {

    /**
     * The possible values for status of a task.
     *
     *         PENDING
     *           |
     *         RUNNING
     *           |
     *      -----+-----------
     *     /         \       \
     * INTERRUPTED SUCCEEDED ERROR
     *
     */
    public static enum State {
        PENDING,
        RUNNING,
        INTERRUPTED,
        SUCCEEDED,
        ERROR
    }

    /* For formatting */
    int LONGEST_STATE = 11;

    /**
     * Returns the name of the task.
     */
    String getName();

    /**
     * Returns true if a failure in this task should halt execution of the
     * entire plan.Some tasks always continue or stop, whereas the behavior
     * of other tasks is specified when they are constructed for a given plan.
     */
    boolean continuePastError();

    /**
     * Because of nested tasks, there may be more tasks held within the
     * umbrella of this task. Tasks which hold nested tasks do no work
     * themselves, and only include their nested children in the task count.
     */
    int getTotalTaskCount();

    /*
     * Nested tasks are used to create parallelism in the execution of plan.
     */
    TaskList getNestedTasks();

    /**
     * Returns the very first job, or phase, which will start off the task.
     * If the task support JSON command result:
     * 1. make sure to call {@link #setTaskResult(CommandResult)} before return
     * Task.State.Error.
     * 2. All exceptions should be wrapped by CommandFaultException.
     * @param taskId is used to schedule any follow on jobs.
     * @param runner is used to schedule any follow on jobs and is only
     * needed by multiphase tasks.
     */
    Callable<Task.State> getFirstJob(int taskId, TaskRunner runner)
        throws Exception;

    /**
     * If this task ends in ERROR or interrupt, it may have work to do to
     * return the store to a consistent state. This is not the same as a
     * rollback; it's permissible for a task to alter the store, and to leave
     * the store in that changed state even if an error occurs. Cleanup should
     * only be implemented if the task needs to ensure that something in the
     * store is consistent.
     * @return null if there is no cleanup to do.
     */
    Runnable getCleanupJob();

    /**
     * Obtains any required locks before plan execution, to avoid
     * conflicts in concurrent plans.
     * @throws PlanLocksHeldException
     */
    void acquireLocks(Planner planner)
        throws PlanLocksHeldException;

    /*
     * Formats any detailed information collected about the task in a way
     * that's usable for plan reporting.
     * @return information to display, or null if there is no additional
     * info.
     */
    String displayExecutionDetails(Map<String, String> details,
                                   String displayPrefix);

    /**
     * Returns true if this task does the same logical actions as otherTask.
     */
    boolean logicalCompare(Task otherTask);

    /**
     * Returns true, if the task should be restarted after returning
     * the INTERRUPTED state. Note that this value is only valid if the
     * returned state is INTERRUPTED.
     */
    boolean restartOnInterrupted();

    /**
     * Returns this task JSON command result.
     */
    CommandResult getTaskResult();

    /**
     * Makes sure to set the command result if
     * {@link #getFirstJob} returns Task.State.Error
     */
    void setTaskResult(CommandResult taskResult);

    /**
     * Returns the type of this task to use to group similar tasks when
     * reporting plan progress.
     *
     * @see PlanProgress
     */
    String getTaskProgressType();
}
