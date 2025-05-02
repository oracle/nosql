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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;


/**
 * Captures status of a single task execution. Tasks may be executed repeatedly
 * as the plan is re-executed.
 *
 * Note that any caller who modifies the TaskRun must synchronize against its
 * owning plan. This coordinates between threads that are:
 *  - concurrently updating the different tasks, because of parallel task
 *    execution
 *  - PlanExecutor threads that are saving the plan instance to the persistent
 *    store.
 *
 * Readers of the task may not necessarily need to synchronize on the plan. It
 * may not be important that the reader get the latest task information, if
 * it's only needed for logging and reporting.
 */
public class TaskRun implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String taskName;
    private final long startTime;
    private long endTime;
    private Task.State state;

    /*
     * An identifier for the task, used to locate per-task status stored in the
     * PlanRun.
     */
    private final int taskNum;

    /*
     * ExceptionTransfer packages a Throwable and an exception message.  It
     * is used to propagate the exception across thread boundaries, and also
     * PlanWaiter, and need not be stored persistently.
     */
    private transient ExceptionTransfer transfer;

    private long cleanupStartTime;
    private long cleanupEndTime;
    private String cleanupFailure;

    private final Task task;

    /*
     * Task and details were added in AdminSchemaVersion 3, for plan status
     * report support. CustomInfo holds key->value pairs that can be store
     * metrics that pertain only to a particular type of task. See
     * PartitionMigrationStatus for an example of how to encapsulate the
     * information in the map, and its creation and extraction.
     * Note that modifications of the details map, like all other modifications
     * to the plan instance, must be synchronized on the plan instance.
     */
    private final Map<String, String> details;

    TaskRun(Task task, Logger logger, int taskNum) {
        this.task = task;
        /* To be compatible with versions before AdminSchemaVersion 3 */
        this.taskName = task.getName();

        setState(Task.State.RUNNING, logger);
        startTime = System.currentTimeMillis();
        this.taskNum = taskNum;
        details = new HashMap<>();
    }

    /**
     * Change the status of this task. Should only be called when synchronized
     * on the owning plan.
     *
     * @param newState the new status of this task.
     */
    void setState(Task.State newState, Logger logger) {
        if (state == null) {
            logger.log(Level.FINE, "TaskRun {0} starting in {1}",
                       new Object[] {taskName, newState});
        } else {
            logger.log(Level.FINE, "TaskRun {0} transitioning from {1} to {2}",
                       new Object[] {taskName, state, newState});
            /*
             * An assertion against a state transition that should never
             * happen.
             */
            if ((state == Task.State.ERROR) &&
                (newState != Task.State.ERROR)) {
                throw new IllegalStateException
                ("Illegal transition from " + state + " to " +
                 newState + " for " + taskName);
            }
        }

        state = newState;
        endTime = System.currentTimeMillis();
    }

    /**
     * Save a information about a task failure. Only call when synchronized on
     * the owning plan. Note that failures can be detected from two types of
     * threads: the thread executing the task and the PlanExecutor, when it is
     * examining the future.
     */
    void saveFailure(Plan plan,
                     Throwable t,
                     String problem,
                     ErrorMessage errorMsg,
                     String[] cleanupJobs,
                     Logger logger) {

        transfer = ExceptionTransfer.newInstance(t, problem, errorMsg,
                                                 cleanupJobs);

        if (t == null) {
            logger.log(Level.WARNING,
                       "{0} of plan {1} ended in state {2} with {3}",
                       new Object[] {task, plan.getId(), state, problem});
        } else if (t instanceof IllegalCommandException) {
            logger.log(Level.INFO,
                       "{0} of plan {1} ended in state {2} with {3}",
                       new Object[] {task, plan.getId(),
                                     state, t.getMessage()});
        } else {
            logger.log(Level.WARNING,
                       "{0} of plan {1} ended in state {2} with {3} {4}\n{5}",
                       new Object[] {task, plan.getId(), state, t, problem,
                                     LoggerUtils.getStackTrace(t)});
        }
    }

    ExceptionTransfer getTransfer() {
        return transfer;
    }

    String getFailureDescription() {
        final ExceptionTransfer et = getTransfer();
        if (et == null) {
            return null;
        }

        /* et.getStackTrace may return null */
        if (et.getStackTrace() != null) {
            return et.getDescription() + ": " + et.getStackTrace();
        }
        return et.getDescription();
    }

    @Override
    public String toString() {
        String ret = taskName + " [" + state + "]";
        final String failure = getFailureDescription();
        if (failure != null) {
            ret += " " + failure;
        }
        if (cleanupStartTime != 0) {
            ret += " cleanup started at " +
                FormatUtils.formatDateTime(cleanupStartTime);
        }
        if (cleanupEndTime != 0) {
            ret += " cleanup ended at " +
                FormatUtils.formatDateTime(cleanupEndTime);
        }

        if (cleanupFailure != null) {
            ret += "cleanup failure:" + cleanupFailure;
        }

        return ret;
    }

    String getTaskName() {
        return taskName;
    }

    Task.State getState() {
        return state;
    }

    int getTaskNum() {
        return taskNum;
    }

    void cleanupStarted() {
        cleanupStartTime = System.currentTimeMillis();
    }

    void cleanupEnded() {
        cleanupEndTime = System.currentTimeMillis();
    }

    void saveCleanupFailure(String failureInfo) {
        cleanupFailure = failureInfo;
    }

    long getStartTime() {
        return startTime;
    }

    Task getTask() {
        return task;
    }

    Map<String, String> getDetails() {
        return details;
    }

    long getEndTime() {
        return endTime;
    }

    /*
     * Provide a customized display of task execution for this task, if it
     * collected extra info, and supports such a display.
     * @return null if there is no detailed information.
     */
    public String displayTaskDetails(String displayPrefix) {
        return task.displayExecutionDetails(details, displayPrefix);
    }
}
