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

import java.io.Serializable;
import java.util.Map;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Planner;

/**
 * A common base class for implementations of the {@link Task} interface.
 */
public abstract class AbstractTask implements Task, Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * If a task cleanup job fails, keep retrying periodically, until it either
     * succeeds or the user interrupts the plan again.
     */
    static final int CLEANUP_RETRY_MILLIS = 120000;

    private transient CommandResult taskResult;
    
    /**
     * Gets the owning plan. If the plan is not known null is returned.
     *
     * @return the owning plan or null.
     */
    protected abstract Plan getPlan();

    @Override
    public final String getName() {
        return getName(new StringBuilder()).toString();
    }

    /**
     * Gets the name of the task. The base implementation returns the simple
     * class name. Subclasses may override this method to provide additional
     * information about the task.
     */
    protected StringBuilder getName(StringBuilder sb) {
        return sb.append(getClass().getSimpleName());
    }

    /**
     * Returns a string in the form:
     *
     *      Task [name]
     *  or
     *      Plan # [planName] task [name]
     *
     * where name is the value returned by getName(). If getPlan() returns null
     * the first string is returned. Otherwise the second string is returned
     * which includes the string returned by plan.toString().
     *
     * In general subclasses should not override toString() but instead
     * override getName() to provide more details about the task.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        /*  If the plan is available, prefix the string with the plan info */
        final Plan p = getPlan();
        if (p == null) {
            sb.append("Task ");
        } else {
            sb.append(p.toString());
            sb.append(" task ");
        }
        sb.append("[");
        getName(sb);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public TaskList getNestedTasks() {
        /* A task list is only returned for parallel tasks. */
        return null;
    }

    /**
     * AbstractTasks are assumed to not have any nested tasks.
     */
    @Override
    public int getTotalTaskCount() {
        return 1;
    }

    /**
     * Most tasks have no cleanup to do. Whatever changes they have executed
     * can be left untouched.
     */
    @Override
    public Runnable getCleanupJob() {
        return null;
    }

    /**
     * Obtain any required locks before plan execution, to avoid conflicts
     * in concurrent plans.
     * @throws PlanLocksHeldException
     */
    @Override
    public void acquireLocks(Planner planner)
        throws PlanLocksHeldException {
        /* default: no locks needed */
    }

    /*
     * Format any detailed information collected about the task in a way
     * that's usable for plan reporting. Should be overridden by tasks to
     * provide customized status reports.
     *
     * @return null if there are no details.
     */
    @Override
    public String displayExecutionDetails(Map<String, String> details,
                                          String displayPrefix) {
        if (details.isEmpty()) {
            return null;
        }

        return details.toString();
    }

    @Override
    public boolean logicalCompare(Task otherTask) {
        throw new NonfatalAssertionException(getName() +
            ": logical comparison is only supported for " +
            "table and security related tasks");
    }

    @Override
    public boolean restartOnInterrupted() {
        return false;
    }

    @Override
    public CommandResult getTaskResult() {
        return taskResult;
    }

    @Override
    public void setTaskResult(CommandResult taskResult) {
        this.taskResult = taskResult;
    }

    @Override
    public String getTaskProgressType() {
        return "generalTask";
    }
}
