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
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.admin.plan.Plan;

/**
 * A list of tasks that is used to carry out a {@link Plan}.  In R1, Plans will
 * consist of a single TaskList with a {@link ExecutionStrategy#SERIAL}
 * execution strategy.
 *
 * In R2, TaskLists will extend Task and will be nestable. A nested TaskList
 * within a Plan would permit parallelizable execution. For example,
 *
 * {@literal
 *      TaskA
 *       |
 *     TaskListB  <--- represents parallel execution
 *       |
 *      Task C
 * }
 */
public class TaskList implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The options for how this list of tasks is to be executed: in serial
     * or in parallel.
     */
    public enum ExecutionStrategy {
        /** Tasks can be executed in parallel, or serially */
        PARALLEL,
        /** Tasks just be executed serially */
        SERIAL
    }

    /**
     * The strategy to use for executing this task list. 
     */
    private final ExecutionStrategy strategy;

    /**
     * The ordered list of tasks to perform. Defined as ArrayList to support
     * cloning.
     */
    private final ArrayList<Task> taskList;

    public TaskList(ExecutionStrategy execOrder) {
        taskList = new ArrayList<>();
        strategy = execOrder;
    }
    
    /**
     * Creates a task list with a single element. The list strategy is PARALLEL.
     */
    public TaskList(Task task) {
        taskList = new ArrayList<>(1);
        strategy = ExecutionStrategy.PARALLEL;
        add(task);
    }

    public final void add(Task t) {
        taskList.add(t);
    }

    /**
     * Returns a cloned task list, which can be used for execution. Because
     * it's a shallow copy, the task attributes such as state may be changed
     * and will be visible to other accessors of the task, which is the 
     * intended effect.
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTasks() {
        return (List<Task>) taskList.clone();
    }

    /**
     * Get the number of all tasks to be done. Because of nested tasks, there
     * may be more tasks than 1. If this is a nested task, the umbrella task
     * has no work, so one only counts the nested tasks.
     */
    public int getTotalTaskCount() {
        int count = 0;
        for (Task t: taskList) {
            count += t.getTotalTaskCount();
        }
        return count;
    }

    /**
     * Returns true if this list is empty.
     * @return true if this list is empty
     */
    public boolean isEmpty() {
        return taskList.isEmpty();
    }

    public ExecutionStrategy getStrategy() {
        return strategy;
    }
}
