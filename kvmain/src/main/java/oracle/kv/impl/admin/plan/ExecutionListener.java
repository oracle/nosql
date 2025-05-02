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

import oracle.kv.impl.admin.plan.task.Task;

/**
 * Listener that tracks plan execution and is notified task by task.
 * Not sure yet how this should hook into monitoring.
 */
public interface ExecutionListener {
    void planStart(Plan plan);
    void planEnd(Plan plan);
    void taskStart(Plan plan, Task task, int taskNum, int totalTasks);
    void taskEnd(Plan plan, Task task, TaskRun taskRun, int taskNum, 
                 int totalTasks);
}
