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

/**
 * A JobWrapper is used to present one phase of a multi-phase task as a
 * Callable that can be executed by a executor service. When call() is invoked,
 * the wrapper invokes the doJob() method. The doJob() method will return a
 * NextJob, which holds both the status of the work just executed, along with a
 * possible follow-on phase, and parameters to use in scheduling the next
 * phase. When phase 1 finishes, the follow-on phase will be scheduled.
 *
 * For example, suppose a task wants to
 *  - phase 1: invoke a partition migration
 *  - phase 2: check migration status
 * then the doJob() method would invoke phase 1 and return a {@link NextJob}
 * that encases phase 2.
 */
public abstract class JobWrapper implements Callable<Task.State> {
    
    protected final int taskId;
    protected final TaskRunner runner;

    /* For debug and logging messages. */
    private final String description;
    
    public interface TaskRunner {
        Task.State dispatchNextJob(int taskId, NextJob nextJob);
        
        public Map<String, String> getDetails(int taskId);
    }
    
    /**
     * @param taskId of the owning task. Needed for submitting any follow on 
     * work for the ensuing phase.
     * @param runner - a parallel task runner that can be used to schedule and
     * execute any ensuing work.
     * @param description - used only for providing information to debugging 
     * and logging output
     */
    public JobWrapper(int taskId, 
                      TaskRunner runner,
                      String description) {

        this.taskId = taskId;
        this.runner = runner;
        this.description = description;
    }

    public abstract NextJob doJob() throws Exception;

    /**
     * Executes the work in the doJob()) method. Upon its completion, 
     * schedule and submits the next phase of work to be done, as 
     * specified by doJob().
     */
    @Override
    public Task.State call() throws Exception {
        NextJob nextAction = doJob();
        return runner.dispatchNextJob(taskId, nextAction);
    }

    public String getDescription() {
        return description;
    }
}