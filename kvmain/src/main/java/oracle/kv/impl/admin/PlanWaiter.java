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

package oracle.kv.impl.admin;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.ExecutionListener;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.TaskRun;
import oracle.kv.impl.admin.plan.task.Task;

/**
 * PlanWaiter is used to wait for a plan to finish execution.
 */
public class PlanWaiter implements ExecutionListener {

    private final CountDownLatch done;

    public PlanWaiter() {
        done = new CountDownLatch(1);
    }

    /**
     * Call after plan execution has begin. Will return when
     *  A. the method is interrupted.
     *  B. the timeout period has expired.
     *  C. the plan has finished, successfully or not.
     *
     * @param timeout if 0, wait without a timeout
     *
     * @return true if the plan completed, false if it timed out.
     * @throws InterruptedException
     */
    boolean waitForPlanEnd(int timeout, TimeUnit timeoutUnit)
        throws InterruptedException {

        if (timeout == 0) {
            done.await();
            return true;
        }

        return done.await(timeout, timeoutUnit);
    }

    @Override
    public void planStart(Plan plan) {
        /* Do nothing */
    }

    @Override
    public void planEnd(Plan plan) {
        done.countDown();

        /*
         * Check if the logger is set; in some test cases, the plan may not
         * be executed by the PlannerImpl, and the logger may not be set.
         */
        Logger useLogger = ((AbstractPlan)plan).getLogger();
        if (useLogger != null) {
            useLogger.log(Level.FINE,
                         "PlanWaiter.planEnd called for {0}, state={1}",
                          new Object[]{plan, plan.getState()});
        }
    }

    @Override
    public void taskStart(Plan plan,
                          Task task,
                          int taskNum,
                          int totalTasks) {
        /* Do nothing */
    }

    @Override
    public void taskEnd(Plan plan,
                        Task task,
                        TaskRun taskRun,
                        int taskNum,
                        int totalTasks) {
        /* Do nothing */
    }
}
