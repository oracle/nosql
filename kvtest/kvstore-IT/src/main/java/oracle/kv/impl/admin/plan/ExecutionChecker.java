/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan;

import oracle.kv.impl.admin.plan.task.Task;

/**
 * Checks that a plan starts and stops, and executes the right number of tasks.
 */
public class ExecutionChecker implements ExecutionListener {

    private final Plan targetPlan;
    private boolean planStarted;
    private boolean planEnded;
    private int numTaskStarts;
    private int numTaskEnds;
    private final int expectedTasks;

    public ExecutionChecker(Plan plan) {
        targetPlan = plan;
        planStarted = false;
        planEnded = false;
        numTaskStarts = 0;
        numTaskEnds = 0;
        expectedTasks = plan.getTaskList().getTotalTaskCount();
    }

    @Override
    public void planStart(Plan plan) {
        assert targetPlan.getId() == plan.getId();
        planStarted = true;
    }

    @Override
    public void planEnd(Plan plan) {
        assert targetPlan.getId() == plan.getId();
        planEnded = true;
    }

    @Override
    public void taskStart(Plan plan,
                          Task task,
                          int taskNum,
                          int totalTasks) {
        assert targetPlan.getId() == plan.getId();
        numTaskStarts++;
   }

    @Override
    public void taskEnd(Plan plan,
                        Task task,
                        TaskRun taskRun,
                        int taskNum,
                        int totalTasks) {
        assert targetPlan.getId() == plan.getId();
        numTaskEnds++;
    }

    /**
     * Verify that the number of tasks executed is correct.
     */
    public void verify() {
        if (!planStarted) {
            throw new RuntimeException(targetPlan + " didn't start");
        }
        
        if (!planEnded) {
            throw new RuntimeException(targetPlan + " didn't end");
        }

        if (numTaskStarts != expectedTasks) {
            throw new RuntimeException("Expected starts = " + expectedTasks + 
                                       " saw " + numTaskStarts);
        }

        if (numTaskEnds != expectedTasks) {
            throw new RuntimeException("Expected ends = " + expectedTasks + 
                                       " saw " + numTaskEnds);
        }

        if (targetPlan.getState() != Plan.State.SUCCEEDED) {
            throw new RuntimeException(" plan state=" +  
                                       targetPlan.getState());
        }
    }
}
