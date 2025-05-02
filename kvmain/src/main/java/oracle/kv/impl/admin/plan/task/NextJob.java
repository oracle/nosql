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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.param.DurationParameter;

/**
 * NextJob adds the notion of scheduling to the phases, or jobs, of a 
 * MultiJobTask. A NextJob instance is sufficient to tell the PlanExecutor
 * 
 *  - what, if any job comes next.
 *  - how to update the current state of the task
 *  - when to schedule the next job.
 */
public class NextJob {   
    
    /* 
     * Convenience instance that indicates that the task has ended with SUCCESS,
     * and there is no follow on work to do.
     */
    public static final NextJob END_WITH_SUCCESS;
    public static final NextJob END_WITH_ERROR;
    
    static {
        END_WITH_SUCCESS = new NextJob(Task.State.SUCCEEDED, null, null);
    }

    static {
        END_WITH_ERROR = new NextJob(Task.State.ERROR, null, null);
    }

    private final Task.State prevJobTaskState;
    private final JobWrapper followOnWork;
    private final TimeUnit timeUnit;
    private final long time;

    /* 
     * Useful details about the job execution, such as an exception message,
     * or execution stats, which should be passed from the executing thread
     * to the main plan executor..
     */
    private final String additionalInfo;
    
    /**
     * If prevJobState if Task.State.ERROR, SUCCESS or INTERRUPT, the task
     * execution should end, and we expect the followOnWork param to be null.
     * @param prevJobState the result of the last the task.
     * @param followOnWork the next job to do
     * @param scheduleInterval how to schedule the next job
     * @param additionalInfo details about the execution of the previous job.
     * Used to pass along error information, and perhaps statistics or status
     * from the last job.
     */
    public NextJob(Task.State prevJobState, 
                   JobWrapper followOnWork,
                   DurationParameter scheduleInterval,
                   String additionalInfo) {
        this.prevJobTaskState = prevJobState;
        this.followOnWork = followOnWork;

        if (((prevJobState == Task.State.ERROR) ||
             (prevJobState == Task.State.SUCCEEDED) ||
             (prevJobState == Task.State.INTERRUPTED)) &&
              followOnWork != null){
            throw new IllegalStateException("Task state was " + prevJobState +
                                            " and there should not be any" +
                                            " additional task work.");
        }

        if (scheduleInterval == null) {
            this.timeUnit = TimeUnit.SECONDS;
            this.time = 0L;
        } else {
           this.timeUnit = scheduleInterval.getUnit();
           this.time = scheduleInterval.getAmount();
        }
        this.additionalInfo = additionalInfo;
    }

    public NextJob(Task.State prevJobState, 
                   JobWrapper followOnWork,
                   DurationParameter scheduleInterval) {
        this(prevJobState, followOnWork, scheduleInterval, null);
    }

    public NextJob(Task.State prevJobState, 
                   String additionalInfo) {
        this(prevJobState, null, null, additionalInfo);
    }

    
    /**
     * The current state of the task, as a result of the previous job.
     */
    public Task.State getPrevJobTaskState() {
        return prevJobTaskState;
    }

    /**
     * Delay used to schedule the next job.
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * Delay used to schedule the next job.
     */
    public long getDelay() {
        return time;
    }

    /**
     * Get the next job.
     */
    public Callable<Task.State> getNextCallable() {
        return followOnWork;
    }

    @Override
    public String toString() {
        if (followOnWork != null) {
            return followOnWork.getDescription() + " scheduled in " +
                time + " " + timeUnit;
        }
        return "none";
    }

    /**
     * For logging.
     */
    public String getDescription() {
        if (followOnWork == null) {
            return "no work";
        }
        
        return followOnWork.getDescription();
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }
}
