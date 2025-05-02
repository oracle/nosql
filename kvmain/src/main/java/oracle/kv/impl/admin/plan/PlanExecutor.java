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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.admin.plan.task.NextJob;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.TaskList;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.ProcessFaultHandler.Procedure;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.KVThreadFactory.KVPrivilegedThreadFactory;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.DatabaseException;

/**
 * The PlanExecutor manages the execution of a single plan. The PlannerImpl
 * invokes the PlanExecutor via an execution service, so that plan submission
 * is asynchronous and multiple plans can be issued by the PlannerImpl, if
 * desired.
 *
 * {@literal
 * PlannerImpl +-------------> plan 1
 *             +------------ > plan 2
 *             +------------ > plan 3
 * }
 *
 * A PlanExecutor uses its own execution service to execute the tasks that
 * constitute the plan. Serial tasks are executed one by one, and the executor
 * waits for task completion before proceeding. A one level deep hierarchy of
 * tasks is supported. A serial task can contain a set of nested tasks,
 * which are to be executed in parallel.
 *
 * Serial task 1
 * Serial task 2
 * Serial task 3 contains nested, parallel tasks 4, 5, 6
 * Serial task 7
 *
 * Only one level of nesting is supported; nested tasks cannot have nested
 * tasks below. This constraint is motivated to keep the scope of require
 * testing bounded. If we needed more levels of nesting in the future, we could
 * do so.
 *
 * Both serial and parallel tasks are submitted to an execution service, even
 * though the serial tasks could be executed in place by the PlanExecutor
 * itself. Using the execution service for both types of tasks makes plan
 * interrupt and cancellation processing uniform.
 *
 * When a serial task is executed, the PlanExecutor submits it to the execution
 * service, and then immediately awaits its completion. When a set of nested,
 * parallel tasks are seen, the PlanExecutor submits the whole set, and then
 * awaits completion of all of the set.
 *
 * All threads in the diagram come from the execution service.
 *
 * {@literal
 *      Plan Executor (thread 1)
 *              |
 *              v
 *          serial task
 *              v
 *          serial task
 *              v
 *          parallel task --+----- thread 2 -> nested task a
 *                          +----- thread 3 -> nested task b
 *                          +----- thread 4 -> nested task c
 *                                       |
 *                                (all nested tasks completed)
 *              +------------------------+
 *              |
 *          serial task
 *              v
 *          serial task
 * }
 *
 * Multiple phase tasks: Parallel tasks may be single or multiple phased tasks.
 * This concept is orthogonal to the parallel vs serial execution choice, but
 * in terms of implementation, we currently only support multiphase tasks when
 * they are run by the parallel task executor, just to limit the number of
 * cases we need to test. A multiple phase is executed as a series of jobs,
 * Callables. The sequence of jobs can be determined dynamically. The point of
 * grouping the jobs within a single task it to make the jobs atomic from the
 * point of task recovery and retries.
 *
 * An example usage for a multiple phase task is partition migration, which
 * requires invocations of multiple RN interfaces. The sequence of invocations
 * is dependent on dynamic state found during the actual task execution. Some
 * of the phases are polling jobs, and may be scheduled with a delay.
 *
 * Executing, interrupting, cleanup of incomplete or failed tasks
 * --------------------------------------------------------------
 * From the system administrator's point of view, a plan executes
 * asynchronously. There is no time limit for plan or task execution. To see
 * information about a running plan, the sys admin will use a CLI command to
 * request plan status. If desired, the sys admin can interrupt the plan, which
 * will make it halt its execution. An interrupt may take some time to notice
 * and process.
 *
 * Interrupting a plan puts it into the Plan.State.INTERRUPT_REQUESTED state.
 * The plan execution framework and each task are required to check for an
 * interrupt request at reasonable intervals. Tasks which return before
 * completing all their desired actions return with Task.State.INTERRUPTED.
 *
 * A task that ended unsuccessfully due to an interrupt, or an error incurred
 * during execution should attempt to leave the store in a consistent
 * state. This is not the same as a rollback; it's permissible for a task to
 * alter the store, and to leave the store in that changed state, as long as
 * the store is consistent. The execution framework will execute the task
 * cleanup job for any task that does not end successfully.
 *
 * A task that ended in the RESTART state will cause the plan to exit while
 * remaining in the RUNNING state. This is so that a plan can be automatically
 * restarted in the case of the admin being restarted, or a master transfer.
 *
 * Cleanups can take a while, and the user may issue additional interrupts to
 * halt the cleanup. In addition, the cleanup itself might fail. Because of
 * that, clean up is a best effort, and not guaranteed. If cleanup doesn't
 * happen, the user should be able to fix the problem by retrying the plan, or
 * by running some other, corrective plan.
 *
 * A plan that is INTERRUPT_REQUESTED finally transitions to INTERRUPTED after
 * all tasks are halted, and either all cleanups are executed, or the cleanup
 * phase has itself been interrupted.
 */
public class PlanExecutor implements Callable<Plan.State> {

    /* The interval to check if a task or task cleanup finished.*/
    private final static int TASK_CHECK_INTERVAL = 1;
    private final static TimeUnit TASK_CHECK_TIME_UNIT = TimeUnit.SECONDS;

    /*
     * TODO: should the size of the thread pool be a parameter? Serial task
     * lists only use one thread at a time. Parallel tasks spawn off tasks
     * concurrently, but those tasks use the multi-job mechanism, and should be
     * asynchronous. The only real issue is when a parallel task is doing a
     * cleanup, because task cleanups are currently implemented as a single
     * Runnable that can take an unbounded amount of time and can consume the
     * thread.
     */
    private final static int POOL_SIZE = 5;

    private final AbstractPlan plan;
    private ScheduledExecutorService pool;
    private final Admin admin;
    private final Planner planner;
    private final PlanRun planRun;
    private final Logger logger;
    private final PlanFaultHandler faultHandler;

    /* The number of tasks in the plan, including all nested tasks. */
    private final int totalTasks;

    /* cleanup jobs that must be executed during this plan run */
    private final Set<CleanupInfo> taskCleanups;

    /* Keep the security context for this plan execution */
    private final ExecutionContext execCtx;

    public static TestHook<Integer> FAULT_HOOK;

    /**
     * A PlanExecutor lives for the duration of a single plan execution.
     * Retried executions spawn new PlanExecutors.
     */
    public PlanExecutor(Admin admin,
                        Planner planner,
                        AbstractPlan plan,
                        PlanRun planRun,
                        Logger logger) {

        this.admin = admin;
        this.planner = planner;
        this.plan = plan;
        this.planRun = planRun;
        this.logger = logger;
        faultHandler = new PlanFaultHandler();
        totalTasks = plan.getTaskList().getTotalTaskCount();

        execCtx = ExecutionContext.getCurrent();
        pool = new ScheduledThreadPoolExecutor
            (POOL_SIZE, new KVPrivilegedThreadFactory("PlanExecutor", execCtx,
                                                      logger));
        taskCleanups = new HashSet<>();
    }

    /**
     * Run a single plan.
     */
    @Override
    public Plan.State call()
        throws Exception {

        plan.setLogger(logger);

        try {
            faultHandler.execute(new Procedure<Exception>() {
               @Override
               public void execute() throws Exception {
                   if (execCtx == null) {
                       planStart();
                   } else {
                       ExecutionContext.runWithContext(
                           (Procedure<Exception>) () -> planStart(), execCtx);
                   }
               }

               private void planStart() throws Exception {
                   taskCleanups.clear();
                   plan.setState(planRun, planner, Plan.State.RUNNING,
                                 "Plan is starting");

                   for (ExecutionListener listener : plan.getListeners()) {
                       listener.planStart(plan);
                   }

                   /*
                    * Make the fact that we are starting persistent, so that
                    * plan recovery will know that the first task may have
                    * started.
                    */
                   admin.savePlan(plan, Admin.CAUSE_EXEC);

                   /*
                    * Since the Admin database is the authority for the
                    * topology and service params, persist any required new
                    * topologies and parameter changes before executing remote
                    * requests, and potentially distributing these changes Some
                    * plans may do the persistent before task execution, while
                    * other tasks require information that is only available at
                    * task execution time. In the latter case, topos and params
                    * are saved during task execution,
                    */
                   plan.preExecutionSave();

                   final TaskList taskList = plan.getTaskList();

                   /*
                    * Currently, we assume that the initial, topmost task list
                    * is always executed serially. Guard against a plan that
                    * starts with a parallel task list.
                    */
                   if (taskList.getStrategy() ==
                       TaskList.ExecutionStrategy.PARALLEL) {
                       throw new IllegalStateException
                          (plan + "does not expect to see a parallel task " +
                          "list at the topmost level");
                   }

                   /* Each task is executed one by one */
                   executeSerialTaskList(taskList);
                }}
                );
        } finally {
            /*
             * Save the plan's final state, and let listeners know that the
             * plan is finished.
             */
            faultHandler.execute(new Procedure<Exception>() {
                @Override
                public void execute() {
                    if (execCtx == null) {
                        planFinished();
                    } else {
                        ExecutionContext.runWithContext(
                            (Runnable) () -> planFinished(), execCtx);
                    }
                }

                private void planFinished() {
                    /*
                     * Task processing is complete, either because we executed
                     * each task, or because the plan was stopped before we got
                     * to all of them.
                     *
                     * If the plan is already in ERROR state, something bad
                     * happened during plan processing, outside the task
                     * execution. The error has already been saved, no need to
                     * do any further processing.
                     */
                    logger.log(Level.FINE,
                               "finish count={0}, totalTasks={1}, " +
                               "errors={2}, interrupts={3}",
                               new Object[] {planRun.getNumFinishedTasks(),
                                             totalTasks,
                                             planRun.getNumErrorTasks(),
                                             planRun.getNumInterruptedTasks()});

                    final Plan.State state = plan.getState();

                    if ((state != Plan.State.ERROR) &&
                        (planRun.getNumErrorTasks() > 0)) {

                            /*
                             * If any tasks failed, put the plan in ERROR.
                             * Tasks may also fail during cleanup.
                             */
                            plan.setState(planRun, planner, Plan.State.ERROR,
                                          "Plan incurred " +
                                          planRun.getNumErrorTasks() +
                                          " failed tasks:" +
                                          planRun.getFailureDescription(false));
                    } else {

                        switch (state) {
                        case ERROR:
                        case INTERRUPTED:
                            break;

                        case INTERRUPT_REQUESTED:
                            /* Indicate that task cleanup has finished */
                            plan.setState(planRun, planner,
                                          Plan.State.INTERRUPTED,
                                          "Plan interrupted," +
                                          planRun.getNumInterruptedTasks() +
                                          " tasks were interrupted");
                            break;

                        case RUNNING:

                            /*
                             * If the plan ended in the RUNNING state, and
                             * there are no unfinished tasks, set to SUCCESS.
                             * Otherwise, if there were no interrupted tasks
                             * leave the plan RUNNING so that it may be
                             * automatically restarted.
                             */
                            if (planRun.getNumFinishedTasks() == totalTasks) {

                                /*
                                 * The plan ended in the RUNNING state and there
                                 * are no unfinished tasks, set to SUCCESS.
                                 */
                                plan.setState(planRun, planner,
                                              Plan.State.SUCCEEDED,
                                              "Plan finished.");

                            } else if (planRun.getNumInterruptedTasks() > 0) {

                                /*
                                 * Tasks were interrupted, go through
                                 * interrupt requested state, as it's not
                                 * permissible to go from RUNNING->INTERRUPTED.
                                 */
                                final int unstarted = totalTasks -
                                                planRun.getNumFinishedTasks();
                                plan.setState(planRun, planner,
                                              Plan.State.INTERRUPT_REQUESTED,
                                              "Plan did not execute " +
                                              unstarted +
                                              " tasks even through interrupt" +
                                              " not requested");
                                plan.setState(planRun, planner,
                                              Plan.State.INTERRUPTED,
                                              "Plan interrupted, " + unstarted +
                                              " tasks not started");
                            }
                            break;

                        default:
                            throw new IllegalStateException(
                                         "Plan finished in unexpected state: " +
                                                            state);
                        }
                    }

                    admin.savePlan(plan, Admin.CAUSE_EXEC);
                    planner.planFinished(plan);

                    for (ExecutionListener listener : plan.getListeners()) {
                        listener.planEnd(plan);
                    }
                }
            }
            );
            pool.shutdownNow();
        }
        return plan.getState();
    }

    /**
     * Each task is executed one by one. If a task contains nested tasks,
     * those tasks are execute in parallel. TODO: must match status report!!
     *
     * @return false if any of the tasks have failed, and if such a task
     * failure is supposed to stop the plan.
     */
    private boolean executeSerialTaskList(final TaskList taskList)
        throws Exception {

        if (taskList.getStrategy() != TaskList.ExecutionStrategy.SERIAL) {
            throw new IllegalStateException
                (plan + " expects a serial task list, not " +
                 taskList.getStrategy());
        }

        /*
         * Execute each task serially, waiting for it to finish before
         * starting the next one.
         */
        for (Task task : taskList.getTasks()) {

            if (plan.isInterruptRequested()) {
                /* Stop spawning off tasks. */
                return false;
            }

            /*
             * Note: this assumes that a task containing nested tasks has no
             * real work of its own to do. Its own doWork method is not called.
             */
            TaskList executeList = task.getNestedTasks();
            if (executeList == null) {
                /*
                 * If there isn't a nested list, wrap the task in a singleton
                 * list for execution.
                 */
                executeList = new TaskList(task);
            }

            if (!executeParallelTaskList(executeList)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Execute the tasks concurrently. Will only return if all tasks have been
     * processed, or the plan is interrupted.
     * @return false if any of the tasks have failed, and if such a task
     * failure is supposed to stop the plan.
     */
    private boolean executeParallelTaskList(final TaskList taskList)
        throws Exception {

        if (taskList.getStrategy() != TaskList.ExecutionStrategy.PARALLEL) {
            throw new IllegalStateException
                (plan + " expects a parallel task list, not " +
                 taskList.getStrategy());
        }

        final ParallelTaskRunner runner = new ParallelTaskRunner(taskList);
        boolean planShouldContinue = true;
        boolean allTasksSubmitted = false;

        /*
         * Submit each task right away. Following the results of the initial
         * execution, each task may then issue additional work.
         */
        try {
            for (Task t : taskList.getTasks()) {
                 runner.submitFirstJob(t);
            }
            allTasksSubmitted = true;
        } finally {

            /*
             * If some jobs weren't even submitted, make sure we don't wait
             * for them.
             */
            runner.clearUnsubmittedTasks();

            /*
             * Wait for all tasks to end. A plan interrupt does not short
             * change this wait, because all tasks should be checking the
             * interrupt flag, and should end if it is set.
             */
            boolean listFinished = false;
            do {
                try {
                    listFinished = runner.checkFinished();
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for completion of " +
                                "parallel tasks: " +
                                LoggerUtils.getStackTrace(e));
                }
                runner.checkForDeadTasks();
                /*
                 * Examine and update new completed parallel tasks to report
                 * plan progress. Only make these checks if all tasks were
                 * successfully submitted, though, since the examine step may
                 * fail otherwise.
                 */
                if (allTasksSubmitted) {
                    switch (runner.examineNewlyCompletedFutures()) {
                    case STOP:
                        planShouldContinue = false;
                        break;
                    case CONTINUE:
                        break;
                    case NOT_DONE:
                        /* Check the futures again if one was not done */
                        listFinished = false;
                        break;
                    default:
                        throw new AssertionError();
                    }
                }
            } while (!listFinished);

            logger.log(Level.FINE, "Parallel task submission: listFinished={0}",
                       listFinished);
        }

        /*
         * If any tasks failed, they would have issued a cleanup.
         */
        waitForTaskCleanups();
        return planShouldContinue;
    }

    /**
     * Set task and plan states appropriately to save failure info. If an
     * exception occurred, rethrow it.
     * @param e because task failures can occur without an exception this
     * param may be null.
     * @throws Exception
     */
    private void recordTaskFailure(TaskRun taskRun,
                                   Task.State taskState,
                                   Exception e,
                                   String additionalInfo,
                                   ErrorMessage errorMsg,
                                   String[] cleanupJobs)
        throws Exception {

        final Throwable trueCause = getTrueCause(e);
        /* Problem is logged by saveFailure() */
        String problem = taskRun.getTaskNum() + "/" + taskRun.getTaskName() +
            " failed.";
        if (additionalInfo != null) {
            problem += " " + additionalInfo;
        }

        /* Set new task and plan states, and save the exception in the plan. */
        plan.setTaskState(taskRun, taskState, logger);
        plan.saveFailure(taskRun, trueCause, problem, errorMsg, cleanupJobs,
                         logger);

        /*
         * Save the failure even if the plan is going to keep on running We
         * want to present the first failure that occurred, to make the error
         * situation more understandable to the user.  PlanRun.saveFailure will
         * save all failures from the plan.
         */
        plan.saveFailure(planRun, ((trueCause == null) ? e : trueCause),
                         problem, errorMsg, cleanupJobs, logger);
        logger.log(Level.FINE,
                   "Record failure of {0}/{1}, final state={2} problem={3} {4}",
                   new Object[] { taskRun.getTaskNum(),
                                  taskRun.getTaskName(), taskState, e,
                                  additionalInfo});
    }

    /**
     * Get true cause:
     * 1. return inner cause if it is ExecutionException.
     * 2. return inner cause if it is CommandFaultException but not
     * IllegalCommandException and inner cause is not null.
     */
    private Throwable getTrueCause(Throwable enwrappedException) {
        Throwable trueCause = enwrappedException;
        if (enwrappedException instanceof ExecutionException) {
            trueCause = enwrappedException.getCause();
        }
        if (trueCause instanceof CommandFaultException) {
            if (trueCause instanceof IllegalCommandException ||
                trueCause.getCause() == null) {
                /* this is already true cause */
            } else {
                trueCause = trueCause.getCause();
            }
        }
        return trueCause;
    }

    /*
     * Set this plan to ERROR state. Something bad happened during plan
     * execution, inbetween tasks.  Note that planRun.saveFailure will log the
     * exception.
     */
    private void putPlanInError(Throwable t,
                                ErrorMessage errorMsg,
                                String[] cleanupJobs) {

        /*
         * The plan may already be in ERROR state if a task failed. If so, just
         * log this new failure, no need to set state again.
         */
        final String problem = "Problem during plan execution";
        plan.saveFailure(planRun, t, problem, errorMsg, cleanupJobs, logger);
        if (plan.getState() == Plan.State.ERROR) {
            logger.log(
                Level.WARNING,
                String.format("Second error in plan execution of %s, "
                              + "already in ERROR", plan),
                t);
        } else {
            plan.setState(planRun, planner, Plan.State.ERROR, problem);
        }
    }

    /**
     * @return true if all task cleanups executed cleanly. Each task should
     * check the planRun to see if a second interrupt request has occurred,
     * and if the cleanup itself should end prematurely.
     */
    boolean waitForTaskCleanups() {

        if (taskCleanups.isEmpty()) {
            return true;
        }

        boolean allSucceeded = true;
        plan.setCleanupStarted();

        /*
         * Fix for KVSTORE-2307: plan verify-data fails due to
         * java.util.ConcurrentModificationException
         * This issue happens when concurrently iterating over the cleanup
         * tasks for one plan task and adding cleanup tasks (see examineFuture
         * method) for other plan task. This waitForTaskCleanups method is
         * called for every plan task after the cleanup task for this plan
         * task has been added in the taskCleanups set in examineFuture
         * method. So it is guaranteed that no cleanup tasks will be left
         * to be processed.
         */
        Set<CleanupInfo> taskCleanupsCopy = new HashSet<>(taskCleanups);
        for (CleanupInfo c : taskCleanupsCopy) {
            final TaskRun tRun = c.getTaskRun();
            boolean cleanupDone = false;
            do {
                try {
                    logger.log(Level.INFO,
                               "Waiting for cleanup of task {0}/{1}",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName()});
                    final Future<?> f = c.getFuture();
                    if (f != null) {

                        /*
                         * The future can be null if there was an exception
                         * before job was submitted.
                         */
                        f.get(TASK_CHECK_INTERVAL, TASK_CHECK_TIME_UNIT);
                    }
                    cleanupDone = true;
                } catch (InterruptedException retry) {
                    logger.log(Level.FINE,
                               "Cleanup of task {0}/{1} interrupted",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName()});
                } catch (TimeoutException retry) {
                    logger.log(Level.FINE,
                               "Cleanup of task {0}/{1} timed out, will retry",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName()});
                } catch (Exception e) {
                    allSucceeded = false;
                    cleanupDone = true;
                    final String info = LoggerUtils.getStackTrace(e);
                    logger.log(Level.WARNING,
                               "Cleanup of task {0}/{1} failed: {2}",
                               new Object[] { tRun.getTaskNum(),
                                              tRun.getTaskName(),
                                              info});
                    plan.saveCleanupFailure(tRun, info);
                } finally {
                    plan.cleanupEnded(tRun);
                }
            } while (!plan.cleanupInterrupted() && !cleanupDone);
        }
        return allSucceeded;
    }

    /**
     * Any RuntimeExceptions or Errors that occur when using the plan fault
     * handler will be saved as information within the PlanRun, and will
     * set the plan to ERROR.
     */
    private class PlanFaultHandler {

        void execute(Procedure<Exception> proc) {
            try {
                proc.execute();
            } catch (KVSecurityException e) {
                putPlanInError(e, ErrorMessage.NOSQL_5100,
                               CommandResult.NO_CLEANUP_JOBS);
                throw e;
            } catch (SessionAccessException | RejectedExecutionException |
                     DatabaseException | Admin.DBOperationFailedException e) {
                putPlanInError(e, ErrorMessage.NOSQL_5400,
                               CommandResult.PLAN_CANCEL);
                throw e;
            } catch (NonfatalAssertionException e) {
                putPlanInError(e, ErrorMessage.NOSQL_5500,
                               CommandResult.NO_CLEANUP_JOBS);
                throw e;
            } catch (Error | RuntimeException e) {
                putPlanInError(e, ErrorMessage.NOSQL_5500,
                               CommandResult.NO_CLEANUP_JOBS);
                throw e;
            } catch (RemoteException e) {
                putPlanInError(e, ErrorMessage.NOSQL_5400,
                               CommandResult.PLAN_CANCEL);
                throw new OperationFaultException
                    ("Problem in plan execution", e);
            } catch (Exception e) {
                putPlanInError(e, ErrorMessage.NOSQL_5500,
                               CommandResult.NO_CLEANUP_JOBS);
                throw new OperationFaultException
                   ("Problem in plan execution", e);
            }
        }
    }

    /**
     * Execute a set of MultiJobTasks in parallel.
     *
     * The ParallelTaskRunner has the logic to concurrently start the first job
     * of each task, and then to process the results of each job. There is no
     * bound on the amount of time for each task. The runner provides a way to
     * query for the completion of all tasks.
     *
     * When the last job of a task is finished, that job returns a terminal
     * task status, and the runner updates TaskRun information, and marks
     * that job as complete.
     */
    private class ParallelTaskRunner implements TaskRunner {

        private static final String RUNNER = "Parallel Task Runner:";
        private final CountDownLatch waitForCompletion;
        private final Map<Integer, TaskInfo> taskInfoMap = new HashMap<>();
        /* The futures from any serial tasks that were submitted */
        private final List<Future<Boolean>> serialTasks = new ArrayList<>();
        private final int numParallelTasks;
        private volatile int numSubmitted = 0;
        private final ExecutorService serialBundlePool;

        /**
         * @param taskList the set of tasks to execute in parallel.
         */
        ParallelTaskRunner(TaskList taskList) {

            numParallelTasks = taskList.getTotalTaskCount();
            waitForCompletion = new CountDownLatch(numParallelTasks);
            serialBundlePool = Executors.
                newCachedThreadPool(
                new KVPrivilegedThreadFactory("serialBundleExecutor",
                                              execCtx, logger));
        }

        public void clearUnsubmittedTasks() {
            if (numSubmitted < numParallelTasks) {
                logger.log(Level.INFO,
                           "{0} only {1} out of {2} tasks started, reduce " +
                           "number of tasks to wait for.",
                           new Object[] { RUNNER, numSubmitted,
                                          numParallelTasks});

                for (int i = 0; i < (numParallelTasks - numSubmitted); i++) {
                    waitForCompletion.countDown();
                }
            }
        }

        /**
         * Mark the task start, and submit the first job of a multi job task
         * the execution service.
         */
        private void submitFirstJob(Task task)
            throws Exception {

            numSubmitted++;

            /*
             * Check if the task has nested tasks to be run serially. Note that
             * in this case the task itself is not run (Task.getFirstJob() is
             * not called)
             */
            final TaskList taskList = task.getNestedTasks();
            if (taskList != null) {
                submitNested(taskList);
                return;
            }

            final TaskRun taskRun = plan.startTask(planRun, task, logger);
            final int taskId = taskRun.getTaskNum();
            try {

                /*
                 * The listener must be executed within the try/catch, so that
                 * any interrupts that occur during the listener execution are
                 * properly handled.
                 */
                for (ExecutionListener listener : plan.getListeners()) {
                    listener.taskStart(plan, task, taskId, totalTasks);
                }

                /*
                 * Initialize the taskInfoMap for this task slot, so it's setup
                 * before the first job is executed.
                 */
                setTaskInfo(taskRun, task);

                /*
                 * The first phase of each task is always the
                 * MultiJobTask.startwork() method.
                 */
                logger.log(Level.FINE, "{0} submitted {1} for task {2}",
                           new Object[] { RUNNER, task.getName(), taskId });
                assert plan.isSystemPlan() ? true :
                                   TestHookExecute.doHookIfSet(FAULT_HOOK, null);

                final int nextJobNumber = getNextJobNumber(taskId);
                final Future<Task.State> f =
                    pool.submit(task.getFirstJob(taskId, this));

                /* Keep track of the future */
                updateTaskInfo(taskId, f, nextJobNumber);

            } catch (RejectedExecutionException e) {
                logger.log(Level.SEVERE, "{0} task {1}/job={2} got {3}",
                           new Object[] {RUNNER, taskId, task.getName(), e});
                throw e;
            } catch (Exception e) {

                /*
                 * Since the schedule execution service should accept any
                 * number of starting jobs, any exception that occurs here is
                 * either a problem in one of the plan listeners, or a bug in
                 * the method. Even though the task didn't execute, manufacture
                 * an error state, and put the task in error mode, because
                 * otherwise the exception will be lost and the task will
                 * appear to hang.
                 */
                recordTaskFailure(taskRun, Task.State.ERROR, e,
                                  "Problem with concurrent start of parallel " +
                                  "tasks",
                                  ErrorMessage.NOSQL_5500,
                                  CommandResult.NO_CLEANUP_JOBS);
            }
        }


        /**
         * Submits a runner to serially execute the tasks in the
         * specified task list.
         */
        private void submitNested(TaskList taskList) {
            /*  Only serial tasks can be nested here. */
            if (taskList.getStrategy() != TaskList.ExecutionStrategy.SERIAL) {
                throw new IllegalStateException
                    ("Only serial tasks can be nested within a parallel bundle");
            }

            /* Submit the serial bundle to a separate pool instead of the task
             * pool in PlanExectuor because the serial bundles may use up the
             * threads in the task pool and cause no threads to run tasks.*/
            final Future<Boolean> f = serialBundlePool.submit(() -> {
                try {
                    final boolean ret = executeSerialTaskList(taskList);
                    waitForCompletion.countDown();
                    return ret;
                } catch (RejectedExecutionException ree) {
                    logger.log(Level.SEVERE,
                               "{0} unable to submit serial task runner: {1}",
                               new Object[] {RUNNER, ree});
                    throw ree;
                }
            });
            serialTasks.add(f);
        }

        /**
         * Examine the NextJob information provided by the previous job, and
         * either schedule a follow on job,or deem this task to be completed.
         * Called by the thread that is executing a phase.
         */
        @Override
        public Task.State dispatchNextJob(int taskId, NextJob nextJob) {

            assert plan.isSystemPlan() ? true :
                                   TestHookExecute.doHookIfSet(FAULT_HOOK, null);

            /* Save the plan to preserve task status generated by this job. */
            admin.savePlan(plan, Admin.CAUSE_EXEC);

            final Task.State prevJobTaskState = nextJob.getPrevJobTaskState();
            switch (prevJobTaskState) {
            case RUNNING:
            case PENDING:
                if (plan.isInterruptRequested()) {
                    logger.log(Level.INFO,
                               "{0}.dispatch: plan is interrupted, " +
                               "{1}/{2} job={3} will " +
                               "not be executed",
                               new Object[] { RUNNER,
                                              taskId,
                                              taskInfoMap.get(taskId).getName(),
                                              nextJob.getDescription()});

                    completeTaskInfo(taskId, nextJob.getAdditionalInfo());
                    waitForCompletion.countDown();
                    return Task.State.INTERRUPTED;
                }

                logger.log(Level.FINE,
                           "{0} task {1}/{2} job={3} will run in {4} {5}",
                           new Object[] { RUNNER,
                                          taskId,
                                          taskInfoMap.get(taskId).getName(),
                                          nextJob.getDescription(),
                                          nextJob.getDelay(),
                                          nextJob.getTimeUnit()});

                final int nextJobNumber = getNextJobNumber(taskId);
                final Future<Task.State> f = pool.schedule
                    (nextJob.getNextCallable(),
                     nextJob.getDelay(),
                     nextJob.getTimeUnit());
                updateTaskInfo(taskId, f, nextJobNumber);
                break;

            case SUCCEEDED:
            case INTERRUPTED:
            case ERROR:
                completeTaskInfo(taskId, nextJob.getAdditionalInfo());
                waitForCompletion.countDown();
                logger.log(Level.FINE,
                           "{0} task {1}/job={2} finished, state={3}",
                           new Object[] {RUNNER, taskId, nextJob,
                                         prevJobTaskState});
                break;
            }
            return prevJobTaskState;
        }

        /**
         * Check if all tasks are finished, waiting TASK_CHECK_INTERVAL and
         * then returning the result. Tasks are responsible for periodically
         * checking if the plan interrupt request flag is set.
         *
         * @return true if all tasks have reported themselves as finished,
         * although they may have either finished successfully or in error.
         */
        boolean checkFinished()
            throws InterruptedException {

            logger.log(Level.FINE,
                       "{0} Wait for {1} out of {2} tasks to complete",
                       new Object[] { RUNNER, waitForCompletion.getCount(),
                                      numParallelTasks});
            boolean done = waitForCompletion.await(TASK_CHECK_INTERVAL,
                                                   TASK_CHECK_TIME_UNIT);
            logger.log(Level.FINE,
                       "{0} {1} out of {2} tasks still outstanding",
                       new Object[] { RUNNER, waitForCompletion.getCount(),
                                      numParallelTasks});


            return done;
        }

        @Override
        public Map<String, String> getDetails(int taskId) {
            synchronized (taskInfoMap) {
                return taskInfoMap.get(taskId).taskRun.getDetails();
            }
        }

        /** Initialize the task info for this task. */
        private void setTaskInfo(TaskRun taskRun, Task task) {
            synchronized (taskInfoMap) {
                taskInfoMap.put(taskRun.getTaskNum(),
                                new TaskInfo(taskRun, task));
            }
        }

        /**
         * Get the job number for the next job for this task, and increment the
         * number for the next request.
         */
        private int getNextJobNumber(int taskId) {
            synchronized (taskInfoMap) {
                return taskInfoMap.get(taskId).getNextJobNumber();
            }
        }

        /**
         * Update the future associated with this task and job number. Only
         * updates the future if it is for a newer job number than the future
         * already recorded, to avoid a multi-thread hazard where a newer job
         * could record its future before an older job recorded its one.
         */
        private void updateTaskInfo(int taskId,
                                    Future<Task.State> newFuture,
                                    int newFutureJobNumber) {
            synchronized (taskInfoMap) {
                final TaskInfo oldInfo = taskInfoMap.get(taskId);
                oldInfo.updateFuture(newFuture, newFutureJobNumber);
            }
        }

        private void completeTaskInfo(int taskId, String additionalInfo) {
            synchronized (taskInfoMap) {
                final TaskInfo oldInfo = taskInfoMap.get(taskId);
                oldInfo.addInfo(additionalInfo);
                oldInfo.completed = true;
            }
        }

        /**
         * Find tasks that have died ungracefully, and failed to execute the
         * final completion steps. Mark them as completed now, and countdown
         * on the waitForCompletion latch.
         */
        void checkForDeadTasks() {
            synchronized (taskInfoMap) {
                for (TaskInfo ti : taskInfoMap.values()) {

                    boolean doCleanup = false;
                    if (ti.future == null) {

                        /*
                         * The future may be null if the task did not truly
                         * start.
                         */
                        if (!ti.completed) {
                            logger.log
                                (Level.INFO,
                                 "{0} cleaning up unstarted task {1} {2}",
                                 new Object[] { RUNNER,
                                                ti.taskRun.getTaskNum(),
                                                ti.task});
                            doCleanup = true;
                        }
                    } else if (ti.future.isDone() && (!ti.completed)) {
                        logger.log(Level.INFO,
                                   "{0} cleaning up dead task {1} {2}",
                                   new Object[] { RUNNER,
                                                  ti.taskRun.getTaskNum(),
                                                  ti.task});
                        doCleanup = true;
                    }

                    if (doCleanup) {
                        ti.completed = true;
                        waitForCompletion.countDown();
                    }
                }
            }
        }

        /**
         * Look at the state of all newly completed (and never examined)
         * parallel tasks, and see if their completion outcome impacts the plan
         * state.
         *
         * @return NOT_DONE if any of tasks marked newly completed are actually
         * still running, STOP if all tasks completed and any of them failed,
         * and CONTINUE if all tasks completed successfully
         */
        ExamineResult examineNewlyCompletedFutures() throws Exception {
            boolean planShouldContinue = true;
            boolean allDone = true;
            Future<?> jobFuture = null;
            synchronized (taskInfoMap) {
                for (TaskInfo taskInfo : taskInfoMap.values()) {
                    if (taskInfo.completed && !taskInfo.examined) {
                        final ExamineResult examineResult =
                            examineFuture(taskInfo.future,
                                          taskInfo.taskRun,
                                          taskInfo.task,
                                          taskInfo.additionalInfo);
                        if (examineResult == ExamineResult.CONTINUE) {
                            taskInfo.examined = true;
                        } else if (examineResult == ExamineResult.STOP) {
                            planShouldContinue = false;
                            taskInfo.examined = true;
                        } else {
                            /*
                             * The task is not done. Save the job future to
                             * wait on if the job also isn't done. Don't set
                             * examined since we'll need to examine this task
                             * again.
                             */
                            allDone = false;
                            jobFuture = taskInfo.future;
                        }
                    }
                }

                /* Do further checks if all tasks are done */
                if (allDone) {
                    if (!planShouldContinue) {
                        return ExamineResult.STOP;
                    }

                    /* Check if any serial tasks should stop the plan */
                    final Iterator<Future<Boolean>> itr =
                        serialTasks.iterator();
                    while (itr.hasNext()) {
                        final Future<Boolean> f = itr.next();
                        if (!f.isDone()) {

                            /*
                             * The task is not done, break so we can wait
                             * outside the synchronized block
                             */
                            allDone = false;
                            jobFuture = f;
                            break;
                        }
                        try {
                            /*
                             * The future is done, so we don't expect this call
                             * to block or timeout, but specify a 0 timeout
                             * just in case, meaning that it shouldn't block.
                             */
                            if (!f.get(0, TimeUnit.SECONDS)) {
                                return ExamineResult.STOP;
                            }
                        } catch (InterruptedException | TimeoutException e) {

                            /*
                             * We don't expect to be interrupted or timeout
                             * here, so act as if the task is still running and
                             * let the caller try again.
                             */
                            allDone = false;
                            break;
                        }

                        /*
                         * If the serial task future returned true, then it
                         * completed successfully: remove it and go to next. We
                         * remove the future because
                         * examineNewlyCompletedFutures() can be called
                         * multiple times. No need to keep checking a success.
                         */
                        itr.remove();
                    }
                    if (allDone) {
                        return ExamineResult.CONTINUE;
                    }
                }
            }

            /*
             * If a task was not done, wait on the job future, if any, for
             * it to be done. If the job is done, sleep to avoid a tight
             * loop. In both cases, return NOT_DONE to tell the caller to
             * try again
             */
            try {
                if ((jobFuture != null) && !jobFuture.isDone()) {
                    jobFuture.get(TASK_CHECK_INTERVAL, TASK_CHECK_TIME_UNIT);
                } else {
                    TASK_CHECK_TIME_UNIT.sleep(TASK_CHECK_INTERVAL);
                }
            } catch (InterruptedException | TimeoutException e) {
                /* Retry later */
            }
            return ExamineResult.NOT_DONE;
        }

        /**
         * A struct to keep all the pieces of information for task execution
         * together.
         */
        private class TaskInfo {
            final TaskRun taskRun;
            final Task task;
            Future<Task.State> future;
            /** The number to use for the next job for this task. */
            int nextJobNumber = 1;
            /** The number of the job associated with the future. */
            int futureJobNumber;
            boolean completed;
            boolean examined;
            String additionalInfo;

            TaskInfo(TaskRun taskRun, Task task) {
                this.taskRun = taskRun;
                this.task = task;
                completed = false;
                examined = false;
            }

            /**
             * Returns the number for the next job, starting with one, and
             * incrementing the value for the next call.
             */
            int getNextJobNumber() {
                return nextJobNumber++;
            }

            /**
             * Update the future associated with this task, but only if the
             * future is associated with a newer job number than the future
             * already stored, if any.
             */
            void updateFuture(Future<Task.State> newFuture,
                              int newFutureJobNumber) {
                if (newFutureJobNumber > futureJobNumber) {
                    future = newFuture;
                    futureJobNumber = newFutureJobNumber;
                }
            }

            void addInfo(String info) {
                if (additionalInfo == null) {
                    additionalInfo = info;
                } else {
                    additionalInfo += info + " ";
                }
            }

            String getName() {
                return taskRun.getTaskName();
            }

            @Override
            public String toString() {
                return "TaskInfo[" +
                    "taskRun=" + taskRun +
                    " task=" + task +
                    " future=" + future +
                    " nextJobNumber=" + nextJobNumber +
                    " futureJobNumber=" + futureJobNumber +
                    " completed=" + completed +
                    " examined=" + examined +
                    " additionalInfo=" + additionalInfo +
                    "]";
            }
        }
    }

    /**
     * Return the current state of the task as recorded in the future.
     */
    private Task.State checkState(Future<Task.State> future)
        throws Exception {

        if (future == null) {
            return Task.State.ERROR;
        }

        if (!future.isDone()) {
            return Task.State.RUNNING;
        }

        try {
            /*
             * The future is done, so we don't expect this call to block or
             * timeout, but specify a 0 timeout just in case, meaning that it
             * shouldn't block.
             */
            return future.get(0, TimeUnit.SECONDS);

        } catch (InterruptedException | TimeoutException e) {

            /*
             * We don't expect to be interrupted or timeout here, so act as if
             * the task is still running and let the caller try again.
             */
            return Task.State.RUNNING;

        } catch (Exception e) {
            /* this task has failed. */
            throw e;
        }
    }

    /** The result of examining a task future. */
    enum ExamineResult {

        /** Task or tasks all completed successfully, plan should continue */
        CONTINUE,

        /** One or more tasks failed, plan should stop */
        STOP,

        /** One or more tasks not done, check should be retried */
        NOT_DONE;
    }

    /**
     * Determine the state of a task based of the value of the future. May be
     * called concurrently, so must be thread safe.
     *
     * Information about the task execution that we may want to save and
     * display is held within the future and in the additionalInfo field.
     *
     * @return NOT_DONE if the task is not done, STOP if the task failed and
     * the plan says to not continue after a task failure, and CONTINUE if the
     * task succeeded or if it failed and the plan says to continue after a
     * failure
     */
    private ExamineResult examineFuture(Future<Task.State> future,
                                        TaskRun taskRun,
                                        Task task,
                                        String additionalInfo)
        throws Exception {

        /**
         * The plan should stop if (a) a failure occurs for a task that is
         * configured to stop the plan upon failure, or (b) an interrupt
         * is seen. In all other cases (task succeeds, or task fails but is
         * not supposed to stop the plan), the plan will continued.
         * A task may have different continurePastError values depending on
         * what failed within the task. This should be checked after an
         * error or exception is thrown.
         */
        boolean planShouldContinue = task.continuePastError();
        boolean notDone = false;

        Task.State tState = null;
        try {
            tState = checkState(future);

            /* The task returned, set the task state appropriately */
            if (tState == null) {
                plan.setTaskState(taskRun, Task.State.INTERRUPTED, logger);
                plan.saveFailure(taskRun,
                                 null, /* throwable */
                                 "Null status returned from future.get for " +
                                 task, ErrorMessage.NOSQL_5400,
                                 CommandResult.PLAN_CANCEL, logger);
                planShouldContinue = false;
            } else if (tState == Task.State.RUNNING) {
                /* The task is not done */
                notDone = true;
            } else if (tState == Task.State.INTERRUPTED) {
                /*
                 * If the task should restart then it succeeded but the plan
                 * should exit so that it can restart at a later time, likely
                 * on some other node. We mark the task as RUNNING and
                 * let the plan exit.
                 */
                if (taskRun.getTask().restartOnInterrupted()) {
                    taskRun.setState(Task.State.RUNNING, logger);
                } else {
                    taskRun.setState(Task.State.INTERRUPTED, logger);
                    plan.saveFailure(taskRun,
                                     null, /* throwable */
                                     "Task didn't complete, plan was " +
                                     "interrupted",
                                     ErrorMessage.NOSQL_5400,
                                     CommandResult.PLAN_CANCEL,
                                     logger);
                }
                planShouldContinue = false;
            } else if (tState == Task.State.ERROR) {
                planShouldContinue = task.continuePastError();
                ErrorMessage errorMsg = ErrorMessage.NOSQL_5500;
                String[] cleanupJobs = CommandResult.NO_CLEANUP_JOBS;
                CommandResult taskResult =
                    taskRun.getTask().getTaskResult();
                if (taskResult != null) {
                    errorMsg = ErrorMessage.getEnum(
                        taskResult.getErrorCode());
                    cleanupJobs = taskResult.getCleanupJobs();
                }
                recordTaskFailure(taskRun, tState, null, additionalInfo,
                                  errorMsg, cleanupJobs);
            } else if (tState == Task.State.SUCCEEDED) {
                taskRun.setState(tState, logger);
                planShouldContinue = true;
            } else {
                taskRun.setState(tState, logger);
            }
        } catch (Exception e) {

            /*
             * The thread that executed the task ran into a failure. Mark
             * this task as failed.
             * All task failed Exceptions should be wrapped by
             * CommandFaultException.
             */
            planShouldContinue = task.continuePastError();
            Throwable trueCause = e;
            ErrorMessage errorMsg = ErrorMessage.NOSQL_5500;
            String[] cleanupJobs = CommandResult.NO_CLEANUP_JOBS;
            if (e instanceof ExecutionException) {
                trueCause = e.getCause();
            }
            if (trueCause instanceof CommandFaultException) {
                errorMsg = ((CommandFaultException)trueCause).getErrorMessage();
                cleanupJobs =
                    ((CommandFaultException)trueCause).getCleanupJobs();
            }
            recordTaskFailure(taskRun, Task.State.ERROR, e,
                              additionalInfo, errorMsg, cleanupJobs);
        } finally {

            if ((future != null) &&
                ((taskRun.getState() == Task.State.ERROR) ||
                 (taskRun.getState() == Task.State.INTERRUPTED))) {

                /*
                 * If this task did execute and has any cleanup work, do it.
                 */
                final Runnable cleanupJob = task.getCleanupJob();
                if (cleanupJob != null) {
                    plan.cleanupStarted(taskRun);
                    logger.log(Level.INFO,
                               "Task {0}/{1} ended in {2}, cleaning up.",
                               new Object[] { taskRun.getTaskNum(),
                                              taskRun.getTaskName(),
                                              taskRun.getState()});
                    Future<?> f= pool.submit(cleanupJob);
                    taskCleanups.add(new CleanupInfo(taskRun, f));
                }
            }

            /* Bookkeeping when the task is complete */
            if (tState != Task.State.RUNNING) {
                plan.incrementEndCount(planRun, taskRun.getState());

                /*
                 * The Future is either done in which case this is a no-op, or
                 * it was interrupted or timed out, in which case it needs to
                 * be Canceled.
                 */
                if ((future != null) && !future.isDone()) {
                    future.cancel(true);
                }

                /*
                 * Keep updating the plan end time, after each task, so it's
                 * available even when plans fail.
                 */
                plan.setEndTime(planRun);

                /* Save the plan after each task. */
                synchronized (plan) {
                    admin.savePlan(plan, Admin.CAUSE_EXEC);
                }

                for (ExecutionListener listener : plan.getListeners()) {
                    listener.taskEnd(plan, task, taskRun, taskRun.getTaskNum(),
                                     totalTasks);
                }
            }
        }

        return notDone ? ExamineResult.NOT_DONE :
            !planShouldContinue ? ExamineResult.STOP :
            ExamineResult.CONTINUE;
    }

    /**
     * A struct to bundle together information needed for task cleanup
     */
    private class CleanupInfo {

        private final TaskRun taskRun;

        /* The future used for executing the cleanup. */
        private final Future<?> future;

        CleanupInfo(TaskRun taskRun, Future<?> future) {
            this.taskRun = taskRun;
            this.future = future;
        }

        TaskRun getTaskRun() {
            return taskRun;
        }

        Future<?> getFuture() {
            return future;
        }
    }

    /**
     * Get a list of unstarted tasks, for status reporting and logical
     * comparison in precisely the order that they will be spawned. Because of
     * that, this must match executeSerialTaskList and executeParallelTaskList.
     *
     * @param startTask the first task to return.
     */
    public static List<Task> getFlatTaskList(Plan plan, int startTask) {
        final TaskList taskList = plan.getTaskList();
        final List<Task> unstarted = new ArrayList<>();
        int taskCount = 0;
        for (Task task : taskList.getTasks()) {

            final TaskList nestedTasks = task.getNestedTasks();

            if (nestedTasks != null) {
                for (Task nested : nestedTasks.getTasks()) {
                    if (taskCount >= startTask) {
                        unstarted.add(nested);
                    }
                    taskCount++;
                }
                continue;
            }

            if (taskCount >= startTask) {
                unstarted.add(task);
            }
            taskCount++;
        }
        return unstarted;
    }

    /* For unit test support */
    public void setExecutor(ScheduledExecutorService executor) {
        this.pool = executor;
    }
}
