/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan;

import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminTestBase;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.PlanWaiter;
import oracle.kv.impl.admin.PlanStore;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.AbstractTask;
import oracle.kv.impl.admin.plan.task.JobWrapper;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.admin.plan.task.NextJob;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.SerialBundle;
import oracle.kv.impl.admin.plan.task.SingleJobTask;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.security.AccessCheckerImpl;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVBuiltInRoleResolver;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/**
 * Tests plan execution, rather than plan content.
 */
public class PlanExecutionTest extends AdminTestBase {

    private static final DurationParameter POLL_PERIOD =
        new DurationParameter("WAIT_BETWEEN_TASKS", TimeUnit.SECONDS, 1L);
    private static final String TEST_USER = "testUser";

    /*
     * Due to slack in the pruning logic, limits below 70 fail the pruning
     * test without upping the fudge factor. (See waitForPruning).
     */
    private final static int PLAN_LIMIT = 70;

    private Planner planner;
    private Admin admin;

    public PlanExecutionTest() {
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        if (admin != null) {
            logger.info("Shutting down Admin from teardown");
            admin.shutdown(false, "test teardown");
        }
        PlanStore.planLimitOverride = 0;
        /* Shutdown the bootstrap handler. */
        LoggerUtils.closeAllHandlers();
        super.tearDown();
    }

    /**
     * Test preconditions such as:
     * - plans must be approved and registered
     * - interrupted plans are cleaned up.
     */
    @Test
    public void testPreconditions()
        throws Exception {

        setupPlanner();

        AtomicBoolean stopHanging = new AtomicBoolean(false);
        CountDownLatch arrivedAtHang = new CountDownLatch(1);
        AbstractPlan hangPlan = new HangPlan("HangPlan", planner,
                                             stopHanging, arrivedAtHang);

        TaskStartWaiter taskStartWaiter = new TaskStartWaiter();
        hangPlan.addListener(taskStartWaiter);

        /* Don't run if plan has not been registered. */
        try {
            admin.savePlan(hangPlan, Admin.CAUSE_CREATE);
            admin.approvePlan(hangPlan.getId());
            fail(hangPlan + " must be registered");
        } catch (IllegalStateException expected) {
            /* IllegalStateException instead of IllegalCommandException
             * because it should not be possible for the user to create
             * a plan with out registering it.
             */
        }

        planner.register(hangPlan);

        /* Don't run if plan has not been approved. */
        try {
            planner.executePlan(hangPlan, false);
            fail(hangPlan + " must be approved");
        } catch (IllegalCommandException expected) {
        }

        admin.approvePlan(hangPlan.getId());

        /* Start the plan. Check that a different plan cannot be registered. */
        PlanRun planRun = planner.executePlan(hangPlan, false);
        AbstractPlan otherPlan = new GoodPlan("OtherPlan", planner);

        /* Wait for the first task to start before checking state. */
        taskStartWaiter.await();
        assertEquals(Plan.State.RUNNING, hangPlan.getState());

        /* Try to cancel a running plan. */
        try {
            planner.cancelPlan(hangPlan.getId());
            fail ("Shouldn't be able to cancel a running plan");
        } catch (IllegalCommandException expected) {
            assertEquals(expected.getErrorMessage(), ErrorMessage.NOSQL_5200);
            assertArrayEquals(expected.getCleanupJobs(),
                              CommandResult.NO_CLEANUP_JOBS);
        }

        assertEquals(Plan.State.RUNNING, hangPlan.getState());
        planner.interruptPlan(hangPlan.getId());

        /* Wait for the interrupt to take effect, and for the plan to end. */
        taskStartWaiter.waitForPlanEnd();

        /* The plan state should be INTERRUPTED */
        assertEquals("failure=" + planRun.getExceptionTransfer().getFailure(),
                     Plan.State.INTERRUPTED, hangPlan.getState());

        /*
         * Check that planner state has been cleared, and that we can run
         * another plan.
         */
        PlanWaiter planWaiter = new PlanWaiter();
        otherPlan.addWaiter(planWaiter);
        planner.register(otherPlan);
        planner.cancelPlan(hangPlan.getId());
        admin.savePlan(otherPlan, Admin.CAUSE_CREATE);
        admin.approvePlan(otherPlan.getId());
        planRun = planner.executePlan(otherPlan, false);
        admin.awaitPlan(otherPlan.getId(), 0, null);
        assertEquals(otherPlan.getLatestRunFailureDescription(),
                     Plan.State.SUCCEEDED, otherPlan.getState());
    }

    /*
     * Tests the case where the executed plan is the logical equivalent to the
     * running plan and is in lock conflict with it. In this rare case the
     * execute should succeed but return the ID of the running plan.
     */
    @Test
    public void testDupPlan() throws Exception {
        setupPlanner();

        AtomicBoolean stopHanging = new AtomicBoolean(false);
        CountDownLatch arrivedAtHang = new CountDownLatch(1);
        AbstractPlan hangPlan = new HangPlan("HangPlan", planner,
                                             stopHanging, arrivedAtHang);

        /* Add a task that will take out a catalog lock */
        hangPlan.addTask(new LockTask(hangPlan));
        TaskStartWaiter taskStartWaiter = new TaskStartWaiter();
        hangPlan.addListener(taskStartWaiter);

        planner.register(hangPlan);
        admin.savePlan(hangPlan, Admin.CAUSE_CREATE);
        admin.approvePlan(hangPlan.getId());
        planner.executePlan(hangPlan, false);

        /* Wait for the first task to start */
        taskStartWaiter.await();
        assertEquals(Plan.State.RUNNING, hangPlan.getState());

        /*
         * Create another plan which takes the same catalog lock, and
         * which "logicalCompare" returns true.
         */
        AtomicBoolean stopHanging2 = new AtomicBoolean(false);
        CountDownLatch arrivedAtHang2 = new CountDownLatch(1);
        AbstractPlan dupPlan = new HangPlan("DupPlan", planner,
                                            stopHanging2, arrivedAtHang2);
        dupPlan.addTask(new LockTask(dupPlan));

        planner.register(dupPlan);
        admin.savePlan(dupPlan, Admin.CAUSE_CREATE);
        admin.approvePlan(dupPlan.getId());

        /*
         * The hang plan is running and holding the catalog lock. Executing the
         * duplicate plan should work but the plan ID returned should be the
         * hang plan ID.
         */
        int dupPlanId = admin.executePlanOrFindMatch(dupPlan.getId());
        assertEquals(dupPlanId, hangPlan.getId());

        /*
         * The dupicate plan should be in the CANCELED state. In theory it could
         * be removed by pruning though that is unlikely here, check anyway.
         */
        Plan p = admin.getPlanById(dupPlan.getId());
        if (p != null) {
            assertEquals(p.getState(), Plan.State.CANCELED);
        }
    }

    /**
     * Ensure that a plan that fails is appropriately cleaned up.
     */
    @Test
    public void testPlanCleanup()
        throws Exception {

        setupPlanner();

        /* See if the planner can recover after running a plan that fails. */
        BadPlan badPlan = runBadPlan();

        /* Make sure the plan ran a cleanup after the main task fail. */
        assertEquals(2, badPlan.getMarker());

        planner.cancelPlan(badPlan.getId());
        runGoodPlan();

        badPlan = runBadPlan();
        assertEquals(2, badPlan.getMarker());
        planner.cancelPlan(badPlan.getId());

        runGoodPlan();
        AbstractPlan errorPlan = new ErrorPlan("ErrorPlan", planner);
        runPlanWithFailure(errorPlan, Plan.State.ERROR, java.lang.Error.class,
                           ERROR_TASK_MSG);
        CommandResult planResult = errorPlan.getCommandResult();
        assertEquals(planResult.getErrorCode(), 5500);
        assertArrayEquals(planResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
        planner.cancelPlan(errorPlan.getId());
    }

    @Test
    public void testPlanCleanupAndRetry() throws InterruptedException {

        setupPlanner();

        /*
         * Run a plan that continually increments a set of counters, using both
         * single, serial and multi-job parallel tasks. The multi-job tasks
         * never finish, and the plan needs to be halted with
         * plan.interrupt. When the task is interrupted, it should be cleaned
         * up. See CountingPlan for a description of the tasks in the plan.
         */
        CountingPlan countingPlan = new CountingPlan("CountingPlan",
                                                     planner,
                                                     5,  /* parallel tasks */
                                                     10); /* serial tasks */
        assertEquals(false, countingPlan.equalOrMore(1));

        admin.getPlanner().register(countingPlan);
        admin.savePlan(countingPlan, "countingPlan");
        admin.approvePlan(countingPlan.getId());

        /*
         * Make the counting tasks incur errors at different point. The
         * first set of errors when goal is <= 10, occur in the serial
         * part of the plan, while the invocations where goal is > 10 occur
         * in the parallel, multi-job task.
         */
        for (int goal = 1; goal < 20; goal+=2) {
            logger.info("Induce error at counting plan, goal=" + goal);
            countingPlan.makeErrorAtCount(goal);
            admin.executePlan(countingPlan.getId(), false);
            Plan.State status = admin.awaitPlan(countingPlan.getId(), 0, null);
            assertEquals(Plan.State.ERROR, status);
            assertEquals(Plan.State.ERROR, countingPlan.getState());
            assertEquals(false, countingPlan.equalOrMore(1));
            countingPlan.clearErrorVal();
        }

        /*
         * Run and interrupt the counting plan several times during the
         * parallel portion of the plan, checking each time that it cleans up.
         */
        for (int goal = 15; goal < 30; goal+=5) {
            logger.info("Iteration of counting plan, goal=" + goal);
            admin.executePlan(countingPlan.getId(), false);

            /* Wait for the counters to get to the value of "goal" */
            while (!countingPlan.equalOrMore(goal)) {
                Thread.sleep(5000);
            }

            assertTrue(countingPlan.minCount() >= goal);

            /*
             * Now interrupt the plan and make sure that after the interrupt
             * is processed, the plan cleanups all the counters back to 0.
             */
            admin.interruptPlan(countingPlan.getId());
            logger.info("Plan interrupted");

            Plan.State status = admin.awaitPlan(countingPlan.getId(), 0, null);
            logger.info("Plan ended");
            assertEquals(Plan.State.INTERRUPTED, status);
            assertEquals(false, countingPlan.equalOrMore(1));

        }

        /*
         * Run and interrupt the counting plan several times during the
         * serial portion of the plan. Since the serial tasks finish
         * successfully, they do not do any task cleanup.
         */
        for (int goal = 2; goal < 9; goal+=2) {
            CountDownLatch pauseLatch = new CountDownLatch(1);
            CountDownLatch arrivalLatch = new CountDownLatch(1);
            countingPlan.makePauseInSerialTasks(goal, pauseLatch, arrivalLatch);
            admin.executePlan(countingPlan.getId(), false);
            arrivalLatch.await();
            assertTrue(countingPlan.equalOrMore(goal));
            planner.interruptPlan(countingPlan.getId());
            pauseLatch.countDown();
            admin.awaitPlan(countingPlan.getId(), 0, null);
            logger.info("Plan ended");

            assertEquals(true, countingPlan.equalOrMore(goal));
            countingPlan.clearPauseInSerialTasks();
            countingPlan.resetCounters();

        }

        planner.cancelPlan(countingPlan.getId());
    }

    /**
     * Test that a running plan is restarted on Admin restart.
     */
    @Test
    public void testRunningPlanRecovery()
            throws InterruptedException, PlanLocksHeldException {
        testPlanRecovery(true);
    }

    /**
     * Test that an approved plan remains in that state on Admin restart.
     */
    @Test
    public void testApprovedPlanRecovery()
            throws InterruptedException, PlanLocksHeldException {
        testPlanRecovery(false);
    }

    private void testPlanRecovery(boolean running)
        throws InterruptedException, PlanLocksHeldException {

        final int waitIncrementMs = 400;
        final int maxWaitMs = 2000;

        setupPlanner();

        AtomicBoolean stopHangEx = new AtomicBoolean(false);
        CountDownLatch arrivedAtHang = new CountDownLatch(1);
        AbstractPlan hangExPlan =
            new HangPlan("hangExclPlan", planner, stopHangEx,
                         arrivedAtHang);
        planner.register(hangExPlan);

        TaskStartWaiter taskStartWaiter = new TaskStartWaiter();
        hangExPlan.addListener(taskStartWaiter);
        planner.approvePlan(hangExPlan.getId());

        if (running) {
            planner.executePlan(hangExPlan, false);

            taskStartWaiter.await();
            assertEquals(Plan.State.RUNNING, hangExPlan.getState());

            /*
             * Make sure that the HangPlan has reached the waiting state, and is
             * not in the middle of trying to write itself out to the database.
             * If we interrupt the write, we will get a spurious
             * IllegalStateException.
             */
            Map<Integer, Plan> allPlans = admin.getPlanRange(1, 10, null);
            admin.savePlan(hangExPlan, "saved by test");
            assertEquals(1, allPlans.size());
            for (Plan p: allPlans.values()) {
                assertEquals(Plan.State.RUNNING, p.getState());
            }
            arrivedAtHang.await();
        } else {
            assertEquals(Plan.State.APPROVED, hangExPlan.getState());
            admin.savePlan(hangExPlan, "saved by test");
        }
        admin.shutdown(true, "test");

        /* Restart */
        setupPlanner();
        /* The HangEx plan should be restarted, and should be RUNNING */
        Map<Integer, Plan> allPlans = admin.getPlanRange(1, 10, null);
        assertEquals(1, allPlans.size());
        Plan resurrected = null;
        for (Plan p: allPlans.values()) {
            resurrected = p;
        }

        if (resurrected == null) {
            throw new IllegalStateException("There should be a RUNNING plan");
        }

        /*
         * Plan restart is asynchronous and there's no simple way to
         * synchronize with the admin so wait.  If it takes more than a few
         * seconds there's a problem.
         */
        int waitMs = 0;
        int resurrectedId = resurrected.getId();
        while (true) {
            /*
             * The Plan returned from admin.getPlans is a copy;
             * we need the real thing here.
             */
            resurrected = admin.getPlanById(resurrectedId);
            if (!running) {
                assertEquals(Plan.State.APPROVED, resurrected.getState());
                break;
            }
            if (resurrected.getState() == Plan.State.RUNNING) {
                break;
            }
            if (waitMs > maxWaitMs) {
                ExceptionTransfer transfer =
                    ((AbstractPlan)resurrected).getExceptionTransfer();
                if (transfer != null) {
                    fail ("Plan is in unexpected state " +
                          resurrected.getState() +
                          " " + transfer.toString());
                } else {
                    logger.info("Plan state is wrong");
                    fail (resurrected + " is in unexpected state " +
                          resurrected.getState());
                }
                break;
            }
            Thread.sleep(waitIncrementMs);
            waitMs += waitIncrementMs;
        }

        admin.shutdown(true, "test");
    }

    /**
     * Check the parallel task infrastructure. The BarrierPlan uses a
     * cyclic barrier to ensure that multiple tasks execute concurrently.
     */
    @Test
    public void testParallelExecution() {
        setupPlanner();

        /* There will be 10 parallel tasks. */
        int numParallelTasks = 10;

        BarrierPlan barrierPlan = new BarrierPlan("BarrierPlan",
                                                  planner, numParallelTasks, 0);
        runPlan(barrierPlan, Plan.State.SUCCEEDED);
        assertEquals(2, barrierPlan.goodTaskCounter.get());
    }

    /**
     * Tests running a serial bundle within a parallel bundle.
     */
    @Test
    public void testParallelSerialExecution() {
        setupPlanner();

        BarrierPlan barrierPlan = new BarrierPlan("BarrierPlan",
                                                  planner,
                                                  10, 10);
        runPlan(barrierPlan, Plan.State.SUCCEEDED);
        assertEquals(12, barrierPlan.goodTaskCounter.get());
    }

    @Test
    public void testPlanInterruptApprLockEla() throws Exception {

        /*
         * Plan topo hold the elasticity lock and in approve state
         */
        setupPlanner();
        Plan topo = new TopoPlan("topo", planner);
        admin.getPlanner().register(topo);
        admin.savePlan(topo, "topo");
        admin.approvePlan(topo.getId());
        planner.lockElasticity(topo.getId(), "topo");

        /*
         * Interrupt the plan, it should also clean up the elasticity lock
         */
        admin.interruptPlan(topo.getId());

        /*
         * Lock elasticity for the next plan should pass.
         */
        planner.lockElasticity(2, "second plan");
    }

    /**
     * Test whether the context can be accessed in plan execution.
     */
    @Test
    public void testPlanExecutionContext() {
        final AuthContext authCtx = new AuthContext(
            new LoginToken(new SessionId(new byte[4]), 0L));
        final KVBuiltInRoleResolver resolver = new KVBuiltInRoleResolver();
        final OperationContext opCtx = new PlanExecutionContext(TEST_USER);
        ExecutionContext execCtx = ExecutionContext.create(new PlanExeChecker(
            resolver, Logger.getLogger("PlanExecutionTest")), authCtx, opCtx);

        /* Run actual test with the context created */
        ExecutionContext.runWithContext(
            (Runnable) () -> {
                setupPlanner();
                AbstractPlan newPlan = new GoodPlan("gd", planner);
                admin.getPlanner().register(newPlan);
                newPlan.addListener(new ContextChecker());
                planner.approvePlan(newPlan.getId());
                try {
                    planner.executePlan(newPlan, false);
                } catch (PlanLocksHeldException e) {
                    /* Should not get an exception here */
                    throw new IllegalCommandException(e.getMessage());
                }
                admin.awaitPlan(newPlan.getId(), 0, null);
            },
            execCtx);
    }

    /**
     * Test Planner failed to submit plan due to RejectedExecution
     *
     * This test waits for the system table monitor to exit. The problem is that
     * the monitor will not exit if a system table requires the new version,
     * or no user tables are created (see note in TestBase.java). The test
     * may pass in some releases but is undependable due to this issue.
     */
    @Test
    public void testPlannerRejectedExecution() throws Exception {
        /* Establish mocks */
        ExecutorService executor = createMock(ExecutorService.class);
        expect(executor.submit(anyObject(PlanExecutor.class))).andThrow(
            new RejectedExecutionException("test"));
        executor.shutdown();
        expectLastCall();
        expect(executor.awaitTermination(anyLong(), anyObject(TimeUnit.class))).
            andReturn(true);
        replay(executor);
        /* execute and verify return code and cleanup jobs */
        setupPlanner();
        try {
            /*
             * Wait for SysTableMonitorThread that will execute system plan.
             * Only after system plan completed, we can use the mock
             * ExecutorService override original ExecutorService safely.
             * Otherwise, the mock ExecutorService.submit will throw Exception
             * as it is called more times than expected.
             */
            admin.joinSysTableMonitorThread();
        } catch (InterruptedException e) {
            fail("fail to join SysTableMonitorThread in Admin");
        }
        planner.setExecutor(executor);
        try {
            runGoodPlan();
        } catch (CommandFaultException expected) {
            assertEquals(expected.getErrorMessage(), ErrorMessage.NOSQL_5400);
            assertArrayEquals(expected.getCleanupJobs(),
                              CommandResult.PLAN_CANCEL);
        }
    }

    /**
     * Test PlanExecutor failed to submit serial task due to RejectedExecution
     */
    @Test
    public void testSerialTaskRejectedExecution() {
        /* Establish mocks */
        setupPlanner();
        AbstractPlan targetPlan = new GoodPlan("GoodPlan", planner);
        admin.getPlanner().register(targetPlan);
        admin.savePlan(targetPlan, "unit test");
        admin.approvePlan(targetPlan.getId());
        PlanRun planRun = targetPlan.startNewRun();
        final PlanExecutor planExec = new PlanExecutor(admin, planner,
                                                       targetPlan,
                                                       planRun, logger);
        ScheduledExecutorService executor =
            createMock(ScheduledExecutorService.class);
        expect(executor.submit(anyObject(PlanExecutor.class))).andThrow(
            new RejectedExecutionException("test"));
        expect(executor.shutdownNow()).andReturn(null);
        replay(executor);
        planExec.setExecutor(executor);
        /* execute and verify return code and cleanup jobs */
        try {
            planExec.call();
            fail("expect get exception");
        } catch (Exception e) { /* ignore */
        }
        Plan plan = planner.getCachedPlan(targetPlan.getId());
        CommandResult cmdResult = plan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5400);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.PLAN_CANCEL);
    }

    /**
     * Test PlanExecutor failed to submit serial task due to RuntimeExecution
     */
    @Test
    public void testSerialTaskRuntimeExecution() {
        /* Establish mocks */
        setupPlanner();
        AbstractPlan targetPlan = new GoodPlan("GoodPlan", planner);
        admin.getPlanner().register(targetPlan);
        admin.savePlan(targetPlan, "unit test");
        admin.approvePlan(targetPlan.getId());
        PlanRun planRun = targetPlan.startNewRun();
        final PlanExecutor planExec = new PlanExecutor(admin, planner,
                                                       targetPlan,
                                                       planRun, logger);
        ScheduledExecutorService executor =
            createMock(ScheduledExecutorService.class);
        expect(executor.submit(anyObject(PlanExecutor.class))).andThrow(
            new RuntimeException("test"));
        expect(executor.shutdownNow()).andReturn(null);
        replay(executor);
        planExec.setExecutor(executor);
        /* execute and verify return code and cleanup jobs */
        try {
            planExec.call();
        } catch (Exception e) { /* ignore */
        }
        final Plan plan = planner.getCachedPlan(targetPlan.getId());
        CommandResult cmdResult = plan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5500);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
    }

    /**
     * Test PlanExecutor failed to submit parallel task due to RejectedExecution
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testParallelTaskRejectedExecution() throws Exception {
        /* Establish mocks */
        setupPlanner();
        AbstractPlan targetPlan = new BarrierPlan("BarrierPlan", planner, 3, 0);
        admin.getPlanner().register(targetPlan);
        admin.savePlan(targetPlan, "unit test");
        admin.approvePlan(targetPlan.getId());
        PlanRun planRun = targetPlan.startNewRun();
        final PlanExecutor planExec = new PlanExecutor(admin, planner,
                                                       targetPlan,
                                                       planRun, logger);
        ScheduledExecutorService executor =
                createMock(ScheduledExecutorService.class);
        Future<Task.State> future = createMock(Future.class);
        /* mock GoodTask future */
        expect(future.get(anyLong(), anyObject(TimeUnit.class))).
            andReturn(Task.State.SUCCEEDED);
        expect(future.isDone()).andReturn(true).times(4);
        replay(future);
        expect(executor.submit(anyObject(Callable.class))).andReturn(future);
        /* mock BarrierTask future that will throw RejectedExecutionException */
        expect(executor.submit(anyObject(Callable.class))).andThrow(
            new RejectedExecutionException("test"));
        expect(executor.shutdownNow()).andReturn(null);
        replay(executor);
        planExec.setExecutor(executor);
        /* execute and verify return code and cleanup jobs */
        try {
            planExec.call();
            fail("expect get exception");
        } catch (Exception e) { /* ignore */
        }
        Plan plan = planner.getCachedPlan(targetPlan.getId());
        CommandResult cmdResult = plan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5400);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.PLAN_CANCEL);
    }

    /**
     * Test PlanExecutor failed to submit parallel task due to RuntimeExecution
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testParallelTaskRunTimeExecution() throws Exception {
        setupPlanner();
        AbstractPlan targetPlan = new BarrierPlan("BarrierPlan", planner, 3, 0);
        admin.getPlanner().register(targetPlan);
        admin.savePlan(targetPlan, "unit test");
        admin.approvePlan(targetPlan.getId());
        PlanRun planRun = targetPlan.startNewRun();
        final PlanExecutor planExec = new PlanExecutor(admin, planner,
                                                       targetPlan,
                                                       planRun, logger);
        ScheduledExecutorService executor =
                createMock(ScheduledExecutorService.class);
        /* mock GoodTask1 future */
        Future<Task.State> future = createMock(Future.class);
        expect(future.get(anyLong(), anyObject(TimeUnit.class))).
            andReturn(Task.State.SUCCEEDED);
        expect(future.isDone()).andReturn(true).times(4);
        replay(future);
        /* mock 3 BarrierTask futures, and the first will throw
         * RuntimeException. */
        expect(executor.submit(anyObject(Callable.class))).andReturn(future);
        expect(executor.submit(anyObject(Callable.class))).andThrow(
            new RuntimeException("test"));
        expect(executor.submit(anyObject(Callable.class))).andReturn(null);
        expect(executor.submit(anyObject(Callable.class))).andReturn(null);
        /* mock GoodTask2 future */
        Future<Task.State> future2 = createMock(Future.class);
        expect(future2.get(anyLong(), anyObject(TimeUnit.class))).
            andReturn(Task.State.SUCCEEDED);
        expect(future2.isDone()).andReturn(true).times(4);
        replay(future2);
        expect(executor.submit(anyObject(Callable.class))).andReturn(future2);
        expect(executor.shutdownNow()).andStubReturn(null);
        replay(executor);
        planExec.setExecutor(executor);
        /* execute and verify return code and cleanup jobs */
        planExec.call();
        Plan plan = planner.getCachedPlan(targetPlan.getId());
        CommandResult cmdResult = plan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5500);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
    }

    /**
     * Test SingleJobTask handle Exceptions.
     */
    @Test
    public void testSingleJobTaskThrowException() {
        setupPlanner();
        /* badtask throw unkown Exception */
        BadPlan badPlan = runBadPlan();
        CommandResult cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5500);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
        planner.cancelPlan(badPlan.getId());
        /* badtask throw CommandFaultException */
        Exception fault = new CommandFaultException(
            "test",ErrorMessage.NOSQL_5400, CommandResult.TOPO_PLAN_REPAIR);
        badPlan = runBadPlan(fault);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5400);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.TOPO_PLAN_REPAIR);
        planner.cancelPlan(badPlan.getId());
        /* badtask throw AdminFaultException */
        fault = new AdminFaultException(new Exception("test"), "test",
                                        ErrorMessage.NOSQL_5200,
                                        CommandResult.NO_CLEANUP_JOBS);
        badPlan = runBadPlan(fault);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5200);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
        planner.cancelPlan(badPlan.getId());
        /* badtask throw DBOperationFailedException */
        fault = new Admin.DBOperationFailedException("test");
        badPlan = runBadPlan(fault);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5400);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.PLAN_CANCEL);
        planner.cancelPlan(badPlan.getId());
        /* badtask throw NonfatalAssertionException */
        fault = new NonfatalAssertionException("test");
        badPlan = runBadPlan(fault);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5500);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
        planner.cancelPlan(badPlan.getId());
        /* badtask throw KVSecurityException */
        fault = new AuthenticationFailureException("test");
        badPlan = runBadPlan(fault);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5100);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.NO_CLEANUP_JOBS);
        planner.cancelPlan(badPlan.getId());
        /* badtask throw SNAFaultException */
        fault = new SNAFaultException(new Exception("test"));
        badPlan = runBadPlan(fault);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), 5400);
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          CommandResult.PLAN_CANCEL);
        planner.cancelPlan(badPlan.getId());
        /* badtask return Error */
        badPlan = new BadPlan("BadPlan", planner, null);
        runPlan(badPlan, Plan.State.ERROR);
        cmdResult = badPlan.getCommandResult();
        assertEquals(cmdResult.getErrorCode(), badTaskResult.getErrorCode());
        assertArrayEquals(cmdResult.getCleanupJobs(),
                          badTaskResult.getCleanupJobs());
        planner.cancelPlan(badPlan.getId());

        runGoodPlan();
    }

    /**
     * Tests plan pruning. There are some heuristics used in this test due
     * to non deterministic behavior of concurrent attempts to prune plans.
     * If an attempt is made and pruning is currently running, the new attempt
     * is not performed.
     *
     * Note that the number of system plans that execute when the store (and
     * test) start may vary.
     */
    @Test
    public void testPlanPruning()
        throws Exception {
        PlanStore.planLimitOverride = PLAN_LIMIT;

        setupPlanner();

        /*
         * Run good plans through. These will result in terminal state plans.
         */
        for (int i = 0; i < PLAN_LIMIT; i++) {
            runGoodPlan("GoodPlanA" + i);
        }

        /*
         * There should be at least PLAN_LIMIT number of plans. There may be
         * more due to system plans in the mix.
         */
        PlanStoreInfo postPsi = getPlanStoreInfo();
        int expected = PLAN_LIMIT;
        if (postPsi.getUserPlans() < expected) {
            fail("Unexpected number of plans, expected at least " + expected +
                 " but found " + postPsi.getUserPlans());
        }

        for (int i = 0; i < PLAN_LIMIT; i++) {
            runGoodPlan("GoodPlanB" + i);
        }

        waitForPruning();

        /*
         * Check that system plans are pruned before user ones
         */
        PlanStoreInfo prePsi = getPlanStoreInfo();
        for (int i = 0; i < PLAN_LIMIT/2; i++) {
            runSystemPlan("GoodSystemPlan" + i);
        }

        waitForPruning();

        postPsi = getPlanStoreInfo();

        /* Check that not too many user plans were pruned. */
        if (postPsi.getUserPlans() < (PLAN_LIMIT - 1)) {
            fail("Unexpected pruning of user plans. pre count " +
                 prePsi.getUserPlans() + "post count " +
                 postPsi.getUserPlans());
        }

        /*
         * Now execute the limit number of bad plans.
         */
        final Exception fault = new BadTaskException(BAD_TASK_MSG);
        for (int i = 0; i < PLAN_LIMIT; i++) {
            runBadPlan("BadPlanA" + i, fault);
        }

        /*
         * The previous good plans should be pruned away.
         */
        waitForPruning();

        postPsi = getPlanStoreInfo();

        /*
         * The check for the number of remaining terminal plans is fuzzy.
         * Plan pruning can start any time the insertion is taking place.
         * Insertion of bad plans will occur while pruning is occurring.
         * Also, the number of system plans can change and affect the number
         * of remaining terminal plans. So just check non-terminal plans.
         */
        if (postPsi.getUserNonTerminal() != PLAN_LIMIT) {
            fail("Unexpected number of user non terminal state plans. " +
                 "Expected " + PLAN_LIMIT + " but only had " +
                 postPsi.getUserNonTerminal());
        }

        /*
         * Insert a combination of plans. Order is important
         * in order to take in account of plans insertion going into
         * different prune time windows.
         */
        for (int i = 0; i < PLAN_LIMIT; i++) {
            runSystemPlan("GoodSystemPlanB" + i);
            runGoodPlan("GoodPlanC" + i);
            runBadPlan("BadPlanB" + i, fault);
        }
        waitForPruning();

        postPsi = getPlanStoreInfo();
        if (postPsi.getTotalPlans() > (PLAN_LIMIT + (PLAN_LIMIT * .1))) {
            fail("Expected less totoal number of plans. " + postPsi);
        }

        if (postPsi.getSystemPlans() > postPsi.getUserNonTerminal() ||
            postPsi.getSystemPlans() > postPsi.getUserTerminal()) {
            fail("Number of system plans greater than user plans. " + postPsi);
        }


        if (postPsi.getUserTerminal() > postPsi.getUserNonTerminal() ||
            postPsi.getUserTerminal() > postPsi.getUserTerminal()) {
            fail("Number of system plans greater than user plans. " + postPsi);
        }
    }

    /**
     * Waits for plan pruning to reduce the number of plans to PLAN_LIMIT or
     * below. Will fail if PLAN_LIMIT is not reached.
     */
    private void waitForPruning() {
        final AtomicInteger nPlans = new AtomicInteger();
        boolean ok = new PollCondition(200, 5000) {
            @Override
            protected boolean condition() {
                nPlans.set(getPlanStoreInfo().getTotalPlans());
                /*
                 * Since pruning is asynchronous we check if under
                 * PLAN_LIMIT+20%. What can happen is if a plan is added during
                 * a scan and the plan does not finish before the scan completes
                 * it will remain in the store until the next scan.
                 */
                return nPlans.get() <= PLAN_LIMIT * 1.2;
            }
        }.await();

        if (!ok) {
            fail("Unexpected number of plans, expected less than or equal to " +
                 PLAN_LIMIT + " but found " + nPlans.get());
        }
    }

    private PlanStoreInfo getPlanStoreInfo() {
        int start = 0;
        int nSys = 0;
        int nUserNonTerm = 0;
        int nUserTerm = 0;
        while (true) {
            final Map<Integer, Plan> plans =
                    admin.getPlanRange(start, Integer.MAX_VALUE, null);
            if (plans.isEmpty()) {
                break;
            }
            for (Plan p : plans.values()) {
                if (p.isSystemPlan()) {
                    nSys++;
                } else if (p.getState().isTerminal()) {
                    nUserTerm++;
                } else {
                    nUserNonTerm++;
                }

                if (p.getId() >= start) {
                    start = p.getId() + 1;
                }
            }
        }
        return new PlanStoreInfo(nSys, nUserTerm, nUserNonTerm);
    }

    private void setupPlanner() {
        admin = new Admin(atc.getParams());
        planner = admin.getPlanner();
    }

    /**
     * Run a plan that will throw a BadTaskException.
     */
    private BadPlan runBadPlan() {

        Exception fault = new BadTaskException(BAD_TASK_MSG);
        return runBadPlan(fault);
    }

    /**
     * Run a plan that will throw a BadTaskException.
     */
    private BadPlan runBadPlan(Exception e) {
        return runBadPlan("BadPlan", e);
    }

    /**
     * Runs a plan that will throw the specified exception.
     *
     * @param planName the plan name
     * @param e the exception thrown by the plan
     * @return the plan
     */
    private BadPlan runBadPlan(String planName,
                               Exception e) {

        BadPlan badPlan = new BadPlan(planName, planner, e);
        runPlanWithFailure(badPlan, Plan.State.ERROR, e.getClass(),
                           e.getMessage());
        return badPlan;
    }

    /**
     * Run a plan that is successful.
     * @throws InterruptedException
     */
    private void runGoodPlan() {
        runGoodPlan("GoodPlan");
    }

    /**
     * Runs a plan that is successful with the specified name.
     *
     * @param planName plan name
     */
    private void runGoodPlan(String planName) {

        GoodPlan goodPlan = new GoodPlan(planName, planner);
        runPlan(goodPlan, Plan.State.SUCCEEDED);
        assertEquals(1, goodPlan.goodTaskCounter.get());
    }

    private void runSystemPlan(String planName) {
        GoodSystemPlan goodPlan =
            new GoodSystemPlan(planName, planner);
        runPlan(goodPlan, Plan.State.SUCCEEDED);
        assertEquals(1, goodPlan.goodTaskCounter.get());
    }

    /**
     * Expect an exception when executing this plan, and check the exception
     * and failure description.
     */
    private void runPlanWithFailure(AbstractPlan plan,
                                    Plan.State expectedState,
                                    Class<?> expectedExceptionClass,
                                    String expectedExceptionDesc) {

        runPlan(plan, expectedState);

        /*
         * The plan should store the Throwable and descriptions of
         * the underlying problem.
         */
        Throwable problem = plan.getExceptionTransfer().getFailure();
        assertEquals(expectedExceptionClass, problem.getClass());

        String completeDesc = plan.getLatestRunFailureDescription();
        String shortDesc = plan.getExceptionTransfer().getDescription();

        if (expectedExceptionDesc != null) {
            assertTrue(completeDesc.contains(expectedExceptionDesc));
            assertTrue(shortDesc.contains(expectedExceptionDesc));
        } else {
            assertNotNull(completeDesc);
            assertNotNull(shortDesc);
        }
    }

    /**
     * Execute the specified plan. Throws any exception that occurs.
     * @return the plan id.
     * @throws InterruptedException
     */
    private int runPlan(AbstractPlan plan, Plan.State expectedState){

        /*
         * Registering the plan is usually done within the planner impl, but
         * for artificially created unit test plans, is done outside.
         */
        admin.getPlanner().register(plan);
        admin.savePlan(plan, "unit test");
        ExecutionChecker checker = new ExecutionChecker(plan);
        plan.addListener(checker);
        TaskStartWaiter waiter = new TaskStartWaiter();
        plan.addListener(waiter);

        /* Approve the plan. */
        admin.approvePlan(plan.getId());

        /* Execute it and wait for it to finish. */
        admin.executePlan(plan.getId(), false);
        Plan.State status = admin.awaitPlan(plan.getId(), 0, null);

        /* If it failed, the executePlan should throw. */
        assertEquals(plan.getLatestRunFailureDescription(),
                     expectedState, status);
        if (expectedState == Plan.State.SUCCEEDED) {
           String failure = plan.getLatestRunFailureDescription();
           if ((failure != null) && failure.length() > 1) {
                fail("Did not expect failure " + failure);
           }
           checker.verify();
        }

        return plan.getId();
    }

    private class PlanStoreInfo {
        private int nSystem;
        private int nUserTerminal;
        private int nUserNonTerminal;

        PlanStoreInfo(int nSys, int nUserTerm, int nUserNonTerm) {
            nSystem = nSys;
            nUserTerminal = nUserTerm;
            nUserNonTerminal = nUserNonTerm;
        }

        int getTotalPlans() {
            return nSystem + nUserTerminal + nUserNonTerminal;
        }

        int getUserPlans() {
            return nUserTerminal + nUserNonTerminal;
        }

        int getSystemPlans() {
            return nSystem;
        }

        int getUserTerminal() {
            return nUserTerminal;
        }

        int getUserNonTerminal() {
            return nUserNonTerminal;
        }

        @Override
        public String toString() {
            return "User non terminal [" + getUserNonTerminal() + "] " +
                   "User terminal [" + getUserTerminal() + "] " +
                   "System [" + getSystemPlans() + "] " +
                   "Total [" +  getTotalPlans() + "] " +
                   "Total user [" + getUserPlans() + "] ";
        }
    }
    /**
     * A Plan with one task, which will fail.
     */
    private static class BadPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;

        private final transient AtomicInteger marker;

        BadPlan(String name, Planner planner,
                Exception fault) {
            super(name, planner);
            marker = new AtomicInteger(0);
            addTask(new BadTask(marker, fault));
        }

        @Override
        public String getDefaultName() {
            return "BadPlan";
        }

        private int getMarker() {
            return marker.get();
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }

        @Override
        public ObjectNode getPlanJson() {
            ObjectNode jsonTop = JsonUtils.createObjectNode();
            jsonTop.put("plan_id", getId());
            return jsonTop;
        }

        @Override
        public String getOperation() {
            return "plan BadPlan";
        }
    }

    private static final String BAD_TASK_MSG = "This is an PROBLEM!";
    private static final CommandResult badTaskResult =
            new CommandResult.CommandFails("test", ErrorMessage.NOSQL_5400,
                                           CommandResult.PLAN_CANCEL);

    private static class BadTask extends SingleJobTask {

        private static final long serialVersionUID = 1L;
        private final transient AtomicInteger marker;
        private volatile Exception fault;

        BadTask(AtomicInteger marker, Exception fault) {
            this.marker = marker;
            this.fault = fault;
        }

        @Override
        protected Plan getPlan() {
            return null;
        }

        @Override
            public Task.State doWork()
            throws Exception {
            marker.incrementAndGet();
            if (fault != null) {
                throw fault;
            }
            this.setTaskResult(badTaskResult);
            return State.ERROR;
        }

        @Override
            public Runnable getCleanupJob() {
            return new Runnable() {
                @Override
                    public void run(){
                    marker.incrementAndGet();
                }
            };
        }

        @Override
            public boolean continuePastError() {
            return false;
        }
    }

    /**
     * A Plan with one task, which will throw a Java Error
     */
    private static class ErrorPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;

        ErrorPlan(String name, Planner planner) {
            super(name, planner);
            addTask(new ErrorTask());
        }

        @Override
        public String getDefaultName() {
            return "ErrorPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }

        @Override
        public ObjectNode getPlanJson() {
            ObjectNode jsonTop = JsonUtils.createObjectNode();
            jsonTop.put("plan_id", getId());
            return jsonTop;
        }

        @Override
        public String getOperation() {
            return "plan ErrorPlan";
        }
    }

    private static final String ERROR_TASK_MSG = "This is an generated ERROR!";

    private static class ErrorTask extends SingleJobTask {

        private static final long serialVersionUID = 1L;

        @Override
        protected Plan getPlan() {
            return null;
        }

        @Override
        public Task.State doWork()
            throws Exception {
            throw new Error(ERROR_TASK_MSG);
        }

        @Override
        public boolean continuePastError() {
            return false;
        }
    }

    /**
     * A Plan with one task, which will succeed.
     */
    private static class GoodPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;
        public final transient AtomicInteger goodTaskCounter;

        GoodPlan(String name, Planner planner) {
            super(name, planner);
            goodTaskCounter = new AtomicInteger(0);
            addTask(new GoodTask(goodTaskCounter));
        }

        @Override
        public void preExecuteCheck(boolean force, Logger plannerlogger) {
            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx != null) {
                final KVStoreUserPrincipal user =
                        ExecutionContext.getSubjectUserPrincipal(
                            exeCtx.requestorSubject());
                assertEquals(user.getName(), TEST_USER);
            }
        }

        @Override
        public String getDefaultName() {
            return "GoodPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }

        @Override
        public ObjectNode getPlanJson() {
            ObjectNode jsonTop = JsonUtils.createObjectNode();
            jsonTop.put("plan_id", getId());
            return jsonTop;
        }

        @Override
        public String getOperation() {
            return "plan GoodPlan";
        }
    }

    private static class GoodTask extends SingleJobTask {

        private static final long serialVersionUID = 1L;
        private final transient AtomicInteger goodTaskCounter;

        GoodTask(AtomicInteger goodTaskCounter) {
            this.goodTaskCounter = goodTaskCounter;
        }

        @Override
        protected Plan getPlan() {
            return null;
        }

        @Override
        public Task.State doWork()
            throws Exception {
            goodTaskCounter.incrementAndGet();
            return Task.State.SUCCEEDED;
        }

        @Override
        public boolean continuePastError() {
            return false;
        }
    }

    /**
     * A Plan with one task, which will succeed.
     */
    private static class GoodSystemPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;
        public final transient AtomicInteger goodTaskCounter;

        GoodSystemPlan(String name, Planner planner) {
            super(name, planner, true);
            goodTaskCounter = new AtomicInteger(0);
            addTask(new GoodTask(goodTaskCounter));
        }

        @Override
        public void preExecuteCheck(boolean force, Logger plannerlogger) {
            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            if (exeCtx != null) {
                final KVStoreUserPrincipal user =
                        ExecutionContext.getSubjectUserPrincipal(
                            exeCtx.requestorSubject());
                assertEquals(user.getName(), TEST_USER);
            }
        }

        @Override
        public String getDefaultName() {
            return "GoodSystemPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }

        @Override
        public ObjectNode getPlanJson() {
            ObjectNode jsonTop = JsonUtils.createObjectNode();
            jsonTop.put("plan_id", getId());
            return jsonTop;
        }

        @Override
        public String getOperation() {
            return "plan GoodSystemPlan";
        }
    }


    /**
     * A Plan with a task which waits until a latch is released.
     */
    private static class HangPlan extends AbstractPlan {
        private static final long serialVersionUID = 1L;

        HangPlan(String name, Planner planner,
                 AtomicBoolean finishHanging,
                 CountDownLatch gotToHangState) {
            super(name, planner);
            addTask(new HangTask(finishHanging, gotToHangState, this));
        }

        @Override
        public String getDefaultName() {
            return "HangPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }
    }

    private static class LockTask extends SingleJobTask {
        private static final long serialVersionUID = 1L;

        private final AbstractPlan plan;

        LockTask(AbstractPlan plan) {
            this.plan = plan;
        }

        @Override
        public void acquireLocks(Planner planner) throws PlanLocksHeldException {
            planner.lock(plan.getId(), plan.getName(),
                         Planner.LockCategory.TABLE, "TEST");
        }

        @Override
        public State doWork() throws Exception {
            return Task.State.SUCCEEDED;
        }

        @Override
        protected Plan getPlan() {
            return plan;
        }

        @Override
        public boolean continuePastError() {
            return true;
        }

        @Override
        public boolean logicalCompare(Task otherTask) {
            return true;
        }
    }

    private static class HangTask extends SingleJobTask {

        private static final long serialVersionUID = 1L;
        private final transient AtomicBoolean finishHanging;
        private final transient CountDownLatch gotToHangState;
        private final AbstractPlan plan;

        HangTask(AtomicBoolean finishHanging,
                 CountDownLatch gotToHangState,
                 AbstractPlan plan) {
            this.finishHanging = finishHanging;
            this.gotToHangState = gotToHangState;
            this.plan = plan;
        }

        @Override
        protected Plan getPlan() {
            return null;
        }

        @Override
        public Task.State doWork()
            throws Exception {
            /*
             * Let the code which is executing the hang test know that we
             * arrived at the hang. Note that finishedHanging and gotToHangState
             * are transient fields, and therefore are not re-initialized
             * if the task runs again after a plan recovery. This is not
             * kosher, and we're only doing this in test situations. If these
             * fields are null, just proceed on and hang indefinitely.
             */
            if (gotToHangState != null) {
                gotToHangState.countDown();
            }
            do {
                if (finishHanging != null) {
                    if (finishHanging.get()) {
                        return Task.State.SUCCEEDED;
                    }
                }

                Thread.sleep(10000);
                if (plan.isInterruptRequested()) {
                    return Task.State.INTERRUPTED;
                }
            } while (true);
        }

        @Override
        public boolean continuePastError() {
            return false;
        }

        @Override
        public boolean logicalCompare(Task otherTask) {
            return true;
        }
    }

    /**
     * A Plan that is meant to execute parallel tasks.
     */
    private static class BarrierPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;
        private transient final CyclicBarrier barrier;
        private transient final AtomicInteger startCounter;
        private transient final AtomicInteger finishCounter;
        final transient AtomicInteger goodTaskCounter;

        /**
         * Tasks are:
         *       GoodTask
         *          |
         *   +------+-------+ - - - - - +
         *   |      |       |           |
         * Barrier Barrier Barrier  Good Tasks
         * Task    Task    Task     (Optional)
         *   |      |       |           |
         *   +------+-------+ - - - - - +
         *          |
         *       GoodTask
         */
        BarrierPlan(String name, Planner planner,
                    int numParallelTasks, int numSerialTasks) {
            super(name, planner);
            barrier = new CyclicBarrier(numParallelTasks);
            startCounter = new AtomicInteger();
            finishCounter = new AtomicInteger();
            goodTaskCounter = new AtomicInteger();

            addTask(new GoodTask(goodTaskCounter));

            ParallelBundle bundle = new ParallelBundle();
            for (int i = 0; i < numParallelTasks; i++) {
                bundle.addTask(new BarrierTask(barrier, startCounter,
                                               finishCounter));
            }
            if (numSerialTasks > 0) {
                final SerialBundle sb = new SerialBundle();
                for (int i = 0; i < numSerialTasks; i++) {
                    /* Add a mix of single and multi-job tasks */
                    if ((i & 1) == 0) {
                        sb.addTask(new GoodTask(goodTaskCounter));
                    } else {
                        sb.addTask(new MultiJobTask(this, 3, goodTaskCounter));
                    }
                }
                bundle.addTask(sb);
            }
            addTask(bundle);
            addTask(new GoodTask(goodTaskCounter));
        }

        @Override
            public String getDefaultName() {
            return "BarrierPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }

        @Override
        public ObjectNode getPlanJson() {
            ObjectNode jsonTop = JsonUtils.createObjectNode();
            jsonTop.put("plan_id", getId());
            return jsonTop;
        }

        @Override
        public String getOperation() {
            return "plan BarrierPlan";
        }
    }

    /**
     * BarrierTask is used to test parallel task execution. It waits until N
     * tasks have started and reached a given point, before returning, thereby
     * checking that the tasks have actually executed.
     */
    private static class BarrierTask extends AbstractTask {

        private static final long serialVersionUID = 1L;
        private final transient CyclicBarrier barrier;
        private final transient AtomicInteger startCounter;
        private final transient AtomicInteger finishCounter;
        private final transient Logger logger;
        private transient volatile int taskTag;

        BarrierTask(CyclicBarrier barrier,
                    AtomicInteger startCounter,
                    AtomicInteger finishCounter) {
            this.barrier = barrier;
            this.startCounter = startCounter;
            this.finishCounter = finishCounter;
            logger = LoggerUtils.getLogger(this.getClass(), "BarrierTask");
        }

        @Override
        protected Plan getPlan() {
            return null;
        }

        @Override
        public Callable<Task.State> getFirstJob(int taskId, TaskRunner runner) {
            return new JobWrapper(taskId, runner, "enter barrier") {
                @Override
                    public NextJob doJob() throws Exception {
                    return startBarrier(taskId, runner);
                }
            };
        }

        public NextJob startBarrier(int taskId, TaskRunner runner)
            throws Exception {

            /* Just for easier debugging */
            taskTag = taskId;

            logger.log(Level.INFO,"Task {0} first phase, started",
                       new Object[] {taskId});
            /*
             * As a sanity check, make sure the startCounter shows that all
             * threads have started.
             */
            startCounter.incrementAndGet();

            /*
             * Spawn a thread to do the barrier waiting, so this task
             * itself is asynchronous and returns quickly, which mimics
             * the way partition migration works in the RNs. The barrier is
             * used to ensure that these tasks actually execute in
             * parallel.
             */
            new Thread() {
                @Override
                    public void run() {
                    try {
                        barrier.await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    finishCounter.incrementAndGet();
                }}.start();

            /*
             * The next phase of the task will require polling for the
             * finish of the barrier.
             */
            return makePollForFinishJob(taskId, runner);
        }

        /**
         * See if all threads passed through the barrier. If they did,
         * this task is finished. If not, return another job that will be
         * scheduled, to poll again.
         */
        private NextJob checkBarrierFinish(int taskId, TaskRunner runner) {

            logger.log(Level.INFO,
                       "Task {0}, checkBarrier: awaiting={1}",
                       new Object[] {taskId, barrier.getNumberWaiting()});

            if (finishCounter.get() == barrier.getParties()) {
                /* Success! This task is done */
                assertEquals(finishCounter.get(), startCounter.get());
                logger.log(Level.INFO, "Task {0} barrier done",
                           new Object[] {taskId});
                return NextJob.END_WITH_SUCCESS;
            }

            return makePollForFinishJob(taskId, runner);
        }

        /**
         * Create a polling job.
         */
        private NextJob makePollForFinishJob(final int taskId,
                                             final TaskRunner runner) {
            JobWrapper pollForFinish =
                new JobWrapper(taskId, runner, "Check Barrier Finish") {
                    @Override
                    public NextJob doJob() throws Exception {
                        return checkBarrierFinish(taskId, runner);
                    }
                };
            return new NextJob(Task.State.RUNNING, pollForFinish, POLL_PERIOD);
        }

        @Override
        public boolean continuePastError() {
            return true;
        }

        @Override
        public StringBuilder getName(StringBuilder sb) {
            return super.getName(sb).append(taskTag);
        }
    }

    /*
     * A simulated topology plan that will hold the elasticity lock.
     */
    private static class TopoPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;

        TopoPlan(String name, Planner planner) {
            super(name, planner);
        }

        @Override
        public String getDefaultName() {
            return "topo";
        }

        @Override
        protected void acquireLocks() throws PlanLocksHeldException {
            planner.lockElasticity(getId(), getName());
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return null;
        }
    }

    /**
     * A Plan which increments N counters, in serial and in parallel.
     */
    private static class CountingPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;
        private final transient List<AtomicInteger> counters;
        private volatile transient CountDownLatch pauseLatch;
        private volatile transient CountDownLatch arrivalLatch;
        private volatile int pauseVal;
        private volatile int errorVal;

        /**
         * If numSerialTasks = 3 and numParallelCounters = 4, the task list
         * looks like:
         *
         *       SingleJobCountingTask -> serial tasks do a single incremenent
         *       SingleJobCountingTask -> all of the counter
         *       SingleJobCountingTask
         *                 |
         *  +--------------+-----------+------------+
         *  |              |           |            |
         * MultiJob     MultiJob     MultiJob     MultiJob
         * CountingTask CountingTask CountingTask CountingTask
         *
         * (multi job counting tasks continously increment a particular
         * counter, and will never end unless interrupted)
         */
        CountingPlan(String name, Planner planner,
                     int numParallelCounters, int numSerialTasks) {
            super(name, planner);

            /* Setup counters */
            counters = new ArrayList<AtomicInteger>();
            for (int i = 0; i < numParallelCounters; i++) {
                counters.add(new AtomicInteger(0));
            }

            /* Make a bunch of serial tasks. */
            for (int i=0; i < numSerialTasks; i++) {
                addTask(new SingleJobCountingTask(this));
            }

            /* Follow by a set of parallel tasks */
            ParallelBundle bundle = new ParallelBundle();
            for (int i = 0; i < numParallelCounters; i++) {
                bundle.addTask(new MultiJobCountingTask(this, i));
            }
            addTask(bundle);

            clearErrorVal();
        }

        @Override
        public String getDefaultName() {
            return "CountingPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }

        /**
         * A way to measure if the tasks have started.
         * @return true if all the counters are >= value.
         */
        boolean equalOrMore(int value) {
            for (AtomicInteger i : counters) {
                if (i.get() < value) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Return the minimum value of any of the counters in the plan.
         */
        int minCount() {
            int min = Integer.MAX_VALUE;
            for (AtomicInteger i : counters) {
                int c = i.get();
                if (c < min) {
                    min = c;
                }
            }
            return min;
        }

        int incrementAndGet(int whichCounter) {
            int val = counters.get(whichCounter).incrementAndGet();
            if (val == errorVal) {
                logger.fine("Induce error at " + val);
                throw new RuntimeException("Induce error at " + val);
            }
            return val;
        }

        int incrementAndGet() {

            for (AtomicInteger c: counters) {
                c.incrementAndGet();
            }

            int val = counters.get(0).get();
            if (val == errorVal) {
                throw new RuntimeException("Induce error at " + val);
            }
            return val;
        }

        void resetCounter(int whichCounter) {
            counters.get(whichCounter).set(0);
        }

        void resetCounters() {
            for (AtomicInteger c: counters) {
                c.set(0);
            }
        }

        /**
         * Make the plan incur an error when the counters count up to
         * this value.
         */
        void makeErrorAtCount(int val) {
            errorVal = val;
        }

        void clearErrorVal() {
            errorVal = -1;
        }

        void makePauseInSerialTasks(int pVal, CountDownLatch pLatch,
                                    CountDownLatch aLatch) {
            pauseVal = pVal;
            pauseLatch = pLatch;
            arrivalLatch = aLatch;
        }

        void clearPauseInSerialTasks() {
            pauseVal = 0;
            pauseLatch = null;
            arrivalLatch = null;
        }

        int getPauseVal() {
            return pauseVal;
        }

        CountDownLatch getPauseLatch() {
            return pauseLatch;
        }

        CountDownLatch getArrivalLatch() {
           return arrivalLatch;
        }
    }

    private static class SingleJobCountingTask extends SingleJobTask {

        private static final long serialVersionUID = 1L;
        private final CountingPlan plan;
        private final transient Logger logger;

        SingleJobCountingTask(CountingPlan plan) {
            this.plan = plan;
            logger = LoggerUtils.getLogger(this.getClass(),
                                           "SingleJobCountingTask");
        }

        @Override
        protected Plan getPlan() {
            return plan;
        }

        @Override
        public Task.State doWork()
            throws InterruptedException {

            int current = plan.incrementAndGet();
            logger.info("incremented to " + current);

            /*
             * The test case may want to pause this task, so it can call
             * planner.interruptPlan();
             */
            if (plan.getPauseVal() == current) {
                plan.getArrivalLatch().countDown();
                plan.getPauseLatch().await();
            }
            return Task.State.SUCCEEDED;
        }

        @Override
        public Runnable getCleanupJob() {
            return new Runnable() {
                @Override
                public void run(){
                    plan.resetCounters();
                }
            };
        }

        @Override
        public boolean continuePastError() {
            return false;
        }
    }

    /**
     * A simple mult-job task.
     */
    private static class MultiJobTask extends AbstractTask {

        private static final long serialVersionUID = 1L;
        private final Plan plan;
        private volatile int nJobs;
        private volatile transient int taskTag;
        private final transient AtomicInteger goodTaskCounter;

        MultiJobTask(Plan plan, int nJobs, AtomicInteger goodTaskCounter) {
            this.plan = plan;
            this.nJobs = nJobs;
            this.goodTaskCounter = goodTaskCounter;
         }

        @Override
        protected Plan getPlan() {
            return plan;
        }

        @Override
         public Callable<Task.State> getFirstJob(int taskId,
                                                 TaskRunner runner) {
            taskTag = taskId;
            return new JobWrapper(taskId, runner, "start counting") {
                @Override
                public NextJob doJob() throws Exception {
                    return next(taskId, runner);
                }
            };
        }

        public NextJob next(int taskId, TaskRunner runner)
            throws Exception {

            if (nJobs <= 0) {
                goodTaskCounter.incrementAndGet();
                return NextJob.END_WITH_SUCCESS;
            }
            nJobs--;


            JobWrapper next =
                new JobWrapper(taskId, runner, "Next_" + nJobs) {
                    @Override
                    public NextJob doJob() throws Exception {
                        return next(taskId, runner);
                    }
                };
            return new NextJob(Task.State.RUNNING, next, null);
        }

        @Override
        public boolean continuePastError() {
            return false;
        }

        @Override
        public StringBuilder getName(StringBuilder sb) {
            return super.getName(sb).append(taskTag);
        }
    }

    /**
     * MultiJobCountingTask is a multi-job, parallel task which increments an
     * AtomicCounter . In between each increment, it returns to the plan
     * executor, which waits for awhile before scheduling the next job of this
     * task. It will count indefinitely until the plan is interrupted and
     * cancelled.
     */
    private static class MultiJobCountingTask extends AbstractTask {

        private static final long serialVersionUID = 1L;
        private final CountingPlan plan;
        private final int counterIndex;
        private volatile transient int taskTag;
        private final transient Logger logger;

        MultiJobCountingTask(CountingPlan plan, int counterIndex) {
            this.counterIndex = counterIndex;
            this.plan = plan;
            logger = LoggerUtils.getLogger(this.getClass(),
                                           "MultiJobCountingTask");
         }

        @Override
        protected Plan getPlan() {
            return plan;
        }

        @Override
         public Callable<Task.State> getFirstJob(int taskId,
                                                 TaskRunner runner) {
            taskTag = taskId;
            return new JobWrapper(taskId, runner, "start counting") {
                @Override
                public NextJob doJob() throws Exception {
                    return count(taskId, runner);
                }
            };
        }

        public NextJob count(int taskId, TaskRunner runner)
            throws Exception {
            int val = plan.incrementAndGet(counterIndex);
            logger.info("incremented counter to " + val);

            JobWrapper nextCount =
                new JobWrapper(taskId, runner, "Count_" + val) {
                    @Override
                    public NextJob doJob() throws Exception {
                        return count(taskId, runner);
                    }
                };
            return new NextJob(Task.State.RUNNING, nextCount, POLL_PERIOD);
        }

        @Override
        public boolean continuePastError() {
            return true;
        }

        @Override
        public StringBuilder getName(StringBuilder sb) {
            return super.getName(sb).append(taskTag);
        }

        @Override
        public Runnable getCleanupJob() {
            return new Runnable() {
                @Override
                public void run(){
                    plan.resetCounter(counterIndex);
                }
            };
        }
    }

    private static class BadTaskException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        BadTaskException(String msg) {
            super(msg);
        }
    }

    /** Waits for a given task to start. */
    private static class TaskStartWaiter implements ExecutionListener {

        private final CountDownLatch taskStarted = new CountDownLatch(1);
        private final CountDownLatch planEnded =  new CountDownLatch(1);

        @Override
        public void taskStart(Plan plan,
                              Task task,
                              int taskNum,
                              int totalTasks) {
            taskStarted.countDown();
        }

        public void await() throws InterruptedException {
            taskStarted.await();
        }

        @Override
            public void planStart(Plan plan) {
        }

        @Override
            public void planEnd(Plan plan) {
            planEnded.countDown();
        }

        @Override
            public void taskEnd(Plan plan,
                                Task task,
                                TaskRun taskRun,
                                int taskNum,
                                int totalTasks) {
        }

        void waitForPlanEnd()
            throws InterruptedException {
            planEnded.await();
        }
    }

    /** An access checker which builds a user subject with sysadmin role. */
    private static class PlanExeChecker extends AccessCheckerImpl {
        public PlanExeChecker(RoleResolver resolver, Logger logger) {
            super(null, resolver, null, logger);
        }

        private volatile String userName = TEST_USER;
        private volatile String userId = "0001";

        @Override
        public Subject identifyRequestor(AuthContext context) {
            if (context == null || context.getLoginToken() == null) {
                throw new AuthenticationRequiredException(
                    "Not authenticated", false /* isReturnSignal */);
            }

            return makeAuthenticatedSubject();
        }

        private Subject makeAuthenticatedSubject() {
            final Set<Principal> subjPrincs = new HashSet<Principal>();
            subjPrincs.add(new KVStoreUserPrincipal(userName, userId));
            subjPrincs.add(KVStoreRolePrincipal.SYSADMIN);
            final Set<Object> publicCreds = new HashSet<Object>();
            final Set<Object> privateCreds = new HashSet<Object>();
            return new Subject(true, subjPrincs, publicCreds, privateCreds);
        }
    }

    class PlanExecutionContext implements OperationContext {
        private final String user;

        private PlanExecutionContext(final String user) {
            this.user = user;
        }

        @Override
        public String describe() {
            return "Plan Execution Context test User " + user;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return new ArrayList<KVStorePrivilege>(
                RoleInstance.SYSADMIN.getPrivileges());
        }
    }

    private static class ContextChecker implements ExecutionListener {
        @Override
        public void taskStart(Plan plan,
                              Task task,
                              int taskNum,
                              int totalTasks) {}


        @Override
        public void planStart(Plan plan) {
            checkContext();
        }

        @Override
        public void planEnd(Plan plan) {
            checkContext();
        }

        private void checkContext() {
            final ExecutionContext exeCtx = ExecutionContext.getCurrent();
            assertNotNull(exeCtx);
            final KVStoreUserPrincipal user =
                ExecutionContext.getSubjectUserPrincipal(
                    exeCtx.requestorSubject());
            assertEquals(user.getName(), TEST_USER);
        }

        @Override
        public void taskEnd(Plan plan,
                            Task task,
                            TaskRun taskRun,
                            int taskNum,
                            int totalTasks) {}
    }
}
