/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminTestBase;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.SingleJobTask;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test plan retries.
 */
public class RetryTest extends AdminTestBase {

    private Planner planner;
    private Admin admin;

    public RetryTest() {
    }

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        if (admin != null) {
            admin.shutdown(true, "test teardown");
        }
        /* Shutdown the bootstrap handler. */
        LoggerUtils.closeAllHandlers();
        super.tearDown();
    }

    /**
     * Ensure that a plan that fails can be re-executed.
     */
    @Test
    public void testPlanRetry()
        throws Exception {

        setupPlanner();
        FailureDirective directive = new FailureDirective(4);
        AbstractPlan retryPlan = new RetryPlan("RetryPlan", planner, directive);
        /*
         * Registering the plan is usually done within the planner impl, but
         * for artificially created unit test plans, is done outside.
         */
        admin.getPlanner().register(retryPlan);
        admin.savePlan(retryPlan, "unit test");

        /* Approve the plan. */
        admin.approvePlan(retryPlan.getId());

        /* Every other task fails. Attempt 1 */
        directive.reset(new boolean[]{true, false, true, false});
        runPlan(retryPlan, directive, Plan.State.ERROR);
        directive.verify();

        /* Different tasks fail. Attempt 2 */
        directive.reset(new boolean[]{false, true, false, true});
        runPlan(retryPlan, directive, Plan.State.ERROR);
        directive.verify();

        /* Nothing fails. Attempt 3 */
        directive.reset(new boolean[]{false, false, false, false});
        runPlan(retryPlan, directive, Plan.State.SUCCEEDED);
        directive.verify();

        /* Attempt 4 -- plan should not be able to be run anymore. */
        try {
            admin.executePlan(retryPlan.getId(), false);
            admin.awaitPlan(retryPlan.getId(), 0, null);
            admin.assertSuccess(retryPlan.getId());
            fail("Shouldn't succeed");
        } catch (IllegalCommandException e) {
            e.getMessage().contains("can't be run, last state was SUCCESS");
        }
    }

    private void setupPlanner() {
        admin = new Admin(atc.getParams());
        planner = admin.getPlanner();
    }

    /**
     * Execute the specified plan. Return any exception that occurs.
     */
    private void runPlan(AbstractPlan plan,
                         FailureDirective directive,
                         Plan.State expectedState) {

        int numExpectedFailures = 0;
        for (Boolean b : directive.failOrNot) {
            if (b) {
                numExpectedFailures++;
            }
        }

        /* Execute it and wait for it to finish. */
        try {
            admin.executePlan(plan.getId(), false);
            admin.awaitPlan(plan.getId(), 0, null);
            admin.assertSuccess(plan.getId());

            /*
             * The plan should only succeed if there were no expected failures
             */
            assertEquals(0, numExpectedFailures);
            assertEquals(plan.getLatestRunFailureDescription(),
                         Plan.State.SUCCEEDED,
                         plan.getState());
            /* If it failed, the executePlan should throw. */
            String failure = plan.getLatestRunFailureDescription();
            if ((failure != null) && failure.length() > 1) {
                fail("Did not expect failure " + failure);
            }
        } catch (OperationFaultException wrapper) {
            assertTrue(numExpectedFailures > 0);

            /*
             * The plan should store the Throwable and descriptions of
             * the underlying problem.
             */
            Throwable problem = plan.getExceptionTransfer().getFailure();
            assertEquals(BadTaskException.class, problem.getClass());

            String completeDesc = plan.getLatestRunFailureDescription();
            String shortDesc = plan.getExceptionTransfer().getDescription();

            assertTrue(completeDesc,
                       completeDesc.contains(directive.getFirstErrorMsg()));
            assertTrue(shortDesc,
                       shortDesc.contains(directive.getFirstErrorMsg()));

            /*
             * The wrapping OperationFaultException should contain information
             * about the underlying problem.
             */
            assertEquals(BadTaskException.class, wrapper.getCause().getClass());
            assertTrue(wrapper.getMessage().contains
                       (directive.getFirstErrorMsg()));
        } catch (Exception e) {
            fail("We don't expect any other kind of exception, but saw " + e);
        } finally {
            assertEquals(expectedState, plan.getState());
        }
    }

    /**
     * A Plan that contains tasks that will fail and succeed.
     */
    private static class RetryPlan extends AbstractPlan {

        private static final long serialVersionUID = 1L;

        RetryPlan(String name, Planner planner,
                  FailureDirective shouldFail) {
            super(name, planner);
            for (int i = 0; i < shouldFail.numTasks; i++) {
                addTask(new FailOrNotTask(i, shouldFail));
            }
        }

        @Override
        public String getDefaultName() {
            return "RetryPlan";
        }

        @Override
        public List<KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }
    }

    private static final String BAD_TASK_MSG = "This is an PROBLEM!";

    private static class FailOrNotTask extends SingleJobTask {

        private static final long serialVersionUID = 1L;
        private int id;
        private transient FailureDirective shouldFail;

        FailOrNotTask(int id, FailureDirective shouldFail) {
            this.id = id;
            this.shouldFail = shouldFail;
        }

        @Override
        protected Plan getPlan() {
            return null;
        }

        @Override
        public Task.State doWork()
            throws Exception {
            shouldFail.executed[id] = true;
            if (shouldFail.failOrNot[id]) {
                shouldFail.failed[id] = true;
                throw new BadTaskException(shouldFail.makeErrorMsg(id));
            }
            shouldFail.failed[id] = false;
            return Task.State.SUCCEEDED;
        }

        @Override
        public boolean continuePastError() {
            return true;
        }
    }

    private static class BadTaskException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        BadTaskException(String msg) {
           super(msg);
        }
    }

    private class FailureDirective {
        final int numTasks;
        boolean[] failOrNot;
        boolean[] executed;
        boolean[] failed;

        FailureDirective(int numTasks) {
            this.numTasks = numTasks;
        }

        void reset(boolean[] failureInstructions) {
            failOrNot = failureInstructions;
            executed = new boolean[numTasks];
            failed = new boolean[numTasks];
        }

        String makeErrorMsg(int id) {
            return "Task" + id + ":" + BAD_TASK_MSG;
        }

        /*
         * When a plan fails, the error message reported is for the first
         * failed task.
         */
        String getFirstErrorMsg() {
            for (int i = 0; i < failOrNot.length; i++) {
                if (failOrNot[i]) {
                    return makeErrorMsg(i);
                }
            }
            return null;
        }

        void verify() {
            for (int i=0; i < numTasks; i++) {
                assertTrue("task " + i + " executed?", executed[i]);
                assertEquals("task " + i, failOrNot[i], failed[i]);
            }
        }
    }
}
