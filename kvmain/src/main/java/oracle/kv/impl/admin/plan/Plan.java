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

import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.PlanWaiter;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.TaskList;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.Ownable;
import oracle.kv.util.ErrorMessage;

import oracle.nosql.common.json.ObjectNode;

/**
 * Encapsulates a definition and mechanism for making a change to the KV Store.
 * Implementations of Plan will define the different types of plans that can be
 * carried out.
 */
public interface Plan extends Ownable {

    /** The default name for a plan. */
    final String NO_NAME = "none";

    /**
     * The available states of a Plan. Plan state is persistent.
     *
     * {@literal
     *
     *             Pending ----+
     *               |         +--(cancel)---> Canceled (terminal state)
     *            Approved ----+
     *               |
     *               |
     *            Running       <-------------+
     *            /  |  \                     |
     *           /   |  (interrupt requested) |
     *          /    |  /  \                  |
     *         /     | /    \                 |
     *    Success  Error Interrupted      (retry)
     *  (terminal    |     |                  |
     *    state)     +-----+------------------+
     *                  |
     *                  |
     *              Canceled (terminal)
     * }
     *
     * All plans that have been created are retained in a history.
     *
     * Once a plan is running, there are only two terminal states: Success and
     * Canceled.
     *
     * When a plan hits an error or has been interrupted by the user, the user
     * must decide whether to retry the plan, or to give up and cancel the
     * plan. If a plan is retried, it has a fresh start, and is treated
     * identically to any other running plan, although we will keep track of
     * how many attempts there have been to run this plan.

     *
     * Note that Plan and {@link Task} state must be reinitialized when an
     * Admin comes up. For example, any RUNNING plans or tasks have presumably
     * been interrupted, and should be reset to INTERRUPTED state at plan
     * recovery.
     */
    public static enum State {
        PENDING(false),   // The plan has been created and is awaiting approval
        APPROVED(false),         // The plan may be run
        CANCELED(true) ,         // The plan did not run to completion, and is
                                 // now invalidated
        RUNNING(false),          // The plan is running
        INTERRUPTED(false),      // The user stopped the plan
        SUCCEEDED(true),         // The plan ran to completion successfully
        ERROR(false),            // The plan failed at any point

        /* Added in R2 */
        INTERRUPT_REQUESTED(false); // The plan is attempting to stop and
                                    // cleanup in response to an interrupt

        private final boolean isTerminal;

        State(boolean isTerminal) {
            this.isTerminal = isTerminal;
        }

        /**
         * Check a transition, and throw if it's not valid.
         */
        public static void validateTransition(State oldState,
                                              State nextState) {
            final String errMsg = oldState.checkTransition(nextState);
            if (errMsg != null) {
                throw new IllegalCommandException
                    ("Plan cannot transition from " + oldState +
                     " to " + nextState +
                     (!errMsg.isEmpty() ? ": " + errMsg : ""),
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
        }

        /**
         * Checks if transition to specified next state is permitted. Returns
         * null for a permitted transition, and a non-null error message string
         * if not permitted. If the error message string is not empty, it
         * should be included in whatever error message is displayed to the
         * user.
         *
         * @return null or an error message
         */
        public String checkTransition(State nextState) {

            /*
             * Users can request three changes to a plan:
             *
             * - Execute, moving from PENDING to APPROVED (which happens
                 implicitly) and from there to RUNNING
             * - Cancel, moving from PENDING, APPROVED, or INTERRUPTED to
             *   CANCELED
             * - Interrupt, moving from RUNNING to INTERRUPT_REQUESTED,
             *   although higher layers also allow interrupting PENDING, which
             *   then moves to CANCELED
             *
             * If one of those three changes is requested when in a
             * user-visible state that does not permit it, this method will
             * return an error message to help give the user guidance. Other
             * not permitted transitions shouldn't be possible, so an empty
             * message is returned for them.
             */
            switch (this) {
            case PENDING:
                switch (nextState) {
                case APPROVED:
                case CANCELED:
                    return null;
                default:
                    break;
                }
                break;
            case APPROVED:
                switch (nextState) {
                case CANCELED:
                case RUNNING:
                case APPROVED:
                    return null;
                default:
                    break;
                }
                break;
            case CANCELED:
                /* Terminal state, no transitions permitted */
                break;
            case RUNNING:
                switch (nextState) {
                case SUCCEEDED:
                case ERROR:
                case INTERRUPT_REQUESTED:
                    return null;
                case CANCELED:
                    return "A running plan must be interrupted before" +
                        " it can be canceled.";
                default:
                    break;
                }
                break;
            case SUCCEEDED:
                /* Terminal state, no transitions permitted */
                break;
            case ERROR:
            case INTERRUPTED:
                switch (nextState) {
                case RUNNING:
                case CANCELED:
                    return null;
                default:
                    break;
                }
                break;
            case INTERRUPT_REQUESTED:
                switch (nextState) {
                case ERROR:
                case INTERRUPTED:
                    return null;
                default:
                    break;
                }
                break;

            default:
                throw new OperationFaultException
                    ("Plan state transition for " + this +
                     " not defined. Attempting to transition to " +
                     nextState);
            }

            /* Failed, but no extra info */
            return "";
        }

        public boolean isTerminal() {
            return isTerminal;
        }

        /**
         * Provide a useful response to the CLI plan wait command.
         */
        public String getWaitMessage(int planId) {

            switch(this) {
            case PENDING:
                return "Plan " + planId + " is pending.";
            case APPROVED:
                return "Plan " + planId + " is approved, please execute.";
            case CANCELED:
                return "Plan " + planId + " has been canceled";
            case RUNNING:
                return "Plan " + planId + " is running";
            case SUCCEEDED:
                return "Plan " + planId + " ended successfully";
            case ERROR:
                return "Plan " + planId + " ended with errors." +
                    " Use \"show plan -id " + planId +
                    "\" for more information";
            case INTERRUPTED:
                return "Plan " + planId + " has been interrupted. Please " +
                    "cancel or retry the plan.";
            case INTERRUPT_REQUESTED:
                return "Plan " + planId + " is processing a request " +
                    "to interrupt the plan. Use \"show plan -id " + planId +
                    "\" for more information";
            default:
                throw new OperationFaultException
                    ("Message for " + this + " not defined.");
            }
        }

        /**
         * @return true if this run of this plan is finished.
         */
        boolean planExecutionFinished() {
            switch(this) {
            case PENDING:
            case APPROVED:
            case INTERRUPT_REQUESTED:
            case RUNNING:
                return false;
            case CANCELED:
            case SUCCEEDED:
            case ERROR:
            case INTERRUPTED:
                return true;
            }
            return false;
        }

        /**
         * @return true if this plan has been approved and can be executed
         * for the first or subsequent time.
         */
        boolean approvedAndCanExecute() {
            switch(this) {
            case APPROVED:
            case ERROR:
            case INTERRUPTED:
            case INTERRUPT_REQUESTED:
            case RUNNING:
                return true;
            default:
                return false;
            }
        }
    }

    /**
     * Return the unique identifier for this plan.
     */
    int getId();

    /**
     * Gets the current state of this plan, as far as it is known by the
     * system.  In the event that an Admin has failed, a Plan may temporarily
     * be in a RUNNING state before moving to INTERRUPTED.
     *
     * @return the most recently computed state
     */
    State getState();

    /**
     * Add a new task to the plan. Should only be used while the plan is being
     * generated.
     */
    void addTask(Task t);

    /**
     * Gets a description of all the tasks that will be run, in order, to carry
     * out this plan.
     *
     * @return the a cloned list of tasks that will be run. The tasks
     * may be modified by the caller.
     */
    TaskList getTaskList();

    /**
     * Plans are named with useful-to-human labels.
     */
    String getName();

    /**
     * A default name provided by the implementing class.
     */
    String getDefaultName();

    /**
     * Initializes the plan object. Transient fields are set, as well
     * as listeners. This method must be called when ever a plan is read
     * from the store.
     */
    void initializePlan(Planner planner, AdminServiceParams aServiceParams);

    /**
     * Null out components of the plan that have a large memory footprint,
     * and that are not needed for display. Meant to reduce the cost of
     * display.
     */
    void stripForDisplay();

    /**
     * If this plan had a failure during execution, save a description of the
     * failure, complete with stack trace. This is persistent, for use when
     * viewing plan execution history.
     */
    String getLatestRunFailureDescription();

    /**
     * Add a listener to receive notification when the plan finishes. Also
     * return the PlanRun that belongs to the current execution.
     */
    PlanRun addWaiter(PlanWaiter waiter);

    /**
     * Remove a plan waiter.
     */
    void removeWaiter(PlanWaiter waiter);

    Date getCreateTime();

    Date getStartTime();

    Date getEndTime();

    /**
     * Set this plan's state to INTERRUPTED. Used when the Admin
     * recovers and finds plans that were in progress before its shutdown.
     */
    void markAsInterrupted();

    /**
     * Return a formatted string representing the history of execution attempts
     * for this plan.
     */
    String showRuns();

    /**
     * Expose the execution state object for display in the GUI.
     */
    ExecutionState getExecutionState();

    /**
     * Return the number of tasks in the plan.
     */
    int getTotalTaskCount();

    /**
     * Support the plan status command by returning a description of finished
     * tasks. Plans may choose to summarize or couch the information in a more
     * user friendly way, rather than necessarily returning a list of all
     * finished tasks.
     * @param fm information should be sent to this formatter.
     */
    void describeFinished(Formatter fm,
                                 List<TaskRun> finished,
                                 int errorCount,
                                 boolean verbose);

    /**
     * Support the plan status command by returning a description of running
     * tasks. Plans may choose to summarize or couch the information in a more
     * user friendly way, rather than necessarily returning a list of all
     * finished tasks.
     * @param fm information should be sent to this formatter.
     */
    void describeRunning(Formatter fm,
                                List<TaskRun> running,
                                boolean verbose);

    /**
     * Support the plan status command by returning a description of tasks that
     * have not started. Plans may choose to summarize or couch the information
     * in a more user friendly way, rather than necessarily returning a list of
     * all finished tasks.
     * @param fm information should be sent to this formatter.
     */
    void describeNotStarted(Formatter fm,
                                   List<Task> notStarted,
                                   boolean verbose);

    /**
     * Take any PlannerImpl locks needed. Used to coordinate plan execution.
     * @throws PlanLocksHeldException if the locks are unavailable
     */
    void getCatalogLocks() throws PlanLocksHeldException;

    /**
     * Do any validation needed before starting the plan. A logger is supplied
     * because the plan's own logger is not set at this point.
     * @param force if true, the -force flag was specified to the CLI. Each
     * plan may have a different interpretation of that force flag. For
     * topology plans, force affects whether the plan will run in the face of
     * topology violations.
     * @throws IllegalCommandException if there is a problem with running the
     * plan.
     */
    void preExecuteCheck(boolean force, Logger executeLogger);

    void saveFailure(PlanRun planRun,
                     Throwable t,
                     String problem,
                     ErrorMessage errorMsg,
                     String[] cleanupJobs,
                     Logger logger);

    ExceptionTransfer getLatestRunExceptionTransfer();

    /**
     * Called in the course of plan execution when metadata is
     * updated and persisted. If true is returned the plan will also be
     * persisted in the same transaction as the metadata. Plans should use this
     * method if they maintain persistent state based on metadata and need to
     * track changes.
     *
     * @param metadata the metadata being updated
     * @return true if the plan should be persisted
     */
    boolean updatingMetadata(Metadata<?> metadata);

    /**
     * Returns a list of privileges required to operate this plan, like execute,
     * cancel, interrupt and so forth.
     */
    List<? extends KVStorePrivilege> getRequiredPrivileges();

    /**
     * Return true if this plan and currentPlan are logically equivalent and
     * do the same actions
     */
    boolean logicalCompare(Plan currentPlan);

    /**
     * Returns the planner handling this plan. Returns null if a planner has
     * not yet been set.
     *
     * @return the planner handling this plan or null
     */
    Planner getPlanner();

    /**
     * Return this plan JSON value as command result "return_value" field.
     */
    ObjectNode getPlanJson();

    /**
     * Return this plan operation as command result "operation" field.
     */
    String getOperation();

    /**
     * Return this plan JSON command result.
     */
    CommandResult getCommandResult();

    /**
     * Returns true if the this plan is a plan generated by the Admin.
     *
     * @return true if the specified plan is a plan generated by the Admin
     */
    boolean isSystemPlan();
}
