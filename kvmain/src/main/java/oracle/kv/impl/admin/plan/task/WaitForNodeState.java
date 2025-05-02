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

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.util.ErrorMessage;

/**
 * Monitors the state of a RepNode or ArbNode, blocking until a certain state
 * has been reached.
 */
public class WaitForNodeState extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /**
     * The node that is to be monitored
     */
    private final ResourceId targetNodeId;

    /**
     * The state the node must be in before finishing this task
     */
    private final ServiceStatus targetState;
    private final AbstractPlan plan;

    /**
     * Creates a task that will block until a given Node has reached
     * a given state.
     *
     * @param desiredState the state to wait for
     */
    public WaitForNodeState(AbstractPlan plan,
                            ResourceId targetNodeId,
                            ServiceStatus desiredState) {
        this.plan = plan;
        this.targetNodeId = targetNodeId;
        this.targetState = desiredState;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {
        final State state =
            Utils.waitForNodeState(plan, targetNodeId, targetState);
            if (state == State.ERROR) {
                final AdminParams ap =
                    plan.getAdmin().getParams().getAdminParams();
                throw new CommandFaultException(
                              String.format("Timed out after %d %s",
                                            ap.getWaitTimeout(),
                                            ap.getWaitTimeoutUnit()),
                              ErrorMessage.NOSQL_5400,
                              CommandResult.PLAN_CANCEL);
            }
            return state;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(targetNodeId)
                               .append(" to reach ").append(targetState);
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
