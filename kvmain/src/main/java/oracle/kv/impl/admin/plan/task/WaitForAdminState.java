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

import java.util.logging.Level;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.util.ErrorMessage;

/**
 * Monitors the state of an Admin, blocking until a certain state has been
 * reached.
 */
public class WaitForAdminState extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /**
     * The node that is to be monitored
     */
    private final AdminId targetAdminId;

    /**
     * The state the node must be in before finishing this task
     */
    private final ServiceStatus targetState;
    private final AbstractPlan plan;
    private final StorageNodeId snId;

    /**
     * Creates a task that will block until a given Admin has reached
     * a given state.
     *
     * @param desiredState the state to wait for
     */
    public WaitForAdminState(AbstractPlan plan,
                             StorageNodeId snId,
                             AdminId targetAdminId,
                             ServiceStatus desiredState) {
        this.plan = plan;
        this.targetAdminId = targetAdminId;
        this.snId = snId;
        this.targetState = desiredState;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Parameters parameters = plan.getAdmin().getCurrentParameters();
        final StorageNodeParams snp = parameters.get(snId);

        /* Get the timeout from the currently running Admin's myParams. */
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        final long waitSeconds =
            ap.getWaitTimeoutUnit().toSeconds(ap.getWaitTimeout());

        final String msg =
            "waiting " + waitSeconds + " seconds for Admin" + targetAdminId +
            " to reach " + targetState;

        plan.getLogger().log(Level.FINE, "{0} {1}", new Object[]{this, msg});

        try {
            ServiceUtils.waitForAdmin(snp.getHostname(), snp.getRegistryPort(),
                                      plan.getLoginManager(),
                                      waitSeconds, targetState,
                                      plan.getLogger());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.PLAN_CANCEL);
            }

            plan.getLogger().log(Level.INFO, "{0} timed out while {1}",
                                 new Object[]{this, msg});
            throw new CommandFaultException(e.getMessage(),
                                            ErrorMessage.NOSQL_5400,
                                            CommandResult.PLAN_CANCEL);
        }

        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(targetAdminId)
                               .append(" to reach ").append(targetState);
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
