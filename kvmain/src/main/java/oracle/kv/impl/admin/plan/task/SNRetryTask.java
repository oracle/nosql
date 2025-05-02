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

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.AbstractPlanWithTopology;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Perform a task that makes a remote call to an SN, retrying the task if it
 * encounters a network failure.
 */
abstract class SNRetryTask<P extends AbstractPlanWithTopology>
        extends AbstractTask {

    private static final long REMOTE_EXCEPTION_MAX_RETRIES = 10;
    private static final long serialVersionUID = 1L;

    protected final P plan;
    protected final StorageNodeId snId;
    private int retries;

    protected SNRetryTask(P plan, StorageNodeId snId) {
        this.plan = plan;
        this.snId = snId;
    }

    /**
     * This task catches and retries remote exceptions automatically, but
     * should give up if the task throws an exception.
     */
    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    protected P getPlan() {
        return plan;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" on SN ").append(snId);
    }

    @Override
    public Callable<State> getFirstJob(int taskId, TaskRunner runner) {
        retries = 0;
        return makeJob(taskId, runner);
    }

    private JobWrapper makeJob(int taskId, TaskRunner runner) {
        return new JobWrapper(taskId, runner, getName()) {
            @Override
            public NextJob doJob() {
                return runJob(taskId, runner);
            }
        };
    }

    private NextJob runJob(int taskId, TaskRunner runner) {
        final Admin admin = plan.getAdmin();
        final RegistryUtils registryUtils =
            new RegistryUtils(plan.getTopology(), admin.getLoginManager(),
                              plan.getLogger());
        try {
            final StorageNodeAgentAPI snApi =
                registryUtils.getStorageNodeAgent(snId);
            final String result = doSNTask(snApi);
            if (result != null) {
                plan.getLogger().info(this + " result: " + result);
            }
            return NextJob.END_WITH_SUCCESS;
        } catch (RemoteException | NotBoundException e) {
            /*
             * Network issue or SN not available, try again later unless too
             * many retries
             */
            if (++retries >= REMOTE_EXCEPTION_MAX_RETRIES) {
                return new NextJob(Task.State.ERROR,
                                   "Failure contacting storage node " + snId +
                                   ": " + e);
            }
            final AdminParams adminParams = admin.getParams().getAdminParams();
            return new NextJob(Task.State.RUNNING,
                               makeJob(taskId, runner),
                               adminParams.getServiceUnreachablePeriod());
        }
    }

    /**
     * Perform the task on the SNA.
     *
     * @param snApi the API for the SNA
     * @return a description of the operation result or null
     * @throws RemoteException if a network failure occurs contacting the SNA
     */
    protected abstract String doSNTask(@NonNull StorageNodeAgentAPI snApi)
        throws RemoteException;
}
