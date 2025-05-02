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
import java.util.concurrent.RejectedExecutionException;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.DatabaseException;

/**
 * A task that is meant to be executed in a single phase of work.
 */
public abstract class SingleJobTask extends AbstractTask {

    private static final long serialVersionUID = 1L;

    /**
     * Contains the all the logic needed to execute the task, to be done as
     * a single job, or phase.
     */
    public abstract Task.State doWork() throws Exception;

    /**
     * Task execution is started off with a Callable that encompasses
     * all the work of the task.
     */
    @Override
    public Callable<Task.State> getFirstJob(int taskNum, TaskRunner runner) {

        return new Callable<Task.State>() {
            @Override
            public State call() throws Exception {
                try {
                    return runner.dispatchNextJob(taskNum,
                                                  new NextJob(doWork(), null));
                } catch (AdminFaultException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e,
                        ErrorMessage.getEnum(
                            e.getCommandResult().getErrorCode()),
                        e.getCommandResult().getCleanupJobs());
                } catch (SNAFaultException | RepNodeAdminFaultException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.PLAN_CANCEL);
                } catch (NotBoundException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5200,
                        CommandResult.NO_CLEANUP_JOBS);
                } catch (RemoteException | SessionAccessException |
                         RejectedExecutionException | DatabaseException |
                         Admin.DBOperationFailedException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.PLAN_CANCEL);
                } catch (NonfatalAssertionException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5500,
                        CommandResult.NO_CLEANUP_JOBS);
                } catch (KVSecurityException | IllegalStateException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5100,
                        CommandResult.NO_CLEANUP_JOBS);
                }
            }
        };
    }
}
