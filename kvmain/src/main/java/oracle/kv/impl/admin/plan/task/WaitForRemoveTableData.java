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

import static oracle.kv.impl.admin.plan.task.Utils.getMaster;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.admin.plan.task.Utils.ShardNotFoundException;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;

/**
 * Wait for table data to be removed on a RepNode.
 */
public class WaitForRemoveTableData extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final RepGroupId groupId;
    private final String namespace;
    private final String tableName;

    /**
     *
     * @param plan
     * @param groupId ID of the current rep group of the partition
     * will stop.
     */
    public WaitForRemoveTableData(AbstractPlan plan,
                                  RepGroupId groupId,
                                  String namespace,
                                  String tableName) {
        this.plan = plan;
        this.groupId = groupId;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public Callable<State> getFirstJob(int taskId, TaskRunner runner) {
        return makeWaitForRemoveTableDataJob(taskId, runner);
    }

    /**
     * @return a wrapper that will invoke a add index job.
     */
    private JobWrapper makeWaitForRemoveTableDataJob(final int taskId,
                                              final TaskRunner runner) {

        return new JobWrapper(taskId, runner, "remove table data") {

            @Override
            public NextJob doJob() {
                return waitForRemoveTableData(taskId, runner);
            }
        };
    }

    /**
     * Query for remove table data to complete
     */
    private NextJob waitForRemoveTableData(int taskId,
                                           TaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        try {
            final RepNodeAdminAPI masterRN = getMaster(plan, groupId);
            if (masterRN == null) {
                /* No master available, try again later. */
                return new NextJob(Task.State.RUNNING,
                                   makeWaitForRemoveTableDataJob(taskId,
                                                                 runner),
                                   ap.getRNFailoverPeriod());
            }

            plan.getLogger().log(Level.FINE, "{0} query for done", this);

            return queryForDone(taskId, runner, ap);
        } catch (ShardNotFoundException snfe) {
            plan.getLogger().log(Level.INFO, "{0} {1}, reporting done",
                                 new Object[] {this, snfe});
            return NextJob.END_WITH_SUCCESS;
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeWaitForRemoveTableDataJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());
        }
    }

    private NextJob queryForDone(int taskId,
                                 TaskRunner runner,
                                 AdminParams ap) {

        try {
            final RepNodeAdminAPI masterRN = getMaster(plan, groupId);
            if (masterRN == null) {
                /* No master to talk to, repeat step2 later. */
                return new NextJob(Task.State.RUNNING,
                                   makeDoneQueryJob(taskId, runner, ap),
                                   ap.getRNFailoverPeriod());
            }

            final boolean done = masterRN.removeTableDataComplete(namespace,
                                                                  tableName);

            plan.getLogger().log(Level.INFO, "{0} done={1}",
                                 new Object[] {this, done});

            return done ? NextJob.END_WITH_SUCCESS :
                          new NextJob(Task.State.RUNNING,
                                      makeDoneQueryJob(taskId, runner, ap),
                                      getCheckIndexTime(ap));
        } catch (ShardNotFoundException snfe) {
            plan.getLogger().log(Level.INFO, "{0} {1}, reporting done",
                                 new Object[] {this, snfe});
            return NextJob.END_WITH_SUCCESS;
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try again later. */
            return new NextJob(Task.State.RUNNING,
                               makeDoneQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * @return a wrapper that will invoke a done query.
     */
    private JobWrapper makeDoneQueryJob(final int taskId,
                                        final TaskRunner runner,
                                        final AdminParams ap) {
        return new JobWrapper(taskId, runner, "query remove table data done") {
            @Override
            public NextJob doJob() {
                return queryForDone(taskId, runner, ap);
            }
        };
    }

    private DurationParameter getCheckIndexTime(AdminParams ap) {
        return ap.getCheckAddIndexPeriod();
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
                                           namespace,
                                           tableName,
                                           null)
                                 .append(" on ").append(groupId.getGroupName());
    }

    /**
     * No true impact on table or index creation, no need to compare.
     */
    @Override
    public boolean logicalCompare(Task t) {
        return true;
    }
}
