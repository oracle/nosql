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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.Admin.RunTransaction;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.admin.plan.task.Utils.ShardNotFoundException;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;

import com.sleepycat.je.Transaction;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;

/**
 * Wait for an add a new index on a RepNode. Each node will populate the
 * new index for its shard.
 */
public class WaitForAddIndex extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private final MetadataPlan<TableMetadata> plan;
    private final RepGroupId groupId;

    private final String indexName;
    private final String tableName;
    private final String namespace;

    private static final long START_WAIT_TIME_MS = 500;

    /*
     * Wait time between check calls. Start at 500ms increasing
     * to getCheckAddIndexPeriod()
     */
    private long waitTimeMS = START_WAIT_TIME_MS;

    /**
     *
     * @param plan
     * @param groupId ID of the current rep group of the partition
     * will stop.
     */
    public WaitForAddIndex(MetadataPlan<TableMetadata> plan,
                           RepGroupId groupId,
                           String namespace,
                           String indexName,
                           String tableName) {
        this.plan = plan;
        this.groupId = groupId;
        this.indexName = indexName;
        this.tableName = tableName;
        this.namespace = namespace;
    }

    @Override
    protected MetadataPlan<TableMetadata> getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public Callable<State> getFirstJob(int taskId, TaskRunner runner) {
        return makeWaitForAddIndexJob(taskId, runner);
    }

    /**
     * @return a wrapper that will invoke a add index job.
     */
    private JobWrapper makeWaitForAddIndexJob(final int taskId,
                                              final TaskRunner runner) {

        return new JobWrapper(taskId, runner, "add index") {

            @Override
            public NextJob doJob() {
                return waitForAddIndex(taskId, runner);
            }
        };
    }

    /**
     * Query for add index to complete
     */
    private NextJob waitForAddIndex(int taskId, TaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        try {
            final RepNodeAdminAPI masterRN = getMaster(plan, groupId);
            if (masterRN == null) {
                /* No master available, try again later. */
                return new NextJob(Task.State.RUNNING,
                                   makeWaitForAddIndexJob(taskId, runner),
                                   ap.getRNFailoverPeriod());
            }

            plan.getLogger().log(Level.FINE, "{0} wait", this);

            return queryForDone(taskId, runner, ap);

        } catch (ShardNotFoundException snfe) {
            plan.getLogger().log(Level.INFO, "{0} {1}, reporting done",
                                 new Object[] {this, snfe});
            return NextJob.END_WITH_SUCCESS;
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeWaitForAddIndexJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());
        }
    }

    private NextJob queryForDone(int taskId,
                                 TaskRunner runner,
                                 AdminParams ap) {
        /*
         * Check whether the index still exist. If not, then that means there
         * has been a failure on some other shard. Log it and exit. [#26975]
         */
        final TableMetadata md = plan.getMetadata();
        if ((md != null) &&
            (md.getIndex(namespace, tableName, indexName) == null)) {
            final String msg = this + ", index missing from metadata, exiting";
            plan.getLogger().log(Level.INFO, msg);
            return new NextJob(Task.State.ERROR, null, null, msg);
        }

        try {
            final RepNodeAdminAPI masterRN = getMaster(plan, groupId);
            if (masterRN == null) {
                /* No master to talk to, repeat step2 later. */
                return new NextJob(Task.State.RUNNING,
                                   makeDoneQueryJob(taskId, runner, ap),
                                   ap.getRNFailoverPeriod());
            }

            final boolean done = masterRN.addIndexComplete(namespace,
                                                           indexName,
                                                           tableName);

            plan.getLogger().log(Level.INFO,
                                 "{0} done={1}",
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
        } catch (Exception re) {
            final String msg = this + " failure on shard, error=" +
                               re.getMessage();
            /*
             * There was a problem. Log it, mark the index as populate failed,
             * and return task failure
             */
            plan.getLogger().log(Level.INFO, msg);
            setPopulateFailed();
            return new NextJob(Task.State.ERROR, null, null, msg);
        }
    }

    /**
     * @return a wrapper that will invoke a done query.
     */
    private JobWrapper makeDoneQueryJob(final int taskId,
                                        final TaskRunner runner,
                                        final AdminParams ap) {
        return new JobWrapper(taskId, runner, "query add index done") {
            @Override
            public NextJob doJob() {
                return queryForDone(taskId, runner, ap);
            }
        };
    }

    private DurationParameter getCheckIndexTime(AdminParams ap) {
        DurationParameter dp = ap.getCheckAddIndexPeriod();

        /* May be zero due to upgrade */
        if (waitTimeMS == 0) {
            waitTimeMS = START_WAIT_TIME_MS;
        }
        if ((dp != null) && (waitTimeMS < dp.toMillis())) {
            dp = new DurationParameter("", TimeUnit.MILLISECONDS, waitTimeMS);
            waitTimeMS += waitTimeMS;
        }
        return dp;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return TablePlanGenerator.makeName(super.getName(sb),
                                           namespace,
                                           tableName,
                                           indexName)
                                 .append(" on ").append(groupId.getGroupName());

    }

    /**
     * No true impact on table or index creation, no need to compare.
     */
    @Override
    public boolean logicalCompare(Task t) {
        return true;
    }

    /**
     * Removes the failed index. Propagation of the new metadata is
     * handled by CompleteAddIndex.
     *
     * Note that the modified metadata may be broadcast by some other plan
     * before this plan is done. This will cause the population on the RNs
     * to stop and they will remove all information about the index. Other
     * waitForAddIndex must stop in this case because the RNs will report
     * not-done forever. (see index check in queryForDone).
     */
    private void setPopulateFailed() {
        final Admin admin = plan.getAdmin();

        /*
         * Run a transaction to remove the index. The index may already be
         * gone due to an earlier task failing.
         */
        new RunTransaction<Void>(admin.getEnv(),
                                 Admin.RunTransaction.sync,
                                 plan.getLogger()) {
            @Override
            protected Void doTransaction(Transaction txn) {
                final TableMetadata md = plan.getMetadata(txn);
                if (md == null) {
                    throw new IllegalStateException("Table metadata not found");
                }

                try {
                    md.dropIndex(namespace, indexName, tableName);
                    admin.saveMetadata(md, txn);
                } catch (IllegalArgumentException iae) {
                    /* Index already gone */
                }
                return null;
            }

            @Override
            protected boolean shuttingDown() {
                return plan.getPlanner().isShutdown();
            }
        }.run();
    }
}
