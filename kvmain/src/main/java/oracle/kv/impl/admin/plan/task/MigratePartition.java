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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.rep.admin.RepNodeAdmin.MigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

/**
 * Move a partition from one RepGroup to another, for topology redistribution.
 */
public class MigratePartition extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private final DeployTopoPlan plan;
    private final RepGroupId sourceRGId;
    private final RepGroupId targetRGId;
    private final PartitionId partitionId;
    private final RepGroupId failedShard;

    /* for logging messages. */
    private transient RepNodeId targetRNId;

    /**
     * We expect that the target RepNode exists before MigratePartition is
     * executed.
     *
     * @param plan
     * @param sourceRGId ID of the current rep group of the partition
     * @param targetRGId ID of the new rep group of the partition
     * @param partitionId ID the partition to migrate
     * will stop.
     * @param failedShard if non-null, logical partition migration from
     * failed shard.
     */
    public MigratePartition(DeployTopoPlan plan,
                            RepGroupId sourceRGId,
                            RepGroupId targetRGId,
                            PartitionId partitionId,
                            RepGroupId failedShard) {
        super();
        this.plan = plan;
        this.sourceRGId = sourceRGId;
        this.targetRGId = targetRGId;
        this.partitionId = partitionId;
        this.failedShard = failedShard;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    /**
     * Find the master of the target RepGroup.
     * @return null if no master can be found, otherwise the admin interface
     * for the master RN.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private RepNodeAdminAPI getTarget()
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        final Topology topology = admin.getCurrentTopology();
        final PingCollector collector =
            new PingCollector(topology, plan.getLogger());
        final RepNode targetMasterRN = collector.getMaster(targetRGId);
        if (targetMasterRN == null) {
            targetRNId = null;
            return null;
        }

        targetRNId = targetMasterRN.getResourceId();
        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils registryUtils =
                new RegistryUtils(topology, loginMgr, plan.getLogger());
        return registryUtils.getRepNodeAdmin(targetMasterRN.getResourceId());
    }

    /**
     * Find the master of the source RepGroup. Differs slightly from
     * getTarget() in that there is no need to save the targetRNId field.
     */
    private RepNodeAdminAPI getSource()
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        final Topology topology = admin.getCurrentTopology();
        final PingCollector collector =
            new PingCollector(topology, plan.getLogger());
        final RepNode masterRN = collector.getMaster(sourceRGId);
        if (masterRN == null) {
            return null;
        }

        final LoginManager loginMgr = admin.getLoginManager();
        final RegistryUtils registryUtils =
                new RegistryUtils(topology, loginMgr, plan.getLogger());
        return registryUtils.getRepNodeAdmin(masterRN.getResourceId());
    }

    /**
     * Partition migration goes through these steps:
     *
     * Step 1: Invoke RepNodeAdminAPI.migration. Retry if target is not
     *         available
     * Step 2: Query target for migration status. Retry if status is not
     *         terminal.
     * Step 2a:If migration status indicates error, cancel the source.
     * Step 3: Update topology in admin db. Retry if there are any failures.
     * Step 4: Broadcast topology to all RNs. Retry if there are any failures.
     *
     * The task is idempotent; if the migration already occurred, step 1 will
     * just return a success status.
     *
     * startWork() begins with Step 1
     */
    @Override
    public Callable<Task.State> getFirstJob(int taskId, TaskRunner runner) {
        return makeRequestMigrationJob(taskId, runner);
    }

    /**
     * Do Step 1 - start the migration.
     */
    private NextJob requestMigration(int taskId, TaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        try {

            if (failedShard != null) {
                return updateTopoInAdminDB(taskId, runner);
            }

            final RepNodeAdminAPI target = getTarget();

            if (target == null) {
                /* No master available, try step 1 again later. */
                return new NextJob(Task.State.RUNNING,
                                   makeRequestMigrationJob(taskId, runner),
                                   ap.getRNFailoverPeriod());
            }

            /* Start a migration */
            plan.getLogger().log(Level.INFO, "{0} migration submitted", this);
            final MigrationState mState = target.migratePartitionV2(partitionId,
                                                                    sourceRGId);

            /* Plan on going to Step 2 or 3 */
            return checkMigrationState(target, mState, taskId, runner, ap);

        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeRequestMigrationJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());

        }
    }

    /**
     * @return a wrapper that will invoke a migration job.
     */
    private JobWrapper makeRequestMigrationJob
        (final int taskId, final TaskRunner runner) {
        return new PartitionJob(taskId, runner, "request migration") {
            @Override
            public NextJob doJob() {
                return requestMigration(taskId, runner);
            }
        };
    }

    /**
     * Take action based on the migration state, which has been obtained via
     * the original migratePartition call, or subsequent queries to the target
     * RN for migration state. Depending on the migration state, the next
     * step is Step 1, Step 2, or Step 3
     * @throws RemoteException
     */
    private NextJob checkMigrationState(RepNodeAdminAPI target,
                                        MigrationState mState,
                                        final int taskId,
                                        final TaskRunner runner,
                                        AdminParams ap) {

        NextJob nextJob = null;

        /*
         * We check the migration state for exception information irregardless
         * of the state value, although in general we really only expect
         * exceptions with ERROR and UNKNOWN. The motivation is just to make
         * this as general purpose as possible, in case there are unexpected
         * cases where exception info is passed in the future.
         *
         * TODO: for now the plan executor is only saving exception info when
         * the task ends in error. An UNKNOWN state actually may have an
         * exception, which we need to propagate. The problem is that this
         * leaves the task running. Perhaps the error information should be
         * saved in the taskRun details?
         */
        final Exception cause = mState.getCause();
        final String errorInfo = (cause == null) ? null : cause.getMessage();

        /*
         * Obtain more details about the migration and save it in the TaskRun
         * This is purely for reporting purposes, so bail out if any exceptions
         * happen.
         */
        try {
            getMigrationDetails(target, mState, taskId,  runner);
        } catch (Exception e) {
            plan.getLogger().log
                (Level.INFO,
                 "{0} migration state={1} " +
                 " exception seen when getting detailed status: {2}",
                 new Object[] {this, mState, LoggerUtils.getStackTrace(e)});
        }

        switch(mState.getPartitionMigrationState()) {
        case ERROR:

            /* The migration has failed, tell the source to cancel. */
            String additionalInfo = "target=" + targetRNId + " state=" + mState;
            if (errorInfo != null) {
                additionalInfo += " " + errorInfo;
            }

            nextJob = cancelMigration(taskId, runner, additionalInfo, ap);
            break;

        case PENDING:
        case RUNNING:

            /*
             * Schedule Step 2, request migration status
             * The migration hasn't finished, so wait a bit, and then
             * poll for the migration state again.
             */
            nextJob = new NextJob(Task.State.RUNNING,
                                  makeStatusQueryJob(taskId, runner, ap),
                                  ap.getCheckPartitionMigrationPeriod(),
                                  errorInfo);
            break;

        case UNKNOWN:

            /*
             * Retry the migration request (step 1). There may have been a
             * failure on the RN and the initial request may not have been
             * recorded, or there was an error getting the info, or a migration
             * error which was lost. UNKNOWN may also be due to asking a
             * replica. In any case, no harm in trying again after a short
             * wait.
             */
            nextJob = new NextJob(Task.State.RUNNING,
                                  makeRequestMigrationJob(taskId, runner),
                                  ap.getRNFailoverPeriod(),
                                  errorInfo);
            break;

        case SUCCEEDED:
            /* Update the topology. */
            nextJob = updateTopoInAdminDB(taskId, runner);
            break;
        }
        return nextJob;
    }

    /**
     * Do Step 2: query for migration status.
     */
    private NextJob queryForStatus(int taskId,
                                   TaskRunner runner,
                                   AdminParams ap) {

        try {
            final RepNodeAdminAPI target = getTarget();
            if (target == null) {
                /* No master to talk to, repeat step2 later. */
                return new NextJob(Task.State.RUNNING,
                                   makeStatusQueryJob(taskId, runner, ap),
                                   ap.getRNFailoverPeriod());
            }

            final MigrationState mstate =
                target.getMigrationStateV2(partitionId);
            plan.getLogger().log(Level.FINE,
                                 "{0} migration state={1}",
                                 new Object[] {this, mstate});
            return checkMigrationState(target, mstate, taskId, runner, ap);
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try step 2 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeStatusQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * Query the target and source, if possible, for more details about
     * migration status, for reporting reasons.
     * @throws NotBoundException
     * @throws RemoteException
     */
    private void getMigrationDetails(RepNodeAdminAPI target,
                                     MigrationState mState,
                                     final int taskId,
                                     final TaskRunner runner)
        throws RemoteException, NotBoundException {

        PartitionMigrationStatus status;
        switch(mState.getPartitionMigrationState()) {
        case RUNNING:
        case SUCCEEDED:
            status = target.getMigrationStatus(partitionId);
            if (status != null) {
                plan.addTaskDetails(runner.getDetails(taskId), status.toMap());
            }

            final RepNodeAdminAPI source = getSource();
            if (source != null) {
                status = source.getMigrationStatus(partitionId);
                if (status != null) {
                    plan.addTaskDetails(runner.getDetails(taskId),
                                        status.toMap());
                }
            }
            break;
        case ERROR:
        case PENDING:
            /* Only the target has status, not the source. */
            status = target.getMigrationStatus(partitionId);
            if (status != null) {
                plan.addTaskDetails(runner.getDetails(taskId), status.toMap());
            }

            break;
        case UNKNOWN:
            /*
             * The information was returned by a replica, which means that
             * there was some kind of failover. There are no details to be
             * had from either source or target.
             */
            break;
        }

    }

    /**
     * @return a wrapper that will invoke a status query.
     */
    private JobWrapper makeStatusQueryJob(final int taskId,
                                          final TaskRunner runner,
                                          final AdminParams ap) {
        return new PartitionJob(taskId, runner, "query migration status") {
            @Override
            public NextJob doJob() {
                return queryForStatus(taskId, runner, ap);
            }
        };
    }

    /**
     * Do Step 2a: cancel a failed migration.
     */
    private NextJob cancelMigration(int taskId,
                                    TaskRunner runner,
                                    String cancelReason,
                                    AdminParams ap) {

        try {

            /* Find the source master. */
            final Admin admin = plan.getAdmin();
            final Topology topology = admin.getCurrentTopology();
            final PingCollector collector =
                new PingCollector(topology, plan.getLogger());
            final RepNode sourceMasterRN = collector.getMaster(sourceRGId);
            if (sourceMasterRN == null) {
                /* Can't contact the source, retry later. */
                return new NextJob(Task.State.RUNNING,
                                   makeCancelJob(taskId, runner,
                                                 cancelReason, ap),
                                   ap.getRNFailoverPeriod());
            }

            final LoginManager loginMgr = admin.getLoginManager();
            final RegistryUtils registryUtils =
                    new RegistryUtils(topology, loginMgr, plan.getLogger());
            final RepNodeId sourceRNId = sourceMasterRN.getResourceId();
            final RepNodeAdminAPI source =
                    registryUtils.getRepNodeAdmin(sourceRNId);

            checkNull("source", source);

            final boolean done = source.canceled(partitionId, targetRGId);

            plan.getLogger().log(Level.INFO,
                                 "{0} source={1} cancellation confirmation={2}",
                                 new Object[] {this, sourceRNId, done});

            if (done) {
                return new NextJob(Task.State.ERROR, cancelReason);
            }

            /*
             * Retry until the cancel works. The user can stop a long running
             * cancel by issuing a plan interrupt.
             */
            return new NextJob(Task.State.RUNNING,
                               makeCancelJob(taskId, runner, cancelReason, ap),
                               ap.getCheckPartitionMigrationPeriod());
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try step 2a again later. */
            return new NextJob(Task.State.RUNNING,
                               makeCancelJob(taskId, runner, cancelReason, ap),
                               ap.getServiceUnreachablePeriod());

        }
    }

    /**
     * @return a wrapper that will invoke a status query.
     */
    private JobWrapper makeCancelJob(final int taskId,
                                     final TaskRunner runner,
                                     final String cancelReason,
                                     final AdminParams ap) {
        return new PartitionJob(taskId, runner, "cancel migration") {
            @Override
            public NextJob doJob() {
                return cancelMigration(taskId, runner, cancelReason, ap);
            }
        };
    }

    /**
     * Do Step 3: update the topology in the admin database. Since this is part
     * of the migration transfer of ownership protocol, this must succeed, so
     * if there is any failure, we will retry indefinitely (until the plan is
     * canceled.)
     */
    private NextJob updateTopoInAdminDB(final int taskId,
                                        final TaskRunner runner) {
        final Admin admin = plan.getAdmin();
        try {

            /*
             * Partition change is done atomically within the Admin.
             * If nothing changed, skip the broadcast.
             */
            if (!admin.updatePartition(partitionId, targetRGId,
                                       plan.getDeployedInfo(), plan)) {
                return NextJob.END_WITH_SUCCESS;
            }

            /* Success, go to step 4: broadcast to other RNs without delay. */
            return broadcastTopo(admin, taskId, runner);

        } catch (Exception e) {

            /*
             * Update failed, admin db is unavailable, retry step 3 after
             * delay.
             */
            final AdminParams ap = admin.getParams().getAdminParams();
            return new NextJob(Task.State.RUNNING,
                               makePersistToDBJob(taskId, runner),
                               ap.getAdminFailoverPeriod());
        }
    }

    /**
     * @return a wrapper that will update the topology in the admin db.
     */
    private JobWrapper makePersistToDBJob(final int taskId,
                                          final TaskRunner runner) {
        return new PartitionJob(taskId, runner, "update topo in admin db") {
            @Override
            public NextJob doJob() {
                return updateTopoInAdminDB(taskId, runner);
            }
        };
    }

    /**
     * Do Step 4: broadcast to all RNs.
     * TODO: Utils.broadcastTopoChangesToRN now has a concept of retrying
     * within it. How does it mesh with this retry?
     */
    private NextJob broadcastTopo(final Admin admin,
                                  final int taskId,
                                  final TaskRunner runner) {
        try {
            /* Send topology changes to all nodes. */
            if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                            admin.getCurrentTopology(),
                                            getName(),
                                            admin.getParams().getAdminParams(),
                                            plan,
                                            failedShard,
                                            Collections.emptySet())) {
                return new NextJob(Task.State.INTERRUPTED,
                                   "task interrupted before new topology " +
                                   "was sent to enough nodes");
            }

            /* Success, finish task.*/
            return NextJob.END_WITH_SUCCESS;

        } catch (Exception e) {
            /* Broadcast failed, repeat step 4 */
            final AdminParams ap = admin.getParams().getAdminParams();
            return new NextJob(Task.State.RUNNING,
                               makeBroadcastJob(taskId, runner, admin),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * @return a wrapper that will broadcast a topology
     */
    private JobWrapper makeBroadcastJob(final int taskId,
                                        final TaskRunner runner,
                                        final Admin admin) {
        return new PartitionJob(taskId, runner, "broadcast topology") {
            @Override
            public NextJob doJob() {
                return broadcastTopo(admin, taskId, runner);
            }
        };
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(partitionId)
                               .append(" from ").append(sourceRGId)
                               .append(" to ").append(targetRGId);
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
        @Override
        public void run() {

            PartitionMigrationState targetState =
                                        PartitionMigrationState.UNKNOWN;

            while (!plan.cleanupInterrupted()) {
                try {

                    /*
                     * Try to cancel the target until we have some known state.
                     */
                    if (targetState == PartitionMigrationState.UNKNOWN) {
                        targetState = cancelTarget();
                    }

                    /*
                     * If the state is null, the source has no record of this
                     * partition migration (and doesn not have the partition).
                     * In this case we will assume the migration was never
                     * started and we do not need to cancel the source.
                     */
                    if (targetState == null) {
                        return;
                    }
                    switch (targetState) {

                        case SUCCEEDED:
                            /*
                             * Cancel can't be done because the partition
                             * migration has completed.
                             */
                            final Admin admin = plan.getAdmin();

                            /* If nothing changed, skip the broadcast */
                            if (!admin.updatePartition(partitionId, targetRGId,
                                                       plan.getDeployedInfo(),
                                                       plan)) {
                                return;
                            }
                            try {
                                if (Utils.broadcastTopoChangesToRNs
                                            (plan.getLogger(),
                                             admin.getCurrentTopology(),
                                             getName(),
                                             admin.getParams().getAdminParams(),
                                             plan)) {
                                    /* all done, no cancel */
                                    return;
                                }

                            } catch (InterruptedException e) {}

                            /* Hmm, the broadcast didn't work, retry. */
                            break;

                        case ERROR:
                            /*
                             * Migration was canceled, so make sure the source
                             * is canceled as well.
                             */
                            if (cancelSource()) {
                                /* all done, canceled */
                                return;
                            }
                            /* Canceling the source didn't work, retry */
                            break;

                        default:
                            /* Canceling the target didn't work, retry */
                            targetState = PartitionMigrationState.UNKNOWN;
                    }
                } catch (Exception e) {
                    plan.getLogger().log
                        (Level.SEVERE,
                         "{0} problem when cancelling migration: {1}",
                         new Object[] {this, LoggerUtils.getStackTrace(e)});
                }

                /*
                 * TODO: would be better to schedule a job, rather
                 * than sleep.
                 */
                try {
                    Thread.sleep(CLEANUP_RETRY_MILLIS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
        };
    }

    /**
     * Attempts to cancel the partition migration on the target.
     *
     * @return the state of the partition migration or null
     */
    private PartitionMigrationState cancelTarget()
            throws RemoteException, NotBoundException {

        final RepNodeAdminAPI target = getTarget();
        if (target == null) {
            plan.getLogger().log(Level.INFO,
                                 "{0} attempted to cancel migration, but " +
                                 "can''t contact target RN", this);
            return PartitionMigrationState.UNKNOWN;
        }

        final MigrationState migrationState = target.canCancelV2(partitionId);
        final PartitionMigrationState state =  (migrationState == null) ? null :
            migrationState.getPartitionMigrationState();

        /*
         * The state return is a little confusing -- SUCCEEDED means that the
         * migration has happened, and ERROR means that it will be stopped.
         */
        final String meaning = (state == PartitionMigrationState.SUCCEEDED) ?
            "migration finished, can't be canceled" :
            ((state == PartitionMigrationState.ERROR) ?
             "migration will be stopped" : "problem canceling migration");

        plan.getLogger().log(Level.INFO,
                             "{0} request to cancel migration: {1} {2}",
                             new Object[] {this, state, meaning});
        return state;
    }

    /**
     * Attempts to cancel the partition migration on the source.
     *
     * @return true if the cancel was successful
     */
    private boolean cancelSource() throws RemoteException, NotBoundException {
        final RepNodeAdminAPI source = getSource();
        if (source == null) {
            return false;
        }

        final boolean canceled = source.canceled(partitionId, targetRGId);
        plan.getLogger().log(Level.INFO,
                             "{0} cancel at source={1}",
                             new Object[] {this, canceled});
        return canceled;
    }

    /**
     * Prepend the partition id onto all status messages for that phase, or job.
     */
    private abstract class PartitionJob extends JobWrapper {

        public PartitionJob(int taskId, TaskRunner runner,
                            String description) {
            super(taskId, runner, description);
        }

        @Override
        public String getDescription() {
            return partitionId + ": " + super.getDescription();
        }
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockRG(planner, plan, sourceRGId);
        LockUtils.lockRG(planner, plan, targetRGId);
    }

    /*
     * Return detailed stats collected on the source and target about migration
     * execution.
     *
     * @return null if there are no details.
     */
    @Override
    public String displayExecutionDetails(Map<String, String> details,
                                          String displayPrefix) {
        final PartitionMigrationStatus targetStatus =
                PartitionMigrationStatus.parseTargetStatus(details);
        if (targetStatus == null) {
            return null;
        }

        return targetStatus.display(displayPrefix);
    }

    @Override
    public String getTaskProgressType() {
        return "migratePartition";
    }
}
