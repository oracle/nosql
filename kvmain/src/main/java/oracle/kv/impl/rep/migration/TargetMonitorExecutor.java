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

package oracle.kv.impl.rep.migration;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.admin.RepNodeAdmin.MigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.migration.PartitionMigrations.MigrationRecord;
import oracle.kv.impl.rep.migration.PartitionMigrations.TargetRecord;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.DatabaseException;


/**
 * An executor for running tasks to monitor migration targets. There is
 * only one monitor task running at a time.
 */
public class TargetMonitorExecutor extends ScheduledThreadPoolExecutor {

    public final static long POLL_PERIOD = 2L;   /* 2 seconds */

    /**
     * The core pool size of the executor. ScheduledThreadPoolExecutor acts as a
     * fixed-sized pool using corePoolSize threads with unbounded queue. If we
     * are changing the executor and maximum pool size, the getNonStaleMaster()
     * method needs to be re-considered.
     */
    public final static int MONITOR_EXECUTOR_CONCURRENCY = 1;

    private final MigrationManager manager;
    private final RepNode repNode;
    private final Logger logger;
    private final RepGroupId sourceRGId;

    /**
     * Test hook to check that the TargetMonitorExecutor#failed method is
     * called in case the partition migration state is set to ERROR in case
     * of migration failure.
     */
    public static TestHook<PartitionId> checkRemoveRecordHook = null;

    TargetMonitorExecutor(MigrationManager manager,
                          RepNode repNode,
                          Logger logger) {
        super(MONITOR_EXECUTOR_CONCURRENCY,
            new KVThreadFactory(" target monitor", logger));
        assert (getCorePoolSize() <= 50) : "Reconsider check() "
            + "when the MONITOR_EXECUTOR_CONCURRENCY is too large "
            + "because we may create too much load "
            + "on the target shard with the NOP requests.";
        this.manager = manager;
        this.repNode = repNode;
        this.logger = logger;
        sourceRGId = new RepGroupId(repNode.getRepNodeId().getGroupId());
        setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /**
     * Schedules a target monitor task.
     */
    void monitorTarget() {
        try {
            schedule(new TargetMonitor(), POLL_PERIOD, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ree) {
            logger.log(Level.WARNING, "Failed to schedule monitor", ree);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        if (t != null) {
            logger.log(Level.INFO, "Target monitor execution failed", t);
            return;
        }

        if (!manager.isMaster()) {
            return;
        }
        @SuppressWarnings("unchecked")
        final Future<TargetMonitor> f = (Future<TargetMonitor>)r;

        TargetMonitor monitor = null;
        try {
            monitor = f.get();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Exception getting monitor", ex);
        }

        /*
         * If the monitor was returned, then it should be re-run if the queue
         * is empty. If there are other monitors in the queue, drop this one
         * and let the other(s) take its place.
         */
        if ((monitor != null) && getQueue().isEmpty()) {

            try {
                schedule(monitor, POLL_PERIOD, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ree) {
                logger.log(Level.WARNING, "Failed to restart monitor", ree);
            }
        }
    }

    /**
     * Callable which checks on completed source migrations. The check calls
     * the target of each completed source migration to see if the migration
     * has completed successfully or if there was an error. In the case of an
     * error, the source record is removed and the local topology updated.
     * This will restore the partition to this node.
     *
     * Monitoring will continue until 1) there are no completed source records,
     * 2) there are no targets which have not completed, or 3) the node
     * is no longer a master (or is shutdown).
     */
    private class TargetMonitor implements Callable<TargetMonitor> {

        @Override
        public TargetMonitor call() {
            boolean keepChecking = true;

            if (!manager.isMaster()) {
                return null;
            }

            final PartitionMigrations migrations = manager.getMigrations();

            if (migrations == null) {
                return this;
            }
            keepChecking = false;

            final Iterator<MigrationRecord> itr = migrations.completed();

            /*
             * Since check() does a remote call, recheck for state
             * change and shutdown.
             */
            while (itr.hasNext() && manager.isMaster()) {
                if (check(itr.next())) {
                    keepChecking = true;
                }
            }
            return keepChecking ? this : null;
        }

        /**
         * Checks whether the specified record represents a completed migration
         * source, and if so contacts the target to see if the operation
         * completed there. If the record needs to be checked again true is
         * returned.
         *
         * The check removes the record and restore the partition if we can be
         * sure the migration failed on the target. Such mechanism has both
         * safety and liveness implications. For safety, if the migration may
         * actually succeed on the target while the source restores the
         * partition, we are creating a split-brain problem on the partition
         * which is owned by both the source and target ([KVSTORE-2640]). For
         * liveness, if the migration failed, but the source does not restore
         * the partition, then we may increase the unavailability window of the
         * partition.
         *
         * The check is more likely to run into safety and liveness issue when
         * this node cannot obtain the latest mastership info of the target rep
         * group. The mastership info is very likely to be stale because in the
         * current mechanism there is no cross-rep-group gossip about rep node
         * state among rep nodes. The
         * RepNodeStateUpdateThread$RefreshRepNodeState only send NOP to the
         * nodes in another rep group when the request handler stub needs
         * repair. Furthermore, request forwarding also skip updating the
         * mastership info if the initial dispatcher (i.e., the client) already
         * received more recent update (see RequestHandlerImpl.requesterMap).
         *
         * In general, the safety is achieved by querying a recent enough master
         * (at least as recent as the target master of the persisted record).
         * The liveness is achieved by retrying the check and querying different
         * nodes. The detail of the correctness is discussed below. Note that
         * the current implementation has some caveats mentioned in the
         * discussion. To fix those caveats, we will need additional support
         * from JEHA and the RepNodeState gossip mechanism and is therefore left
         * for future work.
         *
         * Another issue with this check is that it creates additional load on
         * the target rep group. This is not a concern though because
         * MONITOR_EXECUTOR_CONCURRENCY is small. However, if we ever want to
         * bump that number, we will need to add additional caching mechanisms.
         *
         * # Correctness of the Check
         *
         * The correctness statements are based on the following assumptions and
         * assertions about the implementation:
         *
         * A1. A check C is associated with one and only one persisted
         * SourceRecord, SR;
         *
         * A2. There is one and only one MigrationTarget, MT (hosted on node M),
         * associated with SR;
         *
         * A3. MT may persist one and only one TargetRecord, TR;
         *
         * A4. If TR is persisted, then following masters must be able to
         * observe it;
         *
         * A5. C queries the node Q for the state of the migration;
         *
         * A6. If Q == M or Q.masterChangeTime > SR.startTime, then Q is a valid
         * query node.
         *
         * A7. C removes SR and exits if and only if (1) Q is a valid query node
         * and (2) Q returns ERROR; or Q returns PENDING and Q != M.
         *
         * A8. If the check C does not exit, then the implementation retries
         * with a follow-up check. This forms a series of checks S(C) = [C1, C2,
         * ...]. There exists a series of node-return tuples S(Q, R) = [(Q1,
         * R1), (Q2, R2), ...] corresponding to S(C) such that,
         *
         * - Qi is the node quried by Ci. Qi == null if no master is found for
         * Ci.
         *
         * - Ri is the return value of querying Qi. Ri == null if there is an
         * exception.
         *
         * - If Ri == SUCCEEDED, or Ri == PENDING or ERROR and Qi == M or
         * Qi.changeTime > SR.startTime, then Ci is the last check of S(C). We
         * call such (Qi, Ri) a halt condition.
         *
         * A9. If (Qi, Ri) is not the halt condition, then the master known to a
         * random node other than Qi is chosen as Qi+1.
         *
         * A10. A majority of nodes are reachable during a sufficiently long
         * period of time. Furthermore, an authoritative master is reachable
         * during a sufficiently long period of time.
         *
         * A11. If TR will never be persisted, then S(Q, R) contains (Qi, Ri),
         * where Ri == SUCCEEDED or ERROR if Qi = M or Ri == SUCCEEDED, PENDING
         * or ERROR if Qi != M, if the reachable authoritative master Qi is
         * quried.
         *
         * The above assumptions and assertions include basic assumption and
         * implementation details about partition migration implementation. Here
         * we provide some intuitive explanation.
         *
         * A1 is true because C is created after SR is persisted. Only one SR
         * can be persisted because only one MigrationSource can exist at a time
         * and the partition is guaranteed to be removed when SR is persisted so
         * that no MigrationSource can be created unless SR is removed.
         *
         * A2 is true because the MigrationSource is created in the life cycle
         * of MT. If MT is terminated, the TCP connection is closed which will
         * terminate the MigrationSource.
         *
         * A4 is true because the record is persisted with
         * SYNC/SYNC/SIMPLE_MAJORITY.
         *
         * A3, A5, A6, A7, A8, A9, A11 are true by implementation.
         *
         * A10 is true as our assumed failure model.
         *
         * ## Safety
         *
         * The safety statement is
         *
         * S1: If C removes SR and exits then TR will never be persisted.
         *
         * We can enumerate all the cases.
         *
         * Case 1: Q is the host of MT, i.e., Q == M.
         *
         * If C removes SR, then according to A7, Q returns ERROR. By the
         * implementation, this can only happen if TR is not yet persisted and
         * MT is being cancelled if not already. Therefore TR will never being
         * persisted.
         *
         * Case 2: Q is not the host of MT, i.e., Q != M.
         *
         * If C removes SR and exits, then according to A7, Q returns PENDING.
         * We actually cannot guarantee MT will never persist TR in this case.
         * Not even when Q is the latest authoritative master because M can
         * later become authoritative master again and still persist TR.
         *
         * However, suppose we can add one additional assumption that
         *
         * A12. There is at least one record being persisted after Q becomes the
         * master.
         *
         * This would happen with high probability under usual workload. S1 is
         * true under A12. This can be demonstrated as the following.
         *
         * First, TR has not been persisted yet. Because otherwise, according to
         * A4, Q must observe TR which contradicts with the returning PENDING
         * state. Therefore TR has not been persisted before M becomes
         * non-authoritative. According to A6, Q is a master that has a later
         * term than M. Furthermore, according to A12, Q has more persisted
         * record than M. Therefore, any master later than Q must have more
         * persisted records than M. Hence M must become replica before it can
         * become master again. When that happens MT will be terminated and will
         * never be able to persist the TR.
         *
         * ## Liveness
         *
         * The liveness statement is
         *
         * L1: C eventually removes SR and exits if TR will never be persisted.
         *
         * According to A8, L1 is equivalent to the statement that S(Q, R) is
         * finite if TR will never be persisted.
         *
         * Suppose L1 is not true such that TR will never be persisted, but S(Q,
         * R) is infinite.
         *
         * We can enumerate all the cases.
         *
         * Case 1: S(Q, R) always eventually contains a halt condition.
         *
         * This leads to contradiction.
         *
         * Case 2: S(Q) always eventually contains only (Q, R) tuples that is
         * not a halt condition.
         *
         * According to A8, A9, A10 and the assumption that S(Q, R) is infinite,
         * an authoritative master Qi will eventually be reached. According to
         * A11, if (Qi, Ri) is not a halt condition, then Qi.changeTime <
         * SR.startTime.
         *
         * It must be the case where Qi.changeTime > M.changeTime. Otherwise, Qi
         * cannot be an authoritative master since M persisted at least one
         * durable entry TR.
         *
         * It is possible that M.changeTime < Qi.changeTime < SR.startTime. This
         * is a race condition that the new master is elected before the old
         * master M's migration request is being executed on the source. Under
         * this case, if the migration fails on the new master, the partition
         * will be unavailable until a new migration is restarted. This could be
         * a concern but perhaps is fine because the probability of this
         * happening is small. Furthermore, the admin will eventually restart
         * the migration after it detects failure on the target.
         *
         * @param record
         * @return true if the record needs to be checked again
         */
        private boolean check(MigrationRecord record) {

            if (record instanceof TargetRecord) {
                return false;
            }

            assert record.getSourceRGId().equals(sourceRGId);

            logger.log(Level.FINE, "Check target for {0}", record);

            Exception ex;
            try {
                final RegistryUtils registryUtils =
                    new RegistryUtils(repNode.getTopology(),
                                      repNode.getLoginManager(),
                                      logger);
                /*
                 * Obtain the rep group state before obtain the master. This is
                 * because we will use the change time in the rep group as the
                 * lower bound of the time when the master is established for
                 * the isValidQueryResult() computation below. Reverse the order
                 * will break such assumption.
                 */
                final RepGroupId targetRGId = record.getTargetRGId();
                final RepGroupState rgs =
                    repNode.getRepGroupState(targetRGId);
                final RepNodeState rns = repNode.getMaster(targetRGId);

                if (rns == null) {
                    logger.log(Level.FINE,
                               "Master not found for {0}, sending NOP to " +
                               "update group table", targetRGId);
                    repNode.sendNOP(targetRGId);
                    return true;
                }

                final RepNodeAdminAPI rna = registryUtils.
                                            getRepNodeAdmin(rns.getRepNodeId());

                final MigrationState state =
                    rna.getMigrationStateV2(record.getPartitionId());
                switch (state.getPartitionMigrationState()) {
                    case ERROR:
                        if (!isValidQueryResult(rgs, rns, record)) {
                            repNode.sendRandomNOP(targetRGId,
                                Set.of(rns.getRepNodeId()));
                            return true;
                        }
                        /* Remove record, update local topo */
                        failed(record, state);
                        return false;

                    case SUCCEEDED:
                        /*
                         * If the target has completed successfully then we
                         * don't need to check again.
                         */
                        return false;

                    case UNKNOWN:
                        /*
                         * Likely do not have the master. Sending a NOP will
                         * cause the group table to be updated so that next time
                         * around we will have the correct master.
                         */
                        logger.log(Level.FINE,
                                   "Received UNKNOWN status from {0}, " +
                                   "sending NOP to update group table",
                                   targetRGId);
                        repNode.sendRandomNOP(targetRGId,
                                Set.of(rns.getRepNodeId()));
                        return true;

                    default:
                        if (record.getTargetRNId().equals(rns.getRepNodeId())) {
                            /* Running on same node, keep checking */
                            return true;
                        }
                        if (!isValidQueryResult(rgs, rns, record)) {
                            repNode.sendRandomNOP(targetRGId,
                                Set.of(rns.getRepNodeId()));
                            return true;
                        }
                        /*
                         * Got here means the migration is still running on the
                         * target. However, if the target node has changed,
                         * then the migration this record represents has failed
                         * AND the target mastership has changed.
                         */
                        logger.log(Level.INFO, () -> String.format(
                            "Migration source detected target mastership "
                                + "changed from %s to %s",
                            record.getTargetRNId(), rns.getRepNodeId()));
                        failed(record, state);
                        return false;
                }
            } catch (NotBoundException nbe) {
                ex = nbe;
            } catch (RemoteException re) {
                ex = re;
            } catch (DatabaseException de) {
                ex = de;
            }
            logger.log(Level.INFO,
                       "Exception while monitoring target for {0}: {1}",
                       new Object[]{record, ex});
            return true;
        }

        /**
         * Returns {@code true} if the quried result of the node can be used for
         * the check to restore the partition.
         */
        private boolean isValidQueryResult(RepGroupState rgs,
                                           RepNodeState rns,
                                           MigrationRecord record)
        {
            assert rns != null;
            if (rns.getRepNodeId().equals(record.getTargetRNId())) {
                /*
                 * The node of the target record is always valid.
                 */
                return true;
            }
            if (rgs.getLastChangeTime() > record.getStatus().getStartTime()) {
                /*
                 * The rep group state change time is the lower bound of the
                 * mastership change time of rns and it is larger than the start
                 * time of the record. Therefore, we know rns becomes a master
                 * after the MigrationTarget has been created.
                 *
                 * TODO: There is a caveat as is explained in the comment of the
                 * check() method.
                 *
                 * TODO: There is actually another caveat due to time drift
                 * among the nodes: the state change time is from the target
                 * shard and the record time is from the source.
                 */
                return true;
            }
            return false;
        }

        private void failed(MigrationRecord record,
                            MigrationState state) {
            if (state.getCause() != null) {
                logger.log(Level.INFO,
                           "Migration source detected failure of {0}, " +
                           "target returned {1} ({2}), removing completed " +
                           "record",
                           new Object[] {record, state, state.getCause()});
            } else {
                logger.log(Level.INFO,
                        "Migration source detected failure of {0}, " +
                        "target returned {1}, removing completed record",
                        new Object[] {record, state});
            }

            /* run test hook in unit test only */
            assert TestHookExecute.doHookIfSet(checkRemoveRecordHook,
                                               record.getPartitionId());
            manager.notifyPartitionMigrationRestore(record.getPartitionId());
            /* migration failed, will restart, and need to update PGT */
            manager.removeRecord(record.getPartitionId(), record.getId(),
                                 true, true/* update PGT */);
        }
    }
}

