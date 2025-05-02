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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.NetworkRestorePlan;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.rep.NetworkRestoreStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;

/**
 * Task to keep waiting a network restore execution is done and polling the
 * network restore status from target node.
 */
public class WaitForNetworkRestore extends AbstractTask {

    private static final long serialVersionUID = 1L;
    public static final String BACKUP_STATS_KEY = "NetworkBackupStats";

    private final NetworkRestorePlan plan;
    private final ResourceId sourceNode;
    private final ResourceId targetNode;

    public WaitForNetworkRestore(NetworkRestorePlan plan,
                                 ResourceId sourceNode,
                                 ResourceId targetNode) {
        this.plan = plan;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
    }

    @Override
    protected NetworkRestorePlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public Callable<State> getFirstJob(int taskId, TaskRunner runner) {
        return makeWaitForNetworkRestoreJob(taskId, runner);
    }

    /**
     * @return a wrapper that will invoke a WaitForNetworkRestore job
     */
    private JobWrapper makeWaitForNetworkRestoreJob(int taskId,
                                                    TaskRunner runner) {

        return new JobWrapper(taskId, runner, "network restore") {

            @Override
            public NextJob doJob() {
                return waitForNetworkRestore(taskId, runner);
            }
        };
    }

    /**
     * Wait for network restore to complete
     */
    private NextJob waitForNetworkRestore(int taskId, TaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        RepNodeAdminAPI targetRna = null;
        try {
            targetRna = getRepNodeAdmin((RepNodeId)targetNode);
            if (targetRna == null) {

                /* RepNodeAdmin unavailable, try again later. */
                return new NextJob(Task.State.RUNNING,
                                   makeWaitForNetworkRestoreJob(taskId, runner),
                                   ap.getRNFailoverPeriod());
            }

            plan.getLogger().log(Level.INFO,
                                 "{0} wait for network restore to complete",
                                 this);

            return queryForDone(taskId, runner, ap);

        } catch (RemoteException | NotBoundException e) {
            return new NextJob(Task.State.RUNNING,
                               makeWaitForNetworkRestoreJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());

        }
    }

    /**
     * Find the RepNodeAdmin of given RepNodeId
     */
    private RepNodeAdminAPI getRepNodeAdmin(RepNodeId rnId)
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(),
                              plan.getLogger());
        return registryUtils.getRepNodeAdmin(rnId);
    }

    private NextJob queryForDone(int taskId,
                                 TaskRunner runner,
                                 AdminParams ap) {

        try {
            final RepNodeAdminAPI targetRna =
                    getRepNodeAdmin((RepNodeId)targetNode);
            if (targetRna == null) {
                return new NextJob(Task.State.RUNNING,
                                   makeDoneQueryJob(taskId, runner, ap),
                                   ap.getRNFailoverPeriod());
            }

            final NetworkRestoreStatus status =
                targetRna.getNetworkRestoreStatus();

            if (status == null) {
                final String errorMsg = this + " unable to obtain the status";
                plan.getLogger().log(Level.INFO, errorMsg);
                return new NextJob(Task.State.ERROR, null, null, errorMsg);
            }
            final boolean done = status.isCompleted();
            plan.getLogger().log(Level.INFO, "{0} done={1}",
                                 new Object[] {this, done});
            addTaskDetail(runner, taskId, status);

            if (done) {
                if (status.getException() != null) {
                    return makeNetworkRestoreErrorJob(status.getException());
                }
                return NextJob.END_WITH_SUCCESS;
            }
            return new NextJob(Task.State.RUNNING,
                               makeDoneQueryJob(taskId, runner, ap),
                               getCheckNetworkRestoreTime(ap));
        } catch (RemoteException | NotBoundException e) {
            /* RMI problem, try again later. */
            return new NextJob(Task.State.RUNNING,
                               makeDoneQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        } catch (Exception problem) {
            return makeNetworkRestoreErrorJob(problem);
        }
    }

    /**
     * @return a wrapper that will invoke a done query.
     */
    private JobWrapper makeDoneQueryJob(final int taskId,
                                        final TaskRunner runner,
                                        final AdminParams ap) {
        return new JobWrapper(taskId, runner, "query network restore done") {
            @Override
            public NextJob doJob() {
                return queryForDone(taskId, runner, ap);
            }
        };
    }

    private DurationParameter getCheckNetworkRestoreTime(AdminParams ap) {
        return ap.getCheckNetworkRestorePeriod();
    }

    private NextJob makeNetworkRestoreErrorJob(Exception e) {
        final String errorMsg = this + " failed, error=" + e;
        plan.getLogger().log(Level.INFO, errorMsg);
        return new NextJob(Task.State.ERROR, null, null, errorMsg);
    }

    private void addTaskDetail(TaskRunner runner,
                               int taskId,
                               NetworkRestoreStatus status) {
        if (status == null) {
            return;
        }
        final NetworkBackupStats stats = status.getNetworkBackStats();
        if (stats == null) {
            return;
        }
        plan.addTaskDetails(runner.getDetails(taskId), makeStatsMap(stats));
    }

    private Map<String, String> makeStatsMap(NetworkBackupStats stats) {
        final Map<String, String> statsMap = new HashMap<>();
        statsMap.put(BACKUP_STATS_KEY, stats.toString());
        return statsMap;
    }

    private String displayNetworkBackup(String stats) {
        return BACKUP_STATS_KEY + ":\n" + stats;
    }

    /*
     * Return network backup statistics collected from target node as
     * execution details.
     *
     * @return null if there are no details.
     */
    @Override
    public String displayExecutionDetails(Map<String, String> details,
                                          String displayPrefix) {
        final String backupStats = details.get(BACKUP_STATS_KEY);
        if (backupStats == null) {
            return null;
        }

        return displayNetworkBackup(displayPrefix);
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" from ").append(sourceNode)
                               .append(" to ").append(targetNode);
    }

    /**
     * No true impact on waiting network restore done, don't compare.
     */
    @Override
    public boolean logicalCompare(Task t) {
        return true;
    }
}
