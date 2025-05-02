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

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.VerifyDataPlanV2;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;

/**
 * A task that verifies the btree and log files of databases
 * for admins.
 *
 */
public class VerifyAdminDataV2 extends VerifyData<AdminId> {
    private static final long serialVersionUID = 1L;


    public VerifyAdminDataV2(VerifyDataPlanV2 plan,
                             AdminId targetAdminId,
                             VerificationOptions verifyOptions) {
        super(plan, targetAdminId, verifyOptions);

    }

    @Override
    public boolean continuePastError() {
        /*
         * Don't run verification plan on the other admin's if the verification
         * tool reports in-memory/persistent corruption because
         * if there are in-memory/persistent corruption issues in the other
         * admin's too then it might cause the other admin's to crash leading
         * to loss of quorum.
         */
        return false;
    }

    @Override
    protected Plan getPlan() {
        return plan;
    }

    @Override
    protected NextJob startAndPollVerify(int taskId,
                                         TaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        StorageNodeId curSNId = plan.getPlanner().getAdmin().
            getCurrentParameters().get(targetId).getStorageNodeId();
        final StorageNodeParams snParam = plan.getAdmin().
            getStorageNodeParams(curSNId);
        try {
            CommandServiceAPI cs = RegistryUtils.
                getAdmin(snParam.getHostname(), snParam.getRegistryPort(),
                         plan.getLoginManager(), plan.getLogger());
            checkNull("cs", cs);
            retryTimes = 0;
            VerificationInfo info = null;
            if (started) {
                /*poll for status*/
                info = cs.startAndPollVerify(null, getPlan().getId());
            }

            if (!started || info == null) {
                /* try starting verification */
                info = cs.startAndPollVerify(verifyOptions,
                                             getPlan().getId());
                started = true;
            }
            if (checkForCompletion(info)) {
                if (info.getCurrentState() == VerificationInfo.State.ERROR) {
                    return NextJob.END_WITH_ERROR;
                }
                /*
                 * Fix for KVSTORE-1747
                 * return ERROR if the verification task reports btree and/or
                 * log file corruption
                 */
                if (info.noBtreeCorruptions() &&
                    info.noLogFileCorruptions()) {
                    return NextJob.END_WITH_SUCCESS;
                }
                CommandResult taskResult;
                if (!info.noBtreeCorruptions() &&
                    !info.noLogFileCorruptions()) {
                    taskResult = new CommandFails(
                        "Admin Btree and Logfile corruption",
                        ErrorMessage.NOSQL_5601,
                        CommandResult.NO_CLEANUP_JOBS);
                } else if (!info.noBtreeCorruptions()) {
                    taskResult = new CommandFails(
                        "Admin Btree corruption",
                        ErrorMessage.NOSQL_5600,
                        CommandResult.NO_CLEANUP_JOBS);
                } else {
                    taskResult = new CommandFails(
                        "Admin Logfile corruption",
                        ErrorMessage.NOSQL_5601,
                        CommandResult.NO_CLEANUP_JOBS);
                }
                setTaskResult(taskResult);
                return NextJob.END_WITH_ERROR;
            }

        } catch (RemoteException | NotBoundException e) {
            if (!checkForRetry()) {
                /* Retry connecting the node. */
                return new NextJob(Task.State.ERROR,
                                   "Lost Connection to " + targetId);
            }
            /* Retry connecting the node. */
        }
        return new NextJob(Task.State.RUNNING,
                           makeStartPollVerifyJob(taskId, runner),
                           ap.getServiceUnreachablePeriod());
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        String name = "VerifyAdminData for " + targetId;
        return sb.append(name);
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
            @Override
            public void run() {
                StorageNodeId curSNId = plan.getPlanner().getAdmin().
                    getCurrentParameters().get(targetId).getStorageNodeId();
                final StorageNodeParams snParam = plan.getAdmin().
                                                getStorageNodeParams(curSNId);
                int cleanupRetryTime = 0;
                while (!plan.cleanupInterrupted() &&
                    cleanupRetryTime < LOST_CONNECTION_RETRY) {
                    try {
                        CommandServiceAPI cs =
                            RegistryUtils.getAdmin(snParam.getHostname(),
                                                   snParam.getRegistryPort(),
                                                   plan.getLoginManager(),
                                                   plan.getLogger());
                        if (cs != null) {
                            cs.interruptVerifyData();
                            return;
                        }
                    } catch (Exception e) {
                        /* Failed to interrupt verification, retry*/
                        plan.getLogger().info("Failed to interrupt " +
                            "verification. Exception: " + e.getMessage() +
                            " Retry.");
                    }
                    if (DatabaseUtils.VERIFY_ERROR_HOOK != null) {
                        /* for test only*/
                        break;
                    }

                    try {
                        Thread.sleep(CLEANUP_RETRY_MILLIS);
                    } catch (InterruptedException e) {
                        return;
                    }
                    cleanupRetryTime++;

                }

            }

        };
    }

}
