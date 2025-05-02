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
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.VerifyDataPlanV2;
import oracle.kv.impl.admin.plan.task.JobWrapper.TaskRunner;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;

/**
 * A task that verify the btree and log files of databases
 * for RNs.
 *--
 */
public class VerifyRNDataV2 extends VerifyData<RepNodeId> {

    private static final long serialVersionUID = 1L;


    public VerifyRNDataV2(VerifyDataPlanV2 plan,
                        RepNodeId targetRnIds,
                        VerificationOptions verifyOptions) {
        super(plan, targetRnIds, verifyOptions);
    }

    @Override
    public boolean continuePastError() {
        /*
         * Don't run verification plan on the other rn's if the verification
         * tool reports in-memory/persistent corruption because
         * if there are in-memory/persistent corruption issues in the other
         * rn's too then it might cause the other rn's to crash leading to
         * loss of quorum.
         */
        return false;
    }

    @Override
    protected NextJob startAndPollVerify(int taskId, TaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        try {
            RegistryUtils registry =
                new RegistryUtils(plan.getAdmin().getCurrentTopology(),
                                  plan.getAdmin().getLoginManager(),
                                  plan.getLogger());
            RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(targetId);
            checkNull("rnAdmin", rnAdmin);
            retryTimes = 0;
            VerificationInfo info = null;
            if (started) {
                /*poll for status*/
                info = rnAdmin.startAndPollVerifyData(null,
                                                      getPlan().getId());
            }

            if (!started || info == null) {
                /* try starting verification */
                info = rnAdmin.startAndPollVerifyData(verifyOptions,
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
                        "RN Btree and Logfile corruption",
                        ErrorMessage.NOSQL_5601,
                        CommandResult.NO_CLEANUP_JOBS);
                } else if (!info.noBtreeCorruptions()) {
                    taskResult = new CommandFails(
                        "RN Btree corruption",
                        ErrorMessage.NOSQL_5600,
                        CommandResult.NO_CLEANUP_JOBS);
                } else {
                    taskResult = new CommandFails(
                        "RN Logfile corruption",
                        ErrorMessage.NOSQL_5601,
                        CommandResult.NO_CLEANUP_JOBS);
                }
                setTaskResult(taskResult);
                return NextJob.END_WITH_ERROR;
            }
        } catch (RemoteException | NotBoundException e) {
            if (!checkForRetry()) {
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
        String name = "VerifyRNData for " + targetId;
        return sb.append(name);
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {

            @Override
            public void run() {
                int cleanupRetryTime = 0;
                while (!plan.cleanupInterrupted() &&
                    cleanupRetryTime < LOST_CONNECTION_RETRY) {
                    try {
                        RegistryUtils registry =
                           new RegistryUtils(plan.getAdmin().
                                             getCurrentTopology(),
                                             plan.getAdmin().getLoginManager(),
                                             plan.getLogger());
                        RepNodeAdminAPI rnAdmin =
                                             registry.getRepNodeAdmin(targetId);
                        if (rnAdmin != null) {
                            rnAdmin.interruptVerifyData();
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
