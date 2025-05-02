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
package oracle.kv.impl.client.admin;

import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.security.AuthContext;

/**
 * DDLCheckTask contacts the Admin service to obtain new status for a given
 * plan. The new status is sent on to the DdlStatementExecutor to update
 * any waiting targets.
 */
class DdlCheckTask implements Runnable {

    private final Logger logger;

    private final int planId;
    private final DdlStatementExecutor statementExec;

    private final int maxRetries;
    private final long delayMs;
    private final AuthContext authCtx;

    DdlCheckTask(int planId,
                 DdlStatementExecutor statementExec,
                 int maxRetries,
                 long delayMs,
                 Logger logger,
                 AuthContext authCtx) {
        assert maxRetries > 0;
        this.planId = planId;
        this.statementExec = statementExec;
        this.maxRetries = maxRetries;
        this.delayMs = delayMs;
        this.logger = logger;
        this.authCtx = authCtx;
    }

    @Override
    public void run() {

        /* Call to the server side to get up to date status. */
        try {
            ExecutionInfo newInfo = statementExec.getClientAdminService().
                getExecutionStatus(planId);
            newInfo = DdlFuture.checkForNeedsCancel(newInfo, statementExec,
                                                    planId, authCtx);
            statementExec.updateWaiters(newInfo, null, false);
            statementExec.scheduleCheckTask(planId, maxRetries, delayMs,
                                            authCtx);
        } catch (RemoteException e) {
            final int remaining = maxRetries - 1;
            logger.fine("Got " + e + ", " + remaining + " retries remaining");

            if (remaining > 0) {
                statementExec.scheduleCheckTask(planId, remaining, delayMs,
                                                authCtx);
                return;
            }
            statementExec.shutdownWaitersDueToError(planId, e);
        } catch (Throwable t) {
            logger.info("DDL polling task for plan " + planId +
                        " shut down due to " + t);
            statementExec.shutdownWaitersDueToError(planId, t);
        }
    }
}
