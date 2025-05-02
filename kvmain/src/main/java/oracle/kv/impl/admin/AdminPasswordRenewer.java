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
package oracle.kv.impl.admin;

import java.net.URI;
import java.util.logging.Level;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.security.PasswordRenewResult;
import oracle.kv.impl.security.PasswordRenewer;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;

/**
 *Provide password renewal on Admin node.
 */
public class AdminPasswordRenewer implements PasswordRenewer {

    /* The AdminService being supported */
    private final AdminService adminService;

    /**
     * Construct an AdminPasswordRenewer supporting the provided AdminService
     * instance.
     */
    public AdminPasswordRenewer(AdminService aService) {
        this.adminService = aService;
    }

    @Override
    public PasswordRenewResult renewPassword(String userName,
                                             char[] newPassword) {
        final URI masterURI = adminService.getAdmin().getMasterRmiAddress();
        CommandServiceAPI cs = null;
        int planId = 0;
        String resultMessage = null;
        try {
            cs = ServiceUtils.waitForAdmin(masterURI.getHost(),
                                           masterURI.getPort(),
                                           adminService.getLoginManager(),
                                           40,
                                           ServiceStatus.RUNNING,
                                           adminService.getLogger());

            /* Execute change-user plan to renew user's password */
            planId = cs.createChangeUserPlan("renew-password", userName,
               true /*isEnabled*/, newPassword, false /* retainPassword */,
               false /* clearRetainedPassword */);

            assert planId != 0;
            cs.approvePlan(planId);
            cs.executePlan(planId, false /* force */);
            final Plan.State state = cs.awaitPlan(planId, 0, null);

            /* Ignore other plan execution end state, treat them as failure. */
            if (state == Plan.State.SUCCEEDED) {
                return new PasswordRenewResult(true, resultMessage);
            }
            cancelPlan(cs, planId);
            resultMessage = state.getWaitMessage(planId);
        } catch (Exception e) {
            logPasswordRenewFailure(userName, e);
            cancelPlan(cs, planId);
            resultMessage = e.getMessage();
        }
        return new PasswordRenewResult(false, resultMessage);
    }

    /**
     * Log password renewal failure.
     */
    private void logPasswordRenewFailure(String userName, Exception e) {
        if (e == null) {
            return;
        }
        logMsg(Level.SEVERE, "Attempting to change password of user " +
                userName + " failed: " + e);
    }

    private void cancelPlan(CommandServiceAPI cs, int planId) {
        if (cs == null || planId == 0) {
            return;
        }

        try {
            cs.cancelPlan(planId);
        } catch (Exception e) {
            logCancelPlanFailure(planId, e);
        }
    }

    private void logCancelPlanFailure(int planId, Exception e) {
        if (planId != 0 && e != null) {
            return;
        }
        logMsg(Level.INFO, "Attempts to cancel plan of renew password " +
            planId + " failed: " + (e != null ? e.getMessage() : "null message"));
    }

    /**
     * Log a message, if a logger is available.
     */
    private void logMsg(Level level, String msg) {
        if (adminService != null) {
            adminService.getLogger().log(level, msg);
        }
    }
}
