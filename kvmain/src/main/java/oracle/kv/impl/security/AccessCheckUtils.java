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

package oracle.kv.impl.security;

import java.util.Arrays;
import java.util.List;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.impl.util.server.LoggerUtils.KVAuditInfo;
import oracle.kv.impl.util.server.LoggerUtils.SecurityLevel;

/**
 * Utilities for checking privilege levels when accessing resources like plan
 * and table via command services in a secure store.
 */
public class AccessCheckUtils {

    /**
     * A common implementation of KVStore secure resource access context.  This
     * helps to define and choose different privileges in permission checking
     * for resource owners and non-owners respectively.
     */
    private abstract static class ResourceContext implements OperationContext {

        final Ownable resource;

        ResourceContext(Ownable resource, String resourceType) {
            if (resource == null) {
                throw new IllegalCommandException(
                    resourceType + " to be checked doesn't exist");
            }
            this.resource = resource;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            if (currentUserOwnsResource(resource)) {
                return privilegesForOwner();
            }
            return privilegesForNonOwner();
        }

        /**
         * Returns a list of operation privileges for the owner of the
         * resource.
         */
        abstract List<? extends KVStorePrivilege> privilegesForOwner();

        /**
         * Returns a list of operation privileges for the users other the owner
         * of the resource.
         */
        abstract List<? extends KVStorePrivilege> privilegesForNonOwner();
    }

    /**
     * Defines the required privileges for plan related operations. The
     * required privileges depends on whether the current user is the owner
     * of the plan.
     */
    private abstract static class PlanContext extends ResourceContext {
        private final String ctxDescription;

        /**
         * Constructs a OperationContext of plan operation.
         *
         * @param plan plan to be operated
         * @param desc description of the operation
         */
        PlanContext(Plan plan, String opDesc) {
            super(plan, "Plan");

            ctxDescription = String.format(
                "%s, Plan Name: %s, Plan Id: %d, Owner: %s",
                opDesc, plan.getName(), plan.getId(),
                (plan.getOwner() == null) ? "" : plan.getOwner());
        }

        @Override
        public String describe() {
            return ctxDescription;
        }
    }

    /**
     * Defines the privileges of plan controlling operations.  If the current
     * user is not the owner of the plan, SYSOPER is needed. Otherwise, the
     * privileges required by the specific plan will be checked.
     */
    public static class PlanOperationContext extends PlanContext {

        public PlanOperationContext(Plan plan, String desc) {
            super(plan, desc);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesForOwner() {
            final Plan plan = (Plan) resource;
            return plan.getRequiredPrivileges();
        }

        @Override
        List<? extends KVStorePrivilege> privilegesForNonOwner() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * Defines the privileges of plan access.  If the current user is not the
     * owner of the plan, SYSVIEW is needed. Otherwise, USRVIEW will be checked.
     */
    public static class PlanAccessContext extends PlanContext {

        public PlanAccessContext(Plan plan, String desc) {
            super(plan, desc);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesForOwner() {
            return SystemPrivilege.usrviewPrivList;
        }

        @Override
        List<? extends KVStorePrivilege> privilegesForNonOwner() {
            return SystemPrivilege.sysviewPrivList;
        }
    }

    /**
     * Defines the privileges of table specific operations.  If the current
     * user is the table owner, only USERVIEW is needed. Otherwise, the
     * explicitly-defined table privileges will be checked.
     */
    public static class TableContext extends ResourceContext {

        private final List<KVStorePrivilege> privsToCheck;
        private final String ctxDescription;

        public TableContext(String opDesc,
                            TableImpl table,
                            KVStorePrivilege... privsToCheck) {
            this(opDesc, table, Arrays.asList(privsToCheck));
        }

        public TableContext(String opDesc,
                            TableImpl table,
                            List<KVStorePrivilege> privsToCheck) {
            super(table, "Table");

            this.privsToCheck = privsToCheck;
            this.ctxDescription = String.format(
                "%s, Table name: %s, Table Id: %d, Owner: %s",
                opDesc, table.getFullName(), table.getId(),
                (table.getOwner() == null) ? "" : table.getOwner());
        }

        @Override
        public String describe() {
            return ctxDescription;
        }

        @Override
        List<? extends KVStorePrivilege> privilegesForOwner() {
            return SystemPrivilege.usrviewPrivList;
        }

        @Override
        List<? extends KVStorePrivilege> privilegesForNonOwner() {
            return privsToCheck;
        }
    }

    /**
     * Checks the permission upon the specified operation context in command
     * services resides in admin.
     *
     * @param opCtx operation context
     * @throws SessionAccessException if there is an internal security error
     * @throws ClientAccessException if the a security exception is generated by
     * the requesting client
     */
    public static void checkPermission(AdminService aservice,
                                       final OperationContext opCtx)
        throws SessionAccessException, ClientAccessException {

        final ExecutionContext exeCtx = ExecutionContext.getCurrent();
        if (exeCtx == null) {
            /*
             * A null execution context indicates an insecure store, or the
             * operation is directly called by internal KVStore components via
             * a reference to this object. The only case of the latter is in
             * Admin WebService, which will be deprecated in future. Don't
             * check in these cases.
             */
            return;
        }

        final AccessChecker accessChecker =
            aservice.getAdminSecurity().getAccessChecker();
        if (accessChecker != null) {
            try {
                accessChecker.checkAccess(exeCtx, opCtx);
            } catch (KVSecurityException kvse) {
                throw new ClientAccessException(kvse);
            }
        }
    }

    /**
     * Checks if current user is the owner of the specified resource. False
     * will be returned in one of following cases:
     * <p>
     * 1. Current user cannot be identified, or <br>
     * 2. Resource owner is null, or <br>
     * 3. Current user is NOT the resource owner. <br>
     * <p>
     * Otherwise, true will be returned.
     */
    public static boolean currentUserOwnsResource(Ownable resource) {
        final ResourceOwner currentUser = SecurityUtils.currentUserAsOwner();
        final ResourceOwner owner = resource.getOwner();
        return (currentUser != null) && (currentUser.equals(owner));
    }

    /**
     * Logs security error specified by a KVSecurityException under the current
     * execution context in the form of audit format.
     *
     * @param kvse KVSecurityException
     * @param opDesc description of operation
     * @param logger RateLimitingLogger to eliminate excessive log message
     */
    public static void logSecurityError(KVSecurityException kvse,
                                        String opDesc,
                                        RateLimitingLogger<String> logger) {
        final ExecutionContext execCtx = ExecutionContext.getCurrent();
        if (execCtx != null) {
            logSecurityError(kvse.getMessage(), opDesc, execCtx, logger);
        }
    }

    /**
     * Logs security error under the specified execution context in the form of
     * audit format.
     *
     * @param msg error message
     * @param opDesc description of operation
     * @param execCtx execution context
     * @param logger RateLimitingLogger to eliminate excessive log message
     */
    public static void logSecurityError(String msg,
                                        String opDesc,
                                        ExecutionContext execCtx,
                                        RateLimitingLogger<String> logger) {
        if (logger.getInternalLogger() != null) {
            String authHost = "";
            String userName = "";
            if (execCtx.requestorContext() != null &&
                execCtx.requestorContext().getClientHost() != null) {
                authHost = execCtx.requestorContext().getClientHost();
            }
            final KVStoreUserPrincipal user =
                ExecutionContext.getSubjectUserPrincipal(
                    execCtx.requestorSubject());
            if (user != null) {
                userName = user.getName();
            }
            logger.log(opDesc,
                       SecurityLevel.SEC_WARNING,
                       KVAuditInfo.failure(userName, execCtx.requestorHost(),
                                           authHost, opDesc, msg));
        }
    }
}
