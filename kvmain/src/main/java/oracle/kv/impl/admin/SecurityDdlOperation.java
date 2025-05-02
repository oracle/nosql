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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.admin.DdlHandler.DdlOperation;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleInstance.RoleDescription;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.metadata.SecurityMetadata;

/**
 * This class represents the security Ddl operations and their execution.
 * SecurityDDLOperations are generated after parsing a security ddl statement.
 */
public abstract class SecurityDdlOperation implements DdlOperation {
    final String opName;

    SecurityDdlOperation(String opName) {
        this.opName = opName;
    }

    @Override
    public OperationContext getOperationCtx() {
        return new OperationContext() {
            @Override
            public String describe() {
                return opName;
            }
            @Override
            public List<? extends KVStorePrivilege> getRequiredPrivileges() {
                return privilegesToCheck();
            }
        };
    }

    @Override
    public void perform(DdlHandler ddlHandler) {
        try {
            execute(ddlHandler);
        } catch (IllegalCommandException ice) {
            ddlHandler.operationFails(opName + " failed for: " +
                    ice.getMessage());
        }
    }

    /**
     * Executes the concrete security ddl operation
     *
     * @param ddlHandler ddl handler
     */
    abstract void execute(DdlHandler ddlHandler);

    /**
     * Returns the required privileges in permission check
     */
    abstract List<? extends KVStorePrivilege> privilegesToCheck();

    /**
     * CREATE USER operation
     */
    public static class CreateUser extends SecurityDdlOperation {

        private final String userName;
        private final boolean isEnabled;
        private final boolean isAdmin;
        private final char[] plainPassword;
        private final Long pwdLifetimeInMillis;

        public CreateUser(String userName,
                          boolean isEnabled,
                          boolean isAdmin,
                          char[] plainPassword,
                          Long pwdLifeTimeInMillis) {
            super("CreateUser");

            this.userName = userName;
            this.isAdmin = isAdmin;
            this.isEnabled = isEnabled;
            this.plainPassword =
                Arrays.copyOf(plainPassword, plainPassword.length);
            this.pwdLifetimeInMillis = pwdLifeTimeInMillis;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createCreateUserPlan(opName, userName, isEnabled,
                                     isAdmin, plainPassword,
                                     pwdLifetimeInMillis);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * CREATE EXTERNAL USER operation
     */
    public static class CreateExternalUser extends SecurityDdlOperation {

        private final String userName;
        private final boolean isEnabled;
        private final boolean isAdmin;

        public CreateExternalUser(String userName,
                                  boolean isEnabled,
                                  boolean isAdmin) {
            super("CreateExternalUser");

            this.userName = userName;
            this.isAdmin = isAdmin;
            this.isEnabled = isEnabled;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createCreateExternalUserPlan(opName, userName, isEnabled,
                    isAdmin);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * ALTER USER operation
     */
    public static class AlterUser extends SecurityDdlOperation {

        private final String userName;
        private final Boolean isEnabled;
        private final char[] plainPassword;
        private final boolean retainPassword;
        private final boolean clearRetainedPassword;
        private final Long pwdLifetimeInMillis;

        public AlterUser(String userName,
                         Boolean isEnabled,
                         char[] plainPassword,
                         boolean retainPassword,
                         boolean clearRetainedPassword,
                         Long pwdLifeTimeInMillis) {
            super("AlterUser");

            this.userName = userName;
            this.isEnabled = isEnabled;
            this.plainPassword =
                plainPassword == null ?
                null :
                Arrays.copyOf(plainPassword, plainPassword.length);
            this.retainPassword = retainPassword;
            this.clearRetainedPassword = clearRetainedPassword;
            this.pwdLifetimeInMillis = pwdLifeTimeInMillis;
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.usrviewPrivList;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createChangeUserPlan(opName, userName, isEnabled,
                                     plainPassword, retainPassword,
                                     clearRetainedPassword,
                                     pwdLifetimeInMillis);
            ddlHandler.approveAndExecute(planId);
        }
    }

    /**
     * DROP USER operation
     */
    public static class DropUser extends SecurityDdlOperation {

        private final String userName;
        private final boolean cascade;

        public DropUser(String userName,
                        boolean cascade) {
            super("DropUser");

            this.userName = userName;
            this.cascade = cascade;
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createDropUserPlan(opName, userName, cascade);
            ddlHandler.approveAndExecute(planId);
        }
    }

    /**
     * CREATE ROLE operation
     */
    public static class CreateRole extends SecurityDdlOperation {
        private final String roleName;
        public CreateRole(String roleName) {
            super("CreateRole");
            this.roleName = roleName;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createCreateRolePlan(opName, roleName);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * DROP ROLE operation
     */
    public static class DropRole extends SecurityDdlOperation {
        private final String roleName;
        public DropRole(String roleName) {
            super("DropRole");
            this.roleName = roleName;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createDropRolePlan(opName, roleName);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * GRANT ROLE TO USER operation
     */
    public static class GrantRoles extends SecurityDdlOperation {
        final String grantee;
        final Set<String> roleNames;

        public GrantRoles(String grantee, String[] roleNames) {
            super("GrantRoles");

            this.grantee = grantee;
            this.roleNames = new HashSet<String>();
            Collections.addAll(this.roleNames, roleNames);
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createGrantPlan(opName, grantee, roleNames);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * GRANT ROLE TO ROLE operation
     */
    public static class GrantRolesToRole extends GrantRoles {

        public GrantRolesToRole(String grantee, String[] roleNames) {
            super(grantee, roleNames);
        }

        @Override
        public void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createGrantRolesToRolePlan(opName, grantee, roleNames);
            ddlHandler.approveAndExecute(planId);
        }
    }

    /**
     * GRANT table PRIVILEGE operation
     */
    public static class GrantPrivileges extends SecurityDdlOperation {
        private final String roleName;
        private final String tableName;
        private final String namespace;
        private final Set<String> privs;

        public GrantPrivileges(String roleName,
                               String namespace,
                               String tableName,
                               Set<String> privs) {
            super("GrantPrivileges");

            this.roleName = roleName;
            this.tableName = tableName;
            this.namespace = namespace;
            this.privs = privs;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createGrantPrivilegePlan(opName, roleName,
                                         namespace,
                                         tableName,
                                         privs);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            /*
             * Required privileges are consistent with that in
             * SecurityMetadataPlan.PrivilegePlan.
             */
            return tableName == null ?
                   SystemPrivilege.sysoperPrivList :
                   SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * REVOKE ROLES FROM USER operation
     */
    public static class RevokeRoles extends SecurityDdlOperation {
        final String revokee;
        final Set<String> roleNames;

        public RevokeRoles(String revokee, String[] roleNames) {
            super("RevokeRoles");

            this.revokee = revokee;
            this.roleNames = new HashSet<String>();
            Collections.addAll(this.roleNames, roleNames);
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createRevokePlan(opName, revokee, roleNames);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.sysoperPrivList;
        }
    }

    /**
     * REVOKE ROLES FROM ROLE operation
     */
    public static class RevokeRolesFromRole extends RevokeRoles {

        public RevokeRolesFromRole(String revokee, String[] roleNames) {
            super(revokee, roleNames);
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createRevokeRolesFromRolePlan(opName, revokee, roleNames);
            ddlHandler.approveAndExecute(planId);
        }
    }

    /**
     * REVOKE table PRIVILEGE operation
     */
    public static class RevokePrivileges extends SecurityDdlOperation {
        private final String roleName;
        private final String tableName;
        private final String namespace;
        private final Set<String> privs;

        public RevokePrivileges(String roleName,
                                String namespace,
                                String tableName,
                                Set<String> privs) {
            super("RevokePrivileges");

            this.roleName = roleName;
            this.tableName = tableName;
            this.namespace = namespace;
            this.privs = privs;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createRevokePrivilegePlan(opName, roleName,
                                          namespace,
                                          tableName,
                                          privs);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            /*
             * Required privileges are consistent with that in
             * SecurityMetadataPlan.PrivilegePlan.
             */
            return tableName == null ?
                   SystemPrivilege.sysoperPrivList :
                   SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * GRANT namespace PRIVILEGE operation
     */
    public static class GrantNamespacePrivilegesDdlOp extends
        SecurityDdlOperation {
        private final String roleName;
        private final String namespace;
        private final Set<String> privs;

        public GrantNamespacePrivilegesDdlOp(String roleName,
            String namespace,
            Set<String> privs) {
            super("GrantNamespacePrivileges");

            this.roleName = roleName;
            /* null namespace represents the sysdefault namespace */
            this.namespace = namespace;
            this.privs = privs;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createGrantNamespacePrivilegePlan(opName, roleName, namespace,
                    privs);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            /*
             * Required privileges are consistent with that in
             * SecurityMetadataPlan.PrivilegePlan.
             */
            return SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * REVOKE namespace PRIVILEGE operation
     */
    public static class RevokeNamespacePrivilegesDdlOp extends
        SecurityDdlOperation {
        private final String roleName;
        private final String namespace;
        private final Set<String> privs;

        public RevokeNamespacePrivilegesDdlOp(String roleName,
            String namespace,
            Set<String> privs) {
            super("RevokeNamespacePrivileges");

            this.roleName = roleName;
            /* null namespace represents the sysdefault namespace */
            this.namespace = namespace;
            this.privs = privs;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final int planId = ddlHandler.getAdmin().getPlanner().
                createRevokeNamespacePrivilegePlan(opName, roleName, namespace,
                    privs);
            ddlHandler.approveAndExecute(planId);
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            /*
             * Required privileges are consistent with that in
             * SecurityMetadataPlan.PrivilegePlan.
             */
            return SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * SHOW USER operation
     */
    public static class ShowUser extends SecurityDdlOperation {
        private final String userName;
        private final boolean asJson;

        public ShowUser(String userName, boolean asJson) {
            super("ShowUser");
            this.userName = userName;
            this.asJson = asJson;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final Admin admin = ddlHandler.getAdmin();
            final Map<String, UserDescription> userDescMap = getUserInfo(admin);

            final String userInfoStr;
            if (userDescMap == null || userDescMap.isEmpty()) {
                userInfoStr = asJson ? "{}" : "";
            } else {
                if (userName != null && userDescMap.get(userName) == null) {
                    throw new IllegalCommandException(
                        "User with name of " + userName + " not found");
                }
                userInfoStr = formatUserInfo(userDescMap);
            }
            ddlHandler.setResultString(userInfoStr);
            ddlHandler.operationSucceeds();
        }

        private Map<String, UserDescription> getUserInfo(Admin admin) {
            final SecurityMetadata md =
                admin.getMetadata(SecurityMetadata.class,
                                  MetadataType.SECURITY);
            if (md == null) {
                return null;
            }

            /*
             * If security is not enabled, or current user has SYSVIEW
             * privilege, returns all users' description.
             */
            final ExecutionContext currentCtx =
                ExecutionContext.getCurrent();
            if (currentCtx == null ||
                currentCtx.hasPrivilege(SystemPrivilege.SYSVIEW)) {
                return md.getUsersDescription();
            }
            return md.getCurrentUserDescription();
        }

        /**
         * Builds a user info string based on the user description map
         *
         * @param userDescMap user description map
         * @return formatted string
         */
        private String
            formatUserInfo(Map<String, UserDescription> userDescMap) {

            final StringBuilder sb = new StringBuilder();
            if (asJson) {
                if (userName != null) {
                    sb.append(userDescMap.get(userName).detailsAsJSON());
                } else {
                    boolean first = true;
                    sb.append("{\"users\":[");
                    for (final UserDescription desc : userDescMap.values()) {
                        if (!first) {
                            sb.append(",");
                        }
                        first = false;
                        sb.append(desc.briefAsJSON());
                    }
                    sb.append("]}");
                }
            } else {
                if (userName != null) {
                    sb.append(userDescMap.get(userName).details());
                } else {
                    for (final UserDescription desc : userDescMap.values()) {
                        sb.append("user:");
                        sb.append(desc.brief());
                        sb.append("\n");
                    }
                }
            }
            return sb.toString();
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * SHOW ROLE operation
     */
    public static class ShowRole extends SecurityDdlOperation {
        private final String roleName;
        private final boolean asJson;

        public ShowRole(String roleName, boolean asJson) {
            super("ShowRole");
            this.roleName = roleName;
            this.asJson = asJson;
        }

        @Override
        void execute(DdlHandler ddlHandler) {
            final Admin admin = ddlHandler.getAdmin();
            final Map<String, RoleDescription> roleDescMap = getRoleInfo(admin);

            final String roleInfoStr;
            if (roleDescMap == null || roleDescMap.isEmpty()) {
                roleInfoStr = asJson ? "{}" : "";
            } else {
                if (roleName != null && roleDescMap.get(
                        RoleInstance.getNormalizedName(roleName)) == null) {
                    throw new IllegalCommandException(
                        "Role with name of " + roleName + " not found");
                }
                roleInfoStr = formatRoleInfo(roleDescMap);
            }
            ddlHandler.setResultString(roleInfoStr);
            ddlHandler.operationSucceeds();
        }

        private Map<String, RoleDescription> getRoleInfo(Admin admin) {

            final SecurityMetadata md =
                admin.getMetadata(SecurityMetadata.class,
                                  MetadataType.SECURITY);
            if (md == null) {
                return SecurityMetadata.getBuiltInRoleInfo();
            }
            return md.getRolesDescription();
        }

        /**
         * Builds a role info string based on the role description map
         *
         * @param roleDescMap role description map
         * @return formatted string
         */
        private String
            formatRoleInfo(Map<String, RoleDescription> roleDescMap) {

            final StringBuilder sb = new StringBuilder();
            if (asJson) {
                if (roleName != null) {
                    sb.append(roleDescMap.get(
                        RoleInstance.getNormalizedName(roleName)).
                        detailsAsJSON());
                } else {
                    boolean first = true;
                    sb.append("{\"roles\":[");
                    for (final RoleDescription desc : roleDescMap.values()) {
                        if (!first) {
                            sb.append(",");
                        }
                        first = false;
                        sb.append(desc.briefAsJSON());
                    }
                    sb.append("]}");
                }
            } else {
                if (roleName != null) {
                    sb.append(roleDescMap.get(
                        RoleInstance.getNormalizedName(roleName)).details());
                } else {
                    for (final RoleDescription desc : roleDescMap.values()) {
                        sb.append("role:");
                        sb.append(desc.brief());
                        sb.append("\n");
                    }
                }
            }
            return sb.toString();
        }

        @Override
        List<? extends KVStorePrivilege> privilegesToCheck() {
            return SystemPrivilege.usrviewPrivList;
        }
    }
}
