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

package oracle.kv.impl.admin.plan;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.plan.task.AddExternalUser;
import oracle.kv.impl.admin.plan.task.AddRole;
import oracle.kv.impl.admin.plan.task.AddUser;
import oracle.kv.impl.admin.plan.task.ChangeUser;
import oracle.kv.impl.admin.plan.task.GrantNamespacePrivileges;
import oracle.kv.impl.admin.plan.task.GrantPrivileges;
import oracle.kv.impl.admin.plan.task.GrantRoles;
import oracle.kv.impl.admin.plan.task.GrantRolesToRole;
import oracle.kv.impl.admin.plan.task.NewSecurityMDChange;
import oracle.kv.impl.admin.plan.task.RemoveRole;
import oracle.kv.impl.admin.plan.task.RevokeNamespacePrivileges;
import oracle.kv.impl.admin.plan.task.RevokePrivileges;
import oracle.kv.impl.admin.plan.task.RevokeRoles;
import oracle.kv.impl.admin.plan.task.RevokeRolesFromRole;
import oracle.kv.impl.admin.plan.task.UpdateMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilege.PrivilegeType;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.PasswordHash;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.PasswordHashDigest;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.AdminId;

/**
 * Plan class representing all security metadata operations
 */
public class SecurityMetadataPlan extends MetadataPlan<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private static final SecureRandom random = new SecureRandom();

    public SecurityMetadataPlan(String planName, Planner planner) {
        super(planName, planner, false);
        final AdminServiceParams params = getAdmin().getParams();
        final SecurityParams securityParams = params.getSecurityParams();

        if (!securityParams.isSecure()) {
            throw new IllegalCommandException("Cannot execute " + planName +
                    " plan. " + planName +
                    " plan requires a secure store to be performed " +
                    "successfully.");
        }
    }

    /*
     * Ensure operator does not drop itself
     */
    private static void ensureNotSelfDrop(final String droppedUserName) {
        final KVStoreUserPrincipal currentUserPrincipal =
                KVStoreUserPrincipal.getCurrentUser();
        if (currentUserPrincipal == null) {
            throw new IllegalCommandException(
                "Could not identify current user");
        }
        if (droppedUserName.equals(currentUserPrincipal.getName())) {
            throw new IllegalCommandException(
                "A current online user cannot drop itself");
        }
    }

    @Override
    protected MetadataType getMetadataType() {
        return MetadataType.SECURITY;
    }

    @Override
    protected Class<SecurityMetadata> getMetadataClass() {
        return SecurityMetadata.class;
    }

    @Override
    public String getDefaultName() {
        return "Change SecurityMetadata";
    }

    @Override
    protected void acquireLocks() throws PlanLocksHeldException {
        /*
         * Use the elasticity lock to coordinate the concurrent execution of
         * multiple SecurityMetadataPlans since they may read/update the
         * security metadata simultaneously. Also, the update of security
         * metadata will miss for some RepNodes if happens during topology
         * elasticity operation. Synchronize on the elasticity lock can help
         * prevent this.
         *
         * TODO: need to implement a lock only for security metadata plan?
         */
        planner.lockElasticity(getId(), getName());
    }

    /**
     * Get a PasswordHashDigest instance with default hash algorithm, hash
     * bytes, and iterations
     *
     * @param plainPassword the plain password
     * @return a PasswordHashDigest containing the hashed password and hashing
     * information
     */
    public PasswordHashDigest
        makeDefaultHashDigest(final char[] plainPassword) {

        /* TODO: fetch the parameter from global store configuration */
        final byte[] saltValue =
            PasswordHash.generateSalt(random, PasswordHash.SUGG_SALT_BYTES);
        return PasswordHashDigest.getHashDigest(PasswordHash.SUGG_ALGO,
                                                PasswordHash.SUGG_HASH_ITERS,
                                                PasswordHash.SUGG_SALT_BYTES,
                                                saltValue, plainPassword);
    }


    /**
     * Add security metadata change notification tasks.
     */
    static void addNewMDChangeTasks(Admin admin, AbstractPlan plan) {
        final Parameters parameters = admin.getCurrentParameters();

        for (AdminId adminId : parameters.getAdminIds()) {
            plan.addTask(new NewSecurityMDChange(plan, adminId));
        }
    }

    public static SecurityMetadataPlan
        createCreateUserPlan(String planName,
                             Planner planner,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword,
                             Long pwdLifetime) {
        final String subPlanName =
                (planName != null) ? planName : "Create User";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new AddUser(plan, userName, isEnabled, isAdmin,
                                 plainPassword, pwdLifetime));
        return plan;
    }

    public static SecurityMetadataPlan
        createCreateExternalUserPlan(String planName,
                                     Planner planner,
                                     String userName,
                                     boolean isEnabled,
                                     boolean isAdmin) {
        final String subPlanName =
            (planName != null) ? planName : "Create External User";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new AddExternalUser(plan, userName, isEnabled, isAdmin));
        return plan;
    }

    public static SecurityMetadataPlan
        createChangeUserPlan(String planName,
                             Planner planner,
                             String userName,
                             Boolean isEnabled,
                             char[] plainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword,
                             Long pwdLifetime) {
        final SecurityMetadataPlan plan =
                                    new ChangeUserPlan((planName != null) ?
                                            planName : "Change User", planner);

        plan.addTask(new ChangeUser(plan, userName, isEnabled, plainPassword,
                                    retainPassword, clearRetainedPassword,
                                    pwdLifetime));
        return plan;
    }

    public static AbstractPlan createDropUserPlan(String planName,
                                                  Planner planner,
                                                  String userName,
                                                  boolean cascade) {
        /* Checking for secure store first before self drop */
        AbstractPlan plan = createDropUserPlanV2(planName, planner,
                userName, cascade);
        ensureNotSelfDrop(userName);
        return plan;
    }

    private static RemoveUserPlanV2 createDropUserPlanV2(String planName,
                                                         Planner planner,
                                                         String userName,
                                                         boolean cascade) {
        final String subPlanName = (planName != null) ? planName :
                "Drop User" + ((cascade) ? " Cascade" : "");
        final RemoveUserPlanV2 plan = new RemoveUserPlanV2(
            subPlanName, planner, userName, cascade);
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting roles to a user.
     */
    public static SecurityMetadataPlan
        createGrantPlan(String planName,
                        Planner planner,
                        String grantee,
                        Set<String> roles) {
        final String subPlanName =
            (planName != null) ? planName : "Grant Roles";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);
        plan.addTask(new GrantRoles(plan, grantee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting roles to a role.
     */
    public static SecurityMetadataPlan
        createGrantRolesToRolePlan(String planName,
                                   Planner planner,
                                   String grantee,
                                   Set<String> roles) {
        final String subPlanName =
            (planName != null) ? planName : "Grant Roles (To Role)";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);
        plan.addTask(new GrantRolesToRole(plan, grantee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting table privileges to a role.
     */
    public static SecurityMetadataPlan
        createGrantPrivsPlan(String planName,
                             Planner planner,
                             String roleName,
                             String namespace,
                             String tableName,
                             Set<String> privs) {
        final String subPlanName =
            (planName != null) ? planName : "Grant Privileges";
        final PrivilegePlan plan =
             new PrivilegePlan(subPlanName, planner, privs,
                               (tableName == null));
        plan.addTask(new GrantPrivileges(plan, roleName, namespace,
                                         tableName, privs));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking privileges from a user.
     */
    public static SecurityMetadataPlan
        createRevokePlan(String planName,
                         Planner planner,
                         String revokee,
                         Set<String> roles) {
        final String subPlanName =
            (planName != null) ? planName : "Revoke Roles";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);

        plan.addTask(new RevokeRoles(plan, revokee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking privileges from a role or a user.
     */
    public static SecurityMetadataPlan
        createRevokeRolesFromRolePlan(String planName,
                                      Planner planner,
                                      String revokee,
                                      Set<String> roles) {
        final String subPlanName =
            (planName != null) ? planName : "Revoke Roles (From Role)";
        final RolePlan plan = new RolePlan(subPlanName, planner, roles);

        plan.addTask(
            new RevokeRolesFromRole(plan, revokee, roles));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking table privileges from a role.
     */
    public static SecurityMetadataPlan
        createRevokePrivsPlan(String planName,
                              Planner planner,
                              String roleName,
                              String namespace,
                              String tableName,
                              Set<String> privs) {
        final String subPlanName =
            (planName != null) ? planName : "Revoke Privileges";
        final PrivilegePlan plan =
            new PrivilegePlan(subPlanName, planner, privs, (tableName == null));
        plan.addTask(new RevokePrivileges(plan, roleName, namespace, tableName,
            privs));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for granting namespace privileges to a role.
     */
    public static SecurityMetadataPlan
        createGrantNamespacePrivsPlan(String planName,
                                      Planner planner,
                                      String roleName,
                                      String namespace,
                                      Set<String> privs) {
        final String subPlanName =
            (planName != null) ? planName : "Grant Namespace Privileges";
        final PrivilegePlan plan =
            new PrivilegePlan(subPlanName, planner, privs,
                false /* never a system priv */);
        plan.addTask(new GrantNamespacePrivileges(plan, roleName, namespace,
            privs));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    /**
     * Gets a plan for revoking namespace privileges from a role.
     */
    public static SecurityMetadataPlan
        createRevokeNamespacePrivsPlan(String planName,
                                       Planner planner,
                                       String roleName,
                                       String namespace,
                                       Set<String> privs) {
        final String subPlanName =
            (planName != null) ? planName : "Revoke Namespace Privileges";
        final PrivilegePlan plan =
            new PrivilegePlan(subPlanName, planner, privs,
                false /* never a system priv */);

        final Set<KVStorePrivilege> privileges = new HashSet<KVStorePrivilege>();
        GrantNamespacePrivileges.parseToPrivileges(privs, privileges,
                namespace);

        plan.addTask(new RevokeNamespacePrivileges(plan, roleName,
            namespace, privileges));
        addNewMDChangeTasks(planner.getAdmin(), plan);
        return plan;
    }

    public static SecurityMetadataPlan createCreateRolePlan(String planName,
                                                            Planner planner,
                                                            String roleName) {
        final String subPlanName =
            (planName != null) ? planName : "Create Role";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new AddRole(plan, roleName));
        return plan;
    }

    public static SecurityMetadataPlan createDropRolePlan(String planName,
                                                          Planner planner,
                                                          String roleName) {
        final String subPlanName =
            (planName != null) ? planName : "Drop Role";
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan(subPlanName, planner);
        plan.addTask(new RemoveRole(plan, roleName));
        addNewMDChangeTasks(planner.getAdmin(), plan);

        /*
         * Revoke this role from all users have been granted.
         */
        final SecurityMetadata secMd = plan.getMetadata();
        for (final KVStoreUser user : secMd.getAllUsers()) {

            if (user.getGrantedRoles().contains(roleName.toLowerCase())) {
                plan.addTask(new RevokeRoles(plan, user.getName(),
                                             Collections.singleton(roleName)));
                addNewMDChangeTasks(planner.getAdmin(), plan);
            }
        }

        for (final RoleInstance role : secMd.getAllRoles()) {
            if (role.getGrantedRoles().contains(
                    RoleInstance.getNormalizedName(roleName))) {
                plan.addTask(new RevokeRolesFromRole(
                    plan, role.name(), Collections.singleton(roleName)));
                addNewMDChangeTasks(planner.getAdmin(), plan);
            }
        }
        return plan;
    }

    public static SecurityMetadataPlan createBroadcastSecurityMDPlan
        (Planner planner) {
        final SecurityMetadataPlan plan =
            new SecurityMetadataPlan("Broadcast Security MD", planner);

        plan.addTask(new UpdateMetadata<>(plan));
        return plan;
    }

    @Override
    public List<? extends KVStorePrivilege> getRequiredPrivileges() {
        /* Requires SYSOPER */
        return SystemPrivilege.sysoperPrivList;
    }

    /* ChangeUserPlan needs to override the getRequiredPrivilege */
    private static class ChangeUserPlan extends SecurityMetadataPlan {
        private static final long serialVersionUID = 1L;

        private ChangeUserPlan(String planName, Planner planner) {
            super(planName, planner);
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /* Requires USRVIEW at a minimum */
            return SystemPrivilege.usrviewPrivList;
        }
    }

    /**
     * Represents the grant and revoke operation for roles. Needs to ensure the
     * basic authorization version is met.
     */
    public static class RolePlan extends SecurityMetadataPlan {
        private static final long serialVersionUID = 1L;

        public RolePlan(String planName,
                        Planner planner,
                        Set<String> roles) {
            super((planName != null) ? planName : "Role", planner);
            validateRoleNames(roles);
        }

        /**
         * Check if given role names are valid and assignable system
         * predefined roles, or existing user-defined roles.
         */
        private void validateRoleNames(Set<String> roleNames) {
            final RoleResolver roleResolver =
                planner.getAdmin().getRoleResolver();

            /*
             * Normally, the role resolver should not be null, unless the
             * security is not enabled
             */
            if (roleResolver == null) {
                throw new IllegalCommandException(
                    "Cannot grant or revoke roles. Please make sure the " +
                    "security feature is enabled");
            }

            for (String roleName : roleNames) {
                final RoleInstance role = roleResolver.resolve(roleName);
                if (role == null) {
                    throw new IllegalCommandException(
                        "Role with name : " + roleName + " does not exist");
                } else if (!role.assignable()) {
                    throw new IllegalCommandException(
                        "Role " + roleName + " cannot be granted or revoked");
                }
            }
        }
    }

    /**
     * Privilege plans have different permission requirement from generic
     * SecurityMetadataPlan.
     */
    public static class PrivilegePlan extends SecurityMetadataPlan {
        private static final long serialVersionUID = 1L;

        private static final String ALLPRIVS = "ALL";

        /* If the operation is for system privileges only */
        private final boolean isSystemPrivsOp;

        private PrivilegePlan(String planName,
                              Planner planner,
                              Set<String> privs,
                              boolean isSystemPrivsOp) {
            super((planName != null) ? planName : "Privilege", planner);
            this.isSystemPrivsOp = isSystemPrivsOp;
            validatePrivileges(privs);
        }

        /**
         * Check if given privilege names are valid.
         */
        private void validatePrivileges(Set<String> privNames) {
            for (String privName : privNames) {
                if (!ALLPRIVS.equalsIgnoreCase(privName)) {
                    try {
                        final KVStorePrivilegeLabel privLabel =
                            KVStorePrivilegeLabel.valueOf(
                                privName.toUpperCase(java.util.Locale.ENGLISH));

                        if (!checkPrivConsistency(privLabel)) {
                            throw new IllegalCommandException(
                                "Could not use " + privName + " with type of " +
                                privLabel.getType() + " in this operation " +
                                "which needs privilege type of " +
                                (isSystemPrivsOp ?
                                 "SYSTEM" :
                                 "TABLE or NAMESPACE"));
                        }
                    } catch (IllegalArgumentException iae) {
                        throw new IllegalCommandException(
                            privName + " is not valid privilege name");
                    }
                }
            }
        }

        /*
         * A convenient method to check whether a privilege matches the
         * required type of this operation.
         */
        private boolean checkPrivConsistency(KVStorePrivilegeLabel privLabel) {
            if (privLabel.getType().equals(PrivilegeType.SYSTEM)) {
                return isSystemPrivsOp;
            }
            return !isSystemPrivsOp;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            /*
             * If it is an operation on system privileges, SYSOPER is required.
             * Otherwise only USRVIEW is checked, and nuanced check will be
             * deferred to tasks.
             */
            return isSystemPrivsOp ?
                   SystemPrivilege.sysoperPrivList :
                   SystemPrivilege.usrviewPrivList;
        }
    }
}
