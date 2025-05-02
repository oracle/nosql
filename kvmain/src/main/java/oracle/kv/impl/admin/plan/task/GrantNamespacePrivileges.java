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

import java.util.HashSet;
import java.util.Set;

import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.NamespacePrivilege;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Grant privileges to user-defined role.
 */
public class GrantNamespacePrivileges extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;
    final String roleName;
    final String namespace;
    final Set<KVStorePrivilege> privileges = new HashSet<KVStorePrivilege>();

    public GrantNamespacePrivileges(SecurityMetadataPlan.PrivilegePlan plan,
        String roleName,
        String namespace,
        Set<String> privNames) {
        super(plan);
        this.roleName = roleName;
        this.namespace = namespace;

        final TableMetadata tableMd =
            getPlan().getAdmin().getTableMetadata();
        TableMetadata.NamespaceImpl nsImpl = tableMd.getNamespace(namespace);
        if (nsImpl == null) {
            throw new IllegalCommandException(
                "Namespace '" + namespace + "' does not exist");
        }

        checkPermission(nsImpl);
        parseToPrivileges(privNames, privileges, namespace);
    }

    /**
     * Check if current user has enough permission to operation given
     * namespaceImpl privilege granting and revocation.
     */
    static void checkPermission(TableMetadata.NamespaceImpl namespaceImpl) {
        final ExecutionContext execCtx = ExecutionContext.getCurrent();
        if (execCtx == null) {
            return;
        }

        if ( !AccessCheckUtils.currentUserOwnsResource(namespaceImpl) &&
            !execCtx.hasPrivilege(SystemPrivilege.SYSOPER)) {
            throw new ClientAccessException(
                new UnauthorizedException(
                    "Insufficient privilege granted to grant or revoke " +
                    "privilege on non-owned namespaces."));
        }
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockNamespace(planner, getPlan(), namespace);
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
        Transaction txn) {

        /* Return null if grantee does not exist */
        if (secMd.getRole(roleName) == null) {
            return null;
        }

        final RoleInstance roleCopy = secMd.getRole(roleName).clone();
        secMd.updateRole(roleCopy.getElementId(),
            roleCopy.grantPrivileges(privileges));
        getPlan().getAdmin().saveMetadata(secMd, txn);

        return secMd;
    }

    public static void parseToPrivileges(Set<String> privNames,
        Set<KVStorePrivilege> privileges, String namespace) {

        for (String privName : privNames) {
            if (PrivilegeTask.ALLPRIVS.equalsIgnoreCase(privName)) {
                privileges.addAll(NamespacePrivilege
                    .getAllNamespacePrivileges(namespace));
                return;
            }
            final KVStorePrivilegeLabel privLabel =
                KVStorePrivilegeLabel.valueOf(privName.toUpperCase());

            if (privLabel.equals(KVStorePrivilegeLabel.MODIFY_IN_NAMESPACE)) {
                privileges.add(NamespacePrivilege.get(
                    KVStorePrivilegeLabel.CREATE_TABLE_IN_NAMESPACE,
                    namespace));
                privileges.add(NamespacePrivilege.get(
                    KVStorePrivilegeLabel.DROP_TABLE_IN_NAMESPACE,
                    namespace));
                privileges.add(NamespacePrivilege.get(
                    KVStorePrivilegeLabel.EVOLVE_TABLE_IN_NAMESPACE,
                    namespace));
                privileges.add(NamespacePrivilege.get(
                    KVStorePrivilegeLabel.CREATE_INDEX_IN_NAMESPACE,
                    namespace));
                privileges.add(NamespacePrivilege.get(
                    KVStorePrivilegeLabel.DROP_INDEX_IN_NAMESPACE,
                    namespace));
            } else {
                privileges
                    .add(NamespacePrivilege.get(privLabel, namespace));
            }
        }
    }

    /**
     * Returns true if this GrantNamespacePrivileges will end up granting the
     * same privileges to the same role. Checks that roleName and privilege set
     * are the same.
     */
    @Override
    public boolean logicalCompare(Task t) {
        if (this == t) {
            return true;
        }

        if (t == null) {
            return false;
        }

        if (getClass() != t.getClass()) {
            return false;
        }

        GrantNamespacePrivileges other = (GrantNamespacePrivileges) t;
        if (!roleName.equalsIgnoreCase(other.roleName)) {
            return false;
        }

        return privileges.equals(other.privileges);
    }
}
