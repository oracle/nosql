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

import java.util.Set;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.MultiMetadataPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

public class RevokeNamespacePrivileges extends
    UpdateMetadata<SecurityMetadata>  {

    private static final long serialVersionUID = 1L;
    private final AbstractPlan plan;
    private final String roleName;
    private final String namespace;
    private final Set<KVStorePrivilege> privileges;

    public RevokeNamespacePrivileges(AbstractPlan plan,
        String roleName, String namespace, Set<KVStorePrivilege>
        privileges) {

        super(null);

        if ( !(plan instanceof MultiMetadataPlan ||
               plan instanceof SecurityMetadataPlan) ) {
            throw new IllegalStateException("Not supported for plans of " +
                "class: " + plan.getClass());
        }

        this.plan = plan;
        this.roleName = roleName;
        this.namespace = namespace;
        this.privileges = privileges;

        /* check if namespace exists and permissions */
        final TableMetadata tableMd =
            getPlan().getAdmin().getTableMetadata();
        TableMetadata.NamespaceImpl nsImpl = tableMd.getNamespace(
                NameUtils.switchToInternalUse(namespace));
        if (nsImpl == null) {
            throw new IllegalCommandException(
                "Namespace '" + namespace + "' does not exist");
        }

        GrantNamespacePrivileges.checkPermission(nsImpl);
    }

    @Override
    protected SecurityMetadata getMetadata() {
        if ( plan instanceof MultiMetadataPlan) {
            return ((MultiMetadataPlan) plan)
                .getSecurityMetadata();
        }

        if ( plan instanceof SecurityMetadataPlan) {
            return ((SecurityMetadataPlan) plan)
                .getMetadata();
        }

        throw new IllegalStateException("Not supported for plans of " +
            "class: " + plan.getClass());
    }

    @Override
    protected AbstractPlan getPlan() {
        return this.plan;
    }

    @Override
    protected SecurityMetadata getMetadata(Transaction txn) {
        if ( plan instanceof MultiMetadataPlan ) {
            return ((MultiMetadataPlan) plan)
                .getSecurityMetadata(txn);
        }

        if ( plan instanceof SecurityMetadataPlan) {
            return ((SecurityMetadataPlan) plan)
                .getMetadata(txn);
        }

        throw new IllegalStateException("Not supported for plans of " +
            "class: " + plan.getClass());
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockNamespace(planner, getPlan(), namespace);
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
        Transaction txn) {
        if (secMd.getRole(roleName) != null) {
            final RoleInstance roleCopy = secMd.getRole(roleName).clone();
            secMd.updateRole(roleCopy.getElementId(),
                roleCopy.revokePrivileges(privileges));
            getPlan().getAdmin().saveMetadata(secMd, txn);
        }
        return secMd;
    }

    /**
     * Returns true if this RevokeNamespacePrivileges will end up revoking the
     * same privileges from the same role. Checks that roleName and privilege
     * set are the same.
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

        RevokeNamespacePrivileges other = (RevokeNamespacePrivileges) t;
        if (!roleName.equalsIgnoreCase(other.roleName)) {
            return false;
        }

        return privileges.equals(other.privileges);
    }
}
