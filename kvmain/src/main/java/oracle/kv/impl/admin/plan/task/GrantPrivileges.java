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

import oracle.kv.impl.admin.plan.SecurityMetadataPlan.PrivilegePlan;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Grant privileges to user-defined role.
 */
public class GrantPrivileges extends PrivilegeTask {

    private static final long serialVersionUID = 1L;

    public GrantPrivileges(PrivilegePlan plan,
                           String roleName,
                           String namespace,
                           String tableName,
                           Set<String> privNames) {
        super(plan, roleName, namespace, tableName, privNames);
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

    /**
     * Returns true if this GrantPrivilegs will end up granting the same
     * privileges to the same role. Checks that roleName and privilege set
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

        GrantPrivileges other = (GrantPrivileges) t;
        if (!roleName.equalsIgnoreCase(other.roleName)) {
            return false;
        }

        return privileges.equals(other.privileges);
    }
}
