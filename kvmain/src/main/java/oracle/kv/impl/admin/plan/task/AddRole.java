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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.security.KVBuiltInRoleResolver;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Add a user-defined role
 */
public class AddRole extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private final String roleName;

    public AddRole(SecurityMetadataPlan plan, String roleName) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();
        this.roleName = roleName;

        if (secMd != null && secMd.getRole(roleName) == null) {
            /*
             * Do not allow define role with duplicate name of system
             * built-in roles and privileges
             */
            if (KVBuiltInRoleResolver.resolveRole(roleName) != null) {
                throw new IllegalCommandException(
                    "Role with name " + roleName +
                    " is system-builtin role.");
            }
            try {
                KVStorePrivilegeLabel.valueOf(roleName.toUpperCase());
                throw new IllegalCommandException(
                    "Name of " + roleName +
                    " already exists as a privilege");
            } catch (IllegalArgumentException iae) {
                /* Normal case, the name should not be a priv label name */
            }
        }
    }

    @Override
    protected SecurityMetadata createMetadata() {
            final String storeName =
                    getPlan().getAdmin().getParams().getGlobalParams().
                    getKVStoreName();
            return new SecurityMetadata(storeName);
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata md,
                                              Transaction txn) {

        if (md.getRole(roleName) == null) {
            final RoleInstance newRole = new RoleInstance(roleName);
            md.addRole(newRole);
            getPlan().getAdmin().saveMetadata(md, txn);
        }
        return md;
    }

    /**
     * Returns true if this AddRole will end up creating the same role.
     * Checks that roleName are the same.
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

        AddRole other = (AddRole) t;
        return roleName.equalsIgnoreCase(other.roleName);
    }
}
