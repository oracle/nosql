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
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Remove a user-defined role.
 */
public class RemoveRole extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private final String roleName;

    public RemoveRole(SecurityMetadataPlan plan,
                      String roleName) {
        super(plan);

        /* Check whether the specified role is the system build-in role */
        if (KVBuiltInRoleResolver.resolveRole(roleName) != null) {
            throw new IllegalCommandException(
                "Cannot drop a system built-in role");
        }
        this.roleName = roleName;
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
                                              Transaction txn) {

        if (secMd.getRole(roleName) != null) {

            /* The user-defined role exists, so remove the entry from the MD */
            secMd.removeRole(secMd.getRole(roleName).getElementId());
            getPlan().getAdmin().saveMetadata(secMd, txn);
        }
        /* return MD in case it's a re-execute */
        return secMd;
    }

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

        RemoveRole other = (RemoveRole) t;
        return roleName.equalsIgnoreCase(other.roleName);
    }
}
