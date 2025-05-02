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
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Grant roles to user.
 */
public class GrantRoles extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private final String grantee;
    private final Set<String> roles;

    public GrantRoles(SecurityMetadataPlan plan,
                      String grantee,
                      Set<String> roles) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();

        this.grantee = grantee;
        this.roles = roles;

        /* Return null if grantee does not exist */
        if ((secMd == null) || (secMd.getUser(grantee) == null)) {
            throw new IllegalCommandException(
                "User with name " + grantee + " does not exist in store");
        }
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
                                              Transaction txn) {
        /* Return null if grantee does not exist */
        if (secMd.getUser(grantee) == null) {
            return null;
        }
        final KVStoreUser newCopy = secMd.getUser(grantee).clone();
        secMd.updateUser(newCopy.getElementId(), newCopy.grantRoles(roles));
        getPlan().getAdmin().saveMetadata(secMd, txn);
        return secMd;
    }

    /**
     * Returns true if this GrantRoles will end up granting the same
     * roles to the same user. Checks that grantee and role set
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

        GrantRoles other = (GrantRoles) t;
        if (!grantee.equalsIgnoreCase(other.grantee)) {
            return false;
        }

        return roles.equals(other.roles);
    }
}
