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
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Revoke roles from user.
 */
public class RevokeRoles extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private final String userName;
    private final Set<String> roles;

    public RevokeRoles(SecurityMetadataPlan plan,
                       String userName,
                       Set<String> roles) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();
        this.userName = userName;
        this.roles = roles;

        /* Return null if target user does not exist */
        if ((secMd == null) || (secMd.getUser(userName) == null)) {
            throw new IllegalCommandException(
                "User with name " + userName + " does not exist in store");
        }

        if (roles.contains(RoleInstance.SYSADMIN_NAME)) {
            guardLastSystemUser(secMd);
        }
    }

    /**
     * Guard against revoking sysadmin role from the last enabled admin user.
     */
    private void guardLastSystemUser(final SecurityMetadata secMd) {
        if (secMd == null) {
            return;
        }

        if (secMd.isLastSysadminUser(userName)) {

            throw new IllegalCommandException(
                "Cannot revoke sysadmin role from " +
                "the last enabled admin user " + userName);
        }
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
                                              Transaction txn) {

        /* Return null user does not exist */
        if (secMd.getUser(userName) != null) {
            final KVStoreUser newCopy = secMd.getUser(userName).clone();
            secMd.updateUser(newCopy.getElementId(), newCopy.revokeRoles(roles));
            getPlan().getAdmin().saveMetadata(secMd, txn);
        }
        return secMd;
    }

    /**
     * Returns true if this RevokeRoles will end up revoking the same
     * roles to the same user. Checks that user name and role set
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

        RevokeRoles other = (RevokeRoles) t;
        if (!userName.equals(other.userName)) {
            return false;
        }

        return roles.equals(other.roles);
    }
}
