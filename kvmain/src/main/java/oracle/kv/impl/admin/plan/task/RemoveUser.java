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
import oracle.kv.impl.security.metadata.SecurityMetadata;

import com.sleepycat.je.Transaction;

/**
 * Remove a user
 */
public class RemoveUser extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    protected final String userName;

    public static RemoveUser newInstance(SecurityMetadataPlan plan,
                                         String userName) {
        final RemoveUser removeUser = new RemoveUser(plan, userName);
        removeUser.guardLastSysadminUser();
        return removeUser;
    }

    protected RemoveUser(SecurityMetadataPlan plan, String userName) {
        super(plan);
        this.userName = userName;
    }
    
    /**
     * Guard against removing the last enabled admin user.
     */
    protected void guardLastSysadminUser() {
        final SecurityMetadata secMd = getMetadata();
        if (secMd == null) {
            return;
        }

        if (secMd.isLastSysadminUser(userName)) {
            throw new IllegalCommandException(
                "Cannot drop the last enabled admin user " + userName);
        }
    }

    @Override
    protected SecurityMetadata updateMetadata(SecurityMetadata secMd,
                                              Transaction txn) {
        if (secMd.getUser(userName) != null) {
            guardLastSysadminUser();

            /* The user exists, so remove the entry from the MD */
            secMd.removeUser(secMd.getUser(userName).getElementId());
            getPlan().getAdmin().saveMetadata(secMd, txn);
        }

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

        RemoveUser other = (RemoveUser) t;
        return userName.equals(other.userName);
    }
}
