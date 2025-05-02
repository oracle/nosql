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

import java.util.Arrays;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.plan.SecurityMetadataPlan;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;

import com.sleepycat.je.Transaction;

/**
 * Add a user
 */
public class AddUser extends UpdateMetadata<SecurityMetadata> {

    private static final long serialVersionUID = 1L;

    private final String userName;
    private final boolean isEnabled;
    private final boolean isAdmin;
    private final char[] plainPassword;
    private final Long pwdLifeTime;

    public AddUser(SecurityMetadataPlan plan,
                   String userName,
                   boolean isEnabled,
                   boolean isAdmin,
                   char[] plainPassword,
                   Long pwdLifetime) {
        super(plan);

        final SecurityMetadata secMd = plan.getMetadata();

        Utils.ensureFirstAdminUser(secMd, isEnabled, isAdmin);

        this.userName = userName;
        this.isAdmin = isAdmin;
        this.isEnabled = isEnabled;
        this.plainPassword = Arrays.copyOf(plainPassword, plainPassword.length);
        this.pwdLifeTime = pwdLifetime;

        Utils.checkPreExistingUser(secMd, userName, isEnabled,
                                   isAdmin, plainPassword);

        Utils.checkCreateUserPwPolicies(plainPassword,
                                        plan.getAdmin(), userName);
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
        if (md.getUser(userName) == null) {
            /*
             * The user does not yet exist, so add the entry to the MD.
             */
            final KVStoreUser newUser = KVStoreUser.newInstance(userName);
            newUser.setEnabled(isEnabled).setAdmin(isAdmin).
                    setPassword(((SecurityMetadataPlan)getPlan()).
                    makeDefaultHashDigest(plainPassword));

            if (pwdLifeTime == null) {
                final GlobalParams params =
                    getPlan().getAdmin().getParams().getGlobalParams();
                final long duration = params.getPasswordDefaultLifeTime();
                newUser.setPasswordLifetime(
                    params.getPasswordDefaultLifeTimeUnit().
                        toMillis(duration));
            } else {
                newUser.setPasswordLifetime(pwdLifeTime);
            }
            md.addUser(newUser);
            getPlan().getAdmin().saveMetadata(md, txn);
        }

        /*
         * Wipe out the plain password setting to ensure it does not hang
         * around in in the Java VM memory space.
         */
        SecurityUtils.clearPassword(plainPassword);
        return md;
    }

    /**
     * Returns true if this AddUser will end up creating the same user.
     * Checks that userName, isEnabled, isAdmin, password and password lifetime
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

        AddUser other = (AddUser) t;
        if (!userName.equals(other.userName)) {
            return false;
        }

        if (isEnabled != other.isEnabled || isAdmin != other.isAdmin) {
            return false;
        }

        if (pwdLifeTime == null) {
            if (other.pwdLifeTime != null) {
                return false;
            }
        } else if (!pwdLifeTime.equals(other.pwdLifeTime)) {
            return false;
        }
        /* plain password should not be null */
        return Arrays.equals(plainPassword, other.plainPassword);
    }
}
