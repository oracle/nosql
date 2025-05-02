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

package oracle.kv.impl.security;

import java.util.logging.Level;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.security.metadata.KVStoreUser;

public abstract class PasswordAuthenticator implements Authenticator {

    @Override
    public boolean authenticate(LoginCredentials creds,
                                UserLoginCallbackHandler handler) {
        if (!(creds instanceof PasswordCredentials)) {
            logMessage(Level.INFO, "Not password credentials, " +
                "credentials type is " + creds.getClass());
            return false;
        }

        final PasswordCredentials pwCreds = (PasswordCredentials) creds;
        final String userName = pwCreds.getUsername();
        final KVStoreUser user = loadUserFromStore(userName);

        if (user == null || !user.verifyPassword(pwCreds.getPassword())) {
            logMessage(Level.INFO, "User password credentials are not valid");
            return false;
        }

        if (user.isPasswordExpired()) {
            logMessage(Level.INFO, "User password credentials are expired");
            throw new PasswordExpiredException(String.format(
                "The password of %s has expired, it is required to " +
                "change the password.", userName));
        }
        return true;
    }

    @Override
    public void resetAuthenticator() {
        throw new UnsupportedOperationException(
            "Password authenticator cannot be reset");
    }

    /**
     * Load KVStoreUser instance of given user name from security metadata.
     */
    public abstract KVStoreUser loadUserFromStore(String userName);

    public abstract void logMessage(Level level, String message);
}
