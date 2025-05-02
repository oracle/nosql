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

import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;

/**
 * Interface for user authentication.
 */
public interface Authenticator {

    /**
     * Check the login credentials passed in to see if they are valid.
     *
     * @param loginCreds the login credentials
     * @param handler the user login callback handler
     * @return true if login credential is valid
     */
    public boolean authenticate(LoginCredentials loginCreds,
                                UserLoginCallbackHandler handler);

    /**
     * Reset state of authenticator. This method is aim to be called when
     * this type of authenticator is disabled. The authenticator implementations
     * may maintain some authentication information or states. Calling this
     * method is to ensure the authenticator re-enabled won't contains obsolete
     * information or state.
     */
    public void resetAuthenticator();
}
