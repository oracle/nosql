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

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;

/**
 * Interface for login verification.
 */

public interface UserVerifier {

    /**
     * Check the credentials passed in to see if they are valid.  If they
     * are, return a Subject that represents the user.
     *
     * @param creds the login credentials
     * @param handler the user login callback handler, the underlying
     * password authenticator does not need to use this handler, for operations
     * only need password authenticator like renew password can set null
     * as the value of this parameter
     * @return the Subject representing the user, or null if the credentials
     * are not valid
     * @throws PasswordExpiredException if the password has expired and needs
     * to be renewed
     */
    Subject verifyUser(LoginCredentials creds, UserLoginCallbackHandler handler)
        throws PasswordExpiredException;

    /**
     * Check that the user, if any, represented by the specified Subject is
     * valid.  If the Subject has no associated user principal, it is
     * trivially valid.
     *
     * @param subj a Subject representing a user
     * @return a non-null Subject representing the user if the input Subject
     *    still has a valid identity, or null otherwise
     */
    Subject verifyUser(Subject subj);
}
