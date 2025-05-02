/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;

/**
 * Scaffold implementation of user login.
 */

public class ScaffoldUserVerifier implements UserVerifier {

    private final String username;
    private char[] password;
    private long createTime;
    private long pwdLifeTime;

    public ScaffoldUserVerifier(String username,
                                char[] password) {
        this.username = username;
        this.password = password;
        this.createTime = System.currentTimeMillis();
        this.pwdLifeTime = 0 /* unlimited lifetime */;

    }

    public ScaffoldUserVerifier(String username,
                                char[] password,
                                long pwdLifeTime) {
        this(username, password);
        this.pwdLifeTime = pwdLifeTime;
    }

    @Override
    public Subject verifyUser(LoginCredentials creds,
                              UserLoginCallbackHandler handler)
        throws PasswordExpiredException {

        if (creds instanceof PasswordCredentials) {
            final PasswordCredentials pwCreds = (PasswordCredentials) creds;

            if (!username.equals(pwCreds.getUsername()) ||
                !Arrays.equals(pwCreds.getPassword(), password)) {
                return null;
            }

            if (isPasswordExpire()) {
                throw new PasswordExpiredException(String.format(
                    "The password of %s has expired, it is required to " +
                    "change the password.", username)); 
            }

        } else if (creds instanceof ProxyCredentials) {
            if (!username.equals(creds.getUsername())) {
                return null;
            }
        } else {
            return null;
        }

        return makeAdminSubject();
    }

    private boolean isPasswordExpire() {
        if (pwdLifeTime == 0) {
            return false;
        }
        return pwdLifeTime < 0 ?
               true :
               System.currentTimeMillis() >= (createTime + pwdLifeTime);
    }

    @Override
    public Subject verifyUser(Subject subj)
        throws AuthenticationRequiredException {

        final KVStoreUserPrincipal userPrinc =
            ExecutionContext.getSubjectUserPrincipal(subj);

        if (userPrinc == null) {
            throw new AuthenticationRequiredException(
                "Invalid subject", true /* isReturnSignal */);
        }

        if (username.equals(userPrinc.getName())) {
            return subj;
        }

        throw new AuthenticationRequiredException(
            "Not a valid user", true /* isReturnSignal */);
    }

    public void renewPassword(String userName, char[] newPassword) {
        if (userName.equals(username)) {
            password = newPassword;
            pwdLifeTime = 0;
        }
    }

    private Subject makeAdminSubject() {
        final Set<Principal> adminPrincipals = new HashSet<Principal>();
        adminPrincipals.add(KVStoreRolePrincipal.PUBLIC);
        adminPrincipals.add(new KVStoreUserPrincipal(username));
        final Set<Object> publicCreds = new HashSet<Object>();
        final Set<Object> privateCreds = new HashSet<Object>();
        return new Subject(true, adminPrincipals, publicCreds, privateCreds);
    }
}
