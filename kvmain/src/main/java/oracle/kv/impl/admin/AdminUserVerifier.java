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
package oracle.kv.impl.admin;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.security.Authenticator;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.PasswordAuthenticator;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * Provides logic for authentication of logins against a user database
 * within the admin service.
 */
public class AdminUserVerifier implements UserVerifier {

    /* The AdminService being supported */
    private final AdminService adminService;

    /* Default authenticator - password based */
    private final Authenticator defaultAuthenticator;

    /* External authenticators */
    private final Map<String, Authenticator> authenticators;

    /**
     * Construct an AdminUserVerifier supporting the provided AdminService
     * instance.
     */
    public AdminUserVerifier(AdminService aService) {
        this.adminService = aService;

        if (aService != null && aService.getAdminSecurity() != null) {
            this.authenticators =
                aService.getAdminSecurity().getAuthenticators();
        } else {
            this.authenticators = new HashMap<>();
        }
        this.defaultAuthenticator = new AdminPasswordAuthenticator();
    }

    /**
     * Verify that the login credentials are valid and return a subject that
     * identifies the user.
     */
    @Override
    public Subject verifyUser(LoginCredentials creds,
                              UserLoginCallbackHandler handler) {

        if (defaultAuthenticator.authenticate(creds, handler)) {
            return makeUserSubject(creds.getUsername());
        }

        if (adminService == null) {
            return null;
        }
        final GlobalParams gp = adminService.getParams().getGlobalParams();
        final String[] authMethods = gp.getUserExternalAuthMethods();
        for (String authMethod : authMethods) {
            final Authenticator authen = authenticators.get(authMethod);

            if (authen != null && authen.authenticate(creds, handler)) {

                /*
                 * If authenticator is supposed to generate user subject
                 * itself, return user subject from UserLoginCallbackHandler
                 * without consulting security metadata.
                 */
                if (handler.getUserSessionInfo() != null) {
                    return handler.getUserSessionInfo().getSubject();
                }
                return makeUserSubject(creds.getUsername());
            }
        }
        return null;
    }

    /**
     * Verify that the Subject is valid against underlying metadata.
     */
    @Override
    public Subject verifyUser(Subject subj) {

        final KVStoreUserPrincipal userPrinc =
            ExecutionContext.getSubjectUserPrincipal(subj);

        if (userPrinc == null) {
            /* Presumably an anonymous login - nothing to verify */
            return subj;
        }

        /*
         * Do not check if user exists in security metadata if given
         * subject is an IDCS OAuth one
         */
        final String userId = userPrinc.getUserId();
        if (userId != null &&
            userId.startsWith(SecurityUtils.IDCS_OAUTH_USER_ID_PREFIX)) {
            return subj;
        }

        final SecurityMetadata secMd = adminService.getAdmin().getMetadata(
            SecurityMetadata.class, MetadataType.SECURITY);

        if (secMd == null) {
            logMsg(Level.INFO,
                   "Unable to verify user with no security metadata available");
            return null;
        }

        final KVStoreUser user = secMd.getUser(userPrinc.getName());

        if (user == null || !user.isEnabled()) {
            logMsg(Level.INFO,
                   "User " + userPrinc.getName() + " is not valid");
            return null;
        }

        return subj;
    }

    /**
     * Report whether there is any user account data against which to
     * authenticate.
     */
    public boolean userDataExists() {
        final SecurityMetadata secMd =
            (adminService.getAdmin() != null) ?
            adminService.getAdmin().getMetadata(
                SecurityMetadata.class, MetadataType.SECURITY)
            : null;
        return (secMd != null) && (!secMd.getAllUsers().isEmpty());
    }

    /**
     * Log a message, if a logger is available.
     */
    private void logMsg(Level level, String msg) {
        if (adminService != null) {
            adminService.getLogger().log(level, msg);
        }
    }

    private Subject makeUserSubject(String userName) {
        final SecurityMetadata secMd = adminService.getAdmin().getMetadata(
            SecurityMetadata.class, MetadataType.SECURITY);

        if (secMd == null) {
            return null;
        }
        final KVStoreUser user = secMd.getUser(userName);

        if (user == null || !user.isEnabled()) {
            logMsg(Level.INFO, "User " + userName + " is not valid");
            return null;
        }
        return user.makeKVSubject();
    }

    private class AdminPasswordAuthenticator extends PasswordAuthenticator {

        @Override
        public KVStoreUser loadUserFromStore(String userName) {
            final SecurityMetadata secMd = adminService.getAdmin().getMetadata(
                SecurityMetadata.class, MetadataType.SECURITY);

            if (secMd == null) {
                return null;
            }
            return secMd.getUser(userName);
        }

        @Override
        public void logMessage(Level level, String msg) {
            logMsg(level, msg);
        }
    }
}
