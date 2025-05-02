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

package oracle.kv.impl.security.login;

import java.rmi.RemoteException;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVSecurityException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.ProxyCredentials;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

/**
 * An implementation of the UserLogin interface.
 */

@SecureAPI
public class UserLoginImpl extends VersionedRemoteImpl implements UserLogin {

    /* The object that manages user logins */
    private final UserLoginHandler loginHandler;

    /* The fault handler for this process */
    private final ProcessFaultHandler faultHandler;

    private volatile Logger logger;

    public UserLoginImpl(ProcessFaultHandler faultHandler,
                         UserLoginHandler loginHandler,
                         Logger logger) {

        this.loginHandler = loginHandler;
        this.faultHandler = faultHandler;
        this.logger = logger;
    }

    /**
     * Log a user into the database.
     * @see UserLogin#login
     */
    @Override
    @PublicMethod
    public LoginResult login(final LoginCredentials creds,
                             final short serialVersion)
        throws AuthenticationFailureException, RemoteException {
        return login(creds, getClientHost());
     }

    /*
     * Log a user into the database. This function is used on server side only.
     */
    public LoginResult login(final LoginCredentials creds,
                             String clientHostName)
        throws AuthenticationFailureException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginResult>() {

                @Override
                public LoginResult execute() {

                    try {
                        return loginHandler.login(creds, clientHostName);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
     }

    /**
     * Log a user into the database after renewing his expired password.
     * @see UserLogin#login
     */
    @Override
    @PublicMethod
    public LoginResult renewPasswordLogin(final PasswordCredentials oldCreds,
                                          final char[] newPassword,
                                          final short serialVersion)
        throws AuthenticationFailureException, RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginResult>() {

                @Override
                public LoginResult execute() {

                    try {
                        return loginHandler.renewPasswordLogin(
                            oldCreds, newPassword, getClientHost());
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
     }

    /**
     * Log another user into the database.
     * @see UserLogin#login
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public LoginResult proxyLogin(final ProxyCredentials creds,
                                  final AuthContext authContext,
                                  final short serialVersion)
        throws AuthenticationFailureException, SessionAccessException,
               RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginResult>() {

                @Override
                public LoginResult execute() {
                    try {
                        return loginHandler.proxyLogin(
                            creds, authContext.getClientHost());
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
     }

    /**
     * Request that a login token be replaced with a new token that has a later
     * expiration.
     * @see UserLogin#requestSessionExtension
     */
    @Override
    @PublicMethod
    public LoginToken requestSessionExtension(final LoginToken loginToken,
                                              final short serialVersion)
        throws SessionAccessException, RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<LoginToken>() {

                @Override
                public LoginToken execute() {
                    try {
                        return loginHandler.requestSessionExtension(loginToken);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    } catch (SessionAccessException sae) {
                        throw sae;
                    }
                }
            });
    }

    /**
     * Check an existing LoginToken for validity.
     * @see UserLogin#validateLoginToken
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public Subject validateLoginToken(final LoginToken loginToken,
                                      final AuthContext authCtx,
                                      final short serialVersion)
        throws SessionAccessException, RemoteException {

        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<Subject>() {

                @Override
                public Subject execute() {
                    try {
                        return loginHandler.validateLoginToken(loginToken);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    }
                }
            });
    }

    /**
     * Log out the login token.
     * @see UserLogin#logout
     */
    @Override
    @PublicMethod
    public void logout(final LoginToken loginToken, final short serialVersion)
        throws AuthenticationRequiredException, SessionAccessException,
               RemoteException {

        faultHandler.execute(
            new ProcessFaultHandler.SimpleProcedure() {

                @Override
                public void execute() {
                    try {
                        loginHandler.logout(loginToken);
                    } catch (KVSecurityException kvse) {
                        throw new ClientAccessException(kvse);
                    } catch (SessionAccessException sae) {
                        throw sae;
                    }
                }
            });
    }

    private String getClientHost() {
        final String clientHost =
            AsyncVersionedRemoteDialogResponder.getClientHost();
        if (clientHost == null) {
            logger.severe("getClientHost failed");
        }
        return clientHost;
    }

    /**
     * Reset logger for user login.
     */
    public void resetLogger(Logger newLogger) {
        this.logger = newLogger;
    }
}
