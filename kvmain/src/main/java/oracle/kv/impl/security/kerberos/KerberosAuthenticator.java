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

package oracle.kv.impl.security.kerberos;

import java.util.concurrent.Callable;
import java.util.logging.Level;

import javax.security.auth.login.LoginException;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.Authenticator;
import oracle.kv.impl.security.kerberos.KerberosConfig.StoreKrbConfiguration;
import oracle.kv.impl.security.login.KerberosInternalCredentials;
import oracle.kv.impl.security.login.KerberosLoginResult;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.UserLoginCallbackHandler;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.LoggingCallback;
import oracle.kv.impl.security.login.UserLoginCallbackHandler.LoginResultCallback;
import oracle.kv.impl.util.server.LoggerUtils;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

/**
 * Authenticator implemented with Kerberos authentication mechanism
 */
public class KerberosAuthenticator implements Authenticator {

    /* GSSManager used to create context */
    private volatile GSSManager gssManager;

    /* Login configuration used in JAAS login and GSS-API authentication */
    private StoreKrbConfiguration conf;

    KerberosAuthenticator(SecurityParams sp) {
        this.conf = new StoreKrbConfiguration(sp);
    }

    @Override
    public boolean authenticate(LoginCredentials creds,
                                UserLoginCallbackHandler handler) {

        if (handler == null) {
            throw new IllegalArgumentException("Kerberos authentication " +
                "requires callback handler to be specified");
        }

        if (gssManager == null && !initialize(handler)) {
            return false;
        }
        final LoggingCallback logging = new LoggingCallback();

        if (!(creds instanceof KerberosInternalCredentials)) {
            handler.handle(logging.setLevel(Level.INFO).setMessage(
                "KrbAuthenticator: Not Kerberos credentials, type is " +
                creds.getClass()));
            return false;
        }

        final KerberosInternalCredentials internalCreds =
            (KerberosInternalCredentials)creds;

        try {
            final KerberosLoginResult krbLoginResult =
                KerberosContext.getCurrentContext().runWithContext(
                    new GSSKerberosAuthenticate(internalCreds));

            if (krbLoginResult == null) {
                return false;
            }

            /*
             * Add Kerberos login result into callback handler,
             * in order to pass accept context token back to clients for
             * accomplishing possible mutual authentication.
             */
             if (krbLoginResult.getMutualAuthToken() != null) {
                 handler.handle(new LoginResultCallback() {
                     @Override
                     public LoginResult getLoginResult() {
                         return krbLoginResult;
                     }
                 });
             }
        } catch (Exception e) {
            String failure = "KRBAuthenticator: " + creds.getUsername() +
                " authentication failed " + LoggerUtils.getStackTrace(e);
            handler.handle(logging.setLevel(Level.INFO).setMessage(failure));
            return false;
        }
        return true;
    }

    @Override
    public void resetAuthenticator() {
        gssManager = null;
        KerberosContext.resetContext();
    }

    private boolean initialize(UserLoginCallbackHandler handler) {
        /*
         * Make Kerberos context to use given configuration for
         * subsequent operations
         */
        KerberosContext.setConfiguration(conf);

        /*
         * Initialize GSSManager with current Kerberos context. If no context
         * available, create new Kerberos context with specified configuration
         */
        try {
            gssManager = KerberosContext.getCurrentContext().runWithContext(
                () -> GSSManager.getInstance());
            return true;
        } catch (LoginException le) {
            handler.handle(new LoggingCallback(Level.WARNING,
               "KRBAuthenticator: Kerberos Service login failed, " +
               le.getMessage()));
            return false;
        } catch (Exception e) {
            handler.handle(new LoggingCallback(Level.WARNING,
                "Failed to initialize Kerberos authenticator: " +
                e.getMessage()));
            return false;
        }
    }

    /**
     * Action that use GSS-API with Kerberos to authenticate given credentials.
     */
    private class GSSKerberosAuthenticate
            implements Callable<KerberosLoginResult> {

        private KerberosInternalCredentials internalCreds;

        GSSKerberosAuthenticate(KerberosInternalCredentials creds) {
            this.internalCreds = creds;
        }

        @Override
        public KerberosLoginResult call() throws Exception {
            GSSCredential gssCreds = null;
            GSSContext gssContext = null;

            if (gssManager == null) {
                throw new IllegalStateException("Failed to locate GSSManager");
            }

            try {
                final GSSName serverName = gssManager.createName(
                    conf.getPrincipal(), KerberosConfig.getKrbPrincNameType());

                gssCreds = gssManager.createCredential(
                    serverName,
                    GSSCredential.INDEFINITE_LIFETIME,
                    KerberosConfig.getKerberosMethOid(),
                    GSSCredential.ACCEPT_ONLY);
                gssContext = gssManager.createContext(gssCreds);
                final byte[] initToken = internalCreds.getInitToken();

                if (initToken == null) {
                    throw new IllegalArgumentException(
                         "Kerberos context init token is not valid");
                }
                final byte[] acceptToken =
                    gssContext.acceptSecContext(initToken, 0, initToken.length);

                if (!gssContext.isEstablished()) {
                    throw new IllegalStateException(
                        "Kerberos context is not established");
                }
                return new KerberosLoginResult(acceptToken);
            } finally {
                if (gssContext != null) {
                    gssContext.dispose();
                }

                if (gssCreds != null) {
                    gssCreds.dispose();
                }
            }
        }
    }
}
