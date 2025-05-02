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

import static oracle.kv.KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_KRB_REALM_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY;
import static oracle.kv.KVSecurityConstants.JAAS_LOGIN_CONF_NAME;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.impl.security.kerberos.KerberosConfig.ClientKrbConfiguration;
import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.login.KerberosInternalCredentials;
import oracle.kv.impl.security.login.KerberosLoginResult;
import oracle.kv.impl.security.login.LoginResult;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.KVStoreLogin.PasswordCallbackHandler;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * Kerberos login helper class.<p>
 *
 * It provides abilities to make Kerberos login credentials and perform login
 * by using GSS-API.
 */
public class KerberosLoginHelper {

    /**
     * Login with specified Kerberos credentials.
     *
     * @param loginAPI user login API
     * @param creds    Kerberos credentials
     * @param host     target Login host name
     * @return login result, if requires mutual authentication, return
     * {@link KerberosLoginResult}.
     */
    public static LoginResult kerberosLogin(final UserLoginAPI loginAPI,
                                            final KerberosClientCreds creds,
                                            final String host) {

        final Subject subject = creds.getLoginSubject();
        final String servicePrincipal =
            creds.getKrbServicePrincipals().getPrincipal(host);
        if (servicePrincipal == null) {
            throw new AuthenticationFailureException(
                "Service principal of host " + host + " is not specified");
        }

        try {
            return KerberosContext.runWithContext(
                subject,
                new KerberosGSSLoginAction(
                    creds.getUsername(), servicePrincipal, loginAPI,
                    creds.getRequireMutualAuth()));
        } catch (AuthenticationFailureException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthenticationFailureException(e);
        }
    }

    /**
     * Build KerberosClientCreds according to the security properties.<p>
     *
     * When multiple mechanisms are set in properties, for example,
     * {@link KVSecurityConstants#AUTH_KRB_CCACHE_PROPERTY} and
     * {@link KVSecurityConstants#AUTH_KRB_KEYTAB_PROPERTY},
     * this method will retrieve a ticket or key in following preference order;
     * <ol>
     * <li> credentials cache
     * <li> keytab
     * <li> user password prompt if doNotPrompt is false
     * </ol>
     * Without setting credential cache and keytab property, this method will
     * attempt to retrieve ticket from default credential cache, default keytab
     * or user password prompt if doNotPrompt is false.<p>
     *
     * @param userName the name of the login user.
     * @param sp security configuration properties for making credentials.
     * @param pwdHandler password callback handler, if null do not prompt
     * password
     * @return the credentials for Kerberos authentication
     * @throws AuthenticationFailureException if failed to generate Kerberos
     *         login credentials.
     */
    public static KerberosClientCreds
        buildKerberosCreds(String userName,
                           Properties sp,
                           PasswordCallbackHandler pwdHandler)
        throws AuthenticationFailureException {

        if (userName == null || sp == null) {
            return null;
        }
        if (!KVStoreLogin.isKerberosMech(sp)) {
            return null;
        }
        Configuration clientLoginConfig = null;
        String loginConfEntry = sp.getProperty(JAAS_LOGIN_CONF_NAME);

        /* Check if user provide a valid JAAS configuration */
        if (!hasValidJAASConfiguration(sp)) {

            final String keytab = sp.getProperty(AUTH_KRB_KEYTAB_PROPERTY);
            final String ccache = sp.getProperty(AUTH_KRB_CCACHE_PROPERTY);
            checkFileExist(keytab);
            checkFileExist(ccache);

            /* Generate JAAS login configuration from security properties */
            clientLoginConfig = new ClientKrbConfiguration(
                userName,
                ccache, keytab, (pwdHandler == null) /* doNotPrompt */);
            loginConfEntry = ClientKrbConfiguration.CLIENT_KERBEROS_CONFIG;
        }

        final boolean requireMutualAuth = KVStoreLogin.checkBooleanField(
            sp.getProperty(AUTH_KRB_MUTUAL_PROPERTY), false /* default */);

        /* Get user principal full name */
        userName = getUserFullName(userName,
                                   sp.getProperty(AUTH_KRB_REALM_PROPERTY));

        Subject subject = KerberosContext.getCurrentSubject();
        if (subject == null) {
            subject = new Subject();
        }

        /*
         * If current subject contains valid TGT of specified user, skip the
         * JAAS login process that acquiring TGT from KDC for this user.
         */
        if (!hasValidTGT(subject, userName)) {
            try {
                /* JAAS login using specified configuration */
                final LoginContext loginContext = new LoginContext(
                    loginConfEntry, subject, pwdHandler, clientLoginConfig);
                loginContext.login();
            } catch (LoginException e) {
                throw new AuthenticationFailureException(e);
            }
        }

        if (!hasValidTGT(subject, userName)) {
            throw new AuthenticationFailureException("Building Kerberos " +
                "credential failed: cannot obtain TGT for " + userName);
        }

        /* Make service principals map if specified */
        final String hostPrincPair = sp.getProperty(AUTH_KRB_SERVICES_PROPERTY);

        return new KerberosClientCreds(userName,
                                       subject,
                                       hostPrincPair,
                                       requireMutualAuth);
    }

    /**
     * Return user principal full name with realm name, in case specified user
     * name is principal short name.
     */
    private static String getUserFullName(String userName, String realm) {

        /*
         * Found realm separator '@' means userName is a principal full
         * name, since Kerberos does not allow '@' in the principal name that
         * only can be use as separator of realm.
         */
        if (userName.contains(SecurityUtils.KRB_NAME_REALM_SEPARATOR_STR)) {
            return userName;
        }

        if (realm == null) {
            throw new IllegalArgumentException("Kerberos realm name must be " +
                "specified when using short principal name");
        }

        return userName + SecurityUtils.KRB_NAME_REALM_SEPARATOR_STR + realm;
    }

    /*
     * Check whether a specific file path exist
     */
    private static void checkFileExist(String path) {
        if (path != null && !new File(path).exists()) {
            throw new IllegalArgumentException("The specified path " + path +
                " does not exist");
        }
    }

    /*
     * Check if given subject contains the valid TGT of specified user.
     */
    private static boolean hasValidTGT(Subject subject, String userName) {
        for (final KerberosTicket ticket :
            subject.getPrivateCredentials(KerberosTicket.class)) {

            if (ticket.isCurrent()) {
                final KerberosPrincipal principal = ticket.getClient();
                if (principal.getName().equals(userName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /*
     * Check if users have already set the system property of
     * "java.security.auth.login.config" or set runtime configuration object
     * for NoSQL Kerberos login. To use JAAS login configuration, the property
     * JAAS_LOGIN_CONF_NAME must be specified by user. If login configuration
     * file set in system property or configuration object set in run time does
     * not contains the valid login entry user specified in JAAS_LOGIN_CONF_NAME
     * throw an IllegalArgumentException.
     */
    private static boolean hasValidJAASConfiguration(Properties sp) {

        /* Check if user specified JAAS_LOGIN_CONF_NAME, if not return false. */
        final String jaasLoginEntry = sp.getProperty(JAAS_LOGIN_CONF_NAME);
        if (jaasLoginEntry == null) {
            return false;
        }
        Configuration clientLoginConfig = null;

        /*
         * Load the configuration, this won't be null, even if users do not
         * set JAAS configuration file in system property. It also can acquires
         * the Configuration object users set in the runtime.
         */
        clientLoginConfig = Configuration.getConfiguration();

        /* Locate the entry having name of JAAS_LOGIN_CONF_NAME */
        final AppConfigurationEntry[] entries = clientLoginConfig.
            getAppConfigurationEntry(jaasLoginEntry);
        final AppConfigurationEntry entry = (entries != null) ?
            clientLoginConfig. getAppConfigurationEntry(jaasLoginEntry)[0] :
            null;
        if (entry == null) {
            throw new IllegalArgumentException(
                "Cannot find a JAAS configuration entry named \"" +
                jaasLoginEntry + "\" as specified for finding Kerberos " +
                "credentials by the " + JAAS_LOGIN_CONF_NAME +
                " security property");
        }

        /* Check if the entry is for Kerberos login module */
        if (!entry.getLoginModuleName().equals(
            KerberosConfig.getKrb5LoginModuleName())) {
            throw new IllegalArgumentException(
                "The value \"" + jaasLoginEntry + "\" was specified as" +
                " the name of the JAAS login configuration entry to use" +
                " for Kerberos logins, but the entry does not name a" +
                " Kerberos login module.");
        }


        /*
         * If a valid JAAS login configuration entry has been found, the
         * security properties user specify should not work, warn them these
         * should not be configured.
         */
        final String keytab = sp.getProperty(AUTH_KRB_KEYTAB_PROPERTY);
        final String ccache = sp.getProperty(AUTH_KRB_CCACHE_PROPERTY);
        if (keytab != null || ccache != null) {
            throw new IllegalArgumentException(
                "Specifying a keytab or credential cache in security" +
                " properties is not permitted while using JAAS configuration");
        }
        return true;
    }

    /*
     * Use GSS-API to perform Kerberos login for client.
     */
    private static class KerberosGSSLoginAction
        implements Callable<LoginResult> {

        private final String userName;
        private final String servicePrincipal;
        private final boolean mutualAuthentication;
        private final UserLoginAPI loginAPI;

        KerberosGSSLoginAction(String userName,
                               String service,
                               UserLoginAPI loginAPI,
                               boolean mutualAuthen) {
            this.userName = userName;
            this.servicePrincipal = service;
            this.loginAPI = loginAPI;
            this.mutualAuthentication = mutualAuthen;
        }

        @Override
        public LoginResult call() throws Exception {

            final GSSManager manager = GSSManager.getInstance();
            final GSSName serviceName = manager.createName(
                servicePrincipal, KerberosConfig.getKrbPrincNameType());
            GSSContext context = null;
            try {
                byte[] token = new byte[0];

                /* Use subject credentials to create the context */
                context = manager.createContext(
                    serviceName,
                    KerberosConfig.getKerberosMethOid(),
                    null, /* GSSCredential */
                    GSSContext.DEFAULT_LIFETIME);
                context.requestMutualAuth(mutualAuthentication);

                final byte[] ticketToken =
                    context.initSecContext(token, 0, token.length);
                final LoginResult result = loginAPI.login(
                    new KerberosInternalCredentials(userName, ticketToken));

                if (mutualAuthentication) {
                    if (!(result instanceof KerberosLoginResult)) {
                        throw new AuthenticationFailureException(
                            "Server authenticated to client failed");
                    }

                    final byte[] mutualAuthToken =
                       ((KerberosLoginResult)result).getMutualAuthToken();
                    context.initSecContext(
                        mutualAuthToken, 0, mutualAuthToken.length);
                }

                if (!context.isEstablished()) {
                    throw new AuthenticationFailureException(
                        "Kerberos login fail");
                }
                return result;
            } finally {
                if (context != null) {
                    context.dispose();
                }
            }
        }
    }
}
