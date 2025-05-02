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

package oracle.kv.impl.security.util;

import static oracle.kv.KVSecurityConstants.AUTH_EXT_MECH_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_KRB_CCACHE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_PWDFILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_USERNAME_PROPERTY;
import static oracle.kv.KVSecurityConstants.AUTH_WALLET_PROPERTY;
import static oracle.kv.KVSecurityConstants.CMD_PASSWORD_NOPROMPT_PROPERTY;
import static oracle.kv.KVSecurityConstants.KRB_MECH_NAME;
import static oracle.kv.KVSecurityConstants.SECURITY_FILE_PROPERTY;
import static oracle.kv.KVSecurityConstants.TRANSPORT_PROPERTY;
import static oracle.kv.impl.security.ssl.SSLConfig.KEYSTORE_FILE;
import static oracle.kv.impl.security.ssl.SSLConfig.TRUSTSTORE_FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreException;
import oracle.kv.KerberosCredentials;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.shell.ShellInputReader;

/**
 * Helper class for loading user login information from the security file, and
 * to facilitate the login behavior of client utilities like CommandShell. Note
 * the class is NOT thread-safe.
 * <p>
 * The security file may contain any of the properties defined in
 * {@link SSLConfig} as well as the following authentication properties:
 * <ul>
 * <li>oracle.kv.auth.username</li>
 * <li>oracle.kv.auth.wallet.dir</li>
 * <li>oracle.kv.auth.pwdfile.file</li>
 * <li>oracle.kv.auth.pwdfile.manager</li>
 * <li>oracle.kv.auth.external.mechanism</li>
 * <li>oracle.kv.auth.kerberos.services</li>
 * <li>oracle.kv.auth.kerberos.keytab</li>
 * <li>oracle.kv.auth.kerberos.config</li>
 * <li>oracle.kv.auth.kerberos.ccache</li>
 * <li>oracle.kv.auth.kerberos.mutualAuth</li>
 * </ul>
 * <p>
 * Note that if the oracle.kv.transport is set in the security file, we will
 * assume that the security file contains all necessary transport configuration
 * information provided by users.
 * TODO: the statement above is out-of-date
 */
public class KVStoreLogin {

    private static final String KERBEROS_LOGIN_HELPER_CLASS =
        "oracle.kv.impl.security.kerberos.KerberosLoginHelper";

    private String userName;
    private String securityFilePath;
    private Properties securityProps = null;
    private ShellInputReader reader = null;

    private static final Set<String> fileProperties = new HashSet<String>();
    static {
        fileProperties.add(SECURITY_FILE_PROPERTY);
        fileProperties.add(AUTH_WALLET_PROPERTY);
        fileProperties.add(AUTH_PWDFILE_PROPERTY);
        fileProperties.add(KEYSTORE_FILE);
        fileProperties.add(TRUSTSTORE_FILE);
        fileProperties.add(AUTH_KRB_KEYTAB_PROPERTY);
        fileProperties.add(AUTH_KRB_CCACHE_PROPERTY);
    }

    /**
     * Build a kvstore login without user and login information.
     */
    public KVStoreLogin() {
        this(null, null);
    }

    public KVStoreLogin(final String user, final String security) {
        userName = user;
        securityFilePath = security;
    }

    public String getUserName() {
        return userName;
    }

    public void updateLoginInfo(final String user, final String security) {
        this.userName = user;
        this.securityFilePath = security;
        loadSecurityProperties();
    }

    public Properties getSecurityProperties() {
        return securityProps;
    }

    /**
     * Read the settings of the security file into the inner properties. Note
     * that if the "oracle.kv.transport" is specified in the security file, all
     * transport config information is assumed to be contained in the file.
     *
     * @throws IllegalStateException if anything wrong happens while loading the
     * file
     */
    public void loadSecurityProperties() {
        /*
         * If security file is not set, try read it from system property of
         * oracle.kv.security
         */
        if (securityFilePath == null) {
            securityFilePath =
                System.getProperty(SECURITY_FILE_PROPERTY);
        }

        securityProps = createSecurityProperties(securityFilePath);

        /* If user is not set, try to read from security file */
        if (securityProps != null && userName == null) {
            userName = securityProps.getProperty(AUTH_USERNAME_PROPERTY);
        }

        if (securityFilePath != null && !foundTransportSettings()) {
            throw new IllegalArgumentException(
                "A security file was specified, but the file does not " +
                "provide the required transport setting, which will " +
                "cause user logins to fail");
        }
    }

    private boolean loadNoPasswordPromptProperty() {
        if (securityProps == null) {
            return false;
        }
        final String input =
            securityProps.getProperty(CMD_PASSWORD_NOPROMPT_PROPERTY);
       return checkBooleanField(input, false /* default */);
    }

    /**
     * We delay the instantiation of ShellInputReader as late as when it is
     * really needed, because a background task will stop if it tries to
     * instantiate the ShellInputReaders as described in [#23075]. This can
     * help alleviate the situation when a background task can read password
     * credentials from the security file.
     */
    private ShellInputReader getReader() {
        if (reader == null) {
            reader = ShellInputReader.getReader(System.in, System.out);
        }
        return reader;
    }

    /* For test */
    protected void setReader(final ShellInputReader reader) {
        this.reader = reader;
    }

    /**
     * Get login credentials for login. If the credential cannot be made by
     * using the provided information from the security file, the user name and
     * login password will be required to read from the shell input reader.
     *
     * @throws IOException if fails to get username or password from shell
     * input reader
     */
    public LoginCredentials makeShellLoginCredentials()
        throws IOException {
        final boolean noPasswdPrompt = loadNoPasswordPromptProperty();
        if (userName == null) {
            if (noPasswdPrompt) {
                throw new IllegalArgumentException(
                    "Must specify user name when password prompting is" +
                    " disabled");
            }
            userName = getReader().readLine("Login as:");
        }

        if (isKerberosMech()) {
            PasswordCallbackHandler pwdHandler = null;
            if (!noPasswdPrompt) {
                pwdHandler = new PasswordCallbackHandler(userName, getReader());
            }
            return buildKerberosCreds(userName, securityProps, pwdHandler);
        }

        char[] passwd = PasswordManager.retrievePassword(userName, securityProps);
        if (passwd == null) {
            if (noPasswdPrompt) {
                throw new AuthenticationFailureException(
                    "Invalid user name or password");
            }
            passwd = getReader().readPassword(userName + "'s password:");
        }

        return new PasswordCredentials(userName, passwd);
    }

    /**
     * Get login credentials according to internal security properties.
     */
    public LoginCredentials getLoginCredentials() {
        return makeLoginCredentials(securityProps);
    }

    public String getSecurityFilePath() {
        return securityFilePath;
    }

    /**
     * Check if the transport type is set to SSL or CLEAR. Both the settings
     * from system and security properties will be checked.
     */
    public boolean foundTransportSettings() {
        String transportType = securityProps == null ?
                               null :
                               securityProps.getProperty(TRANSPORT_PROPERTY);
        return (transportType != null &&
                (transportType.equals(SecurityParams.TRANS_TYPE_SSL) ||
                 transportType.equals(SecurityParams.TRANS_TYPE_CLEAR)));
    }

    /**
     * Check if the security properties loaded from file contain transport
     * settings by looking for the definition of "oracle.kv.tranport". In this
     * case, we assume the security properties contain all config information.
     */
    public boolean hasTransportSettings() {
        return securityProps != null &&
               securityProps.getProperty(TRANSPORT_PROPERTY) != null;
    }

    /**
     * Check if current login instance use kerberos authentication mechanism.
     */
    private boolean isKerberosMech() {
        return isKerberosMech(securityProps);
    }

    /**
     * Check if the security properties loaded from file contain external
     * mechanism setting that it will use the kerberos authentication.
     */
    public static boolean isKerberosMech(final Properties securityProps) {
        if (securityProps == null) {
            return false;
        }
        final String extAuthMech =
            securityProps.getProperty(AUTH_EXT_MECH_PROPERTY);
        if (extAuthMech != null) {
            if (extAuthMech.equalsIgnoreCase(KRB_MECH_NAME)) {
                return true;
            }
            throw new IllegalArgumentException(
                "Unsupported external authentication mechanism in the " +
                "configuration.");
        }
        return false;
    }

    /**
     * Initialize the RMI policy and the registryCSF according to the transport
     * settings in the store login.
     */
    public void prepareRegistryCSF() {
        if (hasTransportSettings()) {
            ClientSocketFactory.setRMIPolicy(getSecurityProperties());
        }
        RegistryUtils.initRegistryCSF();
    }

    /**
     * Initialize the RMI policy and the registryCSF according to the transport
     * settings in the store login and given client socket factory timeouts.
     *
     * @param openTimeoutMs registry client socket factory
     *        open timeout in milliseconds
     * @param readTimeoutMs registry client socket factory
     *        read timeout in milliseconds
     */
    public void prepareRegistryCSF(int openTimeoutMs, int readTimeoutMs) {
        if (hasTransportSettings()) {
            ClientSocketFactory.setRMIPolicy(getSecurityProperties());
        }
        RegistryUtils.initRegistryCSF(openTimeoutMs, readTimeoutMs);
    }

    /**
     * Get login credentials for login from the security properties. If
     * either the oracle.kv.auth.username is not set, or password of the
     * specified user is not found in the store, null will be returned.
     */
    public static LoginCredentials
        makeLoginCredentials(final Properties securityProps) {
        if (securityProps == null) {
            return null;
        }

        final String user = securityProps.getProperty(AUTH_USERNAME_PROPERTY);
        if (user == null) {
            return null;
        }

        if (isKerberosMech(securityProps)) {
            /* No password callback for API login */
            return buildKerberosCreds(user, securityProps, null);
        }

        final char[] passwd = PasswordManager
            .retrievePassword(user, securityProps);
        return passwd == null ? null : new PasswordCredentials(user, passwd);
    }

    /**
     * Create a security properties object by reading the settings in the
     * security file.
     */
    public static Properties createSecurityProperties(final String security) {
        if (security == null) {
            return null;
        }

        final File securityFile = new File(security);
        FileInputStream fis = null;

        try {
            fis = new FileInputStream(securityFile);
            final Properties securityProps = new Properties();
            securityProps.load(fis);
            resolveRelativePaths(securityProps, securityFile);
            return securityProps;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) /* CHECKSTYLE:OFF */ {
                /* Ignore */
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Given a set of Properties loaded from the file sourceFile, examine the
     * property settings that name a file or directory, and for each one that
     * is a relative path name, update the property setting to be an absolute
     * path name by interpreting the path as relative to the directory
     * containing sourceFile.
     */
    private static void resolveRelativePaths(Properties securityProps,
                                             File sourceFile) {
        final File sourceDir = sourceFile.getAbsoluteFile().getParentFile();
        for (final String propName : securityProps.stringPropertyNames()) {
            if (fileProperties.contains(propName)) {
                final String propVal = securityProps.getProperty(propName);
                File propFile = new File(propVal);
                if (!propFile.isAbsolute()) {
                    propFile  = new File(sourceDir, propVal);
                    securityProps.setProperty(propName, propFile.getPath());
                }
            }
        }
    }

    /**
     * Build a KerberosClientCreds. This method will try to locate class
     * KerberosLoginHelper on class path, it may throw a ClassNotFoundException
     * since CE does not have support for this. Then it calls buildKerberosCreds
     * method to build this internal Kerberos credentials.
     *
     * @param username user name of the credentials
     * @param sp security properties used for building credentials
     * @param handler a password callback handler that for prompting password,
     * if the value is null, the subsequent Kerberos login will not support
     * password prompting.
     * @return KerberosClientCreds object.
     * @throws AuthenticationFailureException if failed to acquire credentials
     * from KDC
     */
    public static KerberosClientCreds
        buildKerberosCreds(final String username,
                           final Properties sp,
                           final PasswordCallbackHandler handler)
        throws AuthenticationFailureException {
        final Class<?> krbLoginHelperClass;
        try {
            krbLoginHelperClass = Class.forName(KERBEROS_LOGIN_HELPER_CLASS);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("Kerberos authentication was " +
                "configured, but it is only supported in EE version");
        }

        try {
            Method method = krbLoginHelperClass.getMethod("buildKerberosCreds",
                String.class, Properties.class, PasswordCallbackHandler.class);
            try {
                return (KerberosClientCreds)
                        method.invoke(null, username, sp, handler);
            } catch (InvocationTargetException ite) {
                throw ite.getCause();
            }
        } catch (NoSuchMethodException nsme) {
            throw new IllegalStateException("Kerberos login helper " +
                "implementation does not have method buildKerberosCreds");
        } catch (AuthenticationFailureException afe) {
            throw afe;
        } catch (Exception e) {
            throw new IllegalStateException("Unexpected exception while " +
                "building Kerberos credentials", e);
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(
                t, "Unexpected exception: " + t);
        }
    }

    /*
     * Get KerberosClientCredentials of given user, retrieved Kerberos
     * client credentials from KDC and resolved Kerberos login properties.
     */
    public static KerberosClientCreds
        getKrbClientCredentials(KerberosCredentials krbCreds) {

        return buildKerberosCreds(
            krbCreds.getUsername(), krbCreds.getKrbProperties(), null);
    }

    /**
     * Makes a reauthenticate handler with the given credentialsProvider.
     *
     * @param credsProvider credentials provider
     * @return a reauthenticate handler, null if the credentialsProvider is
     * null
     */
    public static ReauthenticateHandler
        makeReauthenticateHandler(final CredentialsProvider credsProvider) {

        return credsProvider == null ?
               null :
               new ReauthenticateHandler() {

                   @Override
                   public void reauthenticate(KVStore kvstore)
                       throws FaultException,
                              AuthenticationFailureException,
                              AuthenticationRequiredException {
                       final LoginCredentials creds =
                           credsProvider.getCredentials();
                       kvstore.login(creds);
                   }
                };
    }

    /*
     * Check given string to boolean value. Throw IllegalArgumentException
     * if given string is a invalid boolean value.
     */
    public static boolean checkBooleanField(String input,
                                            boolean defaultValue) {
        if (input == null) {
            return defaultValue;
        }
        final Pattern pattern =
            Pattern.compile("true|false", Pattern.CASE_INSENSITIVE);
        final Matcher matcher = pattern.matcher(input);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Invalid input for boolean field: " + input);
        }
        return Boolean.parseBoolean(input);
    }

    /**
     * For compatibility with httpproxy. Remove when the proxy uses the logger
     * overloading introduced in 21.3.
     */
    public static AdminLoginManager
        getAdminLoginMgr(final String host,
                         final int port,
                         final LoginCredentials creds)
        throws AuthenticationFailureException {

        return getAdminLoginMgr(host, port, creds,
                                ClientLoggerUtils.getLogger(KVStoreLogin.class,
                                                            "httpproxy"));
    }

    /**
     * Obtain an AdminLoginManager with given host and port.
     *
     * @param host host
     * @param port port
     * @param creds login credentials
     * @param logger the debug logger
     * @return an AdminLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static AdminLoginManager
        getAdminLoginMgr(final String host,
                         final int port,
                         final LoginCredentials creds,
                         final Logger logger)
        throws AuthenticationFailureException {

        return getAdminLoginMgr(new String[] { host + ":" + port  }, creds,
                                logger);
    }

    /**
     * Obtain an AdminLoginManager with given host:port pairs.
     *
     * @param hostPorts the host:port pairs
     * @param creds login credentials
     * @param logger the debug logger
     * @return an AdminLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static AdminLoginManager
        getAdminLoginMgr(final String[] hostPorts,
                         final LoginCredentials creds,
                         final Logger logger)
        throws AuthenticationFailureException {

        return getAdminLoginMgr(hostPorts, creds, true /* autoRenew */,
                                logger);
    }

    /**
     * Obtain an AdminLoginManager with given host:port pairs, specifying
     * whether to renew tokens automatically.
     *
     * @param hostPorts the host:port pairs
     * @param creds login credentials
     * @param autoRenew whether to renew tokens automatically
     * @param logger the debug logger
     * @return an AdminLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static AdminLoginManager
        getAdminLoginMgr(final String[] hostPorts,
                         final LoginCredentials creds,
                         final boolean autoRenew,
                         final Logger logger)
        throws AuthenticationFailureException {

        if (creds != null) {
            final AdminLoginManager loginMgr =
                new AdminLoginManager(creds.getUsername(), autoRenew, logger);
            if (loginMgr.bootstrap(hostPorts, creds)) {
                return loginMgr;
            }
        }
        return null;
    }

    /**
     * Obtain a RepNodeLoginManager with given host and port. This method
     * should only be called in server contexts.
     *
     * @param host host
     * @param port port
     * @param creds login credentials
     * @param storeName the KVStore store name, if known, or else null
     * @return a RepNodeLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise.
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static RepNodeLoginManager
        getRepNodeLoginMgr(final String host,
                           final int port,
                           final LoginCredentials creds,
                           final String storeName)
        throws AuthenticationFailureException {

        return getRepNodeLoginMgr(new String[] { host + ":" + port  },
                                  creds, storeName);
    }

    /**
     * Obtain a RepNodeLoginManager with given host:port pairs. This method
     * should only be called in server contexts.
     *
     * @param hostPorts the host:port pairs
     * @param creds login credentials
     * @param storeName the KVStore store name, if known, or else null
     * @return a RepNodeLoginManager instance if non-null credentials were
     * provided and the login operation was successful, or null otherwise
     * @throws oracle.kv.AuthenticationFailureException if credentials are
     * supplied, but are invalid
     */
    public static RepNodeLoginManager
        getRepNodeLoginMgr(final String[] hostPorts,
                           final LoginCredentials creds,
                           final String storeName)
        throws AuthenticationFailureException {

        if (creds != null) {
            try {
                final RepNodeLoginManager loginMgr =
                    new RepNodeLoginManager(creds.getUsername(),
                                            true /* autoRenew */);
                loginMgr.bootstrap(hostPorts, creds, storeName);
                return loginMgr;
            } catch (KVStoreException kvse) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return null;
    }

    /**
     * Credentials provider used for constructing a reauthenticate handler. A
     * class implements this interface ought to provide login credentials for
     * use in reauthentication.
     */
    public interface CredentialsProvider {
        LoginCredentials getCredentials();
    }

    /**
     * A class provides the login credential via reading the password in the
     * store indicated in the store security properties.
     */
    public static class StoreLoginCredentialsProvider
        implements CredentialsProvider {
        private final Properties props;

        public StoreLoginCredentialsProvider(Properties securityProps) {
            this.props = securityProps;
        }

        @Override
        public LoginCredentials getCredentials() {
            return makeLoginCredentials(props);
        }
    }

    /*
     * Password callback handler for the JAAS login to obtain password from the
     * console input.
     */
    public static class PasswordCallbackHandler implements CallbackHandler {
        private final String name;
        private final ShellInputReader inputReader;

        public PasswordCallbackHandler(String name,
                                       ShellInputReader inputReader) {
            this.name = name;
            this.inputReader = inputReader;
        }

        @Override
        public void handle(Callback[] callbacks)
            throws IOException, UnsupportedCallbackException {

            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    final PasswordCallback pc = (PasswordCallback)callback;
                    final char [] krbPassword = inputReader.readPassword(
                        name + "'s kerberos password:");
                    pc.setPassword(krbPassword);
                    SecurityUtils.clearPassword(krbPassword);
                } else {
                    throw new UnsupportedCallbackException(
                        callback, "Unrecognized Callback");
                }
            }
        }
    }
}
