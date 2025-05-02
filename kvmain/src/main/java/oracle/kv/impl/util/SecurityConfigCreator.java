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

package oracle.kv.impl.util;

import static oracle.kv.impl.admin.param.SecurityParams.TRANS_TYPE_CLEAR;
import static oracle.kv.impl.admin.param.SecurityParams.TRANS_TYPE_SSL;
import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static oracle.kv.impl.security.PasswordManager.WALLET_MANAGER_CLASS;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Stream;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;

import oracle.kv.KVSecurityConstants;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.security.AuthenticatorManager;
import oracle.kv.impl.security.AuthenticatorManager.SystemAuthMethod;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.security.util.ConsolePasswordReader;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.util.SecurityUtils.KadminSetting;
import oracle.kv.impl.security.util.SecurityUtils.Krb5Config;
import oracle.kv.util.shell.Shell;

/**
 * SecurityConfigCreator implements the core operations for creating a
 * security configuration.  It is referenced by both makebootconfig and
 * securityconfig.
 */
public class SecurityConfigCreator {
    private String kvRoot;
    private final IOHelper ioHelper;
    private final ParsedConfig config;
    /* derived from config */
    private final String pwdMgrClass;

    private static final String SHARED_KEY_ALIAS = "shared";
    private static final String SSL_CERT_DN = "CN=NoSQL";
    private static final String SSL_CLIENT_PEER = "CN=NoSQL";
    private static final String SSL_SERVER_PEER = "CN=NoSQL";
    private static final String INTERNAL_AUTH_SSL = "ssl";
    private static final String CERT_MODE_SHARED = "shared";
    private static final String CERT_MODE_SERVER = "server";
    private static final String PWD_ALIAS_KEYSTORE = "keystore";
    private static final String NOT_CREATING_KEYTAB_MESSAGE =
        "The kadmin path was specified as NONE, so not creating " +
        "a keytab for the database server. The keytab must " +
        "be generated and copied to the security configuration " +
        "directory manually.";

    /**
     * Configuration class.
     */
    public static class ParsedConfig {
        private String pwdmgr;
        private String secDir;
        private char[] ksPassword;
        private String certMode;
        private String userExternalAuth;
        private String krbConf;
        private String princInstanceName;
        private boolean printCreatedFiles = true;
        private boolean nossl = false;

        /* Java KeyStore type */
        private String ksType;

        /* client.trust password */
        private char[] ctsPassword;

        /* Settings used for connecting kadmin */
        private KadminSetting kadminSetting = new KadminSetting();

        /* User-defined parameters */
        private List<ParamSetting> userParams = new ArrayList<>();

        /* User-defined Kerberos configuration properties */
        private Properties princConfigProperties =
            SecurityUtils.getDefaultKrbPrincipalProperties();

        public class ParamSetting {
            final ParameterState pstate;
            final String transportName;
            final String paramName;
            final String paramValue;

            ParamSetting(ParameterState pstate,
                         String transportName,
                         String paramName,
                         String paramValue) {
                this.pstate = pstate;
                this.transportName = transportName;
                this.paramName = paramName;
                this.paramValue = paramValue;
            }

            public ParameterState getParameterState() {
                return pstate;
            }

            public String getTransportName() {
                return transportName;
            }

            public String getParamName() {
                return paramName;
            }

            public String getParamValue() {
                return paramValue;
            }
        }

        public void setPwdmgr(String pwdmgrVal) {
            pwdmgr = pwdmgrVal;
        }

        public String getPwdmgr() {
            return pwdmgr;
        }

        public void setPrintCreatedFiles(boolean value) {
            printCreatedFiles = value;
        }

        public boolean isPrintCreatedFiles() {
            return printCreatedFiles;
        }

        public void setKeystorePassword(char[] ksPwdVal) {
            ksPassword = ksPwdVal;
        }

        public char[] getKeystorePassword() {
            return ksPassword;
        }

        public void setSecurityDir(String securityDir) {
            secDir = securityDir;
        }

        public String getSecurityDir() {
            return secDir;
        }

        public void setNoSSL(boolean value) {
            nossl = value;
        }

        public boolean noSSL() {
            return nossl;
        }

        public void setKeyStoreType(String type) {
            if (type != null &&
                !(SecurityUtils.isPKCS12(type)) &&
                !(SecurityUtils.isJKS(type))) {
                throw new IllegalArgumentException(
                    "The value '" + type + "' is not a valid KeyStore type, " +
                    "only PKCS12 and JKS are allowed.");
            }
            ksType = type;
        }

        public String getKeyStoreType() {
            return ksType;
        }

        public void setClientTrustStorePassword(char[] pwdVal) {
            ctsPassword = pwdVal;
        }

        public char[] getClientTrustStorePassword() {
            return ctsPassword;
        }

        public void setCertMode(String certModeVal) {
            if (certModeVal != null &&
                !(CERT_MODE_SHARED.equals(certModeVal)) &&
                !(CERT_MODE_SERVER.equals(certModeVal))) {
                throw new IllegalArgumentException(
                    "The value '" + certModeVal + "' is not a valid " +
                    "certificate mode.  Only " + CERT_MODE_SHARED + " and " +
                    CERT_MODE_SERVER + " are allowed.");
            }
            certMode = certModeVal;
        }

        public String getCertMode() {
            return certMode;
        }

        public void setUserExternalAuth(String externalAuthVal) {
            /*
             * We support multiple external authentication methods, but only
             * one at at time.
             */
            if (externalAuthVal != null) {
                userExternalAuth = externalAuthVal.toUpperCase(Locale.ENGLISH);
                final String[] extAuths = userExternalAuth.split(",");

                for (String extAuth : extAuths) {
                    if (!AuthenticatorManager.isValidAuthMethod(extAuth) &&
                        !AuthenticatorManager.noneAuthMethod(extAuth)) {
                        throw new IllegalArgumentException(
                            "The value '" + extAuth + "' is not a valid " +
                            "external authentication method. " +
                            "Only " +
                            Arrays.toString(SystemAuthMethod.values()) +
                            " are allowed.");
                    }
                }

                if (SecurityUtils.hasKerberos(extAuths) &&
                    SecurityUtils.hasIDCSOAuth(extAuths)) {
                    throw new IllegalArgumentException(
                        "The value '" + extAuths +"' is not valid, cannot " +
                        "enable more than one authentication methods");
                }
            }
            userExternalAuth = externalAuthVal;
        }

        public String getUserExternalAuth() {
            return userExternalAuth;
        }

        public void setKrbConf(String krbConfVal) {
            krbConf = krbConfVal;
        }

        public String getKrbConf() {
            return krbConf;
        }

        public KadminSetting getKadminSetting() {
            return kadminSetting;
        }

        public void setInstanceName(String instanceNameVal) {
            princInstanceName = instanceNameVal;
        }

        public String getInstanceName() {
            return princInstanceName;
        }

        public void setKadminPath(String kadminPath) {
            kadminSetting.setKrbAdminPath(kadminPath);
        }

        public void setAdminPrinc(String adminPrinc) {
            kadminSetting.setKrbAdminPrinc(adminPrinc);
        }

        public void setKadminKeytab(String adminKeytab) {
            kadminSetting.setKrbAdminKeytab(adminKeytab);
        }

        public void setKadminCcache(String adminCcache) {
            kadminSetting.setKrbAdminCcache(adminCcache);
        }

        /**
         * @throws IllegalArgumentException if the parameter setting is not
         * valid.
         */
        public void addParam(String paramSetting) {

            final int equalIdx = paramSetting.indexOf("=");
            if (equalIdx < 0) {
                throw new IllegalArgumentException(
                    "Invalid parameter setting - missing '='");
            }

            final String param = paramSetting.substring(0, equalIdx);
            final String value = paramSetting.substring(equalIdx + 1);
            final String[] paramSplit = param.split(":");

            if (paramSplit.length > 2) {
                throw new IllegalArgumentException(
                    "Invalid parameter name format: " + param);
            }

            final String paramName = paramSplit[paramSplit.length - 1];
            final String transport =
                (paramSplit.length > 1) ? paramSplit[0] : null;
            final ParameterState pstate = ParameterState.lookup(paramName);

            if (pstate == null) {
                throw new IllegalArgumentException(
                    "The name " + paramName + " is not a valid parameter name");
            }

            if (!(pstate.appliesTo(Info.SECURITY) ||
                  pstate.appliesTo(Info.TRANSPORT))) {
                throw new IllegalArgumentException(
                    "The name " + paramName + " is not a valid parameter for " +
                    "a security configuration");
            }

            if (transport != null) {
                if (!pstate.appliesTo(Info.TRANSPORT)) {
                    throw new IllegalArgumentException(
                        paramName + " is not a transport parameter");
                }

                if (!(ParameterState.SECURITY_TRANSPORT_CLIENT.equals(
                          transport) ||
                      ParameterState.SECURITY_TRANSPORT_INTERNAL.equals(
                          transport) ||
                      ParameterState.SECURITY_TRANSPORT_JE_HA.equals(
                          transport))) {
                    throw new IllegalArgumentException(
                        transport + " is not a valid transport name");
                }
            }
            userParams.add(
                new ParamSetting(pstate, transport, paramName, value));
        }

        public List<ParamSetting> getUserParams() {
            return userParams;
        }

        public void addKrbProperty(String paramSetting) {
            final int equalIdx = paramSetting.indexOf("=");
            if (equalIdx < 0) {
                throw new IllegalArgumentException(
                    "Invalid parameter setting - missing '='");
            }

            final String param = paramSetting.substring(0, equalIdx);
            final String value = paramSetting.substring(equalIdx + 1);

            if (princConfigProperties.get(param) == null) {
                throw new IllegalArgumentException("The name " + param +
                    " is not a valid Kerberos configuration parameter");
            }
            princConfigProperties.put(param, value);
        }

        public Properties getKrbPrincProperties() {
            return princConfigProperties;
        }

        public void populateDefaults() {
            if (getSecurityDir() == null) {
                setSecurityDir(FileNames.SECURITY_CONFIG_DIR);
            }
            if (getCertMode() == null) {
                setCertMode(CERT_MODE_SHARED);
            }
            if (getKrbConf() == null) {
                setKrbConf(SecurityUtils.KRB_CONF_FILE);
            }
        }
    }

    /**
     * This functionality wants to be accessed through a couple of paths.
     * provide an intermediate interface that can adapt to either path.
     */
    public interface IOHelper {

        /**
         * Read a password from the user.
         */
        char[] readPassword(String prompt) throws IOException;

        /**
         * Print a line of output.
         */
        void println(String s);
    }

    /**
     * IOHelper for use in the context of a Shell environment.
     */
    static class ShellIOHelper implements IOHelper {
        private Shell shell;
        private PasswordReader passwordReader;

        ShellIOHelper(Shell shell) {
            this.shell = shell;
            passwordReader = null;
            if (shell instanceof SecurityShell) {
                passwordReader = ((SecurityShell) shell).getPasswordReader();
            }
            if (passwordReader == null) {
                passwordReader = new ConsolePasswordReader();
            }
        }

        @Override
        public char[] readPassword(String prompt) throws IOException {
            return passwordReader.readPassword(prompt);
        }

        @Override
        public void println(String s) {
            shell.println(s);
        }
    }

    /**
     * Generic IO Helper
     */
    public static class GenericIOHelper implements IOHelper {
        private PrintStream printStream;
        private PasswordReader passwordReader;

        public GenericIOHelper(PrintStream printStream) {
            this(printStream, new ConsolePasswordReader());
        }

        GenericIOHelper(PrintStream printStream,
                        PasswordReader passwordReader) {
            this.printStream = printStream;
            this.passwordReader = passwordReader;
        }

        @Override
        public char[] readPassword(String prompt) throws IOException {
            return passwordReader.readPassword(prompt);
        }

        @Override
        public void println(String s) {
            printStream.println(s);
        }
    }

    public SecurityConfigCreator(String kvRoot,
                                 ParsedConfig parsedConfig,
                                 IOHelper ioHelper) {
        this.kvRoot = kvRoot;
        this.ioHelper = ioHelper;
        this.config = parsedConfig;
        this.pwdMgrClass =
            SecurityConfigCommand.getPwdmgrClass(config.getPwdmgr());
    }

    /*
     * Create a user login file for secured KVLite.
     */
    public boolean createUserLoginFile(String user,
                                       char[] password,
                                       File securityDir)
        throws PasswordStoreException, Exception {

        final PasswordManager pwdMgr = resolvePwdMgr();

        if (pwdMgr == null) {
            return false;
        }

        /*
         * Make a SecurityParams instance to pass parameters of password
         * manager to the method createUserPasswordStore that is shared
         * with createConfig().
         */
        final SecurityParams sp = new SecurityParams();
        final Properties securityProps = KVStoreLogin
            .createSecurityProperties(
                new File(securityDir, FileNames.CLIENT_SECURITY_FILE)
                .getPath());
        securityProps.setProperty(
            KVSecurityConstants.AUTH_USERNAME_PROPERTY, user);
        if (pwdMgrClass.equals(WALLET_MANAGER_CLASS)) {
            sp.setWalletDir(FileNames.USER_WALLET_DIR);
            securityProps.setProperty(
                KVSecurityConstants.AUTH_WALLET_PROPERTY,
                FileNames.USER_WALLET_DIR);
        } else if (pwdMgrClass.equals(FILE_STORE_MANAGER_CLASS)) {
            sp.setPasswordFile(FileNames.USER_PASSWD_FILE);
            securityProps.setProperty(
                KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                FileNames.USER_PASSWD_FILE);
        }

        createUserPasswordStore(securityDir, sp, user, password);

        final File userSecurityFile =
            new File(securityDir.getPath(), FileNames.USER_SECURITY_FILE);

        securityProps.put(
            SSLConfig.TRUSTSTORE_FILE, FileNames.CLIENT_TRUSTSTORE_FILE);

        ConfigUtils.storeProperties(securityProps, null, userSecurityFile);

        ioHelper.println("Generated password for user " + user + ":" +
                         String.valueOf(password));
        ioHelper.println("User login file: " + userSecurityFile.getPath());

        return true;
    }

    /**
     * Gather input from the user and create the requisite security files.
     * @return true if the operation was successful
     */
    public boolean createConfig() throws PasswordStoreException, Exception {

        config.populateDefaults();

        /* Get a PasswordManager instance */
        final PasswordManager pwdMgr = resolvePwdMgr();
        if (pwdMgr == null) {
            return false;
        }

        /* Make sure the security directory exists - create if needed */
        final File securityDir = prepareSecurityDir();
        if (securityDir == null) {
            return false;
        }

        final boolean nossl = config.noSSL();
        char[] keyStorePassword = config.getKeystorePassword();
        char[] ctsPassword = config.getClientTrustStorePassword();
        if (!nossl) {
            if (keyStorePassword != null) {
                if (!validKeystorePassword(keyStorePassword)) {
                    return false;
                }
            } else {
                /* Get a keystore password - the user may need to know this */
                try {
                    keyStorePassword = promptForKeyStorePassword();
                    if (keyStorePassword == null) {
                        ioHelper.println("No keystore password specified");
                        return false;
                    }
                } catch (IOException ioe) {
                    ioHelper.println("I/O error reading password: " +
                                     ioe.getMessage());
                    return false;
                }
            }

            if (ctsPassword != null) {
                if (!validKeystorePassword(ctsPassword)) {
                    return false;
                }
            }
        }

        /* Prepare the SecurityParams object */
        final SecurityParams sp = nossl ?
            makeSecurityParamsWithoutSSL() : makeSecurityParams();

        if (!nossl) {
            /* Create the keystore file */
            final Properties keyStoreProperties = new Properties();
            keyStoreProperties.setProperty(SecurityUtils.KEY_DISTINGUISHED_NAME,
                                           SSL_CERT_DN);
            if (config.getKeyStoreType() != null) {
                keyStoreProperties.setProperty(SecurityUtils.KEYSTORE_TYPE,
                                               config.getKeyStoreType());
            }
            SecurityUtils.initKeyStore(securityDir,
                                       sp,
                                       keyStorePassword,
                                       ctsPassword,
                                       keyStoreProperties);

            /*
             * Create the password store, which will hold the password for the
             * keystore.
             */
            createUserPasswordStore(securityDir, sp, PWD_ALIAS_KEYSTORE,
                keyStorePassword);
        }

        /*
         * Create requisite security configuration files of Kerberos
         * authentication method.
         */
        if (SecurityUtils.hasKerberos(config.getUserExternalAuth())) {
            /* Check if Kerberos authentication is supported */
            if (!resolveKerberosAuth()) {
                return false;
            }

            /* Parse and validate Kerberos configuration file */
            final Krb5Config krb5Conf = parseKerberosConfig();
            if (krb5Conf == null) {
                return false;
            }

            /* Validate kadmin settings */
            try {
                config.getKadminSetting().validateKadminSetting();
            } catch (IllegalArgumentException iae) {
                ioHelper.println("kadmin setting error " + iae.getMessage());
                return false;
            }

            sp.setKerberosConfFile(krb5Conf.getConfigFilePath());
            sp.setKerberosRealmName(krb5Conf.getDefaultRealm());

            if (!config.getKadminSetting().doNotPerformKadmin()) {
                SecurityUtils.generateKeyTabFile(securityDir,
                                                 sp,
                                                 config.getKadminSetting(),
                                                 config.getKrbPrincProperties(),
                                                 ioHelper);
            } else {
                ioHelper.println(NOT_CREATING_KEYTAB_MESSAGE);
            }
        }

        /* Check if IDCS OAuth authentication method can be enabled */
        if (SecurityUtils.hasIDCSOAuth(config.getUserExternalAuth())) {
            resolveIDCSOAuthAuth();
        }

        /*
         * Now that the password store and keystore have successfully been
         * created, build the security.xml file that ties it all together.
         */
        final File securityXmlFile =
            new File(securityDir.getPath(), FileNames.SECURITY_CONFIG_FILE);
        ConfigUtils.createSecurityConfig(sp, securityXmlFile);

        if (nossl) {
            SecurityUtils.initClientSecurity(sp, securityDir, null);
        }

        /* summarize the configuration for the user */
        if (config.isPrintCreatedFiles()) {
            final List<File> createdFiles = findSecurityFiles(securityDir);
            ioHelper.println("Created files");
            for (File f : createdFiles) {
                ioHelper.println("    " + f.getPath());
            }
        }
        if (nossl) {
            ioHelper.println("It's not recommended to configure " +
                             "Oracle NoSQL Database Security without SSL. " +
                             "All network communication will be in clear text" +
                             " including login credentials, login tokens, " +
                             "and data");
        }
        return true;
    }

    private void createUserPasswordStore(File securityDir,
                                         SecurityParams sp,
                                         String userName,
                                         char[] password) throws IOException {
        final PasswordStore pwdStore = makePasswordStore(securityDir, sp);
        pwdStore.setSecret(userName, password);
        pwdStore.save();
        pwdStore.discard();
    }

    /**
     * Add Kerberos configuration to an existing security configuration.
     *
     * Either adding Kerberos to an existing secure, but non-Kerberos
     * configuration, or adding a new service principal and keytab file to an
     * existing Kerberos configuration.
     */
    public boolean addKerberosConfig() {
        /* Check if Kerberos authentication is supported */
        if (!resolveKerberosAuth()) {
            return false;
        }

        /* Check if kvroot and security directory exist */
        File kvRootDir = new File(kvRoot);
        if (!kvRootDir.exists()) {
            ioHelper.println(
                "The directory " + kvRootDir.getPath() + " does not exist");
            return false;
        }
        if (!kvRootDir.isAbsolute()) {
            kvRootDir = kvRootDir.getAbsoluteFile();
            kvRoot = kvRootDir.getPath();
        }

        final File securityDir = new File(kvRoot, config.getSecurityDir());
        if (!securityDir.exists()) {
            ioHelper.println("The directory " + securityDir.getPath() +
                             " does not exist");
            return false;
        }
        config.populateDefaults();

        /* security.xml file is expected to exist in this case */
        final File secXmlFile =
            new File(securityDir.getPath(), FileNames.SECURITY_CONFIG_FILE);
        if (!secXmlFile.exists()) {
            ioHelper.println("security.xml file does not exist, " +
                "need to run securityconfig create firstly");
            return false;
        }
        final SecurityParams sp = ConfigUtils.getSecurityParams(secXmlFile);
        String oldKeytabName = sp.getKerberosKeytabFile();
        if (oldKeytabName == null) {
            oldKeytabName = FileNames.KERBEROS_KEYTAB_FILE;
        }

        /* Parse and validate Kerberos configuration file */
        final Krb5Config krb5Conf = parseKerberosConfig();
        if (krb5Conf == null) {
            return false;
        }

        try {
            config.getKadminSetting().validateKadminSetting();
        } catch (IllegalArgumentException iae) {
            ioHelper.println("kadmin setting error " + iae.getMessage());
            return false;
        }

        /*
         * Find out if user customized service name or keytab file name
         * in existing configuration
         */
        String newKeytabName = FileNames.KERBEROS_KEYTAB_FILE;
        final List<ParsedConfig.ParamSetting> settings = config.getUserParams();
        for (ParsedConfig.ParamSetting setting : settings) {
            if (setting.paramName.equals(
                ParameterState.SEC_KERBEROS_SERVICE_NAME)) {
                sp.setKerberosServiceName(setting.paramValue);
            } else if (setting.paramName.equals(
                ParameterState.SEC_KERBEROS_KEYTAB_FILE)) {
                newKeytabName = setting.paramValue;
            }
        }

        sp.setKerberosKeytabFile(newKeytabName);
        sp.setKerberosConfFile(krb5Conf.getConfigFilePath());
        sp.setKerberosRealmName(krb5Conf.getDefaultRealm());
        sp.setKerberosInstanceName(config.getInstanceName());

        if (config.getKadminSetting().doNotPerformKadmin()) {
            ioHelper.println(NOT_CREATING_KEYTAB_MESSAGE);

            /* Refresh Kerberos-related configuration in security.xml */
            ConfigUtils.createSecurityConfig(sp, secXmlFile);
            return true;
        }
        final String canonicalPrincName =
            SecurityUtils.getCanonicalPrincName(sp);
        final KerberosPrincipal servicePrinc =
            new KerberosPrincipal(canonicalPrincName);

        /*
         * If target keytab file exists, check if it contains keys of the
         * service principal that current configuration is aim to add. If so,
         * do not generate a new keytab file, otherwise the previous keytab
         * would not work correctly since kvno are bumped automatically by kdc.
         */
        final File newKeytab = new File(securityDir, newKeytabName);
        if (newKeytab.exists()) {
            final KeyTab keytab = KeyTab.getInstance(newKeytab);

            if (keytab.getKeys(servicePrinc).length != 0) {
                ioHelper.println("Keytab file " + newKeytabName +
                    " already contains the keys of " + canonicalPrincName +
                    " not adding this Kerberos service principal.");
                return false;
            }

            /* Remove existing keytab file */
            if (!newKeytab.delete()) {
                ioHelper.println("Existing keytab file " + newKeytabName +
                                 " cannot be removed");
                return false;
            }
        }

        /*
         * If existing configuration use different keytab file name, check if
         * its keytab file contains the keys of the service principal current
         * configuration is aim to create. If so, rename the previous keytab
         * file to current one and not extract a new keytab file.
         */
        if (oldKeytabName != null && !oldKeytabName.equals(newKeytabName)) {
            final File oldKeytab = new File(securityDir, oldKeytabName);
            if (oldKeytab.exists()) {
                final KeyTab keytab = KeyTab.getInstance(oldKeytab);

                if (keytab.getKeys(servicePrinc).length != 0) {

                    /*
                     * New configuration use the different keytab file name,
                     * rename to new keytab file name.
                     */
                    ioHelper.println("Existing keytab file " + oldKeytabName +
                        " already contains the keys of " + canonicalPrincName +
                        ", rename keytab file from " +
                        oldKeytabName + " to " + newKeytabName);

                     if (!oldKeytab.renameTo(newKeytab)) {
                         ioHelper.println("Rename keytab file failed");
                         return false;
                     }

                     /*
                      * Refresh Kerberos-related configuration in
                      * security.xml file
                      */
                     ConfigUtils.createSecurityConfig(sp, secXmlFile);
                     return true;
                }

                /* Remove existing keytab file */
                if (!oldKeytab.delete()) {
                    ioHelper.println("Old keytab file " + oldKeytabName +
                                     " cannot be removed");
                    return false;
                }
            }
        }

        /* Add store service principal and extract keytab file */
        if (SecurityUtils.generateKeyTabFile(securityDir,
                                             sp,
                                             config.getKadminSetting(),
                                             config.getKrbPrincProperties(),
                                             ioHelper)) {
            ioHelper.println("Created file: " + newKeytabName);

            /* Refresh Kerberos-related configuration in security.xml */
            ConfigUtils.createSecurityConfig(sp, secXmlFile);
            return true;
        }

        return false;
    }

    private List<File> findSecurityFiles(File securityDir) {
        final List<File> result = new ArrayList<File>();
        findFiles(securityDir, result);
        return result;
    }

    private void findFiles(File securityDir, List<File> found) {
        for (File f : securityDir.listFiles()) {
            if (f.isDirectory()) {
                findFiles(f, found);
            } else {
                found.add(f);
            }
        }
    }

    /**
     * Resolve the pwdMgrClass to a PasswordManager instance.
     * @return an instance of the password manager class, or null if a problem
     * was encountered
     */
    private PasswordManager resolvePwdMgr() {
        try {
            return PasswordManager.load(pwdMgrClass);
        } catch (ClassNotFoundException cnfe) {
            ioHelper.println("Unable to locate password manager class '" +
                             pwdMgrClass + "'");
            return null;
        } catch (Exception exc) {
            /*
             * There are lots of ways this could fail, none of them likely.
             */
            ioHelper.println("Creation of password manager class failed: " +
                             exc.getMessage());
            return null;
        }
    }

    /**
     * Ensure the existence of the security directory based on the kvRoot
     * input.
     * @return a File object identifying the security directory if all
     *   goes according to plan, or null otherwise.
     */
    private File prepareSecurityDir() {
        /* Check out kvRoot */
        File kvRootDir = new File(kvRoot);
        if (!kvRootDir.exists()) {
            ioHelper.println(
                "The directory " + kvRootDir.getPath() + " does not exist");
            return null;
        }
        if (!kvRootDir.isAbsolute()) {
            kvRootDir = kvRootDir.getAbsoluteFile();
            kvRoot = kvRootDir.getPath();
        }

        /*
         * Then create the security directory and updates subdirectory if
         * needed
         */
        final File securityDir = new File(kvRoot, config.getSecurityDir());
        final String errorMsg = checkSecurityDirectory(securityDir);
        if (errorMsg != null) {
            ioHelper.println(errorMsg);
            return null;
        }
        return securityDir;
    }

    /**
     * Make sure the security directory exists and only contains an empty
     * updates subdirectory. Return null in that case, or else return a string
     * describing what went wrong. Non-private for testing.
     */
    static String checkSecurityDirectory(File securityDir) {
        final Path secPath = securityDir.toPath();
        try {
            Files.createDirectories(secPath.resolve(SECURITY_UPDATES_DIR));
        } catch (IOException e) {
            return "Unable to create directories: " + e;
        }
        try (final Stream<Path> stream = Files.walk(secPath, 2)) {
            if (stream.limit(3).count() <= 2) {
                return null;
            }
            return "The directory " + secPath + " exists and contains files" +
                " other than the " + SECURITY_UPDATES_DIR + " directory";
        } catch (IOException | UncheckedIOException e) {
            return "Problem reading the " + secPath + " directory: " + e;
        }
    }

    private PasswordStore makePasswordStore(File securityDir,
                                            SecurityParams sp) {

        PasswordManager pwdMgr = null;
        try {
            pwdMgr = PasswordManager.load(pwdMgrClass);
        } catch (ClassNotFoundException cnfe) {
            /*
             * This should already have been checked, but we have to deal with
             * the exception, so...
             */
            ioHelper.println("Unable to locate password manager class '" +
                             pwdMgrClass + "'");
            return null;
        } catch (Exception exc) {
            /*
             * There are lots of ways this could fail, none of them likely.
             */
            ioHelper.println("Creation of password manager class failed: " +
                                 exc.getMessage());
            return null;
        }

        final File storeLoc =
            new File(securityDir,
                     new File(pwdMgrClass.equals(WALLET_MANAGER_CLASS) ?
                              sp.getWalletDir() :
                              sp.getPasswordFile()).getPath());
        final PasswordStore pwdStore = pwdMgr.getStoreHandle(storeLoc);

        try {
            /* Create password store as auto-login */
            pwdStore.create(null);
            return pwdStore;
        } catch (IOException ioe) {
            ioHelper.println("Error creating password store: " +
                             ioe.getMessage());
        }
        return null;
    }

    private SecurityParams makeSecurityParamsWithoutSSL() {
        final SecurityParams sp = new SecurityParams();

        /* Security is enabled */
        sp.setSecurityEnabled(true);

        if (SecurityUtils.hasKerberos(config.getUserExternalAuth())) {
            if (config.getInstanceName() != null) {
                sp.setKerberosInstanceName(config.getInstanceName());
            }

            if (config.getKrbConf() != null) {
                sp.setKerberosConfFile(config.getKrbConf());
            }
            sp.setKerberosKeytabFile(FileNames.KERBEROS_KEYTAB_FILE);
        }

        /* Set client transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_CLIENT);
        sp.setTransType(ParameterState.SECURITY_TRANSPORT_CLIENT,
                        TRANS_TYPE_CLEAR);

        /* Set internal transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_INTERNAL);
        sp.setTransType(ParameterState.SECURITY_TRANSPORT_INTERNAL,
                        TRANS_TYPE_CLEAR);

        /* Set JE HA transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_JE_HA);
        sp.setTransType(ParameterState.SECURITY_TRANSPORT_JE_HA,
                        TRANS_TYPE_CLEAR);

        /*
         * Apply any user-specified settings
         */
        SecurityUtils.applyParamsChanges(sp, config.getUserParams());
        return sp;
    }

    private SecurityParams makeSecurityParams() {
        final SecurityParams sp = new SecurityParams();

        /* Security is enabled */
        sp.setSecurityEnabled(true);

        if (pwdMgrClass.equals(WALLET_MANAGER_CLASS)) {
            sp.setWalletDir(FileNames.WALLET_DIR);
        } else {
            sp.setPasswordFile(FileNames.PASSWD_FILE);
            sp.setPasswordClass(pwdMgrClass);
        }

        sp.setKeystorePasswordAlias(PWD_ALIAS_KEYSTORE);

        sp.setKeystoreFile(FileNames.KEYSTORE_FILE);
        sp.setTruststoreFile(FileNames.TRUSTSTORE_FILE);

        /* Set the internal authentication mechanism */
        sp.setInternalAuth(INTERNAL_AUTH_SSL);

        /* Set the certificate mode */
        sp.setCertMode(CERT_MODE_SHARED);

        /* Set the types of key and trust store */
        String ksType = (config.getKeyStoreType() == null) ?
            SecurityUtils.KS_TYPE_DEFAULT : config.getKeyStoreType();
        sp.setKeystoreType(ksType);
        sp.setTruststoreType(ksType);

        if (SecurityUtils.hasKerberos(config.getUserExternalAuth())) {
            if (config.getInstanceName() != null) {
                sp.setKerberosInstanceName(config.getInstanceName());
            }

            if (config.getKrbConf() != null) {
                sp.setKerberosConfFile(config.getKrbConf());
            }
            sp.setKerberosKeytabFile(FileNames.KERBEROS_KEYTAB_FILE);
        }

        /* Set client transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_CLIENT);
        sp.setTransType(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            TRANS_TYPE_SSL);
        sp.setTransServerKeyAlias(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            SHARED_KEY_ALIAS);
        sp.setTransServerIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            "dnmatch(" + SSL_SERVER_PEER + ")");

        /*
         * We only set the ClientAllowProtocols because we might need to be
         * accessed by applications that can't set their protocol level,
         * which would prevent them from connecting to us.
         */
        sp.setTransClientAllowProtocols(
            ParameterState.SECURITY_TRANSPORT_CLIENT,
            SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT);

        /* Set internal transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_INTERNAL);
        sp.setTransType(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            TRANS_TYPE_SSL);
        sp.setTransServerKeyAlias(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            SHARED_KEY_ALIAS);
        sp.setTransClientKeyAlias(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            SHARED_KEY_ALIAS);
        sp.setTransClientAuthRequired(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            true);
        sp.setTransClientIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            "dnmatch(" + SSL_CLIENT_PEER + ")");
        sp.setTransServerIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            "dnmatch(" + SSL_SERVER_PEER + ")");
        /* See above for reason why we only apply this on the client side */
        sp.setTransClientAllowProtocols(
            ParameterState.SECURITY_TRANSPORT_INTERNAL,
            SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT);

        /* Set JE HA transport */
        sp.addTransportMap(ParameterState.SECURITY_TRANSPORT_JE_HA);
        sp.setTransType(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            TRANS_TYPE_SSL);
        sp.setTransServerKeyAlias(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            SHARED_KEY_ALIAS);
        sp.setTransClientAuthRequired(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            true);
        sp.setTransClientIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            "dnmatch(" + SSL_CLIENT_PEER + ")");
        sp.setTransServerIdentityAllowed(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            "dnmatch(" + SSL_SERVER_PEER + ")");

        /*
         * JE doesn't support the notion of client vs. server and we don't
         * have a need to support non-Oracle applications, so set both client
         * and server to use the preferred protocols.
         */
        sp.setTransAllowProtocols(
            ParameterState.SECURITY_TRANSPORT_JE_HA,
            SecurityUtils.PREFERRED_PROTOCOLS_DEFAULT);

        /*
         * Apply any user-specified settings
         */
        SecurityUtils.applyParamsChanges(sp, config.getUserParams());
        return sp;
    }

    private char[] promptForKeyStorePassword()
        throws IOException {

        while (true) {
            final char[] pwd = ioHelper.readPassword(
                "Enter a password for the Java KeyStore:");
            if (pwd == null || pwd.length == 0) {
                return null;
            }
            if (!validKeystorePassword(pwd)) {
                continue;
            }
            final char[] pwd2 = ioHelper.readPassword(
                "Re-enter the KeyStore password for verification:");
            if (pwd2 != null && SecurityUtils.passwordsMatch(pwd, pwd2)) {
                return pwd;
            }
            ioHelper.println("The passwords do not match");
        }
    }

    private boolean validKeystorePassword(char[] pwd) {
        /*
         * Standard requirement for keystore implementations is 6 character
         * minimum, though some implementations might add additional
         * requirements.
         */
        if (pwd.length < SecurityUtils.MIN_STORE_PASSPHRASE_LEN) {
            ioHelper.println("The keystore password must be at least " +
                             SecurityUtils.MIN_STORE_PASSPHRASE_LEN +
                             " characters long");
            return false;
        }
        return true;
    }

    /**
     * Resolve whether Kerberos authentication method is supported.
     */
    private boolean resolveKerberosAuth() {
        if (AuthenticatorManager.isSupported(
            SecurityUtils.KERBEROS_AUTH_NAME)) {
            return true;
        }
        ioHelper.println(
            "Unable to locate Kerberos authenticator class, " +
            "it is possible you are using NoSQL Database Community Edition " +
            "or Basic Edition. " +
            "Kerberos authentication is only available in " +
            "NoSQL Database Enterprise Edition");
        return false;
    }

    /**
     * Resolve whether IDCS OAuth authentication method is supported.
     */
    private boolean resolveIDCSOAuthAuth() {
        if (AuthenticatorManager.isSupported(
            SecurityUtils.OAUTH_AUTH_NAME)) {
            return true;
        }
        ioHelper.println("IDCS OAuth authentication is not supported");
        return false;
    }

    /**
     * Parse and validate Kerberos configuration file.
     */
    private Krb5Config parseKerberosConfig() {
        final File krb5ConfFile = new File(config.getKrbConf());
        if (!krb5ConfFile.exists()) {
            ioHelper.println("Kerberos configuration file does not exist");
            return null;
        }

        /* Parse krb5.conf file and find default realm and its kdc */
        final Krb5Config krb5Conf = new Krb5Config(krb5ConfFile);
        try {
            krb5Conf.parseConfigFile();
        } catch (IOException ioe) {
            ioHelper.println("Parsing kerberos configuration file " +
                config.getKrbConf() + " error: " + ioe.getMessage());
            return null;
        }

        if (krb5Conf.getDefaultRealm() == null || krb5Conf.getKdc() == null) {
            ioHelper.println("Kerberos configuration file does not specify" +
                             "default realm and its kdc correctly");
            return null;
        }
        return krb5Conf;
    }
}
