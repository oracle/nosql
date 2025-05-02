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

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static oracle.kv.impl.security.PasswordManager.WALLET_MANAGER_CLASS;

import java.io.File;
import java.util.Arrays;

import oracle.kv.impl.admin.client.CommandUtils;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.util.SecurityUtils.KadminSetting;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.impl.util.SecurityConfigCreator.ShellIOHelper;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

class SecurityConfigCommand extends CommandWithSubs {

    static final String ROOT_FLAG = "-root";
    static final String PWDMGR_FLAG = "-pwdmgr";
    static final String PASSMGR_WALLET = "wallet";
    static final String PASSMGR_PWDFILE = "pwdfile";
    static final String KEYSTORE_PASSWORD_FLAG = "-kspwd";
    static final String SECURITY_DIR_FLAG = "-secdir";
    static final String CERT_MODE_FLAG = "-certmode";
    static final String SECURITY_PARAM_FLAG = "-security-param";
    static final String PARAM_FLAG = "-param";
    static final String EXTERNAL_AUTH_FLAG = "-external-auth";
    static final String CONFIG_FLAG = "-config";
    static final String SOURCE_ROOT_FLAG = "-source-root";
    static final String SOURCE_SECURITY_DIR_FLAG = "-source-secdir";
    /* Not currently documented */
    static final String NO_SSL_FLAG = "-nossl";

    /* Kerberos-related flags */
    static final String KRB_CONF_FLAG = "-krb-conf";
    static final String KRB_INSTANCE_NAME = "-instance-name";
    static final String KRB_KADMIN_PATH = "-kadmin-path";
    static final String KRB_KADMIN_KEYTAB = "-kadmin-keytab";
    static final String KRB_KADMIN_CCACHE = "-kadmin-ccache";
    static final String KRB_ADMIN_PRINC = "-admin-principal";
    static final String KRB_PRINC_CONF_PARAM = "-princ-conf-param";
    static final String KRB_RENEW_KEYTAB = "-renew-keytab";
    static final String KRB_KEYSALT_LIST = "-keysalt";

    /* Type of Java KeyStores */
    static final String KEYSTORE_TYPE_FLAG = "-kstype";

    /* client.trust store password */
    static final String CLIENT_TRUST_PWD_FLAG = "-ctspwd";

    private static final String BASIC_CREATE_COMMAND_ARGS =
        "[-secdir <security dir>] " +
        "[-pwdmgr {pwdfile | wallet | <class-name>}] " + "\n\t" +
        "[-kspwd <server key and trust store password>] " + "\n\t" +
        "[-kstype <key and trust store type>] " + "\n\t" +
        "[-ctspwd <client.trust password>] " + "\n\t" +
        "[-external-auth {kerberos}]" + "\n\t  " +
        "[-krb-conf <kerberos configuration>] " + "\n\t  " +
        "[-kadmin-path <kadmin utility path>] " + "\n\t  " +
        "[-instance-name <database instance name>] " + "\n\t  " +
        "[-admin-principal <kerberos admin principal name>] " + "\n\t  " +
        "[-kadmin-keytab <keytab file>] " + "\n\t  " +
        "[-kadmin-ccache <credential cache file>] " + "\n\t  " +
        "[-princ-conf-param <param=value>]* ";
    private static final String CREATE_COMMAND_ARGS =
        "-root <secroot> " + "\n\t" +
        BASIC_CREATE_COMMAND_ARGS + "\n\t" +
        "[-param <param=value>]* ";
    /* Not currently documented - shared is default */
    @SuppressWarnings("unused")
    private static final String OTHER_ARGS =
        " [-certmode { shared | server }] ";
    private static final String ADD_SECURITY_COMMAND_ARGS =
        "-root <kvroot> " +
        "[-secdir <security dir>] " +
        "[-config <config.xml>]";
    private static final String REMOVE_SECURITY_COMMAND_ARGS =
        "-root <kvroot> " +
        "[-config <config.xml>]";
    private static final String MERGE_TRUST_COMMAND_ARGS =
        "-root <secroot> " +
        "[-secdir <security dir>] " +
        "-source-root <source secroot> " +
        "[-source-secdir <source secdir>] " +
        "[-ctspwd <client.trust password>]";
    private static final String UPDATE_SECURITY_COMMAND_ARGS =
        "-secdir <security dir> " +
        "[-kstype <keystore type>] " +
        "[-ctspwd <client.trust password>] " +
        "[-param <param=value>]*";
    private static final String VERIFY_SECURITY_COMMAND_ARGS =
        "-secdir <security dir>";
    private static final String SHOW_SECURITY_COMMAND_ARGS =
        "-secdir <security dir>";
    private static final String ADD_KERBEROS_COMMAND_ARGS =
        "-root <secroot> " +
        "[-secdir <security dir>] " + "\n\t" +
        "[-krb-conf <kerberos configuration>] " + "\n\t" +
        "[-kadmin-path <kadmin utility path>]" + "\n\t" +
        "[-instance-name <database instance name>] " + "\n\t" +
        "[-admin-principal <kerberos admin principal name>]" + "\n\t" +
        "[-kadmin-keytab <keytab file>] " + "\n\t" +
        "[-kadmin-ccache <credential cache file>] " + "\n\t" +
        "[-princ-conf-param <param=value>]* " + "\n\t" +
        "[-param <param=value>]*";
    private static final String RENEW_KEYTAB_COMMAND_ARGS =
        "-root <secroot> " +
        "[-secdir <security dir>] " + "\n\t" +
        "[-keysalt <enc:salt[,enc:salt,..]>] " + "\n\t" +
        "[-kadmin-path <kadmin utility path>]" + "\n\t" +
        "[-admin-principal <kerberos admin principal name>]" + "\n\t" +
        "[-kadmin-keytab <keytab file>] " + "\n\t" +
        "[-kadmin-ccache <credential cache file>] ";
    static final String CONFIG_UPDATE_SUCCEED = "Configuration updated.";

    SecurityConfigCommand() {
        super(Arrays.asList(new SecurityConfigCreate(),
                            new SecurityConfigAddKerberos(),
                            new SecurityConfigAddSecurity(),
                            new SecurityConfigRemoveSecurity(),
                            new SecurityConfigMergeTrust(),
                            new SecurityConfigUpdate(),
                            new SecurityConfigVerify(),
                            new SecurityConfigShowConfig(),
                            new SecurityRenewKeytab()),
              "config", 4, 1);
    }

    @Override
    public String getCommandOverview() {
        return "The config command allows configuration of security settings " +
            "for a NoSQL installation.";
    }

    public static String getPwdmgrClass(String pwdmgr) {
        if (pwdmgr == null) {
            return PasswordManager.preferredManagerClass();
        }
        if (pwdmgr.equals(PASSMGR_PWDFILE)) {
            return FILE_STORE_MANAGER_CLASS;
        }
        if (pwdmgr.equals(PASSMGR_WALLET)) {
            return WALLET_MANAGER_CLASS;
        }
        return pwdmgr;
    }

    /**
     * Checks whether the security directory contains basic security files.
     *
     * @param secDirFile security directory
     * @throws ShellException if the basic security files are not found in
     * the security directory.
     */
    private static void checkSecurityDirContents(final File secDirFile)
        throws ShellException {

        /*
         * Checks whether the security.xml, store.trust, and store.keys
         * exist in security dir.
         */
        final String[] checkedFileNames = { FileNames.SECURITY_CONFIG_FILE,
                                            FileNames.TRUSTSTORE_FILE,
                                            FileNames.KEYSTORE_FILE };

        for (final String fileName : checkedFileNames) {
            final File checkedFile = new File(secDirFile, fileName);
            if (!checkedFile.exists() || !checkedFile.isFile()) {
                throw new ShellException(
                    "Security file not found in " + secDirFile.toString() +
                    ": " + fileName);
            }
        }
    }

    private static void validateKeystorePassword(char[] pwd)
        throws ShellException {

        if (pwd.length < SecurityUtils.MIN_STORE_PASSPHRASE_LEN) {
            throw new ShellException(
                "The keystore password must be at least " +
                SecurityUtils.MIN_STORE_PASSPHRASE_LEN + " characters long");
        }
    }

    /**
     * Parsing logic for configuration creation used by makebootconfig.
     */
    static class ConfigParserHelper {
        private final CommandParser parser;
        private final ParsedConfig config;

        public ConfigParserHelper(CommandParser parser) {
            this.parser = parser;
            this.config = new ParsedConfig();
        }

        public boolean checkArg(String arg) {
            if (arg.equals(PWDMGR_FLAG)) {
                config.setPwdmgr(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KEYSTORE_PASSWORD_FLAG)) {
                config.setKeystorePassword(parser.nextArg(arg).toCharArray());
                return true;
            }
            if (arg.equals(CLIENT_TRUST_PWD_FLAG)) {
                config.setClientTrustStorePassword(
                    parser.nextArg(arg).toCharArray());
                return true;
            }
            if (arg.equals(KEYSTORE_TYPE_FLAG)) {
                config.setKeyStoreType(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(SECURITY_DIR_FLAG)) {
                config.setSecurityDir(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(CERT_MODE_FLAG)) {
                config.setCertMode(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(NO_SSL_FLAG)) {
                config.setNoSSL(true);
                return true;
            }
            if (arg.equals(EXTERNAL_AUTH_FLAG)) {
                config.setUserExternalAuth(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KRB_CONF_FLAG)) {
                config.setKrbConf(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KRB_INSTANCE_NAME)) {
                config.setInstanceName(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KRB_KADMIN_PATH)) {
                config.setKadminPath(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KRB_KADMIN_KEYTAB)) {
                config.setKadminKeytab(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KRB_KADMIN_CCACHE)) {
                config.setKadminCcache(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KRB_ADMIN_PRINC)) {
                config.setAdminPrinc(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(SECURITY_PARAM_FLAG)) {
                try {
                    config.addParam(parser.nextArg(arg));
                } catch (IllegalArgumentException iae) {
                    parser.usage("invalid argument usage for " + arg +
                                 " - " + iae.getMessage());
                }
                return true;
            }
            if (arg.equals(KRB_PRINC_CONF_PARAM)) {
                try {
                    config.addKrbProperty(parser.nextArg(arg));
                } catch (IllegalArgumentException iae) {
                    parser.usage("invalid argument usage for " + arg +
                                 " - " + iae.getMessage());
                }
                return true;
            }
            return false;
        }

        public ParsedConfig getConfig() {
            return config;
        }

        /**
         * For makebootconfig, don't report arguments that are supported
         * by the core code.
         */
        public static String getConfigUsage() {
            return BASIC_CREATE_COMMAND_ARGS + "\n\t" +
                "[-security-param <param=value>]*";
        }
    }

    /**
     * SecurityConfigCreate - implements the "create" subcommand.
     */
    private static final class SecurityConfigCreate extends SubCommand {
        private static final String CREATE_COMMAND_NAME = "create";
        private static final String CREATE_COMMAND_DESC =
            "Creates a new security configuration.";

        private SecurityConfigCreate() {
            super(CREATE_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            final ParsedConfig config = new ParsedConfig();
            String kvRoot = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (PWDMGR_FLAG.equals(arg)) {
                    config.setPwdmgr(Shell.nextArg(args, i++, this));
                } else if (KEYSTORE_PASSWORD_FLAG.equals(arg)) {
                    config.setKeystorePassword(
                        Shell.nextArg(args, i++, this).toCharArray());
                } else if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    config.setSecurityDir(Shell.nextArg(args, i++, this));
                } else if (NO_SSL_FLAG.equals(arg)) {
                    config.setNoSSL(true);
                } else if (CERT_MODE_FLAG.equals(arg)) {
                    config.setCertMode(Shell.nextArg(args, i++, this));
                } else if (PARAM_FLAG.equals(arg)) {
                    try {
                        config.addParam(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else if (EXTERNAL_AUTH_FLAG.equals(arg)) {
                    config.setUserExternalAuth(Shell.nextArg(args, i++, this));
                } else if (KRB_CONF_FLAG.equals(arg)) {
                    config.setKrbConf(Shell.nextArg(args, i++, this));
                } else if (KRB_INSTANCE_NAME.equals(arg)) {
                    config.setInstanceName(Shell.nextArg(args, i++, this));
                } else if (KRB_KADMIN_PATH.equals(arg)) {
                    config.setKadminPath(Shell.nextArg(args, i++, this));
                } else if (KRB_KADMIN_KEYTAB.equals(arg)) {
                    config.setKadminKeytab(Shell.nextArg(args, i++, this));
                } else if (KRB_KADMIN_CCACHE.equals(arg)) {
                    config.setKadminCcache(Shell.nextArg(args, i++, this));
                } else if (KRB_ADMIN_PRINC.equals(arg)) {
                    config.setAdminPrinc(Shell.nextArg(args, i++, this));
                } else if (KRB_PRINC_CONF_PARAM.equals(arg)) {
                    try {
                        config.addKrbProperty(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else if (KEYSTORE_TYPE_FLAG.equals(arg)) {
                    try {
                        config.setKeyStoreType(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else if (CLIENT_TRUST_PWD_FLAG.equals(arg)) {
                    config.setClientTrustStorePassword(
                        Shell.nextArg(args, i++, this).toCharArray());
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doCreate(kvRoot, config, shell);
        }

        private String doCreate(String kvRoot,
                                ParsedConfig config,
                                Shell shell)
            throws ShellException {

            final SecurityConfigCreator creator =
                new SecurityConfigCreator(kvRoot,
                                          config,
                                          new ShellIOHelper(shell));

            try {
                if (creator.createConfig()) {
                    return "Created";
                }
                return "Failed";
            } catch (PasswordStoreException pwse) {
                throw new ShellException(
                    "PasswordStore error: " + pwse.getMessage(), pwse);
            } catch (Exception e) {
                throw new ShellException(
                    "Unknown error: " + e.getMessage(), e);
            }
        }

        @Override
        public String getCommandSyntax() {
            return "config create " + CREATE_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return CREATE_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigAddKerberos - implements the "add-kerberos" subcommand.
     */
    private static final class SecurityConfigAddKerberos extends SubCommand {
        private static final String ADD_KRB_COMMAND_NAME = "add-kerberos";
        private static final String ADD_KRB_COMMAND_DESC =
            "Add Kerberos to an existing security configuration or a new" +
            " server principal to an existing Kerberos configuration.";

        private SecurityConfigAddKerberos() {
            super(ADD_KRB_COMMAND_NAME, 8);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            final ParsedConfig config = new ParsedConfig();
            String kvRoot = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    config.setSecurityDir(Shell.nextArg(args, i++, this));
                } else if (PARAM_FLAG.equals(arg)) {
                    try {
                        config.addParam(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else if (KRB_CONF_FLAG.equals(arg)) {
                    config.setKrbConf(Shell.nextArg(args, i++, this));
                } else if (KRB_INSTANCE_NAME.equals(arg)) {
                    config.setInstanceName(Shell.nextArg(args, i++, this));
                } else if (KRB_KADMIN_PATH.equals(arg)) {
                    config.setKadminPath(Shell.nextArg(args, i++, this));
                } else if (KRB_KADMIN_KEYTAB.equals(arg)) {
                    config.setKadminKeytab(Shell.nextArg(args, i++, this));
                } else if (KRB_KADMIN_CCACHE.equals(arg)) {
                    config.setKadminCcache(Shell.nextArg(args, i++, this));
                } else if (KRB_ADMIN_PRINC.equals(arg)) {
                    config.setAdminPrinc(Shell.nextArg(args, i++, this));
                } else if (KRB_PRINC_CONF_PARAM.equals(arg)) {
                    try {
                        config.addKrbProperty(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doAdd(kvRoot, config, shell);
        }

        private String doAdd(String kvRoot, ParsedConfig config, Shell shell)
            throws ShellException {

            if (config.getSecurityDir() == null) {
                config.setSecurityDir(FileNames.SECURITY_CONFIG_DIR);
            }

            final SecurityConfigCreator creator =
                new SecurityConfigCreator(kvRoot,
                                          config,
                                          new ShellIOHelper(shell));

            try {
                if (creator.addKerberosConfig()) {
                    return "Updated Kerberos configuration";
                }
                return "Failed";
            } catch (Exception e) {
                throw new ShellException(
                    "Unknown error: " + e.getMessage(), e);
            }
        }

        @Override
        public String getCommandSyntax() {
            return "config add-kerberos " + ADD_KERBEROS_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return ADD_KRB_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigAddSecurity - implements the "add-security" subcommand.
     */
    private static final class SecurityConfigAddSecurity extends SubCommand {
        private static final String ADD_SECURITY_COMMAND_NAME = "add-security";
        private static final String ADD_SECURITY_COMMAND_DESC =
            "Updates a Storage Node configuration to incorporate a security " +
            "configuration.";

        private SecurityConfigAddSecurity() {
            super(ADD_SECURITY_COMMAND_NAME, 7);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String configXml = null;
            String secDir = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    secDir = Shell.nextArg(args, i++, this);
                } else if (CONFIG_FLAG.equals(arg)) {
                    configXml = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doAddSecurity(kvRoot, configXml, secDir);
        }

        private String doAddSecurity(String kvRoot,
                                     String configXml,
                                     String secDir)
            throws ShellException {

            if (configXml == null) {
                configXml = FileNames.SNA_CONFIG_FILE;
            }

            if (secDir == null) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }

            final File kvRootFile = new File(kvRoot);
            final File configXmlFile = new File(configXml);
            final File secDirFile = new File(secDir);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (configXmlFile.isAbsolute()) {
                throw new ShellException(
                    "The -config argument must be a relative file name.");
            }

            final File rootedConfigXmlFile = new File(kvRootFile, configXml);
            if (!rootedConfigXmlFile.exists()) {
                throw new ShellException(
                    "The file " + configXml + " does not exist in " + kvRoot);
            }

            if (!rootedConfigXmlFile.isFile()) {
                throw new ShellException(
                    rootedConfigXmlFile.toString() + " is not a file.");
            }

            if (secDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -secdir argument must be a relative file name.");
            }

            final File rootedSecDirFile = new File(kvRootFile, secDir);
            if (!rootedSecDirFile.exists()) {
                throw new ShellException(
                    "The file " + secDir + " does not exist in " + kvRoot);
            }

            if (!rootedSecDirFile.isDirectory()) {
                throw new ShellException(
                    rootedSecDirFile.toString() + " is not a directory.");
            }

            checkSecurityDirContents(rootedSecDirFile);
            updateConfigFile(rootedConfigXmlFile, secDirFile, kvRootFile);
            return "Configuration updated.";
        }

        private void updateConfigFile(File configFile, File secDir,
                                      File kvRootFile)
            throws ShellException {

            final BootstrapParams bp;
            try {
                bp = ConfigUtils.getBootstrapParams(configFile);
            } catch (IllegalStateException ise) {
                throw new ShellException(
                    "Failed to load or parse " + configFile + ": " +
                    ise.getMessage());
            }

            if (bp == null) {
                throw new ShellException(
                    "The file " + configFile +
                    " does not contain a bootstrap configuration.");
            }

            /*
             * Set the security directory, which will check whether the
             * configuration has a sufficient large service port range
             * allocated, if set.
             */
            try {
                bp.setSecurityDir(secDir.getPath());
            } catch (IllegalArgumentException iae) {
                throw new ShellException(
                    "The configuration will not work in a secure " +
                    " environment. Please adjust the configuration before " +
                    " enabling security. (" + iae.getMessage() + ")");
            }
            final String storeName = bp.getStoreName();
            File kvConfigPath = null;

            if (storeName != null && !storeName.isEmpty()) {
                kvConfigPath =
                    FileNames.getSNAConfigFile(kvRootFile.toString(),
                                               storeName,
                                               new StorageNodeId(bp.getId()));
            }

            /* before update the config, ensure FTS not enabled */
            final String esCluster = getRegisteredESCluster(kvConfigPath);
            if (esCluster != null && !esCluster.isEmpty()) {
                throw new ShellException(
                    "The configuration cannot be enabled in a store with " +
                    "registered ES cluster " + esCluster + ", please first " +
                    "deregister the ES cluster from the non-secure store," +
                    "and reconfigure.");
            }

            ConfigUtils.createBootstrapConfig(bp, configFile);
        }

        /* Gets registered ES cluster (if any) from SNA config file */
        private String getRegisteredESCluster(File kvConfigPath) {
            if (kvConfigPath == null) {
                /* cannot find storage node parameters without config file */
                return null;
            }
            final StorageNodeParams snp =
                ConfigUtils.getStorageNodeParams(kvConfigPath, null);
            return  snp.getSearchClusterName();
        }

        @Override
        public String getCommandSyntax() {
            return "config add-security " + ADD_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return ADD_SECURITY_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigRemoveSecurity - implements the "remove-security"
     * subcommand.
     */
    private static final class SecurityConfigRemoveSecurity extends SubCommand {
        private static final String REMOVE_SECURITY_COMMAND_NAME =
            "remove-security";
        private static final String REMOVE_SECURITY_COMMAND_DESC =
            "Updates a Storage Node configuration to remove the security " +
            "configuartion.";

        private SecurityConfigRemoveSecurity() {
            super(REMOVE_SECURITY_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String configXml = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (CONFIG_FLAG.equals(arg)) {
                    configXml = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doRemoveSecurity(kvRoot, configXml);
        }

        private String doRemoveSecurity(String kvRoot,
                                        String configXml)
            throws ShellException {

            if (configXml == null) {
                configXml = FileNames.SNA_CONFIG_FILE;
            }

            final File kvRootFile = new File(kvRoot);
            final File configXmlFile = new File(configXml);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (configXmlFile.isAbsolute()) {
                throw new ShellException(
                    "The -config argument must be a relative file name.");
            }

            final File rootedConfigXmlFile = new File(kvRootFile, configXml);
            if (!rootedConfigXmlFile.exists()) {
                throw new ShellException(
                    "The file " + configXml + " does not exist in " + kvRoot);
            }

            if (!rootedConfigXmlFile.isFile()) {
                throw new ShellException(
                    rootedConfigXmlFile.toString() + " is not a file.");
            }

            updateConfigFile(rootedConfigXmlFile);
            return "Configuration updated.";
        }

        private void updateConfigFile(File configFile)
            throws ShellException {

            final BootstrapParams bp;
            try {
                bp = ConfigUtils.getBootstrapParams(configFile);
            } catch (IllegalStateException ise) {
                throw new ShellException(
                    "Failed to load or parse " + configFile + ": " +
                    ise.getMessage());
            }

            if (bp == null) {
                throw new ShellException(
                    "The file " + configFile +
                    " does not contain a bootstrap configuration.");
            }
            final String secDir = bp.getSecurityDir();
            if (secDir == null) {
                throw new ShellException(
                    "The file " + configFile +
                    " does not currently have security configured.");
            }
            bp.setSecurityDir(null);

            ConfigUtils.createBootstrapConfig(bp, configFile);
        }

        @Override
        public String getCommandSyntax() {
            return "config remove-security " + REMOVE_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return REMOVE_SECURITY_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigMergeTrust - implements the "merge-trust" subcommand.
     */
    private static final class SecurityConfigMergeTrust extends SubCommand {
        private static final String MERGE_TRUST_COMMAND_NAME = "merge-trust";
        private static final String MERGE_TRUST_COMMAND_DESC =
            "Merges trust information from a source security directory into " +
            "a security configuration.";

        private SecurityConfigMergeTrust() {
            super(MERGE_TRUST_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String secDir = null;
            String srcKvRoot = null;
            String srcSecDir = null;
            char[] ctsPwd = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    secDir = Shell.nextArg(args, i++, this);
                } else if (SOURCE_ROOT_FLAG.equals(arg)) {
                    srcKvRoot = Shell.nextArg(args, i++, this);
                } else if (SOURCE_SECURITY_DIR_FLAG.equals(arg)) {
                    srcSecDir = Shell.nextArg(args, i++, this);
                } else if (CLIENT_TRUST_PWD_FLAG.equals(arg)) {
                    ctsPwd = Shell.nextArg(args, i++, this).toCharArray();
                }  else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            if (srcKvRoot == null) {
                shell.requiredArg(SOURCE_ROOT_FLAG, this);
            }

            return doMergeTrust(kvRoot, secDir, srcKvRoot, srcSecDir, ctsPwd);
        }

        private String doMergeTrust(String kvRoot,
                                    String secDir,
                                    String srcKvRoot,
                                    String srcSecDir,
                                    char[] ctsPwd)
            throws ShellException {

            if (secDir == null) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }

            if (srcSecDir == null) {
                srcSecDir = FileNames.SECURITY_CONFIG_DIR;
            }

            final File kvRootFile = new File(kvRoot);
            final File secDirFile = new File(secDir);
            final File srcKvRootFile = new File(srcKvRoot);
            final File srcSecDirFile = new File(srcSecDir);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (secDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -secdir argument must be a relative file name.");
            }

            final File rootedSecDirFile = new File(kvRootFile, secDir);
            if (!rootedSecDirFile.exists()) {
                throw new ShellException(
                    "The file " + secDir + " does not exist in " + kvRoot);
            }

            if (!rootedSecDirFile.isDirectory()) {
                throw new ShellException(
                    rootedSecDirFile.toString() + " is not a directory.");
            }

            if (!srcKvRootFile.exists()) {
                throw new ShellException(
                    "The -source-root argument " + srcKvRootFile +
                    " does not exist.");
            }

            if (!srcKvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -source-root argument " + srcKvRootFile +
                    " is not a directory.");
            }

            if (srcSecDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -source-secdir argument must be a " +
                    "relative file name.");
            }

            final File rootedSrcSecDirFile = new File(srcKvRootFile, srcSecDir);
            if (!rootedSrcSecDirFile.exists()) {
                throw new ShellException(
                    "The file " + srcSecDir + " does not exist in " +
                    srcKvRoot);
            }

            if (!rootedSrcSecDirFile.isDirectory()) {
                throw new ShellException(
                    rootedSrcSecDirFile.toString() + " is not a directory.");
            }

            if (ctsPwd != null) {
                validateKeystorePassword(ctsPwd);
            }

            if (SecurityUtils.mergeTrust(
                rootedSrcSecDirFile, rootedSecDirFile, ctsPwd)) {
                return "Configuration updated.";
            }
            return "config merge-trust failed";
        }

        @Override
        public String getCommandSyntax() {
            return "config merge-trust " + MERGE_TRUST_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return MERGE_TRUST_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigUpdate - implements the "update" subcommand.
     */
    private static final class SecurityConfigUpdate extends SubCommand {
        private static final String UPDATE_COMMAND_NAME = "update";
        private static final String UPDATE_COMMAND_DESC =
            "Update the store type of Java KeyStore and TrustStore, or " +
            "security parameters of a security configuration.";

        private SecurityConfigUpdate() {
            super(UPDATE_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            final ParsedConfig config = new ParsedConfig();

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (SECURITY_DIR_FLAG.equals(arg)) {
                    config.setSecurityDir(Shell.nextArg(args, i++, this));
                } else if (PARAM_FLAG.equals(arg)) {
                    try {
                        config.addParam(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else if (KEYSTORE_TYPE_FLAG.equals(arg)) {
                    try {
                        config.setKeyStoreType(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else if (CLIENT_TRUST_PWD_FLAG.equals(arg)) {
                    config.setClientTrustStorePassword(
                        Shell.nextArg(args, i++, this).toCharArray());
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (config.getSecurityDir() == null) {
                shell.requiredArg(SECURITY_DIR_FLAG, this);
            }

            String ksType = config.getKeyStoreType();
            if (ksType != null && !SecurityUtils.isPKCS12(ksType)) {
                throw new ShellException(
                    "Unable to update Java KeyStore to " + ksType +
                    ", only PKCS12 is allowed");
            }

            return doUpdate(config);
        }

        private String doUpdate(ParsedConfig config)
            throws ShellException {

            final File secDirFile = new File(config.getSecurityDir());

            if (!secDirFile.exists()) {
                throw new ShellException(
                    "The -secDir argument " + secDirFile + " does not exist.");
            }

            if (!secDirFile.isDirectory()) {
                throw new ShellException(
                    "The -secDir argument " + secDirFile +
                    " is not a directory.");
            }
            checkSecurityDirContents(secDirFile);

            if (!config.getUserParams().isEmpty()) {
                SecurityUtils.updateSecurityParams(secDirFile,
                                                   config.getUserParams());
            }

            String ksType = config.getKeyStoreType();
            char[] ctsPwd = config.getClientTrustStorePassword();
            if (ksType != null) {
                if (ctsPwd != null) {
                    validateKeystorePassword(ctsPwd);
                }
                if (!SecurityUtils.updateKeyStoreType(
                         secDirFile, ksType, ctsPwd)) {
                    return "Java KeyStore type update failed";
                }
            }
            return CONFIG_UPDATE_SUCCEED;
        }

        @Override
        public String getCommandSyntax() {
            return "config update " + UPDATE_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return UPDATE_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigVerify - implements the "verify" subcommand.
     */
    private static final class SecurityConfigVerify extends SubCommand {
        private static final String VERIFY_COMMAND_NAME = "verify";
        private static final String VERIFY_COMMAND_DESC =
            "Verify the consistency and correctness of a security" +
            " configuration.";

        private SecurityConfigVerify() {
            super(VERIFY_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String secConfigDir = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (SECURITY_DIR_FLAG.equals(arg)) {
                    secConfigDir = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (secConfigDir == null) {
                shell.requiredArg(SECURITY_DIR_FLAG, this);
            }

            return doVerify(secConfigDir);
        }

        private String doVerify(String secConfigDir)
            throws ShellException {

            final File secDirFile = new File(secConfigDir);

            if (!secDirFile.exists()) {
                throw new ShellException(
                    "The -secDir argument " + secDirFile + " does not exist.");
            }

            if (!secDirFile.isDirectory()) {
                throw new ShellException(
                    "The -secDir argument " + secDirFile +
                    " is not a directory.");
            }

            checkSecurityDirContents(secDirFile);
            final String errors = SecurityUtils.verifyConfiguration(secDirFile);

            if (errors != null && !errors.equals("")) {
                return errors + "Security configuration verification failed.";
            }
            return "Security configuration verification passed.";
        }

        @Override
        public String getCommandSyntax() {
            return "config verify " + VERIFY_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return VERIFY_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigShowConfig - implements the "show" subcommand.
     */
    private static final class SecurityConfigShowConfig extends SubCommand {
        private static final String SHOW_COMMAND_NAME = "show";
        private static final String SHOW_COMMAND_DESC =
            "Print out all security configuration information.";

        private SecurityConfigShowConfig() {
            super(SHOW_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String secConfigDir = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (SECURITY_DIR_FLAG.equals(arg)) {
                    secConfigDir = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (secConfigDir == null) {
                shell.requiredArg(SECURITY_DIR_FLAG, this);
            }

            return doShowConfig(secConfigDir);
        }

        private String doShowConfig(String secConfigDir)
            throws ShellException {

            final File secDir = new File(secConfigDir);

            if (!secDir.exists()) {
                throw new ShellException(
                    "The -secDir argument " + secDir + " does not exist.");
            }

            if (!secDir.isDirectory()) {
                throw new ShellException(
                    "The -secDir argument " + secDir +
                    " is not a directory.");
            }

            checkSecurityDirContents(secDir);

            final StringBuilder sb = new StringBuilder();

            /* Formatted parameters saved in security.xml file */
            final SecurityParams sp = SecurityUtils.loadSecurityParams(secDir);
            final ParameterMap pmap = sp.getMap();
            sb.append("Security parameters:\n");
            sb.append(CommandUtils.
                formatParams(pmap, false /* show hidden */, null /* Info */));

            for (ParameterMap tmap : sp.getTransportMaps()) {
                sb.append("\n" + tmap.getName() + " Transport parameters:");
                sb.append("\n" + CommandUtils.formatParams(
                    tmap, true /* show hidden */, null /* Info */));
            }

            /* Print out information of entries in keystore and truststore */
            final String keystoreInfos = SecurityUtils.printKeystores(secDir);
            sb.append("\n" + keystoreInfos);
            return sb.toString();
        }

        @Override
        public String getCommandSyntax() {
            return "config show " + SHOW_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return SHOW_COMMAND_DESC;
        }
    }

    /**
     * SecurityRenewKeytab - implements the "renew-keytab" subcommand.
     */
    private static final class SecurityRenewKeytab extends SubCommand {
        private static final String RENEW_KEYTAB_COMMAND_NAME = "renew-keytab";
        private static final String RENEW_KEYTAB_COMMAND_DESC =
            "Renew a keytab file in a security directory";

        private SecurityRenewKeytab() {
            super(RENEW_KEYTAB_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String secDir = null;
            String keysalt = null;
            String krbAdminPrinc = null;
            String krbAdminKeytab = null;
            String krbAdminCcache = null;
            String krbAdminPath = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    secDir = Shell.nextArg(args, i++, this);
                } else if (KRB_KEYSALT_LIST.equals(arg)) {
                    keysalt = Shell.nextArg(args, i++, this);
                } else if (KRB_KADMIN_PATH.equals(arg)) {
                    krbAdminPath = Shell.nextArg(args, i++, this);
                } else if (KRB_ADMIN_PRINC.equals(arg)) {
                    krbAdminPrinc = Shell.nextArg(args, i++, this);
                } else if (KRB_KADMIN_KEYTAB.equals(arg)) {
                    krbAdminKeytab = Shell.nextArg(args, i++, this);
                } else if (KRB_KADMIN_CCACHE .equals(arg)) {
                    krbAdminCcache = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            final KadminSetting kadminSetting = new KadminSetting().
                setKrbAdminPrinc(krbAdminPrinc).
                setKrbAdminKeytab(krbAdminKeytab).
                setKrbAdminCcache(krbAdminCcache);

            if (krbAdminPath != null) {
                kadminSetting.setKrbAdminPath(krbAdminPath);
            }

            return doRenewKeytab(kvRoot, secDir, keysalt, kadminSetting, shell);
        }

        private String doRenewKeytab(String kvRoot,
                                     String secDir,
                                     String keysalt,
                                     KadminSetting kadminSetting,
                                     Shell shell)
            throws ShellException {

            if (secDir == null) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }

            final File kvRootFile = new File(kvRoot);
            File secDirFile = new File(secDir);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (secDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -secdir argument must be a relative file name.");
            }

            secDirFile = new File(kvRoot, secDir);
            if (!secDirFile.exists()) {
                throw new ShellException(
                    "The file " + secDir + " does not exist in " + kvRoot);
            }
            if (!secDirFile.isDirectory()) {
                throw new ShellException(
                    secDirFile.toString() + " is not a directory.");
            }

            try {
                kadminSetting.validateKadminSetting();
                SecurityUtils.renewKeytab(secDirFile, keysalt, kadminSetting,
                                          new ShellIOHelper(shell));
            } catch (IllegalArgumentException iae) {
                throw new ShellException("kadmin configuration error, " +
                                         iae.getMessage());
            }

            return "Configuration updated.";
        }

        @Override
        public String getCommandSyntax() {
            return "config renew-keytab " + RENEW_KEYTAB_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return RENEW_KEYTAB_COMMAND_DESC;
        }
    }
}
