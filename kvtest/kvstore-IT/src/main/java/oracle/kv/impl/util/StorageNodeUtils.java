/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Utility methods for testing related to Storage Nodes
 */
public class StorageNodeUtils {

    private static final Logger logger =
        LoggerUtils.getLogger(StorageNodeUtils.class, "test");

    public static LoginManager NULL_LOGIN_MGR = null;

    //    protected static final String testhost = "localhost";

    /**
     * Register a StorageNodeAgent to a store using default state.
     */
    public static StorageNodeAgentAPI
        registerStore(StorageNodeAgentAPI snai,
                      LoginManager loginMgr,
                      int port,
                      String testhost,
                      String kvstorename,
                      StorageNodeId snid,
                      boolean hostingAdmin)
        throws Exception {

        /**
         * Register a store
         */
        StorageNodeParams snp =
            new StorageNodeParams(snid, testhost, port, "");
        GlobalParams gp = new GlobalParams(kvstorename);
        snai.register(gp.getMap(), snp.getMap(), hostingAdmin);

        /**
         * Get a new handle.  Register made the last one invalid.
         * The LoginManager, if not null,  also has a stale login handle, but
         * it will refresh it.
         */
        snai = getHandle(kvstorename, testhost, snp.getRegistryPort(),
                         snp.getStorageNodeId(), loginMgr);

        /**
         * If hosting the admin, first configure it so it's happy
         */
        if (hostingAdmin) {
            CommandServiceAPI cs = waitForAdmin(testhost, port, loginMgr);
            cs.configure(kvstorename);
        }
        return snai;
    }

    /**
     * Public static method to start and register an SNA
     */
    public static StorageNodeAgent createRegisteredStore(String testhost,
                                                         String kvstorename,
                                                         PortFinder portFinder,
                                                         String configFileName,
                                                         StorageNodeId snid,
                                                         boolean hostingAdmin,
                                                         boolean useThreads,
                                                         boolean createAdmin,
                                                         SecureOpts secOpts)
        throws Exception {

        String testDir =
            generateBootstrapDir(portFinder, 1, configFileName, secOpts);
        StorageNodeAgent sna =
            startSNA(testDir, configFileName, useThreads, createAdmin);
        int port = portFinder.getRegistryPort();
        StorageNodeAgentAPI snai =
            getBootstrapHandle(testhost, port, sna.getLoginManager());
        registerStore(snai, sna.getLoginManager(), port, testhost,
                      kvstorename, snid, hostingAdmin);
        return sna;
    }

    /**
     * Get a handle for a registered SNA instance.
     */
    public static StorageNodeAgentAPI getHandle(String kvstorename,
                                                String hostname,
                                                int port,
                                                StorageNodeId snid,
                                                LoginManager loginMgr)
        throws Exception {

        String bn =
            RegistryUtils.bindingName(kvstorename,
                                      snid.getFullName(),
                                      RegistryUtils.InterfaceType.MAIN);
        return RegistryUtils.getStorageNodeAgent(hostname, port, bn, loginMgr,
                                                 logger);
    }

    /**
     * Get a bootstrap handle for SNA.
     */
    public static StorageNodeAgentAPI getBootstrapHandle(String hostname,
                                                         int port,
                                                         LoginManager loginMgr)
        throws Exception {
        return RegistryUtils.getStorageNodeAgent
            (hostname, port, GlobalParams.SNA_SERVICE_NAME, loginMgr, logger);
    }

    /**
     * Public method to create an unregistered SNA.
     */
    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              String mgmtImpl,
                              int mgmtPollPort,
                              int mgmtTrapPort,
                              int memoryMB)
        throws Exception {

        return createUnregisteredSNA(rootDir, portFinder, capacity,
                                     configFileName, useThreads, createAdmin,
                                     mgmtImpl, mgmtPollPort, mgmtTrapPort,
                                     memoryMB, null, null);
    }

    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              String mgmtImpl,
                              int memoryMB,
                              List<String> storageDirs,
                              SecureOpts secureOpts)
        throws Exception {

        return createUnregisteredSNA(rootDir, portFinder, capacity,
                                     configFileName, useThreads, createAdmin,
                                     mgmtImpl, 0, 0, memoryMB, storageDirs,
                                     secureOpts);
    }

    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              String mgmtImpl,
                              int mgmtPollPort,
                              int mgmtTrapPort,
                              int memoryMB,
                              List<String> storageDirs,
                              SecureOpts secureOpts,
                              KerberosOpts krbOpts,
                              Set<Parameter> extraParams)
        throws Exception {

        generateBootstrapDir(rootDir, portFinder, capacity, memoryMB,
                             mgmtImpl, mgmtPollPort, mgmtTrapPort,
                             configFileName, storageDirs, secureOpts,
                             krbOpts, true, extraParams);

        StorageNodeAgent sna =
            startSNA(rootDir, configFileName, useThreads, createAdmin);
        return sna;
    }

    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              String mgmtImpl,
                              int mgmtPollPort,
                              int mgmtTrapPort,
                              int memoryMB,
                              List<String> storageDirs,
                              SecureOpts secureOpts)
        throws Exception {

        return createUnregisteredSNA(rootDir,
                                     portFinder,
                                     capacity,
                                     configFileName,
                                     useThreads,
                                     createAdmin,
                                     mgmtImpl,
                                     mgmtPollPort,
                                     mgmtTrapPort,
                                     memoryMB,
                                     storageDirs,
                                     secureOpts,
                                     null /* kerberos opts */,
                                     Collections.emptySet());
    }

    public static StorageNodeAgentAPI
        createNoBootstrapSNA(PortFinder portFinder,
                             int capacity,
                             String configFileName)
        throws Exception {

        generateBootstrapDir(portFinder, capacity, configFileName, false);

        StorageNodeAgent sna =
            startSNA(TestUtils.getTestDir().toString(),
                     configFileName, false, false);
        String bn = sna.getServiceName();
        StorageNodeAgentAPI snai =
            RegistryUtils.getStorageNodeAgent(portFinder.getHostname(),
                                              sna.getRegistryPort(),
                                              bn, sna.getLoginManager(),
                                              logger);
        return snai;
    }

    public static void deleteDirs(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                deleteDirs(c);
            }
        }
        f.delete();
    }

    public static void cleanStoreDir(String testDir, String kvstorename)
        throws Exception {

        File storeDir = new File(testDir + File.separator + kvstorename);
        deleteDirs(storeDir);
    }

    public static String generateBootstrapDir(PortFinder pf,
                                              int capacity,
                                              String configFileName)
        throws Exception {

        return generateBootstrapDir(pf, capacity, configFileName, null);
    }

    public static String generateBootstrapDir(PortFinder pf,
                                              int capacity,
                                              String configFileName,
                                              boolean runBootAdmin)
        throws Exception {

        String testDir = TestUtils.getTestDir().toString();
        return generateBootstrapDir(testDir, pf, capacity, 0, null,
                                    0, 0, configFileName, null, null,
                                    null, runBootAdmin);
    }

    public static String generateBootstrapDir(PortFinder pf,
                                              int capacity,
                                              String configFileName,
                                              SecureOpts secureOpts)
        throws Exception {

        String testDir = TestUtils.getTestDir().toString();
        return generateBootstrapDir(testDir, pf, capacity, 0, null, 0, 0,
                                    configFileName, null, secureOpts,
                                    null, true);
    }

    public static String generateBootstrapDir(String testDir,
                                              PortFinder pf,
                                              int capacity,
                                              int memoryMB,
                                              String mgmtImpl,
                                              int mgmtPollPort,
                                              int mgmtTrapPort,
                                              String configFileName)
        throws Exception {

        return generateBootstrapDir(testDir, pf, capacity, memoryMB,
                                    mgmtImpl, mgmtPollPort, mgmtTrapPort,
                                    configFileName, null, null, null, true);
    }

    public static String generateBootstrapDir(String testDir,
                                              PortFinder pf,
                                              int capacity,
                                              int memoryMB,
                                              String mgmtImpl,
                                              int mgmtPollPort,
                                              int mgmtTrapPort,
                                              String configFileName,
                                              List<String> storageDirs,
                                              SecureOpts secureOpts,
                                              KerberosOpts krbOpts,
                                              boolean runBootAdmin)
        throws Exception {

        return generateBootstrapDir(testDir, pf, capacity, memoryMB,
                                    mgmtImpl, mgmtPollPort, mgmtTrapPort,
                                    configFileName, storageDirs, secureOpts,
                                    krbOpts, runBootAdmin,
                                    Collections.emptySet());
    }

    public static String generateBootstrapDir(String testDir,
                                              PortFinder pf,
                                              int capacity,
                                              int memoryMB,
                                              String mgmtImpl,
                                              int mgmtPollPort,
                                              int mgmtTrapPort,
                                              String configFileName,
                                              List<String> storageDirs,
                                              SecureOpts secureOpts,
                                              KerberosOpts krbOpts,
                                              boolean runBootAdmin,
                                              Set<Parameter> extraParams)
        throws Exception {

        File configfile = new File(testDir + File.separator + configFileName);
        File secfile = new File
            (testDir + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE);
        if (configfile.exists()) {
            return testDir;
        }

        File relSecurityDir = null;
        String userExternalAuth = null;
        if (secureOpts != null && secureOpts.isSecure()) {
            generateSecurityDir(testDir, secureOpts, krbOpts);
            relSecurityDir = new File(secureOpts.getSecurityDir());
            userExternalAuth = secureOpts.getUserExternalAuth();
        }
        int pollPort = pf.getMgmtPollPort();
        int trapPort = pf.getMgmtTrapPort();
        if (mgmtPollPort > 0) {
            pollPort = mgmtPollPort;
        }
        if (mgmtTrapPort > 0) {
            trapPort = mgmtTrapPort;
        }

        TestUtils.generateBootstrapFile(configfile.toString(), testDir,
                                        pf.getHostname(),
                                        pf.getRegistryPort(),
                                        pf.getHaRange(),
                                        pf.getHostname(),
                                        capacity,
                                        memoryMB,
                                        storageDirs,
                                        mgmtImpl,
                                        pollPort,
                                        "localhost",
                                        trapPort,
                                        relSecurityDir,
                                        runBootAdmin,
                                        userExternalAuth,
                                        extraParams);

        TestUtils.generateSecurityPolicyFile
            (secfile.toString(), TestUtils.SEC_POLICY_STRING);
        return testDir;
    }

    public static File generateSecurityDir(String rootDir,
                                           SecureOpts secOpts,
                                           KerberosOpts krbOpts)
        throws Exception {

        File secDir = new File(rootDir, secOpts.getSecurityDir());
        if (secDir.exists()) {
            return secDir;
        }
        TestUtils.generateSecurityDir(secDir, secOpts, krbOpts);
        return secDir;
    }

    /**
     * Start an instance of SNA assuming the bootstrap directory and file have
     * been created.
     */
    public static StorageNodeAgent startSNA(String bootstrapDir,
                                            String bootstrapFile,
                                            boolean useThreads,
                                            boolean createAdmin)
        throws Exception {

        return startSNA(bootstrapDir, bootstrapFile, useThreads, createAdmin,
                        false /* disableServices */);
    }

    public static StorageNodeAgent startSNA(String bootstrapDir,
                                            String bootstrapFile,
                                            boolean useThreads,
                                            boolean createAdmin,
                                            boolean disableServices)
        throws Exception {
        return startSNA(bootstrapDir, bootstrapFile, useThreads, createAdmin,
                        disableServices, null);
    }
    public static StorageNodeAgent startSNA(String bootstrapDir,
                                            String bootstrapFile,
                                            boolean useThreads,
                                            boolean createAdmin,
                                            boolean disableServices,
                                            String restoreSnapshotName)
        throws Exception {
        return startSNA(bootstrapDir, bootstrapFile, useThreads, createAdmin,
                        disableServices, restoreSnapshotName, "true");
    }

    /**
     * Start an instance of SNA assuming the bootstrap directory and file have
     * been created, and specifying whether to disable services.
     */
    public static StorageNodeAgent startSNA(String bootstrapDir,
                                            String bootstrapFile,
                                            boolean useThreads,
                                            boolean createAdmin,
                                            boolean disableServices,
                                            String restoreSnapshotName,
                                            String updateConfig)
        throws Exception {

        final List<String> snaArgs = new ArrayList<String>();
        snaArgs.add(CommandParser.ROOT_FLAG);
        snaArgs.add(bootstrapDir);
        snaArgs.add(StorageNodeAgent.CONFIG_FLAG);
        snaArgs.add(bootstrapFile);
        if (useThreads) {
            snaArgs.add(StorageNodeAgent.THREADS_FLAG);
        }
        if (restoreSnapshotName != null) {
            snaArgs.add(StorageNodeAgent.RESTORE_FROM_SNAPSHOT);
            snaArgs.add(restoreSnapshotName);
            if (updateConfig != null) {
                snaArgs.add(StorageNodeAgent.UPDATE_CONFIG_FLAG);
                snaArgs.add(updateConfig);
            }
        }

        StorageNodeAgentImpl sna = new StorageNodeAgentImpl(createAdmin);
        sna.parseArgs(snaArgs.toArray(new String[snaArgs.size()]));
        /*
         * In testing environments we sometimes run into this exception
         * java.rmi.NoSuchObjectException: no such object in table.
         * cf. [#22835]
         * It appears that retrying several times works around the bug.
         */
        int nretries = 0;
        boolean started = false;
        while (!started) {
            try {
                if (disableServices) {
                    sna.getStorageNodeAgent().disableServices();
                }
                sna.start();
                started = true;
            } catch (IOException e) {
                if (nretries++ > 100 ||
                    !(e instanceof NoSuchObjectException ||
                      e.getCause() instanceof NoSuchObjectException)) {

                    throw e;
                }
            }
        }

        return sna.getStorageNodeAgent();
    }

    public static RepNodeAdminAPI waitForRNAdmin(RepNodeId rnid,
                                                 StorageNodeId snid,
                                                 String storeName,
                                                 String hostname,
                                                 int port,
                                                 int timeout)
        throws Exception {
        ServiceStatus[] target = {ServiceStatus.RUNNING};
        return ServiceUtils.waitForRepNodeAdmin
            (storeName, hostname, port, rnid, snid, NULL_LOGIN_MGR,
             timeout, target, logger);
    }

    /**
     * Wrapper for waitForAdmin that adds timeout and required status.  This
     * version assumes that it will succeed.
     */
    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int port,
                                                 LoginManager loginMgr)
        throws Exception {

        CommandServiceAPI admin = ServiceUtils.waitForAdmin
            (hostname, port, loginMgr, 40, ServiceStatus.RUNNING, logger);
        return admin;
    }

    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int port)
        throws Exception {

        return waitForAdmin(hostname, port, null);
    }

    /**
     * Similar to above but this one may fail and return null.
     */
    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int port,
                                                 int timeout,
                                                 LoginManager loginMgr) {
        try {
            return ServiceUtils.waitForAdmin
                (hostname, port, loginMgr, timeout,
                 ServiceStatus.RUNNING, logger);
        } catch (Exception ignored) {
        }
        return null;
    }

    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int port,
                                                 int timeout) {
        return waitForAdmin(hostname, port, timeout, null);
    }

    protected static void delay(int seconds)
        throws Exception {
        Thread.sleep(seconds*1000);
    }

    /**
     * Security configuration class
     */
    public static class SecureOpts implements Serializable {
        private static final long serialVersionUID = 1;

        /* Whether to make this be a secure install */
        private boolean secure;

        /* Whether SSL is disabled on all transports */
        private boolean nossl;

        /*
         * If secure, optionally set the security directory, relative to
         * kvRoot.
         */
        private String securityDir;

        /*
         * If secure, optionally set enabled user authentication methods.
         */
        private String userExternalAuth;

        /*
         * If secure, optionally set source keystore file name.
         */
        private String keystore = SSLTestUtils.SSL_KS_NAME;

        /*
         * If secure, optionally set source truststore file name.
         */
        private String truststore = SSLTestUtils.SSL_TS_NAME;

        /*
         * If secure, optionally set source client truststore file name.
         */
        private String clientTrust = SSLTestUtils.SSL_CTS_NAME;

        /*
         * If secure, optionally set the name of password file to use to access
         * the keystore.
         */
        private String passwordFile = SSLTestUtils.SSL_PW_NAME;

        public SecureOpts() {
            secure = false;
            securityDir = FileNames.SECURITY_CONFIG_DIR;
        }

        public boolean isSecure() {
            return secure;
        }

        public SecureOpts setSecure(boolean secure) {
            this.secure = secure;
            return this;
        }

        public boolean noSSL() {
            return nossl;
        }

        public SecureOpts setNoSSL(boolean nossl) {
            this.nossl = nossl;
            return this;
        }

        public String getSecurityDir() {
            return securityDir;
        }

        public String getUserExternalAuth() {
            return userExternalAuth;
        }

        public String getSrcKeyStore() {
            return keystore;
        }

        public String getSrcTrustStore() {
            return truststore;
        }

        public String getPasswordFile() {
            return passwordFile;
        }

        public String getClientTrust() {
            return clientTrust;
        }

        public SecureOpts setSecurityDir(String secDir) {
            securityDir = secDir;
            return this;
        }

        public SecureOpts setUserExternalAuth(String userExtAuth) {
            userExternalAuth = userExtAuth;
            return this;
        }

        public SecureOpts setKeystore(String srcKeystore) {
            keystore = srcKeystore;
            return this;
        }

        public SecureOpts setTruststore(String srcTruststore) {
            truststore = srcTruststore;
            return this;
        }

        public SecureOpts setPasswordFile(String passwordFile) {
            this.passwordFile = passwordFile;
            return this;
        }

        public SecureOpts setClientTruststore(String srcTruststore) {
            clientTrust = srcTruststore;
            return this;
        }
    }

    /**
     * Kerberos configuration class.
     */
    public static class KerberosOpts {
        public static String SERVICE_NAME_DEFAULT = "oraclenosql";
        public static String REALM_NAME_DEFAULT = "EXAMPLE.COM";
        public static String KEYTAB_NAME_DEFAULT = "store.keytab";
        public static String KRB_CONF_DEFAULT = "krb5.conf";

        private String realm;
        private String service;
        private String instance;
        private String krbConf;
        private File keytabFile;

        public KerberosOpts() {
            /* Initialize with default value for unit tests */
            realm = REALM_NAME_DEFAULT;
            service = SERVICE_NAME_DEFAULT;
            krbConf = null;
            instance = "";
            keytabFile = null;
        }

        public KerberosOpts(KerberosOpts other) {
            realm = other.realm;
            service = other.service;
            krbConf = other.krbConf;
            instance = other.instance;
            keytabFile = other.keytabFile;
        }

        public KerberosOpts setRealm(String realmName) {
            realm = realmName;
            return this;
        }

        public KerberosOpts setServiceName(String serviceName) {
            service = serviceName;
            return this;
        }

        public KerberosOpts setInstanceName(String instanceName) {
            instance = instanceName;
            return this;
        }

        public KerberosOpts setKrbConf(String krbConfFile) {
            krbConf = krbConfFile;
            return this;
        }

        public KerberosOpts setDefaultKrbConf(String rootDir) {
            if (krbConf == null) {
                File root = new File(rootDir);
                if (!root.exists()) {
                    throw new IllegalArgumentException(
                        "root directory specified does not exist");
                }

                krbConf = root.getAbsolutePath() + File.separator +
                          KRB_CONF_DEFAULT;
            }
            return this;
        }

        public KerberosOpts setKeytab(File keytab) {
            keytabFile = keytab;
            return this;
        }

        public String getRealm() {
            return realm;
        }

        public String getServiceName() {
            return service;
        }

        public String getInstanceName() {
            return instance;
        }

        public String getKrbConf() {
            return krbConf;
        }

        public String getKeytab() {
            return keytabFile == null ?
                   KEYTAB_NAME_DEFAULT :
                   keytabFile.getName();
        }

        public File getKeytabFile() {
            return keytabFile;
        }
    }
}
