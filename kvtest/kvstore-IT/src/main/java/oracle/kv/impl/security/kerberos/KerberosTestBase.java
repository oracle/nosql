/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.DDLTestUtils;

import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Kerberos test base class implemented on Apache Kerby.
 */
public class KerberosTestBase extends SecureTestBase {

    private static final File KDC_LOG =
        new File(TestUtils.getTestDir(), "kdc.log");
    private static SimpleKdcServer kdc;
    protected static File kdcDir;
    protected static Properties conf;

    /**
     * To customize the MiniKdc properties, subclass should initialize this
     */
    protected static Properties userConf;

    protected static void startKdc() throws Exception {
        kdcDir = TestUtils.getTestKdcDir();
        if (!kdcDir.exists()) {
            kdcDir.mkdir();
        }
        assertTrue(KDC_LOG.createNewFile());

        /* redirect KDC log to test destination directory */
        @SuppressWarnings("unchecked")
        final List<Logger> loggers =
            Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        final PatternLayout layout = new PatternLayout();
        layout.setConversionPattern("%d{DATE} %-4r [%t] %-5p %c %x - %m%n");
        final FileAppender appender =
            new FileAppender(layout, KDC_LOG.getAbsolutePath());

        for (Logger logger : loggers) {
            /* Only use designated file to record kdc log */
            logger.removeAllAppenders();
            logger.addAppender(appender);
        }
        kdc = new SimpleKdcServer();
        FreePortLocator fpl = new FreePortLocator("localhost", 29000, 29100);
        int serverPort = fpl.next();
        kdc.setKdcHost("localhost");
        kdc.setAllowUdp(false);
        kdc.setAllowTcp(true);
        kdc.setKdcTcpPort(serverPort);
        kdc.setWorkDir(kdcDir);
        kdc.init();
        kdc.start();
    }

    protected static void stopKdc()
        throws Exception {

        if (kdc != null) {
            /* stop method will delete kdc directory */
            kdc.stop();
        }
        /* redirect Kdc log to test destination directory */
        @SuppressWarnings("unchecked")
        final List<Logger> loggers =
            Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.removeAllAppenders();
        }
    }

    protected static void deleteKdcLog() {
        assertTrue(KDC_LOG.delete());
    }

    /**
     * Add a principal with its name and password.
     */
    protected static void addPrincipal(String princName, String pwd)
        throws Exception {

        kdc.createPrincipal(princName, pwd);
    }

    /**
     * Add multiple principals and extract their keys to a keytab file.
     */
    protected static void addPrincipal(File keytabFile, String ... principals)
        throws Exception {

        for (String principal : principals) {
            String generatedPassword = UUID.randomUUID().toString();
            kdc.createPrincipal(principal, generatedPassword);
            kdc.exportPrincipal(principal, keytabFile);
        }
    }

    protected static SimpleKdcServer getKdc() {
        return kdc;
    }

    protected static String getRealm() {
        return kdc.getKdcConfig().getKdcRealm();
    }

    protected Properties getConf() {
        return conf;
    }

    protected SecurityParams makeSecurityParams(File secDir,
                                                File keytab,
                                                String instance) {
        final SecurityParams secParams = new SecurityParams();
        secParams.setConfigDir(secDir);
        File krb5Config = new File(TestUtils.getTestKdcDir(), "krb5.conf");
        assertTrue(krb5Config.exists());
        secParams.setKerberosConfFile(krb5Config.getAbsolutePath());
        secParams.setKerberosInstanceName(instance);
        secParams.setKerberosRealmName(getRealm());
        secParams.setKerberosKeytabFile(keytab.getName());
        return secParams;
    }

    /**
     * Log this user into a secured store.
     */
    protected static KVStore loginKVStoreUser(String userName, String password) {
        LoginCredentials creds =
                new PasswordCredentials(userName, password.toCharArray());
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              createStore.getHostname() + ":" +
                              createStore.getRegistryPort());
        kvConfig.setCheckInterval(1, TimeUnit.SECONDS);
        Properties props = new Properties();
        createStore.addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        return KVStoreFactory.getStore(kvConfig, creds, null);
    }

    protected static void addExternalUser(KVStore store, String userName)
        throws Exception {

        DDLTestUtils.execStatement(
            store, "CREATE USER \"" + userName + "\" IDENTIFIED EXTERNALLY");
    }
}
