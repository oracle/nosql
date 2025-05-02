/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.kerberos;

import static oracle.kv.impl.param.ParameterState.GP_LOGIN_CACHE_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_EXTEND_ALLOW;
import static oracle.kv.impl.param.ParameterState.GP_SESSION_TIMEOUT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KerberosCredentials;
import oracle.kv.Key;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.util.StorageNodeUtils.KerberosOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a basic test to make sure that a Kerberos KVStore configuration
 * can be created and accessed, as well as a variety of error cases.
 */
public class SecureKerberosTest extends KerberosTestBase {

    private static final String ADMIN_NAME = "admin";
    private static final String ADMIN_PW = "NoSql00__7654321";
    private static final String KRB_USER_NAME = "krbuser";
    private static final String SERVICE_PRINCIPAL = "oraclenosql";
    private static final String JAAS_LOGIN_CONF_ENTRY = "nosqlLogin";
    private static final File KRB_USER_KEYTAB =
        new File(TestUtils.getTestDir(), KRB_USER_NAME + ".keytab");

    private static KVStore adminStore;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        TestUtils.clearTestDirectory();

        startKdc();

        /* Copy kdc generated krb5.conf to test directory */
        TestUtils.copyFile("krb5.conf",
                           TestUtils.getTestKdcDir(),
                           TestUtils.getTestDir());
        File tmpKeytab = new File(TestUtils.getTestDir(),
                                  KerberosOpts.KEYTAB_NAME_DEFAULT);
        addPrincipal(tmpKeytab, SERVICE_PRINCIPAL);

        /* Add the first admin user */
        users.add(new SecureUser(ADMIN_NAME, ADMIN_PW, true /* admin */));

        /* Enable Kerberos as authentication method */
        userExternalAuth = "KERBEROS";
        krbOpts = new KerberosOpts[] {
            new KerberosOpts().setKeytab(tmpKeytab)
        };

        startKVStore();

        /* Add a Kerberos user*/
        adminStore = loginKVStoreUser(ADMIN_NAME, ADMIN_PW);
        addExternalUser(adminStore, KRB_USER_NAME + "@" + getRealm());
        addPrincipal(KRB_USER_KEYTAB, KRB_USER_NAME);
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
        stopKdc();
        deleteKdcLog();
    }

    @Override
    public void setUp() {
    }

    @Override
    public void tearDown() {
    }

    /**
     * This tests a successful login/logout sequence
     */
    @Test
    public void testLogin() throws Exception {
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = createDefaultSecProperties();
        kvConfig.setSecurityProperties(props);
        KVStore store = KVStoreFactory.getStore(kvConfig);
        store.logout();
        store.close();
    }

    /**
     * This tests a successful login/logout sequence while using admin-only SN
     * as helper host
     */
    @Test
    public void testLoginAdminOnlySN() throws Exception {
        krbOpts = new KerberosOpts[] {krbOpts[0], krbOpts[0]};
        createStore.setKrbOpts(krbOpts);
        createStore.setAddSNToInitiallySingleSNStore();
        int portNo = createStore.addSN(1, true);
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + portNo);
        Properties props = createDefaultSecProperties();
        kvConfig.setSecurityProperties(props);
        KVStore store = KVStoreFactory.getStore(kvConfig);
        store.logout();
        store.close();
    }

    @Test
    public void testReauthenticate()
        throws Exception {

        try {
            grantRoles(KRB_USER_NAME + "@" + getRealm(), "readwrite");

            /*
             * Start by changing authentication policy to have very short
             * timeouts, and to not allow session extension.
             */
            setAuthPolicy(GP_SESSION_TIMEOUT + "=" + "5 SECONDS" +
                          ";" + GP_LOGIN_CACHE_TIMEOUT + "=" + "1 SECONDS" +
                          ";" + GP_SESSION_EXTEND_ALLOW +"=" + "false");

            KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + registryPorts[0]);
            Properties props = createDefaultSecProperties();
            kvConfig.setSecurityProperties(props);

            final KerberosCredentials creds =
                new KerberosCredentials(KRB_USER_NAME, props);
            KrbReauth reauth = new KrbReauth(creds);
            KVStore store = KVStoreFactory.getStore(kvConfig, creds, reauth);
            Key aKey = Key.createKey("foo");
            store.put(aKey, Value.createValue(new byte[0]));
            Key lobKey = Key.createKey("foo" + kvConfig.getLOBSuffix());
            store.putLOB(lobKey, new ByteArrayInputStream(
                new byte[1000]), null, 0, null);

            /*
             * Test to be sure that re-authentication happens transparently
             */
            for (int i = 0; i < 30; i++) {
                ValueVersion vv = store.get(aKey);
                assertNotNull(vv);

                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertTrue(reauth.completed > 0);

            /*
             * Test to be sure that re-authentication works for LOB API.
             */
            reauth.reset(creds);
            for (int i = 0; i < 30; i++) {
                InputStreamVersion isv = store.getLOB(lobKey, null, 0, null);
                assertNotNull(isv);

                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertTrue(reauth.completed > 0);

            /*
             * Test what happen when re-authentication fails.
             */

            /* Make Kerberos login properties contains error entries */
            Properties badProps = new Properties();
            badProps.setProperty(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                                 HOST + ":" + SERVICE_PRINCIPAL);

            /* Make a fake keytab that does not have any keys */
            File badKeytab = new File(TestUtils.getTestDir(), "badkeytab");
            assertTrue(badKeytab.createNewFile());
            badProps.put(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
                         badKeytab.getAbsolutePath());
            badProps.put(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                         getRealm());
            KerberosCredentials badCreds =
                new KerberosCredentials(KRB_USER_NAME, badProps);
            reauth.reset(badCreds);

            for (int i = 0; i < 30; i++) {
                try {
                    ValueVersion vv = store.get(aKey);
                    assertNotNull(vv);
                } catch (AuthenticationRequiredException are) {
                    break;
                }
                if (reauth.attempts > 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
            assertTrue(reauth.attempts > 0);
            assertTrue(reauth.completed == 0);
            assertTrue(reauth.afeCount > 0);

            /* Test to be sure that we get ARE when re-authentication fails */
            reauth.reset(badCreds);
            try {
                store.get(aKey);
                fail("Expected ARE");
            } catch (AuthenticationRequiredException are) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
            assertTrue(reauth.afeCount > 0);

            /*
             * Now, tell the reauthenticationHandler to squash AFE so that
             * it pretends to succeed.  We should get ARE here too.
             */
            reauth.reset(badCreds);
            reauth.setSquashAFE(true);

            try {
                store.get(aKey);
                fail("Expected ARE");
            } catch (AuthenticationRequiredException are) {
                /* ignore */
            }
            assertTrue(reauth.afeCount > 0);

            /* Return the store to working order */
            store.login(creds);
            ValueVersion vv = store.get(aKey);
            assertNotNull(vv);
            store.logout();
            store.close();
        } finally {
            revokeRoles(KRB_USER_NAME + "@" + getRealm(), "readwrite");
        }
    }

    @Test
    public void testLoginWithJAAS()
        throws Exception {

        try {
            Map<String,String> krbOptions = new HashMap<String,String>();
            krbOptions.put("doNotPrompt", "true");
            krbOptions.put("useKeyTab", "true");
            krbOptions.put("keyTab", KRB_USER_KEYTAB.getAbsolutePath());
            krbOptions.put("storeKey", "true");
            krbOptions.put("principal", KRB_USER_NAME);
            AppConfigurationEntry ace = new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                LoginModuleControlFlag.REQUIRED,
                krbOptions);
            DynamicConfiguration dynConf =
                new DynamicConfiguration(new AppConfigurationEntry[]{ ace });
            Configuration.setConfiguration(dynConf);

            LoginContext lc = null;
            final Subject subj = new Subject();
            try {
                lc = new LoginContext("client", subj, null, dynConf);
                lc.login();
            } catch (LoginException le) {
                fail("Cannot create LoginContext, " + le.getMessage());
            } catch (SecurityException se) {
                fail("Cannot create LoginContext, " + se.getMessage());
            }

            Properties krbProperties = new Properties();
            krbProperties.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                              HOST + ":" + SERVICE_PRINCIPAL);
            krbProperties.put(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                              getRealm());
            krbProperties.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY,
                              "true");
            KerberosContext.runWithContext(
                subj, new GetStoreAction(krbProperties));
        } finally {
            Configuration.setConfiguration(null);
        }
    }

    @Test
    public void testLoginWithJAASLoginFile()
        throws Exception {

        File jaasLoginFile = createLoginConfigFile();

        /* Set JAAS login file via system property */
        try {
            System.setProperty("java.security.auth.login.config",
                               jaasLoginFile.getAbsolutePath());
            LoginContext lc = null;
            Subject subj = new Subject();
            lc = new LoginContext(JAAS_LOGIN_CONF_ENTRY, subj);
            lc.login();

            Properties krbProperties = new Properties();
            krbProperties.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                              HOST + ":" + SERVICE_PRINCIPAL);
            krbProperties.put(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                              getRealm());
            krbProperties.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY,
                              "true");

            /* Specify error JAAS login configuration entry name */
            krbProperties.put(KVSecurityConstants.JAAS_LOGIN_CONF_NAME,
                              "errorEntryName");
            try {
                KerberosContext.runWithContext(
                    subj, new GetStoreAction(krbProperties));
                fail("error entry name");
            } catch (IllegalStateException ise) {
                Throwable cause = ise.getCause();
                assertTrue(cause instanceof IllegalArgumentException);
                assertThat("cannot find speicified entry", cause.getMessage(),
                    containsString("Cannot find a JAAS configuration entry"));
            }

            /* Revert to correct entry name */
            krbProperties.put(KVSecurityConstants.JAAS_LOGIN_CONF_NAME,
                              JAAS_LOGIN_CONF_ENTRY);
            KerberosContext.runWithContext(
                subj, new GetStoreAction(krbProperties));

            /*
             * Test login without subject. Kerberos credentials subject have
             * won't be passed in, but client code will use JAAS login file to
             * login and acquire credential internally.
             */
            getStoreWithKrbCreds(krbProperties);
        } finally {
            /* cleanup that remove JAAS login file configuration */
            System.getProperties().remove("java.security.auth.login.config");

            /* cleanup runtime configuration */
            Configuration.setConfiguration(null);
        }
    }

    @Test
    public void testLoginWithErrorJAASConfig()
        throws Exception {

        File jaasLoginFile = createErrorLoginConfigFile();

        /* Set JAAS login file via system property */
        try {
            System.setProperty("java.security.auth.login.config",
                               jaasLoginFile.getAbsolutePath());
            LoginContext lc = null;
            Subject subj = new Subject();
            try {
                lc = new LoginContext(JAAS_LOGIN_CONF_ENTRY, subj);
                lc.login();
                fail("Login failed");
            } catch (LoginException le) {
                assertThat("unable to obtain password", le.getMessage(),
                    containsString("Unable to obtain password from user"));
            }

            Properties krbProperties = new Properties();
            krbProperties.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                              HOST + ":" + SERVICE_PRINCIPAL);
            krbProperties.put(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                              getRealm());
            krbProperties.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY,
                              "true");
            krbProperties.put(KVSecurityConstants.JAAS_LOGIN_CONF_NAME,
                              JAAS_LOGIN_CONF_ENTRY);
            try {
                KerberosContext.runWithContext(
                    subj, new GetStoreAction(krbProperties));
                fail("Login failed");
            } catch (AuthenticationFailureException afe) {
                assertThat("unable to obtain password", afe.getMessage(),
                    containsString("Unable to obtain password from user"));
            }

            try {
                getStoreWithKrbCreds(krbProperties);
                fail("Login failed");
            } catch (AuthenticationFailureException afe) {
                assertThat("unable to obtain password", afe.getMessage(),
                    containsString("Unable to obtain password from user"));
            }
        } finally {
            /* cleanup that remove JAAS login file configuration */
            System.getProperties().remove("java.security.auth.login.config");

            /* cleanup runtime configuration */
            Configuration.setConfiguration(null);
        }
    }

    @Test
    public void testPrincipalNames() {
        KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        Properties props = new Properties();
        addTransportProps(props);

        /* Using principal short name, must specify realm name */
        props.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY, KRB_USER_NAME);
        props.put(KVSecurityConstants.AUTH_EXT_MECH_PROPERTY, "KERBEROS");
        props.put(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
                  KRB_USER_KEYTAB.getAbsolutePath());
        try {
            kvConfig.setSecurityProperties(props);
            KVStoreFactory.getStore(kvConfig);
            fail("Expect IllegalStateException");
        } catch (IllegalStateException ise) {
            Throwable cause = ise.getCause();
            assertTrue(cause instanceof IllegalArgumentException);
            assertThat("must specify realm", cause.getMessage(),
                containsString("realm name must be specified"));
        }

        /* Using full principal name without specifying realm name */
        props.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                  KRB_USER_NAME + "@" + getRealm());
        kvConfig.setSecurityProperties(props);
        KVStore store = KVStoreFactory.getStore(kvConfig);
        store.logout();
        store.close();
    }

    /* Create the default test properties for security configuration */
    private Properties createDefaultSecProperties() {
        Properties result = new Properties();
        result.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY, KRB_USER_NAME);
        result.put(KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY,
                   HOST + ":" + SERVICE_PRINCIPAL);
        result.put(KVSecurityConstants.AUTH_EXT_MECH_PROPERTY, "KERBEROS");
        result.put(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                   getRealm());
        result.put(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
                   KRB_USER_KEYTAB.getAbsolutePath());
        result.put(KVSecurityConstants.AUTH_KRB_MUTUAL_PROPERTY, "true");
        addTransportProps(result);

        return result;
    }

    /* Create a JAAS login configuration file */
    private File createLoginConfigFile()
        throws Exception {

        File loginFile = File.createTempFile("login", ".conf");
        try (PrintWriter writer = new PrintWriter(loginFile)) {
            String jaasLoginConf = String.format(
                "%s {\n" +
                "com.sun.security.auth.module.Krb5LoginModule required\n" +
                "useKeyTab=true\n" +
                "keyTab=\"%s\"\n" +
                "storeKey=true\n" +
                "principal=%s\n" +
                "doNotPrompt=true;\n};",
                JAAS_LOGIN_CONF_ENTRY,
                /* replace windows file separator */
                KRB_USER_KEYTAB.getAbsolutePath().replace('\\', '/'),
                KRB_USER_NAME);
            writer.println(jaasLoginConf);
        }
        return loginFile;
    }

    /*
     * Create an error JAAS login configuration file that contains a
     * non-existent keytab file
     */
    private File createErrorLoginConfigFile()
        throws Exception {

        File loginFile = File.createTempFile("login", ".conf");
        try (PrintWriter writer = new PrintWriter(loginFile)) {
            String jaasLoginConf = String.format(
                "%s {\n" +
                "com.sun.security.auth.module.Krb5LoginModule required\n" +
                "useKeyTab=true\n" +
                "keyTab=\"%s\"\n" +
                "storeKey=true\n" +
                "principal=%s\n" +
                "doNotPrompt=true;\n};",
                JAAS_LOGIN_CONF_ENTRY,
                "nonExistent",
                KRB_USER_NAME);
            writer.println(jaasLoginConf);
        }
        return loginFile;
    }

    /**
     * A simple re-authentication handler that provides some tracking
     * information so that we can see whether it gets called.
     */
    private static class KrbReauth implements ReauthenticateHandler {
        private int attempts;
        private int completed;
        private int afeCount;
        private KerberosCredentials creds;
        private boolean squashAFE;

        private KrbReauth(KerberosCredentials creds) {
            reset(creds);
        }

        private void setSquashAFE(boolean squashIt) {
            squashAFE = squashIt;
        }

        private void reset(KerberosCredentials newCreds) {
            attempts = 0;
            completed = 0;
            afeCount = 0;
            creds = newCreds;
            squashAFE = false;
        }

        @Override
        public void reauthenticate (KVStore store) {
            attempts++;
            try {
                store.login(creds);
                completed++;
            } catch (AuthenticationFailureException afe) {
                afeCount++;
                if (!squashAFE) {
                    throw afe;
                }
            }
        }
    }

    private static class GetStoreAction implements Callable<Void> {

        private Properties krbProperties;

        GetStoreAction(Properties krbProperties) {
            this.krbProperties = krbProperties;
        }

        @Override
        public Void call() throws Exception {
            getStoreWithKrbCreds(krbProperties);
            return null;
        }
    }

    private static void getStoreWithKrbCreds(Properties krbProperties)
        throws Exception {

        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);

        /* security properties contains required SSL properties */
        final Properties securityProps = new Properties();
        addTransportProps(securityProps);
        kvConfig.setSecurityProperties(securityProps);

        final KerberosCredentials krbCreds =
            new KerberosCredentials(KRB_USER_NAME, krbProperties);
        final KVStore store =
            KVStoreFactory.getStore(kvConfig, krbCreds, null);
        store.logout();
        store.close();
    }

    private static class DynamicConfiguration extends Configuration {
        private AppConfigurationEntry[] ace;

        DynamicConfiguration(AppConfigurationEntry[] ace) {
            this.ace = ace;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            return ace;
        }
    }
}
