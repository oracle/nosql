/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.runners.Parameterized.Parameters;

/**
 * Base class for tests using ClientTestServices.
 */
public class ClientTestBase extends BasicClientTestBase {

    static List<String> EMPTY_LIST = Collections.emptyList();
    protected static final Properties SECURE_LOGIN_PROPS = new Properties();
    protected static final String AUTOLOGIN_FILE_PATH =
        TestUtils.getTestDir() + File.separator + "autologin";

    protected KVStore store;
    protected KVStoreConfig config;
    protected final boolean secure;

    /** Controls whether {@link #setUp} should start services. */
    protected boolean startServices = true;

    private ClientTestServicesRunner servicesRunner;

    static {
        SECURE_LOGIN_PROPS.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                               "testuser");
        SECURE_LOGIN_PROPS.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                               TestUtils.getTestDir() + File.separator +
                               "pwdfile");
        if (TestUtils.isSSLDisabled()) {
            SECURE_LOGIN_PROPS.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                                   "clear");
        } else {
            SECURE_LOGIN_PROPS.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                                   KVSecurityConstants.SSL_TRANSPORT_NAME);
            SECURE_LOGIN_PROPS.put(
                KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                TestUtils.getTestDir() + File.separator +
                FileNames.SECURITY_CONFIG_DIR + File.separator +
                SSLTestUtils.SSL_CTS_NAME);
        }
    }

    /**
     * Provides the default parameters for use by subclasses that are tests
     * parameterized to run in secure and non-secure modes.
     */
    @Parameters(name="store_secure={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    /**
     * Returns a ClientTestBase with non-secured store.
     */
    public ClientTestBase() {
        this.secure = false;
    }

    /**
     * Returns a ClientTestBase with a store configured by secure flag.
     */
    public ClientTestBase(boolean secure) {
        this.secure = secure;
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        if (startServices) {
            open();
        }
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        close();
    }

    /* Exists so subclasses can customize the base configuration */
    void customizeConfig() {
    }

    /* Opens a store. */
    protected void open()
        throws IOException {

        startServices();
        createKVStore();
    }

    /** Creates a KVStore instance */
    protected void createKVStore() throws IOException {
        customizeConfig();
        if (secure) {
            final File pwdfile = new File(
                SECURE_LOGIN_PROPS.getProperty(
                    KVSecurityConstants.AUTH_PWDFILE_PROPERTY));

            assertTrue(pwdfile.exists());
            config.setSecurityProperties(SECURE_LOGIN_PROPS);

            /* Create the autologin file */
            final File autoLoginFile = new File(AUTOLOGIN_FILE_PATH);
            if (autoLoginFile.exists()) {
                autoLoginFile.delete();
            }
            SECURE_LOGIN_PROPS.store(new FileWriter(autoLoginFile), null);
            assertTrue(autoLoginFile.exists());
        }
        store = KVStoreFactory.getStore(config);
    }

    /**
     * Starts KVStore services in ClientTestServices process and initializes
     * KVStoreConfig (config field).
     */
    protected void startServices()
        throws IOException {

        startServices(secure ?
                      Collections.singletonList("-security") :
                      EMPTY_LIST);
    }

    /**
     * Starts KVStore services in ClientTestServices process and initializes
     * KVStoreConfig (config field).
     *
     * @param testCommands - Additional commands passed to the
     * ClientTestServices process.  Supported commands are documented at
     * ClientTestServices#main.
     */
    protected void startServices(List<String> testCommands)
        throws IOException {

        servicesRunner =
            new ClientTestServicesRunner(kvstoreName, testCommands);
        config = servicesRunner.getConfig();
    }

    protected void close()
        throws Exception {

        try {
            TestUtils.closeAll(store, servicesRunner);
        } finally {
            store = null;
            servicesRunner = null;
        }
    }

    /**
     * Check that the RMI Registry's lookup method is not called if async is
     * enabled.
     */
    public void checkRMIRegistryLookup() {
        if (!KVStoreConfig.getDefaultUseAsync() ||
            !RegistryUtils.useMiscAsyncServices) {
            return;
        }

        final AtomicReference<IllegalStateException> exception =
            new AtomicReference<>();

        RegistryUtils.registryLookupHook = (ignore) ->
            exception.set(
                new IllegalStateException("Unexpected call to RMI registry"));

        tearDowns.add(() -> {
                RegistryUtils.registryLookupHook = null;
                if (exception.get() != null) {
                    throw exception.get();
                }
            });
    }
}
