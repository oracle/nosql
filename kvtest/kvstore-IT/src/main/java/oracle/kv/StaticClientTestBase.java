/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import oracle.kv.impl.api.ops.ClientTestServicesRunner;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.table.Table;

/**
 * This class provides static methods and utility methods for client
 * tests that want to use the pattern of:
 * 1. create a store (shared by all test cases in the class)
 * 2. for each test:
 *  a) clear the store of all tables and data
 *  b) run the test
 *  c) clear again
 *
 * This provides the same basic framework as when a new store is created
 * for each test case but in the cases where there is not a lot of data it
 * is much faster.
 *
 * Classes that extend this class must implement @BeforeClass and
 * @AfterClass methods that call staticSetUp() and staticTearDown(),
 * respectively, to do the store creation and destruction.
 *
 * In order to run on a secure store the second parameter to staticSetUp()
 * should be true.
 *
 * The default non-static setUp() and tearDown() methods clear data and are
 * sufficient for most needs.
 */
public class StaticClientTestBase extends TestBase {

    protected static ClientTestServicesRunner runner;
    protected static KVStore store;
    static List<String> EMPTY_LIST = Collections.emptyList();
    protected static final Properties SECURE_LOGIN_PROPS = new Properties();
    protected static final String AUTOLOGIN_FILE_PATH =
        TestUtils.getTestDir() + File.separator + "autologin";

    static {
        SECURE_LOGIN_PROPS.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                               KVSecurityConstants.SSL_TRANSPORT_NAME);
        SECURE_LOGIN_PROPS.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                               TestUtils.getTestDir() + File.separator +
                               FileNames.SECURITY_CONFIG_DIR + File.separator +
                               SSLTestUtils.SSL_CTS_NAME);
        SECURE_LOGIN_PROPS.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                               "testuser");
        SECURE_LOGIN_PROPS.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                               TestUtils.getTestDir() + File.separator +
                               "pwdfile");
    }

    /**
     * @param storeName a store name that will not collide with other tests
     * @param secure if true will create a secure store for the tests
     */
    public static void staticSetUp(String storeName,
                                   boolean secure)
        throws Exception {
        if (runner != null) {
            runner.close();
            runner = null;
        }

        runner = new ClientTestServicesRunner(
            storeName,
            (secure ?
             Collections.singletonList("-security") : EMPTY_LIST));

        KVStoreConfig config = runner.getConfig();
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

    public static void staticTearDown()
        throws Exception {
        try {
            TestUtils.closeAll(store, runner);
        } finally {
            store = null;
            runner = null;
        }
    }

    @Override
    public void setUp()
        throws Exception {

        removeAllTables();
    }

    @Override
    public void tearDown()
        throws Exception {

        removeAllTables();
    }

    /* Warning - do not use if test is using tables */
    protected void removeAllData() {
        Iterator<Key> iter = store.storeKeysIterator
            (Direction.UNORDERED, 10);
        try {
            while (iter.hasNext()) {
                store.delete(iter.next());
            }
        } catch (Exception e) {
            System.err.println("Exception cleaning store: " + e);
        }
    }

    protected void removeAllTables()
        throws Exception {

        if (store != null) {
            removeTables(store.getTableAPI().getTables());
        }
    }

    protected void removeTables(Map<String, Table> tables) {

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getValue().getChildTables().size() > 0) {
                removeTables(entry.getValue().getChildTables());
            }
            TableImpl table = (TableImpl)entry.getValue();

            if (table.isSystemTable()) {
                continue;
            }
            String name = entry.getValue().getFullName();
            String drop = "drop table if exists " + name;
            executeDdl(drop);
        }
    }

    protected void executeDdl(String statement) {
        StatementResult res = store.executeSync(statement);
        assertTrue(res.isSuccessful());
    }
}
