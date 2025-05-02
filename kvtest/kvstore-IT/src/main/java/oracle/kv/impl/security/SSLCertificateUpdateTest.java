/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.kv.Durability;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.Value;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore.SecureUser;

public class SSLCertificateUpdateTest extends SecureTestBase {
    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(USER_NAME, USER_PW, true /* admin */));
        numSNs = 1;

        /* Start with a merged trust files, skip the merge-trust process */
        secureOpts = new SecureOpts().setSecure(true).
            setKeystore(SSLTestUtils.SSL_KS_NAME).
            setTruststore(SSLTestUtils.SSL_MERGED_TS_NAME);
        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    /**
     * Notes: This test did not call super setUp method to clean test directory.
     */
    @Override
    public void setUp()
        throws Exception {

        /*
         * The CreateStore model executes the SNA in the same process as
         * the client.  When we run a tests that deliberately botches the
         * client transport config, we need to repair that config before
         * running additional tests.
         */
        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testBasic()
        throws Exception {

        grantRoles(USER_NAME, "readwrite");

        TestUtils.copyFile(SSLTestUtils.SSL_CTS_NAME,
                           SSLTestUtils.getTestSSLDir(),
                           FileNames.CLIENT_TRUSTSTORE_FILE,
                           TestUtils.getTestDir());

        final LoginCredentials creds =
                new PasswordCredentials(USER_NAME,USER_PW.toCharArray());
        final Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        final String clientTrust = new File(
            TestUtils.getTestDir(), FileNames.CLIENT_TRUSTSTORE_FILE).
            getAbsolutePath();
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  clientTrust);
        final KVStoreConfig kvConfig =
                new KVStoreConfig(createStore.getStoreName(),
                                  HOST + ":" + registryPorts[0]);
        kvConfig.setSecurityProperties(props);

        /* Implement ReauthenticationHandler in case authentication failed */
        try (KVStore store = KVStoreFactory.getStore(kvConfig, creds,
                new ReauthenticateHandler() {
                @Override
                public void reauthenticate(KVStore kstore) {
                    kstore.login(creds);
                }
            })) {
            Key aKey = Key.createKey("foo");
            store.put(aKey, Value.createValue(new byte[0]), null,
                        Durability.COMMIT_SYNC, 0, null);
            assertNotNull(store.get(aKey));

            /* Replace keystore file in security directory */
            TestUtils.copyFile(SSLTestUtils.SSL_NEW_KS_NAME,
                               SSLTestUtils.getTestSSLDir(),
                               FileNames.KEYSTORE_FILE,
                               new File(createStore.getRootDir(),
                                        secureOpts.getSecurityDir()));

            /* Replace client trust file contains new certificate */
            TestUtils.copyFile(SSLTestUtils.SSL_MERGED_CTS_NAME,
                    SSLTestUtils.getTestSSLDir(),
                    FileNames.CLIENT_TRUSTSTORE_FILE,
                    TestUtils.getTestDir());

            /* Restart SNA to load new key store */
            createStore.shutdownSNA(0, false /* force */);

            /*
             * Do not use the awaitServices facility, because CreateStore is
             * executed in the same process as the client. The previous opened
             * KVStore handle has configured client RMI policy that cause the
             * await service use the registry initialized using old trust store.
             */
            createStore.startSNA(0, false /* disableServices */,
                                 false /* awaitServices*/);

            /*
             * Use store.get as both a test case and a way to know when the
             * newly-restarted RN is available. This avoids a random sleep,
             * waiting for the RN.
             */
            int count = 0;
            while (true) {
                try {
                    store.get(aKey);
                    break;
                } catch (Exception e) {
                    if (count > 20) {
                        throw e;
                    }
                    count++;
                    Thread.sleep(3000);
                }
            }
       }
    }
}
