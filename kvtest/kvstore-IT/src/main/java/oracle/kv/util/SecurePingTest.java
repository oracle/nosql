/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.util.Properties;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVVersion;
import oracle.kv.impl.security.SecureTestBase;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Check that Ping can run against with a secure store
 */

public class SecurePingTest extends SecureTestBase {
    private final String TEST_RELEASE_ID = "sn-release-id";

    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(USER_NAME, USER_PW, true /* admin */));
        SecureTestBase.useThreads = true;
        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
        StorageNodeStatus.setTestKVVersion(null);
    }

    /**
     * Note: TestBase.setup clears the unit test sandbox, which has been
     * initialized with a secure store. Refrain from doing this routine setup,
     * or we'll lose all the setup done in staticSetup(). TODO: why are
     * the contents of staticSetup() done there, rather here in setUp?
     */
    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    /**
     * This tests the execution of the ping command against a secure store.
     */
    @Test
    public void testSecureAccess()
        throws Exception {

        /* An insecure ping against a secure store should fail */
        String output = PingUtils.doPing(logger, "-helper-hosts",
                                         getHelperHosts(), "-json-v1",
                                         "-no-exit", "-hidden");
        logger.fine(output);
        PingUtils.checkResult(output, Ping.ExitCode.EXIT_USAGE,
                              ErrorMessage.NOSQL_5100);

        /*
         * Create a client security file and a password store for doing a
         * secure ping.
         */
        final File testDir = TestUtils.getTestDir();
        final File passwordFile = new File(testDir, "test.passwd");
        TestUtils.makePasswordFile(passwordFile, USER_NAME, USER_PW);

        Properties props = new Properties();
        props.put(KVSecurityConstants.AUTH_USERNAME_PROPERTY, USER_NAME);
        props.put(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                  passwordFile.toString());
        addTransportProps(props);
        props.put(KVSecurityConstants.CMD_PASSWORD_NOPROMPT_PROPERTY, "true");
        final File propFile = new File(testDir, "props.security");
        ConfigUtils.storeProperties(props, "test properties", propFile);

        /* Inject a faked release id to SN for testing [#25345] */
        KVVersion snKVVersion = new KVVersion(12, 1, 4, 3, 3, null);
        snKVVersion.setReleaseId(TEST_RELEASE_ID);
        StorageNodeStatus.setTestKVVersion(snKVVersion);

        /* Call ping with proper credentials, should succeed. */
        output = PingUtils.doPing(logger, "-helper-hosts", getHelperHosts(),
                                  "-json-v1","-no-exit", "-hidden",
                                  "-security", propFile.toString(),
                                  "-username", USER_NAME);
        assertThat("Release Id", output, containsString(TEST_RELEASE_ID));

        PingUtils.checkResult(output, Ping.ExitCode.EXIT_OK,
                              ErrorMessage.NOSQL_5000);
        PingUtils.checkJsonOutput(output, getTopology(),
                                  createStore.getAdmin().getParameters(),
                                  false);

        /*
         * Try running ping with a username that doesn't have a password in
         * password storage should fail.
         */
        output = PingUtils.doPing(logger, "-helper-hosts", getHelperHosts(),
                                  "-json-v1","-no-exit",
                                  "-hidden", "-security",
                                  propFile.toString(), "-username", "BadUser");
        PingUtils.checkResult(output, Ping.ExitCode.EXIT_USAGE,
                              ErrorMessage.NOSQL_5100);
    }
}
