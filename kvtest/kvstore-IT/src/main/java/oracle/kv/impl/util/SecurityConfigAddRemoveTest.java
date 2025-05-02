/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test the securityconfig add-security and remove-security command-line
 * interface.
 */
public class SecurityConfigAddRemoveTest extends SecurityConfigTestBase {

    private File testRoot = null;
    private final String existingFile = "aFile";
    private final String nonExistingFile = "notAFile";
    private final String aSecurityConfig = "securityx";
    private final String aBootConfig = "configx.xml";
    private final String invalidConfig = "invalidConfig.xml";
    private final String emptySecDir = "emptySecDir";
    private CreateStore createStore = null;

    TestPasswordReader pwReader = new TestPasswordReader(null);

    @Override
    public void setUp() throws Exception {

        super.setUp();
        testRoot = new File(TestUtils.getTestDir(), "testroot");
        removeDirectory(testRoot, 22);
        testRoot.mkdir();

        new File(testRoot, existingFile).createNewFile();
        new File(testRoot, invalidConfig).createNewFile();
        new File(testRoot, emptySecDir).mkdir();

        makeSecurityConfig(testRoot, new File(aSecurityConfig));
        makeBootConfig(new File(testRoot, aBootConfig), aSecurityConfig);
    }

    @Override
    public void tearDown() throws Exception {

        if (createStore != null) {
            createStore.shutdown(true);
        }
        super.tearDown();
        removeDirectory(testRoot, 22);
    }

    /**
     * Test addition error conditions.
     */
    @Test
    public void testAddWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        "/nonexistent/directory"});

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        new File(testRoot, existingFile).
                                        getPath()});

        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * config.xml not a relative path name
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        new File(testRoot, existingFile).
                                        getAbsolutePath()});
        assertTrue(s.indexOf("must be a relative file name") != -1);

        /**
         * config.xml not in root dir
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        nonExistingFile});
        assertTrue(s.indexOf("does not exist in") != -1);

        /**
         * config.xml not a file
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aSecurityConfig});
        assertTrue(s.indexOf("is not a file") != -1);

        /**
         * config.xml is invalid
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        invalidConfig,
                                        "-secdir",
                                        aSecurityConfig});
        assertTrue(s.indexOf("Failed to load or parse") != -1);

        /**
         * security directory not relative
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aBootConfig,
                                        "-secdir",
                                        new File(testRoot,
                                                 aSecurityConfig).
                                        getPath()});
        assertTrue(s.indexOf("must be a relative file name") != -1);

        /**
         * security directory not in root dir
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aBootConfig,
                                        "-secdir",
                                        nonExistingFile});
        assertTrue(s.indexOf("does not exist in") != -1);

        /**
         * security directory is not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aBootConfig,
                                        "-secdir",
                                        existingFile});
        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * security directory is empty
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aBootConfig,
                                        "-secdir",
                                        emptySecDir});
        assertTrue(s.indexOf("Security file not found in") != -1);
    }

    /**
     * Test removal error conditions.
     */
    @Test
    public void testRemoveWithErrors()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /**
         * non-existent directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        "/nonexistent/directory"});

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * not a directory
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        new File(testRoot,
                                                 existingFile).
                                        getPath()});

        assertTrue(s.indexOf("is not a directory") != -1);

        /**
         * config.xml not a relative path name
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        new File(testRoot, existingFile).
                                        getAbsolutePath()});
        assertTrue(s.indexOf("must be a relative file name") != -1);

        /**
         * config.xml not in root dir
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        nonExistingFile});
        assertTrue(s.indexOf("does not exist in") != -1);

        /**
         * config.xml not a file
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aSecurityConfig});
        assertTrue(s.indexOf("is not a file") != -1);

        /**
         * config.xml is invalid
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        invalidConfig});
        assertTrue(s.indexOf("Failed to load or parse") != -1);
    }

    /**
     * Test creation error conditions.
     */
    @Test
    public void testAddAndRemove()
        throws Exception {

        String s;

        /* Auto-login wallets need no passphrase */
        pwReader.setPassword(null);

        /*
         * add security using explicit args
         */
        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aBootConfig,
                                        "-secdir",
                                        aSecurityConfig});
        assertTrue(s.indexOf("Configuration updated") != -1);

        BootstrapParams bp =
            ConfigUtils.getBootstrapParams(
                new File(testRoot, aBootConfig));
        assertNotNull(bp);
        assertNotNull(bp.getSecurityDir());

        /*
         * remove security using explicit args
         */

        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        testRoot.getPath(),
                                        "-config",
                                        aBootConfig});
        assertTrue(s.indexOf("Configuration updated") != -1);

        bp = ConfigUtils.getBootstrapParams(
            new File(testRoot, aBootConfig));
        assertNotNull(bp);
        assertNull(bp.getSecurityDir());

        /*
         * add security using default args
         */
        final String stdConfig = FileNames.SNA_CONFIG_FILE;
        final String stdSecurity = FileNames.SECURITY_CONFIG_DIR;

        makeBootConfig(new File(testRoot, stdConfig), stdSecurity);
        makeSecurityConfig(testRoot, new File(stdSecurity));


        s = runCLICommand(pwReader,
                          new String[] {"config", "add-security",
                                        "-root",
                                        testRoot.getPath() });
        assertTrue(s.indexOf("Configuration updated") != -1);

        bp = ConfigUtils.getBootstrapParams(new File(testRoot, stdConfig));
        assertNotNull(bp);
        assertNotNull(bp.getSecurityDir());

        /*
         * remove security using default args
         */

        s = runCLICommand(pwReader,
                          new String[] {"config", "remove-security",
                                        "-root",
                                        testRoot.getPath()});
        assertTrue(s.indexOf("Configuration updated") != -1);

        bp = ConfigUtils.getBootstrapParams(new File(testRoot, stdConfig));
        assertNotNull(bp);
        assertNull(bp.getSecurityDir());
    }
}
