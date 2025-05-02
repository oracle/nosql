/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.util.TestUtils.NULL_PRINTSTREAM;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assume;
import org.junit.Test;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.util.SecurityConfigCreator.GenericIOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.util.CreateStore;

/**
 * Test the verify command-line interface.
 */
public class VerifyCommandTest extends DiagnosticShellTestBase {
    protected CreateStore createStore = null;
    private File rootdir0;
    private File rootdir1;
    private File rootdir2;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        kvstoreName = "kvtest-" + getClass().getName() + "-" +
                /* Filter out illegal characters */
                testName.getMethodName().replaceAll("[^-_.a-zA-Z0-9]", "-");
        suppressSystemError();
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown();
        }
        deleteTemporaryFiles();
        resetSystemError();
        super.tearDown();
    }

    /**
     * Create configuration files of SNAs
     * @param isSecured is to indicate whether the configuration file is
     * secured or not
     * @throws Exception
     */
    private void createTemporaryFiles(boolean isSecured) throws Exception {
        /* Add SNA info into configuration file */
        for (int i = 0; i < 3; i++) {
            runCLICommand(new String[] {"setup",
                    "-add",
                    "-store", kvstoreName,
                    "-sn", "sn" + (i + 1),
                    "-host", "localhost",
                    "-sshusername", System.getProperty("user.name"),
                    "-rootdir", "~/kvroot" + i,
                    "-configdir", outputDir.getAbsolutePath()});
        }

        /* Create temporary files */
        rootdir0 = new File(System.getProperty("user.home"), "kvroot0");
        if (!rootdir0.exists()) {
            rootdir0.mkdir();
        }

        rootdir1 = new File(System.getProperty("user.home"), "kvroot1");
        if (!rootdir1.exists()) {
            rootdir1.mkdir();
        }

        rootdir2 = new File(System.getProperty("user.home"), "kvroot2");
        if (!rootdir2.exists()) {
            rootdir2.mkdir();
        }

        String host = "localhost";
        int port = 5000;
        String haPortRange = (port+2) + "," + (port+3);

        BootstrapParams bp0 =
            new BootstrapParams(rootdir0.getAbsolutePath(), host, host,
                                haPortRange, null /*servicePortRange*/,
                                "mystore", port, -1,
                                1 /*capacity*/,
                                null /*storageType*/,
                                isSecured ? "security" : null,
                                !isSecured,
                                null);

        port = 6000;
        haPortRange = (port+2) + "," + (port+3);

        BootstrapParams bp1 =
            new BootstrapParams(rootdir1.getAbsolutePath(), host, host,
                                haPortRange, null /*servicePortRange*/,
                                "mystore", port, -1,
                                1 /*capacity*/,
                                null /*storageType*/,
                                isSecured ? "security" : null,
                                !isSecured,
                                null);

        port = 7000;
        haPortRange = (port+2) + "," + (port+3);

        BootstrapParams bp2 =
            new BootstrapParams(rootdir2.getAbsolutePath(), host, host,
                                haPortRange, null /*servicePortRange*/,
                                "mystore", port, -1,
                                1 /*capacity*/,
                                null /*storageType*/,
                                isSecured ? "security" : null,
                                !isSecured,
                                null);

        ConfigUtils.createBootstrapConfig(bp0,
                                          new File(rootdir0,
                                                   "config.xml").toString());

        ConfigUtils.createBootstrapConfig(bp1,
                                          new File(rootdir1,
                                                   "config.xml").toString());

        ConfigUtils.createBootstrapConfig(bp2,
                                          new File(rootdir2,
                                                   "config.xml").toString());
    }

    private void deleteTemporaryFiles() {
        /* Delete all temporary directories */
        if (rootdir0 != null) {
            deleteDirectory(rootdir0);
        }

        if (rootdir1 != null) {
            deleteDirectory(rootdir1);
        }

        if (rootdir2 != null) {
            deleteDirectory(rootdir2);
        }
    }

    /**
     * Test verify -snascheck command for non-secured kvstore
     * @throws Exception
     */
    @Test
    public void testSnasCheckNonSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);
        createTemporaryFiles(false);

        /* Help flag test */
        String s = runCLICommand(new String[] {"verify", "-checkMulti",
                "-help"});
        assertTrue(s.indexOf("Determine whether each SN's") != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"verify", "-checkMulti", "-xxx"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* No existing configuration file test */
        s = runCLICommand(new String[] {"verify", "-checkMulti"});

        /* No existing configuration file, FileNotFoundException is expected */
        assertTrue(s.indexOf("Cannot find file") != -1);

        /* Config check test */
        s = runCLICommand(new String[] {"verify", "-checkMulti",
                "-configdir", outputDir.getAbsolutePath()});

        /* Copy configuration file check test */
        assertTrue(s.indexOf("Fetched configuration file from") != -1);

        /* Environment check test */
        assertTrue(s.indexOf("Multi-SNs compatibility check") != -1);

        /* Clock skew test */
        assertTrue(s.indexOf("Clock skew") != -1);

        /* Java version test */
        assertTrue(s.indexOf("Java version") != -1);

        /* Network connection status test */
        assertTrue(s.indexOf("Network connection status") != -1);

        /* Security Policy test */
        assertTrue(s.indexOf("Security Policy") != -1);
        assertTrue(s.indexOf("Security policies are consistent") != -1);
        assertTrue(s.indexOf("non-secured") != -1);

        /* KVStore version test */
        assertTrue(s.indexOf("KVStore version") != -1);

        /* OverLap ports test */
        assertTrue(s.indexOf("Port Ranges") != -1);
        assertTrue(s.indexOf("No conflicts in port ranges for SNs hosted " +
                "on the same node") != -1);
    }

    /**
     * Test verify -snascheck command for secured kvstore
     * @throws Exception
     */
    @Test
    public void testSnasCheckSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        createTemporaryFiles(true);

        /* Create security folders */
        final ParsedConfig config = new ParsedConfig();
        config.populateDefaults();

        config.setPwdmgr("wallet");
        config.setKeystorePassword("123456".toCharArray());
        config.setSecurityDir("security");

        SecurityConfigCreator scCreator = new SecurityConfigCreator(
            rootdir0.getAbsolutePath(),
            config,
            new GenericIOHelper(NULL_PRINTSTREAM));
        scCreator.createConfig();

        cp(new File(rootdir0, "security"), rootdir1);
        cp(new File(rootdir0, "security"), rootdir2);

        /* Config check test */
        String s = runCLICommand(new String[] {"verify", "-checkMulti",
                "-configdir", outputDir.getAbsolutePath()});

        /* Security Policy test */
        assertTrue(s.indexOf("Security Policy") != -1);
        assertTrue(s.indexOf("Security policies are consistent") != -1);
        assertTrue(s.indexOf("security policy 1") != -1);
    }

    /**
     * Test verify -snascheck command for inconsistent security
     * @throws Exception
     */
    @Test
    public void testSnasCheckInconsistentSecurity() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        createTemporaryFiles(true);

        /* Create security folders */
        final ParsedConfig config1 = new ParsedConfig();
        config1.setPwdmgr("wallet");
        config1.setKeystorePassword("123456".toCharArray());
        config1.setSecurityDir("security");

        config1.populateDefaults();

        SecurityConfigCreator scCreator1 = new SecurityConfigCreator(
            rootdir0.getAbsolutePath(),
            config1,
            new GenericIOHelper(NULL_PRINTSTREAM));
        scCreator1.createConfig();

        /* Create security folders */
        final ParsedConfig config2 = new ParsedConfig();
        config2.setPwdmgr("wallet");
        config2.setKeystorePassword("123456".toCharArray());
        config2.setSecurityDir("security");

        config2.populateDefaults();

        SecurityConfigCreator scCreator2 = new SecurityConfigCreator(
            rootdir1.getAbsolutePath(),
            config2,
            new GenericIOHelper(NULL_PRINTSTREAM));
        scCreator2.createConfig();

        cp(new File(rootdir0, "security"), rootdir2);

        /* Config check test */
        String s = runCLICommand(new String[] {"verify", "-checkMulti",
                "-configdir", outputDir.getAbsolutePath()});

        /* Security Policy test */
        assertTrue(s.indexOf("Security Policy") != -1);
        assertTrue(s.indexOf("2 different security policies are found") != -1);
        assertTrue(s.indexOf("security policy 1") != -1);
        assertTrue(s.indexOf("security policy 2") != -1);
    }

    /**
     * Test verify -snascheck command for overlap ports
     * @throws Exception
     */
    @Test
    public void testSnasCheckOverlapPorts() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);
        createTemporaryFiles(false);

        cp(new File(rootdir0, "config.xml"), new File(rootdir0, "config1.xml"));

        /* Config check test */
        String s = runCLICommand(new String[] {"verify", "-checkMulti",
                "-configdir", outputDir.getAbsolutePath()});

        /* OverLap ports test */
        assertTrue(s.indexOf("Port Ranges") != -1);
        assertTrue(s.indexOf("is used by") != -1);
    }

    /**
     * Test verify -config command
     * @throws Exception
     */
    @Test
    public void testConfig() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        createTemporaryFiles(false);

        /* Help flag test */
        String s = runCLICommand(new String[] {"verify", "-checkLocal",
                "-help"});
        assertTrue(s.indexOf("Verify that each SN's configuration file is")
                   != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"verify", "-checkLocal", "-xxx"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* No existing configuration file test */
        s = runCLICommand(new String[] {"verify", "-checkLocal"});

        /* No existing configuration file, FileNotFoundException is expected */
        assertTrue(s.indexOf("Cannot find file") != -1);

        /* Config check test */
        s = runCLICommand(new String[] {"verify", "-checkLocal",
                "-configdir", outputDir.getAbsolutePath()});

        /* Copy configuration file check test */
        assertTrue(s.indexOf("Fetched configuration file from") != -1);

        /* SNA parameter check test */
        assertTrue(s.indexOf("SN Local Configuration check") != -1);

        /* No violate found test */
        assertTrue(s.indexOf("SN configuration is valid.") != -1);

        String host = "localhost";
        int port = 7000;
        String haPortRange = port + "," + (port+2);

        BootstrapParams bp3 =
            new BootstrapParams(rootdir2.getAbsolutePath(), host, host,
                                haPortRange, null /*servicePortRange*/,
                                "mystore", port, -1, 1 /*capacity*/,
                                null /*storageType*/,
                                null /*securityDir*/,
                                true /*isHostingAdmin*/,
                                null);

        ConfigUtils.createBootstrapConfig(bp3,
                                          new File(rootdir2,
                                                   "config1.xml").toString());

        s = runCLICommand(new String[] {"verify", "-checkLocal",
                "-configdir", outputDir.getAbsolutePath()});

        /* overlap port found test */
        assertTrue("output=" + s,
                   s.indexOf("is already assigned as") != -1);
    }

    /**
     * Test update configuration file from topology
     * @throws Exception
     */
    @Test
    public void testUpdateConfigFile() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "localhost",
                "-sshusername", System.getProperty("user.name"),
                "-rootdir", "~/kvroot",
                "-configdir", outputDir.getAbsolutePath()});

        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      1 /*9*/,   /* Storage Nodes */
                                      1 /*3*/,   /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.start();

        String s = runCLICommand(new String[] {"verify", "-checkLocal",
                "-host", "localhost",
                "-port", "5000",
                "-configdir", outputDir.getAbsolutePath()});

        assertTrue(s.indexOf("Status: done") != -1);

        s = runCLICommand(new String[] {"verify", "-checkMulti",
                "-host", "localhost",
                "-port", "6000",
                "-configdir", outputDir.getAbsolutePath()});

        assertTrue(s.indexOf("Updating configuration file failed:") != -1);
    }

    /**
     * Implement cp command
     * @param sourceFile
     * @param targetFile
     * @throws IOException
     */
    private void cp(File sourceFile, File targetFile) throws IOException {

        if (sourceFile.exists()) {
            if (sourceFile.isFile()) {
                if (targetFile.getParentFile().exists()) {
                    if (!targetFile.exists() || targetFile.isFile()) {
                        copyFile(sourceFile, targetFile);
                    } else {
                        copyFile(sourceFile,
                                 new File(targetFile, sourceFile.getName()));
                    }
                } else {
                    System.err.println("Cannot copy " + sourceFile + " to " +
                            targetFile + ": " + targetFile + " or " +
                            targetFile.getParent() + " does not exist");
                }

            } else {
                if (targetFile.exists() && targetFile.isDirectory()) {
                    copyDirectiory(sourceFile,
                                   new File(targetFile, sourceFile.getName()));
                } else {
                    System.err.println("Cannot copy " + sourceFile + " to " +
                            targetFile + ": " + targetFile +
                            " Not a directory");
                }
            }
        } else {
            System.err.println("Cannot find " + sourceFile);
        }
    }
    /**
     * Copy a directory to another directory
     * @param sourceDir
     * @param targetDir
     * @throws IOException
     */
    private void copyDirectiory(File sourceDir,File targetDir)
            throws IOException {
        targetDir.mkdirs();
        File[] file = sourceDir.listFiles();
        for (File sourceFile : file) {
            if(sourceFile.isFile()) {
                File targetFile = new File(targetDir, sourceFile.getName());
                copyFile(sourceFile, targetFile);
            }
            if(sourceFile.isDirectory()) {
                File dir1 = new File(sourceDir, sourceFile.getName());
                File dir2 = new File(targetDir, sourceFile.getName());

                copyDirectiory(dir1, dir2);
            }
        }

    }

    /**
     * Copy a file to another path
     * @param sourcefile
     * @param targetFile
     * @throws IOException
     */
    public void copyFile(File sourcefile,File targetFile) throws IOException {
        FileInputStream input = new FileInputStream(sourcefile);
        BufferedInputStream inbuff = new BufferedInputStream(input);

        /* Create parent folder of target file */
        targetFile.getParentFile().mkdirs();
        FileOutputStream out = new FileOutputStream(targetFile);
        BufferedOutputStream outbuff = new BufferedOutputStream(out);

        byte[] b = new byte[1024 * 5];
        int len = 0;
        while ((len = inbuff.read(b)) != -1) {
            outbuff.write(b, 0, len);
        }

        outbuff.flush();

        inbuff.close();
        outbuff.close();
        out.close();
        input.close();
    }
}
