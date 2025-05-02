/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.diagnostic.DiagnosticConfigFile;
import oracle.kv.impl.diagnostic.SNAInfo;
import oracle.kv.util.CreateStore;

import org.junit.Assume;
import org.junit.Test;

/**
 * Test the collect command-line interface.
 */
public class CollectCommandTest extends DiagnosticShellTestBase {
    protected CreateStore createStore = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        kvstoreName = "kvtest-" + getClass().getName() + "-" +
                /* Filter out illegal characters */
                testName.getMethodName().replaceAll("[^-_.a-zA-Z0-9]", "-");
        suppressSystemOut();
        suppressSystemError();
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown();
        }
        resetSystemOut();
        resetSystemError();
        super.tearDown();
    }

    /**
     * Test collect -logfile command for secured kvstore
     * @throws Exception
     */
    @Test
    public void testLogfileSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        /* Add SNA info into configuration file */
        for (int i = 0; i < 3; i++) {
            runCLICommand(new String[] {"setup",
                    "-add",
                    "-store", "mystore",
                    "-sn", "sn" + (i + 1),
                    "-host", "localhost",
                    "-sshusername", System.getProperty("user.name"),
                    "-rootdir", "~/kvroot" + i,
                    "-configdir", outputDir.getAbsolutePath()});
        }

        /* Help flag test */
        String s = runCLICommand(new String[] {"collect", "-logfiles",
                "-help"});
        assertTrue(s.indexOf("Collect log files of all SNAs") != -1);

        /* Unknown argument test */
        s = runCLICommand(new String[] {"collect", "-logfiles", "-xxx"});

        /* Check the result when there is a unknown argument */
        assertTrue(s.indexOf("Unknown argument: -xxx") != -1);

        /* No existing configuration file test */
        s = runCLICommand(new String[] {"collect", "-logfiles"});

        /* No existing configuration file, FileNotFoundException is expected */
        assertTrue(s.indexOf("Cannot find file") != -1);

        /* Create temporary files */
        File rootdir0 = new File(System.getProperty("user.home"), "kvroot0");
        if (!rootdir0.exists()) {
            rootdir0.mkdir();
        }

        File logdir0 = new File(rootdir0, "log");
        if (!logdir0.exists()) {
            logdir0.mkdir();
        }

        File xmlconfig0 = new File(rootdir0, "config.xml");
        if (!xmlconfig0.exists()) {
            xmlconfig0.createNewFile();
        }

        File rootdir1 = new File(System.getProperty("user.home"), "kvroot1");
        if (!rootdir1.exists()) {
            rootdir1.mkdir();
        }

        File logdir1 = new File(rootdir1, "log");
        if (!logdir1.exists()) {
            logdir1.mkdir();
        }

        File xmlconfig1 = new File(rootdir1, "config.xml");
        if (!xmlconfig1.exists()) {
            xmlconfig1.createNewFile();
        }

        File rootdir2 = new File(System.getProperty("user.home"), "kvroot2");
        if (!rootdir2.exists()) {
            rootdir2.mkdir();
        }

        File logdir2 = new File(rootdir2, "log");
        if (!logdir2.exists()) {
            logdir2.mkdir();
        }

        File xmlconfig2 = new File(rootdir2, "config.xml");
        if (!xmlconfig2.exists()) {
            xmlconfig2.createNewFile();
        }

        /* Create log files */
        File file1 = new File(logdir0, "admin1_0.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
        bw.write("2014-07-30 07:33:48.737 UTC SEC_INFO [admin1] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin1] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin1] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin1] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        File file2 = new File(logdir1, "admin2_0.log");
        bw = new BufferedWriter(new FileWriter(file2));
        bw.write("2014-07-30 07:33:48.738 UTC SEC_INFO [admin2] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin2] Initializing " +
                "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin2] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin2] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin2] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        File file3 = new File(logdir2, "admin3_0.log");
        bw = new BufferedWriter(new FileWriter(file3));
        bw.write("2014-07-30 07:33:48.739 UTC SEC_INFO [admin3] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin3] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin3] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin3] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin3] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        /* Collect log file test */
        File savedir = new File("savedir");
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Compressed log file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath()});

        assertTrue(s.indexOf("Total: 5    Completed: 5    Status: done") != -1);

        /* Assert whether the log files are fetched to local disk */
        File subDirectory = getSubLogDirectory(savedir);
        File adminLog1 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn1.zip");

        File adminLog2 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn2.zip");

        File adminLog3 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn3.zip");

        File masterLogFile = new File(subDirectory.getAbsolutePath() +
                                      File.separator + "admin_master.log");

        File securityEventFile = new File(subDirectory.getAbsolutePath() +
                                      File.separator + "security_event.log");
        assertTrue(adminLog1.exists());
        assertTrue(adminLog2.exists());
        assertTrue(adminLog3.exists());
        assertTrue(masterLogFile.exists());
        assertTrue(securityEventFile.exists());

        /* Delete all files in save directory */
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Non-compressed log file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath(), "-nocompress"});

        assertTrue(s.indexOf("Total: 5    Completed: 5    Status: done") != -1);

        /* Assert whether the log files are fetched to local disk */
        subDirectory = getSubLogDirectory(savedir);
        adminLog1 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn1" +
                                   File.separator + "kvroot0" +
                                   File.separator + "log", "admin1_0.log");

        adminLog2 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn2" +
                                   File.separator + "kvroot1" +
                                   File.separator + "log", "admin2_0.log");

        adminLog3 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn3" +
                                   File.separator + "kvroot2" +
                                   File.separator + "log", "admin3_0.log");

        masterLogFile = new File(subDirectory.getAbsolutePath() +
                                      File.separator + "admin_master.log");

        securityEventFile = new File(subDirectory.getAbsolutePath() +
                                      File.separator + "security_event.log");

        assertTrue(adminLog1.exists());
        assertTrue(adminLog2.exists());
        assertTrue(adminLog3.exists());
        assertTrue(masterLogFile.exists());
        assertTrue(securityEventFile.exists());

        /* Delete all temporary directories */
        deleteDirectory(rootdir0);
        deleteDirectory(rootdir1);
        deleteDirectory(rootdir2);
        deleteDirectory(savedir);
    }

    /**
     * Test collect -logfile command for non-secured kvstore
     * @throws Exception
     */
    @Test
    public void testLogfileNonSecured() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        /* Add SNA info into configuration file */
        for (int i = 0; i < 3; i++) {
            runCLICommand(new String[] {"setup",
                    "-add",
                    "-store", "mystore",
                    "-sn", "sn" + (i + 1),
                    "-host", "localhost",
                    "-sshusername", System.getProperty("user.name"),
                    "-rootdir", "~/kvroot" + i,
                    "-configdir", outputDir.getAbsolutePath()});
        }

        /* Create temporary files */
        File rootdir0 = new File(System.getProperty("user.home"), "kvroot0");
        if (!rootdir0.exists()) {
            rootdir0.mkdir();
        }

        File logdir0 = new File(rootdir0, "log");
        if (!logdir0.exists()) {
            logdir0.mkdir();
        }

        File xmlconfig0 = new File(rootdir0, "config.xml");
        if (!xmlconfig0.exists()) {
            xmlconfig0.createNewFile();
        }

        File rootdir1 = new File(System.getProperty("user.home"), "kvroot1");
        if (!rootdir1.exists()) {
            rootdir1.mkdir();
        }

        File logdir1 = new File(rootdir1, "log");
        if (!logdir1.exists()) {
            logdir1.mkdir();
        }

        File xmlconfig1 = new File(rootdir1, "config.xml");
        if (!xmlconfig1.exists()) {
            xmlconfig1.createNewFile();
        }

        File rootdir2 = new File(System.getProperty("user.home"), "kvroot2");
        if (!rootdir2.exists()) {
            rootdir2.mkdir();
        }

        File logdir2 = new File(rootdir2, "log");
        if (!logdir2.exists()) {
            logdir2.mkdir();
        }

        File xmlconfig2 = new File(rootdir2, "config.xml");
        if (!xmlconfig2.exists()) {
            xmlconfig2.createNewFile();
        }

        /* Create log files */
        File file1 = new File(logdir0, "admin1_0.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin1] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin1] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin1] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        File file2 = new File(logdir1, "admin2_0.log");
        bw = new BufferedWriter(new FileWriter(file2));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin2] Initializing " +
                "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin2] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin2] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin2] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        File file3 = new File(logdir2, "admin3_0.log");
        bw = new BufferedWriter(new FileWriter(file3));
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin3] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin3] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin3] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin3] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        /* Collect log file test */
        File savedir = new File("savedir");
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Compressed log file test */
        String s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath()});

        assertTrue(s.indexOf("Total: 5    Completed: 5    Status: done") != -1);

        File subDirectory = getSubLogDirectory(savedir);
        File securityEventFile = new File(subDirectory.getAbsolutePath() +
                                          File.separator +
                                          "security_event.log");
        /* No security event found, and no security event file created */
        assertFalse(securityEventFile.exists());

        /* Delete all files in save directory */
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Non-compressed log file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath(), "-nocompress"});

        assertTrue(s.indexOf("Total: 5    Completed: 5    Status: done") != -1);

        /* No security event found, and no security event file created */
        subDirectory = getSubLogDirectory(savedir);
        securityEventFile = new File(subDirectory.getAbsolutePath() +
                                          File.separator +
                                          "security_event.log");
        assertFalse(securityEventFile.exists());

        /* Delete all temporary directories */
        deleteDirectory(rootdir0);
        deleteDirectory(rootdir1);
        deleteDirectory(rootdir2);
        deleteDirectory(savedir);
    }

    /**
     * Test collect -logfile command for kvstore with storagedir,
     * rnlogdir and admindir.
     * @throws Exception
     */
    @Test
    public void testLogfileRNLogDirStorageAdminDir() throws Exception {

        List<String> mountPoints = new ArrayList<String>();
        List<String> rnLogMountPoints = new ArrayList<String>();
        String adminMountPoint;

        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        /* Add SNA info into configuration file */
        runCLICommand(new String[] {"setup",
                "-add",
                "-store", "mystore",
                "-sn", "sn1",
                "-host", "localhost",
                "-sshusername", System.getProperty("user.name"),
                "-rootdir", "~/kvroot0",
                "-configdir", outputDir.getAbsolutePath()});

        /* Create temporary files */
        File rootdir0 = new File(System.getProperty("user.home"), "kvroot0");
        if (!rootdir0.exists()) {
            rootdir0.mkdir();
        }

        File logdir0 = new File(rootdir0, "log");
        if (!logdir0.exists()) {
            logdir0.mkdir();
        }

        File xmlconfig0 = new File(rootdir0, "config.xml");
        if (!xmlconfig0.exists()) {
            xmlconfig0.createNewFile();
        }

        File storagedir0 = new File(System.getProperty("user.home"), "rnstorage0");
        if (!storagedir0.exists()) {
            storagedir0.mkdir();
        }

        File rnlogdir0 = new File(System.getProperty("user.home"), "rnlogdir0");
        if (!rnlogdir0.exists()) {
            rnlogdir0.mkdir();
        }

        File admindir0 = new File(System.getProperty("user.home"), "admindir0");
        if (!admindir0.exists()) {
            admindir0.mkdir();
        }

        mountPoints.add(storagedir0.getAbsolutePath());
        rnLogMountPoints.add(rnlogdir0.getAbsolutePath());
        adminMountPoint = admindir0.getAbsolutePath();

        String host = "localhost";
        int port = 5000;
        String haPortRange = (port+2) + "," + (port+3);

        BootstrapParams bp0 =
            new BootstrapParams(rootdir0.getAbsolutePath(), host, host,
                                haPortRange, null /*servicePortRange*/,
                                "mystore", port, 0,
                                1 /*capacity*/,
                                null /*storageType*/,
                                null,
                                true,
                                null);

        /* Add system info */
        bp0.setStorgeDirs(mountPoints, null);
        bp0.setRNLogDirs(rnLogMountPoints, null);
        bp0.setAdminDir(adminMountPoint, null);

        ConfigUtils.createBootstrapConfig(bp0,
                new File(rootdir0,
                         "config.xml").toString());

        /* Create log files */
        File file1 = new File(logdir0, "admin1_0.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
        bw.write("2014-07-30 07:33:48.737 UTC SEC_INFO [admin1] KVAuditInfo X");
        bw.newLine();
        bw.write("2014-07-30 07:34:55.838 UTC INFO [admin1] Initializing " +
                        "Admin for store: mystore");
        bw.newLine();
        bw.write("2014-07-30 07:34:56.332 UTC INFO [admin1] JE: Master " +
                        "changed to 1");
        bw.newLine();
        bw.write("2014-07-30 07:42:57.188 UTC INFO [admin1] JE: " +
                        "Master changed to 3");
        bw.newLine();
        bw.write("2014-07-30 07:48:35.996 UTC INFO [admin1] JE: " +
                        "Master changed to 1");
        bw.newLine();
        bw.close();

        /* Create rgx-rny log files */
        File file2 = new File(rnlogdir0, "rg1-rn1_0.log");
        bw = new BufferedWriter(new FileWriter(file2));
        bw.write("2018-02-15 11:15:57.533 UTC INFO [rg1-rn1] 0 Starting service "
                + "process: rg1-rn1, Java command line arguments: "
                + "[-XX:+UseLargePages, -XX:+AlwaysPreTouch, "
                + "-Dje.rep.skipHelperHostResolution=true, "
                + "-XX:+UseG1GC, -XX:MaxGCPauseMillis=100, "
                + "-XX:InitiatingHeapOccupancyPercent=85, "
                + "-XX:G1HeapRegionSize=32m, "
                + "-XX:G1MixedGCCountTarget=32, "
                + "-XX:G1RSetRegionEntries=2560, "
                + "-XX:G1HeapWastePercent=5, "
                + "-XX:-ResizePLAB, -XX:+DisableExplicitGC, "
                + "-XX:+PrintGCDetails, -XX:+PrintGCDateStamps, "
                + "-XX:+PrintGCApplicationStoppedTime, "
                + "-XX:+UseGCLogFileRotation, -XX:NumberOfGCLogFiles=10, "
                + "-XX:GCLogFileSize=1048576, "
                + "-Xloggc:/npe/data/sn1/rnlog01/rg1-rn1.gc, "
                + "-XX:ParallelGCThreads=4, -Dkvdns.networkaddress.cache.ttl=-1]");
        bw.newLine();
        bw.close();

        /*
         * Nothing to add for storagedir0 and admindir0 since with new server
         * changes, these directories are empty.
         */

        /* Collect log file test */
        File savedir = new File("savedir");
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Non-compressed log file test */
        String s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath(), "-nocompress"});

        assertTrue(s.indexOf("Total: 3    Completed: 3    Status: done") != -1);

        int length = storagedir0.getAbsolutePath().length();
        String newMountPointsName =
            storagedir0.getAbsolutePath().substring(0,length-11).
            replace('/', '_');

        /* Assert whether the log files are fetched to local disk */
        File subDirectory = getSubLogDirectory(savedir);
        File adminLog0 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn1" +
                                   File.separator + "kvroot0" +
                                   File.separator + "log", "admin1_0.log");

        File storageDir0 = new File (subDirectory.getAbsolutePath() +
                                     File.separator + "mystore_sn1" +
                                     File.separator + newMountPointsName +
                                     "_rnstorage0");

        File rnlogDir0 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn1" +
                                   File.separator + newMountPointsName +
                                   "_rnlogdir0");

        File adminDir0 = new File (subDirectory.getAbsolutePath() +
                                   File.separator + "mystore_sn1" +
                                   File.separator + newMountPointsName +
                                   "_admindir0");

        File masterLogFile = new File(subDirectory.getAbsolutePath() +
                                      File.separator + "admin_master.log");

        File securityEventFile = new File(subDirectory.getAbsolutePath() +
                                          File.separator + "security_event.log");
        assertTrue(adminLog0.exists());
        assertTrue(storageDir0.exists());
        assertTrue(rnlogDir0.exists());
        assertTrue(adminDir0.exists());
        assertTrue(masterLogFile.exists());
        assertTrue(securityEventFile.exists());

        /* Delete all files in save directory */
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Compressed log file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath()});

        assertTrue(s.indexOf("Total: 3    Completed: 3    Status: done") != -1);

        /* Assert whether the log files are fetched to local disk */
        subDirectory = getSubLogDirectory(savedir);
        File kvstoreLog1 = new File (subDirectory.getAbsolutePath() +
                                     File.separator + "mystore_sn1.zip");

        masterLogFile = new File(subDirectory.getAbsolutePath() +
                                 File.separator + "admin_master.log");

        securityEventFile = new File(subDirectory.getAbsolutePath() +
                                     File.separator + "security_event.log");

        assertTrue(kvstoreLog1.exists());
        assertTrue(masterLogFile.exists());
        assertTrue(securityEventFile.exists());

        /* Delete all temporary directories */
        deleteDirectory(rootdir0);
        deleteDirectory(storagedir0);
        deleteDirectory(rnlogdir0);
        deleteDirectory(admindir0);
        deleteDirectory(savedir);
    }

    /**
     * Test invalid configuration file
     * @throws Exception
     */
    @Test
    public void testInvalidConfigFile() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        /* Add SNA info into configuration file */
        SNAInfo snaInfo = new SNAInfo("mystore", "sn1", "localhost",
                                      System.getProperty("user.name"),
                                      "~/kvroot");
        DiagnosticConfigFile congfigFile =
                new DiagnosticConfigFile(outputDir.getAbsolutePath());
        congfigFile.add(snaInfo);
        congfigFile.add(snaInfo);

        /* Collect log file test */
        File savedir = new File("savedir");
        deleteDirectory(savedir);
        if (!savedir.exists()) {
            savedir.mkdir();
        }

        /* Duplicated lines test */
        String s = runCLICommand(new String[] {"collect", "-logfiles",
                    "-configdir", outputDir.getAbsolutePath(),
                    "-savedir", savedir.getAbsolutePath()});

        assertTrue(s.indexOf("Duplicated lines in configuration") != -1);

        s = runCLICommand(new String[] {"setup", "-clear",
                "-configdir", outputDir.getAbsolutePath()});

        /* Clear config file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                    "-configdir", outputDir.getAbsolutePath(),
                    "-savedir", savedir.getAbsolutePath()});

        assertTrue(s.indexOf("should not be empty") != -1);

        /* No config file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", ".",
                "-savedir", savedir.getAbsolutePath()});

        assertTrue(s.indexOf("Cannot find file") != -1);

        BufferedWriter bw = null;

        try {
            File file = new File(outputDir, "sn-target-list");
            bw = new BufferedWriter(new FileWriter(file, true));
            bw.newLine();

            bw.write("xxx");
        } catch (Exception ex) {
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException ex) {

            }
        }

        /* Invalid config file test */
        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-configdir", outputDir.getAbsolutePath(),
                "-savedir", savedir.getAbsolutePath()});
        assertTrue(s.indexOf("Invalid configuration file") != -1);
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

        String s = runCLICommand(new String[] {"collect", "-logfiles",
                "-host", "localhost",
                "-port", "5000",
                "-configdir", outputDir.getAbsolutePath()});

        assertTrue(s.indexOf("Status: done") != -1);

        s = runCLICommand(new String[] {"collect", "-logfiles",
                "-host", "localhost",
                "-port", "6000",
                "-configdir", outputDir.getAbsolutePath()});

        assertTrue(s.indexOf("Updating configuration file failed:") != -1);
    }

    /**
     * Get sub log directory under specified directory
     * @param directory
     * @return
     */
    private File getSubLogDirectory(File directory) {
        if (directory.isFile()) {
            return null;
        }

        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.getName().contains("logs_")) {
                return file;
            }
        }
        return null;
    }
}
