/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic.ssh;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import oracle.kv.impl.diagnostic.SNAInfo;

import org.junit.Assume;
import org.junit.Test;

/**
 * Tests SSHClient.
 *
 * How to turn on Apache Mina debug logging:
 * 1.Add slf4j-log4j as follow to dependencies in kvtest/kvstore-IT/pom.xml
 *  <dependency>
 *    <groupId>org.slf4j</groupId>
 *    <artifactId>slf4j-log4j12</artifactId>
 *    <version>${slf4j.version}</version>
 *  </dependency>
 *
 * 2. Enable slf4j-log4j logging
 * a. edit kvtest/resources/log4j-default.properties to enable debug logging
 * b. run test with customized logging properties file
 * mvn -Pit.kvstore clean verify -pl kvtest/kvstore-IT -am \
 *     -Dit.test=SSHClientTest \
 *     -Dlog4j.configuration=file://<absolute path of logging properties file>
 *
 * Example log4j debug logging properties
 * log4j.rootLogger=DEBUG, Appender
 * log4j.appender.Appender=org.apache.log4j.ConsoleAppender
 * log4j.appender.Appender.target=System.out
 * log4j.appender.Appender.layout=org.apache.log4j.PatternLayout
 * log4j.appender.Appender.layout.ConversionPattern %d [%t] %-5p %c %x - %m%n
 */
public class SSHClientTest extends ClientTestBase {

    @Test
    public void testCheckFile() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        String store = "mystore";
        String sn = "sn1";
        String user = System.getProperty("user.name");
        String host = "localhost";
        String rootdir = "~/kvroot1";
        SNAInfo snaInfo = new SNAInfo(store, sn, host, user, rootdir);

        File file = new File("checked.file");
        if (!file.exists()) {
            file.createNewFile();
        }

        /* Test checkFile(String) */
        SSHClient client = SSHClientManager.getClient(snaInfo);
        assertTrue(client.checkFile(file.getAbsolutePath()));
        file.delete();
        assertFalse(client.checkFile(file.getAbsolutePath()));
    }

    @Test
    public void testCheckDirectory() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        String store = "mystore";
        String sn = "sn1";
        String user = System.getProperty("user.name");
        String host = "localhost";
        String rootdir = "~/kvroot1";
        SNAInfo snaInfo = new SNAInfo(store, sn, host, user, rootdir);

        File file = new File("checked.dir");
        if (!file.exists()) {
            file.mkdir();
        }

        /* Test checkFile(String) */
        SSHClient client = SSHClientManager.getClient(snaInfo);
        assertTrue(client.checkFile(file.getAbsolutePath()));
        file.delete();
        assertFalse(client.checkFile(file.getAbsolutePath()));
    }

    @Test
    public void testGetLogFiles() throws Exception {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);

        /* Generate directories and log files */
        File directory = new File("directory");
        if (!directory.exists()) {
            directory.mkdir();
        }

        File subDirectory = new File(directory, "subDirectory");
        if (!subDirectory.exists()) {
            subDirectory.mkdir();
        }

        File file1 = new File(subDirectory, "sn1.log");
        if (!file1.exists()) {
            file1.createNewFile();
        }

        File file2 = new File(subDirectory, "admin1.log");
        if (!file2.exists()) {
            file2.createNewFile();
        }

        /* Create a jdb file */
        File file3 = new File(subDirectory, "00001.jdb");
        if (!file3.exists()) {
            file3.createNewFile();
        }

        File file4 = new File(subDirectory, "00001.bup");
        if (!file4.exists()) {
            file4.createNewFile();
        }

        assertTrue(isContainFileWithSuffix(subDirectory, ".jdb"));
        assertTrue(isContainFileWithSuffix(subDirectory, ".bup"));

        String store = "mystore";
        String sn = "sn1";
        String user = System.getProperty("user.name");
        String host = "localhost";
        String rootdir = directory.getAbsolutePath();
        SNAInfo snaInfo = new SNAInfo(store, sn, host, user, rootdir);

        SSHClient client = SSHClientManager.getClient(snaInfo);
        File outputDir = new File("outputDir");
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

        /* Test to get compressed log files */
        client.getLogFiles(snaInfo, outputDir.getAbsolutePath(), true);
        File logZipFile = new File(outputDir + File.separator +
                                   snaInfo.getStoreName() + "_" +
                                   snaInfo.getStorageNodeName() + ".zip");
        File logDirFile = new File(outputDir + File.separator +
                                   snaInfo.getStoreName() + "_" +
                                   snaInfo.getStorageNodeName());
        assertTrue(logZipFile.exists());
        assertTrue(logZipFile.isFile());
        assertTrue(logDirFile.exists());
        assertTrue(logDirFile.isDirectory());

        assertFalse(isContainFileWithSuffix(logDirFile, ".jdb"));
        assertFalse(isContainFileWithSuffix(logDirFile, ".bup"));

        deleteDirectory(outputDir);

        /* Test to get not compressed log files */
        client.getLogFiles(snaInfo, outputDir.getAbsolutePath(), false);
        assertFalse(logZipFile.exists());
        assertTrue(logDirFile.exists());
        assertTrue(logDirFile.isDirectory());

        assertFalse(isContainFileWithSuffix(logDirFile, ".jdb"));
        assertFalse(isContainFileWithSuffix(logDirFile, ".bup"));

        /* Delete output directory */
        deleteDirectory(outputDir);

        /* Delete all temporary files */
        file1.delete();
        file2.delete();
        file3.delete();
        file4.delete();
        subDirectory.delete();
        directory.delete();
    }

    @Test
    public void testSSHLogon() {
        /* This test case is not supported run under Windows platform */
        Assume.assumeFalse(isWindows);
        String password = "Fake password";

        /* Test open() 1  */
        SSHClient client = new SSHClient("localhost", "Nonexistinguser");
        client.openByPassword(password);
        client.close();
        assertFalse(client.isOpen());

        password = "Incorrect password";
        /* Test open() 2  */
        client = new SSHClient("localhost", System.getProperty("user.name"));
        client.openByPassword(password);
        client.close();
        assertFalse(client.isOpen());

        /* Test open() 3 */
        client = new SSHClient("localhost", System.getProperty("user.name"));
        client.open();
        assertTrue(client.isOpen());
        client.close();

        /* Test open() 4 */
        client = new SSHClient("localhost", "Nonexistinguser");
        client.open(password);
        client.close();
        assertFalse(client.isOpen());
    }

    /**
     * All files and directories under the specified path;
     * @param path
     */
    private void deleteDirectory(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (File file : files) {
            deleteDirectory(file);
        }
        path.delete();
    }

    /**
     * Check whether existing the file with the specified suffix.
     */
    private boolean isContainFileWithSuffix(File path, String fileSuffix) {
        if (!path.exists())
            return false;
        if (path.isFile()) {
            return path.getName().endsWith(fileSuffix);
        }
        File[] files = path.listFiles();
        for (File file : files) {
            boolean ret = isContainFileWithSuffix(file, fileSuffix);
            if(ret) {
                return true;
            }
        }
        return false;
    }
}
