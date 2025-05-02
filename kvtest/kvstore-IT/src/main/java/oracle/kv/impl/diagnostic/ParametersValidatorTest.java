/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;


import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

import oracle.kv.TestBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests PortConflictValidator.
 */
public class ParametersValidatorTest extends TestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testPort() throws InterruptedException {
        /*
         * Test the return of checkPort(String, int) method of
         * ParametersValidator
         */

        String retMsg = null;

        /*
         * Check whether there is a port available between 5000 and 5010
         */
        for (int i=5000; i<5010; i++) {
            retMsg = ParametersValidator.checkPort("port" + i, i);
            if(retMsg == null) {
                break;
            }
        }
        assertNull(retMsg);

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /*
         * Use the ports 5000-5010 and then check whether they are available or
         * not
         */
        ServerSocket ss = null;
        for (int i=5000; i<5010; i++) {
            try {
                ss = new ServerSocket(i);
                retMsg = ParametersValidator.checkPort("port" + i, i);
                assertEquals(retMsg, "Specified port" + i + " " + i +
                        " is already in use");
            } catch (IOException e) {
            } finally {
                if (ss != null) {
                    try {
                        ss.close();
                        ss = null;
                    } catch (IOException e) {
                    }
                }
            }
        }

        /*
         * Test the return of checkRangePorts(String, String) method of
         * ParametersValidator
         */

        /*
         * Check whether all ports between begin and end available or not
         */
        for (int i=5; i<10; i++) {
            int begin = i* 1000;
            int end = begin + 10;
            retMsg = ParametersValidator.
                    checkRangePorts("rangePort1", begin + "," + end);
            if (retMsg == null)  {
                break;
            }
        }
        assertNull(retMsg);

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /*
         * Use the ports 5000 and then check all ports between 5000 and 5010
         * available or not
         */
        ss = null;
        try {
            ss = new ServerSocket(5000);
            retMsg = ParametersValidator.
                    checkRangePorts("rangePort1", "5000,5010");

            assertEquals(retMsg, "Specified rangePort1 5000 is already in use");
        } catch (IOException e) {
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                    ss = null;
                } catch (IOException e) {
                }
            }
        }

        /*
         * Test the return of checkRangePortsNumber(String, String, int)
         * method of ParametersValidator
         */
        retMsg = ParametersValidator.
                checkRangePortsNumber("rangePort1", "5000,5010", 5);
        assertNull(retMsg);

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        retMsg = ParametersValidator.
                checkRangePortsNumber("rangePort2", "6000,6003", 5);
        assertEquals(retMsg, "Specified rangePort2 6000,6003 size is less " +
                "than the number of nodes 5");

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    @Test
    public void testDirectoryAndFile() throws IOException {
        /*
         * Test the return of checkDirectory(String, String) method of
         * ParametersValidator
         */

        String retMsg = null;
        /* Create a directory and check whether it exists or not */
        File file =  new File("testdirectory1");
        if (!file.exists()) {
            file.mkdir();
        }

        retMsg = ParametersValidator.
                checkDirectory("directory1", file.getAbsolutePath());
        assertNull(retMsg);

        /* Delete the directorytest1 and check */
        String filePath = file.getAbsolutePath();
        file.delete();
        retMsg = ParametersValidator.
                checkDirectory("directory1", filePath);
        assertEquals(retMsg,
                     "Specified directory1 " + filePath + " does not exist");

        /* Create a file and check whether it exists or not */
        file =  new File("testfile1");
        if (!file.exists()) {
            file.createNewFile();
        }

        retMsg = ParametersValidator.checkFile("file1", file.getAbsolutePath());
        assertNull(retMsg);

        /* Delete the testfile1 and check */
        String fileParentPath = new File(file.getAbsolutePath()).getParent();
        String fileName = file.getName();
        file.delete();
        retMsg = ParametersValidator.checkFile("file1", fileParentPath,
                                               fileName);
        assertEquals(retMsg,
                     "Specified file1 " + fileName +
                     " does not exist in " + fileParentPath);

        /*
         * Create a new directory and check whether for negative
         * storagedirsize
         */
        file =  new File("testdirectory2");
        if (!file.exists()) {
        	file.mkdir();
        }

        retMsg = ParametersValidator.
                checkDirectory("directory2", file.getAbsolutePath());
        assertNull(retMsg);

        retMsg = ParametersValidator.
                checkDirectory("directory2", file.getAbsolutePath(), -100);
        assertEquals(retMsg,
                     "Invalid directory size specified for directory2");
        file.delete();

        /*
         * Create a file and check for scenario when directory does not
         * have write permission
         */
        file =  new File("testdirectory3");
        if (!file.exists()) {
            file.mkdir();
        }

        retMsg = ParametersValidator.
                checkDirectory("directory3", file.getAbsolutePath());
        assertNull(retMsg);

        file.setReadOnly();
        retMsg = ParametersValidator.
                checkDirectory("directory3", file.getAbsolutePath());
        assertEquals(retMsg, "Specified directory for directory3" +
                     " does not have write permission");
        file.delete();
    }

    @Test
    public void testPositiveInteger() {
        /*
         * Test the return of checkPositiveInteger(String, int) method of
         * ParametersValidator
         */
        String retMsg = null;
        retMsg = ParametersValidator.
                checkPositiveInteger("positiveInteger1", 1);
        assertNull(retMsg);


        retMsg = ParametersValidator.checkPositiveInteger("positiveInteger1",
                                                          -1);
        assertEquals(retMsg, "-1 is invalid; positiveInteger1 must be >= 0");
    }

    @Test
    public void testHostname() {
        /*
         * Test the return of checkHostname(String, String) method of
         * ParametersValidator
         */
        String retMsg = null;
        retMsg = ParametersValidator.
                checkHostname("hostname1", "localhost");
        assertNull(retMsg);


        retMsg = ParametersValidator.
                checkHostname("hostname1", "helloworld");
        assertEquals(retMsg, "Specified hostname1 helloworld not reachable");
    }

    @Test
    public void testCPUNumber() {
        /*
         * Test the return of checkCPUNumber(String, int) method of
         * ParametersValidator
         */
        String retMsg = null;
        retMsg = ParametersValidator.checkCPUNumber("cpunumber1", 1);
        assertNull(retMsg);


        retMsg = ParametersValidator.checkCPUNumber("cpunumber1", 1000);
        assertTrue(retMsg.contains("invalid"));
    }

    @Test
    public void testMemorySize() {
        /*
         * Test the return of testMemorySize(String, int) method of
         * ParametersValidator
         */
        String retMsg = null;
        retMsg = ParametersValidator.checkMemorySize("memory1", 1, 1);
        assertNull(retMsg);

        retMsg = ParametersValidator.checkMemorySize("memory1", 10000000, 1);
        assertTrue(retMsg.contains("invalid"));

        retMsg = ParametersValidator.checkMemorySize("memory1",
                                                     50000,
                                                     3,
                                                     40000L * 1024 * 1024,
                                                     32);
        assertTrue("Check message for 32-bit truncation: " + retMsg,
                   retMsg.contains("32-bit"));

        retMsg = ParametersValidator.checkMemorySize("memory1",
                                                     50000,
                                                     3,
                                                     40000L * 1024 * 1024,
                                                     64);
        assertFalse("Check message without 32-bit truncation: " + retMsg,
                    retMsg.contains("32-bit"));
    }

    /**
     * Test problem with computing available memory on 32-bit machines
     * [#24550].
     */
    @Test
    public void testComputeAvailableMemoryMB() {
        final long kilo = 1 << 10;
        final long mega = 1 << 20;
        final long giga = 1 << 30;
        assertEquals("No info available",
                     0,
                     ParametersValidator.computeAvailableMemoryMB(
                         1, 0, 0));
        assertEquals("Int-size unknown",
                     42*kilo,
                     ParametersValidator.computeAvailableMemoryMB(
                         1, 42*giga, 0));
        assertEquals("64-bit, cap=1",
                     5*kilo,
                     ParametersValidator.computeAvailableMemoryMB(
                         1, 5*giga, 64));
        assertEquals("64-bit, cap=4",
                     5*kilo,
                     ParametersValidator.computeAvailableMemoryMB(
                         4, 5*giga, 64));
        assertEquals("32-bit, cap=1, bigger than max",
                     Integer.MAX_VALUE / mega,
                     ParametersValidator.computeAvailableMemoryMB(
                         1, 8*giga, 32));
        assertEquals("32-bit, cap=4, bigger than max",
                     (int) ((((long) Integer.MAX_VALUE) * 4) / mega),
                     ParametersValidator.computeAvailableMemoryMB(
                         4, 8*giga, 32));
        assertEquals("32-bit, cap=4, smaller than max",
                     4*kilo,
                     ParametersValidator.computeAvailableMemoryMB(
                         4, 4*giga, 32));
    }
}
