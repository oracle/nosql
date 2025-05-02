/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.diagnostic;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.util.PollCondition;

import oracle.kv.impl.util.StorageTypeDetector.StorageType;
import org.junit.Test;

/**
 * Tests BootConfigVerifier.
 */
public class BootConfigVerifierTest extends TestBase {

    private File rootDir;
    private BootstrapParams parameters;
    private List<String> mountPoints;
    private List<String> rnLogMountPoints;
    private String adminMountPoint;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        suppressSystemError();
        mountPoints = new ArrayList<String>();
        rnLogMountPoints = new ArrayList<String>();

        File mountPoint1 = new File("mountPoint1");
        File mountPoint2 = new File("mountPoint2");
        File mountPoint3 = new File("mountPoint3");

        if (!mountPoint1.exists()) {
            mountPoint1.mkdir();
        }
        if (!mountPoint2.exists()) {
            mountPoint2.mkdir();
        }
        if (!mountPoint3.exists()) {
            mountPoint3.mkdir();
        }

        mountPoints.add(mountPoint1.getAbsolutePath());
        mountPoints.add(mountPoint2.getAbsolutePath());
        mountPoints.add(mountPoint3.getAbsolutePath());

        File rnLogMountPoint1 = new File("rnLogMountPoint1");
        File rnLogMountPoint2 = new File("rnLogMountPoint2");
        File rnLogMountPoint3 = new File("rnLogMountPoint3");

        if (!rnLogMountPoint1.exists()) {
            rnLogMountPoint1.mkdir();
        }
        if (!rnLogMountPoint2.exists()) {
            rnLogMountPoint2.mkdir();
        }
        if (!rnLogMountPoint3.exists()) {
            rnLogMountPoint3.mkdir();
        }

        rnLogMountPoints.add(rnLogMountPoint1.getAbsolutePath());
        rnLogMountPoints.add(rnLogMountPoint2.getAbsolutePath());
        rnLogMountPoints.add(rnLogMountPoint3.getAbsolutePath());

        File adminMountPoint1 = new File("adminMountPoint");

        if (!adminMountPoint1.exists()) {
            adminMountPoint1.mkdir();
        }

        adminMountPoint = adminMountPoint1.getAbsolutePath();

        /* create a BootstrapParams object to store all parameters */
        rootDir = new File("tempkvroot");

        parameters = new BootstrapParams(
                rootDir.getAbsolutePath()/*root directory*/,
                "localhost"/*Hostname*/,
                "localhost"/*HA hostname*/,
                "13002,13010"/*HA range port*/,
                "15002,15020"/*Service range port*/,
                null/*storeName*/,
                13000/*registry port*/,
                -1,
                3/*capacity*/,
                null/*storageType*/,
                null/*securityDir*/,
                true/*hostingAdmin*/,
                null);

        /* Add system info */
        parameters.setNumCPUs(1);
        parameters.setMemoryMB(1);
        parameters.setStorgeDirs(mountPoints, null);
        parameters.setRNLogDirs(rnLogMountPoints, null);
        parameters.setAdminDir(adminMountPoint, null);

        parameters.setMgmtTrapHost("localhost");
        parameters.setMgmtTrapPort(13021);
        parameters.setMgmtPollingPort(15021);
    }

    @Override
    public void tearDown()
        throws Exception {

        resetSystemError();
        if (rootDir != null) {
            rootDir.delete();
        }

        for (String filePath : mountPoints) {
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
        }

        for (String filePath : rnLogMountPoints) {
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
        }
        File file = new File(adminMountPoint);
        if (file.exists()) {
            file.delete();
        }
        super.tearDown();
    }

    @Test
    public void testNoReturnOnError() throws InterruptedException {
        /*
         * Test the return of verify method of BootConfigVerifier
         * when the parameter returnOnError is set as false.
         */
        BootConfigVerifier bootConfigVerifier =
                new BootConfigVerifier(parameters, false);

        /* Return of verify is true when returnOnError is set as false */
        assertTrue(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    @Test
    public void testPort() throws InterruptedException {
        /*
         * Test the return of verify method of BootConfigVerifier
         * when the parameter returnOnError is set as false.
         */

        /* Create root directory */
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        final BootConfigVerifier bootConfigVerifier =
                new BootConfigVerifier(parameters, true);
        /*
         * To avoid port not released completely, use PollCondition to poll
         * the result of verification periodically
         */
        boolean result = new PollCondition(1000, 30000) {
            @Override
            protected boolean condition() {
                return bootConfigVerifier.verify();
            }
        }.await();
        assertTrue(result);

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /* Set ports for BootstrapParams */
        parameters.setHAPortRange("13000,13010");
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /* Set ports for BootstrapParams */
        parameters.setHAPortRange("13002,13010");

        ServerSocket ss = null;
        try {
            /* Bind port 13000 and check whether BootConfigVerifier finds it */
            ss = new ServerSocket(13000);
            assertFalse(bootConfigVerifier.verify());
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

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /* Change capacity */
        parameters.setCapacity(10);

        parameters.setHAPortRange("13002,13010");
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /*
         * Reinstantiate a new object of BootstrapParams to not start
         * admin node
         */
        parameters = new BootstrapParams(
                rootDir.getAbsolutePath()/*root directory*/,
                "localhost"/*Hostname*/,
                "localhost"/*HA hostname*/,
                "13002,13010"/*HA range port*/,
                "15002,15040"/*Service range port*/,
                null/*storeName*/,
                13000/*registry port*/,
                -1,
                9/*capacity*/,
                null/*storageType*/,
                null/*securityDir*/,
                false/*hostingAdmin*/,
                null);


        for (int i=3; i<9; i++) {
            File mountPoint = new File("mountPoint" + (i+1));
            if (!mountPoint.exists()) {
                mountPoint.mkdir();
            }
            mountPoints.add(mountPoint.getAbsolutePath());
        }

        /* Add system info */
        parameters.setNumCPUs(1);
        parameters.setMemoryMB(1);
        parameters.setStorgeDirs(mountPoints, null);

        parameters.setMgmtTrapHost("localhost");
        parameters.setMgmtTrapPort(13041);
        parameters.setMgmtPollingPort(15041);

        /*
         * KVStore need the number of ports is equal to the capacity
         * when do not start admin node
         */
        final BootConfigVerifier verifier =
                new BootConfigVerifier(parameters, true);
        result = new PollCondition(1000, 30000) {
            @Override
            protected boolean condition() {
                return verifier.verify();
            }
        }.await();
        assertTrue(result);

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    @Test
    public void testHost() throws InterruptedException {
        /* Create root directory */
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        parameters.setHAHostname("xxx");
        BootConfigVerifier bootConfigVerifier =
                new BootConfigVerifier(parameters, true);
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    @Test
    public void testDirectory() throws InterruptedException {
        if (rootDir.exists()) {
            rootDir.delete();
        }

        BootConfigVerifier bootConfigVerifier =
                new BootConfigVerifier(parameters, true);
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        /* Remove a mount point */
        File file = new File(mountPoints.get(0));
        file.delete();

        /* Recreate root directory */
        rootDir.mkdir();

        assertFalse(bootConfigVerifier.verify());
        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    /**
     * Test to check whether the size of storage directories on a disk is
     * greater than disk size.
     * @throws IOException
     */
    @Test
    public void testAllStorageDirectoriesSizeOnDisk() throws IOException {
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        /*
         * Test the return of checkAllStorageDirectoriesSizeOnDisk(ParameterMap)
         * method of ParametersValidator
         */
        String directory = mountPoints.get(0);
        FileStore fileStore = Files.getFileStore(Path.of(directory));
        long totalSpace = fileStore.getTotalSpace();

        if (totalSpace > 0) {
            double totalSpaceInGB = (double) totalSpace / (1024 * 1024 * 1024);

            /*
             * Test to check the size of all directories in a disk is less than
             * the disk capacity.
             *
             * Expected result is true (no error occurred)
             */
            int sizeInGB = (int) (totalSpaceInGB / 3);
            String sizeString = sizeInGB + "_gb";
            String[] sizes = {sizeString, sizeString, sizeString};
            parameters.setStorgeDirs(mountPoints, Arrays.asList(sizes));
            String retMsg = ParametersValidator.
                            checkAllStorageDirectoriesSizeOnDisk(
                            parameters.getStorageDirMap());
            assertNull(retMsg);

            /*
             * Test to check the size of all directory in a disk is greater
             * than the disk capacity.
             * Expected result is false (error occurred)
             */
            sizeInGB = (int) (Math.ceil(totalSpaceInGB / 3));
            sizeString = sizeInGB + "_gb";
            sizes = new String[]{sizeString, sizeString, sizeString};
            parameters.setStorgeDirs(mountPoints, Arrays.asList(sizes));
            retMsg = ParametersValidator.
                     checkAllStorageDirectoriesSizeOnDisk(
                     parameters.getStorageDirMap());
            assertNotNull(retMsg);
            String expectedPattern = "Error: The combined requested storage";
            assertTrue(retMsg.startsWith(expectedPattern));
        }
    }

    /**
     * Test no enough storage dirs specified
     * @throws InterruptedException
     */
    @Test
    public void testLessStorageDirs() throws InterruptedException {
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        /* Change the capacity to 4 */
        parameters.setCapacity(4);

        BootConfigVerifier bootConfigVerifier =
                new BootConfigVerifier(parameters, true);
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);


        /* Change the capacity to 3 */
        parameters.setCapacity(3);

        bootConfigVerifier = new BootConfigVerifier(parameters, true);
        assertTrue(bootConfigVerifier.verify());

        for (File file : rootDir.listFiles()) {
            file.delete();
        }
        rootDir.delete();

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    @Test
    public void testSystemInfo() throws InterruptedException {
        /* Create root directory */
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        parameters.setNumCPUs(100);
        parameters.setMemoryMB(1);

        BootConfigVerifier bootConfigVerifier =
                new BootConfigVerifier(parameters, true);
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);

        parameters.setNumCPUs(1);
        parameters.setMemoryMB(2000000);

        bootConfigVerifier = new BootConfigVerifier(parameters, true);
        assertFalse(bootConfigVerifier.verify());

        /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
        Thread.sleep(5000);
    }

    /**
     * Tests the storage types of a storage directory. The valid storage
     * types are HD, NVME, SSD, and UNKNOWN. If the storage type is not
     * valid then BootConfigVerifier can't verify the parameters.
     * @throws InterruptedException for Thread.sleep()
     */
    @Test
    public void testStorageType() throws InterruptedException {
        /* Create root directory */
        if (!rootDir.exists()) {
            rootDir.mkdir();
        }

        /* A few invalid storage type examples */
        String[] invalidTypes = {
                "nfs",
                "hard drive",
                "null",
                "network-file-system"
        };

        for (String type : invalidTypes) {
            parameters.setStorageType(type);
            BootConfigVerifier bootConfigVerifier =
                    new BootConfigVerifier(parameters, true);
            assertFalse(bootConfigVerifier.verify());

            /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
            Thread.sleep(5000);
        }

        for (StorageType type : StorageType.values()) {
            parameters.setStorageType(type.name());
            BootConfigVerifier bootConfigVerifier =
                    new BootConfigVerifier(parameters, true);
            assertTrue(bootConfigVerifier.verify());

            /* Wait 5000ms to ensure the status of ports are not WAIT_TIME */
            Thread.sleep(5000);
        }
    }
}
