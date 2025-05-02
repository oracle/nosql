/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.diagnostic;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;

/**
 * Check parameters are valid or not against environment or requirement.
 * The parameters include network ports, network hosts, directories,
 * memory size and the number of CPUs.
 */

public class ParametersValidator {

    private static final String LOCAL_IP = "127.0.0.1";
    private static final String BROADCAST_IP = "0.0.0.0";

    /**
     * Bind a socket address to check the port in the address is
     * available or not.
     *
     * @param socketAddress
     *
     * @return true when the port in socket address is available; return false
     * when the port is already in use.
     */
    private static boolean bindPort(InetSocketAddress socketAddress) {
        boolean isFree = true;
        ServerSocket ss = null;
        try {
            ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(socketAddress);
        } catch (IOException e) {
            isFree = false;
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    isFree = false;
                }
            }
        }

        return isFree;
    }

    /**
     * Bind a port to check the port is available or not.
     *
     * @param port
     *
     * @return true when the port is available; return false when the port
     * is already in use.
     */
    private static boolean bindPort(int port) {
        boolean isFree = true;
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
        } catch (IOException e) {
            isFree = false;
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    isFree = false;
                }
            }
        }

        return isFree;
    }

    /**
     * Check a port is available or not against the current environment.
     *
     * @param parameterName associated name of port
     * @param port value of port
     *
     * @return null when the port is available; return error message when the
     * port is already in use.
     */
    public static String checkPort(String parameterName, int port) {
        try {
            if (bindPort(port) &&
               bindPort(new InetSocketAddress(InetAddress.getLocalHost().
                   getHostName(), port)) &&
               bindPort(new InetSocketAddress(BROADCAST_IP, port)) &&
               bindPort(new InetSocketAddress(LOCAL_IP, port))) {
                return null;
            }
            return "Specified " + parameterName + " " +
                   port + " is already in use";
        } catch (UnknownHostException ex) {
            return ex.toString();
        }
    }

    /**
     * Check a directory exists or not.
     *
     * @param parameterName associated name of directory
     * @param directoryName path of directory
     *
     * @return null when the directory exists; return error message when the
     * directory does not exist.
     */
    public static String checkDirectory(String parameterName,
                                        String directoryName) {
        return checkDirectory(parameterName, directoryName, 0L);
    }

    /**
     * Checks that a directory exists.  If requiredSize is not zero, the
     * directory's size will be checked to see if it is at least that size.
     * If requiredSize is less than zero, then invalid directory size message
     * will be returned. If directory does not have write permission then
     * specified message will be returned.
     *
     * @param parameterName associated name of directory
     * @param directoryName path of directory
     * @param requiredSize size of the directory or 0
     *
     * @return null when the directory exists; return error message when the
     * directory does not exist.
     */
    public static String checkDirectory(String parameterName,
                                        String directoryName,
                                        long requiredSize) {
        if (requiredSize < 0) {
            return "Invalid directory size specified for " + parameterName;
        }
        final File file = new File(directoryName);
        final String reason = FileUtils.verifyDirectory(file, requiredSize);
        if (reason != null) {
            return "Specified " + parameterName + " " + reason;
        }
        if (!file.canWrite()) {
            return "Specified directory for " + parameterName +
                    " does not have write permission";
        }
        return null;
    }

    /**
     * Check whether the size of storage directories on a disk is less than
     * or equal to disk size.
     *
     * @param storageDirMap the storage directory map
     * @return null when size of storage directories is less than or equal to
     * the disk size; return error message when size of storage directories is
     * greater than the disk size or an error occurs or if there is another
     * problem checking the directories
     */

    public static String checkAllStorageDirectoriesSizeOnDisk(
        ParameterMap storageDirMap)
    {
        if (storageDirMap.isEmpty()) {
            return null;
        }
        return FileUtils.verifyAllStorageDirectoriesSizeOnDisk(
            storageDirMap);
    }

    /**
     * Check whether the number of storage directories greater than capacity.
     *
     * @param numStorageDir the number of storage directories
     * @param capacity
     * @param type  directory type e.g. storage or log directory
     * @return null when the number of storage directories equal to or greater
     * than capacity; return error message when the number of storage
     * directories less than capacity.
     */
    public static String checkNumberOfDirectories(int numStorageDir,
                                                  int capacity,
                                                  String type) {
        if (capacity > numStorageDir) {
            return "The specified number of " + type + " directories " +
                    numStorageDir + " is less than the capacity " + capacity;
        }
        return null;
    }

    /**
     * Check a file exists or not.
     *
     * @param parameterName associated name of file
     * @param directoryName path of directory contains the file
     * @param fileName name of file
     *
     * @return null when the file exists; return error message when the
     * file does not exist.
     */
    public static String checkFile(String parameterName,
                                    String directoryName,
                                    String fileName) {
        File file = new File(directoryName, fileName);
        if (file.exists() && file.isFile()) {
            return null;
        }
        return "Specified " + parameterName + " " + fileName +
               " does not exist in " + directoryName;
    }

    /**
     * Check a file exists or not.
     *
     * @param parameterName associated name of file
     * @param filePath path of file
     *
     * @return null when the file exists; return error message when the
     * file does not exist.
     */
    public static String checkFile(String parameterName, String filePath) {
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            return null;
        }
        return "Specified " + parameterName + " " + filePath +
               " does not exist";
    }

    /**
     * Check a host is reachable or not.
     *
     * @param parameterName associated name of host
     * @param hostname name of host
     *
     * @return null when the host is reachable; return error message when the
     * file is unreachable.
     */
    public static String checkHostname(String parameterName, String hostname) {

        boolean isReachable = true;
        try {
            InetAddress.getByName(hostname);
        } catch (UnknownHostException ex) {
            isReachable = false;
        }
        if (isReachable) {
            return null;
        }
        return "Specified " + parameterName + " " +
               hostname +  " not reachable";
    }

    /**
     * Check a local host is resolvable or not.
     *
     *
     * @return null when the local host is resolvable; return error message
     * when the file is not resolvable.
     */
    public static String checkLocalHostname() {

        boolean isResolvable = true;
        try {
            InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            isResolvable = false;
        }
        if (isResolvable) {
            return null;
        }
        return "The local host name could not be resolved into an address";
    }

    /**
     * Check ports within a range are available or not.
     *
     * @param parameterName associated name of the range of ports
     * @param rangePorts a range of ports
     *
     * @return null when all ports are available; return error message when a
     * or more ports are not available.
     */
    public static String checkRangePorts(String parameterName,
                                         String rangePorts) {
        StringTokenizer tokenizer = new StringTokenizer(rangePorts,
                ParameterUtils.HELPER_HOST_SEPARATOR);
        int firstHAPort = Integer.parseInt(tokenizer.nextToken());
        int secondHAPort = Integer.parseInt(tokenizer.nextToken());
        String retMsg;
        for (int i=firstHAPort; i<=secondHAPort; i++) {
            retMsg = checkPort(parameterName, i);
            if (retMsg != null) {
                return retMsg;
            }
        }
        return null;
    }

    /**
     * Check a number is greater than or equal to 0 or not.
     *
     * @param parameterName associated name of the number
     * @param positiveInteger value of the number
     *
     * @return null when the number is greater than or equal to 0; return
     * error message when the number is less than 0.
     */
    public static String checkPositiveInteger(String parameterName,
                                              int positiveInteger) {
        if (positiveInteger < 0) {
            return positiveInteger + " is invalid; " +
                   parameterName + " must be >= 0";
        }
        return null;

    }



    /**
     * Check the number of ports within a range is greater than or equal to
     * the expected number or not.
     *
     * @param parameterName associated name of the range of ports
     * @param rangePorts a range of ports
     * @param expectedNumberOfPort the expected number of ports
     *
     * @return null when the number of ports within a range greater than or
     * equal to the expected number; return error message when the number
     * of ports within a range is less than the expected number
     */
    public static String checkRangePortsNumber(String parameterName,
                                               String rangePorts,
                                               int expectedNumberOfPort) {
        StringTokenizer tokenizer = new StringTokenizer(rangePorts,
                ParameterUtils.HELPER_HOST_SEPARATOR);
        int firstHAPort = Integer.parseInt(tokenizer.nextToken());
        int secondHAPort = Integer.parseInt(tokenizer.nextToken());

        if (secondHAPort - firstHAPort + 1 < expectedNumberOfPort) {
            return "Specified " + parameterName + " " + rangePorts +
                   " size is less than the number of nodes " +
                   expectedNumberOfPort;
        }
        return null;
    }

    /**
     * Check the number of CPU of computer is greater than or equal to
     * the expected number or not.
     *
     * @param parameterName associated name of the CPU number
     * @param cpuNums the expected number of CPU
     *
     * @return null when the number of CPU of computer is greater than or
     * equal to the expected number; return error message when the number
     * of CPU is less than the expected number
     */
    public static String checkCPUNumber(String parameterName, int cpuNums) {
        OperatingSystemMXBean bean =
                ManagementFactory.getOperatingSystemMXBean();

        if (cpuNums > bean.getAvailableProcessors()) {
            return cpuNums + " is invalid; " + parameterName +" must be <= " +
                   bean.getAvailableProcessors() +
                   "(the number of available processors)";
        }
        return null;
    }

    /**
     * Check the memory size of computer is greater than or equal to
     * the expected number or not.
     *
     * @param parameterName associated name of the memory size
     * @param memSizeMB the expected memory size in MB
     * @param capacity the capacity of a SNA
     *
     * @return null when the memory size of computer is greater than or
     * equal to the expected size; return error message when memory size
     * is less than the expected size
     */
    public static String checkMemorySize(String parameterName,
                                         int memSizeMB, int capacity) {
        OperatingSystemMXBean bean =
                ManagementFactory.getOperatingSystemMXBean();
        long physicalMemory = 0;
        String jvmDataModelString = null;
        try {
            final Class<? extends OperatingSystemMXBean> beanClass =
                Class.forName("com.sun.management.OperatingSystemMXBean")
                .asSubclass(OperatingSystemMXBean.class);
            if (beanClass.isInstance(bean)) {
                final Method m = beanClass.getMethod(
                    "getTotalPhysicalMemorySize");
                m.setAccessible(true);
                physicalMemory = (Long) m.invoke(bean);

                /*
                 * This call will work because, if the above worked, we are
                 * likely using a Sun JVM.
                 */
                jvmDataModelString = System.getProperty("sun.arch.data.model");
            }
        } catch (Exception e) {
        }
        final int numJVMs = capacity == 0 ? 1 : capacity;
        int jvmDataModel = 0;
        if (jvmDataModelString != null) {
            try {
                jvmDataModel = Integer.parseInt(jvmDataModelString);
            } catch (NumberFormatException e) {
            }
        }
        return checkMemorySize(parameterName, memSizeMB, numJVMs,
                               physicalMemory, jvmDataModel);
    }

    /*
     * Check the memory size.
     *
     * @param parameterName associated name of the memory size
     * @param memSizeMB the expected memory size in MB
     * @param numJVMs the number of JVMs using the memory
     * @param physicalMemory available memory in bytes
     * @param jvmDataModel 0 if unknown; otherwise 32 or 64 depending on the
     * data model being used by the JVM
     *
     * @return an error message string or null if no error
     */
    static String checkMemorySize(String parameterName,
                                  int memSizeMB,
                                  int numJVMs,
                                  long physicalMemory,
                                  int jvmDataModel) {
        final int availableMemMB =
            computeAvailableMemoryMB(numJVMs, physicalMemory, jvmDataModel);

        if (memSizeMB > availableMemMB) {
            final String memorySizeWarning = (jvmDataModel == 32) ?
                ", which may be limited because of using a 32-bit Java" +
                " virtual machine" :
                "";
            return memSizeMB + " is invalid; " +
                parameterName + " must be <= " +
                availableMemMB + " (the total available memory in MB" +
                memorySizeWarning + ")";
        }

        return null;
    }

    /**
     * Compute the amount of memory available in megabytes.
     *
     * @param capacity the number of virtual machines
     * @param physicalMemory the total physical memory available in bytes, or 0
     * if not known
     * @param jvmDataModel 0 if unknown; otherwise 32 or 64 depending on the
     * data model being used by the jvm
     *
     * @return memory in MB that will actually be used for RN heap. It may be
     * all the allocated physical memory, or adjusted downwards of the JVM
     * being used is 32 bit and cannot address all of it.
     */
    static int computeAvailableMemoryMB(int capacity,
                                        long physicalMemory,
                                        int jvmDataModel) {
        if (jvmDataModel == 32) {

            /*
             * Limit heap per RN to 2GB on a 32-bit JVM.  As a practical matter
             * this number may be smaller yet due to memory used for other
             * purposes by the JVM, because the underlying machine architecture
             * itself is 32-bit, or due to other JVM limitations, but we don't
             * care enough about 32-bit JVMs to worry about this.
             */
            final long max32bitHeap = Integer.MAX_VALUE;
            final long heapPerRN = physicalMemory / capacity;
            if (heapPerRN > max32bitHeap) {
                physicalMemory = max32bitHeap * capacity;
            }
        }
        return (int) (physicalMemory >> 20);
    }

    /**
     * Checks if storageType is valid. StorageType.parseType will
     * throw an IllegalArgumentException if the type is invalid.
     *
     * @param storageType storage type for the SN
     * @return null if the storageType is valid, otherwise return
     * a String message saying that the type isn't supported.
     */
    static String checkStorageType(String storageType) {
        try {
            StorageType.parseType(storageType);
            return null;
        } catch (IllegalArgumentException ex) {
            return ex.getMessage();
        }
    }
}
