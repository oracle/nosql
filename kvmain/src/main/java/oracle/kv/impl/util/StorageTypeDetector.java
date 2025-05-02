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

package oracle.kv.impl.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

/**
 *  The class is to detect the type of storage.
 */
public class StorageTypeDetector {

    /**
     * Storage types.
     */
    public enum StorageType {
        UNKNOWN, HD, SSD, NVME;

        /**
         * Returns a enum storage type corresponding to the input value. The
         * value is case-insensitive. Throws IllegalArgumentException if value
         * is null or does not correspond to a supported type.
         *
         * @param value case-insensitive type string
         * @return a enum storage type
         */
        public static StorageType parseType(String value) {
            if ((value == null) || ("".equals(value))) {
                throw new IllegalArgumentException("Invalid storage type: " +
                                                    value);
            }

            final String upperCaseType =
                                value.toUpperCase(java.util.Locale.ENGLISH);
            try {
                return StorageType.valueOf(upperCaseType);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid storage type: " +
                                                   value);
            }
        }
    }

    /**
     * Returns the type of the storage underlying the specified file. If the
     * type cannot be determined StorageType.UNKNOWN is returned.
     *
     * @param filePath a file name
     * @return the storage type
     * @throws NoSuchFileException if the file does not exits
     */
    public static StorageType detectType(String filePath)
            throws NoSuchFileException {
        return detectType(new File(filePath));
    }

    /**
     * Returns the type of the storage underlying the specified file. If the
     * type cannot be determined StorageType.UNKNOWN is returned.
     *
     * @param file a file name
     * @return the storage type
     * @throws NoSuchFileException if the file does not exits
     */
    public static StorageType detectType(File file)
            throws NoSuchFileException {
        if (!file.exists()) {
            throw new NoSuchFileException(file.getAbsolutePath());
        }
        final String os = System.getProperty("os.name");
        if (os.indexOf("Linux") == -1) {
            /* Only support linux (perhaps MacOS?) for now */
            return StorageType.UNKNOWN;
        }

        /*
         * The algorithm of determining the type of a partition is to
         * check the value of the rotational property of the physical disk
         * which holds the partition.
         */
        try {
            final FileStore fileStore =
                    Files.getFileStore(FileSystems.getDefault().
                            getPath(file.getAbsolutePath()));
            /* Get the partition info where the file is */
            final String partitionPath = fileStore.name();
            final String partitionName = partitionPath.replace("/dev/", "");
            /*
             * NVMe devices and partitions typically follow a standard naming
             * convention https://itpeernetwork.intel.com/finding-your-new-intel
             * -ssd-for-pcie-think-nvme-not-scsi/
             */
            if (partitionName.startsWith("nvme")) {
                return StorageType.NVME;
            }

            /* Now look for SSDs */
            /*
             * The rotational property of the physical disks in the file
             * /sys/block/<disk name>/queue/rotational
             */
            final File blockDir = new File("/sys/block");

            /*
             * Iterate all physical disks in /sys/block and check which the
             * physical disk name forms the partition name prefix.
             */
            for (File diskFile : blockDir.listFiles()) {
                if (!partitionName.startsWith(diskFile.getName(), 0)) {
                    continue ;
                }
                /* Found block device associated with the partition */
                return isSSD(diskFile) ? StorageType.SSD :  StorageType.HD;
            }
            return StorageType.UNKNOWN;
        } catch (IOException e) {
            return StorageType.UNKNOWN;
        }
    }

    private static boolean isSSD(File file) {

        File rotaFile = new File(file, "queue/rotational");
        if (!rotaFile.exists()) {
            return false;
        }

        /* The rotational property of SSD/NVME is 0, and HD is 1 */
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(rotaFile));
            String rotationalValue = reader.readLine().trim();
            if (rotationalValue.equals("0")) {
                return true;
            }
            return false;
        } catch (IOException e) {
            /* Determine the disk is not SSD when catch exception during read */
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
