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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Properties;

import oracle.kv.KVVersion;

/**
 * Utility class to handle snapshot related file operations.
 */
public class SnapshotFileUtils {

    /**
     * Define whether to override and update configuration when restore
     * snapshot.
     * TRUE - Override and update existing configurations.
     * FALSE - Do not override or update existing configurations.
     * UNKNOWN - No specification. By default the restore will check for
     * difference between restore file and existing file. If they are the same,
     * do not do the restore. If they are different, throw exception with
     * warning message enforce user to specify update config option.
     */
    public static enum UpdateConfigType {
        TRUE,
        FALSE,
        UNKNOWN
    }

    private static final String SNAPSHOT_STATUS_FILE_NAME = "snapshot.stat";

    private static final String VERSION_KEY = "version";

    /**
     * Create snapshot of configuration into specific snapshot directory.
     * @param src source file to snapshot.
     * @param snapshotDir snapshot directory to store the snapshot of
     * configuration.
     */
    public static void snapshotConfig(File src,
                                      File snapshotDir)
        throws IOException {

        /* Fail if source file does not exist */
        if (!src.exists()) {
            throw new IOException(
                "Specified snapshot config does not exist: " +
                src.toString());
        }

        final File dest = new File(snapshotDir, src.getName());

        /* Create snapshot directory if not exist */
        if (!snapshotDir.exists()) {
            snapshotDir.mkdirs();
        }

        if (src.isDirectory()) {
            FileUtils.copyDir(src, dest);
        } else {
            FileUtils.copyFile(src, dest);
        }
    }

    /**
     * Remove snapshot of configurations
     * @param rootDir root directory which contains snapshot base directory.
     * @param snapshotName full name of snapshot to be removed. when
     * snapshotName is null, the method will delete all the snapshots under
     * snapshot base directory.
     */
    public static void removeSnapshotConfig(File rootDir,
                                            String snapshotName) {

        if (snapshotName == null) {
            /* Delete all snapshots under base directory */
            final File snapshotBaseDir =
                FileNames.getSnapshotBaseDir(rootDir);
            for (File file : snapshotBaseDir.listFiles()) {
                if (file.isDirectory() && file.exists()) {
                    FileUtils.deleteDirectory(file);
                }
            }
        } else {
            /* Delete specified snapshot */
            final File snapshotDir =
                FileNames.getSnapshotNamedDir(rootDir, snapshotName);
            if (snapshotDir.exists()) {
                FileUtils.deleteDirectory(snapshotDir);
            } else {
                throw new IllegalStateException(
                    "Fail to find snapshot dir: " + snapshotDir.toString());
            }
        }
    }

    /**
     * Restore configurations from snapshot.
     * @param dest target configuration to be restored.
     * @param snapshotDir snapshot directory contains snapshot of
     *                    configurations.
     * @param isUpdateConfig whether the restore should update the existing
     * configuration.
     * @throws IOException
     */
    public static void restoreSnapshotConfig(File dest,
                                             File snapshotDir,
                                             UpdateConfigType isUpdateConfig)
        throws IOException {

        if (isUpdateConfig == UpdateConfigType.FALSE) {
            return;
        }

        try {

            final File parent = dest.getParentFile();
            if (!parent.exists()) {
                throw new IllegalStateException(
                    "Target's parent directory does not exist: " +
                    parent.toString());
            }

            if (!snapshotDir.exists()) {
                throw new IllegalStateException(
                    "Snapshot directory does not exist: " +
                    snapshotDir.toString());
            }

            final File src = new File(snapshotDir, dest.getName());
            if (!src.exists()) {
                throw new IllegalStateException(
                    "Snapshot config does not exist: " + src.toString());
            }

            /*
             * If restore target already exists, user did not specify whether to
             * update configuration, then compare the snapshot file and target
             * file. If snapshot file and target file are not different, do
             * not override. If snapshot file and target file are different, throw
             * exception with warning message to allow user to explicitly specify
             * -update-config option.
             */
            if (isUpdateConfig.equals(UpdateConfigType.UNKNOWN) &&
                dest.exists()) {
                if (src.isDirectory() && dest.isDirectory()) {
                    if (!compareDirContent(src, dest)) {
                        throw new IllegalStateException(
                            "config file is different, use -update-config true");
                    }
                } else {
                    if (!compareFileContent(src, dest)) {
                        throw new IllegalStateException(
                            "config file is different, use -update-config true");
                    }
                }
                return;
            }

            /* Override target file */
            if (dest.exists()) {
                if (dest.isDirectory()) {
                    FileUtils.deleteDirectory(dest);
                } else {
                    dest.delete();
                }
            }

            if (src.isDirectory()) {
                FileUtils.copyDir(src, dest);
            } else {
                FileUtils.copyFile(src, dest);
            }
        } catch (Exception e) {
            snapshotOpFail(SnapshotOp.RESTORE, snapshotDir);
            /* re-throw the exception after operation fail is marked */
            throw new IllegalStateException(e);
        }
    }

    /**
     * Snapshot operation type.
     * SNAPSHOT - snapshot operation to backup configurations.
     * RESTORE - restore operation to recover configurations.
     */
    public enum SnapshotOp {
        SNAPSHOT,
        RESTORE
    }

    /**
     * Snapshot operation status.
     * STARTED - operation has started.
     * COMPLETED - operation has completed.
     */
    private enum SnapshotOpStatus {
        STARTED,
        COMPLETED
    }

    /**
     * Simple interface to define an executable task.
     */
    public interface SnapshotConfigTask {
        void execute() throws IOException;
    }

    /**
     * Snapshot task handler interface.
     */
    public interface SnapshotTaskHandler {
        void handleTask(SnapshotConfigTask task);
    }

    /*
     * Class to handle lock of of the snapshot directory. Multiple processes
     * working on the same directory will be blocked until the lock file of
     * is released by current working process.
     */
    public static class SnapshotLockHandler implements SnapshotTaskHandler {

        private static final String SNAPSHOT_LOCK_FILE_NAME =
            "snapshot.lck";
        private static final int MAX_RETRY_ACQUIRE_LOCK = 10;
        private static final long AWAIT_TIME_MILLIS = 1000;
        private final File lockFile;

        public SnapshotLockHandler(File snapshotDir) {
            this.lockFile = new File(snapshotDir, SNAPSHOT_LOCK_FILE_NAME);
        }

        @Override
        public void handleTask(SnapshotConfigTask task) {

            /* try to lock the directory */
            for (int i=0; i < MAX_RETRY_ACQUIRE_LOCK; i++) {
                try (final FileChannel channel =
                         new RandomAccessFile(lockFile, "rwd").getChannel();
                     final FileLock locker = channel.tryLock();) {
                    if (locker != null) {
                        task.execute();
                        break;
                    }
                } catch (OverlappingFileLockException ignore) {
                } catch (IOException ioe) {
                    throw new IllegalStateException(ioe);
                }
                /* Reach maximum retry */
                if (i == MAX_RETRY_ACQUIRE_LOCK -1) {
                    throw new IllegalStateException(
                        "Fail to acquire lock after " +
                        MAX_RETRY_ACQUIRE_LOCK + " retry");
                }
                try {
                    Thread.sleep(AWAIT_TIME_MILLIS);
                } catch (InterruptedException ignore) {}
            }
        }
    }

    /**
     * Mark the status file on the snapshot directory to indicate start of
     * snapshot operation.
     * @param op current operation to be marked. It could be SNAPSHOT
     *           or RESTORE.
     * @param snapshotDir the snapshot directory to create the status file.
     * @throws IOException
     */
    public static void snapshotOpStart(SnapshotOp op,
                                       File snapshotDir) throws IOException {

        final File statusFile =
            new File(snapshotDir, SNAPSHOT_STATUS_FILE_NAME);

        if (!statusFile.exists()) {
            statusFile.createNewFile();
        }

        /* Read status file */
        final Properties props = new Properties();
        try (final FileInputStream statusInStream =
           new FileInputStream(statusFile)) {
           props.load(statusInStream);
        }

        final String statusKey = op.toString();
        final String status = props.getProperty(statusKey);
        if (status != null &&
            status.equals(SnapshotOpStatus.STARTED.toString())) {
            throw new IllegalStateException(
                "unexpected snapshot operation state");
        }

        /*
         * Check completion of specific SNAPSHOT operation
         * before run RESTORE operation
         */
        if (op.equals(SnapshotOp.RESTORE)) {
            final String snapshotKey = SnapshotOp.SNAPSHOT.toString();
            final String snapStatus =
                props.getProperty(snapshotKey);
            if (snapStatus == null ||
                snapStatus.equals(SnapshotOpStatus.STARTED.toString())) {
                throw new IllegalStateException(
                    "Cannot not restore incomplete snapshot");
            }
        }

        /* Update status to STARTED */
        props.put(
            statusKey, SnapshotOpStatus.STARTED.toString());
        props.put(
            VERSION_KEY,
            KVVersion.CURRENT_VERSION.
                getNumericVersionString());

        try (final FileOutputStream statusOutStream =
            new FileOutputStream(statusFile)) {
            props.store(
                statusOutStream, statusFile.getParentFile().getName());
        }
    }

    /*
     * Remove the snapshot op key in the stat file when op fail.
     */
    public static void snapshotOpFail(SnapshotOp op,
                                      File snapshotDir) throws IOException {

        final File statusFile =
            new File(snapshotDir, SNAPSHOT_STATUS_FILE_NAME);

        if (!statusFile.exists()) {
            return;
        }

        /* Read status file */
        final Properties props = new Properties();
        try (final FileInputStream statusInStream =
           new FileInputStream(statusFile)) {
           props.load(statusInStream);
        }

        final String statusKey = op.toString();
        props.remove(statusKey);

        try (final FileOutputStream statusOutStream =
            new FileOutputStream(statusFile)) {
            props.store(
                statusOutStream, statusFile.getParentFile().getName());
        }
    }

    /**
     * Mark the status file on the snapshot directory to indicate completion
     * of snapshot operation.
     * @param op current operation to be marked. It could be SNAPSHOT
     *           or RESTORE.
     * @param snapshotDir the snapshot directory to create the status file.
     * @throws IOException
     */
    public static void snapshotOpComplete(SnapshotOp op,
                                          File snapshotDir)
        throws IOException {

        final File statusFile =
            new File(snapshotDir, SNAPSHOT_STATUS_FILE_NAME);

        final Properties props = new Properties();

        /* reload status file */
        try (final FileInputStream statusInStream =
            new FileInputStream(statusFile)) {
            props.load(statusInStream);
        }

        final String statusKey = op.toString();
        /* Update status to COMPLETE */
        props.put(
            statusKey, SnapshotOpStatus.COMPLETED.toString());

        try (final FileOutputStream statusOutStream =
            new FileOutputStream(statusFile)) {
            props.store(
                statusOutStream, statusFile.getParentFile().getName());
        }
    }

    /*
     * Detect difference of two files
     */
    private static boolean compareFileContent(File file1, File file2) {

        if (!file1.exists() || !file2.exists()) {
            return false;
        }

        if (file1.isDirectory() || file2.isDirectory()) {
            return false;
        }

        /* Compute hash of bytes of file to detect difference */
        try {
            final byte[] b1 = Files.readAllBytes(file1.toPath());
            final byte[] hash1 = MessageDigest.getInstance("MD5").digest(b1);
            final byte[] b2 = Files.readAllBytes(file2.toPath());
            final byte[] hash2 = MessageDigest.getInstance("MD5").digest(b2);
            return Arrays.equals(hash1, hash2);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /*
     * Detect difference of two directories
     */
    private static boolean compareDirContent(File dir1, File dir2) {
        for (File file : dir1.listFiles()) {
            if (file.isDirectory()) {
                if (!compareDirContent(file, new File(dir2, file.getName()))) {
                    return false;
                }
            } else {
                if (!compareFileContent(file,
                    new File(dir2, file.getName()))) {
                    return false;
                }
            }
        }
        return true;
    }
}
