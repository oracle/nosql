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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.SizeParameter;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Collection of utilities for file operations
 */
@NonNullByDefault
public class FileUtils {

    /**
     * Cache for computing SHA-256 message digests used to compare file
     * contents.
     */
    private static final ThreadLocal<MessageDigest> sha256MessageDigestCache =
        ThreadLocal.withInitial(() -> {
                try {
                    return MessageDigest.getInstance("SHA-256");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException(
                        "SHA-256 MessageDigest not found: " + e.getMessage(),
                        e);
                }
            });

    /**
     * Cache of byte arrays for reading data from an input stream when
     * computing message digests.
     */
    private static final ThreadLocal<byte[]> byteArrayCache =
        ThreadLocal.withInitial(() -> new byte[4096]);

    /**
     * Copy a file
     * @param sourceFile the file to copy from, which must exist
     * @param destFile the file to copy to.  The file is created if it does
     *        not yet exist.
     */
    public static void copyFile(File sourceFile, File destFile)
        throws IOException {

        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        try (final FileInputStream source = new FileInputStream(sourceFile);
             final FileOutputStream dest = new FileOutputStream(destFile)) {
            final FileChannel sourceChannel = source.getChannel();
            dest.getChannel().transferFrom(sourceChannel, 0,
                                           sourceChannel.size());
        }
    }

    /**
     * Copy a directory.
     * @param fromDir the directory to copy from, which must exist.
     * @param toDir the directory to copy to. The directory is created if it
     *        does not yet exist.
     */
    public static void copyDir(File fromDir, File toDir)
        throws IOException {

        if (fromDir == null || toDir == null) {
            throw new NullPointerException("File location error");
        }

        if (!fromDir.isDirectory()) {
            throw new IllegalStateException(
                fromDir +  " should be a directory");
        }

        if (!fromDir.exists()) {
            throw new IllegalStateException(
                fromDir +  " does not exist");
        }

        if (!toDir.exists() && !toDir.mkdirs()) {
            throw new IllegalStateException(
                "Unable to create copy dest dir:" + toDir);
        }

        File [] fileList = fromDir.listFiles();
        if (fileList != null && fileList.length != 0) {
            for (File file : fileList) {
                if (file.isDirectory()) {
                    copyDir(file, new File(toDir, file.getName()));
                } else {
                    copyFile(file, new File(toDir, file.getName()));
                }
            }
        }
    }

    /**
     * Write a string to file.
     */
    public static void writeStringToFile(File destFile, String text)
        throws IOException {

        try (final BufferedWriter out =
                 new BufferedWriter(new FileWriter(destFile))) {
            out.write(text);
        }
    }

    /**
     * Write binary data to file.
     */
    public static void writeBytesToFile(final File destFile,
                                        final byte[] bytes)
        throws IOException {

        OutputStream output = null;
        try {
            output = new BufferedOutputStream(new FileOutputStream(destFile));
            output.write(bytes);
        }
        finally {
            if (output != null) {
                output.close();
            }
        }
    }

    /**
     * Recursively delete a directory and its contents.  Makes use of features
     * introduced in Java 7.  Does NOT follow symlinks.
     */
    public static boolean deleteDirectory(File d) {
        try {
            Files.walkFileTree(d.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult
                    visitFile(@Nullable Path path,
                              @Nullable BasicFileAttributes attrs)
                    throws IOException {

                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(
                    @Nullable Path path, @Nullable IOException ioe)
                    throws IOException {

                    if (ioe != null) {
                        throw ioe;
                    }

                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Verifies that the specified file exists and is a directory. If both
     * of the checks succeed null is returned, otherwise a description of the
     * failure is returned.
     *
     * @param directory the directory to verify
     * @return a reason string if the verify fails or null
     */
    public static @Nullable String verifyDirectory(File directory) {
        if (!directory.exists()) {
            return directory.getAbsolutePath() + " does not exist";
        }
        if (!directory.isDirectory()) {
            return directory.getAbsolutePath() + " is not a directory";
        }
        return null;
    }

    /**
     * Verifies that the specified file exists and is a directory. If
     * requiredSize is &gt; 0 the directory size is also checked. If all of
     * the checks succeed null is returned, otherwise a description of the
     * failure is returned.
     *
     * @param directory the directory to verify
     * @param requiredSize the required size of the directory, or 0L
     * @return a reason string if the verify fails or null
     */
    public static @Nullable String verifyDirectory(File directory,
                                                   long requiredSize) {
        final String reason = verifyDirectory(directory);
        if ((reason == null) && (requiredSize > 0L)) {
            final long actual = getDirectorySize(directory);
            if (actual < requiredSize) {
                return  directory.getAbsoluteFile() + " size of " + actual +
                        " is less than the required size of " + requiredSize;
            }
        }
        return reason;
    }

    /**
     * Verifies whether the size of storage directories is less than or
     * equal to the disk size.
     *
     * @param storageDirMap the storage directory map
     * @return null when size of storage directories is less than or equal
     * to the disk size; return error message when size of storage
     * directories is greater than the disk size or if there is an error
     * occurred
     */
    public static @Nullable String verifyAllStorageDirectoriesSizeOnDisk(
        ParameterMap storageDirMap) {

        Map<FileStore, long[]> fileSysToTotalSizeAndRequiredSizeMap =
            new HashMap<>();
        try {
            for (Parameter storageDir : storageDirMap) {
                long size = SizeParameter.getSize(storageDir);
                String dirName = storageDir.getName();
                File file = new File(dirName);
                Path path = file.toPath();
                long actualSize = FileUtils.getDirectorySize(file);
                FileStore fileSystemStore = Files.getFileStore(path);
                if (!fileSysToTotalSizeAndRequiredSizeMap.
                    containsKey(fileSystemStore)) {
                    fileSysToTotalSizeAndRequiredSizeMap.put(fileSystemStore,
                        new long[]{actualSize, size});
                } else {
                    fileSysToTotalSizeAndRequiredSizeMap.
                        get(fileSystemStore)[1] += size;
                }
            }
        } catch (Exception e) {
            return "Error occurred: " + e.getMessage();
        }

        for (Map.Entry<FileStore, long[]> entry :
            fileSysToTotalSizeAndRequiredSizeMap.entrySet()) {
            FileStore fileStore = entry.getKey();
            long[] sizes = entry.getValue();
            long actualSize = sizes[0];
            long requiredSize = sizes[1];
            if (actualSize < requiredSize) {
                return String.format(
                    "Error: The combined requested storage directory size of" +
                    " %s GB exceeds the actual disk size of %s GB for the %s" +
                    " file store.\n",
                    convertBytesToGB(requiredSize),
                    convertBytesToGB(actualSize),
                    fileStore);
            }
        }
        return null;
    }

    private static String convertBytesToGB(long bytes) {
        double spaceInGB = 0.0;
        if (bytes > 0) {
            spaceInGB = (double) bytes / (1024 * 1024 * 1024);
        }
        return String.format("%.2f", spaceInGB);
    }

    /**
     * Gets the size of the specified directory. If the directory does not
     * exist, is not a directory, or if there is an exception getting the
     * size an IllegalArgumentException is thrown.
     *
     * @param directoryName the directory to get the size
     * @return the directory size
     */
    public static long getDirectorySize(String directoryName) {
        final File directory = new File(directoryName);
        final @Nullable String reason = verifyDirectory(directory);
        if (reason != null) {
            throw new IllegalArgumentException("Cannot get size, " + reason);
        }
        return getDirectorySize(directory);
    }

    /**
     * Gets the size of the specified directory. If there is an exception
     * getting the size an IllegalArgumentException is thrown.
     *
     * @param directory the directory to get the size
     * @return the directory size
     */
    public static long getDirectorySize(File directory) {
        try {
            final FileStore fileStore =
                       Files.getFileStore(FileSystems.getDefault().
                                          getPath(directory.getAbsolutePath()));
            /*
             * Fix for KVSTORE-2015: KVLite fails to start with Java 11 when
             * using NFS directory
             *
             * In Java 11, FileStore.getTotalSpace returns a negative value for
             * an NFS-mounted directory on Linux, but it returns Long.MAX_VALUE
             * for that case when using Java 17. The doc for getTotalSpace
             * doesn't say it returns MAX_VALUE when the size isn't known, but
             * the negative value seems to be a bug, so return MAX_VALUE if the
             * value is negative so that things work properly with Java 11.
             */
            long totalSpace = fileStore.getTotalSpace();
            if (totalSpace < 0) {
                totalSpace = Long.MAX_VALUE;
            }
            return totalSpace;
        } catch (IOException ex) {
            throw new IllegalArgumentException("Exception getting size for: " +
                                               directory.getAbsoluteFile() +
                                              " - " + ex.getLocalizedMessage());
        }
    }

    /**
     * Computes the SHA-256 message digest of the contents of a file and
     * returns it has a hex-formatted string, or returns null if the file is
     * not found.
     *
     * @param file the file for which to compute the message digest
     * @return the hex-formatted SHA-256 message digest or null
     * @throws IOException if a problem occurs reading the file
     */
    public static @Nullable String computeSha256Hash(File file)
        throws IOException
    {
        final byte[] bytes = byteArrayCache.get();
        try (final InputStream fis = Files.newInputStream(file.toPath())) {
            return computeSha256Hash(fis,
                                     is -> {
                                         while (is.read(bytes) >= 0) {
                                             continue;
                                         }
                                     });
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    /**
     * An InputStream consumer that throws IOException.
     */
    @FunctionalInterface
    public interface InputStreamConsumer<E extends Throwable> {
        void accept(InputStream in) throws IOException, E;
    }

    /**
     * Computes the SHA-256 message digest while an input stream is consumed
     * and returns it has a hex-formatted string, or null if the file is not
     * found.
     *
     * @param is the input stream
     * @param consumer a consumer of the input stream
     * @return the hex-formatted SHA-256 message digest or null
     * @throws IOException if a problem occurs reading the input stream
     */
    public static <E extends Throwable>
        String computeSha256Hash(InputStream is,
                                 InputStreamConsumer<E> consumer)
        throws IOException, E
    {
        final MessageDigest md = sha256MessageDigestCache.get();
        md.reset();
        try (final DigestInputStream dis = new DigestInputStream(is, md)) {
            consumer.accept(dis);
            final byte[] digest = md.digest();
            /* Represent as a positive integer */
            final BigInteger bi = new BigInteger(1, digest);
            /*
             * Format in hex, padding with leading zeros to length 64, which is
             * the length of a SHA-256 message digest
             */
            return String.format("%064x", bi);
        }
    }

    /**
     * Returns the modification time of the file represented in our standard
     * date/time format, or null if the file is not found or the file system
     * doesn't support modification times.
     *
     * @param file the file
     * @return the modification time string or null
     * @throws IOException if there is a file system error
     */
    public static @Nullable String getFormattedFileModTime(File file)
        throws IOException
    {
        try {
            final long lastModified =
                Files.getLastModifiedTime(file.toPath()).toMillis();
            return (lastModified > 0) ?
                FormatUtils.formatDateTimeMillis(lastModified) :
                null;
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    /**
     * Return the contents of a file as a String, including up to the specified
     * maximum number of characters.
     *
     * @param file the name of the file
     * @param maxSize the maximum number of characters
     * @param wasTruncated set to true if the output was truncated
     * @return the contents of the file, up to maxSize
     * @throws IOException if there is a file system error
     */
    public static String readFileWithMaxSize(File file,
                                             int maxSize,
                                             AtomicBoolean wasTruncated)
        throws IOException
    {
        final StringBuilder sb = new StringBuilder();
        try (final BufferedReader in = new BufferedReader(
                 new FileReader(file))) {
            for (int i = 0; true; i++) {
                final int c = in.read();
                if (c == -1) {
                    break;
                }
                if (i < maxSize) {
                    sb.append((char) c);
                } else {
                    wasTruncated.set(true);
                    break;
                }
            }
        }
        return sb.toString();
    }
}
