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

package oracle.kv.util;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.zip.GZIPOutputStream;

import oracle.kv.impl.util.KVThreadFactory;

import com.sleepycat.je.utilint.DoubleExpMovingAvg;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A logging FileHandler that compresses rotated log files. Log files usually
 * contain a lot of redundancy, so using the standard gzip compression
 * algorithm to compress them can greatly reduce the amount of space needed in
 * the file system. This class uses gzip and adjusts the maximum file size to
 * account for the expected compression ratio, meaning that much more logging
 * data can be saved in the same amount of space.
 *
 * <p>CompressingFileHandler replaces the unique number that FileHandler uses
 * to name rotated files (kvstore_0.log, kvstore_1.log) with a value based on
 * the date and time that the file was created (kvstore_20220712-094644.log.*,
 * kvstore_20220713-220117.log.*). When it first renames files to include the
 * date, it adds the .tmp suffix, which it then replaces with the .gz suffix
 * when the file is compressed. Plan to use the gzcat command to view these
 * files rather than storing uncompressed versions of the files in the file
 * system, to avoid using too much space.
 *
 * <p>This handler relies on the underlying FileHandler to rotate files when
 * they get too large. It then renames the files so that the FileHandler will
 * not continue to rotate or delete them, compresses them, and deletes ones
 * beyond the specified count.
 *
 * <p>The file limit specified by the caller is adjusted to take space savings
 * from compression into account. The implementation assumes that there is a
 * fixed compression ratio specified by COMPRESSION_RATIO and increases the
 * limit so that the files will use the same amount of space specified by the
 * combination of the limit and count values.
 *
 * <p>The underlying FileHandler names the current log file with version number
 * 0, for example kvstore_0.log, and rotates files to larger version numbers.
 * This class renames the files using a date-based version number so that the
 * files have stable names, to avoid problems when compressing existing logs
 * and on restarts. The NonCompressingFileHandler should be used when disabling
 * compression so that existing compressed or renamed logs are deleted as
 * needed.
 *
 * <p>This class notices when the underlying FileHandler opens a new output
 * stream, meaning a log rotation has just occurred. It takes that opportunity
 * rename the rotated files to keep FileHandler from rotating them again or
 * deleting them, and arranges to compress them asynchronously in a separate
 * thread. If compression is still underway when it notices that another
 * rotation has occurred, it waits for the compression to complete.
 */
public class CompressingFileHandler extends BasicCompressingFileHandler {

    /**
     * A conservative estimate of the ratio of the size of a compressed file
     * relative to the original file. 3% or 4% seems typical, but use 10% to be
     * safe.
     */
    private static final double COMPRESSION_RATIO = 0.1;

    /**
     * Computes a moving average of the compression ratio over this number of
     * recent file compressions.
     */
    private static final int COMPRESSION_AVG_PERIOD = 20;

    private final int originalLimit;
    private final ExecutorService executor;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Semaphore compressionSemaphore = new Semaphore(1);
    private final AtomicLong compressedCount = new AtomicLong();
    private final AtomicLong deletedCount = new AtomicLong();
    private final DoubleExpMovingAvg averageCompression =
        new DoubleExpMovingAvg("avgCompress", COMPRESSION_AVG_PERIOD);

    /**
     * Creates a FileHandler that compresses rotated log files. The pattern can
     * only use the %g directive, which must appear only once. No other
     * directives are permitted. This class will recalculate the limit to use
     * the requested amount of space (limit times count) under the assumption
     * that files will be compressed to 10% of the original size. If the limit
     * is 0, file sizes are not limited, so there is no log rotation and no
     * compression. The extraLogging parameter controls whether logging
     * produced by this class itself should be included in the log. It should
     * be enabled for files in standard logging format, but not for ones that
     * use specialized formats where these log records would not fit in.
     *
     * @param pattern the pattern for naming the output file
     * @param limit the maximum size of a log file in bytes
     * @param count the number of files to use
     * @param extraLogging whether to log additional log records with
     * information generated by this class
     * @throws IOException if there are IO problems opening files
     * @throws IllegalArgumentException if pattern is an empty string, contains
     * '%' directives other than a single '%g', or does not contain a '%g'
     * directive, if limit is less than 0, or if count is less than 1
     */
    public CompressingFileHandler(String pattern,
                                  int limit,
                                  int count,
                                  boolean extraLogging)
        throws IOException
    {
        super(pattern, computeLimit(limit, count), count, extraLogging);
        originalLimit = limit;
        executor = Executors.newFixedThreadPool(1, createThreadFactory());

        /* Initiate file compression on startup */
        checkFiles();
        addExtraLogMessage(String.format("Created with originalLimit=%d" +
                                         " computedLimit=%d" +
                                         " count=%d",
                                         getLimit(),
                                         getComputedLimit(),
                                         count),
                           null /* exception */);
    }

    /**
     * Before continuing to open a new logging file, wait for any pending
     * compression to complete, and then check files to rename files, delete
     * excess files and schedule compression.
     */
    @Override
    protected synchronized void setOutputStream(OutputStream out) {
        checkFiles();
        super.setOutputStream(out);
    }

    /**
     * Set the closed flag and shutdown the executor before calling the super
     * class.
     */
    @Override
    /* Using atomic for coordination instead */
    @SuppressWarnings("sync-override")
    public void close() {
        if (closed.compareAndSet(false, true)) {
            executor.shutdownNow();
        }
        super.close();
    }

    /** Return the requested file limit, not the computed one. */
    @Override
    public int getLimit() {
        return originalLimit;
    }

    /** Returns the computed file limit. */
    int getComputedLimit() {
        return super.getLimit();
    }

    /** Returns the number of files that have been compressed. */
    long getCompressedCount() {
        return compressedCount.get();
    }

    /** Returns the number of files that have been deleted. */
    long getDeletedCount() {
        return deletedCount.get();
    }

    /**
     * Returns a moving average of the compression ratio for compressed files.
     */
    double getAverageCompression() {
        return averageCompression.get();
    }

    /**
     * Compute the correct file limit size based on the specified limit and
     * count if files are compressed by COMPRESSION_RATIO.
     */
    static int computeLimit(int limit, int count) {
        if (limit < 0) {
            throw new IllegalArgumentException(
                "The limit parameter must be 0 or greater, found: " + limit);
        }
        if (count < 1) {
            throw new IllegalArgumentException(
                "The count parameter must be 1 or greater, found: " + count);
        }

        /* If the limit is zero, there is no rotation and no compression */
        if (limit == 0) {
            return 0;
        }

        /*
         * The space used by log files in multiples of the file limit. The
         * maximum amount of file usage occurs when the current log file has
         * reached the limit is about to be rotated, the previous limit-size
         * file is just getting done being compressed, and all the remaining
         * files were compressed down from the limit size. So there are two
         * full sized files: the current one and the rotated one being
         * compressed. And there are count - 1 compressed files: one for every
         * file except for the current file.
         */
        final double fileSize = 2 + ((count - 1) * COMPRESSION_RATIO);

        /* The total size for all logs */
        final long totalSize = ((long) limit) * count;

        /*
         * The file limit is the total size divided by the number of multiples
         * of the file limit size.
         */
        final long longLimit = (long) (totalSize / fileSize);
        return (longLimit < Integer.MAX_VALUE) ?
            (int) longLimit :
            Integer.MAX_VALUE;
    }

    /** Records version information for a file. */
    static class FileInfo implements Comparable<FileInfo> {
        final Path path;
        final String version;
        final String creationTime;
        final @Nullable String unique;
        FileInfo(Path path,
                 String version,
                 String creationTime,
                 @Nullable String unique) {
            this.path = path;
            this.version = version;
            this.creationTime = creationTime;
            this.unique = unique;
        }
        boolean isTemp() {
            return path.toString().endsWith(DOT_TEMP);
        }
        boolean isCompressed() {
            return path.toString().endsWith(DOT_GZIP);
        }
        @Override
        public String toString() {
            return "FileInfo[" + "path=" + path + " version=" + version + "]";
        }
        @Override
        public int compareTo(FileInfo other) {
            return compareDateVersions(creationTime, unique,
                                       other.creationTime, other.unique);
        }
    }

    /**
     * Process the current set of files, waiting first for any pending
     * compression to complete.
     */
    void checkFiles() {

        /*
         * If compressionSemaphore is null, then this call is being made when
         * the superclass constructor opens the existing current log file. Skip
         * the call to checkFiles here and let the constructor for this class
         * do it after things get initialized.
         */
        if (compressionSemaphore == null) {
            return;
        }

        /* Wait for any pending compression to complete */
        try {
            compressionSemaphore.acquire();
            compressionSemaphore.release();
        } catch (InterruptedException e) {
            if (!closed.get()) {
                unexpected("acquiring compression semaphore", e);
            }
            return;
        }

        /* Collect matching files, skipping the current one */
        final List<MatchInfo> matches = new ArrayList<>();
        try (final DirectoryStream<Path> paths = newDirectoryStream()) {
            for (final Path path : paths) {
                final Matcher matcher = regexPattern.matcher(path.toString());
                if (!matcher.matches()) {
                    continue;
                }
                /* Skip current file */
                if ("0".equals(matcher.group(1))) {
                    continue;
                }

                matches.add(new MatchInfo(path, matcher));
            }
        } catch (IOException e) {
            unexpected("reading directory", e);
            return;
        }

        /* Done if no files beyond the current file */
        if (matches.isEmpty()) {
            return;
        }

        /* Sort files in natural order with oldest first */
        matches.sort(null);

        /*
         * Rename any newly rotated files with unique names based on creation
         * time and an optional unique count, and collect information using
         * date-based versions.
         */
        final List<FileInfo> files = new ArrayList<>();
        String lastCreationTime = null;
        int lastUnique = 0;

        /*
         * Walk the files from oldest to newest so we use higher unique counts
         * for newer files with the same creation time
         */
        for (final MatchInfo current : matches) {
            final String version = current.getVersion();
            final Matcher dateMatcher = DATE_PATTERN.matcher(version);
            final String creationTime;
            int unique = 0;
            if (!dateMatcher.matches()) {

                /* A newly rotated file */
                final long creationTimeMillis = getCreationTime(current.path);
                creationTime = formatDate(creationTimeMillis);
                if ((lastCreationTime != null) &&
                    (lastCreationTime.equals(creationTime))) {
                    unique = lastUnique + 1;
                }

                final @Nullable FileInfo renamed =
                    renameToTemp(current, creationTime, unique);
                if (renamed != null) {
                    files.add(renamed);
                }
            } else {
                creationTime = dateMatcher.group(1);
                final String uniqueString = dateMatcher.group(2);
                if (uniqueString != null) {
                    unique = Integer.parseInt(uniqueString);
                }
                files.add(new FileInfo(current.path, version, creationTime,
                                       uniqueString));
            }
            lastCreationTime = creationTime;
            lastUnique = unique;
        }

        /* Done if no files still present after any renaming failures */
        if (files.isEmpty()) {
            return;
        }

        /* Sort files in reverse order with newest first */
        files.sort(Comparator.reverseOrder());

        /*
         * The following steps process groups of files associated with the same
         * original log file. Each group has at most two of three possible
         * files: the original file, the temp file, and the compressed file.
         * The temp file is created by renaming the original file that used a
         * generation number to one with a timestamp and an optional unique
         * count. Because the renaming is within a single directory, we expect
         * it to be atomic, so only one of the original and temp files should
         * appear. While the temp file is being compressed, both the temp and
         * compressed files will appear. Note that, with the sorting, all files
         * in the same group sort next to each other.
         */

        /*
         * Track the number of groups of related files, starting with 1 for
         * the current file, which needs to be included in the count even if it
         * is not processed here.
         */
        int filesCount = 1;

        /* Collect related files from the same group */
        FileInfo temp = null;
        FileInfo compressed = null;

        /*
         * Collect files to delete and compress. We visit the newest files
         * first, but want to process them last, so use dequeues as stacks that
         * we push and pop from the front.
         */
        final Deque<Path> toDelete = new LinkedList<>();
        final Deque<Path> toCompress = new LinkedList<>();

        final Iterator<FileInfo> iter = files.iterator();
        FileInfo current = iter.next();
        while (true) {

            /* Categorize current file */
            if (current.isTemp()) {
                temp = current;
            } else if (current.isCompressed()) {
                compressed = current;
            } else {
                /*
                 * Ignore other files, which might be ones that a user
                 * uncompressed explicitly
                 */
            }

            /* Check for more related files */
            final @Nullable FileInfo next =
                iter.hasNext() ? iter.next() : null;
            if (next == null) {
                if ((temp == null) && (compressed == null)) {

                    /* No files in this group and no more files */
                    break;
                }
            } else if (current.version.equals(next.version) ||
                       ((temp == null) && (compressed == null))) {

                /* Next file is related, or current group was empty */
                current = next;
                continue;
            }

            /* Process group of related files */
            filesCount++;

            if (filesCount > getCount()) {

                /*
                 * If there are more files than the max count, then schedule
                 * the files for deletion
                 */
                if (temp != null) {
                    toDelete.addFirst(temp.path);
                }
                if (compressed != null) {
                    toDelete.addFirst(compressed.path);
                }
            } else if (temp != null) {

                /*
                 * If there are both compressed and temp versions of the file,
                 * then the compression might not be complete. Delete the
                 * compressed file.
                 */
                if (compressed != null) {
                    deleteFile(compressed.path);
                }

                /* Compress file */
                toCompress.addFirst(temp.path);
            }
            if (next == null) {
                break;
            }
            current = next;
            temp = null;
            compressed = null;
        }

        /* Delete files oldest first */
        for (final Path p : toDelete) {
            if (deleteFile(p)) {
                deletedCount.getAndIncrement();
            }
        }

        /* Schedule compression of files oldest first */
        scheduleCompression(toCompress);
    }

    /**
     * Schedule compression of the specified files in another thread after
     * acquiring the semaphore. Semaphore will be released when compression is
     * done.
     */
    void scheduleCompression(Collection<Path> toCompress) {
        try {
            compressionSemaphore.acquire();
        } catch (InterruptedException e) {
            if (!closed.get()) {
                unexpected("acquiring compression semaphore", e);
            }
            return;
        }
        executor.execute(() -> compressAll(toCompress));
    }

    /**
     * Compress the specified files and release the semaphore when done.
     */
    void compressAll(Collection<Path> paths) {
        try {
            for (final Path path : paths) {
                if (closed.get()) {
                    break;
                }
                compressFile(path);
            }
            addExtraLogMessage(
                String.format("Stats:" +
                              " unexpectedCount=%d" +
                              " compressedCount=%d" +
                              " deletedCount=%d" +
                              " averageCompression=%.2f%%",
                              getUnexpectedCount(),
                              compressedCount.get(),
                              deletedCount.get(),
                              100 * averageCompression.get()),
                null /* exception */);
        } finally {
            compressionSemaphore.release();
        }
    }

    /**
     * Compress the file and delete the original if the compression completes
     * successfully.
     */
    private void compressFile(Path path) {
        final String pathString = path.toString();
        if (!pathString.endsWith(DOT_TEMP)) {
            throw new IllegalStateException(
                "Attempt to compress file with wrong suffix: " + path);
        }
        final String noSuffix = pathString.substring(
            0, pathString.length() - (DOT_TEMP.length()));
        final Path newPath = Paths.get(noSuffix + DOT_GZIP);
        if (createCompressedFile(path, newPath)) {
            deleteFile(path);
        }
    }

    /**
     * Create a compressed version of a file, returning whether the operation
     * succeeded. Do not create the compressed file if unable to open the input
     * file for reading, and do not overwrite an existing file.
     *
     * @param path the path of the original file
     * @param compressed the path for the compressed file
     * @return whether the operation succeeded
     */
    boolean createCompressedFile(Path path, Path compressed) {
        try {
            createCompressedFileInternal(path, compressed);
        } catch (IOException|RuntimeException e) {
            unexpected("creating compressed file", e);
            return false;
        }
        try {
            final long originalSize = getFileSize(path);
            final long compressedSize = getFileSize(compressed);
            final double compressionRatio =
                ((double) compressedSize) / originalSize;
            final long count = compressedCount.incrementAndGet();
            averageCompression.add(compressionRatio, count);
        } catch (IOException e) {
            /*
             * Skip updating the compression ratio if we get an error
             * attempting to get the file sizes
             */
            unexpected("checking compressed file compression ratio", e);
        }
        return true;
    }

    /**
     * Create a compressed version of a file
     *
     * @param path the path of the original file
     * @param compressed the path for the compressed file
     * @throws IOException if path does not exist, if compressed does exist, or
     * if an IO failure occurs while writing the compressed file
     */
    void createCompressedFileInternal(Path path, Path compressed)
        throws IOException
    {
        try (final InputStream in = Files.newInputStream(path);
             final GZIPOutputStream out = new GZIPOutputStream(
                 Files.newOutputStream(compressed,
                                       StandardOpenOption.CREATE_NEW,
                                       StandardOpenOption.WRITE))) {
            copyStream(in, out);
        }
    }

    /* TODO: Replace with InputStream.transferTo when we move to Java 9. */
    private void copyStream(InputStream in, OutputStream out)
        throws IOException
    {
        final byte[] buffer = new byte[8192];
        while (true) {
            final int count = in.read(buffer);
            if (count < 0) {
                break;
            }
            out.write(buffer, 0, count);
        }
    }

    /**
     * Renames a rotated file to a temp file that uses the creation time and an
     * optional unique value. Returns null if there was a problem renaming the
     * file.
     */
    private @Nullable FileInfo renameToTemp(MatchInfo matchInfo,
                                            String creationTime,
                                            int unique) {
        final String pathString = matchInfo.path.toString();
        if (RENAMED_PATTERN.matcher(pathString).matches()) {
            throw new IllegalStateException(
                "Attempt to rename file with wrong suffix: " + matchInfo.path);
        }
        final int versionStart = matchInfo.matcher.start(1);
        final String prefix = pathString.substring(0, versionStart);
        final int versionEnd = matchInfo.matcher.end(1);
        final String suffix = pathString.substring(versionEnd);

        final String version =
            creationTime + ((unique == 0) ? "" : "-" + unique);
        final String newFileName = prefix + version + suffix + DOT_TEMP;
        final Path newPath = Paths.get(newFileName);
        if (!renameFile(matchInfo.path, newPath)) {
            return null;
        }
        return new FileInfo(newPath, version, creationTime,
                            (unique == 0) ? null : String.valueOf(unique));
    }

    /**
     * Rename a file, returning whether the operation succeeded.
     *
     * @param source the path of the file
     * @param target the new path for the file
     * @return whether the operation succeeded
     */
    private boolean renameFile(Path source, Path target) {
        try {
            renameFileInternal(source, target);
            return true;
        } catch (IOException|RuntimeException e) {
            unexpected("renaming file", e);
            return false;
        }
    }

    /** Rename a file. */
    void renameFileInternal(Path source, Path target) throws IOException {
        Files.move(source, target, ATOMIC_MOVE);
    }

    /**
     * No logger, so call unexpected method for uncaught exceptions.
     */
    private KVThreadFactory createThreadFactory() {
        return new KVThreadFactory("CompressingFileHandler-" + pattern,
                                   null /* logger */) {
            @Override
            public Thread.UncaughtExceptionHandler
                makeUncaughtExceptionHandler()
            {
                return (t, e) -> unexpected("in log compression thread", e);
            }
        };
    }

    @Override
    String getLogPrefix() {
        return "Compressing file handler: ";
    }
}
