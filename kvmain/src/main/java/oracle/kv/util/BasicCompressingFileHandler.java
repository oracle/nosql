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

import static java.util.logging.Level.INFO;
import static oracle.kv.impl.util.FileNames.GZIP_FILE_SUFFIX;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.LogRecord;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.impl.util.RateLimiting;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A base class for FileHandlers that manage compressed rotated log files.
 */
abstract class BasicCompressingFileHandler extends FileHandler {

    /**
     * A regex pattern that matches the date-based version used for compressed
     * files, usually without a unique suffix (like 20220520-141311), but with
     * a unique suffix if needed (like 20220520-141311-1). Regex capture group
     * 1 represents the creation time portion, and group 2 represents the
     * optional unique count added when multiple files have the exact same
     * creation date.
     */
    static final Pattern DATE_PATTERN =
        Pattern.compile("([\\d]+-[\\d]+)(?:-(\\d+))?");

    /**
     * The suffix for files that have been renamed to prevent the original
     * logging system from rotating them. Once the log file has this suffix,
     * the file is no longer subject to the standard Java FileHandler log
     * rotation.
     */
    static final String TEMP_SUFFIX = "tmp";

    /** The full suffix for temp files, including the dot. */
    static final String DOT_TEMP = "." + TEMP_SUFFIX;

    /** The full suffix for compressed files, including the dot. */
    static final String DOT_GZIP = "." + GZIP_FILE_SUFFIX;

    /** A regular expression string matching all suffixes. */
    static final String ALL_SUFFIXES =
        "([.]" + GZIP_FILE_SUFFIX +
        "|[.]" + TEMP_SUFFIX + ")";

    /** Regex matching all files renamed with suffixes. */
    static final Pattern RENAMED_PATTERN =
        Pattern.compile(".*" + ALL_SUFFIXES);

    /**
     * The DateFormat pattern used to format dates for version numbers of
     * compressed files, for example 20220428-134711.
     */
    private static final String DATE_FORMAT_PATTERN = "yyyyMMdd-HHmmss";

    /** Convert dates to UTC. */
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /** Use a thread local for the date format since it is not thread safe. */
    private static final ThreadLocal<DateFormat> DATE_FORMAT =
        ThreadLocal.withInitial(() -> createDateFormat());

    /**
     * The maximum number of extra log records that can be queued or processed
     * at a time. Extra log records are created to log information about this
     * handler itself. The handler doesn't do its own logging, to avoid
     * deadlocks. Instead, it stores the records in a queue and processes them
     * along with other log records supplied to the handler.
     */
    private static final int MAX_EXTRA_LOG_RECORDS = 10;

    /** Period for rate limiting logging of extra log records. */
    private static final int RATE_LIMITING_PERIOD_MS =
        10 * 60 * 60 * 1000; /* 10 minutes */

    /**
     * Maximum number of categories for rate limiting logging of extra log
     * records.
     */
    private static final int RATE_LIMITING_MAX_OBJS = 20;

    private static final boolean DEBUG_EXTRA_LOGGING = false;

    final String pattern;
    private final boolean extraLogging;
    final Pattern regexPattern;
    private final Path directory;

    private final BlockingQueue<LogRecord> extraLogRecords =
        new LinkedBlockingQueue<>(MAX_EXTRA_LOG_RECORDS);
    private final RateLimiting<String> rateLimiting =
        new RateLimiting<>(RATE_LIMITING_PERIOD_MS, RATE_LIMITING_MAX_OBJS);
    private final AtomicLong unexpectedCount = new AtomicLong();

    /**
     * Creates a FileHandler with support for compressed rotated log files. The
     * pattern can only use the %g directive, which must appear only once. No
     * other directives are permitted. The extraLogging parameter controls
     * whether logging produced by this class itself should be included in the
     * log. It should be enabled for files in standard logging format, but not
     * for ones that use specialized formats where these log records would not
     * fit in.
     *
     * @param pattern the pattern for naming the output file
     * @param limit the maximum number of bytes to write to any one file
     * @param count the number of files to use
     * @param extraLogging whether to log additional log records with
     * information generated by this class
     * @throws IOException if there are IO problems opening files
     * @throws IllegalArgumentException if pattern is an empty string, contains
     * '%' directives other than a single '%g', or does not contain a '%g'
     * directive, if limit is less than 0, or if count is less than 1
     */
    BasicCompressingFileHandler(String pattern,
                                int limit,
                                int count,
                                boolean extraLogging)
        throws IOException
    {
        super(pattern, limit, count, true /* append */);
        this.pattern = pattern;
        this.extraLogging = extraLogging;
        regexPattern = getRegex(pattern);
        directory = Paths.get(pattern).toAbsolutePath().getParent();
    }

    /** Also publish extra log records, if any. */
    @Override
    public synchronized void publish(LogRecord record) {
        super.publish(record);

        /*
         * Limit the processing of records to make sure we don't delay
         * publishing the provided record for too long. The synchronization
         * means that other outside log requests will wait until the extra log
         * records are published.
         */
        for (int i = 0; i < MAX_EXTRA_LOG_RECORDS; i++) {
            final LogRecord extra = extraLogRecords.poll();
            if (extra == null) {
                break;
            }
            /*
             * Event times of extra records may in some cases be out of order
             * with respect to other records. Although we could check event
             * times here and order the extra records relative to the main
             * record passed in the original publish call, later records could
             * already have been logged by earlier publish calls, so it doesn't
             * seem worth doing.
             */
            super.publish(extra);
        }
    }

    /** Returns the number of unexpected errors encountered. */
    long getUnexpectedCount() {
        return unexpectedCount.get();
    }

    /** Resets the number of unexpected errors encountered -- for testing. */
    void resetUnexpectedCount() {
        unexpectedCount.set(0);
    }

    /**
     * Return a regex pattern that matches the specified logging pattern,
     * including with temporary and compressed suffixes. The pattern will have
     * a first capturing group that matches the version, which may either be
     * the integer used by the underlying Java FileHandler or the
     * date-formatted one used for compressed files.
     *
     * @throws IllegalArgumentException if pattern is an empty string, contains
     * '%' directives other than a single '%g', or does not contain a '%g'
     * directive
     */
    static Pattern getRegex(String pattern) {
        int versionPos = -1;
        char lastChar = 0;
        for (int i = 0; i < pattern.length(); i++) {
            final char c = pattern.charAt(i);
            if ((i > 0) && (lastChar == '%')) {
                if (c == 'g') {
                    if (versionPos != -1) {
                        throw new IllegalArgumentException(
                            "Found '%g' multiple times: " + pattern);
                    }
                    /* Save the position of the '%' */
                    versionPos = i - 1;
                } else {
                    throw new IllegalArgumentException(
                        "Found '%' directive other than '%g': " + pattern);
                }
            }
            lastChar = c;
        }
        if (versionPos == -1) {
            throw new IllegalArgumentException(
                "The '%g' directive was not found: " + pattern);
        }

        /*
         * Quote all of the text in the pattern other than the '%g', which will
         * be replaced by a regex, so that any regexp characters used in
         * pathnames -- like the backslash Windows uses as a separator -- are
         * not treated specially.
         */
        return Pattern.compile(
            Pattern.quote(pattern.substring(0, versionPos)) +
            "([-\\d]+)" +
            ((pattern.length() > versionPos + 2) ?
             Pattern.quote(pattern.substring(versionPos + 2)) :
             "") +
            ALL_SUFFIXES + "?");
    }

    /**
     * Records a pattern match for a file. The pattern matches both files in
     * the original numbered format, and in the date-based format used when the
     * files are renamed.
     */
    static class MatchInfo implements Comparable<MatchInfo> {
        final Path path;
        final Matcher matcher;
        MatchInfo(Path path, Matcher matcher) {
            this.path = path;
            this.matcher = matcher;
        }
        /** Returns the version portion of the filename */
        String getVersion() {
            return matcher.group(1);
        }
        boolean isTemp() {
            return path.toString().endsWith(DOT_TEMP);
        }
        @Override
        public String toString() {
            return "MatchInfo[path=" + path + " matcher=" + matcher + "]";
        }
        /** Sort instances with newest version numbers first */
        @Override
        public int compareTo(MatchInfo other) {
            final String version = getVersion();
            final Matcher dateMatcher = DATE_PATTERN.matcher(version);
            final String version2 = other.getVersion();
            final Matcher dateMatcher2 = DATE_PATTERN.matcher(version2);

            if (!dateMatcher.matches()) {

                /* Newly rotated file come after already processed ones */
                if (dateMatcher2.matches()) {
                    return 1;
                }

                /*
                 * Otherwise, sort in reverse integer version order, since the
                 * lower version is newer for original file handler versions
                 */
                try {
                    return -Integer.compare(Integer.parseInt(version),
                                            Integer.parseInt(version2));
                } catch (NumberFormatException e) {
                    /*
                     * We expect the values to be valid integers, but, just in
                     * case they are not, compare them in reverse dictionary
                     * order
                     */
                    return -version.compareTo(version2);
                }
            }
            if (!dateMatcher2.matches()) {

                /* Already processed file comes before newly rotated one */
                return -1;
            }

            return compareDateVersions(
                dateMatcher.group(1), dateMatcher.group(2),
                dateMatcher2.group(1), dateMatcher2.group(2));
        }
    }

    /**
     * Compare the creation time and unique values from two different files.
     * Sorts older creation times first and, for the same creation time, lower
     * unique values -- which represent older items -- first.
     */
    static int compareDateVersions(String creationTime,
                                   @Nullable String unique,
                                   String creationTime2,
                                   @Nullable String unique2) {

        /* Compare by creation time */
        final int compare = creationTime.compareTo(creationTime2);
        if (compare != 0) {
            return compare;
        }

        /* Null unique is older */
        if (unique == null) {
            return (unique2 == null) ? 0 : -1;
        }
        if (unique2 == null) {
            return 1;
        }

        try {
            return Integer.compare(Integer.parseInt(unique),
                                   Integer.parseInt(unique2));
        } catch (NumberFormatException e) {
            /*
             * If there is a problem converting to ints, then sort by
             * dictionary order
             */
            return unique.compareTo(unique2);
        }
    }

    /**
     * Return a directory stream of the files in the directory.
     *
     * @throws IOException if an I/O error occurs
     */
    DirectoryStream<Path> newDirectoryStream() throws IOException {
        return Files.newDirectoryStream(directory);
    }

    /** Returns the size of the file. */
    long getFileSize(Path path) throws IOException {
        return Files.size(path);
    }

    /**
     * Delete a file, returning whether the operation succeeded.
     *
     * @param path the path of the file
     * @return whether the operation succeeded
     */
    boolean deleteFile(Path path) {
        try {
            deleteFileInternal(path);
            return true;
        } catch (IOException|RuntimeException e) {
            unexpected("deleting file", e);
            return false;
        }
    }

    /** Delete a file. */
    void deleteFileInternal(Path path) throws IOException {
        Files.deleteIfExists(path);
    }

    /**
     * Returns the creation time in milliseconds of the file with the specified
     * path. Returns the current time if there is an I/O error when attempting
     * to read the file attributes.
     */
    long getCreationTime(Path path) {
        try {
            return getCreationTimeInternal(path);
        } catch (IOException e) {
            unexpected("reading file attributes", e);
            /* This shouldn't happen, but use the current time if it does */
            return System.currentTimeMillis();
        }
    }

    /** Returns the creation time in milliseconds of a file. */
    long getCreationTimeInternal(Path path) throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class)
            .creationTime().toMillis();
    }

    /**
     * Note an unexpected exception when performing the specified operation.
     * Tally the number of unexpected operations and use rate limited logging
     * to log them. Note that we could use the Handler.reportError method for
     * this purpose, but it only prints the first message to stderr, which
     * didn't seem very useful.
     */
    void unexpected(String operation, Throwable e) {
        unexpectedCount.getAndIncrement();
        if (rateLimiting.isHandleable(operation)) {
            addExtraLogMessage(
                "Unexpected problem " + operation + ": " + e.getMessage(), e);
        }
    }

    /**
     * Requests that a message be logged with the specified message and the
     * exception, if it is non-null.
     */
    void addExtraLogMessage(String msg,
                            @Nullable Throwable exception) {
        if (DEBUG_EXTRA_LOGGING) {
            synchronized (System.out) {
                System.out.println("Extra logging: " + msg);
                if (exception != null) {
                    exception.printStackTrace(System.out);
                }
            }
        }
        if (!extraLogging) {
            return;
        }
        final LogRecord record = new LogRecord(INFO, getLogPrefix() + msg);
        if (exception != null) {
            record.setThrown(exception);
        }
        extraLogRecords.offer(record);
    }

    /**
     * Returns a string that should be used to prefix all extra log messages.
     */
    abstract String getLogPrefix();

    private static DateFormat createDateFormat() {
        final DateFormat result = new SimpleDateFormat(DATE_FORMAT_PATTERN);
        result.setTimeZone(UTC);
        return result;
    }

    /** Returns the specified time formatted in the appropriate pattern. */
    static String formatDate(long timeMillis) {
        return DATE_FORMAT.get().format(timeMillis);
    }
}
