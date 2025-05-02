/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep.utilint.net;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;

import com.sleepycat.je.rep.net.InstanceLogger;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.CommonLoggerUtils;

/**
 * A class that checks for modifications to a KeyStore file and supplies the
 * contents as an input stream to the specified consumer when it detects that
 * the keystore file has changed. The consumer is called initially in the
 * constructor so that any problems with the keystore file are noticed on
 * startup. Any subsequent problems are logged but otherwise ignored. If the
 * file is being modified when the constructor is called, the constructor will
 * wait for up to 60 seconds for the updates to complete.
 * <p>
 * Callers should call the {@link #check} method prior to performing operations
 * that depend on the keystore, and that will cause the consumer supplied to
 * the constructor to be called if a change is detected. The check method
 * checks the modification time of the file and defers reading files that were
 * modified in the current second. Since there is no synchronization between
 * reading and writing the keystore file, callers may see incompletely written
 * file contents when the consumer is called for a noticed change. The consumer
 * can either throw an exception in this case or accept the input normally. If
 * an exception is thrown, it will be logged and otherwise ignored. Whether or
 * not the consumer throws an exception, the change to the file in the current
 * second will cause the consumer to be called again the next time the check
 * method is called. We expect the caller to retry failed SSL operations and,
 * in the process of doing that, call the check method again which will reread
 * the modified file.
 * <p>
 * To avoid a flood of log messages about a persistent issue with a keystore
 * file, error messages are only logged once a day if checks continue to see
 * failures.
 */
public class KeyStoreCache {

    /** If non-null, returns the current time, for testing. */
    static volatile LongSupplier currentTimeMillis;

    /**
     * If non-null, called to sleep for the specified amount of time, for
     * testing.
     */
    static volatile Sleep sleep;

    /**
     * The message digest algorithm used to message digest values created on
     * keystores to detect changes.
     */
    private static final String MD_ALGORITHM = "SHA-256";

    /** Count the number of errors seen reading keystores, for testing */
    private static final AtomicLong errorCount = new AtomicLong();

    /**
     * The maximum amount of time to wait for a keystore file to finish being
     * modified.
     */
    private static final int MAX_MODIFY_MS = 60 * 1000;

    /**
     * The amount of time to wait before logging another problem with the
     * keystore if there are repeated problems.
     */
    private static final int PROBLEM_LOGGING_INTERVAL_MS =
        24 * 60 * 60 * 1000;

    private final Path path;
    private final KeyStoreConsumer consumer;
    private final InstanceLogger logger;
    private final MessageDigest md;
    private final byte[] buffer = new byte[1024];
    private byte[] digest;
    private long nextCheck;
    private long lastModified;
    private long lastErrorReport;

    /**
     * Creates an instance of this class, waiting up to 60 seconds for the
     * completion of modifications to the specified keystore file. The consumer
     * will be called by the constructor, rethrowing any runtime exceptions the
     * consumer throws. When the consumer is called by subsequent calls to the
     * check method, any exceptions it throws will be logged but otherwise
     * ignored.
     *
     * @param path the pathname of the keystore
     * @param consumer the consumer to supply with the contents of the keystore
     * file in the constructor and when the check method notices the keystore
     * file has changed
     * @param logger for logging
     * @throws IOException if a I/O problem occurs reading the keystore file
     * @throws GeneralSecurityException if there is a problem with the keystore
     */
    KeyStoreCache(String path,
                  KeyStoreConsumer consumer,
                  InstanceLogger logger)
        throws IOException, GeneralSecurityException
    {
        this.path = Path.of(path);
        this.consumer = requireNonNull(consumer, "consumer must not be null");
        this.logger = requireNonNull(logger, "logger must not be null");
        md = MessageDigest.getInstance(MD_ALGORITHM);
        checkInternal(true /* wait */);
    }

    /**
     * Checks if the underlying keystore file has changed and calls the
     * consumer specified in the constructor with the updated file contents if
     * a change is detected. Any exceptions when checking for changes, reading
     * the file, or calling the consumer will be logged but otherwise ignored.
     *
     * @return whether a change was detected
     */
    synchronized boolean check() {
        final long now = currentTimeMillis();
        try {
            return checkInternal(false /* wait */);
        } catch (GeneralSecurityException|IOException|RuntimeException e) {
            errorCount.incrementAndGet();
            if ((lastErrorReport == 0) ||
                (now > (lastErrorReport + PROBLEM_LOGGING_INTERVAL_MS))) {
                lastErrorReport = now;
                logger.log(Level.WARNING,
                           "Problem accessing keystore " + path + ": " +
                           CommonLoggerUtils.getStackTrace(e));
            }
            return false;
        }
    }

    /**
     * If wait is true, wait as needed until the file has not been modified in
     * the current second.
     */
    private synchronized boolean checkInternal(boolean wait)
        throws GeneralSecurityException, IOException
    {
        final long initialNow = currentTimeMillis();
        /*
         * nextCheck will be 0 when this method is first called by the
         * constructor
         */
        if (initialNow < nextCheck) {
            logger.log(Level.FINE,
                       () -> "Before next check time: path=" + path +
                       " now=" + initialNow + " nextCheck=" + nextCheck);
            return false;
        }
        long nextNow = initialNow;
        long waitMax = initialNow + MAX_MODIFY_MS;
        while (true) {
            final long now = nextNow;
            final long newLastModified =
                Files.getLastModifiedTime(path).toMillis();
            /*
             * lastModified will be 0 when this code is first called by the
             * constructor
             */
            if (newLastModified == lastModified) {
                logger.log(Level.FINE,
                           () -> "Not modified: path=" + path +
                           " now=" + now + " lastModified=" + lastModified);
                return false;
            }
            /*
             * Check at the start of the next second after the last
             * modification
             */
            final long earliestCheck = roundSeconds(newLastModified) + 1000;
            if (now >= earliestCheck) {
                lastModified = newLastModified;
                break;
            }
            if (!wait) {
                logger.log(Level.FINE,
                           () -> "Modified in current second, skip:" +
                           " path=" + path + " now=" + now +
                           " newLastModified=" + newLastModified);
                return false;
            }
            logger.log(Level.FINE,
                       () -> "Wait until next second: path=" + path +
                       " now=" + now + " newLastModified=" + newLastModified);
            try {
                sleep(earliestCheck - now);
            } catch (InterruptedException e) {
                final InterruptedIOException iioe =
                    new InterruptedIOException(
                        "Interrupted while waiting until next second" +
                        " to read modified keystore: " + e.getMessage());
                iioe.initCause(e);
                throw iioe;
            }
            nextNow = currentTimeMillis();
            if (nextNow > waitMax) {
                lastModified = newLastModified;
                logger.log(Level.FINE,
                           () -> "Giving up waiting:" + " path=" + path +
                           " initialNow=" + initialNow + " now=" + now);
                break;
            }
        }
        final long finalNow = nextNow;
        nextCheck = roundSeconds(finalNow) + 1000;
        logger.log(Level.FINE,
                   () -> "Modified:" + " path=" + path + " now=" + finalNow +
                   " lastModified=" + lastModified +
                   " nextCheck=" + nextCheck);
        return writeUpdatedFile(finalNow);
    }

    /**
     * Write the updated file, supply the update, clear the last error report,
     * and return true if the file contents have changed, otherwise return
     * false.
     */
    private boolean writeUpdatedFile(long now)
        throws GeneralSecurityException, IOException
    {
        try (final DigestInputStream digestIn =
                 new DigestInputStream(Files.newInputStream(path), md);
             final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            while (true) {
                final int count = digestIn.read(buffer);
                if (count <= 0) {
                    break;
                }
                out.write(buffer, 0, count);
            }
            final byte[] newDigest = md.digest();
            if (Arrays.equals(newDigest, digest)) {
                logger.log(Level.FINE,
                           () -> "Same digest:" + " path=" + path +
                           " now=" + now +
                           " digest=" + describeDigest(digest));
                return false;
            }
            digest = newDigest;
            final byte[] contents = out.toByteArray();
            logger.log(Level.FINE,
                       () -> "New digest:" + " path=" + path +
                       " now=" + now + " digest=" + describeDigest(digest));
            supplyUpdate(contents, digest, lastModified);
            lastErrorReport = 0;
            return true;
        }
    }

    /**
     * Supply an update to the consumer. This is a separate method so it can
     * be overridden for testing.
     */
    void supplyUpdate(byte[] contents, byte[] digest, long lastModified)
        throws GeneralSecurityException, IOException
    {
        /*
         * ByteArrayInputStream.close is a no-op, so don't worry about closing
         * the stream
         */
        consumer.accept(new ByteArrayInputStream(contents));
    }

    /**
     * Converts a digest value into a String in hexadecimal format.
     *
     * @param digest the digest
     * @return the digest in hexadecimal format
     * @throws IllegalArgumentException if digest is empty
     */
    static String describeDigest(byte[] digest) {
        requireNonNull(digest, "digest must not be null");
        if (digest.length == 0) {
            throw new IllegalArgumentException("digest must not be empty");
        }
        return String.format("%0" + (digest.length*2) + "x",
                             new BigInteger(1, digest));
    }

    /**
     * Return the number of errors encountered while reading the keystore, for
     * testing.
     */
    static long getErrorCount() {
        return errorCount.get();
    }

    /** An interface for a test replacement for Thread.sleep. */
    interface Sleep {
        void sleep(long millis) throws InterruptedException;
    }

    /** Round down to current second */
    private static long roundSeconds(long timeMillis) {
        return (timeMillis / 1000) * 1000;
    }

    private static long currentTimeMillis() {
        return (currentTimeMillis != null) ?
            currentTimeMillis.getAsLong() :
            TimeSupplier.currentTimeMillis();
    }

    private static void sleep(long millis) throws InterruptedException {
        if (sleep != null) {
            sleep.sleep(millis);
        } else {
            Thread.sleep(millis);
        }
    }

    /**
     * A consumer that accepts an input stream associated with a keystore, with
     * the ability to throw {@link GeneralSecurityException} and {@link
     * IOException}
     */
    @FunctionalInterface
    public interface KeyStoreConsumer {

        /**
         * Processes the specified input stream associated with a keystore.
         *
         * @param inputStream the input stream associated with a keystore
         * @throws IOException if there is a problem reading data from the
         * input stream
         * @throws GeneralSecurityException if there is a problem with the
         * keystore
         */
        void accept(InputStream inputStream)
            throws IOException, GeneralSecurityException;
    }
}
