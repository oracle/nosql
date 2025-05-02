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

package com.sleepycat.je.rep.impl.networkRestore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileInfoReq;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileInfoResp;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileReq;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.LogVerifier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * The LogFileFeeder supplies log files to a client. There is one instance of
 * this class per client that's currently active. LogFileFeeders are created by
 * the FeederManager and exist for the duration of the session with the client.
 */
public class LogFileFeeder extends StoppableThread {

    /**
     * Time to wait for the next request from the client, 5 minutes.
     */
    private static final int SOCKET_TIMEOUT_MS = 5 * 60 * 1000;
    public static volatile int TEST_SOCKET_TIMEOUT_MS = 0;

    /*
     * 8K transfer size to take advantage of increasingly prevalent jumbo
     * frame sizes and to keep disk i/o contention to a minimum.
     */
    static final int TRANSFER_BYTES = 0x2000;

    static final int READ_FILE_BYTES = 0x10000;

    /*
     * The parent FeederManager that creates and maintains LogFileFeeder
     * instances.
     */
    private final FeederManager feederManager;

    /* The channel on which the feeder communicates with the client. */
    private final NamedChannel namedChannel;

    /* The channel on which the feeder communicates with the client. */
    private final Protocol protocol;

    /* The client node requesting the log files. */
    private int clientId;

    private final FeederCreator feederCreator;

    /* Used to compute a SHA256 during a transfer, or if client requests it. */
    final MessageDigest messageDigest;

    /* Logger shared with the FeederManager. */
    final private Logger logger;
    
    public volatile static boolean testSocketTimeout = false;

    private final int id;

    public LogFileFeeder(final FeederManager feederManager,
                         final NamedChannel channel,
                         final Protocol protocol,
                         final FeederCreator feederCreator,
                         final int id)
        throws DatabaseException {
        super(feederManager.getEnvImpl(), "Log File Feeder " + id);

        this.feederManager = feederManager;
        this.protocol = protocol;
        this.feederCreator = feederCreator;
        this.clientId = feederCreator.getClientId();
        this.id = id;
        logger = feederManager.logger;
        this.namedChannel = channel;

        final String algorithm = getHashAlgorithm();
        try {
            messageDigest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            LoggerUtils.severe(logger, feederManager.getEnvImpl(),
                               "The " + algorithm + " algorithm was not " +
                               "made available by the security provider" +
                               LoggerUtils.getStackTraceForSevereLog(e));
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    public void shutdown() {
        if (shutdownDone(logger)) {
            return;
        }


        shutdownThread(logger);
        feederCreator.logFileFeeders.remove(id, this);
        LoggerUtils.info(logger, feederManager.getEnvImpl(),
                         "LogFileFeeder " + id + " for client:" + clientId +
                         " shutdown.");
    }

    @Override
    protected int initiateSoftShutdown() {
        /*
         * The feeder will get an I/O exception and exit, since it can't use
         * the channel after it has been closed.
         */
        RepUtils.shutdownChannel(namedChannel);
        return SOCKET_TIMEOUT_MS;
    }

    /**
     * The main driver loop that enforces the protocol message sequence and
     * implements it.
     */
    @Override
    public void run() {
        try {
            configureChannel();
            sendRequestedFiles();
        } catch (ClosedByInterruptException e) {
            LoggerUtils.fine
                (logger, feederManager.getEnvImpl(),
                "Log File feeder " + id +
                 " Ignoring ClosedByInterruptException normal shutdown");
            throw new FileTransferException(e.getMessage(), e);
        } catch (IOException e) {
            LoggerUtils.warning(logger, feederManager.getEnvImpl(),
                                "Log File feeder " + id +
                                " IO Exception: " + e.getMessage());
            throw new FileTransferException(e.getMessage(), e);
        } catch (ProtocolException e) {
            LoggerUtils.severe(logger, feederManager.getEnvImpl(),
                               "Log File feeder " + id +
                               " Protocol Exception: " + e.getMessage() +
                               LoggerUtils.getStackTraceForSevereLog(e));
            throw new FileTransferException(e.getMessage(), e);
        } catch (Exception e) {
            throw new EnvironmentFailureException
                (feederManager.getEnvImpl(),
                 EnvironmentFailureReason.UNCAUGHT_EXCEPTION,
                 e);
        } finally {
            try {
                namedChannel.getChannel().close();
            } catch (IOException e) {
                LoggerUtils.warning(logger, feederManager.getEnvImpl(),
                                    "Log File feeder " + id +
                                    " io exception on " +
                                    "channel close: " + e.getMessage());
            }
            shutdown();

            LoggerUtils.info
                (logger, feederManager.getEnvImpl(),
                 "Logfilefeeder " + id + " for client:" +
                 clientId + " exited");
        }
    }

    /**
     * Send files in response to request messages. The request sequence looks
     * like the following:
     *
     *  [FileReq | FileInfoReq]+ Done
     *
     * The response sequence to a FileReq looks like:
     *
     *  FileStart <file byte stream> FileEnd
     *
     */
    private void sendRequestedFiles()
        throws IOException, ProtocolException, DatabaseException {

        /* Loop until Done message causes ProtocolException. */
        while (true) {
            try {
                final FileReq fileReq = protocol.read(
                    namedChannel.getChannel(), FileReq.class);
                final String fileName = fileReq.getFileName();

                /*
                 * Calculate the full path for a specified log file name,
                 * especially when this Feeder is configured to run with sub
                 * directories.
                 */
                final FileManager fMgr =
                    feederManager.getEnvImpl().getFileManager();
                final File file = new File(fMgr.getFullFileName(fileName));

                if (!file.exists()) {
                    throw EnvironmentFailureException.unexpectedState
                        ("Log file not found: " + fileName);
                }
                /* Freeze the length and last modified date. */
                final long length = file.length();
                final long lastModified = file.lastModified();
                final byte[] digest;
                final FileInfoResp resp;
                Protocol.FileInfoResp cachedResp =
                    feederManager.statResponses.get(fileName);
                final byte[] cachedDigest =
                    ((cachedResp != null) &&
                     (cachedResp.getFileLength() == length) &&
                     (cachedResp.getLastModifiedTime() == lastModified)) ?
                    cachedResp.getDigestSHA256() : null;

                if (fileReq instanceof FileInfoReq) {
                    if  (cachedDigest != null) {
                        digest = cachedDigest;
                    } else if (((FileInfoReq) fileReq).getNeedSHA256()) {
                        digest = getSHA256Digest(file,
                            length, protocol.getHashAlgorithm()).digest();
                    } else {
                        // Digest not requested
                        digest = new byte[0];
                    }
                    resp = protocol.new FileInfoResp
                        (fileName, length, lastModified, digest);
                } else {
                    protocol.write(protocol.new FileStart
                                   (fileName, length, lastModified),
                                   namedChannel);
                    digest = sendFileContents(file, length);
                    if ((cachedDigest != null) &&
                         !Arrays.equals(cachedDigest, digest)) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Inconsistent cached and computed digests");
                    }
                    resp = protocol.new FileEnd
                        (fileName, length, lastModified, digest);
                }
                /* Cache for subsequent requests, if it was computed. */
                if (digest.length > 0) {
                    feederManager.statResponses.put(fileName, resp);
                }
                if (testSocketTimeout) {
                    LoggerUtils.info(logger, feederManager.getEnvImpl(),
                        "Skipping response to test socket timeout.");
                } else {
                    protocol.write(resp, namedChannel);
                }
            } catch (ProtocolException pe) {
                if (pe.getUnexpectedMessage() instanceof Protocol.Done) {
                    return;
                }
                throw pe;
            }
        }
    }

    /**
     * Returns the SHA256 has associated with the file.
     */
    static MessageDigest getSHA256Digest(
    final File file, final long length, final String algorithm)
        throws IOException, DatabaseException {

        final MessageDigest messageDigest;

        try {
            messageDigest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
        try (FileInputStream fileStream = new FileInputStream(file)) {
            ByteBuffer buffer = ByteBuffer.allocate(READ_FILE_BYTES);
            for (long bytes = length; bytes > 0; ) {
                final int readSize = (int) Math.min(READ_FILE_BYTES, bytes);
                final int readBytes =
                    fileStream.read(buffer.array(), 0, readSize);
                if (readBytes == -1) {
                    throw new IOException("Premature EOF. Was expecting: " +
                        readSize);
                }
                messageDigest.update(buffer.array(), 0, readBytes);
                bytes -= readBytes;
            }
        }
        return messageDigest;
    }

    /**
     * Sends over the contents of the file and computes the SHA-1 hash. Note
     * that the method does not rely on EOF detection, but rather on the
     * promised file size, since the final log file might be growing while the
     * transfer is in progress. The client uses the length sent in the FileResp
     * message to maintain its position in the network stream. It expects to
     * see a FileInfoResp once it has read the agreed upon number of bytes.
     *
     * Since JE log files are append only, there is no danger that we will send
     * over any uninitialized file blocks.
     *
     * @param file the log file to be sent.
     * @param length the number of bytes to send
     * @return the digest associated with the file that was sent
     */
    private byte[] sendFileContents(final File file, final long length)
        throws IOException {

        final LogVerifier verifier =
            new LogVerifier(feederManager.getEnvImpl(), file.getName(), -1L);

        try (FileInputStream fileStream = new FileInputStream(file)) {
            final FileChannel fileChannel = fileStream.getChannel();
            messageDigest.reset();
            final ByteBuffer buffer =
                ByteBuffer.allocateDirect(TRANSFER_BYTES);
            final byte[] array =
                (buffer.hasArray()) ? buffer.array() : new byte[TRANSFER_BYTES];
            int transmitBytes = 0;

            while (true) {
                buffer.clear();
                if (fileChannel.read(buffer) < 0) {
                    verifier.verifyAtEof();
                    break;
                }

                buffer.flip();
                final int lim = buffer.limit();
                final int off;
                if (buffer.hasArray()) {
                    off = buffer.arrayOffset();
                } else {
                    off = 0;
                    buffer.get(array, 0, lim);
                    buffer.rewind();
                }
                verifier.verify(array, off, lim);
                messageDigest.update(array, off, lim);
                transmitBytes += namedChannel.getChannel().write(buffer);
            }

            if (transmitBytes != length) {
                String msg = "File length:" + length + " does not match the " +
                    "number of bytes that were transmitted:" +
                    transmitBytes;

                throw new IllegalStateException(msg);
            }

            final String msg =
                String.format("LogFileFeeder " + id  + " :sent file: %s" +
                              " Length:%,d bytes to client:%d",
                    file, length, clientId);
            LoggerUtils.info(logger, feederManager.getEnvImpl(), msg);
        }
        return messageDigest.digest();
    }

    /**
     * Sets up the channel to facilitate efficient transfer of large log files.
     */
    private void configureChannel()
        throws IOException {

        LoggerUtils.fine
            (logger, feederManager.getEnvImpl(),
             "Log File Feeder " + id + " accepted connection from " +
             namedChannel);
        int timeout = SOCKET_TIMEOUT_MS;
        if (TEST_SOCKET_TIMEOUT_MS != 0) {
            timeout = TEST_SOCKET_TIMEOUT_MS;
        }
        namedChannel.getChannel().socket().setSoTimeout(timeout);

        /*
         * Enable Nagle's algorithm since throughput is important for the large
         * files we will be transferring.
         */
        namedChannel.getChannel().socket().setTcpNoDelay(false);
    }

    /**
     * @see StoppableThread#getLogger
     */
    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Should be used instead of {@link Protocol#MAX_VERSION} to allow
     * overriding with {@link FeederManager#setTestProtocolMaxVersion}.
     */
    private int getProtocolMaxVersion() {
        final int testVersion = feederManager.getTestProtocolMaxVersion();
        return (testVersion != 0) ? testVersion : Protocol.MAX_VERSION;
    }

    /**
     * Gets the hash algorithm.  Is always SHA-256 in practice since log files
     * are not backwards compatible with older JE versions that expect a SHA1
     * hash, but SHA1 can be returned in testing when simulating talking to
     * older systems.  See {@link FeederManager#setTestProtocolMaxVersion}.
     * @return
     */
    private String getHashAlgorithm() {
        if (getProtocolMaxVersion() >= Protocol.VERSION_3) {
            return "SHA-256";
        }
        return "SHA1";
    }
}
