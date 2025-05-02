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


import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StoppableThread;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

/**
 * When FeederManager receives a FEEDER_SERVICE service request from a client,
 * it will create a FeederCreator dedicated to that client.
 * FeederCreator will send file list and file info to the client, then regisger
 * a new service, [LOG_FEEDER_SERVICE + clientId] in serviceDispatcher. Client
 * side will open up multiple threads to send [LOG_FEEDER_SERVICE + clientId]
 * service request to the FeederCreator, for each service request,
 * FeederCreator will create a corresponding LogFileFeeder thread and start
 * the thread. LogFileFeeder's responsibility only limits to receiving FileReq
 * and transferring the requested file.
 */
public class FeederCreator extends StoppableThread {


    /*
     * The queue into which the ServiceDispatcher queues socket channels for
     * new Feeder instances.
     */
    private final BlockingQueue<DataChannel> channelQueue =
        new LinkedBlockingQueue<>();

    /*
     * The parent FeederManager that creates and maintains LogFileFeeder
     * instances.
     */
    private final FeederManager feederManager;

    /* The channel on which the feeder communicates with the client. */
    private final NamedChannel namedChannel;

    /* Wait indefinitely for somebody to request the service. */
    private static final long POLL_TIMEOUT = Long.MAX_VALUE;

    /*
     * The dbBackup instance that's used to manage the list of files that will
     * be transferred. It's used to ensure that a consistent set is transferred
     * over to the client. If an open dbBackup exists for the client, it's
     * established in the checkProtocol method immediately after the client has
     * been identified.
     */
    private DbBackup dbBackup = null;

    private final ServiceDispatcher serviceDispatcher;

    /* Logger shared with the FeederManager. */
    final private Logger logger;

    /* Identifies the Feeder Service. */
    public static final String LOG_FEEDER_SERVICE = "LogFileFeeder";

    /* The client node requesting the log files. */
    private int clientId;

    static final int READ_FILE_BYTES = 0x10000;

    private volatile boolean doNotRenewLease;

    public volatile static boolean testSocketTimeout = false;

    /*
     * The purpose of keep record of a connectionId is, consider this scenario:
     * Node 1 is the feeder and node 2 is the client, node 2 connects with
     * node 1 and first round of attempt will fail according to NetworkRestore.
     * At this time FeederCreator hasn't shut down completely and
     * service[LOG_FEEDER_SERVICE + clientId] is still registered, but node 2
     * issues second attempt, a new FeederCreator will be created and try to
     * register service[service[LOG_FEEDER_SERVICE + clientId], which will
     * throw out an EnvironmentFailureException.unexpectedState indicating
     * that the service is already registered. So a connectionId is needed to
     * differentiate connections between the same client and feeders.
     */
    public int connectionId;

    public boolean serviceRegistered;


    /*
     * Every LogFileFeeder add itself to this Set
     */
    final ConcurrentHashMap<Integer, LogFileFeeder> logFileFeeders =
            new ConcurrentHashMap<>();

    public FeederCreator(FeederManager feederManager,
                         DataChannel channel,
                         ServiceDispatcher serviceDispatcher) {
        super(feederManager.getEnvImpl(), "Feeder Creator");
        this.feederManager = feederManager;
        this.namedChannel = new NamedChannel(channel, feederManager.nameIdPair);
        this.serviceDispatcher = serviceDispatcher;
        logger = feederManager.logger;
    }

    public void shutdown() {

        if (shutdownDone(logger)) {
            return;
        }

        shutdownThread(logger);
        for (LogFileFeeder feeder :
                new ArrayList<>(logFileFeeders.values())) {
            feeder.shutdown();
        }
        if (serviceRegistered) {
            serviceDispatcher.cancel(LOG_FEEDER_SERVICE + clientId +
                                     connectionId);
        }
        feederManager.feederCreators.remove(clientId);

        LoggerUtils.info(logger, feederManager.getEnvImpl(),
                         "FeederCreator for client:" + clientId +
                         " is shutdown.");
    }

    @Override
    public void run() {

        final NameIdPair remoteNameIdPair =
                new NameIdPair(RepUtils.getRemoteHost(namedChannel.getChannel()));
        try {

            Protocol protocol = negotiateProtocol(remoteNameIdPair);

            if (protocol == null) {
                return; /* Server not compatible with client. */
            }

            int  feederNum = 0;

            checkFeeder(protocol, remoteNameIdPair);
            sendFileList(protocol);

            ExecutorService executor;

            /*
             * Only protocol with version >= VERSION_4 supports transferring
             * files in multi-thread.
             */
            if (protocol.getVersion() >= Protocol.VERSION_4) {

                sendAllFileInfo(protocol);

                /* get the connectionId from client */
                Protocol.ThreadCount clientConnectionId = protocol.read(
                        namedChannel.getChannel(), Protocol.ThreadCount.class);

                /* get the feeder count number from client */
                Protocol.ThreadCount feederCount = protocol.read(
                    namedChannel.getChannel(), Protocol.ThreadCount.class);

                int processorNum = Runtime.getRuntime().availableProcessors();
                /*
                 * Adjust the threadCount based on the number of processors,
                 * under the assumption that server and client have the same
                 * hardware resource.
                 */
                int threadCount = Math.min(feederCount.getThreadCount(),
                    Math.min(processorNum / 2, 10));

                if (threadCount <= 0) {
                    threadCount = 2;
                }

                protocol.write(protocol.new ThreadCount(threadCount),
                    namedChannel.getChannel());

                connectionId = clientConnectionId.getThreadCount();

                /* clientId is initialized in negotiateProtocol */
                serviceDispatcher.register
                        (serviceDispatcher.new
                                LazyQueuingService(LOG_FEEDER_SERVICE +
                                clientId + connectionId,
                                channelQueue, this));

                serviceRegistered = true;

                List<Future<?>> futures = new ArrayList<>();
                executor = Executors.newFixedThreadPool(threadCount);

                for (int i = 0; i < threadCount; i ++) {

                    final DataChannel channel =
                            channelQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                    if (channel == RepUtils.CHANNEL_EOF_MARKER) {
                        LoggerUtils.info(logger, envImpl,
                            "Feeder Creator soft shutdown.");
                        break;
                    }
                    feederNum ++;

                    Protocol feederProtocol = makeProtocol(
                        protocol.getVersion(),
                        remoteNameIdPair);

                    LogFileFeeder feeder = new LogFileFeeder(feederManager,
                            new NamedChannel(channel,
                                    feederManager.nameIdPair),
                            feederProtocol, this, feederNum);
                    logFileFeeders.put(feederNum, feeder);
                    Future<?> future = executor.submit(feeder);
                    futures.add(future);
                }

                executor.shutdown();

                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

                int failedThreadCount = 0;

                for (Future<?> future : futures) {
                    try {
                        future.get();
                        LoggerUtils.info(logger, feederManager.getEnvImpl(),
                            "LogFileFeeder completed successfully.");

                    } catch (ExecutionException e) {
                        failedThreadCount ++;
                        LoggerUtils.info(logger, feederManager.getEnvImpl(),
                            "LogFileFeeder failed with exception: " +
                            e.getCause());
                    } catch (InterruptedException e) {
                        LoggerUtils.info(logger, feederManager.getEnvImpl(),
                            "LogFileFeeder was interrupted: " +
                            e.getCause());
                    }
                }
                if (failedThreadCount == threadCount) {
                    throw new IOException("All LogFileFeeder threads failed");
                }
            } else {
                feederNum ++;
                LogFileFeeder feeder = new LogFileFeeder(
                    feederManager, namedChannel, protocol, this, feederNum);
                logFileFeeders.put(feederNum, feeder);
                feeder.start();
                feeder.join();
            }

            /* Done, cleanup */
            dbBackup.endBackup();
            dbBackup = null;
        } catch (ClosedByInterruptException e) {
            LoggerUtils.fine
                (logger, feederManager.getEnvImpl(),
                 "FeederCreator for client: " + clientId +
                 "Ignoring ClosedByInterruptException normal shutdown");
        } catch (IOException e) {
            LoggerUtils.warning(logger, feederManager.getEnvImpl(),
                "FeederCreator for client: " + clientId +
                " IO Exception: " + e.getMessage());
        } catch (BinaryProtocol.ProtocolException e) {
            LoggerUtils.severe(logger, feederManager.getEnvImpl(),
                "FeederCreator for client: " + clientId +
                " Protocol Exception: " + e.getMessage() +
                LoggerUtils.getStackTraceForSevereLog(e));
        } catch (InterruptedException e) {
            LoggerUtils.severe(logger, feederManager.getEnvImpl(),
                "FeederCreator for client: " + clientId +
                " InterruptedException : " + e.getMessage() +
                LoggerUtils.getStackTraceForSevereLog(e));
        } catch (Exception e) {
            LoggerUtils.severe(logger, feederManager.getEnvImpl(),
                "FeederCreator for client: " + clientId +
                " EnvironmentFailureException : " + e.getMessage() +
                LoggerUtils.getStackTraceForSevereLog(e));
        } finally {
            try {
                namedChannel.getChannel().close();
            } catch (IOException e) {
                LoggerUtils.warning(logger, feederManager.getEnvImpl(),
                    "FeederCreator for client: " + clientId +
                    " io exception on " +
                    "channel close: " + e.getMessage());
            }
            shutdown();
            if (dbBackup != null) {
                if (feederManager.shutdown.get() || doNotRenewLease) {
                    dbBackup.endBackup();
                } else {

                    /*
                     * Establish lease so client can resume within the lease
                     * period.
                     */
                    @SuppressWarnings("unused")
                    final FeederManager.Lease lease =
                            feederManager.new Lease(clientId,
                                    feederManager.leaseDuration,
                                    dbBackup);
                    LoggerUtils.info(logger, feederManager.getEnvImpl(),
                            "Lease created for node: " + clientId);
                }
            }
            LoggerUtils.info
                    (logger, feederManager.getEnvImpl(),
                     "Feeder Creator for client: " + clientId + " exited");
        }
    }

    /**
     * Implements the message exchange used to determine whether this feeder
     * is suitable for use the client's backup needs. The feeder may be
     * unsuitable if it's already busy, or it's not current enough to service
     * the client's needs.
     */
    private void checkFeeder(final Protocol protocol,
                             final NameIdPair remoteNameIdPair)
        throws IOException, DatabaseException {

        protocol.read(namedChannel.getChannel(), Protocol.FeederInfoReq.class);
        int feeders = feederManager.getActiveFeederCount() -
                1 /* Exclude this one */;
        long rangeFirst = NULL_VLSN;
        long rangeLast = NULL_VLSN;
        if (feederManager.getEnvImpl() instanceof RepImpl) {
            /* Include replication stream feeders as a load component. */
            final RepImpl repImpl = (RepImpl) feederManager.getEnvImpl();
            feeders +=
                    repImpl.getRepNode().feederManager().activeReplicaCount();
            final VLSNRange range = repImpl.getVLSNIndex().getRange();
            rangeFirst = range.getFirst();
            rangeLast = range.getLast();
        }

        final String msg =
            String.format("Network restore responding to node: %s" +
                          " feeders:%,d" +
                          " vlsn range:%,d-%,d",
                          remoteNameIdPair, feeders,
                          rangeFirst, rangeLast);
        LoggerUtils.info(logger, feederManager.getEnvImpl(), msg);
        protocol.write(
            protocol.new FeederInfoResp(
                feeders, rangeFirst, rangeLast, getProtocolLogVersion()),
                namedChannel);
    }

    /**
     * Processes the request for the list of files that constitute a valid
     * backup. If a leased DbBackup instance is available, it uses it,
     * otherwise it creates a new instance and uses it instead.
     */
    private void sendFileList(final Protocol protocol)
        throws IOException, BinaryProtocol.ProtocolException, DatabaseException {
        /* Wait for the request message. */
        protocol.read(namedChannel.getChannel(), Protocol.FileListReq.class);
        if (dbBackup == null) {
            dbBackup = new DbBackup(feederManager.getEnvImpl());
            dbBackup.setNetworkRestore(clientId);
            dbBackup.startBackup();
        } else {
            feederManager.leaseRenewalCount++;
        }

        /*
         * Remove the subdirectory header of the log files, because the nodes
         * that need to copy those log files may not configure the spreading
         * log files into sub directories feature.
         */

        final String[] files = dbBackup.getLogFilesInBackupSet();
        for (int i = 0; i < files.length; i++) {
            if (files[i].contains(File.separator)) {
                files[i] = files[i].substring
                    (files[i].indexOf(File.separator) + 1);
            }
        }
        protocol.write(protocol.new FileListResp(files), namedChannel);
    }

    private void sendAllFileInfo(Protocol protocol)
        throws IOException, BinaryProtocol.ProtocolException, DatabaseException {

        while (true) {
            try {
                final Protocol.FileInfoReq fileInfoReq = protocol.read(
                    namedChannel.getChannel(), Protocol.FileInfoReq.class);

                final String fileName = fileInfoReq.getFileName();
                if (fileName.equals("END")) {
                    return;
                }
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
                final Protocol.FileInfoResp resp;
                Protocol.FileInfoResp cachedResp =
                    feederManager.statResponses.get(fileName);
                final byte[] cachedDigest =
                    ((cachedResp != null) &&
                     (cachedResp.getFileLength() == length) &&
                     (cachedResp.getLastModifiedTime() == lastModified)) ?
                     cachedResp.getDigestSHA256() : null;

                if  (cachedDigest != null) {
                    digest = cachedDigest;
                } else if (fileInfoReq.getNeedSHA256()) {
                    digest = getSHA256Digest(file,
                            length, protocol.getHashAlgorithm()).digest();
                } else {
                    // Digest not requested
                    digest = new byte[0];
                }

                resp = protocol.new FileInfoResp
                        (fileName, length, lastModified, digest);

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
            } catch (BinaryProtocol.ProtocolException pe) {
                if (pe.getUnexpectedMessage() instanceof Protocol.Done) {
                    return;
                }
                throw pe;
            }
        }
    }

    private Protocol negotiateProtocol(final NameIdPair remoteNameIdPair)
        throws IOException, BinaryProtocol.ProtocolException {

        /* The initial protocol uses MIN_VERSION to support old clients. */
        Protocol protocol =
            makeProtocol(Protocol.MIN_VERSION, remoteNameIdPair);

        /* The client sends the highest version it supports. */
        final BinaryProtocol.ClientVersion clientVersion =
            protocol.read(namedChannel.getChannel(),
                          Protocol.ClientVersion.class);
        clientId = clientVersion.getNodeId();

        final FeederManager.Lease lease = feederManager.leases.get(clientId);
        if (lease != null) {
            dbBackup = lease.terminate();
        }

        final FeederCreator prev =
            feederManager.feederCreators.put(clientId, this);
        if (prev != null) {
            final SocketAddress prevFeederAddress =
                    prev.namedChannel.getChannel().getRemoteAddress();
            LoggerUtils.warning(logger, feederManager.getEnvImpl(),
                                "FeederCreator with client id:" + clientId +
                                " already present; originated from " +
                                prevFeederAddress +
                                " new connection originated from:" +
                                namedChannel.getChannel().getRemoteAddress());
        }

        if (clientVersion.getVersion() < Protocol.VERSION_3 &&
                getProtocolMaxVersion() >= Protocol.VERSION_3) {
            /*
             * VERSION_2 client does not support version negotiation and also
             * does not support this server's file format. The only way to
             * cause the client to remove this server from its list (so it
             * doesn't retry forever when there is no other compatible server)
             * is to close the connection by returning null here.
             *
             * For testing, however, we allow a V2 client when this provider
             * is configured for testing to use V2. This simulates the
             * behavior of V2 code.
             */
            LoggerUtils.warning(logger, feederManager.getEnvImpl(),
                    "Cannot provide log file feeder for client id=" + clientId +
                            ". Client version=" + clientVersion.getVersion() +
                            " does not support server's log format and connection " +
                            "must be closed to cause rejection by older client.");
            return null;
        }

        /*
         * The client supports version negotiation. Use the highest version
         * allowed by client and server, that is at least the minimum version
         * supported by the server. If the client doesn't support this
         * version, it will not attempt to use the server.
         */
        protocol = makeProtocol(
            Math.max(
                Protocol.MIN_VERSION,
                Math.min(getProtocolMaxVersion(),
                         clientVersion.getVersion())),
            remoteNameIdPair);

        protocol.write(protocol.new ServerVersion(), namedChannel);

        return protocol;
    }

    private Protocol makeProtocol(final int version,
                                  final NameIdPair remoteNameIdPair) {
        return new Protocol(
            feederManager.nameIdPair, remoteNameIdPair, version,
            feederManager.getEnvImpl());
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
     * Should be used instead of {@link LogEntryType#LOG_VERSION} to allow
     * overriding with {@link FeederManager#setTestProtocolLogVersion}.
     */
    private int getProtocolLogVersion() {
        final int testVersion = feederManager.getTestProtocolLogVersion();
        return (testVersion != 0) ? testVersion : LogEntryType.LOG_VERSION;
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

    @Override
    protected Logger getLogger() {
        return null;
    }

    protected int getClientId() {
        return clientId;
    }

    /**
     * Don't renew the lease when shutting down.
     */
    void preventLeaseRenewal() {
        doNotRenewLease = true;
    }
}
