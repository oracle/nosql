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

import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.BACKUP_FILE_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.DISPOSED_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.EXPECTED_BYTES;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.FETCH_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.SKIP_COUNT;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.TRANSFERRED_BYTES;
import static com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStatDefinition.TRANSFER_RATE;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.RestoreMarker;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FeederInfoResp;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileEnd;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileInfoResp;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileListResp;
import com.sleepycat.je.rep.impl.networkRestore.Protocol.FileStart;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ProtocolException;
import com.sleepycat.je.rep.utilint.BinaryProtocol.ServerVersion;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.AtomicIntStat;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvgRateStat;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * This class implements a hot network backup that permits it to obtain a
 * consistent set of log files from any running environment that provides a
 * LogFileFeeder service. This class thus plays the role of a client, and the
 * running environment that of a server.
 * <p>
 * The log files that are retrieved over the network are placed in a directory
 * that can serve as an environment directory for a JE stand alone or HA
 * environment. If log files are already present in the target directory, it
 * will try reuse them, if they are really consistent with those on the server.
 * Extant log files that are no longer part of the current backup file set are
 * deleted or are renamed, depending on how the backup operation was
 * configured.
 * <p>
 * Renamed backup files have the following syntax:
 *
 * NNNNNNNN.bup.<i>backup number</i>
 *
 * where the backup number is the number associated with the backup attempt,
 * rather than with an individual file. That is, the backup number is increased
 * by one each time a backup is repeated in the same directory and log files
 * actually needed to be renamed.
 * <p>
 * The implementation tries to be resilient in the face of network failures and
 * minimizes the amount of work that might need to be done if the client or
 * server were to fail and had to be restarted. Users of this API must be
 * careful to ensure that the execute() completes successfully before accessing
 * the environment. The user fails to do this, the InsufficientLogException
 * will be thrown again when the user attempts to open the environment. This
 * safeguard is implemented using the {@link RestoreMarker} mechanism.
 */
public class NetworkBackup {

    /**
     * Non-zero value overrides {@link Protocol#MAX_VERSION}.
     */
    private static volatile int testProtocolMaxVersion = 0;

    /* The server that was chosen to supply the log files. */
    private final InetSocketAddress serverAddress;

    /* The environment directory into which the log files will be backed up */
    private final File envDir;

    /* The id used during logging to identify a node. */
    private final NameIdPair clientNameId;

    /*
     * Determines whether any existing log files in the envDir should be
     * retained under a different name (with a BUP_SUFFIX), or whether it
     * should be deleted.
     */
    private final boolean retainLogfiles;

    /*
     * The minimal VLSN that the backup must cover. Used to ensure that the
     * backup is sufficient to permit replay of a replication stream from a
     * feeder. It's NULL_VLSN if the VLSN does not matter, that is, it's a
     * backup for a standalone environment.
     */
    private final long minVLSN;

    /*
     * The client abandons a backup attempt if the server is loaded beyond this
     * threshold
     */
    private final int serverLoadThreshold;

    /* The RepImpl instance used in Protocol; it may be null during tests */
    private final RepImpl repImpl;

    private final FileManager fileManager;

    /* The factory for creating new channels */
    private final DataChannelFactory channelFactory;

    /* The protocol used to communicate with the server. */
    private Protocol protocol;

    /* The channel connecting this client to the server. */
    private DataChannel channel;

    /*
     * The message digest used to compute the digest as each log file is pulled
     * over the network.
     */
    private MessageDigest messageDigest;

    /*
     * This queue will store the results of getFileTransferLengths(),
     * and each GetFileTask thread will concurrently poll from this
     * to retrieve a file.
     */
    private final ConcurrentLinkedQueue<FileAndLength> taskQueue;

    /*
     * Number of GetFileTask threads, default to 2.
     */
    private int threadCount = 2;

    /*
     * Counter to give each GetFileTask thread an id.
     */
    private AtomicInteger id = new AtomicInteger(0);

    /*
     * At the Feeder side, after negotiating the protocol,
     * feeder will be informed of this clientId, FeederCreator will register
     * a service named as ["LogFileFeeder" + clientId]. GetFileTask thread
     * needs this clientId to do service handshake with FeederCreator.
     */
    private int clientId;

    /* Statistics on number of files actually fetched and skipped */
    private final StatGroup statistics;
    private final AtomicIntStat backupFileCount;
    private final AtomicIntStat disposedCount;
    private final AtomicIntStat fetchCount;
    private final AtomicIntStat skipCount;
    private final AtomicLongStat expectedBytes;
    private final AtomicLongStat transferredBytes;
    private final LongAvgRateStat transferRate;

    private final Logger logger;

    private CyclicBarrier testBarrier = null;

    /**
     * The receive buffer size associated with the socket used for the log file
     * transfers
     */
    private final int receiveBufferSize;

    /**
     * Time to wait for a request from the client.  Calculating the file
     * checksum can take a long time if the file is large and the system
     * is overloaded, so this value is rather large.
     */
    private static final int SOCKET_TIMEOUT_MS = 90000;

    public static volatile int TEST_SOCKET_TIMEOUT_MS = 0;

    /**
     * The number of times to retry on a digest exception. That is, when the
     * SHA256 hash as computed by the server for the file does not match the
     * hash as computed by the client for the same file.
     */
    private static final int DIGEST_RETRIES = 5;

    /*
     * Save the properties from the instigating InsufficientLogException in
     * order to persist the exception into a RestoreRequired entry.
     */
    private final Properties exceptionProperties;

    /*
     * Be prepared to create a marker file saying that the log can't be
     * recovered.
     */
    private final RestoreMarker restoreMarker;

    /* For testing */
    private TestHook<File> interruptHook;
    private boolean failDuringRestore = false;

    /*
     * keep a connectionId so that at the feeder side different connections
     * won't affect each other.
     */
    public int connectionId;

    public NetworkBackup(final InetSocketAddress serverSocket,
                         final int receiveBufferSize,
                         final File envDir,
                         final NameIdPair clientNameId,
                         final boolean retainLogfiles,
                         final int serverLoadThreshold,
                         final long minVLSN,
                         final RepImpl repImpl,
                         final FileManager fileManager,
                         final LogManager logManager,
                         final DataChannelFactory channelFactory,
                         final Properties exceptionProperties,
                         final int threadCount)
            throws IllegalArgumentException {


            this(serverSocket,
                 receiveBufferSize,
                 envDir,
                 clientNameId,
                 retainLogfiles,
                 serverLoadThreshold,
                 minVLSN,
                 repImpl,
                 fileManager,
                 logManager,
                 channelFactory,
                 exceptionProperties);
            this.threadCount = threadCount;
    }

    /**
     * Creates a configured backup instance which when executed will backup the
     * files to the environment directory.
     *
     * @param serverSocket the socket on which to contact the server
     * @param receiveBufferSize the receive buffer size to be associated with
     * the socket used for the log file transfers.
     * @param envDir the directory in which to place the log files
     * @param clientNameId the id used to identify this client
     * @param retainLogfiles determines whether obsolete log files should be
     * retained by renaming them, instead of deleting them.
     * @param serverLoadThreshold only backup from this server if it has fewer
     * than this number of feeders active.
     * @param repImpl is passed in as a distinct field from the log manager and
     * file manager because it is used only for logging and environment
     * invalidation. A network backup may be invoked by unit tests without
     * an enclosing environment.
     * @param minVLSN the VLSN that should be covered by the server. It ensures
     * that the log files are sufficiently current for this client's needs.
     * @throws IllegalArgumentException if the environment directory is not
     * valid. When used internally, this should be caught appropriately.
     */
    public NetworkBackup(final InetSocketAddress serverSocket,
                         final int receiveBufferSize,
                         final File envDir,
                         final NameIdPair clientNameId,
                         final boolean retainLogfiles,
                         final int serverLoadThreshold,
                         final long minVLSN,
                         final RepImpl repImpl,
                         final FileManager fileManager,
                         final LogManager logManager,
                         final DataChannelFactory channelFactory,
                         final Properties exceptionProperties)
        throws IllegalArgumentException {

        super();
        this.serverAddress = serverSocket;
        this.receiveBufferSize = receiveBufferSize;

        if (!envDir.exists()) {
            throw new IllegalArgumentException("Environment directory: " +
                                               envDir + " not found");
        }
        this.envDir = envDir;
        this.clientNameId = clientNameId;
        this.retainLogfiles = retainLogfiles;
        this.serverLoadThreshold = serverLoadThreshold;
        this.minVLSN = minVLSN;
        this.repImpl = repImpl;
        this.fileManager = fileManager;
        this.channelFactory = channelFactory;
        taskQueue = new ConcurrentLinkedQueue<>();
        logger = LoggerUtils.getLogger(getClass());
        statistics = new StatGroup(NetworkBackupStatDefinition.GROUP_NAME,
                                   NetworkBackupStatDefinition.GROUP_DESC);
        backupFileCount = new AtomicIntStat(statistics, BACKUP_FILE_COUNT);
        disposedCount = new AtomicIntStat(statistics, DISPOSED_COUNT);
        fetchCount = new AtomicIntStat(statistics, FETCH_COUNT);
        skipCount = new AtomicIntStat(statistics, SKIP_COUNT);
        expectedBytes = new AtomicLongStat(statistics, EXPECTED_BYTES);
        transferredBytes = new AtomicLongStat(
            statistics, TRANSFERRED_BYTES);
        transferRate = new LongAvgRateStat(
            statistics, TRANSFER_RATE, 10000, SECONDS);

        this.exceptionProperties = exceptionProperties;
        restoreMarker = new RestoreMarker(fileManager, logManager, true);
    }

    /**
     * Convenience overloading.
     */
    public NetworkBackup(final InetSocketAddress serverSocket,
                         final File envDir,
                         final NameIdPair clientNameId,
                         final boolean retainLogfiles,
                         final FileManager fileManager,
                         final LogManager logManager,
                         final DataChannelFactory channelFactory)
        throws DatabaseException {

        this(serverSocket,
             0,
             envDir,
             clientNameId,
             retainLogfiles,
             Integer.MAX_VALUE,
             NULL_VLSN,
             null,
             fileManager,
             logManager,
             channelFactory,
             new Properties());
    }

    /**
     * Returns statistics associated with the NetworkBackup execution.
     */
    public NetworkBackupStats getStats() {
        return new NetworkBackupStats(statistics.cloneGroup(false));
    }

    /**
     * Execute the backup.
     */
    public String[] execute()
        throws IOException,
               DatabaseException,
               ServiceConnectFailedException,
               LoadThresholdExceededException,
               InsufficientVLSNRangeException,
               IncompatibleServerException,
               RestoreMarker.FileCreationException {

        try {
            int timeout = SOCKET_TIMEOUT_MS;
            if (TEST_SOCKET_TIMEOUT_MS != 0) {
                timeout = TEST_SOCKET_TIMEOUT_MS;
            }
            channel = channelFactory.
                connect(serverAddress,
                        (repImpl != null) ? repImpl.getHostAddress() : null,
                        new ConnectOptions().
                        setTcpNoDelay(true).
                        setReceiveBufferSize(receiveBufferSize).
                        setOpenTimeout(timeout).
                        setReadTimeout(timeout));
            ServiceDispatcher.doServiceHandshake
                (channel, FeederManager.FEEDER_SERVICE);

            final NameIdPair remoteNameIdPair =
                 new NameIdPair(serverAddress.getHostString());

            protocol = negotiateProtocol(remoteNameIdPair);
            clientId = protocol.new ClientVersion().getNodeId();
            try {
                messageDigest =
                    MessageDigest.getInstance(protocol.getHashAlgorithm());
            } catch (NoSuchAlgorithmException e) {
                // Should not happen -- if it does it's a JDK config issue
                throw EnvironmentFailureException.unexpectedException(e);
            }
            checkServer(remoteNameIdPair);
            final String[] fileNames = getFileList();

            LoggerUtils.info(logger, repImpl,
                "Restoring from:" + serverAddress +
                " Allocated network receive buffer size:" +
                channel.socket().getReceiveBufferSize() +
                "(" + receiveBufferSize + ")" +
                " candidate log file count:" + fileNames.length);

            if (protocol.getVersion() >= Protocol.VERSION_4) {
                final List<FileAndLength> fileTransferLengths =
                        getFileTransferLengths(fileNames);
                taskQueue.addAll(fileTransferLengths);
                connectionId = (int) (Math.random() * 100);

                /*
                 * Send message to FeederCreator about connectionId, use
                 * ThreadCount class to pass the Id.
                 */
                protocol.write(protocol.new ThreadCount(connectionId),
                        channel);

                /*
                 * Send message to FeederCreator about the number of threads
                 * transferring files in parallel
                 */
                protocol.write(protocol.new ThreadCount(threadCount),
                               channel);

                Protocol.ThreadCount negotiatedThreadCount =
                    protocol.read(channel, Protocol.ThreadCount.class);
                final int finalThreadCount = negotiatedThreadCount.
                    getThreadCount();
                ExecutorService executor = Executors.newFixedThreadPool(
                        finalThreadCount);

                List<Future<?>> futures = new ArrayList<>();

                LoggerUtils.info(logger, repImpl,
                    "NetworkBackup creates " + finalThreadCount +
                    " GetFileTask threads to support file transferring.");

                for (int i = 0; i < finalThreadCount; i++) {
                    Protocol taskProtocol = makeProtocol(
                        protocol.getVersion(), remoteNameIdPair);
                    Future<?> future = executor.submit(
                        new GetFileTask(taskQueue,
                                        id.incrementAndGet(),taskProtocol));
                    futures.add(future);
                }

                executor.shutdown();

                try {
                    executor.awaitTermination(Long.MAX_VALUE,
                                              TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new IOException("NetworkRestore thread got interrupted.");
                }

                int failedThreadCount = 0;
                // check status of every task
                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        LoggerUtils.info(logger, repImpl,
                            "GetFileTask failed with exception: " +
                            e.getCause());
                        failedThreadCount ++;
                    } catch (InterruptedException e) {
                        LoggerUtils.info(logger, repImpl,
                            "GetFileTask was interrupted:" +
                            e.getCause());
                    }
                }

                if (failedThreadCount == finalThreadCount) {
                    throw new IOException("All GetFileTask threads failed.");
                }
                if (!taskQueue.isEmpty()) {
                    StringBuilder msg = new StringBuilder();
                    for (FileAndLength fileAndLength : taskQueue) {
                        msg.append(fileAndLength.file.getName()).append(" ");
                    }
                    throw new IOException("Failed to retrieve files: "
                        + msg.toString());
                }
            } else {
                getFiles(fileNames);
            }

            cleanup(fileNames);
            assert fileManager.listJDBFiles().length == fileNames.length :
                "envDir=" + envDir + " list=" +
                Arrays.asList(fileManager.listJDBFiles()) +
                " fileNames=" + Arrays.asList(fileNames);

            /*
             * The fileNames array is sorted in getFileList method, so we can
             * use the first and last array elements to get the range of the
             * files to be restored.
             */
            final long fileBegin = fileManager.getNumFromName(fileNames[0]);
            final long fileEnd =
                fileManager.getNumFromName(fileNames[fileNames.length - 1]);
            /* Return file names with sub directories' names if exists. */
            return fileManager.listFileNames(fileBegin, fileEnd);
        } finally {
            if (channel != null) {
                /*
                 * Closing the socket directly is not correct.  Let the channel
                 * do the work (necessary for correct TLS operation).
                */
                channel.close();
            }
            LoggerUtils.info(logger, repImpl,
                "Backup file total: " +
                backupFileCount.get() +
                ".  Files actually fetched: " +
                fetchCount.get() +
                ".  Files skipped(available locally): " +
                skipCount.get() +
                ".  Local files renamed/deleted: " +
                disposedCount.get());
        }
    }

    /**
     * Ensures that the log file feeder is a suitable choice for this backup:
     * The feeder's VLSN range end must be GTE the minVSLN and its load must
     * be LTE the serverLoadThreshold.
     */
    private void checkServer(final NameIdPair remoteNameIdPair)
        throws IOException,
               ProtocolException,
               IncompatibleServerException,
               LoadThresholdExceededException,
               InsufficientVLSNRangeException {

        protocol.write(protocol.new FeederInfoReq(), channel);

        final FeederInfoResp resp =
            protocol.read(channel, FeederInfoResp.class);

        if (resp.getLogVersion() > LogEntryType.LOG_VERSION) {
            throw new IncompatibleServerException(
                "Server " + remoteNameIdPair + " log version (" +
                    resp.getLogVersion() + ") cannot be used for network" +
                    " restore. Server version is greater than client" +
                    " version (" + LogEntryType.LOG_VERSION + ")");
        }
        if (resp.getRangeLast() < minVLSN) {
            throw new InsufficientVLSNRangeException(
                minVLSN,
                resp.getRangeFirst(), resp.getRangeLast(),
                resp.getActiveFeeders());
        }
        if (resp.getActiveFeeders() > serverLoadThreshold) {
            throw new LoadThresholdExceededException(
                serverLoadThreshold,
                resp.getRangeFirst(), resp.getRangeLast(),
                resp.getActiveFeeders());
        }
    }

    /**
     * Delete or rename residual jdb files that are not part of the log file
     * set and finally delete the marker file.
     *
     * <p>This method is only invoked after all required files have been
     * copied over from the server, and it does a final check that all
     * required files exist.</p>
     */
    private void cleanup(final String[] fileNames)
        throws IOException {

        LoggerUtils.fine(logger, repImpl, "Cleaning up");

        final Set<String> logFileSet = new HashSet<>(Arrays.asList(fileNames));
        final StringBuilder logFiles = new StringBuilder();
        for (final String string : logFileSet) {

            /*
             * Use the full path of this file in case the environment uses
             * multiple data directories.
             */
            final File file = new File(fileManager.getFullFileName(string));
            if (!file.exists()) {
                throw EnvironmentFailureException.unexpectedState
                    ("Missing file: " + file);
            }
            logFiles.append(file.getCanonicalPath()).append(", ");
        }

        String names = logFiles.toString();
        if (names.length() > 0) {
            names = names.substring(0, names.length()-2);
        }
        LoggerUtils.fine(logger, repImpl, "Log file set: " + names);

        /*
         * Delete/rename the residual files and then delete the marker file.
         * The marker file is deleted last since it is the durable indication
         * that the files can be used for recovery.
         *
         * Do this only after confirming above that the required files exist
         * [#27346].
         */
        final String markerFileName = RestoreMarker.getMarkerFileName();
        for (File file : fileManager.listJDBFiles()) {
            final String fname = file.getName();
            if (!logFileSet.contains(fname) &&
                !markerFileName.equals(fname)) {
                disposeFile(file);
            }
        }

        if (failDuringRestore) {
            throw new EnvironmentFailureException(
                repImpl,
                EnvironmentFailureReason.TEST_INVALIDATE);
        }
        restoreMarker.removeMarkerFile(fileManager);
    }

    /**
     * Retrieves all the files in the list, that are not already in the envDir.
     */
    private void getFiles(final String[] fileNames)
        throws IOException, DatabaseException,
                RestoreMarker.FileCreationException {

        LoggerUtils.info(logger, repImpl,
            fileNames.length + " files in backup set");

        /* Get all file transfer lengths first, so we can track progress */
        final List<FileAndLength> fileTransferLengths =
            getFileTransferLengths(fileNames);

        for (final FileAndLength entry : fileTransferLengths) {
            if (testBarrier != null) {
                try {
                    testBarrier.await();
                } catch (InterruptedException e) {
                    // Ignore just a test mechanism
                } catch (BrokenBarrierException e) {
                    throw EnvironmentFailureException.unexpectedException(e);
                }
            }

            for (int i = 0; i < DIGEST_RETRIES; i++) {
                try {
                    getFile(entry.file);
                    fetchCount.increment();
                    break;
                } catch (DigestException e) {
                    if ((i + 1) == DIGEST_RETRIES) {
                        throw new IOException("Digest mismatch despite "
                            + DIGEST_RETRIES + " attempts");
                    }

                    /* Account for the additional transfer */
                    expectedBytes.add(entry.length);
                }
            }
        }

        /* All done, shutdown conversation with the server. */
        protocol.write(protocol.new Done(), channel);
    }

    /** Store File and file length pair. */
    private static class FileAndLength {
        FileAndLength(final File file, final long length) {
            this.file = file;
            this.length = length;
        }
        final File file;
        final long length;
    }

    /**
     * Returns information about files that need to be transferred, and updates
     * expectedBytes and skipCount accordingly.  This method tries to avoid
     * requesting the SHA-256 if the file lengths are not equal, since computing
     * the SHA-256 if it's not already cached requires a pass over the log
     * file. Note that the server will always send back the SHA-256 value if it
     * has it cached.
     *
     * In this method, if a  file is present with correct length, then it can
     * be skipped, a file is present with incorrect length should be considered
     * as obsolete and got deleted right away.
     */
    private List<FileAndLength> getFileTransferLengths(
        final String[] fileNames) throws IOException, DatabaseException,
        RestoreMarker.FileCreationException {

        final List<FileAndLength> fileTransferLengths = new ArrayList<>();
        for (final String fileName : fileNames) {

            /*
             * Use the full path of this file in case the environment uses
             * multiple data directories.
             */
            final File file = new File(fileManager.getFullFileName(fileName));
            protocol.write(protocol.new FileInfoReq(fileName, false), channel);
            FileInfoResp statResp =
                protocol.read(channel, Protocol.FileInfoResp.class);
            final long fileLength = statResp.getFileLength();

            /*
             * See if we can skip the file if it is present with correct length
             */
            if (file.exists() && (fileLength == file.length())) {

                /* Make sure we have the message digest */
                if (statResp.getDigestSHA256().length == 0) {
                    protocol.write(
                        protocol.new FileInfoReq(fileName, true), channel);
                    statResp =
                        protocol.read(channel, Protocol.FileInfoResp.class);
                }
                final String algorithm = protocol.getHashAlgorithm();
                final byte[] digest = LogFileFeeder.getSHA256Digest(
                    file, fileLength, algorithm).digest();
                if (Arrays.equals(digest, statResp.getDigestSHA256())) {
                    LoggerUtils.info(logger, repImpl,
                        "File: " + file.getCanonicalPath() +
                        " length: " + fileLength + " available" +
                        " with matching " + algorithm + ", copy skipped");
                    skipCount.increment();
                    continue;
                }
            } else if (file.exists()) {
                disposedCount.increment();

                restoreMarker.createMarkerFile
                        (RestoreRequired.FailureType.NETWORK_RESTORE,
                                exceptionProperties);

                final long fileNumber = fileManager.getNumFromName(file.getName());
                if (retainLogfiles) {
                    final boolean renamed;
                    try {
                        renamed = fileManager.renameFile(
                            fileNumber,FileManager.BUP_SUFFIX);
                    } catch (IOException e) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Could not rename log file " + file.getPath() +
                             " because of exception: " + e.getMessage());
                    }

                    if (!renamed) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Could not rename log file " +  file.getPath());
                    }
                    LoggerUtils.fine(logger, repImpl,
                                     "Renamed log file: " + file.getPath());
                } else {
                    final boolean deleted = file.delete();
                    if (!deleted) {
                        throw EnvironmentFailureException.unexpectedState
                                ("Could not delete file: " + file);
                    }
                }
            }
            fileTransferLengths.add(new FileAndLength(file, fileLength));
            expectedBytes.add(fileLength);
        }

        /* send a message indicating the end of FileInfo exchange phase*/
        if (protocol.getVersion() >= Protocol.VERSION_4) {
            protocol.write(protocol.new FileInfoReq("END", false), channel);
        }

        return fileTransferLengths;
    }

    protected void getFile(final File file)
        throws DigestException, RestoreMarker.FileCreationException, IOException {
            getFile(file, channel, protocol,messageDigest);
    }

    /**
     * Requests and obtains the specific log file from the server. The file is
     * first created under a name with the .tmp suffix and is renamed to its
     * true name only after its digest has been verified.
     *
     * This method is protected to facilitate error testing.
     */
    protected void getFile(final File file, final DataChannel channel,
                           final Protocol protocol, final MessageDigest digest)
        throws IOException, ProtocolException, DigestException,
                RestoreMarker.FileCreationException {

        LoggerUtils.fine(logger, repImpl, "Requesting file: " + file);
        protocol.write(protocol.new FileReq(file.getName()), channel);
        final FileStart fileResp =
            protocol.read(channel, Protocol.FileStart.class);

        /*
         * Delete the tmp file if it already exists.
         *
         * Use the full path of this file in case the environment uses multiple
         * data directories.
         */
        final File tmpFile = new File(
            fileManager.getFullFileName(file.getName()) +
                FileManager.TMP_SUFFIX);
        if (tmpFile.exists()) {
            final boolean deleted = tmpFile.delete();
            if (!deleted) {
                throw EnvironmentFailureException.unexpectedState
                    ("Could not delete file: " + tmpFile);
            }
        }

        /*
         * Use a direct buffer to avoid an unnecessary copies into and out of
         * native buffers.
         */
        final ByteBuffer buffer =
                ByteBuffer.allocateDirect(LogFileFeeder.TRANSFER_BYTES);
        digest.reset();

        /* Write the tmp file. */
        final FileOutputStream fileStream = new FileOutputStream(tmpFile);
        final FileChannel fileChannel = fileStream.getChannel();

        try {
            /* Compute the transfer rate roughly once each MB */
            final int rateInterval = 0x100000 / LogFileFeeder.TRANSFER_BYTES;
            int count = 0;

            /* Copy over the file contents. */
            for (long bytes = fileResp.getFileLength(); bytes > 0;) {
                final int readSize =
                    (int) Math.min(LogFileFeeder.TRANSFER_BYTES, bytes);
                buffer.clear();
                buffer.limit(readSize);
                final int actualBytes = channel.read(buffer);
                if (actualBytes == -1) {
                    throw new IOException("Premature EOF. Was expecting:"
                                          + readSize);
                }
                bytes -= actualBytes;

                buffer.flip();
                fileChannel.write(buffer);

                buffer.rewind();
                digest.update(buffer);
                transferredBytes.add(actualBytes);

                /* Update the transfer rate at interval and last time */
                if (((++count % rateInterval) == 0) || (bytes <= 0)) {
                    transferRate.add(
                        transferredBytes.get(), TimeSupplier.currentTimeMillis());
                }
            }

            if (logger.isLoggable(Level.INFO)) {
                LoggerUtils.info(logger, repImpl,
                    String.format(
                        "Fetched log file: %s, size: %,d bytes," +
                        " %s bytes," +
                        " %s bytes," +
                        " %s bytes/second",
                        file.getName(),
                        fileResp.getFileLength(),
                        transferredBytes,
                        expectedBytes,
                        transferRate));
            }
        } finally {
            fileStream.close();
        }

        final FileEnd fileEnd = protocol.read(channel, Protocol.FileEnd.class);

        /* Check that the read is successful. */
        if (!Arrays.equals(digest.digest(), fileEnd.getDigestSHA256())) {
            LoggerUtils.warning(logger, repImpl,
                "digest mismatch on file: " + file);
            throw new DigestException();
        }

        /*
         * We're about to alter the files that exist in the log, either by
         * deleting file N.jdb, or by renaming N.jdb.tmp -> N, and thereby
         * adding a file to the set in the directory. Create the marker that
         * says this log is no longer coherent and can't be recovered. Marker
         * file creation can safely be called multiple times; the file will
         * only be created the first time.
         */
        restoreMarker.createMarkerFile
            (RestoreRequired.FailureType.NETWORK_RESTORE,
             exceptionProperties);

        /*
         * synchronize this since when getFile() running concurrently doHook
         * will go wrong
         */
        synchronized (this) {
            assert TestHookExecute.doHookIfSet(interruptHook, file);
        }

        /*
         * Since this file could be the task left from another failed
         * GetFileTask thread and the file has been renamed. So delete
         * it first before renaming.
         */
        if (file.exists()) {
            final boolean deleted = file.delete();
            if (!deleted) {
                throw EnvironmentFailureException.unexpectedState
                    ("Could not delete file: " + file);
            }
        }

        LoggerUtils.fine(logger, repImpl, "Renamed " + tmpFile + " to " + file);
        final boolean renamed = tmpFile.renameTo(file);
        if (!renamed) {
            throw EnvironmentFailureException.unexpectedState
                ("Rename from: " + tmpFile + " to " + file + " failed");
        }

        /*
         * Note that we no longer update the modified time to match the
         * original, which was done to aid debugging, because the BackupManager
         * needs to be able to detect when a network restore modifies a log
         * file.
         */
    }

    /**
     * Remove the file from the current set of log files in the directory.
     */
    private void disposeFile(final File file) {
        disposedCount.increment();
        final long fileNumber = fileManager.getNumFromName(file.getName());
        if (retainLogfiles) {
            final boolean renamed;
            try {
                renamed =
                    fileManager.renameFile(fileNumber, FileManager.BUP_SUFFIX);
            } catch (IOException e) {
                throw EnvironmentFailureException.unexpectedState
                    ("Could not rename log file " + file.getPath() +
                     " because of exception: " + e.getMessage());
            }

            if (!renamed) {
                throw EnvironmentFailureException.unexpectedState
                    ("Could not rename log file " +  file.getPath());
            }
            LoggerUtils.fine(logger, repImpl,
                "Renamed log file: " + file.getPath());
        } else {
            final boolean deleted;
            try {
                deleted = fileManager.deleteFile(fileNumber);
            } catch (IOException e) {
                throw EnvironmentFailureException.unexpectedException
                    ("Could not delete log file " + file.getPath() +
                     " during network restore.", e);
            }
            if (!deleted) {
                throw EnvironmentFailureException.unexpectedState
                    ("Could not delete log file " +  file.getPath());
            }
            LoggerUtils.fine(logger, repImpl,
                "deleted log file: " + file.getPath());
        }
    }

    /**
     * Carries out the message exchange to obtain the list of backup files.
     */
    private String[] getFileList()
        throws IOException, ProtocolException {

        protocol.write(protocol.new FileListReq(), channel);
        final FileListResp fileListResp = protocol.read(
            channel, Protocol.FileListResp.class);
        final String[] fileList = fileListResp.getFileNames();
        Arrays.sort(fileList); //sort the file names in ascending order
        backupFileCount.set(fileList.length);
        return fileList;
    }

    /**
     * Verify that the protocols are compatible, switch to a different protocol
     * version, if we need to.
     */
    private Protocol negotiateProtocol(final NameIdPair remoteNameIdPair)
        throws IOException, IncompatibleServerException {

        /*
         * Send MAX_VERSION even though VERSION_2 servers do not support
         * version negotiation. The VERSION_2 server will log a warning, but
         * will allow the client to decide whether to proceed.
         */
        final Protocol protocol =
            makeProtocol(getProtocolMaxVersion(), remoteNameIdPair);
        protocol.write(protocol.new ClientVersion(), channel);

        final ServerVersion serverVersion =
            protocol.read(channel, ServerVersion.class);

        if (serverVersion.getVersion() < Protocol.MIN_VERSION ||
            serverVersion.getVersion() > getProtocolMaxVersion()) {
            throw new IncompatibleServerException(
                "Server " + remoteNameIdPair + " protocol version (" +
                    serverVersion.getVersion() + ") cannot be used for" +
                    " network restore. Server version is not a supported" +
                    " client version (min=" + Protocol.MIN_VERSION +
                    ", max=" + getProtocolMaxVersion() + ")");
        }

        return makeProtocol(serverVersion.getVersion(), remoteNameIdPair);
    }

    private Protocol makeProtocol(final int version,
                                  final NameIdPair remoteNameIdPair) {
        return new Protocol(clientNameId, remoteNameIdPair, version, repImpl);
    }

    /*
     * @hidden
     *
     * A test entry point used to simulate a slow network restore.
     */
    public void setTestBarrier(final CyclicBarrier  testBarrier) {
        this.testBarrier = testBarrier;
    }

    /* For unit testing only */
    public void setInterruptHook(final TestHook<File> hook) {
        interruptHook = hook;
    }

    /* For unit testing only */
    public void setFailDuringRestore(boolean fail) {
        failDuringRestore = fail;
    }

    /* For unit testing */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Exception indicating that the digest sent by the server did not match
     * the digest computed by the client, that is, the log file was corrupted
     * during transit.
     */
    @SuppressWarnings("serial")
    protected static class DigestException extends Exception
        implements NotSerializable {
    }

    /**
     * Indicates the server is fundamentally incompatible with the client and
     * must not be used for the restore.
     */
    @SuppressWarnings("serial")
    public static class IncompatibleServerException extends Exception
        implements NotSerializable {

        IncompatibleServerException(final String msg) {
            super(msg);
        }
    }

    /**
     * Indicates that the server did not qualify for the restore, but can
     * be tried again later if another server doesn't qualify. Qualifiers
     * can change over time.
     */
    @SuppressWarnings("serial")
    public static class RejectedServerException extends Exception
        implements NotSerializable {

        /* The actual range covered by the server. */
        final long rangeFirst;
        final long rangeLast;

        /* The actual load of the server. */
        final int activeServers;

        RejectedServerException(final long rangeFirst,
                                final long rangeLast,
                                final int activeServers) {
            this.rangeFirst = rangeFirst;
            this.rangeLast = rangeLast;
            this.activeServers = activeServers;
        }

        public long getRangeLast() {
            return rangeLast;
        }

        public int getActiveServers() {
            return activeServers;
        }
    }

    /**
     * Exception indicating that the server vlsn range did not cover the VLSN
     * of interest.
     */
    @SuppressWarnings("serial")
    public static class InsufficientVLSNRangeException
        extends RejectedServerException implements NotSerializable {

        /* The VLSN that must be covered by the server. */
        private final long minVLSN;

        InsufficientVLSNRangeException(final long minVLSN,
                                       final long rangeFirst,
                                       final long rangeLast,
                                       final int activeServers) {
            super(rangeFirst, rangeLast, activeServers);
            this.minVLSN = minVLSN;
        }

        @Override
        public String getMessage() {
            return "Insufficient VLSN range. Needed VLSN: " + minVLSN +
                   " Available range: " +
                   "[" + rangeFirst + ", " + rangeLast + "]";
        }
    }

    @SuppressWarnings("serial")
    public static class LoadThresholdExceededException
        extends RejectedServerException implements NotSerializable {

        private final int threshold;

        LoadThresholdExceededException(final int threshold,
                                       final long rangeFirst,
                                       final long rangeLast,
                                       final int activeServers) {
            super(rangeFirst, rangeLast, activeServers);
            assert(activeServers > threshold);
            this.threshold = threshold;
        }

        @Override
        public String getMessage() {
            return "Active server threshold: " + threshold + " exceeded. " +
                "Active servers: " + activeServers;
        }
    }

    /**
     * Should be used instead of {@link Protocol#MAX_VERSION} to allow
     * overriding with {@link #setTestProtocolMaxVersion}.
     */
    private static int getProtocolMaxVersion() {
        return (testProtocolMaxVersion != 0) ?
            testProtocolMaxVersion : Protocol.MAX_VERSION;
    }

    /**
     * Non-zero value overrides {@link Protocol#MAX_VERSION}.
     */
    public static void setTestProtocolMaxVersion(final int version) {
        testProtocolMaxVersion = version;
    }

    /*
     * GetFileTask's job is to retrieve tasks from taskQueue and then get files
     * from server.
     * One thing to note: if the taskQueue is empty then throw out an
     * exception,reason is below:
     * Suppose we have two GetFileTask threads and only one file to transfer.
     * Thread 1 polls from the queue but failed in the middle.
     * Thread 2 finds nothing in the queue and exit directly.
     * Thread 2 exits successfully, it is considered that the whole backup
     * succeed. Since If there exists a GetFileTask exits successfully without
     * any exception, then that means backup succeeds.
     * Though in this case the backup should be considered as failed.
     * So if a GetFileTask sees the queue to be empty then throws out an
     * exception.
     */
    class GetFileTask implements Runnable {

        private ConcurrentLinkedQueue<FileAndLength> taskQueue;
        private int id;
        private Protocol protocol;

        public GetFileTask (ConcurrentLinkedQueue<FileAndLength> taskQueue,
                            int id,
                            Protocol protocol) {
            this.taskQueue = taskQueue;
            this.protocol = protocol;
            this.id = id;
        }
        @Override
        public void run() {
            FileAndLength fileInfo = null;

            int timeout = SOCKET_TIMEOUT_MS;
            if (TEST_SOCKET_TIMEOUT_MS != 0) {
                timeout = TEST_SOCKET_TIMEOUT_MS;
            }

            DataChannel channel = null;
            try {
                channel = channelFactory.
                        connect(serverAddress,
                                (repImpl != null) ? repImpl.getHostAddress() :
                                null,
                                new ConnectOptions().
                                    setTcpNoDelay(true).
                                    setReceiveBufferSize(receiveBufferSize).
                                    setOpenTimeout(timeout).
                                    setReadTimeout(timeout));
                ServiceDispatcher.doServiceHandshake
                        (channel, FeederCreator.LOG_FEEDER_SERVICE + clientId +
                         connectionId);

                MessageDigest taskDigest;
                try {
                    taskDigest =
                        MessageDigest.getInstance(protocol.getHashAlgorithm());
                } catch (NoSuchAlgorithmException e) {
                    // Should not happen -- if it does it's a JDK config issue
                    throw EnvironmentFailureException.unexpectedException(e);
                }

                if (testBarrier != null) {
                    try {
                        testBarrier.await();
                    } catch (InterruptedException e) {
                        // Ignore just a test mechanism
                    } catch (BrokenBarrierException e) {
                        throw EnvironmentFailureException.unexpectedException(e);
                    }
                }

                boolean firstRound = true;
                while ((fileInfo = taskQueue.poll()) != null) {
                    firstRound = false;
                    for (int i = 0; i < DIGEST_RETRIES; i++) {
                        try {
                            getFile(fileInfo.file, channel, protocol, taskDigest);
                            fetchCount.increment();
                            break;
                        } catch (DigestException e) {
                            if ((i + 1) == DIGEST_RETRIES) {
                                throw new IOException("Digest mismatch despite "
                                        + DIGEST_RETRIES + " attempts");
                            }
                            /* Account for the additional transfer */
                            expectedBytes.add(fileInfo.length);
                        } catch (RestoreMarker.FileCreationException e) {
                            throw new IOException();
                        }
                    }
                }
                if (firstRound) {
                    throw new FileTransferException("GetFileTask thread " + id +
                        " without doing any file transferring", null);
                }

                /* All done, shutdown conversation with the server. */
                protocol.write(protocol.new Done(), channel);
            } catch (DatabaseException |
                     IOException |
                     ServiceConnectFailedException e) {
                /* put it back */
                if (fileInfo != null) {
                    taskQueue.add(fileInfo);
                }
                LoggerUtils.warning(logger, repImpl,
                    "GetFileTask thread failed" + e);
                throw new FileTransferException(
                    "GetFileTask thread" + id + "failed", e);
            } finally {
                if (channel != null) {
                    /*
                     * Closing the socket directly is not correct.  Let the channel
                     * do the work (necessary for correct TLS operation).
                     */
                    try {
                        channel.close();
                    } catch (IOException e) {
                        throw new FileTransferException(
                            "failed to close channel of GetFileTask " + id, e);
                    }
                }
            }

        }
    }
}
