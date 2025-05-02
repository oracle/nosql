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

package com.sleepycat.je.rep.arbiter.impl;

import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_MASTER;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_ACKS;
import static com.sleepycat.je.rep.arbiter.impl.ArbiterStatDefinition.ARB_N_REPLAY_QUEUE_OVERFLOW;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicaRetryException;
import com.sleepycat.je.rep.ReplicaConnectRetryException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThread;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThreadBase;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.BaseProtocol.ShutdownRequest;
import com.sleepycat.je.rep.stream.InputWireRecord;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshake;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshakeConfig;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.BinaryProtocol.MessageOp;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.NamedChannelWithTimeout;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.RepUtils.Clock;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.StringStat;

/**
 * The ArbiterAcker is used to acknowledge transactions. A feeder
 * connection is established with the current master. Commit and Heartbeat
 * messages are sent by the master. The ArbiterAcker responds and persistently
 * tracks the high VLSN of the commit messages that it acknowledges.
 *
 * There are configuration parameters that are used.
 * RepParams.REPLICA_MESSAGE_QUEUE_SIZE used for the replay queue size and
 *                                      in the computation of the output
 *                                      queue size.
 * RepParams.REPLICA_TIMEOUT used for the Arbiter feeder channel timeout.
 * RepParams.PRE_HEARTBEAT_TIMEOUT used for the Arbiter feeder channel timeout
 *                                 before the first heartbeat is sent.
 * RepParams.REPLICA_RECEIVE_BUFFER_SIZE used for the datachannel buffer size.
 * RepParams.REPSTREAM_OPEN_TIMEOUT used for the datachannel open timeout.
 * RepParams.MAX_CLOCK_DELTA - used for ReplicaFeederHandshake maximum clock
 *                             delta.
 * RepParams.HEARTBEAT_INTERVAL heartbeat interval in millis.
 * RepParams.ENABLE_GROUP_ACKS enables output thread ack grouping.
 *
 * The main Arbiter thread reads messages from the feeder channel and queues
 * the message on the request queue. The request thread reads entries
 * from the request queue. The request thread may queue an entry on the
 * output queue. The ArbiterOutputThread reads from the output queue and
 * writes to the network channel.
 *  read from network -> ArbiterAcker main thread  -> requestQueue
 *  requestQueue      -> RequestThread             -> outputQueue
 *  outputQueue       -> ArbiterOutputThread       -> writes to network
 */
class ArbiterAcker {

    /*
     * Defines the possible types of exits that can be requested from the
     * RequestThread.
     */
    private enum RequestExitType {
        IMMEDIATE, /* An immediate exit; ignore queued requests. */
        SOFT       /* Process pending requests in queue, then exit */
    }
    /* Number of times to retry on a network connection failure. */
    private static final int NETWORK_RETRIES = 2 ;

    /*
     * Service unavailable retries. These are typically the result of service
     * request being made before the node is ready to provide them. For
     * example, the feeder service is only available after a node has
     * transitioned to becoming the master.
     */
    private static final int SERVICE_UNAVAILABLE_RETRIES = 10;

    /*
     * The number of ms to wait between above retries, allowing time for the
     * master to assume its role, and start listening on its port.
     */
    private static final int CONNECT_RETRY_SLEEP_MS = 1000;

    /* The queue poll interval, 1 second */
    private final static long QUEUE_POLL_INTERVAL_NS = 1000000000l;

    /* The exception that provoked the ArbiterAcker exit. */
    private Exception shutdownException = null;

    private final RepImpl repImpl;
    private final Logger logger;
    private NamedChannelWithTimeout arbiterFeederChannel;
    private final Clock clock;
    private Protocol protocol;
    private final ArbiterImpl arbiterImpl;

    private final BlockingQueue<Long> outputQueue;

    /*
     * The message queue used for communications between the network read
     * thread and the request thread.
     */
    private final BlockingQueue<Message> requestQueue;

    private ArbiterOutputThread arbiterOutputThread;
    private RequestThread requestThread;

    /*
     * The last commit entry acknowledged.
     */
    private volatile long lastReplayedVLSN = INVALID_VLSN;

    private volatile int masterHeartbeatId = -1;

    /* The in-memory DTVLSN maintained by the Arbiter. */
    private long dtvlsn = NULL_VLSN;

    /* The master term maintained by the replica */
    private long masterTerm = MasterTerm.MIN_TERM;

    /* Statistics */
    private final StatGroup stats;

    /*
     * The number of times a message entry could not be inserted into
     * the queue within the poll period and had to be retried.
     */
    private final LongStat nReplayQueueOverflow;
    /* Number of transactions acknowledged */
    private final LongStat nAcks;
    /* Current or last master that was connected */
    private final StringStat masterStat;

    /*
     * The maximum number of entries pulled out of the request queue that
     * are grouped together. There is at most one write to the data file for
     * this group.
     */
    private final int N_MAX_GROUP_XACT = 100;
    private final List<Message> groupMessages = new ArrayList<>();
    private final List<Long> groupXact = new ArrayList<>();
    private final long FSYNC_INTERVAL = 1000;
    private long lastFSyncTime;

    ArbiterAcker(ArbiterImpl arbiterImpl,
                 RepImpl repImpl) {
        this.arbiterImpl = arbiterImpl;
        this.repImpl = repImpl;
        logger = repImpl.getLogger();

        clock = new Clock(RepImpl.getClockSkewMs());
        /* Set up the request queue. */
        final int requestQueueSize = repImpl.getConfigManager().
            getInt(RepParams.REPLICA_MESSAGE_QUEUE_SIZE);

        requestQueue = new ArrayBlockingQueue<>(requestQueueSize);

        /*
         * The factor of 2 below is somewhat arbitrary. It should be > 1 X so
         * that the RequestThread can completely process the buffered
         * messages in the face of a network drop and 2X to allow for
         * additional headroom and minimize the chances that the operation
         * might be blocked due to the limited queue length.
         */
        final int outputQueueSize = 2 *
            repImpl.getConfigManager().getInt(
                RepParams.REPLICA_MESSAGE_QUEUE_SIZE);
        outputQueue = new ArrayBlockingQueue<>(outputQueueSize);

        stats = new StatGroup(ArbiterStatDefinition.GROUP_NAME,
                              ArbiterStatDefinition.GROUP_DESC);
        nReplayQueueOverflow =
            new LongStat(stats, ARB_N_REPLAY_QUEUE_OVERFLOW);
        nAcks = new LongStat(stats, ARB_N_ACKS);
        masterStat = new StringStat(stats, ARB_MASTER);
    }

    private void initializeConnection()
        throws ReplicaConnectRetryException,
               IOException {
            createArbiterFeederChannel();
            arbiterImpl.refreshCachedGroup();
            ReplicaFeederHandshake handshake =
                new ReplicaFeederHandshake(new RepFeederHandshakeConfig());
            protocol = handshake.execute();

            arbiterImpl.refreshCachedGroup();

            /* read heartbeat and respond */
            masterHeartbeatId = protocol.
                read(arbiterFeederChannel.getChannel(),
                     Protocol.Heartbeat.class).
                getHeartbeatId();
            queueAck(ReplicaOutputThread.HEARTBEAT_ACK);

            /* decrement latch to indicate we are connected */
            arbiterImpl.getReadyLatch().countDown();
            arbiterImpl.notifyJoinGroup();
    }

    public int getMasterHeartbeatId() {
        return masterHeartbeatId;
    }

    /**
     * The core Arbiter control loop. The loop exits when it
     * encounters one of the following possible conditions:
     *
     * 1) The connection to the master can no longer be maintained, due to
     * connectivity issues, or because the master has explicitly shutdown its
     * connections due to an election.
     *
     * 2) The node becomes aware of a change in master, that is, assertSync()
     * fails.
     *
     * 3) The loop is interrupted, which is interpreted as a request to
     * shutdown the Arbiter node as a whole.
     *
     * 4) It fails to establish its node information in the master as it
     * attempts to join the replication group for the first time.
     *
     * Normal exit from this run loop results in the Arbiter node retrying
     * finding the group master.
     * A thrown exception, on the other hand, results in the Arbiter
     * node as a whole terminating its operation and no longer participating in
     * the replication group, that is, it enters the DETACHED state.
     *
     * @throws InterruptedException
     * @throws DatabaseException if the environment cannot be closed/for a
     * re-init
     * @throws GroupShutdownException
     */
    void runArbiterAckLoop()
        throws InterruptedException,
               DatabaseException,
               GroupShutdownException {

        Class<? extends ReplicaRetryException> retryExceptionClass = null;
        int retryCount = 0;
        try {

            while (true) {
                try {
                    runArbiterAckLoopInternal();
                    /* Normal exit */
                    break;
                } catch (ReplicaRetryException e) {
                    if (!arbiterImpl.getMasterStatus().inSync()) {
                        LoggerUtils.fine(logger, repImpl,
                                         "Retry terminated, out of sync.");
                        break;
                    }
                    if ((e.getClass() == retryExceptionClass) ||
                        (e.getNRetries() == 0)) {
                        if (++retryCount >= e.getNRetries()) {
                            /* Exit replica retry elections */
                            LoggerUtils.info
                                (logger, repImpl,
                                 "Failed to recover from exception: " +
                                 e.getMessage() + ", despite " + e.getNRetries()
                                 + " retries.\n" +
                                 LoggerUtils.getStackTrace(e));
                            break;
                        }
                    } else {
                        retryCount = 0;
                        retryExceptionClass = e.getClass();
                    }
                    LoggerUtils.fine(logger, repImpl, "Retry #: " +
                                     retryCount + "/" + e.getNRetries() +
                                     " Will retry Arbiter loop after " +
                                     e.getRetrySleepMs() + "ms. ");
                    Thread.sleep(e.getRetrySleepMs());
                    if (!arbiterImpl.getMasterStatus().inSync()) {
                        break;
                    }
                }
            }
        } finally {
            arbiterImpl.resetReadyLatch(shutdownException);
        }
        /* Exit use elections to try a different master. */
    }

    void shutdown() {
        if (requestThread != null) {
            try {
                requestThread.shutdownThread(logger);
            } catch (Exception e) {
                /* Ignore so shutdown can continue */
                LoggerUtils.info(logger,  repImpl,
                    "Request thread error shutting down." + e);
            }
        }
        if (arbiterOutputThread != null) {
            arbiterOutputThread.shutdownThread(logger);
            try {
                arbiterOutputThread.join();
            } catch(InterruptedException e) {
                /* Ignore we will clean up via killing IO channel anyway. */
            }
        }
        RepUtils.shutdownChannel(arbiterFeederChannel);
    }

    private void runArbiterAckLoopInternal()
        throws InterruptedException,
               ReplicaRetryException {

        shutdownException = null;
        LoggerUtils.info(logger, repImpl,
                         "Arbiter loop started with master: " +
                         arbiterImpl.getMasterStatus().getNodeMasterNameId());
        try {
            initializeConnection();
            arbiterImpl.setState(ReplicatedEnvironment.State.REPLICA);
            doRunArbiterLoopInternalWork();
            arbiterImpl.setState(ReplicatedEnvironment.State.UNKNOWN);
        } catch (ClosedByInterruptException closedByInterruptException) {
            if (arbiterImpl.isShutdown()) {
                LoggerUtils.info(logger, repImpl,
                                 "Arbiter loop interrupted for shutdown.");
                return;
            }
            LoggerUtils.warning(logger, repImpl,
                                "Arbiter loop unexpected interrupt.");
            throw new InterruptedException
                (closedByInterruptException.getMessage());
        } catch (IOException | UnknownMasterException e) {
            /*
             * Master may have changed with the master shutting down its
             * connection as a result. Or there may be a lack of quorum
             * preventing selection of a master. Normal course of events, log
             * it and return to the outer node level loop.
             */
            LoggerUtils.fine(logger, repImpl,
                             "Arbiter exception: " + e.getMessage() +
                             "\n" + LoggerUtils.getStackTrace(e));
        } catch (ReplicaRetryException e) {
            /* Propagate it outwards. Node does not need to shutdown. */
            throw e;
        } catch (GroupShutdownException e) {
            shutdownException = e;
            throw e;
        } catch (RuntimeException e) {
            shutdownException = e;
            LoggerUtils.severe(logger, repImpl,
                               "Arbiter unexpected exception " + e +
                                " " + LoggerUtils.getStackTraceForSevereLog(e));
            throw e;
        } catch (MasterObsoleteException e) {
            /* expected change in masters from an election. */
            LoggerUtils.fine(logger, repImpl, e.getMessage());
        } catch (Exception e) {
            shutdownException = e;
            LoggerUtils.severe(logger, repImpl,
                               "Arbiter unexpected exception " + e +
                               " " + LoggerUtils.getStackTraceForSevereLog(e));
            throw EnvironmentFailureException.unexpectedException(e);
        } finally {
            loopExitCleanup();
        }
    }

    protected void doRunArbiterLoopInternalWork()
       throws Exception {

        final int timeoutMs = repImpl.getConfigManager().
                getDuration(RepParams.REPLICA_TIMEOUT);
        arbiterFeederChannel.setTimeoutMs(timeoutMs);

        requestQueue.clear();
        outputQueue.clear();

        arbiterOutputThread =
                new ArbiterOutputThread(this,
                                        repImpl,
                                        outputQueue,
                                        protocol,
                                        arbiterFeederChannel.getChannel(),
                                        arbiterImpl.getArbiterVLSNTracker());
        arbiterOutputThread.start();

        requestThread = new RequestThread();
        requestThread.start();

        long maxPending = 0;

        try {
            while (true) {
                Message message = protocol.read(arbiterFeederChannel);

                if (arbiterImpl.isShutdownOrInvalid() || (message == null)) {
                    return;
                }

                while (!requestQueue.
                        offer(message,
                              QUEUE_POLL_INTERVAL_NS,
                              TimeUnit.NANOSECONDS)) {
                    /* Offer timed out. */
                    if (!requestThread.isAlive()) {
                        return;
                    }
                    /* Retry the offer */
                    nReplayQueueOverflow.increment();
                }

                final int pending = requestQueue.size();
                if (pending > maxPending) {
                    maxPending = pending;
                    LoggerUtils.fine(logger, repImpl,
                                     "Max pending request log items:" +
                                      maxPending);
                }
            }
        } catch (IOException ioe) {

            /*
             * Make sure messages in the queue are processed. Ensure, in
             * particular, that shutdown requests are processed and not ignored
             * due to the IOEException resulting from a closed connection.
             */
            requestThread.exitRequest = RequestExitType.SOFT;
        } finally {

            if (requestThread.exitRequest == RequestExitType.SOFT) {

                /*
                 * Drain all queued messages, exceptions may be generated
                 * in the process. They logically precede IO exceptions.
                 */
                requestThread.join();
            }

            try {

                if (requestThread.exception != null) {
                    /* request thread is dead or exiting. */
                    throw requestThread.exception;
                }

                if (arbiterOutputThread.getException() != null) {
                    throw arbiterOutputThread.getException();
                }
            } finally {

                /* Ensure thread has exited in all circumstances */
                requestThread.exitRequest = RequestExitType.IMMEDIATE;
                requestThread.join();
                arbiterOutputThread.shutdownThread(logger);
            }
        }
    }

    StatGroup loadStats(StatsConfig config)
        throws DatabaseException {
        masterStat.set(
            arbiterImpl.getMasterStatus().getNodeMasterNameId().toString());
        StatGroup copyStats = stats.cloneGroup(config.getClear());
        return copyStats;
    }

    /**
     * Performs the cleanup actions upon exit from the internal arbiter loop.
     *
     */
    private void loopExitCleanup() {

        if (shutdownException != null) {
            if (shutdownException instanceof ReplicaRetryException) {
                LoggerUtils.fine(logger, repImpl,
                                 "Retrying connection to feeder. Message: " +
                                 shutdownException.getMessage());
            } else if (shutdownException instanceof GroupShutdownException) {
                LoggerUtils.info(logger, repImpl,
                                 "Exiting inner Arbiter loop." +
                                 " Master requested shutdown.");
            } else {
                LoggerUtils.warning
                    (logger, repImpl,
                     "Exiting inner Arbiter loop with exception " +
                     shutdownException + "\n" +
                     LoggerUtils.getStackTrace(shutdownException));
            }
        } else {
            LoggerUtils.fine(logger, repImpl, "Exiting inner Arbiter loop." );
        }

        shutdown();
    }

    /**
     * Returns a channel used by the Arbiter to connect to the Feeder. The
     * socket is configured with a read timeout that's a multiple of the
     * heartbeat interval to help detect, or initiate a change in master.
     *
     * @throws IOException
     * @throws ReplicaConnectRetryException
     */
    private void createArbiterFeederChannel()
        throws IOException, ReplicaConnectRetryException {

        DataChannel dataChannel = null;

        final DbConfigManager configManager = repImpl.getConfigManager();
        final int timeoutMs = configManager.
            getDuration(RepParams.PRE_HEARTBEAT_TIMEOUT);

        try {
            dataChannel =
                repImpl.getChannelFactory().
                connect(arbiterImpl.getMasterStatus().getNodeMaster(),
                        repImpl.getHostAddress(),
                        repImpl.getFeederConnectOptions());

            arbiterFeederChannel =
                new NamedChannelWithTimeout(repImpl,
                                            logger,
                                            arbiterImpl.getChannelTimeoutTask(),
                                            dataChannel,
                                            timeoutMs);

            ServiceDispatcher.doServiceHandshake
                (dataChannel, FeederManager.FEEDER_SERVICE);
        } catch (ConnectException e) {

            /*
             * A network problem, or the node went down between the time we
             * learned it was the master and we tried to connect.
             */
            throw new ReplicaConnectRetryException(e.getMessage(),
                                            NETWORK_RETRIES,
                                            CONNECT_RETRY_SLEEP_MS);
        } catch (ServiceConnectFailedException e) {

            /*
             * The feeder may not have established the Feeder Service
             * as yet. For example, the transition to the master may not have
             * been completed. Wait longer.
             */
           if (e.getResponse() == Response.UNKNOWN_SERVICE) {
               throw new ReplicaConnectRetryException(e.getMessage(),
                                               SERVICE_UNAVAILABLE_RETRIES,
                                               CONNECT_RETRY_SLEEP_MS);
           }
           throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Process a heartbeat message. It queues a response and updates
     * the consistency tracker with the information in the heartbeat.
     *
     * @param xid
     * @throws IOException
     */
    private void queueAck(Long xid)
        throws IOException {
        try {
            outputQueue.put(xid);
        } catch (InterruptedException ie) {

            /*
             * Have the higher levels treat it like an IOE and
             * exit the thread.
             */
            throw new IOException("Ack I/O interrupted", ie);
        }
    }

    /**
     * Process the shutdown message from the master and return the
     * GroupShutdownException that must be thrown to exit the Replica loop.
     *
     * @return the GroupShutdownException
     */
    private GroupShutdownException processShutdown(ShutdownRequest shutdown)
        throws IOException {

        /*
         * Acknowledge the shutdown message right away, since the checkpoint
         * operation can take a long time to complete. Long enough to exceed
         * the feeder timeout on the master. The master only needs to know that
         * the replica has received the message.
         */
        queueAck(ReplicaOutputThreadBase.SHUTDOWN_ACK);

        /*
         * Turn off network timeouts on the replica, since we don't want the
         * replica to timeout the connection. The connection itself is no
         * longer used past this point and will be reclaimed as part of normal
         * replica exit cleanup.
         */
        arbiterFeederChannel.setTimeoutMs(Integer.MAX_VALUE);
        final String masterHostName =
            arbiterImpl.getMasterStatus().getGroupMaster().getHostName();
        return new GroupShutdownException(
            logger,
            repImpl,
            masterHostName,
            arbiterImpl.getArbiterVLSNTracker().get(),
            shutdown.getShutdownTimeMs());
    }

    /**
     * Apply the operation represented by this log entry on this Arbiter node.
     */
    private Message replayEntries(Message firstMessage) throws IOException {
        boolean doSync = false;
        long highVLSN = 0;
        Message shutdownMessage = null;
        groupXact.clear();
        groupMessages.clear();
        groupMessages.add(firstMessage);
        requestQueue.drainTo(groupMessages, N_MAX_GROUP_XACT);
        for (int i = 0;i < groupMessages.size(); i++) {
            final Message message = groupMessages.get(i) ;

            final MessageOp messageOp = message.getOp();

            if (messageOp.getMessageClass() ==
                Protocol.SHUTDOWN_REQUEST_CLASS) {
                shutdownMessage = message;
            } else if (messageOp.getMessageClass() ==
                Protocol.HEARTBEAT_CLASS) {
                groupXact.add(ReplicaOutputThreadBase.HEARTBEAT_ACK);
            } else {
                InputWireRecord wireRecord =
                    ((Protocol.Entry) message).getWireRecord();
                final byte entryType = wireRecord.getEntryType();
                lastReplayedVLSN = wireRecord.getVLSN();

                if (LOG_TXN_COMMIT.equalsType(entryType)) {
                    Protocol.Commit commitEntry = (Protocol.Commit) message;
                    if (commitEntry.getReplicaSyncPolicy() ==
                        SyncPolicy.SYNC) {
                        doSync = true;
                    }

                    LogEntry logEntry = wireRecord.getLogEntry();
                    if (lastReplayedVLSN > highVLSN) {
                        highVLSN = lastReplayedVLSN;
                    }
                    final TxnCommit masterCommit =
                        (TxnCommit) logEntry.getMainItem();

                    long nextDTVLSN = masterCommit.getDTVLSN();
                    masterTerm = masterCommit.getMasterTerm();
                    if (nextDTVLSN == UNINITIALIZED_VLSN) {
                        /* Pre-DTVLSN log commit record. */
                        nextDTVLSN = wireRecord.getVLSN();
                    }
                    /*
                     * The Arbiter, unlike Replicas, does not receive commits
                     * in ascending VLSN order, so discard lower DTVLSNs.
                     */
                    dtvlsn = nextDTVLSN > dtvlsn ? nextDTVLSN : dtvlsn;
                    groupXact.add(logEntry.getTransactionId());
                    nAcks.increment();
                    if (logger.isLoggable(Level.FINEST)) {
                        LoggerUtils.finest(logger, repImpl,
                                           "Arbiter ack commit record " +
                                            wireRecord);
                    }
                } else {
                    String errMsg = "Illegal message type recieved by " +
                            " Arbiter. [" + wireRecord + "]";
                    throw new IllegalStateException(errMsg);
                }
            }
        }

        if (doSync ||
            (lastFSyncTime + FSYNC_INTERVAL) <= TimeSupplier.currentTimeMillis()) {
            doSync = true;
            lastFSyncTime = TimeSupplier.currentTimeMillis();
        }

        arbiterImpl.getArbiterVLSNTracker().write(
            highVLSN,
            dtvlsn,
            arbiterImpl.getMasterStatus().getGroupMasterNameId().getId(),
            masterTerm,
            doSync);
        for (Long element : groupXact) {
            queueAck(element);
        }
        return shutdownMessage;
    }

    class RequestThread extends StoppableThread {

        volatile private Exception exception;

        /*
         * Set asynchronously when a shutdown is being requested.
         */
        volatile RequestExitType exitRequest = null;

        /* The queue poll interval, 1 second */
        private final static long REQUEST_QUEUE_POLL_INTERVAL_NS = 1000000000l;

        protected RequestThread() {
            super(repImpl, "RequestThread");
        }

        @Override
        protected int initiateSoftShutdown() {
           /* Use immediate, since the stream will continue to be read. */
           exitRequest = RequestExitType.IMMEDIATE;
           return 0;
        }

        @Override
        public void run() {

            LoggerUtils.fine(logger, repImpl,
                             "Request thread started. Message queue size:" +
                              requestQueue.remainingCapacity());
            try {
                while (true) {
                    final Message message =
                        requestQueue.poll(REQUEST_QUEUE_POLL_INTERVAL_NS,
                                          TimeUnit.NANOSECONDS);

                    if ((exitRequest == RequestExitType.IMMEDIATE) ||
                        ((exitRequest == RequestExitType.SOFT) &&
                         (message == null)) ||
                         arbiterImpl.isShutdownOrInvalid()) {
                        return;
                    }
                    arbiterImpl.getMasterStatus().assertSync();
                    if (message == null) {
                        /* Timeout on poll. */
                        continue;
                    }
                    Message shutdownMessage = replayEntries(message);
                    if (shutdownMessage != null) {
                        throw processShutdown(
                            (ShutdownRequest) shutdownMessage);
                    }
                }
            } catch (Exception e) {
                exception = e;

                /*
                 * Bring it to the attention of the main thread by freeing
                 * up the "offer" wait right away.
                 */
                requestQueue.clear();

                /*
                 * Get the attention of the main arbiter thread in case it's
                 * waiting in a read on the socket channel.
                 */
                LoggerUtils.fine(logger, repImpl,
                                 "closing arbiterFeederChannel = " +
                                 arbiterFeederChannel);
                RepUtils.shutdownChannel(arbiterFeederChannel);

                LoggerUtils.info(logger, repImpl,
                                 "ArbiterAcker thread exiting with exception:" +
                                  e.getMessage());
            }
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    private class RepFeederHandshakeConfig
        implements ReplicaFeederHandshakeConfig {

        @Override
        public RepImpl getRepImpl() {
            return repImpl;
        }

        @Override
        public NameIdPair getNameIdPair() {
            return arbiterImpl.getNameIdPair();
        }

        @Override
        public Clock getClock() {
            return clock;
        }

        @Override
        public NodeType getNodeType() {
            return NodeType.ARBITER;
        }

        @Override
        public RepGroupImpl getGroup() {
            return arbiterImpl.getGroup();
        }

        @Override
        public NamedChannel getNamedChannel() {
            return arbiterFeederChannel;
        }
    }
}
