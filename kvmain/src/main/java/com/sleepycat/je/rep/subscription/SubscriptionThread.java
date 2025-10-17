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

package com.sleepycat.je.rep.subscription;

import static com.sleepycat.je.rep.impl.node.ReplicaOutputThreadBase.FILTER_CHANGE_REQ;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConnectRetryException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.ChannelTimeoutTask;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.ReplicaOutputThread;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.stream.BaseProtocol;
import com.sleepycat.je.rep.stream.ChangeResultHandler;
import com.sleepycat.je.rep.stream.FeederFilterChange;
import com.sleepycat.je.rep.stream.FeederFilterChangeResult;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshake;
import com.sleepycat.je.rep.stream.ReplicaFeederHandshakeConfig;
import com.sleepycat.je.rep.stream.SubscriberFeederSyncup;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.NamedChannelWithTimeout;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.Response;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Main thread created by Subscription to stream log entries from feeder
 */
public class SubscriptionThread extends StoppableThread {

    /* the stream mode in reconnect */
    static BaseProtocol.EntryRequestType STREAM_MODE_RECONNECT =
        BaseProtocol.EntryRequestType.DEFAULT;

    /* wait time in ms in soft shut down */
    private final static int SOFT_SHUTDOWN_WAIT_MS = 5 * 1000;

    private final Logger logger;
    private final SubscriptionConfig config;
    private final SubscriptionStat stats;

    /* communication queues and working threads */
    private final BlockingQueue<Long> outputQueue;
    private final BlockingQueue<Object> inputQueue;
    private final Queue<FeederFilterChange> changeQueue;

    /* change request map indexed by request id supplied by client */
    private final ConcurrentMap<String, FeederFilterChange> changeMap;

    private SubscriptionProcessMessageThread messageProcThread;

    /* communication channel between subscriber and feeder */
    private NamedChannelWithTimeout namedChannel;
    /* task to register channel with timeout */
    private ChannelTimeoutTask channelTimeoutTask;
    /* protocol used to communicate with feeder */
    private Protocol protocol;

    /* requested VLSN from which to stream log entries */
    private final long reqVLSN;

    /*
     * volatile because it can be concurrently accessed by the subscription
     * thread itself in checkOutputThread(), and another thread trying to
     * shut down subscription by calling shutdown()
     */
    private volatile SubscriptionOutputThread outputThread;

    private volatile SubscriptionStatus status;

    private volatile int masterHeartbeatId = -1;

    /* stored exception */
    private volatile Exception storedException;

    /* change result handler map indexed by request id supplied by client */
    private final ConcurrentMap<String, ChangeResultHandler> handlerMap;

    /*
     * For unit test only. The hook will be called by unit test to inject an
     * exception into msg queue, which to be processed by the callback function
     * defined in unit test.
     */
    private TestHook<SubscriptionThread> exceptionHandlingTestHook;

    /**
     * For unit test only. The hook will be called by unit test to inject
     * simulated failure in handshake.
     */
    static TestHook<String> handshakeFailureTestHook = null;

    /* # of retry in main loop */
    private final AtomicInteger numRetry;

    /* wrapper of envImpl */
    private final RepImpl repImpl;

    SubscriptionThread(ReplicatedEnvironment env,
                       long reqVLSN,
                       SubscriptionConfig config,
                       SubscriptionStat stats) {

        super(RepInternal.getNonNullRepImpl(env),
              "SubscriptionMain-" + config.getSubNodeName());
        setUncaughtExceptionHandler(new SubscriptionThreadExceptionHandler());

        this.reqVLSN = reqVLSN;
        this.config = config;
        this.stats = stats;
        protocol = null;
        namedChannel = null;
        /* init subscription input and output queue */
        inputQueue =
            new ArrayBlockingQueue<>(config.getInputMessageQueueSize());
        outputQueue =
            new ArrayBlockingQueue<>(config.getOutputMessageQueueSize());
        changeQueue = new ConcurrentLinkedQueue<>();
        status = SubscriptionStatus.INIT;
        storedException = null;
        exceptionHandlingTestHook = null;
        handlerMap = new ConcurrentHashMap<>();
        numRetry = new AtomicInteger(0);
        repImpl = (RepImpl) envImpl;
        logger = repImpl.getLogger();
        changeMap = new ConcurrentHashMap<>();
    }

    /**
     * Returns subscription status to client
     *
     * @return subscription status
     */
    public SubscriptionStatus getStatus() {
        return status;
    }

    public int getMasterHeartbeatId() {
        return masterHeartbeatId;
    }

    /**
     * Returns stored exception
     *
     * @return stored exception
     */
    Exception getStoredException() {
        return storedException;
    }

    /**
     * Returns subscription configuration
     * @return subscription configuration
     */
    SubscriptionConfig getConfig() {
        return config;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected int initiateSoftShutdown() {
        return SOFT_SHUTDOWN_WAIT_MS;
    }

    @Override
    public void run() {

        LoggerUtils.info(logger, repImpl,
                         lm("Start subscription from request vlsn=" + reqVLSN +
                            ", configured mode=" + getStreamMode() +
                            ", from feeder=" + config.getFeederHostPort()));

        try {
            final int maxRetry = config.getMaxConnectRetries();
            while (!isShutdown()) {
                try {

                    /* reconnection should resume from left-over vlsn */
                    if (numRetry.get() > 0) {
                        LoggerUtils.logMsg(
                            logger, repImpl, Level.INFO,
                            lm("Subscription to re-connect to " +
                               "feeder=" + config.getFeederHostPort() +
                               ", configured mode=" + config.getStreamMode() +
                               ", reconnect mode=" + getStreamMode() +
                               ", numRetry=" + numRetry.get() +
                               ", maxRetry=" + maxRetry));
                    }

                    initializeConnection();
                    LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                       () -> lm("Create auxiliary msg " +
                                                "processing and output " +
                                                "threads"));

                    if (!createAuxThread()) {
                        /* fail to create helper threads, report error */
                        status = SubscriptionStatus.UNKNOWN_ERROR;
                        break;
                    }

                    /* subscription succeed, start streaming data */
                    status = SubscriptionStatus.SUCCESS;
                    /* stay in the loop till exception thrown or shutdown */
                    loopInternal();

                } catch (ConnectionException e) {
                    if (numRetry.get() == maxRetry) {
                        LoggerUtils.info(logger, repImpl,
                                         lm("Shut down after reaching max " +
                                            "retry=" + maxRetry + " to " +
                                            "connect feeder=" +
                                            config.getFeederHostPort() +
                                            ", error=" + e.getMessage()));
                        LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                           () -> LoggerUtils.getStackTrace(e));
                        storedException = e;
                        status = SubscriptionStatus.CONNECTION_ERROR;
                        break;
                    }
                    numRetry.incrementAndGet();
                    LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                       () -> lm("Fail to connect feeder=" +
                                                config.getFeederHostPort() +
                                                ", sleepMs= " +
                                                e.getRetrySleepMs() +
                                                ", will reconnect"));
                    Thread.sleep(e.getRetrySleepMs());
                } catch (ReplicaConnectRetryException e) {
                    if (numRetry.get() == maxRetry) {
                        LoggerUtils.info(logger, repImpl,
                                         lm("Shut down after reaching max " +
                                            "retry=" + maxRetry + " to " +
                                            "connect feeder=" +
                                            config.getFeederHostPort() +
                                            ", error=" + e.getMessage()));
                        LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                           () -> LoggerUtils.getStackTrace(e));
                        storedException = e;
                        status = SubscriptionStatus.CONNECTION_ERROR;
                        break;
                    }
                    numRetry.incrementAndGet();
                    LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                       () -> lm("Fail to connect feeder=" +
                                                config.getFeederHostPort() +
                                                ", sleepMs= " +
                                                e.getRetrySleepMs() +
                                                ", will reconnect"));
                    Thread.sleep(e.getRetrySleepMs());
                } finally {
                    /* shut down helper threads */
                    shutDownAuxThreads();
                }
            }
        } catch (FilterChangeException fce) {
            storedException = fce;
            LoggerUtils.warning(logger, repImpl,
                                lm("Subscription thread exited due to filter " +
                                   "change failure=" + fce));
            status = SubscriptionStatus.FILTER_CHANGE_ERROR;
        } catch (ReplicationSecurityException ure) {
            storedException = ure;
            LoggerUtils.warning(logger, repImpl,
                                lm("Subscription thread exited due to " +
                                   "security check failure=" + ure));
            status = SubscriptionStatus.SECURITY_CHECK_ERROR;
        } catch (GroupShutdownException e) {
            if (messageProcThread.isAlive()) {
                try {
                    /* let message processing thread finish up */
                    messageProcThread.join();
                } catch (InterruptedException ie) {
                    /* ignore since we will shut down, just log */
                    LoggerUtils.logMsg(
                        logger, repImpl, Level.FINE,
                        () -> lm("Interrupted in shutting down message " +
                                 "processing thread, error=" + ie +
                                 "\n" + LoggerUtils.getStackTrace(ie)));
                }
            }
            storedException = e;
            LoggerUtils.info(logger, repImpl,
                             lm("Received group shutdown, error=" + e +
                                "\n" + LoggerUtils.getStackTrace(e)));
            status = SubscriptionStatus.GRP_SHUTDOWN;
        } catch (InsufficientLogException e) {
            storedException = e;
            LoggerUtils.info(logger, repImpl,
                             lm("Unable to subscribe from requested vlsn=" +
                                reqVLSN + ", error=" + e));
            LoggerUtils.logMsg(logger, envImpl, Level.FINE,
                               () -> LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.VLSN_NOT_AVAILABLE;
        } catch (EnvironmentFailureException e) {
            storedException = e;
            LoggerUtils.warning(logger, repImpl,
                                lm("Unable to sync up with feeder due to" +
                                   "env failure= " + e));
            LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                               () -> LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } catch (InterruptedException e) {
            storedException = e;
            LoggerUtils.warning(logger, repImpl,
                                lm("Interrupted exception=" + e));
            LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                               () -> LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } catch (InternalException e) {
            storedException = e;
            final Throwable cause = e.getCause();
            LoggerUtils.warning(logger, repImpl,
                                lm("Internal exception=" + e +
                                   ", cause=" + cause));
            LoggerUtils.logMsg(logger, envImpl, Level.FINE,
                               () -> LoggerUtils.getStackTrace(e));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } catch (RuntimeException exp) {
            LoggerUtils.warning(logger, repImpl,
                                lm("Unexpected exception=" + exp + "\n" +
                                   LoggerUtils.getStackTrace(exp)));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } catch (Error err) {
            LoggerUtils.warning(logger, repImpl,
                                lm("subscription thread error=" + err + "\n" +
                                   LoggerUtils.getStackTrace(err)));
            status = SubscriptionStatus.UNKNOWN_ERROR;
        } finally {
            shutdown();
        }
    }

    /**
     * Queue the filter change
     *
     * @param change filter change
     * @param handler change result handler
     *
     * @throws IOException if fail to queue the msg
     */
    synchronized void changeFilter(FeederFilterChange change,
                                   ChangeResultHandler handler)
        throws IOException {

        if (change == null) {
            throw new IllegalArgumentException("Filter change cannot be null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("Change result handler cannot " +
                                               "be null");
        }
        if (handlerMap.containsKey(change.getReqId())) {
            /* req id should be unique and duplicate request is not allowed */
            final String msg = "A change result handler with " +
                               "request id " + change.getReqId() + " is " +
                               "already registered.";
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new IllegalArgumentException(msg);
        }

        if (changeMap.containsKey(change.getReqId())) {
            final String msg = "Change request id " + change.getReqId() +
                               " already exists, change:" + change + ", " +
                               "existing change: " +
                               changeMap.get(change.getReqId());
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new IllegalStateException(msg);
        }

        /* set callback */
        handlerMap.put(change.getReqId(), handler);
        /* queue the change */
        changeQueue.add(change);

        /* remember pending changes */
        changeMap.put(change.getReqId(), change);

        /* notify output thread to flush changes in queue */
        queueAck(FILTER_CHANGE_REQ);
    }

    /**
     * Unit test only
     */
    int getNumRetry() {
        return numRetry.get();
    }

    /**
     * Unit test only
     */
    NamedChannel getNamedChannel() {
        return namedChannel;
    }

    /**
     * For unit test
     *
     * @param exceptionHandlingTestHook test hook
     */
    void setExceptionHandlingTestHook(
        TestHook<SubscriptionThread> exceptionHandlingTestHook) {
        this.exceptionHandlingTestHook = exceptionHandlingTestHook;
    }

    /**
     * shutdown the subscriber and all auxiliary threads, close channel to
     * the Feeder.
     */
    void shutdown() {

        /* Note start of shutdown and return if already requested */
        if (shutdownDone(logger)) {
            return;
        }

        if (Thread.currentThread() == this) {
            /* shutdown by thread itself */
            LoggerUtils.info(logger, repImpl,
                             lm("Thread starts shutting itself down"));
        }

        inputQueue.clear();
        outputQueue.clear();

        shutdownThread(logger);

        LoggerUtils.info(logger, repImpl,
                         lm("Subscription thread shut down with status=" +
                            status));

        /* finally we shut down channel to feeder */
        RepUtils.shutdownChannel(namedChannel);
        if (channelTimeoutTask != null) {
            channelTimeoutTask.cancel();
        }
        LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                           () -> lm("Channel to feeder=" +
                                    config.getFeederHostPort() +
                                    " has shut down."));
    }

    /**
     * Enqueue message received from feeder into input queue
     *
     * @param message  message received from feeder
     *
     * @throws InterruptedException if enqueue is interrupted
     * @throws InternalException if consumer thread is gone unexpectedly
     */
    void offer(Object message)
        throws InterruptedException, InternalException {

        /* Don't enqueue msg if thread is shutdown */
        if (isShutdown()) {
            return;
        }

        RepImpl repImpl = (RepImpl)envImpl;

        while (!inputQueue.offer(message,
                                 SubscriptionConfig.QUEUE_POLL_INTERVAL_MS,
                                 TimeUnit.MILLISECONDS)) {
            /*
             * Offer timed out.
             *
             * There are three cases:
             *
             * Case 1: This thread was shutdown (shutdown() is called) while
             * waiting to add to the queue.  Regardless of the state of the msg
             * proc thread, we just return and let caller capture the shutdown
             * signal and exit;
             *
             * Case 2: The msg proc thread is dead for some reason, this is an
             * exception we throw IE and let caller to capture this IE and
             * exit;
             *
             * Case 3: The msg proc thread is alive, try again.
             */

            /* Case 1 */
            if (isShutdown()) {
                return;
            }

            if (!messageProcThread.isAlive()) {
                /* Case 2 */
                final String err = "Thread consuming input queue is gone, " +
                                   "start shutdown process";
                LoggerUtils.warning(logger, repImpl, lm(err));
                throw new InternalException(err);
            }
            /* Case 3: count the overflow and retry */
            stats.getNumReplayQueueOverflow().increment();
        }
    }

    /**
     * For unit test only
     */
    ConcurrentMap<String, ChangeResultHandler> getHandleMap() {
        return handlerMap;
    }

    /**
     * Create connection to feeder and execute handshake
     *
     * @throws InternalException if unable to connect to source node due to
     * protocol error
     * @throws EnvironmentFailureException if fail to handshake with source, or
     * source does not have enough log to start streaming
     * @throws ConnectionException if unable to connect to source node
     * @throws ReplicationSecurityException if authentication failure
     */
    private void initializeConnection() throws InternalException,
        EnvironmentFailureException, ConnectionException,
        ReplicationSecurityException, ReplicaConnectRetryException {

        /* open a channel to feeder */
        LoggerUtils.logMsg(
            logger, repImpl, Level.FINE,
            () -> lm("Subscription=" + config.getSubNodeName() +
                     " start open channel and handshake with feeder"));

        /*
         * null out ID to allow the feeder connection to assign
         * it a new one
         */
        ((RepImpl) envImpl).getNameIdPair().revertToNull();

        try {

            openChannel();
            ReplicaFeederHandshake handshake =
                new ReplicaFeederHandshake(new SubFeederHandshakeConfig
                                               (config.getNodeType()));

            protocol = handshake.execute();

            /* check if negotiated protocol version is high enough */
            final int minReqVersion = config.getMinProtocolVersion();
            if (protocol.getVersion() < minReqVersion) {
                throw new BinaryProtocol.ProtocolException(
                    "HA protocol version (" + protocol.getVersion() + ") is " +
                    "lower than minimal required version (" + minReqVersion +
                    ")");
            }

            LoggerUtils.logMsg(
                logger, repImpl, Level.FINE,
                () -> lm("Subscription=" + config.getSubNodeName() +
                         " sync-up with feeder at vlsn=" + reqVLSN));
            SubscriberFeederSyncup syncup =
                new SubscriberFeederSyncup(namedChannel, protocol,
                                           config.getFeederFilter(),
                                           (RepImpl) envImpl,
                                           getStreamMode(),
                                           config.getPartGenDBName(),
                                           logger);
            /*
             * if the first connection or nothing is received, use
             * the original requested vlsn, otherwise use the last received
             * vlsn in stats
             */
            final long requestedVLSN;
            if (numRetry.get() == 0 ||
                stats.getHighVLSN() == NULL_VLSN ||
                stats.getHighVLSN() == INVALID_VLSN) {
                requestedVLSN = reqVLSN;
            } else {
                /*
                 * JE subscriber streams from feeder and delivers operations
                 * to a FIFO queue for downstream processing, which will be
                 * eventually be processed by NoSQLSubscriber.onNext() in KV.
                 * In reconnection at JE subscriber upon ConnectionException,
                 * the streamed ops in the FIFO queue will not be lost, and
                 * the reconnection is transparent to downstream processing,
                 * therefore it is enough to resume from the last delivered
                 * VLSN.
                 */
                requestedVLSN = stats.getHighVLSN();
            }

            final long startVLSN = syncup.execute(requestedVLSN);
            /* after sync-up the part generation db id is ready */
            stats.setPartGenDBId(syncup.getPartGenDBId());
            LoggerUtils.logMsg(
                logger, repImpl, Level.FINE,
                () -> lm("Sync-up with feeder done, start vlsn=" +
                         startVLSN + ", partition generation db id=" +
                         stats.getPartGenDBId() + " for given db name=" +
                         config.getPartGenDBName()));

            if (startVLSN != NULL_VLSN) {

                final BinaryProtocol.Message msg = protocol.read(namedChannel);
                final BinaryProtocol.MessageOp op = msg.getOp();

                if (op.getMessageClass() == Protocol.HEARTBEAT_CLASS) {
                    /* normally, a heartbeat */
                    stats.setStartVLSN(startVLSN);
                    setStatsFromHeartbeat(msg);
                    queueAck(ReplicaOutputThread.HEARTBEAT_ACK);

                    LoggerUtils.info(logger, repImpl,
                                     lm("Subscription=" +
                                        config.getSubNodeName() +
                                        ", configured mode=" +
                                        config.getStreamMode() +
                                        ", connect mode=" + getStreamMode() +
                                        " successfully connect to feeder=" +
                                        config.getFeederHostPort() +
                                        ", original reqVLSN=" + reqVLSN +
                                        (numRetry.get() == 0 ? "" :
                                            ", reqVLSN=" + requestedVLSN +
                                            " in numTry=" + numRetry) +
                                        ", start vlsn=" + startVLSN));

                    return;
                } else if (op.getMessageClass() ==
                    Protocol.SECURITY_FAILURE_RESPONSE_CLASS) {
                    /* feeder fails security check in syncup */
                    final Protocol.SecurityFailureResponse resp =
                        (Protocol.SecurityFailureResponse) msg;

                    LoggerUtils.warning(logger, repImpl,
                                        lm("Receiving security check " +
                                           "failure message from feeder=" +
                                           config.getFeederHostPort() +
                                           ", message=" + resp.getMessage()));

                    throw new ReplicationSecurityException(
                        resp.getMessage(), config.getSubNodeName(), null);
                }

                /* unexpected message */
                throw new BinaryProtocol.ProtocolException(
                    msg, Protocol.Heartbeat.class);

            } else {
                throw new InsufficientLogException((RepImpl) envImpl, reqVLSN);
            }
        } catch (IOException e) {
            throw new ConnectionException("Unable to connect due to " +
                                          e.getMessage() +
                                          ",  will retry later.",
                                          config.getSleepBeforeRetryMs(),
                                          e);
        } catch (EnvironmentFailureException e) {
            LoggerUtils.warning(logger, repImpl,
                                lm("Fail to handshake with feeder: " +
                                   e.getMessage()));
            throw e;
        } catch (BinaryProtocol.ProtocolException e) {
            final String msg = ("Unable to connect to feeder=" +
                                config.getFeederHostPort() +
                                " due to error=" + e);
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new InternalException(msg, e);
        }
    }

    /**
     * Sets stats from heartbeat message
     * @param msg message
     */
    private void setStatsFromHeartbeat(BinaryProtocol.Message msg) {
        final BaseProtocol.Heartbeat hb = ((BaseProtocol.Heartbeat) msg);
        stats.setLastFilterVLSN(hb.getLastFilterVLSN());
        stats.setLastPassVLSN(hb.getLastPassVLSN());
        stats.setLastModTimeMs(hb.getLastModTimeMs());
        stats.setLastCommitTimeMs(hb.getLastCommitTimeMs());
        masterHeartbeatId = hb.getHeartbeatId();
    }

    /**
     * Create auxiliary message processing and output thread
     */
    private boolean createAuxThread() {

        inputQueue.clear();
        outputQueue.clear();

        /* start output thread over data channel to send response to feeder */
        outputThread =
                new SubscriptionOutputThread(this,
                                             repImpl, outputQueue, protocol,
                                             namedChannel.getChannel(),
                                             config.getAuthenticator(),
                                             changeQueue, stats);
        /*
         * output thread can be shutdown and set to null anytime, thus
         * use a cached copy to ensure it is alive before start it
         */
        final SubscriptionOutputThread cachedOutputThread = outputThread;
        if (cachedOutputThread != null) {
            cachedOutputThread.start();
            LoggerUtils.logMsg(
                logger, repImpl, Level.FINE,
                () -> lm("Output thread created for subscription=" +
                         config.getSubNodeName()));
            /* start thread to consume data in input queue */
            messageProcThread =
                new SubscriptionProcessMessageThread(repImpl, inputQueue,
                                                     config, stats);
            messageProcThread.start();
            LoggerUtils.logMsg(
                logger, repImpl, Level.FINE,
                () -> lm("message processing thread created for " +
                         "subscription " + config.getSubNodeName()));
            return true;
        } else {
            LoggerUtils.info(logger, repImpl,
                             lm("Subscription=" +  config.getSubNodeName() +
                               " just shut down, no need to create auxiliary " +
                                "threads"));
            return false;
        }
    }

    /**
     * Open a data channel to feeder
     *
     * @throws ConnectionException unable to connect due to error and need retry
     * @throws InternalException fail to handshake with feeder
     * @throws ReplicationSecurityException if unauthorized to stream
     * from feeder
     */
    private void openChannel() throws ConnectionException,
        InternalException, ReplicationSecurityException {

        if (repImpl == null) {
            throw new IllegalStateException("Replication env is unavailable.");
        }

        try {
            DataChannelFactory.ConnectOptions connectOpts =
                new DataChannelFactory
                    .ConnectOptions()
                    .setTcpNoDelay(config.TCP_NO_DELAY)
                    .setReceiveBufferSize(config.getReceiveBufferSize())
                    .setOpenTimeout((int) config
                        .getStreamOpenTimeout(TimeUnit.MILLISECONDS))
                    .setBlocking(config.BLOCKING_MODE_CHANNEL);

            InetSocketAddress localAddr = repImpl.getHostAddress();
            if (localAddr.getHostName().equals(
                SubscriptionConfig.ANY_ADDRESS.getHostName())) {
                localAddr = SubscriptionConfig.ANY_ADDRESS;
            }
            if (logger.isLoggable(Level.FINE)) {
                LoggerUtils.fine(logger, repImpl,
                                 lm("Connect to addr=" +
                                    config.getInetSocketAddress() +
                                    " from local address " + localAddr +
                                    " with connect option " + connectOpts));
            }
            final DataChannel channel =
                repImpl.getChannelFactory()
                       .connect(config.getInetSocketAddress(),
                                localAddr,
                                connectOpts);
            assert TestHookExecute.doExpHookIfSet(handshakeFailureTestHook);
            ServiceDispatcher.doServiceHandshake(channel,
                                                 FeederManager.FEEDER_SERVICE,
                                                 config.getAuthInfo());
            if (logger.isLoggable(Level.FINE)) {
                LoggerUtils.fine(logger, repImpl,
                                 lm("Channel opened to service=" +
                                    FeederManager.FEEDER_SERVICE +
                                    " at feeder=" + config.getFeederHost() +
                                    "[address=" + config.getFeederHostAddr() +
                                    " port=" + config.getFeederPort() + "]"));
            }
            final int timeoutMs = repImpl.getConfigManager().
                getDuration(RepParams.PRE_HEARTBEAT_TIMEOUT);

            channelTimeoutTask = new ChannelTimeoutTask(new Timer(true));
            namedChannel =
                new NamedChannelWithTimeout(repImpl, logger, channelTimeoutTask,
                                            channel, timeoutMs);
        } catch (IOException cause) {
            /* retry if unable to connect to feeder */
            throw new ConnectionException("Fail to open channel to feeder " +
                                          "due to " + cause.getMessage() +
                                          ", will retry later",
                                          config.getSleepBeforeRetryMs(),
                                          cause);
        } catch (ServiceDispatcher.ServiceConnectFailedException se) {
            /*
             * The feeder may not have established the Feeder Service
             * as yet. For example, the transition to the master may not have
             * been completed.
             */
            Response response = se.getResponse();
            if (response == ServiceDispatcher.Response.UNKNOWN_SERVICE) {
                final long ts = config.getSleepBeforeRetryMs();
                final String msg = "Fail to open channel to node=" +
                                   config.getFeederHostPort() +
                                   ", response=" + response.name() +
                                   ", service exception=" + se +
                                   ", will retry after waiting for time" +
                                   "(ms)=" + ts;
                LoggerUtils.info(logger, repImpl, lm(msg));
                throw new ConnectionException(msg, ts, se);
            }

            /*
             * No retry since INVALID response is returned only on security
             * check failure
             */
            if (response == ServiceDispatcher.Response.INVALID) {
                final String msg = "Fail to open channel to node=" +
                                   config.getFeederHostPort() +
                                   ", fail to authenticate" +
                                   ", dispatcher response=" + response.name() +
                                   ", service exception=" + se;
                LoggerUtils.info(logger, repImpl, lm(msg));
                throw new ReplicationSecurityException(
                    msg, config.getSubNodeName(), se);
            }

            final String msg = "Subscription=" + config.getSubNodeName() +
                               "failed to handshake for service=" +
                               FeederManager.FEEDER_SERVICE + " with node=" +
                               config.getFeederHostPort() +
                               ", response=" + response;
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new InternalException(msg);

        } catch (Exception exp) {
            /* wrap and surface all other exceptions */
            final String msg = "Subscription=" + config.getSubNodeName() +
                               "failed to handshake for service=" +
                               FeederManager.FEEDER_SERVICE + " with node=" +
                               config.getFeederHostPort() +
                               ", error=" + exp;
            LoggerUtils.warning(logger, repImpl, lm(msg));
            throw new InternalException(msg);
        }

        LoggerUtils.info(logger, repImpl,
                         lm("Subscription=" + config.getSubNodeName() +
                            ", configured mode=" + config.getStreamMode() +
                            ", connect mode=" + getStreamMode() +
                            " has successfully created a channel to node=" +
                            config.getFeederHostPort()));

    }

    /**
     * Internal loop to dequeue message from channel to the feeder,
     * process shutdown, heartbeat messages and filter change response
     * messages, and relay data operations to the input queue to be consumed
     * by input thread.
     *
     * @throws InternalException if error in reading messages from channel or
     *                           enqueue message into input queue
     * @throws GroupShutdownException if receive shutdown message from feeder
     * @throws ReplicationSecurityException if output thread exits due to
     * security check failure. In this case the main subscription need to
     * exit without retry.
     * @throws FilterChangeException if fail to change the filter. In this case
     * the main subscription need to exit without retry.
     */
    private void loopInternal() throws InternalException,
        GroupShutdownException, ReplicationSecurityException,
        FilterChangeException {

        RepImpl repImpl = (RepImpl)envImpl;
        /* set the logger for filter for local change */
        config.getFeederFilter().setLogger(logger);
        try {

            LoggerUtils.info(logger, repImpl,
                             lm("Start reading messages from feeder=" +
                                config.getFeederHostPort()));
            while (!isShutdown()) {

                checkOutputThread();

                BinaryProtocol.Message message = protocol.read(namedChannel);
                if ((message == null)) {
                    LoggerUtils.info(logger, repImpl,
                                     lm("Subscription=" +
                                        config.getSubNodeName() +
                                        " has nothing stream, exit loop."));
                    return;
                }

                assert TestHookExecute.doHookIfSet(exceptionHandlingTestHook,
                                                   this);
                assert TestHookExecute.doIOHookIfSet(exceptionHandlingTestHook);

                stats.getNumMsgReceived().increment();

                BinaryProtocol.MessageOp messageOp = message.getOp();
                if (messageOp.getMessageClass() == Protocol.HEARTBEAT_CLASS) {
                    LoggerUtils.logMsg(logger, repImpl, Level.FINEST,
                                       () -> lm("Receive heartbeat from=" +
                                                namedChannel.getNameIdPair()));
                    setStatsFromHeartbeat(message);
                    queueAck(ReplicaOutputThread.HEARTBEAT_ACK);
                } else if (messageOp.getMessageClass() ==
                    Protocol.FILTER_CHANGE_RESPONSE_CLASS) {
                    final Protocol.FilterChangeResp resp =
                        (Protocol.FilterChangeResp) message;
                    final FeederFilterChangeResult result = resp.getResult();
                    final String reqId = result.getReqId();
                    final ChangeResultHandler rh = handlerMap.remove(reqId);
                    /* callback to notify caller that result is available */
                    if (rh == null) {
                        final String err = "Change result handler does not " +
                                           "exist for change request=" + reqId;
                        LoggerUtils.warning(logger, envImpl, lm(err));
                        throw new InternalException(err);
                    }

                    rh.onResult(result);
                    if (result.getStatus().equals(
                        FeederFilterChangeResult.Status.OK)) {
                        /*
                         * Apply the feeder filter change to the local filter
                         * for reconnection.
                         */
                        final FeederFilterChange changeReq = changeMap.
                            remove(reqId);
                        if (changeReq == null) {
                            final String err = "Change request does not " +
                                "exist for reqId=" + reqId;
                            LoggerUtils.warning(logger, envImpl, lm(err));
                            throw new InternalException(err);
                        }
                        final FeederFilterChangeResult localResult =
                            config.getFeederFilter().applyChange(changeReq,
                                                                 repImpl);
                        if (!localResult.getStatus().
                            equals(FeederFilterChangeResult.Status.OK)) {
                            /*
                             * Failed to apply the change to the local filter,
                             * though the change succeeded at the feeder.
                             */
                            final String err = "Failed to apply filter " +
                                "change with id=" + reqId + "to the local " +
                                "feeder filter=" + config.getFeederFilter();
                            LoggerUtils.warning(logger, envImpl, lm(err));
                            throw new FilterChangeException(
                                result.getReqId(), err);
                        }

                        LoggerUtils.info(logger, envImpl,
                                         lm("Change applied at feeder=" +
                                            namedChannel.getNameIdPair() +
                                            ", result=" + result));
                    } else if (result.getStatus().equals(
                        FeederFilterChangeResult.Status.NOT_APPLICABLE)) {
                        LoggerUtils.info(logger, envImpl,
                                         lm("Not applicable change at feeder=" +
                                            namedChannel.getNameIdPair() +
                                            ", result=" + result));
                    } else if (result.getStatus().equals(
                        FeederFilterChangeResult.Status.FAIL)) {
                        final String err = "Fail to apply filter change" +
                                           " with reqId=" + result.getReqId() +
                                           " at feeder=" +
                                           namedChannel.getNameIdPair() +
                                           ", error=" + result.getError();
                        LoggerUtils.warning(logger, envImpl, lm(err));
                        /*
                         * terminate subscription due to unknown state of
                         * server-side filter
                         */
                        throw new FilterChangeException(result.getReqId(), err);
                    } else {
                        final String err =
                            "Unsupported status of change result" +
                            " with reqId=" + result.getReqId() +
                            " at feeder=" + namedChannel.getNameIdPair() +
                            ", result=" + result;
                        LoggerUtils.warning(logger, envImpl, lm(err));
                        throw new FilterChangeException(result.getReqId(), err);
                    }
                } else if (messageOp.getMessageClass() ==
                    Protocol.SECURITY_FAILURE_RESPONSE_CLASS) {
                    final Protocol.SecurityFailureResponse resp =
                        (Protocol.SecurityFailureResponse) message;
                    LoggerUtils.logMsg(
                        logger, envImpl, Level.FINE,
                        () -> lm("Receiving security check " +
                                 "failure message from feeder=" +
                                 config.getFeederHostPort() +
                                 ", message=" + resp.getMessage()));

                    throw new ReplicationSecurityException(
                        resp.getMessage(), config.getSubNodeName(), null);

                } else if (messageOp.getMessageClass() ==
                    Protocol.SHUTDOWN_REQUEST_CLASS) {

                    LoggerUtils.info(logger, envImpl,
                                     lm("Receive shutdown request from " +
                                        "feeder=" + config.getFeederHostPort() +
                                        ", shutdown subscriber"));

                    /*
                     * create a shutdown request, make it in the queue so
                     * client is able to see that in callback, and throw an
                     * exception.
                     *
                     * The message processing thread will exit when seeing a
                     * GroupShutdownException
                     */
                    Protocol.ShutdownRequest req =
                        (Protocol.ShutdownRequest) message;
                    Exception exp =
                        new GroupShutdownException(logger, repImpl,
                                                   config.getFeederHost(),
                                                   stats.getHighVLSN(),
                                                   req.getShutdownTimeMs());
                    offer(exp);
                    throw exp;
                } else {
                    /* a regular data entry message */
                    offer(message);

                    final long pending = inputQueue.size();
                    if (pending > stats.getMaxPendingInput().get()) {
                        stats.getMaxPendingInput().set(pending);
                        LoggerUtils.logMsg(logger, envImpl, Level.FINEST,
                                           () -> lm("Max pending request log" +
                                                    " items=" + pending));
                    }
                }

            }
        } catch (IOException e) {
            /* first check security exception in output thread */
            checkOutputThread();

            /*
             * connection to feeder dropped, wrap with ConnectionException
             * and the caller run() method will capture it and re-connect if
             * necessary
             */
            LoggerUtils.info(logger, envImpl,
                             lm("Connection to feeder=" +
                                config.getFeederHostPort() +
                                " dropped with error=" + e));
            throw new ConnectionException("Unable to connect, error= " +
                                          e.getMessage() +
                                          ", will retry later.",
                                          config.getSleepBeforeRetryMs(),
                                          e);
        } catch (GroupShutdownException | ReplicationSecurityException |
            FilterChangeException exp) {
            /* throw to caller, let caller deal with it */
            throw exp;
        } catch (Exception e) {
            /* first check security exception in output thread */
            checkOutputThread();

            final String err = "Consumer loop has to exit, error=" + e;
            LoggerUtils.warning(logger, envImpl,
                                lm(err) + ", dump stack\n" +
                                LoggerUtils.getStackTrace(e));
            /* other exception is thrown as IE */
            throw new InternalException(err, e);
        }
    }

    /**
     * Checks status of output thread and propagates RSE to main
     * loop. If output thread exited due to RSE, the main thread need to
     * capture it to set the subscription status correctly. For other
     * exceptions, output thread uses the traditional mechanism to notify the
     * main subscription thread: simply shut down channel.
     */
    private void checkOutputThread()
        throws InternalException, ReplicationSecurityException {

        /*
         * output thread can be shutdown and set to null anytime, thus
         * use a cached copy to avoid NPE after the first check
         */
        final SubscriptionOutputThread cachedOutputThread = outputThread;

        /* output thread already gone */
        if (cachedOutputThread == null) {
            /*
             * if output thread is set to null only when subscription thread
             * shut down. If we reach here, it means the subscription thread
             * is shut down right after isShutdown check in loopInternal().
             * We simply return and subscription thread would detect the shut
             * down in next check of isShutdown in loopInternal().
             */
            LoggerUtils.logMsg(logger, envImpl, Level.FINE,
                               () -> lm("Output thread no longer exists"));
            return;
        }

        final Exception exp = cachedOutputThread.getException();
        if (exp instanceof ReplicationSecurityException) {
            final ReplicationSecurityException rse =
                (ReplicationSecurityException) exp;
            LoggerUtils.warning(logger, envImpl,
                                lm("Output thread exited due to security " +
                                   "check failure=" + rse));
            throw rse;
        }
    }

    /**
     * Enqueue an ack message in output queue
     *
     * @param xid txn id to enqueue
     *
     * @throws IOException if fail to queue the msg
     */
    private void queueAck(Long xid) throws IOException {

        try {
            outputQueue.put(xid);
        } catch (InterruptedException ie) {

            /*
             * If interrupted while waiting, have the higher levels treat
             * it like an IOE and exit the thread.
             */
            throw new IOException("Ack I/O interrupted", ie);
        }
    }

    /*-----------------------------------*/
    /*-         Inner Classes           -*/
    /*-----------------------------------*/

    /**
     * Subscriber-Feeder handshake config
     */
    private class SubFeederHandshakeConfig
            implements ReplicaFeederHandshakeConfig {

        private final NodeType nodeType;
        private final RepImpl repImpl;
        SubFeederHandshakeConfig(NodeType nodeType) {
            this.nodeType = nodeType;
            repImpl = (RepImpl)envImpl;
        }

        @Override
        public RepImpl getRepImpl() {
            return repImpl;
        }

        @Override
        public NameIdPair getNameIdPair() {
            return getRepImpl().getNameIdPair();
        }

        @Override
        public RepUtils.Clock getClock() {
            return new RepUtils.Clock(RepImpl.getClockSkewMs());
        }

        @Override
        public NodeType getNodeType() {
            return nodeType;
        }

        @Override
        public NamedChannel getNamedChannel() {
            return namedChannel;
        }

        /* create a group impl from group name and group uuid */
        @Override
        public RepGroupImpl getGroup() {

            RepGroupImpl repGroupImpl = new RepGroupImpl(
                    config.getGroupName(),
                    true, /* unknown group uuid */
                    repImpl.getCurrentJEVersion());

            /* use uuid if specified, otherwise unknown uuid will be used */
            if (config.getGroupUUID() != null) {
                repGroupImpl.setUUID(config.getGroupUUID());
            }
            return repGroupImpl;
        }
    }

    /**
     * Thrown to indicate that the Subscriber must retry connecting to the same
     * master, after some period of time.
     */
    private static class ConnectionException extends RuntimeException {

        private final long retrySleepMs;

        ConnectionException(String message,
                            long retrySleepMs,
                            Throwable cause) {
            super(message, cause);
            this.retrySleepMs = retrySleepMs;
        }

        /**
         * Get thread sleep time before retry
         *
         * @return sleep time in ms
         */
        long getRetrySleepMs() {
            return retrySleepMs;
        }

        @Override
        public String getMessage() {
            final String str = super.getMessage();
            return ((str == null || str.isEmpty()) ?
                   "Fail to connect" : str) + ", will retry after " +
                   "sleeping(ms)=" + retrySleepMs;
        }
    }

    public static RuntimeException makeTestConnectionException() {
        return new ConnectionException("test", 123, null);
    }

    /**
     * Handle exceptions uncaught in SubscriptionThread
     */
    private class SubscriptionThreadExceptionHandler
        implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LoggerUtils.severe(logger, repImpl,
                               lm("Error=" + e + " in SubscriptionThread {" +
                                  t + " } was uncaught.\n" +
                                  LoggerUtils.getStackTraceForSevereLog(e)));
        }
    }

    private void shutDownAuxThreads() {
        /* shutdown aux threads */
        if (messageProcThread != null) {
            try {
                messageProcThread.shutdownThread(logger);
                LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                   () -> lm("Message processing thread has " +
                                            "shut down."));
            } catch (Exception e) {
                /* Ignore so shutdown can continue */
                LoggerUtils.warning(logger, repImpl,
                                    lm("Error in shutdown msg processing " +
                                       "thread, error=" + e + ", continue " +
                                       "shutdown the subscription thread."));
            } finally {
                messageProcThread = null;
            }
        }
        if (outputThread != null) {
            try {
                outputThread.shutdownThread(logger);
                LoggerUtils.logMsg(logger, repImpl, Level.FINE,
                                   () -> lm("Output thread has shut down."));

            } catch (Exception e) {
                /* Ignore we will clean up via killing IO channel anyway. */
                LoggerUtils.warning(logger, repImpl,
                                    lm("Error in shutdown output thread, " +
                                       "error=" + e + ", continue shutdown " +
                                       "subscription thread."));
            } finally {
                outputThread = null;
            }
        }
    }

    BaseProtocol.EntryRequestType getStreamMode() {
        if (numRetry.get() == 0) {
            /* first connect using configured mode */
            return config.getStreamMode();
        }
        /* reconnect, using default mode to resume from requested VLSN */
        return STREAM_MODE_RECONNECT;
    }

    private String lm(String msg) {
        return "[SubscriptionThread-" + config.getSubNodeName() + "] " + msg;
    }

	public StatGroup getProtocolStats(StatsConfig sConfig) {
		StatGroup protocolStats =
                new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                              BinaryProtocolStatDefinition.GROUP_DESC);
		if (protocol != null) {
			protocolStats.addAll(protocol.getStats(sConfig));
		}
		return protocolStats;
	}
}

