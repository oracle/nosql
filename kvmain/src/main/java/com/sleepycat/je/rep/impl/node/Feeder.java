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

package com.sleepycat.je.rep.impl.node;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.log.ChecksumException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationSecurityException;
import com.sleepycat.je.rep.SyncUpFailedException;
import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.MasterTransfer.VLSNProgress;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.ArbiterFeederSource;
import com.sleepycat.je.rep.stream.BaseProtocol.Ack;
import com.sleepycat.je.rep.stream.BaseProtocol.Commit;
import com.sleepycat.je.rep.stream.BaseProtocol.GroupAck;
import com.sleepycat.je.rep.stream.BaseProtocol.HeartbeatResponse;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.FeederFilterChangeResult;
import com.sleepycat.je.rep.stream.FeederReplicaHandshake;
import com.sleepycat.je.rep.stream.FeederReplicaSyncup;
import com.sleepycat.je.rep.stream.FeederReplicaSyncup.NetworkRestoreException;
import com.sleepycat.je.rep.stream.FeederSource;
import com.sleepycat.je.rep.stream.MasterFeederSource;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.rep.stream.OutputWireRecord;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.rep.utilint.NamedChannel;
import com.sleepycat.je.rep.utilint.NamedChannelWithTimeout;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.AtomicLongComponent;
import com.sleepycat.je.utilint.LatencyPercentile;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvg;
import com.sleepycat.je.utilint.LongAvgRate;
import com.sleepycat.je.utilint.LongAvgRateStat;
import com.sleepycat.je.utilint.LongDiffStat;
import com.sleepycat.je.utilint.LongMax;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * There is an instance of a Feeder for each client that needs a replication
 * stream. Either a master, or replica (providing feeder services) may
 * establish a feeder.
 *
 * A feeder is created in response to a request from a Replica, and is shutdown
 * either upon loss of connectivity, or upon a change in mastership.
 *
 * The protocol used to validate and negotiate a connection is synchronous, but
 * once this phase has been completed, the communication between the feeder and
 * replica is asynchronous. To handle the async communications, the feeder has
 * three threads associated with it:
 *
 * 1) A log output thread whose sole purpose is to pump log records
 * down to the replica as fast as the network will allow it.
 *
 * 2) A heartbeat thread that sends heartbeats to the replica at regular
 * intervals.
 *
 * 3) An input thread that listens for responses to transaction commits and
 * heart beat responses.
 *
 * <p>The feeder maintains several statistics that provide information about
 * the replication rate for each replica.  By comparing this information to
 * information about master replication maintained by the FeederTxns class, it
 * is also possible to estimate the lag between replicas and the master.
 *
 * <p>The statistics facilities do not expect the set of available statistics
 * to change dynamically.  To handle recording statistics about the changing
 * set of replicas, the statistics are represented as maps that associated node
 * names with statistics.  Each feeder adds individual statistics in these maps
 * at startup, and removes them at shutdown time to make sure that the
 * statistics in the map only reflect up-to-date information.
 *
 * <p>Some notes about the specific statistics:<dl>
 *
 * <dt>replicaDelay
 *
 * <dd>The difference between the commit times of the latest transaction
 * committed on the master and the transaction most recently processed by the
 * replica. The master timestamp comes from the lastHeartbeatCommitTimestamp
 * statistic maintained by Feeder. The feeder determines the commit timestamp
 * of the replica's most recently processed transaction by obtaining timestamps
 * from commit records being sent to the replica, and noting the last one prior
 * to sending a heartbeat. When a heartbeat response is received, if the latest
 * replica VLSN included in the response is equal or greater to the one
 * recorded when the heartbeat request was sent, then the delay is computed by
 * comparing the commit timestamp for that most recently sent transaction with
 * the timestamp of receiving the heartbeat response. Replicas can send
 * heartbeat responses on their own, which are marked with a unique heartbeat
 * ID of -1 so that they can be ignored when looking for a response that
 * matches a request. Computing the delay by matching requests and responses
 * depends on the fact that the replica processes transactions and heartbeats
 * in order, and only sends a heartbeat response once all preceding
 * transactions have been processed. If the master processes transactions at a
 * fast enough rate that additional transactions are generated while waiting
 * for a heartbeat response, then the value of this statistic will be the
 * difference between the master timestamp (adjusted based on the VLSN rate)
 * and the timestamp of receiving the associated heartbeat response. The
 * replica delay includes the roundtrip latency of the network and any time
 * spent due to buffering of replication data, but it represents the roundtrip
 * latency of the network in absence of commits. When there are no commits, the
 * value of this statistic is computed by comparing the timestamps of sending a
 * heartbeat request and receiving an associated heartbeat response (by
 * comparing heartbeat IDs) on the master.
 *
 * <dt>replicaLastCommitTimestamp
 *
 * <dd>The commit timestamp of the last transaction committed before the most
 * recent heartbeat for which a heartbeat response has been received.  This
 * statistic represents the commit time on the master of the most recent data
 * known to have been processed on the replica.  It provides the information
 * used for the replica component of the replicaDelay statistic.
 *
 * <dt>replicaLastCommitVLSN
 *
 * <dd>The VLSN of the committed transaction described for
 * replicaLastCommitTimestamp.  This statistic provides the information used
 * for the replica component of the replicaVLSNLag statistic.
 *
 * <dt>replicaVLSNLag
 *
 * <dd>The difference between the VLSN of the latest transaction committed on
 * the master and the one most recently processed by the replica.  The master
 * VLSN comes from the lastCommitVLSN statistic maintained by FeederTxns.  This
 * statistic is similar to replicaDelay, but provides information about the
 * VLSN lag rather than the time delay.
 *
 * <dt>replicaVLSNRate
 *
 * <dd>An exponential moving average of the rate of change of the
 * replicaLastCommitVLSN statistic over time, averaged over a 10 second time
 * period.  This statistic provides information about how quickly the replica
 * is processing replication data, which can be used, along with the vlsnRate
 * statistic maintained by FeederTxns, to estimate the amount of time it will
 * take for the replica to catch up with the master.</dl>
 */
final public class Feeder {
	
    /*
     * A heartbeat is written with this period by the feeder output thread.
     */
    private final long heartbeatNs;

    /* The manager for all Feeder instances. */
    private final FeederManager feederManager;

    /* The replication node that is associated with this Feeder */
    private final RepNode repNode;
    /* The RepImpl that is associated with this rep node. */
    private final RepImpl repImpl;

    /* The socket on which the feeder communicates with the Replica. */
    private final NamedChannelWithTimeout feederReplicaChannel;

    /* The Threads that implement the Feeder */
    private final InputThread inputThread;
    private final LogThread logThread;
    private final HeartbeatThread heartbeatThread;

    /* The filter to be used for records written to the replication stream.*/
    private FeederFilter feederFilter;

    /* security check interval in ms */
    private final long securityChkIntvMs;

    private boolean isArbiterFeeder = false;

    /* The source of log records to be sent to the Replica. */
    private FeederSource feederSource;

    /* Negotiated message protocol version for the replication stream. */
    private int protocolVersion;

    private boolean canSendPingMessage;

    private boolean includeLocalDurableVLSN;

    /**
     * The current position of the feeder, that is, the log record with this
     * VLSN will be sent next to the Replica. Note that this does not mean that
     * the replica has actually processed all log records preceding feederVLSN.
     * The records immediately preceding feederVLSN (down to replicaAckVLSN)
     * may be in OutputThread.batchBuff, or in the network, in transit to the
     * replica.
     *
     * The feederVLSN can only move forwards post feeder-replica syncup.
     * However, it can move forwards or backwards as matchpoints are
     * negotiated during syncup.
     */
    private volatile long feederVLSN = NULL_VLSN;

    /**
     * The VLSN for the last entry that was written to the network and actually
     * sent to the replica.
     *
     * The following invariant must always hold: feederVLSN >= feederSentVLSN
     */
    private volatile long feederSentVLSN = NULL_VLSN;

    /**
     * The latest commit or abort that the replica has reported receiving,
     * either by ack (in the case of a commit), or via heartbeat response.  It
     * serves as a rough indication of the replay state of the replica that is
     * used in exception messages.
     *
     * The following invariant must always hold:
     * replicaTxnEndLSN <= feederSentVLSN <= feederVLSN
     */
    private volatile long replicaTxnEndVLSN = NULL_VLSN;

    private volatile long replicaDurableVLSN = NULL_VLSN;

    /* The time that the feeder last heard from its Replica */
    private volatile long lastResponseTime = 0l;

    /**
     * The millisecond time that the feeder last sent a heartbeat message to
     * its Replica.
     */
    private volatile long lastSendHeartbeatMillis = 0l;

    /**
     * The ID number of a heartbeat message that the feeder last sent to its
     * Replica, or -1 if no heartbeat has been sent.
     */
    private volatile int lastHeartbeatId = -1;

    /* The timestamp of the most recently written commit record or 0 */
    private volatile long lastCommitTimestamp;

    /* The VLSN of the most recently written commit record or 0 */
    private volatile long lastCommitVLSN;

    /*
     * Used to communicate our progress when getting ready for a Master
     * Transfer operation.
     */
    private volatile MasterTransfer masterXfr;
    private volatile boolean caughtUp = false;

    /* Used to track the status of the master. */
    private final MasterStatus masterStatus;

    /*
     * Determines whether the Feeder has been shutdown. Usually this is held
     * within the StoppableThread, but the Feeder's two child threads have
     * their shutdown coordinated by the parent Feeder.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final Logger logger;

    /* The Feeder's node ID. */
    private final NameIdPair nameIdPair;

    /**
     * The replica node ID, that is, the node that is the recipient of the
     * replication stream. Its established at the time of the Feeder/Replica
     * handshake.
     */
    private volatile NameIdPair replicaNameIdPair = NameIdPair.NULL;

    /**
     * The agreed upon log format that should be used for writing log entries
     * to send to the replica, or zero if not yet known.
     */
    private volatile int streamLogVersion = 0;

    /** The JE version of the replica, or null if not known. */
    private volatile JEVersion replicaJEVersion = null;

    /** The RepNodeImpl of the replica, or null if not known. */
    private volatile RepNodeImpl replicaNode = null;

    /**
     * The VLSN of the most recent log entry that committed a transaction and
     * was sent to the replica before the last heartbeat was sent, or 0 if no
     * such log entries have been sent since the previous heartbeat.
     */
    private volatile long lastHeartbeatCommitVLSN;

    /**
     * The timestamp of the most recent log entry that committed a transaction
     * and was sent to the replica before the last heartbeat was sent, or 0 if
     * no such log entries have been sent since the previous heartbeat.
     */
    private volatile long lastHeartbeatCommitTimestamp;

    /** The VLSN generation rate of the master in VLSNs/minute. */
    private final LongAvgRateStat vlsnRate;

    /**
     * A test hook that is called before a message is written.  Note that the
     * hook is inherited by the ReplicaFeederHandshake, and will be kept in
     * place there for the entire handshake.
     */
    private volatile TestHook<Message> writeMessageHook;

    /**
     * A test hook that is used to set the writeMessageHook for newly created
     * feeders.
     */
    private static volatile TestHook<Message> initialWriteMessageHook;

    /**
     * Returns a configured DataChannel
     *
     * @param channel the channel to be configured
     * @return the configured DataChannel
     * @throws IOException
     */
    private NamedChannelWithTimeout configureChannel(DataChannel channel)
        throws IOException {

        try {
            channel.configureBlocking(true);
            String remoteEndpoint = "unknown";
            try {
                SocketAddress address = channel.getRemoteAddress();
                if (address != null) {
                    remoteEndpoint = address.toString();
                } else {
                    LoggerUtils.info
                        (logger, repImpl, "Could not determine remote address.");
                }
            } catch (IOException e) {
                LoggerUtils.info
                    (logger, repImpl, "Could not determine remote address. " +
                     e.getMessage());
            }
            LoggerUtils.info
                (logger, repImpl, "Feeder accepted connection from " +
                 remoteEndpoint);
            final int timeoutMs = repNode.getConfigManager().
                getDuration(RepParams.PRE_HEARTBEAT_TIMEOUT);
            final boolean tcpNoDelay = repNode.getConfigManager().
                    getBoolean(RepParams.FEEDER_TCP_NO_DELAY);

            /* Set use of Nagle's algorithm on the socket. */
            channel.socket().setTcpNoDelay(tcpNoDelay);
            return new NamedChannelWithTimeout(repNode, channel, timeoutMs);
        } catch (IOException e) {
            LoggerUtils.warning(logger, repImpl,
                                "IO exception while configuring channel " +
                                "Exception:" + e.getMessage());
            throw e;
        }
    }

    Feeder(FeederManager feederManager, DataChannel dataChannel)
        throws DatabaseException, IOException {

        this.feederManager = feederManager;
        repNode = feederManager.repNode();
        repImpl = repNode.getRepImpl();
        masterStatus = repNode.getMasterStatus();
        nameIdPair = repNode.getNameIdPair();
        feederSource = null;
        logger = LoggerUtils.getLogger(getClass());

        feederReplicaChannel = configureChannel(dataChannel);
        /*
         * Initialization of heartbeatNs must precede its use in OutputThread
         * ctor below.
         */
        heartbeatNs =
            TimeUnit.MILLISECONDS.toNanos(repNode.getHeartbeatInterval());
        inputThread = new InputThread();
        logThread = new LogThread();
        heartbeatThread = new HeartbeatThread();
        vlsnRate = repImpl.getFeederTxns().getVLSNRate();
        writeMessageHook = initialWriteMessageHook;

        feederFilter = null;

        /* get authenticator from containing rn */
        securityChkIntvMs = repNode.getSecurityCheckInterval();
        LoggerUtils.info(logger, repImpl,
                         "Feeder created with nameId=" + nameIdPair +
                         ", channel authenticator=" +
                         dataChannel.getStreamAuthenticator() +
                         ", security check interval ms=" + securityChkIntvMs);
    }

    void startFeederThreads() {
        inputThread.start();
    }

    /**
     * @hidden
     * Place holder Feeder for testing only
     */
    public Feeder() {
        feederManager = null;
        repNode = null;
        repImpl = null;
        masterStatus = null;
        feederSource = null;
        feederReplicaChannel = null;
        nameIdPair = NameIdPair.NULL;
        logger = LoggerUtils.getLoggerFixedPrefix(getClass(), "TestFeeder");
        heartbeatNs = 0;
        inputThread = null;
        logThread = null;
        heartbeatThread = null;
        shutdown.set(true);
        vlsnRate = null;
        writeMessageHook = initialWriteMessageHook;
        feederFilter = null;
        securityChkIntvMs = 0;
    }

    /**
     * Creates the MasterFeederSource, which must be done while all files in
     * the VLSNIndex range are protected by syncup.
     */
    public void initMasterFeederSource(long startVLSN)
        throws IOException {

        replicaTxnEndVLSN = VLSN.getPrev(startVLSN);
        if (replicaTxnEndVLSN >= repNode.getCurrentTxnEndVLSN()) {
            caughtUp = true;
        }
        feederVLSN = startVLSN;
        feederSource = new MasterFeederSource(repNode.getRepImpl(),
                                              repNode.getVLSNIndex(),
                                              replicaNameIdPair,
                                              startVLSN);

        /*
         * at this time both replica node type and feeder filter have been set,
         * set feeder source if feeder requires to stream durable entries
         * only. This applies to master feeder source only.
         */
        if (isDurableOnly()) {
            final MasterFeederSource mfs = (MasterFeederSource) feederSource;
            mfs.setDurableOnly();
            LoggerUtils.info(logger, repImpl,
                             "Set feeder to stream durable entries " +
                             "only, replica=" + replicaNameIdPair);
        }
    }

    private void initArbiterFeederSource() {

        feederSource = new ArbiterFeederSource(repNode.getRepImpl());
        feederVLSN = NULL_VLSN;
        isArbiterFeeder = true;
    }

    /* Get the protocol stats of this Feeder. */
    public StatGroup getProtocolStats(StatsConfig config) {
        Protocol protocol = logThread.protocol;
        final StatGroup pstat = (protocol == null) ?
           new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                         BinaryProtocolStatDefinition.GROUP_DESC) :
           protocol.getStats(config);

        protocol = heartbeatThread.protocol;
        if (protocol != null) {
            pstat.addAll(protocol.getStats(config));
        }
        return pstat;
    }

    void resetStats() {
        Protocol protocol = logThread.protocol;
        if (protocol != null) {
            protocol.resetStats();
        }

        protocol = heartbeatThread.protocol;
        if (protocol != null) {
            protocol.resetStats();
        }
    }

    void setMasterTransfer(MasterTransfer mt) {
        masterXfr = mt;
        if (caughtUp) {
            adviseMasterTransferProgress();
        }
    }

    void adviseMasterTransferProgress() {
        MasterTransfer mt = masterXfr;
        if (mt != null) {
            mt.noteProgress
                (new VLSNProgress(replicaTxnEndVLSN,
                                  replicaNameIdPair.getName(), true));
        }
    }

    public RepNode getRepNode() {
        return repNode;
    }

    public NameIdPair getReplicaNameIdPair() {
        return replicaNameIdPair;
    }

    public void setFeederFilter(FeederFilter filter) {
        feederFilter = filter;
        LoggerUtils.info(logger, repImpl, "Set feeder filter=" + filter);
    }

    public FeederFilter getFeederFilter() {
        return feederFilter;
    }

    /**
     * Get the time of the most recent message received from the replica.
     */
    public long getLastResponseTime() {
        return lastResponseTime;
    }

    /**
     * Returns the latest commit VLSN that was acked by the replica, or
     * NULL_VLSN if no commit was acked since the time the feeder was
     * established.
     */
    public long getReplicaTxnEndVLSN() {
        return replicaTxnEndVLSN;
    }

    public long getReplicaDurableVLSN() {
        return replicaDurableVLSN;
    }

    /**
     * Returns the next VLSN that will be sent to the replica. It will
     * return VLSN.NULL if the Feeder is in the process of being created and
     * FeederReplicaSyncup has not yet happened.
     */
    public long getFeederVLSN() {
        return feederVLSN;
    }

    /**
     * Return the channel associated with this feeder.
     */
    public Channel getChannel() {
        return feederReplicaChannel;
    }

    public boolean canSendPingMessage() {
        return canSendPingMessage;
    }

    public boolean includeLocalDurableVLSN() {
        return includeLocalDurableVLSN;
    }

    /**
     * Returns the JE version supported by the replica, or {@code null} if the
     * value is not yet known.
     *
     * @return the replica JE version or {@code null}
     */
    public JEVersion getReplicaJEVersion() {
        return replicaJEVersion;
    }

    /**
     * Returns a RepNodeImpl that describes the replica, or {@code null} if the
     * value is not yet known.  The value will be non-null if the feeder
     * handshake has completed successfully.
     *
     * @return the replica node or {@code null}
     */
    public RepNodeImpl getReplicaNode() {
        return replicaNode;
    }

    /**
     * Unit test only
     */
    public long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    /**
     * Shutdown the feeder, closing its channel and releasing its threads.  May
     * be called internally upon noticing a problem, or externally when the
     * RepNode is shutting down.
     */
    public void shutdown(Exception shutdownException) {

        boolean changed = shutdown.compareAndSet(false, true);
        if (!changed) {
            return;
        }

        MasterTransfer mt = masterXfr;
        final String replicaName = replicaNameIdPair.getName();
        if (mt != null) {
            mt.giveUp(replicaName);
        }
        feederManager.removeFeeder(this);

        /* Shutdown feeder source to remove file protection. */
        if (feederSource != null) {
            feederSource.shutdown(repImpl);
        }

        final StatGroup pstats = getProtocolStats(StatsConfig.DEFAULT);
        if (inputThread.protocol != null)  {
            pstats.addAll(inputThread.protocol.getStats(StatsConfig.DEFAULT));
        }

        feederManager.incStats(pstats);

        /* Remove replica stats */
        feederManager.getReplicaDelayMap().removeStat(replicaName);
        feederManager.getReplicaAvgDelayMsMap().removeStat(replicaName);
        feederManager.getReplica95DelayMsMap().removeStat(replicaName);
        feederManager.getReplica99DelayMsMap().removeStat(replicaName);
        feederManager.getReplicaMaxDelayMsMap().removeStat(replicaName);
        feederManager.getReplicaLastCommitTimestampMap().removeStat(
            replicaName);
        feederManager.getReplicaLastCommitVLSNMap().removeStat(replicaName);
        feederManager.getReplicaLocalDurableVLSNMap().removeStat(replicaName);
        feederManager.getReplicaVLSNLagMap().removeStat(replicaName);
        feederManager.getReplicaNHeartbeatReceivedMap().removeStat(replicaName);
        feederManager.getReplicaNHeartbeatSentMap().removeStat(replicaName);
        feederManager.getReplicaVLSNRateMap().removeStat(replicaName);

        LoggerUtils.info(logger, repImpl,
                         "Shutting down feeder for replica=" + replicaName +
                         ((shutdownException == null) ?
                          "" :
                          (" reason=" + shutdownException +
                           ", protocol stats= ")) +
                         RepUtils.writeTimesString(pstats) +
                         ((shutdownException == null) ? "" :
                         ", shutdown exception stack=\n" +
                         LoggerUtils.getStackTrace(shutdownException)));

        final String feederState =
            String.format("Feeder state at exit -- " +
                          "master commit VLSN:%,d " +
                          "feeder VLSN:%,d sent VLSN:%,d buffer:%,d bytes " +
                          "replica commit VLSN:%,d ",
                          repNode.getCurrentTxnEndVLSN(),
                          feederVLSN,
                          feederSentVLSN,
                          logThread.batchBuff.position(),
                          replicaTxnEndVLSN);

        LoggerUtils.info(logger, repImpl, feederState);
        // Do not bother trying to catch up if we experienced an exception.
        if (repNode.getReplicaCloseCatchupMs() >= 0 &&
            shutdownException == null) {

            /*
             * Need to shutdown the group cleanly, wait for it to let the
             * replica catchup and exit in the allowed time period.
             */
            try {

                /*
                 * Note that we wait on the Input thread, since it's the one
                 * that will exit on the ShutdownResponse message from the
                 * Replica. The output thread will exit immediately after
                 * sending the ShutdownRequest.
                 */
                inputThread.join();
                /* Timed out, or the input thread exited; keep going. */
            } catch (InterruptedException e) {
                LoggerUtils.warning(logger, repImpl,
                                    "Interrupted while waiting to join " +
                                    "thread:" + logThread);
            }
        }

        logThread.shutdownThread(logger);
        heartbeatThread.shutdownThread(logger);
        inputThread.shutdownThread(logger);

        /*
         * Feeder is truly done, now that its threads have been shutdown. Only
         * decrement if the shutdown finishes normally. Any unhandled
         * exceptions from the code above should result in the environment being
         * invalidated.
         */
        feederManager.decrementManagedFeederCount();

        LoggerUtils.finest(logger, repImpl,
                           feederReplicaChannel + " isOpen=" +
                           feederReplicaChannel.getChannel().isOpen());
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    public ArbiterFeederSource getArbiterFeederSource() {
        if (feederSource != null &&
            feederSource instanceof ArbiterFeederSource) {
            return (ArbiterFeederSource)feederSource;
        }

        return null;
    }

    /**
     * Unit test only
     * @return master feeder source or null
     */
    public MasterFeederSource getMasterFeederSource() {
        if (feederSource != null &&
            feederSource instanceof MasterFeederSource) {
            return (MasterFeederSource) feederSource;
        }

        return null;
    }

    private StreamAuthenticator getAuthenticator() {
        return feederReplicaChannel.getChannel().getStreamAuthenticator();
    }

    /**
     * Implements the thread responsible for processing the responses from a
     * Replica.
     */
    private class InputThread extends StoppableThread {

        Protocol protocol = null;

        /*
         * Per-replica stats stored in a map in the feeder manager.  These can
         * only be set once the replica name is found following the handshake.
         *
         * See the class javadoc comment for more information about these
         * statistics and how they can be used to gather information about
         * replication rates.
         */
        private volatile AtomicLongComponent replicaDelay;
        private volatile LongAvg replicaAvgDelayMs;
        private volatile LatencyPercentile replica95DelayMs;
        private volatile LatencyPercentile replica99DelayMs;
        private volatile LongMax replicaMaxDelayMs;
        private volatile AtomicLongComponent replicaLastCommitTimestamp;
        private volatile AtomicLongComponent replicaLastCommitVLSN;
        private volatile AtomicLongComponent replicaLastLocalDurableVLSN;
        private volatile LongDiffStat replicaVLSNLag;
        private volatile LongAvgRate replicaVLSNRate;
        private volatile LongAvg replicaAckLatency;
        private volatile AtomicLongComponent replicaNHeartbeatReceived;

        InputThread() {
            /*
             * The thread will be renamed later on during the life of this
             * thread, when we're sure who the replica is.
             */
            super(repImpl, new IOThreadsHandler(), "Feeder Input");
        }

        /**
         * Does the initial negotiation to validate replication group wide
         * consistency and establish the starting VLSN. It then starts up the
         * Output thread and enters the response loop.
         */
        @Override
        public void run() {

            /* Set to indicate an error-initiated shutdown. */
            Error feederInputError = null;
            Exception shutdownException = null;

            try {
                FeederReplicaHandshake handshake =
                    new FeederReplicaHandshake(repNode,
                                               Feeder.this,
                                               feederReplicaChannel);
                protocol = handshake.execute();
                canSendPingMessage = protocol.supportSyncupPingMessage();
                includeLocalDurableVLSN = protocol.includeLocalDurableVLSN();
                protocolVersion = protocol.getVersion();
                replicaNameIdPair = handshake.getReplicaNameIdPair();
                streamLogVersion = handshake.getStreamLogVersion();
                replicaJEVersion = handshake.getReplicaJEVersion();
                replicaNode = handshake.getReplicaNode();

                /*
                 * Rename the thread when we get the replica name in, so that
                 * it's clear who is on the other end.
                 */
                Thread.currentThread().setName("Feeder Input for " +
                                               replicaNameIdPair.getName());

                if (replicaNode.getType().isArbiter()) {
                    initArbiterFeederSource();
                } else {
                    FeederReplicaSyncup syncup = new FeederReplicaSyncup(
                            Feeder.this, feederReplicaChannel, protocol);

                    /*
                     * Sync-up produces the VLSN of the next log record needed
                     * by the replica, one beyond the last commit or abort it
                     * already has. Sync-up calls initMasterFeederSource while
                     * the VLSNIndex range is protected.
                     */
                    syncup.execute();
                }

                /* Set up stats */
                replicaDelay =
                    feederManager.getReplicaDelayMap().createStat(
                        replicaNameIdPair.getName());
                replicaAvgDelayMs =
                    feederManager.getReplicaAvgDelayMsMap().createStat(
                        replicaNameIdPair.getName());
                replica95DelayMs =
                    feederManager.getReplica95DelayMsMap().createStat(
                        replicaNameIdPair.getName());
                replica99DelayMs =
                    feederManager.getReplica99DelayMsMap().createStat(
                        replicaNameIdPair.getName());
                replicaMaxDelayMs =
                    feederManager.getReplicaMaxDelayMsMap().createStat(
                        replicaNameIdPair.getName());
                replicaLastCommitTimestamp =
                    feederManager.getReplicaLastCommitTimestampMap()
                                 .createStat(replicaNameIdPair.getName());
                replicaLastCommitVLSN =
                    feederManager.getReplicaLastCommitVLSNMap()
                                 .createStat(replicaNameIdPair.getName());
                replicaLastLocalDurableVLSN =
                        feederManager.getReplicaLocalDurableVLSNMap()
                                .createStat(replicaNameIdPair.getName());
                replicaVLSNLag = feederManager.getReplicaVLSNLagMap()
                                              .createStat(
                                                  replicaNameIdPair.getName(),
                                                  repNode.getFeederTxns()
                                                         .getLastCommitVLSN());
                replicaVLSNRate = feederManager.getReplicaVLSNRateMap()
                                               .createStat(
                                                   replicaNameIdPair.getName());
                replicaAckLatency =
                    feederManager.getReplicaAckLatencyMap().
                    createStat(replicaNameIdPair.getName());
                replicaNHeartbeatReceived =
                        feederManager.getReplicaNHeartbeatReceivedMap().createStat(
                            replicaNameIdPair.getName());

                /* Start the thread to pump out log records */
                lastCommitTimestamp = 0;
                lastCommitVLSN = 0;
                logThread.start();
                heartbeatThread.start();
                lastResponseTime = TimeSupplier.currentTimeMillis();
                masterStatus.assertSync();
                feederManager.activateFeeder(Feeder.this);

                runResponseLoop();
            } catch (ReplicationSecurityException ue) {
                shutdownException = ue;
                LoggerUtils.warning(logger, repImpl, ue.getMessage());
            } catch (NetworkRestoreException e) {
                shutdownException = e;
                /* The replica will retry after a network restore. */
                LoggerUtils.info(logger, repImpl, e.getMessage());
            } catch (IOException | SyncUpFailedException e) {
                /* Trio of benign "expected" exceptions below. */
                shutdownException = e; /* Expected. */
            } catch (MasterObsoleteException e) {
                shutdownException = e; /* Expected. */
            } catch (ExitException e) {
                shutdownException = e;
                LoggerUtils.warning(logger, repImpl,
                                    "Exiting feeder loop: " + e.getMessage());
            } catch (Error e) {
                feederInputError = e;
                repNode.getRepImpl().invalidate(e);
            } catch (ChecksumException e) {
                shutdownException = e;

                /* An internal, unexpected error. Invalidate the environment. */
                throw new EnvironmentFailureException
                    (repNode.getRepImpl(),
                     EnvironmentFailureReason.LOG_CHECKSUM, e);
            } catch (RuntimeException e) {
                shutdownException = e;

                /*
                 * An internal error. Shut down the rep node as well for now
                 * by throwing the exception out of the thread.
                 *
                 * In future we may want to close down just the impacted Feeder
                 * but this is the safe course of action.
                 */
                LoggerUtils.severe(logger, repImpl,
                                   "Unexpected exception: " + e.getMessage() +
                                   " " +
                                   LoggerUtils.getStackTraceForSevereLog(e));
                throw e;
            } finally {
                if (feederInputError != null) {
                    /* Propagate the error, skip cleanup. */
                    throw feederInputError;
                }

                /*
                 * Shutdown the feeder in its entirety, in case the input
                 * thread is the only one to notice a problem. The Replica can
                 * decide to re-establish the connection
                 */
                LoggerUtils.info(logger, repImpl,
                                 "Feeder input thread to shut down feeder in " +
                                 "its entirety" +
                                 ", replica=" + replicaNameIdPair.getName() +
                                 ", exception=" + shutdownException) ;
                shutdown(shutdownException);
                cleanup();
            }
        }

        /*
         * This method deals with responses from the Replica. There are exactly
         * two types of responses from the Replica:
         *
         * 1) Responses acknowledging a successful commit by the Replica.
         *
         * 2) Responses to heart beat messages.
         *
         * This loop (like the loop in the LogThread and HeartbeatThread) is
         * terminated under one of the following conditions:
         *
         * 1) The thread detects a change in masters.
         * 2) There is network connection issue (which might also be an
         *    indication of an unfolding change in masters).
         * 3) If the replica closes its connection -- variation of the above.
         *
         * In addition, the loop will also exit if it gets a ShutdownResponse
         * message sent in response to a ShutdownRequest sent by the
         * LogThread.
         */
        private void runResponseLoop()
            throws IOException, MasterObsoleteException {

            /*
             * Start the acknowledgment loop. It's very important that this
             * loop be wait/contention free.
             */
            while (!checkShutdown()) {
                Message response = protocol.read(feederReplicaChannel);
                if (checkShutdown()) {

                    /*
                     * Shutdown quickly, in particular, don't update sync
                     * VLSNs.
                     */
                    break;
                }
                masterStatus.assertSync();

                lastResponseTime = TimeSupplier.currentTimeMillis();
                /*
                 * Whenever we get a message from a replica check if we are
                 * still within the authorized master window.  If not, call
                 * isAuthorizedMaster to try to get authorization again.  If
                 * we succeed, notify any transactions waiting for
                 * authorization.
                 */
                if (!feederManager.withinAuthoritativeWindow(lastResponseTime) &&
                    feederManager.isAuthoritativeMaster(masterStatus, null)) {
                    if (logger.isLoggable(Level.FINE)) {
                        LoggerUtils.fine(logger, repImpl,
                            "Feeder " + repNode.getName() +
                            " waking up authorized master waiters at time " +
                            lastResponseTime);
                    }
                    feederManager.notifyAllAuthMasterWaiter();
                }

                if (response.getOp().getMessageClass() ==
                    Protocol.HEARTBEAT_RESPONSE_CLASS) {
                    processHeartbeatResponse(response);
                } else if (response.getOp().getMessageClass() ==
                    Protocol.ACK_CLASS) {

                    /*
                     * Check if a commit has been waiting for this
                     * acknowledgment and signal any waiters.
                     */
                    long txnId = ((Ack) response).getTxnId();
                    if (logger.isLoggable(Level.FINE)) {
                        LoggerUtils.fine(logger, repImpl, "Ack for: " + txnId);
                    }
                    if (includeLocalDurableVLSN()) {
                        long replicaLocalDurableVLSN = ((Ack) response).
                                getLocalDurableVLSN();
                        deemAcked(txnId, replicaLocalDurableVLSN);
                    } else {
                        deemAcked(txnId);
                    }
                }  else if (response.getOp().getMessageClass() ==
                    Protocol.GROUP_ACK_CLASS) {
                    final long txnIds[] = ((GroupAck) response).getTxnIds();
                    if (includeLocalDurableVLSN()) {
                        final long replicaLocalDurableVLSN =
                                ((GroupAck) response).getLocalDurableVLSN();
                        for (long txnId : txnIds) {
                            if (logger.isLoggable(Level.FINE)) {
                                LoggerUtils.fine(logger, repImpl,
                                        "Group Ack for: " + txnId);
                            }
                            deemAcked(txnId, replicaLocalDurableVLSN);
                        }
                    } else {
                        for (long txnId : txnIds) {
                            if (logger.isLoggable(Level.FINE)) {
                                LoggerUtils.fine(logger, repImpl,
                                        "Group Ack for: " + txnId);
                            }
                            deemAcked(txnId);
                        }
                    }
                } else if (response.getOp().getMessageClass() ==
                    Protocol.SHUTDOWN_RESPONSE_CLASS) {
                    LoggerUtils.info(logger, repImpl,
                                     "Shutdown confirmed by replica " +
                                     replicaNameIdPair.getName());
                    // Wake up any waiting transactions.
                    feederManager.notifyAllAuthMasterWaiter();
                    /* Exit the loop and the thread. */
                    break;
                } else if (response.getOp().getMessageClass() ==
                    Protocol.REAUTHENTICATE_CLASS) {
                    if (!processReauthenticate(response)) {
                        final String err = "replica " +
                                           feederReplicaChannel
                                               .getNameIdPair().getName() +
                                           " fails the security check in " +
                                           "reauthentication.";
                        /* signal client */
                        makeSecurityCheckResponse(err);
                    }
                } else if (response.getOp().getMessageClass() ==
                    Protocol.FILTER_CHANGE_REQUEST_CLASS) {
                    processFilterChangeRequest(response);
                } else {
                    throw EnvironmentFailureException.unexpectedState
                        ("Unexpected message: " + response);
                }
            }
        }

        private void deemAcked(long txnId) {
            final MasterTxn mtxn = Feeder.this.deemAcked(txnId);
            if (mtxn != null) {
                long ackWaitStartNs = mtxn.getAckWaitStartNs();
                if (ackWaitStartNs > 0) {
                    long nowNs = System.nanoTime();
                    replicaAckLatency.add(nowNs - ackWaitStartNs);
                }
            }
        }

        private void deemAcked(long txnId, long replicaLocalDurableVLSN) {
            final MasterTxn mtxn = Feeder.this.deemAcked(
                    txnId, replicaLocalDurableVLSN);
            if (mtxn != null) {
                long ackWaitStartNs = mtxn.getAckWaitStartNs();
                if (ackWaitStartNs > 0) {
                    long nowNs = System.nanoTime();
                    replicaAckLatency.add(nowNs - ackWaitStartNs);
                }
            }
        }

        private void processHeartbeatResponse(Message response) {
            /* Last response has been updated, keep going. */
            final HeartbeatResponse hbResponse =
                (Protocol.HeartbeatResponse)response;

            replicaNHeartbeatReceived.add(1);
            /*
             * For arbiters we do not process the response, but it is still
             * important for preventing the channel from timing out.
             */
            if (replicaNode.getType().isArbiter()) {
                return;
            }

            final long replicaTxnVLSN = hbResponse.getTxnEndVLSN();

            /* All further work requires the replica's VLSN */
            if (replicaTxnVLSN == INVALID_VLSN) {
                return;
            }

            replicaTxnEndVLSN = replicaTxnVLSN;
            final long replicaTxnVLSNSeq = replicaTxnVLSN;
            long replicaLocalDurableVLSN = NULL_VLSN;

            if (includeLocalDurableVLSN()) {
                replicaLocalDurableVLSN =
                    hbResponse.getLocalDurableVLSN();
                if (replicaLocalDurableVLSN == INVALID_VLSN) {
                    return;
                }
                replicaDurableVLSN = replicaLocalDurableVLSN;
                feederManager.updateDTVLSNWithLocalVLSN(replicaLocalDurableVLSN);
            } else {
                feederManager.updateDTVLSN(replicaTxnVLSNSeq);
            }

            if (replicaTxnVLSN >= repNode.getCurrentTxnEndVLSN()) {

                caughtUp = true;
                adviseMasterTransferProgress();
            }

            /*
             * Only tally statistics for the commit VLSN and timestamp if both
             * values were recorded when the heartbeat was requested.  Make
             * computations based directly on the measured heartbeat delay if
             * the heartbeat reply confirms that the requested VLSN has been
             * processed.  Otherwise, use the master VLSN rate to estimate the
             * delay.
             */
            final long commitVLSN = lastHeartbeatCommitVLSN;
            final long commitTimestamp = lastHeartbeatCommitTimestamp;
            if ((commitVLSN == 0) || (commitTimestamp == 0)) {
                /*
                 * If the heartbeat response matches the heartbeat request,
                 * use network roundtrip latency for heartbeats to estimate
                 * replica delay in absence of commits.
                 */
                if ((hbResponse.getHeartbeatId() == lastHeartbeatId) &&
                    (lastHeartbeatId != -1)) {
                    updateReplicaDelayStats(lastSendHeartbeatMillis);
                }
                return;
            }
            final long statCommitVLSN = (commitVLSN <= replicaTxnVLSNSeq) ?
                commitVLSN : replicaTxnVLSNSeq;

            /* Set the depended-on stats first */
            replicaLastCommitVLSN.set(statCommitVLSN);
            if (includeLocalDurableVLSN()) {
                replicaLastLocalDurableVLSN.set(replicaLocalDurableVLSN);
            }
            replicaVLSNLag.set(statCommitVLSN, lastResponseTime);
            replicaVLSNRate.add(statCommitVLSN, lastResponseTime);

            final long statCommitTimestamp;
            if (commitVLSN <= replicaTxnVLSNSeq) {
                /*
                 * VLSN in hb response is same or newer than last commit vlsn
                 * sent in a hb by master, that is, replica has moved on since
                 * the last heartbeat.
                 */
                statCommitTimestamp = commitTimestamp;
            } else {

                /* Adjust the commit timestamp based on the VLSN rate */
                final long vlsnRatePerMinute = vlsnRate.get();
                if (vlsnRatePerMinute <= 0) {
                    return;
                }
                final long vlsnLag = commitVLSN - replicaTxnVLSNSeq;
                final long timeLagMillis =
                    (long) (60000.0 * ((double) vlsnLag / vlsnRatePerMinute));
                statCommitTimestamp = commitTimestamp - timeLagMillis;
            }
            replicaLastCommitTimestamp.set(statCommitTimestamp);

            final long delayStartMs = (commitVLSN == replicaTxnVLSNSeq) ?
                /* Replica is caught up. set delay = network roundtrip time. */
                lastSendHeartbeatMillis :
                statCommitTimestamp;
            updateReplicaDelayStats(delayStartMs);
        }

        private void updateReplicaDelayStats(long timestamp) {
            final long delay = lastResponseTime - timestamp;
            replicaDelay.set(delay);
            replicaAvgDelayMs.add(delay);
            replica95DelayMs.add(delay);
            replica99DelayMs.add(delay);
            replicaMaxDelayMs.add(delay);
        }

        void processFilterChangeRequest(Message msg) throws IOException {
            /* ignore the request if no feeder filter installed */
            if (feederFilter == null) {
                return;
            }
            final Protocol.FilterChangeReq request =
                (Protocol.FilterChangeReq)msg;
            final FeederFilterChangeResult result =
                feederFilter.applyChange(request.getChange(), repImpl);

            /* construct response */
            final Protocol proto = inputThread.protocol;
            final Protocol.FilterChangeResp resp =
                proto.new FilterChangeResp(result);

            final String replica = feederReplicaChannel.getNameIdPair()
                                                       .getName();
            try {
                proto.write(resp, feederReplicaChannel);
            } catch (IOException ioe) {
                LoggerUtils.warning(logger, repImpl,
                                    "Fail to send filter change response to  " +
                                    "replica " + replica +
                                    ", message fail to send: " +
                                    resp);
                throw ioe;
            }
        }

        /*
         * Returns true if the InputThread should be shutdown, that is, if the
         * thread has been marked for shutdown and it's not a group shutdown
         * request. For a group shutdown the input thread will wait for an
         * acknowledgment of the shutdown message from the Replica.
         */
        private boolean checkShutdown() {
            return shutdown.get() &&
                   (repNode.getReplicaCloseCatchupMs() < 0);
        }

        @Override
        protected int initiateSoftShutdown() {

            /*
             * Provoke an I/O exception that will cause the input thread to
             * exit.
             */
            RepUtils.shutdownChannel(feederReplicaChannel);
            return repNode.getThreadWaitInterval();
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /**
     * Base message output class for the log and heartbeat threads.
     */
    private abstract class OutputThread extends StoppableThread {
        Protocol protocol = null;

        OutputThread(final String threadName) {
            /*
             * The thread will be renamed later on during the life of this
             * thread, when we know who the replica is.
             */
            super(repImpl, new IOThreadsHandler(), threadName,
                  repImpl.getFileManager().getFeederStatsCollector());
        }

        /** Write a protocol message to the channel. */
        protected void writeMessage(final Message message,
                                    final NamedChannel namedChannel)
            throws IOException {

            assert TestHookExecute.doHookIfSet(writeMessageHook, message);
            protocol.write(message, namedChannel);
        }

        /**
         * Sends a heartbeat message, if we have exceeded the heartbeat
         * interval.
         *
         * @throws IOException
         */
        protected void sendHeartbeat()
            throws IOException {

            final long vlsn = repNode.getCurrentTxnEndVLSN();
            lastHeartbeatId = (lastHeartbeatId < Integer.MAX_VALUE) ?
                              lastHeartbeatId + 1 : 0;
            final long nowMillis = TimeSupplier.currentTimeMillis();
            final long filterVLSN = (feederFilter == null) ? 0 :
                feederFilter.getFilterVLSN();
            final long lastPassVLSN = (feederFilter == null) ? 0 :
                feederFilter.getLastPassVLSN();
            final long lastModTime = (feederFilter == null) ? 0 :
                feederFilter.getLastModTimeMs();
            writeMessage(protocol.new Heartbeat(nowMillis, vlsn,
                                                lastHeartbeatId,
                                                filterVLSN, lastPassVLSN,
                                                lastCommitTimestamp,
                                                lastModTime),
                         feederReplicaChannel);
            lastSendHeartbeatMillis = nowMillis;

            if (isArbiterFeeder) {
                return;
            }

            /* Record the most recent transaction end or clear */
            if (lastCommitTimestamp != 0) {
                lastHeartbeatCommitTimestamp = lastCommitTimestamp;
                lastHeartbeatCommitVLSN = lastCommitVLSN;
            } else {
                lastHeartbeatCommitTimestamp = 0;
                lastHeartbeatCommitVLSN = 0;
            }
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    /**
     * Simply pumps out log entries as rapidly as it can.
     */
    private class LogThread extends OutputThread {
        /*
         * The max time (1 sec) that we wait for a record to show up regardless
         * of heartbeats, and other strange test configurations. This setting
         * ensures that the network write buffer is always flushed after 1 sec.
         */
        final static long MAX_BATCH_WAIT_NS = 1_000_000_000l;

        private long totalTransferDelay = 0;

        /**
         * Determines whether writing to the network connection for the replica
         * suffices as a commit acknowledgment.
         */
        private final boolean commitToNetwork;

        /**
         * The threshold used to trigger the logging of transfers of commit
         * records.
         */
        private final int transferLoggingThresholdMs;

        /**
         * The max time interval during which feeder records for txns requiring
         * acks are batched.
         */
        private final int ackTxnBatchNs;

        /**
         * The direct byte buffer holding the batched feeder records.
         */
        private final ByteBuffer batchBuff;

        private final VLSNIndex vlsnIndex;

        /* The time at which the group shutdown was initiated. */
        private long shutdownRequestStart = 0;

        /*
         * The delay between writes of a replication message. Note that
         * setting this to a non-zero value effectively turns off message
         * batching.
         */
        final int testDelayMs;

        LogThread() {
            super("Feeder Output");
            final DbConfigManager configManager = repNode.getConfigManager();
            commitToNetwork = configManager.
                getBoolean(RepParams.COMMIT_TO_NETWORK);
            transferLoggingThresholdMs = configManager.
                getDuration(RepParams.TRANSFER_LOGGING_THRESHOLD);

            assert heartbeatNs != 0; /* Must be initialized. */
            ackTxnBatchNs = Math.min(configManager.
                               getInt(RepParams.FEEDER_ACK_TXN_BATCH_NS),
                               (int)(heartbeatNs * 4));
            final int batchBuffSize = configManager.
                            getInt(RepParams.FEEDER_BATCH_BUFF_KB) * 1024;
            batchBuff = ByteBuffer.allocateDirect(batchBuffSize);
            vlsnIndex = repNode.getVLSNIndex();
            testDelayMs = feederManager.getTestDelayMs();
            if (testDelayMs > 0) {
                LoggerUtils.info(logger, repImpl,
                                 "Test delay of:" + testDelayMs + "ms." +
                                 " after each message sent");
            }
        }

        /**
         * Determines whether we should exit the output loop. If we are trying
         * to shutdown the Replica cleanly, that is, this is a group shutdown,
         * the method delays the shutdown until the Replica has had a chance
         * to catch up to the current commit VLSN on this node, after which
         * it sends the Replica a Shutdown message.
         *
         * @return true if the output thread should be shutdown.
         *
         * @throws IOException
         */
        private boolean checkShutdown()
            throws IOException {

            if (!shutdown.get()) {
                return false;
            }
            if (repNode.getReplicaCloseCatchupMs() >= 0) {
                if (shutdownRequestStart == 0) {
                    shutdownRequestStart = TimeSupplier.currentTimeMillis();
                }
                /* Determines if the feeder has waited long enough. */
                boolean timedOut =
                    (TimeSupplier.currentTimeMillis() - shutdownRequestStart) >
                    repNode.getReplicaCloseCatchupMs();
                if (!timedOut &&
                    !isArbiterFeeder &&
                    feederVLSN <= repNode.getCurrentTxnEndVLSN()) {
                    /*
                     * Replica is not caught up. Note that feederVLSN at stasis
                     * is one beyond the last value that was actually sent,
                     * hence the <= instead of < above.
                     */
                    return false;
                }
                /* Replica is caught up or has timed out, shut it down. */
                writeMessage(protocol.new ShutdownRequest(shutdownRequestStart),
                             feederReplicaChannel);

                String shutdownMessage =
                    String.format("Shutdown message sent to: %s. " +
                                  "Feeder vlsn: %,d. " +
                                  "Shutdown elapsed time: %,dms",
                                  replicaNameIdPair,
                                  feederVLSN,
                                  (TimeSupplier.currentTimeMillis() -
                                   shutdownRequestStart));
                LoggerUtils.info(logger, repImpl, shutdownMessage);
                return true;
            }
            return true;
        }

        @Override
        protected int initiateSoftShutdown() {
            assert shutdown.get();
            try {
                /* Wait for thread to notice the shutdown and flush
                 * any outstanding write buffers.
                 */
                this.join(TimeUnit.NANOSECONDS.toMillis(MAX_BATCH_WAIT_NS));
            } catch (InterruptedException e) {
                /* Ignore */
            }

            /*
             * Provoke an I/O exception that will cause the output thread to
             * exit.
             */
            RepUtils.shutdownChannel(feederReplicaChannel);
            return repNode.getThreadWaitInterval();
        }

        @Override
        public void run() {
            protocol =
                Protocol.get(repNode, replicaNameIdPair,
                             protocolVersion, protocolVersion,
                             streamLogVersion);
            Thread.currentThread().setName
                ("Log Thread for " + getReplicaNameIdPair().getName());
            {
                VLSNRange range = vlsnIndex.getRange();
                LoggerUtils.info
                    (logger, repImpl, String.format
                     ("Feeder output thread for replica %s started at " +
                      "VLSN %,d master at %,d (DTVLSN:%,d) " +
                      "VLSN delta=%,d socket=%s",
                      replicaNameIdPair.getName(),
                      feederVLSN,
                      range.getLast(),
                      repNode.getAnyDTVLSN(),
                      range.getLast() - feederVLSN,
                      feederReplicaChannel));
            }

            /* Set to indicate an error-initiated shutdown. */
            Error feederOutputError = null;
            Exception shutdownException = null;
            try {

                /*
                 *  Always start out with a heartbeat; the replica is counting
                 *  on it.
                 */
                sendHeartbeat();
                feederReplicaChannel.setTimeoutMs(
                    repImpl.getFeederReplicaChannelTimeoutMillis());

                while (!checkShutdown()) {
                    if (feederVLSN >= repNode.getCurrentTxnEndVLSN()) {

                        /*
                         * The replica is caught up, if we are a Primary stop
                         * playing that role, and start requesting acks from
                         * the replica.
                         */
                        repNode.getArbiter().endArbitration();
                    }

                    if (!doSecurityCheck()) {
                        final String err = "replica " +
                                           feederReplicaChannel.getNameIdPair()
                                                               .getName() +
                                           " fails security check during " +
                                           "streaming";
                        /* signal client */
                        makeSecurityCheckResponse(err);
                    }

                    writeAvailableEntries();

                    masterStatus.assertSync();

                    if (testDelayMs > 0) {
                        Thread.sleep(testDelayMs);
                    }
                }

            } catch (IOException | MasterObsoleteException |
                     InterruptedException e) {
                /* Benign "expected" exceptions */
                shutdownException = e;  /* Expected. */
            } catch (ReplicationSecurityException ure) {
                shutdownException = ure;
                /* dump warning if client is not authorized */
                LoggerUtils.warning(logger, repImpl,
                                    "Unauthorized replication stream " +
                                    "consumer " + ure.getConsumer() +
                                    ", exception: " + ure.getMessage());

            } catch (RuntimeException e) {
                shutdownException = e;

                /*
                 * An internal error. Shut down the rep node as well for now
                 * by throwing the exception out of the thread.
                 *
                 * In future we may want to close down just the impacted
                 * Feeder but this is the safe course of action.
                 */
                LoggerUtils.severe(logger, repImpl,
                                   "Unexpected runtime exception=" + e +
                                   ", stack=\n" +
                                   LoggerUtils.getStackTraceForSevereLog(e));
                throw e;
            } catch (Error e) {
                LoggerUtils.severe(logger, repImpl,
                                   "Unexpected error=" + e + ", stack=\n" +
                                   LoggerUtils.getStackTraceForSevereLog(e));
                feederOutputError = e;
                repNode.getRepImpl().invalidate(e);
            } finally {
                if (feederOutputError != null) {
                    /* Propagate the error, skip cleanup. */
                    throw feederOutputError;
                }
                LoggerUtils.info(logger, repImpl,
                                 "Feeder output for " +
                                 replicaNameIdPair.getName() +
                                 " shutdown. feeder VLSN: " + feederVLSN +
                                 " currentTxnEndVLSN: " +
                                 repNode.getCurrentTxnEndVLSN() +
                                 " batch buffer pending bytes:" +
                                 batchBuff.position());

                /*
                 * Shutdown the feeder in its entirety, in case the output
                 * thread is the only one to notice a problem. The Replica can
                 * decide to re-establish the connection
                 */
                shutdown(shutdownException);
                cleanup();
            }
        }

        /**
         * Write as many readily "available" log entries as possible to the
         * network. The term "available" is used in the sense that these values
         * are typically sitting around in the JE or FS cache especially for
         * messages that are recent enough to need timely acknowledgement. The
         * method tried to batch multiple entries, to minimize the number of
         * network calls permitting better packet utilization and fewer network
         * related interrupts, since FEEDER_TCP_NO_DELAY is set on the channel.
         *
         * The size of the batch is limited by one of:
         *
         * 1) The number of "available" trailing vlsn entries between the
         * current position of the feeder and the end of the log.
         *
         * 2) The size of the batchWriteBuffer.
         *
         * 3) The time it takes to accumulate the batch without exceeding the
         * minimum of:
         *
         *    a) heartbeat interval, a larger time window typically in effect
         *    when the replica is not in the ack window. It effectively favors
         *    batching.
         *
         *    b) (batchNs + time to first ack requiring) transaction,
         *    typically in effect when the replica is in the ack window and
         *    more timely acks are needed.
         *
         *    c) The max time allowed for batch accumulation is further
         *    restricted via MAX_BATCH_WAIT_NS, regardless of the heartbeat and
         *    batchNs settings.
         *
         * This adaptive time interval strategy effectively adapts the batch
         * sizes to the behavior needed of the replica at any given instant
         * in time.
         */
        private void writeAvailableEntries()
            throws DatabaseException, InterruptedException,
                   IOException, MasterObsoleteException {

            /*
             * Set the initial limit at the heartbeat and constrain it, if
             * the batch contains commits that need acks. The batchLimitNS
             * calculation is slightly sloppy in that it does not allow for
             * disk and network latencies, but that's ok, since heartbeats are
             * sent in another thread.  It's the feeder timeout that's the
             * main worry here; it's 30 sec by default and is set at 10s for
             * KVS, so lots of built in slop.
             */
            long nowNs = System.nanoTime();
            final long batchStartNs = nowNs;
            /*
             * Set the initial wait time to the heartbeat * 4, bounding it. It
             * will be adjusted downwards, if the batch contains commits that
             * needed acknowledgment.
             */
            long waitNs = Math.min(heartbeatNs * 4, MAX_BATCH_WAIT_NS);
            long batchLimitNs = batchStartNs + waitNs;
            boolean batchNeedsAcks = false;
            int nMessages = 0;
            batchBuff.clear();

            do {
                OutputWireRecord record =
                    feederSource.getWireRecord(
                        feederVLSN, waitNs,
                        (feederFilter != null ?
                            feederFilter.includeBeforeImage() :
                            false));

                try {
                    masterStatus.assertSync();

                    if (record == null) {
                        /* Caught up -- no more records from feeder source */
                        lastCommitTimestamp = repNode.getFeederTxns().
                            getLastCommitTimestamp().get();
                        lastCommitVLSN = repNode.getFeederTxns().
                            getLastCommitVLSN().get();
                        break;
                    }

                    /* update stats before applying filter */
                    final long txnId = record.getCommitAbortTxnId();
                    final long commitTimestamp = record.getCommitTimeStamp();
                    if (commitTimestamp != 0) {
                        lastCommitTimestamp = commitTimestamp;
                        lastCommitVLSN = record.getVLSN();
                    }

                    /* apply the filter if it is available */
                    if (feederFilter != null) {
                        if (isDurableOnly() && !durable(feederVLSN)) {
                            /*
                             * current entry is not durable yet, stop batching
                             * and flush existing entries in buffer if any
                             */
                            break;
                        }

                        if (feederFilter.execute(record, repImpl) == null) {
                            /* skip the record, go to the next VLSN */
                            feederVLSN = VLSN.getNext(feederVLSN);
                            nowNs = System.nanoTime();
                            if (batchNeedsAcks) {
                                waitNs = ackTxnBatchNs -
                                    (nowNs - batchStartNs);
                            }
                            continue;
                        }
                    }

                    if (commitToNetwork && txnId != 0) {
                        if (includeLocalDurableVLSN()) {
                            deemAcked(txnId, NULL_VLSN);
                        } else {
                            deemAcked(txnId);
                        }
                    }

                    if (isArbiterFeeder) {
                        feederVLSN = record.getVLSN();
                    }

                    validate(record);
                    final Message message = createMessage(txnId, record);

                    if (!batchNeedsAcks && (txnId != 0)) {
                        final Commit commit = (Commit) message;
                        if (commit.getNeedsAck()) {
                            batchNeedsAcks = true;
                            /*
                             * Tighten the time constraints if acks are needed.
                             * If no acks are needed accumulate until the
                             * buffer is full, that is, favor throughput
                             * over latency.
                             */
                            final long ackLimitNs =
                                batchStartNs + ackTxnBatchNs;
                            batchLimitNs = ackLimitNs < batchLimitNs ?
                                ackLimitNs : batchLimitNs;
                        }
                    }
                    assert TestHookExecute.doHookIfSet(
                        writeMessageHook, message);

                    nMessages = protocol.bufferWrite(
                        feederReplicaChannel, batchBuff,
                        ++nMessages, message);
                } finally {
                    /* Return LogItem buffer to buffer pool. */
                    if (record != null && envImpl.isValid()) {
                        record.decrementUse();
                    }
                }

                feederSentVLSN = feederVLSN;
                feederVLSN = VLSN.getNext(feederVLSN);
                nowNs = System.nanoTime();
                if (batchNeedsAcks) {
                    /* calculate the residual wait time for fetching the next
                     * record at the top of the loop */
                    waitNs = ackTxnBatchNs - (nowNs - batchStartNs);
                }
            } while ((testDelayMs == 0) && /* Don't batch if set by test. */
                     (vlsnIndex.getLatestAllocatedVal() >=
                      feederVLSN) /* Feeder caught up */ &&
                     (ackTxnBatchNs > 0) &&
                     ((nowNs - batchLimitNs) < 0) &&
                     !shutdown.get()) ;

            if (batchBuff.position() == 0) {
                /* No entries -- timed out waiting for one. */
                return;
            }

            /*
             * We have collected the largest possible batch given the
             * batching constraints, flush it out.
             */
            protocol.flushBufferedWrites(feederReplicaChannel,
                                         batchBuff,
                                         nMessages);
        }

        /**
         * Returns true if the given vlsn is durable, e.g., equal or less
         * than the valid DTVLSN
         * @param vlsn given vlsn
         * @return true if the given vlsn is durable, false otherwise
         */
        private boolean durable(long vlsn) {
            final long dtVLSN = repNode.getAnyDTVLSN();
            return (dtVLSN != NULL_VLSN) && (vlsn <= dtVLSN);
        }

        /**
         * Converts a log entry into a specific Message to be sent out by the
         * Feeder.
         *
         * @param txnId > 0 if the entry is a LOG_TXN_COMMIT
         *
         * @return the Message representing the entry
         *
         * @throws DatabaseException
         */
        private Message createMessage(long txnId,
                                      OutputWireRecord wireRecord)

            throws DatabaseException {

            /* A vanilla entry */
            if (txnId == 0) {
                return protocol.new Entry(wireRecord);
            }

            boolean needsAck;

            MasterTxn ackTxn = repNode.getFeederTxns().getAckTxn(txnId);
            SyncPolicy replicaSync = SyncPolicy.NO_SYNC;
            if (ackTxn != null) {
                ackTxn.stampRepWriteTime();
                long messageTransferMs = ackTxn.messageTransferMs();
                totalTransferDelay  += messageTransferMs;
                if (messageTransferMs > transferLoggingThresholdMs) {
                    final String message =
                        String.format("Feeder for: %s, Txn: %,d " +
                                      " log to rep stream time %,dms." +
                                      " Total transfer time: %,dms.",
                                      replicaNameIdPair.getName(),
                                      txnId, messageTransferMs,
                                      totalTransferDelay);
                    LoggerUtils.info(logger, repImpl, message);
                }

                /*
                 * Only request an acknowledgment if we are not committing to
                 * the network and DurabilityQuorum says the acknowledgment
                 * qualifies
                 */
                needsAck = !commitToNetwork &&
                    repNode.getDurabilityQuorum().replicaAcksQualify(
                        replicaNode);
                replicaSync = ackTxn.getCommitDurability().getReplicaSync();
            } else {

                /*
                 * Replica is catching up. Specify the weakest and leave it
                 * up to the replica.
                 */
                needsAck = false;
                replicaSync = SyncPolicy.NO_SYNC;
            }

            return protocol.new Commit(needsAck, replicaSync, wireRecord);
        }

        /**
         * Sanity check the outgoing record.
         */
        private void validate(OutputWireRecord record) {

            /* Check that we've fetched the right message. */
            if (record.getVLSN() != feederVLSN) {
                throw EnvironmentFailureException.unexpectedState
                    ("Expected VLSN:" + feederVLSN + " log entry VLSN:" +
                     record.getVLSN());
            }

            if (!repImpl.isRepConverted()) {
                assert record.verifyNegativeSequences("node=" + nameIdPair);
            }
        }
    }

    /**
     * Sends heartbeats to the replica at regular intervals. The heartbeats are
     * written by a dedicated thread to ensure that they are sent out on a
     * timely basis and are not delayed by I/O stalls, lock contention, etc.
     * Lack of heartbeats from a node is interpreted as the node having failed,
     * which could have an impact on availability at the application level as
     * the shard goes about holding an election, or is unable to supply needed
     * acknowledgments.
     *
     * Note that despite the use of the dedicated thread, heartbeats are still
     * susceptible to GC pauses, or network stalls, being interpreted as a
     * missing heartbeat but such false alarms should be infrequent in a
     * correctly configured store.
     *
     */
    private class HeartbeatThread extends OutputThread {
    	private volatile AtomicLongComponent replicaNHeartbeatSent;

        public HeartbeatThread() {
            super("Heartbeat Thread");
        }

        @Override
        protected int initiateSoftShutdown() {
            assert shutdown.get();
            return -1;
        }

        @Override
        public void run() {
        	replicaNHeartbeatSent =
                    feederManager.getReplicaNHeartbeatSentMap().createStat(
                        replicaNameIdPair.getName());
            protocol = Protocol.get(repNode, replicaNameIdPair,
                protocolVersion, protocolVersion, streamLogVersion);
            final String name = getReplicaNameIdPair().getName();
            Thread.currentThread().setName("Heartbeat Thread for " + name);

            Exception shutdownException = null;
            Error heartbeatError = null;
            try {
                while (!shutdown.get()) {
                    /*
                     * We skip the security check since the LogThread performs
                     * the check and they share the same channel.  The
                     * LogThread will shut this thread down if the security
                     * check fails.
                     */
                    Thread.sleep(repNode.getHeartbeatInterval());
                    if (shutdown.get()) {
                        break;
                    }
                    masterStatus.assertSync();
                    replicaNHeartbeatSent.add(1);
                    sendHeartbeat();
                }
            } catch (IOException | MasterObsoleteException |
                            InterruptedException e) {
                /* Benign "expected" exceptions */
                shutdownException = e;  /* Expected. */
            } catch (RuntimeException e) {
                shutdownException = e;

                /*
                 * An internal error. Shut down the rep node as well for now
                 * by throwing the exception out of the thread.
                 *
                 * In future we may want to close down just the impacted
                 * Feeder but this is the safe course of action.
                 */
                LoggerUtils.severe(logger, repImpl,
                    "Unexpected exception in Heartbeat Thread: " +
                    e.getMessage() + LoggerUtils.getStackTraceForSevereLog(e));
                throw e;
            } catch (Error e) {
                heartbeatError = e;
                repNode.getRepImpl().invalidate(e);
            } finally {
                if (heartbeatError != null) {
                    /* Propagate the error, skip cleanup. */
                    throw heartbeatError;
                }
                LoggerUtils.info(logger, repImpl, "Feeder heartbeat thread " +
                    replicaNameIdPair.getName() + " shutdown.");

                shutdown(shutdownException);
                cleanup();
            }
        }
    }

    private MasterTxn deemAcked(long txnId) {
        final MasterTxn txn =
            repNode.getFeederTxns().noteReplicaAck(replicaNode, txnId);
        if (txn == null) {
            /* Txn did not call for an ack,or this is a delayed ack and
             * a quorum of acks was already received.
             */
            return null;
        }
        final long commitVLSN = txn.getCommitVLSN();
        if (commitVLSN == INVALID_VLSN) {
            return null;
        }
        if (commitVLSN > replicaTxnEndVLSN) {
            replicaTxnEndVLSN = commitVLSN;

            if (txn.getPendingAcks() <= 0) {
                /*
                 * We could do better for ACK all, when we get a majority of
                 * acks but not all of them but we don't worry about optimizing
                 * this failure case. The heartbeat response will correct it.
                 */
                repNode.updateDTVLSN(replicaTxnEndVLSN);
            }
        }
        caughtUp = true;
        adviseMasterTransferProgress();
        return txn;
    }

    private MasterTxn deemAcked(long txnId, long replicaDurableVLSN) {
        final MasterTxn txn =
                repNode.getFeederTxns().noteReplicaAck(replicaNode, txnId);
        if (txn == null) {
            /* Txn did not call for an ack,or this is a delayed ack and
             * a quorum of acks was already received.
             */
            return null;
        }
        final long commitVLSN = txn.getCommitVLSN();
        if (commitVLSN == INVALID_VLSN) {
            return null;
        }

        if (commitVLSN > replicaTxnEndVLSN) {
            replicaTxnEndVLSN = commitVLSN;
        }
        /*
         * replicaDurableVLSN could be null_vlsn, if commitToNetwork == true
         */
        if (this.replicaDurableVLSN < replicaDurableVLSN) {
            this.replicaDurableVLSN = replicaDurableVLSN;

            if (txn.getPendingAcks() <= 0) {
                /*
                 * We could do better for ACK all, when we get a majority of
                 * acks but not all of them but we don't worry about optimizing
                 * this failure case. The heartbeat response will correct it.
                 */
                feederManager.updateDTVLSNWithLocalVLSN(replicaDurableVLSN);
            }
        }
        caughtUp = true;
        adviseMasterTransferProgress();
        return txn;
    }

    /**
     * Defines the handler for the RepNode thread. The handler invalidates the
     * environment by ensuring that an EnvironmentFailureException is in place.
     *
     * The handler communicates the cause of the exception back to the
     * FeederManager's thread by setting the repNodeShutdownException and then
     * interrupting the FM thread. The FM thread upon handling the interrupt
     * notices the exception and propagates it out in turn to other threads
     * that might be coordinating activities with it.
     */
    private class IOThreadsHandler implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LoggerUtils.severe(logger, repImpl,
                               "Uncaught exception in feeder thread " + t +
                               e.getMessage() +
                               LoggerUtils.getStackTraceForSevereLog(e));

            /* Bring the exception to the parent thread's attention. */
            feederManager.setRepNodeShutdownException
                (EnvironmentFailureException.promote
                 (repNode.getRepImpl(),
                  EnvironmentFailureReason.UNCAUGHT_EXCEPTION,
                  "Uncaught exception in feeder thread:" + t,
                  e));

            /*
             * Bring it to the FeederManager's attention, it's currently the
             * same as the rep node's thread.
             */
            repNode.interrupt();
        }
    }

    /**
     * A marker exception that wraps the real exception. It indicates that the
     * impact of wrapped exception can be contained, that is, it's sufficient
     * cause to exit the Feeder, but does not otherwise impact the RepNode.
     */
    @SuppressWarnings("serial")
    public static class ExitException extends Exception
        implements NotSerializable {
        /*
         * If true, cause the remote replica to throw an EFE instead of
         * retrying.
         */
        final boolean failReplica;

        public ExitException(String message) {
            super(message);
            this.failReplica = true;
        }

        public ExitException(Throwable cause,
                             boolean failReplica) {
            super(cause);
            this.failReplica = failReplica;
        }

        public boolean failReplica() {
            return failReplica;
        }
    }

    /**  For debugging and exception messages. */
    public String dumpState() {
        return "feederVLSN=" + feederVLSN +
                " replicaTxnEndVLSN=" + replicaTxnEndVLSN +
            ((replicaNode != null) && !replicaNode.getType().isElectable() ?
             " nodeType=" + replicaNode.getType() :
             "");
    }

    /**
     * Set a test hook that will be called before sending a message using the
     * protocol's write method, supplying the hook with the message as an
     * argument.
     */
    public void setWriteMessageHook(final TestHook<Message> writeMessageHook) {
        this.writeMessageHook = writeMessageHook;
    }

    /**
     * Get the test hook to be called before sending a message using the
     * protocol's write method.
     */
    public TestHook<Message> getWriteMessageHook() {
        return writeMessageHook;
    }

    /**
     * Set the value of the write message hook that will be used for newly
     * created feeders.
     */
    public static void setInitialWriteMessageHook(
        final TestHook<Message> initialWriteMessageHook) {

        Feeder.initialWriteMessageHook = initialWriteMessageHook;
    }

    /* Returns if feeder needs to do security checks */
    public boolean needSecurityChecks() {

        /* no check for non-secure store without an authenticator */
        final StreamAuthenticator authenticator = getAuthenticator();
        if (authenticator == null) {
            return false;
        }

        final DataChannel channel = feederReplicaChannel.getChannel();
        return DataChannel.needSecurityCheck(channel);
    }

    /**
     * Authenticates the replication stream consumer and checks authorization
     *
     * @return false if fail the security check, true otherwise
     */
    private boolean doSecurityCheck() {

        if (!needSecurityChecks()) {
            return true;
        }

        final StreamAuthenticator authenticator = getAuthenticator();
        final long curr = TimeSupplier.currentTimeMillis();
        if ((curr - authenticator.getLastCheckTimeMs()) >= securityChkIntvMs) {
            /* both authentication and authorization */
            return authenticator.checkAccess();
        }

        return true;
    }

    /**
     * Re-authenticates the stream consumer if applicable
     *
     * @param msg reauth message
     * @return false if fail to reauthenticate, true otherwise.
     */
    private boolean processReauthenticate(Message msg) {

        /* ignore if replica is not an external node */
        if (!getReplicaNode().getType().isExternal()) {
            return true;
        }

        /* ignore the message if no authentication is enabled */
        final StreamAuthenticator authenticator = getAuthenticator();
        if (authenticator == null) {
            return true;
        }

        final Protocol.ReAuthenticate reauth = (Protocol.ReAuthenticate)msg;
        authenticator.setToken(reauth.getTokenBytes());
        /* both authentication and authorization */
        final boolean ret = authenticator.checkAccess();
        LoggerUtils.info(logger, repImpl,
                         "Feeder=" + nameIdPair + " reauthenticate" +
                         " result=" + (ret ? "success" : "failure") +
                         ", channel id=" + authenticator.getChannelId());
        return ret;
    }

    /**
     * Sends a security check response to client and if failure, wait for a
     * grace period before throwing an exception to caller
     *
     * @param err      error message sent to client
     */
    public void makeSecurityCheckResponse(String err)
        throws ReplicationSecurityException {

        final Protocol proto = inputThread.protocol;
        final Protocol.SecurityFailureResponse response =
            proto.new SecurityFailureResponse(err);
        final String replica = feederReplicaChannel.getNameIdPair().getName();
        try {
            proto.write(response, feederReplicaChannel);
            LoggerUtils.fine(logger, repImpl, "Need to shut down after " +
                             StreamAuthenticator.SECURITY_FAILURE_WAIT_TIME_MS +
                             " ms, security failure message sent: " + err);

            Thread.sleep(StreamAuthenticator.SECURITY_FAILURE_WAIT_TIME_MS);
        } catch (InterruptedException ie) {
            LoggerUtils.fine(logger, repImpl, "Interrupted in sleep, ignore");
        } catch (IOException ioe) {
            LoggerUtils.warning(logger, repImpl,
                                "Fail to send security failure message to " +
                                "replica " + replica +
                                ", message if fail to pass " + err);
        }

        throw new ReplicationSecurityException(err, replica, null);
    }

    /**
     * Returns true if the replica node is a subscription, only external
     * or secondary node is allowed in subscription
     */
    public boolean isSubscription() {
        if (replicaNode == null) {
            return false;
        }
        final NodeType type = replicaNode.getType();
        return type.isExternal() || type.isSecondary();
    }

    /**
     * Returns true if the feeder requires to stream durable entries only,
     * false otherwise;
     */
    private boolean isDurableOnly() {
        final boolean durable =
            feederFilter != null && feederFilter.durableEntriesOnly();
        return isSubscription() && durable;
    }
}
