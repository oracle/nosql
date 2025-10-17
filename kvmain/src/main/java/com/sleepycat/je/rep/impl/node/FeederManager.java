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

import static com.sleepycat.je.rep.QuorumPolicy.SIMPLE_MAJORITY;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_FEEDERS_CREATED;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_FEEDERS_SHUTDOWN;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_TXN_AUTH_FAILED;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_TXN_AUTH_NO_WAIT;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_TXN_AUTH_SUCCEED;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.N_TXN_AUTH_WAITED;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_95_DELAY_MS_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_99_DELAY_MS_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_AVG_ACK_LATENCY_NS_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_AVG_DELAY_MS_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_DELAY_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LAST_COMMIT_TIMESTAMP_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LAST_COMMIT_VLSN_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_LOCAL_DURABLE_VLSN_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_MAX_DELAY_MS_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_VLSN_LAG_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_VLSN_RATE_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_N_HEARTBEAT_RECEIVED_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.REPLICA_N_HEARTBEAT_SENT_MAP;
import static com.sleepycat.je.rep.impl.node.FeederManagerStatDefinition.TXN_AUTH_AVG_WAIT;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Elections;
import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.impl.node.MasterTransfer.VLSNProgress;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition;
import com.sleepycat.je.rep.utilint.IntRunningTotalStat;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.AtomicLongMapStat;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LatencyPercentileMapStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvgMapStat;
import com.sleepycat.je.utilint.LongAvgRateMapStat;
import com.sleepycat.je.utilint.LongDiffMapStat;
import com.sleepycat.je.utilint.LongMaxMapStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.PollCondition;
import com.sleepycat.je.utilint.RateLimitingLogger;
import com.sleepycat.je.utilint.StatGroup;

/**
 * FeedManager is responsible for the creation and management of the Feeders
 * used to respond to connections initiated by a Replica. runFeeders() is the
 * central loop that listens for replica connections and manages the lifecycle
 * of individual Feeders. It's re-entered each time the node becomes a Master
 * and is exited when its status changes.
 *
 * There is a single instance of FeederManager that is created for a
 * replication node. There are many instances of Feeders per FeederManager.
 * Each Feeder instance represents an instance of a connection between the node
 * serving as the feeder and the replica.
 *
 * Note that the FeederManager and the Replica currently reuse the Replication
 * node's thread of control. When we implement r2r we will need to revisit the
 * thread management to provide for concurrent operation of the FeederManger
 * and the Replica.
 */
final public class FeederManager {

    private final RepNode repNode;

    /*
     * The queue into which the ServiceDispatcher queues socket channels for
     * new Feeder instances.
     */
    private final BlockingQueue<DataChannel> channelQueue =
        new LinkedBlockingQueue<>();

    /*
     * Feeders are stored in either nascentFeeders or activeFeeders, and not
     * both.  To avoid deadlock, if locking both collections, lock
     * nascentFeeders first and then activeFeeders.
     */

    /*
     * Nascent feeders that are starting up and are not yet active. They have
     * network connections but have not synched up or completed handshakes.
     * They are moved into the active feeder map, once they become active.
     */
    private final Set<Feeder> nascentFeeders =
        Collections.synchronizedSet(new HashSet<Feeder>());

    /*
     * The collection of active feeders currently feeding replicas. The map is
     * indexed by the Replica's node name. Access to this map must be
     * synchronized, since it's updated concurrently by the Feeders that share
     * it.
     *
     * A feeder is considered to be active after it has completed the handshake
     * sequence with its associated Replica.
     */
    private final ConcurrentHashMap<String, Feeder> activeFeeders;

    /**
     * The number of feeders currently being managed by the FeederManager. This
     * includes feeders that are in the process of starting up or shutting down.
     * The count is increments when a Feeder is first created and is decremented
     * after all cleanup associated with the Feeder has been completed.
     */
    private final AtomicInteger managedFeederCount = new AtomicInteger(0);

    /*
     * The number of active ack feeders feeding electable, i.e. acking, nodes.
     */
    private final AtomicInteger ackFeeders = new AtomicInteger(0);

    /*
     * The number of arbiter feeders; there can only be one currently. It's
     * Atomic for consistency with ackFeeders.
     */
    private final AtomicInteger arbiterFeeders = new AtomicInteger(0);
    private String arbiterFeederName;

    /**
     * Object on which transactions wait while the master is being determined
     * to be authoritative.
     */
    private final AuthoritativeMasterWaiter authMasterWaiter;

    /**
     * The master is authoritative until the current time passes the time saved
     * in authorizedWindow.
     */
    private volatile long authoritativeWindow;

    private final RateLimitingLogger<FeederManager> nonAuthMasterLogger;

    /**
     * For testing only.
     *
     * Makes it so that the master always appears to not be authoritative.
     */
    private volatile boolean preventAuthoritative = false;

    /*
     * Disables use of new auth master functionality, reverting to old
     * style deprecated behavior. Switch present for testing during transition
     * away from old behavior. Eliminate at first opportunity.
     */
    private final boolean disableAuthoritativeMaster;

    /*
     * A test delay introduced in the feeder loop to simulate a loaded master.
     * The feeder waits this amount of time after each message is sent.
     */
    private int testDelayMs = 0;

    /* Set to true to force a shutdown of the FeederManager. */
    AtomicBoolean shutdown = new AtomicBoolean(false);

    /*
     * Non null if the replication node must be shutdown as well. This is
     * typically the result of an unexpected exception in the feeder.
     */
    private RuntimeException repNodeShutdownException;

    /**
     * Used to manage the flushing of the DTVLSN via a null TXN commit
     * when appropriate.
     */
    private final DTVLSNFlusher dtvlsnFlusher;

    private final Logger logger;

    /* FeederManager statistics. */
    private final StatGroup stats;
    private final IntStat nFeedersCreated;
    private final IntStat nFeedersShutdown;
    private final AtomicLongMapStat replicaDelayMap;
    private final AtomicLongMapStat replicaNHeartbeatSentMap;
    private final AtomicLongMapStat replicaNHeartbeatReceivedMap;
    private final LongAvgMapStat replicaAvgDelayMsMap;
    private final LatencyPercentileMapStat replica95DelayMsMap;
    private final LatencyPercentileMapStat replica99DelayMsMap;
    private final LongMaxMapStat replicaMaxDelayMsMap;
    private final AtomicLongMapStat replicaLastCommitTimestampMap;
    private final AtomicLongMapStat replicaLastCommitVLSNMap;
    private final AtomicLongMapStat replicaLocalDurableVLSNMap;
    private final LongDiffMapStat replicaVLSNLagMap;
    private final LongAvgRateMapStat replicaVLSNRateMap;
    private final LongAvgMapStat replicaAckLatencyMap;
    private final LongStat nTxnAuthWaited;
    private final LongStat nTxnAuthNoWait;
    private final LongStat nTxnAuthFailed;
    private final LongStat nTxnAuthSucceed;
    private final LongStat txnAuthAvgWait;

    /* The poll timeout used when accepting feeder connections. */
    public final long pollTimeoutMs ;

    /* Identifies the Feeder Service. */
    public static final String FEEDER_SERVICE = "Feeder";

    /** The moving average period in milliseconds */
    private static final long MOVING_AVG_PERIOD_MILLIS = 10000;

    /*
     * The total amount of time to wait for all Feeders to exit when the
     * FeederManager is shutdown. The value is generously large, since we
     * typically expect Feeders to exit rapidly when shutdown. If the
     * FeederManager takes longer than this time period to shutdown, it's taken
     * to be an indication of a problem and a full thread dump is logged to help
     * identify the tardy feeder.
     */
    private static final int FEEDER_SHUTDOWN_WAIT_MILLIS = 30000;

    FeederManager(RepNode repNode) {
        this.repNode = repNode;
        activeFeeders = new ConcurrentHashMap<>();
        logger = LoggerUtils.getLogger(getClass());
        stats = new StatGroup(FeederManagerStatDefinition.GROUP_NAME,
                              FeederManagerStatDefinition.GROUP_DESC);
        nFeedersCreated = new IntRunningTotalStat(stats, N_FEEDERS_CREATED);
        nFeedersShutdown = new IntRunningTotalStat(stats, N_FEEDERS_SHUTDOWN);

        /*
         * Treat lags as valid for twice the heartbeat interval, to allow for
         * minor networking delays when receiving heartbeats; delays represent
         * network roundtrip latencies when there are no commits.
         */
        final long validityMillis = 2 * repNode.getHeartbeatInterval();
        replicaDelayMap =
            new AtomicLongMapStat(stats, REPLICA_DELAY_MAP);
        replicaAvgDelayMsMap =
            new LongAvgMapStat(stats, REPLICA_AVG_DELAY_MS_MAP);
        replica95DelayMsMap = new LatencyPercentileMapStat(
            stats, REPLICA_95_DELAY_MS_MAP, 0.95f);
        replica99DelayMsMap = new LatencyPercentileMapStat(
            stats, REPLICA_99_DELAY_MS_MAP, 0.99f);
        replicaMaxDelayMsMap =
            new LongMaxMapStat(stats, REPLICA_MAX_DELAY_MS_MAP);
        replicaLastCommitTimestampMap =
            new AtomicLongMapStat(stats, REPLICA_LAST_COMMIT_TIMESTAMP_MAP);
        replicaLastCommitVLSNMap =
            new AtomicLongMapStat(stats, REPLICA_LAST_COMMIT_VLSN_MAP);
        replicaLocalDurableVLSNMap =
                new AtomicLongMapStat(stats, REPLICA_LOCAL_DURABLE_VLSN_MAP);
        replicaVLSNLagMap =
            new LongDiffMapStat(stats, REPLICA_VLSN_LAG_MAP, validityMillis);
        replicaVLSNRateMap = new LongAvgRateMapStat(
            stats, REPLICA_VLSN_RATE_MAP, MOVING_AVG_PERIOD_MILLIS, MINUTES);
        replicaAckLatencyMap =
            new LongAvgMapStat(stats, REPLICA_AVG_ACK_LATENCY_NS_MAP);
        replicaNHeartbeatSentMap =
                new AtomicLongMapStat(stats, REPLICA_N_HEARTBEAT_SENT_MAP);
        replicaNHeartbeatReceivedMap =
                new AtomicLongMapStat(stats, REPLICA_N_HEARTBEAT_RECEIVED_MAP);

        nTxnAuthWaited = new LongStat(stats, N_TXN_AUTH_WAITED);
        nTxnAuthNoWait = new LongStat(stats, N_TXN_AUTH_NO_WAIT);
        nTxnAuthFailed = new LongStat(stats, N_TXN_AUTH_FAILED);
        nTxnAuthSucceed = new LongStat(stats, N_TXN_AUTH_SUCCEED);
        txnAuthAvgWait = new LongStat(stats, TXN_AUTH_AVG_WAIT);

        pollTimeoutMs = repNode.getConfigManager().
            getDuration(RepParams.FEEDER_MANAGER_POLL_TIMEOUT);
        dtvlsnFlusher = new DTVLSNFlusher();
        authMasterWaiter = new AuthoritativeMasterWaiter();
        disableAuthoritativeMaster = repNode.getConfigManager().
            getBoolean(RepParams.DISABLE_AUTHORITATIVE_MASTER);
        nonAuthMasterLogger = new RateLimitingLogger<>(
            (int) TimeUnit.MINUTES.toMillis(1), 10, logger);
    }

    /**
     * Returns the statistics associated with the FeederManager.
     *
     * @return the statistics
     */
    public StatGroup getFeederManagerStats(StatsConfig config) {

        synchronized (stats) {
            return stats.cloneGroup(config.getClear());
        }
    }

    /* Get the protocol stats for this FeederManager. */
    public StatGroup getProtocolStats(StatsConfig config) {
        /* Aggregate stats that have not yet been aggregated. */
        StatGroup protocolStats =
            new StatGroup(BinaryProtocolStatDefinition.GROUP_NAME,
                          BinaryProtocolStatDefinition.GROUP_DESC);
        activeFeeders.forEachValue(Long.MAX_VALUE, (feeder) -> {
            protocolStats.addAll(feeder.getProtocolStats(config));
        });

        return protocolStats;
    }

    /* Reset the feeders' stats of this FeederManager. */
    public void resetStats() {
        synchronized (stats) {
            stats.clear();
        }
        activeFeeders.forEachValue(Long.MAX_VALUE, feeder -> {
            feeder.resetStats();
        });
    }

    /**
     * Accumulates statistics from a terminating feeder.
     * @param feederStats  stats of feeder
     */
    void incStats(StatGroup feederStats) {
        synchronized (stats) {
            stats.addAll(feederStats);
        }
    }

    public int getTestDelayMs() {
        return testDelayMs;
    }

    public void setTestDelayMs(int testDelayMs) {
        this.testDelayMs = testDelayMs;
    }

    public void setPreventAuthoritative(boolean prevent) {
        preventAuthoritative = prevent;
    }

    /**
     * Returns the RepNode associated with the FeederManager
     * @return
     */
    RepNode repNode() {
        return repNode;
    }

    /**
     * Returns the Feeder associated with the node, if such a feeder is
     * currently active.
     */
    public Feeder getFeeder(String nodeName) {
        return activeFeeders.get(nodeName);
    }

    public Feeder getArbiterFeeder() {
        return activeFeeders.get(arbiterFeederName);
    }

    /*
     * For test use only.
     */
    public Feeder putFeeder(String nodeName, Feeder feeder) {
        /*
         * Can't check for an electable node since the feeder object can be
         * mocked for testing so it does not have a rep node.
         */
        ackFeeders.incrementAndGet();
        return activeFeeders.put(nodeName, feeder);
    }

    AtomicLongMapStat getReplicaDelayMap() {
        return replicaDelayMap;
    }

    LongAvgMapStat getReplicaAckLatencyMap() {
        return replicaAckLatencyMap;
    }

    LongAvgMapStat getReplicaAvgDelayMsMap() {
        return replicaAvgDelayMsMap;
    }

    LatencyPercentileMapStat getReplica95DelayMsMap() {
        return replica95DelayMsMap;
    }

    LatencyPercentileMapStat getReplica99DelayMsMap() {
        return replica99DelayMsMap;
    }

    LongMaxMapStat getReplicaMaxDelayMsMap() {
        return replicaMaxDelayMsMap;
    }

    AtomicLongMapStat getReplicaLastCommitTimestampMap() {
        return replicaLastCommitTimestampMap;
    }

    AtomicLongMapStat getReplicaLastCommitVLSNMap() {
        return replicaLastCommitVLSNMap;
    }

    AtomicLongMapStat getReplicaLocalDurableVLSNMap() {
        return replicaLocalDurableVLSNMap;
    }

    LongDiffMapStat getReplicaVLSNLagMap() {
        return replicaVLSNLagMap;
    }

    LongAvgRateMapStat getReplicaVLSNRateMap() {
        return replicaVLSNRateMap;
    }
    
    AtomicLongMapStat getReplicaNHeartbeatSentMap() {
    	return replicaNHeartbeatSentMap;
    }

    AtomicLongMapStat getReplicaNHeartbeatReceivedMap() {
    	return replicaNHeartbeatReceivedMap;
    }
    
    void setRepNodeShutdownException(RuntimeException rNSE) {
        this.repNodeShutdownException = rNSE;
    }

    /**
     * The numbers of Replicas currently "active" with this feeder. Active
     * currently means they are connected. It does not make any guarantees
     * about where they are in the replication stream. They may, for example,
     * be too far behind to participate in timely acks.
     *
     * @return the active replica count
     */
    public int activeReplicaCount() {
        return activeFeeders.size();
    }

    public int activeAckReplicaCount() {
        return ackFeeders.get();
    }

    public int activeAckArbiterCount() {
        return arbiterFeeders.get();
    }

    public long txnAuthWaitedCount() {
        return nTxnAuthWaited.get();
    }

    public long txnAuthNoWaitCount() {
        return nTxnAuthNoWait.get();
    }

    public long txnAuthFailedCount() {
        return nTxnAuthFailed.get();
    }

    public long txnAuthSucceedCount() {
        return nTxnAuthSucceed.get();
    }

    public long txnAuthAverageWait() {
        return txnAuthAvgWait.get();
    }

    /**
     * Returns the set of Replicas that are currently active with this feeder.
     * A replica is active if it has completed the handshake sequence.
     *
     * @return the set of replica node names
     */
    public Set<String> activeReplicas() {
        /*
         * Create a copy to avoid inadvertent concurrency conflicts, since the
         * keySet is a view of the underlying map.
         */
        return new HashSet<>(activeFeeders.keySet());
    }

    /**
     * Returns if the given time is within the window of time in which we know
     * the master is still authorized.
     * @param timeMs is the time to check in milliseconds since UTC
     * @return true if within authoritative window
     */
    public boolean withinAuthoritativeWindow(final long timeMs) {
        if (authoritativeWindow > timeMs) {
            return true;
        }
        return false;
    }

    /**
     * Returns a definitive answer to whether this node is currently the master
     * by checking both its status as a master and that a sufficient number
     * of nodes agree that it's the master based on the number of feeder
     * connections to it. Currently, the sufficient number is just a simple
     * majority. Such an authoritative answer is needed in a network partition
     * situation to detect a master that may be isolated on the minority side
     * of a network partition.
     *
     * @return true if the node is definitely the master. False if it's not or
     * we cannot be sure.
     */
    boolean isAuthoritativeMaster(
        final MasterStatus masterStatus, final Durability durable) {
        // Used in testing.
        if (preventAuthoritative) {
            return false;
        }
        final ElectionQuorum quorum = repNode.getElectionQuorum();

        if (disableAuthoritativeMaster) {
            return quorum.isAuthoritativeMasterOld(masterStatus, this);
        }

        if (!masterStatus.isGroupMaster()) {
            return false;
        }

        /*
         * If not enough time has passed since last authorizing the master
         * to elect a new one then no need to do the expensive check.
         */
        final long currentTime = TimeSupplier.currentTimeMillis();
        if (withinAuthoritativeWindow(currentTime)) {
            return true;
        }

        /*
         * Check that there are not enough missing or lagging replicas to
         * elect a new master if there is a network partition.
         */
        final int master = 1;
        final int simpleQuorumSize = quorum.
            getElectionQuorumSize(SIMPLE_MAJORITY);
        /* Total number of feeders currently active. */
        final int activeReplicas =
            activeAckReplicaCount() + activeAckArbiterCount();
        if ((activeReplicas + master) < simpleQuorumSize ) {
            /*
             * If using arbiters to bypass the durability quorum requirement do
             * not do authoritative master check, otherwise arbiters would be
             * useless since they really only take effect when there is no
             * quorum.
             */
            if (durable != null) {
                ReplicaAckPolicy policy = durable.getReplicaAck();
                if (policy != null) {
                    final int requiredAcks = repNode.getDurabilityQuorum().
                        getCurrentRequiredAckCount(policy);
                   if (activeReplicas >= requiredAcks) {
                        return true;
                   }
                }
            }
        }
        long minElectionTime = repNode.getElections().getMinElectionTime();
        /*
         * If the default election delay is ever set to 0, it would make it
         * impossible to ever authorize the master, so use the default
         * value in that case.
         */
        if (minElectionTime == 0) {
            minElectionTime = Elections.DEFAULT_MIN_ELECTION_TIME;
        }

        int caughtUpReplicas = 0;
        long oldestResponse = currentTime;
        synchronized (activeFeeders) {
            for (final Feeder feeder: activeFeeders.values()) {
                final RepNodeImpl replica = feeder.getReplicaNode();
                final long respTime = feeder.getLastResponseTime();
                /*
                 * TODO: It would be nice to get the most recent oldest
                 * response time that still insures enough replicas have
                 * responded to prevent another master from being elected.
                 */
                if (quorum.nodeTypeParticipates(replica.getType()) &&
                    ((currentTime - respTime) < minElectionTime)) {
                    caughtUpReplicas++;
                    if (respTime < oldestResponse) {
                        oldestResponse = respTime;
                    }
                    if (simpleQuorumSize <= (caughtUpReplicas + master)) {
                        break;
                    }
                }
            }
        }
        if (simpleQuorumSize > (caughtUpReplicas + master)) {
            return false;
        }
        /*
         * It is possible to have a concurrency issue here and have
         * authoritativeWindow be set to an older value than it is current.
         * That is okay, it just means we might get a false negative when
         * checking if the master is in the authorized window.
         */
        final long newAuthWindow = oldestResponse + minElectionTime;
        if (newAuthWindow > authoritativeWindow) {
            authoritativeWindow = newAuthWindow;
            if (logger.isLoggable(Level.FINE)) {
                LoggerUtils.fine(logger, repNode.getRepImpl(),
                            "New authorizedWindow " + authoritativeWindow);
            }
        }
        return true;
    }

    /**
     * Gets the names of electable replicas that have communicated with the
     * master recently.  Only used when throwing an
     * InsufficientReplicaException.
     */
    public Set<String> getCaughtUpReplicas() {
        final ElectionQuorum quorum = repNode.getElectionQuorum();
        final long currentTime = TimeSupplier.currentTimeMillis();
        final long minElectionTime =
            repNode.getElections().getMinElectionTime();
        synchronized (activeFeeders) {
            return activeFeeders.values().stream().
                   filter(f -> quorum.
                          nodeTypeParticipates(f.getReplicaNode().getType()) &&
                          (currentTime - f.getLastResponseTime()) <
                           minElectionTime).
                   map(f -> f.getReplicaNameIdPair().getName()).
                   collect(Collectors.toSet());
        }
    }

    /**
     * Calculate the average wait time by multiplying the previous average wait
     * time by the previous total times waiting, adding the new wait time,
     * then dividing it by the total wait time.
     */
    private void calculateAuthAverageWait(final long startTime) {
        final long total = nTxnAuthSucceed.get() + nTxnAuthFailed.get();
        final long previousTotal = total - 1;
        final long wait = startTime - TimeSupplier.currentTimeMillis();
        if (total < 2) {
            txnAuthAvgWait.set(wait);
        } else {
            final long avg =
                (((previousTotal) * txnAuthAvgWait.get()) + wait)/total;
            txnAuthAvgWait.set(avg);
        }
    }

    /**
     * This class exists only to create an object on which transactions can
     * call {@link java.lang.Object#wait(long)} while waiting for the master
     * to be determined as authoritative.
     */
    private class AuthoritativeMasterWaiter {
        /**
         * Number of transactions waiting for the master to be authoritative.
         */
        private int waiters = 0;

        public synchronized int getWaiters() {
            return waiters;
        }

        public synchronized void incrementWaiters() {
            waiters++;
        }

        public synchronized void decrementWaiters() {
            waiters--;
        }
    };

    /**
     * Wake up all threads waiting for the master to be authoritative.
     */
    public void notifyAllAuthMasterWaiter() {
        synchronized (authMasterWaiter) {
            if (authMasterWaiter.getWaiters() != 0) {
                authMasterWaiter.notifyAll();
            }
        }
    }

    /**
     * Used only in testing.
     */
    public int getAuthWaiters() {
    	return authMasterWaiter.getWaiters();
    }

    /*
     * Check if a change of state has happened while waiting for authoritative
     * master that will prevent the current master from become authoritative.
     * This includes if the node shuts down, the node is no longer the master,
     * the FeederManager is shutting down, the environment is no longer valid,
     * or the group and the node are not in sync on who is the master.
     */
    private void checkStateChange(Txn txn) {
        if (repNode.isShutdown() || !repNode.isMaster() ||
            shutdown.get() || !repNode.getRepImpl().isValid()) {
            nTxnAuthFailed.increment();
            LoggerUtils.info(logger, repNode.getRepImpl(),
                            "Transaction " + txn.getId() +
                            " failed waiting for authoritative master " +
                            "because the node state changed.");
            /*
             * During master transfer there is a small window of time when the
             * FeederManager is shutdown but the node is still marked as
             * master.
             */
            if (!repNode.isMaster() || !repNode.getMasterStatus().inSync() ||
                (shutdown.get() &&
                (!repNode.isShutdown() || !repNode.getRepImpl().isValid()))) {
                if (txn.isMasterTxn()) {
                    throw new ReplicaWriteException(txn,
                        repNode.getRepImpl().getStateChangeEvent());
                }
                throw new ReplicaStateException("Transaction " +
                    txn.getId() + " failed because master state changed.");
            }
            throw EnvironmentFailureException.unexpectedState(
                            "Transaction " + txn.getId() +
                            " failed because Environment is closed or is" +
                            " no longer valid.");
        }
    }

    /**
     * Confirms that this node is the master, and that no other node in the
     * group has also been elected master (such as due to a network partition).
     * The method waits for the authoritative wait timeout if need be for the
     * master to confirm it's authoritative. Null Txns never wait under any
     * circumstance and do not wait here either, but simply return.
     *
     * @param txn waiting for the node to be authorized as the master.
     *
     * @throws InsufficientReplicasException if the master cannot be confirmed
     * as authoritative within the timeout period.
     */
    public void awaitAuthoritativeMaster(Txn txn)
                    throws InterruptedException {

        checkStateChange(txn);
        if (txn.isNullTxn()) {
            /* Null txns don't wait. */
            return;
        }
        final Durability durable = txn.getDefaultDurability();
        if (isAuthoritativeMaster(repNode.getMasterStatus(), durable)) {
            nTxnAuthSucceed.increment();
            nTxnAuthNoWait.increment();
            return;
        }
        /* Calculate the transaction timeout. */
        final long startTime = TimeSupplier.currentTimeMillis();
        long wait = txn.getAuthoritativeTimeout();
        final long timeoutTime = startTime + wait;

        /*
         * Waiting threads can be woken up by spurious wakeups, so place
         * the call to wait in a loop that re-checks if the master is
         * authorized.
         */
        do {
            long currentTime = TimeSupplier.currentTimeMillis();
            if (logger.isLoggable(Level.FINE)) {
                LoggerUtils.fine(logger, repNode.getRepImpl(),
                 "Transaction " + txn.getId() +
                 " waiting for authoritive master for " + wait +
                 "ms from " + startTime);
            }

            if (wait > 0) {
                synchronized (authMasterWaiter) {
                    authMasterWaiter.incrementWaiters();
                    try {
                        if (!withinAuthoritativeWindow(currentTime)) {
                            authMasterWaiter.wait(wait);
                        }
                    } finally {
                        authMasterWaiter.decrementWaiters();
                    }
                }
            }
            /* Break loop if a fatal change has occurred. */
            checkStateChange(txn);

            /*
             * Double check that the thread actually timed out.
             */
            currentTime = TimeSupplier.currentTimeMillis();
            if (timeoutTime <= currentTime) {
                if (durable != null) {
                    ReplicaAckPolicy policy = durable.getReplicaAck();
                    if (ReplicaAckPolicy.SIMPLE_MAJORITY.equals(policy) &&
                        !repNode.getArbiter().isActive() &&
                        repNode.getArbiter().activateArbitration()) {
                        return;
                    }
                }
                final ElectionQuorum quorum = repNode.getElectionQuorum();
                LoggerUtils.logMsg(nonAuthMasterLogger, repNode.getRepImpl(),
                    this, Level.INFO,
                    "Transaction " + txn.getId() +
                    " timed out waiting for authoritative master at " +
                    Instant.ofEpochMilli(currentTime).toString()  +
                    " auth window:" +
                    Instant.ofEpochMilli(authoritativeWindow).toString()
                    );
                nTxnAuthFailed.increment();
                calculateAuthAverageWait(startTime);
                throw new InsufficientReplicasException(
                    txn,
                    quorum.getElectionQuorumSize(SIMPLE_MAJORITY) - 1,
                    getCaughtUpReplicas());
            }
            wait = timeoutTime - currentTime;
        } while (!isAuthoritativeMaster(repNode.getMasterStatus(), durable));
        final String fmt = "Transaction %,d succeeded after waiting %,d ms " +
             "for authorized master";
        final long elapsedMs = TimeSupplier.currentTimeMillis() - startTime;
        LoggerUtils.info(logger, repNode.getRepImpl(),
                         String.format(fmt, txn.getId(), elapsedMs));
        nTxnAuthSucceed.increment();
        calculateAuthAverageWait(startTime);
    }

    /**
     * Returns the set of active replicas and arbiters, that are currently
     * active with this feeder and are supplying acknowledgments. A replica is
     * active if it has completed the handshake sequence. An Arbiter is only
     * returned if it's in active arbitration.
     *
     * @param includeArbiters include active arbiters in the list of returned
     * node names if true; exclude arbiters otherwise.
     *
     * @return the set of replica and if includeArbiters active arbiter node names
     */
    public  Set<String> activeAckReplicas(boolean includeArbiters) {
        final Set<String> nodeNames = new HashSet<>();
        activeFeeders.forEachEntry(Long.MAX_VALUE, (entry) -> {
            final Feeder feeder = entry.getValue();

            /* The replica node should be non-null for an active feeder */
            final RepNodeImpl replica = feeder.getReplicaNode();
            if (!replica.getType().isElectable()) {
                return;
            }

            if (replica.getType().isArbiter()) {
                if (!includeArbiters ||
                    !feeder.getRepNode().getArbiter().isActive()) {
                    /* Skip the arbiter. */
                    return;
                }
            }

            nodeNames.add(entry.getKey());
        });
        return nodeNames;
    }

    public Map<String, Feeder> activeReplicasMap() {
        return new HashMap<>(activeFeeders);
    }

    /**
     * Transitions a Feeder to being active, so that it can be used in
     * considerations relating to commit acknowledgments and decisions about
     * choosing feeders related to system load.
     *
     * @param feeder the feeder being transitioned.
     */
    void activateFeeder(Feeder feeder) {
        synchronized (nascentFeeders) {
            synchronized (activeFeeders) {
                boolean removed = nascentFeeders.remove(feeder);
                if (feeder.isShutdown()) {
                    return;
                }
                assert(removed);
                String replicaName = feeder.getReplicaNameIdPair().getName();
                assert(!feeder.getReplicaNameIdPair().equals(NameIdPair.NULL));
                Feeder dup = activeFeeders.get(replicaName);
                if ((dup != null) && !dup.isShutdown()) {
                    throw EnvironmentFailureException.
                        unexpectedState(repNode.getRepImpl(),
                                        feeder.getReplicaNameIdPair() +
                                        " is present in both nascent and " +
                                        "active feeder sets");
                }
                activeFeeders.put(replicaName, feeder);
                if (feeder.getReplicaNode().getType().isArbiter()) {
                    assert(arbiterFeeders.get() == 0);
                    arbiterFeeders.incrementAndGet();
                    arbiterFeederName = replicaName;

                } else if (feeder.getReplicaNode().getType().isElectable()) {
                    ackFeeders.incrementAndGet();
                }

                MasterTransfer xfr = repNode.getActiveTransfer();
                if (xfr != null) {
                    xfr.addFeeder(feeder);
                }
            }
        }
    }

    /**
     * Remove the feeder from the sets used to track it. Invoked when a feeder
     * is shutdown.
     *
     * @param feeder
     */
    void removeFeeder(Feeder feeder) {
        assert(feeder.isShutdown());
        final String replicaName = feeder.getReplicaNameIdPair().getName();
        synchronized (nascentFeeders) {
            synchronized (activeFeeders) {
                nascentFeeders.remove(feeder);
                if (activeFeeders.remove(replicaName) != null) {
                    if (arbiterFeederName != null &&
                        arbiterFeederName.equals(replicaName)) {
                        arbiterFeeders.decrementAndGet();
                        arbiterFeederName = null;
                    } else if (feeder.getReplicaNode().getType().isElectable()) {
                        ackFeeders.decrementAndGet();
                    }
                }
            }
        }

        final RepNodeImpl node = feeder.getReplicaNode();
        if ((node != null) && node.getType().hasTransientId()) {
            repNode.removeTransientNode(node);
        }
    }

    /**
     * Clears and shuts down the runFeeders by inserting a special EOF marker
     * value into the queue.
     */
    void shutdownQueue() {
        if (!repNode.isShutdown()) {
            throw EnvironmentFailureException.unexpectedState
                ("Rep node is still active");
        }
        channelQueue.clear();
        /* Add special entry so that the channelQueue.poll operation exits. */
        channelQueue.add(RepUtils.CHANNEL_EOF_MARKER);
    }

    /**
     * The core feeder listener loop that is run either in a Master node, or in
     * a Replica that is serving as a Feeder to other Replica nodes. The core
     * loop accepts connections from Replicas as they come in and establishes a
     * Feeder on that connection.
     *
     * The loop can be terminated for one of the following reasons:
     *
     *  1) A change in Masters.
     *
     *  2) A forced shutdown, via a thread interrupt.
     *
     *  3) A server socket level exception.
     *
     * The timeout on the accept is used to ensure that the check is done at
     * least once per timeout period.
     */
    void runFeeders()
        throws DatabaseException {

        if (shutdown.get()) {
            throw EnvironmentFailureException.unexpectedState
                ("Feeder manager was shutdown");
        }
        Exception feederShutdownException = null;
        LoggerUtils.info(logger, repNode.getRepImpl(),
                         "Feeder manager accepting requests.");

        try {
            repNode.getServiceDispatcher().
                register(FEEDER_SERVICE, channelQueue);

            /*
             * The Feeder is ready for business, indicate that the node is
             * ready by counting down the latch and releasing any waiters.
             */
            repNode.getReadyLatch().countDown();

            while (true) {
                if (!repNode.isAuthoritativeMaster() && 
                        repNode.getActiveTransfer() != null) {
                    // network partition happened during xfr
                    repNode.getActiveTransfer().noteProgress(
                            new VLSNProgress(INVALID_VLSN, null, false));
                }

                final DataChannel feederReplicaChannel =
                    channelQueue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);

                if (feederReplicaChannel == RepUtils.CHANNEL_EOF_MARKER) {
                    LoggerUtils.info(logger, repNode.getRepImpl(),
                                     "Feeder manager soft shutdown.");
                    return;
                }

                try {
                    repNode.getMasterStatus().assertSync();
                } catch (MasterObsoleteException mse) {
                    RepUtils.shutdownChannel(feederReplicaChannel);
                    throw mse;
                }
                if (feederReplicaChannel == null) {
                    if (repNode.isShutdownOrInvalid()) {
                        /* Timeout and shutdown request */
                        LoggerUtils.info(logger, repNode.getRepImpl(),
                                         "Feeder manager forced shutdown.");
                        return;
                    }

                    /* Flush the DTVLSN if it's warranted. */
                    dtvlsnFlusher.flush();

                    continue;
                }

                final Feeder feeder;
                try {
                    feeder = new Feeder(this, feederReplicaChannel);
                } catch (IOException e) {
                    RepUtils.shutdownChannel(feederReplicaChannel);
                    continue;
                }
                nFeedersCreated.increment();
                nascentFeeders.add(feeder);
                managedFeederCount.incrementAndGet();
                feeder.startFeederThreads();
            }
        } catch (MasterObsoleteException e) {
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Master change: " + e.getMessage());

            feederShutdownException = new UnknownMasterException("Node " +
                                repNode.getRepImpl().getName() +
                                " is not a master anymore");
        } catch (InterruptedException e) {
            if (this.repNodeShutdownException != null) {

                /*
                 * The interrupt was issued to propagate an exception from one
                 * of the Feeder threads. It's not a normal exit.
                 */
                LoggerUtils.warning(logger, repNode.getRepImpl(),
                                    "Feeder manager unexpected interrupt");
                throw repNodeShutdownException; /* Terminate the rep node */
            }
            if (repNode.isShutdown()) {
                LoggerUtils.info(logger, repNode.getRepImpl(),
                                 "Feeder manager interrupted for shutdown");
                return;
            }
            feederShutdownException = e;
            LoggerUtils.warning(logger, repNode.getRepImpl(),
                                "Feeder manager unexpected interrupt");
        } finally {
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Start shutting down feeder manager" +
                             ", reason=" + feederShutdownException);
            repNode.resetReadyLatch(feederShutdownException);
            repNode.getServiceDispatcher().cancel(FEEDER_SERVICE);
            shutdownFeeders(feederShutdownException);
            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Feeder manager exited. CurrentTxnEnd VLSN: " +
                             repNode.getCurrentTxnEndVLSN());
        }
    }

    /**
     * Shuts down all the feeders managed by the FeederManager. It waits for
     * all Feeder activity to stop before returning.
     *
     * @param feederShutdownException the exception provoking the shutdown.
     */
    private void shutdownFeeders(Exception feederShutdownException) {
        boolean changed = shutdown.compareAndSet(false, true);
        if (!changed) {
            return;
        }

        try {
            // Wake up any waiting transactions.
            notifyAllAuthMasterWaiter();

            /* Copy sets for safe iteration in the presence of deletes.*/
            final Set<Feeder> feederSet;
            synchronized (nascentFeeders) {
                synchronized (activeFeeders) {
                    feederSet = new HashSet<>(activeFeeders.values());
                    feederSet.addAll(nascentFeeders);
                }
            }

            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Feeder Manager shutting down feeders." +
                             " Active and nascent feeders=" + feederSet.size() +
                             " Managed feeders=" + managedFeederCount.get() +
                             (feederShutdownException == null ? "" :
                                  ", reason=" + feederShutdownException));

            feederSet.forEach((feeder) -> {
                nFeedersShutdown.increment();
                feeder.shutdown(feederShutdownException);
            });

            /*
             * This extra check below is used to ensure that there is no
             * premature return while feeder threads are still running. This
             * could happen, for example, in the face of concurrent shutdowns
             * from this method and the Feeder Input/Output threads where
             * Feeder.shutdown may have removed the Feeder from the set of
             * nascent and active feeders as part of theFeeder shutdown, or the
             * shutdown() method call above may return immediately due to the
             * ongoing shutdown initiated by the Feeder.
             */
            boolean shutdownCompleted =
                new PollCondition(1, FEEDER_SHUTDOWN_WAIT_MILLIS) {

                @Override
                protected boolean condition() {
                    return managedFeederCount.get() == 0;
                }
            }.await();

            if (!shutdownCompleted) {
                final String msg = this +
                    " Feeder Manager shutdown failed to quiesce. Feeder count:" +
                    managedFeederCount.get() + " after waiting for " +
                    FEEDER_SHUTDOWN_WAIT_MILLIS + " ms.";

                LoggerUtils.severe(logger, repNode.getRepImpl(), msg);
                LoggerUtils.fullThreadDump(logger, repNode.getRepImpl(),
                                           Level.SEVERE);
                throw new IllegalStateException(msg, feederShutdownException);
            }

            // Wake up any waiting transactions.
            notifyAllAuthMasterWaiter();

            LoggerUtils.info(logger, repNode.getRepImpl(),
                             "Feeder Manager completed shutting down " +
                             "feeders.");

        } finally {
            activeFeeders.clear();
            nascentFeeders.clear();
        }
    }

    /**
     * Shuts down a specific feeder. It's typically done in response to the
     * removal of a member from the group.
     */
    public void shutdownFeeder(RepNodeImpl node) {
        Feeder feeder = activeFeeders.get(node.getName());
        if (feeder == null) {
            return;
        }
        nFeedersShutdown.increment();
        feeder.shutdown(null);
    }

    /*
     * For debugging help, and for expanded exception messages, dump feeder
     * related state.  If acksOnly is true, only include information about
     * feeders for replicas that supply acknowledgments.
     */
    public String dumpState(final boolean acksOnly) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, Feeder>> feeds = activeFeeders.entrySet();
        if (feeds.size() == 0) {
            sb.append("No feeders.");
        } else {
            sb.append("Current feeds:");
            feeds.forEach((feedEntry) -> {
                final Feeder feeder = feedEntry.getValue();

                /*
                 * Ignore secondary and external nodes if only want nodes
                 * that provide acknowledgments
                 */
                if (acksOnly) {
                    final NodeType nodeType = feeder.getReplicaNode().getType();
                    if (nodeType.isSecondary() || nodeType.isExternal()) {
                        return;
                    }
                }
                sb.append("\n ").append(feedEntry.getKey()).append(": ");
                sb.append(feeder.dumpState());
            });
        }
        return sb.toString();
    }

    /**
     * Returns a count of the number of feeders whose replicas are counted in
     * durability decisions and have acknowledged txn-end VLSNs {@literal >=}
     * the commitVLSN argument.
     *
     * @param commitVLSN the commitVLSN being checked
     */
    public int getNumCurrentAckFeeders(long commitVLSN) {
        final DurabilityQuorum durabilityQuorum =
            repNode.getDurabilityQuorum();
        int count[] = {0};
        activeFeeders.forEachValue(Long.MAX_VALUE, (feeder) -> {
            if ((commitVLSN <= feeder.getReplicaTxnEndVLSN()) &&
                durabilityQuorum.replicaAcksQualify(feeder.getReplicaNode())) {
                count[0]++;
            }
        });
        return count[0];
    }

    /**
     * Update the Master's DTVLSN if we can conclude based upon the state of
     * the replicas that the DTVLSN needs to be advanced.
     *
     * This method is invoked when a replica heartbeat or replica ack reports
     * a more recent local durable vlsn. This (sometimes) redundant form of DTVLS update
     * is useful in circumstances when the value could not be maintained via
     * the usual ack response processing:
     *
     * 1) The application is using no ack transactions explicitly.
     *
     * 2) There were ack transaction timeouts due to network problems and the
     * acks were never received or were received after the timeout had expired.
     */
    public void updateDTVLSNWithLocalVLSN(long replicaLocalVLSN) {

        final long currDTVLSN = repNode.getDTVLSN();
        final long currLocalDurableVLSN = repNode.getLocalDurableVLSN();
        if (replicaLocalVLSN <= currDTVLSN) {
            /* Nothing to update, a lagging replica that's catching up */
            return;
        }

        /*
         * Update the global dtvlsn on the master side by query the local
         * durable vlsn on every node of the quorum, including master's, sort
         * all these vlsn in descending order, and choose the one with index
         * equals to durableAckCount.
         */
        final DurabilityQuorum durabilityQuorum = repNode.getDurabilityQuorum();
        final int durableAckCount = durabilityQuorum.
            getCurrentRequiredAckCount(ReplicaAckPolicy.SIMPLE_MAJORITY);
        final int quorumSize = durabilityQuorum.
            getCurrentRequiredAckCount(ReplicaAckPolicy.ALL) + 1;
        int ackCount[] = {0};
        LinkedList<Long> allLocalDurableVLSN = new LinkedList<>();
        allLocalDurableVLSN.add(currLocalDurableVLSN);
        synchronized (activeFeeders) {
            activeFeeders.forEachValue(Long.MAX_VALUE, (feeder) -> {

                if (!durabilityQuorum.
                    replicaAcksQualify(feeder.getReplicaNode())) {
                    return;
                }

            final long replicaDurableVLSN =
                feeder.getReplicaDurableVLSN();
            allLocalDurableVLSN.add(replicaDurableVLSN);
            ++ackCount[0];
            });
        }

        /*
         * There might be some nodes inside the quorum being not active
         */
        while(allLocalDurableVLSN.size() < quorumSize) {
            allLocalDurableVLSN.add(NULL_VLSN);
        }

        if (ackCount[0] >= durableAckCount) {
            /*
             * Received response from a majority of replicas,
             * then the new potential dtvlsn will be determined among
             * all local durable vlsn from all nodes, including feeder's.
             */
            Collections.sort(allLocalDurableVLSN, Comparator.reverseOrder());

            repNode.updateDTVLSN(allLocalDurableVLSN.get(durableAckCount));
        }
    }

    /**
     * Update the Master's DTVLSN if we can conclude based upon the state of
     * the replicas that the DTVLSN needs to be advanced.
     *
     * This method is invoked when a replica heartbeat reports a more recent
     * txn VLSN. This (sometimes) redundant form of DTVLS update is useful in
     * circumstances when the value could not be maintained via the usual ack
     * response processing:
     *
     * 1) The application is using no ack transactions explicitly.
     *
     * 2) There were ack transaction timeouts due to network problems and the
     * acks were never received or were received after the timeout had expired.
     */
    public void updateDTVLSN(long heartbeatVLSN) {

        final long currDTVLSN = repNode.getDTVLSN();
        if (heartbeatVLSN <= currDTVLSN) {
            /* Nothing to update, a lagging replica that's catching up */
            return;
        }

        final DurabilityQuorum durabilityQuorum = repNode.getDurabilityQuorum();
        final int durableAckCount = durabilityQuorum.
            getCurrentRequiredAckCount(ReplicaAckPolicy.SIMPLE_MAJORITY);

        long min[] = {Long.MAX_VALUE};
        int ackCount[] = {0};

        synchronized (activeFeeders) {
            activeFeeders.forEachValue(Long.MAX_VALUE, (feeder) -> {

                if (!durabilityQuorum.
                    replicaAcksQualify(feeder.getReplicaNode())) {
                    return;
                }

                final long replicaTxnVLSN =
                    feeder.getReplicaTxnEndVLSN();

                if (replicaTxnVLSN <= currDTVLSN) {
                    return;
                }

                if (replicaTxnVLSN < min[0]) {
                    min[0] = replicaTxnVLSN;
                }

                if (++ackCount[0] >= durableAckCount) {
                    /*
                     * If a majority of replicas have vlsns >= durable txn
                     * vlsn, advance the DTVLSN.
                     */
                    repNode.updateDTVLSN(min[0]);
                    return;
                }
            });
            /* DTVLSN unchanged. */
            return;
        }
    }

    /**
     * Writes a null (no modifications) commit record when it detects that the
     * DTVLSN is ahead of the persistent DTVLSN and needs to be updated.
     *
     * Note that without this mechanism, the in-memory DTVLSN would always be
     * ahead of the persisted VLSN, since in general DTVLSN(vlsn) < vlsn. That
     * is, the commit or abort log record containing the DTVLSN always has a
     * more recent VLSN than the one it contains.
     */
    private class DTVLSNFlusher {

        /**
         * The number of feeder ticks for which the in-memory DTVLSN must be
         * stable before it's written to the log as a null TXN. We are using
         * this "tick" indirection to avoid yet another call to the clock. A
         * "tick" in this context is the FEEDER_MANAGER_POLL_TIMEOUT.
         */
        final int targetStableTicks;

        /**
         * The number of ticks for which the DTVLSN has been stable.
         */
        private int stableTicks = 0;

        public DTVLSNFlusher() {
            final int heartbeatMs = repNode.getConfigManager().
                getInt(RepParams.HEARTBEAT_INTERVAL);
            targetStableTicks =
                (int) Math.max(1, (8 * heartbeatMs) / pollTimeoutMs);
        }

        /**
         * Used to track whether the DTVLSN has been stable enough to write
         * out. While it's changing application commits and aborts are writing
         * it out, so no need to write it here.
         */
        private long stableDTVLSN = NULL_VLSN;

        /**
         * Update each time we actually persist the DTVLSN via a null txn. It
         * represents the DTVLSN that's been written out.
         */
        private long persistedDTVLSN = NULL_VLSN;

        /* Identifies the Txn that was used to persist the DTVLSN. */
        private long nullTxnVLSN = NULL_VLSN;

        /**
         * Persists the DTVLSN if necessary. The DTVLSN is persisted if the
         * version in memory is more current than the version on disk and has
         * not changed for targetStableTicks.
         */
        void flush() {
            final long dtvlsn = repNode.getDTVLSN();

            if (dtvlsn == nullTxnVLSN) {
                /* Don't save VLSN from null transaction as DTVLSN */
                return;
            }

            if (dtvlsn > stableDTVLSN) {
                stableTicks = 0;
                stableDTVLSN = dtvlsn;

                /* The durable DTVLSN is being actively updated. */
                return;
            }

            if (dtvlsn < stableDTVLSN) {
                /* Enforce the invariant that the DTVLSN cannot decrease. */
                throw new IllegalStateException("The DTVLSN sequence cannot decrease" +
                                                "current DTVLSN:" + dtvlsn +
                                                " previous DTVLSN:" + stableDTVLSN);
            }

            /* DTVLSN == stableDTVLSN */
            if (++stableTicks <= targetStableTicks) {
                /*
                 * Increase the stable tick counter. it has not been stable
                 * long enough.
                 */
                return;
            }

            stableTicks = 0;

            /* dtvlsn has been stable */
            if (stableDTVLSN > persistedDTVLSN) {
                if (repNode.getActiveTransfer() != null) {
                    /*
                     * Don't attempt writing a transaction. while a transfer is
                     * in progress and txns will be blocked.
                     */
                    LoggerUtils.info(logger, repNode.getRepImpl(),
                                     "Skipped null txn updating DTVLSN: " +
                                     dtvlsn + " Master transfer in progress");
                    return;
                }
                final RepImpl repImpl = repNode.getRepImpl();
                final MasterTxn nullTxn =
                    MasterTxn.createNullTxn(repImpl, false);
                /*
                 * We don't want to wait for any reason, if the txn fails,
                 * we can try later.
                 */
                nullTxn.setTxnTimeout(1);
                try {
                    nullTxn.commit();
                    LoggerUtils.fine(logger, repNode.getRepImpl(),
                                     "Persisted DTVLSN: " + dtvlsn +
                                     " via null txn:" + nullTxn.logString());
                    nullTxnVLSN = nullTxn.getCommitVLSN();
                    persistedDTVLSN = dtvlsn;
                    stableDTVLSN = persistedDTVLSN;
                } catch (Exception e) {
                    nullTxn.abort();
                    LoggerUtils.warning(logger, repNode.getRepImpl(),
                               "Failed to write null txn updating DTVLSN: " +
                               nullTxn.logString() +
                               " Reason:" + e.getMessage());
                }
            }
        }
    }

    /**
     * Invoked only after a Feeder has completed its shutdown.
     */
    void decrementManagedFeederCount() {
        managedFeederCount.getAndDecrement();
    }

    /**
     * As a way to ensure that any inflight non-durable txns at the time of
     * failure in the preceding term are made durable in a new term without
     * having to wait for the application itself to create a durable txn. That
     * is, it speeds up the final state of these non-durable txns, since the
     * ack of the null txn advances the dtvlsn.
     *
     * This satisfies the one described in Section 5.4.2 and Figure 8 in the
     * <a href="https://raft.github.io/raft.pdf">Raft manuscript</a>
     */
    void sendNullTxnWhenNodeBecomesMaster() {
        /* It is created a sync NullTxn to ensure durability. */
        final MasterTxn nullTxn =
                MasterTxn.createNullTxn(repNode.getRepImpl(), true);
        nullTxn.setTxnTimeout(1);
        try {
            nullTxn.commit();
            LoggerUtils.fine(logger, repNode.getRepImpl(),
                    "Success to write null txn when "
                            + repNode.getNodeName()
                            + " was elected as master. "
                            + nullTxn.logString());
        } catch (Exception e) {
            nullTxn.abort();
            LoggerUtils.warning(logger, repNode.getRepImpl(),
                    "Failed to write null txn when "
                            + repNode.getNodeName()
                            + " was elected as master. "
                            + nullTxn.logString() + " Reason:"
                            + e.getMessage());
        }
    }
}
