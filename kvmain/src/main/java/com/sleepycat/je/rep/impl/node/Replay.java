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

import static com.sleepycat.je.ExtinctionFilter.ExtinctionStatus.NOT_EXTINCT;
import static com.sleepycat.je.log.LogEntryType.LOG_NAMELN_TRANSACTIONAL;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_ABORT;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.LATEST_COMMIT_LAG_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_ABORTS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMITS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_ACKS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_NO_SYNCS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_SYNCS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_COMMIT_WRITE_NO_SYNCS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_LNS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_NAME_LNS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_NOT_PREPROCESSED;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_PREPROCESS_MISS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_REPLAY_QUEUE_AVG_SIZE;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_REPLAY_QUEUE_OVERFLOWS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.OUTPUT_QUEUE_95_DELAY_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.OUTPUT_QUEUE_99_DELAY_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.OUTPUT_QUEUE_AVG_DELAY_NS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.OUTPUT_QUEUE_MAX_DELAY_NS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.REPLAY_QUEUE_95_DELAY_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.REPLAY_QUEUE_99_DELAY_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.REPLAY_QUEUE_AVG_DELAY_NS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.REPLAY_QUEUE_MAX_DELAY_NS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.TXN_95_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.TXN_99_MS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.TXN_AVG_NS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.TXN_MAX_NS;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.beforeimage.BeforeImageContext;
import com.sleepycat.je.beforeimage.BeforeImageLNLogEntry;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.ExtinctionFilter.ExtinctionStatus;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree.TruncateDbResult;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.ReplayPreprocessor;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.log.DbOpReplicationContext;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.DbOperationType;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.NameLNLogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.recovery.RollbackTracker;
import com.sleepycat.je.rep.LogFileRewriteListener;
import com.sleepycat.je.rep.SyncupProgress;
import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.impl.RepGroupDB;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.stream.InputWireRecord;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.rep.stream.Protocol;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.txn.ReplayTxn;
import com.sleepycat.je.rep.utilint.BinaryProtocol;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.txn.RollbackEnd;
import com.sleepycat.je.txn.RollbackStart;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnAbort;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.txn.GroupCommitReplica;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LatencyPercentileStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvgStat;
import com.sleepycat.je.utilint.LongMaxStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.SimpleTxnMap;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.VLSN;
import com.sleepycat.utilint.StringUtils;

/**
 * Replays log records from the replication stream, and manages the
 * transactions for those records.
 *
 * The Replay module has a lifetime equivalent to the environment owned by
 * this replicator. Its lifetime is longer than the feeder/replica stream.
 * For example, suppose this is nodeX:
 *
 * t1 - Node X is a replica, node A is master. Replay X is alive
 * t2 - Node X is a replica, node B takes over as master. X's Replay module
 *      is still alive and has the same set of active txns. It doesn't matter
 *      to X that the master has changed.
 * t3 - Node X becomes the master. Now its Replay unit is cleared, because
 *      anything managed by the Replay is defunct.
 */
public class Replay {

    /* These are strings for the rollback logging. */
    private static final String RBSTATUS_START =
        "Started Rollback";
    private static final String RBSTATUS_NO_ACTIVE =
        "No active txns, nothing to rollback";
    private static final String RBSTATUS_RANGE_EQUALS =
        "End of range equals matchpoint, nothing to rollback";
    private static final String RBSTATUS_LOG_RBSTART =
        "Logged RollbackStart entry";
    private static final String RBSTATUS_MEM_ROLLBACK =
        "Finished in-memory rollback";
    private static final String RBSTATUS_INVISIBLE =
        "Finished invisible setting";
    private static final String RBSTATUS_FINISH =
        "Finished rollback";

    /*
     * DatabaseEntry objects reused during replay, to minimize allocation in
     * high performance replay path.
     */
    private final DatabaseEntry replayKeyEntry = new DatabaseEntry();
    private final DatabaseEntry replayDataEntry = new DatabaseEntry();
    private final DatabaseEntry delDataEntry = new DatabaseEntry();

    private final RepImpl repImpl;

    /**
     *  If a commit replay operation takes more than this threshold, it's
     *  logged. This information helps determine whether ack timeouts on the
     *  master are due to a slow replica, or the network.
     */
    private final long ackTimeoutLogThresholdNs;

    /**
     * ActiveTxns is a collection of txn objects used for applying replicated
     * transactions. This collection should be empty if the node is a master.
     *
     * Note that there is an interesting relationship between ActiveTxns and
     * the txn collection managed by the environment TxnManager. ActiveTxns is
     * effectively a subset of the set of txns held by the
     * TxnManager. ReplayTxns must be sure to register and unregister
     * themselves from ActiveTxns, just as all Txns must register and
     * unregister with the TxnManager's set. One implementation alternative to
     * having an ActiveTxns map here is to search the TxnManager set (which is
     * a set, not a map) for a given ReplayTxn. Another is to subclass
     * TxnManager so that replicated nodes have their own replayTxn map, just
     * as XATxns have a XID->Txn map.
     *
     * Both alternatives seemed too costly in terms of performance or elaborate
     * in terms of code to do for the current function. It seems clearer to
     * make the ActiveTxns a map in the one place that it is currently
     * used. This choice may change over time, and should be reevaluated if the
     * implementation changes.
     *
     * The ActiveTxns key is the transaction id. These transactions are closed
     * when:
     * - the replay unit executes a commit received over the replication stream
     * - the replay unit executes an abort received over the replication stream
     * - the replication node realizes that it has just become the new master,
     *   and was not previously the master.
     *
     * Note that the Replay class has a lifetime that is longer than that of a
     * RepNode. This means in particular, that transactions may be left open,
     * and will be resumed when a replica switches from one master to another,
     * creating a new RepNode in the process. Because of that, part of the
     * transaction may be implemented by the rep stream from one master and
     * another part by another.
     *
     * The map is synchronized, so simple get/put operations do not require
     * additional synchronization.  However, iteration requires synchronization
     * and copyActiveTxns can be used in most cases.
     */
    private final SimpleTxnMap<ReplayTxn> activeTxns;

    /*
     * The entry representing the last replayed txn commit. Supports the
     * replica's notion of consistency.
     */
    private volatile ReplayTxn lastReplayedTxn = null;

    /*
     * The last replayed entry of any kind. Supports PointConsistencyPolicy.
     */
    private volatile long lastReplayedVLSN = INVALID_VLSN;

    /*
     * The last replayed DTVLSN in the stream. It's used to ensure that the
     * DTVLSNs in the stream are correctly sequenced.
     */
    private long lastReplayedDTVLSN = NULL_VLSN;

    /*
     * The sync policy to be used in the absence of an ACK request. The replica
     * in this case has some latitude about how it syncs the commit.
     */
    private final SyncPolicy noAckSyncPolicy = SyncPolicy.NO_SYNC;

    /**
     *  The RepParams.REPLAY_LOGGING_THRESHOLD configured logging threshold.
     */
    private final long replayLoggingThresholdNs;

    /*
     * State that is reinitialized by the reinit() method each time a replay
     * loop is started with a new feeder.
     */

    /**
     *  All writes (predominantly acks) are queued here, so they do not block
     *  the replay thread.
     */
    private final BlockingQueue<Long> outputQueue;

    /**
     * The timeout for blocking on the outputQueue when offering ack items. This
     * timeout is set to be 4x the replicaFeederChannel timeout. This timeout
     * is a preventative measure so that the replay thread does not block
     * indefinitely when something happens to the output thread. The 4x factor
     * is somewhat arbitrary but just long enough to make sure we do not get
     * too sensitive to output thread being stalled on the network.
     */
    private volatile long outputQueueOfferTimeoutMillis;

    /**
     * Holds the state associated with group commits.
     */
    private com.sleepycat.je.txn.GroupCommitReplica groupCommit = null;

    /* Maintains the statistics associated with stream replay. */
    private final StatGroup statistics;
    private final LongStat nCommits;
    private final LongStat nCommitAcks;
    private final LongStat nCommitSyncs;
    private final LongStat nCommitNoSyncs;
    private final LongStat nCommitWriteNoSyncs;
    private final LongStat nAborts;
    private final LongStat nNameLNs;
    private final LongStat nLNs;
    private final LongStat nNotPreprocessed;
    private final LongStat nPreprocessMiss;
    private final LongStat nReplayQueueOverflows;
    private final LongAvgStat nReplayQueueAvgSize;
    private final LongStat latestCommitLagMs;

    private volatile long heartbeatRequestEnqueueTime = 0;
    private volatile long heartbeatRequestMasterNow = 0;
    private final LongAvgStat replayQueueAvgDelayNs;
    private final LatencyPercentileStat replayQueue95DelayMs;
    private final LatencyPercentileStat replayQueue99DelayMs;
    private final LongMaxStat replayQueueMaxDelayNs;

    private volatile long heartbeatResponseEnqueueNs = 0;
    private final LongAvgStat outputQueueAvgDelayNs;
    private final LatencyPercentileStat outputQueue95DelayMs;
    private final LatencyPercentileStat outputQueue99DelayMs;
    private final LongMaxStat outputQueueMaxDelayNs;

    private final LongAvgStat txnAvgNs;
    private final LatencyPercentileStat txn95Ms;
    private final LatencyPercentileStat txn99Ms;
    private final LongMaxStat txnMaxNs;

    private final Logger logger;

    /** Cached current term. Set when the ReplayThread starts to run. */
    private volatile long currTerm;
    /** Cached current master. Set when the ReplayThread starts to run. */
    private volatile NameIdPair currMaster;

    /** Hook to stall ouputqueue. */
    static public com.sleepycat.je.utilint.TestHook<Void> softShutdownHook;

    public Replay(RepImpl repImpl,
                  @SuppressWarnings("unused") NameIdPair nameIdPair) {

        /*
         * This should have already been caught in
         * ReplicatedEnvironment.setupRepConfig, but it is checked here anyway
         * as an added sanity check. [#17643]
         */
        if (repImpl.isReadOnly()) {
            throw EnvironmentFailureException.unexpectedState
                ("Replay created with readonly ReplicatedEnvironment");
        }

        this.repImpl = repImpl;
        final DbConfigManager configManager = repImpl.getConfigManager();

        ackTimeoutLogThresholdNs = MILLISECONDS.toNanos(configManager.
            getDuration(RepParams.REPLICA_ACK_TIMEOUT));

        /*
         * The factor of 2 below is somewhat arbitrary. It should be > 1 X so
         * that the ReplicaOutputThread can completely process the buffered
         * messages in the face of a network drop and 2X to allow for
         * additional headroom and minimize the chances that the replay might
         * be blocked due to the limited queue length.
         */
        final int outputQueueSize = 2 *
            configManager.getInt(RepParams.REPLICA_MESSAGE_QUEUE_SIZE);
        outputQueue = new ArrayBlockingQueue<>(outputQueueSize);
        outputQueueOfferTimeoutMillis =
            4 * repImpl.getReplicaFeederChannelTimeoutMillis();

        /*
         * The Replay module manages all write transactions and mimics a
         * writing application thread. When the node comes up, it populates
         * the activeTxn collection with ReplayTxns that were resurrected
         * at recovery time.
         */
        activeTxns = new SimpleTxnMap<>(1024);

        /*
         * Configure the data entry used for deletion to avoid fetching the
         * old data during deletion replay.
         */
        delDataEntry.setPartial(0, 0, true);

        logger = LoggerUtils.getLogger(getClass());
        statistics = new StatGroup(ReplayStatDefinition.GROUP_NAME,
                                   ReplayStatDefinition.GROUP_DESC);

        nCommits = new LongStat(statistics, N_COMMITS);
        nCommitAcks = new LongStat(statistics, N_COMMIT_ACKS);
        nCommitSyncs = new LongStat(statistics, N_COMMIT_SYNCS);
        nCommitNoSyncs = new LongStat(statistics, N_COMMIT_NO_SYNCS);
        nCommitWriteNoSyncs =
            new LongStat(statistics, N_COMMIT_WRITE_NO_SYNCS);
        nAborts = new LongStat(statistics, N_ABORTS);
        nNameLNs = new LongStat(statistics, N_NAME_LNS);
        nLNs = new LongStat(statistics, N_LNS);
        nReplayQueueOverflows =
            new LongStat(statistics, N_REPLAY_QUEUE_OVERFLOWS);
        nReplayQueueAvgSize =
            new LongAvgStat(statistics, N_REPLAY_QUEUE_AVG_SIZE);
        nNotPreprocessed = new LongStat(statistics, N_NOT_PREPROCESSED);
        nPreprocessMiss = new LongStat(statistics, N_PREPROCESS_MISS);
        latestCommitLagMs = new LongStat(statistics, LATEST_COMMIT_LAG_MS);

        replayQueueAvgDelayNs =
            new LongAvgStat(statistics, REPLAY_QUEUE_AVG_DELAY_NS);
        replayQueue95DelayMs = new LatencyPercentileStat(
            statistics, REPLAY_QUEUE_95_DELAY_MS, 0.95f);
        replayQueue99DelayMs = new LatencyPercentileStat(
            statistics, REPLAY_QUEUE_99_DELAY_MS, 0.99f);
        replayQueueMaxDelayNs = new LongMaxStat(
            statistics, REPLAY_QUEUE_MAX_DELAY_NS);

        outputQueueAvgDelayNs =
            new LongAvgStat(statistics, OUTPUT_QUEUE_AVG_DELAY_NS);
        outputQueue95DelayMs = new LatencyPercentileStat(
            statistics, OUTPUT_QUEUE_95_DELAY_MS, 0.95f);
        outputQueue99DelayMs = new LatencyPercentileStat(
            statistics, OUTPUT_QUEUE_99_DELAY_MS, 0.99f);
        outputQueueMaxDelayNs = new LongMaxStat(
            statistics, OUTPUT_QUEUE_MAX_DELAY_NS);

        txnAvgNs = new LongAvgStat(statistics, TXN_AVG_NS);
        txn95Ms =
            new LatencyPercentileStat(statistics, TXN_95_MS, 0.95f);
        txn99Ms =
            new LatencyPercentileStat(statistics, TXN_99_MS, 0.99f);
        txnMaxNs = new LongMaxStat(statistics, TXN_MAX_NS);

        replayLoggingThresholdNs = MILLISECONDS.toNanos(configManager.
           getDuration(RepParams.REPLAY_LOGGING_THRESHOLD));
    }

    /**
     * Marks that a replay thread is newly started.
     *
     * Caches the current term and master value to be used in replayEntry.
     */
    public void markReplayThreadStart() {
        final MasterStatus masterStatus =
            repImpl.getRepNode().getMasterStatus();
        currTerm = masterStatus.getNodeMasterIdTerm().term;
        currMaster = masterStatus.getNodeMasterNameId();
    }

    /**
     * Note that a heartbeat request has been received over the network and is
     * about to be added to the replay queue. If no heartbeat request is being
     * tracked, note the enqueuing time and the heartbeat's master time. When
     * we dequeue this same request, as determined by matching the master time,
     * we will compute the time it spent in the queue and use that to tally
     * statistics about the replay queue delays.
     */
    void noteEnqueueHeartbeatRequest(Protocol.Heartbeat heartbeat) {
        if (heartbeatRequestEnqueueTime == 0) {
            heartbeatRequestEnqueueTime = TimeSupplier.currentTimeMillis();
            heartbeatRequestMasterNow = heartbeat.getMasterNow();
        }
    }

    /**
     * Note that an incoming heartbeat request has been retrieved from the
     * replay queue in preparation for processing.
     *
     * <p>Then, if no heartbeat response is being tracked, note the time of
     * enqueuing the heartbeat response to the output queue.
     *
     * @return whether to track the heartbeat response time
     */
    boolean noteDequeueHeartbeatRequest(Protocol.Heartbeat heartbeat) {
        if ((heartbeatRequestEnqueueTime != 0) &&
            (heartbeatRequestMasterNow == heartbeat.getMasterNow())) {
            heartbeatRequestEnqueueTime = 0;
        }
        if (heartbeatResponseEnqueueNs == 0) {
            heartbeatResponseEnqueueNs = System.nanoTime();
            return true;
        }
        return false;
    }

    /**
     * Note that an outgoing heartbeat response has been retrieved from the
     * output queue in preparation for sending it out over the network.  Tally
     * output queue delay statistics if we were tracking a heartbeat response.
     */
    void noteDequeueHeartbeatResponse() {
        if (heartbeatResponseEnqueueNs != 0) {
            final long delayNs =
                System.nanoTime() - heartbeatResponseEnqueueNs;
            final long delayMs = TimeUnit.NANOSECONDS.toMillis(delayNs);
            outputQueueAvgDelayNs.add(delayNs);
            outputQueueMaxDelayNs.setMax(delayNs);
            outputQueue95DelayMs.add(delayMs);
            outputQueue99DelayMs.add(delayMs);
            heartbeatResponseEnqueueNs = 0;
        }
    }

    BlockingQueue<Long> getOutputQueue() {
        return outputQueue;
    }

    /**
     * Reinitialize for replay from a new feeder
     */
    public void reset() {
        outputQueue.clear();
        /* reset so that sequencing checks work in case of rollback */
        lastReplayedDTVLSN = VLSN.NULL_VLSN;
        lastReplayedTxn = null;
        groupCommit = (GroupCommitReplica)repImpl.getTxnManager().getGroupCommit();
        /*
         * Note that lastReplayedVLSN is reset to the Matchpoint during the
         * replica feeder handshake and is not modified here.
         */
    }

    LongStat getReplayQueueOverflows() {
        return nReplayQueueOverflows;
    }

    public LongAvgStat getReplayQueueAvgSize() {
        return nReplayQueueAvgSize;
    }

    /**
     * Actions that must be taken before the recovery checkpoint, whether
     * the environment is read/write or read/only.
     */
    public void preRecoveryCheckpointInit(RecoveryInfo recoveryInfo) {
        for (Txn txn : recoveryInfo.replayTxns.values()) {

            /*
             * ReplayTxns need to know about their owning activeTxn map,
             * so they can remove themselves at close. We are casting upwards,
             * because the non-HA code is prohibited from referencing
             * Replication classes, and the RecoveryInfo.replayTxns collection
             * doesn't know that it's got ReplayTxns.
             */
            ((ReplayTxn) txn).registerWithActiveTxns(activeTxns);
        }
        lastReplayedVLSN = repImpl.getVLSNIndex().getRange().getLast();
    }

    ReplayTxn getLastReplayedTxn() {
        return lastReplayedTxn;
    }

    long getLastReplayedVLSN() {
        return lastReplayedVLSN;
    }

    /**
     * When mastership changes, all inflight replay transactions are aborted.
     * Replay transactions need only be aborted by the node that has become
     * the new master (who was previously a Replica). The replay transactions
     * on the other replicas who have not changed roles are
     * resolved by the abort record issued by said new master.
     */
    void abortOldTxns()
        throws DatabaseException {

        final MasterIdTerm masterIdTerm = repImpl.getRepNode().getNodeState().
            getMasterIdTerm();

        for (ReplayTxn replayTxn : copyActiveTxns().values()) {
            /* The DTVLSN and VLSN will be corrected when it's written to the
             * log */
            final TxnAbort dummyTxnEnd =
                new TxnAbort(replayTxn.getId(), replayTxn.getLastLsn(),
                             () -> new MasterIdTerm(
                                 masterIdTerm.nodeId, masterIdTerm.term),
                             NULL_VLSN);

            replayTxn.abort(ReplicationContext.MASTER, dummyTxnEnd);
        }
        assert activeTxns.isEmpty() : "Unexpected txns in activeTxns = " +
            activeTxns;
    }

    private void updateCommitStats(final boolean needsAck,
                                   final SyncPolicy syncPolicy,
                                   final long startTimeNanos,
                                   final long masterCommitTimeMs,
                                   final long replicaCommitTimeMs,
                                   final long masterTxnTimeNs) {

        final long now = System.nanoTime();
        final long commitNanos = now - startTimeNanos;

        if (commitNanos > ackTimeoutLogThresholdNs &&
            logger.isLoggable(Level.INFO)) {
            LoggerUtils.info
                (logger, repImpl,
                 "Replay commit time: " + (commitNanos / 1000000) +
                 " ms exceeded log threshold: " +
                 (ackTimeoutLogThresholdNs / 1000000));
        }

        nCommits.increment();

        if (needsAck) {
            nCommitAcks.increment();
        }

        if (syncPolicy == SyncPolicy.SYNC) {
            nCommitSyncs.increment();
        } else if (syncPolicy == SyncPolicy.NO_SYNC) {
            nCommitNoSyncs.increment();
        } else if (syncPolicy == SyncPolicy.WRITE_NO_SYNC) {
            nCommitWriteNoSyncs.increment();
        } else {
            throw EnvironmentFailureException.unexpectedState
                ("Unknown sync policy: " + syncPolicy);
        }

        updateTxnStats(masterTxnTimeNs);

        /*
         * Tally the lag between master and replica commits, even if clock skew
         * makes the lag appear negative.  The documentation already warns that
         * the value will be affected by clock skew, so users can adjust for
         * that, but only if we don't throw the information way.
         */
        final long replicaLagMs = replicaCommitTimeMs - masterCommitTimeMs;
        latestCommitLagMs.set(replicaLagMs);
    }

    private void updateTxnStats(long txnNs) {
        if (txnNs == 0) {
            /* Elapsed time not available. */
            return ;
        }
        txnAvgNs.add(txnNs);
        txnMaxNs.setMax(txnNs);

        final long txnMs = txnNs/1_000_000;
        txn95Ms.add(txnMs);
        txn99Ms.add(txnMs);
    }

    public void incPreprocessMiss() {
        nPreprocessMiss.increment();
    }

    /**
     * For LN entries: creates a ReplayPreprocessor, attaches it to the
     * WireRecord and returns it. For other entries: returns null.
     */
    ReplayPreprocessor addPreprocessor(BinaryProtocol.Message message) {

        final InputWireRecord wireRecord = getLNWireRecord(message);
        if (wireRecord == null) {
            return null;
        }

        final ReplayPreprocessor preprocessor = new ReplayPreprocessor(
            repImpl, (LNLogEntry<?>) wireRecord.getLogEntry());

        wireRecord.setPreprocessor(preprocessor);
        return preprocessor;
    }

    /**
     * Called to release resources for the processor if necessary. This is
     * critical when a processor is executed but never used by replay, since
     * it may have acquired locks, etc, and these resources must be freed to
     * avoid leaks and deadlocks.
     */
    void closePreprocessor(BinaryProtocol.Message message) {
        if (message == null) {
            return;
        }
        final InputWireRecord wireRecord = getLNWireRecord(message);
        if (wireRecord == null) {
            return;
        }
        final ReplayPreprocessor preprocessor = wireRecord.getPreprocessor();
        if (preprocessor == null) {
            return;
        }
        preprocessor.close();
    }

    private static InputWireRecord getLNWireRecord(
        BinaryProtocol.Message message) {

        if (!(message instanceof Protocol.Entry)) {
            return null;
        }

        final Protocol.Entry entry = (Protocol.Entry) message;
        final InputWireRecord wireRecord = entry.getWireRecord();
        final byte entryType = wireRecord.getEntryType();

        if (LOG_TXN_COMMIT.equalsType(entryType) ||
            LOG_TXN_ABORT.equalsType(entryType) ||
            LOG_NAMELN_TRANSACTIONAL.equalsType(entryType)) {
            return null;
        }

        /* All other entry types are LNs. */
        return wireRecord;
    }

    /**
     * Apply the operation represented by this log entry on this replica node.
     */
    public void replayEntry(long startNs,
                            Protocol.Entry entry)
        throws DatabaseException,
               IOException,
               InterruptedException,
               MasterObsoleteException {

        updateEntryDelayStats(startNs, entry);

        final InputWireRecord wireRecord = entry.getWireRecord();
        final LogEntry logEntry = wireRecord.getLogEntry();

        /*
         * Sanity check that the replication stream is in sequence. We want to
         * forestall any possible corruption from replaying invalid entries.
         */
        if (!VLSN.follows(wireRecord.getVLSN(), lastReplayedVLSN)) {
            throw EnvironmentFailureException.unexpectedState
                (repImpl,
                 "Rep stream not sequential. Current VLSN: " +
                 lastReplayedVLSN +
                 " next log entry VLSN: " + wireRecord.getVLSN());
        }

        if (logger.isLoggable(Level.FINEST)) {
            LoggerUtils.finest(logger, repImpl, "Replaying " + wireRecord);
        }

        final ReplayTxn repTxn = getReplayTxn(entry.getCreateNs(), logEntry);

        updateReplicaSequences(logEntry);
        final byte entryType = wireRecord.getEntryType();

        lastReplayedVLSN = wireRecord.getVLSN();

        try {
            final long txnId = repTxn.getId();

            if (LOG_TXN_COMMIT.equalsType(entryType)) {
                Protocol.Commit commitEntry = (Protocol.Commit) entry;

                final boolean needsAck = commitEntry.getNeedsAck();
                final SyncPolicy txnSyncPolicy =
                    commitEntry.getReplicaSyncPolicy();
                final SyncPolicy implSyncPolicy =
                    needsAck ?
                    getImplSyncPolicy(txnSyncPolicy) :
                    noAckSyncPolicy;

                logReplay(repTxn, needsAck, implSyncPolicy);

                final TxnCommit commit = (TxnCommit) logEntry.getMainItem();
                final ReplicationContext commitContext =
                    new ReplicationContext(lastReplayedVLSN);
                repTxn.setMasterCommitState(commitContext, commit);
                repTxn.checkCommitFollows(lastReplayedTxn);
                updateDTVLSN(commit);

                if (needsAck) {

                    /*
                     * Only wait if the replica is not lagging and the
                     * durability requires it.
                     */
                    repImpl.getRepNode().getVLSNFreezeLatch()
                        .awaitThaw(currTerm, currMaster);
                    repImpl.getRepNode().getMasterStatus().assertSync();
                }

                try {
                    repTxn.commit(txnSyncPolicy, commitContext);
                } catch (DatabaseException e) {
                    /*
                     * Txn.postLogCommitHook() will call GroupCommit.bufferCommit(),
                     * which may throw out a ThreadInterruptedException, and
                     * the root cause should be in Replay.queueAck().
                     * Convert the exception back to IOException here.
                     */
                    if (e.getMessage().contains("Ack I/O interrupted")) {
                        throw new IOException("Ack I/O interrupted",
                            e.getCause());
                    }
                    throw e;
                }

                final long masterCommitTimeMs = commit.getTime().getTime();
                lastReplayedTxn = repTxn;

                updateCommitStats(needsAck, implSyncPolicy, startNs,
                                  masterCommitTimeMs,
                                  TimeSupplier.currentTimeMillis(),
                                  repTxn.elapsedNs());

                /* Respond to the feeder. */
                if (needsAck) {
                    /*
                     * Need an ack, queue it under certain condition.
                     * 1. If groupCommit is off, then queue the ack right away.
                     * 2. If groupCommit is on:
                     *    a. If repTxn is in Sync mode, no need to queue the ack,
                     *       since when repTxn commits, it has already been added
                     *       to the pending buffer. Also avoid to add it to the
                     *       pending buffer again using isGroupCommitted() as a
                     *       check.
                     *    b. If repTxn is not in Sync mode:
                     *       If the groupCommit pending buffer is empty,
                     *       queue the ack right away.
                     *       Otherwise, add the repTxn to the groupCommit buffer
                     *       in order to keep the ack in order.
                     */
                    if (groupCommit.queueAck(txnSyncPolicy)) {
                        queueAck(txnId);
                    } else if (!repTxn.isGroupCommitted()) {
                         groupCommit.bufferCommit(System.nanoTime(),
                                                  repTxn, repTxn.getVLSN());
                    }
                }

                /*
                 * The group refresh and recalculation can be expensive, since
                 * it may require a database read. Do it after the ack.
                 */
                if (repTxn.getRepGroupDbChange() && canRefreshGroup(repTxn)) {
                    repImpl.getRepNode().refreshCachedGroup();
                }

            } else if (LOG_TXN_ABORT.equalsType(entryType)) {

                nAborts.increment();
                final TxnAbort abort = (TxnAbort) logEntry.getMainItem();
                final ReplicationContext abortContext =
                    new ReplicationContext(wireRecord.getVLSN());
                if (logger.isLoggable(Level.FINEST)) {
                    LoggerUtils.finest(logger, repImpl,
                                       "abort called for " + txnId +
                                       " masterId=" +
                                       abort.getMasterId() +
                                       " repContext=" + abortContext);
                }
                // TODO: remod repTxn.checkCommitFollows(lastReplayedTxn);
                updateDTVLSN(abort);
                repTxn.abort(abortContext, abort);
                lastReplayedTxn = repTxn;
                if (repTxn.getRepGroupDbChange() && canRefreshGroup(repTxn)) {

                    /*
                     * Refresh is the safe thing to do on an abort, since a
                     * refresh may have been held back from an earlier commit
                     * due to this active transaction.
                     */
                    repImpl.getRepNode().refreshCachedGroup();
                }
                updateTxnStats(repTxn.elapsedNs());

            } else if (LOG_NAMELN_TRANSACTIONAL.equalsType(entryType)) {

                repImpl.getRepNode().getReplica().clearDbTreeCache();
                nNameLNs.increment();
                applyNameLN(repTxn, wireRecord);

            } else {
                nLNs.increment();
                /* A data operation. */
                assert wireRecord.getLogEntry() instanceof LNLogEntry;
                applyLN(repTxn, wireRecord);
            }

            /* Remember the last VLSN applied by this txn. */
            repTxn.setLastAppliedVLSN(lastReplayedVLSN);

        } catch (DatabaseException e) {
            e.addErrorMessage("Problem seen replaying entry " + wireRecord);
            throw e;
        } finally {
            final long elapsedNs = System.nanoTime() - startNs;
            if (elapsedNs > replayLoggingThresholdNs) {
                LoggerUtils.info(logger, repImpl,
                                 "Replay time for entry type:" +
                                 LogEntryType.findType(entryType) + " " +
                                 NANOSECONDS.toMillis(elapsedNs) + "ms " +
                                 "exceeded threshold:" +
                                 NANOSECONDS.
                                     toMillis(replayLoggingThresholdNs) +
                                 "ms");
            }
        }
    }

    /* Update the delay stats for the entry, to track how long the entry
     * has been in the queue. */
    private void updateEntryDelayStats(long nowNs, Protocol.Entry entry) {
        final long delayNs = nowNs - entry.getCreateNs();
        final long delayMs = TimeUnit.NANOSECONDS.toMillis(delayNs);
        replayQueueAvgDelayNs.add(delayNs);
        replayQueueMaxDelayNs.setMax(delayNs);
        replayQueue95DelayMs.add(delayMs);
        replayQueue99DelayMs.add(delayMs);
    }

    /**
     * Update the replica's in-memory DTVLSN using the value in the
     * commit/abort entry.
     *
     * In the normal course of events, DTVLSNs should not decrease. However,
     * there is just one exception: if the rep stream transitions from a post
     * to a pre-dtvlsn stream, it will transition from a positive to the
     * UNINITIALIZED_VLSN.
     *
     * A transition from a pre to a post-dtvlsn transition (from zero to some
     * positive value), observes the "DTVLSNs should not decrease" rule
     * automatically.
     */
    private void updateDTVLSN(final TxnEnd txnEnd) {
        final long txnDTVLSN = txnEnd.getDTVLSN();

        if (txnDTVLSN == UNINITIALIZED_VLSN) {
            /*
             * A pre DTVLSN format entry, simply set it as the in-memory DTVLSN
             */
            final long prevDTVLSN = repImpl.getRepNode().setDTVLSN(txnDTVLSN);
            if (prevDTVLSN != UNINITIALIZED_VLSN) {
                LoggerUtils.info(logger, repImpl,
                                 "Transitioned to pre DTVLSN stream." +
                                 " DTVLSN:" + prevDTVLSN +
                                 " at VLSN:" + lastReplayedVLSN);

            }
            lastReplayedDTVLSN = txnDTVLSN;
            return;
        }

        /* Sanity check. */
        if (txnDTVLSN < lastReplayedDTVLSN) {
            String msg = "DTVLSNs must be in ascending order in the stream. " +
                " prev DTVLSN:" + lastReplayedDTVLSN +
                " next DTVLSN:" + txnDTVLSN + " at VLSN: " +
                lastReplayedVLSN;
          throw EnvironmentFailureException.unexpectedState(repImpl, msg);
        }

        if ((lastReplayedDTVLSN == UNINITIALIZED_VLSN) &&
            (txnDTVLSN > 0)) {
            LoggerUtils.info(logger, repImpl,
                             "Transitioned to post DTVLSN stream." +
                             " DTVLSN:" + txnDTVLSN +
                             " at VLSN:" + lastReplayedVLSN);
        }

        lastReplayedDTVLSN = txnDTVLSN;
        repImpl.getRepNode().setDTVLSN(txnDTVLSN);
    }

    /**
     * Queue the request ack for an async ack write to the network.
     */
    public boolean queueAck(final long txnId) throws IOException {
        try {
            return outputQueue.offer(
                txnId, outputQueueOfferTimeoutMillis,
                TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            /*
             * Have the higher levels treat it like an IOE and
             * exit the thread.
             */
            throw new IOException("Ack I/O interrupted", ie);
        }
    }

    /**
     * Logs information associated with the replay of the txn commit
     */
    private void logReplay(ReplayTxn repTxn,
                           boolean needsAck,
                           SyncPolicy syncPolicy) {

        if (!logger.isLoggable(Level.FINE)) {
            return;
        }

        if (needsAck) {
            LoggerUtils.fine(logger, repImpl,
                             "Replay: got commit for txn=" + repTxn.getId() +
                             ", ack needed, replica sync policy=" +
                             syncPolicy +
                             " vlsn=" + lastReplayedVLSN);
        } else {
            LoggerUtils.fine(logger, repImpl,
                             "Replay: got commit for txn=" + repTxn.getId() +
                             " ack not needed" +
                             " vlsn=" + lastReplayedVLSN);
        }
    }

    /**
     * Returns true if there are no other activeTxns that have also modified
     * the membership database and are still open, since they could potentially
     * hold write locks that would block the read locks acquired during the
     * refresh operation.
     *
     * @param txn the current txn being committed or aborted
     *
     * @return true if there are no open transactions that hold locks on the
     * membership database.
     */
    private boolean canRefreshGroup(ReplayTxn txn) {

        /*
         * Use synchronized rather than copyActiveTxns, since this is called
         * during replay and there is no nested locking to worry about.
         */
        synchronized (activeTxns) {
            // TODO: very inefficient
            for (ReplayTxn atxn : activeTxns.getMap().values()) {
                if (atxn == txn) {
                    continue;
                }
                if (atxn.getRepGroupDbChange()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Update this replica's node, txn and database sequences with any ids in
     * this log entry. We can call update, even if the replay id doesn't
     * represent a new lowest-id point, or if the apply is not successful,
     * because the apply checks that the replay id is < the sequence on the
     * replica. We just want to ensure that if this node becomes the master,
     * its sequences are in sync with what came before in the replication
     * stream, and ids are not incorrectly reused.
     */
    private void updateReplicaSequences(LogEntry logEntry) {

        /* For now, we assume all replay entries have a txn id. */
        repImpl.getTxnManager().updateFromReplay(logEntry.getTransactionId());

        /* If it's a database operation, update the database id. */
        if (logEntry instanceof NameLNLogEntry) {
            NameLNLogEntry nameLogEntry = (NameLNLogEntry) logEntry;
            nameLogEntry.postFetchInit(false /*isDupDb*/);
            NameLN nameLN = (NameLN) nameLogEntry.getLN();
            repImpl.getDbTree().updateFromReplay(nameLN.getId());
        }
    }

    /**
     * Create a replayTxn from a MasterTxn if needed, due to a transition from
     * a master to a replica.
     */
    public ReplayTxn getReplayTxn(final MasterTxn mtxn)
        throws DatabaseException {

        final long txnId = mtxn.getTransactionId();
        return activeTxns.
            computeIfAbsent(txnId,
                            ((long tid) ->
                            new ReplayTxn(repImpl, tid, mtxn.getStartNs(),
                                          activeTxns, logger) {
                                @Override
                                protected
                                boolean registerImmediately() {
                                    return false;
                                }
                            }));
    }

    /**
     * Create a replay txn from a log entry if needed.
     */
    private ReplayTxn getReplayTxn(long txnStartNs, LogEntry logEntry) {

        final long txnId = logEntry.getTransactionId();
        return activeTxns.
            computeIfAbsent(txnId, ((long tid) ->
                                    new ReplayTxn(repImpl, tid, txnStartNs,
                                                  activeTxns, logger)));
    }

    /**
     * Replays the NameLN.
     *
     * Note that the operations: remove, rename and truncate need to establish
     * write locks on the database. Any open handles are closed by this
     * operation by virtue of the ReplayTxn's importunate property.  The
     * application will receive a LockPreemptedException if it subsequently
     * accesses the database handle.
     */
    private void applyNameLN(ReplayTxn repTxn,
                             InputWireRecord wireRecord)
        throws DatabaseException {

        NameLNLogEntry nameLNEntry = (NameLNLogEntry) wireRecord.getLogEntry();
        final NameLN nameLN = (NameLN) nameLNEntry.getLN();

        String databaseName = StringUtils.fromUTF8(nameLNEntry.getKey());

        final DbOpReplicationContext repContext =
            new DbOpReplicationContext(wireRecord.getVLSN(), nameLNEntry);

        DbOperationType opType = repContext.getDbOperationType();
        DatabaseImpl dbImpl = null;
        try {
            switch (opType) {
                case CREATE:
                {
                    DatabaseConfig dbConfig =
                        repContext.getCreateConfig().getReplicaConfig(repImpl);

                    dbImpl = repImpl.getDbTree().createReplicaDb
                      (repTxn, databaseName, dbConfig, nameLN, repContext);

                    /*
                     * We rely on the RepGroupDB.DB_ID value, so make sure
                     * it's what we expect for this internal replicated
                     * database.
                     */
                    if ((dbImpl.getId().getId() == RepGroupDB.DB_ID) &&
                        !DbType.REP_GROUP.getInternalName().equals
                        (databaseName)) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Database: " +
                             DbType.REP_GROUP.getInternalName() +
                             " is associated with id: " +
                             dbImpl.getId().getId() +
                             " and not the reserved database id: " +
                             RepGroupDB.DB_ID);
                    }

                    TriggerManager.runOpenTriggers(repTxn, dbImpl, true);
                    break;
                }

                case REMOVE: {
                    dbImpl = repImpl.getDbTree().getDb(nameLN.getId());
                    try {
                        repImpl.getDbTree().removeReplicaDb
                            (repTxn, databaseName, nameLN.getId(), repContext);
                        TriggerManager.runRemoveTriggers(repTxn, dbImpl);
                    } catch (DatabaseNotFoundException e) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Database: " + dbImpl.getName() +
                             " Id: " + nameLN.getId() +
                             " not found on the Replica.");
                    }
                    break;
                }

                case TRUNCATE: {
                    dbImpl = repImpl.getDbTree().getDb
                        (repContext.getTruncateOldDbId());
                    try {
                        TruncateDbResult result =
                        repImpl.getDbTree().truncateReplicaDb
                            (repTxn, databaseName, false, nameLN, repContext);
                        TriggerManager.runTruncateTriggers(repTxn, result.newDb);
                    } catch (DatabaseNotFoundException e) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Database: " + dbImpl.getName() +
                             " Id: " + nameLN.getId() +
                             " not found on the Replica.");
                    }

                    break;
                }

                case RENAME: {
                    dbImpl = repImpl.getDbTree().getDb(nameLN.getId());
                    try {
                        dbImpl =
                        repImpl.getDbTree().renameReplicaDb
                            (repTxn, dbImpl.getName(), databaseName, nameLN,
                             repContext);
                        TriggerManager.runRenameTriggers(repTxn, dbImpl,
                                                         databaseName);
                    } catch (DatabaseNotFoundException e) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Database rename from: " + dbImpl.getName() +
                             " to " + databaseName +
                             " failed, name not found on the Replica.");
                    }
                    break;
                }

                case UPDATE_CONFIG: {
                    /* Get the replicated database configurations. */
                    DatabaseConfig dbConfig =
                        repContext.getCreateConfig().getReplicaConfig(repImpl);

                    /* Update the NameLN and write it to the log. */
                    dbImpl = repImpl.getDbTree().getDb(nameLN.getId());
                    final String dbName = dbImpl.getName();
                    repImpl.getDbTree().updateNameLN
                        (repTxn, dbName, repContext);

                    /* Set the new configurations to DatabaseImpl. */
                    dbImpl.setConfigProperties
                        (repTxn, dbName, dbConfig, repImpl);

                    repImpl.getDbTree().modifyDbRoot(dbImpl);

                    break;
                }

                default:
                    throw EnvironmentFailureException.unexpectedState
                        ("Illegal database op type of " + opType.toString() +
                         " from " + wireRecord + " database=" + databaseName);
            }
        } finally {
            if (dbImpl != null) {
                repImpl.getDbTree().releaseDb(dbImpl);
            }
        }
    }

    private void applyLN(
        final ReplayTxn repTxn,
        final InputWireRecord wireRecord)
        throws DatabaseException {

        final LNLogEntry<?> lnEntry = (LNLogEntry<?>) wireRecord.getLogEntry();
        final DatabaseId dbId = lnEntry.getDbId();

        /*
         * If this is a change to the rep group db, remember at commit time,
         * and refresh this node's group metadata.
         */
        if (dbId.getId() == RepGroupDB.DB_ID) {
            repTxn.noteRepGroupDbChange();
        }

        final DatabaseImpl dbImpl =
            repImpl.getRepNode().getReplica().getDbCache().get(dbId, repTxn);

        lnEntry.postFetchInit(dbImpl);

        final ReplicationContext repContext =
            new ReplicationContext(wireRecord.getVLSN());

        ReplayPreprocessor preprocessor = wireRecord.getPreprocessor();
        if (preprocessor != null) {
            preprocessor = preprocessor.takeForReplay();
            if (preprocessor == null) {
                nNotPreprocessed.increment();
            }
        }

        try (final Cursor cursor = DbInternal.makeCursor(
                dbImpl, repTxn, null /*cursorConfig*/)) {

            final LN ln = lnEntry.getLN();

            /* In a dup DB, do not expect embedded LNs or non-empty data. */
            if (dbImpl.getSortedDuplicates() &&
                (lnEntry.isEmbeddedLN() ||
                 (ln.getData() != null && ln.getData().length > 0))) {

                throw EnvironmentFailureException.unexpectedState(
                    dbImpl.getEnv(),
                    "[#25288] emb=" + lnEntry.isEmbeddedLN() +
                    " key=" + Key.getNoFormatString(lnEntry.getKey()) +
                    " data=" + Key.getNoFormatString(ln.getData()) +
                    " vlsn=" + ln.getVLSNSequence());
            }

            if (ln.isDeleted()) {
                replayKeyEntry.setData(lnEntry.getKey());
                
                /**
                 * Before Image Support for replication
                 */
                OperationResult result = null;
                if (lnEntry instanceof BeforeImageLNLogEntry) {
                    BeforeImageContext bImgCtx =  new BeforeImageContext(
                            ((BeforeImageLNLogEntry)lnEntry)
                             .getBeforeImageExpiration(),
                            ((BeforeImageLNLogEntry)lnEntry)
                             .isBeforeImageExpirationInHours());

                     result = DbInternal.searchAndDelete(
                             cursor, replayKeyEntry, preprocessor,
                             lnEntry.getModificationTime(),
                             lnEntry.getExpirationTime(), repContext,
                             null /*cacheMode*/, true /*allowBlindDelete*/,
                             bImgCtx);
        
                } else {
                    result = DbInternal.searchAndDelete(
                            cursor, replayKeyEntry, preprocessor,
                            lnEntry.getModificationTime(),
                            lnEntry.getExpirationTime(), repContext,
                            null /*cacheMode*/, true /*allowBlindDelete*/,
                            null);
                }

                if (result == null) {
                    handleDeletionFailure(
                        dbImpl, cursor, lnEntry, wireRecord, repContext);
                }
            } else {
                replayKeyEntry.setData(lnEntry.getKey());
                replayDataEntry.setData(ln.getData());

                final OperationResult result = DbInternal.putForReplay(
                    cursor, replayKeyEntry, replayDataEntry, lnEntry,
                    preprocessor, PutMode.OVERWRITE, repContext);

                /*
                 * The put call cannot fail with a null return since
                 * PutMode.OVERWRITE is used but we check it for good measure.
                 */
                if (result == null) {
                    throw fatalOpException(
                        lnEntry, wireRecord,
                        repImpl.getExtinctionStatus(dbImpl, lnEntry.getKey()));
                }
            }
        }

        /*
         * Queue new extinct scans for later processing. Must be done after
         * applying the LN so the record is present in the Btree when the
         * extinct scanner thread tries to read it.
         */
        if (lnEntry.getLogType().equals(
            LogEntryType.LOG_EXTINCT_SCAN_LN_TRANSACTIONAL)) {

            repImpl.getExtinctionScanner().replay(
                lnEntry.getKey(), lnEntry.getData());
        }
    }

    /**
     * For a deletion we expect the record to exist in the Btree and this
     * method handles the case where it does not exist.
     *
     * <p>Allow the deletion failure for records that are expired (within TTL
     * clock tolerance), since these can be deleted from the Btree by
     * background IN compression. A record may expire after the master
     * deletion op and before the replica replay, e.g., due to a lagging
     * replica or a replica that was down during the deletion.</p>
     *
     * <p>Also allow the deletion failure for EXTINCT and MAYBE_EXTINCT
     * records to account for apps that try to delete extinct records. This is
     * illegal in the API but should not bring down the RN.</p>
     *
     * <p>If the deletion failure is not allowed, an EFE is thrown to
     * invalidate the env. If the failure is allowed, the deletion is logged
     * directly (without accessing the Btree) to ensure creation of a complete
     * replication stream with no gaps in the VLSN sequence. [#27329]
     */
    private void handleDeletionFailure(
        final DatabaseImpl dbImpl,
        final Cursor cursor,
        final LNLogEntry<?> lnEntry,
        final InputWireRecord wireRecord,
        final ReplicationContext repContext) {

        final ExtinctionStatus extinctionStatus =
            repImpl.getExtinctionStatus(dbImpl, lnEntry.getKey());

        final boolean isExpired = repImpl.expiresWithin(
            lnEntry.getExpiration(), lnEntry.isExpirationInHours(),
            repImpl.getTtlClockTolerance());

        if (!isExpired && extinctionStatus == NOT_EXTINCT) {
            throw fatalOpException(lnEntry, wireRecord, extinctionStatus);
        }

        final CursorImpl cursorImpl = DbInternal.getCursorImpl(cursor);
        cursorImpl.logDeletionForReplay(lnEntry, repContext);
    }

    private EnvironmentFailureException fatalOpException(
        final LNLogEntry<?> lnEntry,
        final InputWireRecord wireRecord,
        final ExtinctionStatus extinctionStatus) {

        final String expTime = (lnEntry.getExpiration() == 0) ?
            "none" :
            TTL.formatExpiration(
                lnEntry.getExpiration(),
                lnEntry.isExpirationInHours());

        final String key = repImpl.getExposeUserData() ?
            Key.dumpString(lnEntry.getKey(), 0) : "unknown";

        return new EnvironmentFailureException(
            repImpl,
            EnvironmentFailureReason.LOG_INCOMPLETE,
            "Replicated operation could  not be applied. " +
                " vlsn=" + wireRecord.getVLSN() +
                " expirationTime=" + expTime +
                " extinctionStatus=" + extinctionStatus +
                " key=" + key + " " + wireRecord);
    }

    /**
     * Go through all active txns and rollback up to but not including the log
     * entry represented by the matchpoint VLSN.
     *
     * Effectively truncate these rolled back log entries by making them
     * invisible. Flush the log first, to make sure these log entries are out
     * of the log buffers and are on disk, so we can reliably find them through
     * the FileManager.
     *
     * Rollback steps are described in
     * https://sleepycat.oracle.com/trac/wiki/Logging#Recoverysteps. In
     * summary,
     *
     * 1. Log and fsync a new RollbackStart record
     * 2. Do the rollback in memory. There is no need to explicitly
     *    log INs made dirty by the rollback operation.
     * 3. Do invisibility masking by overwriting LNs.
     * 4. Fsync all overwritten log files at this point.
     * 5. Write a RollbackEnd record, for ease of debugging
     *
     * Note that application read txns  can continue to run during syncup.
     * Reader txns cannot access records that are being rolled back, because
     * they are in txns that are not committed, i.e, they are write locked.
     * The rollback interval never includes committed txns, and we do a hard
     * recovery if it would include them.
     */
    public void rollback(long matchpointVLSN, long matchpointLsn) {

        String rollbackStatus = RBSTATUS_START;

        final Map<Long, ReplayTxn> localActiveTxns = copyActiveTxns();
        try {
            if (localActiveTxns.size() == 0) {
                /* no live read/write txns, nothing to do. */
                rollbackStatus = RBSTATUS_NO_ACTIVE;
                return;
            }

            VLSNRange range = repImpl.getVLSNIndex().getRange();
            if (range.getLast() == matchpointVLSN) {
                /* nothing to roll back. */
                rollbackStatus = RBSTATUS_RANGE_EQUALS;
                return;
            }

            repImpl.setSyncupProgress(SyncupProgress.DO_ROLLBACK);

            /*
             * Stop the log file backup service, since the files will be in an
             * inconsistent state while the rollback is in progress.
             */
            repImpl.getRepNode().shutdownNetworkBackup();

            /*
             * Set repImpl's isRollingBack to true, and invalidate all the in
             * progress DbBackup.
             */
            repImpl.setBackupProhibited(true);
            repImpl.invalidateBackups(DbLsn.getFileNumber(matchpointLsn));

            /*
             * Let the rest of the node know we doing a rollback.
             */
            repImpl.setReplayRollback(true);

            /*
             * 1. Log RollbackStart. The fsync guarantees that this marker will
             * be present in the log for recovery. It also ensures that all log
             * entries will be flushed to disk and the TxnChain will not have
             * to worry about entries that are in log buffers when constructing
             * the rollback information.
             */
            LogManager logManager = repImpl.getLogManager();
            LogEntry rollbackStart = SingleItemEntry.create(
                LogEntryType.LOG_ROLLBACK_START,
                new RollbackStart(
                    matchpointVLSN, matchpointLsn, localActiveTxns.keySet()));
            long rollbackStartLsn =
                logManager.logForceFlush(rollbackStart,
                                         true, // fsyncRequired,
                                         ReplicationContext.NO_REPLICATE);
            rollbackStatus = RBSTATUS_LOG_RBSTART;

            /*
             * 2. Do rollback in memory. Undo any operations that were logged
             * after the matchpointLsn, and save the LSNs for those log
             * entries.. There should be something to undo, because we checked
             * earlier that there were log entries after the matchpoint.
             */
            List<Long> rollbackLsns = new ArrayList<>();
            for (ReplayTxn replayTxn : localActiveTxns.values()) {
                Collection<Long> txnRollbackLsns =
                    replayTxn.rollback(matchpointLsn);

                /*
                 * Txns that were entirely rolled back should have been removed
                 * from the activeTxns map.
                 */
                assert checkRemoved(replayTxn) :
                    "Should have removed " + replayTxn;

                rollbackLsns.addAll(txnRollbackLsns);
            }
            rollbackStatus = RBSTATUS_MEM_ROLLBACK;
            assert rollbackLsns.size() != 0 : dumpActiveTxns(matchpointLsn);

            /*
             * 3 & 4 - Mark the rolled back log entries as invisible.  But
             * before doing so, invoke any registered rewrite listeners, so the
             * application knows that existing log files will be modified.
             *
             * After all are done, fsync the set of files. By waiting, some may
             * have made it out on their own.
             */
            LogFileRewriteListener listener = repImpl.getLogRewriteListener();
            if (listener != null) {
                listener.rewriteLogFiles(getFileNames(rollbackLsns));
            }
            RollbackTracker.makeInvisible(repImpl, rollbackLsns);
            rollbackStatus = RBSTATUS_INVISIBLE;

            /*
             * 5. Log RollbackEnd. Flush it so that we can use it to optimize
             * recoveries later on. If the RollbackEnd exists, we can skip the
             * step of re-making LNs invisible.
             */
            logManager.logForceFlush(
                SingleItemEntry.create(LogEntryType.LOG_ROLLBACK_END,
                                       new RollbackEnd(matchpointLsn,
                                                       rollbackStartLsn)),
                 true, // fsyncRequired
                 ReplicationContext.NO_REPLICATE);

            /*
             * Restart the backup service only if all the steps of the
             * rollback were successful.
             */
            repImpl.getRepNode().restartNetworkBackup();
            repImpl.setBackupProhibited(false);
            rollbackStatus = RBSTATUS_FINISH;
        } finally {
            repImpl.setReplayRollback(false);

            /* Reset the lastReplayedVLSN so it's correct when we resume. */
            lastReplayedVLSN = matchpointVLSN;
            LoggerUtils.info(logger, repImpl,
                             "Rollback to matchpoint " + matchpointVLSN +
                             " at " + DbLsn.getNoFormatString(matchpointLsn) +
                             " status=" + rollbackStatus);
        }
    }

    /* For debugging support */
    private String dumpActiveTxns(long matchpointLsn) {
        StringBuilder sb = new StringBuilder();
        sb.append("matchpointLsn=");
        sb.append(DbLsn.getNoFormatString(matchpointLsn));
        for (ReplayTxn replayTxn : copyActiveTxns().values()) {
            sb.append("txn id=").append(replayTxn.getId());
            sb.append(" locks=").append(replayTxn.getWriteLockIds());
            sb.append("lastLogged=");
            sb.append(DbLsn.getNoFormatString(replayTxn.getLastLsn()));
            sb.append("\n");
        }

        return sb.toString();
    }

    private Set<File> getFileNames(List<Long> lsns) {
        Set<Long> fileNums = new HashSet<>();
        Set<File> files = new HashSet<>();

        for (long lsn : lsns) {
            fileNums.add(DbLsn.getFileNumber(lsn));
        }
        for (long fileNum : fileNums) {
            files.add(new File(FileManager.getFileName(fileNum)));
        }
        return files;
    }

    private boolean checkRemoved(ReplayTxn txn) {
        if (txn.isClosed()) {
            if (activeTxns.get(txn.getId()) != null) {
                return false;
            }
        }

        return true;
    }

    /**
     * Make a copy of activeTxns to avoid holding its mutex while iterating.
     * Can be used whenever the cost of the HashMap copy is not significant.
     */
    private Map<Long, ReplayTxn> copyActiveTxns() {
        return activeTxns.getMap();
    }

    /**
     * Release all transactions, database handles, etc held by the replay
     * unit. The Replicator is closing down and Replay will not be invoked
     * again.
     */
    public void close() {

        for (ReplayTxn replayTxn : copyActiveTxns().values()) {
            try {
                if (logger.isLoggable(Level.FINE)) {
                    LoggerUtils.fine(logger, repImpl,
                                     "Unregistering open replay txn: " +
                                     replayTxn.getId());
                }
                replayTxn.cleanup();
            } catch (DatabaseException e) {
                LoggerUtils.fine(logger, repImpl,
                                 "Replay txn: " + replayTxn.getId() +
                                 " unregistration failed: " + e.getMessage());
            }
        }
        assert activeTxns.isEmpty();
    }

    /**
     * Returns a copy of the statistics associated with Replay
     */
    public StatGroup getStats(StatsConfig config) {
        if (groupCommit != null) {
            statistics.addAll(groupCommit.getStats(config));
        }
        return statistics.cloneGroup(config.getClear());
    }

    public void resetStats() {
        statistics.clear();
        groupCommit.clearStats();
    }

    /* For unit tests */
    public SimpleTxnMap<ReplayTxn> getActiveTxns() {
        return activeTxns;
    }

    public String dumpState() {
        StringBuilder sb = new StringBuilder();
        sb.append("lastReplayedTxn=").append(lastReplayedTxn);
        sb.append(" lastReplayedVLSN=").append(lastReplayedVLSN);
        sb.append(" numActiveReplayTxns=").append(activeTxns.size());
        sb.append("\n");
        return sb.toString();
    }

    /**
     * Write out any pending acknowledgments. See GroupCommit.flushPendingAcks
     * for details. This method is invoked after each log entry is read from
     * the replication stream.
     *
     * @param nowNs the time at the reading of the log entry
     */
    void flushPendingAcks(long nowNs)
        throws IOException {

        groupCommit.flushPendingAcks(nowNs);
    }

    public void setPendingAcks(int value) {
        groupCommit.setPendingAcks(value);
        outputQueueOfferTimeoutMillis = 0;
    }

    /**
     * See GroupCommit.getPollIntervalNs(long)
     */
    long getPollIntervalNs(long defaultNs) {
        return groupCommit.
            getPollIntervalNs(defaultNs);
    }

    /**
     * Returns the sync policy to be implemented at the replica. If
     * group commit is active, and SYNC is requested it will return
     * NO_SYNC instead to delay the fsync.
     *
     * @param txnSyncPolicy the sync policy as stated in the txn
     *
     * @return the sync policy to be implemented by the replica
     */
    private SyncPolicy getImplSyncPolicy(SyncPolicy txnSyncPolicy) {
        return ((txnSyncPolicy ==  SyncPolicy.SYNC) && groupCommit.isEnabled()) ?
                SyncPolicy.NO_SYNC : txnSyncPolicy;
    }

}
