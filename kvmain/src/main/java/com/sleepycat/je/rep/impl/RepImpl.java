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

package com.sleepycat.je.rep.impl;

import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;
import static com.sleepycat.je.rep.impl.RepParams.FEEDER_TCP_NO_DELAY;
import static com.sleepycat.je.rep.impl.RepParams.NODE_NAME;
import static com.sleepycat.je.rep.impl.RepParams.REPLAY_FREE_DISK_PERCENT;
import static com.sleepycat.je.rep.impl.RepParams.REPLICA_RECEIVE_BUFFER_SIZE;
import static com.sleepycat.je.rep.impl.RepParams.REPSTREAM_OPEN_TIMEOUT;
import static com.sleepycat.je.rep.impl.RepParams.TEST_JE_VERSION;
import static com.sleepycat.je.rep.impl.RepParams.VLSN_MAX_DIST;
import static com.sleepycat.je.rep.impl.RepParams.VLSN_MAX_MAP;
import static com.sleepycat.je.rep.impl.RepParams.VLSN_STRIDE;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Formatter;
import java.util.logging.Level;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.cleaner.ExtinctionScanner;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.RestoreRequired;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.recovery.VLSNRecoveryProxy;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.LogFileRewriteListener;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.RepStatManager;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.SyncupProgress;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.node.Feeder;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.MasterTransfer;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.impl.node.NodeState;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.Replay;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.stream.ArbiterFeederSource;
import com.sleepycat.je.rep.stream.FeederFilter;
import com.sleepycat.je.rep.stream.FeederReader;
import com.sleepycat.je.rep.stream.FeederTxns;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.subscription.Subscription;
import com.sleepycat.je.rep.txn.MasterThreadLocker;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.rep.txn.ReadonlyTxn;
import com.sleepycat.je.rep.txn.ReplayTxn;
import com.sleepycat.je.rep.txn.ReplicaThreadLocker;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.StatCaptureRepDefinitions;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.rep.vlsn.VLSNRecoveryTracker;
import com.sleepycat.je.statcap.StatManager;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.ThreadLocker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.txn.VersionedWriteTxnEnd;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.utilint.BooleanStat;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvgStat;
import com.sleepycat.je.utilint.RateLimitingLogger;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StringStat;
import com.sleepycat.je.utilint.VLSN;

public class RepImpl
    extends EnvironmentImpl
    implements RepEnvConfigObserver {

    private VLSNIndex vlsnIndex;
    /* VLSNIndexAccess coordinates the closing of the vlsn index */
    private final VLSNIndexAccess vlsnIndexAccess = new VLSNIndexAccess();

    private final FeederTxns feederTxns;

    /*
     * The repNode is only non-null when the replicated environment has joined
     * a group. It's null otherwise.
     */
    private volatile RepNode repNode;
    private volatile UUID uuid;
    private Replay replay;

    /*
     * This is the canonical nameIdPair instance used by the node. The internal
     * Id part of the pair will be updated when the node actually joins the
     * group.
     */
    private NameIdPair nameIdPair;

    private final NodeState nodeState;

    /*
     * The clockskew used by this environment in ms. It's only used by testing
     * to inject clock skew between ReplicatedEnvironments.
     */
    private static int clockSkewMs = 0;

    /*
     * A handle to the group database. This handle is initialized lazily when
     * the contents of the database are first required. It's set to null upon
     * shutdown. The handle must be initialized lazily because the database is
     * created by the master, and we only know master identity later.  The
     * RepImpl manages the rep group database, so that the lifetime of the
     * databaseImpl handle can be managed more easily to mesh with the opening
     * and closing of the RepImpl.
     */
    private DatabaseImpl groupDbImpl = null;

    /* The status presents whether this replica is doing rollback. */
    private boolean backupProhibited = false;

    /*
     * Represents whether this Environment is allowed to convert a
     * non-replicated Environment to replicated.
     */
    private boolean allowConvert = false;

    /**
     * The {@link RepParams#CACHE_RECORD_VERSION} param.
     */
    private boolean cacheVLSN;

    private boolean optimizeStreamForSize;

    /*
     * True if TTL or record extinction is available. Volatile is not used,
     * since checking more than once is idempotent.
     */
    private boolean isTTLAvailable = false;
    private boolean isRecordExtinctionAvailable = false;

    /* Keep an eye on the ongoing DbBackups. */
    private final Set<DbBackup> backups = new HashSet<>();

    /*
     * True if replay is in the process of marking some log records invisible
     * while syncing with a master.  This can cause false VLSNs out of order
     * errors in DbVerifyLog [KVSTORE-1291].
     */
    private volatile boolean replayRollback = false;
    /*
     * The list of observers who are notified when a mutable rep param changes.
     */
    private final List<RepEnvConfigObserver> repConfigObservers;

    private final long insufficientRepTimeout;
    /*
     * Lock used to control access and lazy initialization of groupDbImpl,
     * ensuring that there is exactly one database made. A mutex is used rather
     * than synchronization to allow us to probe for contention on the
     * groupDbImpl.
     */
    private final ReentrantLock groupDbLock = new ReentrantLock();

    private int replicaAckTimeout;
    private int arbiterAckTimeout;
    private int replayTxnTimeout;
    private ReplicaConsistencyPolicy defaultConsistencyPolicy;
    private boolean allowArbiterAck;

    /*
     * Arbiters, subscribers and networkBackup use RepImpls which are read
     * only and have some daemon functionality disabled.
     */
    private boolean isArbiter;
    private boolean isSubscriber;
    private boolean isNetworkBackup;

    /*
     * NodeStats are currently not public, but we may want to evaluate
     * and decide if they would be useful, perhaps as a debugging aid.
     */
    private final StatGroup nodeStats;
    private final BooleanStat hardRecoveryStat;
    private final StringStat hardRecoveryInfoStat;
    private final LongAvgStat localTxnAvgNs;

    /**
     * Used to block transaction commit/abort execution in the preLogCommitAbort
     * hooks, before entering phase 2 of master transfer.
     */
    private volatile CountDownLatch blockTxnLatch = new CountDownLatch(0);

    /**
     * A lock used to coordinate access to {@link #blockTxnLatch}.
     * <p>
     * When a Master Transfer operation completes Phase 1, it sets a new {@code
     * CountDownLatch} in order to block the completion of transactions at the
     * commit or abort stage. We must avoid having it do so at an awkward
     * moment.
     * <p>
     * One such case is a brief period between the time a transaction "awaits"
     * the latch (in {@code checkBlock()}) and the time it publishes its VLSN.
     * We must avoid having Master Transfer read its "ultimate goal" VLSN
     * during that interval.
     *
     * The read lock is acquired by each transaction (regardless of whether a
     * MT is in progress) in the preLogCommitAbort hooks and is released in the
     * post hooks. The "fair" write lock is only acquired during phase 2 of MT.
     * It allows txns with existing read locks to finish, thus writing their
     * commit/abort log entries. The sole purpose of this latch is to allow for
     * changing the blockTxnLatch and is only held for the duration of the
     * change.
     *
     * With the transition to thread-oblivious StampedLock it should be
     * possible to eliminate the blockTxnLatch by extending the lifetime of the
     * write lock so that it's kept in place until the MT completes. This lock
     * simplification is something to explore in future.
     *
     * @see MasterTransfer
     * @see ReplicatedEnvironment#transferMaster
     */
    private final StampedLock blockLatchLock = new StampedLock();

    /* application listener for syncups. */
    private final ProgressListener<SyncupProgress> syncupProgressListener;

    /* Application callback to be notified before we overwrite log files. */
    private final LogFileRewriteListener logRewriteListener;

    /* Configuration for ServiceDispatcher communication */
    private final ReplicationNetworkConfig repNetConfig;

    /* disable opening tcp/ip ports */
    private final boolean disableOpeningPort;

    /*
     * Factory for creating channel instances.  Not available until
     * initializeChannelFactory is called.
     */
    private volatile DataChannelFactory channelFactory;

    /**
     * Used for testing, to create log files with
     * VLSN.UNINITIALIZED_VLSN as the value for the dtvlsn
     */
    private static boolean simulatePreDTVLSNMaster = false;

    /**
     * Unit test only
     * To prevent DTVLSN from growing beyond a given VLSN, used in unit test
     * to simulate a case that DTVLSN is blocked
     */
    public static volatile long CAPPED_DTVLSN = NULL_VLSN;
    /**
     * Unit test only, true to cap DTVLSN with {@link #CAPPED_DTVLSN}
     */
    private static volatile boolean isCappedDTVLSNMaster = false;

    /*
     * Used to verify VLSN invariants as they are written to the log on the
     * master
     */
    private long prevLoggedVLSN = NULL_VLSN;

    private long prevCommitVLSN = NULL_VLSN;
    private long prevLoggedDTVLSN = NULL_VLSN;
    private long prevLoggedTerm = MasterTerm.MIN_TERM;

    /*
     * The filter transmitted to a Feeder so that records can be filtered at
     * the source
     */
    private final FeederFilter feederFilter;
    
    private volatile Subscription subscription;

    /**
     * The feeder authenticator generator is instantiated by the replicated
     * environment creator.
     */
    private volatile StreamAuthenticator authenticator = null;

    /**
     * An JEHA-protocol-related (election and replay) rate limiting logger.
     */
    protected final RateLimitingLogger<String> replicationWarningLogger;

    public RepImpl(File envHome,
                   EnvironmentConfig envConfig,
                   EnvironmentImpl sharedCacheEnv,
                   ReplicationConfig repConfig)
        throws EnvironmentNotFoundException, EnvironmentLockedException {

        super(envHome, envConfig, sharedCacheEnv, repConfig);

        disableOpeningPort = repConfig.getDisableOpeningPort();

        allowConvert = RepInternal.getAllowConvert(repConfig);

        repConfigObservers = new ArrayList<>();
        addRepConfigObserver(this);
        /* added envStatLogger to RepConfigObserver, so change in
           repMutableConfig can be updated in the je config file */
        addRepConfigObserver(envStatLogger);

        repNetConfig = repConfig.getRepNetConfig();
        nodeState = new NodeState(nameIdPair, this);

        insufficientRepTimeout = getConfigManager().
                        getDuration(RepParams.INSUFFICIENT_REPLICAS_TIMEOUT);

        replicationWarningLogger = new RateLimitingLogger<>(
            (int) TimeUnit.MINUTES.toMillis(10) /* 10 minutes per warning */,
            10 /* number of warning events */,
            envLogger);

        if (isArbiter || isSubscriber || isNetworkBackup ) {
            nodeStats = null;
            syncupProgressListener = null;
            logRewriteListener = null;
            hardRecoveryStat = null;
            hardRecoveryInfoStat = null;
            feederTxns = null;
            feederFilter = null;
            localTxnAvgNs = null;
            return;
        }

        feederTxns = new FeederTxns(this);
        replay = new Replay(this, nameIdPair);

        nodeStats = new StatGroup(RepImplStatDefinition.GROUP_NAME,
                                  RepImplStatDefinition.GROUP_DESC);
        hardRecoveryStat = new BooleanStat(nodeStats,
                                           RepImplStatDefinition.HARD_RECOVERY);
        hardRecoveryInfoStat =
            new StringStat(nodeStats, RepImplStatDefinition.HARD_RECOVERY_INFO,
                           "This node did not incur a hard recovery.");
        localTxnAvgNs = feederTxns.getLocalTxnAvgNs();
        syncupProgressListener = repConfig.getSyncupProgressListener();
        logRewriteListener = repConfig.getLogFileRewriteListener();

        feederFilter = repConfig.getFeederFilter();

        authenticator = repConfig.getAuthenticator();

        uuid = RepGroupImpl.UNKNOWN_UUID;
    }

    /**
     * Used to re-initialize state each time a node transitions to the master
     * state, since the earlier values may not be meaningful
     */
    public void initAsMaster() {
        prevLoggedVLSN = NULL_VLSN;
        prevCommitVLSN = NULL_VLSN;
        prevLoggedDTVLSN = NULL_VLSN;
        prevLoggedTerm = MasterTerm.MIN_TERM;
        getTxnManager().setupGroupCommitMaster();
    }

    /**
     * Called by the EnvironmentImpl constructor.  Some rep params,
     * preserveVLSN for example, are accessed by the EnvironmentImpl via
     * methods (getPreserveVLSN for example), so they need to be initialized
     * early.
     */
    @Override
    protected void initConfigParams(EnvironmentConfig envConfig,
                                    ReplicationConfig repConfig) {

        /* Init standalone config params first. */
        super.initConfigParams(envConfig, repConfig);

        /* Init rep config params. */
        replicaAckTimeout =
            configManager.getDuration(RepParams.REPLICA_ACK_TIMEOUT);
        replayTxnTimeout =
            configManager.getDuration(RepParams.REPLAY_TXN_LOCK_TIMEOUT);
        defaultConsistencyPolicy = RepUtils.getReplicaConsistencyPolicy
            (configManager.get(RepParams.CONSISTENCY_POLICY));
        cacheVLSN =
            configManager.getBoolean(RepParams.CACHE_RECORD_VERSION);
        optimizeStreamForSize =
            configManager.getBoolean(RepParams.OPTIMIZE_STREAM_FOR_SIZE);
        allowArbiterAck =
            configManager.getBoolean(RepParams.ALLOW_ARBITER_ACK);
        isArbiter =
            configManager.getBoolean(RepParams.ARBITER_USE);
        isSubscriber =
            configManager.getBoolean(RepParams.SUBSCRIBER_USE);
        isNetworkBackup =
            configManager.getBoolean(RepParams.NETWORKBACKUP_USE);
        arbiterAckTimeout =
            configManager.getDuration(RepParams.ARBITER_ACK_TIMEOUT);
    }

    @Override
    protected Formatter initFormatter() {

        /*
         * The nameIdPair field is assigned here rather than in the constructor
         * because of base class/subclass dependencies. initFormatter() is
         * called by the base class constructor, and nameIdPair must be
         * available at that time.
         */
        nameIdPair = new NameIdPair(configManager.get(NODE_NAME));
        return new ReplicationFormatter(nameIdPair);
    }

    @Override
    protected DbConfigManager
        initConfigManager(EnvironmentConfig envConfig,
                          ReplicationConfig repConfig) {
        return new RepConfigManager(envConfig, repConfig);
    }

    @Override
    public boolean getAllowRepConvert() {
        return allowConvert;
    }

    @Override
    protected DbConfigManager resetConfigManager(EnvironmentConfig newConfig) {
        /* Save all the replication related properties. */
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        ReplicationConfig repConfig = repConfigManager.makeReplicationConfig();
        return new RepConfigManager(newConfig, repConfig);
    }

    public ReplicationConfig cloneRepConfig() {
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        return repConfigManager.makeReplicationConfig();
    }

    public ReplicationMutableConfig cloneRepMutableConfig() {
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        return repConfigManager.makeReplicationConfig();
    }

    public void setRepMutableConfig(ReplicationMutableConfig config)
        throws DatabaseException {

        /* Clone the current config. */
        RepConfigManager repConfigManager = (RepConfigManager) configManager;
        ReplicationConfig newConfig = repConfigManager.makeReplicationConfig();

        /* Copy in the mutable props. */
        config.copyMutablePropsTo(newConfig);
        repConfigManager = new RepConfigManager
            (configManager.getEnvironmentConfig(), newConfig);

        /*
         * Update the current config and notify observers.  The config manager
         * is replaced with a new instance that uses the new configuration.
         * This avoids synchronization issues: other threads that have a
         * reference to the old configuration object are not impacted.
         *
         * Notify listeners in reverse order of registration so that the
         * environment listener is notified last and can start daemon threads
         * after they are configured.
         */
        for (int i = repConfigObservers.size() - 1; i >= 0; i -= 1) {
            RepEnvConfigObserver o = repConfigObservers.get(i);
            o.repEnvConfigUpdate(repConfigManager, newConfig);
        }
    }

    @Override
    public void repEnvConfigUpdate(RepConfigManager configMgr,
                                   ReplicationMutableConfig newConfig)
        throws DatabaseException {

        allowArbiterAck =
            configMgr.getBoolean(RepParams.ALLOW_ARBITER_ACK);

        if (repNode == null) {
            return;
        }

        repNode.getArbiter().processConfigChange(newConfig);

        repNode.getElectionQuorum().setElectableGroupSizeOverride
            (newConfig.getElectableGroupSizeOverride());

        /* Account for mutation of deprecated HA LogFlusher params. */
        getLogFlusher().configFlushTask(configMgr);

        repNode.getReplica().getDbCache().setConfig(configMgr);
    }

    public synchronized void addRepConfigObserver(RepEnvConfigObserver o) {
        repConfigObservers.add(o);
    }

    /**
     * The VLSNIndex must be created, merged and flushed before the recovery
     * checkpoint. This method should be called even if there is no recovery
     * checkpoint, because it sets up needed data structures.
     *
     * On the face of it, it seems that one could flush the VLSNIndex cache
     * after the recovery checkpoint, before the Replicator constructor returns
     * and before any user level HA operations can start. That's not sufficient
     * because the recovery checkpoint is shortening the recovery interval for
     * future recoveries, and any information that has been garnered must be
     * persisted. Here's an example of what might happen after a series of
     * recoveries if we fail to flush VLSNIndex as part of the recovery
     * checkpoint:
     *
     * Environment recovers for first time, brand new environment
     *    recovery did not find any VLSNs in log, because log is brand new
     *    recovery logs ckpt 1start
     *    recovery logs ckpt 1 end
     *
     *    VLSN 1 logged
     *    VLSN 2 logged
     *    VLSN 3 logged
     *
     *  crash .... Environment recovers
     *  recovery crawls log from ckpt 1 start onward, finds VLSNs 1-3
     *  recovery logs ckpt 2 start
     *  recovery logs ckpt 2 end
     *  VLSN index instantiated, VLSNs 1-3 added in but not written too disk
     *
     *  crash ... Environment recovers
     *  recovery crawls log from ckpt start 2 start onward, finds no VLSNs.
     *
     * Instead, the flushed VLSN has to be logged before the checkpoint end
     * record that is used for the next recovery.
     */
    @Override
    public void preRecoveryCheckpointInit(RecoveryInfo recoveryInfo) {

        int stride = configManager.getInt(VLSN_STRIDE);
        int maxMappings = configManager.getInt(VLSN_MAX_MAP);
        int maxDist = configManager.getInt(VLSN_MAX_DIST);

        /*
         * Our local nameIdPair field isn't set yet because we haven't finished
         * our initialization, so get it from the config manager.
         */
        NameIdPair useNameIdPair =
            new NameIdPair(configManager.get(NODE_NAME));

        vlsnIndex = new VLSNIndex(this, DbType.VLSN_MAP.getInternalName(),
                                  useNameIdPair, stride, maxMappings, maxDist,
                                  recoveryInfo);
        replay.preRecoveryCheckpointInit(recoveryInfo);
    }

    /**
     * Returns the current state associated with this ReplicatedEnvironment
     *
     * @return the externally visible ReplicatedEnvironment state
     */
    public ReplicatedEnvironment.State getState() {
        return nodeState.getRepEnvState();
    }

    /**
     * Returns the blockLatchLock
     */
    public StampedLock getBlockLatchLock() {
        return blockLatchLock;
    }

    /**
     * Returns the state change event that transitioned the
     * ReplicatedEnviroment to its current state.
     */
    public StateChangeEvent getStateChangeEvent() {
        return nodeState.getStateChangeEvent();
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    /**
     * Wait for this node to join a replication group and return whether it is
     * a MASTER or REPLICA. Note that any method that creates or clears the
     * repNode field must be synchronized.
     */
    public synchronized ReplicatedEnvironment.State
        joinGroup(ReplicaConsistencyPolicy consistency,
                  QuorumPolicy initialElectionPolicy)
        throws ReplicaConsistencyException, DatabaseException {

        startupTracker.start(Phase.TOTAL_JOIN_GROUP);
        try {
            if (repNode == null) {
                repNode = new RepNode(this, replay, nodeState);
            }

            final ReplicatedEnvironment.State state =
                repNode.joinGroup(consistency, initialElectionPolicy);
            uuid = repNode.getUUID();
            return state;
        } catch (IOException ioe) {
            throw EnvironmentFailureException.unexpectedException
                (this, "Problem attempting to join on " + getSocket(), ioe);
        } finally {
            startupTracker.stop(Phase.TOTAL_JOIN_GROUP);
        }
    }

    /**
     * Initialize the DataChannelFactory in our configuration for use.
     * This is public to allow access by the ReplicatedEnvironment constructor.
     * @throws IllegalArgumentException if the ReplicationNetworkConfig
     * is invalid.
     */
    public void initializeChannelFactory() {
        if (channelFactory != null) {
            return;
        }

        synchronized (this) {
            if (channelFactory == null) {
                channelFactory =
                    DataChannelFactoryBuilder.construct(
                        repNetConfig,
                        DataChannelFactoryBuilder.makeLoggerFactory(this));
            }
        }
    }

    @Override
    protected Environment createInternalEnvironment() {
        return new InternalReplicatedEnvironment
            (getEnvironmentHome(), cloneRepConfig(), cloneConfig(), this);
    }

    /**
     * @see EnvironmentImpl#setupClose
     * Release all replication resources that can be released before the
     * checkpoint. Note that any method that creates or clears the repNode
     * field must be called from a synchronized caller.
     *
     * Note that the vlsnIndex is closed as a callback, from
     * postCheckpointPreEnvClose()
     * @throws DatabaseException
     *
     */
    @Override
    protected synchronized void setupClose(PrintWriter errors)
        throws DatabaseException {

        if (groupDbImpl != null) {
            getDbTree().releaseDb(groupDbImpl);
            groupDbImpl = null;
            LoggerUtils.fine
                (envLogger, this, "Group member database shutdown");
        }

        try {
            shutdownRepNode();
        } catch (InterruptedException e) {
            appendException(errors, e, "shutting down node " + nameIdPair);
        }
    }

    /**
     * Utility method to shutdown RepNode if it's not null.
     */
    private void shutdownRepNode() throws InterruptedException {
        if (repNode != null) {
            repNode.shutdown();
            feederTxns.shutdown();
            repNode = null;
        }
    }

    /**
     * Close any resources that need to be closed after the closing checkpoint.
     * Note that since Replay.close closes open transactions, it must be
     * invoked after the checkpoint has been completed, so that the checkpoint
     * operation can correctly account for the open transactions.
     */
    @Override
    protected synchronized void postCheckpointClose(boolean checkpointed)
        throws DatabaseException {

        if (replay != null) {
            replay.close();
            replay = null;
        }

        vlsnIndexAccess.closeVLSNIndex(checkpointed);
    }

    /**
     * @see EnvironmentImpl#setupClose
     *
     * Note: this conversion process will iterate over all user created
     * databases in the environment, which could be potentially be a costly
     * affair. However, let's opt for simplicity and defer any optimizations
     * until we see whether this is an important use case.
     */
    @Override
    protected void postRecoveryConversion() {

        super.postRecoveryConversion();

        if (needRepConvert) {
            /* Set NameDb to replicated. */
            DatabaseImpl nameDb = null;
            try {
                nameDb = dbMapTree.getDb(DbTree.NAME_DB_ID);
                if (!nameDb.isReplicated()) {
                    nameDb.setIsReplicatedBit();
                    nameDb.setDirty();
                }
            } finally {
                if (nameDb != null) {
                    dbMapTree.releaseDb(nameDb);
                }
            }

            /* Set user defined databases to replicated. */
            Map<DatabaseId, String> idNameMap = dbMapTree.getDbNamesAndIds();
            for (DatabaseId id : idNameMap.keySet()) {
                DatabaseImpl db = null;
                try {
                    db = dbMapTree.getDb(id);
                    if (db != null &&
                        !DbTree.isReservedDbName(idNameMap.get(id))) {

                        db.setIsReplicatedBit();
                        db.setDirty();
                    }
                } finally {
                    if (db != null) {
                        dbMapTree.releaseDb(db);
                    }
                }
            }

            /*
             * Do a checkpointer to flush dirty datbaseImpls that are converted
             * to replicated and write the current VLSNRange to the log.
             */
            CheckpointConfig ckptConfig = new CheckpointConfig();
            ckptConfig.setForce(true);
            ckptConfig.setMinimizeRecoveryTime(true);
            invokeCheckpoint(ckptConfig, "Environment conversion");
        }
    }

    /*
     * Close enough resources to support reopening the environment in the same
     * JVM.
     * @see EnvironmentImpl#doCloseAfterInvalid()
     */
    @Override
    public synchronized void doCloseAfterInvalid() {

        try {
            shutdownRepNode();
        } catch (Exception ignore) {
        }

        super.doCloseAfterInvalid();
    }

    /**
     * Used by error handling to forcibly close an environment, and by tests to
     * close an environment to simulate a crash.  Database handles do not have
     * to be closed before calling this method.  A checkpoint is not performed.
     * The various thread pools will be shutdown abruptly.
     *
     * @throws DatabaseException
     */
    @Override
    public void abnormalClose()
        throws DatabaseException {

        /*
         * Shutdown the daemons, and the checkpointer in particular, before
         * nulling out the vlsnIndex.
         */
        shutdownDaemons();

        try {
            shutdownRepNode();
        } catch (InterruptedException ignore) {
            /* ignore */
        }

        try {
            vlsnIndexAccess.abnormalCloseVLSNIndex();
        } catch (DatabaseException ignore) {
            /* ignore */
        }

        try {
            super.abnormalClose();
        } catch (DatabaseException ignore) {
            /* ignore */
        }
    }

    /**
     * A replicated log entry has been written on this node. Update the
     * {@literal VLSN->LSN} mapping. Called outside the log write latch.
     * @throws DatabaseException
     */
    @Override
    public void registerVLSN(LogItem logItem) {
        vlsnIndex.put(logItem);
    }

    /**
     * {@literal
     * Generate the next VLSN and update the DTVLSN value in the item. Note
     * that this method is only invoked when the node is in the Master state,
     * since the master assigns new VLSNs and DTVLSNs, and the replicas simply
     * preserve them.
     *
     * The DTVLSN value must be calculated under the same latch as the updating
     * of the VLSN to ensure that the following invariants are maintained:
     *
     * lsn1 > lsn2 ==> VLSN(lsn1) > VLSN(lsn2)
     * vlsn2 > vlsn1 ==> DTVLSN(vlsn2) >= DTVLSN(vlsn1)
     *
     * where vlsn2 and vlsn1 are transaction commit or abort records.
     *
     * Replicas, when replaying their stream, verify that this invariant is
     * maintained.
     *
     * Commit/Abort records for Replication groups that have a single electable
     * and durable node have their dtvlsn written as the associated VLSN, that
     * is, DTVLSN(vlsn) == vlsn. For all other RG configurations, DTVLSN(vlsn)
     * < vlsn.
     *
     * Commit/Abort Log records that are created by replaying an HA stream from
     * a pre DTVLSN feeder, will have their dtvlsns set to
     * VLSN.UNINITIALIZED_VLSN during replica replay. They do not
     * follow this code path.
     * }
     *
     * @param entry the log entry with which the VLSN will be associated. If
     * the log entry represents a commit or abort entry, its DTVLSN is modified
     * so that it's correct when it's serialized out.
     */
    @Override
    public long assignVLSNs(LogEntry entry) {
        final long vlsn = nextLogVLSN(entry);

        final byte itemType = entry.getLogType().getTypeNum();
        if (itemType != LogEntryType.LOG_TXN_COMMIT.getTypeNum() &&
            itemType !=  LogEntryType.LOG_TXN_ABORT.getTypeNum()) {
            return  vlsn;
        }

        /*
         * A commit or abort record. Compute the DTVLSN value to associate with
         * the commit record and store it into the buffer at the appropriate
         * position.
         */
        final long dtvlsn;

        if (simulatePreDTVLSNMaster) {
            dtvlsn = UNINITIALIZED_VLSN;
        } else if (repNode.isNeedsAcks()) {
            /*
             * Use the dtvlsn value being tracked via acknowledgments from
             * replicas when replication is being used for durability.
             */
            dtvlsn = getRepNode().getDTVLSN();
        } else {
            /*
             * Replicated environment, but replication is not being used for
             * durability. That is, the commit is self-acknowledged, set dtvlsn
             * == vlsn.
             */
            dtvlsn = getRepNode().updateDTVLSN(vlsn);
        }

        final VersionedWriteTxnEnd txnEnd =
            (VersionedWriteTxnEnd)entry.getMainItem();

        /*
         * As a cheap sanity check, commits/aborts on the master are created
         * with VLSN.NULL values, so they can be verified here.
         */
        final long checkDTVLSN = txnEnd.getDTVLSN();
        if (checkDTVLSN != NULL_VLSN) {
            throw new IllegalStateException("NULL DTVLSN expected at VLSN:" +
                                            vlsn + " not " + checkDTVLSN);
        }

        txnEnd.setDTVLSN(dtvlsn);

        /* Verify sequence invariants */
        checkLogSequencing(vlsn, txnEnd);

        return vlsn;
    }

    /**
     * Returns the next validated VLSN value suitable for use in the log
     * entry
     */
    private long nextLogVLSN(LogEntry entry) {
        final long vlsn = vlsnIndex.bump(this);

        if (!VLSN.isNext(prevLoggedVLSN, vlsn)) {
            String msg = "VLSNs must be a contiguous sequence." +
                " Log entry:" + entry.dumpRep() +
                " prevVLSN:" + prevCommitVLSN +
                " next VLSN:" + vlsn;
            throw EnvironmentFailureException.unexpectedState(this, msg);
        }
        prevLoggedVLSN = vlsn;
        return vlsn;
    }

    /**
     * Checks the correct sequencing of vlsns and DTVLSNs and master terms
     * in commit entries
     *
     * @param commitVLSN the vlsn associated with the txnEnd
     * @param txnEnd the txn commit/abort
     */
    private void checkLogSequencing(final long commitVLSN,
                                    final VersionedWriteTxnEnd txnEnd) {
        if (prevCommitVLSN >= commitVLSN) {
            String msg = "VLSNs must be in ascending order in the log." +
                " Txn:" + txnEnd.dumpLog() +
                " prevVLSN:" + prevCommitVLSN +
                " next VLSN:" + commitVLSN;
            throw EnvironmentFailureException.unexpectedState(this, msg);
        }

        final long dtvlsn = txnEnd.getDTVLSN();
        if (dtvlsn < prevLoggedDTVLSN) {
            String msg = "DTVLSNs must be in ascending order in the log." +
                " Txn:" + txnEnd.dumpLog() +
                " prev DTVLSN:" + prevLoggedDTVLSN +
                " at VLSN: " + commitVLSN;
            throw EnvironmentFailureException.unexpectedState(this, msg);
        }

        final long txnTerm = txnEnd.getMasterTerm();
        if (!MasterTerm.follows(prevLoggedTerm, txnTerm)) {
            String msg = "Terms must be in ascending order in the log. " +
                " Txn:" + txnEnd.dumpLog() +
                " prev master term:" + MasterTerm.logString(prevLoggedTerm) +
                " at VLSN:" + commitVLSN ;
            throw EnvironmentFailureException.unexpectedState(this, msg);
        }

        prevCommitVLSN = commitVLSN;
        prevLoggedDTVLSN = dtvlsn;
        prevLoggedTerm = txnTerm;
    }

    /**
     * sets Replay VLSN Counter of the vlsn index with this vlsn.
     * returns prev vlsn in the counter.
     */
    @Override
    public long setReplayVLSNCounter(long vlsn) {
        return vlsnIndex.setReplayVLSNCounter(vlsn);
    }

    /**
     * Flush any information that needs to go out at checkpoint.  Specifically,
     * write any in-memory {@literal VLSN->LSN} mappings to the VLSNIndex
     * database so we are guaranteed that the VLSNIndex database will recover
     * properly.  This must be committed with noSync because
     *  - the ensuing checkpoint end record will be logged with an fsync and
     *    will effectively force this out
     *  - it's important to minmize lock contention on the vlsn index and
     *    any fsync done during a checkpoint will be expensive, as there may
     *    be quite a lot to push to disk. We don't want to incur that cost
     *    while holding locks on the vlsn index. [#20702]
     */
    @Override
    public void preCheckpointEndFlush()
        throws DatabaseException {

        if (vlsnIndex != null) {
            vlsnIndex.flushToDatabase(Durability.COMMIT_NO_SYNC);
        }
    }

    @Override
    public boolean isMaster() {

        /*
         * The volatile repNode field might be modified by joinGroup(),
         * leaveGroup, or close(), which are synchronized. Keep this method
         * unsynchronized, assign to a temporary field to guard against a
         * change.
         */
        RepNode useNode = repNode;
        if (useNode == null) {
            return false;
        }
        return useNode.isMaster();
    }

    public void setChangeListener(StateChangeListener listener) {
        StateChangeListener prevListener = nodeState.getChangeListener();
        nodeState.setChangeListener(listener);

        /*
         * Call back so that it's aware of the last state change event and
         * the application can initialize itself correctly as a master or
         * replica.
         */
        final StateChangeEvent stateChangeEvent =
            nodeState.getStateChangeEvent();
        try {
            /* Invoke application code and handle any app exceptions. */
            listener.stateChange(stateChangeEvent);
        } catch (Exception e) {
            /* Revert the change. */
            nodeState.setChangeListener(prevListener);
            LoggerUtils.severe
                (envLogger, this,
                 "State Change listener exception: " + e.getMessage() + "\n" +
                 LoggerUtils.getStackTraceForSevereLog(e));
            /* An application error. */
            throw new EnvironmentFailureException
                (this, EnvironmentFailureReason.LISTENER_EXCEPTION, e);
        }
    }

    public StateChangeListener getChangeListener() {
        return nodeState.getChangeListener();
    }

    public VLSNIndex getVLSNIndex() {
        return vlsnIndex;
    }

    public FeederTxns getFeederTxns() {
        return feederTxns;
    }

    public ReplicatedEnvironmentStats getStats(StatsConfig config) {
        return getStats(config, statKey);
    }

    @Override
    public Collection<StatGroup> getRepStatGroups(StatsConfig config,
                                                  Integer statKey1) {
        ReplicatedEnvironmentStats res = getStats(config, statKey1);
        return (res == null) ? null : res.getStatGroups();
    }

    @Override
    public SortedSet<String> getStatCaptureProjections() {
        return new StatCaptureRepDefinitions().getStatisticProjections();
    }

    @Override
    public StatManager createStatManager() {
        return new RepStatManager(this);
    }

    public FeederFilter getFeederFilter() {
        return feederFilter;
    }

    public ReplicatedEnvironmentStats getStatsInternal(StatsConfig config) {
        if (repNode == null) {
        	if (isSubscriber) {
        		return RepInternal.makeReplicatedEnvironmentStats(this, config);
        	}
            return null;
        }
        return repNode.getStats(config);
    }

    public ReplicatedEnvironmentStats getStats(
            StatsConfig config,
            Integer contextKey) {
        return ((RepStatManager)statManager).getRepStats(config, contextKey);
    }

    public Replay getReplay() {
        return replay;
    }

    /**
     * Ensures that the environment is currently a Master before proceeding
     * with an operation that requires it to be the master.
     *
     * @throws UnknownMasterException if the node is disconnected
     * @throws ReplicaWriteException if the node is currently a replica
     */
    public NodeState checkIfMaster(Locker locker)
        throws UnknownMasterException, ReplicaWriteException {

        switch (nodeState.getRepEnvState()) {
            case MASTER:
                return nodeState;

            case REPLICA:
                throw new ReplicaWriteException(locker, nodeState.
                                                getStateChangeEvent());

            case UNKNOWN:
            case DETACHED:
                throw new UnknownMasterException(locker, nodeState.
                                                 getStateChangeEvent());

            default:
                throw EnvironmentFailureException.unexpectedState
                    ("Unexpected state: " + nodeState.getRepEnvState());
        }
    }

    /**
     * @return the repNode. May return null.
     */
    public RepNode getRepNode() {
        return repNode;
    }

    /**
     * Create an appropriate type of ThreadLocker. Specifically, it creates an
     * MasterThreadLocker if the node is currently a Master, and a
     * ReplicaThreadLocker otherwise, that is, if the node is a Replica, or
     * it's currently in a DETACHED state.
     *
     * @return an instance of MasterThreadLocker or ReplicaThreadLocker
     */
    @Override
    public ThreadLocker createRepThreadLocker() {
        return (getState().isMaster() ?
                new MasterThreadLocker(this) :
                new ReplicaThreadLocker(this));
    }

    /**
     * Create an appropriate type of Replicated transaction. Specifically,
     * it creates a MasterTxn, if the node is currently a Master, a ReadonlyTxn
     * otherwise, that is, if the node is a Replica, or it's currently in a
     * DETACHED state.
     *
     * Note that a ReplicaTxn, used for transaction replay on a Replica is not
     * created on this path. It's created explicitly in the Replay loop by a
     * Replica.
     *
     * @param config  the transaction configuration
     *
     * @return an instance of MasterTxn or ReadonlyTxn
     * @throws DatabaseException
     */
    @Override
    public Txn createRepUserTxn(TransactionConfig config)
        throws DatabaseException {

        return (getState().isMaster() &&
                !config.getReadOnly() &&
                !config.getLocalWrite()) ?
            MasterTxn.create(this, config) :
            new ReadonlyTxn(this, config);
    }

    /**
     * Installs the commit-blocking latch that is used to halt the commit/abort
     * of transactions, in the final phase of a master transfer.
     */
    public void blockTxnCompletion(CountDownLatch blocker)
        throws InterruptedException {

        final long timeoutMs = 2 * MasterTransfer.getPhase2TimeoutMs();
        final long startTxnId = getTxnManager().getLastReplicatedTxnId();
        long wstamp = blockLatchLock.tryWriteLock(timeoutMs, TimeUnit.MILLISECONDS);
        if (wstamp == 0) {
            final long currentTxnId = getTxnManager().getLastReplicatedTxnId();
            final String msg =
                " Failed to aquire write lock on block latch within" +
                " timeout: " + timeoutMs  + " ms" +
                " txnId at start:" + startTxnId +
                " current: " + currentTxnId +
                " txn id delta:" + (startTxnId - currentTxnId) +
                " write locked:" +  blockLatchLock.isWriteLocked() +
                " read lock count:" + blockLatchLock.getReadLockCount();
            LoggerUtils.fullThreadDump(envLogger, this, Level.SEVERE);
            throw new MasterTransferFailureException(msg);
        }
        try {
            blockTxnLatch = blocker;
        } finally {
            blockLatchLock.unlockWrite(wstamp);
        }
    }

    /**
     * Releases the transaction block latch.
     */
    public void unblockTxnCompletion() {
        LoggerUtils.info(envLogger, this, "Releasing commit block latch");
        blockTxnLatch.countDown();
    }

    /**
     * This hook is used primarily to perform the final checks before allowing
     * the commit operation to proceed. The following checks are performed
     * here:
     *
     * 1) Check for master
     * 2) Check for sufficient Feeder connections to ensure that the commit
     *    policy could be implemented. There is no guarantee that they will all
     *    ack the commit request.
     *
     * The method also associates a latch with the transaction. The latch is
     * used to delay the commit operation until a sufficient number of commits
     * have been received.
     *
     * In addition, when mastership transfers are done, and this node is the
     * original master, commits and aborts are blocked so as to avoid hard
     * recovery after electing a new master, see [#18081].
     *
     * @param txn the master transaction being committed
     *
     * @throws InsufficientReplicasException if the feeder is not in contact
     * with enough replicas.
     * @throws RestartRequiredException if the environment is invalid.
     * @throws UnknownMasterException if the current master is unknown.
     * @throws ReplicaWriteException if the node transitioned to a Replica
     * after the transaction was initiated.
     */
    public void preLogCommitHook(MasterTxn txn)
        throws InsufficientReplicasException,
               RestartRequiredException,
               UnknownMasterException,
               ReplicaWriteException,
               EnvironmentFailureException {

        checkIfInvalid();
        /*
         * For master transfer, checkBlock before checkIfMaster so that we
         * block in the UNKNOWN state prior to conversion to a replay txn.
         */
        checkBlock(txn);
        checkIfMaster(txn);

        /* Still a master, check for a sufficient number of connections */
        int activeReplicaCount =
            repNode.feederManager().activeAckReplicaCount();
        ReplicaAckPolicy ackPolicy =
            txn.getCommitDurability().getReplicaAck();
        int requiredAckCountFromReplica = txn.getRequiredAckCount();
        /*
         * If groupCommit is used for this txn, requiredAckCount decrements by one
         * since below here what we need is the number of acks from replicas,
         * don't count in the ack from the master itself here.
         */
        if (txn.isGroupCommitted()) {
            requiredAckCountFromReplica --;
        }
        if (envLogger.isLoggable(Level.FINE)) {
            LoggerUtils.fine(envLogger, this,
                             "Txn " + txn.getId() + " requires: " +
                             requiredAckCountFromReplica + " active: " +
                             activeReplicaCount +
                             " replica acks. Commit Policy: " + ackPolicy);
        }

        if (!txn.isNullTxn() && /* Ignore replicas for null txn initiation */
            requiredAckCountFromReplica > activeReplicaCount) {
            /* Check for possible activation of Primary */
            if (ackPolicy.equals(ReplicaAckPolicy.SIMPLE_MAJORITY) &&
                repNode.getArbiter().activateArbitration()) {
                txn.setRequiredAckCount(0);
            } else if (useArbiter(txn)) {
                /*
                 * Note we could change the check to allow a degraded
                 * write from any group size. Limit is place at rep group
                 * size of two due to the priority of requirements
                 * and lower cost of testing.
                 */
                txn.setArbiterAck(true);
            } else {
                /*
                 * Capture the set to ensure it's consistent with the exception
                 * message.
                 */
                final boolean includeArbiters =
                    !ackPolicy.equals(ReplicaAckPolicy.ALL);
                final Set<String> activeAckRepSet =
                    repNode.feederManager().activeAckReplicas(includeArbiters);

                if (!txn.isNullTxn() /* Ignore replicas for null txns */ &&
                    (requiredAckCountFromReplica > activeAckRepSet.size())) {
                    /* No change in window, throw exception */
                    InsufficientReplicasException ire =
                    new InsufficientReplicasException
                        (txn, ackPolicy, requiredAckCountFromReplica, activeAckRepSet);
                    LoggerUtils.info(envLogger, this, ire.getMessage());
                    throw ire;
                }

                /**
                 * A new replica became active in the window between the
                 * first fast check and the second slower check, just continue
                 */
            }
        }
        feederTxns.setupForAcks(txn);
    }

    /*
     * Block transaction commits/aborts if this node is the original master
     * and we're doing Master Transfer.
     *
     * This method should be called exactly once per txn commit/abort from the
     * preLog hooks
     */
    private void checkBlock(MasterTxn txn) {
        try {
            txn.lockReadBlockLatch();

            if (blockTxnLatch.getCount() > 0) {
                LoggerUtils.info(envLogger, this,
                                 "Block transaction: " + txn.getId() +
                                 " pending master transfer. Write locks = " +
                                 txn.getWriteLockIds());
            }

            /*
             * Wait for phase 2 to complete. Use a defensive 2X timeout to
             * ensure app threads are not on indefinite hold in case of a bug.
             * A timeout must invalidate the env to avoid an invalid txn state.
             */
            final long timeoutMs = 2 * MasterTransfer.getPhase2TimeoutMs();
            if (!blockTxnLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw EnvironmentFailureException.unexpectedState(this,
                    "Txn:" + txn.toString() + " blocked for a period" +
                        " exceeding  expected phase 2 master transfer" +
                        " timeout limit: " + timeoutMs  + " ms");
            }

            checkIfInvalid();

            /*
             * Check again, after the block! The block may be a result of a
             * master->replica transfer, and if this node transitions from
             * master to replica, this node will be disqualified from being
             * able to commit transactions.
             */
            checkIfMaster(txn);

        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(this, e);
        }
    }

    /**
     * It ensures that the feeder obtains the requisite number of
     * acknowledgments required for a successful commit.
     *
     * @param txn The MasterTxn that was committed locally.
     *
     * @return whether the commit LogItem was queued by the arbiter.
     * See ArbiterFeederSource.addCommit.
     *
     * @throws InterruptedException if the thread was interrupted while
     * waiting for acknowledgments.
     * @throws InsufficientAcksException if the master received an insufficient
     * number of commit acknowledgments within the replica commit timeout
     * period.
     * @throws EnvironmentFailureException
     */
    public boolean postLogCommitHook(MasterTxn txn, LogItem commitItem)
       throws InsufficientAcksException,
              InterruptedException,
              EnvironmentFailureException {
        final long txnTimeout = txn.getTxnTimeout();
        int timeout = replicaAckTimeout;

        if ((txnTimeout != 0) && (txnTimeout < replicaAckTimeout)) {
            timeout = (int) txnTimeout;
        }

        txn.unlockReadBlockLatch();
        localTxnAvgNs.add(System.nanoTime() - txn.getStartNs());
        return postLogCommitHookInternal(txn, commitItem, timeout);
    }

    private boolean postLogCommitHookInternal(MasterTxn txn,
                                              LogItem commitItem,
                                              int ackTimeout)
        throws InsufficientAcksException,
               InterruptedException,
               EnvironmentFailureException {

        if (!isValid()) {
            final int currentRequiredAckCount = repNode.getDurabilityQuorum().
            getCurrentRequiredAckCount(txn.getCommitDurability().
                                       getReplicaAck());
            if (currentRequiredAckCount > 0) {
                /* Throw a more actionable and accurate exception than EFE */
                final String msg =
                    "Acks could not be obtained because the environment" +
                    "was invalidated";
                LoggerUtils.info(envLogger, this, msg);
                throw new InsufficientAcksException(msg);
            }
            /* No acks are required, transaction is complete. */
            return false;
        }

        /* Check if using Arbiter for transaction. */
        boolean queuedCommit = false;
        if (txn.getArbiterAck()) {
            // get the arbiter acker source and add txn id to its queue.
            Feeder arbFeeder = repNode.feederManager().getArbiterFeeder();
            if (arbFeeder != null) {
                ArbiterFeederSource as = arbFeeder.getArbiterFeederSource();
                queuedCommit = as.addCommit(commitItem);
            }
        }

        /* Don't do master check, the transaction has already been committed */
        try {
            feederTxns.awaitReplicaAcks(txn, ackTimeout);
        } catch (InsufficientAcksException e) {
            if (txn.getArbiterAck() == false && useArbiter(txn)) {
                txn.setArbiterAck(true);
                assert !queuedCommit;
                return postLogCommitHookInternal(
                    txn, commitItem, arbiterAckTimeout);
            }
            LoggerUtils.info(envLogger, this, e.getMessage());
            throw e;
        }

        return queuedCommit;
    }

    /**
     * Invoked before aborting a MasterTxn, this happens when the master is
     * going to be a replica because of mastership transfer. We do this to make
     * sure that the replica going to be the master has the most recent log and
     * no hard recovery would happen after its election, see SR [#18081].
     *
     * @param txn The MasterTxn that was aborted locally.
     *
     * @throws ReplicaWriteException if the node transitioned to a Replica
     * after the transaction was initiated.
     * @throws UnknownMasterException if the current master is unknown.
     * @throws EnvironmentFailureException
     */
    public void preLogAbortHook(MasterTxn txn)
        throws EnvironmentFailureException,
               ReplicaWriteException,
               UnknownMasterException {

        checkIfInvalid();
        /*
         * For master transfer, checkBlock before checkIfMaster so that we
         * block in the UNKNOWN state prior to conversion to a replay txn.
         */
        checkBlock(txn);
        checkIfMaster(txn);
    }

    /**
     * Releases the block latch lock, if held.  This hook is called in the
     * normal course of Txn.abort(), once the abort log record has been written
     * and the associated VLSN stored.
     */
    public void postLogAbortHook(MasterTxn txn) {
        txn.unlockReadBlockLatch();
        localTxnAvgNs.add(System.nanoTime() - txn.getStartNs());
    }

    /**
     * Removes any pending acknowledgments that were registered by the
     * preLogCommitHook. This hook is called only rarely when a
     * {@code commit()} fails and therefore must be aborted, e.g. during a
     * MasterTransfer, when the blocked txn on being unblocked realizes its now
     * a replica and must not write a commit log record.
     */
    public void postLogCommitAbortHook(MasterTxn txn) {
        LoggerUtils.info(envLogger, this,
                         "post log commit abort hook for txn: " + txn.getId());

        txn.unlockReadBlockLatch();
        feederTxns.clearTransactionAcks(txn);
    }

    /* Asserts that no txns are logging commit/abort records and
     * that no MTs are in progress during master/replica state transsitions.
     */
    public void checkLockStateAtTransition() throws InterruptedException {
        /*
         * Be generous, we don't expect to wait this long, except during a
         * leaked lock problem
         */
        final long timeoutMs = 10 * MasterTransfer.getPhase2TimeoutMs();

        /* Acquire a writelock just to check that there are no outstanding
         * read or write locks.
         */
        long wstamp =
            blockLatchLock.tryWriteLock(timeoutMs, TimeUnit.MILLISECONDS);
        if (wstamp == 0) {
            /* Dump the threads to help debug the reason for the stall. */
            LoggerUtils.fullThreadDump(envLogger, this, Level.SEVERE);
            String msg = "Write locked: " + blockLatchLock.isWriteLocked() +
                " Read lock count:"  + blockLatchLock.getReadLockCount();
            LoggerUtils.warning(envLogger, this, msg);
            throw EnvironmentFailureException.unexpectedState(this, msg);
        }
        blockLatchLock.unlockWrite(wstamp);
    }

    /**
     * Create a ReplayTxn for recovery processing.
     */
    @Override
    public Txn createReplayTxn(long txnId)
        throws DatabaseException {

        return
            new ReplayTxn(this, TransactionConfig.DEFAULT, txnId, envLogger);
    }

    /**
     * Used by environment recovery to get a tracker to collect VLSN-LSN
     * mappings that are within the recovery part of the log. These might
     * not be reflected in the persistent mapping db.
     */
    @Override
    public VLSNRecoveryProxy getVLSNProxy() {
        int stride = configManager.getInt(RepParams.VLSN_STRIDE);
        int maxMappings = configManager.getInt(RepParams.VLSN_MAX_MAP);
        int maxDist = configManager.getInt(RepParams.VLSN_MAX_DIST);

        return new VLSNRecoveryTracker(this, stride, maxMappings, maxDist);
    }

    public UUID getUUID() {
        return uuid;
    }

    /**
     * Used during testing to introduce artificial clock skews.
     */
    public static void setSkewMs(int skewMs) {
        clockSkewMs = skewMs;
    }

    public static int getClockSkewMs() {
        return clockSkewMs;
    }

    /**
     * Truncate the head of the VLSNIndex to allow file deletion, if possible.
     */
    @Override
    public boolean tryVlsnHeadTruncate(long bytesNeeded) {
        return vlsnIndex.tryTruncateFromHead(bytesNeeded);
    }

    /**
     * Truncate the head of the VLSNIndex to unprotect the given file, if
     * possible.
     */
    @Override
    public boolean tryTruncateVlsnHead(long deleteEnd, long deleteFileNum) {
        return vlsnIndex.tryTruncateFromHead(deleteEnd, deleteFileNum);
    }

    /**
     * Clears the cached checksum for a file when it may be overwritten
     * (e.g., entries may be erased).
     */
    @Override
    public void clearedCachedFileChecksum(String fileName) {
        final RepNode rn = getRepNode();
        if (rn != null) {
            rn.clearedCachedFileChecksum(fileName);
        }
    }

    public int getNodeId() {
        return nameIdPair.getId();
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    @Override
    public long getReplayTxnTimeout() {
        return replayTxnTimeout;
    }

    /* Return the default consistency policy. */
    @Override
    public ReplicaConsistencyPolicy getDefaultConsistencyPolicy() {
        return defaultConsistencyPolicy;
    }

    @Override
    public ReplicaConsistencyPolicy getConsistencyPolicy(String propValue) {
        return RepUtils.getReplicaConsistencyPolicy(propValue);
    }

    /**
     * The default consistency is not currently mutable in the API, but can be
     * set for testing purposes.
     *
     * TODO: Make it mutable in the API, since Durability is mutable.
     */
    public void setDefaultConsistencyPolicy(ReplicaConsistencyPolicy policy) {
        defaultConsistencyPolicy = policy;
    }

    /* Returns the on disk LSN for VLSN. */
    private long getLsnForVLSN(long vlsn, int readBufferSize) {
        /* Returns the file number which is nearest to the vlsn. */
        long fileNumber = vlsnIndex.getLTEFileNumber(vlsn);

        /* Start reading from the nearest file. */
        FeederReader feederReader =
            new FeederReader(this,
                             vlsnIndex,
                             DbLsn.makeLsn(fileNumber, 0),
                             readBufferSize);

        try {
            feederReader.initScan(vlsn);

            /*
             * Go on scan the log until FeederReader find the target VLSN,
             * thrown out an EnvironmentFailureException if it can't be found.
             */
            if (!feederReader.readNextEntry()) {
                throw EnvironmentFailureException.unexpectedState
                    ("VLSN not found: " + vlsn);
            }
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        return feederReader.getLastLsn();
    }

    /**
     * Returns the logged durable txn VLSN. The logged DTVLSN is part of the
     * last txn commit or abort entry.
     *
     * @return the persistent DTVLSN. The value may be
     * VLSN.UNINITIALIZED_VLSN if the environment was newly created,
     * that is, it has no transactions in it as yet, or if the last entry was
     * created by a pre-DTVLSN master.
     *
     * @throws FileNotFoundException if the file containing the last txn commit
     * or abort entry does not exist
     */
    public long getLoggedDTVLSN()
        throws FileNotFoundException {

        final long lastTxnEnd = getLastTxnEnd();

        if (VLSN.isNull(lastTxnEnd)) {
            /* A brand new environment with no transactions in it. */
            return UNINITIALIZED_VLSN;
        }

        final long lsn = getLsnForVLSN(lastTxnEnd,
                                       1024 /* buffer size for txn end */);
        final TxnEnd txnEnd = (TxnEnd) getLogManager().
            getLogEntryHandleNotFound(lsn, -1).getMainItem();

        long dtvlsn = txnEnd.getDTVLSN();
        if (dtvlsn != UNINITIALIZED_VLSN) {
            return dtvlsn;
        }

        /* A JE version <= 7.1 log entry. */
        LoggerUtils.logMsg(envLogger, this, Level.INFO,
                           "Pre DTVLSN log, starting with zero dtvlsn");

        return dtvlsn;
    }

    /**
     * Returns the last TxnEnd entry in the logs, or null if the env does not
     * as yet have any replicated committed txns
     */
    public TxnEnd getLastTxnEndEntry() {
        final long lastTxnEnd = vlsnIndexAccess.getLastTxnEnd();

        if (VLSN.isNull(lastTxnEnd)) {
            /*
             * A brand new environment with no transactions in it, e.g. a
             * replica that has yet to receive txns from a master
             */
            return null;
        }

        final long lsn = getLsnForVLSN(lastTxnEnd,
                                       1024 /* buffer size for txn end */);
        return (TxnEnd) getLogManager().
            getLogEntryHandleNotFound(lsn, -1).getMainItem();
    }

    /* Returns the end of the log. */
    @Override
    public long getEndOfLog() {
        return vlsnIndex.getRange().getLast();
    }

    /**
     * Returns true if the VLSN should be cached in the BIN when the LN falls
     * out of cache. The {@link RepParams#CACHE_RECORD_VERSION} param.
     */
    @SuppressWarnings("javadoc")
    @Override
    public boolean getCacheVLSN() {
        return cacheVLSN;
    }

    @Override
    public boolean optimizeRepStreamForSize() {
        return optimizeStreamForSize;
    }

    @Override
    public void incReplayPreprocessMiss() {
        if (replay != null) {
            replay.incPreprocessMiss();
        }
    }

    /**
     * @see EnvironmentImpl#getName
     */
    @SuppressWarnings("javadoc")
    @Override
    public String getName() {
        return nameIdPair + ":" + super.getName();
    }

    @Override
    public String getNodeName() {
        return nameIdPair.getName();
    }

    @Override
    public boolean getIsMaster() {
        return getState().isMaster();
    }

    /**
     * Return true if this environment is part of a replication group.
     */
    @Override
    public boolean isReplicated() {
        return true;
    }

    /**
     * Return true if this environment is used as an Arbiter.
     */
    @Override
    public boolean isArbiter() {
        return isArbiter;
    }

    @Override
    public boolean isSubscriber() {
        return isSubscriber;
    }

    /**
     * Check whether this environment can be opened on an existing environment
     * directory.
     */
    @Override
    public void checkRulesForExistingEnv(boolean dbTreeReplicatedBit)
        throws UnsupportedOperationException {

        if (!dbTreeReplicatedBit) {

            /*
             * We are attempting to open an existing, non-replicated
             * environment.
             */
            throw new UnsupportedOperationException
                ("This environment must be converted for replication." +
                 " using com.sleepycat.je.rep.util.DbEnableReplication.");
        }
    }

    /**
     * Returns the hostname associated with this node.
     *
     * @return the hostname
     */
    public String getHostName() {
        String hostAndPort = configManager.get(RepParams.NODE_HOST_PORT);
        int colonToken = hostAndPort.indexOf(":");
        return (colonToken >= 0) ?
               hostAndPort.substring(0, colonToken) :
               hostAndPort;
    }

    /**
     * Returns the socket address associated with this node, with a port of zero
     * suitable for binding locally to any free port associated with the
     * address.
     */
    public InetSocketAddress getHostAddress() {
        return new InetSocketAddress(getHostName(), 0);
    }

    /**
     * Returns the options used to configure the feeder channel connection.
     *
     * Note that soTimeout is not set since it's a blocking channel and
     * setSoTimeout has no effect on a blocking nio channel.
     *
     * Push responses out rapidly, they are small (heart beat or commit
     * response) and need timely delivery to the master. (tcpNoDelay = true)
     *
     */
    public ConnectOptions getFeederConnectOptions() {
        return new ConnectOptions().
            setTcpNoDelay(configManager.getBoolean(FEEDER_TCP_NO_DELAY)).
            setReceiveBufferSize(configManager.
                                 getInt(REPLICA_RECEIVE_BUFFER_SIZE)).
            setOpenTimeout(configManager.getDuration(REPSTREAM_OPEN_TIMEOUT)).
            setBlocking(true);
    }

    /**
     * Returns the  port used by the replication node.
     *
     * @return the port number
     */
    public int getPort() {
        String hostAndPort = configManager.get(RepParams.NODE_HOST_PORT);
        int colonToken = hostAndPort.indexOf(":");
        return (colonToken >= 0) ?
                Integer.parseInt(hostAndPort.substring(colonToken + 1)) :
                configManager.getInt(RepParams.DEFAULT_PORT);
    }

    /* Convenience method for returning replication sockets. */
    public InetSocketAddress getSocket() {
        return new InetSocketAddress(getHostName(), getPort());
    }

    /**
     * Returns the JE version that is currently running on this node,
     * consulting the TEST_JE_VERSION configuration parameter for a test
     * override.
     */
    public JEVersion getCurrentJEVersion() {
        final String testJEVersion = configManager.get(TEST_JE_VERSION);
        return testJEVersion.isEmpty() ?
            JEVersion.CURRENT_VERSION :
            new JEVersion(testJEVersion);
    }

    /**
     * Returns the set of sockets associated with helper nodes.
     *
     * @return the set of helper sockets, returns an empty set if there
     * are no helpers.
     */
    public Set<InetSocketAddress> getHelperSockets() {
        String helperHosts = configManager.get(RepParams.HELPER_HOSTS);
        return HostPortPair.getSockets(helperHosts);
    }

    /**
     * Called when a node has identified itself as the master, which is when the
     * RepNode.selfElect is called. The database should not exist at this point.
     *
     * Lock hierarchy: {@literal GroupDbLock -> sync on EnvironmentImpl}
     *
     * @throws DatabaseException
     */
    public DatabaseImpl createGroupDb() {
        assert isMaster();
        return createGroupDbInternal();
    }

    /*
     * This is currently only used by DbResetRepGroup when the RepGroupDB is
     * missing.  It bypasses the isMaster check in createGroupDb because this
     * is called during the bootstrapping of the current node into the master,
     * so there is no master present and cannot be.  Since this is called only
     * by DbResetRepGroup to replace the missing RepGroupDB, any other nodes in
     * the group have crashed due to the missing database and will have to be
     * brought back up through NetworkRestore with this node once it is fixed
     * and running as Master.
     */
    public DatabaseImpl createGroupDbInternal() throws DatabaseException {
        try {
            groupDbLock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        try {
            if (groupDbImpl != null) {
                throw EnvironmentFailureException.unexpectedState
                    ("GroupDb should not exist.");
            }

            DatabaseImpl newDbImpl = null;
            Txn txn = null;
            try {
                TransactionConfig txnConfig = new TransactionConfig();
                txnConfig.setDurability(new Durability(SyncPolicy.SYNC,
                                                       SyncPolicy.SYNC,
                                                       ReplicaAckPolicy.NONE));
                txnConfig.setConsistencyPolicy(NO_CONSISTENCY);
                txn = new MasterTxn(this, txnConfig);

                /* Database should not exist yet, create it now */
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(true);
                dbConfig.setTransactional(true);
                dbConfig.setExclusiveCreate(true);
                dbConfig.setReplicated(true);

                newDbImpl = getDbTree().createInternalDb
                    (txn, DbType.REP_GROUP.getInternalName(), dbConfig);
                txn.commit();
                txn = null;
            } finally {
                if (txn != null) {
                    txn.abort();
                }
            }

            groupDbImpl = newDbImpl;
        } finally {
            groupDbLock.unlock();
        }
        return groupDbImpl;
    }

    /**
     * Open the group db, which should exist already, using NO_CONSISTENCY.
     */
    public DatabaseImpl getGroupDb()
        throws DatabaseNotFoundException,
               DatabaseException {

        /* Acquire the lock. */
        try {
            groupDbLock.lockInterruptibly();
        } catch(InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }

        Txn txn = null;
        try {
            if (groupDbImpl != null) {
                return groupDbImpl;
            }

            DatabaseImpl newDbImpl = null;
            TransactionConfig txnConfig = new TransactionConfig();
            txnConfig.setConsistencyPolicy(NO_CONSISTENCY);
            txn = new ReadonlyTxn(this, txnConfig);

            newDbImpl = getDbTree().getDb(txn,
                                          DbType.REP_GROUP.getInternalName(),
                                          null /* databaseHandle */,
                                          false);
            if (newDbImpl == null) {
                throw new DatabaseNotFoundException
                    (DbType.REP_GROUP.getInternalName());
            }
            txn.commit();
            txn = null;

            groupDbImpl = newDbImpl;
            return groupDbImpl;
        } finally {
            if (txn != null) {
                txn.abort();
            }
            groupDbLock.unlock();
        }
    }

    /**
     * Return true if the node has been configured as a Designated Primary.
     * This does not necessarily mean that the node is actively operating in
     * designated primary mode. See
     * {@link com.sleepycat.je.rep.arbitration.Arbiter#isActive}
     */
    public boolean isDesignatedPrimary() {
        return getConfigManager().getBoolean(RepParams.DESIGNATED_PRIMARY);
    }

    @Override
    public boolean addDbBackup(DbBackup backup) {
        synchronized (backups) {
            if (backupProhibited) {
                return false;
            }
            boolean added = backups.add(backup);
            assert added;
        }

        super.addDbBackup(backup);
        return true;
    }

    @Override
    public void removeDbBackup(DbBackup backup) {
        synchronized (backups) {
            boolean removed = backups.remove(backup);
            assert removed;
        }
        super.removeDbBackup(backup);
    }

    /* Invalidate all the on going DbBackups, used in Replay.rollback(). */
    public void invalidateBackups(long fileNumber) {
        synchronized (backups) {
            for (DbBackup backup : backups) {
                backup.invalidate(fileNumber);
            }
        }
    }

    /* Set the backupProhibited status, used in Replay.rollback(). */
    public void setBackupProhibited(boolean backupProhibited) {
        synchronized (backups) {
            this.backupProhibited = backupProhibited;
        }
    }

    @Override
    public int getReplayFreeDiskPercent() {
        return getConfigManager().getInt(REPLAY_FREE_DISK_PERCENT);
    }

    /**
     * Sets up the environment for group shutdown when the environment is
     * closed.
     *
     * @see ReplicatedEnvironment#shutdownGroup(long, TimeUnit)
     */
    public void shutdownGroupSetup(long timeoutMs) {
        final int backupCount = getBackupCount();
        if (backupCount > 0) {
            throw new IllegalStateException
                ("Environment has " + backupCount +
                 " DbBackups in progress.");
        }

        repNode.shutdownGroupOnClose(timeoutMs);
    }

    public String transferMaster(Set<String> replicas,
                               long timeout,
                               boolean force,
                               String reason) {
        return repNode.transferMaster(replicas, timeout, force, reason);
    }

    /**
     * Dump interesting aspects of the node's state. Currently for debugging
     * use, possibly useful for field support.
     */
    public String dumpState() {
        StringBuilder sb = new StringBuilder();

        sb.append(getNameIdPair());
        sb.append("[").append(getState()).append("] " );

        if (repNode != null) {
            sb.append(repNode.dumpState());
        }

        if (vlsnIndex != null) {
            sb.append("vlsnRange=");
            sb.append(vlsnIndex.getRange()).append("\n");
        }

        if (replay != null) {
            sb.append(replay.dumpState());
        }

        return sb.toString();
    }

    /**
     * Dumps the state associated with all active Feeders that supply
     * acknowledgments, along with identifying information about the node and
     * its current HA state.
     */
    public String dumpAckFeederState() {
        return getNameIdPair() + "[" + getState() + "]" +
               repNode.dumpAckFeederState() ;
    }

    /**
     * If this node was started with a hard recovery, preserve that
     * information.
     */
    public void setHardRecoveryInfo(RollbackException e) {
        hardRecoveryStat.set(true);
        hardRecoveryInfoStat.set(e.getMessage());
    }

    public StatGroup getNodeStats() {
        return nodeStats;
    }

    /**
     * Ensure that the in-memory vlsn index encompasses all logged entries
     * before it is flushed to disk. A No-Op for non-replicated systems.
     * [#19754]
     */
    @Override
    public void awaitVLSNConsistency() {
        vlsnIndex.awaitConsistency();
    }

    /**
     * Sets if the node is currently performing rollback while syncing with a
     * master.  This is currently just used by DbVerifyLog to tell it to
     * restart because the logs are about to change under it.
     */
    @Override
    public void setReplayRollback(final boolean rollback) {
        replayRollback = rollback;
    }

    /**
     * Returns if the node is currently in the process of replay rollback while
     * syncing with a new master.
     */
    @Override
    public boolean isReplayRollbackRunning() {
        return replayRollback;
    }

    public void setSyncupProgress(SyncupProgress progress) {
        setSyncupProgress(progress, 0, -1);
    }

    public void setSyncupProgress(SyncupProgress progress, long n, long total) {
        if (syncupProgressListener == null) {
            return;
        }

        if (!(syncupProgressListener.progress(progress, n, total))) {
            throw new EnvironmentFailureException
              (this, EnvironmentFailureReason.PROGRESS_LISTENER_HALT,
              "ReplicatedEnvironmentConfig.syncupProgressListener: ");
        }
    }

    /**
     * Test method to create pre-DTVLSN logs. When this is turned on in a test
     * environment the dtvlsn value in the log is written as a
     * UNINITIALIZED_VLSN (zero), which is the value that
     * deserialization would assign to it if a new replica came across an older
     * version commit or abort record.
     */
    public static void setSimulatePreDTVLSNMaster(
        boolean simulatePreDTVLSNMaster) {

        RepImpl.simulatePreDTVLSNMaster = simulatePreDTVLSNMaster;
    }

    public static boolean isSimulatePreDTVLSNMaster() {
        return simulatePreDTVLSNMaster;
    }

    /**
     * Unit test only, true to simulate capped DTVLSN
     */
    public static boolean isCappedDTVLSNMaster() {
        return isCappedDTVLSNMaster;
    }

    /**
     * Unit test only, set to use capped DTVLSN
     */
    public static void setCappedDTVLSNMaster(boolean cappedDTVLSN) {
        RepImpl.isCappedDTVLSNMaster = cappedDTVLSN;
    }

    public long getInsufficientReplicaTimeout() {
        return insufficientRepTimeout;
    }

    public LogFileRewriteListener getLogRewriteListener() {
        return logRewriteListener;
    }

    public ReplicationNetworkConfig getRepNetConfig() {
        return repNetConfig;
    }

    public DataChannelFactory getChannelFactory() {
        initializeChannelFactory();
        return channelFactory;
    }

    @Override
    public void invalidate(EnvironmentFailureException e) {
        super.invalidate(e);
        unblockTxnCompletion();
    }

    public long getLastTxnEnd() {
        return vlsnIndexAccess.getLastTxnEnd();
    }

    /**
     * Private class to prevent used of the close() method by the application
     * on an internal handle.
     */
    private static class InternalReplicatedEnvironment
        extends ReplicatedEnvironment {

        public InternalReplicatedEnvironment(File environmentHome,
                                             ReplicationConfig cloneRepConfig,
                                             EnvironmentConfig cloneConfig,
                                             RepImpl envImpl) {
            super(environmentHome, cloneRepConfig, cloneConfig,
                  null /*consistencyPolicy*/, null /*initialElectionPolicy*/,
                  false /*joinGroup*/, envImpl);
        }

        @Override
        protected boolean isInternalHandle() {
            return true;
        }

        @Override
        public synchronized void close() {
            throw EnvironmentFailureException.unexpectedState
                ("close() not permitted on an internal environment handle");
        }
    }

    /**
     * Peruse the environment wide transaction table, and return a set of
     * all existing MasterTxns.
     */
    public Set<MasterTxn> getExistingMasterTxns() {
        return getTxnManager().getTxns(MasterTxn.class);
    }

    /**
     * RepImpl supplies the last txn abort or commit vlsn for use cases such as
     * determining how caught up a feeder or master transfer is. This info is
     * usually obtained from the VLSNRange via the VLSNIndex, but in some types
     * of environment shutdowns, the VLSNIndex may need to be nulled out.  When
     * that happens, VLSNIndexAccess will switch over from using the VLSNIndex
     * to obtain the range, to using a reference to the last known
     * VLSNRange. Note that the VLSNRange instance held within VLSNIndex is
     * constantly being replaced when the replication stream is active., and
     * that's why LastTxnEndAccess generally obtains the range via the
     * VLSNIndex, rather keeping a reference to a VLSNRange instance.
     */
    private class VLSNIndexAccess {

        private VLSNRange savedRange;

        synchronized long getLastTxnEnd() {
            if (vlsnIndex != null) {
                return vlsnIndex.getRange().getLastTxnEnd();
            }
            return savedRange.getLastTxnEnd();
        }

        /**
         * Save the last range so the lastTxnEnd value can continue
         * to be available, and null out the vlsnIndex.
         */
        synchronized void closeVLSNIndex(boolean checkpointed) {
            if (vlsnIndex != null) {
                vlsnIndex.close(checkpointed);
                savedRange = vlsnIndex.getRange();
                vlsnIndex = null;
            }
        }

        /**
         * Save the last range so the lastTxnEnd value can continue
         * to be available, and null out the vlsnIndex.
         */
        synchronized void abnormalCloseVLSNIndex() {
            if (vlsnIndex != null) {
                vlsnIndex.abnormalClose();
                savedRange = vlsnIndex.getRange();
                vlsnIndex = null;
            }
        }
    }

    /**
     * Checks that writing records with a TTL is allowed.
     *
     * @throws IllegalStateException if any node in the group is less than
     * JE_TTL_VERSION.
     */
    @Override
    public void checkTTLAvailable() {
        if (isTTLAvailable) {
            return;
        }
        checkFeatureAvailable("TTL", TTL.getMinJEVersion());
        isTTLAvailable = true;
    }

    @Override
    public void checkRecordExtinctionAvailable() {
        if (isRecordExtinctionAvailable) {
            return;
        }
        checkFeatureAvailable(
            "Record Extinction", ExtinctionScanner.getMinJEVersion());
        isRecordExtinctionAvailable = true;
    }

    private void checkFeatureAvailable(final String name,
                                       final JEVersion requiredJEVersion) {
        try {
            repNode.setMinJEVersion(requiredJEVersion);
        } catch (MinJEVersionUnsupportedException e) {
            if (e.nodeVersion == null) {
                throw new IllegalStateException(
                    name + " is not currently supported." +
                        " The version running on node " + e.nodeName +
                        " could not be determined," +
                        " but this feature requires version " +
                        requiredJEVersion.getNumericVersionString() +
                        " or later.");
            }
            throw new IllegalStateException(
                name + " is not currently supported." +
                    " Node " + e.nodeName + " is running version " +
                    e.nodeVersion.getNumericVersionString() +
                    ", but this feature requires version " +
                    requiredJEVersion.getNumericVersionString() +
                    " or later.");
        }
    }

    /**
     * Recovery encountered a RestoreRequired marker.
     */
    @Override
    public void handleRestoreRequired(RestoreRequired restoreRequired) {

        switch (restoreRequired.getFailureType()) {
        case NETWORK_RESTORE:
            /*
             * A network restore must be done to get a coherent copy of
             * the log files into this environment's directory.
             */
            throw new InsufficientLogException(
                restoreRequired.getProperties(),
                configManager.get(RepParams.HELPER_HOSTS),
                repNetConfig);
        default:
            /* Not a type we can handle, go to the default behavior */
            super.handleRestoreRequired(restoreRequired);
        }
    }

    private boolean useArbiter(MasterTxn txn) {
        if (allowArbiterAck &&
            repNode.getGroup().getAckGroupSize() == 2 &&
            repNode.feederManager().activeAckArbiterCount() > 0 &&
            txn.getCommitDurability().getReplicaAck() ==
            ReplicaAckPolicy.SIMPLE_MAJORITY) {
            return true;
        }
        return false;
    }

    public StreamAuthenticator getAuthenticator() {
        return authenticator;
    }

    public boolean getDisableOpeningPort() {
        return disableOpeningPort;
    }

    public RateLimitingLogger<String> getReplicationWarningLogger() {
        return replicationWarningLogger;
    }

    /**
     * Returns the channel timeout if the RepNode is a replica.
     */
    public int getReplicaFeederChannelTimeoutMillis() {
        return getConfigManager().
            getDuration(RepParams.REPLICA_TIMEOUT);
    }

    /**
     * Returns the channel timeout if the RepNode is a feeder.
     */
    public int getFeederReplicaChannelTimeoutMillis() {
        return getConfigManager().
            getDuration(RepParams.FEEDER_TIMEOUT);
    }
 
    public void setSubscription(final Subscription sub) {
    	subscription = sub;
    }
    
    public Subscription getSubscription() {
    	return subscription;
    }
}
