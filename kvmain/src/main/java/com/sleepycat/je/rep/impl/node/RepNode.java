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

import static com.sleepycat.je.rep.ReplicatedEnvironment.State.MASTER;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.REPLICA;
import static com.sleepycat.je.rep.ReplicatedEnvironment.State.UNKNOWN;
import static com.sleepycat.je.rep.impl.RepImpl.CAPPED_DTVLSN;
import static com.sleepycat.je.rep.impl.node.ChannelTimeoutStatDefinition.N_CHANNEL_TIMEOUT_MAP;
import static com.sleepycat.je.rep.impl.node.MasterTransferStatDefinition.N_MASTER_TRANSFERS;
import static com.sleepycat.je.rep.impl.node.MasterTransferStatDefinition.N_MASTER_TRANSFERS_SUCCESS;
import static com.sleepycat.je.rep.impl.node.MasterTransferStatDefinition.N_MASTER_TRANSFERS_FAILURE;
import static com.sleepycat.je.rep.impl.RepParams.DBTREE_CACHE_CLEAR_COUNT;
import static com.sleepycat.je.rep.impl.RepParams.ENV_CONSISTENCY_TIMEOUT;
import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;
import static com.sleepycat.je.rep.impl.RepParams.HEARTBEAT_INTERVAL;
import static com.sleepycat.je.rep.impl.RepParams.IGNORE_SECONDARY_NODE_ID;
import static com.sleepycat.je.rep.impl.RepParams.NODE_TYPE;
import static com.sleepycat.je.rep.impl.RepParams.RESET_REP_GROUP_RETAIN_UUID;
import static com.sleepycat.je.rep.impl.RepParams.SECURITY_CHECK_INTERVAL;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.BitSet;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.sleepycat.je.rep.ReplicaConnectRetryException;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.FileProtector;
import com.sleepycat.je.cleaner.FileProtector.ProtectedFileSet;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.StartupTracker.Phase;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.rep.GroupShutdownException;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.QuorumPolicy;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.arbitration.Arbiter;
import com.sleepycat.je.rep.elections.Acceptor;
import com.sleepycat.je.rep.elections.Acceptor.PrePromiseResult;
import com.sleepycat.je.rep.elections.Elections;
import com.sleepycat.je.rep.elections.ElectionsConfig;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterObsoleteException;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol;
import com.sleepycat.je.rep.impl.BinaryNodeStateProtocol.BinaryNodeStateResponse;
import com.sleepycat.je.rep.impl.BinaryNodeStateService;
import com.sleepycat.je.rep.impl.GroupService;
import com.sleepycat.je.rep.impl.MinJEVersionUnsupportedException;
import com.sleepycat.je.rep.impl.NodeStateService;
import com.sleepycat.je.rep.impl.PointConsistencyPolicy;
import com.sleepycat.je.rep.impl.RepGroupDB;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepGroupImpl.NodeConflictException;
import com.sleepycat.je.rep.impl.RepGroupProtocol;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupResponse;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.DataChannelFactory.ConnectOptions;
import com.sleepycat.je.rep.stream.FeederTxns;
import com.sleepycat.je.rep.stream.MasterChangeListener;
import com.sleepycat.je.rep.stream.MasterStatus;
import com.sleepycat.je.rep.stream.MasterSuggestionGenerator;
import com.sleepycat.je.rep.subscription.StreamAuthenticator;
import com.sleepycat.je.rep.txn.ReplayTxn;
import com.sleepycat.je.rep.util.AtomicLongMax;
import com.sleepycat.je.rep.util.ldiff.LDiffService;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.rep.utilint.RepUtils.Clock;
import com.sleepycat.je.rep.utilint.RepUtils.ExceptionAwareCountDownLatch;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnAbort;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.AtomicLongMapStat;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.StoppableThread;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * Represents a replication node. This class is the locus of operations that
 * manage the state of the node, master, replica, etc. Once the state of a node
 * has been established the thread of control passes over to the Replica or
 * FeederManager instances.
 *
 * Note that both Feeders and the Replica instance may be active in future when
 * we support r2r replication, in addition to m2r replication. For now however,
 * either the FeederManager is active, or the Replica is and the same common
 * thread control can be shared between the two.
 */
public class RepNode extends StoppableThread {

    /** Test hook called each run loop. For unit testing */
    public static volatile TestHook<RepNode> runLoopTestHook;
    /** Test hook called after election for each run loop. For unit testing */
    public static volatile TestHook<RepNode> runLoopAfterElectionTestHook;

    /*
     * The unique node name and internal id that identifies the node within
     * the rep group. There is a canonical instance of this that's updated
     * when the node joins the group.
     */
    private final NameIdPair nameIdPair;

    /* The service dispatcher used by this replication node. */
    private final ServiceDispatcher serviceDispatcher;

    /* The election instance for this node */
    private Elections elections;

    /* The locus of operations when the node is a replica. */
    private final Replica replica;

    /* Used when the node is a feeder. */
    private FeederManager feederManager;

    /*
     * The status of the Master. Note that this is the leading state as
     * communicated to this node via the Listener. The node itself may not as
     * yet have responded to this state change announced by the Listener. That
     * is, nodeState, may reflect a different state until the transition to
     * this state has been completed.
     */
    private final MasterStatus masterStatus;

    /* Election managers and hooks */

    private final ElectionStatesContinuation electionStateContinuation;
    private final Acceptor.PrePromiseHook prePromiseHook;
    private final MasterSuggestionGenerator suggestionGenerator;
    private final Learner.PreLearnHook preLearnHook;
    private final MasterChangeListener changeListener;

    /*
     * The VLSN that denotes the end of the immediately preceding term. All
     * vlsns higher than this value are associated with the current term. It's
     * set whenever the node makes a state transition to a master for a new
     * term and is reset to null, when the new term ends.
     */
    private long prevTermEndVLSN = VLSN.NULL_VLSN;

    /*
     * Represents the application visible state of this node. It may lag the
     * state as described by masterStatus.
     */
    private final NodeState nodeState;

    private final RepImpl repImpl;

    /* The encapsulated internal replication group database. */
    final RepGroupDB repGroupDB;

    /*
     * The latch used to indicate that the node has a well defined state as a
     * Master or Replica and has finished the node-specific initialization that
     * will permit it to function immediately in that capacity.
     *
     * For a Master it means that it's ready to start accepting connections
     * from Replicas.
     *
     * For a Replica, it means that it has established a connection with a
     * Feeder, completed the handshake process that validates it as being a
     * legitimate member of the group, established a sync point, and is ready
     * to start replaying the replication stream.
     */
    private volatile ExceptionAwareCountDownLatch readyLatch = null;

    /*
     * Latch used to freeze txn commit VLSN advancement during an election.
     */
    private final CommitFreezeLatch vlsnFreezeLatch = new CommitFreezeLatch();

    /**
     * Describes the nodes that form the group. This information is dynamic
     * it's initialized at startup and subsequently as a result of changes
     * made either directly to it, when the node is a master, or via the
     * replication stream, when it is a Replica.
     *
     * Always use the setGroup() method to set this iv, so that needsAck in
     * particular is updated in unison.
     */
    volatile private RepGroupImpl group;

    /**
     * Acks needed. Determines whether durability needs acknowledgments from
     * other nodes, that is, the rep group has more than one data node that's
     * also electable.
     *
     * Only update via the setGroup method.
     */
    volatile private boolean needsAcks = false;

    /*
     * Determines the election policy to use when the node holds its very first
     * elections
     */
    private QuorumPolicy electionQuorumPolicy = QuorumPolicy.SIMPLE_MAJORITY;

    /*
     * Amount of time to sleep between retries when a new node tries to locate
     * a master.
     */
    private static final int MASTER_QUERY_RETRY_MS = 1000;

    /*
     * Amount of time to sleep between retries when a new node tries to locate
     * the group info.
     */
    private static final int GROUP_QUERY_RETRY_MS = 1000;

    /* Number of times to retry joining on a retryable exception. */
    private static final int JOIN_RETRIES = 10;

    /*
     * Encapsulates access to current time, to arrange for testing of clock
     * skews.
     */
    private final Clock clock;

    private com.sleepycat.je.rep.impl.networkRestore.FeederManager
        logFeederManager;
    private LDiffService ldiff;
    private NodeStateService nodeStateService;
    private BinaryNodeStateService binaryNodeStateService;
    private GroupService groupService;

    /* The currently in-progress Master Transfer operation, if any. */
    private volatile MasterTransfer xfrInProgress = null;

    private final StatGroup masterTransferStats;
    private final IntStat nMasterTransfers;
    private final IntStat nMasterTransfersSuccess;
    private final IntStat nMasterTransfersFailure;

    /*
     * Stats showing what NamedChannelWithTimeout have timed out due to
     * channel inactivity.
     */
    private final StatGroup channelTimeoutStats;
    private final AtomicLongMapStat nChannelTimeoutMap;

    /* Determines how long to wait for a replica to catch up on a close. */
    private long replicaCloseCatchupMs = -1;

    /*
     * A timer used for misc short-lived scheduled tasks:
     * ChannelTimeoutTask, Elections.RebroadcastTask.
     */
    private final Timer timer;
    private final ChannelTimeoutTask channelTimeoutTask;

    final Logger logger;

    /* Locus of election and durability quorum decisions */
    private final ElectionQuorum electionQuorum;
    private final DurabilityQuorum durabilityQuorum;

    private final Arbiter arbiter;
    private final NodeType nodeType;

    /* The time when the node first came up, and the handle became available */
    private long joinTimeMs = 0;

    /** Manages the allocation of node IDs for secondary nodes. */
    private final TransientIds transientIds =
        new TransientIds(RepGroupImpl.MAX_NODES_WITH_TRANSIENT_ID);

    /**
     * Synchronize on this object when setting the minimum JE version or adding
     * a secondary node, which could change the JE versions of the nodes to
     * check when setting a new minimum.
     *
     * @see #setMinJEVersion
     * @see #addTransientIdNode
     */
    private final Object minJEVersionLock = new Object();

    /* Used by tests only. */
    private int logVersion = LogEntryType.LOG_VERSION;

    /* For unit testing */
    private Set<TestHook<Integer>> convertHooks;

    /**
     * The in-memory DTVLSN. It represents smallest local durable vlsn among
     * all replicas.
     *
     * At a master, knowledge of this replication state may have been
     * communicated explicitly due to the use of SIMPLE_MAJORITY or ALL ACKs,
     * or it may have been communicated via a heartbeat indicating the progress
     * of replication at a replica.
     *
     * At a replica, this state is obtained from commit/abort records in the
     * replication stream.
     *
     * This field is initialized from its persistent value whenever the
     * environment is first opened. It may be the null VLSN value for brand new
     * environments. This value can only advance as increasing numbers of
     * transactions are acknowledged.
     *
     * @see <a href="https://<wikihost>/trac/wiki/JEReplicationDurableTxnVLSN">
     * DTVLSN</a>
     *
     */
    private final AtomicLongMax dtvlsn = new AtomicLongMax(NULL_VLSN);

    /**
     * The in-memory localDurableVLSN. It represents the highest local
     * durable vlsn on disk of this replication node.
     *
     * When FileManager.endOfLog.force() is called, this field will be updated
     * accordingly.
     *
     * And this field is initialized in the same fashion as DTVLSN. Although
     * this value can decrease as rollback during syncup.
     *
     * This value will be sent from replica to feeder to compute the dtvlsn.
     */
    private final AtomicLongMax localDurableVLSN = new AtomicLongMax(NULL_VLSN);

    /** latch to wait for a given VLSN to be durable */
    private volatile VLSNIndex.VLSNAwaitLatch dtVLSNLatch = null;

    /**
     * If not null, a test hook that is called with the name of the current
     * node during the query for group membership before the node sleeps after
     * failing to obtain information about the group master -- for unit
     * testing.
     */
    public static volatile TestHook<String>
        queryGroupForMembershipBeforeSleepHook;

    /**
     * If not null, called by queryGroupForMembership with the name of the
     * current node before querying learners for the master -- for unit
     * testing.
     */
    public static volatile TestHook<String>
        queryGroupForMembershipBeforeQueryForMaster;

    /**
     * If not null, a test hook that is called with the name of the current
     * node before attempting to contact each network restore supplier, for
     * unit testing.
     */
    public static volatile TestHook<String> beforeFindRestoreSupplierHook;

    public RepNode(RepImpl repImpl,
                   Replay replay,
                   NodeState nodeState)
        throws IOException, DatabaseException {

        super(repImpl, "RepNode " + repImpl.getNameIdPair(),
              /* Shares the feeder bucket. */
              repImpl.getFileManager().getFeederStatsCollector());

        this.repImpl = repImpl;
        readyLatch = new ExceptionAwareCountDownLatch(repImpl, 1);
        nameIdPair = repImpl.getNameIdPair();
        logger = LoggerUtils.getLogger(getClass());

        this.serviceDispatcher =
            new ServiceDispatcher(getSocket(), repImpl,
                                  repImpl.getChannelFactory());
        serviceDispatcher.start();
        clock = new Clock(RepImpl.getClockSkewMs());
        this.repGroupDB = new RepGroupDB(repImpl);

        masterStatus = new MasterStatus(nameIdPair);
        replica = ReplicaFactory.create(this, replay);

        feederManager = new FeederManager(this);

        electionStateContinuation = new ElectionStatesContinuation(repImpl);
        suggestionGenerator = new MasterSuggestionGenerator(this);
        prePromiseHook = new ElectionPrePromiseHook(this);
        preLearnHook = new ElectionPreLearnHook(this);
        changeListener = new MasterChangeListener(this);

        this.nodeState = nodeState;

        electionQuorum = new ElectionQuorum(repImpl);
        durabilityQuorum = new DurabilityQuorum(repImpl);

        utilityServicesStart();
        timer = new Timer(true);
        channelTimeoutTask = new ChannelTimeoutTask(timer);

        arbiter = new Arbiter(repImpl);
        nodeType = NodeType.valueOf(getConfigManager().get(NODE_TYPE));

        masterTransferStats = new StatGroup(
            MasterTransferStatDefinition.GROUP_NAME,
            MasterTransferStatDefinition.GROUP_DESC);
        nMasterTransfers = new IntStat(
            masterTransferStats, N_MASTER_TRANSFERS);
        nMasterTransfersSuccess = new IntStat(
            masterTransferStats, N_MASTER_TRANSFERS_SUCCESS);
        nMasterTransfersFailure = new IntStat(
            masterTransferStats, N_MASTER_TRANSFERS_FAILURE);

        channelTimeoutStats = new StatGroup(
            ChannelTimeoutStatDefinition.GROUP_NAME,
            ChannelTimeoutStatDefinition.GROUP_DESC);
        nChannelTimeoutMap = new AtomicLongMapStat(
            channelTimeoutStats, N_CHANNEL_TIMEOUT_MAP);

        updateDTVLSNMax(repImpl.getLoggedDTVLSN());
        /*
         * local durable vlsn will be instantiated to the VLSN of the
         * last LOG_TXN_END.
         */
        updateLocalDurableVLSN(repImpl.getLastTxnEnd());
        LoggerUtils.info(logger, repImpl,
                           String.format("DTVLSN at start:%,d", dtvlsn.get()));
    }

    private void utilityServicesStart() {
        ldiff = new LDiffService(serviceDispatcher, repImpl);
        logFeederManager =
            new com.sleepycat.je.rep.impl.networkRestore.FeederManager
            (serviceDispatcher, repImpl, nameIdPair);

        /* Register the node state querying service. */
        nodeStateService = new NodeStateService(serviceDispatcher, this);
        serviceDispatcher.register(nodeStateService);

        binaryNodeStateService =
            new BinaryNodeStateService(serviceDispatcher, this);
        groupService = new GroupService(serviceDispatcher, this);
        serviceDispatcher.register(groupService);
    }

    /* Create a placeholder node, for test purposes only. */
    public RepNode(NameIdPair nameIdPair) {
        this(nameIdPair, null);
    }

    public RepNode() {
        this(NameIdPair.NULL);
    }

    public RepNode(NameIdPair nameIdPair,
                   ServiceDispatcher serviceDispatcher) {
        super("RepNode " + nameIdPair);
        repImpl = null;
        clock = new Clock(0);

        this.nameIdPair = nameIdPair;
        this.serviceDispatcher = serviceDispatcher;

        this.repGroupDB = null;

        masterStatus = new MasterStatus(NameIdPair.NULL);
        replica = null;
        feederManager = null;
        electionStateContinuation = null;
        suggestionGenerator = null;
        prePromiseHook = (p) -> PrePromiseResult.createProceed();
        preLearnHook = (p) -> true;
        changeListener = null;
        nodeState = null;
        logger = null;
        timer = null;
        channelTimeoutTask = null;
        electionQuorum = null;
        durabilityQuorum = null;
        arbiter = null;
        nodeType = NodeType.ELECTABLE;
        masterTransferStats = new StatGroup(
            MasterTransferStatDefinition.GROUP_NAME,
            MasterTransferStatDefinition.GROUP_DESC);
        nMasterTransfers = new IntStat(
            masterTransferStats, N_MASTER_TRANSFERS);
        nMasterTransfersSuccess = new IntStat(
            masterTransferStats, N_MASTER_TRANSFERS_SUCCESS);
        nMasterTransfersFailure = new IntStat(
            masterTransferStats, N_MASTER_TRANSFERS_FAILURE);

        channelTimeoutStats = new StatGroup(
            ChannelTimeoutStatDefinition.GROUP_NAME,
            ChannelTimeoutStatDefinition.GROUP_DESC);
        nChannelTimeoutMap = new AtomicLongMapStat(
            channelTimeoutStats, N_CHANNEL_TIMEOUT_MAP);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns the time when the node came up, and the handle became
     * available. It's zero if a successful handle was never established.
     */
    public long getJoinTimeMs() {
        return joinTimeMs;
    }

    /**
     * Returns the node type of this node.
     */
    public NodeType getNodeType() {
        return nodeType;
    }

    /**
     * Returns the nodeState. Only used by unit tests
     */
    public NodeState getNodeState() {
        return nodeState;
    }

    /**
     * Returns the electionStateContinuation.
     */
    public ElectionStatesContinuation getElectionStateContinuation() {
        return electionStateContinuation;
    }

    /**
     * Returns the term associated with the master, or if the node is not
     * currently the master, the master term associated with the last log
     * entry.
     */
    public MasterIdTerm getMasterIdTerm() {
        final MasterIdTerm masterIdTerm = nodeState.getMasterIdTerm();
        if (masterIdTerm != null) {
            return masterIdTerm;
        }

        /* Not a master, get the master term from the last commit log entry */
        TxnEnd txnEnd = repImpl.getLastTxnEndEntry();
        if (txnEnd == null) {
            /* No rep txns a brand new env */
            return MasterIdTerm.PRIMODAL_ID_TERM;
        }
        return txnEnd.getMasterIdTerm();
    }

    /**
     * Returns the timer associated with this RepNode
     */
    public Timer getTimer() {
        return timer;
    }

    public ServiceDispatcher getServiceDispatcher() {
        return serviceDispatcher;
    }

    /**
     * Returns the accumulated statistics for this node. The method
     * encapsulates the statistics associated with its two principal components
     * the FeederManager and the Replica.
     */
    public ReplicatedEnvironmentStats getStats(StatsConfig config) {
        return RepInternal.makeReplicatedEnvironmentStats(repImpl, config);
    }

    public void resetStats() {
        channelTimeoutStats.clear();
        masterTransferStats.clear();
        feederManager.resetStats();
        replica.resetStats();
    }

    public ExceptionAwareCountDownLatch getReadyLatch() {
        return readyLatch;
    }

    public CommitFreezeLatch getVLSNFreezeLatch() {
        return vlsnFreezeLatch;
    }

    public void resetReadyLatch(Exception exception) {
        ExceptionAwareCountDownLatch old = readyLatch;
        readyLatch = new ExceptionAwareCountDownLatch(repImpl, 1);
        if (old.getCount() != 0) {
            /* releasing latch in some error situation. */
            old.releaseAwait(exception);
        }
    }

    /* The methods below return the components of the rep node. */
    public FeederManager feederManager() {
        return feederManager;
    }

    public com.sleepycat.je.rep.impl.networkRestore.FeederManager
            getLogFeederManager() {
        return logFeederManager;
    }

    public Replica replica() {
        return replica;
    }

    public Clock getClock() {
        return clock;
    }

    public Replica getReplica() {
        return replica;
    }

    public RepGroupDB getRepGroupDB() {
        return repGroupDB;
    }

    /**
     * Retrieves the node's current snapshot image of the group definition.
     * <p>
     * There is a very brief period of time, during node start-up, where this
     * can be <code>null</code>.  But after that it should always return a
     * valid object.
     */
    public RepGroupImpl getGroup() {
        return group;
    }

    /**
     * Returns the UUID associated with the replicated environment.
     */
    public UUID getUUID() {
        if (group == null) {
            throw EnvironmentFailureException.unexpectedState
                ("Group info is not available");
        }
        return group.getUUID();
    }

    /**
     * Returns the nodeName associated with this replication node.
     *
     * @return the nodeName
     */
    public String getNodeName() {
        return nameIdPair.getName();
    }

    /**
     * Returns the nodeId associated with this replication node.
     *
     * @return the nodeId
     */
    public int getNodeId() {
        return nameIdPair.getId();
    }

    public NameIdPair getNameIdPair() {
        return nameIdPair;
    }

    public InetSocketAddress getSocket() {
        return repImpl.getSocket();
    }

    public String getHostName() {
         return repImpl.getHostName();
    }

    public int getPort() {
        return repImpl.getPort();
    }

    public MasterStatus getMasterStatus() {
        return masterStatus;
    }

    /**
     * Returns a definitive answer to whether this node is currently the master
     * by checking both its status as a master and whether the group agrees
     * that it is the master.
     *
     * Such an authoritative answer is needed in a network partition situation
     * to detect a master that may be isolated on the minority side of a
     * network partition.
     *
     * @return true if the node is definitely the master. False if it's not or
     * we cannot be sure.
     */
    public boolean isAuthoritativeMaster() {
        return (electionQuorum.
            isAuthoritativeMasterOld(getMasterStatus(), feederManager));
    }

    /**
     * Waits until this node gets messages from enough replicas to confirm
     * that it is the master.
     * @param txn waiting for the node to be authorized as the master
     * @throws InterruptedException
     */
    public void awaitAuthoritativeMaster(Txn txn) throws InterruptedException {
        feederManager.awaitAuthoritativeMaster(txn);
    }

    public int getHeartbeatInterval() {
        return getConfigManager().getInt(HEARTBEAT_INTERVAL);
    }

    /* For unit testing only. */
    public void setVersion(int version) {
        logVersion = version;
    }

    public int getLogVersion() {
        return logVersion;
    }

    public int getElectionPriority() {

        /* A node should not become master if it cannot write. */
        if (repImpl.getDiskLimitViolation() != null) {
            return 0;
        }

        final int priority =
            getConfigManager().getInt(RepParams.NODE_PRIORITY);

        final int defaultPriority =
            Integer.parseInt(RepParams.NODE_PRIORITY.getDefault());

        return (getConfigManager().getBoolean(RepParams.DESIGNATED_PRIMARY) &&
                (priority == defaultPriority)) ?
            defaultPriority + 1 : /* Raise its priority. */
            priority; /* Explicit priority, leave it intact. */
    }

    /*
     * Amount of time to wait for a thread to finish on a shutdown. It's
     * a multiple of a heartbeat, since a thread typically polls for a
     * shutdown once per heartbeat.
     */
    public int getThreadWaitInterval() {
        return getHeartbeatInterval() * 16;
    }

    int getDbTreeCacheClearingOpCount() {
        return getConfigManager().getInt(DBTREE_CACHE_CLEAR_COUNT);
    }

    public RepImpl getRepImpl() {
        return repImpl;
    }

    public LogManager getLogManager() {
        return repImpl.getLogManager();
    }

    DbConfigManager getConfigManager() {
        return repImpl.getConfigManager();
    }

    public VLSNIndex getVLSNIndex() {
        return repImpl.getVLSNIndex();
    }

    public FeederTxns getFeederTxns() {
        return repImpl.getFeederTxns();
    }

    public Elections getElections() {
        return elections;
    }

    public MasterSuggestionGenerator getSuggestionGenerator() {
        return suggestionGenerator;
    }

    /* Used by unit tests only. */
    public QuorumPolicy getElectionPolicy() {
        return electionQuorumPolicy;
    }

    /**
     * Returns an array of nodes suitable for feeding log files for a network
     * restore.
     *
     * @return an array of feeder nodes
     */
    public RepNodeImpl[] getLogProviders() {
        final Set<RepNodeImpl> nodes = getGroup().getDataMembers();
        return nodes.toArray(new RepNodeImpl[nodes.size()]);
    }

    public ChannelTimeoutTask getChannelTimeoutTask() {
        return channelTimeoutTask;
    }

    public boolean isMaster() {
        return masterStatus.isNodeMaster();
    }

    /* Get the current master name if it exists. */
    public String getMasterName() {
        if (masterStatus.getGroupMasterNameId().getId() ==
            NameIdPair.NULL_NODE_ID) {
            return null;
        }

        return masterStatus.getGroupMasterNameId().getName();
    }

    /**
     * Returns the latest VLSN associated with a replicated commit. Note that
     * since the lastTxnEndVLSN is computed outside the write log latch, via
     * EnvironmentImpl.registerVLSN(LogItem) it's possible for it to be behind
     * on an instantaneous basis, but it will eventually catch up when the
     * updates quiesce.
     */
    public long getCurrentTxnEndVLSN() {
        return repImpl.getLastTxnEnd();
    }

    /**
     * Returns the instantaneous non-null DTVLSN value. The value should be non
     * null once initialization has been completed.
     *
     * The returned value can be VLSN.UNINITIALIZED_VLSN if the node
     * is a replica in a pre-dtvlsn log segment, or a master that has not as
     * yet seen any acknowledged transactions.
     */
    public long getDTVLSN() {
        final long retValue = dtvlsn.get();
        if (VLSN.isNull(retValue)) {
            throw new IllegalStateException("DTVLSN cannot be null");
        }
        return retValue;
    }

    public long getLocalDurableVLSN() {
        return localDurableVLSN.get();
    }

    /**
     * Returns a DTVLSN (possibly null) for logging/debugging purposes.
     */
    public long getAnyDTVLSN() {
        return dtvlsn.get();
    }

    /**
     * Updates the DTVLSN with a potentially new DTVLSN value, if the vlsn
     * value is higher than the vlsn at the start of this term.
     *
     * Note that this method is only invoked when the node is a Master. The
     * Replica simply sets the DTVLSN to a specific value to the value it
     * receives in the log record.
     *
     * @param candidateDTVLSN the new candidate DTVLSN
     *
     * @return the new DTVLSN which is either the candidatDTVLSN or a more
     * recent DTVLSN &gt; candidateDTVLSN. If the candidateVLSN is &lt; the vlsn
     * that represents the start of the term it returns the current dtvlsn
     */
    public long updateDTVLSN(long candidateDTVLSN) {
        if (RepImpl.isSimulatePreDTVLSNMaster()) {
            return UNINITIALIZED_VLSN;
        }
        if (RepImpl.isCappedDTVLSNMaster()) {
            /* unit test only */
            final long vlsn = Math.min(candidateDTVLSN, CAPPED_DTVLSN);
            return updateDTVLSNMax(vlsn);
        }

        return (candidateDTVLSN > prevTermEndVLSN) ?
            updateDTVLSNMax(candidateDTVLSN) : dtvlsn.get();
    }

    /**
     * Sets the DTVLSN to a specific value. This method is used exclusively by
     * the Replica as it maintains the DTVLSN based upon the contents of the
     * replication stream.
     *
     * @return the previous DTVLSN value
     */
    public long setDTVLSN(long newDTVLSN) {
        return dtvlsn.set(newDTVLSN);
    }

    /**
     * Sets the LocalDurableVLSN to a specific value. This method is used only by
     * the Replica right after syncup End. Note that the newVLSN(which is
     * matchpointVLSN) could be greater than localDurablVLSN, in that case
     * remain localDurableVLSN unchanged.
     */
    public void setLocalDurableVLSN(long newVLSN) {
        localDurableVLSN.resetToSmallerValue(newVLSN);
    }

    /**
     * Sets the group metadata associated with the RepNode and updates any
     * local derived data.
     */
    public void setGroup(RepGroupImpl repGroupImpl) {
        group = repGroupImpl;
        needsAcks = durabilityQuorum.
            getCurrentRequiredAckCount(ReplicaAckPolicy.SIMPLE_MAJORITY) > 0;
    }

    /*
     * Testing API used to force this node as a master. The mastership is
     * communicated upon election completion via the Listener. It's the
     * responsibility of the caller to ensure that only one node is forced
     * at a time via this API.
     *
     * @param force true to force this node as the master, false reverts back
     *              to use of normal (non-preemptive) elections.
     */
    public void forceMaster(boolean force)
        throws InterruptedException, DatabaseException {

        suggestionGenerator.forceMaster(force);
        /* Initiate elections to make the changed proposal heard. */
        refreshCachedGroup();
        elections.initiateElection(group, electionQuorumPolicy);
        LoggerUtils.info(logger, repImpl, "Forced master " +
                         (force ? "" : "not ") + "in effect");
    }

    int getSecurityCheckInterval() {
        return getConfigManager().getInt(SECURITY_CHECK_INTERVAL);
    }

    StreamAuthenticator getAuthenticator() {
        if (repImpl == null) {
            return null;
        }

        return repImpl.getAuthenticator();
    }

    /**
     * Starts up the thread in which the node does its processing as a master
     * or replica. It then waits for the newly started thread to transition it
     * out of the DETACHED state, and returns upon completion of this
     * transition.
     *
     * @throws DatabaseException
     */
    private void startup(QuorumPolicy initialElectionPolicy)
        throws DatabaseException {

        if (isAlive()) {
            return;
        }

        if (nodeState.getRepEnvState().isDetached()) {
            nodeState.changeToUnknownAndNotify();
        }
        elections = new Elections(new RepElectionsConfig(this),
                                  electionStateContinuation,
                                  prePromiseHook,
                                  suggestionGenerator,
                                  preLearnHook,
                                  changeListener);

        if (electionQuorum.nodeTypeParticipates(nodeType)) {
            /* Electable members should participate in elections */
            elections.participate();
        } else {
            /* All other nodes are just interested in election outcomes. */
            elections.startLearner();
        }

        repImpl.getStartupTracker().start(Phase.RESTORE_ELECTION_STATE);
        try {
            restoreElectionState();
            this.electionQuorumPolicy = initialElectionPolicy;
        } finally {
            repImpl.getStartupTracker().stop(Phase.RESTORE_ELECTION_STATE);
        }

        start();
    }

    /**
     * This method must be invoked when a RepNode is first initialized and
     * subsequently every time there is a change to the replication group.
     * <p>
     * The Master should invoke this method each time a member is added or
     * removed, and a replica should invoke it each time it detects the commit
     * of a transaction that modifies the membership database.
     * <p>
     * In addition, it must be invoked after a syncup operation, since it may
     * revert changes made to the membership table.
     *
     * @throws DatabaseException
     */
    public RepGroupImpl refreshCachedGroup()
        throws DatabaseException {

        setGroup(repGroupDB.getGroup());

        elections.updateRepGroup(group);
        if (nameIdPair.hasNullId()) {
            RepNodeImpl n = group.getMember(nameIdPair.getName());
            if (n != null) {

                /*
                 * Don't update the node ID for a secondary node if
                 * IGNORE_SECONDARY_NODE_ID is true.  In that case, we are
                 * trying to convert a previously electable node to a secondary
                 * node, so the information about the electable node ID in the
                 * local copy of the rep group DB should be ignored.
                 */
                if (!nodeType.isSecondary() ||
                    !getConfigManager().getBoolean(IGNORE_SECONDARY_NODE_ID)) {
                    /* May not be sufficiently current in the rep stream. */
                    nameIdPair.update(n.getNameIdPair());
                }
            }
        }
        return group;
    }

    /**
     * Removes a node so that it's no longer a member of the group.
     *
     * Note that names referring to removed nodes cannot be reused.
     *
     * @param nodeName identifies the node to be removed
     *
     * @throws MemberNotFoundException if the node denoted by
     * <code>memberName</code> is not a member of the replication group.
     *
     * @throws MasterStateException if the member being removed is currently
     * the Master
     *
     * @see <a href="https://sleepycat.oracle.com/trac/wiki/DynamicGroupMembership#DeletingMembers">Member Deletion</a>
     */
    public void removeMember(String nodeName) {
        removeMember(nodeName, false);
    }

    /**
     * Remove or delete a node from the group.  If deleting a node, the node
     * must not be active.
     *
     * <p>Note that names referring to removed nodes cannot be reused, but
     * names for deleted nodes can be.
     *
     * @param nodeName identifies the node to be removed or deleted
     *
     * @param delete whether to delete the node rather than just remove it
     *
     * @throws MemberActiveException if {@code delete} is {@code true} and
     * the node is currently active
     *
     * @throws MemberNotFoundException if the node denoted by
     * <code>memberName</code> is not a member of the replication group.
     *
     * @throws MasterStateException if the member being removed or deleted is
     * currently the Master
     */
    public void removeMember(String nodeName, boolean delete) {
        checkValidity(
            nodeName, delete ? "Deleting member" : "Removing member");

        if (delete && feederManager.getFeeder(nodeName) != null) {
            throw new MemberActiveException(
                "Attempt to delete an active node: " + nodeName);
        }

        /*
         * First remove it from the cached group, effectively setting new
         * durability requirements, for the ensuing group db updates.
         */
        RepNodeImpl node = group.removeMember(nodeName, delete);

        /*
         * Shutdown any feeder that may be active with the replica. Unless
         * deleting, any subsequent attempts by the replica to rejoin the group
         * will result in a failure.
         */
        feederManager.shutdownFeeder(node);
        repGroupDB.removeMember(node, delete);
    }

    /**
     * Update the network address of a node.
     *
     * Note that an alive node's address can't be updated, we'll throw an
     * ReplicaStateException for this case.
     *
     * @param nodeName identifies the node to be updated
     * @param newHostName the new host name of this node
     * @param newPort the new port of this node
     */
    public void updateAddress(String nodeName,
                              String newHostName,
                              int newPort) {
        final RepNodeImpl node =
            checkValidity(nodeName, "Updating node's address");

        /* Check whether the node is still alive. */
        if (feederManager.getFeeder(nodeName) != null) {
            throw new ReplicaStateException
                ("Can't update the network address for a live node.");
        }

        /* Update the node information in the group database. */
        node.setHostName(newHostName);
        node.setPort(newPort);
        node.setQuorumAck(false);
        repGroupDB.updateMember(node, true);
    }

    /**
     * Transfer the master role to one of the specified replicas.
     * <p>
     * We delegate most of the real work to an instance of the {@link
     * MasterTransfer} class.  Here, after some simple initial validity
     * checking, we're concerned with coordinating the potential for multiple
     * overlapping Master Transfer operation attempts.  The possible outcomes
     * are:
     * <ol>
     * <li>complete success ({@code done == true})
     * <ul>
     * <li>
     * don't unblock txns here; that'll happen automatically as part of the
     * usual handling when the environment transitions from {@literal
     * master->replica} state.
     * <li>
     * don't clear xfrInProgress, because we don't want to allow another
     * attempt to supersede
     * </ul>
     * <li>timeout before establishing a winner (no superseder)
     * <ul>
     * <li>unblock txns
     * <li>clear xfrInProgress
     * </ul>
     * <li>superseded (see {@link #setUpTransfer})
     * <ul>
     * <li>abort existing op (if permitted), unblock txns before unleashing the
     * new one
     * <li>replace xfrInProgress
     * </ul>
     * <li>env is closed (or invalidated because of an error) during the
     * operation
     * <ul>
     * <li>release the block
     * <li>leave xfrInProgress as is.
     * </ul>
     * </ol>
     *
     * @param replicas candidate targets for new master role
     * @param timeout time limit, in msec
     * @param force whether to replace any existing, in-progress
     * transfer operation
     * @param reason why the master transfer is being performed
     */
    public String transferMaster(Set<String> replicas,
                                 long timeout,
                                 boolean force,
                                 String reason) {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException
                ("Parameter 'replicas' cannot be null or empty");
        }
        if (!nodeState.getRepEnvState().isMaster()) {
            throw new IllegalStateException("Not currently master");
        }
        if (replicas.contains(getNodeName())) {

            /*
             * The local node is on the list of candidate new masters, and
             * we're already master: the operation is trivially satisfied.
             */
            return getNodeName();
        }
        for (String rep : replicas) {
            RepNodeImpl node = group.getNode(rep);
            if (node == null || node.isRemoved()) {
                throw new IllegalArgumentException
                    ("Node '" + rep +
                     "' is not currently an active member of the group");
            } else if (!node.getType().isElectable()) {
                throw new IllegalArgumentException
                    ("Node '" + rep +
                     "' must have node type ELECTABLE, but had type " +
                     node.getType());
            }
        }

        MasterTransfer xfr = setUpTransfer(replicas, timeout, force, reason);
        boolean done = false;
        try {
            nMasterTransfers.increment();
            String winner = xfr.transfer();
            nMasterTransfersSuccess.increment();
            done = true;
            xfr.logMasterTransferEnd(true, null);
            return winner;
        } catch (Exception e) {
            nMasterTransfersFailure.increment();
            xfr.logMasterTransferEnd(false, e.getMessage());
            throw e;
        } finally {
            synchronized (this) {
                if (xfrInProgress == xfr && !done) {
                    xfrInProgress = null;
                }
            }
        }
    }

    /**
     * Sets up a Master Transfer operation, ensuring that only one operation
     * can be in progress at a time.
     */
    synchronized private MasterTransfer setUpTransfer(Set<String> replicas,
                                                      long timeout,
                                                      boolean force,
                                                      String reason) {
        boolean reject = false; // initial guess, refine below if nec.
        if (xfrInProgress != null) {
            reject = true;      // next best guess, refine below again if nec.

            /*
             * If the new operation is "forcing", see if we can abort the
             * existing one.
             */
            if (force &&
                xfrInProgress.abort
                (new MasterTransferFailureException("superseded"))) {
                reject = false;
                xfrInProgress.logMasterTransferEnd(false,
                    "a new master transfer is starting.");


                repImpl.unblockTxnCompletion();
            }
        }
        if (reject) {
            throw new MasterTransferFailureException
                ("another Master Transfer (started at " +
                 new Date(xfrInProgress.getStartTime()) +
                 ") is already in progress");
        }
        xfrInProgress = new MasterTransfer(replicas, timeout, reason, this);
        return xfrInProgress;
    }

    public MasterTransfer getActiveTransfer() {
        return xfrInProgress;
    }

    /**
     * Called by the RepNode when a transition to replica status has completely
     * finished.
     */
    public synchronized void clearActiveTransfer() {
        xfrInProgress = null;
    }

    /**
     * Performs some basic validity checking, common code for some
     * Group Membership operations.
     *
     * @param nodeName name of a replica node on which an operation is
     * to be performed
     * @param actionName textual description of the operation (for
     * exception message)
     * @return the named node
     */
    private RepNodeImpl checkValidity(String nodeName, String actionName)
        throws MemberNotFoundException {

        if (!nodeState.getRepEnvState().isMaster()) {
            throw EnvironmentFailureException.unexpectedState
                ("Not currently a master. " + actionName + " must be " +
                 "invoked on the node that's currently the master.");
        }

        final RepNodeImpl node = group.getNode(nodeName);
        if (node == null) {
            throw new MemberNotFoundException("Node:" + nodeName +
                                              "is not a member of the group:" +
                                              group.getName());
        }

        if (node.isRemoved() && node.isQuorumAck()) {
            throw new MemberNotFoundException("Node:" + nodeName +
                                              "is not currently a member of " +
                                              "the group:" + group.getName() +
                                              " It had been removed.");
        }

        /* Check if the node is the master itself. */
        if (nodeName.equals(getNodeName())) {
            throw new MasterStateException(getRepImpl().
                                           getStateChangeEvent());
        }

        return node;
    }

    /**
     * Restores the election state when a node first starts up.
     *
     * <p>
     * We must ensure the invariant that a node does not ack a lower term entry
     * when it has promised a higher term proposal even across the node restart
     * with the possibility of disk files lost. This is managed by
     * ElectionStateContinuation. There are complications, however, due to
     * whether this is an only node, a new node, an existing one, etc. which
     * needs to be queried for its own states or from other nodes. These
     * situations are handled here.
     *
     * <p>
     * Nodes that need a election state barrier cannot directly find and
     * connect to a master since the existing master might be of a lower term.
     * Instead an election is needed. Nodes that do not need a barrier can be
     * optimized to find the master directly. Such optimization is also
     * included in this method.
     */
    private void restoreElectionState() throws DatabaseException {
        if (restoreWithGroupReset()) {
            return;
        }

        refreshCachedGroup();
        LoggerUtils.info(logger, repImpl, String.format(
            "Current group size: %s", group.getElectableGroupSize()));

        ensureNodeNotRemoved();

        if (restoreWithNonElectableNode()) {
            return;
        }
        if (restoreWithStartupNode()) {
            return;
        }
        if (restoreWithNewElectableNode()) {
            return;
        }
        restoreWithExistingElectableNode();
    }

    /**
     * Handles group reset for election state restore and returns {@code true}
     * if that is the case.
     */
    private boolean restoreWithGroupReset() {
        if (!repImpl.getConfigManager().
            getBoolean(RepParams.RESET_REP_GROUP)) {
            return false;
        }
        /*
         * Invoked by DbResetRepGroup utility. No barrier needed since we
         * are the only node.
         */
        electionStateContinuation.init(false);
        reinitSelfElect();
        return true;
    }

    /**
     * Ensures node not removed.
     */
    private void ensureNodeNotRemoved() {
        final RepNodeImpl thisNode = group.getNode(nameIdPair.getName());
        if ((thisNode != null) && thisNode.isRemoved()) {
            throw EnvironmentFailureException.unexpectedState
                ("Node: " + nameIdPair.getName() +
                 " was previously removed.");
        }
    }

    /**
     * Handles non-electable node for election state restore and returns {@code
     * true} if that is the case.
     */
    private boolean restoreWithNonElectableNode() throws DatabaseException {

        if (nodeType.isElectable()) {
            return false;
        }
        /*
         * No barrier needed for non-electable nodes. Only electable nodes can
         * make promise.
         */
        electionStateContinuation.init(false);
        /* Finds the master. */
        findMaster();
        return true;
    }

    /**
     * Finds the master.
     */
    private void findMaster() throws DatabaseException {
        if (group.hasUnknownUUID()) {
            try {
                findMasterOrNotifyNetworkRestore();
            } catch (InterruptedException e) {
                throw EnvironmentFailureException.unexpectedException(e);
            }
        } else {
            findMasterWithLearners();
        }
    }

    private void findMasterWithLearners() throws DatabaseException {
        elections.getLearner().queryForMaster(getHelpers());
    }

    /**
     * Handles the single startup node for election state restore and returns
     * {@code true} if that is the case.
     */
    private boolean restoreWithStartupNode() {
        assert nodeType.isElectable();
        final RepNodeImpl thisNode = group.getNode(nameIdPair.getName());
        final Set<InetSocketAddress> helperSockets = repImpl.getHelperSockets();
        final boolean isStartupNode =
            (thisNode == null)
            && group.hasUnknownUUID()
            && (group.getElectableGroupSize() == 0)
            && (helperSockets.size() == 1)
            && (serviceDispatcher.getSocketAddress()
                .equals(helperSockets.iterator().next()));
        if (!isStartupNode) {
            return false;
        }
        LoggerUtils.info(logger, repImpl, String.format(
            "The very first group start up with node %s", nameIdPair));
        /* No barrier for the startup node.  */
        electionStateContinuation.init(false);
        /* Directly select itself for the master. */
        selfElect();
        elections.updateRepGroup(group);
        return true;
    }

    /**
     * Handles a new node for election state restore and returns {@code true}
     * if that is the case.
     */
    private boolean restoreWithNewElectableNode() {
        assert nodeType.isElectable();
        final RepNodeImpl thisNode = group.getNode(nameIdPair.getName());
        if (!((thisNode == null) && group.hasUnknownUUID())) {
            return false;
        }

        try {
            /* First try to see if we need to do a network restore. */
            findMasterOrNotifyNetworkRestore();
            /* No need to do network restore. Updates the group and node Id. */
            updateGroupAndNodeInfo();
            /*
             * Node ID still not set meaning no info present, i.e., truly a new
             * node.
             */
            if (nameIdPair.hasNullId()) {
                /* No barrier.  */
                electionStateContinuation.init(false);
                /*
                 * Can connect to the master directly. Should already have one,
                 * but check again just in case.
                 */
                if (masterStatus.getGroupMasterNameId().hasNullId()) {
                    findMaster();
                }
                return true;
            }
        } catch (InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
        return false;
    }

    /**
     * Queries the helpers to obtain the group info and updates the group and
     * node ID.
     *
     * This method is called because we do not have environment log files to
     * initialize ourselves and we have decided that a network restore is not
     * necessary. We cannot directly connect to the master, however, because it
     * might be in a lower term. Yet we do not have the node ID and the
     * electable members we needed for election. Hence query and update.
     *
     * TODO: Currently, we query all the helpers and trust the information as
     * long as we obtain one RepGroupImpl. This decision needs refinement. That
     * is, we need to deal with the inconsistency if there is any among the
     * helpers.  Membership change would be the cause of such inconsistency. We
     * need some way to tell if the obtained group info is the "latest".
     */
    private void updateGroupAndNodeInfo() throws InterruptedException {
        assert group.hasUnknownUUID();

        final RepGroupProtocol protocol = getRepGroupProtocol();
        while (true) {

            final Set<InetSocketAddress> helpers = getHelpers();
            /*
             * For each attempt, we iterate through the helpers and obtain at
             * most one group info. We retry the attempts until we obtain at
             * least one group info.
             */
            for (InetSocketAddress helper : helpers) {
                if (isShutdownOrInvalid()) {
                    throw new InterruptedException(
                        "RepNode shuts down during query about itself");
                }

                final GroupResponse groupResp =
                    queryGroupFromHelper(helper, protocol);
                if (groupResp == null) {
                    continue;
                }
                /* Obtained a valid group info. */
                final RepGroupImpl groupInfo = groupResp.getGroup();
                /*
                 * Updates the group membership so that the election can be
                 * executed on the correct set of nodes.
                 */
                group.includeElectableMembers(groupInfo);
                /* Sets the id. */
                RepNodeImpl me = groupInfo.getNode(nameIdPair.getName());
                if (me == null) {
                    LoggerUtils.info(logger, repImpl, String.format(
                        "Node %s does not have any info " +
                        "present in %s from helper %s",
                        nameIdPair, groupInfo, helper));
                    return;
                }
                if (me.isRemoved()) {
                    throw EnvironmentFailureException
                        .unexpectedState(String.format(
                            "Node %s was previously removed " +
                            "according to helper %s",
                            nameIdPair, helper));
                }
                final int id = me.getNodeId();
                LoggerUtils.info(logger, repImpl, String.format(
                    "Node %s is an existing node with id %s", nameIdPair, id));
                nameIdPair.setId(id);
                return;
            }
            Thread.sleep(GROUP_QUERY_RETRY_MS);
        }
    }

    /**
     * Obtains helper sockets.
     */
    private Set<InetSocketAddress> getHelpers() {
        final Set<InetSocketAddress> helpers =
            new HashSet<>(repImpl.getHelperSockets());
        helpers.addAll(group.getAllHelperSockets());
        checkLoopbackAddresses(helpers);
        return helpers;
    }

    /**
     * Returns the rep group protocol.
     */
    private RepGroupProtocol getRepGroupProtocol() {

        return new RepGroupProtocol(
            group.getName(), nameIdPair, repImpl,
            repImpl.getChannelFactory());
    }

    /**
     * Queries the group info and returns {@code null} if there are errors.
     */
    private GroupResponse queryGroupFromHelper(InetSocketAddress helper,
                                               RepGroupProtocol protocol) {
        final MessageExchange msg = protocol.new MessageExchange(
            helper, GroupService.SERVICE_NAME,
            protocol.new GroupRequest());

        /*
         * Just as we did in the queryForMaster() case, quietly ignore any
         * unsurprising response error or socket exceptions; we'll retry
         * later if we end up not finding any Network Restore suppliers.
         */
        msg.run();
        ResponseMessage response = msg.getResponseMessage();
        if (response == null ||
            protocol.RGFAIL_RESP.equals(response.getOp())) {
            return null;
        } else if (!protocol.GROUP_RESP.equals(response.getOp())) {
            LoggerUtils.warning(logger, repImpl,
                                "Expected GROUP_RESP, got " +
                                response.getOp() + ": " + response);
            return null;
        }
        return (GroupResponse) response;
    }

    /**
     * Handles an existing node for election state restore.
     */
    private void restoreWithExistingElectableNode() {
        assert nodeType.isElectable();

        /*
         * Initialize with barrier. This must be done after we find the master
         * since if the master is of an older term, it will be rejected.
         */
        electionStateContinuation.init(true);
        /*
         * Updates the allowed term before wait.
         */
        nodeState.updateAllowedNextTerm(
            electionStateContinuation.getPromisedTerm());
        /* Waits past the barrier */
        try {
            electionStateContinuation.waitPastBarrier();
        } catch (InterruptedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
        /*
         * Clears the master status and find the current master (again). We
         * must ensure that the master we proceed to connect to is of a higher
         * term than the barrier. The previously learned master may not satisfy
         * that. Simply clear and learn again. And if the master is still of a
         * lower term, clear again.  It is OK or even intended that we continue
         * without a master if the current is of a lower term since the RepNode
         * loop will initiate an election.
         */
        masterStatus.clearGroupMaster();
        findMasterWithLearners();
        if (masterStatus.getGroupMasterTerm()
            < nodeState.getAllowedNextTerm()) {
            masterStatus.clearGroupMaster();
        }
    }

    /**
     * This method enforces the requirement that all addresses within a
     * replication group, must be loopback addresses or they must all be
     * non-local ip addresses. Mixing them means that the node with a loopback
     * address cannot be contacted by a different node.  Addresses specified by
     * hostnames that currently have no DNS entries are assumed to not be
     * loopback addresses.
     *
     * @param helperSockets the helper nodes used by this node when contacting
     * the master.
     */
    private void checkLoopbackAddresses(Set<InetSocketAddress> helperSockets) {

        final InetAddress myAddress = getSocket().getAddress();
        final boolean isLoopback = myAddress.isLoopbackAddress();

        for (InetSocketAddress socketAddress : helperSockets) {
            final InetAddress nodeAddress = socketAddress.getAddress();

            /*
             * If the node address was specified with a hostname that does not,
             * at least currently, have a DNS entry, then the address will be
             * null.  We can safely assume this will not happen for loopback
             * addresses, whose host names and addresses are both fixed.
             */
            final boolean nodeAddressIsLoopback =
                (nodeAddress != null) && nodeAddress.isLoopbackAddress();

            if (nodeAddressIsLoopback == isLoopback) {
                continue;
            }
            String message = getSocket() +
                " the address associated with this node, " +
                (isLoopback? "is " : "is not ") +  "a loopback address." +
                " It conflicts with an existing use, by a different node " +
                " of the address:" +
                socketAddress +
                (!isLoopback ? " which is a loopback address." :
                 " which is not a loopback address.") +
                " Such mixing of addresses within a group is not allowed, " +
                "since the nodes will not be able to communicate with " +
                "each other.";
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Communicates with existing nodes in the group in order to figure out how
     * to start up, in the case where the local node does not appear to be in
     * the (local copy of the) GroupDB, typically because the node is starting
     * up with an empty env directory.  It could be that this is a new node
     * (never before been part of the group).  Or it could be a pre-existing
     * group member that has lost its env dir contents and wants to be restored
     * via a Network Restore operation.
     * <p>
     * We first try to find a currently running master node.  (An authoritative
     * master can easily handle either of the above-mentioned situations.)  If
     * we can't find a master, we look for other running nodes that may know of
     * us (by asking them for their Group information).
     * <p>
     * We query the designated helpers and all known learners.  The helpers are
     * the ones that were identified via the node's configuration, while the
     * learners are the ones currently in the member database.  We use both in
     * order to cast the widest possible net.
     * <p>
     * Returns normally when the master is found.
     *
     * @throws InterruptedException if the current thread is interrupted,
     *         typically due to a shutdown
     * @throws InsufficientLogException if the environment requires a network
     *         restore
     * @see #findRestoreSuppliers
     */
    private void findMasterOrNotifyNetworkRestore()
        throws InterruptedException {

        final Set<InetSocketAddress> helpers = getHelpers();
        if (helpers.isEmpty()) {
            throw EnvironmentFailureException.unexpectedState
                ("Need a helper to add a new node into the group");
        }

        NameIdPair groupMasterNameId;
        while (true) {
            assert TestHookExecute.doHookIfSet(
                queryGroupForMembershipBeforeQueryForMaster,
                nameIdPair.getName());
            elections.getLearner().queryForMaster(helpers);
            if (isShutdownOrInvalid()) {
                throw new InterruptedException("Node is shutdown or invalid");
            }
            groupMasterNameId = masterStatus.getGroupMasterNameId();
            if (!groupMasterNameId.hasNullId()) {
                /* A new, or pre-query, group master. */
                if (nameIdPair.hasNullId() &&
                    groupMasterNameId.getName().equals(nameIdPair.getName())) {

                    /*
                     * Residual obsolete information in replicas, ignore it.
                     * Can't be master if we don't know our own id, but some
                     * other node does! This state means that the node was a
                     * master in the recent past, but has had its environment
                     * deleted since that time.
                     */
                    Thread.sleep(MASTER_QUERY_RETRY_MS);
                    continue;
                }

                if (checkGroupMasterIsAlive(groupMasterNameId)) {
                    /* Use the current group master if it's alive. */
                    break;
                }
            }

            /*
             * If there's no master, or the last known master cannot be
             * reached, see if anyone thinks we're actually already in the
             * group, and could supply us with a Network Restore. (Remember,
             * we're here only if we didn't find ourselves in the local
             * GroupDB. So we could be in a group restore from backup
             * situation.)
             */
            if (!(nodeType.isSecondary() || nodeType.isExternal())) {
                findRestoreSuppliers(helpers);
            }

            assert TestHookExecute.doHookIfSet(
                queryGroupForMembershipBeforeSleepHook, nameIdPair.getName());

            /*
             * The node could have been shutdown or invalidated while we were
             * looking for restore suppliers
             */
            if (isShutdownOrInvalid()) {
                throw new InterruptedException("Node is shutdown or invalid");
            }

            Thread.sleep(MASTER_QUERY_RETRY_MS);
        }
        LoggerUtils.info(logger, repImpl, "New node " + nameIdPair.getName() +
                         " located master: " + groupMasterNameId);
    }

    /**
     * Check that the master found by querying other group nodes is indeed
     * alive and that we are not dealing with obsolete cached information.
     *
     * @return true if the master node could be contacted and was truly alive
     *
     * TODO: handle protocol version mismatch here and in DbPing, also
     * consolidate code so that a single copy is shared.
     */
    private boolean checkGroupMasterIsAlive(NameIdPair groupMasterNameId) {

        DataChannel channel = null;

        try {
            final InetSocketAddress masterSocket =
                masterStatus.getGroupMaster();


            /* Build the connection. Set the parameter connectTimeout.*/
            channel = repImpl.getChannelFactory().
                connect(masterSocket,
                        repImpl.getHostAddress(),
                        new ConnectOptions().
                        setTcpNoDelay(true).
                        setOpenTimeout(5000).
                        setReadTimeout(5000));
            final BinaryNodeStateProtocol protocol =
                new BinaryNodeStateProtocol(channel, null);
            ServiceDispatcher.doServiceHandshake
                (channel, BinaryNodeStateService.SERVICE_NAME);

            /* Send a NodeState request to the node. */
            protocol.write
                (protocol.new
                    BinaryNodeStateRequest(groupMasterNameId.getName(),
                                           group.getName()),
                 channel);

            /* Get the response and return the NodeState. */
            BinaryNodeStateResponse response =
                protocol.read(channel, BinaryNodeStateResponse.class);

            ReplicatedEnvironment.State state = response.getNodeState();
           return (state != null) && state.isMaster();
        } catch (Exception e) {
            LoggerUtils.info(logger, repImpl,
                             "Queried master:" + groupMasterNameId +
                             " unavailable. Reason:" + e);
            return false;
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ioe) {
                    /* Ignore it */
                }
            }
        }
    }

    /**
     * Sets up a Network Restore, as part of the process of restoring an entire
     * group from backup, by producing an appropriate {@code
     * InsufficientLogException} if possible.
     * <p>
     * Queries each of the supplied helper hosts for their notion of the group
     * make-up.  If any of them consider us to be already in the group, then
     * instead of joining the group as a new node we ought to try a Network
     * Restore; and the node(s) that do already know of us are the suitable
     * suppliers for it.
     *
     * @throws InsufficientLogException in the successful case, if one or more
     * suitable suppliers for a Network Restore can be found; otherwise just
     * returns.
     *
     * @throws InterruptedException if the node was shutdown or invalidated
     * while we were looking for a network restore supplier
     */
    public void findRestoreSuppliers(Set<InetSocketAddress> helpers)
        throws InterruptedException {

        final Set<ReplicationNode> suppliers = new HashSet<>();
        RepGroupProtocol protocol = getRepGroupProtocol();

        for (InetSocketAddress helper : helpers) {

            assert TestHookExecute.doHookIfSet(
                beforeFindRestoreSupplierHook, nameIdPair.getName());

            /*
             * The node could have been shutdown or invalidated while we were
             * looking for a network restore supplier [#25314]
             */
            if (isShutdownOrInvalid()) {
                throw new InterruptedException("Node is shutdown or invalid");
            }

            final GroupResponse groupResp =
                queryGroupFromHelper(helper, protocol);
            if (groupResp == null) {
                continue;
            }

            /*
             * If the response from the remote node shows that I am already a
             * member of the group, add the node to the list of nodes that will
             * serve the Network Restore.
             *
             * TODO: Perhaps we should ensure that the supplier has a recent
             * enough term. If we are restoring from a lower term replica when
             * higher terms are available, we might lose some durable entries.
             */
            RepGroupImpl groupInfo = groupResp.getGroup();
            RepNodeImpl me = groupInfo.getNode(nameIdPair.getName());
            if (me == null || me.isRemoved() || !me.isQuorumAck()) {
                continue;
            }

            ReplicationNode supplier = groupInfo.getMember(helper);
            if (supplier != null) {
                suppliers.add(supplier);
            }
        }

        if (suppliers.isEmpty()) {
            return;
        }

        throw new InsufficientLogException(this, suppliers);
    }

    /**
     * Elects this node as the master. The operation is only valid when the
     * group consists of just this node, and when this is an ELECTABLE node.
     *
     * @throws DatabaseException
     * @throws IllegalStateException if the node type is not ELECTABLE
     */
    private void selfElect() throws DatabaseException {

        if (!nodeType.isElectable()) {
            throw new IllegalStateException(
                "Cannot elect node " + nameIdPair.getName() +
                " as master because its node type, " + nodeType +
                ", is not ELECTABLE");
        }
        nameIdPair.setId(RepGroupImpl.getFirstNodeId());

        /* Master by default of a nascent group. */
        final Proposal proposal = elections.getProposer().nextProposal();
        final Learner learner = elections.getLearner();
        learner.processResult(proposal, suggestionGenerator.get(proposal));
        LoggerUtils.info(logger, repImpl, "Nascent group. " +
                         nameIdPair.getName() +
                         " is master by virtue of being the first node.");
        masterStatus.sync();
        try {
            nodeState.changeAndNotify(MASTER,
                                      masterStatus.getNodeMasterNameId(),
                                      proposal.getTimeMs());
        } catch (MasterObsoleteException e) {
            throw new IllegalStateException(
                "Unexpected master obsolete for a single node", e);
        }
        prevTermEndVLSN = repImpl.getVLSNIndex().initAsMaster();
        /*
         * Start it off as this value. It will be rapidly updated, as
         * transactions are committed.
         */
        updateDTVLSNMax(UNINITIALIZED_VLSN);
        repGroupDB.addFirstNode();
        refreshCachedGroup();
        /* Unsync so that the run loop does not call for an election. */
        masterStatus.unSync();
    }

    /**
     * Establishes this node as the master, after re-initializing the group
     * with this as the sole node in the group. This method is used solely
     * as part of the DbResetRepGroup utility.
     *
     * @throws IllegalStateException if the node type is not ELECTABLE
     */
    private void reinitSelfElect() {
        if (!nodeType.isElectable()) {
            throw new IllegalStateException(
                "Cannot elect node " + nameIdPair.getName() +
                " as master because its node type, " + nodeType +
                ", is not ELECTABLE");
        }

        /* Establish an empty group so transaction commits can proceed. */
        setGroup(repGroupDB.emptyGroup);
        /*
         * We don't have a node id at this point, simply use 1 since we know
         * it's valid. It will subsequently be set to the next node id in
         * sequence.
         */
        nameIdPair.setId(RepGroupImpl.getFirstNodeId());
        LoggerUtils.info(logger, repImpl, "Reinitializing group to node " +
                         nameIdPair);

        /*
         * Unilaterally transition the nodeState to Master, so that write
         * transactions needed to reset the group and establish this node can
         * be issued against the environment.
         */

        final Consumer<MasterObsoleteException> moeHandler =
            (moe) -> {
                /* not possible */
                throw new IllegalStateException( "Unexpected exception", moe);
            };
        nodeState.changeAndNotify(MASTER, nameIdPair,
                                  TimeSupplier.currentTimeMillis(),
                                  moeHandler);
        prevTermEndVLSN = repImpl.getVLSNIndex().initAsMaster();

        final int nodeId = nameIdPair.getId();
        for (ReplayTxn replayTxn :
             repImpl.getTxnManager().getTxns(ReplayTxn.class)) {
            LoggerUtils.info(logger, repImpl,
                             "Aborting incomplete replay txn:" +
                              nameIdPair + " as part of group reset");
            /* The DTVLSN and VLSN will be corrected when it's written to the
             * log */
            final TxnAbort dummyTxnEnd =
                new TxnAbort(replayTxn.getId(), replayTxn.getLastLsn(),
                             () -> new MasterIdTerm(
                                 nodeId,
                                 getNodeState().getMasterIdTerm().term),
                             NULL_VLSN);
            replayTxn.abort(ReplicationContext.MASTER, dummyTxnEnd);
        }

        /*
         * Start using new log files. The file ensures that we can safely
         * truncate the past VLSNs.
         */
        repImpl.forceLogFileFlip();

        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setForce(true);

        /*
         * The checkpoint ensures that we do not have to replay VLSNs from the
         * prior group and that we have a complete VLSN index on disk.
         */
        repImpl.invokeCheckpoint(ckptConfig, "Reinit of RepGroup");
        long lastOldVLSN = repImpl.getVLSNIndex().getRange().getLast();

        /* Now create the new rep group on disk. */
        repGroupDB.reinitFirstNode();
        refreshCachedGroup();

        long lastOldFile =
            repImpl.getVLSNIndex().getLTEFileNumber(lastOldVLSN);

        /*
         * Discard the VLSN index covering the pre group reset VLSNS, to ensure
         * that the pre reset part of the log is never replayed. We don't want
         * to replay this part of the log, since it contains references to
         * repnodes via node ids that are no longer part of the reset rep
         * group. Note that we do not reuse rep node ids, that is, rep node id
         * sequence continues across the reset operation and is not itself
         * reset. Nodes joining the new group will need to do a network restore
         * when they join the group.
         *
         * Don't perform the truncation if RESET_REP_GROUP_RETAIN_UUID is true.
         * In that case, we are only removing the rep group members, but
         * retaining the remaining information, because we will be restarting
         * the rep group in place with an old secondary acting as an electable
         * node.
         */
        final boolean retainUUID =
            getConfigManager().getBoolean(RESET_REP_GROUP_RETAIN_UUID);
        if (!retainUUID) {
            repImpl.getVLSNIndex().truncateFromHead(lastOldVLSN, lastOldFile);
        }

        /* Unsync so that the run loop does not call for an election. */
        masterStatus.unSync();
    }


    /**
     * When a disk limit is violated, the node state will transition to
     * UNKNOWN and wait for disk to become available again before it
     * transitions to a Replica or Master State. This method will not exit
     * until one of the following occurs:
     *
     * 1. The disk limit violation is cleared.
     * 2. The node is shutdown or invalidated.
     * 3. The thread is interrupted, in which case InterruptedException is
     *    thrown.
     */
    private void waitWhileDiskLimitViolation()
        throws InterruptedException {

        LoggerUtils.info(logger, repImpl,
            "Node waiting for disk space to become available. " +
            "Disk limit violation: " + getRepImpl().getDiskLimitViolation());

        while (getRepImpl().getDiskLimitViolation() != null) {

            if (isShutdownOrInvalid()) {
                return;
            }

            Thread.sleep(1000);
        }

        LoggerUtils.info(logger, repImpl, "Disk limit violation cleared. " +
            getRepImpl().getCleaner().getDiskLimitMessage());
    }

    /**
     * The top level Master/Feeder or Replica loop in support of replication.
     * It's responsible for driving the node level state changes resulting
     * from elections initiated either by this node, or by other members of the
     * group.
     * <p>
     * The thread is terminated via an orderly shutdown initiated as a result
     * of an interrupt issued by the shutdown() method. Any exception that is
     * not handled by the run method itself is caught by the thread's uncaught
     * exception handler, and results in the RepImpl being made invalid.  In
     * that case, the application is responsible for closing the Replicated
     * Environment, which will provoke the shutdown.
     * <p>
     * Note: This method currently runs either the feeder loop or the replica
     * loop. With R to R support, it would be possible for a Replica to run
     * both. This will be a future feature.
     */
    @Override
    public void run() {
        /* Set to indicate an error-initiated shutdown. */
        Error repNodeError = null;
        try {
            LoggerUtils.info(logger, repImpl,
                             "Node " + nameIdPair.getName() + " started" +
                             (!nodeType.isElectable() ?
                              " as " + nodeType :
                              ""));
            while (!isShutdownOrInvalid()) {
                assert TestHookExecute.doHookIfSet(runLoopTestHook, this);
                if (nodeState.getRepEnvState() != UNKNOWN) {
                    /* Avoid unnecessary state changes. */
                    nodeState.changeToUnknownAndNotify();
                }

                if (getRepImpl().getDiskLimitViolation() != null) {
                    /*
                     * Progress is not possible while out of disk. So stay in
                     * the UNKNOWN state, participating in elections at
                     * election priority zero to help establish election quorum
                     * but avoid being elected master.
                     */
                    waitWhileDiskLimitViolation();
                }

                /*
                 * Initiate elections if we don't have a group master, or there
                 * is a master, but we were unable to use it.
                 */
                long electionDuration = Long.MAX_VALUE;
                if (masterStatus.getGroupMasterNameId().hasNullId() ||
                    masterStatus.inSync()) {

                    /*
                     * But we can't if we don't have our own node ID yet or if
                     * we are not ELECTABLE.
                     */
                    if (nameIdPair.hasNullId() || !nodeType.isElectable()) {
                        findMasterOrNotifyNetworkRestore();
                    } else {
                        electionDuration = TimeSupplier.currentTimeMillis();
                        elections.initiateElection(
                            group, electionQuorumPolicy);
                        electionDuration =
                            TimeSupplier.currentTimeMillis() - electionDuration;
                        /*
                         * Subsequent elections must always use a simple
                         * majority.
                         */
                        electionQuorumPolicy = QuorumPolicy.SIMPLE_MAJORITY;
                        assert TestHookExecute.doHookIfSet(
                            runLoopAfterElectionTestHook, this);
                    }
                    /* In case elections were shut down. */
                    if (isShutdownOrInvalid()) {
                        return;
                    }
                }

                /* Start syncing this node to the new group master */
                masterStatus.sync();

                if (masterStatus.getNodeMaster() == null) {
                    /*
                     * After the election, we still do not have a mater
                     * address. This can occur for the rare case when the local
                     * learner does not learn the value when the proposer is
                     * done. See Elections.logIfElectionNotConcluded.
                     */
                    continue;
                }

                if (masterStatus.isNodeMaster()) {
                    prevTermEndVLSN =
                        repImpl.getVLSNIndex().initAsMaster();
                    /* Master is ready for business. */
                    try {
                        nodeState.changeAndNotify
                            (MASTER,
                             masterStatus.getNodeMasterNameId(),
                             masterStatus.getNodeMasterIdTerm().term);
                    } catch (MasterObsoleteException e) {
                        logMasterObsolete(e);
                        continue;
                    }
                    /*
                     * Do the master transition cleanup after the
                     * transition to master so that we can write abort
                     * log entries of inflight entries in the new term.
                     */
                    replica.masterTransitionCleanup();

                    /*
                     * Update the JE version information stored for the master
                     * in the RepGroupDB, if needed.
                     */
                    maybeUpdateMasterJEVersion();

                    final long minimum = elections.getMinElectionTime();
                    if (electionDuration < minimum) {
                        final long wait = minimum - electionDuration;
                        LoggerUtils.info(logger, repImpl,
                          "Election took less than " +
                          minimum + ", waiting " + wait +
                          " milliseconds to pad election time.");
                        Thread.sleep(wait);
                        elections.incrementElectionsDelayed();
                    }
                    feederManager.runFeeders();
                    prevTermEndVLSN = VLSN.NULL_VLSN;

                    /*
                     * At this point, the feeder manager has been shutdown.
                     * Re-initialize the VLSNIndex put latch mechanism, which
                     * is present on masters to maintain a tip cache of the
                     * last record on the replication stream, and by all
                     * nodes when doing checkpoint vlsn consistency waiting.
                     * Create a new feeder manager, should this node become a
                     * master later on.
                     * Set the node to UNKNOWN state right away, because the
                     * MasterTxn will use node state to prevent the advent of
                     * any replicated writes.  Once the VLSNIndex is
                     * initialized for replica state, the node will NPE if it
                     * attempts execute replicated writes.
                     */
                    nodeState.changeToUnknownAndNotify();
                    repImpl.getVLSNIndex().initAsReplica();
                    assert runConvertHooks();
                    feederManager = new FeederManager(this);
                } else {

                    /* Initialize the VLSN Counters meant for master and replica.
                     * Also remove any left over vlsnPutLatch.
                     */
                    repImpl.getVLSNIndex().initReplayVLSNCounter();
                    /*
                     * Replica will notify us when connection is successfully
                     * made, and Feeder handshake done, at which point we'll
                     * update nodeState.
                     */
                    replica.replicaTransitionCleanup();
                    replica.runReplicaLoop();
                }
            }
        } catch (InterruptedException e) {
            LoggerUtils.fine(logger, repImpl,
                             "RepNode main thread interrupted - " +
                             " forced shutdown.");
        } catch (GroupShutdownException e) {
            saveShutdownException(e);
            LoggerUtils.fine(logger, repImpl,
                             "RepNode main thread sees group shutdown - " + e);
        } catch (InsufficientLogException e) {
            saveShutdownException(e);
        } catch (RuntimeException e) {
            LoggerUtils.fine(logger, repImpl,
                             "RepNode main thread sees runtime ex - " + e);
            saveShutdownException(e);
            throw e;
        } catch (Error e) {
            LoggerUtils.fine(logger, repImpl, e +
                             " incurred during repnode loop");
            repNodeError = e;
            repImpl.invalidate(e);
        } finally {
            try {
                LoggerUtils.info(logger, repImpl,
                                 "RepNode main thread shutting down.");

                if (repNodeError != null) {
                    LoggerUtils.info(logger, repImpl,
                                     "Node state at shutdown:\n"+
                                     repImpl.dumpState());
                    throw repNodeError;
                }
                Throwable exception = getSavedShutdownException();

                if (exception == null) {
                    LoggerUtils.fine(logger, repImpl,
                                     "Node state at shutdown:\n"+
                                     repImpl.dumpState());
                } else {
                    LoggerUtils.info(logger, repImpl,
                                     "RepNode shutdown exception:\n" +
                                     exception.getMessage() +
                                     repImpl.dumpState());
                }

                try {
                    shutdown();
                } catch (DatabaseException e) {
                    RepUtils.chainExceptionCause(e, exception);
                    LoggerUtils.severe(logger, repImpl,
                                       "Unexpected exception during shutdown" +
                                       e +
                                       LoggerUtils.getStackTraceForSevereLog(e));
                    throw e;
                }
            } catch (InterruptedException e1) {
                // Ignore exceptions on exit
            }
            nodeState.changeToDetachedAndNotify();
            cleanup();
        }
    }

    /**
     * Logs the MasterObsoleteException.
     *
     * This exception is thrown from NodeState#changeAndNotify when it noticed
     * the term it is about to transit to is obsolete. The same exception is
     * also thrown from the replica loop but is already caught inside the
     * thread.
     *
     * We already made a check in the Learner.preLearnHook so that, in the
     * common case, the countDownLatch should not be triggered when a lower
     * term result is learned. There could be a race, however, that after the
     * preLearnHook is passed and before we transit to a new master, the
     * acceptor made a new promise. In this case, we simply initiate a new
     * election.  This is less optimal as we should not start a new election
     * disturbing the current one, but instead simply wait. Since it is not the
     * common case, I think it is OK.
     *
     * Note that the acceptor may still make new promise after we transit to or
     * connect to the master. This will be detected by the CommitFreezeLatch.
     */
    private void logMasterObsolete(MasterObsoleteException e) {
        LoggerUtils.info(logger, repImpl, String.format(
            "Learned master obsolete due to a race: %s", e));
    }

    /**
     * Update the information stored for the master in the RepGroupDB if
     * storing it is supported and the current version is different from the
     * recorded version.
     */
    private void maybeUpdateMasterJEVersion() {

        /* Check if storing JE version information is supported */
        if (group.getFormatVersion() < RepGroupImpl.FORMAT_VERSION_3) {
            return;
        }

        final JEVersion currentJEVersion = repImpl.getCurrentJEVersion();
        final RepNodeImpl node = group.getMember(nameIdPair.getName());

        if (currentJEVersion.equals(node.getJEVersion())) {
            return;
        }
        node.updateJEVersion(currentJEVersion);
        repGroupDB.updateMember(node, false);
    }

    void notifyReplicaConnected() throws MasterObsoleteException {
        nodeState.changeAndNotify(REPLICA,
                                  masterStatus.getNodeMasterNameId(),
                                  masterStatus.getNodeMasterIdTerm().term);
    }

    /**
     * Returns true if the node has been shutdown or if the underlying
     * environment has been invalidated. It's used as the basis for exiting
     * the FeederManager or the Replica.
     */
    public boolean isShutdownOrInvalid() {
        if (isShutdown()) {
            return true;
        }
        if (getRepImpl().wasInvalidated()) {
            saveShutdownException(getRepImpl().getInvalidatingException());
            return true;
        }
        return false;
    }

    /**
     * Used to shutdown all activity associated with this replication stream.
     * If method is invoked from different thread of control, it will wait
     * until the rep node thread exits. If it's from the same thread, it's the
     * caller's responsibility to exit the thread upon return from this method.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    public void shutdown()
        throws InterruptedException, DatabaseException {

        if (shutdownDone(logger)) {
            return;
        }

        LoggerUtils.info(logger, repImpl,
                         "Shutting down node " + nameIdPair +
                         " DTVLSN:" + getAnyDTVLSN());

        /* Stop accepting any new network requests. */
        serviceDispatcher.preShutdown();

        if (elections != null) {
            elections.shutdown();
        }

        /* Initiate the FeederManger soft shutdown if it's active. */
        feederManager.shutdownQueue();

        if ((getReplicaCloseCatchupMs() >= 0) &&
            (nodeState.getRepEnvState().isMaster())) {

            /*
             * A group shutdown. Shutting down the queue will cause the
             * FeederManager to shutdown its feeders and exit.
             */
            this.join();
        }

        /* Shutdown the replica, if it's active. */
        replica.shutdown();

        shutdownThread(logger);

        LoggerUtils.info(logger, repImpl,
                         "RepNode main thread: " + this.getName() + " exited.");
        /* Shut down all other services. */
        utilityServicesShutdown();

        /* Shutdown all the services before shutting down the dispatcher. */
        MasterTransfer mt = getActiveTransfer();
        if (mt != null) {
            Exception ex = getSavedShutdownException();
            if (ex == null) {
                ex = new MasterTransferFailureException("shutting down");
            }
            mt.abort(ex);
            mt.logMasterTransferEnd(false, "the RepNode is shutting down.");
        }
        serviceDispatcher.shutdown();
        LoggerUtils.info(logger, repImpl,
                         nameIdPair + " shutdown completed.");
        masterStatus.setGroupMaster(null, 0, NameIdPair.NULL, null);
        readyLatch.releaseAwait(getSavedShutdownException());

        /* Cancel the TimerTasks. */
        channelTimeoutTask.cancel();
        timer.cancel();
    }

    /**
     * Soft shutdown for the RepNode thread. Note that since the thread is
     * shared by the FeederManager and the Replica, the FeederManager or
     * Replica specific soft shutdown actions should already have been done
     * earlier.
     */
    @Override
    protected int initiateSoftShutdown() {
        return getThreadWaitInterval();
    }

    private void utilityServicesShutdown() {
        if (ldiff != null) {
            ldiff.shutdown();
        }

        if (logFeederManager != null) {
            logFeederManager.shutdown();
        }

        if (binaryNodeStateService != null) {
            binaryNodeStateService.shutdown();
        }

        if (nodeStateService != null) {
            serviceDispatcher.cancel(NodeStateService.SERVICE_NAME);
        }

        if (groupService != null) {
            serviceDispatcher.cancel(GroupService.SERVICE_NAME);
        }
    }

    /**
     * Must be invoked on the Master via the last open handle.
     *
     * Note that the method itself does not shutdown the group. It merely
     * sets replicaCloseCatchupMs, indicating that the ensuing handle close
     * should shutdown the Replicas. The actual coordination with the closing
     * of the handle is implemented by ReplicatedEnvironment.shutdownGroup().
     *
     * @see ReplicatedEnvironment#shutdownGroup(long, TimeUnit)
     */
    public void shutdownGroupOnClose(long timeoutMs)
        throws IllegalStateException {

        if (!nodeState.getRepEnvState().isMaster()) {
            throw new IllegalStateException
                ("Node state must be " + MASTER +
                 ", not " + nodeState.getRepEnvState());
        }
        replicaCloseCatchupMs = (timeoutMs < 0) ? 0 : timeoutMs;
    }

    /**
     * JoinGroup ensures that a RepNode is actively participating in a
     * replication group. It's invoked each time a replicated environment
     * handle is created.
     *
     * If the node is already participating in a replication group, because
     * it's not the first handle to the environment, it will return without
     * having to wait. Otherwise it will wait until a master is elected and
     * this node is active, either as a Master, or as a Replica.
     *
     * If the node joins as a replica, it will wait further until it has become
     * sufficiently consistent as defined by its consistency argument. By
     * default it uses PointConsistencyPolicy to ensure that it is at least as
     * consistent as the master as of the time the handle was opened.
     *
     * A node can also join in the Unknown state if it has been configured to
     * do so via ENV_UNKNOWN_STATE_TIMEOUT.
     *
     * @throws UnknownMasterException If a master cannot be established within
     * ENV_SETUP_TIMEOUT, unless ENV_UNKNOWN_STATE_TIMEOUT has
     * been set to allow the creation of a handle while in the UNKNOWN state.
     *
     * @return MASTER, REPLICA, or UNKNOWN (if ENV_UNKNOWN_STATE_TIMEOUT
     * is set)
     */
    public ReplicatedEnvironment.State
        joinGroup(ReplicaConsistencyPolicy consistency,
                  QuorumPolicy initialElectionPolicy)
        throws ReplicaConsistencyException, DatabaseException {

        final JoinGroupTimeouts timeouts =
                new JoinGroupTimeouts(getConfigManager());

        startup(initialElectionPolicy);
        LoggerUtils.finest(logger, repImpl, "joinGroup " +
                           nodeState.getRepEnvState());

        DatabaseException exitException = null;
        int retries = 0;
        repImpl.getStartupTracker().start(Phase.BECOME_CONSISTENT);
        repImpl.getStartupTracker().setProgress
           (RecoveryProgress.BECOME_CONSISTENT);
        try {
            for (retries = 0; retries < JOIN_RETRIES; retries++ ) {
                try {
                    /* Wait for Feeder/Replica to be fully initialized. */
                    boolean done = getReadyLatch().awaitOrException
                        (timeouts.getTimeout(), TimeUnit.MILLISECONDS);

                    /*
                     * Save the state, and use it from this point forward,
                     * since the node's state may change again.
                     */
                    final ReplicatedEnvironment.State finalState =
                        nodeState.getRepEnvState();
                    if (!done) {

                        /* An election or setup, timeout. */
                        if (finalState.isReplica()) {
                            if (timeouts.timeoutIsForUnknownState()) {

                                /*
                                 * Replica syncing up; move onwards to the
                                 * setup timeout and continue with the syncup.
                                 */
                                timeouts.setSetupTimeout();
                                continue;
                            }
                            throw new ReplicaConsistencyException
                                (String.format("Setup time exceeded %,d ms",
                                               timeouts.getSetupTimeout()),
                                               null);
                        }

                        if (finalState.isUnknown() &&
                            timeouts.timeoutIsForUnknownState()) {
                            joinTimeMs = TimeSupplier.currentTimeMillis();
                            return UNKNOWN;
                        }
                        break;
                    }

                    switch (finalState) {
                        case UNKNOWN:

                            /*
                             * State flipped between release of ready latch and
                             * nodeState.getRepEnvState() above; retry for a
                             * Master/Replica state.
                             */
                            continue;

                        case REPLICA:
                            joinAsReplica(consistency);
                            break;

                        case MASTER:
                            LoggerUtils.info(logger, repImpl,
                                             "Joining group as master");
                            break;

                        case DETACHED:
                            throw EnvironmentFailureException.
                                unexpectedState("Node in DETACHED state " +
                                                "while joining group.");
                    }
                    joinTimeMs = TimeSupplier.currentTimeMillis();
                    return finalState;
                } catch (InterruptedException e) {
                    throw EnvironmentFailureException.unexpectedException(e);
                } catch (MasterStateException e) {
                    /* Transition to master while establishing consistency. */
                    LoggerUtils.warning(logger, repImpl,
                                        "Join retry due to master transition: "
                                        + e.getMessage());
                    continue;
                } catch (RestartRequiredException e) {
                    LoggerUtils.warning(logger, repImpl,
                                        "Environment needs to be restarted: " +
                                        e.getMessage());
                    throw e;
                } catch (DatabaseException e) {
                    Throwable cause = e.getCause();
                    if ((cause != null) &&
                        (cause.getClass() ==
                         ReplicaConnectRetryException.class)) {

                        /*
                         * The master may have changed. Retry if there is time
                         * left to do so. It may result in a new master.
                         */
                        exitException = e;
                        if (timeouts.getTimeout() > 0) {
                            LoggerUtils.warning(logger, repImpl,
                                                "Join retry due to exception: "
                                                + cause.getMessage());
                            continue;
                        }
                    }
                    throw e;
                }
            }
        } finally {
            repImpl.getStartupTracker().stop(Phase.BECOME_CONSISTENT);
        }

        /* Timed out or exceeded retries. */
        if (exitException != null) {
            LoggerUtils.warning(logger, repImpl, "Exiting joinGroup after " +
                                retries + " retries." + exitException);
            throw exitException;
        }
        throw new UnknownMasterException(String.format(
            "Master not found after setup timeout %s ms, " +
            "current state is %s",
            timeouts.getSetupTimeout(),
            repImpl.getStateChangeEvent().getState()));
    }

    /**
     * Join the group as a Replica ensuring that the node is sufficiently
     * consistent as defined by its consistency policy.
     *
     * @param consistency the consistency policy to use when joining initially
     */
    private void joinAsReplica(ReplicaConsistencyPolicy consistency)
        throws InterruptedException {

        if (consistency == null) {
            final int consistencyTimeout =
                getConfigManager().getDuration(ENV_CONSISTENCY_TIMEOUT);
            consistency = new PointConsistencyPolicy
                (replica.getMasterTxnEndVLSN(),
                 consistencyTimeout, TimeUnit.MILLISECONDS);
        }

        /*
         * Wait for the replica to become sufficiently consistent.
         */
        consistency.ensureConsistency(repImpl);

        /*
         * Flush changes to the file system. The flush ensures in particular
         * that any member database updates defining this node itself are not
         * lost in case of a process crash. See SR 20607.
         */
        repImpl.getLogManager().flushNoSync();

        LoggerUtils.info(logger, repImpl, "Joined group as a replica. " +
                         " join consistencyPolicy=" + consistency +
                         " " + repImpl.getVLSNIndex().getRange());
    }

    /**
     * Marks the start of the search for a matchpoint that happens during a
     * syncup. The lower bound of the VLSN range must remain stable during
     * syncup to prevent deleting files that are being read by a syncup file
     * reader.
     * <p>
     * A feeder may have multiple syncups in action. The caller
     * should call {@link #syncupEnded} when the syncup is done, passing the
     * value returned by this method.
     *
     * @param syncupNode identifies the other node involved in the the syncup,
     * and is the name to be used in LogSizeStats.
     *
     * @return the ProtectedFileSet protecting the VLSNIndex range and
     * identifying the syncup in LogSizeStats.
     */
    public ProtectedFileSet syncupStarted(NameIdPair syncupNode) {

        return getVLSNIndex().protectRangeHead(
            FileProtector.SYNCUP_NAME + "-" + syncupNode.toString());
    }

    public void syncupEnded(ProtectedFileSet fileSet) {
        repImpl.getFileProtector().removeFileProtection(fileSet);
    }

    long getReplicaCloseCatchupMs() {
        return replicaCloseCatchupMs;
    }

    public Arbiter getArbiter() {
        return arbiter;
    }

    /**
     * Shuts down the Network backup service *before* a rollback is initiated
     * as part of syncup, thus ensuring that NetworkRestore does not see an
     * inconsistent set of log files. Any network backup operations that are in
     * progress at this node are aborted. The client of the service will
     * experience network connection failures and will retry with this node
     * (when the service is re-established at this node), or with some other
     * node.
     * <p>
     * restartNetworkBackup() is then used to restart the service after it was
     * shut down.
     */
    final public void shutdownNetworkBackup() {
        logFeederManager.shutdown();
        logFeederManager = null;
    }

    /**
     * Restarts the network backup service *after* a rollback has been
     * completed and the log files are once again in a consistent state.
     */
    final public void restartNetworkBackup() {
        if (logFeederManager != null) {
            throw EnvironmentFailureException.unexpectedState(repImpl);
        }
        logFeederManager =
            new com.sleepycat.je.rep.impl.networkRestore.FeederManager
            (serviceDispatcher, repImpl, nameIdPair);
    }

    /**
     * Clears the cached checksum for a file when it may be overwritten
     * (e.g., entries may be erased).
     */
    public void clearedCachedFileChecksum(String fileName) {

        /* Use local var to avoid NPE. */
        com.sleepycat.je.rep.impl.networkRestore.FeederManager manager =
            logFeederManager;

        if (manager != null) {
            manager.clearedCachedFileChecksum(fileName);
        }
    }

    /**
     * Dumps the states associated with any active Feeders as well as
     * the composition of the group itself.
     */
    public String dumpState() {
        return  "\n" + feederManager.dumpState(false /* acksOnly */) +
            "\n" + getGroup();
    }

    /**
     * Dumps the state associated with all active Feeders that supply
     * acknowledgments.
     */
    public String dumpAckFeederState() {
        return  "\n" + feederManager.dumpState(true /* acksOnly */) + "\n";
    }

    public ElectionQuorum getElectionQuorum() {
        return electionQuorum;
    }

    public DurabilityQuorum getDurabilityQuorum() {
        return durabilityQuorum;
    }

    public void setConvertHook(TestHook<Integer> hook) {
        if (convertHooks == null) {
            convertHooks = new HashSet<>();
        }
        convertHooks.add(hook);
    }

    public static void setRunLoopTestHook(TestHook<RepNode> hook) {
        runLoopTestHook = hook;
    }

    private boolean runConvertHooks () {
        if (convertHooks == null) {
            return true;
        }

        for (TestHook<Integer> h : convertHooks) {
            assert TestHookExecute.doHookIfSet(h, 0);
        }
        return true;
    }

    /**
     * Get the group minimum JE version.
     *
     * <p>Returns the minimum JE version that is required for all nodes that
     * join this node's replication group.  The version returned is supported
     * by all current and future group members.  The minimum JE version is
     * guaranteed to only increase over time, so long as the data for the
     * environment is not rolled back or lost.
     *
     * @return the group minimum JE version
     */
    public JEVersion getMinJEVersion() {
        synchronized (minJEVersionLock) {
            return group.getMinJEVersion();
        }
    }

    /**
     * Checks if all data nodes in the replication group support the specified
     * JE version.  Updates the group minimum JE version, and the group format
     * version, as needed to require all nodes joining the group to be running
     * at least the specified JE version.
     *
     * <p>This method should only be called on the master, because attempts to
     * update the rep group DB on an replica will fail.
     *
     * @param newMinJEVersion the new minimum JE version
     * @throws DatabaseException if an error occurs when accessing the
     * replication group database
     * @throws MinJEVersionUnsupportedException if the version is not supported
     * by one or more current group members
     */
    public void setMinJEVersion(final JEVersion newMinJEVersion)
        throws MinJEVersionUnsupportedException {

        /*
         * Synchronize here on minJEVersionLock to prevent new secondary nodes
         * from being added while updating the minimum JE version.  Electable
         * nodes are stored in the RepGroupDB, so the check performed on that
         * class's setMinJEVersion within a transaction insures that all
         * current nodes have been checked before the minimum JE version is
         * increased.  But secondary nodes are not stored persistently, so
         * other synchronization is needed for them.
         */
        synchronized (minJEVersionLock) {

            /* Check if at least this version is already required */
            final JEVersion groupMinJEVersion = group.getMinJEVersion();
            if (groupMinJEVersion.compareTo(newMinJEVersion) >= 0) {
                return;
            }

            for (final RepNodeImpl node : group.getDataMembers()) {
                JEVersion nodeJEVersion = node.getJEVersion();
                if (getNodeName().equals(node.getName())) {

                    /* Use the current software version for the local node */
                    nodeJEVersion = repImpl.getCurrentJEVersion();
                } else {

                    /* Use the version recorded by the feeder for replicas */
                    final Feeder feeder =
                        feederManager.getFeeder(node.getName());
                    if (feeder != null) {
                        final JEVersion currentReplicaJEVersion =
                            feeder.getReplicaJEVersion();
                        if (currentReplicaJEVersion != null) {
                            nodeJEVersion = currentReplicaJEVersion;
                        }
                    }
                }
                if ((nodeJEVersion == null) ||
                    (newMinJEVersion.compareTo(nodeJEVersion) > 0)) {
                    throw new MinJEVersionUnsupportedException(
                        newMinJEVersion, node.getName(), nodeJEVersion);
                }
            }
            repGroupDB.setMinJEVersion(newMinJEVersion);
        }
    }

    /**
     * Returns true if acks are needed by the group for durability. This is the
     * case if the rep group has &gt; 1 data node that's also electable.
     */
    public boolean isNeedsAcks() {
        return needsAcks;
    }

    /**
     * Adds a transient ID node to the group.  Assign a node ID and add the
     * node to the RepGroupImpl.
     *
     * @param node the node
     * @throws IllegalStateException if the store does not currently support
     *         secondary nodes or the node doesn't meet the current minimum JE
     *         version
     * @throws NodeConflictException if the node conflicts with an existing
     *         persistent node
     */
    public void addTransientIdNode(final RepNodeImpl node) {
        if (!node.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Attempt to call addTransientIdNode with a" +
                " node without transient ID: " + node);
        }
        final JEVersion requiredJEVersion =
            RepGroupImpl.FORMAT_VERSION_3_JE_VERSION;
        try {
            setMinJEVersion(requiredJEVersion);
        } catch (MinJEVersionUnsupportedException e) {
            if (e.nodeVersion == null) {
                throw new IllegalStateException(
                    "Secondary nodes are not currently supported." +
                    " The version running on node " + e.nodeName +
                    " could not be determined," +
                    " but this feature requires version " +
                    requiredJEVersion.getNumericVersionString() +
                    " or later.");
            }
            throw new IllegalStateException(
                "Secondary nodes are not currently supported." +
                " Node " + e.nodeName + " is running version " +
                e.nodeVersion.getNumericVersionString() +
                ", but this feature requires version " +
                requiredJEVersion.getNumericVersionString() +
                " or later.");
        }

        /*
         * Synchronize on minJEVersionLock to coordinate with setMinJEVersion
         */
        synchronized (minJEVersionLock) {
            final JEVersion minJEVersion = group.getMinJEVersion();
            if (node.getJEVersion().compareTo(minJEVersion) < 0) {
                throw new IllegalStateException(
                    "The node does not meet the minimum required version" +
                    " for the group." +
                    " Node " + node.getNameIdPair().getName() +
                    " is running version " + node.getJEVersion() +
                    ", but the minimum required version is " +
                    minJEVersion);
            }
            if (!node.getNameIdPair().hasNullId()) {
                throw new IllegalStateException(
                    "New " + node.getType().toString().toLowerCase() +
                    " node " + node.getNameIdPair().getName() +
                    " already has an ID: " + node.getNameIdPair().getId());
            }
            node.getNameIdPair().setId(transientIds.allocateId());
            group.addTransientIdNode(node);
        }
    }

    /**
     * Removes a node with transient id from the group.  Remove the node from
     * the RepGroupImpl and deallocate the node ID.
     *
     * @param node the node
     */
    public void removeTransientNode(final RepNodeImpl node) {
        if (!node.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Attempt to call removeTransientNode with a" +
                " node without transient ID: " + node);
        }
        group.removeTransientNode(node);
        transientIds.deallocateId(node.getNodeId());
    }

    private class RepElectionsConfig implements ElectionsConfig {
        private final RepNode repNode;
        RepElectionsConfig(RepNode repNode) {
            this.repNode = repNode;
        }

        @Override
        public String getGroupName() {
            return repNode.getRepImpl().getConfigManager().get(GROUP_NAME);
        }

        @Override
        public NameIdPair getNameIdPair() {
            return repNode.getNameIdPair();
        }

        @Override
        public ServiceDispatcher getServiceDispatcher() {
            return repNode.getServiceDispatcher();
        }

        @Override
        public int getElectionPriority() {
            return repNode.getElectionPriority();
        }

        @Override
        public int getLogVersion() {
            return repNode.getLogVersion();
        }

        @Override
        public RepImpl getRepImpl() {
            return repNode.getRepImpl();
        }

        @Override
        public RepNode getRepNode() {
            return repNode;
        }
    }

    /**
     * Track node IDs for node with transient IDs.  IDs are allocated from the
     * specified number of values at the high end of the range of integers.
     */
    static class TransientIds {
        private final int size;
        private final BitSet bits;

        /** Creates an instance that allocates the specified number of IDs. */
        TransientIds(final int size) {
            this.size = size;
            assert size > 0;
            bits = new BitSet(size);
        }

        /**
         * Allocates a free ID, throwing IllegalStateException if none are
         * available.
         */
        synchronized int allocateId() {

            /*
             * Note that scanning for the next clear bit is somewhat
             * inefficient, but this inefficiency shouldn't matter given the
             * small number of secondary nodes expected.  If needed, the next
             * improvement would probably be to remember the last allocated ID,
             * to avoid repeated scans of an initial range of already allocated
             * bits.
             */
            final int pos = bits.nextClearBit(0);
            if (pos >= size) {
                throw new IllegalStateException("No more secondary node IDs");
            }
            bits.set(pos);
            return Integer.MAX_VALUE - pos;
        }

        /**
         * Deallocates a previously allocated ID, throwing
         * IllegalArgumentException if the argument was not allocated by
         * allocateId or if the ID is not currently allocated.
         */
        synchronized void deallocateId(final int id) {
            if (id < Integer.MAX_VALUE - size) {
                throw new IllegalArgumentException(
                    "Illegal secondary node ID: " + id);
            }
            final int pos = Integer.MAX_VALUE - id;
            if (!bits.get(pos)) {
                throw new IllegalArgumentException(
                    "Secondary node ID is not currently allocated: " + id);
            }
            bits.clear(pos);
        }
    }

    /**
     * Gets the stats related to master transfer.
     */
    public StatGroup getMasterTransferStats(StatsConfig config) {
        return masterTransferStats.cloneGroup(config.getClear());
    }

    public StatGroup getChannelTimeoutStats(StatsConfig config) {
        return channelTimeoutStats.cloneGroup(config.getClear());
    }

    public void incrementChannelTimeout(String name) {
        nChannelTimeoutMap.createStat(name).add(1);
    }

    private synchronized long updateDTVLSNMax(long vlsn) {
        final long ret = dtvlsn.updateMax(vlsn);
        if (dtVLSNLatch != null) {
            dtVLSNLatch.countDown(vlsn);
        }
        return ret;
    }

    public synchronized void updateLocalDurableVLSN(long vlsn) {
        localDurableVLSN.updateMax(vlsn);
    }

    public void waitForDTVLSN(long vlsn, long waitNs)
        throws InterruptedException, VLSNIndex.WaitTimeOutException {

        synchronized (this) {
            if (durable(vlsn)) {
                /* already durable */
                return;
            }
            if (dtVLSNLatch == null) {
                dtVLSNLatch = new VLSNIndex.VLSNAwaitLatch(vlsn);
            }
        }

        /*
         * Do any waiting outside the synchronization section. If the
         * waited-for VLSN has already arrived, the waitLatch will have been
         * counted down, and we'll go through.
         */
        if (!dtVLSNLatch.await(waitNs, TimeUnit.NANOSECONDS) ||
            dtVLSNLatch.isTerminated()) {
            /* Timed out waiting for a durable VLSN, or was terminated. */
            throw new VLSNIndex.WaitTimeOutException();
        }
    }

    /**
     * Returns true if the given vlsn is durable, e.g., equal or less
     * than the valid DTVLSN
     *
     * @param vlsn given vlsn
     * @return true if the given vlsn is durable, false otherwise
     */
    public boolean durable(long vlsn) {
        final long dtVLSN = dtvlsn.get();
        return (dtVLSN != NULL_VLSN) && (vlsn <= dtVLSN);
    }
}
