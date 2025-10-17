/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.rep;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.KVVersion;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.api.ops.ResourceTracker;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.TableChangeList;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.pubsub.CheckpointTableManager;
import oracle.kv.impl.rep.RepEnvHandleManager.StateChangeListenerFactory;
import oracle.kv.impl.rep.RepNodeService.KVStoreCreator;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdmin.MigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.masterBalance.MasterBalanceManager;
import oracle.kv.impl.rep.masterBalance.MasterBalanceManagerInterface;
import oracle.kv.impl.rep.masterBalance.MasterBalanceStateTracker;
import oracle.kv.impl.rep.migration.MigrationManager;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationTable;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.security.SignatureHelper;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.metadata.SecurityMetadata.KerberosInstance;
import oracle.kv.impl.security.metadata.SecurityMetadataInfo;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.security.util.SNKrbInstance;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.tif.TextIndexFeederManager;
import oracle.kv.impl.tif.TextIndexFeederTopoTracker;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.table.Index;
import oracle.kv.table.Table;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.TimeConsistencyPolicy;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;
import com.sleepycat.je.rep.impl.node.FeederManager;
import com.sleepycat.je.utilint.TaskCoordinator;

/**
 * A RepNode represents the data stored in a particular node. It is a member of
 * a replication group, holding some number of partitions of the total key
 * space of the data store.
 * <p>
 * This class is responsible for the management of the canonical replication
 * environment handle and database handles. That is, it provides the plumbing
 * to facilitate requests and does not handle the implementation of any user
 * level operations. All such operations are handled by the RequestHandler and
 * its sub-components.
 * <p>
 */
public class RepNode implements TopologyManager.PostUpdateListener,
                                TopologyManager.PreUpdateListener {

    /**
     * Test hook executed before sending NOP to a node in a specified rep group.
     * A use case is to decide which node to send the NOP to in a test avoiding
     * sending the NOP to a random node.
     */
    public static volatile TestHook<RepNode> SEND_RG_NOP_HOOK = null;

    /* Number of retries for DB operations. */
    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays */
    private static final long RETRY_TIME_MS = 500;

    /* Name of the non-replicated database persisting the topology */
    private static final String TOPOLOGY_DB_NAME = "TopologyDatabase";

    /*
     * Currently only one topology is stored with the key of 0. If this
     * changes then we need to add more key constants or generate keys/entries
     * at runtime.
     */
    private static final Long TOPOLOGY_KEY = Long.valueOf(0);
    private static final DatabaseEntry TOPOLOGY_KEY_ENTRY = new DatabaseEntry();
    static {
        LongBinding.longToEntry(TOPOLOGY_KEY, TOPOLOGY_KEY_ENTRY);
    }

    private final RepNodeId repNodeId;

    /**
     * The parameters used to configure the rep node.
     */
    private final Params params;

    private final RequestDispatcher requestDispatcher;

    /**
     * The topology manager. All access to the topology, which can change
     * dynamically, is through the manager.
     */
    private final TopologyManager topoManager;

    /**
     * The migration manager. Handles all of the partition migration duties.
     */
    private volatile MigrationManager migrationManager;

    /**
     * The RN side master balancing component.
     */
    private MasterBalanceManagerInterface masterBalanceManager;

    /**
     * The security metadata manager. All access to the security metadata is
     * through the manager.
     */
    private SecurityMetadataManager securityMDManager;

    /*
     * Manages the partition database handles.
     */
    private PartitionManager partitionManager;

    /*
     * Manages the secondary database handles.
     */
    private TableManager tableManager;

    /*
     * Manages TextIndexFeeder within RepNode
     */
    private TextIndexFeederManager textIndexFeederManager;

    /*
     * Topology tracker for text index feeder
     */
    private TextIndexFeederTopoTracker textIndexFeederTopoTracker;

    /**
     * The manager for the environment holding all the databases for the
     * partitions owned by this RepNode. The replicated environment handle can
     * change dynamically as exceptions are encountered. The manager
     * coordinates access to the single shared replicated environment handle.
     */
    private RepEnvHandleManager envManager;

    /* Service reference may be null during unit tests */
    private final RepNodeService repNodeService;

    /* True if the RepNode has been stopped. */
    private volatile boolean stopped = false;

    private final Logger logger;

    /* Time when the RepNode instance was created. */
    private final long serviceStartTime;

    /*
     * Time when the RepNode had it's last JE replication node state change
     * event, or 0 if no event has been seen.
     */
    private volatile long stateChangeTime;

    /* SN mgmt agent to register RN replicated state change event listener */
    private ReplicationStateListener replicationStateListener;

    /* Overrides the version returned from getStoreVersion(). For unit tests */
    public static KVVersion storeVersionOverride = null;

    public RepNode(Params params, RequestDispatcher requestDispatcher,
                   RepNodeService repNodeService) {
        this.params = params;
        this.requestDispatcher = requestDispatcher;
        this.topoManager = requestDispatcher.getTopologyManager();
        this.repNodeService = repNodeService;
        serviceStartTime = System.currentTimeMillis();
        logger = LoggerUtils.getLogger(this.getClass(), params);
        final RepNodeParams repNodeParams = params.getRepNodeParams();
        repNodeId = repNodeParams.getRepNodeId();
    }

    public GlobalParams getGlobalParams() {
        return params.getGlobalParams();
    }

    public RepNodeParams getRepNodeParams() {
        return params.getRepNodeParams();
    }

    public StorageNodeParams getStorageNodeParams() {
        return params.getStorageNodeParams();
    }

    public LoadParameters getAllParams() {
        final LoadParameters ret = new LoadParameters();
        ret.addMap(params.getGlobalParams().getMap());
        ret.addMap(params.getStorageNodeParams().getMap());
        ret.addMap(params.getRepNodeParams().getMap());
        return ret;
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns the dispatchers exception handler. An exception caught by this
     * handler results in the process being restarted by the SNA.
     *
     * <p>See: RepNodeService.ThreadExceptionHandler
     *
     * @return the dispatchers exception handler
     */
    public UncaughtExceptionHandler getExceptionHandler() {
        return requestDispatcher.getExceptionHandler();
    }

    /**
     * For testing only.
     */
    public MasterBalanceStateTracker getBalanceStateTracker() {
        return masterBalanceManager.getStateTracker();
    }

    public RequestDispatcher getRequestDispatcher() {
        return requestDispatcher;
    }

    // TODO: incremental changes to configuration properties
    /* Public access for unit test */
    public void initialize(StateChangeListenerFactory listenerFactory) {

        if (migrationManager == null) {
            migrationManager = new MigrationManager(this, params);
        }

        /*
         * Must precede opening of the environment handle below.
         */
        if (masterBalanceManager == null) {
            masterBalanceManager = MasterBalanceManager.create(this, logger);
        }
        masterBalanceManager.initialize();
        envManager = new RepEnvHandleManager(this, listenerFactory, params,
                                             repNodeService);

        if (tableManager == null) {
            tableManager = new TableManager(this, params);
            if (repNodeService != null) {
                repNodeService.addParameterListener(tableManager);
            }
        }

        /*
         * tif relies on tableManager to get the text indices, so it must be
         * initialized after tableManager is initialized.
         */
        if (textIndexFeederManager == null &&
            (VersionUtil.getJavaMajorVersion() >= 11)) {
            textIndexFeederManager =
                new TextIndexFeederManager(this, params);
            if (repNodeService != null) {
                repNodeService.addParameterListener(textIndexFeederManager);
            }
        }

        /* Set up the security metadata manager */
        if (securityMDManager == null) {
            securityMDManager = new SecurityMetadataManager(
                this, params.getGlobalParams().getKVStoreName(), logger);
        }

        if (partitionManager == null) {
            partitionManager = new PartitionManager(this, tableManager, params);
        }
        topoManager.setLocalizer(migrationManager);
        topoManager.addPreUpdateListener(this);
        topoManager.addPostUpdateListener(this);

        final SignatureHelper<Topology> topoSignatureHelper =
            repNodeService == null ?
            null :
            repNodeService.getRepNodeSecurity().getTopoSignatureHelper();

        if (topoSignatureHelper != null) {
            final TopoSignatureManager topoSignatureManager =
                new TopoSignatureManager(topoSignatureHelper, logger);

            topoManager.addPreUpdateListener(topoSignatureManager);
            topoManager.addPostUpdateListener(topoSignatureManager);
        }

        if (textIndexFeederTopoTracker == null &&
            (VersionUtil.getJavaMajorVersion() >= 11)) {
            textIndexFeederTopoTracker =
                new TextIndexFeederTopoTracker(this,
                                               textIndexFeederManager,
                                               logger);
            topoManager.addPostUpdateListener(textIndexFeederTopoTracker);
        }
    }

    public boolean isMaster() {
        /* Don't wait, we just care about the state: master or not */
        final ReplicatedEnvironment env = envManager.getEnv(0);
        return (env != null && env.getState().isMaster());
    }

    /**
     * Returns the partition manager.
     *
     * @return the partition manager
     */
    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    /**
     * Returns the partition generation table.
     * @return the partition generation table
     */
    public PartitionGenerationTable getPartGenTable() {
        assert partitionManager != null;
        return partitionManager.getPartGenTable();
    }

    /**
     * Returns the partition Db config.
     *
     * @return the partition Db config
     */
    public DatabaseConfig getPartitionDbConfig() {
        assert partitionManager != null;
        return partitionManager.getPartitionDbConfig();
    }

    /**
     * Opens the database handles used by this rep node.
     * The databases are created if they do not already exist.
     * <p>
     * This method is invoked at startup. At this time, new databases may be
     * created if this node is the master and databases need to be created for
     * the partitions assigned to this node. If the node is a replica, it may
     * need to wait until the databases created on the master have been
     * replicated to it.
     * <p>
     * Post startup, this method is invoked to re-establish database handles
     * whenever the associated environment handle is invalidated and needs to
     * be re-established. Or via the TopologyManager's listener interface
     * whenever the Topology has been updated.
     *
     * @param repEnv the replicated environment handle
     *
     */
    void updateDbHandles(ReplicatedEnvironment repEnv) {

        final Topology topology = topoManager.getLocalTopology();

        if (topology == null) {
            /* No topology and no partitions. Node is being initialized. */
            return;
        }

        migrationManager.updateDbHandles(repEnv);
        securityMDManager.updateDbHandles(repEnv);

        /*
         * Initialize in-memory partition generation table, must call
         * after MigrationManager updated the database handles, which opens
         * generation database and before opening the partition databases.
         */
        partitionManager.initPartGenTable();
        partitionManager.updateDbHandles(topology, repEnv);
        tableManager.updateDbHandles(repEnv);
    }

    @Override
    public void preUpdate(Topology newTopology) {
        /* Don't wait, we just care about the state: master or not */
        final ReplicatedEnvironment env = envManager.getEnv(0);
        if (!isMaster(env)) {
            return;
        }

        try {
            requestDispatcher.getTopologyManager().
                checkPartitionChanges(new RepGroupId(repNodeId.getGroupId()),
                                      newTopology);
        } catch (IllegalStateException ise) {
            if (!isAuthoritativeMaster(env)) {
                getLogger().log(Level.INFO,
                    () -> String.format("Non-authoratitive master "
                        + "failed partition change check: %s", ise));
                throw new OperationFaultException("not authoritative master");
            }
            /* The Topology checks failed, force a shutdown. */
            getExceptionHandler().
                uncaughtException(Thread.currentThread(), ise);
        }
    }

    private boolean isMaster(ReplicatedEnvironment env) {
        try {
            if ((env == null) || !env.getState().isMaster()) {
                return false;
            }
        } catch (EnvironmentFailureException e) {
            /* It's in the process of being re-established. */
            return false;
        } catch (IllegalStateException iae) {
            /* A closed environment. */
            return false;
        }
        return true;
    }

    private boolean isAuthoritativeMaster(ReplicatedEnvironment env) {
        try {
            return getIsAuthoritativeMaster(env);
        } catch (EnvironmentFailureException e) {
            /* It's in the process of being re-established. */
            return false;
        } catch (IllegalStateException iae) {
            /* A closed environment. */
            return false;
        }
    }

    /**
     * Implements the Listener method for topology changes. It's invoked by
     * the TopologyManager and should not be invoked directly.
     * <p>
     * Update the partition map if the topology changes and store it.
     *
     * TODO - This method persist the topology in the local metadata store. For
     * historical reasons this can't use the general metadata handling facility.
     * It would be good to combine some, if not all of this code with the
     * general code.
     */
    @Override
    public boolean postUpdate(Topology newTopology) {

        final ReplicatedEnvironment env = envManager.getEnv(1);
        if (env == null) {
            throw new OperationFaultException("Could not obtain env handle");
        }

        int attempts = NUM_DB_OP_RETRIES;
        RuntimeException lastException = null;
        while (!stopped) {
            if (!env.isValid()) {
                throw new OperationFaultException(
                        "Failed in persistence of " + newTopology.getType() +
                        " metadata, environment not valid");
            }
            updateDbHandles(env);

            try {
                writeTopology(newTopology, env);

                /*
                 * Ensure it's in stable storage, since the database is
                 * non-transactional. For unit tests, use WRITE_NO_SYNC
                 * rather than SYNC, for improved performance.
                 */
                final boolean flushSync = !TestStatus.isWriteNoSyncAllowed();
                env.flushLog(flushSync);
                logger.log(
                    Level.INFO,
                    () ->
                    String.format("Topology stored seq# %s",
                                  newTopology.getSequenceNumber()));
                return false;
            } catch (ReplicaWriteException |
                     UnknownMasterException |
                     InsufficientReplicasException |
                     DiskLimitException rwe) {
                lastException = rwe;
                if (!hasAvailableLogSize()) {
                    /* Do not retry if we have a disk limit problem. */
                    break;
                }
            } catch (RuntimeException rte) {
                /*
                 * Includes ISEs or EFEs in particular, due to the environment
                 * having been closed or invalidated, but can also include NPE
                 * that may be thrown in certain circumstances.
                 */
                if (!env.isValid()) {
                    logger.info("Encountered failed env during topo update. " +
                                rte.getMessage());
                    /*
                     * The RN will recover on its own, fail the update, forcing
                     * the invoker of the update to retry.
                     */
                    throw new OperationFaultException("Failed topology update",
                                                      rte);
                }
                /* Will cause the process to exit. */
                throw rte;
            }
            attempts--;
            if (attempts == 0) {
                throw new OperationFaultException(
                        "Failed in persistence of " + newTopology.getType() +
                        " metadata, operation timed out", lastException);
            }
            try {
                Thread.sleep(RETRY_TIME_MS);
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
        return false;
    }

    /**
     * Update the topology at this node with the topology changes in topoInfo.
     *
     * @param topoInfo contains the changes to be made to the topo at this RN
     *
     * @return the post-update topology sequence number
     */
    private int updateTopology(TopologyInfo topoInfo) {
        if (topoInfo.isEmpty()) {
            /* Unexpected, should not happen. */
            logger.warning("Empty change list sent for topology update");

            return getTopoSequenceNumber();
        }

        final int topoSeqNum = getTopoSequenceNumber();

        if (topoInfo.getChanges().get(0).getSequenceNumber() >
            (topoSeqNum + 1)) {
            /*
             * Allow for cases where the update pushes may be obsolete, e.g.
             * when an SN is being migrated, or more generally when its log
             * files have been deleted and it's being re-initialized
             */
            logger.info("Ignoring topo update request. " +
                        "Topo seq num: " + topoSeqNum +
                        " first change: " +
                        topoInfo.getChanges().get(0).getSequenceNumber());
            return topoSeqNum;
        }
        topoManager.update(topoInfo);
        return getTopoSequenceNumber();
    }

    /**
     * Returns the current topo sequence number associated with the RN,
     * or Topology.EMPTY_SEQUENCE_NUMBER if the RN has not been initialized
     * with a topology.
     */
    private int getTopoSequenceNumber() {
        final Topology topology = getTopology();
        return (topology != null) ?
               topology.getSequenceNumber() :
               Topology.EMPTY_SEQUENCE_NUMBER;
    }

    /**
     * Updates the local topology and partition Db map. The basis of the update
     * is the current "official" topology and is only done iff the current
     * topology sequence number is greater than or equal to the specified
     * topoSeqNum. Otherwise, the local topology is left unchanged.
     *
     * @return true if the update was successful
     */
    public boolean updateLocalTopology() {
        if (topoManager.updateLocalTopology()) {
            return true;
        }

        /*
         * We need a newer topology. Send a NOP to the master, since that is
         * where topoSeqNum came from. The master will send the topology
         * in the response.
         */
        logger.log(Level.FINE, "Sending NOP to update topology");
        return sendNOP(new RepGroupId(repNodeId.getGroupId()));
    }

    /**
     * Returns the SecurityMetadataManager for this RepNode.
     */
    public SecurityMetadataManager getSecurityMDManager() {
        return securityMDManager;
    }

    /**
     * Returns the TopologyManager for this RepNode.
     */
    public TopologyManager getTopologyManager() {
        return topoManager;
    }

    /**
     * Returns the TableManager for this RepNode.
     */
    public TableManager getTableManager() {
        return tableManager;
    }

    /**
     * Returns the TextIndexFeederManager for this RepNode.
     */
    public TextIndexFeederManager getTextIndexFeederManager() {
        return (VersionUtil.getJavaMajorVersion() < 11) ?
                                                      null :
                                                      textIndexFeederManager;
    }

    /**
     * Sends a NOP to a node in the specified group. It attempts to send the NOP
     * to the master of that group if known. Otherwise it sends it to a random
     * node in the group.
     *
     * @param groupId
     * @return true if the operation was successful
     */
    public boolean sendNOP(RepGroupId groupId) {
        assert TestHookExecute.doHookIfSet(SEND_RG_NOP_HOOK, this);

        final RepGroupState rgs = requestDispatcher.getRepGroupStateTable().
                                                        getGroupState(groupId);
        final LoginManager lm =
                repNodeService.getRepNodeSecurity().getLoginManager();
        RepNodeState rns = rgs.getMaster();

        if (rns == null) {
            rns = rgs.getRandomRN(null, null);
        }

        logger.log(Level.FINE, "Sending NOP to {0}", rns.getRepNodeId());
        return executeNOPInternal(rns, lm);
    }

    /**
     * Sends a NOP to a random node in the specified group excluding the
     * specified RNs.
     *
     * @param groupId
     * @param excludeRNs
     * @return true if the operation was successful
     */
    public boolean sendRandomNOP(RepGroupId groupId, Set<RepNodeId> excludeRNs) {

        final RepGroupState rgs =
            requestDispatcher.getRepGroupStateTable().getGroupState(groupId);
        final LoginManager lm =
            repNodeService.getRepNodeSecurity().getLoginManager();
        final RepNodeState rns = rgs.getRandomRN(null, excludeRNs);

        logger.log(Level.FINE, "Sending NOP to {0}", rns.getRepNodeId());
        return executeNOPInternal(rns, lm);
    }

    private boolean executeNOPInternal(RepNodeState rns, LoginManager lm) {
        try {
            if (requestDispatcher.executeNOP(rns, 1000, lm) != null) {
                return true;
            }
        } catch (Exception ex) {
            logger.log(Level.WARNING,
                       "Exception sending NOP to " + rns.getRepNodeId(),
                       ex);
        }
        return false;

    }

    /* For test */
    boolean sendNOP(RepNodeId rnId) {
        final RepNodeState rns = requestDispatcher.getRepGroupStateTable().
                                                        getNodeState(rnId);
        final LoginManager lm =
                repNodeService.getRepNodeSecurity().getLoginManager();
        logger.log(Level.FINE, "Sending NOP to {0}", rns.getRepNodeId());

        try {
            if (requestDispatcher.executeNOP(rns, 1000, lm) != null) {
                return true;
            }
        } catch (Exception ex) {
            logger.log(Level.WARNING,
                       "Exception sending NOP to " + rns.getRepNodeId(),
                       ex);
        }
        return false;
    }

    /**
     * Returns the rep node state for the master of the specified group. If
     * the master is not known, null is returned.
     *
     * @param groupId
     * @return the rep node state
     */
    public RepNodeState getMaster(RepGroupId groupId) {
        return requestDispatcher.getRepGroupStateTable().getGroupState(groupId)
            .getMaster();
    }

    /**
     * Returns the rep group state.
     */
    public RepGroupState getRepGroupState(RepGroupId groupId) {
        return requestDispatcher.getRepGroupStateTable().getGroupState(groupId);
    }

    /**
     * Invoked when the replicated environment has been invalidated due to an
     * exception that requires an environment restart, performing the restart
     * asynchronously in a separate thread.
     *
     * @param prevRepEnv the handle that needs to be re-established.
     *
     * @param rbe the exception that required re-establishment of the handle.
     */
    public void asyncEnvRestart(ReplicatedEnvironment prevRepEnv,
                                RestartRequiredException rbe) {
        envManager.asyncRenewRepEnv(prevRepEnv, rbe);
    }

    /**
     * Used to inform the RN about state change events associated with the
     * environment. It, in turn, informs the environment manager.
     */
    public void noteStateChange(ReplicatedEnvironment repEnv,
                                StateChangeEvent stateChangeEvent) {
        if (stopped) {
            return;
        }

        envManager.noteStateChange(repEnv, stateChangeEvent);
        migrationManager.noteStateChange(stateChangeEvent);
        tableManager.noteStateChange(stateChangeEvent);
        masterBalanceManager.noteStateChange(stateChangeEvent);
        if (textIndexFeederManager != null) {
            textIndexFeederManager.noteStateChange(stateChangeEvent);
        }
        /* Inform SN mgmt agent about state change event */
        if (replicationStateListener != null) {
            replicationStateListener.doNotify(stateChangeEvent);
        }
        stateChangeTime = System.currentTimeMillis();
    }

    void setReplicationStateListener(ReplicationStateListener listener) {
        replicationStateListener = listener;
    }

    /**
     * Returns the environment handle.
     *
     * @param timeoutMs the max amount of time to wait for a handle to become
     * available
     *
     * @see RepEnvHandleManager#getEnv
     */
    public ReplicatedEnvironment getEnv(long timeoutMs) {
        return envManager.getEnv(timeoutMs);
    }

    /**
     * Returns the underlying implementation handle for the environment or null
     * if it cannot be obtained.
     */
    public RepImpl getEnvImpl(long timeoutMs) {
        final ReplicatedEnvironment env = getEnv(timeoutMs);
        return (env != null) ? RepInternal.getRepImpl(env) : null;
    }

    /**
     * Returns whether the underlying JE environment is known to be the
     * authoritative master, without waiting for the environment.
     */
    public boolean getIsAuthoritativeMaster() {
        return getIsAuthoritativeMaster(getEnv(0));
    }

    public boolean getIsAuthoritativeMaster(ReplicatedEnvironment env) {
        if (env == null) {
            return false;
        }
        final RepImpl repImpl = RepInternal.getRepImpl(env);
        if (repImpl == null) {
            return false;
        }
        final com.sleepycat.je.rep.impl.node.RepNode repNode =
            repImpl.getRepNode();
        if (repNode == null) {
            return false;
        }
        return repNode.isAuthoritativeMaster();
    }

    /*
     * Returns the RepEnvHandleManager.
     */
    RepEnvHandleManager getRepEnvManager() {
        return envManager;
    }

    /**
     * Returns network backup statistics for a network restore that is
     * currently underway to prepare for opening the environment, or null if no
     * network restore is in progress or if statistics are otherwise not
     * available.
     */
    public NetworkBackupStats getNetworkRestoreStats() {
        if (envManager != null) {
            return envManager.getNetworkRestoreStats();
        }
        return null;
    }

    /**
     * Returns the string value of the StorageType enum or null.
     */
    public String getStorageType() {
        return (envManager == null)? null:
                                     envManager.envStorageType().toString();
    }
    /**
     * Invoked to provide a clean shutdown of the RepNode.
     */
    public void stop(boolean force) {
        if (stopped) {
            logger.info("RepNode already stopped.");
            return;
        }
        stopped = true;
        logger.info("Shutting down RepNode" + (force ? "(force)" : ""));

        migrationManager.shutdown(force);
        tableManager.shutdown();
        if (textIndexFeederManager != null) {
            textIndexFeederManager.shutdown(false);
        }
        closeDbHandles(force);

        if (envManager != null) {
            envManager.stopAsyncNetworkRestore(force);
            if (force) {
                /* Use an internal interface to close without checkpointing */
                final ReplicatedEnvironment env = envManager.getEnv(1);
                if (env != null) {
                    final EnvironmentImpl envImpl =
                        DbInternal.getEnvironmentImpl(env);
                    if (envImpl != null) {
                        try {
                            envImpl.close(false);
                        } catch (DatabaseException e) {
                            /*
                             * Ignore exceptions during a forced close, just
                             * bail out.
                             */
                            logger.log(Level.INFO,  "Ignoring exception " +
                                       "during forced close:", e);
                        }
                    }
                }
            } else {
                envManager.closeEnv();
            }
        }

        /*
         * Shutdown, after the env handle has been closed, so that state
         * changes are communicated by the tracker.
         */
        masterBalanceManager.shutdown();
    }

    /**
     * Closes all database handles, typically as a precursor to closing the
     * environment handle itself. The caller is assumed to have made provisions
     * if any to ensure that the handles are no longer in use.
     */
    void closeDbHandles(boolean force) {
        migrationManager.closeDbHandles(force);
        tableManager.closeDbHandles();
        partitionManager.closeDbHandles();
        securityMDManager.closeDbHandles();
    }

    /**
     * Gets the set of partition ids managed by this node.
     *
     * @return the set of partition ids
     */
    public Set<PartitionId> getPartitions() {
        return partitionManager.getPartitions();
    }

    /**
     * Returns the partition associated with the key.
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the partitionId associated with the key
     */
    public PartitionId getPartitionId(byte[] keyBytes) {
        return partitionManager.getPartitionId(keyBytes);
    }

    /**
     * Returns the database associated with the key.
     *
     * @param keyBytes the key used for looking up the database
     *
     * @return the database associated with the key or null
     */
    public Database getPartitionDB(byte[] keyBytes) {
        return partitionManager.getPartitionDB(keyBytes);
    }

    /**
     * Returns the database associated with the partition.
     *
     * @param partitionId the partition used for looking up the database.
     *
     * @return the database associated with the partition.
     *
     * @throws IncorrectRoutingException is the partition DB is not found
     */
    public Database getPartitionDB(PartitionId partitionId)
            throws IncorrectRoutingException {

        final Database partitionDb =
                partitionManager.getPartitionDB(partitionId);

        if (partitionDb != null) {
            return partitionDb;
        }

        /*
         * The partition database handle was not found. This could be because
         * the request was incorrectly routed or this is the correct node but
         * database has not yet been opened. Check which case it is before
         * throwing an IncorrectRoutingException.
         */
        final String message;
        final Topology topology = getLocalTopology();

        if (topology == null) {
            message = "Partition: " + partitionId + " not present at RepNode " +
                      repNodeId;
        } else {
            /*
             * If the partition id is out of the valid range, null might
             * be returned from partition map, in which case we log and throw
             * IncorrectRoutingException.
             */
            final Partition part = topology.getPartitionMap().get(partitionId);
            if (part == null) {
                message = "Partition: " + partitionId +
                          " is out of range and does not exist in the store.";

            } else if (part.getRepGroupId().getGroupId() ==
                    repNodeId.getGroupId()) {
                /*
                 * According to the local topo, the partition should be here. We
                 * will still thrown an IncorrectRoutingException to buy time as
                 * the operation will be retried.
                 */
                message = "Partition: " + partitionId +
                          " missing from RepNode " +
                          repNodeId + ", topology seq#: " +
                          topology.getSequenceNumber();
                logger.log(Level.FINE, message);
                partitionManager.updateDbHandles(topology);
            } else {
                message = "Partition: " + partitionId +
                          " not present at RepNode " + repNodeId +
                          ", topology seq#: " + topology.getSequenceNumber();
            }
        }

        throw new IncorrectRoutingException(message, partitionId);
    }

    /**
     * Starts up the RepNode and creates a replicated environment handle.
     * Having created the environment, it then initializes the topology manager
     * with a copy of the stored topology from it.
     *
     * @return the environment handle that was established
     */
    public ReplicatedEnvironment startup() {

        /* Create the replicated environment handle. */
        final boolean created = envManager.renewRepEnv(null, null);
        assert created;

        /*
         * Initialize topology from the topology entity store if it's available
         * there. Note that the absence of a topology will cause the admin
         * component to wait until one is supplied via the RepNodeAdmin
         * interface.
         */
        final ReplicatedEnvironment env = envManager.getEnv(1 /* ms */);

        if (env == null) {
            throw new IllegalStateException("Could not obtain environment " +
                                            "handle without waiting.");
        }

        /*
         * Update the topology manager with a saved copy of the topology
         * from the store.
         */
        final Topology topo = readTopology(env);

        /* The local topo could be null if there was a rollback */
        try {
            if (topo == null) {
                logger.info("Store did not contain a topology");
            } else if (topoManager.update(topo)) {
                logger.log(Level.INFO,
                        "Topology fetched sequence#: {0}, updated " +
                        "topology seq# {1}",
                        new Object[]{topo.getSequenceNumber(),
                            topoManager.getTopology().
                                getSequenceNumber()});
            }
        } catch (OperationFaultException e) {
            /*
             * We can live with an OperationFaultException during topology
             * update. Simply log and move on.
             */
            final Topology currTopo = topoManager.getTopology();
            final String mesgPrefix = "Failed to update topology";
            if (currTopo == null) {
                logger.log(Level.WARNING,
                        "{0}. No topology persisted.", mesgPrefix);
            } else {
                logger.log(Level.WARNING,
                        "{0}. Persisted topology seq# {1}.",
                        new Object[] {
                            mesgPrefix,
                            currTopo.getSequenceNumber() });
            }
        }

        /* Start thread to push HA state changes to the SNA. */
        masterBalanceManager.startTracker();
        migrationManager.startTracker();
        tableManager.startTracker();
        if (textIndexFeederManager != null) {
            textIndexFeederManager.startTracker();
        }
        envManager.setEnvForVerifyManager(env);
        return env;
    }

    ReplicationNetworkConfig getRepNetConfig() {
        return envManager.getRepNetConfig();
    }

    public RepNodeId getRepNodeId() {
        return repNodeId;
    }

    public long getServiceStartTime() {
        return serviceStartTime;
    }

    public long getStateChangeTime() {
        return stateChangeTime;
    }

    /**
     * Returns the task coordinator associated with the RN
     */
    public TaskCoordinator getTaskCoordinator() {
        return (repNodeService != null) ?
            repNodeService.getTaskCoordinator() :
             /* Dummy for unit test, it's unused */
            new TaskCoordinator(logger, null /* envImpl */,
                                Collections.emptySet());
    }

    public ResourceTracker getAggregateThroughputTracker() {
        return (repNodeService != null) ?
                repNodeService.getAggregateThroughputTracker() :
                /* Dummy for unittest, it's unused  */
                new ResourceTracker() {

                    @Override
                    public int addWriteBytes(int bytes, int nIndexWrites) {
                        throw new UnsupportedOperationException
                            ("Method not implemented: " + "addWriteBytes");
                    }

                    @Override
                    public int addReadBytes(int bytes, boolean isAbsolute) {
                        throw new UnsupportedOperationException
                            ("Method not implemented: " + "addReadBytes");
                    }

                    @Override
                    public int getReadKBToAdd(int bytes, boolean isAbsolute) {
                        throw new UnsupportedOperationException
                            ("Method not implemented: " + "getReadKBToAdd");
                    }
                };
    }

    public Topology getTopology() {
        return topoManager.getTopology();
    }

    /**
     * Returns the local topology for this node. This should only be used to
     * direct client requests and should NEVER be sent to another node.
     */
    public Topology getLocalTopology() {
        return topoManager.getLocalTopology();
    }

    /**
     * Redirect to the master balance manager.
     *
     * @see RepNodeAdmin#initiateMasterTransfer
     */
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit) {
        return masterBalanceManager.
            initiateMasterTransfer(replicaId, timeout, timeUnit);
    }

    /**
     * Returns the migration manager.
     * @return the migration manager
     */
    public MigrationManager getMigrationManager() {
        return migrationManager;
    }

    /* -- Admin partition migration methods -- */

    /**
     * Starts a partition migration. This must be called
     * after the node is initialized. This method will start a target thread
     * which requests the partition form the source node.
     *
     * @param partitionId ID of the partition to migrate
     * @param sourceRGId ID the current rep group of the partition
     * @return the migration state
     */
    public MigrationState migratePartition(PartitionId partitionId,
                                           RepGroupId sourceRGId) {

        /* Source group is this group? Bad. */
        if (repNodeId.getGroupId() == sourceRGId.getGroupId()) {
            return new MigrationState(PartitionMigrationState.ERROR,
                new IllegalArgumentException("Invalid source " + sourceRGId));
        }

        /* If its already here, then we are done. */
        if (partitionManager.isPresent(partitionId)) {
            return new MigrationState(PartitionMigrationState.SUCCEEDED);
        }
        return migrationManager.migratePartition(partitionId, sourceRGId);
    }

    /**
     * Returns the state of a partition migration. A return of SUCCEEDED
     * indicates that the specified partition is complete and durable.
     *
     * If the return value is PartitionMigrationState.ERROR,
     * canceled(PartitionId, RepGroupId) must be invoked on the migration
     * source repNode.
     *
     * @param partitionId a partition ID
     * @return the migration state
     */
    public MigrationState getMigrationState(PartitionId partitionId) {

        /* If its already here, then we are done. */
        if (partitionManager.isPresent(partitionId)) {
            return new MigrationState(PartitionMigrationState.SUCCEEDED);
        }
        final MigrationState state =
                            migrationManager.getMigrationState(partitionId);

        /*
         * If ERROR, re-check if the partition is here, as a migration
         * could have completed just after the check above.
         */
        if (state.getPartitionMigrationState().
                equals(PartitionMigrationState.ERROR)) {
            if (partitionManager.isPresent(partitionId)) {
                return new MigrationState(PartitionMigrationState.SUCCEEDED);
            }
        }
        return state;
    }

    /**
     * Requests that a partition migration for the specified partition
     * be canceled. Returns the migration state if there was a migration in
     * progress, otherwise null is returned.
     * If the migration can be canceled it will be stopped and
     * PartitionMigrationState.ERROR is returned. If the migration has passed
     * the "point of no return" in the Transfer of Control protocol or is
     * already completed PartitionMigrationState.SUCCEEDED is returned.
     * All other states indicate that the cancel should be retried.
     *
     * @param partitionId a partition ID
     * @return a migration state or null
     */
    public MigrationState canCancel(PartitionId partitionId) {
        return migrationManager.canCancel(partitionId);
    }

    /**
     * Cleans up a source migration stream after a cancel or error.
     *
     * @param partitionId a partition ID
     * @param targetRGId the target rep group ID
     * @return true if the cleanup was successful
     */
    public boolean canceled(PartitionId partitionId, RepGroupId targetRGId) {
        return migrationManager.canceled(partitionId, targetRGId);
    }

    /**
     * Gets the status of partition migrations on this node.
     *
     * @return the partition migration status
     */
    public PartitionMigrationStatus[] getMigrationStatus() {
        return migrationManager.getStatus();
    }

    /**
     * Wait for the RN to become consistent. Serves
     * as an indication that the RN is both alive and synced up.
     */
    public boolean awaitConsistency(int timeout, TimeUnit timeoutUnit) {

        final long timeoutMs = TimeUnit.MILLISECONDS.convert(timeout,
                                                             timeoutUnit);
        final ReplicatedEnvironment env = envManager.getEnv(timeoutMs);

        if (env == null) {

            /*
             * Environment handle unavailable, return false since we can't
             * determine its consistency.
             */
            return false;
        }

        /*
         * The RN is considered consistent when the lag is less than the
         * ACK timeout.
         */
        final long permissibleLagMs =
                envManager.getReplicaAckTimeout(TimeUnit.MILLISECONDS);

        final TransactionConfig txnConfig = new TransactionConfig();

        final ReplicaConsistencyPolicy reachedTime =
            new TimeConsistencyPolicy(permissibleLagMs,
                                      TimeUnit.MILLISECONDS,
                                      timeout, timeoutUnit);
        txnConfig.setConsistencyPolicy(reachedTime);
        try {
            /* The transaction is just a mechanism to check consistency */
            final Transaction txn = env.beginTransaction(null, txnConfig);
            TxnUtil.abort(txn);
        } catch (ReplicaConsistencyException notReady) {
            logger.info(notReady.toString());
            return false;
        }

        return true;
    }

    public PartitionMigrationStatus
                                getMigrationStatus(PartitionId partitionId) {
        return migrationManager.getStatus(partitionId);
    }

    /* -- Unit tests -- */

    /**
     * Handles version changes when the version found in the environment does
     * not match the KVVersion.CURRENT. If localVersion is not null it is the
     * previous version of repNode. Note that localVersion may be a patch
     * version less than the CURRENT version (downgrade). If localVerion is null
     * this node is being initialized.
     *
     * @param localVersion the version currently in the environment or null
     */
    void versionChange(Environment env, KVVersion localVersion) {
        final Topology topo = readTopology(env);
        if (topo == null) {
            return;
        }
        if (topo.upgrade()) {
            writeTopology(topo, env);
        } else {
            TopologyManager.checkVersion(logger, topo);
        }
    }

    /**
     * Returns the sequence number associated with the metadata at the RN.
     * If the RN does not contain the metadata null is returned.
     *
     * @param type a metadata type
     *
     * @return the sequence number associated with the metadata
    */
    public Integer getMetadataSeqNum(MetadataType type) {
        if (type == MetadataType.TABLE) {
            /*
             * Return the sequence number of TableMetadata cached in
             * TableManager.
             */
            return tableManager.getTableMetadataSeqNum();
        }
        final Metadata<?> md = getMetadata(type);
        return (md == null) ? Metadata.EMPTY_SEQUENCE_NUMBER :
                              md.getSequenceNumber();
    }

    /**
     * Gets the metadata for the specified type. If the RN does not contain the
     * metadata null is returned.
     *
     * @param type a metadata type
     *
     * @return metadata or null
     */
    public Metadata<?> getMetadata(MetadataType type) {
        return getMetadata(type, SerialVersion.CURRENT);
    }

    /**
     * Gets the metadata for the specified type. If the RN does not contain the
     * metadata null is returned.
     *
     * @param type a metadata type
     * @param  serialVersion the version number that the Metadata is compatible
     * with
     *
     * @return metadata or null
     */
    public Metadata<?> getMetadata(MetadataType type, short serialVersion) {
        switch (type) {
            case TOPOLOGY:
                return getTopology();
            case TABLE:
                return tableManager.getTableMetadata(serialVersion);
            case SECURITY:
                return securityMDManager.getSecurityMetadata();
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    /**
     * Gets metadata information for the specified type starting from the
     * specified sequence number.
     *
     * @param type a metadata type
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     */
    public MetadataInfo getMetadata(MetadataType type, int seqNum) {
        switch (type) {
            case TOPOLOGY:
                final Topology topo = getTopology();
                return (topo == null) ? TopologyInfo.EMPTY_TOPO_INFO :
                                        getTopology().getChangeInfo(seqNum);
            case TABLE:
                final TableMetadata md = tableManager.getTableMetadata();
                return (md == null) ? TableChangeList.EMPTY_TABLE_INFO :
                                      md.getChangeInfo(seqNum);
            case SECURITY:
                final SecurityMetadata securityMD =
                    securityMDManager.getSecurityMetadata();
                return (securityMD == null) ?
                       SecurityMetadataInfo.EMPTY_SECURITYMD_INFO :
                       securityMD.getChangeInfo(seqNum);
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    /**
     * Gets metadata information for the specified type and key starting from
     * the specified sequence number. If the metadata is not present or cannot
     * be accessed, null is returned.
     *
     * @param type a metadata type
     * @param key a metadata key
     * @param seqNum a sequence number
     * @param  serialVersion the version number that the Metadata is compatible
     * with
     *
     * @return metadata info describing the changes or null
     *
     * @throws UnsupportedOperationException if the operation is not supported
     * by the specified metadata type
     */
    public MetadataInfo getMetadata(MetadataType type,
                                    MetadataKey key,
                                    int seqNum,
                                    short serialVersion) {
        switch (type) {
            case TOPOLOGY:
                /* Topology doesn't rely on MetadataKey for lookup */
                throw new
                    UnsupportedOperationException("Operation not supported " +
                                                  "for metadata type: " + type);
            case SECURITY:
                /* Security doesn't rely on MetadataKey for lookup */
                throw new
                    UnsupportedOperationException("Operation not supported " +
                                                  "for metadata type: " + type);
            case TABLE:
                final TableMetadata md =
                                tableManager.getTableMetadata(serialVersion);
                if (md == null) {
                    return null;
                }
                return md.getInfo(key);
        }
        throw new IllegalArgumentException("Unknown metadata type: " + type);
    }

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param newMetadata the latest metadata
     *
     * @return true if the update is successful
     */
    public boolean updateMetadata(Metadata<?> newMetadata) {
        switch (newMetadata.getType()) {
            case TOPOLOGY:
                return topoManager.update((Topology)newMetadata);
            case TABLE:
                return tableManager.updateMetadata(newMetadata);
            case SECURITY:
                return securityMDManager.update((SecurityMetadata)newMetadata);
        }
        throw new IllegalArgumentException("Unknown metadata: " + newMetadata);
    }

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param metadataInfo describes the changes to be applied
     *
     * @return the post-update metadata sequence number at the node
     */
    public int updateMetadata(MetadataInfo metadataInfo) {
        switch (metadataInfo.getType()) {
            case TOPOLOGY:
                return updateTopology((TopologyInfo)metadataInfo);
            case TABLE:
                return tableManager.updateMetadata(metadataInfo);
            case SECURITY:
                return securityMDManager.update(
                    (SecurityMetadataInfo)metadataInfo);
        }
        throw new IllegalArgumentException("Unknown metadata: " + metadataInfo);
    }

    public boolean addIndexComplete(String namespace,
                                    String indexId,
                                    String tableName) {
        return tableManager.addIndexComplete(namespace, indexId, tableName);
    }

    public boolean removeTableDataComplete(String namespace,
                                           String tableName) {
        return tableManager.removeTableDataComplete(namespace, tableName);
    }

    /**
     * Gets the secondary database for the specified index. Throws a
     * RNUnavailableException if the database is not found.
     *
     * @param indexName the index name
     * @param tableName the table name
     *
     * @return a secondary database
     *
     * @throws RNUnavailableException if the secondary DB is not found
     */
    public SecondaryDatabase getIndexDB(String namespace,
                                        String indexName,
                                        String tableName) {
        final SecondaryDatabase db = tableManager.getIndexDB(namespace,
                                                             indexName,
                                                             tableName);
        if (db != null) {
            return db;
        }

        /*
         * The secondary db was not found. This could be because the table
         * metadata has not been initialized, the metadata is out of date, or
         * the secondary DB has not yet been opened. Figure out what it is, and
         * throw RNUnavailableException.
         */
        String message;
        final TableMetadata md =  tableManager.getTableMetadata();

        if (md == null) {
            message = "Table metadata not yet initialized";
        } else if (getIndex(namespace, indexName, tableName) == null) {
            message = "Index " + indexName + " not present on RepNode, table " +
                      "metadata seq#: " + md.getSequenceNumber();
        } else {
            message = "Secondary database for " + indexName + " not yet " +
                      "initialized";
        }
        throw new RNUnavailableException(message);
    }

    public LoginManager getLoginManager() {
        return repNodeService == null ? null : repNodeService.getLoginManager();
    }

    public KVStore getKVStore() {
        if (repNodeService == null) {
            return null;
        }
        KVStoreCreator creator = repNodeService.getKVStoreCreator();
        return creator.getKVStore();
    }

    /**
    * Gets the table instance for the specified ID. If no table is defined,
    * or the table is being deleted null is returned.
    *
    * @param tableId a table ID
    * @return the table instance or null
    * @throws RNUnavailableException is the table metadata is not yet
    * initialized
    */
    public TableImpl getTable(long tableId) {
        return tableManager.getTable(tableId);
    }

   /**
    * Gets the specified table with an optional resource cost. If the table
    * is not found, null is returned. The specified cost will be charged
    * against the table's resource limits. If the cost is greater than 0
    * and the table has resource limits and those limits have been exceeded,
    * either by this call, or by other table activity a ResourceLimitException
    * will be thrown.
    *
    * @return the table instance or null
    * @throws RNUnavailableException is the table metadata is not yet
    * initialized
    */
    public TableImpl getTable(String namespace, String tableName, int cost) {
        return tableManager.getTable(namespace, tableName, cost);
    }

    public ResourceInfo exchangeResourceInfo(long sinceMillis,
                                        Collection<UsageRecord> usageRecords) {
        return tableManager.getResourceInfo(sinceMillis, usageRecords,
                                            repNodeId, getTopoSequenceNumber());
    }

    /**
     * Gets a r2-compat table instance with the specified name. If no r2-compat
     * table is defined or the RnNis not initialized, null is returned.
     *
     * @param tableName the name of r2-compat table
     * @return the table instance
     */
    public TableImpl getR2CompatTable(String tableName) {
        return tableManager.getR2CompatTable(tableName);
    }

    /**
     * Return the named index. If the index or table does not exist, or the
     * table metadata cannot be accessed null is returned.
     *
     * @param indexName the index name
     * @param tableName the table name
     * @return the named index or null
     */
    public Index getIndex(String namespace,
                          String indexName,
                          String tableName) {
        final TableMetadata md =  tableManager.getTableMetadata();
        if (md != null) {
            final TableImpl table = md.getTable(namespace, tableName);
            if (table != null) {
                return table.getIndex(indexName);
            }
        }
        return null;
    }

    /**
     * Indicate whether RepNode is stopped or not.
     * @return true if RepNode is stopped, otherwise return false
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * Return store Kerberos service principal names of all storage nodes.
     *
     * @return the list of service principal names with storage node Ids
     */
    public KerberosPrincipals getKerberosPrincipals() {
        final SecurityMetadata secMd = securityMDManager.getSecurityMetadata();
        if (secMd != null) {
            final List<SNKrbInstance> instanceNames = new ArrayList<>();
            for (KerberosInstance instance : secMd.getAllKrbInstanceNames()) {
                instanceNames.add(new SNKrbInstance(
                    instance.getInstanceName(),
                    instance.getStorageNodeId().getStorageNodeId()));
            }
            return new KerberosPrincipals(instanceNames.toArray(
                new SNKrbInstance[instanceNames.size()]));
        }
        return new KerberosPrincipals(null);
    }

    /**
     * Start a asynchronous network restore from given source node. The task
     * would close the database handles to prevent accessing to the environment
     * that is running network restore. After restore complete the environment
     * will be renewed, database handles will be re-established.
     *
     * @param sourceNode Network restore source node
     * @param retainOriginalLogFile if retain the original log files after
     * network restore
     * @param minVLSN minimual VLSN the network restore will cover
     * @return true if the network restore is started, false if there is a
     * network restore task already running.
     */
    public boolean startAsyncNetworkRestore(RepNodeId sourceNode,
                                            boolean retainOriginalLogFile,
                                            long minVLSN) {
        return envManager.startAsyncNetworkRestore(sourceNode,
                                                   retainOriginalLogFile,
                                                   minVLSN);
    }

    /**
     * Query status of network restore if there is a asynchronous network
     * restore task is running on this node.
     * @return status of a running network restore task, or null if no restore
     * task is running.
     */
    public NetworkRestoreStatus getAsyncNetworkRestoreStatus() {
        return envManager.getAsyncNetworkRestoreStatus();
    }

    /**
     * Reads the topology from the RNs persistent store. If the topology has
     * not yet been initialized null is returned.
     *
     * @return the topology or null
     */
    private synchronized Topology readTopology(Environment env) {
        return readTopology(env, null /* dbConfig */);
    }

    /*
     * Public static method for standalone utilities. Internal callers should use synchronized method to
     * prevent concurrent database opens.
     */
    public static Topology readTopology(Environment env,
                                        DatabaseConfig dbConfig) {
        Topology topo = null;
        try (final Database db = getTopoDatabase(env, dbConfig)) {
            final DatabaseEntry value = new DatabaseEntry();
            final OperationStatus status = db.get(null, TOPOLOGY_KEY_ENTRY,
                                                  value,
                                                  LockMode.READ_UNCOMMITTED);
            if (status == OperationStatus.SUCCESS) {
                topo = SerializationUtil.getObject(value.getData(),
                                                   Topology.class);
            }
        }
        return topo;
    }

    /**
     * Writes the specified topology to the RNs persistent store.
     *
     * @param topo the topology to store
     */
    private synchronized void writeTopology(Topology topo, Environment env) {
        writeTopology(topo, env, null /* dbConfig */);
    }

    /*
     * Public static method for standalone utilities. Internal callers should use synchronized method to
     * prevent concurrent database opens.
     */
    public static void writeTopology(Topology topo,
                                     Environment env,
                                     DatabaseConfig dbConfig) {
        try (final Database db = getTopoDatabase(env, dbConfig)) {
            final DatabaseEntry value =
                            new DatabaseEntry(SerializationUtil.getBytes(topo));
            db.put(null, TOPOLOGY_KEY_ENTRY, value);
        }
    }

    /**
     * Gets the non-replicated database used to persist the topology. If the
     * database does not exist, one will be created. It's the caller's
     * responsibility to close the handle.
     */
    private static Database getTopoDatabase(Environment env,
                                            DatabaseConfig dbConfig) {
        if (dbConfig == null) {
            dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            dbConfig.setReplicated(false);
        }
        return env.openDatabase(null, TOPOLOGY_DB_NAME, dbConfig);
    }

    static Database openTopoDatabase(Environment env,
                                     DatabaseConfig dbConfig,
                                     Transaction txn) {
        return env.openDatabase(txn, TOPOLOGY_DB_NAME, dbConfig);
    }

    static Topology getTopology(Database db) {
        Topology topo = null;
        final DatabaseEntry value = new DatabaseEntry();
        final OperationStatus status = db.get(null, TOPOLOGY_KEY_ENTRY, value,
                LockMode.READ_UNCOMMITTED);
        if (status == OperationStatus.SUCCESS) {
            topo = SerializationUtil.getObject(value.getData(), Topology.class);
        }
        return topo;
    }

    public interface ReplicationStateListener {
        void doNotify(StateChangeEvent sce);
    }

    /**
     * Verify data for this repnode.
     */
    public VerificationInfo startAndPollVerifyData(VerificationOptions options,
                                                   int planId) {
        return envManager.startAndPollVerify(options, planId);
    }

    /**
     * Interrupt the verification.
     */
    public boolean interruptVerify() {
        return envManager.interruptVerify();
    }

    /**
     * Returns {@code true} if the rep node environment has positive log size.
     *
     * The method returns true if the environment is not ready.
     */
    public boolean hasAvailableLogSize() {
        final Long availableLogSize = getAvailableLogSize();
        if (availableLogSize == null) {
            return true;
        }
        return availableLogSize > 0;
    }

    /**
     * Returns the available log space of the rep node environment, {@code
     * null} if the environment is not ready.
     */
    public Long getAvailableLogSize() {
        final ReplicatedEnvironment env = envManager.getEnv(1);
        if (env == null) {
            return null;
        }
        return env.getStats((new StatsConfig()).setFast(true)).
            getAvailableLogSize();
    }

    /**
     * Waits for table operations to complete.
     * See RequestHandlerImpl.awaitTableOps.
     */
     public int awaitTableOps(int seqNum) {
         /* repNodeService may be null in unit tests */
         return (repNodeService == null) ? 0 :
                          repNodeService.getReqHandler().awaitTableOps(seqNum);
     }

    /**
     * Gets store version. If not set yet or if it is in an illegal format (this
     * should never happen), return the {@link KVVersion#PREREQUISITE_VERSION}.
     *
     * @return the store version
     */
    public KVVersion getStoreVersion() {
        if (storeVersionOverride != null) {
            logger.warning("Store version override " +
                           storeVersionOverride.getNumericVersionString());
            return storeVersionOverride;
        }

        KVVersion ver = KVVersion.PREREQUISITE_VERSION;
        /* repNodeService may be null in unit tests */
        if (repNodeService == null) {
            return KVVersion.CURRENT_VERSION;
        }

        try {
            final String sv =
                repNodeService.getParams().getGlobalParams().getStoreVersion();
            if (sv != null) {
                ver = KVVersion.parseVersion(sv);
            }
        } catch (Exception e) {
            logger.warning("Exception getting store version from params: " +
                           e.getMessage());
        }
        return ver;
    }

    /**
     * Returns true if the RN is serving stream client
     *
     * @return true if the RN is serving stream client, false otherwise.
     */
    boolean servStreamClient() {
        if(stopped) {
            return false;
        }

        /* check env to see if master and if there are any stream feeders */
        final ReplicatedEnvironment repEnv = envManager.getEnv(1000);
        if ((repEnv == null) || !repEnv.getState().isMaster()) {
            return false;
        }

        final RepImpl repImpl = RepInternal.getRepImpl(repEnv);
        if (repImpl.getRepNode() == null || !repImpl.getRepNode().isMaster()) {
            return false;
        }

        final FeederManager fm = repImpl.getRepNode().feederManager();
        if (fm == null) {
            return false;
        }

        final long numStrFeeders = fm.activeReplicasMap().values().stream()
                                     .filter(f -> f.getReplicaNode()
                                                   .getType()
                                                   .isExternal())
                                     .count();
        logger.fine(() -> "# active stream feeders on " + getRepNodeId() +
                          " is "  + numStrFeeders);
        return numStrFeeders > 0;
    }

    /**
     * Checks if stream client and give grace period to stream client to
     * catch up before stopping and destroying the RN
     */
   void waitForStream() {
       /*
        * check if stream client and give grace period to stream client to
        * catch up before stopping and destroying the RN
        */
       final int gid = repNodeId.getGroupId();
       final TableMetadata tm = tableManager.getTableMetadata();

       final int pollIntvMs = 5000;
       final long timeoutMs = getRepNodeParams().getContractWaitDurationMs();
       final Set<Table> allCkptTbl =
           tm.getTables().values().stream()
             .filter(CheckpointTableManager::isCkptTable)
             .collect(Collectors.toSet());
       final long vlsn = partitionManager.getPartGenTable()
                                         .getHighVLSNClosedGen();
       logger.info("Wait for " + allCkptTbl.size() +
                   " streams clients (ckpt tables: " + allCkptTbl +
                   ") to catch up in " + timeoutMs + " ms, high vlsn: " + vlsn);

       final boolean success = new PollCondition(pollIntvMs, timeoutMs) {
           @Override
           protected boolean condition() {
               /*
                * check each checkpoint table that there is no checkpoint
                * that RN needs wait for, meaning all streams caught up.
                */
               return allCkptTbl.stream()
                                .allMatch(t -> isCkptHigh(t, gid, vlsn));
           }
       }.await();

       if (!success) {
           /* time out */
           logger.warning(() -> "Timeout in waiting for streams to catch up, " +
                                "continue. List of checkpoint tables of " +
                                "delayed streams:" +
                                allCkptTbl.stream()
                                          .filter(t -> !isCkptHigh(t, gid,
                                                                   vlsn))
                                          .collect(Collectors.toSet()));
       }
   }

    /* Checks if the shard checkpoint has reached the high VLSN */
    private boolean isCkptHigh(Table table, int gid, long highVLSN) {
        final long vlsn = CheckpointTableManager.getCkptVLSN(getKVStore(),
                                                             table, gid);
        /* return true if ckpt vlsn has reached or gone beyond the high vlsn */
        return highVLSN <= vlsn;
    }
}
