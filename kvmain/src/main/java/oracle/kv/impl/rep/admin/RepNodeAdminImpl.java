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

package oracle.kv.impl.rep.admin;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REP_NODE_ADMIN_TYPE_FAMILY;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.fault.ClientAccessException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.mgmt.RepNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.NetworkRestoreStatus;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.util.DbBackup;

/**
 * The implementation of the RN administration protocol.
 */
@SecureAPI
public class RepNodeAdminImpl
    extends VersionedRemoteImpl implements RepNodeAdmin {

    /**
     *  The repNode being administered
     */
    private final RepNode repNode;
    private final RepNodeService repNodeService;

    /**
     * The fault handler associated with the service.
     */
    private final RepNodeAdminFaultHandler faultHandler;
    private final Logger logger;

    /**
     * The exportable/bindable version of this object
     */
    private RepNodeAdmin exportableRepNodeAdmin;

    private DbBackup dbBackup;

    /**
     * A conditional instance, created if the class can be found.
     */
    private RemoteTestInterface rti;
    private static final int REQUEST_QUIESCE_POLL_MS = 100;
    private static final int REQUEST_QUIESCE_MS = 10000;

    /* Set to true during shutdown when a shutdown request is in progress */
    private volatile boolean shutdownActive = false;

    private static final String TEST_INTERFACE_NAME=
        "oracle.kv.impl.rep.RepNodeTestInterface";

    public RepNodeAdminImpl(RepNodeService repNodeService, RepNode repNode) {

        this.repNodeService = repNodeService;
        this.repNode = repNode;
        rti = null;
        logger =
            LoggerUtils.getLogger(this.getClass(), repNodeService.getParams());

        faultHandler = new RepNodeAdminFaultHandler(repNodeService,
                                                    logger,
                                                    ProcessExitCode.RESTART);
    }

    private void assertRunning() {

        ServiceStatus status =
            repNodeService.getStatusTracker().getServiceStatus();
        if (status != ServiceStatus.RUNNING) {
            throw new IllegalRepNodeServiceStateException
                ("RepNode is not RUNNING, current status is " + status);
        }
    }

    /**
     * Create the test interface if it can be found.
     */
    private void startTestInterface() {
        try {
            Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            Constructor<?> c = cl.getConstructor(repNodeService.getClass());
            rti = (RemoteTestInterface) c.newInstance(repNodeService);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) {
        }
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {

                /*
                 * Make sure that the service is running so that we know the
                 * environment has already been set.  Otherwise, we may have
                 * started initializing the environment, meaning that these new
                 * parameters would be missed.
                 */
                assertRunning();

                repNodeService.newParameters();
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                repNodeService.newGlobalParameters();
            }
        });
    }

    /*
     * Note that the topology related update operations, including updateTopology
     * and updateMetadata(Topology), require only USRVIEW as basic authentication
     * check rather than SYSOPER in that of Admin. That's because the
     * RepNodeStateUpdateThread on the client will rely on these operations to
     * perform crossing-shard topology propagation, who not always have the
     * SYSOPER privileges. The merely basic check leaves a security hole for
     * attack from clients with masquerade topology updates. This hole has been
     * addressed by using the signed topology updates as in SR[#23709].
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public Topology getTopology(AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Topology>() {

            @Override
            public Topology execute() {
                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                       repNode.getTopology() : null ;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                if (status != ServiceStatus.RUNNING) {
                    return 0;
                }

                final Topology topology = repNode.getTopology();
                return (topology != null) ?
                        topology.getSequenceNumber() :
                        Topology.EMPTY_SEQUENCE_NUMBER;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public LoadParameters getParams(AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                /* Service does not need to be in the running state. */
                return repNode.getAllParams();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void configure(final Set<Metadata<? extends MetadataInfo>> metadataSet,
                          AuthContext authCtx,
                          short serialVersion) {
        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                /*
                 * Ensure RepNode update topology first in order to open
                 * database handles before update other types of metadata.
                 */
                for (Iterator<Metadata<?>> iter = metadataSet.iterator();
                     iter.hasNext();) {
                    Metadata<?> md = iter.next();
                    if (md.getType().equals(MetadataType.TOPOLOGY)) {
                        repNode.updateMetadata(md);
                        iter.remove();
                        break;
                    }
                }

                for (Metadata<?> md : metadataSet) {
                    repNode.updateMetadata(md);
                }
            }
        });
    }

    /**
     * Shutdown the rep node service, without specifying the reason. Note that
     * invoking shutdown will result in a callback via the {@link #stop()
     * method}, which will unregister the admin from the registry. It should
     * not impact this remote call which is already in progress.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public void shutdown(final boolean force, final boolean checkStream,
                         AuthContext authCtx, short serialVersion)
        throws RemoteException {

        shutdown(force, checkStream, null /* reason */, authCtx,
                 serialVersion);
    }

    /**
     * Shutdown the rep node service and specify an optional reason. Note that
     * invoking shutdown will result in a callback via the {@link #stop()
     * method}, which will unregister the admin from the registry. It should
     * not impact this remote call which is already in progress.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void shutdown(final boolean force,
                         final boolean checkStream,
                         final String reason,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException {

        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                shutdownActive = true;
                try {
                    repNodeService.stop(force, checkStream, reason);
                } finally {
                    shutdownActive = false;
                }
            }
        });
    }

    /**
     * Starts up the admin component, binding its stub in the registry, so that
     * it can start accepting remote admin requests.
     *
     * @throws RemoteException
     */
    public void startup()
        throws RemoteException {

        faultHandler.execute
        (new ProcessFaultHandler.Procedure<RemoteException>() {

            @Override
            public void execute() throws RemoteException {
                final String kvsName = repNodeService.getParams().
                        getGlobalParams().getKVStoreName();
                final RepNodeParams rnp = repNodeService.getRepNodeParams();
                final StorageNodeParams snp =
                    repNodeService.getParams().getStorageNodeParams();

                final String csfName = ClientSocketFactory.
                        factoryName(kvsName,
                                    RepNodeId.getPrefix(),
                                    InterfaceType.ADMIN.interfaceName());

                RMISocketPolicy rmiPolicy = repNodeService.getParams().
                    getSecurityParams().getRMISocketPolicy();
                SocketFactoryPair sfp =
                    rnp.getAdminSFP(rmiPolicy,
                                    snp.getServicePortRange(),
                                    csfName, kvsName);

                if (sfp.getServerFactory() != null) {
                    sfp.getServerFactory().setConnectionLogger(logger);
                }
                initExportableRepNodeAdmin();
                repNodeService.rebind(
                    exportableRepNodeAdmin,
                    InterfaceType.ADMIN,
                    sfp.getClientFactory(),
                    sfp.getServerFactory(),
                    REP_NODE_ADMIN_TYPE_FAMILY,
                    RepNodeAdminResponder::new);
                logger.info("RepNodeAdmin registered");
                startTestInterface();
            }
        });
    }

    private void initExportableRepNodeAdmin() {
        try {
            exportableRepNodeAdmin =
                SecureProxy.create(
                    RepNodeAdminImpl.this,
                    repNodeService.getRepNodeSecurity().getAccessChecker(),
                    faultHandler);
            logger.info(
                "Successfully created secure proxy for the repnode admin");
        } catch (ConfigurationException ce) {
            logger.info("Unable to create proxy: " + ce + " : " +
                        ce.getMessage());
            throw new IllegalStateException("Unable to create proxy", ce);
        }
    }

    /**
     * Unbind the admin entry from the registry.
     *
     * If any exceptions are encountered, during the unbind, they are merely
     * logged and otherwise ignored, so that other components can continue
     * to be shut down.
     */
    public void stop() {
        try {
            repNodeService.unbind(exportableRepNodeAdmin,
                                  RegistryUtils.InterfaceType.ADMIN);
        } catch (RemoteException e) {
            /* Ignore */
        }
        logger.info("RepNodeAdmin stopping");
        if (rti != null) {
            try {
                rti.stop(SerialVersion.CURRENT);
            } catch (RemoteException e) {
                /* Ignore */
            }

            /*
             * Wait for the admin requests to quiesce within the
             * requestQuiesceMs period now that new admin requests have been
             * blocked.
             */
            final boolean quiesced =
                new PollCondition(REQUEST_QUIESCE_POLL_MS,
                                  REQUEST_QUIESCE_MS) {

                @Override
                protected boolean condition() {
                    return faultHandler.getActiveRequests() ==
                           (shutdownActive ? 1 : 0);
                }

            }.await();

            if (!quiesced) {
                logger.info(faultHandler.getActiveRequests() +
                            " admin requests were active on close.");
            }
        }
    }

    @Override
    @PublicMethod
    public RepNodeStatus ping(AuthContext authCtx, short serialVersion) {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<RepNodeStatus>() {

            @Override
            public RepNodeStatus execute() {
                ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();
                State state = State.DETACHED;
                long currentVLSN = 0;
                ReplicatedEnvironmentStats repEnvStats = null;
                EnvironmentStats envStats = null;
                long availableLogSize = 0;
                long usedLogSize = 0;
                boolean envStatsAvailable = false;
                try {
                    ReplicatedEnvironment env = repNode.getEnv(1);
                    if (env != null) {
                        final RepImpl repImpl = RepInternal.getRepImpl(env);

                        /* May be null if env is invalidated */
                        final VLSNIndex vlsnIndex = (repImpl == null) ?
                                null :
                                RepInternal.getRepImpl(env).getVLSNIndex();
                        /* May be null if DETACHED. */
                        currentVLSN =  (vlsnIndex == null) ?
                                NULL_VLSN :
                                vlsnIndex.getRange().getLast();
                        try {
                            state = env.getState();
                        } catch (IllegalStateException iae) {
                            /* State cannot be queried if detached. */
                            state = State.DETACHED;
                        }
                        try {
                            repEnvStats = env.getRepStats(StatsConfig.DEFAULT);
                            /*
                             * Retrieving dynamic availableLogSize and
                             * totalLogSize for RepNode.
                             */
                            envStats = env.getStats(StatsConfig.DEFAULT);
                            if (envStats != null) {
                                availableLogSize =
                                    envStats.getAvailableLogSize();
                                usedLogSize = envStats.getTotalLogSize();
                                envStatsAvailable = true;
                            }
                        } catch (IllegalStateException iae) {
                            /* Can fail if environment has been closed */
                        }
                    }
                } catch (EnvironmentFailureException ignored) {

                    /*
                     * The environment could be invalid.
                     */
                }
                final String haHostPort =
                    repNode.getRepNodeParams().getJENodeHostPort();
                final String enabledRequestType =
                    repNode.getRepNodeParams().getEnabledRequestType().name();


                return new RepNodeStatus(status, state,
                                         currentVLSN, haHostPort,
                                         enabledRequestType,
                                         repNode.getMigrationStatus(),
                                         repEnvStats,
                                         availableLogSize, usedLogSize,
                                         envStatsAvailable,
                                         repNode.getNetworkRestoreStats(),
                                         repNode.getIsAuthoritativeMaster(),
                                         repNode.getStorageType(),
                                         repNode.getServiceStartTime(),
                                         repNode.getStateChangeTime());
            }
        });
    }

    @Override
    @PublicMethod
    public long getVlsn(AuthContext authCtx, short serialVersion) {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Long>() {

            @Override
            public Long execute() {
                long currentVLSN = 0;
                try {
                    ReplicatedEnvironment env = repNode.getEnv(1);
                    if (env != null) {
                        final RepImpl repImpl = RepInternal.getRepImpl(env);

                        /* May be null if env is invalidated */
                        final VLSNIndex vlsnIndex = (repImpl == null) ?
                                null :
                                RepInternal.getRepImpl(env).getVLSNIndex();
                        /* May be null if DETACHED. */
                        currentVLSN =  (vlsnIndex == null) ?
                                NULL_VLSN :
                                vlsnIndex.getRange().getLast();
                    }
                } catch (EnvironmentFailureException ignored) {

                    /*
                     * The environment could be invalid.
                     */
                }
                return currentVLSN;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public RepNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<RepNodeInfo>() {

            @Override
            public RepNodeInfo execute() {
                /* Service does not need to be in the running state. */
                return new RepNodeInfo(repNode);
            }
        });
    }

    @Override
    @PublicMethod
    public String getHAHostPort(AuthContext authCtx, short serialVersion) {
        /* Service does not need to be in the running state. */
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<String>)
            () -> repNode.getRepNodeParams().getJENodeHostPort());
    }

    @Override
    @PublicMethod
    public ReplicatedEnvironment.State
        getReplicationState(AuthContext authCtx, short serialVersion) {
        /* Service does not need to be in the running state. */
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<ReplicatedEnvironment.State>)
            () -> {
                try {
                    final ReplicatedEnvironment env = repNode.getEnv(1);
                    if (env != null) {
                        try {
                            return env.getState();
                        } catch (IllegalStateException iae) {
                            /* State cannot be queried if detached. */
                        }
                    }
                } catch (EnvironmentFailureException ignored) {
                    /* The environment could be invalid */
                }
                return State.DETACHED;
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public String [] startBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<String []>() {

            @Override
            public String [] execute() {
                assertRunning();

                if (dbBackup != null) {
                    logger.warning("startBackup: dbBackup not null");
                    dbBackup.endBackup();
                }

                /*
                 * TODO: consider a checkpoint...
                 */
                ReplicatedEnvironment env = repNode.getEnv(1);
                if (env == null) {
                    throw new
                        OperationFaultException("Environment unavailable");
                }
                logger.info("startBackup: starting backup/snapshot");
                dbBackup = new DbBackup(env);
                dbBackup.startBackup();
                return dbBackup.getLogFilesInBackupSet();
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public long stopBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Long>() {

            @Override
            public Long execute() {
                assertRunning();
                logger.info("Ending backup/snapshot");
                long lastFile = -1;
                if (dbBackup != null) {
                    lastFile = dbBackup.getLastFileInBackupSet();
                    dbBackup.endBackup();
                    dbBackup = null;
                }
                return lastFile;
            }

        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean updateMemberHAAddress(final String groupName,
                                         final String targetNodeName,
                                         final String targetHelperHosts,
                                         final String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {

            @Override
            public Boolean execute() {
                assertRunning();
                try {
                    repNodeService.updateMemberHAAddress(groupName,
                                                         targetNodeName,
                                                         targetHelperHosts,
                                                         newNodeHostPort);
                    return true;
                } catch (UnknownMasterException | ReplicaStateException e) {
                    return false;
                }
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean deleteMember(final String groupName,
                                final String targetNodeName,
                                final String targetHelperHosts,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.Operation<Boolean, RemoteException>() {

            @Override
            public Boolean execute() {
                assertRunning();
                try {
                    repNodeService.deleteMember(groupName,
                                                targetNodeName,
                                                targetHelperHosts);
                    return true;
                } catch (UnknownMasterException | ReplicaStateException e){
                    return false;
                }
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean initiateMasterTransfer(final RepNodeId replicaId,
                                          final int timeout,
                                          final TimeUnit timeUnit,
                                          final AuthContext authCtx,
                                          final short serialVersion)
        throws RemoteException {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Boolean>() {

                @Override
                public Boolean execute() {
                    final ServiceStatus status = repNodeService.
                        getStatusTracker().getServiceStatus();

                    return (status != ServiceStatus.RUNNING) ?
                        false :
                        repNode.initiateMasterTransfer(replicaId,
                                                       timeout, timeUnit);
                }
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public MigrationState migratePartitionV2(final PartitionId partitionId,
                                             final RepGroupId sourceRGId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<MigrationState>() {

            @Override
            public MigrationState execute() {
                assertRunning();
                return repNode.migratePartition(partitionId, sourceRGId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public MigrationState getMigrationStateV2(final PartitionId partitionId,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<MigrationState>() {

            @Override
            public MigrationState execute() {
                try {
                    assertRunning();
                    return repNode.getMigrationState(partitionId);

                } catch (IllegalRepNodeServiceStateException irnsse) {
                    return new MigrationState(PartitionMigrationState.UNKNOWN,
                                              irnsse);
                }
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public MigrationState canCancelV2(final PartitionId partitionId,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<MigrationState>() {

            @Override
            public MigrationState execute() {
                try {
                    assertRunning();
                    return repNode.canCancel(partitionId);
                } catch (IllegalRepNodeServiceStateException irnsse) {
                    return new MigrationState(PartitionMigrationState.UNKNOWN,
                                              irnsse);
                }
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean canceled(final PartitionId partitionId,
                            final RepGroupId targetRGId,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                final ServiceStatus status = repNodeService.
                        getStatusTracker().getServiceStatus();

                return (status != ServiceStatus.RUNNING) ?
                        false : repNode.canceled(partitionId, targetRGId);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public PartitionMigrationStatus
                    getMigrationStatus(final PartitionId partitionId,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException {

        return faultHandler.
        execute(
           new ProcessFaultHandler.SimpleOperation<PartitionMigrationStatus>() {

            @Override
            public PartitionMigrationStatus execute() {
                assertRunning();
                return repNode.getMigrationStatus(partitionId);
            }
        });
    }

    /**
     * This method is deprecated as of 21.2. It should no longer be called,
     * because calls would have come from the SN on the same host, which should
     * have been upgraded at the same time and so should no longer call this
     * method. But leave the method as a stub for now just in case.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public void installStatusReceiver(RepNodeStatusReceiver receiver,
                                      AuthContext authCtx,
                                      short serialVersion) {
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean awaitConsistency(final long targetTime, /* unused */
                                    final int timeout,
                                    final TimeUnit timeoutUnit,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                return repNode.awaitConsistency(timeout, timeoutUnit);
            }
        });
    }

    @Override
    /*
     * TOPOLOGY requires USRVIEW, TABLE requires DBVIEW, and others require
     * INTLOPER.
     */
    @SecureInternalMethod
    public int getMetadataSeqNum(final MetadataType type,
                                 final AuthContext authCtx,
                                 short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                /* Check access rights */
                checkAccessPermission(new MetadataAccessContext(type));

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                if (status != ServiceStatus.RUNNING) {
                    return Metadata.EMPTY_SEQUENCE_NUMBER;
                }

                return repNode.getMetadataSeqNum(type);
            }
        });
    }

    @Override
    /*
     * TOPOLOGY requires USRVIEW, TABLE requires DBVIEW, and others require
     * INTLOPER.
     */
    @SecureInternalMethod
    public Metadata<?> getMetadata(final MetadataType type,
                                   final AuthContext authCtx,
                                   short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Metadata<?>>() {

            @Override
            public Metadata<?> execute() {
                /* Check access rights */
                checkAccessPermission(new MetadataAccessContext(type));

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                    repNode.getMetadata(type, serialVersion) : null ;
            }
        });
    }

    @Override
    /*
     * TOPOLOGY requires USRVIEW, TABLE requires DBVIEW, and others require
     * INTLOPER.
     */
    @SecureInternalMethod
    public MetadataInfo getMetadata(final MetadataType type,
                                    final int seqNum,
                                    final AuthContext authCtx,
                                    short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<MetadataInfo>() {

            @Override
            public MetadataInfo execute() {
                /* Check access rights */
                checkAccessPermission(new MetadataAccessContext(type));

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                       repNode.getMetadata(type, seqNum) : null ;
            }
        });
    }

    @Override
    /*
     * TOPOLOGY requires USRVIEW, TABLE requires DBVIEW, and others require
     * INTLOPER.
     */
    @SecureInternalMethod
    public MetadataInfo getMetadata(final MetadataType type,
                                    final MetadataKey key,
                                    final int seqNum,
                                    final AuthContext authCtx,
                                    short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<MetadataInfo>() {

            @Override
            public MetadataInfo execute() {
                /* Check access rights */
                checkAccessPermission(new MetadataAccessContext(type));

                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();

                return (status == ServiceStatus.RUNNING) ?
                    repNode.getMetadata(type, key, seqNum, serialVersion) :
                    null ;
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.DBVIEW })
    public MetadataInfo getTableById(final long tableId,
                                     final AuthContext authCtx,
                                     short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<MetadataInfo>() {
            @Override
            public MetadataInfo execute() {
                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();
                if (status == ServiceStatus.RUNNING) {
                    TableImpl table = repNode.getTable(tableId);
                   if (table != null &&
                       table.getRequiredSerialVersion() > serialVersion) {
                       throw new OperationFaultException(
                       "The table's minimum required version is greater "
                       + "than the serial version. "
                       + "The table's minimum required version is "
                       + table.getRequiredSerialVersion()
                       + " but the serial version is " + serialVersion);
                    }
                    return table;
                }
                return null;
            }
         });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.DBVIEW })
    public MetadataInfo getTable(final String namespace,
                                 final String tableName,
                                 final int cost,
                                 AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<MetadataInfo>() {
            @Override
            public MetadataInfo execute() {
                final ServiceStatus status =
                    repNodeService.getStatusTracker().getServiceStatus();
                if (status == ServiceStatus.RUNNING) {
                   TableImpl table = repNode.getTable(namespace,
                                                      tableName,
                                                      cost);
                   if (table != null &&
                       table.getRequiredSerialVersion() > serialVersion) {
                       throw new OperationFaultException(
                       "The table's minimum required version is greater "
                       + "than the serial version. "
                       + "The table's minimum required version is "
                       + table.getRequiredSerialVersion()
                       + " but the serial version is " + serialVersion);
                   }
                   return table;
                }
                return null;
            }
         });
    }

    @Override
    @SecureInternalMethod
    public void updateMetadata(final Metadata<?> newMetadata,
                               AuthContext authCtx,
                               short serialVersion) {

        faultHandler.execute(new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                assertRunning();

                /* Check access rights */
                checkAccessPermission(
                    new MetadataUpdateContext(newMetadata.getType()));
                if (!repNode.updateMetadata(newMetadata)) {
                    throw new
                       OperationFaultException("Update " +
                                               newMetadata.getType() +
                                               " metadata seq# " +
                                               newMetadata.getSequenceNumber() +
                                               " failed");
                }
            }
        });
    }

    @Override
    @SecureInternalMethod
    public int updateMetadata(final MetadataInfo metadataInfo,
                              AuthContext authCtx,
                              short serialVersion) {

        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                assertRunning();

                /* Check access rights */
                checkAccessPermission(
                    new MetadataUpdateContext(metadataInfo.getType()));
                return repNode.updateMetadata(metadataInfo);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean addIndexComplete(final String namespace,
                                    final String indexId,
                                    final String tableName,
                                    AuthContext authCtx,
                                    short serialVersion) {
        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {
            @Override
            public Boolean execute() {
                return repNode.addIndexComplete(namespace, indexId, tableName);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean removeTableDataComplete(final String namespace,
                                           final String tableName,
                                           AuthContext authCtx,
                                           short serialVersion) {
        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {
            @Override
            public Boolean execute() {
                return repNode.removeTableDataComplete(namespace, tableName);
            }
        });
    }

    @Override
    /*
     * Require USERVIEW for Kerberos principals information so that all clients
     * are able to access. Kerberos principals information must be widely
     * available, since clients need to know all RNs' Kerberos information for
     * its attempts to obtian a new Kerbeors TGS token.
     */
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public KerberosPrincipals getKerberosPrincipals(AuthContext authCtx,
                                                    short serialVersion) {

        return faultHandler.execute(new ProcessFaultHandler.
            SimpleOperation<KerberosPrincipals>() {
                @Override
                public KerberosPrincipals execute() {
                    return repNode.getKerberosPrincipals();
                }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER})
    public boolean startNetworkRestore(final RepNodeId sourceNode,
                                       final boolean retainOriginalLogFile,
                                       final long minVLSN,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                return repNode.startAsyncNetworkRestore(sourceNode,
                                                        retainOriginalLogFile,
                                                        minVLSN);
            }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER})
    public NetworkRestoreStatus getNetworkRestoreStatus(AuthContext authCtx,
                                                        short serialVersion) {
        return faultHandler.execute(new ProcessFaultHandler.
            SimpleOperation<NetworkRestoreStatus>() {
                @Override
                public NetworkRestoreStatus execute() {
                    return repNode.getAsyncNetworkRestoreStatus();
                }
        });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSVIEW })
    public ResourceInfo exchangeResourceInfo(
                                    final long sinceMillis,
                                    final Collection<UsageRecord> usageRecords,
                                    AuthContext authCtx,
                                    short serialVersion) {
        return faultHandler.
        execute(
            new ProcessFaultHandler.SimpleOperation<ResourceInfo>() {

            @Override
            public ResourceInfo execute() {
                try {
                    assertRunning();
                    return repNode.exchangeResourceInfo(sinceMillis,
                                                        usageRecords);
                } catch (IllegalRepNodeServiceStateException irnsse) {
                    return null;
                }
            }
        });
    }

    /**
     * Verify that the caller of an operation has sufficient
     * authorization to access it.
     *
     * @param opCtx operation context
     * @throws SessionAccessException if there is an internal security error
     * @throws KVSecurityException if the a security exception is generated by
     * the requesting client
     */
    private void checkAccessPermission(OperationContext opCtx)
        throws SessionAccessException, KVSecurityException {

        final AccessChecker accessChecker =
            repNodeService.getRepNodeSecurity().getAccessChecker();

        if (accessChecker != null) {
            try {
                accessChecker.checkAccess(ExecutionContext.getCurrent(),
                                          opCtx);
            } catch (KVSecurityException kvse) {
                throw new ClientAccessException(kvse);
            }
        }
    }

    /**
     * Provides an implementation of OperationContext for access checking when
     * Metadata is requested.
     */
    private static class MetadataAccessContext implements OperationContext {
        private final MetadataType mdType;

        private MetadataAccessContext(MetadataType type) {
            this.mdType = type;
        }

        @Override
        public String describe() {
            return "Metadata request for type: " + mdType;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            switch(mdType) {
            case TABLE:
                return SystemPrivilege.dbviewPrivList;
            case TOPOLOGY:
                return SystemPrivilege.usrviewPrivList;
            default:
                return SystemPrivilege.internalPrivList;
            }
        }
    }


    /**
     * Provides an implementation of OperationContext for access checking when
     * Metadata is updated.
     */
    private static class MetadataUpdateContext implements OperationContext {
        private final MetadataType mdType;

        private MetadataUpdateContext(MetadataType type) {
            this.mdType = type;
        }

        @Override
        public String describe() {
            return "Metadata update for type: " + mdType;
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            switch(mdType) {
            case TOPOLOGY:
                return SystemPrivilege.usrviewPrivList;
            default:
                return SystemPrivilege.internalPrivList;
            }
        }
    }

    /**
     * Verify data for this repnode.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER })
    @Deprecated
    public void verifyData(final boolean verifyBtree,
                           final boolean verifyLog,
                           final boolean verifyIndex,
                           final boolean verifyRecord,
                           final long btreeDelay,
                           final long logDelay,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException, IOException {
        faultHandler.execute(new ProcessFaultHandler.Procedure<IOException>() {

            @Override
            public void execute() throws IOException {
                throw new IllegalCommandException(
                    "This API has been deprecated. Please " +
                    "upgrade the store and rerun the plan.");

            }

        });

    }

    /**
     * Verify data for this repnode.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public VerificationInfo startAndPollVerifyData(
                                             final VerificationOptions options,
                                             final int planId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {
        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<VerificationInfo>() {


            @Override
            public VerificationInfo execute() {
                assertRunning();
                return repNode.startAndPollVerifyData(options, planId);

            }

        });
    }

    /**
     * Interrupt the running verification for this repnode.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.SYSOPER,
                                     KVStorePrivilegeLabel.READ_ANY })
    public boolean interruptVerifyData(AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException {
        return faultHandler.execute(
            new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                assertRunning();
                return repNode.interruptVerify();
            }

        });
    }
}
