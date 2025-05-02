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

package oracle.kv.impl.sna;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams.KrbPrincipalInfo;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ProcessFaultHandler.SimpleProcedure;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommandResult;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * The class that implements the StorageNodeAgentInterface (SNA) interface used
 * to manage all processes on a physical machine (Storage Node, or SN).  The
 * SNA is responsible for
 *
 * 1.  Hosting the RMI registry for processes on the SN
 * 2.  Configuring and starting processes on the SN (RepNode and Admin)
 * 3.  Monitoring process state and restarting them if necessary
 * 4.  Terminating processes if requested, gracefully if possible.
 *
 * Once provisioned, the process that hosts this object is intended to be
 * started automatically by the Operating System though a mechanism such as
 * /etc/init on *nix.  There is one instance of this object for each KVStore
 * provisioned on a Storage Node, most likely hosted by independent processes.
 *
 * This is the interface class.  Most of the work is done in the
 * StorageNodeAgent class.
 */
@SecureAPI
public final class StorageNodeAgentImpl
    extends VersionedRemoteImpl implements StorageNodeAgentInterface {

    private final StorageNodeAgent sna;
    private final SNAFaultHandler faultHandler;

    /**
     * A conditional instance, created if the class can be found.
     */
    private RemoteTestInterface rti;
    private static final String TEST_INTERFACE_NAME =
        "oracle.kv.impl.sna.StorageNodeAgentTestInterface";

    public StorageNodeAgentImpl() {
        this(false);
    }

    /**
     * A constructor that allows the caller to indicate that the bootstrap
     * admin service should or should not be started.
     */
    public StorageNodeAgentImpl(boolean createBootstrapAdmin) {

        sna = new StorageNodeAgent(this, createBootstrapAdmin);

        faultHandler = new SNAFaultHandler(this);
        rti = null;
    }

    public StorageNodeAgent getStorageNodeAgent() {
        return sna;
    }

    public SNAFaultHandler getFaultHandler() {
        return faultHandler;
    }

    /**
     * A bunch of pass-through methods do the StorageNodeAgent methods.
     */
    public Logger getLogger() {
        return sna.getLogger();
    }

    public void parseArgs(String args[]) {
        sna.parseArgs(args);
    }

    public void start()
        throws IOException {

        sna.start();
    }

    public void addShutdownHook() {
        sna.addShutdownHook();
    }

    public boolean isRegistered() {
        return sna.isRegistered();
    }

    public String getStoreName() {
        return sna.getStoreName();
    }

    public int getRegistryPort() {
        return sna.getRegistryPort();
    }

    /**
     * Can the test interface be created?
     */
    public void startTestInterface() {
        try {
            Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            Constructor<?> c = cl.getConstructor(getClass());
            rti = (RemoteTestInterface) c.newInstance(this);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) {
        }
    }

    public void stopTestInterface()
        throws RemoteException {

        if (rti != null) {
            rti.stop(SerialVersion.CURRENT);
        }
    }

    private void checkRegistered(String method) {
        sna.checkRegistered(method);
    }

    private void logInfo(String msg) {
        sna.getLogger().info(msg);
    }

    private void logFine(String msg) {
        sna.getLogger().fine(msg);
    }

    /**
     * Begin StorageNodeAgentInterface
     *
     * General considerations:
     * o Only one register() call during the lifetime of the agent.
     * o Single-thread the state-changing calls.
     * o Ensure that services never see or read an inconsistent configuration
     *   file.
     */


    @Override
    @PublicMethod
    public StorageNodeStatus ping(AuthContext authCtx,
                                  short serialVersion) {

        return faultHandler.execute
            ((ProcessFaultHandler.SimpleOperation<StorageNodeStatus>)() ->
                    sna.getStatus(authCtx, serialVersion));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized List<ParameterMap> register(final ParameterMap gpMap,
                                                    final ParameterMap snpMap,
                                                    final boolean hostingAdmin,
                                                    AuthContext authCtx,
                                                    short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.
                        Operation<List<ParameterMap>, RemoteException>)() -> {
                GlobalParams gp = new GlobalParams(gpMap);
                StorageNodeParams snp = new StorageNodeParams(snpMap);
                return sna.register(gp, snp, hostingAdmin);
            });
    }

    /**
     * Stops a running Storage Node Agent and maybe all running services,
     * without specifying a reason.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public synchronized void shutdown(final boolean stopServices,
                                      final boolean force,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        shutdown(stopServices, force, null /* reason */, authCtx,
                 serialVersion);
    }

    /**
     * Stops a running Storage Node Agent and maybe all running services.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void shutdown(final boolean stopServices,
                                      final boolean force,
                                      final String reason,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                sna.shutdown(stopServices, force, reason);
            });
    }

    /**
     * Create and start a Admin instance.  If this SN is hosting the primary
     * Admin and this method is called the Admin is not started.  It is assumed
     * to be already managed.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean createAdmin(final ParameterMap adminParams,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() ->
                    sna.createAdmin(new AdminParams(adminParams)));
    }

    /**
     * Start an already-created Admin instance
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean startAdmin(AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("startAdmin");
                logInfo("startAdmin called");

                AdminParams ap =
                        ConfigUtils.getAdminParams(sna.getKvConfigFile());
                if (ap == null) {
                    String msg =
                            "Attempt to start an Admin when none is configured";
                    logInfo(msg);
                    throw new IllegalStateException(msg);
                }
                return sna.startAdmin(ap);
            });
    }

    /**
     * Stop the running Admin instance, without specifying a reason.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public synchronized boolean stopAdmin(final boolean force,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException {

        return stopAdmin(force, null /* reason */, authCtx, serialVersion);
    }

    /**
     * Stop the running Admin instance
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean stopAdmin(final boolean force,
                                          final String reason,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("stopAdmin");
                return sna.stopAdmin(null, force, reason);
            });
    }

    /**
     * Destroy the Admin instance, without specifying a reason. After this it
     * will be necessary to use createAdmin() if another Admin is required.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public synchronized boolean destroyAdmin(final AdminId adminId,
                                             final boolean deleteData,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return destroyAdmin(adminId, deleteData, null /* reason */, authCtx,
                            serialVersion);
    }

    /**
     * Destroy the Admin instance.  After this it will be necessary to use
     * createAdmin() if another Admin is required.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean destroyAdmin(final AdminId adminId,
                                             final boolean deleteData,
                                             final String reason,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("destroyAdmin");
                return sna.destroyAdmin(adminId, deleteData, reason);
            });
    }

    /**
     * Does this RepNode exist in the configuration file?
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean repNodeExists(final RepNodeId repNodeId,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("repNodeExists");
                if (sna.lookupRepNode(repNodeId) != null) {
                    return true;
                }
                return false;
            });
    }

    /**
     * Create a new RepNode instance based on the configuration information.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean
        createRepNode(final ParameterMap params,
                      final Set<Metadata<? extends MetadataInfo>> metadataSet,
                      AuthContext authCtx,
                      short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("createRepNode");
                RepNodeParams repNodeParams = new RepNodeParams(params);
                return sna.createRepNode(repNodeParams, metadataSet);
            });
    }

    /**
     * Start an already-create RepNode.  This implicitly sets its state to
     * active.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean startRepNode(final RepNodeId repNodeId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("startRepNode");
                return sna.startRepNode(repNodeId);
            });
    }

    /**
     * Stop a running RepNode, check stream client if necessary, without
     * specifying a reason.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public synchronized boolean stopRepNode(final RepNodeId rnid,
                                            final boolean force,
                                            boolean chkStream,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws  RemoteException {

        return stopRepNode(rnid, force, chkStream, null /* reason */,
                           authCtx, serialVersion);
    }

    /**
     * Stop a running RepNode, checking stream client if necessary.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean stopRepNode(final RepNodeId rnid,
                                            final boolean force,
                                            final boolean chkStream,
                                            final String reason,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws  RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("stopRepNode");
                return sna.stopRepNode(rnid, force, chkStream, reason);
            });
    }

    /**
     * Stop and destroy a RepNode, without specifying a reason. After this it
     * will be necessary to re-create the instance using createRepNode().
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public synchronized boolean destroyRepNode(final RepNodeId rnid,
                                               final boolean deleteData,
                                               AuthContext authCtx,
                                               short serialVersion)
        throws RemoteException {

        return destroyRepNode(rnid, deleteData, null /* reason */, authCtx,
                              serialVersion);
    }

    /**
     * Stop and destroy a RepNode.  After this it will be necessary to
     * re-create the instance using createRepNode().
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean destroyRepNode(final RepNodeId rnid,
                                               final boolean deleteData,
                                               final String reason,
                                               AuthContext authCtx,
                                               short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("destroyRepNode");
                String serviceName = rnid.getFullName();
                logInfo(serviceName + ": destroyRepNode called");

                /**
                 * Ignore the return value from stopRepNode.  The node may
                 * be running or not, it doesn't matter.
                 */
                final boolean retval;
                try {
                    sna.stopRepNode(rnid, true /* force */,
                                    false /* checkStream */, reason);
                } finally {
                    retval =
                            sna.removeConfigurable(rnid, null, deleteData);
                }
                return retval;
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void checkParameters(ParameterMap params, ResourceId id,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("checkParameters");
                logInfo("checkParameters called");
                sna.checkParams(params, id);
            });
    }

    /**
     * Only write the new parameters. The admin will notify the RepNode or
     * restart it if that is required. Allow this call to be made for RepNodes
     * that are not running. The parameters are a full replacement, not a merge
     * set.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void newRepNodeParameters(final ParameterMap params,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("newRepNodeParameters");
                RepNodeParams repNodeParams = new RepNodeParams(params);
                String serviceName =
                        repNodeParams.getRepNodeId().getFullName();
                logInfo(serviceName + ": newRepNodeParameters called");

                /**
                 * Change the config file so the RN can see the state.
                 */
                sna.replaceRepNodeParams(repNodeParams);
            });
    }

    /**
     * Only write the new parameters.  The admin will notify the Admin or
     * restart it if that is required.  Allow this call to be made for Admin
     * instances that are not running.  The parameters are a full replacement,
     * not a merge set.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void newAdminParameters(final ParameterMap params,
                                                AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("newAdminParameters");
                AdminId adminId = new AdminParams(params).getAdminId();
                logInfo(adminId.getFullName() +
                            ": newAdminParameters called");

                /**
                 * Change the config file so the Admin can see the state.
                 */
                sna.replaceAdminParams(adminId, params);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void newStorageNodeParameters
        (final ParameterMap params,
         AuthContext authCtx,
         short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("newStorageNodeParameters");
                logInfo("newStorageNodeParameters called");
                sna.replaceStorageNodeParams(params);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newGlobalParameters(final ParameterMap params,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("newGlobalParameters");
                final GlobalParams globalParams = new GlobalParams(params);
                logInfo("newGlobalParameters called");

                /*
                * Change the config file so the admin and repnode can see the
                * state
                */
                sna.replaceGlobalParams(globalParams);
            });
    }

    /**
     * Snapshot methods
     */

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void createSnapshot(final RepNodeId rnid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("snapshot RepNode");
                logInfo("createSnapshot called for " + rnid +
                            ", snapshot name: " + name);
                sna.snapshotRepNode(rnid, name);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void removeSnapshot(final RepNodeId rnid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("removeSnapshot");
                logInfo("removeSnapshot called, name is " + name);
                sna.removeSnapshot(rnid, name);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void removeAllSnapshots(final RepNodeId rnid,
                                                AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("removeAllSnapshots");
                logInfo("removeAllSnapshots called");
                sna.removeSnapshot(rnid, null);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void createSnapshot(final AdminId aid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("snapshot Admin");
                logInfo("createSnapshot called for " + aid +
                            ", snapshot name: " + name);
                sna.snapshotAdmin(aid, name);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void removeSnapshot(final AdminId aid,
                                            final String name,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("removeSnapshot");
                logInfo("removeSnapshot called, name is " + name);
                sna.removeSnapshot(aid, name);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void removeAllSnapshots(final AdminId aid,
                                                AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("removeAllSnapshots");
                logInfo("removeAllSnapshots called");
                sna.removeSnapshot(aid, null);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public String [] listSnapshots(AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<String[], RemoteException>)() -> {
                checkRegistered("listSnapshots");
                logInfo("listSnapshots called");
                return sna.listSnapshots();
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void createSnapshotConfig(String snapshotName,
                                     AuthContext nullCtx,
                                     short serialVersion)
        throws RemoteException {

            faultHandler.execute
                ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                    checkRegistered("createSnapshotConfig");
                    logInfo(
                        "createSnapshotConfig called, snapshot name: " +
                        snapshotName);
                    sna.createSnapshotConfig(snapshotName);
                });

    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void removeSnapshotConfig(final String snapshotName,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("removeSnapshotConfig");
                logInfo(
                    "removeSnapshotConfig called, snapshot name: " +
                    snapshotName);
                sna.removeSnapshotConfig(snapshotName);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized void removeAllSnapshotConfigs(AuthContext authCtx,
                                                      short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("removeSnapshotConfig");
                logInfo("removeSnapshotConfig called, snapshot name");
                sna.removeSnapshotConfig(null);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public LoadParameters getParams(AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.SimpleOperation<LoadParameters>)() ->
                    sna.getParams());
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public StorageNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException {
        return faultHandler.execute
            ((ProcessFaultHandler.SimpleOperation<StorageNodeInfo>)() ->
                    sna.getInfo());
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public KrbPrincipalInfo getKrbPrincipalInfo(AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.SimpleOperation<KrbPrincipalInfo>)() ->
                    sna.getKrbPrincipalInfo());
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public StringBuilder getStartupBuffer(final ResourceId rid,
                                          AuthContext authCtx,
                                          final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.SimpleOperation<StringBuilder>)() -> {
                checkRegistered("getStartupBuffer");
                return sna.getStartupBuffer(rid);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void updateNodeStatus(ResourceId serviceId,
                                 ServiceStatusChange newStatus,
                                 AuthContext authCtx,
                                 short serialVersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.updateNodeStatus(serviceId, newStatus));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void updateAdminStatus(ServiceStatusChange newStatus,
                                  boolean isMaster,
                                  AuthContext authCtx,
                                  short serialVersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.updateAdminStatus(newStatus, isMaster));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void receiveStats(ResourceId serviceId,
                             StatsPacket packet,
                             AuthContext authCtx,
                             short serialVersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.receiveStats(serviceId, packet));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void receiveAdminStats(StatsPacket packet,
                                  AuthContext authCtx,
                                  short serialVersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.receiveAdminStats(packet));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void receiveNewParams(ResourceId serviceId,
                                 ParameterMap newMap,
                                 AuthContext authCtx,
                                 short serialVersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.receiveNewParams(serviceId, newMap));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void updatePlanStatus(String planStatus,
                                 AuthContext authCtx,
                                 short serialVCersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.updatePlanStatus(planStatus));
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void updateReplicationState(RepNodeId rnId,
                                       StateChangeEvent changeEvent,
                                       AuthContext authCtx,
                                       short serialVCersion) {
        faultHandler.execute((SimpleProcedure) () ->
                             sna.updateReplicationState(rnId, changeEvent));
    }

    /**
     * End StorageNodeAgentInterface
     */

    /**
     * The Storage Node Agent main.  It requires two arguments:
     * -root <SNA_bootstrapdir>
     *    This is the directory in which the SNA looks for a bootstrap
     *    configuration file.  It is not the KV store root but could be if
     *    desired.
     * -config <bootstrap_file_name>
     *    This is the file that contains the bootstrap parameters required by
     *    BootstrapParams.  It has a specific format.
     * [-threads ] If present this argument means "run in thread mode" which
     *    causes the SNA to create threads instead of processes for its managed
     *    services.
     *
     * The following two options control the operation of the SNA:
     * -shutdown
     *    This will stop the SNA if it is running.
     * -disable-services
     *    This will disable the services managed by the SNA. If specified
     *    without the -shutdown option, all services will be disabled before
     *    the SNA starts. If the SNA is already running an attempt to start
     *    the SNA with the -disable-services option will fail without disabling
     *    services. If the disabling services fail for any reason, the SNA will
     *    not start.
     *    If the -shutdown option is specified, the SNA is first shutdown
     *    (if running) then the SNA's services are disabled.
     */
    public static void main(String[] args) {
        try {
            new Main().doMain(args);
        } catch (Throwable e) {
            System.exit(1);
        }
    }

    /**
     * Use a separate class to implement the main method, to simplify testing.
     */
    static class Main {
        private final StorageNodeAgentImpl snai = new StorageNodeAgentImpl();
        private final StorageNodeAgent sna = snai.getStorageNodeAgent();

        /**
         * Perform the main operation, throwing a RuntimeException that
         * contains an appropriate message and the underlying cause if the
         * operation fails.
         */
        void doMain(String... args) {
            try {
                sna.parseArgs(args);
                final File configPath = new File(sna.getBootstrapDir(),
                                                 sna.getBootstrapFile());
                if (sna.getSNAParser().getShutdown()) {
                    if (!configPath.exists()) {
                        throw new IllegalStateException(
                            "Bootstrap config file " + configPath +
                            " does not exist");
                    }

                    printlnVerbose("Stopping SNA based on " + configPath);

                    /* Try telling the SNA to stop. */
                    try {
                        sna.stopRunningAgent();
                    } catch (RuntimeException e) {
                        printlnVerbose(e.getMessage());

                        /*
                         * This code will run if there is a problem shutting
                         * down the SNA above.
                         */
                        printlnVerbose(
                            "Attempting to stop all SNAs in root directory " +
                            sna.getBootstrapDir());

                        /*
                         * Kill processes related to the target kvRoot.
                         */
                        final String rootPath = CommandParser.ROOT_FLAG + " " +
                            sna.getBootstrapDir();
                        ManagedService.killManagedProcesses(
                            "StorageNodeAgentImpl", rootPath, null,
                            sna.getLogger());
                        ManagedService.killManagedProcesses
                            (rootPath, null, sna.getLogger());
                    }

                    if (sna.getSNAParser().getDisableServices()) {
                        printlnVerbose("Disabling services");
                        sna.disableServices();
                    }

                } else if (sna.getSNAParser().getStatus()) {
                    ServiceStatus status = sna.getRunningAgentStatus();

                    String message;
                    CommandResult result;

                    if (status == ServiceStatus.UNREACHABLE) {
                        message = "Cannot contact SNA";
                        result = new CommandResult.CommandFails
                            (message, ErrorMessage.NOSQL_5300,
                             CommandResult.NO_CLEANUP_JOBS);
                    } else {
                        ObjectNode returnValue = JsonUtils.createObjectNode();
                        returnValue.put("sna_status", status.name());
                        message = "SNA Status : " + status.name();
                        result = new CommandResult.CommandSucceeds
                            (returnValue.toString());
                    }

                    if (sna.getJsonVersion() == CommandParser.JSON_V2) {
                        System.out.println(
                            ShellCommandResult.toJsonReport(
                                sna.getSNAParser().getCommand(), result));
                    } else if (sna.getJsonVersion() == CommandParser.JSON_V1) {
                        System.out.println(
                            Shell.toJsonReport(
                                sna.getSNAParser().getCommand(), result));
                    } else {
                        System.out.println(message);
                    }
                } else {
                    if (sna.getSNAParser().getDisableServices()) {
                        printlnVerbose("Disabling services before starting");
                        sna.disableServices();
                    }
                    sna.addShutdownHook();
                    printlnVerbose("Starting SNA based on " + configPath);
                    sna.start();
                }
            } catch (Throwable e) {
                String command = sna.getSNAParser().getCommand();
                String message = "Failed to " + command + ": " + e.getMessage();

                if (sna.getLogger() != null) {
                    sna.getLogger().severe(
                        message + "\n" + LoggerUtils.getStackTrace(e));
                }

                if (sna.getJsonVersion() == CommandParser.JSON_V2) {
                    CommandResult result =
                        new CommandResult.CommandFails
                        (message, ErrorMessage.NOSQL_5300,
                         CommandResult.NO_CLEANUP_JOBS);
                    System.err.println(
                        ShellCommandResult.toJsonReport(command, result));
                } else if (sna.getJsonVersion() == CommandParser.JSON_V1) {
                    CommandResult result =
                        new CommandResult.CommandFails
                        (message, ErrorMessage.NOSQL_5300,
                         CommandResult.NO_CLEANUP_JOBS);
                    System.err.println(Shell.toJsonReport(command, result));
                } else {
                    System.err.println(message);
                }

                throw new RuntimeException(message, e);
            }
        }

        /**
         * Print a message to standard error if verbose output is enabled.
         * Can be overridden for testing. Disabled when the -json flag is
         * specified, because the verbose text output would result in invalid
         * json output.
         */
        void printlnVerbose(String message) {
            if (sna.verbose() && sna.getJsonVersion() == -1) {
                System.err.println(message);
            }
        }
    }

    /**
     * Begin MasterBalancingInterface
     */

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void noteState(final StateInfo stateInfo,
                          final AuthContext authCtx,
                          final short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("noteState");
                logInfo("noteState called");
                sna.getMasterBalanceManager().
                        noteState(stateInfo, authCtx, serialVersion);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public MDInfo getMDInfo(final AuthContext authCtx,
                            final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<MDInfo, RemoteException>)() -> {
                checkRegistered("getMD");
                logFine("getMD called");
                return sna.getMasterBalanceManager().getMDInfo(authCtx,
                                                               serialVersion);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean getMasterLease(final MasterLeaseInfo masterLease,
                                  final AuthContext authCtx,
                                  final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("lease");
                logFine("Master lease called");
                return sna.getMasterBalanceManager().
                        getMasterLease(masterLease, authCtx, serialVersion);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean cancelMasterLease(final StorageNode lesseeSN,
                                     final RepNode rn,
                                     final AuthContext authCtx,
                                     final short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("cancelLease");
                logFine("cancelLease called");
                return sna.getMasterBalanceManager().
                        cancelMasterLease(lesseeSN, rn, authCtx, serialVersion);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void overloadedNeighbor(final StorageNodeId storageNodeId,
                                   final AuthContext authCtx,
                                   final short serialVersion)
        throws RemoteException {

        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                sna.getMasterBalanceManager().
                    overloadedNeighbor(storageNodeId, authCtx, serialVersion);
            });
    }

    /**
     * Does this ArbNode exist in the configuration file?
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean arbNodeExists(final ArbNodeId arbNodeId,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("arbNodeExists");
                return sna.lookupArbNode(arbNodeId) != null;
            });
    }

    /**
     * Create a new ArbNode instance based on the configuration information.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean
        createArbNode(final ParameterMap params,
                      AuthContext authCtx,
                      short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("createArbNode");
                ArbNodeParams arbNodeParams = new ArbNodeParams(params);
                return sna.createArbNode(arbNodeParams);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean startArbNode(final ArbNodeId arbNodeId, AuthContext authCtx,
            short serialVersion) throws RemoteException {
        return faultHandler.execute
               ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                    checkRegistered("startArbNode");
                    return sna.startArbNode(arbNodeId);
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public boolean stopArbNode(ArbNodeId arbNodeId,
                               boolean force,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException {

        return stopArbNode(arbNodeId, force, null /* reason */, authCtx,
                           serialVersion);
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public boolean stopArbNode(final ArbNodeId arbNodeId,
                               final boolean force,
                               final String reason,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("stopArbNode");
                return sna.stopArbNode(arbNodeId, force, reason);
            });
    }

    /**
     * Stop and destroy a ArbNode, without specifying a reason. After this it
     * will be necessary to re-create the instance using createArbNode().
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    @Deprecated
    public synchronized boolean destroyArbNode(final ArbNodeId anid,
                                               final boolean deleteData,
                                               AuthContext authCtx,
                                               short serialVersion)
        throws RemoteException {

        return destroyArbNode(anid, deleteData, null /* reason */, authCtx,
                              serialVersion);
    }

    /**
     * Stop and destroy a ArbNode.  After this it will be necessary to
     * re-create the instance using createArbNode().
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public synchronized boolean destroyArbNode(final ArbNodeId anid,
                                               final boolean deleteData,
                                               final String reason,
                                               AuthContext authCtx,
                                               short serialVersion)
        throws RemoteException {

        return faultHandler.execute
            ((ProcessFaultHandler.Operation<Boolean, RemoteException>)() -> {
                checkRegistered("destroyArbNode");
                String serviceName = anid.getFullName();
                logInfo(serviceName + ": destroyArbNode called");

                /**
                 * Ignore the return value from stopArbNode. The node may
                 * be running or not, it doesn't matter.
                 */
                final boolean retval;
                try {
                    sna.stopArbNode(anid, true /* force */, reason);
                } finally {
                    retval =
                            sna.removeConfigurable(anid, null, deleteData);
                }
                return retval;
            });
    }

    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.INTLOPER })
    public void newArbNodeParameters(final ParameterMap params,
            AuthContext authCtx, short serialVersion) throws RemoteException {
        faultHandler.execute
            ((ProcessFaultHandler.Procedure<RemoteException>)() -> {
                checkRegistered("newArbNodeParameters");
                ArbNodeParams arbNodeParams = new ArbNodeParams(params);
                String serviceName =
                        arbNodeParams.getArbNodeId().getFullName();
                logInfo(serviceName + ": newArbNodeParameters called");

                /**
                 * Change the config file so the AN can see the state.
                 */
                sna.replaceArbNodeParams(arbNodeParams);
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.INTLOPER})
    public String getTlsCredentialsInfo(AuthContext authCtx,
                                        short serialVersion) {
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<String>) () -> {
                checkRegistered("getTlsCredentialsInfo");
                return sna.getTlsCredentialsInfo();
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.INTLOPER})
    public String retrieveTlsCredentials(AuthContext authCtx,
                                       short serialVersion) {
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<String>) () -> {
                checkRegistered("retrieveTlsCredentials");
                return sna.retrieveTlsCredentials();
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.INTLOPER})
    public CredentialHashes verifyTlsCredentialUpdates(boolean force,
                                                       AuthContext authCtx,
                                                       short serialVersion) {
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<CredentialHashes>) () -> {
                checkRegistered("verifyTlsCredentialUpdates");
                return sna.verifyTlsCredentialUpdates(force);
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.INTLOPER})
    public String addTruststoreUpdates(AuthContext authCtx,
                                       short serialVersion) {
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<String>) () -> {
                checkRegistered("addTruststoreUpdates");
                return sna.addTruststoreUpdates();
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.INTLOPER})
    public String installKeystoreUpdate(String keystoreHash,
                                        AuthContext authCtx,
                                        short serialVersion) {
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<String>) () -> {
                checkRegistered("installKeystoreUpdate");
                return sna.installKeystoreUpdate(keystoreHash);
            });
    }

    @Override
    @SecureAutoMethod(privileges={KVStorePrivilegeLabel.INTLOPER})
    public String installTruststoreUpdate(String truststoreHash,
                                          AuthContext authCtx,
                                          short serialVersion) {
        return faultHandler.execute(
            (ProcessFaultHandler.SimpleOperation<String>) () -> {
                checkRegistered("installTruststoreUpdate");
                return sna.installTruststoreUpdate(truststoreHash);
            });
    }
}
