/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.param.SecurityParams.KrbPrincipalInfo;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.sna.StorageNodeAgentInterface;
import oracle.kv.impl.sna.StorageNodeInfo;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * A NOP StorageNodeAgent class that can be used used as the basis for building
 * mock RepNodeAdmin classes to test specific methods of interest by providing
 * suitable overriding methods.
 */
public class StorageNodeAgentNOP extends VersionedRemoteImpl
       implements StorageNodeAgentInterface {

    @Override
    public short getSerialVersion() {
        return SerialVersion.CURRENT;
    }

    @Override
    public void noteState(StateInfo stateInfo,
                          AuthContext authContext,
                          short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "noteState");
    }

    @Override
    public MDInfo getMDInfo(AuthContext authContext,
                            short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMD");
    }

    @Override
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMasterLease");
    }

    @Override
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     AuthContext authContext,
                                     short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "cancelMasterLease");
    }

    @Override
    public StorageNodeStatus ping(AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "ping");
    }

    @Override
    public List<ParameterMap>  register(ParameterMap globalParams,
                                        ParameterMap storageNodeParams,
                                        boolean hostingAdmin,
                                        AuthContext authContext,
                                        short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "register");
    }

    @Deprecated
    @Override
    public void shutdown(boolean stopServices,
                         boolean force,
                         AuthContext authContext,
                         short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "shutdown");
    }

    @Override
    public void shutdown(boolean stopServices,
                         boolean force,
                         String reason,
                         AuthContext authContext,
                         short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "shutdown");
    }

    @Override
    public boolean createAdmin(ParameterMap adminParams,
                               AuthContext authContext,
                               short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "createAdmin");
    }

    @Override
    public boolean startAdmin(AuthContext authContext,
                              short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "startAdmin");
    }

    @Deprecated
    @Override
    public boolean stopAdmin(boolean force,
                             AuthContext authContext,
                             short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "stopAdmin");
    }

    @Override
    public boolean stopAdmin(boolean force,
                             String reason,
                             AuthContext authContext,
                             short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "stopAdmin");
    }

    @Override
    public boolean repNodeExists(RepNodeId repNodeId,
                                 AuthContext authContext,
                                 short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "repNodeExists");
    }

    @Override
    public boolean createRepNode(ParameterMap repNodeParams,
                                 Set<Metadata<? extends MetadataInfo>> metadataSet,
                                 AuthContext authContext,
                                 short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "createRepNode");
    }

    @Override
    public boolean startRepNode(RepNodeId repNodeId,
                                AuthContext authContext,
                                short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "startRepNode");
    }

    @Deprecated
    @Override
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               boolean chkStream,
                               AuthContext authContext,
                               short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "stopRepNode");
    }

    @Override
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               boolean chkStream,
                               String reason,
                               AuthContext authContext,
                               short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "stopRepNode");
    }

    @Override
    public void newRepNodeParameters(ParameterMap repNodeParams,
                                     AuthContext authContext,
                                     short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "newRepNodeParameters");
    }

    @Override
    public void newAdminParameters(ParameterMap adminParams,
                                   AuthContext authContext,
                                   short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "newAdminParameters");
    }

    @Override
    public void checkParameters(ParameterMap params, ResourceId id,
                                AuthContext authCtx,
                                short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "checkStorageNodeParameters");
    }

    @Override
    public void newStorageNodeParameters(ParameterMap params,
                                         AuthContext authContext,
                                         short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "newStorageNodeParameters");
    }

    @Override
    public void newGlobalParameters(ParameterMap params,
                                    AuthContext authContext,
                                    short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "newStorageNodeParameters");
    }

    @Override
    public LoadParameters getParams(AuthContext authContext,
                                    short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getParams");
    }

    @Override
    public KrbPrincipalInfo getKrbPrincipalInfo(AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                "getKrbPrincipalInfo");
    }

    @Override
    public StringBuilder getStartupBuffer(final ResourceId rid,
                                          AuthContext authContext,
                                          final short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getStartupBuffer");
    }

    @Override
    public void createSnapshot(RepNodeId rnid, String name,
                               AuthContext authContext, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "createSnapshot");
    }

    @Override
    public void createSnapshot(AdminId aid, String name,
                               AuthContext authContext, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "createSnapshot");
    }

    @Override
    public void removeSnapshot(RepNodeId rnid, String name,
                               AuthContext authContext, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "removeSnapshot");
    }

    @Override
    public void removeSnapshot(AdminId aid, String name,
                               AuthContext authContext, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "removeSnapshot");
    }

    @Override
    public void removeAllSnapshots(RepNodeId rnid,
                                   AuthContext authContext, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "removeAllSnapshots");
    }

    @Override
    public void removeAllSnapshots(AdminId aid,
                                   AuthContext authContext, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "removeAllSnapshots");
    }

    @Override
    public String[] listSnapshots(AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "listSnapshots");
    }

    @Deprecated
    @Override
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "destroyRepNode");
    }

    @Override
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  String reason,
                                  AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "destroyRepNode");
    }

    @Deprecated
    @Override
    public boolean destroyAdmin(AdminId adminId,
                                boolean deleteData,
                                AuthContext authContext,
                                short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "destroyAdmin");
    }

    @Override
    public boolean destroyAdmin(AdminId adminId,
                                boolean deleteData,
                                String reason,
                                AuthContext authContext,
                                short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "destroyAdmin");
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   AuthContext authContext,
                                   short serialVersion) throws RemoteException {
       /* NOP */
    }

    @Override
    public boolean arbNodeExists(ArbNodeId arbNodeId, AuthContext authCtx,
            short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "arbNodeExists");
    }

    @Override
    public boolean createArbNode(ParameterMap arbNodeParams,
            AuthContext authCtx, short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                "createArbNode");
    }

    @Override
    public boolean startArbNode(ArbNodeId arbNodeId, AuthContext authCtx,
            short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "startArbNode");
    }

    @Deprecated
    @Override
    public boolean stopArbNode(ArbNodeId arbNodeId, boolean force,
            AuthContext authCtx, short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "stopArbNode");
    }

    @Override
    public boolean stopArbNode(ArbNodeId arbNodeId, boolean force,
                               String reason, AuthContext authCtx,
                               short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "stopArbNode");
    }

    @Deprecated
    @Override
    public boolean destroyArbNode(ArbNodeId arbNodeId, boolean deleteData,
            AuthContext authCtx, short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "destroyArbNode");
    }

    @Override
    public boolean destroyArbNode(ArbNodeId arbNodeId, boolean deleteData,
                                  String reason, AuthContext authCtx,
                                  short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "destroyArbNode");
    }

    @Override
    public void newArbNodeParameters(ParameterMap arbNodeParams,
            AuthContext authCtx, short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "newArbNodeParameters");
    }

    @Override
    public StorageNodeInfo getInfo(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
            "getInfo");
    }

    @Override
    public void createSnapshotConfig(String name,
                                     AuthContext nullCtx,
                                     short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
            "createSnapshotConfig");
    }

    @Override
    public void removeSnapshotConfig(String name,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "removeSnapshotConfig");
    }

    @Override
    public void removeAllSnapshotConfigs(AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
            "removeAllSnapshotConfigs");
    }

    @Override
    public void updateNodeStatus(ResourceId serviceId,
                                 ServiceStatusChange newStatus,
                                 AuthContext authCtx,
                                 short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: updateNodeStatus");
    }

    @Override
    public void updateAdminStatus(ServiceStatusChange newStatus,
                                  boolean isMaster,
                                  AuthContext authCtx,
                                  short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: updateAdminStatus");
    }

    @Override
    public void receiveStats(ResourceId serviceId,
                             StatsPacket packet,
                             AuthContext authCtx,
                             short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: receiveStats");
    }

    @Override
    public void receiveAdminStats(StatsPacket packet,
                                  AuthContext authCtx,
                                  short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: receiveAdminStats");
    }

    @Override
    public void receiveNewParams(ResourceId serviceId,
                                 ParameterMap newMap,
                                 AuthContext authCtx,
                                 short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: receiveNewParams");
    }

    @Override
    public void updatePlanStatus(String planStatus,
                                 AuthContext authCtx,
                                 short serialVCersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: updatePlanStatus");
    }

    @Override
    public void updateReplicationState(RepNodeId rnId,
                                       StateChangeEvent changeEvent,
                                       AuthContext authCtx,
                                       short serialVCersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: updateReplicationState");
    }

    @Override
    public String getTlsCredentialsInfo(AuthContext authCtx,
                                        short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: getTlsCredentialsInfo");
    }

    @Override
    public String retrieveTlsCredentials(AuthContext authCtx,
                                         short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: retrieveTlsCredentials");
    }

    @Override
    public CredentialHashes verifyTlsCredentialUpdates(boolean force,
                                                       AuthContext authCtx,
                                                       short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: verifyTlsCredentialUpdates");
    }

    @Override
    public String addTruststoreUpdates(AuthContext authCtx,
                                       short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: addTruststoreUpdates");
    }

    @Override
    public String installKeystoreUpdate(String keystoreHash,
                                        AuthContext authCtx,
                                        short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: installKeystoreUpdate");
    }

    @Override
    public String installTruststoreUpdate(String truststoreHash,
                                          AuthContext authCtx,
                                          short serialVersion) {
        throw new UnsupportedOperationException(
            "Method not implemented: installTruststoreUpdate");
    }

}
