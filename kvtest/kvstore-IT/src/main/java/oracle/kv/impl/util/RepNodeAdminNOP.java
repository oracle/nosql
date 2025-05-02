/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.mgmt.RepNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.NetworkRestoreStatus;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeInfo;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * A NOP StorageNodeAgent class that can be used as the basis for building mock
 * StorageNodeAgent classes to test specific methods of interest by providing
 * suitable overriding methods.
 */
public class RepNodeAdminNOP
    extends VersionedRemoteImpl implements RepNodeAdmin {

    @Override
    public void configure(Set<Metadata<? extends MetadataInfo>> metadataSet,
                          AuthContext authCtx,
                          short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "configure");
    }

    @Override
    public void newParameters(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "newParameters");
    }

    @Override
    public void newGlobalParameters(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "newGlobalParameters");
    }

    @Deprecated
    @Override
    public void shutdown(boolean force,
                         boolean waitForStream,
                         AuthContext authCtx,
                         short serialVersion) {

        throw new UnsupportedOperationException("Method not implemented: " +
        "shutdown");
    }

    @Override
    public void shutdown(boolean force,
                         boolean waitForStream,
                         String reason,
                         AuthContext authCtx,
                         short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "shutdown");
    }

    @Override
    public Topology getTopology(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getTopology");
    }

    @Override
    public String getHAHostPort(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getHAHostPort");
    }

    @Override
    public ReplicatedEnvironment.State
        getReplicationState(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getReplicationState");
    }

    @Override
    public RepNodeStatus ping(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "ping");
    }

    @Override
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "getTopoSeqNum");
    }

    @Override
    public long getVlsn(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "getVlsn");
    }

    @Override
    public RepNodeInfo getInfo(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
        "getInfo");
    }

    @Override
    public String [] startBackup(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "startBackup");
    }

    @Override
    public long stopBackup(AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "stopBackup");
    }

    @Override
    public LoadParameters getParams(AuthContext authCtx, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented");
    }

    @Override
    public boolean deleteMember(String groupName,
                                String targetNodeName,
                                String targetHelperHosts,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented");
    }

    @Override
    public boolean updateMemberHAAddress(String groupName,
                                         String targetNodeName,
                                         String targetHelperHosts,
                                         String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented");
    }

    @Override
    public MigrationState migratePartitionV2(PartitionId partitionId,
                                             RepGroupId sourceRGId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MigrationState getMigrationStateV2(PartitionId partitionId,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean canceled(PartitionId partitionId,
                            RepGroupId targetRGId,
                            AuthContext authCtx,
                            short serialVersion) throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "canceled");
    }

    @Override
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "transferMaster");
    }

    @Deprecated
    @Override
    public void installStatusReceiver(RepNodeStatusReceiver receiver,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "installStatusReceiver");
    }

    @Override
    public MigrationState canCancelV2(PartitionId partitionId,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "canCancelV2");
    }

    @Override
    public PartitionMigrationStatus getMigrationStatus(PartitionId partitionId,
                                                       AuthContext authCtx,
                                                       short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMigrationStatus");
    }

    @Override
    public boolean awaitConsistency(long targetTime,
                                    int timeout,
                                    TimeUnit timeoutUnit,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {

        throw new UnsupportedOperationException("Method not implemented: " +
                                                "awaitConsistency");
    }

    @Override
    public int getMetadataSeqNum(MetadataType type,
                                 AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMetadataSeqNum");
    }

    @Override
    public Metadata<?> getMetadata(MetadataType type,
                                   AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMetadata");
    }

    @Override
    public MetadataInfo getMetadata(MetadataType type, int seqNum,
                                    AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMetadata");
    }

    @Override
    public MetadataInfo getMetadata(MetadataType type, MetadataKey key,
                                    int seqNum,
                                    AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getMetadata");
    }

    @Override
    public void updateMetadata(Metadata<?> newMetadata,
                               AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "updateMetadata");
    }

    @Override
    public boolean addIndexComplete(String namespace,
                                    String indexName,
                                    String tableName,
                                    AuthContext authCtx, short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int updateMetadata(MetadataInfo metadataInfo,
                              AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "updateMetadata");
    }

    @Override
    public boolean removeTableDataComplete(String namespace,
                                           String tableName,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public KerberosPrincipals getKerberosPrincipals(AuthContext authCtx,
                                                    short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean startNetworkRestore(RepNodeId sourceNode,
                                    boolean retainOriginalLogFile,
                                    long minVLSN,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NetworkRestoreStatus getNetworkRestoreStatus(AuthContext authCtx,
                                                        short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    @Deprecated
    public void verifyData(boolean verifyBtree,
                           boolean verifyLog,
                           boolean verifyIndex,
                           boolean verifyRecord,
                           long btreeDelay,
                           long logDelay,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    @Override
    public VerificationInfo startAndPollVerifyData(VerificationOptions options,
                                                   int planId,
                                                   AuthContext authCtx,
                                                   short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    @Override
    public boolean interruptVerifyData(AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ResourceInfo exchangeResourceInfo(long sinceMillis,
                                           Collection<UsageRecord> usageRecords,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MetadataInfo getTableById(long tableId,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MetadataInfo getTable(String namespace, String tableName, int cost,
                                 AuthContext authCtx, short serialVersion) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
