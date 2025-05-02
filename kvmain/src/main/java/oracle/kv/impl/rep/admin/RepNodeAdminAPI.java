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

import static oracle.kv.impl.util.SerialVersion.SHUTDOWN_REASON_VERSION;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.NetworkRestoreStatus;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdmin.MigrationState;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;

/**
 * The full administrative interface to a RepNode.
 */
public class RepNodeAdminAPI extends ClientRepNodeAdminAPI {

    private RepNodeAdminAPI(RepNodeAdmin remote, LoginHandle loginHdl)
        throws RemoteException {

        super(remote, loginHdl);
    }

    public static RepNodeAdminAPI wrap(RepNodeAdmin remote,
                                       LoginHandle loginHdl)
        throws RemoteException {

        return new RepNodeAdminAPI(remote, loginHdl);
    }

    /**
     * Provides the configuration details of this RepNode.  This method may
     * be called once and only once.  It is expected that it will be called
     * by a {@link oracle.kv.impl.sna.StorageNodeAgentInterface}
     * after the SNA has started the RepNode process for the first time.
     * No methods other than shutdown may be called on the RepNode until
     * it has been configured.
     */
    public void configure(Set<Metadata<? extends MetadataInfo>> metadataSet)
        throws RemoteException {

        proxyRemote.configure(metadataSet, NULL_CTX, getSerialVersion());
    }

    /**
     * Notifies the RN that new parameters are available in the storage node
     * configuration file and should be reread.
     */
    public void newParameters()
        throws RemoteException {

        proxyRemote.newParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Notifies the RN that new global parameters are available in the storage
     * node configuration file and should be reread.
     */
    public void newGlobalParameters()
        throws RemoteException {

        proxyRemote.newGlobalParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the in-memory values of the parameters for the RN. Used for
     * configuration verification.
     */
    public LoadParameters getParams()
        throws RemoteException {
        return proxyRemote.getParams(NULL_CTX, getSerialVersion());
    }

    /**
     * Shuts down this RepNode process cleanly without checking streams or
     * providing a reason. Convenience method for testing only.
     *
     * @param force force the shutdown
     */
    public void shutdown(boolean force)
        throws RemoteException {

        shutdown(force, false /* checkStream */, "For testing");
    }

    /**
     * Shuts down this RepNode process, and check stream client if necessary.
     * If reason is non-null, include it in the message logged when the service
     * shuts down.
     *
     * @param force force the shutdown
     * @param checkStream check stream client before shutdown if true
     * @param reason the reason for the shutdown, or null
     */
    /* Permit call to deprecated overloading */
    @SuppressWarnings("deprecation")
    public void shutdown(boolean force, boolean checkStream, String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            proxyRemote.shutdown(force, checkStream, NULL_CTX,
                                 getSerialVersion());
            return;
        }

        proxyRemote.shutdown(force, checkStream, reason, NULL_CTX,
                             getSerialVersion());
    }

    /**
     * Returns the <code>RepNodeStatus</code> associated with the rep node.
     *
     * @return the service status
     */
    public RepNodeStatus ping()
        throws RemoteException {

        return proxyRemote.ping(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns administrative and configuration information from the
     * repNode. Meant for diagnostic and debugging support.
     */
    public RepNodeInfo getInfo()
        throws RemoteException {

        return proxyRemote.getInfo(NULL_CTX, getSerialVersion());
    }

    public String [] startBackup()
        throws RemoteException {

        return proxyRemote.startBackup(NULL_CTX, getSerialVersion());
    }

    public long stopBackup()
        throws RemoteException {

        return proxyRemote.stopBackup(NULL_CTX, getSerialVersion());
    }

    /**
     * @param groupName
     * @param targetNodeName
     * @param targetHelperHosts
     * @param newNodeHostPort
     * @return true if this node's address can be updated in the JE
     * group database, false if there is no current master, and we need to
     * retry.
     * @throws RemoteException
     */
    public boolean updateMemberHAAddress(String groupName,
                                         String targetNodeName,
                                         String targetHelperHosts,
                                         String newNodeHostPort)
        throws RemoteException{

        return proxyRemote.updateMemberHAAddress(groupName,
                                                 targetNodeName,
                                                 targetHelperHosts,
                                                 newNodeHostPort,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }


    /**
     * @param groupName
     * @param targetNodeName
     * @param targetHelperHosts
     * @return true if the node was deleted from the JE group database, false
     * if there is no current master, and we need to retry.
     * @throws RemoteException
     */
    public boolean deleteMember(String groupName,
                                String targetNodeName,
                                String targetHelperHosts)
        throws RemoteException{

        return proxyRemote.deleteMember(groupName,
                                        targetNodeName,
                                        targetHelperHosts,
                                        NULL_CTX,
                                        getSerialVersion());
    }

    /**
     * Initiates a partition migration from the source node. This call must be
     * made on the destination (target) node. The admin must take the following
     * actions for each of the possible returns:
     *
     * SUCCEEDED - Update the topology to reflect the partition's new
     *             location and broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * UNKNOWN - Retry the migration
     * All others - Enter a loop to monitor migration progress by calling
     *              getMigrationState(PartitionId)
     *
     * @param partitionId the ID of the partition to migrate
     * @param sourceRGId the ID of the partitions current location
     * @return the migration state
     * @throws RemoteException
     */
    @Deprecated
    public PartitionMigrationState migratePartition(PartitionId partitionId,
                                                    RepGroupId sourceRGId)
            throws RemoteException {
        return migratePartitionV2(partitionId,
                                  sourceRGId).getPartitionMigrationState();
    }

    /**
     * Initiates a partition migration from the source node. This call must be
     * made on the destination (target) node. The admin must take the following
     * actions for each of the possible returns:
     *
     * SUCCEEDED - Update the topology to reflect the partition's new
     *             location and broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * UNKNOWN - Retry the migration
     * All others - Enter a loop to monitor migration progress by calling
     *              getMigrationState(PartitionId)
     *
     * @param partitionId the ID of the partition to migrate
     * @param sourceRGId the ID of the partitions current location
     * @return the migration state
     * @throws RemoteException
     */
    public MigrationState migratePartitionV2(PartitionId partitionId,
                                             RepGroupId sourceRGId)
            throws RemoteException {

        return proxyRemote.migratePartitionV2(partitionId, sourceRGId, NULL_CTX,
                                              getSerialVersion());
    }

    /**
     * Returns the state of a partition migration. The admin must take the
     * following actions for each of the possible returns:
     *
     * SUCCEEDED - Update the topology to reflect the partition's new
     *             location an broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * PENDING - No action, retry after delay
     * RUNNING - No action, retry after delay
     * UNKNOWN - Retry the migration
     *
     * For each call to getMigrationState, verify that the target mastership
     * has not changed.
     *
     * @param partitionId a partition ID
     * @return the migration state
     * @throws RemoteException
     */
    @Deprecated
    public PartitionMigrationState getMigrationState(PartitionId partitionId)
            throws RemoteException {
        return getMigrationStateV2(partitionId).getPartitionMigrationState();
    }

    /**
     * Returns the state of a partition migration. The admin must take the
     * following actions for each of the possible returns:
     *
     * SUCCEEDED - Update the topology to reflect the partition's new
     *             location an broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * PENDING - No action, retry after delay
     * RUNNING - No action, retry after delay
     * UNKNOWN - Retry the migration
     *
     * For each call to getMigrationState, verify that the target mastership
     * has not changed.
     *
     * @param partitionId a partition ID
     * @return the migration state
     * @throws RemoteException
     */
    public MigrationState getMigrationStateV2(PartitionId partitionId)
            throws RemoteException {

        return
            proxyRemote.getMigrationStateV2(partitionId, NULL_CTX,
                                            getSerialVersion());
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
     * As with getMigrationState(PartitionId) if the return value is
     * PartitionMigrationState.ERROR, canceled(PartitionId, RepGroupId) must be
     * invoked on the migration source repNode.
     *
     * @param partitionId a partition ID
     * @return a migration state or null
     * @throws RemoteException
     */
    @Deprecated
    public PartitionMigrationState canCancel(PartitionId partitionId)
        throws RemoteException {
        return canCancelV2(partitionId).getPartitionMigrationState();
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
     * As with getMigrationState(PartitionId) if the return value is
     * PartitionMigrationState.ERROR, canceled(PartitionId, RepGroupId) must be
     * invoked on the migration source repNode.
     *
     * @param partitionId a partition ID
     * @return a migration state or null
     * @throws RemoteException
     */
    public MigrationState canCancelV2(PartitionId partitionId)
        throws RemoteException {

        return proxyRemote.canCancelV2(partitionId, NULL_CTX,
                                       getSerialVersion());
    }


    /**
     * Cleans up a source migration stream after a cancel or error. If the
     * cleanup was successful true is returned. If false is returned, the
     * call should be retried. This method must be invoked on the master
     * if the source rep group.
     *
     * This method  must be invoked on the migration source repNode whenever
     * PartitionMigrationState.ERROR is returned from a call to
     * getMigrationState(PartitionId) or cancelMigration(PartitionId).
     *
     * @param partitionId a partition ID
     * @param targetRGId the target rep group ID
     * @return true if the cleanup was successful
     * @throws RemoteException
     */
    public boolean canceled(PartitionId partitionId, RepGroupId targetRGId)
        throws RemoteException {

        return proxyRemote.canceled(partitionId, targetRGId, NULL_CTX,
                                    getSerialVersion());
    }

    /**
     * Gets the status of partition migrations for the specified partition. If
     * no status is available, null is returned.
     *
     * @param partitionId a partition ID
     * @return the partition migration status or null
     */
    public PartitionMigrationStatus getMigrationStatus(PartitionId partitionId)
        throws RemoteException {

        return proxyRemote.getMigrationStatus(partitionId, NULL_CTX,
                                              getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#initiateMasterTransfer
     */
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit)
        throws RemoteException {

        return proxyRemote.initiateMasterTransfer(replicaId, timeout, timeUnit,
                                                  NULL_CTX, getSerialVersion());
    }

    public boolean awaitConsistency(long stopTime, int timeout, TimeUnit unit)
        throws RemoteException {
        return proxyRemote.awaitConsistency(stopTime, timeout, unit,
                                       NULL_CTX, getSerialVersion());
    }

    public boolean addIndexComplete(String namespace,
                                    String indexId,
                                    String tableName)
        throws RemoteException {
        return proxyRemote.addIndexComplete(namespace, indexId, tableName,
                                            NULL_CTX, getSerialVersion());
    }

    public boolean removeTableDataComplete(String namespace,
                                           String tableName)
        throws RemoteException {
        return proxyRemote.removeTableDataComplete(namespace, tableName,
                                                   NULL_CTX, getSerialVersion());
    }

    public boolean startNetworkRestore(RepNodeId sourceNode,
                                       boolean retainOriginalLogFile,
                                       long minVLSN)
        throws RemoteException {
        return proxyRemote.startNetworkRestore(sourceNode,
                                               retainOriginalLogFile, minVLSN,
                                               NULL_CTX, getSerialVersion());
    }

    public NetworkRestoreStatus getNetworkRestoreStatus()
       throws RemoteException {
        return proxyRemote.getNetworkRestoreStatus(NULL_CTX,
                                                   getSerialVersion());
    }

    /**
     * Verify data.
     **/
    public VerificationInfo startAndPollVerifyData(VerificationOptions options,
                                                   int planId)
        throws RemoteException {
        return proxyRemote.startAndPollVerifyData(options, planId, NULL_CTX,
                                           getSerialVersion());
    }

    public boolean interruptVerifyData() throws RemoteException {
        return proxyRemote.interruptVerifyData(NULL_CTX,
                                               getSerialVersion());
    }

    /* Resource tracking */
    public ResourceInfo exchangeResourceInfo(long sinceMillis,
                                           Collection<UsageRecord> usageRecords)
            throws RemoteException {
        return proxyRemote.exchangeResourceInfo(sinceMillis, usageRecords,
                                                NULL_CTX, getSerialVersion());
    }
}
