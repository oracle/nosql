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

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.RepNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.NetworkRestoreStatus;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;

/**
 * The administrative interface to a RepNode process.
 */
public interface RepNodeAdmin extends ClientRepNodeAdmin {

    PartitionMigrationState[] PARTITION_MIGRATION_STATE_VALUES =
        PartitionMigrationState.values();

    /**
     * The possible values for state of a partition migration. The state is
     * returned from the migratePartition and getMigrationState methods.
     *
     * The legal transitions are:
     *
     * {@literal
     *                      ------> SUCCEEDED
     * PENDING --> RUNNING /
     *     ^          |    \------> ERROR
     *     |__________|
     * }
     */
    public enum PartitionMigrationState {

        /** The partition migration is waiting to start. */
        PENDING,

        /** The partition migration is in-progress. */
        RUNNING,

        /**
         * The partition migration is complete and the partition is durable
         * at its new location
         */
        SUCCEEDED,

        /**
         * An error has occurred or the partition migration has been canceled.
         */
        ERROR,

        /**
         * Indicates that the state is not known.
         */
        UNKNOWN;
    }

    public class MigrationState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final PartitionMigrationState state;
        private final Exception cause;

        public MigrationState(PartitionMigrationState state) {
            this.state = state;
            this.cause = null;
        }

        public MigrationState(PartitionMigrationState state, Exception cause) {
            this.state = state;
            this.cause = cause;
        }

        public PartitionMigrationState getPartitionMigrationState() {
            return state;
        }

        public Exception getCause() {
            return cause;
        }

        @Override
        public String toString() {
            return state.toString();
        }
    }


    /**
     * Provides the configuration details of this RepNode.
     * It is expected that it will be called by a
     * {@link oracle.kv.impl.sna.StorageNodeAgentInterface}
     * after the SNA has started the RepNode process for the first time.
     * No methods other than shutdown may be called on the RepNode until
     * it has been configured.
     *
     * @since 3.0
     * @param metadataSet the set of metadata to initialize the RepNode.
     */
    public void configure(Set<Metadata<? extends MetadataInfo>> metadataSet,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    public void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new global parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Return this RN's view of its current parameters. Used for configuration
     * verification.
     *
     * @since 3.0
     */
    public LoadParameters getParams(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Shuts down this RepNode process cleanly without specifying a reason.
     *
     * @param force force the shutdown
     * @param checkStream check stream client before shutdown if true
     *
     * @since 19.1
     * @deprecated since 22.3
     */
    @Deprecated
    public void shutdown(boolean force,
                         boolean checkStream,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Shuts down this RepNode process cleanly.
     *
     * @param force force the shutdown
     * @param checkStream check stream client before shutdown if true
     * @param reason the reason for the shutdown, or null
     *
     * @since 22.3
     */
    public void shutdown(boolean force,
                         boolean checkStream,
                         String reason,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Returns the <code>RepNodeStatus</code> associated with the rep node.
     *
     * @return the service status
     *
     * @since 3.0
     */
    public RepNodeStatus ping(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns administrative and configuration information from the
     * repNode. Meant for diagnostic and debugging support.
     *
     * @since 3.0
     */
    public RepNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     *
     * @since 3.0
     */
    public String [] startBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     *
     * @since 3.0
     */
    public long stopBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     *
     * @since 3.0
     */
    public boolean updateMemberHAAddress(String groupName,
                                         String fullName,
                                         String targetHelperHosts,
                                         String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
    *
    * @since 4.0
    */
   public boolean deleteMember(String groupName,
                               String fullName,
                               String targetHelperHosts,
                               AuthContext authCtx,
                               short serialVersion)
       throws RemoteException;

    /**
     * Initiates a partition migration from the source node. This call must be
     * made on the destination (target) node. The admin must take the following
     * actions for each of the possible returns:
     *
     * SUCCEEDED - The topology is updated to reflect the partitions new
     *             location an broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * All others - Enter a loop to monitor migration progress by calling
     *              getMigrationState(PartitionId)
     *
     * Note that PartitionMigrationState.UNKNOWN is never returned by
     * this method.
     *
     * @param partitionId the ID of the partition to migrate
     * @param sourceRGId the ID of the partitions current location
     * @param serialVersion the serial version
     * @return the migration state
     * @throws RemoteException
     * @since 18.1
     */
    public MigrationState migratePartitionV2(PartitionId partitionId,
                                             RepGroupId sourceRGId,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException;

    /**
     * Returns the state of a partition migration. The admin must take the
     * following actions for each of the possible returns:
     *
     * SUCCEEDED - The topology is updated to reflect the partitions new
     *             location an broadcast to the store.
     * ERROR - Invoke canceled(PartitionId, RepGroupId) on the master source
     *         repNode.
     * PENDING - No action, retry after delay
     * RUNNING - No action, retry after delay
     * UNKNOWN - Verify target mastership has not changed, retry after delay
     *
     * @param partitionId a partition ID
     * @param serialVersion the serial version
     * @return the migration state
     * @throws RemoteException
     * @since 18.1
     */
    public MigrationState getMigrationStateV2(PartitionId partitionId,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException;

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
     * @param serialVersion the serial version
     * @return a migration state or null
     * @throws RemoteException
     * @since 18.1
     */
    public MigrationState canCancelV2(PartitionId partitionId,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

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
     * @param serialVersion the serial version
     * @return true if the cleanup was successful
     * @throws RemoteException
     *
     * @since 3.0
     */
    public boolean canceled(PartitionId partitionId,
                            RepGroupId targetRGId,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * Gets the status of partition migrations for the specified partition. If
     * no status is available, null is returned.
     *
     * @param partitionId a partition ID
     * @return the partition migration status or null
     *
     * @since 3.0
     */
    public PartitionMigrationStatus getMigrationStatus(PartitionId partitionId,
                                                       AuthContext authCtx,
                                                       short serialVersion)
        throws RemoteException;

    /**
     * Requests a transfer of master status to the specific replica. This node
     * must currently be the master. The actual transfer is accomplished in the
     * background and may fail. The caller needs to be resilient to this
     * possibility.
     * <p>
     * The request may be rejected, if the node is not currently the master, or
     * it already has an outstanding request for a master transfer to a
     * different replica.
     *
     * @param replicaId the replica that is the target of the master transfer
     * @param timeout the timeout period associated with the transfer
     * @param timeUnit the time unit associated with the timeout
     *
     * @return true if the master transfer request has been accepted
     *
     * @since 3.0
     */
   public boolean initiateMasterTransfer(RepNodeId replicaId,
                                         int timeout,
                                         TimeUnit timeUnit,
                                         AuthContext authCtx,
                                         short serialVersion)
       throws RemoteException;

    /**
     * Install a receiver for RepNode status updates, for delivering metrics
     * and service change information to the standardized monitoring/management
     * agent.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    public void installStatusReceiver(RepNodeStatusReceiver receiver,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Return true if this RN is consistent with the target time.
     * Wait timeoutSeconds for consistency.
     *
     * @param targetTime the time to be consistent
     * @param timeout timeout time
     * @param timeoutUnit unit of timeout time
     *
     * @return true if this RN is consistent
     *
     * @since 3.0
     */
    public boolean awaitConsistency(long targetTime,
                                    int timeout,
                                    TimeUnit timeoutUnit,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * @since 4.4
     */
    public boolean addIndexComplete(String namespace,
                                    String indexId,
                                    String tableName,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * @since 4.4
     */
    public boolean removeTableDataComplete(String namespace,
                                           String tableName,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Start a network restore from given source node.
     * @param sourceNode restore source node
     * @param retainOriginalLogFile if retain original log files
     * @param minVLSN minimum VLSN the restore would fetch from source node
     * @return true if network restore task is started.
     *
     * @since 4.4
     */
    public boolean startNetworkRestore(RepNodeId sourceNode,
                                       boolean retainOriginalLogFile,
                                       long minVLSN,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * Get status of network restore that is currently running on this node.
     * @return status of network restore, or null if no restore is running.
     *
     * @since 4.4
     */
    public NetworkRestoreStatus getNetworkRestoreStatus(AuthContext authCtx,
                                                        short serialVersion)
        throws RemoteException;

    /**
     * Verify data for this node.
     *
     * @param verifyBtree verifies the btree of databases
     * @param verifyLog verifies the log files
     * @param verifyIndex verifies the index
     * @param verifyRecord verifies the data records in disk
     * @param btreeDelay delay between batches for btree verification
     * @param logDelay delay between log file reads
     * @throws IOException
     * @since 18.1
     * @deprecated as of 19.3
     */
    @Deprecated
    public void verifyData(boolean verifyBtree,
                           boolean verifyLog,
                           boolean verifyIndex,
                           boolean verifyRecord,
                           long btreeDelay,
                           long logDelay,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException, IOException;

    /**
     * Verify data for this node.
     *
     * @param options properties for verification.
     * @since 19.1
     */
    public VerificationInfo startAndPollVerifyData(VerificationOptions options,
                                                   int planId,
                                                   AuthContext authCtx,
                                                   short serialVersion)
        throws RemoteException;


    /**
     * Interrupt the data verification.
     * @since 19.1
     */
    public boolean interruptVerifyData(AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * Exchanges resource usage information for this node. If sinceMillis &gt;
     * 0 the resource data collected after that time will be returned.
     * If usageRecords is not null the records will be processed to either
     * set of clear resource overages. See UsageRecord for more details.
     *
     * @param sinceMillis return information since this time if &gt; 0
     * @param usageRecords a set of resource usage records or null
     * @return the resource usage information for this node
     * @since 4.x
     */
    public ResourceInfo exchangeResourceInfo(long sinceMillis,
                                           Collection<UsageRecord> usageRecords,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;
}
