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
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.registry.VersionedRemote;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * The interface to the Storage Node Agent.  The SNA is run as a process on
 * each of the Storage Nodes.  It provides process control for each Storage
 * Node as well as a mechanism for passing parameters to the processes it
 * controls.
 *
 * Before a StorageNodeAgent can be used as part of a store, it must be
 * registered by calling the {@link #register} method.  Until an SNA
 * is registered, all other methods will throw an exception.
 *
 * Exceptions thrown from this interface are nearly always indicative of a
 * serious problem such as a corrupt configuration or network problem.  In
 * general the "worker" methods try hard to do what they've been asked.  Most
 * state-changing operations are idempotent in that they can be retried and
 * will ignore the fact that it may be a retry.  This handles the situation
 * where the caller may have exited before knowing the resulting state of the
 * call.
 *
 * A number of the methods imply an expected state when called.  For example,
 * calling createRepNode() implies that the caller expects that the RepNode in
 * question has not already been created.  Rather than throwing an exception
 * the method should log the situation and return a value to the caller
 * indicating that things were not as expected.  The sense of the return values
 * used is true for "the implied state was correct" and false for "the implied
 * state was not correct."  The return values do not indicate success or
 * failure of the operation.  If the operation does not throw an exception it
 * succeeded.
 */
public interface StorageNodeAgentInterface extends
    VersionedRemote, MasterBalancingInterface  {

    /**
     * Returns the service status associated with the SNA
     *
     * @since 3.0
     */
    public StorageNodeStatus ping(AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Registers this Storage Node to be part of a store. This method should be
     * called at most once during the lifetime of a Storage Node. All other
     * methods will fail until this method has been called. Uses the bootstrap
     * hostname and port.
     *
     * After this method is called the handle used to access it will no longer
     * be valid and will need to be re-acquired. The name of the service will
     * also have changed to its permanent name.
     *
     * @param globalParams kvstore wide settings, including the store name.
     * @param storageNodeParams parameters for the new storage node required
     * for it to set up normal service, including registry port and storage
     * node id.
     * @param hostingAdmin set to true if this Storage Node is physically
     * hosting the Admin for the store.
     *
     * @return List<ParameterMap> which has two parameter maps, one for basic
     * storage node information and one that is the map of mount points.  This
     * information is destined for the caller's copy of StorageNodeParams.
     *
     * @since 3.0
     */
    public List<ParameterMap> register(ParameterMap globalParams,
                                       ParameterMap storageNodeParams,
                                       boolean hostingAdmin,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * Stops a running Storage Node Agent, optionally stopping all running
     * services it is managing, without specifying a reason.
     *
     * @param stopServices if true stop running services
     *
     * @since 3.0
     * @deprecated since 22.3
     */
    @Deprecated
    public void shutdown(boolean stopServices,
                         boolean force,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Stops a running Storage Node Agent, optionally stopping all running
     * services it is managing.
     *
     * @param stopServices if true stop running services
     * @param force force a shutdown
     * @param reason the reason for the shutdown, or null
     * @since 22.3
     */
    public void shutdown(boolean stopServices,
                         boolean force,
                         String reason,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Creates and starts a Admin instance in the store.  This will cause a new
     * process to be created containing the Admin.  This should be called for
     * each instance up the Admin, up to the desired Admin replication factor.
     * The Storage Node Agent will continue to start this Admin instance upon
     * future restarts unless it is explicitly stopped.
     *
     * @param adminParams the configuration parameters of this Admin instance
     *
     * @return true if the Admin is successfully created.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean createAdmin(ParameterMap adminParams,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Starts a Admin instance that has already been defined on this node.  The
     * Admin will be started automatically by this StorageNodeAgent if the
     * Storage Node is restarted.
     *
     * @return true if the operation succeeds.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist.
     *
     * @since 3.0
     */
    public boolean startAdmin(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Stops a Admin instance that has already been defined on this node,
     * without specifying a reason. The Admin will no longer be started
     * automatically if the Storage Node is restarted.
     *
     * @param force force a shutdown
     *
     * @return true if the Admin was running, false if it was not.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist
     *
     * @since 3.0
     * @deprecated since 22.3
     */
    @Deprecated
    public boolean stopAdmin(boolean force,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Stops a Admin instance that has already been defined on this node. The
     * Admin will no longer be started automatically if the Storage Node is
     * restarted.
     *
     * @param force force a shutdown
     * @param reason the reason for the shutdown, or null
     * @return true if the Admin was running, false if it was not.
     * @throws RuntimeException if the operation fails or the service does not
     * exist
     * @since 22.3
     */
    public boolean stopAdmin(boolean force,
                             String reason,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes an Admin instance running on this Storage Node,
     * without specifying a reason. Since the StorageNodeAgent cannot know if
     * this is the only Admin instance or not, care should be taken by the
     * Admin itself to prevent removal of the last Admin instance. This method
     * will stop the admin if it is running.
     *
     * @param adminId the unique identifier of the Admin
     *
     * @param deleteData true if the data stored on disk for this Admin
     *                   should be deleted
     *
     * @return true if the Admin existed, false if it did not.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     * @deprecated since 22.3
     */
    @Deprecated
    public boolean destroyAdmin(AdminId adminId,
                                boolean deleteData,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes an Admin instance running on this Storage Node.
     * Since the StorageNodeAgent cannot know if this is the only Admin
     * instance or not, care should be taken by the Admin itself to prevent
     * removal of the last Admin instance.  This method will stop the admin if
     * it is running.
     *
     * @param adminId the unique identifier of the Admin
     * @param deleteData true if the data stored on disk for this Admin
     *                   should be deleted
     * @param reason the reason for the shutdown, or null
     *
     * @return true if the Admin existed, false if it did not.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 22.3
     */
    public boolean destroyAdmin(AdminId adminId,
                                boolean deleteData,
                                String reason,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Query whether a give RepNode has been defined on this Storage Node, as
     * indicated by its configuration existing in the store's configuration
     * file.  This is not an indication of its runtime status.
     *
     * @param repNodeId the unique identifier of the RepNode
     *
     * @return true if the specified RepNode exists in the configuration file
     *
     * @since 3.0
     */
    public boolean repNodeExists(RepNodeId repNodeId,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Creates and starts a {@link oracle.kv.impl.rep.RepNode} instance
     * on this Storage Node.  This will cause a new process to be started to
     * run the RepNode.  The StorageNodeAgent will continue to start this
     * RepNode if the Storage Node is restarted unless the RepNode is stopped
     * explicitly.
     *
     * Once the configuration file is written so that a restart of the SNA will
     * also start the RepNode this call will unconditionally succeed, even if
     * it cannot actually start or contact the RepNode itself.  This is so that
     * the state of the SNA is consistent with the topology in the admin
     * database.
     *
     * @param repNodeParams the configuration of the RepNode to create
     *
     * @param metadataSet the metadata set for the RepNode
     *
     * @return true if the RepNode is successfully created.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     */
    public boolean createRepNode(ParameterMap repNodeParams,
                                 Set<Metadata<? extends MetadataInfo>> metadataSet,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Starts a {@link oracle.kv.impl.rep.RepNode} that has already been
     * defined on this Storage Node.  The RepNode will be started automatically
     * if the Storage Node is restarted or the RepNode exits unexpectedly.
     *
     * @param repNodeId the unique identifier of the RepNode to start
     *
     * @return true if the operation succeeds.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist.
     *
     * @since 3.0
     */
    public boolean startRepNode(RepNodeId repNodeId,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Stops a {@link oracle.kv.impl.rep.RepNode} that has already been defined
     * on this Storage Node, without specifying a reason. The RepNode will not
     * be started if the Storage node is restarted until {@link #startRepNode}
     * is called.
     *
     * @param repNodeId the unique identifier of the RepNode to stop
     *
     * @param force force a shutdown
     *
     * @param chkStream true if check stream clients
     *
     * @return true if the RepNode was running, false if it was not.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 19.1
     * @deprecated since 22.3
     */
    @Deprecated
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               boolean chkStream,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Stops a {@link oracle.kv.impl.rep.RepNode} that has already been defined
     * on this Storage Node. The RepNode will not be started if the Storage
     * node is restarted until {@link #startRepNode} is called.
     *
     * @param repNodeId the unique identifier of the RepNode to stop
     * @param force force a shutdown
     * @param chkStream true if check stream clients
     * @param reason the reason for the shutdown, or null
     * @return true if the RepNode was running, false if it was not.
     * @throws RuntimeException if the operation failed.
     * @since 22.3
     */
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               boolean chkStream,
                               String reason,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes the {@link oracle.kv.impl.rep.RepNode} with
     * the specified RepNodeId, without specifying a reason.
     *
     * @param repNodeId the unique identifier of the RepNode to destroy
     * @param deleteData true if the data stored on disk for this RepNode
     *                   should be deleted
     *
     * @return true if the RepNode is successfully destroyed.  This will be the
     * case if it does not exist in the first place.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 3.0
     * @deprecated since 22.3
     */
    @Deprecated
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes the {@link oracle.kv.impl.rep.RepNode} with
     * the specified RepNodeId.
     *
     * @param repNodeId the unique identifier of the RepNode to destroy
     * @param deleteData true if the data stored on disk for this RepNode
     *                   should be deleted
     * @param reason the reason for the shutdown, or null
     *
     * @return true if the RepNode is successfully destroyed.  This will be the
     * case if it does not exist in the first place.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 22.3
     */
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  String reason,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

     /**
     * Modifies the parameters of a (@link oracle.kv.impl.rep.RepNode}
     * RepNode managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  If the service needs
     * notification of the new parameters that is done by the admin/planner.
     *
     * @param repNodeParams the new parameters to configure the rep node. This
     * is a full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the RepNode is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
    public void newRepNodeParameters(ParameterMap repNodeParams,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Query whether a give ArbNode has been defined on this Storage Node, as
     * indicated by its configuration existing in the store's configuration
     * file.  This is not an indication of its runtime status.
     *
     * @param arbNodeId the unique identifier of the ArbNode
     *
     * @return true if the specified ArbNode exists in the configuration file
     *
     * @since 4.0
     */
    public boolean arbNodeExists(ArbNodeId arbNodeId,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Creates and starts a {@link ArbNode} instance
     * on this Storage Node.  This will cause a new process to be started to
     * run the ArbNode.  The StorageNodeAgent will continue to start this
     * ArbNode if the Storage Node is restarted unless the ArbNode is stopped
     * explicitly.
     *
     * Once the configuration file is written so that a restart of the SNA will
     * also start the ArbNode this call will unconditionally succeed, even if
     * it cannot actually start or contact the ArbNode itself.  This is so that
     * the state of the SNA is consistent with the topology in the admin
     * database.
     *
     * @param arbNodeParams the configuration of the ArbNode to create
     *
     * @return true if the ArbNode is successfully created.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 4.0
     */
    public boolean createArbNode(ParameterMap arbNodeParams,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;


    /**
     * Starts a {@link ArbNode} that has already been
     * defined on this Storage Node.  The ArbNode will be started automatically
     * if the Storage Node is restarted or the ArbNode exits unexpectedly.
     *
     * @param arbNodeId the unique identifier of the ArbNode to start
     *
     * @return true if the operation succeeds.
     *
     * @throws RuntimeException if the operation fails or the service does not
     * exist.
     *
     * @since 4.0
     */
    public boolean startArbNode(ArbNodeId arbNodeId,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Stops a {@link ArbNode} that has already been defined on this Storage
     * Node, without specifying a reason. The ArbNode will not be started if
     * the Storage node is restarted until {@link #startArbNode} is called.
     *
     * @param arbNodeId the unique identifier of the ArbNode to stop
     *
     * @param force force a shutdown
     *
     * @return true if the ArbNode was running, false if it was not.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 4.0
     * @deprecated since 22.3
     */
    @Deprecated
    public boolean stopArbNode(ArbNodeId arbNodeId,
                               boolean force,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Stops a {@link ArbNode} that has already been defined on this Storage
     * Node. The ArbNode will not be started if the Storage node is restarted
     * until {@link #startArbNode} is called.
     *
     * @param arbNodeId the unique identifier of the ArbNode to stop
     * @param force force a shutdown
     * @param reason the reason for the shutdown, or null
     * @return true if the ArbNode was running, false if it was not.
     * @throws RuntimeException if the operation failed.
     * @since 22.3
     */
    public boolean stopArbNode(ArbNodeId arbNodeId,
                               boolean force,
                               String reason,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes the {@link ArbNode} with
     * the specified ArbNodeId, without specifying a reason.
     *
     * @param arbNodeId the unique identifier of the ArbNode to destroy
     *
     * @param deleteData true if the data stored on disk for this ArbNode
     *                   should be deleted
     *
     * @return true if the ArbNode is successfully destroyed.  This will be the
     * case if it does not exist in the first place.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 4.0
     * @deprecated since 22.3
     */
    @Deprecated
    public boolean destroyArbNode(ArbNodeId arbNodeId,
                                  boolean deleteData,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Permanently removes the {@link ArbNode} with
     * the specified ArbNodeId.
     *
     * @param arbNodeId the unique identifier of the ArbNode to destroy
     * @param deleteData true if the data stored on disk for this ArbNode
     *                   should be deleted
     * @param reason the reason for the shutdown, or null
     *
     * @return true if the ArbNode is successfully destroyed.  This will be the
     * case if it does not exist in the first place.
     *
     * @throws RuntimeException if the operation failed.
     *
     * @since 22.3
     */
    public boolean destroyArbNode(ArbNodeId arbNodeId,
                                  boolean deleteData,
                                  String reason,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Checks the specified parameters.  Throws an IllegalArgumentException if
     * a parameter is found to be invalid. If id is non-null then the parameters
     * are for that service. Otherwise  the global parameters are checked.
     *
     * @param params parameter map to check
     * @param id the service associated with the parameters or null
     *
     * @throws IllegalArgumentException if an invalid parameter is found
     *
     * @since 4.3
     */
    public void checkParameters(ParameterMap params, ResourceId id,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

     /**
     * Modifies the parameters of a (@link ArbNode}
     * ArbNode managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  If the service needs
     * notification of the new parameters that is done by the admin/planner.
     *
     * @param arbNodeParams the new parameters to configure the arb node. This
     * is a full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the ArbNode is not configured or the
     * operation failed.
     *
     * @since 4.0
     */
    public void newArbNodeParameters(ParameterMap arbNodeParams,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Modifies the parameters of an (@link oracle.kv.impl.admin.Admin}
     * Admin managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  Any required notification
     * is done by the admin/planner.
     *
     * @param adminParams the new parameters to configure the admin.  This is a
     * full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the admin is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
     public void newAdminParameters(ParameterMap adminParams,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Modifies the parameters of the current Storage Node.  The new
     * parameters will be written out to the storage node's configuration file
     * and if also present, the bootstrap config file.
     *
     * @param params the new parameters to configure the storage
     * node.  This can be a partial set but must include both bootstrap and
     * StorageNodeParams to change.  It may also be a map of mount points to
     * be applied to the storage node and the bootstrap parameters.
     *
     * @throws RuntimeException if the StorageNode is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
    public void newStorageNodeParameters(ParameterMap params,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Modifies the global parameters of the current Storage Node. The new
     * parameters will be written out to the storage node's configuration file.
     * Any required notification is done by the admin/planner.
     *
     * @param params the new store-wide global parameters
     *
     * @throws RuntimeException if the StorageNode is not configured or the
     * operation failed.
     *
     * @since 3.0
     */
    public void newGlobalParameters(ParameterMap params,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Get SNA parameters.
     *
     * @since 3.0
     */
    public LoadParameters getParams(AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Returns current information from the SN.
     */
    StorageNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get SNA Kerberos service principal information.
     *
     * @since 3.5
     */
    public KrbPrincipalInfo getKrbPrincipalInfo(AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException;

    /**
     * Returns information about service start problems if the service is
     * started as a process.  Problems may be JVM initialization or
     * synchronous failures from the service itself during startup.
     *
     * @param rid is the ResourceId of the service
     *
     * @return the buffer of startup information if there was a problem.  Null
     * is returned if there was no startup problem.
     *
     * @throws RuntimeException if the service does not exist.
     *
     * @since 3.0
     */
    public StringBuilder getStartupBuffer(ResourceId rid,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException;

    /**
     * Snapshot methods.
     */

    /**
     * Create the named snapshot.
     *
     * @since 3.0
     */
    public void createSnapshot(RepNodeId rnid,
                               String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Create the named snapshot.
     * @since 3.0
     */
    public void createSnapshot(AdminId aid, String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Remove the named snapshot from all managed services on this storage node
     *
     * @since 3.0
     */
    public void removeSnapshot(RepNodeId rnid, String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * @since 3.0
     */
    public void removeSnapshot(AdminId aid, String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Remove all snapshots all managed services on this storage node
     *
     * @since 3.0
     */
    public void removeAllSnapshots(RepNodeId rnid,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * @since 3.0
     */
    public void removeAllSnapshots(AdminId aid,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * List the snapshots present on this Storage Node.  The SN will choose the
     * first managed service it can find and return the list of file names.
     *
     * @return an arry of file names for the snapshots.  If no snapshots are
     * present this is a zero-length array.
     *
     * @since 3.0
     */
    public String [] listSnapshots(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Snapshot configurations of current storage node in specified snapshot.
     * @param snapshotName full name of the snapshot.
     */
    public void createSnapshotConfig(String snapshotName,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Remove snapshot configurations of current storage node for specified
     * snapshot.
     * @param snapshotName full name of the snapshot to be removed.
     */
    public void removeSnapshotConfig(String snapshotName,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Remove all snapshots of configurations for current storage node.
     */
    public void removeAllSnapshotConfigs(AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Note that the status has changed for an RN or Arbiter service managed by
     * this SN
     *
     * @param serviceId the ID of the RN or Arbiter
     * @param newStatus information about the changed service status
     * @since 21.2
     */
    void updateNodeStatus(ResourceId serviceId,
                          ServiceStatusChange newStatus,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * Note that the status has changed for the Admin service managed by this
     * SN.
     *
     * @param newStatus information about the changed service status
     * @param isMaster whether the admin is the master
     * @since 21.2
     */
    void updateAdminStatus(ServiceStatusChange newStatus,
                           boolean isMaster,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Note new statistics from an RN or Arbiter service managed by this SN.
     *
     * @param serviceId the ID of the RN or Arbiter
     * @param packet the statistics
     * @since 21.2
     */
    void receiveStats(ResourceId serviceId,
                      StatsPacket packet,
                      AuthContext authCtx,
                      short serialVersion)
        throws RemoteException;

    /**
     * Note new statistics from the admin managed by this SN.
     *
     * @param packet the statistics
     * @since 21.2
     */
    void receiveAdminStats(StatsPacket packet,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Note that parameters have changed for a service managed by this SN.
     *
     * @param serviceId the ID of the service
     * @param newMap the new service parameters
     * @since 21.2
     */
    void receiveNewParams(ResourceId serviceId,
                          ParameterMap newMap,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * Note that status has changed for a plan being run by the admin managed
     * by this SN.
     *
     * @param planStatus a string describing the plan status change
     * @since 21.2
     */
    void updatePlanStatus(String planStatus,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * Note that the replication state has changed for an RN managed by this
     * SN.
     *
     * @param rnId the ID of the RN
     * @param changeEvent an event describing the state change
     * @since 21.2
     */
    void updateReplicationState(RepNodeId rnId,
                                StateChangeEvent changeEvent,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Returns information about TLS credentials for this SNA as a
     * JSON-formatted string.
     *
     * @return information about TLS credentials in JSON format
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    String getTlsCredentialsInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Attempts to retrieve any new TLS credentials and store them in the
     * update directory.
     *
     * @return a description of the results of retrieving credentials
     * @throws IllegalStateException if there was a problem retrieving the
     * credentials
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String retrieveTlsCredentials(AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Verify the correctness of TLS credential updates, returning the hashes
     * of the keystore and truststore files that will be installed after the
     * updates, if any, are installed. If the force flag was specified, skip
     * verifying the credentials within the keystores.
     *
     * @param force whether the force flag was specified
     * @return the credential hashes
     * @throws RemoteException if a network failure occurs
     * @throws IllegalStateException if a problem is detected
     * @since 24.4
     */
    public CredentialHashes verifyTlsCredentialUpdates(boolean force,
                                                       AuthContext authCtx,
                                                       short serialVersion)
        throws RemoteException;

    /**
     * Add entries from the truststore update found in the update directory to
     * the installed truststore, and update the client truststore.
     *
     * @returns a description of the update performed
     * @throws IllegalStateException if the update fails
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String addTruststoreUpdates(AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;


    /**
     * Install the keystore update found in the update directory.
     *
     * @returns a description of the update performed
     * @throws IllegalStateException if the update fails
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String installKeystoreUpdate(String keystoreHash,
                                        AuthContext authCtx,
                                        short serialVersion)
        throws RemoteException;

    /**
     * Install the truststore update found in the update directory, and update
     * the client truststore.
     *
     * @returns a description of the update performed
     * @throws IllegalStateException if the update fails
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String installTruststoreUpdate(String truststoreHash,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException;
}
