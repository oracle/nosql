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

import static oracle.kv.impl.util.SerialVersion.SHUTDOWN_REASON_VERSION;
import static oracle.kv.impl.util.SerialVersion.TLS_CREDENTIALS_VERSION;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.param.SecurityParams.KrbPrincipalInfo;
import oracle.kv.impl.arb.ArbNode;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MDInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MasterLeaseInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.StateInfo;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.RemoteAPI;

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
 * general the "worker" methods try hard to do what they've been asked.  Return
 * values are used to indicate unexpected states to the caller.
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
public class StorageNodeAgentAPI extends RemoteAPI {

    /* Null value that will be filled in by proxyRemote */
    private final static AuthContext NULL_CTX = null;

    private final StorageNodeAgentInterface proxyRemote;

    private StorageNodeAgentAPI(StorageNodeAgentInterface remote,
                                LoginHandle loginHdl)
        throws RemoteException {

        super(remote);
        this.proxyRemote = ContextProxy.create(remote, loginHdl,
                                               getSerialVersion());
    }

    public static StorageNodeAgentAPI wrap(StorageNodeAgentInterface remote,
                                           LoginHandle loginHdl)
        throws RemoteException {

        return new StorageNodeAgentAPI(remote, loginHdl);
    }

    /**
     * Returns the service status associated with the SNA.
     */
    public StorageNodeStatus ping()
        throws RemoteException {

        return proxyRemote.ping(NULL_CTX, getSerialVersion());
    }

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
     * TODO: is there any way to check the diskTotal and memTotal fields, which
     * were input through the PlannerAdmin UI, against what is really on this
     * storage node?
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
     */
    public List<ParameterMap> register(ParameterMap globalParams,
                                       ParameterMap storageNodeParams,
                                       boolean hostingAdmin)
        throws RemoteException {

        return proxyRemote.register(globalParams, storageNodeParams,
                                    hostingAdmin,
                                    NULL_CTX, getSerialVersion());
    }

    /**
     * Stops a running Storage Node Agent, optionally stopping all running
     * services it is managing, without specifying a reason. Convenience method
     * for testing only.
     *
     * @param stopServices if true stop running services
     *
     * @param force force service stop
     */
    public void shutdown(boolean stopServices, boolean force)
        throws RemoteException {

        shutdown(stopServices, force, "For testing");
    }

    /**
     * Stops a running Storage Node Agent, optionally stopping all running
     * services it is managing. If reason is non-null, include it in the
     * message logged when the service shuts down.
     *
     * @param stopServices if true stop running services
     * @param force force service stop
     * @param reason the reason for the shutdown, or null
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public void shutdown(boolean stopServices, boolean force, String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            proxyRemote.shutdown(stopServices, force, NULL_CTX,
                                 getSerialVersion());
            return;
        }

        proxyRemote.shutdown(stopServices, force, reason, NULL_CTX,
                             getSerialVersion());
    }

    /**
     * Creates and starts a Admin instance in the store.  This will cause a new
     * process to be created containing the Admin.  This should be called for
     * each instance up the Admin, up to the desired Admin replication factor.
     * The Storage Node Agent will continue to start this Admin instance upon
     * future restarts unless it is explicitly stopped.
     *
     * @param adminParams the configuration parameters of this Admin instance
     *
     * @return true if the Admin did not already exist, false if it existed
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean createAdmin(ParameterMap adminParams)
        throws RemoteException {

        return proxyRemote.createAdmin(adminParams,
                                       NULL_CTX, getSerialVersion());
    }

    /**
     * Starts a Admin instance that has already been defined on this node.  The
     * Admin will be started automatically by this StorageNodeAgent if the
     * Storage Node is restarted.
     *
     * @return true if the Admin was not started, false if it was already
     * started.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean startAdmin()
        throws RemoteException {

        return proxyRemote.startAdmin(NULL_CTX, getSerialVersion());
    }

    /**
     * Stops a Admin instance that has already been defined on this node,
     * without specifying a reason. Convenience method for testing only.
     *
     * @param force force a shutdown
     *
     * @return true if the Admin was running, false if it was not.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean stopAdmin(boolean force)
        throws RemoteException {

        return stopAdmin(force, "For testing");
    }

    /**
     * Stops a Admin instance that has already been defined on this node. If
     * reason is non-null, include it in the message logged when the service
     * shuts down. The Admin will no longer be started automatically if the
     * Storage Node is restarted.
     *
     * @param force force a shutdown
     * @param reason the reason for the shutdown, or null
     *
     * @return true if the Admin was running, false if it was not.
     *
     * @throws RuntimeException if the operation failed.
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public boolean stopAdmin(boolean force, String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            return proxyRemote.stopAdmin(force, NULL_CTX, getSerialVersion());
        }

        return proxyRemote.stopAdmin(force, reason, NULL_CTX,
                                     getSerialVersion());
    }

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
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public boolean destroyAdmin(AdminId adminId,
                                boolean deleteData,
                                String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            return proxyRemote.destroyAdmin(adminId, deleteData,
                                            NULL_CTX, getSerialVersion());
        }
        return proxyRemote.destroyAdmin(adminId, deleteData, reason,
                                        NULL_CTX, getSerialVersion());
    }

    /**
     * Query whether a give RepNode has been defined on this Storage Node, as
     * indicated by its configuration existing in the store's configuration
     * file.  This is not an indication of its runtime status.
     *
     * @param repNodeId the unique identifier of the RepNode
     *
     * @return true if the specified RepNode exists in the configuration file
     */
    public boolean repNodeExists(RepNodeId repNodeId)
        throws RemoteException {

        return proxyRemote.repNodeExists(repNodeId,
                                         NULL_CTX, getSerialVersion());
    }

    /**
     * Creates and starts a {@link oracle.kv.impl.rep.RepNode} instance
     * on this Storage Node.  This will cause a new process to be started to
     * run the RepNode.  The StorageNodeAgent will continue to start this
     * RepNode if the Storage Node is restarted unless the RepNode is stopped
     * explicitly.
     *
     * @param repNodeParams the configuration of the RepNode to create
     *
     * @param metadataSet the metadata set for the new RepNode
     *
     * @return true if the RepNode is successfully created.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean createRepNode(ParameterMap repNodeParams,
                              Set<Metadata<? extends MetadataInfo>> metadataSet)
        throws RemoteException {

        return proxyRemote.createRepNode(repNodeParams, metadataSet,
                                         NULL_CTX, getSerialVersion());
    }

    /**
     * Starts a {@link oracle.kv.impl.rep.RepNode} that has already been
     * defined on this Storage Node.  The RepNode will be started automatically
     * if the Storage Node is restarted or the RepNode exits unexpectedly.
     *
     * @param repNodeId the unique identifier of the RepNode to start
     *
     * @return true if the RepNode was not already running, false if it was.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean startRepNode(RepNodeId repNodeId)
        throws RemoteException {

        return proxyRemote.startRepNode(repNodeId,
                                        NULL_CTX, getSerialVersion());
    }

    /**
     * Stops a {@link oracle.kv.impl.rep.RepNode} that has already been
     * defined on this Storage Node, without specifying a reason. Convenience
     * method for testing only.
     *
     * @param repNodeId the unique identifier of the RepNode to stop
     * @param force force a shutdown
     * @param checkStream check stream client if true
     *
     * @return true if the RepNode was running, false if it was not.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean stopRepNode(RepNodeId repNodeId, boolean force,
                               boolean checkStream)
        throws RemoteException {

        return stopRepNode(repNodeId, force, checkStream, "For testing");
    }

    /**
     * Stops a {@link oracle.kv.impl.rep.RepNode} that has already been defined
     * on this Storage Node. If reason is non-null, include it in the message
     * logged when the service shuts down. The RepNode will not be started if
     * the Storage node is restarted until {@link #startRepNode} is called.
     *
     * @param repNodeId the unique identifier of the RepNode to stop
     * @param force force a shutdown
     * @param checkStream check stream client if true
     * @param reason the reason for the shutdown, or null
     * @return true if the RepNode was running, false if it was not.
     * @throws RuntimeException if the operation failed.
     */
    @SuppressWarnings("deprecation")
    public boolean stopRepNode(RepNodeId repNodeId,
                               boolean force,
                               boolean checkStream,
                               String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            return proxyRemote.stopRepNode(repNodeId, force, checkStream,
                                           NULL_CTX, getSerialVersion());
        }

        return proxyRemote.stopRepNode(repNodeId, force, checkStream,
                                       reason, NULL_CTX, getSerialVersion());
    }

    /**
     * Permanently removes the SNA's knowledge of the {@link
     * oracle.kv.impl.rep.RepNode} with the specified RepNodeId, without
     * specifying a reason. Convenience method for testing only.
     *
     * @param repNodeId the unique identifier of the RepNode to destroy
     * @param deleteData true if the data stored on disk for this RepNode
     *                   should be deleted
     *
     * @return true if the RepNode existed, false if did not.  The running
     *   state of the RepNode is not relevant to the return value.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData)
        throws RemoteException {

        return destroyRepNode(repNodeId, deleteData, "For testing");
    }

    /**
     * Permanently removes the SNA's knowledge of the
     * {@link oracle.kv.impl.rep.RepNode} with
     * the specified RepNodeId.
     *
     * @param repNodeId the unique identifier of the RepNode to destroy
     * @param deleteData true if the data stored on disk for this RepNode
     *                   should be deleted
     * @param reason the reason for the shutdown, or null
     *
     * @return true if the RepNode existed, false if did not.  The running
     *   state of the RepNode is not relevant to the return value.
     *
     * @throws RuntimeException if the operation failed.
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public boolean destroyRepNode(RepNodeId repNodeId,
                                  boolean deleteData,
                                  String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            return proxyRemote.destroyRepNode(repNodeId, deleteData,
                                              NULL_CTX, getSerialVersion());
        }
        return proxyRemote.destroyRepNode(repNodeId, deleteData, reason,
                                          NULL_CTX, getSerialVersion());
    }

     /**
     * Modifies the parameters of a {@link oracle.kv.impl.rep.RepNode}
     * RepNode managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  If the service needs
     * notification of the new parameters that is done by the admin/planner.
     *
     * @param repNodeParams the new parameters to configure the rep node. This
     * is a full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the RepNode is not configured or the
     * operation failed.
     */
    public void newRepNodeParameters(ParameterMap repNodeParams)
        throws RemoteException {

        proxyRemote.newRepNodeParameters(repNodeParams,
                                         NULL_CTX, getSerialVersion());
    }

    /**
     * Query whether a give ArbNode has been defined on this Storage Node, as
     * indicated by its configuration existing in the store's configuration
     * file.  This is not an indication of its runtime status.
     *
     * @param arbNodeId the unique identifier of the ArbNode
     *
     * @return true if the specified ArbNode exists in the configuration file
     */
    public boolean arbNodeExists(ArbNodeId arbNodeId)
        throws RemoteException {

        return proxyRemote.arbNodeExists(arbNodeId,
                                         NULL_CTX, getSerialVersion());
    }

    /**
     * Creates and starts a {@link ArbNode} instance
     * on this Storage Node.  This will cause a new process to be started to
     * run the ArbNode. The StorageNodeAgent will continue to start this
     * ArbNode if the Storage Node is restarted unless the ArbNode is stopped
     * explicitly.
     *
     * @param arbNodeParams the configuration of the ArbNode to create
     *
     * @return true if the ArbNode was started or existed,
     * false if it was not started
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean createArbNode(ParameterMap arbNodeParams)
        throws RemoteException {

        return proxyRemote.createArbNode(arbNodeParams,
                                         NULL_CTX, getSerialVersion());
    }

    /**
     * Starts a {@link ArbNode} that has already been
     * defined on this Storage Node.  The ArbNode will be started automatically
     * if the Storage Node is restarted or the ArbNode exits unexpectedly.
     *
     * @param arbNodeId the unique identifier of the ArbNode to start
     *
     * @return true if the ArbNode was not already running, false if it was.
     *
     * @throws RuntimeException if the operation failed.
     */
    public boolean startArbNode(ArbNodeId arbNodeId)
        throws RemoteException {

        return proxyRemote.startArbNode(arbNodeId,
                                        NULL_CTX, getSerialVersion());
    }

    /**
     * Stops a {@link ArbNode} that has already been defined on this Storage
     * Node. If reason is non-null, include it in the message logged when the
     * service shuts down. The ArbNode will not be started if the Storage node
     * is restarted until {@link #startArbNode} is called.
     *
     * @param arbNodeId the unique identifier of the ArbNode to stop
     * @param force force a shutdown
     * @param reason the reason for the shutdown, or null
     * @return true if the ArbNode was running, false if it was not.
     * @throws RuntimeException if the operation failed.
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public boolean stopArbNode(ArbNodeId arbNodeId,
                               boolean force,
                               String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            return proxyRemote.stopArbNode(arbNodeId, force, NULL_CTX,
                                           getSerialVersion());
        }

        return proxyRemote.stopArbNode(arbNodeId, force, reason, NULL_CTX,
                                       getSerialVersion());
    }

    /**
     * Permanently removes the SNA's knowledge of the
     * {@link ArbNode} with
     * the specified ArbNodeId.
     *
     * @param arbNodeId the unique identifier of the ArbNode to destroy
     * @param deleteData true if the data stored on disk for this ArbNode
     *                   should be deleted
     * @param reason the reason for the shutdown, or null
     *
     * @return true if the ArbNode existed, false if did not.  The running
     *   state of the ArbNode is not relevant to the return value.
     *
     * @throws RuntimeException if the operation failed.
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public boolean destroyArbNode(ArbNodeId arbNodeId,
                                  boolean deleteData,
                                  String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            return proxyRemote.destroyArbNode(arbNodeId, deleteData,
                                              NULL_CTX, getSerialVersion());
        }
        return proxyRemote.destroyArbNode(arbNodeId, deleteData, reason,
                                          NULL_CTX, getSerialVersion());
    }

    /**
     * Modifies the parameters of a (@link oracle.kv.impl.rep.ArbNode}
     * ArbNode managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  If the service needs
     * notification of the new parameters that is done by the admin/planner.
     *
     * @param arbNodeParams the new parameters to configure the rep node. This
     * is a full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the ArbNode is not configured or the
     * operation failed.
     */
    public void newArbNodeParameters(ParameterMap arbNodeParams)
        throws RemoteException {

        proxyRemote.newArbNodeParameters(arbNodeParams,
                                         NULL_CTX, getSerialVersion());
    }

    /**
     * Modifies the parameters of an {@link oracle.kv.impl.admin.Admin}
     * Admin managed by this StorageNode.  The new parameters will be written
     * out to the storage node's configuration file.  Any required notification
     * is done by the admin/planner.
     *
     * @param adminParams the new parameters to configure the admin.  This is a
     * full set of replacement parameters, not partial.
     *
     * @throws RuntimeException if the admin is not configured or the
     * operation failed.
     */
     public void newAdminParameters(ParameterMap adminParams)
        throws RemoteException {

         proxyRemote.newAdminParameters(adminParams,
                                        NULL_CTX, getSerialVersion());
     }

     /**
      * Checks the specified parameters on the SN.
      *
      * @param params parameters to check
      */
    public void checkParameters(ParameterMap params, ResourceId id)
        throws RemoteException {
        proxyRemote.checkParameters(params, id, NULL_CTX, getSerialVersion());
    }

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
      */
    public void newStorageNodeParameters(ParameterMap params)
        throws RemoteException {

        proxyRemote.newStorageNodeParameters(params,
                                             NULL_CTX, getSerialVersion());
    }

    /**
     * Modifies the global parameters of the current Storage Node. The new
     * parameters will be written out to the storage node's configuration file.
     * Any required notification is done by the admin/planner.
     *
     * @param params the new store-wide global parameters
     *
     * @throws RuntimeException if the StorageNode is not configured or the
     * operation failed.
     */
    public void newGlobalParameters(ParameterMap params)
        throws RemoteException {

        proxyRemote.newGlobalParameters(params, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the SN's current parameters as stored in its configuration
     * file. Used for configuration verification.
     */
    public LoadParameters getParams()
        throws RemoteException {
        return proxyRemote.getParams(NULL_CTX, getSerialVersion());
    }

    public StorageNodeInfo getStorageNodeInfo() throws RemoteException {
        return proxyRemote.getInfo(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the SN's Kerberos service principal information.
     */
    public KrbPrincipalInfo getKrbPrincipalInfo()
        throws RemoteException {
        return proxyRemote.getKrbPrincipalInfo(NULL_CTX, getSerialVersion());
    }

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
     */
    public StringBuilder getStartupBuffer(ResourceId rid)
        throws RemoteException {
        return proxyRemote.getStartupBuffer(rid, NULL_CTX, getSerialVersion());
    }


    /**
     * Snapshot methods.
     */
    public void createSnapshot(RepNodeId rnid, String name)
        throws RemoteException {

        proxyRemote.createSnapshot(rnid, name, NULL_CTX, getSerialVersion());
    }

    public void createSnapshot(AdminId aid, String name)
        throws RemoteException {

        proxyRemote.createSnapshot(aid, name, NULL_CTX, getSerialVersion());
    }

    public void removeSnapshot(RepNodeId rnid, String name)
        throws RemoteException {

        proxyRemote.removeSnapshot(rnid, name, NULL_CTX, getSerialVersion());
    }

    public void removeSnapshot(AdminId aid, String name)
        throws RemoteException {

        proxyRemote.removeSnapshot(aid, name, NULL_CTX, getSerialVersion());
    }

    public void removeAllSnapshots(RepNodeId rnid)
        throws RemoteException {

        proxyRemote.removeAllSnapshots(rnid, NULL_CTX, getSerialVersion());
    }

    public void removeAllSnapshots(AdminId aid)
        throws RemoteException {

        proxyRemote.removeAllSnapshots(aid, NULL_CTX, getSerialVersion());
    }

    public String [] listSnapshots()
        throws RemoteException {

        return proxyRemote.listSnapshots(NULL_CTX, getSerialVersion());
    }

    public void createSnapshotConfig(String snapshotName)
        throws RemoteException {
        proxyRemote.createSnapshotConfig(
            snapshotName, NULL_CTX, getSerialVersion());
    }

    public void removeSnapshotConfig(String snapshotName)
        throws RemoteException {
        proxyRemote.removeSnapshotConfig(
            snapshotName, NULL_CTX, getSerialVersion());
    }

    public void removeAllSnapshotConfigs()
        throws RemoteException {
        proxyRemote.removeAllSnapshotConfigs(NULL_CTX, getSerialVersion());
    }

    public void updateNodeStatus(ResourceId serviceId,
                                 ServiceStatusChange newStatus)
        throws RemoteException
    {
        proxyRemote.updateNodeStatus(serviceId, newStatus, NULL_CTX,
                                     getSerialVersion());
    }

    public void updateAdminStatus(ServiceStatusChange newStatus,
                                  boolean isMaster)
        throws RemoteException
    {
        proxyRemote.updateAdminStatus(newStatus, isMaster, NULL_CTX,
                                      getSerialVersion());
    }

    public void receiveStats(ResourceId serviceId, StatsPacket packet)
        throws RemoteException
    {
        proxyRemote.receiveStats(serviceId, packet, NULL_CTX,
                                 getSerialVersion());
    }

    public void receiveAdminStats(StatsPacket packet)
        throws RemoteException
    {
        proxyRemote.receiveAdminStats(packet, NULL_CTX, getSerialVersion());
    }

    public void receiveNewParams(ResourceId serviceId, ParameterMap newMap)
        throws RemoteException
    {
        proxyRemote.receiveNewParams(serviceId, newMap, NULL_CTX,
                                     getSerialVersion());
    }

    public void updatePlanStatus(String planStatus) throws RemoteException {
        proxyRemote.updatePlanStatus(planStatus, NULL_CTX, getSerialVersion());
    }

    public void updateReplicationState(RepNodeId rnId,
                                       StateChangeEvent changeEvent)
        throws RemoteException
    {
        proxyRemote.updateReplicationState(rnId, changeEvent, NULL_CTX,
                                           getSerialVersion());
    }

    /**
     * Methods related to master balancing
     */
    public void noteState(StateInfo stateInfo)
        throws RemoteException {

        proxyRemote.noteState(stateInfo, NULL_CTX, getSerialVersion());
    }

    public MDInfo getMDInfo()
        throws RemoteException {

        return proxyRemote.getMDInfo(NULL_CTX, getSerialVersion());
    }

    public boolean getMasterLease(final MasterLeaseInfo masterLease)
        throws RemoteException {

        return proxyRemote.getMasterLease(masterLease,
                                          NULL_CTX, getSerialVersion());
    }

    public boolean cancelMasterLease(final StorageNode lesseeSN,
                                     final RepNode rn)
        throws RemoteException {

        return proxyRemote.cancelMasterLease(lesseeSN, rn,
                                             NULL_CTX, getSerialVersion());
    }

    public void overloadedNeighbor(StorageNodeId storageNodeId)
        throws RemoteException {

        proxyRemote.overloadedNeighbor(storageNodeId,
                                       NULL_CTX, getSerialVersion());
    }

    /**
     * Returns information about TLS credentials for this SNA as a
     * JSON-formatted string.
     *
     * <p>The result will have "installedCredentials" and "pendingUpdates"
     * entries, each with "keystore" and "truststore" entries. For each
     * credential file, the value will have a "file" entry that provides the
     * name of the file relative to the security subdirectory under the SNA's
     * root directory, a "hash" entry that represents the SHA-256 hash of the
     * file contents represented as a 64-character hexadecimal value or null if
     * the file was not found, and, if the file was found, a "modTime" entry
     * that represents the modification time of the file in the UTC timezone.
     *
     * <p>Sample output:
     *
     * {@literal
     * {
     *   "installedCredentials": {
     *     "keystore": {
     *       "file": "store.keys",
     *       "hash":
     *      "b157d9dc61bed1e2642425adb6791dd13e2c0868160b693963a85596130c388d",
     *       "modTime": "2024-06-07 09:21:53 UTC"
     *     },
     *     "truststore": {
     *       "file": "store.trust",
     *       "hash":
     *      "c5a4c67820b64d324a1b8ed493041e04ef91697f64d4a605d750b9dbe8708f6b",
     *       "modTime": "2024-06-07 09:21:57 UTC"
     *     }
     *   },
     *   "pendingUpdates": {
     *     "truststore": {
     *       "file": "updates/store.trust",
     *       "hash":
     *      "4fe0231942e732d829cf97cb3742c9aa86ae06db8a80939b6fb834382c205a53",
     *       "modTime": "2024-06-08 10:00:00 UTC"
     *     }
     *   }
     * }
     * }
     *
     * @return information about TLS credentials in JSON format
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String getTlsCredentialsInfo() throws RemoteException {
        checkSerialVersion(TLS_CREDENTIALS_VERSION, "getTlsCredentialsInfo");
        return proxyRemote.getTlsCredentialsInfo(NULL_CTX, getSerialVersion());
    }

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
    public String retrieveTlsCredentials() throws RemoteException {
        checkSerialVersion(TLS_CREDENTIALS_VERSION, "retrieveTlsCredentials");
        return proxyRemote.retrieveTlsCredentials(
            NULL_CTX, getSerialVersion());
    }

    /**
     * Verify the correctness of TLS credential updates, returning the hashes
     * of the keystore and truststore files that will be installed after the
     * updates, if any, are installed. If the force flag was specified, skip
     * verifying the credentials within the keystores.
     *
     * @param force whether the force flag was specified.
     * @return the credential hashes
     * @throws RemoteException if a network failure occurs
     * @throws IllegalStateException if a problem is detected
     * @since 24.4
     */
    public CredentialHashes verifyTlsCredentialUpdates(boolean force)
        throws RemoteException
    {
        checkSerialVersion(TLS_CREDENTIALS_VERSION,
                           "verifyTlsCredentialUpdates");
        return proxyRemote.verifyTlsCredentialUpdates(
            force, NULL_CTX, getSerialVersion());
    }

    /**
     * Keystore and truststore hashes.
     *
     * @since 24.4
     */
    public static class CredentialHashes implements Serializable {
        private static final long serialVersionUID = 1;
        public final HashInfo keystore;
        public final HashInfo truststore;

        public static class HashInfo implements Serializable {
            private static final long serialVersionUID = 1;
            public final String hash;
            public final boolean update;
            public HashInfo(String hash, boolean update) {
                this.hash = hash;
                this.update = update;
            }
            @Override
            public String toString() {
                return String.format("%s/%s",
                                     hash,
                                     update ? "update" : "install");
            }
        }

        public CredentialHashes(String keystoreHash,
                                boolean keystoreUpdate,
                                String truststoreHash,
                                boolean truststoreUpdate) {
            this.keystore = new HashInfo(keystoreHash, keystoreUpdate);
            this.truststore = new HashInfo(truststoreHash, truststoreUpdate);
        }

        @Override
        public String toString() {
            return String.format("{keystore=%s truststore=%s}",
                                 keystore,
                                 truststore);
        }
    }

    /**
     * Add entries from the truststore update found in the update directory to
     * the installed truststore, and update the client truststore.
     *
     * @returns a description of the update performed
     * @throws IllegalStateException if the update fails
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String addTruststoreUpdates() throws RemoteException {
        checkSerialVersion(TLS_CREDENTIALS_VERSION, "addTruststoreUpdates");
        return proxyRemote.addTruststoreUpdates(NULL_CTX, getSerialVersion());
    }

    /**
     * Install the keystore update found in the update directory.
     *
     * @returns a description of the update performed
     * @throws IllegalStateException if the update fails
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String installKeystoreUpdate(String keystoreHash)
        throws RemoteException
    {
        checkSerialVersion(TLS_CREDENTIALS_VERSION, "installKeystoreUpdate");
        return proxyRemote.installKeystoreUpdate(keystoreHash, NULL_CTX,
                                                 getSerialVersion());
    }

    /**
     * Install the truststore update found in the update directory and update
     * the client truststore.
     *
     * @returns a description of the update performed
     * @throws IllegalStateException if the update fails
     * @throws RemoteException if a network failure occurs
     * @since 24.4
     */
    public String installTruststoreUpdate(String truststoreHash)
        throws RemoteException
    {
        checkSerialVersion(TLS_CREDENTIALS_VERSION,
                           "installTruststoreUpdate");
        return proxyRemote.installTruststoreUpdate(truststoreHash, NULL_CTX,
                                                   getSerialVersion());
    }

    /**
     * Checks if the target node's serial version is greater than or equal to
     * version. Throws an UnsupportedOperationException if the check fails.
     *
     * @param version the required serial version
     * @param method method name to include in the exception
     */
    private void checkSerialVersion(short version, String method) {
        if (getSerialVersion() < version) {
            throw new UnsupportedOperationException(
                 "There was an attempt to invoke " + method + " on a " +
                 "StorageNode that is running an earlier, incompatible " +
                 "release.  Please upgrade all components of the store to " +
                 SerialVersion.getKVVersion(version).getNumericVersionString() +
                 " or above. (Internal local version=" + version +
                ", internal service version=" + getSerialVersion() + ")");
        }
    }
}
