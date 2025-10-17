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

package oracle.kv.impl.admin;

import static oracle.kv.impl.util.SerialVersion.BEFORE_IMAGE_VERSION;
import static oracle.kv.impl.util.SerialVersion.JSON_COLLECTION_VERSION;
import static oracle.kv.impl.util.SerialVersion.SHUTDOWN_REASON_VERSION;
import static oracle.kv.impl.util.SerialVersion.TABLE_MD_IN_STORE_VERSION;
import static oracle.kv.impl.util.SerialVersion.TLS_CREDENTIALS_VERSION;

import java.net.URI;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogRecord;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Snapshot.SnapResultSummary;
import oracle.kv.impl.admin.Snapshot.SnapshotOperation;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.RemoteAPI;
import oracle.kv.table.FieldDef;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.ErrorMessage;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This is the interface used by the command line client.
 */
public final class CommandServiceAPI extends RemoteAPI {

    private static final AuthContext NULL_CTX = null;

    private final CommandService proxyRemote;

    private CommandServiceAPI(CommandService remote, LoginHandle loginHdl)
        throws RemoteException {

        super(remote);
        this.proxyRemote =
            ContextProxy.create(remote, loginHdl, getSerialVersion());
    }

    public static CommandServiceAPI wrap(CommandService remote,
                                         LoginHandle loginHdl)
        throws RemoteException {

        return new CommandServiceAPI(remote, loginHdl);
    }

    /**
     * Throws UnsupportedOperationException if a method is not supported by the
     * admin service.
     */
    private void checkMethodSupported(short expectVersion)
        throws UnsupportedOperationException {

        if (getSerialVersion() < expectVersion) {
            final String errMsg =
                    "Command not available because service has not yet been" +
                    " upgraded.  (Internal local version=" + expectVersion +
                    ", internal service version=" + getSerialVersion() + ")";
            throw new AdminFaultException(
                new UnsupportedOperationException(errMsg), errMsg,
                ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }
    }

    /**
     * Returns the CommandService's status, which can only be RUNNNING.
     */
    public ServiceStatus ping()
        throws RemoteException {

        return proxyRemote.ping(NULL_CTX, getSerialVersion());
    }

    /**
     * Return a list of the names of all storage node pools.
     */
    public List<String> getStorageNodePoolNames()
        throws RemoteException {

        return proxyRemote.getStorageNodePoolNames(NULL_CTX,
                                                   getSerialVersion());
    }

    /**
     * Add a new StorageNodePool.
     */
    public void addStorageNodePool(String name)
        throws RemoteException {

        proxyRemote.addStorageNodePool(name, NULL_CTX, getSerialVersion());
    }

    /**
     * Clone an existing Storage Node pool so as to a new StorageNodePool.
     */
    public void cloneStorageNodePool(String name, String source)
        throws RemoteException {

        proxyRemote.cloneStorageNodePool(name, source, NULL_CTX,
                                         getSerialVersion());
    }

    public void removeStorageNodePool(String name)
        throws RemoteException {

        proxyRemote.removeStorageNodePool(name, NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of the storage node ids in a pool.
     */
    public List<StorageNodeId> getStorageNodePoolIds(String name)
        throws RemoteException {

        return proxyRemote.getStorageNodePoolIds(name, NULL_CTX,
                                                 getSerialVersion());
    }

    /**
     * Add a storage node to the pool with the given name.
     */
    public void addStorageNodeToPool(String name, StorageNodeId snId)
        throws RemoteException {

        proxyRemote.addStorageNodeToPool(name, snId, NULL_CTX,
                                         getSerialVersion());
    }

    /**
     * Removed a storage node from the pool with the given name.
     */
    public void removeStorageNodeFromPool(String name, StorageNodeId snId)
        throws RemoteException {

        proxyRemote.removeStorageNodeFromPool(name, snId, NULL_CTX,
                                              getSerialVersion());
    }

    /**
     * TODO: delete? Unused.
     */
    public void replaceStorageNodePool(String name, List<StorageNodeId> ids)
        throws RemoteException {

        proxyRemote.replaceStorageNodePool(name, ids, NULL_CTX,
                                           getSerialVersion());
    }

    /**
     * Create a topology with the "topology create" command.
     */
    public String createTopology(String candidateName,
                                 String snPoolName,
                                 int numPartitions,
                                 boolean json)
        throws RemoteException {
        return createTopology(candidateName, snPoolName, numPartitions,
                              json, SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
    }

    /**
     * Create a topology with the "topology create" command.
     * @param jsonVersion The serial version that supports specific JSON
     * output
     */
    public String createTopology(String candidateName,
                                 String snPoolName,
                                 int numPartitions,
                                 boolean json,
                                 short jsonVersion)
        throws RemoteException {

        checkMethodSupported(jsonVersion);
        return proxyRemote.createTopology(
            candidateName, snPoolName, numPartitions, json, jsonVersion,
            NULL_CTX, getSerialVersion());
    }

    /**
     * Copy a topology with the "topology clone-current" command.
     */
    public String copyCurrentTopology(String candidateName)
        throws RemoteException {

        return proxyRemote.copyCurrentTopology(candidateName, NULL_CTX,
                                               getSerialVersion());
    }

    /**
     * Copy a topology with the "topology clone" command.
     */
    public String copyTopology(String candidateName, String sourceCandidate)
        throws RemoteException {

        return proxyRemote.copyTopology(candidateName, sourceCandidate,
                                        NULL_CTX, getSerialVersion());
    }

    /**
     * List topology candidates with the "topology list" command.
     */
    public List<String> listTopologies()
        throws RemoteException {

        return proxyRemote.listTopologies(NULL_CTX, getSerialVersion());
    }

    /**
     * Delete a topology candidate with the "topology delete" command.
     */
    public String deleteTopology(String candidateName)
        throws RemoteException {

        return proxyRemote.deleteTopology(candidateName, NULL_CTX,
                                          getSerialVersion());
    }

    /**
     * Rebalance a topology with the "topology rebalance" command.
     */
    public String rebalanceTopology(String candidateName, String snPoolName,
                                    DatacenterId dcId)
        throws RemoteException {

        return proxyRemote.rebalanceTopology(candidateName, snPoolName,
                                             dcId, NULL_CTX, getSerialVersion());
    }

    /**
     * Change the topology with the "topology change-repfactor" command.
     */
    public String changeRepFactor(String candidateName, String snPoolName,
                                  DatacenterId dcId, int repFactor)
        throws RemoteException {

        return proxyRemote.changeRepFactor(
            candidateName, snPoolName, dcId,
            repFactor, NULL_CTX, getSerialVersion());
    }

    /**
     * Change a topology with the "topology change-zone-type" command.
     */
    public String changeZoneType(String candidateName, DatacenterId dcId,
                                 DatacenterType type)
        throws RemoteException {
        return proxyRemote.changeZoneType(candidateName, dcId, type,
                                          NULL_CTX, getSerialVersion());
    }

    public String changeZoneMasterAffinity(String candidateName,
                                           DatacenterId dcId,
                                           boolean masterAffinity)
        throws RemoteException {
        return proxyRemote.changeZoneMasterAffinity(candidateName, dcId,
                                                    masterAffinity, NULL_CTX,
                                                    getSerialVersion());
    }

    /**
     * Change a topology with the "topology change-zone-arbiters" command.
     */
    public String changeZoneArbiters(String candidateName, DatacenterId dcId,
                                     boolean allowArbiters)
        throws RemoteException {

        return proxyRemote.changeZoneArbiters(candidateName,
                                              dcId,
                                              allowArbiters,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Change the topology with the "topology redistribute" command.
     */
    public String redistributeTopology(String candidateName, String snPoolName)
        throws RemoteException {

        return proxyRemote.redistributeTopology(candidateName, snPoolName,
                                                NULL_CTX, getSerialVersion());
    }

    /**
     * Change the topology with the "topology contract" command.
     */
    public String contractTopology(String candidateName, String snPoolName)
        throws RemoteException {

        return proxyRemote.contractTopology(candidateName, snPoolName,
                                            NULL_CTX, getSerialVersion());
    }

    /**
     * Change the topology with the "topology remove-shard" command
     * by removing failed shard from topology.
     */
    public String removeFailedShard(RepGroupId failedShard,
                                    String candidateName)
        throws RemoteException {

        return proxyRemote.removeFailedShard(failedShard,
                                             candidateName, NULL_CTX,
                                             getSerialVersion());
    }

    /**
     * Show the transformation steps required to go from topology candidate
     * startTopoName to candidate targetTopoName, using the version 1 JSON
     * output format.
     */
    public String preview(String targetTopoName, String startTopoName,
                          boolean verbose)
        throws RemoteException {

        return preview(targetTopoName, startTopoName, verbose,
                       SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
    }

    /**
     * Show the transformation steps required to go from topology candidate
     * startTopoName to candidate targetTopoName, using the JSON output format
     * specified for the specified serial version.
     *
     * @param jsonVersion The serial version that supports specific JSON
     * output
     */
    public String preview(String targetTopoName, String startTopoName,
                          boolean verbose, short jsonVersion)
        throws RemoteException {
        checkMethodSupported(jsonVersion);
        return proxyRemote.preview(targetTopoName, startTopoName,
                                   verbose, jsonVersion,
                                   NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of the Admins and their parameters.
     */
    public List<ParameterMap> getAdmins()
        throws RemoteException {

        return proxyRemote.getAdmins(NULL_CTX, getSerialVersion());
    }

    /**
     * Get the specified plan.
     */
    public Plan getPlanById(int planId)
        throws RemoteException {

        return proxyRemote.getPlanById(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Approve the identified plan.
     */
    public void approvePlan(int planId)
        throws RemoteException {

        proxyRemote.approvePlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Starts the execution of the specified plan. The plan will proceed
     * asynchronously. The force parameter is passed to the plan when executed.
     * Each plan will have a its own interpretation of the force value,
     * including ignoring it.
     *
     * @param force value passed to the plan at execution time
     */
    public void executePlan(int planId, boolean force)
        throws RemoteException {

        proxyRemote.executePlan(planId, force, NULL_CTX, getSerialVersion());
    }

    /**
     * Wait for the plan to finish. If a timeout period is specified, return
     * either when the plan finishes or the timeout occurs.
     * @return the current plan status when the call returns. If the call timed
     * out, the plan may still be running.
     * @throws RemoteException
     */
    public Plan.State awaitPlan(int planId, int timeout, TimeUnit timeUnit)
        throws RemoteException {
        return proxyRemote.awaitPlan(planId, timeout, timeUnit, NULL_CTX,
                                     getSerialVersion());
    }

    /**
     * Hidden command - check that a plan has a status of SUCCESS, and throw
     * an exception containing error information if it has failed.
     */
    public void assertSuccess(int planId)
         throws RemoteException {
         proxyRemote.assertSuccess(planId, NULL_CTX, getSerialVersion());
    }
    /**
     * Cancel a plan.
     */
    public void cancelPlan(int planId)
        throws RemoteException {

        proxyRemote.cancelPlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Interrupt a plan.
     */
    public void interruptPlan(int planId)
        throws RemoteException {

        proxyRemote.interruptPlan(planId, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to deploy a new Datacenter.
     */
    public int createDeployDatacenterPlan(String planName,
                                          String datacenterName,
                                          int repFactor,
                                          DatacenterType datacenterType,
                                          boolean allowArbiters,
                                          boolean masterAffinity)
        throws RemoteException {

        return proxyRemote.createDeployDatacenterPlan(
            planName, datacenterName, repFactor, datacenterType,
            allowArbiters, masterAffinity,
            NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to deploy a new StorageNode.
     */
    public int createDeploySNPlan(String planName,
                                  DatacenterId datacenterId,
                                  String hostName,
                                  int registryPort,
                                  String comment)
        throws RemoteException {

        return proxyRemote.createDeploySNPlan(planName, datacenterId, hostName,
                                              registryPort, comment,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to deploy a new primary Admin service instance.
     */
    public int createDeployAdminPlan(String planName,
                                     StorageNodeId snid)
        throws RemoteException {

        return createDeployAdminPlan(planName, snid,
                                     AdminType.PRIMARY);

    }

    /**
     * Create a new Plan to deploy a new Admin service instance with the
     * specified type. If type is null, the type of the Admin defaults to
     * the type of zone in which the Admin is deployed.
     */
    public int createDeployAdminPlan(String planName,
                                     StorageNodeId snid,
                                     AdminType type)
        throws RemoteException {

        return proxyRemote.createDeployAdminPlan(planName, snid,
                                                 0/*vestigial http port argument*/,
                                                 type, NULL_CTX,
                                                 getSerialVersion());
    }

    /**
     * Create a new Plan to remove the specified Admin (if <code>aid</code> is
     * non-<code>null</code> and <code>dcid</code> is <code>null</code>), or
     * all Admins deployed to the specified datacenter (if <code>dcid</code> is
     * non-<code>null</code> and <code>aid</code> is <code>null</code>).
     *
     * @param planName the name to assign to the created Plan
     *
     * @param dcid the id of the datacenter containing the Admins to remove.
     * If this parameter and the <code>aid</code> parameter are both
     * non-<code>null</code> or both <code>null</code>, then an
     * <code>IllegalArgumentException</code> is thrown.
     *
     * @param aid the id of the specific Admin to remove. If this parameter
     * and the <code>dcid</code> parameter are both non-<code>null</code> or
     * both <code>null</code>, then an <code>IllegalArgumentException</code>
     * is thrown.
     *
     * @param failedSN if true, remove admin hosted on a failed SN
     *
     * @throws IllegalArgumentException if the <code>dcid</code> parameter and
     * the <code>aid</code> parameter are both non-<code>null</code> or both
     * <code>null</code>.
     */
    public int createRemoveAdminPlan(String planName,
                                     DatacenterId dcid,
                                     AdminId aid,
                                     boolean failedSN)
        throws RemoteException {

        if (dcid != null && aid != null) {
            throw new IllegalArgumentException(
                "dcid and aid parameters cannot both be non-null");
        }

        if (dcid == null && aid == null) {
            throw new IllegalArgumentException(
                "dcid and aid parameters cannot both be null");
        }

        return proxyRemote.createRemoveAdminPlan(planName, dcid, aid, failedSN,
                                                 NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to shut down the repnodes in a kvstore.
     */
    public int createStopAllRepNodesPlan(String planName)
        throws RemoteException {

        return proxyRemote.createStopAllRepNodesPlan(planName, NULL_CTX,
                                                     getSerialVersion());
    }

    /**
     * Create a new Plan to start up the repnodes in a kvstore.
     */
    public int createStartAllRepNodesPlan(String planName)
        throws RemoteException {

        return proxyRemote.createStartAllRepNodesPlan(planName, NULL_CTX,
                                                      getSerialVersion());
    }

    /**
     * Stop a given set of services.
     */
    public int createStopServicesPlan(String planName,
                                      Set<? extends ResourceId> serviceIds)
        throws RemoteException {

        return proxyRemote.createStopServicesPlan(planName, serviceIds,
                                                  NULL_CTX, getSerialVersion());
    }

    /**
     * Start a given set of services.
     */
    public int createStartServicesPlan(String planName,
                                       Set<? extends ResourceId> serviceIds)
        throws RemoteException {

        return proxyRemote.createStartServicesPlan(planName, serviceIds,
                                                   NULL_CTX,
                                                   getSerialVersion());
    }

    /**
     * Create a new Plan to alter a service's parameters.
     */
    public int createChangeParamsPlan(String planName,
                                      ResourceId rid,
                                      ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeParamsPlan(
            planName, rid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for all RepNodes deployed to the
     * specified datacenter; or all RepNodes in all datacenters if
     * <code>null</code> is input for the <code>dcid</code> parameter.
     */
    public int createChangeAllParamsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeAllParamsPlan(
            planName, dcid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for all ArbNodes deployed to the
     * specified datacenter; or all ArbNodes in all datacenters if
     * <code>null</code> is input for the <code>dcid</code> parameter.
     */
    public int createChangeAllANParamsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeAllANParamsPlan(
            planName, dcid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for all Admins deployed to the
     * specified datacenter; or all Admins in all datacenters if
     * <code>null</code> is input for the <code>dcid</code> parameter.
     */
    public int createChangeAllAdminsPlan(String planName,
                                         DatacenterId dcid,
                                         ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeAllAdminsPlan(
            planName, dcid, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for global scoped, non-security
     * parameters. The specified parameters will be changed for all
     * SN-managed components in the store.
     *
     * @since 4.3
     */
    public int createChangeGlobalComponentsParamsPlan(String planName,
                                                      ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeGlobalComponentsParamsPlan(
            planName, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to alter parameters for global security parameters.
     * The specified parameters will be changed for all admin and repnode
     * services from storage nodes in the store.
     */
    public int createChangeGlobalSecurityParamsPlan(String planName,
                                                    ParameterMap newParams)
        throws RemoteException {

        return proxyRemote.createChangeGlobalSecurityParamsPlan(
            planName, newParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to add a user of kvstore.
     */
    public int createCreateUserPlan(String planName,
                                    String userName,
                                    boolean isEnabled,
                                    boolean isAdmin,
                                    char[] plainPassword)
        throws RemoteException {

        return proxyRemote.createCreateUserPlan(
            planName, userName, isEnabled, isAdmin, plainPassword,
            NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to move all services from the old storage node to a
     * new storage node.
     */
    public int createMigrateSNPlan(String planName,
                                   StorageNodeId oldNode,
                                   StorageNodeId newNode)
        throws RemoteException {

        return proxyRemote.createMigrateSNPlan(planName, oldNode, newNode,
                                               0/*unused http port*/, NULL_CTX,
                                               getSerialVersion());
    }

    /**
     * Create a new Plan to remove a storageNode from the store. The SN must
     * be stopped and must not host any services.
     */
    public int createRemoveSNPlan(String planName, StorageNodeId targetNode)
        throws RemoteException {

        return proxyRemote.createRemoveSNPlan(planName, targetNode,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to remove a datacenter from the store. The
     * datacenter must be empty.
     */
    public int createRemoveDatacenterPlan(String planName,
                                          DatacenterId targetId)
        throws RemoteException {

        return proxyRemote.createRemoveDatacenterPlan(
            planName, targetId, NULL_CTX, getSerialVersion());
    }

    /**
     * Create a plan that will alter the topology. Topology candidates are
     * created with the CLI topology
     * {create|rebalance|redistribute|change-repfactor|move-repnode| commands.
     *
     * Allow to pass failedShard identifier during deploy topology after
     * running remove-shard topology command for removing failed shard.
     */
    public int createDeployTopologyPlan(String planName, String candidateName,
                                        RepGroupId failedShard)
        throws RemoteException {

        return proxyRemote.createDeployTopologyPlan(planName,
                                                    candidateName,
                                                    failedShard,
                                                    NULL_CTX,
                                                    getSerialVersion());
    }

    /**
     * Create a plan that will perform a failover by converting offline zones
     * to secondary zones and secondary zones to primary zones.
     *
     * @param planName the name of the plan, or {@code null} for the default
     * name
     * @param newPrimaryZones the IDs of zones to make primary zones
     * @param offlineZones the IDs of offline zones
     * @return the ID of the new plan
     * @throws IllegalCommandException if any of the IDs specified in
     * newPrimaryZones or offlineZones are not associated with existing zones,
     * or if the specified zones and topology do not result in any online
     * primary zones
     * @throws UnsupportedOperationException if the operation is not supported
     * by the current serial version
     * @throws RemoteException if a communication error occurs
     * @since 3.4
     */
    public int createFailoverPlan(String planName,
                                  Set<DatacenterId> newPrimaryZones,
                                  Set<DatacenterId> offlineZones)
        throws RemoteException {

        return proxyRemote.createFailoverPlan(
            planName, newPrimaryZones, offlineZones, NULL_CTX,
            getSerialVersion());
    }

    /**
     * Create a plan that will repair mismatches in the topology.
     */
    public int createRepairPlan(String planName)
        throws RemoteException {

        return proxyRemote.createRepairPlan(planName,
                                            NULL_CTX,
                                            getSerialVersion());
    }

    /*
     * NOTE: this method/mechanism is not used by normal table
     * creation anymore. It's used by the (deprecated) CLI.
     * DDL-created tables use a different pattern, via DDLHandler and
     * TableDdlOperation.
     */
    @SuppressWarnings("deprecation")
    public int createAddTablePlan(String planName,
                                  String namespace,
                                  String tableName,
                                  String parentName,
                                  FieldMap fieldMap,
                                  List<String> primaryKey,
                                  List<Integer> primaryKeySizes,
                                  List<String> majorKey,
                                  TimeToLive ttl,
                                  TableLimits limits,
                                  boolean r2compat,
                                  int schemaId,
                                  boolean jsonCollection,
                                  Map<String, FieldDef.Type> mrCounters,
                                  String description)
        throws RemoteException {

        if (jsonCollection) {
            /*
             * jsonCollection was added earlier but not used until this version
             */
            checkMethodSupported(JSON_COLLECTION_VERSION);
        }

        namespace = NameUtils.switchToInternalUse(namespace);

        final short serialVersion = getSerialVersion();
        /*
         * call older method if not using JSON collection in case the
         * server is older. This can be removed when the pre-req changes.
         */
        if (!jsonCollection) {
            return proxyRemote.createAddTablePlan(
                planName, namespace, tableName,
                parentName, fieldMap,
                primaryKey, primaryKeySizes,
                majorKey, ttl, limits,
                r2compat, schemaId,
                description,
                NULL_CTX,
                serialVersion);
        }

        return proxyRemote.createAddTablePlan(planName, namespace, tableName,
                                              parentName, fieldMap,
                                              primaryKey, primaryKeySizes,
                                              majorKey, ttl, limits,
                                              r2compat, schemaId,
                                              jsonCollection, mrCounters,
                                              description,
                                              NULL_CTX,
                                              serialVersion);
    }

    public int createRemoveTablePlan(final String planName,
                                     final String namespace,
                                     final String tableName)
        throws RemoteException {

        final String namespaceName = NameUtils.switchToInternalUse(namespace);
        return proxyRemote.createRemoveTablePlan(planName,
                                                 namespaceName,
                                                 tableName,
                                                 true, /* remove data */
                                                 NULL_CTX,
                                                 getSerialVersion());
    }

    public int createAddIndexPlan(String planName,
                                  String namespace,
                                  String indexName,
                                  String tableName,
                                  String[] indexedFields,
                                  FieldDef.Type[] indexedTypes,
                                  boolean indexNulls,
                                  String description)
        throws RemoteException {

        namespace = NameUtils.switchToInternalUse(namespace);

        final short serialVersion = getSerialVersion();
        return proxyRemote.createAddIndexPlan(planName,
                                              namespace,
                                              indexName,
                                              tableName,
                                              indexedFields,
                                              indexedTypes,
                                              indexNulls,
                                              false,
                                              description,
                                              NULL_CTX,
                                              serialVersion);
    }

    public int createRemoveIndexPlan(String planName,
                                     String namespace,
                                     String indexName,
                                     String tableName)
        throws RemoteException {

        namespace = NameUtils.switchToInternalUse(namespace);

        return proxyRemote.createRemoveIndexPlan(planName,
                                                 namespace,
                                                 indexName,
                                                 tableName,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }

    public int createEvolveTablePlan(String planName,
                                     String namespace,
                                     String tableName,
                                     int tableVersion,
                                     FieldMap fieldMap,
                                     TimeToLive ttl,
                                     TimeToLive beforeImgTTL,
                                     Set<Integer> regions)
        throws RemoteException {

        namespace = NameUtils.switchToInternalUse(namespace);

        final short serialVersion = getSerialVersion();

        if (serialVersion < BEFORE_IMAGE_VERSION) {
            return proxyRemote.createEvolveTablePlan(planName,
                                                     namespace,
                                                     tableName,
                                                     tableVersion, fieldMap,
                                                     ttl,
                                                     /* ignore before img ttl */
                                                     null,
                                                     regions,
                                                     NULL_CTX,
                                                     serialVersion);
        }

        return proxyRemote.createEvolveTablePlan(planName,
                                                 namespace,
                                                 tableName,
                                                 tableVersion, fieldMap,
                                                 ttl,
                                                 beforeImgTTL,
                                                 regions,
                                                 NULL_CTX,
                                                 serialVersion);
    }

    public int createNetworkRestorePlan(String planName,
                                        ResourceId sourceNode,
                                        ResourceId targetNode,
                                        boolean retainOrigLog)
        throws RemoteException {

        final short serialVersion = getSerialVersion();

        return proxyRemote.createNetworkRestorePlan(planName,
                                                    sourceNode,
                                                    targetNode,
                                                    retainOrigLog,
                                                    NULL_CTX,
                                                    serialVersion);
    }

    /**
     * Create a plan to set the type of client request enabled for the whole
     * store or specified shards.
     *
     * @param planName the name of plan
     * @param requestType type of client request enabled
     * @param resourceIds resource IDs to be set enabled request type,
     *        currently only can be shard IDs
     * @param entireStore if true the whole store will be configured with given
     *        type for client request
     */
    public int createEnableRequestsPlan(String planName,
                                        String requestType,
                                        Set<? extends ResourceId> resourceIds,
                                        boolean entireStore)
        throws RemoteException {

        final short serialVersion = getSerialVersion();

        return proxyRemote.createEnableRequestsPlan(planName,
                                                    requestType,
                                                    resourceIds,
                                                    entireStore,
                                                    NULL_CTX,
                                                    serialVersion);
    }

   /**
     * Configure the Admin with a store name.  This command can be used only
     * when the AdminService is running in bootstrap/configuration mode.
     */
    public void configure(String storeName)
        throws RemoteException {

        proxyRemote.configure(storeName, NULL_CTX, getSerialVersion());
    }

    /**
     * Checks if the store is ready.
     *
     * If forTables is false returns true if the store is deployed and ready
     * for Key/Value operations. If forTables is true, returns true if the
     * store is ready and all system tables have been created and initialized.
     *
     * If forTables is true this method may return false during an upgrade if
     * there are new system tables and need to be created.
     *
     * If forTables is true writes to system tables will be enabled. Therefore,
     * it should not be set unless the caller is certain that the store will
     * only be used for table operations.
     */
    public boolean isStoreReady(boolean forTables)
            throws RemoteException {

        checkMethodSupported(TABLE_MD_IN_STORE_VERSION);
        return proxyRemote.isStoreReady(forTables,
                                        NULL_CTX, getSerialVersion());
    }

    /**
     * If configured, return the store name, otherwise, null.
     */
    public String getStoreName()
        throws RemoteException {

        return proxyRemote.getStoreName(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the pathname of the KV root directory (KVHOME).
     */
    public String getRootDir()
        throws RemoteException {

        return proxyRemote.getRootDir(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the current realized topology for listing or browsing.
     */
    public Topology getTopology()
        throws RemoteException {

        return proxyRemote.getTopology(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the brief and detailed description of all users for showing
     *
     *@return a sorted map of {username, {brief info, detailed info}}
     */
    public Map<String, UserDescription> getUsersDescription()
        throws RemoteException {

        return proxyRemote.getUsersDescription(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the specified Metadata for listing or browsing.
     */
    @Nullable
    public <T extends Metadata<? extends MetadataInfo>> T
                                         getMetadata(final Class<T> returnType,
                                                     final MetadataType metadataType)
        throws RemoteException {

        return proxyRemote.getMetadata(returnType, metadataType,
                                       NULL_CTX, getSerialVersion());
    }

    public void putMetadata(final Metadata<?> metadata)
        throws RemoteException {

        proxyRemote.putMetadata(metadata, NULL_CTX, getSerialVersion());
    }

    /**
     * Retrieve the topology that corresponds to this candidate name.  Invoked
     * with the "topology view candidateName" command.
     */
    public TopologyCandidate getTopologyCandidate(String candidateName)
        throws RemoteException {

        return proxyRemote.getTopologyCandidate(candidateName, NULL_CTX,
                                                getSerialVersion());
    }

    /**
     * Return the whole Parameters from the admin database for listing or
     * browsing.
     */
    public Parameters getParameters()
        throws RemoteException {

        return proxyRemote.getParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the RepNodeParameters from the admin database for the specified
     * node.
     */
    public ParameterMap getRepNodeParameters(RepNodeId id)
        throws RemoteException {

        return proxyRemote.getRepNodeParameters(
            id, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the Policy parameters from the admin database.
     */
    public ParameterMap getPolicyParameters()
        throws RemoteException {

        return proxyRemote.getPolicyParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Notifies this admin that new parameters are available in its storage
     * node configuration file and should be reread.
     */
    public void newParameters()
        throws RemoteException {

        proxyRemote.newParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Notifies this admin that new global parameters are available in its
     * storage node configuration file and should be reread.
     */
    public void newGlobalParameters()
        throws RemoteException {

        proxyRemote.newGlobalParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Notifies this admin that new security metadata changes are available and
     * need to be applied.
     */
    public void newSecurityMDChange()
        throws RemoteException {

        proxyRemote.newSecurityMDChange(NULL_CTX, getSerialVersion());
    }

    /**
     * Stop the admin service, without specifying a reason. Convenience method
     * for testing only.
     */
    public void stop(boolean force)
        throws RemoteException {

        stop(force, "For testing");
    }

    /**
     * Stop the admin service. If reason is non-null, include it in the message
     * logged when the service shuts down.
     *
     * @param force force the shutdown
     * @param reason the reason for the shutdown, or null
     */
    /* Permit call to deprecated overloading */
    @SuppressWarnings("deprecation")
    public void stop(boolean force, String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            proxyRemote.stop(force, NULL_CTX, getSerialVersion());
            return;
        }

        proxyRemote.stop(force, reason, NULL_CTX, getSerialVersion());
    }

    /**
     * Set the policy parameters.
     */
    public void setPolicies(ParameterMap policyParams)
        throws RemoteException {

        proxyRemote.setPolicies(policyParams, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the current health status for each component.
     */
    public Map<ResourceId, ServiceChange> getStatusMap()
        throws RemoteException {

        return proxyRemote.getStatusMap(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the current performance status for each component.
     */
    public Map<ResourceId, PerfEvent> getPerfMap()
        throws RemoteException {

        return proxyRemote.getPerfMap(NULL_CTX, getSerialVersion());
    }

    /**
     * Return the status reporting events that have occurred since a point in
     * time.
     */
    public RetrievedEvents<ServiceChange> getStatusSince(long since)
        throws RemoteException {

        return proxyRemote.getStatusSince(since, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the performance reporting events that have occurred since a point
     * in time.
     */
    public RetrievedEvents<PerfEvent> getPerfSince(long since)
        throws RemoteException {

        return proxyRemote.getPerfSince(since, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the log records that have been logged since a point in time.
     */
    public RetrievedEvents<LogRecord> getLogSince(long since)
        throws RemoteException {

        return proxyRemote.getLogSince(since, NULL_CTX, getSerialVersion());
    }

    /**
     * Return the plan state change events that have occured since a point in
     * time.
     */
    public RetrievedEvents<PlanStateChange> getPlanSince(long since)
        throws RemoteException {

        return proxyRemote.getPlanSince(since, NULL_CTX, getSerialVersion());
    }

    public Map<String, Long> getLogFileNames()
        throws RemoteException {

        return proxyRemote.getLogFileNames(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns status information about the admin.
     *
     * @return admin status
     * @since 3.4
     */
    public AdminStatus getAdminStatus()
        throws RemoteException {

        return proxyRemote.getAdminStatus(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the address for the RMI service for the master admin, or null if
     * there is no current master.
     *
     * @return the RMI address or null
     * @throws RemoteException if a communication problem occurs
     */
    public URI getMasterRmiAddress()
        throws RemoteException {

        return proxyRemote.getMasterRmiAddress(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the address for the web service for the master admin, or null if
     * there is no current master.
     *
     * @return the web service address or null
     * @throws RemoteException if a communication problem occurs
     */
    public URI getMasterWebServiceAddress()
        throws RemoteException {

        return proxyRemote.getMasterWebServiceAddress(NULL_CTX,
                                                      getSerialVersion());
    }

    public List<CriticalEvent> getEvents(long startTime,
                                         long endTime,
                                         CriticalEvent.EventType type)
        throws RemoteException {

        return proxyRemote.getEvents(startTime, endTime, type, NULL_CTX,
                                     getSerialVersion());
    }

    public CriticalEvent getOneEvent(String eventId)
        throws RemoteException {

        return proxyRemote.getOneEvent(eventId, NULL_CTX, getSerialVersion());
    }

    public String [] startBackup()
        throws RemoteException {

        return proxyRemote.startBackup(NULL_CTX, getSerialVersion());
    }

    public long stopBackup()
        throws RemoteException {

        return proxyRemote.stopBackup(NULL_CTX, getSerialVersion());
    }

    public void updateMemberHAAddress(AdminId targetId,
                                      String targetHelperHosts,
                                      String newNodeHostPort)
        throws RemoteException {

        proxyRemote.updateMemberHAAddress(targetId,
                                          targetHelperHosts,
                                          newNodeHostPort,
                                          NULL_CTX, getSerialVersion());
    }

    /**
     * Run verification on the current topology.
     *
     * @param showProgress include information in the human-readable format in
     * addition to the initial startup message and the final verification
     * messages
     * @param listAll list information for all services checked
     * @param json if true, produce output in JSON format, otherwise in a
     * human-readable format.
     * @return the verification results
     */
    public VerifyResults verifyConfiguration(boolean showProgress,
                                             boolean listAll,
                                             boolean json)
        throws RemoteException {

        return proxyRemote.verifyConfiguration(showProgress,
                                               listAll,
                                               json,
                                               NULL_CTX, getSerialVersion());
    }

    /**
     * Run upgrade check on the current topology.
     */
    public VerifyResults verifyUpgrade(KVVersion targetVersion,
                                       List<StorageNodeId> snIds,
                                       boolean showProgress,
                                       boolean listAll,
                                       boolean json)
        throws RemoteException {

        return proxyRemote.verifyUpgrade(targetVersion, snIds,
                                         showProgress, listAll, json,
                                         NULL_CTX, getSerialVersion());
    }

    public VerifyResults verifyPrerequisite(KVVersion targetVersion,
                                            KVVersion prerequisiteVersion,
                                            List<StorageNodeId> snIds,
                                            boolean showProgress,
                                            boolean listAll,
                                            boolean json)
        throws RemoteException {

        return proxyRemote.verifyPrerequisite(targetVersion,
                                              prerequisiteVersion,
                                              snIds, showProgress,
                                              listAll, json,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of nodes to upgrade in an order which will maintain
     * store availability.
     */
    public String getUpgradeOrder(KVVersion targetVersion,
                                  KVVersion prerequisiteVersion)
        throws RemoteException {

        return proxyRemote.getUpgradeOrder(targetVersion,
                                           prerequisiteVersion,
                                           NULL_CTX, getSerialVersion());
    }

    /**
     * Get a list of node sets to upgrade the set of storage node in
     * order of the list.
     */
    public List<Set<StorageNodeId>>
        getUpgradeOrderList(KVVersion targetVersion,
                            KVVersion prerequisiteVersion)
        throws RemoteException {

        return proxyRemote.getUpgradeOrderList(targetVersion,
                                               prerequisiteVersion,
                                               NULL_CTX,
                                               getSerialVersion());
    }

    /**
     * Returns the in-memory values of the parameters for this admin.
     */
    public LoadParameters getParams()
        throws RemoteException {

        return proxyRemote.getParams(NULL_CTX, getSerialVersion());
    }

    public String getStorewideLogName()
        throws RemoteException {

        return proxyRemote.getStorewideLogName(NULL_CTX, getSerialVersion());
    }

    /**
     * List realized topologies with the "show topology history" command.
     */
    public List<String> getTopologyHistory(boolean concise)
        throws RemoteException {

        return proxyRemote.getTopologyHistory(
            concise, NULL_CTX, getSerialVersion());
    }

    /**
     * Return a status description of a running plan. See StatusReport for
     * how to set options bits.
     */
    public String getPlanStatus(int planId,
                                long options,
                                boolean json)
        throws RemoteException {

        return proxyRemote.getPlanStatus(planId, options, json, NULL_CTX,
                                         getSerialVersion());
    }

    /**
     * If a candidate name is specified, validate that topology, using the
     * version 1 JSON output format. If candidateName is null, validate the
     * current, deployed topology.
     * @return a display of the validation results
     */
    public String validateTopology(String candidateName)
        throws RemoteException {
        return validateTopology(candidateName,
                                SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
    }

    /**
     * If a candidate name is specified, validate that topology, using the JSON
     * output format specified for the specified serial version. If
     * candidateName is null, validate the current, deployed topology.
     * @param candidateName
     * @param jsonVersion The serial version that supports specific JSON
     * output
     * @throws RemoteException
     */
    public String validateTopology(String candidateName, short jsonVersion)
        throws RemoteException {
        checkMethodSupported(jsonVersion);
        return proxyRemote.validateTopology(candidateName, jsonVersion,
                                            NULL_CTX, getSerialVersion());
    }

    /**
     * Move a partition to the specified shard.
     */
    public String movePartition(String candidateName, PartitionId partId,
                                RepGroupId rgId)
            throws RemoteException {
        return proxyRemote.movePartition(candidateName, partId, rgId, NULL_CTX,
                                         getSerialVersion());
    }

    /**
     * Move a RN off its current SN. If the a target SN is specified, it will
     * only attempt move to that SN. This is an unadvertised option. If no
     * target is specified (if snId is null), the system will choose a new SN.
     */
    public String moveRN(String candidateName, RepNodeId rnId,
                         StorageNodeId snId)
        throws RemoteException {
        return proxyRemote.moveRN(candidateName, rnId, snId, NULL_CTX,
                                  getSerialVersion());
    }

    /**
     * Create a new Plan to alter a user's information.
     */
    public int createChangeUserPlan(String planName,
                                    String userName,
                                    Boolean isEnabled,
                                    char[] newPlainPassword,
                                    boolean retainPassword,
                                    boolean clearRetainedPassword)
        throws RemoteException {

        return proxyRemote.createChangeUserPlan(
            planName, userName, isEnabled, newPlainPassword, retainPassword,
            clearRetainedPassword, NULL_CTX, getSerialVersion());
    }

    public int createDropUserPlan(String planName, String userName)
        throws RemoteException {
        return proxyRemote.createDropUserPlan(planName, userName,
                                              NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to grant roles to user in KVStore.
     */
    public int createGrantPlan(String planName,
                               String grantee,
                               Set<String> roles)
        throws RemoteException {

        return proxyRemote.createGrantPlan(planName, grantee, roles,
                                           NULL_CTX, getSerialVersion());
    }

    /**
     * Create a new Plan to revoke roles from user in KVStore.
     */
    public int createRevokePlan(String planName,
                                String userName,
                                Set<String> roles)
        throws RemoteException {

        return proxyRemote.createRevokePlan(planName, userName, roles,
                                            NULL_CTX, getSerialVersion());
    }

    public boolean verifyUserPassword(String userName, char[] password)
        throws RemoteException {
        return proxyRemote.verifyUserPassword(userName, password,
                                              NULL_CTX, getSerialVersion());

    }

    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany)
        throws RemoteException {

        return proxyRemote.getPlanIdRange
            (startTime, endTime, howMany, NULL_CTX, getSerialVersion());
    }

    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany)
        throws RemoteException {

        return proxyRemote.getPlanRange(firstPlanId, howMany,
                                        NULL_CTX, getSerialVersion());
    }

    public SnapResultSummary executeSnapshotOperation(SnapshotOperation sop,
                                                      String sname,
                                                      DatacenterId dcId)
        throws RemoteException {
        return proxyRemote.executeSnapshotOperation(sop, sname, dcId,
                                                    NULL_CTX,
                                                    getSerialVersion());
    }

    public String[] listSnapshots(StorageNodeId snid)
        throws RemoteException {
        return proxyRemote.listSnapshots(snid, NULL_CTX, getSerialVersion());
    }

    /**
     * Repairs the admin quorum by updating the JE HA rep group membership to
     * match the currently available admins, setting an electable group size
     * override on each admin to obtain quorum, and converting requested
     * secondary admins to primary admins.  If there are no existing primary
     * admins, establishes one secondary as the initial master by resetting the
     * JE replication group, which updates the group membership and allows the
     * other secondary admins to join.
     *
     * <p>First checks that the admins specified by the zoneIds and adminIds
     * parameters match the currently available admin nodes.  The nodes
     * specified must be currently available, and must include all available
     * primary nodes.  If nodes are specified by zone, then the zone must be
     * present in the topology and all nodes in the specified zone must be
     * available.
     *
     * <p>Then attempts to modify admin parameters and adjust the JE rep group
     * membership.  The command fails if any of those operations fail.
     *
     * <p>Returns null if the repair should be retried after the current admin
     * is restarted.
     *
     * <p>Since this method changes the types of admins without changing the
     * associated zone types, and also changes the types of offline admins
     * which cannot be notified of the change, the caller should call 'plan
     * failover' and 'plan repair-topology' commands to complete the repairs.
     *
     * @param zoneIds include admins in zones with these IDs
     * @param adminIds include admins with these IDs
     * @return the current set of admins or null
     * @throws IllegalCommandException if a requested admin is not found, if a
     * primary admin is found that was not requested, or if a requested zone is
     * not found
     * @throws IllegalStateException if there is a problem repairing the admin
     * quorum
     * @throws RemoteException if a communication error occurs
     * @throws UnsupportedOperationException if operation is not supported by
     * all nodes
     * @since 3.4
     */
    public Set<AdminId> repairAdminQuorum(Set<DatacenterId> zoneIds,
                                          Set<AdminId> adminIds)
        throws RemoteException {

        return proxyRemote.repairAdminQuorum(
            zoneIds, adminIds, NULL_CTX, getSerialVersion());
    }

    /**
     * Creates a plan to inform the Store of the existence of an ES node, and
     * stores it by its plan id.
     * @param planName the name of the plan
     * @param clusterName - the cluster name of the ES cluster.
     * @param transportHp - transport host:port of any node in the ES cluster.
     * @param secure - ES Cluster is set up as secured or not.
     * @param forceClear - if true, allows deletion of an existing ES index.
     * @return the plan id of the created plan
     * @since 4.0
     */
    public int createRegisterESClusterPlan(final String planName,
                                           final String clusterName,
                                           final String transportHp,
                                           final boolean secure,
                                           final boolean forceClear)
        throws RemoteException {

        return proxyRemote.createRegisterESClusterPlan(planName,
                                                       clusterName,
                                                       transportHp,
                                                       secure,
                                                       forceClear,
                                                       NULL_CTX,
                                                       getSerialVersion());
    }

    /**
     * Creates a plan to cause the Store to forget about a registered ES
     * cluster.  Only one cluster may be registered, so no identifying
     * information is needed.
     * @param planName the name of the plan
     * @return the plan id of the created plan
     * @since 4.0
     */
    public int createDeregisterESClusterPlan(final String planName)
        throws RemoteException {

        return proxyRemote.createDeregisterESClusterPlan(planName,
                                                         NULL_CTX,
                                                         getSerialVersion());
    }

    /**
     * Verifies data on this admin.
     *
     * @param options properties of the verification
     * @throws RemoteException
     * @since 18.1
     */
    public VerificationInfo startAndPollVerify(VerificationOptions options,
                                               int planId)
        throws RemoteException {
        return proxyRemote.startAndPollVerify(options, planId, NULL_CTX,
                               getSerialVersion());

    }

    /**
     * Interrupt the data verification
     * @throws RemoteException
     */
    public boolean interruptVerifyData() throws RemoteException {
        return proxyRemote.interruptVerifyData(NULL_CTX,
                                               getSerialVersion());
    }

    /**
     * Create a plan to verify data on a service node.
     *
     * @param planName the name of the plan
     * @param rid id of the node
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyServicePlan(String planName,
                                       ResourceId rid,
                                       VerificationOptions verifyOptions)
        throws RemoteException {
        return proxyRemote.createVerifyServicePlan(planName, rid, verifyOptions,
                                                   NULL_CTX,
                                                   getSerialVersion());
    }

    /**
     * Create a plan to verify data on all admins that are deployed to the
     * specified zone or all zones.
     *
     * @param planName the name of the plan
     * @param dcid datacenter id
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyAllAdminsPlan(String planName,
                                         DatacenterId dcid,
                                         VerificationOptions verifyOptions)
        throws RemoteException {
        return proxyRemote.createVerifyAllAdminsPlan(planName, dcid,
                                                     verifyOptions,
                                                     NULL_CTX,
                                                     getSerialVersion());
    }

    /**
     * Create a plan to verify data on all rns that are deployed to the
     * specified zone or all zones.
     *
     * @param planName the name of the plan
     * @param dcid datacenter id
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyAllRepNodesPlan(String planName,
                                           DatacenterId dcid,
                                           VerificationOptions verifyOptions)
        throws RemoteException {
        return proxyRemote.createVerifyAllRepNodesPlan(planName, dcid,
                                                       verifyOptions, NULL_CTX,
                                                       getSerialVersion());

    }

    /**
     * Create a plan to verify data on all rns and admins and that are deployed
     * to the specified zone or all zones.
     *
     * @param planName the name of the plan
     * @param dcid datacenter id
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyAllServicesPlan(String planName,
                                           DatacenterId dcid,
                                           VerificationOptions verifyOptions)
        throws RemoteException {
        return proxyRemote.createVerifyAllServicesPlan(planName, dcid,
                                                       verifyOptions, NULL_CTX,
                                                       getSerialVersion());

    }

    /**
     * Creates a plan to set limits on a table.
     */
    public int createTableLimitPlan(String planName,
                                    String namespace,
                                    String tableName,
                                    TableLimits newLimits)
        throws RemoteException {

        namespace = NameUtils.switchToInternalUse(namespace);
        return proxyRemote.createTableLimitPlan(planName,
                                                namespace, tableName,
                                                newLimits,
                                                NULL_CTX, getSerialVersion());
    }

    /**
     * Creates a plan to retrieve and install updates to the set of shared TLS
     * credentials used by SNAs in the store.
     *
     * @param retrieve whether to retrieve updated credentials
     * @param install whether to install pending updates
     * @return the plan ID
     * @throws IllegalArgumentException if both retrieve and install are false
     * @throws RemoteException if a network error occurs
     * @since 24.4
     */
    public int createUpdateTlsCredentialsPlan(String planName,
                                              boolean retrieve,
                                              boolean install)
        throws RemoteException
    {
        checkMethodSupported(TLS_CREDENTIALS_VERSION);
        return proxyRemote.createUpdateTlsCredentialsPlan(
            planName, retrieve, install, NULL_CTX, getSerialVersion());
    }

    /**
     * Returns information about TLS credentials for all SNAs as a
     * JSON-formatted string. Throws a CommandFaultException if the command
     * fails as a whole, typically when the admin cannot be reached.
     * Information about failures when contacting individual SNAs are included
     * in the return string.
     *
     * <p>First, provides two summary entries that represent the overall status
     * of TLS credentials.
     *
     * <p>The "installedCredentialsStatus" entry has value "consistent" if all
     * SNs report having the same pair of keystore and truststore files
     * installed. The value is "mixed" if any SNAs are found to have different
     * files installed. The value is "maybe-consistent" if all SNAs that
     * responded reported having the same pair of files installed, but not all
     * SNAs could be contacted.
     *
     * <p>The "pendingUpdatesStatus" entry has the value "none" if no SNAs have
     * any pending updates, or if the update files match currently installed
     * files. The value is "consistent" if there are pending updates and all
     * installed files will be consistent once the pending updates are
     * installed. The value is "mixed" if there are pending updates and some
     * installed files will different after the pending updates are installed.
     * The value is "maybe-consistent" if there are pending updates, all SNAs
     * that responded will have the same files after the updates are installed,
     * but not all SNAs could be contacted.
     *
     * <p>Information about SNAs will be stored in the "sns" entry with a value
     * that maps SN names to information about the associated SNA.
     *
     * <p>If there was a problem contacting an SNA, the value for that SNA will
     * have an "exception" entry whose value is a string describing the
     * problem. Otherwise, the SNA will have "installedCredentials" and
     * "pendingUpdates" entries, each with "keystore" and "truststore" entries.
     * For each credential file, the value will have a "file" entry that
     * provides the name of the file relative to the security subdirectory
     * under the SN's root directory, a "hash" entry that represents the
     * SHA-256 hash of the file contents represented as a 64-character
     * hexadecimal value or null if the file was not found, and, if the file
     * was found, a "modTime" entry that represents the modification time of
     * the file in the UTC timezone.
     *
     * <p>Sample output:
     *
     * {@literal
     * {
     *   "installedCredentialsStatus": "consistent",
     *   "pendingUpdatesStatus": "consistent",
     *   "sns": {
     *     "sn1": {
     *       "installedCredentials": {
     *         "keystore": {
     *           "file": "store.keys",
     *           "hash":
     *      "b157d9dc61bed1e2642425adb6791dd13e2c0868160b693963a85596130c388d",
     *           "modTime": "2024-06-07 09:21:53 UTC"
     *         },
     *         "truststore": {
     *           "file": "store.trust",
     *           "hash":
     *      "c5a4c67820b64d324a1b8ed493041e04ef91697f64d4a605d750b9dbe8708f6b",
     *           "modTime": "2024-06-07 09:21:57 UTC"
     *         }
     *       },
     *       "pendingUpdates": {
     *         "truststore": {
     *           "file": "updates/store.trust",
     *           "hash":
     *      "4fe0231942e732d829cf97cb3742c9aa86ae06db8a80939b6fb834382c205a53",
     *           "modTime": "2024-06-08 10:00:00 UTC"
     *         }
     *       }
     *     },
     *     "sn2": { ... },
     *     "sn3": { ... }
     *   }
     * }
     * }
     *
     * @return information about TLS credentials in JSON format
     * @throws CommandFaultException if the command fails
     * @throws RemoteException if network error occurs
     * @since 24.4
     */
    public String getTlsCredentialsInfo() throws RemoteException {
        checkMethodSupported(TLS_CREDENTIALS_VERSION);
        return proxyRemote.getTlsCredentialsInfo(NULL_CTX, getSerialVersion());
    }
}
