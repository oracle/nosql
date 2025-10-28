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

import java.io.IOException;
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
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.AdminStatusReceiver;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.TrackerListener;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
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
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.table.FieldDef;
import oracle.kv.table.TimeToLive;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This is the interface used by the command line client.
 */
public interface CommandService extends VersionedRemote {

    /**
     * Returns the CommandService's status, which can only be RUNNNING.
     *
     * @since 3.0
     */
    ServiceStatus ping(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /* -- Topology related APIs -- */

    /**
     * Return a list of the names of all storage node pools.
     *
     * @since 3.0
     */
    List<String> getStorageNodePoolNames(AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Add a new StorageNodePool.
     *
     * @since 3.0
     */
    void addStorageNodePool(String name,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * Clone an existing StorageNodePool as a new StorageNodePool.
     *
     * @since 4.0
     */
    void cloneStorageNodePool(String name,
                              String source,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * Remove a storage node pool.
     *
     * @since 3.0
     */
    void removeStorageNodePool(String name,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Get a list of the storage node ids in a pool.
     *
     * @since 3.0
     */
    List<StorageNodeId> getStorageNodePoolIds(String name,
                                              AuthContext authCtx,
                                              short serialVersion)
        throws RemoteException;

    /**
     * Add a storage node to the pool with the given name.
     *
     * @since 3.0
     */
    void addStorageNodeToPool(String name,
                              StorageNodeId snId,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * Remove a storage node from the pool with the given name.
     *
     * @since 4.0
     */
    void removeStorageNodeFromPool(String name,
                                   StorageNodeId snId,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Replace the contents of a storage node pool.
     *
     * @since 3.0
     */
    void replaceStorageNodePool(String name,
                                List<StorageNodeId> ids,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Creates a named topology. The initial configuration will be based on the
     * storage nodes specified by the <code>snPoolName</code> parameter. The
     * number of partitions for a topology is fixed once the topology is
     * created and cannot be changed. The command will throw
     * <code>IllegalCommandException</code> if there is not enough capacity
     * in the specified <code>snPoolName</code> to satisfy the number of the
     * replication nodes needed to create the topology.
     * @param candidateName name of the topology candidate
     * @param snPoolName pool contain the storage nodes used to create
     * topology
     * @param numPartitions number of partitions for the topology
     * @param json whether to return JSON style string
     * @param jsonVersion version of JSON style string
     * @param authCtx
     * @param serialVersion
     * @return string representing create topology result
     * @throws RemoteException
     * @since 18.1
     */
    String createTopology(String candidateName,
                          String snPoolName,
                          int numPartitions,
                          boolean json,
                          short jsonVersion,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * Creates a copy of the current (deployed) topology and associates it with
     * the <code>candidateName</code> parameter. Changes to the copy will have
     * no effect on the current topology.
     *
     * @throws IllegalCommandException is @param candidateName is already
     * associated with a topology
     *
     * @since 3.0
     */
    String copyCurrentTopology(String candidateName,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Returns the list of named topologies. If no named topologies exist an
     * empty list is returned.
     *
     * @since 3.0
     */
    List<String> listTopologies(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Delete a named topology. Removing a topology that was used to create a
     * plan will not affect the plan. If @param candidateName is not associated
     * with a topology, this method does nothing.
     *
     * @since 3.0
     */
    String deleteTopology(String candidateName,
                          AuthContext authCtx,
                          short serialVersion)
        throws RemoteException;

    /**
     * Re-balances the replication nodes of a data center or store. The set of
     * replication nodes in the topology having the name referenced by the
     * <code>candidateName</code> parameter will be re-balanced across the
     * storage nodes specified by the <code>snPoolName</code> parameter. Both
     * the replication factor and the number of shards is not changed. If the
     * value input for the <code>dcId</code> parameter is not
     * <code>null</code>, the re-balance will only be done to nodes in that
     * data center, otherwise the re-balance will be for all nodes in the
     * store. The method will fail if there is not enough capacity in the
     * specified pool to satisfy the number of the replication nodes needed to
     * implement the change.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 3.0
     */
    String rebalanceTopology(String candidateName,
                             String snPoolName,
                             DatacenterId dcId,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Changes the replication factor of a data center. Does not modify the
     * allowArbiters or masterAffinity fields because the only RF changes that
     * would require that (reduce primary RF to 0) are not permitted. Adds or
     * removes replication nodes in the topology having the name referenced by
     * the <code>candidateName</code> parameter. The data center's replication
     * factor is changed and new replication nodes are added and rebalanced
     * across the storage nodes specified by the <code>snPoolName</code>
     * parameter. The number of shards is not changed. The method will fail if
     * there is not enough capacity in the pool referenced by
     * <code>snPoolName</code> to satisfy the number of the replication nodes
     * needed to implement the change, or the rep-factor is smaller than the
     * current data center's replication factor. If the current replication
     * factor and rep-factor are equal this method will do nothing.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology, or if
     * attempting to reduce the replication factor of a primary zone
     *
     * @since 3.0
     */
    String changeRepFactor(String candidateName, String snPoolName,
                           DatacenterId dcId, int repFactor,
                           AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Changes the type of the specified data center, adjusting allowArbiters
     * and masterAffinity as needed.
     *
     * @since 3.4
     */
    String changeZoneType(String candidateName, DatacenterId dcId,
                          DatacenterType type,
                          AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Changes master affinity.
     *
     * @throws IllegalCommandException if masterAffinity is true and the zone
     * is either not a primary zone or is a primary zone with RF 0
     *
     * @since 18.1
     */
    String changeZoneMasterAffinity(String candidateName, DatacenterId dcId,
                                    boolean masterAffinity,
                                    AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Changes the allow arbiter attribute of the specified data center.
     *
     * @throws IllegalCommandException if allowArbiters is true and the zone is
     * not a primary zone, or if allowArbiters is false and the zone is a
     * primary zone with RF 0
     *
     * @since 4.0
     */
    String changeZoneArbiters(String candidateName, DatacenterId dcId,
                              boolean allowArbiters,
                     AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Redistributes partitions in the topology having the name referenced by
     * the <code>candidateName</code> parameter. The number of shards will be
     * recalculated and new replication nodes will be added as needed. The new
     * replication nodes will be assigned to the storage nodes specified by the
     * <code>snPoolName</code> parameter. Partitions from existing shards will
     * be re-assigned to the new shards. The method will fail if there is not
     * enough capacity in the pool referenced by <code>snPoolName</code> to
     * satisfy the number of the replication nodes needed to implement the
     * change.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 3.0
     */
    String redistributeTopology(String candidateName,
                                String snPoolName,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Contracts storage nodes in the topology having the name referenced by
     * the <code>candidateName</code> parameter. The number of shards will be
     * recalculated and replication nodes will be removed as needed. The to be
     * removed replication nodes will be from the storage nodes not specified by
     * the <code>snPoolName</code> parameter.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 4.0
     */
    String contractTopology(String candidateName,
                            String snPoolName,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * Removes a shard from the topology specified by the
     * <code>candidateName</code> parameter by removing the failed shard
     * referenced by the <code>failedShard</code> parameter. The final number
     * of shards will be one less and replication nodes will be removed from
     * failed shard.
     *
     * @throws IllegalCommandException if the name referenced by
     * <code>candidateName</code> is not associated with a topology.
     *
     * @since 18.1
     */
    String removeFailedShard(RepGroupId failedShard,
                             String candidateName,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Displays the steps necessary to migrate one topology to another.
     *
     * @param targetTopoName name of the topology to be changed to
     * @param startTopoName is not {@code null} then that topology is used as
     * the starting point otherwise the deployed store's topology is used.
     * @param verbose whether to display verbose output
     * @param jsonVersion specify which version of JSON string to display the
     * return result
     * @param authCtx
     * @param serialVersion
     * @throws RemoteException
     * @since 18.1
     */
    String preview(String targetTopoName, String startTopoName,
                   boolean verbose, short jsonVersion,
                   AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /* -- Plan APIs -- */

    /**
     * Get a list of the Admins and their parameters.
     *
     * @since 3.0
     */
    List<ParameterMap> getAdmins(AuthContext authCtx, short serialVersion)
            throws RemoteException;

    /**
     * Get the specified plan.
     *
     * @since 3.0
     */
    Plan getPlanById(int planId,
                     AuthContext authCtx,
                     short serialVersion) throws RemoteException;

    /**
     * Approve the identified plan.
     *
     * @since 3.0
     */
    void approvePlan(int planId, AuthContext authCtx,
                     short serialVersion) throws RemoteException;

    /**
     * Starts the execution of the specified plan. The plan will proceed
     * asynchronously. The force parameter is passed to the plan when executed.
     * Each plan will have a its own interpretation of the force value,
     * including ignoring it.
     *
     * @param force value passed to the plan at execution time
     *
     * @since 3.0
     */
    void executePlan(int planId, boolean force,
                     AuthContext authCtx,
                     short serialVersion)
        throws RemoteException;

   /**
     * Wait for the plan to finish. If a timeout period is specified, return
     * either when the plan finishes or the timeout occurs.
     * @return the current plan status when the call returns. If the call timed
     * out, the plan may still be running.
     *
     * @since 3.0
     */
    Plan.State awaitPlan(int planId,
                         int timeout,
                         TimeUnit timeUnit,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Cancel a plan.
     *
     * @since 3.0
     */
    void cancelPlan(int planId,
                    AuthContext authCtx,
                    short serialVersion) throws RemoteException;

    /**
     * Interrupt a plan.
     *
     * @since 3.0
     */
    void interruptPlan(int planId,
                       AuthContext authCtx,
                       short serialVersion) throws RemoteException;

    /**
     * Create a new Plan to deploy a new Datacenter with the specified type and
     * whether to allow arbiters.
     * This command is used by R18.1 or later clients.
     *
     * @since 18.1
     */
    int createDeployDatacenterPlan(String planName,
                                   String datacenterName,
                                   int repFactor,
                                   DatacenterType datacenterType,
                                   boolean allowArbiters,
                                   boolean masterAffinity,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a new StorageNode.
     *
     * @since 3.0
     */
    int createDeploySNPlan(String planName,
                           DatacenterId datacenterId,
                           String hostName,
                           int registryPort,
                           String comment,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a new Admin service instance of the specified
     * type. If type is null the Admin service will get its type from the
     * containing zone.
     *
     * @since 3.3
     */
    int createDeployAdminPlan(String planName,
                              StorageNodeId snid,
                              int httpPort,
                              AdminType type,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

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
     *
     * @since 18.1
     */
    int createRemoveAdminPlan(String planName,
                              DatacenterId dcid,
                              AdminId aid,
                              boolean failedSN,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to deploy a topology.
     *
     */
    int createDeployTopologyPlan(String planName,
                                 String candidateName,
                                 RepGroupId failedShard,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Create a new plan that performs a failover.
     *
     * @since 3.4
     */
    int createFailoverPlan(String planName,
                           Set<DatacenterId> newPrimaryZones,
                           Set<DatacenterId> offlineZones,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to shut down the repnodes in a kvstore.
     *
     * @since 3.0
     */
    int createStopAllRepNodesPlan(String planName,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to start up the repnodes in a kvstore.
     *
     * @since 3.0
     */
    int createStartAllRepNodesPlan(String planName,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Stop a set of services.
     *
     * @since 3.4
     */
    int createStopServicesPlan(String planName,
                               Set<? extends ResourceId> serviceIds,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Start a set of services.
     *
     * @since 3.4
     */
    int createStartServicesPlan(String planName,
                                Set<? extends ResourceId> serviceIds,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter a service's parameters.
     *
     * @since 3.0
     */
    int createChangeParamsPlan(String planName,
                               ResourceId rid,
                               ParameterMap newParams,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for all RepNodes deployed to the
     * specified datacenter. If <code>null</code> is input for the
     * <code>dcid</code> parameter, then the specified parameters will be
     * changed for all RepNodes from each of the datacenters making up the
     * store.
     *
     * @since 3.0
     */
    int createChangeAllParamsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
    * Create a new Plan to alter parameters for all ArbNodes deployed to the
    * specified datacenter. If <code>null</code> is input for the
    * <code>dcid</code> parameter, then the specified parameters will be
    * changed for all ArbNodes from each of the datacenters making up the
    * store.
    *
    * @since 4.0
    */
    int createChangeAllANParamsPlan(String planName,
                                    DatacenterId dcid,
                                    ParameterMap newParams,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for all admin services deployed to
     * the specified datacenter. If <code>null</code> is input for the
     * <code>dcid</code> parameter, then the specified parameters will be
     * changed for all admin services from each of the datacenters making up
     * the store.
     *
     * @since 3.0
     */
    int createChangeAllAdminsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for global scoped, non-security
     * parameters. The specified parameters will be changed for all
     * SN-managed components in the store.
     *
     * @since 4.3
     */
    int createChangeGlobalComponentsParamsPlan(String planName,
                                               ParameterMap newParams,
                                               AuthContext authCtx,
                                               short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to alter parameters for global security parameters.
     * The specified parameters will be changed for all admin and repnode
     * services from storage nodes in the store.
     *
     * @since 3.0
     */
    int createChangeGlobalSecurityParamsPlan(String planName,
                                             ParameterMap newParams,
                                             AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to change a user's information.
     *
     * @since 3.0
     */
    int createChangeUserPlan(String planName,
                             String userName,
                             Boolean isEnabled,
                             char[] newPlainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to add a user of kvstore.
     *
     * @since 3.0
     */
    int createCreateUserPlan(String planName,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword,
                             AuthContext authCtx,
                             short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove a user of kvstore.
     *
     * @since 3.0
     */
    int createDropUserPlan(String planName,
                           String userName,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to grant roles to user in KVStore.
     *
     * @since 3.1
     */
    int createGrantPlan(String planName,
                        String grantee,
                        Set<String> roles,
                        AuthContext authCtx,
                        short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to revoke roles from user  in KVStore.
     *
     * @since 3.1
     */
    int createRevokePlan(String planName,
                         String target,
                         Set<String> roles,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to move all services from the old storage node to a
     * new storage node.
     *
     * @since 3.0
     */
    int createMigrateSNPlan(String planName,
                            StorageNodeId oldNode,
                            StorageNodeId newNode,
                            int newHttpPort,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove a storage node from the store.
     *
     * @since 3.0
     */
    int createRemoveSNPlan(String planName,
                           StorageNodeId targetNode,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to remove a datacenter from the store.
     *
     * @since 3.0
     */
    int createRemoveDatacenterPlan(String planName,
                                   DatacenterId targetId,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Create a plan that will address topology inconsistencies.
     */
    int createRepairPlan(String planName,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;


    /**
     * @since 4.4
     * @deprecated as of 20.3
     */
    @Deprecated
    public int createAddTablePlan(String planName,
                                  String namespace,
                                  String tableName,
                                  String parentName,
                                  FieldMap fieldMap,
                                  List<String> primaryKey,
                                  List<Integer> primaryKeySizes,
                                  List<String> majorKey,
                                  TimeToLive ttl,
                                  boolean r2compat,
                                  int schemaId,
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * @since 18.1
     * @deprecated as of 23.3
     */
    @SuppressWarnings("dep-ann")
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
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * @since 22.4
     * @deprecated as of 23.3
     */
    @SuppressWarnings("dep-ann")
    public int createAddTablePlan(final String planName,
                                  final String namespace,
                                  final String tableName,
                                  final String parentName,
                                  final FieldMap fieldMap,
                                  final List<String> primaryKey,
                                  final List<Integer> primaryKeySizes,
                                  final List<String> shardKey,
                                  final TimeToLive ttl,
                                  final TableLimits limits,
                                  final boolean r2compat,
                                  final int schemaId,
                                  final boolean jsonCollection,
                                  final String description,
                                  AuthContext authCtx,
                                  final short serialVersion)
        throws RemoteException;

    /**
     * Create a new Plan to create a new Table in the store.
     *
     * @param planName the name of the plan
     * @param namespace namespace for the new table. This is optional.
     * @param tableName the id of the new table.  This is used in its generated
     * Key objects so it should be short to save space in the store.
     * @param parentName set to a qualified ("." separated) path to a parent
     * table if the new table is a child table, null otherwise.
     * @param fieldMap an object that represents the map of {@link FieldDef}
     * objects that comprises the table, along with the field declaration
     * order.
     * @param primaryKey the list of fields that comprise the primary key for
     * this table.  It must contain at least one field.  For child tables it is
     * a superset of its parent table's primary key.  Primary key fields turn
     * into Keys in requests.
     * @param shardKey the list of primary key fields that comprise the shard
     * key portion of the primary key. This must be strict,
     * ordered subset of the primaryKey if set. It is only used for top-level
     * tables.  For child tables the major/minor split is either that of
     * the parent table or the boundary between the parent and child table
     * primary keys.  The Key components generated by a child table are
     * implicitly minor-only.
     * @param ttl default Time-to-Live duration for the new table.
     * Time-To_live for child tables are independent of their parent table.
     * @param limits table resource limits, or null. Limits can only be set on
     * top-level tables.
     * @param jsonCollection true if the table is a jsonCollection.
     * Added in 22.4
     * @param mrCounters non-null map of counters if the table is a
     * jsonCollection and has mrCounters
     * Added in 23.3
     * @param description an option description of the table, used for
     * human-readable purposes.  This string does not affect table records.
     * @since 22.4
     */
    public int createAddTablePlan(String planName,
                                  String namespace,
                                  String tableName,
                                  String parentName,
                                  FieldMap fieldMap,
                                  List<String> primaryKey,
                                  List<Integer> primaryKeySizes,
                                  List<String> shardKey,
                                  TimeToLive ttl,
                                  TableLimits limits,
                                  boolean r2compat,
                                  int schemaId,
                                  boolean jsonCollection,
                                  Map<String, FieldDef.Type> mrCounters,
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * As of 18.1 removeData == false is not supported. A false value will
     * throw an IllegalArgumentException.
     *
     * @since 4.4
     */
    public int createRemoveTablePlan(String planName,
                                     String namespace,
                                     String tableName,
                                     boolean removeData,
                                     AuthContext authCtx,
                                     short serialVersion)
         throws RemoteException;

    /**
     * @since 4.4
     * @deprecated as of 19.2, in favor of version that takes an indexNulls
     * boolean
     */
    @Deprecated
    public int createAddIndexPlan(String planName,
                                  String namespace,
                                  String indexName,
                                  String tableName,
                                  String[] indexedFields,
                                  FieldDef.Type[] indexedTypes,
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * @since 19.2
     * Adds indexNulls param.
     */
    public int createAddIndexPlan(String planName,
                                  String namespace,
                                  String indexName,
                                  String tableName,
                                  String[] indexedFields,
                                  FieldDef.Type[] indexedTypes,
                                  boolean indexNulls,
                                  boolean isUnique,
                                  String description,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * @since 4.4
     */
    public int createRemoveIndexPlan(String planName,
                                     String namespace,
                                     String indexName,
                                     String tableName,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * @since 20.1
     * @deprecated since 25.3
     */
    @Deprecated
    public int createEvolveTablePlan(String planName,
                                     String namespace,
                                     String tableName,
                                     int tableVersion,
                                     FieldMap fieldMap,
                                     TimeToLive ttl,
                                     Set<Integer> regions,
                                     AuthContext authCtx,
                                     short serialVersion)
         throws RemoteException;

    /**
     * @since 25.3
     * @param beforeImgTTL specify before image ttl, added in 25.3
     */
    public int createEvolveTablePlan(String planName,
                                     String namespace,
                                     String tableName,
                                     int tableVersion,
                                     FieldMap fieldMap,
                                     TimeToLive ttl,
                                     TimeToLive beforeImgTTL,
                                     Set<Integer> regions,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Creates a new plan to perform network restore from source node to target
     * node.
     *
     * @since 4.4
     */
    public int createNetworkRestorePlan(String planName,
                                        ResourceId sourceNode,
                                        ResourceId targetNode,
                                        boolean retainOrigLog,
                                        AuthContext authCtx,
                                        short serialVersion)
        throws RemoteException;

    /**
     * Creates a new plan to enable request type on shards or entire store.
     *
     * @since 18.1
     */
    public int createEnableRequestsPlan(String planName,
                                        String requestType,
                                        Set<? extends ResourceId> resIds,
                                        boolean entireStore,
                                        AuthContext authCtx,
                                        short serialVersion)
        throws RemoteException;

    /**
     * Configure the Admin with a store name.  This command can be used only
     * when the AdminService is running in bootstrap/configuration mode.
     *
     * @since 3.0
     */
    void configure(String storeName, AuthContext authCtx, short serialVersion)
        throws RemoteException;

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
     *
     * @since 23.3
     */
    boolean isStoreReady(boolean forTables,
                         AuthContext authCtx, short serialVersion)
            throws RemoteException;

    /**
     * If configured, return the store name, otherwise, null.
     *
     * @since 3.0
     */
    String getStoreName(AuthContext authCtx,
                        short serialVersion) throws RemoteException;

    /**
     * Return the pathname of the KV root directory (KVHOME).
     *
     * @since 3.0
     */
    String getRootDir(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Return the whole Topology for listing or browsing.
     *
     * @since 3.0
     */
    Topology getTopology(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Return the specified Metadata for listing or browsing.
     */
    @Nullable
    <T extends Metadata<? extends MetadataInfo>> T
                                  getMetadata(final Class<T> returnType,
                                              final MetadataType metadataType,
                                              AuthContext authCtx,
                                              short serialVersion)
         throws RemoteException;

    /**
     * Saves the specified Metadata
     */
    void putMetadata(final Metadata<?> metadata,
                     AuthContext authCtx,
                     short serialVersion)
         throws RemoteException;

    /**
     * Retrieve the topology that corresponds to this candidate name.  Invoked
     * with the "topology view candidateName" command.
     *
     * @since 3.0
     */
    TopologyCandidate getTopologyCandidate(String candidateName,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Return the whole Parameters for listing or browsing.
     *
     * @since 3.0
     */
    Parameters getParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Return the RepNodeParameters for the specified node.
     *
     * @since 3.0
     */
    ParameterMap getRepNodeParameters(RepNodeId id,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Return the Policy parameters from the admin.
     *
     * @since 3.0
     */
    ParameterMap getPolicyParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new global parameters are available in the storage node
     * configuration file and that these should be reread.
     *
     * @since 3.0
     */
    public void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new security metadata change need to be applied.
     *
     * @since 3.2
     */
    void newSecurityMDChange(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Stop the admin service without specifying a reason.
     *
     * @since 3.0
     * @deprecated since 22.3
     */
    @Deprecated
    void stop(boolean force, AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Stop the admin service.
     *
     * @since 22.3
     */
    void stop(boolean force,
              String reason,
              AuthContext authCtx,
              short serialVersion)
        throws RemoteException;

    /**
     * Set the policy parameters.
     *
     * @since 3.0
     */
    void setPolicies(ParameterMap policyParams,
                     AuthContext authCtx,
                     short serialVersion)
        throws RemoteException;

    /**
     * Return the current health status for each component.
     *
     * @since 3.0
     */
    Map<ResourceId, ServiceChange> getStatusMap(AuthContext authCtx,
                                                short serialVersion)
        throws RemoteException;

    /**
     * Return the current performance status for each component.
     *
     * @since 3.0
     */
    Map<ResourceId, PerfEvent> getPerfMap(AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException;

    /**
     * Return the status reporting events that have occurred since a point in
     * time.
     *
     * @since 3.0
     */
    RetrievedEvents<ServiceChange> getStatusSince(long since,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException;

    /**
     * Return the performance reporting events that have occurred since a point
     * in time.
     *
     * @since 3.0
     */
    RetrievedEvents<PerfEvent> getPerfSince(long since,
                                            AuthContext authCtx,
                                            short serialVersion)
        throws RemoteException;

    /**
     * Return the log records that have been logged since a point in time.
     *
     * @since 3.0
     */
    RetrievedEvents<LogRecord> getLogSince(long since,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Return the plan state change events that have occured since a point in
     * time.
     *
     * @since 3.0
     */
    RetrievedEvents<PlanStateChange> getPlanSince(long since,
                                                  AuthContext authCtx,
                                                  short serialVersion)
        throws RemoteException;

    /**
     * Register a log tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void registerLogTrackerListener(TrackerListener tl,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a log tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void removeLogTrackerListener(TrackerListener tl,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Register a status tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void registerStatusTrackerListener(TrackerListener tl,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a status tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void removeStatusTrackerListener(TrackerListener tl,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Register a perf tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void registerPerfTrackerListener(TrackerListener tl,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a perf tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void removePerfTrackerListener(TrackerListener tl,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Register a plan tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void registerPlanTrackerListener(TrackerListener tl,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Remove the registration of a plan tracker listener.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void removePlanTrackerListener(TrackerListener tl,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Get a map of log file names.
     *
     * @since 3.0
     */
    Map<String, Long> getLogFileNames(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get the Admin service status.
     *
     * @since 3.4
     */
    AdminStatus getAdminStatus(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get the master Admin's RMI address.
     *
     * @since 3.0
     */
    URI getMasterRmiAddress(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get the master Admin's web service address.
     */
    URI getMasterWebServiceAddress(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get a list of critical events.
     *
     * @since 3.0
     */
    List<CriticalEvent> getEvents(long startTime, long endTime,
                                  CriticalEvent.EventType type,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Get a single critical event.
     *
     * @since 3.0
     */
    CriticalEvent getOneEvent(String eventId,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * Start a backup.
     *
     * @since 3.0
     */
    String[] startBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Stop a backup.
     *
     * @since 3.0
     */
    long stopBackup(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Update the HA address for an admin member.
     *
     * @since 3.0
     */
    void updateMemberHAAddress(AdminId targetId,
                               String targetHelperHosts,
                               String newNodeHostPort,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Verify store configuration.
     *
     * @since 3.3
     */
    VerifyResults verifyConfiguration(boolean showProgress,
                                      boolean listAll,
                                      boolean json,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Verify upgrade state.
     *
     * @since 3.5
     */
    VerifyResults verifyUpgrade(KVVersion targetVersion,
                                List<StorageNodeId> snIds,
                                boolean showProgress,
                                boolean listAll,
                                boolean json,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Verify upgrade prerequisites.
     *
     * @since 3.5
     */
    VerifyResults verifyPrerequisite(KVVersion targetVersion,
                                     KVVersion prerequisiteVersion,
                                     List<StorageNodeId> snIds,
                                     boolean showProgress,
                                     boolean listAll,
                                     boolean json,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Get the Admins configuration parameters.
     *
     * @since 3.0
     */
    LoadParameters getParams(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get the name of the store-wide log file.
     *
     * @since 3.0
     */
    String getStorewideLogName(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * List realized topologies with the "show topology history" command.
     *
     * @since 3.0
     */
    List<String> getTopologyHistory(boolean concise,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * An unadvertised entry point which lets the caller check that a plan
     * succeeded, and provokes an exception containing information about the
     * failure if the plan failed. Used for testing and for situations where we
     * need to programmatically obtain an exception if the plan failed.
     *
     * @throws OperationFaultException if the plan did not end successfully.
     *
     * @since 3.0
     */
    void assertSuccess(int planId, AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Get the status of an Admin plan.
     *
     * @since 3.5
     */
    String getPlanStatus(int planId,
                         long options,
                         boolean json,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Copy a topolgy within the Admin.
     *
     * @since 3.0
     */
    String copyTopology(String sourceCandidateName,
                        String targetCandidateName,
                        AuthContext authCtx,
                        short serialVersion)
        throws RemoteException;

    /**
     * Check validity of a topology.
     * @param candidateName name of the topology candidate to be validated
     * @param jsonVersion specify which version of JSON format to display the
     * validation result
     * @param authCtx
     * @param serialVersion
     * @return string result represent the validation. Result will be a JSON
     * string if JSON version is specified.
     * @throws RemoteException
     * @since 18.1
     */
    String validateTopology(final String candidateName,
                            final short jsonVersion,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * Move a single partition.
     *
     * @since 21.2
     */
    String movePartition(String candidateName,
                         PartitionId partId,
                         RepGroupId rgId,
                         AuthContext NULL_CTX,
                         short serialVersion)
        throws RemoteException;

    /**
     * Move an RN.
     *
     * @since 3.0
     */
    String moveRN(String candidateName,
                  RepNodeId rnId,
                  StorageNodeId snId,
                  AuthContext authCtx,
                  short serialVersion)
        throws RemoteException;

    /**
     * Install a receiver for Admin status updates, for delivering service
     * change information to the standardized monitoring/management agent.
     *
     * @since 3.0
     * @deprecated since 21.2
     */
    @Deprecated
    void installStatusReceiver(AdminStatusReceiver asr,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Get a list of nodes to upgrade in an order which will maintain
     * store availability.
     *
     * @since 3.0
     */
    String getUpgradeOrder(KVVersion targetVersion,
                           KVVersion prerequisiteVersion,
                           AuthContext authCtx,
                           short serialVersion)
        throws RemoteException;

    /**
     * Get a collection of node sets to upgrade the set of storage node in
     * order of the list.
     * @param targetVersion target upgrade version
     * @param prerequisiteVersion required upgrade version
     * @param authCtx
     * @param serialVersion
     * @return list of storage node sets that to make up the upgrade order
     * @throws RemoteException
     */
    List<Set<StorageNodeId>>
        getUpgradeOrderList(KVVersion targetVersion,
                            KVVersion prerequisiteVersion,
                            AuthContext authCtx,
                            short serialVersion)
        throws RemoteException;

    /**
     * Retrieve the beginning plan id and number of plans that satisfy the
     * request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] number of
     * plan ids in the range.
     *
     * Operates in three modes:
     *
     *    mode A requests howMany plans ids following startTime
     *    mode B requests howMany plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *        howMany is ignored in mode C.
     * @since 3.0
     */
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany,
                                AuthContext authCtx,
                                short serialVersion)
        throws RemoteException;

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped.
     *
     * @since 3.0
     */
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Return the brief and detailed description of all users for display
     *
     * @since 3.0
     */
    Map<String, UserDescription> getUsersDescription(AuthContext authCtx,
                                                     short serialVersion)
        throws RemoteException;

    /**
     * Verify if the specified password is correct for the user
     *
     * @since 3.0
     */
    boolean verifyUserPassword(String userName,
                               char[] password,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Executes snapshot operations for one or all datacenters, using the admin
     * to coordinate operations and locking.
     *
     * @param sop snapshot operation
     * @param sname name of snapshot
     * @param dcId id of datacenter or null for all datacenters
     * @return operation result
     *
     * @since 4.4
     */
    public SnapResultSummary executeSnapshotOperation(SnapshotOperation sop,
                                                      String sname,
                                                      DatacenterId dcId,
                                                      AuthContext authCtx,
                                                      short serialVersion)
        throws RemoteException;

    /**
     * Returns an array of names of snapshots from a specified storage node. If
     * no storage node id is specified, an arbitrary storage node will be
     * chosen.
     *
     * @param snid id of the storage node. If null, an arbitrary storage node
     * will be chosen.
     * @return an array of snapshot names
     *
     * @since 3.0
     */
    public String[] listSnapshots(StorageNodeId snid,
                                  AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException;

    /**
     * Repairs the admin quorum by updating the JE HA rep group membership.
     *
     * @since 3.4
     */
    public Set<AdminId> repairAdminQuorum(Set<DatacenterId> zoneIds,
                                          Set<AdminId> adminIds,
                                          AuthContext authCtx,
                                          short serialVersion)
        throws RemoteException;

    /**
     * Creates a plan to inform the Store of the existence of an ES node, and
     * stores it by its plan id.
     *
     * @param planName - the name of the plan
     * @param clusterName - the cluster name of the ES cluster.
     * @param transportHp - transport host:port of any node in the ES cluster.
     * @param secure - ES Cluster is set up as secured or not.
     * @param forceClear - if true, allows deletion of an existing ES index.
     * @return the plan id of the created plan
     * @since 4.0
     */
    int createRegisterESClusterPlan(String planName,
                                    String clusterName,
                                    String transportHp,
                                    boolean secure,
                                    boolean forceClear,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Creates a plan to cause the Store to forget about a registered ES
     * cluster.  Only one cluster may be registered, so no identifying
     * information is needed.
     *
     * @param planName the name of the plan
     * @return the plan id of the created plan
     * @since 4.0
     */
    int createDeregisterESClusterPlan(String planName,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

    /**
     * Verify data on this admin.
     *
     * @param options configuration of the verification
     * @throws RemoteException
     * @since 18.1
     */
    public VerificationInfo startAndPollVerify(VerificationOptions options,
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
    * Verify data on this admin.
    *
    * @param verifyBtree verifies the btree of databases
    * @param verifyLog verifies log files of databases
    * @param verifyIndex verifies the indexes
    * @param verifyRecord verifies the data records in disk
    * @param btreeDelay delay between batches for btree verification
    * @param logDelay delay between log file reads
    * @throws RemoteException
    * @throws IOException
    * @since 4.6
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
     * Create a plan to verify data on a node.
     *
     * @param planName
     * @param rid id of the node to be verified
     * @param verifyOptions properties for verification
     * @return plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyServicePlan(String planName,
                                       ResourceId rid,
                                       VerificationOptions verifyOptions,
                                       AuthContext authCtx,
                                       short serialVersion)
        throws RemoteException;

    /**
     * Create a plan to verify data on all admins that are deployed to the
     * specified zone or all zones.
     *
     * @param planName
     * @param dcid datacenter id
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyAllAdminsPlan(String planName,
                                         DatacenterId dcid,
                                         VerificationOptions verifyOptions,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Create a plan to verify data on all rns that are deployed to the
     * specified zone or all zones.
     *
     * @param planName
     * @param dcid datacenter id
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyAllRepNodesPlan(String planName,
                                           DatacenterId dcid,
                                           VerificationOptions verifyOptions,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Create a plan to verify data on all rns and admins that are deployed to
     * the specified zone or all zones.
     *
     * @param planName
     * @param dcid datacenter id
     * @param verifyOptions properties for verification
     * @return the plan id
     * @throws RemoteException
     * @since 18.1
     */
    public int createVerifyAllServicesPlan(String planName,
                                           DatacenterId dcid,
                                           VerificationOptions verifyOptions,
                                           AuthContext authCtx,
                                           short serialVersion)
        throws RemoteException;

    /**
     * Creates a plan to set limits on a table.
     */
    public int createTableLimitPlan(String planName,
                                    String namespace,
                                    String tableName,
                                    TableLimits newLimits,
                                    AuthContext NULL_CTX, short serialVersion)
        throws RemoteException;

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
                                              boolean install,
                                              AuthContext NULL_CTX,
                                              short serialVersion)
        throws RemoteException;

    /**
     * Returns information about TLS credentials for all SNAs as a
     * JSON-formatted string.
     *
     * @return information about TLS credentials in JSON format
     * @throws RemoteException if network error occurs
     * @since 24.4
     */
    String getTlsCredentialsInfo(AuthContext NULL_CTX, short serialVersion)
        throws RemoteException;
}
