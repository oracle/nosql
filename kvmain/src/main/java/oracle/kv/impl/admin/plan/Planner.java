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

package oracle.kv.impl.admin.plan;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.SnConsistencyUtils.ParamCheckResults;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.IdentityColumnInfo;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.FieldDef;
import oracle.kv.table.SequenceDef;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.ErrorMessage;

/**
 * The Planner creates and executes plans.  Plans are the means by which
 * topological changes are made to a store.  They are also used for creating
 * and modifying system metadata, such as tables, indexes, and security
 * information.
 *
 * Plan creation consists of populating the plan with tasks which perform the
 * actual work.  A single plan often comprises a number of tasks. Plan
 * execution is asynchronous, which is especially important to long-running
 * plans and those which affect many nodes in the system.  Plan execution is
 * idempotent in order to be resilient in the face of node failures, including
 * that of the admin node handling the plan.
 *
 * Error Handling
 * ==============
 * IllegalCommandException is used to indicate user error, such as a bad
 * parameter, or a user-provoked illegal plan transition. It is thrown
 * synchronously, in direct response to a user action. Examples:
 *  Bad parameter when creating a plan:
 *  - The user should fix the parameter and resubmit the
 *  User tries an illegal plan state transition, such as executing a plan that
 *  is not approved, approving a plan that is not pending, or executing a plan
 *  that has completed, etc.
 *  - The user should be notified that this was an illegal action
 *
 * OperationFaultException is thrown when plan execution runs into some kind of
 * resource problem, such as a RejectedExecutionException from lack of threads,
 * or a network problem, lack of ports, timeout, etc. In this case, the user is
 * notified and the GUI will present the option of retrying or rolling back the
 * plan.
 *
 * An AdminFaultException is thrown when an unexpected exception occurs during
 * plan execution. The fault handler processes the exception in such a way that
 * the Admin will not go down, but that the exception will be logged under
 * SEVERE and will be dumped to stderr. The problem is not going to get any
 * better without installing a bug fix, but the Admin should not go down.
 * The UI presents the option of retrying or rolling back the plan.
 *
 * Concurrency Limitations:
 * ========================
 * Plans may be created and approved for an indeterminate amount of
 * time before they are executed. However, topology dependent plans must clone
 * a copy of the topology at creation time, and use that to create a set of
 * directions to execute. Because of that, the topology must stay constant from
 * that point to execution point, and therefore only one topology changing
 * plan can be implemented at a time.
 *
 * Synchronization:
 * ========================
 *
 * New plan creation is serialized by synchronizing on the planner object (via
 * method synchronization). Manipulation of existing plans, such as execution
 * or restart etc, is synchronized at the Admin.
 *
 * In general, we would like to express the monitor locking sequence as going
 * from big objects to smaller objects, so the Admin would be locked before the
 * Planner, and the Planner before the Plan.  However we observe that in some
 * cases the plan is locked and then wants to update itself, requiring it the
 * thread to synchronize on the Planner, violating the aforementioned ideal.
 * Hence the desire to eliminate synchronization on the Planner for updates to
 * existing plans.  This is a TBD: see deadlocks described in [#22963] and
 * [#22992], both of which we believe have been eliminated, but other deadlocks
 * might be lurking, and therefore a comprehensive survey of synchronization in
 * Admin is called for.
 */

public class Planner {

    /**
     * The executor that we'll use for carrying out execution of the plan and
     * the tasks within it.
     */
    private ExecutorService executor;

    private final Logger logger;
    private final Admin admin;
    private final AtomicInteger planIdGenerator;

    /**
     * A dummy plan ID for use when locking for a command, not a plan. This
     * value will be different from all real plan IDs, because they are
     * non-negative.
     */
    private static final int COMMAND_LOCK_PLAN_ID = -1;

    private final Catalog catalog;

    /**
     */
    public Planner(Admin admin,
                   AdminServiceParams params,
                   int nextPlanId) {

        this.admin = admin;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        executor = Executors.newCachedThreadPool
            (new KVThreadFactory("Planner", logger));
        catalog = new Catalog();
        planIdGenerator = new AtomicInteger(nextPlanId);
    }

    /**
     * Returns the next plan ID.
     */
    public int getNextPlanId() {
        return planIdGenerator.get();
    }

    /**
     * Returns the next plan ID and advances the ID. The new ID is persisted.
     */
    public int getAndIncrementPlanId() {
        final int next = planIdGenerator.getAndIncrement();
        admin.saveNextId(planIdGenerator.get());
        return next;
    }

    /**
     * Review all in progress plans. Anything that is in RUNNING state did
     * not finish, and should be deemed to be interrupted. Should be called
     * by the Admin explicitly after the planner is constructed.
     * {@literal
     * 1.RUNNING plans ->INTERRUPT_REQUESTED -> INTERRUPTED, and
     *    will be restarted.
     * 2.INTERRUPT_REQUESTED plans -> INTERRUPTED and are not restarted. The
     *    failover is as if the cleanup phase was interrupted by the user.
     * 3.INTERRUPTED and APPROVED plans are left as is.
     * }
     */
    public Plan recover(Plan inProgressPlan) {
        if (inProgressPlan == null) {
            return null;
        }

        Plan restart = null;
        final State originalState = inProgressPlan.getState();
        switch (originalState) {
            case RUNNING:
                inProgressPlan.markAsInterrupted();
                /* Rerun it */
                restart = inProgressPlan;
                logger.log(Level.INFO,
                           "{0} originally in {1}, transitioned to {2}, will" +
                           " be restarted automatically",
                           new Object[] {inProgressPlan, originalState,
                                         inProgressPlan.getState()});
                break;
            case INTERRUPT_REQUESTED:
                /*
                * Let it move to interrupted state and stay there. The user had
                * previously requested an interrupt.
                */
                inProgressPlan.markAsInterrupted();
                logger.log(Level.INFO,
                           "{0} originally in {1}, transitioned to {2}, will" +
                           " not be restarted automatically",
                           new Object[] {inProgressPlan, originalState,
                                         inProgressPlan.getState()});
                break;
            default:
                logger.log(Level.INFO,
                           "{0} in {1} state will" +
                           " not be restarted automatically",
                           new Object[] {inProgressPlan, originalState});
                break;
        }

        /*
         * All non-terminated plans, including those that are in ERROR or
         * INTERRUPT state should be put in the catalog. Even the
         * non-restarted ones need to be there, so the user can decide manually
         * whether to retry them.
         */
        register(inProgressPlan);
        admin.savePlan(inProgressPlan, "Plan Recovery");
        return restart;
    }

    /**
     * Registers the specified plan. Public access for unit test.
     */
    public void register(Plan plan) {
        catalog.addNewPlan(plan);
    }


    /* For unit test support. */

    public void clearLocks(int planId) {
        catalog.clearLocks(planId);
    }

    public void clearLocksForCommand() {
        catalog.clearLocks(COMMAND_LOCK_PLAN_ID);
    }

    /**
     * Shuts down the planner. No new plans will be executed. If force is true
     * any running plans are interrupted. If force is false and wait is true
     * then this method will wait for executing plans to complete. The wait
     * flag is ignored if force is true.
     *
     * @param force interrupt running plans if true
     * @param wait wait for running plans to complete if force is false
     */
    public void shutdown(boolean force, boolean wait) {
        if (force) {
            executor.shutdownNow();
            return;
        }

        executor.shutdown();

        if (wait) {
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.log(Level.INFO, "Shutdown Planner failed: {0}", e);
            }
        }
    }

    /**
     * Returns true if the planner has been shutdown.
     *
     * @return true if the planner has been shutdown
     */
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    /*
     * NOTE that all plan creation is serial, so that each type of plan can
     * validate itself in a stable context. The catalog, and plan registration,
     * check for runtime constraints, such as plan exclusiveness, but since
     * registration is only done after the plan is created, it can't validate
     * for non-runtime constraints.  For example, the DeployStorePlan must
     * check whether the topology holds any other repNodes at creation time.
     */

    /**
     * Creates a data center and registers it with the topology.
     */
    public synchronized int
    createDeployDatacenterPlan(String planName,
                               String datacenterName,
                               int repFactor,
                               DatacenterType datacenterType,
                               boolean allowArbiters,
                               boolean masterAffinity) {

        final DeployDatacenterPlan plan =
            new DeployDatacenterPlan(planName, this,
                                     admin.getCurrentTopology(),
                                     datacenterName, repFactor,
                                     datacenterType,
                                     allowArbiters,
                                     masterAffinity);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a storage node and registers it with the topology.
     */
    public synchronized int
        createDeploySNPlan(String planName,
                           DatacenterId datacenterId,
                           StorageNodeParams inputSNP) {

            final DeploySNPlan plan =
                new DeploySNPlan(planName, this,
                                 admin.getCurrentTopology(),
                                 datacenterId, inputSNP);
            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates an Admin instance and updates the Parameters to reflect it.
     */
    public synchronized int createDeployAdminPlan(String name,
                                                  StorageNodeId snid,
                                                  AdminType type) {

        final DeployAdminPlan plan = new DeployAdminPlan(name,
                                                         this, snid,
                                                         type);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /*
     * Creates a PRIMARY Admin instance. For unit tests.
     */
    public int createDeployAdminPlan(String name,
                                     StorageNodeId snid) {
        return createDeployAdminPlan(name, snid, AdminType.PRIMARY);
    }

    /**
     * If <code>victim</code> is not <code>null</code>, then removes the Admin
     * with specified <code>AdminId</code>. Otherwise, if <code>dcid</code> is
     * not <code>null</code>, then removes all Admins in the specified
     * datacenter. If <code>failedSN</code> is true then remove admins hosted
     * on the failed SNs.
     */
    public synchronized int createRemoveAdminPlan(String name,
                                                  DatacenterId dcid,
                                                  AdminId victim,
                                                  boolean failedSN) {

        final RemoveAdminPlan plan =
            new RemoveAdminPlan(name, this, dcid, victim, failedSN);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan to deploy the specified TopologyCandidate. Allow
     * for passing for failedShard parameter during deploy topology
     */
    public synchronized int createDeployTopoPlan(String planName,
                                                 String candidateName,
                                                 RepGroupId failedShard) {
        final DeployTopoPlan plan =
            DeployTopoPlan.create(planName,
                                  this,
                                  admin.getCurrentTopology(),
                                  admin.getCandidate(candidateName),
                                  failedShard);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createFailoverPlan(
        String planName,
        Set<DatacenterId> newPrimaryZones,
        Set<DatacenterId> offlineZones) {

        final FailoverPlan plan = FailoverPlan.create(
            planName, this, admin.getCurrentTopology(),
            newPrimaryZones, offlineZones);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan to stop of all RepNodes in the store.
     */
    public synchronized int
        createStopAllRepNodesPlan(String planName) {

            final StopAllRepNodesPlan plan =
                new StopAllRepNodesPlan(planName, this,
                                        admin.getCurrentTopology());
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan to start all RepNodes the store.
     */
    public synchronized int
        createStartAllRepNodesPlan(String planName) {

            final StartAllRepNodesPlan plan =
                new StartAllRepNodesPlan(planName, this,
                                         admin.getCurrentTopology());
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan to stop the specified services.
     */
    public synchronized int
        createStopServicesPlan(String planName,
                               Set<? extends ResourceId> serviceIds) {
        final Plan plan = new StopServicesPlan(planName, this,
                                               admin.getCurrentTopology(),
                                               serviceIds);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan to start the specified services.
     */
    public synchronized int
        createStartServicesPlan(String planName,
                                Set<? extends ResourceId> serviceIds) {
        final Plan plan = new StartServicesPlan(planName, this,
                                                admin.getCurrentTopology(),
                                                serviceIds);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan for replacing a potentially failed node in the store.
     * Any resources known to be allocated on the failed node will be
     * moved to the new node.  The new node must be an as yet unused node.
     */
    public synchronized int
        createMigrateSNPlan(String planName,
                            StorageNodeId oldNode,
                            StorageNodeId newNode) {

            final MigrateSNPlan plan =
                new MigrateSNPlan(planName, this,
                                  admin.getCurrentTopology(),
                                  oldNode, newNode);

            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates a plan for removing a storageNode. Removal is only permitted
     * for stopped storageNodes which do not house any services. It's meant to
     * remove defunct storageNodes after a migration has been run, or if
     * an initial deployment failed.
     */
    public synchronized int
        createRemoveSNPlan(String planName,
                           StorageNodeId targetNode) {
            final RemoveSNPlan plan =
                new RemoveSNPlan(planName, this,
                                 admin.getCurrentTopology(), targetNode);

            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates a plan for removing a datacenter. Removal is only permitted for
     * <em>empty</em> datacenters; that is, datacenters which contain no
     * storage nodes.
     */
    public synchronized int
        createRemoveDatacenterPlan(String planName,
                                   DatacenterId targetId) {
            final RemoveDatacenterPlan plan =
                new RemoveDatacenterPlan(planName, this,
                                         admin.getCurrentTopology(),
                                         targetId);

            register(plan);
            return admin.saveCreatePlan(plan);
        }

    public synchronized int createAddTablePlan(String planName,
                                               TableImpl table,
                                               String parentName,
                                               boolean systemPlan,
                                               SequenceDef sequenceDef) {
        final Plan plan = TablePlanGenerator.
            createAddTablePlan(planName, this, table, parentName, systemPlan,
                sequenceDef);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates evolve table plan for user tables.
     */
    public int createEvolveTablePlan(String planName,
                                     String namespace,
                                     String tableName,
                                     int tableVersion,
                                     FieldMap fieldMap,
                                     TimeToLive ttl,
                                     TimeToLive beforeImgTTL,
                                     IdentityColumnInfo identityColumnInfo,
                                     Set<Integer> regions) {
        return createEvolveTablePlan(planName,
                                     namespace,
                                     tableName,
                                     tableVersion,
                                     fieldMap,
                                     ttl,
                                     beforeImgTTL,
                                     null, /* description (no change) */
                                     false /* systemTable */,
                                     identityColumnInfo /*identityColumnInfo*/,
                                     null /* sequenceDefChange */,
                                     regions);
    }

    /**
     * Creates evolve table plan.
     */
    public synchronized int createEvolveTablePlan(
        String planName,
        String namespace,
        String tableName,
        int tableVersion,
        FieldMap fieldMap,
        TimeToLive ttl,
        TimeToLive beforeImgTTL,
        String description,
        boolean systemTable,
        IdentityColumnInfo newIdentityColumnInfo,
        SequenceDef sequenceDefChange,
        Set<Integer> regions) {

        final Plan plan = TablePlanGenerator.
            createEvolveTablePlan(planName, this,
                                  namespace,
                                  tableName,
                                  tableVersion, fieldMap,
                                  ttl,
                                  beforeImgTTL,
                                  description,
                                  systemTable,
                                  newIdentityColumnInfo,
                                  sequenceDefChange,
                                  regions);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createRemoveTablePlan(String planName,
                                                  String namespace,
                                                  String tableName) {
        final Plan plan = TablePlanGenerator.
            createRemoveTablePlan(planName, this,
                                  admin.getCurrentTopology(),
                                  namespace, tableName);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createAddIndexPlan(String planName,
                                               String namespace,
                                               String indexName,
                                               String tableName,
                                               String[] indexedFields,
                                               FieldDef.Type[] indexedTypes,
                                               boolean indexNulls,
                                               boolean isUnique,
                                               String description,
                                               boolean systemTable) {
        final Plan plan = TablePlanGenerator.
            createAddIndexPlan(planName, this,
                               admin.getCurrentTopology(),
                               namespace, indexName, tableName,
                               indexedFields, indexedTypes,
                               indexNulls, isUnique,
                               description, systemTable);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createRemoveIndexPlan(String planName,
                                                  String namespace,
                                                  String indexName,
                                                  String tableName,
                                                  boolean override) {
        final Plan plan = TablePlanGenerator.
            createRemoveIndexPlan(planName, this,
                                  admin.getCurrentTopology(),
                                  namespace, indexName, tableName, override);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createAddTextIndexPlan
        (String planName, String namespace, String indexName, String tableName,
         AnnotatedField[] ftsFields, Map<String,String> properties,
         String description, boolean override) {

        final Plan plan = TablePlanGenerator.
            createAddTextIndexPlan(planName, this,
                                   namespace, indexName, tableName,
                                   ftsFields, properties, description,
                                   override);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createBroadcastTableMDPlan() {
        final Plan plan = TablePlanGenerator.createBroadcastTableMDPlan(this);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createBroadcastSecurityMDPlan() {
        final Plan plan = SecurityMetadataPlan.
            createBroadcastSecurityMDPlan(this);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * TODO: future: consolidate change parameters plans for RN, SN, and admin.
     */
    public synchronized int
        createChangeParamsPlan(String planName,
                               ResourceId rid,
                               ParameterMap newParams) {

            Plan plan = null;
            if (rid instanceof RepNodeId) {
                final Set<RepNodeId> ids = new HashSet<>();
                ids.add((RepNodeId) rid);
                plan =
                    new ChangeParamsPlan(planName, this,
                                         admin.getCurrentTopology(),
                                         ids, newParams);
            } else if (rid instanceof StorageNodeId) {
                plan =
                    new ChangeSNParamsPlan(planName, this,
                                           (StorageNodeId) rid, newParams);
            } else if (rid instanceof AdminId) {
                plan =
                    new ChangeAdminParamsPlan(planName, this,
                                              (AdminId) rid, newParams);
            } else if (rid instanceof ArbNodeId) {
                final Set<ArbNodeId> ids = new HashSet<>();
                ids.add((ArbNodeId) rid);
                plan =
                    new ChangeANParamsPlan(planName, this,
                                           admin.getCurrentTopology(),
                                           ids, newParams);
            }
            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates a plan to apply parameters to all ArbNodes deployed in the
     * specified datacenter or if null, to all ArbNodes in the store.
     */
    public synchronized int
        createChangeAllANParamsPlan(String planName,
                                    DatacenterId dcid,
                                    ParameterMap newParams) {
        Set<ArbNodeId> anIds = admin.getCurrentTopology().getArbNodeIds(dcid);
        final Plan plan =
            new ChangeANParamsPlan(planName, this,
                                   admin.getCurrentTopology(),
                                   anIds, newParams);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan to apply parameters to all RepNodes deployed in the
     * specified datacenter.
     */
    public synchronized int
        createChangeAllParamsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams) {

            final Plan plan =
                new ChangeAllParamsPlan(planName, this,
                                        admin.getCurrentTopology(),
                                        dcid, newParams);
            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates a plan to apply parameters to all Admins deployed in the
     * specified datacenter.
     */
    public synchronized int
        createChangeAllAdminsPlan(String planName,
                                  DatacenterId dcid,
                                  ParameterMap newParams) {

            final Plan plan =
                new ChangeAdminParamsPlan(planName, this, null,
                                          dcid, admin.getCurrentTopology(),
                                          newParams);
            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates a plan to apply new global components parameters to all services
     * deployed in the store.
     */
    public synchronized int
        createChangeGlobalComponentsParamsPlan(String planName,
                                               ParameterMap newParams,
                                               KVVersion storeVersion) {
            final Plan plan =
                new ChangeGlobalComponentsParamsPlan(planName, this,
                                                     admin.getCurrentTopology(),
                                                     newParams,
                                                     storeVersion);
            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates a plan to apply new global security parameters to all services
     * deployed in the store.
     */
    public synchronized int
        createChangeGlobalSecurityParamsPlan(String planName,
                                             ParameterMap newParams) {
        final Plan plan =
                new ChangeGlobalSecurityParamsPlan(planName, this,
                                                   admin.getCurrentTopology(),
                                                   newParams);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a user and add it to the kvstore.
     */
    public synchronized int
        createCreateUserPlan(String planName,
                             String userName,
                             boolean isEnabled,
                             boolean isAdmin,
                             char[] plainPassword,
                             Long pwdLifetime) {

            final SecurityMetadataPlan plan =
                SecurityMetadataPlan.createCreateUserPlan(
                    planName, this, userName, isEnabled,
                    isAdmin, plainPassword, pwdLifetime);
            register(plan);
            return admin.saveCreatePlan(plan);
        }

    /**
     * Creates an external user and add it to the kvstore.
     */
    public synchronized int
        createCreateExternalUserPlan(String planName,
                                     String userName,
                                     boolean isEnabled,
                                     boolean isAdmin) {

        final SecurityMetadataPlan plan =
                SecurityMetadataPlan.createCreateExternalUserPlan(
                    planName, this, userName, isEnabled, isAdmin);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Changes the information of a kvstore user.
     */
    public synchronized int
        createChangeUserPlan(String planName,
                             String userName,
                             Boolean isEnabled,
                             char[] plainPassword,
                             boolean retainPassword,
                             boolean clearRetainedPassword,
                             Long pwdLifetime) {

        final SecurityMetadataPlan plan =
                SecurityMetadataPlan.createChangeUserPlan(
                    planName, this, userName, isEnabled,
                    plainPassword, retainPassword, clearRetainedPassword,
                    pwdLifetime);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Removes a user with the specified name from the store.
     */
    public synchronized int
        createDropUserPlan(String planName, String userName, boolean cascade) {

        final AbstractPlan plan = SecurityMetadataPlan.createDropUserPlan(
            planName, this, userName, cascade);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a user-defined role and add it to the kvstore.
     */
    public synchronized int createCreateRolePlan(String planName,
                                                 String roleName) {

        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createCreateRolePlan(planName, this, roleName);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Removes a user-defined role with the specified name from the store.
     */
    public synchronized int createDropRolePlan(String planName,
                                               String roleName) {

        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createDropRolePlan(planName, this, roleName);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Grant given roles to user in the store.
     */
    public synchronized int
        createGrantPlan(String planName, String grantee, Set<String> roles) {

        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createGrantPlan(planName,
                                                 this, grantee, roles);

        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Grant given roles to another role.
     */
    public synchronized int
        createGrantRolesToRolePlan(String planName,
                                   String grantee,
                                   Set<String> roles) {

        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createGrantRolesToRolePlan(
                planName, this, grantee, roles);

        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Revoke given roles from user in the store.
     */
    public synchronized int
        createRevokePlan(String planName, String revokee, Set<String> roles) {

        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createRevokePlan(planName,
                                                  this, revokee, roles);

        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Grant given roles to another role.
     */
    public synchronized int
        createRevokeRolesFromRolePlan(String planName,
                                      String revokee,
                                      Set<String> roles) {

        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createRevokeRolesFromRolePlan(
                planName, this, revokee, roles);

        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that grants a set of privileges on, optionally, a table
     * to a role.
     *
     * @param planName plan name
     * @param roleName role name
     * @param tableName table name, null if granting only system privileges
     * @param privs privileges to grant
     * @return planId
     */
    public synchronized int
        createGrantPrivilegePlan(String planName,
                                 String roleName,
                                 String namespace,
                                 String tableName,
                                 Set<String> privs) {
        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createGrantPrivsPlan(planName,
                                                      this, roleName, namespace,
                                                      tableName, privs);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that revokes a set of privileges on, optionally, a table
     * from a role.
     *
     * @param planName plan name
     * @param roleName role name
     * @param tableName table name, null if revoking only system privileges
     * @param privs privileges to revoke
     * @return planId
     */
    public synchronized int
        createRevokePrivilegePlan(String planName,
                                  String roleName,
                                  String namespace,
                                  String tableName,
                                  Set<String> privs) {
        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createRevokePrivsPlan(
                planName, this, roleName, namespace,
                tableName, privs);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that grants a set of privileges on a namespace to a role.
     *
     * @param planName plan name
     * @param roleName role name
     * @param namespace namespace name
     * @param privs privileges to grant
     * @return planId
     */
    public synchronized int
        createGrantNamespacePrivilegePlan(String planName,
                                          String roleName,
                                          String namespace,
                                          Set<String> privs) {
        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createGrantNamespacePrivsPlan(
                planName, this, roleName, namespace, privs);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that revokes a set of privileges on a namespace
     * from a role.
     *
     * @param planName plan name
     * @param roleName role name
     * @param namespace namespace name, null if revoking only system privileges
     * @param privs privileges to revoke
     * @return planId
     */
    public synchronized int
        createRevokeNamespacePrivilegePlan(String planName,
                                           String roleName,
                                           String namespace,
                                           Set<String> privs) {
        final SecurityMetadataPlan plan =
            SecurityMetadataPlan.createRevokeNamespacePrivsPlan(planName, this,
                roleName, namespace, privs);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that verifies and repairs the store topology.
     */
    public synchronized int createRepairPlan(String planName) {

        final RepairPlan plan = new RepairPlan(planName, this);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that performs network restore from source node to target
     * node.
     *
     * @param planName plan name
     * @param sourceNode restore source node id
     * @param targetNode restore target node id
     * @param retainOrigLog if retain original log file
     * @return planId
     */
    public synchronized int createNetworkRestorePlan(String planName,
                                                     ResourceId sourceNode,
                                                     ResourceId targetNode,
                                                     boolean retainOrigLog) {

        final NetworkRestorePlan plan = new NetworkRestorePlan(planName, this,
                                                               sourceNode,
                                                               targetNode,
                                                               retainOrigLog);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public Integer createSNConsistencyPlan(String planName,
                                           StorageNodeId snId,
                                           ParamCheckResults pcr) {
        SNParameterConsistencyPlan plan =
            new SNParameterConsistencyPlan(planName,
                                           this, snId, pcr);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public Integer
        createUpdateSoftwareVersionPlan(String planName,
                                        HashMap<StorageNodeId, String> update) {
        final Plan plan =
            new UpdateSoftwareVersionPlan(planName, this, update);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Creates a plan that enables specified request type on given shards or
     * the entire store.
     *
     * @param planName plan name
     * @param requestType type of request is being enabled
     * @param resourceIds resource IDs will be enabled specified request type
     * @param entireStore if the entire store will be enabled given request type
     * @return planId
     */
    public synchronized int
        createEnableRequestsPlan(final String planName,
                                 final String requestType,
                                 final Set<? extends ResourceId> resourceIds,
                                 final boolean entireStore) {

        final EnableRequestsTypePlan plan =
            new EnableRequestsTypePlan(planName, this, requestType,
                                       resourceIds, entireStore);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Submits a plan for asynchronous execution. If a previous plan is still
     * executing, we will currently throw an exception. In the future, plans
     * may be queued, but queuing would require policies and mechanism to
     * determine what should happen to the rest of the queue if a plan fails.
     * For example, should we "run the next plan, but only if the current
     * succeeds" or ".. regardless of if the current succeeds", etc.
     *
     * Plan execution can be repeated, in order to retry a plan.
     * @throws PlanLocksHeldException
     * @throws IllegalCommandException if there is a problem with executing the
     * plan.
     */
    public PlanRun executePlan(Plan plan, boolean force)
        throws PlanLocksHeldException {

        /* For now, a Planner will only execute an AbstractPlan. */
        final AbstractPlan targetPlan;
        if (plan instanceof AbstractPlan) {
            targetPlan = (AbstractPlan) plan;
        } else {
            throw new NonfatalAssertionException
                ("Unknown Plan type: " + plan.getClass() +
                 " cannot be executed");
        }

        /* Check any preconditions for running the plan */
        targetPlan.validateStartOfRun();

        /* Check that the catalog's rules for running this plan are ok */
        catalog.validateStart(plan);
        boolean success = false;
        PlanRun planRun = null;
        try {
            /**
             * Make sure we can get any plan-exclusion locks we need. Lock
             * before doing checks, to make sure that the topology does not
             * change.
             *
             * Synchronize the catalog to avoid deadlocks due to plans
             * attempting to lock the same locks in different order.
             */
            synchronized (catalog) {
                plan.getCatalogLocks();
            }

            /* Validate that this plan can run */
            plan.preExecuteCheck(force, logger);

            /*
             * Executing a plan equates to executing each of its tasks and
             * monitoring their state.  We'll kick off this process by running a
             * PlanExecutor in another thread.
             */
            planRun = targetPlan.startNewRun();
            final PlanExecutor planExec = new PlanExecutor(admin, this,
                                                           targetPlan,
                                                           planRun, logger);

            final Future<State> future = executor.submit(planExec);

            /*
             * Note that Catalog.addPlanFuture guards against the possibility
             * that the execute thread has finished before the future is added
             * to the catalog.
             */
            catalog.addPlanFuture(targetPlan, future);
            success = true;
            return planRun;
        } catch (RejectedExecutionException e) {
            final String problem =
                "Plan did not start, insufficient resources for " +
                "executing a plan";
            if (planRun != null) {
                plan.saveFailure(planRun, e, problem, ErrorMessage.NOSQL_5400,
                                 CommandResult.PLAN_CANCEL, logger);
            }
            planFinished(targetPlan);
            throw new CommandFaultException(
                problem, new OperationFaultException(problem, e),
                ErrorMessage.NOSQL_5400, CommandResult.PLAN_CANCEL);
        } finally {
            if (!success) {
                catalog.clearCurrentExecutingPlan(targetPlan);
            }
        }
    }

    /**
     * Used by the PlanExecutor to indicate that it's finished execution.
     */
    void planFinished(Plan plan) {
        catalog.clearLocks(plan.getId());
        catalog.clearPlan(plan);
    }

    public Admin getAdmin() {
        return admin;
    }

    public synchronized int createAddNamespacePlan(String planName,
        String namespace) {
        final Plan plan = TablePlanGenerator.
            createAddNamespacePlan(planName, this, namespace);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createRemoveNamespacePlan(String planName,
        String namespace, boolean cascade) {
        final Plan plan = TablePlanGenerator.
            createRemoveNamespacePlan(planName, this, admin.getCurrentTopology(),
                                      namespace, cascade);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createAddRegionPlan(String planName,
                                                String regionName) {
        final Plan plan = TablePlanGenerator.createAddRegionPlan(planName,
                                                                 this,
                                                                 regionName);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createDropRegionPlan(String planName,
                                                 String regionName) {
        final Plan plan = TablePlanGenerator.createDropRegionPlan(planName,
                                                                  this,
                                                                  regionName);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public synchronized int createSetLocalRegionNamePlan(String planName,
                                                         String regionName) {
        final Plan plan =
                TablePlanGenerator.createSetLocalRegionNamePlan(planName,
                                                                this,
                                                                regionName);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /* -- plan locking -- */

    /**
     * Lock categories. These are provided to avoid name conflicts between
     * components. They have no other locking semantics. Feel free to add as
     * needed.
     */
    public enum LockCategory {
        TOPO,       /* topology resources, sn, an, rn, ... */
        TABLE,      /* table metadata including indexes */
        REGION,     /* regions */
        ELASTICITY  /* elasticity */
    }

    /**
     * Locks the specified name within the specified category. Locks the
     * specified name within the specified category. If the lock is a
     * compound name a read lock is taken on each sub-component. Throws
     * PlanLocksHeldException if the name is already locked.
     */
    public void lock(int planId, String planName,
                     LockCategory category, String... name)
        throws PlanLocksHeldException {
        catalog.lock(planId, planName, category, name);
    }

    /**
     * Locks the elasticity lock. Throws PlanLocksHeldException if already
     * locked.
     */
    public void lockElasticity(int planId, String planName)
        throws PlanLocksHeldException {
        catalog.lock(planId, planName, LockCategory.ELASTICITY, "");
    }

    /**
     * Locks the elasticity lock for the specified command. Throws
     * PlanLocksHeldException if already locked.
     */
    public void lockElasticityForCommand(String commandName)
        throws PlanLocksHeldException {
        catalog.lock(COMMAND_LOCK_PLAN_ID, commandName,
                     LockCategory.ELASTICITY, "");
    }

    /**
     * A collection of non-finished plans. It is used to enforce runtime
     * constraints such as:
     * - execution time locks taken. Locks must be acquire for each execution,
     * because the locks are transient and only last of the life of the
     * Planner instance.
     *
     * The catalog lets the Admin query plan status, and interrupt running
     * plans.
     *
     * The locking methods are synchronized, however is multiple locks are
     * being taken the caller should first synchronize on the catalog instance.
     * For example, locks are taken out at the beginning of plan execution
     * from a call to plan.getCategoryLocks(). The monitor on the catalog
     * should be held during that call to avoid deadlocks.
     */
    private class Catalog {
        private Plan currentExecutingPlan;
        private final Map<Integer, Future<State>> futures = new HashMap<>();
        private final Map<Integer, Plan> planMap = new HashMap<>();

        /*
         * Locks.
         */
        private final Map<String, Lock> locks = new HashMap<>();

        /**
         * Locks the specified name within the specified category. A lock name
         * is a string consisting of the category followed by a period and the
         * specified name(s). For example the lock: (category, name) would
         * generate the lock name:
         *
         *      category.name
         *
         * A compound name is when there are more than one name. For example:
         * (category, name1, name2) would generate the lock name:
         *
         *      category.name1.name2
         *
         * If successful, an exclusive lock is taken on the lock name.
         *
         * If the lock name is a compound name, in addition to the exclusive
         * lock on the full name, a read lock is taken on each sub-string.
         * For example if the lock name is: category.name1.name2.name3
         * three different locks would be acquired:
         *
         *      category.name1              for read
         *      category.name1.name2        for read
         *      category.name1.name2.name3  exclusive
         *
         * A name may be null. If so, that part of the lock name is empty.
         * For example the lock: (category, null, name2) becomes:
         *
         *      category..name2
         *
         * Throws PlanLocksHeldException if any of the locks could not be
         * acquired.
         *
         * TODO - Should we take out read locks on the just the category?
         * Currently the name could be null - locking "category." but we
         * are not taking read locks on that name.
         */
        synchronized void lock(int planId, String planName,
                               LockCategory category, String... name)
            throws PlanLocksHeldException {
            assert name.length > 0;

            /**
             * The lock name is category.name1.name2. ... nameN.
             */
            final StringBuilder sb = new StringBuilder(category.toString());
            for (String c : name) {
                sb.append(".");
                if ((c == null) || c.isEmpty()) {
                    continue;
                }
                sb.append(c);

                /* Get a read lock on the sub-component */
                lock(planId, planName, sb.toString(), true);
            }
            /* Get an exclusive lock on the final string */
            lock(planId, planName, sb.toString(), false);
        }

        private void lock(int planId,
                          String planName,
                          String lockName,
                          boolean forRead)
                throws PlanLocksHeldException {
            assert Thread.holdsLock(this);

            logger.log(Level.FINE, "Plan {0} [{1}] locking {2} for read={3}",
                       new Object[]{planId, planName, lockName, forRead});

            Lock lk = locks.get(lockName);
            if (lk == null) {
                lk = new Lock();
            }

            if (!lk.get(planId, planName, forRead)) {
                logger.log(Level.FINE,
                           "Plan {0} [{1}] failed to lock {2} for read={3}",
                           new Object[]{planId, planName, lockName, forRead});
                throw cantLock(planId, planName, lk);
            }
            locks.put(lockName, lk);
        }

        /**
         * Removes all locks that pertain to this plan
         */
        synchronized void clearLocks(int planId) {
            final Iterator<Lock> iter = locks.values().iterator();
            while (iter.hasNext()) {
                if (iter.next().releaseForPlanId(planId)) {
                    iter.remove();
                }
            }
        }

        /**
         * Constructs a PlanLocksHeldException.
         */
        private PlanLocksHeldException cantLock(int planId,
                                                String planName,
                                                Lock lock) {
            int lockedPlans = 0;
            int lockingPlanId = 0;
            final StringBuilder sb = new StringBuilder();
            sb.append("Couldn't execute ");
            sb.append(buildName(planId, planName));
            sb.append(" because ");

            if (lock.exclusive) {
                sb.append(buildName(lock.lockingPlanId, lock.lockingPlanName));
                lockingPlanId = lock.lockingPlanId;
            } else {
                assert !lock.readLocks.isEmpty();

                int size = lock.readLocks.size();
                for (Entry<Integer, String> e : lock.readLocks.entrySet()) {
                    sb.append(buildName(e.getKey(), e.getValue()));
                    if (size - lockedPlans == 2) {
                        sb.append(" and ");
                    } else if (size - lockedPlans > 2) {
                        sb.append(", ");
                    }
                    /* PLHE only takes one id, just use the last one */
                    lockingPlanId = e.getKey();
                    lockedPlans++;
                }
            }
            if (lockedPlans > 1) {
                sb.append(" are running, and are probably holding conflicting" +
                          " locks. Wait until each plan/command is" +
                          " finished or interrupted ");
            } else {
                sb.append(" is running, and is probably holding a conflicting" +
                          " lock. Wait until that plan/command is" +
                          " finished or interrupted ");
            }

            sb.append("then execute the new plan/command again.");
            return new PlanLocksHeldException(sb.toString(), lockingPlanId);
        }

        private String buildName(int planId, String planName) {
            StringBuilder retName = new StringBuilder();
            if (planId != COMMAND_LOCK_PLAN_ID) {
                retName.append("Plan " + planId);
                if ((planName != null) && (!planName.isEmpty())) {
                    retName.append(" (" + planName + ")");
                }
            } else {
                if ((planName != null) && (!planName.isEmpty())) {
                    retName.append(planName + " ");
                }
                retName.append("command");
            }
            return retName.toString();
        }

        /**
         * A lock to coordinate operations. The lock can be unlocked,
         * exclusive locked, and read locked. Only one plan can have an
         * exclusive lock, while multiple plans can have read locks. If
         * the lock is exclusive there can be no readers.
         */
        private class Lock {
            /* True if an exclusive lock */
            boolean exclusive;

            /* Owners of the exclusive lock. Valid iff exclusive == true */
            int lockingPlanId;
            String lockingPlanName;

            /* Read locks, must be empty if exclusive == true */
            final Map<Integer, String> readLocks = new HashMap<>();

            /**
             * Creates an unlocked lock.
             */
            Lock() {}

            /**
             * Gets a read or exclusive lock. If forRead is false, the lock
             * will be acquired and success returned if unlocked and there
             * are no readers other than the specified plan, or the plan
             * already owns the lock. If the plan has a read lock, and no
             * other read locks are present, the lock is upgraded to an
             * exclusive lock.
             *
             * If forRead is true the plan will be added to the lock as as
             * reader if unlocked.
             *
             * @retun true if successful
             */
            synchronized boolean get(int planId,
                                     String planName,
                                     boolean forRead) {
                if (forRead) {
                    /* If exclusive, return success if the plan is the owner */
                    if (exclusive) {
                        return lockingPlanId == planId;
                    }
                    readLocks.put(planId, planName);
                    return true;
                }

                /* Exclusive lock, check for readers */
                if (!readLocks.isEmpty()) {
                    assert exclusive == false;

                    /*
                     * If only one reader, and it is the plan, clear the readers
                     * and continue which will get the lock.
                     */
                    if ((readLocks.size() > 1) ||
                        !readLocks.containsKey(planId)) {
                        return false;
                    }
                    readLocks.clear();
                }
                assert readLocks.isEmpty();

                if (exclusive && (lockingPlanId != planId)) {
                    return false;
                }
                exclusive = true;
                lockingPlanId = planId;
                lockingPlanName = planName;
                return true;
            }

            /**
             * Releases the lock for the specified plan. If this plan owns
             * an exclusive lock it is released. If the plan was a reader
             * it is removed from the set of readers. Returns true if this
             * lock is completely released (unlocked and no readers).
             */
            synchronized boolean releaseForPlanId(int planId) {
                if (!exclusive) {
                    readLocks.remove(planId);
                    return readLocks.isEmpty();
                }
                assert readLocks.isEmpty();

                if (lockingPlanId == planId) {
                    exclusive = false;
                    lockingPlanId = 0;
                    lockingPlanName = null;
                    return true;
                }
                return false;
            }
        }

        synchronized void addNewPlan(Plan plan) {
            planMap.put(plan.getId(), plan);
        }

        synchronized void addPlanFuture(Plan plan, Future<State> future) {

            /*
             * There is the small possibility that the plan execution thread
             * will have executed and finished the plan before we save its
             * future. Check so that we don't needlessly add it.
             */
            if (!plan.getState().isTerminal()) {
                futures.put(plan.getId(), future);
            }
        }

        /**
         * Check that the plan is registered. For exclusive plans, check to be
         * sure that the plan is not already executing and otherwise set the
         * currently executing plan.
         */
        synchronized void validateStart(Plan plan) {

            /*
             * Plans that were created through Planner should be
             * registered. This is really an assertion check for internal
             * testing plans.
             */
            if (getPlan(plan.getId()) == null) {
                throw new NonfatalAssertionException
                    (plan + " must be registered.");
            }
        }

        /** Clear the currently executing plan. */
        synchronized void clearCurrentExecutingPlan(Plan plan) {
            if (currentExecutingPlan == plan) {
                currentExecutingPlan = null;
            }
        }

        synchronized void clearPlan(Plan plan) {
            futures.remove(plan.getId());

            clearCurrentExecutingPlan(plan);

            if (!plan.getState().isTerminal()) {
                return;
            }

            planMap.remove(plan.getId());
        }

        Plan getPlan(int planId) {
            return planMap.get(planId);
        }
    }

    /**
     * Returns the specified plan from the cache of active Plans.
     */
    public Plan getCachedPlan(int planId) {
        return catalog.getPlan(planId);
    }

    private Plan getFromCatalog(int planId) {

        final Plan plan = getCachedPlan(planId);

        if (plan == null) {
            /*
             * Plan ids may be specified via the CLI, so a user specified
             * invalid id may result in this problem, and this should be an
             * IllegalCommandException.
             */
            throw new IllegalCommandException("Plan " + planId +
                                              " is not an active plan");
        }
        return plan;
    }

    /**
     */
    public void approvePlan(int planId) {
        final Plan plan = getFromCatalog(planId);
        try {
            ((AbstractPlan) plan).requestApproval();
        } catch (IllegalStateException e) {

            /*
             * convert this to IllegalCommandException, since this is a
             * user initiated action.
             */
            throw new IllegalCommandException(e.getMessage());
        }
    }

    /**
     * Cancels a PENDING or APPROVED plan. Returns the canceled plan.
     */
    public Plan cancelPlan(int planId) {
        final AbstractPlan plan = (AbstractPlan) getFromCatalog(planId);
        try {
            plan.requestCancellation();
            planFinished(plan);
            return plan;
        } catch (IllegalStateException e) {

            /*
             * convert this to IllegalCommandException, since this is a
             * user initiated action.
             */
            throw new IllegalCommandException(e.getMessage(),
                                              ErrorMessage.NOSQL_5200,
                                              CommandResult.NO_CLEANUP_JOBS);
        }
    }

    /**
     * Interrupt a RUNNING plan.  Users must retry or rollback interrupted
     * plans.
     */
    public void interruptPlan(int planId) {
        final Plan plan = getFromCatalog(planId);
        final AbstractPlan aplan = (AbstractPlan) plan;
        if (aplan.cancelIfNotStarted()) {
            /*
             * If the plan isn't even running, change state to CANCEL,
             * and clean up the related plan lock. No need to do any interrupt
             * processing.
             */
            planFinished(aplan);
            return;
        }

        final String errMsg =
            plan.getState().checkTransition(State.INTERRUPT_REQUESTED);
        if (errMsg != null) {
            throw new IllegalCommandException
                ("Can't interrupt plan " + plan + " in state " +
                 plan.getState() + ": " +
                 (!errMsg.isEmpty() ? ": " + errMsg : ""));
        }

        logger.info("User requesting interrupt of " + plan);
        aplan.requestInterrupt();
    }

    /**
     * Returns the logger for this planner.
     *
     * @return the logger
     */
    Logger getLogger() {
        return logger;
    }

    /* For unit test support */
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Registers an externally managed Elasticsearch cluster and updates
     * SNA Parameters to reflect it.
     */
    public Integer createRegisterESClusterPlan(String planName,
                                               String clusterName,
                                               String transportHp,
                                               boolean secure,
                                               boolean forceClear) {
        final Plan plan =
            new RegisterESPlan(planName, this,
                               clusterName, transportHp, secure,
                               forceClear);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    public Integer createDeregisterESClusterPlan(String planName) {
        final Plan plan = new DeregisterESPlan(planName, this);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Create a plan to verify data on a service node.
     */
    public synchronized int
        createVerifyServicePlan(String planName,
                                ResourceId rid,
                                VerificationOptions verifyOptions) {
        Plan plan = null;
        if (rid instanceof RepNodeId) {
            plan = new VerifyDataPlanV2(planName, this, null,
                                        Collections.singleton((RepNodeId)rid),
                                        verifyOptions);

        } else if (rid instanceof AdminId) {
            plan = new VerifyDataPlanV2(planName, this,
                                        Collections.singleton((AdminId)rid), null,
                                        verifyOptions);
        } else {
            throw new IllegalCommandException(rid + " is not a valid id" +
                                              " to be verified");
        }
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /**
     * Create a plan to verify data on all admins that are deployed to the
     * specified zone or all zones.
     */
    public synchronized int
        createVerifyAllAdminsPlan(String planName,
                                  DatacenterId dcid,
                                  VerificationOptions verifyOptions) {

        /* get all admins */
        Set<AdminId> allId = getAdmin().getCurrentParameters().
            getAdminIds(dcid, admin.getCurrentTopology());

        Plan plan = new VerifyDataPlanV2(planName, this, allId, null,
                                       verifyOptions);
        register(plan);
        return admin.saveCreatePlan(plan);

    }

    /**
     * Creates a plan to verify data on all rns that are deployed to the
     * specified zone or all zones.
     */
    public synchronized int
        createVerifyAllRepNodesPlan(String planName,
                                    DatacenterId dcid,
                                    VerificationOptions verifyOptions) {

        Set<RepNodeId> allId = admin.getCurrentTopology().getRepNodeIds(dcid);
        Plan plan = new VerifyDataPlanV2(planName, this, null, allId,
                                       verifyOptions);

        register(plan);
        return admin.saveCreatePlan(plan);

    }

    /**
     * Creates a plan to verify data on all rns and admins that are deployed to
     * the specified zone or all zones.
     */
    public synchronized int
        createVerifyAllServicesPlan(String planName,
                                    DatacenterId dcid,
                                    VerificationOptions verifyOptions) {
        Set<AdminId> allAdminId = getAdmin().getCurrentParameters().
            getAdminIds(dcid, admin.getCurrentTopology());
        Set<RepNodeId> allRnId = admin.getCurrentTopology().getRepNodeIds(dcid);

        Plan plan = new VerifyDataPlanV2(planName, this, allAdminId,
                                         allRnId, verifyOptions);

        register(plan);
        return admin.saveCreatePlan(plan);

    }

    /**
     * Creates a plan to set limits on a table.
     */
    public Integer createTableLimitPlan(String planName,
                                        String namespace, String tableName,
                                        TableLimits newLimits) {
        final Plan plan = TablePlanGenerator.
                createSetTableLimitPlan(planName, this,
                                        namespace, tableName, newLimits);
        register(plan);
        return admin.saveCreatePlan(plan);
    }

    /** Creates a plan to retrieve and install TLS credentials. */
    public int createUpdateTlsCredentialsPlan(String planName,
                                              boolean retrieve,
                                              boolean install) {
        final Plan plan =
            new UpdateTlsCredentialsPlan(
                planName, this, admin.getCurrentTopology(), retrieve, install);
        register(plan);
        return admin.saveCreatePlan(plan);
    }
}
