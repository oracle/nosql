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

package oracle.kv.impl.admin.plan.task;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.TopologyCheck;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.FailoverPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

/**
 * Move a single AN to a new storage node.
 * 1. stop/disable AN
 * 2. change params and topo
 * 3. update the other members of the rep group.
 * 4. broadcast the topo changes
 * 5. turn off the disable bit and tell the new SN to deploy the AN
 * 6. wait for the new AN to come up then delete files of the old AN.
 */
public class RelocateAN extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    private final ArbNodeId anId;
    private final StorageNodeId oldSN;
    private final StorageNodeId newSN;
    private final AbstractPlan plan;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<Integer> FAULT_HOOK;

    public RelocateAN(AbstractPlan plan,
                      StorageNodeId oldSN,
                      StorageNodeId newSN,
                      ArbNodeId anId) {

        super();
        this.oldSN = oldSN;
        this.newSN = newSN;
        this.plan = plan;
        this.anId = anId;

        /*
         * This task does not support moving an AN within the same SN.  Also
         * more safeguards should be added when deleting the old AN.
         */
        if (oldSN.equals(newSN)) {
            throw new NonfatalAssertionException("The RelocateAN task does " +
                                                 "not support relocating to " +
                                                 "the same Storage Node");
        }
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    /**
     * Use the RNLocationCheck and the current state of the JE HA repGroupDB to
     * repair any inconsistencies between the AdminDB, the SNA config files,
     * and the JE HA repGroupDB.
     */
    private boolean checkAndRepairLocation() {

        final Admin admin = plan.getAdmin();
        final Logger logger = plan.getLogger();
        final TopologyCheck checker =
            new TopologyCheck(this.toString(), logger,
                              admin.getCurrentTopology(),
                              admin.getCurrentParameters());
        try {

            /* ApplyRemedy will throw an exception if there is a problem */
            Remedy remedy =
                checker.checkLocation(admin, newSN, anId,
                                      false /* calledByDeployNewRN */,
                                      true /* makeRNEnabled */,
                                      oldSN);
            if (!remedy.isOkay()) {
                logger.log(Level.INFO, "{0} check of new SN: {1}",
                           new Object[]{this, remedy});
            }

            final boolean newDone = checker.applyRemedy(remedy, plan);

            remedy =
                checker.checkLocation(admin, oldSN, anId,
                                      false /* calledByDeployNewRN */,
                                      true /* makeRNEnabled */,
                                      oldSN);
            if (!remedy.isOkay()) {
                logger.log(Level.INFO, "{0} check of old SN: {1}",
                           new Object[]{this, remedy});
            }

            final boolean oldDone = checker.applyRemedy(remedy, plan);

            return newDone && oldDone;
        } catch (Exception e) {
            /* Ignore errors */
            logger.log(Level.WARNING,"{0} unable to check/repair location "+
                       "due to exception: {1}",
                       new Object[]{this, e});
        }
        return false;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Set<DatacenterId> offlineZones;

        if (plan instanceof FailoverPlan) {
            FailoverPlan tmp = (FailoverPlan)plan;
            offlineZones = tmp.getOfflineZones();
        } else {
            offlineZones = Collections.emptySet();
        }

        /*
         * Prevent the inadvertent downgrade of a AN version by checking
         * that the destination SN is a version that is >= source SN.
         */
        checkVersions();

        /*
         * Before doing any work, make sure that the topology, params,
         * SN config files, and JE HA rep group are consistent. This is
         * most definitive if the JE HA repGroupDB can be read, which
         * is only possible if there is a master of the group. The correct
         * location can be deduced in some other limited cases too.
         */
        final boolean done = checkAndRepairLocation();

        /* Check the topology after any fixes */
        final Topology current = admin.getCurrentTopology();
        ArbNode an = current.get(anId);

        if (done && an.getStorageNodeId().equals(newSN)) {
            /*
             * The check has been done, any small repairs needed were done, all
             * is consistent, and the AN is already living on the new
             * SN. Nothing more to be done with the topology and params.
             */
            plan.getLogger().log(Level.FINE,
                                 "{0} {1} is already on {2}, no additional " +
                                 "metadata changes needed.",
                                  new Object[]{this, anId, newSN});
        } else {

            final RepGroupId rgId = current.get(anId).getRepGroupId();

            try {
                /*
                 * Relocation requires bringing down one AN in the shard.
                 * Before any work is done, check if the shard is going to be
                 * resilient enough. Does it currently have a master? If the
                 * target node is brought down, will the shard have a quorum?
                 * If at all possible, avoid changing admin metadata if it is
                 * very likely that the shard can't update its own JE HA group
                 * membership.
                 */
                HealthCheck.create(plan.getAdmin(), toString(), anId).await();
            } catch (OperationFaultException e) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.PLAN_CANCEL);
            }

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 1);

            /* Step 1. Stop and disable the AN. */
            try {
                Utils.disableAN(plan, oldSN, anId);
            } catch (Exception e) {
                throw new CommandFaultException(
                    e.getMessage(), e, ErrorMessage.NOSQL_5400,
                    CommandResult.TOPO_PLAN_REPAIR);
            }

            try {
                Utils.stopAN(plan,  oldSN,  anId);
            } catch (Exception e) {
                // Ignore if unable to contact SN
            }

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 2);

            /* Step 2. Change params and topo, as one transaction. */
            changeParamsAndTopo(oldSN, newSN, rgId);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 3);

            /*
             * Step 3. Tell the HA group about the new location of this
             * node. This requires a quorum to update the HA group db, and may
             * take some retrying, as step 1 might have actually shut down the
             * master of the HA group.
             */
            try {
                Utils.changeHAAddress(admin.getCurrentTopology(),
                                      admin.getCurrentParameters(),
                                      admin.getParams().getAdminParams(),
                                      anId, oldSN, newSN, plan);
            } catch (OperationFaultException e) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.TOPO_PLAN_REPAIR);
            }

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 4);

            /*
             * Step 4. Send topology change to RNs.
             */
            final Topology topo = admin.getCurrentTopology();
            if (!Utils.broadcastTopoChangesToRNs
                (plan.getLogger(), topo,
                 "relocate " + anId + " from " + oldSN + " to " + newSN,
                 admin.getParams().getAdminParams(),
                 plan, null, offlineZones)) {

                /*
                 * The plan is interrupted before enough nodes saw the new
                 * topology.
                 */
                return State.INTERRUPTED;
            }

            /* Send the updated params to the RN's peers */
            try {
                Utils.refreshParamsOnPeers(plan, anId);
            } catch (Exception e) {

                /*
                 * Could not update parameters for all RNs in the rep group.
                 * Let the SN or repair correct the inconsistency.
                 */
                plan.getLogger().log(Level.WARNING,
                                     "{0} changing {1} location but unable " +
                                     "to refresh parameters for all Group " +
                                     "members",
                                     new Object[]{this, anId});
            }
            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 5);

            /*
             * Step 5. Remove the disable flag for this AN, and deploy the AN on
             * the new SN.
             */
            createStartAN(plan, newSN, anId);
            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 6);
        }

        /*
         * Step 6: Destroy the old AN. Make sure the new AN is up and is current
         * with its master. The ANLocationCheck repair does not do this step,
         * so check if it's needed at this time.
         */
        destroyArbNode();
        return State.SUCCEEDED;
    }

    /**
     * Complain if the new SN is at an older version than the old SN.
     */
    private void checkVersions() {
        final Admin admin = plan.getAdmin();
        final RegistryUtils regUtils =
            new RegistryUtils(admin.getCurrentTopology(),
                              admin.getLoginManager(),
                              plan.getLogger());

        KVVersion oldVersion = null;
        KVVersion newVersion = null;
        try {
            final StorageNodeAgentAPI oldSNA =
                    regUtils.getStorageNodeAgent(oldSN);
            oldVersion = oldSNA.ping().getKVVersion();
        } catch (Exception e) {
            // Skip version check if problem contacting SN
            return;
        }

        try {
            final StorageNodeAgentAPI newSNA =
                    regUtils.getStorageNodeAgent(newSN);
            newVersion = newSNA.ping().getKVVersion();
        } catch (Exception e) {
            // Skip version check if problem contacting SN
            return;
        }

        if (VersionUtil.compareMinorVersion(oldVersion, newVersion) > 0) {
            throw new OperationFaultException
                (anId + " cannot be moved from " +  oldSN + " to " + newSN +
                 " because " + oldSN + " is at version " + oldVersion +
                 " and " + newSN + " is at older version " + newVersion +
                 ". Please upgrade " + newSN +
                 " to a version that is equal or greater than " + oldVersion);
        }
    }

    /**
     * Deletes the old AN on the original SN. Returns SUCCESS if the delete was
     * successful. This method calls awaitCOnsistency() on the new node
     * to make sure it is up and healthy before deleting the old node.
     *
     * @return SUCCESS if the old AN was deleted
     */
    private State destroyArbNode() {
        try {
            if (Utils.destroyArbNode(plan.getAdmin(), plan, oldSN, anId)) {
                return State.SUCCEEDED;
            }
            plan.getLogger().log(Level.WARNING,
                                 "{0} failed to remove {1} from {2} after " +
                                 "relocation to {3} due to timeout waiting " +
                                 "for SN",
                                 new Object[]{this, anId,
                                              oldSN, newSN});
        } catch (Exception e) {
            plan.getLogger().log(Level.WARNING,
                                 "{0} failed to remove {1} from {2} after " +
                                 "relocation to {3} due to exception {4}",
                                 new Object[]{this, anId,
                                              oldSN, newSN, e});
        }
        return State.ERROR;
    }

    /**
     * Reset the service disable flag, create/start
     * the AN.
     */
    static public void createStartAN(AbstractPlan plan,
                                     StorageNodeId targetSNId,
                                     ArbNodeId targetANId) {

        setDisable(plan, targetANId, false);
        try {
            createStartANInternal(plan, targetSNId, targetANId);
        } catch (Exception e) {

           /*
            * Just log message the AN was not started. Let the operation
            * continue.
            */
            plan.getLogger().log(Level.WARNING,
                                 "{0} error starting up {1} on {2}. " +
                                 "Exception {4}.",
                                 new Object[]{plan, targetANId,
                                              targetSNId, e});
        }
    }

    /**
     * Start/Create the AN, update its params.
     * @throws RemoteException
     * @throws NotBoundException
     */
    static private void createStartANInternal(AbstractPlan plan,
                                              StorageNodeId targetSNId,
                                              ArbNodeId targetANId)
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();

        /*
         * Update the SN after any AdminDB param changes are done. Refetch
         * the params and topo because they might have been updated.
         */
        final Topology topo = admin.getCurrentTopology();
        final ArbNodeParams anp =
            new ArbNodeParams(admin.getArbNodeParams(targetANId));

        plan.getLogger().log(Level.INFO,
                             "{0} starting up {0} on {1} with  {2}",
                             new Object[]{plan, targetANId,
                                          targetSNId, anp});

        final RegistryUtils regUtils =
            new RegistryUtils(topo, admin.getLoginManager(), plan.getLogger());
        final StorageNodeAgentAPI sna =
                regUtils.getStorageNodeAgent(targetSNId);

        /*
         * Update the AN's configuration file if the AN is present, since
         * createArbNode only updates the parameters for a new node
         */
        final boolean anExists = sna.arbNodeExists(targetANId);
        if (anExists) {
            sna.newArbNodeParameters(anp.getMap());
        }

        try {
            Utils.changeHAAddress(topo,
                                  admin.getCurrentParameters(),
                                  admin.getParams().getAdminParams(),
                                  targetANId, targetSNId,
                                  null, plan);
        } catch (Exception e) {
             throw new CommandFaultException(e.getMessage(), e,
                                             ErrorMessage.NOSQL_5200,
                                             CommandResult.NO_CLEANUP_JOBS);
        }
        /* Start or create the AN */
        try {
            sna.createArbNode(anp.getMap());
        } catch (IllegalStateException e) {
            throw new CommandFaultException(e.getMessage(), e,
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * Refresh the arbNodeAdmin parameters for an existing node in case it
         * was already running, since the start or create will be a no-op if
         * the AN was already up
         */
        if (anExists) {
            try {
                Utils.waitForNodeState(plan, targetANId, ServiceStatus.RUNNING);
            } catch (Exception e) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.TOPO_PLAN_REPAIR);
            }
            final ArbNodeAdminAPI anAdmin =
                    regUtils.getArbNodeAdmin(targetANId);
            anAdmin.newParameters();
        }

        /* Register this arbNode with the monitor. */
        StorageNode sn = topo.get(targetSNId);
        admin.getMonitor().registerAgent(sn.getHostname(),
                                         sn.getRegistryPort(),
                                         targetANId);
    }


    static private void setDisable(AbstractPlan plan,
                                   ArbNodeId targetANId,
                                   boolean disableFlag) {
        final Admin admin = plan.getAdmin();

        final ArbNodeParams anp =
            new ArbNodeParams(admin.getArbNodeParams(targetANId));
        if (disableFlag != anp.isDisabled()) {
            anp.setDisabled(disableFlag);
            admin.updateParams(anp);
        }
    }

    /**
     * Update and persist the params and topo to make the AN refer to the new
     * SN. Check to see if this has already occurred, to make the work
     * idempotent.
     */
    private void changeParamsAndTopo(StorageNodeId before,
                                     StorageNodeId after,
                                     RepGroupId rgId) {

        final Parameters parameters = plan.getAdmin().getCurrentParameters();
        final Topology topo = plan.getAdmin().getCurrentTopology();
        final PortTracker portTracker =
                new PortTracker(topo, parameters, after);

        /* Modify pertinent params and topo */
        final StorageNodeId origParamsSN =
                parameters.get(anId).getStorageNodeId();
        final StorageNodeId origTopoSN = topo.get(anId).getStorageNodeId();
        final ChangedParams changedParams = transferANParams
            (parameters, portTracker, topo, before, after, rgId);
        final Set<ArbNodeParams> changedANParams = changedParams.getANP();
        final Set<RepNodeParams> changedRNParams = changedParams.getRNP();
        final boolean topoChanged = transferTopo(topo, before, after);

        /*
         * Sanity check that params and topo are in sync, both should be
         * either unchanged or changed
         */
        if (!changedANParams.isEmpty() != topoChanged) {
            final String msg =
                anId + " params and topo out of sync. Original params SN=" +
                origParamsSN + ", orignal topo SN=" + origTopoSN +
                " source SN=" + before + " destination SN=" + after;
            throw new CommandFaultException(msg,
                                            new IllegalStateException(msg),
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        /* Only do the update if there has been a change */
        if (!(topoChanged || !changedRNParams.isEmpty())) {
            plan.getLogger().log(Level.FINE,
                                 "{0} no change to params or topology, no " +
                                 "need to update in order to move {1} " +
                                 "from {2} to {3}",
                                 new Object[]{this, anId, before, after});
            return;
        }

        plan.getAdmin().saveTopoAndParams(topo,
                                          plan.getDeployedInfo(),
                                          changedRNParams,
                                          Collections.<AdminParams>emptySet(),
                                          changedANParams,
                                          plan);
        plan.getLogger().log(Level.INFO,
                             "{0} updating params and topo for move of {1} " +
                             "from {2} to {3}: {4}",
                             new Object[]{this, anId,
                                          before, after, changedANParams});

    }

    /**
     * The params fields that have to be updated are:
     * For the AN that is to be moved:
     *   a. new JE HA nodehostport value
     *   b. new mount point
     *   c. new storage node id
     *   d. calculate JE cache size, which may change due to the capacity
     *      and memory values of the destination storage node.
     * For the other RNs in this shard:
     *   a. new helper host values, that point to this new location for our
     *      relocated AN
     */
    private ChangedParams transferANParams(Parameters parameters,
                                           PortTracker portTracker,
                                           Topology topo,
                                           StorageNodeId before,
                                           StorageNodeId after,
                                           RepGroupId rgId) {

        final Set<ArbNodeParams> changedAnp = new HashSet<>();
        final Set<RepNodeParams> changedRnp = new HashSet<>();

        final ArbNodeParams anp = parameters.get(anId);

        if (anp.getStorageNodeId().equals(after)) {

            /* We're done, this task ran previously. */
            plan.getLogger().log(Level.INFO,
                                "{0} {1} already transferred to {2}",
                                new Object[]{this, anId, after});
            return new ChangedParams(changedAnp, changedRnp);
        }

        /*
         * Sanity check -- this RNP should be pointing to the before SN, not
         * to some third party SN!
         */
        if (!anp.getStorageNodeId().equals(before)) {
            final String msg =
                "Attempted to transfer " + anId + " from " + before + " to " +
                after + " but unexpectedly found it residing on " +
                anp.getStorageNodeId();
                throw new CommandFaultException(msg,
                                                new IllegalStateException(msg),
                                                ErrorMessage.NOSQL_5200,
                                                CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * Change the SN, helper hosts, nodeHostPort
         */
        int haPort = portTracker.getNextPort(after);

        final String newSNHAHostname = parameters.get(after).getHAHostname();
        final String oldNodeHostPort = anp.getJENodeHostPort();
        final String nodeHostPort = newSNHAHostname + ":" + haPort;
        plan.getLogger().log(Level.INFO,
                             "{0} transferring HA port for {1} from {2} to {3}",
                             new Object[]{this, anp.getArbNodeId(),
                                          oldNodeHostPort, nodeHostPort});

        anp.setStorageNodeId(after);
        anp.setJENodeHostPort(nodeHostPort);

        /*
         * Setting the helper hosts is not strictly necessary, as it should
         * not have changed, but take this opportunity to update the helper
         * list in case a previous param change had been interrupted.
         */
        anp.setJEHelperHosts(
            Utils.findHelpers(anId, parameters, topo));

        changedAnp.add(anp);

        /* Change the helper hosts for other RNs in the group. */
        for (RepNode peer : topo.get(rgId).getRepNodes()) {
            RepNodeId peerId = peer.getResourceId();

            final RepNodeParams peerParam = parameters.get(peerId);
            final String oldHelper = peerParam.getJEHelperHosts();
            final String newHelpers = oldHelper.replace(oldNodeHostPort,
                                                        nodeHostPort);
            peerParam.setJEHelperHosts(newHelpers);
            changedRnp.add(peerParam);
        }
        return new ChangedParams(changedAnp, changedRnp);
    }

    /**
     * Find all RepNodes that refer to the old node, and update the topology to
     * refer to the new node.
     * @return true if a change has been made, return false if the AN is already
     * on the new SN.
     */
    private boolean transferTopo(Topology topo, StorageNodeId before,
                                 StorageNodeId after) {

        final ArbNode an = topo.get(anId);
        final StorageNodeId inUseSNId = an.getStorageNodeId();
        if (inUseSNId.equals(before)) {
            final ArbNode updatedAN = new ArbNode(after);
            final RepGroup rg = topo.get(an.getRepGroupId());
            rg.update(an.getResourceId(), updatedAN);
            return true;
        }

        if (inUseSNId.equals(after)) {
            return false;
        }

        final String msg =
            an + " expected to be on old SN " + before + " or new SN " + after +
            " but instead is on " + inUseSNId;
        throw new CommandFaultException(msg, new RuntimeException(msg),
                                        ErrorMessage.NOSQL_5200,
                                        CommandResult.NO_CLEANUP_JOBS);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public Runnable getCleanupJob() {
        return new Runnable() {
           @Override
               public void run() {
               try {
                   cleanupRelocation();
               } catch (Exception e) {
                   plan.getLogger().log
                       (Level.SEVERE,
                        "{0}: problem when cancelling relocation {1}",
                        new Object[] {this, LoggerUtils.getStackTrace(e)});

                   /*
                    * Don't try to continue with cleanup; a problem has
                    * occurred. Future, additional invocations of the plan
                    * will have to figure out the context and do cleanup.
                    */
                   throw new RuntimeException(e);
               }
           }
        };
    }

    /**
     * Do the minimum cleanup : when this task ends, check
     *  - the kvstore metadata as known by the admin (params, topo)
     *  - the configuration information, including helper hosts, as stored in
     *  the SN config file
     *  - the JE HA groupdb
     * and attempt to leave it all consistent. Do not necessarily try to revert
     * to the topology before the task.
     */
    private void cleanupRelocation() {

        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 7);

        final boolean done = checkAndRepairLocation();
        final Topology current = plan.getAdmin().getCurrentTopology();
        final ArbNode an = current.get(anId);

        if (done) {
            if (an.getStorageNodeId().equals(newSN)) {
                plan.getLogger().log(Level.INFO,
                                    "{0} cleanup, shard is consistent, {1} " +
                                    "is on the target {2}",
                                    new Object[]{this, anId, newSN});

                /* attempt to delete the old AN */
                destroyArbNode();
            }
            plan.getLogger().log(Level.INFO,
                                 "{0} cleanup, shard is " +
                                 "consistent, {1} is on {2}",
                               new Object[]{this, anId, an.getStorageNodeId()});
        } else {
            plan.getLogger().log(Level.INFO,
                                 "{0} cleanup, shard did not have " +
                                 "master, no cleanup attempted since " +
                                 "authoritative information is lacking", this);
        }
    }

    /**
     * This is the older style cleanup, which attempts to reason about how far
     * the task proceeded, and then attempts to revert to the previous state.
     */
    @SuppressWarnings("unused")
    private boolean checkLocationConsistency()
        throws InterruptedException, RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 7);

        /*
         * If step 5 occurred (enable bit on, AN pointing to new SN, then the HA
         * group and the params/topo are consistent, so attempt to delete the
         * old RN.
         */
        final ArbNodeParams anp = admin.getArbNodeParams(anId);
        if ((anp.getStorageNodeId().equals(newSN)) &&
            !anp.isDisabled()) {
            return destroyArbNode() == State.SUCCEEDED;
        }

        /*
         * If the ArbNodeParams still point at the old SN, steps 2 and 3 did
         * not occur, nothing to clean up
         */
        if (anp.getStorageNodeId().equals(oldSN)) {
            /*
             * If the original AN was disabled, attempt to re-enable it. Note
             * that this may enable a node which was disabled before the plan
             * run.
             */
            if (anp.isDisabled()) {
                Utils.startAN(plan, oldSN, anId);
            }
            return true;
        }

        /*
         * We are somewhere between steps 1 and 5. Revert both of the kvstore
         * params and topo, and the JE HA update, and the peer RNs helper
         * hosts.
         */
        Topology topo = admin.getCurrentTopology();
        changeParamsAndTopo(newSN, oldSN, topo.get(anId).getRepGroupId());
        Utils.refreshParamsOnPeers(plan, anId);

        Utils.changeHAAddress(topo,
                              admin.getCurrentParameters(),
                              admin.getParams().getAdminParams(),
                              anId, newSN, oldSN, plan);

        /* refresh the topo, it's been updated */
        topo = admin.getCurrentTopology();
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             topo,
                                            "revert relocation of  " + anId +
                                             " and move back from " +
                                             newSN + " to " + oldSN,
                                             admin.getParams().getAdminParams(),
                                             plan)) {
            /*
             * The plan is interrupted before enough nodes saw the new
             * topology.
             */
            return false;
        }

        return true;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        final StorageNodeParams snpOld =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(oldSN) : null);
        final StorageNodeParams snpNew =
            (plan.getAdmin() != null ?
             plan.getAdmin().getStorageNodeParams(newSN) : null);
        return super.getName(sb).append(" move ").append(anId)
                  .append(" from ")
                  .append(snpOld != null ? snpOld.displaySNIdAndHost() : oldSN)
                  .append(" to ")
                  .append(snpNew != null ? snpNew.displaySNIdAndHost() : newSN);
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockRG(planner, plan, new RepGroupId(anId.getGroupId()));
        LockUtils.lockSN(planner, plan, oldSN);
        LockUtils.lockSN(planner, plan, newSN);
    }

    class ChangedParams {
        private final Set<ArbNodeParams> anParams;
        private final Set<RepNodeParams> rnParams;
        ChangedParams(Set<ArbNodeParams> anp, Set<RepNodeParams> rnp) {
            anParams = anp;
            rnParams = rnp;
        }
        Set<ArbNodeParams> getANP() {
            return anParams;
        }
        Set<RepNodeParams> getRNP() {
            return rnParams;
        }
    }
}
