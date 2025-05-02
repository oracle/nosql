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
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.admin.topo.LogDirectory;
import oracle.kv.impl.admin.topo.StorageDirectory;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
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
 * Move a single RN to a new storage node.
 * 1. stop/disable RN
 * 2. change params and topo
 * 3. update the other members of the rep group.
 * 4. broadcast the topo changes
 * 5. turn off the disable bit and tell the new SN to deploy the RN
 * 6. wait for the new RN to come up and become consistent with the shard
 *    master, then delete the log files of the old RN.
 */
public class RelocateRN extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    private final RepNodeId rnId;
    private final StorageNodeId oldSN;
    private final StorageNodeId newSN;

    /*
     * Note that we now use "storage directory" instead of "mount point", but
     * since the field is serialized it would be a pain to change.
     */
    private final String newMountPoint;

    /*
     * If deserializing from an old version newStorageDirectorySize will
     * be 0. It is assumed that checks have already been made to prevent
     * a non-zero size before the Admins are upgraded.
     */
    private final long newStorageDirectorySize;

    /*
     * RN log directory
     */
    private final String newLogDirectory;

    /*
     * RN log directory size
     */
    private final long newLogDirectorySize;

    private final AbstractPlan plan;

    /* Hook to inject failures at different points in task execution */
    public static TestHook<Integer> FAULT_HOOK;

    public RelocateRN(AbstractPlan plan,
                      StorageNodeId oldSN,
                      StorageNodeId newSN,
                      RepNodeId rnId,
                      StorageDirectory newStorageDirectory,
                      LogDirectory newLogDir) {
        super();

        /*
         * This task does not support moving an RN within the same SN.
         * Additional checks would be needed to make sure the directories
         * are different. Also more safeguards should be added when deleting
         * the old RN.
         */
        if (oldSN.equals(newSN)) {
            throw new NonfatalAssertionException("The RelocateRN task does " +
                                                 "not support relocating to " +
                                                 "the same Storage Node");
        }
        this.oldSN = oldSN;
        this.newSN = newSN;
        this.plan = plan;
        this.rnId = rnId;
        if (newStorageDirectory == null) {
            newMountPoint = null;
            newStorageDirectorySize = 0L;
        } else {
            newMountPoint = newStorageDirectory.getPath();
            newStorageDirectorySize = newStorageDirectory.getSize();
        }
        if (newLogDir == null) {
            newLogDirectory = null;
            newLogDirectorySize = 0L;
        } else {
            newLogDirectory = newLogDir.getPath();
            newLogDirectorySize = newLogDir.getSize();
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
     * @throws NotBoundException
     * @throws RemoteException
     */
    private boolean checkAndRepairLocation()
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();
        final Logger logger = plan.getLogger();
        final TopologyCheck checker =
            new TopologyCheck(this.toString(), logger,
                              admin.getCurrentTopology(),
                              admin.getCurrentParameters());

        final StorageDirectory newStorageDir =
                   new StorageDirectory(newMountPoint, newStorageDirectorySize);

        /* ApplyRemedy will throw an exception if there is a problem */
        /*
         * TODO : Check if this check needs to be done for logdirectory
         * in case of RN relocation elasticity operation. If yes, then will be
         * done in follow up code drop.
         */
        Remedy remedy =
            checker.checkLocation(admin, newSN, rnId,
                                  false /* calledByDeployNewRN */,
                                  true /* makeRNEnabled */,
                                  oldSN, newStorageDir);
        if (!remedy.isOkay()) {
            logger.log(Level.INFO, "{0} check of newSN: {1}",
                       new Object[]{this, remedy});
        }

        final boolean newDone = checker.applyRemedy(remedy, plan);

        remedy = checker.checkLocation(admin, oldSN, rnId,
                                       false /* calledByDeployRN */,
                                       true /* makeRNEnabled */,
                                       oldSN, newStorageDir);
        if (!remedy.isOkay()) {
            logger.log(Level.INFO, "{0} check of oldSN: {1}",
                       new Object[]{this, remedy});
        }

        boolean oldDone = checker.applyRemedy(remedy, plan);
        return newDone && oldDone;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin admin = plan.getAdmin();
        final Logger logger = plan.getLogger();
        long stopRNTime;

        /*
         * Prevent the inadvertent downgrade of a RN version by checking
         * that the destination SN is a version that is >= source SN.
         */
        try {
            checkVersions();
        } catch (OperationFaultException e) {
            throw new CommandFaultException(e.getMessage(), e,
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

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
        RepNode rn = current.get(rnId);

        if (done && rn.getStorageNodeId().equals(newSN)) {
            /*
             * The check has been done, any small repairs needed were done, all
             * is consistent, and the RN is already living on the new
             * SN. Nothing more to be done with the topology and params.
             */
            logger.log(Level.INFO,
                       "{0} {1} is already on {2}, no additional metadata " +
                       "changes needed.",
                       new Object[]{this, rnId, newSN});
            stopRNTime = System.currentTimeMillis();
        } else {

            /*
             * There's work to do to update the topology, params, and JE HA
             * repGroupDB. Make sure both old and new SNs are up.
             */
        final LoginManager loginMgr = admin.getLoginManager();
            try {
                Utils.confirmSNStatus(current,
                                      loginMgr,
                                      logger,
                                      oldSN,
                                      true,
                                      "Please ensure that " + oldSN +
                                      " is deployed and running before " +
                                      "attempting a relocate " + rnId + ".");
                Utils.confirmSNStatus(current,
                                      loginMgr,
                                      logger,
                                      newSN,
                                      true,
                                      "Please ensure that " + newSN +
                                      " is deployed and running before " +
                                      "attempting a relocate " + rnId + ".");
            } catch (OperationFaultException e) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5200,
                                                CommandResult.NO_CLEANUP_JOBS);
            }

            final RepGroupId rgId = current.get(rnId).getRepGroupId();
            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 1);

            /* Step 1. Stop and disable the RN. */
            try {
                Utils.stopRN(plan, oldSN, rnId,
                        true, /* await for healthy */
                        false /* not failure */);
            } catch (Exception e) {
                throw new CommandFaultException(
                    e.getMessage(), e, ErrorMessage.NOSQL_5400,
                    CommandResult.TOPO_PLAN_REPAIR);
            }

            /*
             * Assert that the RN's disable bit is set, because the task cleanup
             * implementation uses that as an indication that step 5 executed.
             */
            final RepNodeParams rnp = admin.getRepNodeParams(rnId);
            if (!rnp.isDisabled()) {
                final String msg = "Expected disabled bit to be set " +
                    "for " + rnId  +  ": " + rnp;
                throw new CommandFaultException(msg,
                                                new IllegalStateException(msg),
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.TOPO_PLAN_REPAIR);
            }
            stopRNTime = System.currentTimeMillis();

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
                                      rnId, oldSN, newSN, plan);
            } catch (OperationFaultException e) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.TOPO_PLAN_REPAIR);
            }

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 4);

            /*
             * Step 4. Send topology change to all nodes, send param changes
             * with updated helper hosts to RN peers
             */
            final Topology topo = admin.getCurrentTopology();
            if (!Utils.broadcastTopoChangesToRNs
                (logger, topo,
                 "relocate " + rnId + " from " + oldSN + " to " + newSN,
                 admin.getParams().getAdminParams(), plan)) {

                /*
                 * The plan is interrupted before enough nodes saw the new
                 * topology.
                 */
                return State.INTERRUPTED;
            }

            /* Send the updated params to the RN's peers */
            Utils.refreshParamsOnPeers(plan, rnId);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 5);

            /*
             * Step 5. Remove the disable flag for this RN, and deploy the RN on
             * the new SN.
             */
            startRN(plan, newSN, rnId);

            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 6);
        }

        /*
         * Step 6: Destroy the old RN. Make sure the new RN is up and is current
         * with its master. The RNLocationCheck repair does not do this step,
         * so check if it's needed at this time.
         */
        return destroyRepNode(stopRNTime);
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

        final String errorMsg =
            " cannot be contacted. Please ensure that it " +
            "is deployed and running before attempting to deploy " +
            "this topology";

        KVVersion oldVersion = null;
        KVVersion newVersion = null;
        try {
            StorageNodeAgentAPI oldSNA = regUtils.getStorageNodeAgent(oldSN);
            oldVersion = oldSNA.ping().getKVVersion();
        } catch (RemoteException | NotBoundException e) {
            throw new OperationFaultException(oldSN + errorMsg);
        }

        try {
            StorageNodeAgentAPI newSNA = regUtils.getStorageNodeAgent(newSN);
            newVersion = newSNA.ping().getKVVersion();
        } catch (RemoteException | NotBoundException e) {
            throw new OperationFaultException(newSN + errorMsg);
        }

        if (VersionUtil.compareMinorVersion(oldVersion, newVersion) > 0) {
            throw new OperationFaultException
                (rnId + " cannot be moved from " +  oldSN + " to " + newSN +
                 " because " + oldSN + " is at version " + oldVersion +
                 " and " + newSN + " is at older version " + newVersion +
                 ". Please upgrade " + newSN +
                 " to a version that is equal or greater than " + oldVersion);
        }
    }

    /**
     * Deletes the old RN on the original SN. Returns SUCCESS if the delete was
     * successful. This method calls awaitCOnsistency() on the new node
     * to make sure it is up and healthy before deleting the old node.
     *
     * @return SUCCESS if the old RN was deleted
     */
    private State destroyRepNode(long stopRNTime) {
        try {
            if (Utils.destroyRepNode(plan, stopRNTime, oldSN, rnId)) {
                return State.SUCCEEDED;
            }
        } catch (InterruptedException ie) {
            return State.INTERRUPTED;
        }

        final String msg = "Time out while waiting for " + rnId +
            " to come up on " + newSN +
            " and become consistent with" +
            " the master of the shard before deleting" +
            " the RepNode from its old home on " +
            oldSN;
        throw new CommandFaultException(msg, new RuntimeException(msg),
                                        ErrorMessage.NOSQL_5400,
                                        CommandResult.TOPO_PLAN_REPAIR);
    }

    /**
     * Start the RN, update its params.
     *
     * @throws RemoteException if a communication error occurs
     * @throws NotBoundException if a service is not found
     */
    static public void startRN(AbstractPlan plan,
                               StorageNodeId targetSNId,
                               RepNodeId targetRNId)
        throws RemoteException, NotBoundException {

        final Admin admin = plan.getAdmin();

        /*
         * Update the SN after any AdminDB param changes are done. Refetch
         * the params and topo because they might have been updated.
         */
        final Topology topo = admin.getCurrentTopology();
        final RepNodeParams rnp =
                new RepNodeParams(admin.getRepNodeParams(targetRNId));
        if (rnp.isDisabled()) {
            rnp.setDisabled(false);
            admin.updateParams(rnp);
        }
        plan.getLogger().log(Level.INFO,
                             "{0} starting up {1} on {2} with  {3}",
                             new Object[]{plan, targetRNId, targetSNId, rnp});

        final RegistryUtils regUtils =
            new RegistryUtils(topo, admin.getLoginManager(), plan.getLogger());
        final StorageNodeAgentAPI sna =
                regUtils.getStorageNodeAgent(targetSNId);

        /*
         * Update the RN's configuration file if the RN is present, since
         * createRepNode only updates the parameters for a new node
         */
        final boolean rnExists = sna.repNodeExists(targetRNId);
        if (rnExists) {
            sna.newRepNodeParameters(rnp.getMap());
        }

        /* Start or create the RN */
        try {
            sna.createRepNode(rnp.getMap(), Utils.getMetadataSet(topo, plan));
        } catch (IllegalStateException e) {
            throw new CommandFaultException(e.getMessage(), e,
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * Refresh the repNodeAdmin parameters for an existing node in case it
         * was already running, since the start or create will be a no-op if
         * the RN was already up
         */
        if (rnExists) {
            try {
                Utils.waitForNodeState(plan, targetRNId,
                                          ServiceStatus.RUNNING);
            } catch (Exception e) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5400,
                                                CommandResult.TOPO_PLAN_REPAIR);
            }
            RepNodeAdminAPI rnAdmin = regUtils.getRepNodeAdmin(targetRNId);
            rnAdmin.newParameters();
        }

        /* Register this repNode with the monitor. */
        final StorageNode sn = topo.get(targetSNId);
        admin.getMonitor().registerAgent(sn.getHostname(),
                                         sn.getRegistryPort(),
                                         targetRNId);
    }

    /**
     * Update and persist the params and topo to make the RN refer to the new
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
                parameters.get(rnId).getStorageNodeId();
        final StorageNodeId origTopoSN = topo.get(rnId).getStorageNodeId();
        final ChangedParams changedParams =
            transferRNParams(parameters, portTracker, topo,
                             before, after, rgId);
        final boolean topoChanged = transferTopo(topo, before, after);

        /*
         * Sanity check that params and topo are in sync, both should be
         * either unchanged or changed
         */
        final Set<RepNodeParams> changedRNParams = changedParams.getRNP();
        final Set<ArbNodeParams> changedANParams = changedParams.getANP();
        if (!changedRNParams.isEmpty() != topoChanged) {
            final String msg =
                rnId + " params and topo out of sync. Original params SN=" +
                origParamsSN + ", orignal topo SN=" + origTopoSN +
                " source SN=" + before + " destination SN=" + after;
                throw new CommandFaultException(msg,
                                                new IllegalStateException(msg),
                                                ErrorMessage.NOSQL_5500,
                                                CommandResult.NO_CLEANUP_JOBS);
        }

        /* Only do the update if there has been a change */
        final Logger logger = plan.getLogger();
        if (!(topoChanged || !changedANParams.isEmpty())) {
            logger.log(Level.INFO,
                       "{0} no change to params or topology, no need to " +
                       "update in order to move {1} from {2} to {3}",
                       new Object[]{this, rnId, before, after});
            return;
        }

        plan.getAdmin().saveTopoAndParams(topo,
                                          plan.getDeployedInfo(),
                                          changedRNParams,
                                          Collections.<AdminParams>emptySet(),
                                          changedANParams,
                                          plan);
        logger.log(Level.INFO,
                   "{0} pdating params and topo for move of {1} from " +
                   "{2} to {3}: {4}",
                   new Object[]{this, rnId, before, after, changedRNParams});
    }

    /**
     * The params fields that have to be updated are:
     * For the RN that is to be moved:
     *   a. new JE HA nodehostport value
     *   b. new mount point
     *   c. new storage node id
     *   d. calculate JE cache size, which may change due to the capacity
     *      and memory values of the destination storage node.
     * For the other RNs in this shard:
     *   a. new helper host values, that point to this new location for our
     *      relocated RN
     */
    private ChangedParams transferRNParams(Parameters parameters,
                                                PortTracker portTracker,
                                                Topology topo,
                                                StorageNodeId before,
                                                StorageNodeId after,
                                                RepGroupId rgId) {

        final Set<RepNodeParams> changed = new HashSet<>();
        final Set<ArbNodeParams> changedArbp = new HashSet<>();

        final RepNodeParams rnp = parameters.get(rnId);
        final ParameterMap policyMap = parameters.copyPolicies();

        if (rnp.getStorageNodeId().equals(after)) {

            /*
             * We're done, this task ran previously. Note that this does not
             * notice if a RN is on the same SN, but its mount point has
             * changed. In R2, we deliberately do not yet support automatic
             * movement of RNs across mount points on the same SN; it's left
             * to the user to do manually.
             */
            plan.getLogger().log(Level.INFO,
                                 "{0} {1} already transferred to {2}",
                                 new Object[]{this, rnId, after});
            return new ChangedParams(changedArbp, changed);
        }

        /*
         * Sanity check -- this RNP should be pointing to the before SN, not
         * to some third party SN!
         */
        if (!rnp.getStorageNodeId().equals(before)) {
            final String msg =
                "Attempted to transfer " + rnId + " from " + before + " to " +
                after + " but unexpectedly found it residing on " +
                rnp.getStorageNodeId();
            throw new CommandFaultException(msg,
                                            new OperationFaultException(msg),
                                            ErrorMessage.NOSQL_5500,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * Change the SN, helper hosts, nodeHostPort, storage directory and
         * log directory for this RN
         */
        final int haPort = portTracker.getNextPort(after);

        final String newSNHAHostname = parameters.get(after).getHAHostname();
        final String oldNodeHostPort = rnp.getJENodeHostPort();
        final String nodeHostPort = newSNHAHostname + ":" + haPort;
        plan.getLogger().log(Level.INFO,
                             "{0} transferring HA port for {1} from {2} to {3}",
                             new Object[]{this, rnp.getRepNodeId(),
                                          oldNodeHostPort, nodeHostPort});

        rnp.setStorageNodeId(after);
        rnp.setJENodeHostPort(nodeHostPort);
        rnp.setStorageDirectory(newMountPoint, newStorageDirectorySize);
        rnp.setLogDirectory(newLogDirectory, newLogDirectorySize);

        /*
         * Setting the helper hosts is not strictly necessary, as it should
         * not have changed, but take this opportunity to update the helper
         * list in case a previous param change had been interrupted.
         */
        rnp.setJEHelperHosts(
            Utils.findHelpers(rnId, parameters, topo));

        /*
         * Update the RN heap, JE cache size, and parallelGCThreads params,
         * which are a function of the characteristics  of the hosting storage
         * node
         */
        final StorageNodeParams snp = parameters.get(after);
        Utils.setRNPHeapCacheGC(policyMap, snp, rnp, topo);
        changed.add(rnp);

        /* Change the helper hosts for other RNs in the group. */
        for (RepNode peer : topo.get(rgId).getRepNodes()) {
            final RepNodeId peerId = peer.getResourceId();
            if (peerId.equals(rnId)) {
                continue;
            }

            final RepNodeParams peerParam = parameters.get(peerId);
            final String oldHelper = peerParam.getJEHelperHosts();
            final String newHelpers = oldHelper.replace(oldNodeHostPort,
                                                  nodeHostPort);
            peerParam.setJEHelperHosts(newHelpers);
            changed.add(peerParam);
        }
        for (ArbNode peer : topo.get(rgId).getArbNodes()) {
            final ArbNodeId peerId = peer.getResourceId();
            final ArbNodeParams peerParam = parameters.get(peerId);
            final String oldHelper = peerParam.getJEHelperHosts();
            final String newHelpers = oldHelper.replace(oldNodeHostPort,
                                                        nodeHostPort);
            peerParam.setJEHelperHosts(newHelpers);
            changedArbp.add(peerParam);
        }
        return new ChangedParams(changedArbp, changed);
    }

    /**
     * Find all RepNodes that refer to the old node, and update the topology to
     * refer to the new node.
     * @return true if a change has been made, return false if the RN is already
     * on the new SN.
     */
    private boolean transferTopo(Topology topo, StorageNodeId before,
                                 StorageNodeId after) {

        final RepNode rn = topo.get(rnId);
        final StorageNodeId inUseSNId = rn.getStorageNodeId();
        if (inUseSNId.equals(before)) {
            final RepNode updatedRN = new RepNode(after);
            final RepGroup rg = topo.get(rn.getRepGroupId());
            rg.update(rn.getResourceId(),updatedRN);
            return true;
        }

        if (inUseSNId.equals(after)) {
            return false;
        }

        final String msg = rn + " expected to be on old SN " +
            before + " or new SN " + after + " but instead is on " + inUseSNId;
            throw new CommandFaultException(msg,
                                            new IllegalStateException(msg),
                                            ErrorMessage.NOSQL_5500,
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
     * @throws NotBoundException
     * @throws RemoteException
     */
    private void cleanupRelocation()
        throws RemoteException, NotBoundException {

        assert TestHookExecute.doHookIfSet(FAULT_HOOK, 7);

        final boolean done = checkAndRepairLocation();
        final Topology current = plan.getAdmin().getCurrentTopology();
        final RepNode rn = current.get(rnId);

        if (done) {
            if (rn.getStorageNodeId().equals(newSN)) {
                plan.getLogger().log(Level.INFO, "{0} cleanup, shard is " +
                                     " consistent, {1} is on the target {2}",
                                     new Object[]{this, rnId, newSN});

                /* attempt to delete the old RN */
                destroyRepNode(System.currentTimeMillis());
            }
            plan.getLogger().log(Level.INFO, "{0} cleanup, shard is " +
                                  "consistent, {1} is on {2}",
                               new Object[]{this, rnId, rn.getStorageNodeId()});
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
         * If step 5 occurred (enable bit on, RN pointing to new SN, then the HA
         * group and the params/topo are consistent, so attempt to delete the
         * old RN.
         */
        final RepNodeParams rnp = admin.getRepNodeParams(rnId);
        if ((rnp.getStorageNodeId().equals(newSN)) &&
            !rnp.isDisabled()) {
            return
                destroyRepNode(System.currentTimeMillis()) == State.SUCCEEDED;
        }

        /*
         * If the RepNodeParams still point at the old SN, steps 2 and 3 did
         * not occur, nothing to clean up
         */
        if (rnp.getStorageNodeId().equals(oldSN)) {
            /*
             * If the original RN was disabled, attempt to re-enable it. Note
             * that this may enable a node which was disabled before the plan
             * run.
             */
            if (rnp.isDisabled()) {
                Utils.startRN(plan, oldSN, rnId);
            }
            return true;
        }

        /*
         * We are somewhere between steps 1 and 5. Revert both of the kvstore
         * params and topo, and the JE HA update, and the peer RNs helper
         * hosts.
         */
        Topology topo = admin.getCurrentTopology();
        changeParamsAndTopo(newSN, oldSN, topo.get(rnId).getRepGroupId());
        Utils.refreshParamsOnPeers(plan, rnId);

        Utils.changeHAAddress(topo,
                              admin.getCurrentParameters(),
                              admin.getParams().getAdminParams(),
                              rnId, newSN, oldSN, plan);

        /* refresh the topo, it's been updated */
        topo = admin.getCurrentTopology();
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             topo,
                                            "revert relocation of  " + rnId +
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
        return super.getName(sb).append(" move ").append(rnId)
                  .append(" from ")
                  .append(snpOld != null ? snpOld.displaySNIdAndHost() : oldSN)
                  .append(" to ")
                  .append(snpNew != null ? snpNew.displaySNIdAndHost() : newSN);
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockRG(planner, plan, new RepGroupId(rnId.getGroupId()));
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
