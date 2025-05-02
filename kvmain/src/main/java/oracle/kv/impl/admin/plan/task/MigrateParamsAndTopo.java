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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.admin.topo.StorageDirectory;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Update parameters to disable all the services running on the target storage
 * node. This is purely a change to the admin database.We assume that the
 * target node is already down, and that there is no need to issue remote
 * requests to stop those services.
 *
 * Suppose we are migrating the services on {@literal SN1 -> SN20}, which
 * causes the topology to change from version {@literal 5 -> 6}, and suppose
 * that succeeds. The basic steps that occurred were:
 *
 *  1. create a new topo and params
 *  2. broadcast the topo changes
 *  3. ask the new SN to create the desired services.
 *
 * If we repeat this plan, the second plan execution will find that there are
 * no topology changes between what is desired and what is currently stored in
 * the admin db. We placidly accept this and continue nevertheless to do steps
 * 2 and 3, because we do not know whether the previous attempt was interrupted
 * between steps 1 and 2, or whether it succeeded. Because of that, if there
 * are no changes to the topology found, we will broadcast the entire topology
 * instead of just the delta.
 */
public class MigrateParamsAndTopo extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final StorageNodeId oldNode;
    private final StorageNodeId newNode;
    private final TopologyPlan plan;

    /*
     * Note that the params and topology should be updated as a single
     * transaction to avoid any inconsistencies in the admin db's view of
     * the store.
     */
    private final Set<RepNodeParams> changedRepNodeParams;
    private final Set<AdminParams> changedAdminParams;
    private Set<ArbNodeParams> changedArbNodeParams;

    public MigrateParamsAndTopo(TopologyPlan plan,
                                StorageNodeId oldNode,
                                StorageNodeId newNode) {

        super();
        this.oldNode = oldNode;
        this.newNode = newNode;
        this.plan = plan;

        changedAdminParams = new HashSet<>();
        changedRepNodeParams = new HashSet<>();
        changedArbNodeParams = new HashSet<>();
    }

    @Override
    protected TopologyPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {
        final Parameters parameters = plan.getAdmin().getCurrentParameters();
        final PortTracker portTracker =
            new PortTracker(plan.getTopology(), parameters, newNode);

        /* Modify pertinent params. */
        transferParamsToNewNode(parameters, portTracker);

        /* Modify pertinent topology */
        transferTopoToNewNode();

        /*
         * Save topo and params to the administrative db to preserve a
         * consistent view of the change. Note that if this plan has been
         * retried it's possible that the topology created by this plan
         * has already been saved.
         */
        if (plan.isFirstExecutionAttempt()) {
            plan.getAdmin().saveTopoAndParams(plan.getTopology(),
                                              plan.getDeployedInfo(),
                                              changedRepNodeParams,
                                              changedAdminParams,
                                              changedArbNodeParams,
                                              plan);
        }
        /* Send topology changes to all nodes.*/
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(),
                                             plan.getTopology(),
                                            "replace " + oldNode + " with " +
                                             newNode,
                                             plan.getAdmin().getParams().
                                                        getAdminParams(),
                                             plan)) {
            return State.INTERRUPTED;
        }

        return Task.State.SUCCEEDED;
    }

    /**
     * Find all RepNodeParams and AdminParams for services on the old node,
     * and update them to refer to the new node.
     */
    private void transferParamsToNewNode(Parameters parameters,
                                         PortTracker portTracker) {
        /*
         * Find all params that still refer to the old SN. Move them to the
         * new SN, and set their HA hostport..
         */

        final String newNodeHAHostname =
                parameters.get(newNode).getHAHostname();

        AdminId foundAdmin = null;
        for (AdminParams ap: parameters.getAdminParams()) {

            if (ap.getStorageNodeId().equals(oldNode)) {
                if (foundAdmin != null) {
                    /* Should only be one Admin on any SN. */
                    throw new NonfatalAssertionException
                        ("More than one admin service exists on " + oldNode +
                         ": " + foundAdmin + ", " + ap.getAdminId());
                }

                foundAdmin = ap.getAdminId();
                ap.setStorageNodeId(newNode);
                final int haPort = portTracker.getNextPort(newNode);

                // TODO: clean this up in the future so that setting the
                // ha hostnameport is consistent with the way other
                // fields are set.

                final String nodeHostPort = newNodeHAHostname + ":" + haPort;
                plan.getLogger().log(Level.INFO,
                                     "{0} transferring HA port for {1} " +
                                     "from {2} to {3}",
                                     new Object[]{this, foundAdmin,
                                                  ap.getNodeHostPort(),
                                                  nodeHostPort});
                ap.setJEInfo(nodeHostPort,
                             findAdminHelpers(parameters, ap.getAdminId()));
                changedAdminParams.add(ap);
            }
        }

        final StorageNodeParams snp = parameters.get(newNode);

        /* Recording the allocated directory information to avoid conflict */
        final Map<RepNodeId, StorageDirectory> assignedRNs = new TreeMap<>();

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {

            if (rnp.getStorageNodeId().equals(oldNode)) {

                final StorageDirectory sd =
                    Utils.findStorageDir(
                        snp,
                        rnp.getStorageDirectorySize(), assignedRNs);

                /* Fail the plan when not all the RNs are able to relocate */
                if (sd == null) {
                    throw new IllegalCommandException(
                        "Cannot find suitable storage dir for " +
                        rnp.getRepNodeId() + " to move from " + oldNode +
                        " to " + newNode);
                }

                assignedRNs.put(rnp.getRepNodeId(), sd);

                rnp.setStorageNodeId(newNode);
                rnp.setStorageDirectory(sd.getPath(), sd.getSize());

                final int haPort = portTracker.getNextPort(newNode);

                // TODO: clean this up in the future so that setting the
                // ha hostnameport is consistent with the way other
                // fields are set.

                final String nodeHostPort = newNodeHAHostname + ":" + haPort;
                plan.getLogger().log(Level.INFO,
                                     "{0} transferring HA port for {1} " +
                                     "from {2} to {3}",
                                     new Object[]{this, rnp.getRepNodeId(),
                                                  rnp.getJENodeHostPort(),
                                                  nodeHostPort});
                rnp.setJENodeHostPort(nodeHostPort);
                rnp.setJEHelperHosts
                   (findRNHelpers(parameters,rnp.getRepNodeId()));
                changedRepNodeParams.add(rnp);
            }
        }

        for (ArbNodeParams anp: parameters.getArbNodeParams()) {

            if (anp.getStorageNodeId().equals(oldNode)) {

                anp.setStorageNodeId(newNode);
                final int haPort = portTracker.getNextPort(newNode);

                // TODO: clean this up in the future so that setting the
                // ha hostnameport is consistent with the way other
                // fields are set.

                final String nodeHostPort = newNodeHAHostname + ":" + haPort;
                plan.getLogger().log(Level.INFO,
                                     "{0} transferring HA port for {1} " +
                                     "from {2} to {3}",
                                     new Object[]{this, anp.getArbNodeId(),
                                                  anp.getJENodeHostPort(),
                                                  nodeHostPort});
                anp.setJENodeHostPort(nodeHostPort);
                anp.setJEHelperHosts
                   (findRNHelpers(parameters, anp.getArbNodeId()));
                changedArbNodeParams.add(anp);
            }
        }
    }

    /**
     * Generate helper hosts by appending all the nodeHostPort values for all
     * other members of this HA repGroup.
     */
    private String findRNHelpers(Parameters parameters, ResourceId resId) {

        final Topology topo = plan.getTopology();
        RepNodeId rnId = null;
        ArbNodeId anId = null;
        final RepGroup rg;
        if (resId instanceof RepNodeId) {
            rnId = (RepNodeId)resId;
            rg = topo.get(topo.get(rnId).getRepGroupId());
        } else {
            anId = (ArbNodeId)resId;
            rg = topo.get(topo.get(anId).getRepGroupId());
        }

        final StringBuilder helperHosts = new StringBuilder();
        for (RepNode rn : rg.getRepNodes()) {
            if (rn.getResourceId().equals(rnId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }

            final RepNodeParams peerParams = parameters.get(rn.getResourceId());
            helperHosts.append(peerParams.getJENodeHostPort());
        }

        for (ArbNode an : rg.getArbNodes()) {
            if (an.getResourceId().equals(anId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }

            final ArbNodeParams peerParams = parameters.get(an.getResourceId());
            helperHosts.append(peerParams.getJENodeHostPort());
        }

        return helperHosts.toString();
    }

    /**
     * Generate helper hosts by appending all the nodeHostPort values for all
     * other members of this HA repGroup.
     */
    private String findAdminHelpers(Parameters parameters, AdminId adId) {

        final StringBuilder helperHosts = new StringBuilder();
        for (AdminParams otherParams : parameters.getAdminParams()) {
            if (otherParams.getAdminId().equals(adId)) {
                continue;
            }

            if (helperHosts.length() != 0) {
                helperHosts.append(",");
            }
            helperHosts.append(otherParams.getNodeHostPort());
        }

        return helperHosts.toString();
    }

    /**
     * Find all RepNodes and ArbNodes that refer to the old node, and update
     * the topology to refer to the new node. Push the topology changes to all
     * nodes in the system.
     */
    private void transferTopoToNewNode() {

        final Topology topo = plan.getTopology();

        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                if (rn.getStorageNodeId().equals(oldNode)) {
                    final RepNode updatedRN = new RepNode(newNode);
                    rg.update(rn.getResourceId(), updatedRN);
                }
            }
            for (ArbNode an : rg.getArbNodes()) {
                if (an.getStorageNodeId().equals(oldNode)) {
                    final ArbNode updatedAN = new ArbNode(newNode);
                    rg.update(an.getResourceId(), updatedAN);
                }
            }
        }
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, oldNode);
        LockUtils.lockSN(planner, plan, newNode);
        final Topology topo = plan.getTopology();
        for (RepNodeId rnid : topo.getHostedRepNodeIds(oldNode)) {
           LockUtils.lockANRN(planner, plan, rnid);
        }
        for (RepNodeId rnid : topo.getHostedRepNodeIds(newNode)) {
            LockUtils.lockANRN(planner, plan, rnid);
         }
        for (ArbNodeId anid : topo.getHostedArbNodeIds(oldNode)) {
            LockUtils.lockANRN(planner, plan, anid);
         }
         for (ArbNodeId anid : topo.getHostedArbNodeIds(newNode)) {
             LockUtils.lockANRN(planner, plan, anid);
          }
    }

    /**
     * Initialize the changedArbNodeParams if it is null because it was created
     * by a version from before the field was added.
     */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();
        if (changedArbNodeParams == null) {
            changedArbNodeParams = new HashSet<>();
        }
    }
}
