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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.FailoverPlan;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * A task, and associated utility methods, for repairing quorum for a shard.
 */
public class RepairShardQuorum extends SingleJobTask {

    private static final long serialVersionUID = 1L;
    private static final long INVALID_VLSN = -1;

    private final AbstractPlan plan;
    private final RepGroupId rgId;
    private final Set<DatacenterId> allPrimaryZones;
    private final Set<DatacenterId> offlineZones;

    /**
     * Creates the task.  The primary and offline zones must not overlap and
     * must include all primary zones in the current topology.
     *
     * @param plan the associated plan
     * @param rgId the ID of the shard to repair
     * @param allPrimaryZones IDs of all primary zones
     * @param offlineZones IDs of offline zones
     */
    public RepairShardQuorum(AbstractPlan plan,
                             RepGroupId rgId,
                             Set<DatacenterId> allPrimaryZones,
                             Set<DatacenterId> offlineZones) {
        this.plan = plan;
        this.rgId = rgId;
        this.allPrimaryZones = allPrimaryZones;
        this.offlineZones = offlineZones;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork() throws Exception {
        if (!repairQuorum(plan, rgId, allPrimaryZones, offlineZones)) {
            return State.ERROR;
        }
        return State.SUCCEEDED;
    }

    /**
     * Verify that there are no active RNs in the specified offline zones, and
     * that a quorum of RNs is active in each shard in the specified primary
     * zones.  The primary and offline zones must not overlap and must include
     * all primary zones in the current topology.
     *
     * @param plan the running plan
     * @param allPrimaryZones the IDs of all proposed primary zones
     * @param offlineZones the IDs of all zones believed to be offline
     * @param topo the topology to use for the check
     * @param params the parameters to use for the check
     * @param loginManager the login manager
     * @param logger the logger to use for logging
     * @param force if false, the plan will check if the proposed topology
     * introduces data loss of secondary zones if they have more recent data.
     * If it does, the plan will not execute. If true, the plan will skip this
     * data loss check.
     * @throws IllegalCommandException if any RNs in the shard in offline zones
     * are found to be active, or if the number of active RNs per shard in
     * primary zones is less than the quorum
     */
    public static void verify(AbstractPlan plan,
                              Set<DatacenterId> allPrimaryZones,
                              Set<DatacenterId> offlineZones,
                              Topology topo,
                              Parameters params,
                              LoginManager loginManager,
                              Logger logger,
                              boolean force) {
        verify(plan, null, allPrimaryZones, offlineZones,
               getRNInfo(null, topo, loginManager, logger),
               topo, params, logger, force);
    }

    /**
     * Verify a single shard using the specified RNInfo, or all shards if
     * rgId is null.
     */
    private static void verify(AbstractPlan plan,
                               RepGroupId rgId,
                               Set<DatacenterId> allPrimaryZones,
                               Set<DatacenterId> offlineZones,
                               Map<RepNodeId, RNInfo> rnInfoMap,
                               Topology topo,
                               Parameters params,
                               Logger logger,
                               boolean force) {
        assert Collections.disjoint(allPrimaryZones, offlineZones);
        int primaryRepFactor = 0;
        int primaries = 0;
        int secondaries = 0;
        for (final Datacenter dc : topo.getDatacenterMap().getAll()) {
            final DatacenterId dcId = dc.getResourceId();
            assert !dc.getDatacenterType().isPrimary() ||
                offlineZones.contains(dcId) ||
                allPrimaryZones.contains(dcId)
                : "Existing primary zones should be specified in offline" +
                " or primary zones sets";
            if (allPrimaryZones.contains(dcId)) {
                primaryRepFactor += dc.getRepFactor();

                if (dc.getDatacenterType().isPrimary()) {
                    primaries++;
                }
            }
            if (dc.getDatacenterType().isSecondary() &&
                !offlineZones.contains(dc.getResourceId())) {
                secondaries++;
            }
        }

        /*
         * If current store has both of primary and secondary zones alive,
         * it's possible data loss would happen after repair, when the
         * secondary is ahead of primary nodes, because the new master will
         * be selected from an existing primary node, if any.
         */
        final boolean possibleDataLoss = (primaries > 0) && (secondaries > 0);
        final int quorum = (primaryRepFactor + 1) / 2;
        final StringBuilder errorMessage = new StringBuilder();
        final Collection<RepGroup> repGroups = (rgId != null) ?
            Collections.singleton(topo.get(rgId)) :
            topo.getRepGroupMap().getAll();

        /*
         * A map contains all secondary nodes are ahead of primary node
         * in each group, which will be used for data loss check.
         */
        final Map<RepNodeId, VLSNInfo> secondariesAhead = new HashMap<>();
        for (final RepGroup rg : repGroups) {
            int active = 0;

            /* Initialize this with invalid VLSN value -1 */
            long maxPrimaryVLSN = INVALID_VLSN;
            for (final RepNode rn : rg.getRepNodes()) {
                final RepNodeId rnId = rn.getResourceId();
                final DatacenterId dcId = topo.getDatacenterId(rnId);
                final RNInfo rnInfo = rnInfoMap.get(rnId);
                if (rnInfo == null) {
                    continue;
                }
                if (offlineZones.contains(dcId)) {
                    errorMessage.append("\n  Node ").append(rnId)
                                .append(" was found active in offline zone ")
                                .append(topo.get(dcId));
                }
                if (allPrimaryZones.contains(dcId)) {
                    active++;

                    /*
                     * Find maximum VLSN of all active primary node of this
                     * group. If proposed primary zones won't possibly
                     * introduce data loss, skip collecting info for
                     * verification.
                     */
                    if (possibleDataLoss) {
                        final NodeType nodeType = rnInfo.params.getNodeType();
                        if (nodeType.isElectable()) {
                            final long rnVLSN = getVLSN(plan, rnInfo);
                            if (rnVLSN > maxPrimaryVLSN) {
                                maxPrimaryVLSN = rnVLSN;
                            }
                        }
                    }
                }
            }

            if (active < quorum) {
                errorMessage.append("\n  Insufficient active nodes in shard ")
                            .append(rg.getResourceId())
                            .append(". Found ").append(active)
                            .append(" active RNs, but require ").append(quorum)
                            .append(" for quorum");
            }
            if (!force && possibleDataLoss) {

                /**
                 * Unable to find any valid VlSN from primary node, couldn't
                 * determine if data loss would happen after failover, warns
                 * users to check node health and retry the plan.
                 */
                if (maxPrimaryVLSN == INVALID_VLSN) {
                    errorMessage.append("\n  Unable to find sequence number ")
                                .append("information from active primary ")
                                .append("nodes in shard ")
                                .append(rg.getResourceId())
                                .append(". Check health of primary nodes in ")
                                .append("this shard then rerun this plan. ");
                } else {
                    /*
                     * Find secondary nodes are ahead of all active
                     * primary nodes by comparing their VLSN with
                     * the maximum VLSN of primary nodes in this group.
                     */
                    findSecondariesAhead(plan, offlineZones, topo,
                                         maxPrimaryVLSN, rg, secondariesAhead,
                                         rnInfoMap, errorMessage);
                }
            }
        }

        if (!force && possibleDataLoss) {
            verifyDataLoss(secondariesAhead, errorMessage);
        }

        /* Check for Admins still alive in the offline zone */
        final Map<AdminId, ServiceStatus> status =
                Utils.getAdminStatus(plan, params, logger);
        for (Entry<AdminId, ServiceStatus> adminStatus : status.entrySet()) {
            final AdminId adminId = adminStatus.getKey();
            final AdminParams adminParams = params.get(adminId);
            final StorageNodeId snId = adminParams.getStorageNodeId();
            final DatacenterId dcId = topo.getDatacenter(snId).getResourceId();
            if (offlineZones.contains(dcId) &&
                adminStatus.getValue().isAlive()) {
                errorMessage.append("\n  Admin ").append(adminId)
                            .append(" was found active in offline zone ")
                            .append(topo.get(dcId));
            }
        }
        if (errorMessage.length() > 0) {
            throw new IllegalCommandException(
                "Verification for failover failed:" + errorMessage);
        }
    }

    private static void verifyDataLoss(Map<RepNodeId, VLSNInfo> secondaries,
                                       StringBuilder errorMessage) {
        if (secondaries.size() == 0) {
            return;
        }

        /*
         * Found there are secondary nodes are ahead of primary nodes in
         * some shards. Warns users performing this plan would cause
         * data loss in these shards and suggests users to run network restore
         * if they want to retain the recent data in secondaries.
         */
        errorMessage.append("\n  Failover plan may result in data loss in ");
        final Set<String> groups = new HashSet<>();
        for (RepNodeId rnId : secondaries.keySet()) {
            groups.add(rnId.getGroupName());
        }
        errorMessage.append(groups.toString())
                    .append(".")
                    .append(" To continue with this plan, rerun it with the")
                    .append(" -force flag.")
                    .append(" To keep data available in secondary nodes,")
                    .append(" run network restore plan to restore recent data")
                    .append(" from secondary nodes to primary nodes.")
                    .append(" Then rerun this failover plan.");
        for (Entry<RepNodeId, VLSNInfo> entry : secondaries.entrySet()) {
            errorMessage.append("\n  In shard rg")
                        .append(entry.getKey().getGroupId())
                        .append(", secondary node ")
                        .append(entry.getKey())
                        .append(" is ahead of all active primary nodes")
                        .append(", sequence number delta = ")
                        .append(entry.getValue().getdelta())
                        .append(", the maximum vlsn of primary nodes = ")
                        .append(entry.getValue().maxPrimaryVLSN);
        }
    }

    /**
     * Repairs the shard quorum by updating the JE HA rep group membership to
     * match the currently available RNs in the specified primary zones,
     * setting an electable group size override on each RN to obtain quorum,
     * and converting nodes to primary nodes as needed.  The primary and
     * offline zones must not overlap and must include all primary zones in the
     * current topology.  If there are no existing primary nodes, establishes
     * one secondary as the initial master by resetting the JE replication
     * group, which updates the group membership and allows the other secondary
     * nodes to join.
     *
     * <p>First checks that a quorum of the RNs in the shard in the specified
     * primary zones is available, and that no RNs from the shard, or admins,
     * in the offline zones are available.
     *
     * <p>Then attempts to modify RN parameters and adjust the JE rep group
     * membership.  The command fails if any of those operations fail.
     *
     * @param plan the running plan
     * @param rgId the ID of the shard
     * @param allPrimaryZones the IDs of all zones that will be primary zones
     * @param offlineZones the IDs of offline zones
     * @return whether the operation succeeded
     * @throws IllegalCommandException if any RNs in the shard in offline zones
     * are found to be active, or if the number of active RNs per shard in
     * primary zones is less than the quorum
     * @throws IllegalStateException if the JE HA replication group membership
     * cannot be obtained
     */
    private static boolean repairQuorum(AbstractPlan plan,
                                        RepGroupId rgId,
                                        Set<DatacenterId> allPrimaryZones,
                                        Set<DatacenterId> offlineZones) {

        checkNull("rgId", rgId);

        /* Get information from the admin DB */
        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        final Map<RepNodeId, RNInfo> rnInfoMap =
            getRNInfo(rgId, topo, admin.getLoginManager(), plan.getLogger());

        final boolean force = ((FailoverPlan)plan).isForce();
        verify(plan, rgId, allPrimaryZones, offlineZones, rnInfoMap, topo,
               admin.getCurrentParameters(), plan.getLogger(), force);

        int existingPrimaries = 0;
        long maxPrimaryVLSN = INVALID_VLSN;
        for (final RNInfo rnInfo : rnInfoMap.values()) {
            if (rnInfo.params.getNodeType().isElectable()) {
                existingPrimaries++;
                final RepNodeId rnId = rnInfo.rn.getResourceId();
                final DatacenterId dcId = topo.getDatacenterId(rnId);

                if (allPrimaryZones.contains(dcId)) {
                    final long rnVLSN = getVLSN(plan, rnInfo);
                    if (rnVLSN > maxPrimaryVLSN) {
                        maxPrimaryVLSN = rnVLSN;
                    }
                }
            }
        }

        /*
         * Find secondary nodes are ahead of primary node having maximum VLSN,
         * add them to the map that contains secondary nodes will rollback
         * during repair and possibly encounter rollback limit error.
         */
        final Map<RepNodeId, VLSNInfo> rollbackSecondaries = new HashMap<>();
        final StringBuilder errorMessage = new StringBuilder();
        findSecondariesAhead(plan, offlineZones, topo, maxPrimaryVLSN,
                             topo.get(rgId), rollbackSecondaries, rnInfoMap,
                             errorMessage);
        if (errorMessage.length() > 0) {
            if (!force) {
                throw new IllegalCommandException(
                    "Acquiring secondary nodes VLSN info failed:" +
                    errorMessage.toString());
            }
            plan.getLogger().info("Repair shard quorum: " +
                                  errorMessage.toString());
        }

        /*
         * If there are no existing primary nodes, then establish the initial
         * master by using the first node in the set of nodes that should be
         * primary to reset the JE replication group.  This operation resets
         * the group membership, so there is no need to delete members
         * explicitly or set the electable group size override.
         */
        if (existingPrimaries == 0) {
            plan.getLogger().info(
                "Repair shard quorum: no existing primaries");
            boolean nextIsFirst = true;
            for (RNInfo rnInfo : rnInfoMap.values()) {
                final boolean first = nextIsFirst;
                nextIsFirst = false;
                final DatacenterId dcId = topo.getDatacenterId(rnInfo.rn);
                if (!allPrimaryZones.contains(dcId)) {
                    continue;
                }
                if (!repairNodeParams(plan, rgId, rnInfo, 0, first)) {
                    return false;
                }

                /*
                 * Clear the reset rep group flag for the first node, after
                 * getting its updated info
                 */
                if (first) {
                    rnInfo = getRNInfo(
                        rnInfo.rn.getResourceId(), topo,
                        new RegistryUtils(topo, admin.getLoginManager(),
                                          plan.getLogger()),
                        plan.getLogger());
                    if ((rnInfo == null) ||
                        !repairNodeParams(plan, rgId, rnInfo, 0, false)) {
                        return false;
                    }
                }
            }
            return true;
        }

        /*
         * There are secondary nodes found that may rollback during repair.
         * Check rollback limit of each node, if the limit is smaller than the
         * number of VLSN delta with maximum primary VLSN, change the limit of
         * this node to maximum value temporarily, so it won't fail because of
         * rollback limit error.
         */
        if (rollbackSecondaries.size() > 0) {
            /*
             * Suppress deprecation warning for TXN_ROLLBACK_LIMIT, which has
             * been deprecated since JE 25.3.7. Retain the logic to avoid
             * upgrade issue, setting this parameter would be a no-op in JE
             * 25.3.7 and later releases.
             */
            @SuppressWarnings("deprecation")
            final String jeParams =
                ReplicationConfig.TXN_ROLLBACK_LIMIT + "=" + Integer.MAX_VALUE;

            for (final RNInfo rnInfo : rnInfoMap.values()) {
                final RepNodeId rnId = rnInfo.rn.getResourceId();

                if (rnInfo.params.getNodeType().isSecondary()) {
                    final VLSNInfo vlsnInfo = rollbackSecondaries.get(rnId);
                    if (vlsnInfo == null) {
                        continue;
                    }

                    /*
                     * If detect the rollback error, change the limit to
                     * maximum value of this node.
                     */
                    if (detectRollbackLimitError(rnInfo, vlsnInfo.getdelta())) {
                        if (!changeNodeJEParams(plan, rgId, rnInfo, jeParams)) {
                            return false;
                        }
                    } else {
                        /*
                         * If this node won't encounter rollback limit error,
                         * remove it from the map of secondaries ahead.
                         */
                        rollbackSecondaries.remove(rnId);
                    }
                }
            }
        }

        /*
         * Update the electable group size override on existing primary nodes
         * to establish quorum.  Although we could use reset rep group to do
         * this, it is probably safer to do it by modifying the electable group
         * size override, since that allows the nodes to perform an election.
         */
        for (final RNInfo rnInfo : rnInfoMap.values()) {
            if (rnInfo.params.getNodeType().isElectable()) {
                if (!repairNodeParams(plan, rgId, rnInfo, existingPrimaries,
                                      false)) {
                    return false;
                }
            }
        }

        /*
         * Update the JE HA group membership information, if needed, to
         * remove nodes that are not in the requested primary set.
         */
        plan.getLogger().log(Level.INFO,
                             "{0} get JE rep group membership", plan);
        final Set<InetSocketAddress> primarySockets = new HashSet<>();
        final StringBuilder helperHosts = new StringBuilder();
        for (final RNInfo rnInfo : rnInfoMap.values()) {
            final String hostPort = rnInfo.params.getJENodeHostPort();
            final DatacenterId dcId = topo.getDatacenterId(rnInfo.rn);
            if (allPrimaryZones.contains(dcId)) {
                primarySockets.add(HostPortPair.getSocket(hostPort));
            }
            if (helperHosts.length() != 0) {
                helperHosts.append(ParameterUtils.HELPER_HOST_SEPARATOR);
            }
            helperHosts.append(hostPort);
        }

        final ReplicationGroupAdmin rga =
            admin.getReplicationGroupAdmin(rgId.getGroupName(),
                                           helperHosts.toString());
        final ReplicationGroup rg = Admin.getReplicationGroup(rga);
        final Set<ReplicationNode> electableNodes = rg.getElectableNodes();
        for (final ReplicationNode jeRN : electableNodes) {
            if (jeRN.getType() == NodeType.ARBITER) {
                continue;
            }
            if (!primarySockets.contains(jeRN.getSocketAddress())) {
                deleteMember(plan, rga, rgId, jeRN);
            }
        }

        /* Clear special parameters on the primary nodes */
        for (final RNInfo rnInfo : rnInfoMap.values()) {
            if (rnInfo.params.getNodeType().isElectable()) {
                if (!repairNodeParams(plan, rgId, rnInfo, 0, false)) {
                    return false;
                }
            }
        }

        /*
         * Convert any secondary nodes that should be primary nodes after
         * quorum has been established so that they can join the existing
         * group. If rollabck limit has been changed because of possible
         * data loss, revert JE parameters of secondary nodes.
         */
        if (existingPrimaries < rnInfoMap.size()) {
            for (final RNInfo rnInfo : rnInfoMap.values()) {
                if (rnInfo.params.getNodeType().isElectable()) {
                    continue;
                }

                final DatacenterId dcId = topo.getDatacenterId(rnInfo.rn);
                final RepNodeId rnId = rnInfo.rn.getResourceId();

                /*
                 * If this RN in the rollback secondary map, means
                 * its JE parameter has been changed previously to
                 * temporary value, revert to its original one.
                 */
                String jeParams = null;
                if (rollbackSecondaries.keySet().contains(rnId)) {
                    /*
                     * Wait this node consistent with current master,
                     * expect to revert the previous JE parameters after
                     * rollback finish.
                     */
                    Utils.awaitConsistent(plan, rnInfo.rn.getResourceId());
                    jeParams = rnInfo.params.getMap().
                       get(ParameterState.JE_MISC).asString();

                    /*
                     * If no JE parameters was set on this node,
                     * use the default value.
                     */
                    if (jeParams == null) {
                        jeParams = ParameterState.JE_MISC_DEFAULT;
                    }
                }

                if (!allPrimaryZones.contains(dcId)) {
                    if (!changeNodeJEParams(plan, rgId, rnInfo, jeParams)) {
                        return false;
                    }
                    continue;
                }

                if (!repairNodeParams(plan, rgId, rnInfo, 0, false, jeParams)) {
                    return false;
                }
            }
        }

        return true;
    }

    /** Stores information about an RN. */
    private static class RNInfo {
        final RepNode rn;
        final RepNodeAdminAPI rna;
        final RepNodeParams params;
        RNInfo(RepNode rn, RepNodeAdminAPI rna, LoadParameters params) {
            this.rn = rn;
            this.rna = rna;
            this.params = (params == null) ? null :
                new RepNodeParams(
                    params.getMapByType(ParameterState.REPNODE_TYPE));
        }
    }

    /**
     * Returns information about all active RNs in a shard, or in all shards if
     * rgId is null.
     */
    private static Map<RepNodeId, RNInfo> getRNInfo(RepGroupId rgId,
                                                    Topology topo,
                                                    LoginManager loginManager,
                                                    Logger logger) {
        final RegistryUtils regUtils =
            new RegistryUtils(topo, loginManager, logger);
        final Map<RepNodeId, RNInfo> rnInfoMap = new HashMap<>();
        final Collection<RepNodeId> repNodeIds = (rgId != null) ?
            topo.getSortedRepNodeIds(rgId) :
            topo.getRepNodeIds();
        for (final RepNodeId rnId : repNodeIds) {
            final RNInfo rnInfo = getRNInfo(rnId, topo, regUtils, logger);
            if (rnInfo != null) {
                rnInfoMap.put(rnId, rnInfo);
            }
        }
        return rnInfoMap;
    }

    /** Return information about a single RN, or null if not available. */
    private static RNInfo getRNInfo(RepNodeId rnId,
                                    Topology topo,
                                    RegistryUtils regUtils,
                                    Logger logger) {
        final RepNode rn = topo.get(rnId);
        try {
            final RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(rnId);
            final LoadParameters params = rna.getParams();
            return new RNInfo(rn, rna, params);
        } catch (RemoteException | NotBoundException re) {
            logger.info("Unable to reach " + rnId + ": " + re);
            return null;
        }
    }

    /**
     * Find secondary nodes are having larger VLSN than given maximum primary
     * VLSN in this group and put this node in given map.
     * @param plan this running plan
     * @param offlineZones the IDs of all zones believed to be offline
     * @param topo the topology to use for the check
     * @param maxPrimaryVLSN the maximum VLSN of primary nodes
     * @param repGroup RepGroup
     * @param secAhead a map store will store all secondary nodes ahead found
     * @param rnInfoMap map contains all RN information
     * @param errorMessage container to store the error messages.
     */
    private static void findSecondariesAhead(AbstractPlan plan,
                                             Set<DatacenterId> offlineZones,
                                             Topology topo,
                                             long maxPrimaryVLSN,
                                             RepGroup repGroup,
                                             Map<RepNodeId, VLSNInfo> secAhead,
                                             Map<RepNodeId, RNInfo> rnInfoMap,
                                             StringBuilder errorMessage) {
        for (final RepNode rn : repGroup.getRepNodes()) {
            final RepNodeId rnId = rn.getResourceId();
            final DatacenterId dcId = topo.getDatacenterId(rnId);
            final RNInfo rnInfo = rnInfoMap.get(rnId);

            if (rnInfo == null) {
                if (topo.get(dcId).getDatacenterType().isSecondary() &&
                    !offlineZones.contains(dcId)) {
                    errorFindingVLSN(rnId, errorMessage);
                }
                continue;
            }
            final NodeType nodeType = rnInfo.params.getNodeType();
            if (!nodeType.isSecondary()) {
                continue;
            }

            final long vlsn = getVLSN(plan, rnInfo);

            if (vlsn == INVALID_VLSN) {
                /**
                 * Unable to find the VLSN of a secondary node, couldn't
                 * determine if data loss would happen after failover, warns
                 * users to check node health and retry the plan.
                 */
                errorFindingVLSN(rnId, errorMessage);
                continue;
            }

            /*
             * If this node is secondary and having VLSN larger than
             * current maximum primary VLSN. It's possible the data
             * in this secondary node will lose after repair.
             */
            if (vlsn > maxPrimaryVLSN) {
                secAhead.put(rnId, new VLSNInfo(vlsn, maxPrimaryVLSN));
            }
        }
    }

    private static void errorFindingVLSN(RepNodeId rnId,
                                         StringBuilder errorMessage) {
        if (errorMessage != null) {
            errorMessage.append("\n  Unable to find sequence number ")
                        .append("information from ").append(rnId)
                        .append(". Check health of this node ")
                        .append("then rerun this plan. ");
        }
    }

    /**
     * Detect if given RN will encounter rollback limit error.
     *
     * Suppress deprecation warning for TXN_ROLLBACK_LIMIT, which has been
     * deprecated since JE 25.3.7. Retain the logic to avoid upgrade issue,
     * setting this parameter would be a no-op in JE 25.3.7 and later releases.
     */
    @SuppressWarnings("deprecation")
    private static boolean detectRollbackLimitError(RNInfo rnInfo,
                                                    long rollbackNum) {
        final RepNodeParams rnParams = rnInfo.params;
        int rollbackLimit =
            Integer.parseInt(RepParams.TXN_ROLLBACK_LIMIT.getDefault());
        final String jeParams =
            rnParams.getMap().get(ParameterState.JE_MISC).asString();

        /* If rollback limit is not set, use the default value */
        if (jeParams.indexOf(ReplicationConfig.TXN_ROLLBACK_LIMIT) != -1) {
            final Properties props = new Properties();
            try {
                props.load(ConfigUtils.getPropertiesStream(jeParams));
                final String value =
                    props.getProperty(ReplicationConfig.TXN_ROLLBACK_LIMIT);
                rollbackLimit = Integer.parseInt(value);
            } catch (Exception e) {
                /*
                 * ignore the failure, consider it would exceed
                 * the rollback limit for safety.
                 */
                return true;
            }
        }
        if (rollbackLimit < rollbackNum) {
            return true;
        }
        return false;
    }

    /* Stores VLSN of current node and the maximum primary VLSN */
    private static class VLSNInfo {
        final long vlsn;
        final long maxPrimaryVLSN;

        VLSNInfo(long vlsn, long maxPrimaryVLSN) {
            this.vlsn = vlsn;
            this.maxPrimaryVLSN = maxPrimaryVLSN;
        }

        long getdelta() {
            return (vlsn - maxPrimaryVLSN);
        }
    }

    private static boolean deleteMember(AbstractPlan plan,
                                        ReplicationGroupAdmin jeAdmin,
                                        RepGroupId rgId,
                                        ReplicationNode jeRN) {
        final String name = jeRN.getName();
        plan.getLogger().log(Level.INFO,
                             "{0} repair shard quorum: delete member: {1}",
                             new Object[]{plan, name});
        try {
            jeAdmin.deleteMember(name);
            return true;
        } catch (IllegalArgumentException iae) {
            /* Already a secondary, ignore */
            return true;
        } catch (UnknownMasterException ume) {
            logError(plan, rgId, "the master was not found");
            return false;
        } catch (MemberActiveException mae) {
            /* This is unlikely since the node was offline */
            logError(plan, rgId, jeRN + " is active");
            return false;
        } catch (MemberNotFoundException mnfe) {
            logError(plan, rgId, jeRN + " was not found");
            return false;
        } catch (MasterStateException mse) {
            logError(plan, rgId, jeRN + " is currently the master");
            return false;
        } catch (OperationFailureException ofe) {
            logError(plan, rgId, "unexpected exception: " + ofe);
            return false;
        }
    }

    private static void logError(AbstractPlan plan,
                                 RepGroupId rgId,
                                 String cause) {
        plan.getLogger().log(
            Level.INFO, "{0} couldn''t repair quorum for {1} because {2}",
            new Object[] {plan, rgId, cause });
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    /**
     * Update the node parameters as needed to make it a primary node,
     * and have the requested electable group size override and reset
     * rep group settings. Returns whether the update was successful.
     */
    private static boolean repairNodeParams(AbstractPlan plan,
                                            RepGroupId rgId,
                                            RNInfo rnInfo,
                                            int groupSizeOverride,
                                            boolean resetRepGroup) {
        return repairNodeParams(plan, rgId, rnInfo, groupSizeOverride,
                                resetRepGroup, null);
    }

    /**
     * Update the node parameters as needed to make it a primary node,
     * and have the requested electable group size override and reset
     * rep group settings as well as changing JE parameters. Returns
     * whether the update was successful.
     * @param jeParams if null, not changing JE parameters
     */
    private static boolean repairNodeParams(AbstractPlan plan,
                                            RepGroupId rgId,
                                            RNInfo rnInfo,
                                            int groupSizeOverride,
                                            boolean resetRepGroup,
                                            String jeParams) {
        final RepNodeId rnId = rnInfo.rn.getResourceId();
        plan.getLogger().info("Repair node params: " + rnId +
                              ", groupSizeOverride: " + groupSizeOverride +
                              ", resetRepGroup: " + resetRepGroup +
                              ((jeParams != null) ?
                              ", jeMiscParams: " + jeParams
                              : ""));
        if (rnInfo.rna == null) {
            logError(plan, rgId, rnId + " is not running");
            return false;
        }

        final StorageNodeId snId = rnInfo.rn.getStorageNodeId();
        final RepNodeParams rnParams = rnInfo.params;
        final boolean currentIsPrimary = rnParams.getNodeType().isElectable();
        final int currentGroupSizeOverride =
            rnParams.getElectableGroupSizeOverride();
        final boolean currentResetRepGroup = rnParams.getResetRepGroup();

        assert !resetRepGroup || !currentIsPrimary
            : "Only reset replication group for secondary node";

        /* Check if node is OK as is */
        if (currentIsPrimary &&
            (currentGroupSizeOverride == groupSizeOverride) &&
            (currentResetRepGroup == resetRepGroup) &&
            jeParams == null) {
            plan.getLogger().info("Repair node params: OK: " + rnId);
            return true;
        }

        rnParams.setNodeType(NodeType.ELECTABLE);
        rnParams.setElectableGroupSizeOverride(groupSizeOverride);
        rnParams.setResetRepGroup(resetRepGroup);

        /* If given JE parameters is null, not changing */
        if (jeParams != null) {
            final ParameterMap paramsMap = rnParams.getMap();
            paramsMap.setParameter(ParameterState.JE_MISC, jeParams);
        }

        try {
            WriteNewParams.writeNewParams(plan, rnParams.getMap(), rnId, snId,
                                          true /* checkRemoved */);

            /*
             * If current is primary and no JE parameters changes, no
             * need to restart this node.
             */
            if (currentIsPrimary && jeParams == null) {
                plan.getLogger().info("Repair node params: no restart: " +
                                      rnId);
                rnInfo.rna.newParameters();
                return true;
            }
            plan.getLogger().info("Repair node params: restart: " + rnId);

            /*
             * We do not wait for this or other nodes to be consistent,
             * stopRN(..., false), because this method is called when shard
             * quorum has been lost, so there is reason to think that there may
             * not be a master yet.  In particular, an election probably won't
             * work until we have set the electable group size override on
             * enough nodes that one of them ends up becoming the master.
             */
            Utils.stopRN(plan, snId, rnId,
                    false, /* not await for healthy */
                    false /* not failure */);
            Utils.startRN(plan, snId, rnId);
            Utils.waitForNodeState(plan, rnId, ServiceStatus.RUNNING);
            return true;
        } catch (Exception e) {
            plan.getLogger().log(
                Level.INFO,
                "Problem attempting to update the quorum for RN: " + rnId +
                ", SN ID: " + snId,
                e);
            return false;
        }
    }

    /**
     * Change JE parameters of given node, and also restart.
     * @param jeParams if null, not changing and restart.
     */
    private static boolean changeNodeJEParams(AbstractPlan plan,
                                              RepGroupId rgId,
                                              RNInfo rnInfo,
                                              String jeParams) {
        if (jeParams == null) {
            return true;
        }
        final RepNodeId rnId = rnInfo.rn.getResourceId();
        plan.getLogger().info("Change node JE params: " + rnId +
                              ", jeMiscParams: " + jeParams);
        if (rnInfo.rna == null) {
            logError(plan, rgId, rnId + " is not running");
            return false;
        }

        final StorageNodeId snId = rnInfo.rn.getStorageNodeId();
        final ParameterMap paramsMap = rnInfo.params.getMap().copy();
        paramsMap.setParameter(ParameterState.JE_MISC, jeParams);

        try {
            WriteNewParams.writeNewParams(plan, paramsMap, rnId, snId);
            plan.getLogger().info("Change node JE params: restart: " + rnId);
            Utils.stopRN(plan, snId, rnId,
                    false, /* not await for healthy */
                    false /* not failure */);
            Utils.startRN(plan, snId, rnId);
            Utils.waitForNodeState(plan, rnId, ServiceStatus.RUNNING);
            return true;
        } catch (Exception e) {
            plan.getLogger().log(
                Level.INFO,
                "Problem attempting to update JE params for RN: " + rnId +
                ", SN ID: " + snId,
                e);
            return false;
        }
    }

    private static long getVLSN(AbstractPlan plan, RNInfo rnInfo) {
        long vlsn = INVALID_VLSN;

        /* Retry five times to query VLSN of RN */
        for (int retry = 0; retry < 5; retry++) {
            try {
                final RepNodeStatus rnStatus = rnInfo.rna.ping();
                if (rnStatus != null) {
                    vlsn = rnStatus.getVlsn();
                }
                if (vlsn != INVALID_VLSN && vlsn != 0) {
                    break;
                }

                /*
                 * VLSN info of RN may not be available at this moment,
                 * wait a second and retry.
                 */
                Thread.sleep(1000);
                vlsn = INVALID_VLSN;
            } catch (RemoteException | InterruptedException e) {
                plan.getLogger().log(
                    Level.INFO, "Unable to get vlsn of " +
                    rnInfo.rn.getResourceId() + ": " + e);
            }
        }
        return vlsn;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(rgId);
    }
}
