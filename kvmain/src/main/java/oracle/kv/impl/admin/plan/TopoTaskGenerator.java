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

import static oracle.kv.impl.topo.Datacenter.MASTER_AFFINITY;
import static oracle.kv.impl.topo.Datacenter.NO_MASTER_AFFINITY;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.plan.task.AddPartitions;
import oracle.kv.impl.admin.plan.task.BroadcastMetadata;
import oracle.kv.impl.admin.plan.task.BroadcastTopo;
import oracle.kv.impl.admin.plan.task.CheckRNMemorySettings;
import oracle.kv.impl.admin.plan.task.DeployNewARB;
import oracle.kv.impl.admin.plan.task.DeployNewRN;
import oracle.kv.impl.admin.plan.task.DeployShard;
import oracle.kv.impl.admin.plan.task.MigratePartition;
import oracle.kv.impl.admin.plan.task.NewArbNodeParameters;
import oracle.kv.impl.admin.plan.task.NewNthANParameters;
import oracle.kv.impl.admin.plan.task.NewNthRNParameters;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.RelocateAN;
import oracle.kv.impl.admin.plan.task.RelocateRN;
import oracle.kv.impl.admin.plan.task.RemoveAN;
import oracle.kv.impl.admin.plan.task.RemoveRepNode;
import oracle.kv.impl.admin.plan.task.RemoveShard;
import oracle.kv.impl.admin.plan.task.Task;
import oracle.kv.impl.admin.plan.task.UpdateAdminParams;
import oracle.kv.impl.admin.plan.task.UpdateDatacenter.UpdateDatacenterV2;
import oracle.kv.impl.admin.plan.task.UpdateHelperHostV2;
import oracle.kv.impl.admin.plan.task.UpdateNthANHelperHost;
import oracle.kv.impl.admin.plan.task.UpdateNthRNHelperHost;
import oracle.kv.impl.admin.plan.task.UpdateRepNodeParams;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.plan.task.WaitForAdminState;
import oracle.kv.impl.admin.plan.task.WaitForNodeState;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.admin.topo.TopologyDiff.RelocatedAN;
import oracle.kv.impl.admin.topo.TopologyDiff.RelocatedPartition;
import oracle.kv.impl.admin.topo.TopologyDiff.RelocatedRN;
import oracle.kv.impl.admin.topo.TopologyDiff.ShardChange;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

/**
 * Populate the target plan with the sequence of tasks and the new param
 * instances required to move from the current to a new target topology.
 */
class TopoTaskGenerator {

    /* This plan will be populated with tasks by the generator. */
    private final DeployTopoPlan plan;
    private final TopologyCandidate candidate;
    private final Logger logger;
    private final TopologyDiff diff;
    private final Set<DatacenterId> offlineZones;
    private final RepGroupId failedShard;

    TopoTaskGenerator(DeployTopoPlan plan,
                      Topology source,
                      TopologyCandidate candidate,
                      AdminServiceParams adminServiceParams,
                      Set<DatacenterId> offlineZones,
                      RepGroupId failedShard) {
        this.plan = plan;
        this.candidate = candidate;
        logger = LoggerUtils.getLogger(this.getClass(), adminServiceParams);
        diff = new TopologyDiff(source, null, candidate,
                                plan.getAdmin().getCurrentParameters(),
                                true /* validate */,
                                !offlineZones.isEmpty() /* forFailover */);
        logger.fine(() -> "task generator sees diff of " + diff.display(true));
        this.offlineZones = offlineZones;
        this.failedShard = failedShard;
    }

    /**
     * Compare the source and the candidate topologies, and generate tasks that
     * will make the required changes.
     *
     * Note that the new repGroupIds in the candidate topology cannot be taken
     * verbatim. The topology class maintains a sequence used to create ids for
     * topology components; one cannot specify the id value for a new
     * component. Since we want to provide some independence between the
     * candidate topology and the topology deployment -- we don't want to
     * require that components be created in precisely some order to mimic
     * the candidate -- we refer to the shards by ordinal value. That is,
     * we refer to the first, second, third, etc shard.
     *
     * The task generator puts the new shards into a list, and generates the
     * tasks with a plan shard index that is the order from that list. The
     * DeployTopoPlan will create and maintain a list of newly generated
     * repgroup/shard ids, and generated tasks will use their ordinal value,
     * as indicated by the planShardIdx and to find the true repgroup id
     * to use.
     */
    void generate() {

        /*
         * If -failed-shard option is specified then it is important to
         * check if it is same as -failed-shard option specified at
         * topology remove-shard before proceeding further.
         *
         * If removedShards from topology diff is greater than one or
         * empty then remove shard operation was not run before and
         * some other elasticity operation leading to removing of none
         * or two or more shards, we should not proceed in this scenario
         * with -failed-shard option specified.
         */
        if (failedShard != null) {
            Set<RepGroupId> removedShards = diff.getRemovedShards();
            if (removedShards.size() > 1 ||
                removedShards.isEmpty() ||
                !failedShard.equals(removedShards.iterator().next())) {

                final String msg =
                    "The shard specified with the -failed-shard option " +
                    "for plan deploy-topology (" + failedShard + ") " +
                    "does not match the shards specified for removal " +
                    "with topology remove-shard or topology contract (" +
                    (removedShards.isEmpty() ? "None" : removedShards) + ").";
                throw new CommandFaultException(
                    msg, new IllegalStateException(msg),
                    ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
        }

        makeDatacenterUpdates();

        if (diff.getRemovedShards().isEmpty()) {

            /*
             * Execute the RN relocations before creating new RNs, so that a
             * given SN does not become temporarily over capacity, or end up
             * housing two RNs in the same mount point. RNs relocation occurs
             * at this point only when no shard will be removed. The extra
             * available SNs are not from removed shards.
             */
            makeRelocatedRNTasks();
            makeRelocatedANTasks();
            makeCreateRNandARBTasks();
            makeRemoveANTasks();
            makeRemoveRNTasks();
        } else {
            makeCreateRNandARBTasks();
        }

        /*
         * Broadcast all of the above topo changes now so any migrations run
         * smoothly
         *
         * We need not broadcast topo changes to the RN's of failed shard
         */
        plan.addTask(new BroadcastTopo(plan, failedShard));

        /*
         * Load current security metadata, may be null if it has not been
         * initialized.
         */
        SecurityMetadata secMd = getSecurityMetadata();

        /*
         * Update security metadata with Kerberos configuration information if
         * store is secured and enabled Kerberos.
         */
        secMd = updateWithKerberosInfo(secMd);

        /*
         * Since all RNs are expected to be active now, broadcast the security
         * metadata to them, so that the login authentication is able to work.
         */
        plan.addTask(new BroadcastMetadata<>(plan, secMd));

        makePartitionTasks();

        /* Execute remove shards after the partitions migration task */
        if (makeRemoveShardTasks()) {
            /*
             * Relocate RNs at this point when there are removed shards. Because
             * removed shards release some SNs.
             */
            makeRelocatedRNTasks();
            makeRelocatedANTasks();
        }

        /* Broadcast topology At the end of deploy topo plan */
        plan.addTask(new BroadcastTopo(plan));
    }

    /*
     * Adds tasks related to changes in zones.
     */
    private void makeDatacenterUpdates() {
       /*
        * This method only checks for changes in primary and secondary zones.
        * If more zone types are created, this will have to change.
        */
        assert DatacenterType.VALUES_COUNT == 2;

        final Topology target = candidate.getTopology();

        /*
         * Do the primary zones first, then secondary to avoid leaving a store
         * with no primary zone.
         */
        for (Datacenter dc : target.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isPrimary()) {
                makeDatacenterUpdates(target, dc);
            }
        }
        for (Datacenter dc : target.getDatacenterMap().getAll()) {
            if (dc.getDatacenterType().isSecondary()) {
                makeDatacenterUpdates(target, dc);
            }
        }
    }

    private void makeDatacenterUpdates(Topology target, Datacenter dc) {
        final DatacenterId dcId = dc.getResourceId();

        /*
         * Add a task to check to see if the zone attributes are up to date.
         * Add the task unconditionally, so it's checked at plan execution
         * time.
         */
        plan.addTask(new UpdateDatacenterV2(plan, dcId, dc.getRepFactor(),
                                            dc.getDatacenterType(),
                                            dc.getAllowArbiters(),
                                            dc.getMasterAffinity()));
        /*
         * Set new value of je.rep.node.priority for RN when master affinity is
         * changed. The change has two steps:
         * 1. WriteNewParams task writes the new value for node priority
         * 2. NewRepNodeParameters task refresh the changed value for RepNode
         */
        if (diff.affinityChanged(dcId)) {
            boolean masterAffinity = dc.getMasterAffinity();
            Parameters params = plan.getAdmin().getCurrentParameters();
            final Topology current = plan.getAdmin().getCurrentTopology();
            Set<RepNodeId> rnIds = current.getRepNodeIds(dcId);

            /* Add tasks to change node priority for RepNodes */
            for (RepNodeId rnId : rnIds) {
                RepNodeParams rnp = params.get(rnId);
                ParameterMap pMap = rnp.getMap();

                if (masterAffinity) {
                    rnp.setJENodePriority(MASTER_AFFINITY);
                } else {
                    rnp.setJENodePriority(NO_MASTER_AFFINITY);
                }

                ParameterMap filtered = pMap.readOnlyFilter();
                StorageNodeId snId = current.get(rnId).getStorageNodeId();

                /*
                 * Add task to write the new value for parameter
                 * je.rep.node.priority
                 */
                plan.addTask(new WriteNewParams(plan, filtered, rnId, snId,
                                                false));

                /*
                 * Add task to refresh parameter je.rep.node.priority in RepNode
                 */
                plan.addTask(new NewRepNodeParameters(plan, rnId));
            }
        }

        /* If the type did not change we are done */
        if (!diff.typeChanged(dcId)) {
            return;
        }

        /* Propogate information about the zone type change */
        plan.addTask(new BroadcastTopo(plan));

        /*
         * If the zone type changed then add tasks to change and restart
         * the Admins and RNs in the zone.
         *
         * If the zone is offline, then only update the admin database, and
         * don't attempt to update its Admins or RNs.  Those changes should be
         * done using repair topology after the zone comes back online and
         * those services, and the SNA, are accessible again.
         */
        final boolean zoneIsOffline = offlineZones.contains(dc.getResourceId());

        /*
         * Add change Admin tasks. Must be done serially in case admins are
         * being restarted.
         */
        final Parameters parameters = plan.getAdmin().getCurrentParameters();
        for (AdminParams ap : parameters.getAdminParams()) {

            final StorageNode sn =
                target.getStorageNodeMap().get(ap.getStorageNodeId());

            if (sn == null) {
                final String msg =
                        "Inconsistency between the parameters and the " +
                        "topology, " + ap.getStorageNodeId() + " was not " +
                        "found in parameters";
                throw new CommandFaultException(
                    msg, new IllegalStateException(msg),
                    ErrorMessage.NOSQL_5400, CommandResult.TOPO_PLAN_REPAIR);
            }

            if (sn.getDatacenterId().equals(dcId)) {
                final AdminId adminId = ap.getAdminId();
                plan.addTask(new UpdateAdminParams(plan, adminId,
                                                   zoneIsOffline));
                /*
                 * If this is an online primary Admin, wait for it to
                 * restart before proceeding
                 */
                if (!zoneIsOffline && dc.getDatacenterType().isPrimary()) {
                    plan.addTask(
                        new WaitForAdminState(plan, ap.getStorageNodeId(),
                                              adminId, ServiceStatus.RUNNING));
                }
            }
        }

        /*
         * Add change RN tasks. We can safely change one RN per
         * shard in parallel. This is to avoid the newly switched over
         * nodes from becoming masters by outnumbering the existing
         * nodes in the quorum.
         */
        final int rf = dc.getRepFactor();
        final ParallelBundle[] updateTasks = new ParallelBundle[rf];
        final ParallelBundle[] waitTasks = new ParallelBundle[rf];
        for (int i = 0; i < rf; i++) {
            updateTasks[i] = new ParallelBundle();
            waitTasks[i] = new ParallelBundle();
        }

        /*
         * Use the current topology when mapping over RNs to update.  If an RN
         * is not deployed yet, then it doesn't need updating
         */
        final Topology currentTopo = plan.getAdmin().getCurrentTopology();
        for (RepGroup rg: currentTopo.getRepGroupMap().getAll()) {
            int i = 0;
            for (RepNode rn: rg.getRepNodes()) {
                final StorageNode sn =
                        rn.getStorageNodeId().getComponent(target);

                if (sn.getDatacenterId().equals(dcId)) {
                    final RepNodeId rnId = rn.getResourceId();
                    updateTasks[i].addTask(new UpdateRepNodeParams(plan, rnId,
                                                                zoneIsOffline,
                                                                false, false));

                    /*
                     * If this is an online primary node, add a task to wait
                     * for it to restart.
                     */
                    if (!zoneIsOffline && dc.getDatacenterType().isPrimary()) {
                        waitTasks[i].addTask(
                                new WaitForNodeState(plan, rnId,
                                                     ServiceStatus.RUNNING));
                    }
                    i++;
                }
            }
        }

        for (int i = 0; i < rf; i++) {
            plan.addTask(updateTasks[i]);
            plan.addTask(waitTasks[i]);
        }
    }

    private boolean makeRemoveShardTasks() {
        makeRemoveANTasks();

        /* Get the list of to be removed shards */
        Set<RepGroupId> removedShards = diff.getRemovedShards();
        /* Return false when no shard to be removed */
        if (removedShards.isEmpty()) {
            return false;
        }

        /* Firstly, remove all RepNodes under the to-be-removed shards */
        final Topology currentTopo = plan.getAdmin().getCurrentTopology();
        ParallelBundle bundle = new ParallelBundle();
        for (RepGroupId rgId : removedShards) {
            RepGroup rg = currentTopo.get(rgId);
            for (RepNode rn : rg.getRepNodes()) {
                Task t = new RemoveRepNode(plan, rn.getResourceId(),
                                           failedShard != null);
                bundle.addTask(t);
            }
        }
        plan.addTask(bundle);

        /* Add remove shard tasks for all to be removed shards */
        for (RepGroupId rgId : removedShards) {
            plan.addTask(new RemoveShard(plan, rgId));
            /* Broadcast topology as shards are removed */
            plan.addTask(new BroadcastTopo(plan));
        }
        return true;
    }

    /**
     * Create tasks to execute all the RN and AN creations.
     */
    private void makeCreateRNandARBTasks() {

        Topology target = candidate.getTopology();

        /* These are the brand new shards */
        List<RepGroupId> newShards = diff.getNewShards();
        for (int planShardIdx = 0;
             planShardIdx < newShards.size();
             planShardIdx++) {

            RepGroupId candidateShard = newShards.get(planShardIdx);
            ShardChange change = diff.getShardChange(candidateShard);
            String snSetDescription = change.getSNSetDescription(target);

            /* We wouldn't expect a brand new shard to host old RNs. */
            if (change.getRelocatedRNs().size() > 0) {
                final String msg =
                    "New shard " + candidateShard + " to be deployed on " +
                    snSetDescription + ", should not host existing RNs " +
                    change.getRelocatedRNs();
                throw new CommandFaultException(
                    msg, new IllegalStateException(msg),
                    ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }

            if (change.getRelocatedANs().size() > 0) {
                final String msg =
                    "New shard " + candidateShard + " to be deployed on " +
                    snSetDescription + ", should not host existing ANs " +
                    change.getRelocatedANs();
                throw new CommandFaultException(
                    msg, new IllegalStateException(msg),
                    ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }


            /* Make the shard. */
            plan.addTask(new DeployShard(plan,
                                         planShardIdx,
                                         snSetDescription));

            /* Make all the new RNs that will go on this new shard */

            /*
             * Create the first RN in a primary datacenter first, so it can be
             * the self-electing node and can act as the helper for the
             * remaining nodes, including any non-electable ones
             */
            final List<RepNodeId> newRnIds =
                new ArrayList<>(change.getNewRNs());
            for (final Iterator<RepNodeId> i = newRnIds.iterator();
                 i.hasNext(); ) {
                final RepNodeId rnId = i.next();
                final Datacenter dc = target.getDatacenter(rnId);
                if (dc.getDatacenterType().isPrimary()) {
                    i.remove();
                    newRnIds.add(0, rnId);
                    break;
                }
            }

            for (final RepNodeId proposedRNId : newRnIds) {
                RepNode rn = target.get(proposedRNId);
                plan.addTask(new DeployNewRN(plan,
                                             rn.getStorageNodeId(),
                                             planShardIdx,
                                             diff.getStorageDir(proposedRNId),
                                             diff.getRNLogDir(proposedRNId)));
            }

            /* Add new arbiter nodes. */
            for (final ArbNodeId proposedARBId : change.getNewANs()) {
                ArbNode arb = target.get(proposedARBId);
                plan.addTask(new DeployNewARB(plan,
                                             arb.getStorageNodeId(),
                                             planShardIdx));
            }

            /*
             * After the RNs have been created and stored in the topology
             * update their helper hosts.
             */
            for (int i = 0; i < change.getNewRNs().size(); i++) {
                plan.addTask(new UpdateNthRNHelperHost(plan, planShardIdx, i));
                plan.addTask(new NewNthRNParameters(plan, planShardIdx, i));
            }

            /*
             * After the ANs have been created and stored in the topology
             * update their helper hosts.
             */
            for (int i = 0; i < change.getNewANs().size(); i++) {
                plan.addTask(new UpdateNthANHelperHost(plan, planShardIdx, i));
                plan.addTask(new NewNthANParameters(plan, planShardIdx, i));
            }


        }

        /* These are the shards that existed before, but have new RNs */
        for (Map.Entry<RepGroupId, ShardChange> change :
             diff.getChangedShards().entrySet()) {

            RepGroupId rgId = change.getKey();
            if (newShards.contains(rgId)) {
                continue;
            }

            /* The removed shard has no new RN, so filter out it */
            RepGroup rg = target.get(rgId);
            if (rg == null) {
                continue;
            }

            /* Make all the new RNs that will go on this new shard */
            for (RepNodeId proposedRNId : change.getValue().getNewRNs()) {
                RepNode rn = target.get(proposedRNId);
                plan.addTask(new DeployNewRN(plan,
                                             rn.getStorageNodeId(),
                                             rgId,
                                             diff.getStorageDir(proposedRNId),
                                             diff.getRNLogDir(proposedRNId)));
            }

            /* Make all the new ANs that will go on this new shard */
            for (ArbNodeId proposedANId : change.getValue().getNewANs()) {
                ArbNode an = target.get(proposedANId);
                plan.addTask(new DeployNewARB(plan,
                                             an.getStorageNodeId(),
                                             rgId));
            }

            /*
             * After the new RNs have been created and stored in the topology
             * update the helper hosts for all the RNs in the shard, including
             * the ones that existed before.
             */
            makeUpdateHelperParamsTasks(rgId);
        }
    }


    /**
     * Create tasks to move an RN from one SN to another.
     * The relocation requires three actions that must seen atomic:
     *  1. updating kvstore metadata (topology, params(disable bit, helper
     *   hosts for the target RN and all other members of the HA group) and
     *   broadcast it to all members of the shard. This requires an Admin rep
     *   group quorum
     *  2. updating JE HA rep group metadata (groupDB) and share this with all
     *   members of the JE HA group. This requires a shard master and
     *   quorum. Since we are in the business of actively shutting down one of
     *   the members of the shard, this is clearly a moment of vulnerability
     *  3. Moving the environment data to the new SN
     *  4. Delete the old environment from the old SN
     *
     * Once (1) and (2) are done, the change is logically committed. (1) and
     * (2) can fail due to lack of write availability. The RN is unavailable
     * from (1) to the end of (3). There are a number of options that can short
     * these periods of unavailability, but none that can remove it entirely.
     *
     * One option for making the time from 1->3 shorter is to reach into the JE
     * network backup layer that is the foundation of NetworkRestore, in order
     * to reduce the amount of time used by (3) to transfer data to the new SN.
     * Another option that would make the period from 1-> 2 less
     * vulnerable to lack of quorum is to be able to do writes with a less than
     * quorum number of acks, which is a pending JE HA feature.
     *
     * Both options lessen but do not remove the unavailability periods and the
     * possibility that the changes must be reverse. RelocateRN embodies step
     * 1 and 2 and attempt to clean up if either step fails.
     */
    private void makeRelocatedRNTasks() {

        Set<StorageNodeId> sourceSNs = new HashSet<>();
        for (Map.Entry<RepGroupId, ShardChange> change :
             diff.getChangedShards().entrySet()) {

            for (RelocatedRN reloc : change.getValue().getRelocatedRNs()) {
                RepNodeId rnId = reloc.getRnId();
                StorageNodeId oldSNId = reloc.getOldSNId();
                StorageNodeId newSNId = reloc.getNewSNId();

                /*
                 * Stop the RN, update its params and topo, update the helper
                 * host param for all members of the group, update its HA group
                 * address, redeploy it on the new SN, and delete the RN from
                 * the original SN once the new RN has come up, and is
                 * consistent with the master. Also ask the original SN to
                 * delete the files from the environment.
                 */
                plan.addTask(new RelocateRN(plan, oldSNId, newSNId, rnId,
                                            diff.getStorageDir(rnId),
                                            diff.getRNLogDir(rnId)));
                sourceSNs.add(oldSNId);
            }

            /*
             * SNs that were previously over capacity and have lost an RN may
             * now be able to increase the per-RN memory settings. Check
             * all the source SNs.
             */
            for (StorageNodeId snId : sourceSNs) {
                plan.addTask(new CheckRNMemorySettings(plan, snId));
            }
        }
    }

    private void makeRelocatedANTasks() {

        for (Map.Entry<RepGroupId, ShardChange> change :
             diff.getChangedShards().entrySet()) {

            boolean addedTask = false;

            RepGroupId rgId = change.getKey();

            for (RelocatedAN reloc : change.getValue().getRelocatedANs()) {
                ArbNodeId anId = reloc.getArbId();
                StorageNodeId oldSNId = reloc.getOldSNId();
                StorageNodeId newSNId = reloc.getNewSNId();

                /*
                 * Stop the AN, update its params and topo, update the helper
                 * host parameters in the SN configuration for all members
                 * of the group, update its HA group
                 * address, redeploy it on the new SN, and delete the AN from
                 * the original SN once the new AN has come up.
                 * Also ask the original SN to delete the files from
                 * the environment.
                 */
                plan.addTask(new RelocateAN(plan, oldSNId, newSNId, anId));
                addedTask = true;
            }

            if (addedTask) {
                /*
                 * After the new ANs have been relocated the topology
                 * update the helper hosts for all the RNs in the shard.
                 */
                makeUpdateHelperParamsTasks(rgId);
            }
        }
    }

    private void makeRemoveANTasks() {

        for (Map.Entry<RepGroupId, ShardChange> change :
             diff.getChangedShards().entrySet()) {

            boolean addedTask = false;
            RepGroupId rgId = change.getKey();

            for (ArbNodeId anId : change.getValue().getRemovedANs()) {

                /*
                 * Stop the AN, update its params and topo, update the helper
                 * host parameters in the SN configuration for all members
                 * of the group, update its HA group address.
                 */
                plan.addTask(new RemoveAN(plan, anId));
                addedTask = true;
            }

            if (addedTask) {
                /*
                 * After the new ANs have been removed the topology
                 * update the helper hosts for all the RNs in the shard.
                 */
                makeUpdateHelperParamsTasks(rgId);
            }
        }
    }

    private void makeRemoveRNTasks() {
        for (final Entry<RepGroupId, ShardChange> entry :
                 diff.getChangedShards().entrySet())
        {
            final RepGroupId rgId = entry.getKey();
            final ShardChange change = entry.getValue();
            boolean addedTask = false;
            for (final RepNodeId rnId : change.getRemovedRNs()) {
                plan.addTask(new RemoveRepNode(plan, rnId, false));
                addedTask = true;
            }

            if (addedTask) {

                /*
                 * After RNs have been removed, update helper hosts for all RNs
                 * in the shard
                 */
                makeUpdateHelperParamsTasks(rgId);
            }
        }
    }

    /**
     * Partition related tasks. For a brand new deployment, add all the
     * partitions. For redistributions, generate one task per migrated
     * partition.
     */
    private void makePartitionTasks() {

        /* Brand new deployment -- only create new partitions, no migrations. */
        if (diff.getNumCreatedPartitions() > 0) {
            List<RepGroupId> newShards = diff.getNewShards();

            /*
             * If the topology build was not completed there may not yet be any
             * shards.
             */
            if (newShards.isEmpty()) {
                return;
            }
            List<Integer> partitionCount = new ArrayList<>(newShards.size());
            for (RepGroupId rgId : newShards) {
                ShardChange change = diff.getShardChange(rgId);
                int newParts = change.getNumNewPartitions();
                partitionCount.add(newParts);
            }

            plan.addTask(new AddPartitions(plan, partitionCount,
                                           diff.getNumCreatedPartitions()));
            return;
        }

        if (diff.getChangedShards().isEmpty()) {
            return;
        }

        /* A redistribution. Run all partition migrations in parallel. */
        final ParallelBundle bundle = new ParallelBundle();
        for (Map.Entry<RepGroupId,ShardChange> entry :
             diff.getChangedShards().entrySet()){

            RepGroupId targetRGId = entry.getKey();
            List<RelocatedPartition> pChanges =
                entry.getValue().getMigrations();
            for(RelocatedPartition pt : pChanges) {
                Task t = new MigratePartition(plan,
                                              pt.getSourceShard(),
                                              targetRGId,
                                              pt.getPartitionId(),
                                              failedShard);
                bundle.addTask(t);
            }
        }
        plan.addTask(bundle);
    }

    private void makeUpdateHelperParamsTasks(RepGroupId rgId)
    {
        Topology target = candidate.getTopology();

        /*
         * Filter out the removed shard, because no need to update parameters
         * for removed shard
         */
        if (target.get(rgId) == null) {
            return;
        }

        for (RepNode member : target.get(rgId).getRepNodes()) {
            RepNodeId rnId = member.getResourceId();
            plan.addTask(new UpdateHelperHostV2(plan, rnId, rgId));
            if (!offlineZones.contains(target.getDatacenterId(rnId))) {
                plan.addTask(new NewRepNodeParameters(plan, rnId));
            }
        }

        for (ArbNode member : target.get(rgId).getArbNodes()) {
            ArbNodeId anId = member.getResourceId();
            plan.addTask(new UpdateHelperHostV2(plan, anId, rgId));
            if (!offlineZones.contains(target.getDatacenterId(anId))) {
                plan.addTask(new NewArbNodeParameters(plan, anId));
            }
        }
    }

    /**
     * Return current security metadata stored on admin, may be null if security
     * metadata has not been initialized.
     */
    private SecurityMetadata getSecurityMetadata() {
        final Admin admin = plan.getAdmin();
        return admin.getMetadata(SecurityMetadata.class, MetadataType.SECURITY);
    }

    /**
     * Attempt to store Kerberos configuration information in security metadata
     * if current store enabled security and configured Kerberos as user
     * external authentication method.
     *
     * @return security metadata after updated Kerberos info.
     */
    private SecurityMetadata updateWithKerberosInfo(SecurityMetadata md) {
        final AdminServiceParams adminParams = plan.getAdmin().getParams();
        final SecurityParams securityParams = adminParams.getSecurityParams();

        if (!securityParams.isSecure()) {
            return md;
        }
        final GlobalParams globalParams = adminParams.getGlobalParams();
        if (SecurityUtils.hasKerberos(
                globalParams.getUserExternalAuthMethods())) {

            try {
                Utils.storeKerberosInfo(plan, md, logger);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Unexpected error occur while storing Kerberos " +
                    "principal in metadata: " + e.getMessage(),
                    e);
            }
        }

        return md;
    }
}
