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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.DeleteTopo;
import oracle.kv.impl.admin.plan.task.ParallelBundle;
import oracle.kv.impl.admin.plan.task.RepairShardQuorum;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

/**
 * Performs a failover by repairing shard quorum and deploying the specified
 * topology.
 */
public class FailoverPlan extends DeployTopoPlan {

    private static final long serialVersionUID = 1L;
    private static final String TOPO_CANDIDATE_NAME_PREFIX =
        TopologyCandidate.INTERNAL_NAME_PREFIX + "failover-plan-";

    private final Set<DatacenterId> allPrimaryZones;
    private final Set<DatacenterId> offlineZones;
    private volatile boolean isForce;

    private FailoverPlan(String planName,
                         Planner planner,
                         Topology current,
                         TopologyCandidate candidate,
                         Set<DatacenterId> allPrimaryZones,
                         Set<DatacenterId> offlineZones) {
        super(planName, planner, current, candidate);
        this.allPrimaryZones = allPrimaryZones;
        this.offlineZones = offlineZones;
        this.isForce = false;
    }

    /**
     * Creates a plan that will perform a failover, converting the specified
     * offline zones to secondary zones, and optionally converting secondary
     * zones to primary zones.
     *
     * @param planName a custom name for the plan, or null for the default
     * @param planner the planner to use to manage plans
     * @param current the current topology
     * @param newPrimaryZones IDs of any new primary zones
     * @param offlineZones IDs of offline zones
     * @return the created plan
     * @throws IllegalCommandException if any zone IDs are not found, if the
     * new primary zones and offline zones overlap, if the offline zones set is
     * empty, or if the specified zones and topology do not result in any
     * online primary zones
     */
    public static FailoverPlan create(String planName,
                                      Planner planner,
                                      Topology current,
                                      Set<DatacenterId> newPrimaryZones,
                                      Set<DatacenterId> offlineZones) {
        final Set<DatacenterId> allPrimaryZones =
            getAllPrimaryZones(current, newPrimaryZones, offlineZones);

        /*
         * There is a window where two failover plan names could end up with the
         * same postfix ID. There is also a possibility the the postfix could
         * be different than the plan ID. However this is highly unlikely in
         * practice and does not seem worth fixing.
         */
        final int canId = planner.getNextPlanId();
        final String candidateName = TOPO_CANDIDATE_NAME_PREFIX + canId;

        Admin admin = planner.getAdmin();
        admin.createFailoverTopology(candidateName,
                                     Parameters.DEFAULT_POOL_NAME,
                                     newPrimaryZones,
                                     offlineZones);
        final TopologyCandidate candidate = admin.getCandidate(candidateName);
        final FailoverPlan plan =
            new FailoverPlan(planName, planner, current, candidate,
                             allPrimaryZones, offlineZones);

        plan.generateTasks(current, candidate, null);

        return plan;
    }

    /* Return if this plan is executed with force flag */
    public boolean isForce() {
        return isForce;
    }

    /**
     * Returns a set of the IDs of the zones that should be primary zones after
     * performing the failover, including all current zones not included in the
     * offline zones, plus any secondary zones in newPrimaryZones.  Also
     * perform all checks on newPrimaryZones and offlineZones parameters.
     */
    private static Set<DatacenterId> getAllPrimaryZones(
        Topology current,
        Set<DatacenterId> newPrimaryZones,
        Set<DatacenterId> offlineZones)
        throws IllegalCommandException {

        final Set<DatacenterId> zonesNotFound = new HashSet<>();
        for (final DatacenterId dcId : newPrimaryZones) {
            if (current.get(dcId) == null) {
                zonesNotFound.add(dcId);
            }
        }
        for (final DatacenterId dcId : offlineZones) {
            if (current.get(dcId) == null) {
                zonesNotFound.add(dcId);
            }
        }
        if (!zonesNotFound.isEmpty()) {
            throw new IllegalCommandException(
                "Zones were not found: " + zonesNotFound);
        }
        if (!Collections.disjoint(newPrimaryZones, offlineZones)) {
            throw new IllegalCommandException(
                "The primary zones and offline zones must not overlap");
        }
        if (offlineZones.isEmpty()) {
            throw new IllegalCommandException(
                "The offline zones must not be empty");
        }
        final Set<DatacenterId> allPrimaryZones =
            new HashSet<>(newPrimaryZones);
        for (final Datacenter zone : current.getDatacenterMap().getAll()) {
            if (zone.getDatacenterType().isPrimary() &&
                !offlineZones.contains(zone.getResourceId())) {
                allPrimaryZones.add(zone.getResourceId());
            }
        }
        if (allPrimaryZones.isEmpty()) {
            throw new IllegalCommandException(
                "The options to the failover command did not result in any" +
                " online primary zones, but at least one online primary" +
                " zone is required");
        }
        return allPrimaryZones;
    }

    @Override
    protected void generateTasks(Topology current,
                                 TopologyCandidate candidate,
                                 RepGroupId failedShard) {

        /* failedShard is always null */
        assert failedShard == null;

        /* Add tasks first to repair quorum in all shards in parallel */
        final ParallelBundle tasks = new ParallelBundle();
        for (final RepGroupId rgId : current.getRepGroupMap().getAllIds()) {
            tasks.addTask(new RepairShardQuorum(this, rgId, allPrimaryZones,
                                                offlineZones));
        }
        addTask(tasks);
        super.generateTasks(current, candidate, failedShard);

        /* Remove the generated topology candidate */
        addTask(new DeleteTopo(this, candidateName));
    }

    @Override
    public String getDefaultName() {
        return "Failover";
    }

    @Override
    public void preExecuteCheck(boolean force, Logger executeLogger) {
        super.preExecuteCheck(force, executeLogger);

        /* Verify that all shards meet the initial requirements */
        final Admin admin = getAdmin();
        RepairShardQuorum.verify(this, allPrimaryZones, offlineZones,
                                 admin.getCurrentTopology(),
                                 admin.getCurrentParameters(),
                                 admin.getLoginManager(),
                                 executeLogger,
                                 force);
        isForce = force;
    }

    /** First remove the generated topology candidate. */
    @Override
    synchronized void requestCancellation() {
        try {
            getAdmin().deleteTopoCandidate(candidateName);
        } catch (IllegalCommandException e) {
            /* The candidate was not found -- OK */
        }
        super.requestCancellation();
    }

    @Override
    public Set<DatacenterId> getOfflineZones() {
        return offlineZones;
    }
}
