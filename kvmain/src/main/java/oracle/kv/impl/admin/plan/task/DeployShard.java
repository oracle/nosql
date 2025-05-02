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

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;

/**
 * Add a new rep group (shard) to the topology. This is purely an update to the
 * topology stored in the administrative db and does not require any remote
 * calls.
 */
public class DeployShard extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected final DeployTopoPlan plan;
    private final int planShardIdx;

    /*
     * For debugging a description of the SNs that host the shard. This
     * is provided because the shard id only may not mean much to the user;
     * SNs host/ports and ids will have more significance.
     */
    private final String snDescList;

    /**
     */
    public DeployShard(DeployTopoPlan plan,
                       int planShardIdx,
                       String snDescList) {
        this.plan = plan;
        this.planShardIdx = planShardIdx;
        this.snDescList = snDescList;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Logger logger = plan.getLogger();

        final Admin plannerAdmin = plan.getAdmin();
        final Topology current = plannerAdmin.getCurrentTopology();

        /* The shard has been created from a previous invocation of the plan. */
        final RepGroupId shardId = plan.getShardId(planShardIdx);
        if (shardId != null) {
            RepGroup rg = current.get(shardId);
            if (rg != null) {
                logger.log(Level.INFO,
                           "{0} {1} was previously created.",
                           new Object[]{this, shardId});
                return State.SUCCEEDED;
            }

            /*
             * Clear the shard id, the plan was saved, but the current topology
             * doesn't have this shard.
             */
            plan.setNewShardId(planShardIdx, null);
            logger.log(Level.FINE,
                       "{0} {1} not present in topology, although saved "+
                       "in plan, reset and repeat creation",
                       new Object[]{this, shardId});
        }

        /*
         * Create the shard (RepGroup) and partitions and persist its presence
         * in the topology. Note that the plan associates the RepGroupId with a
         * shard index, and this mapping will be preserved when the plan is
         * saved after each task.
         */
        final RepGroup repGroup = new RepGroup();
        current.add(repGroup);
        plan.setNewShardId(planShardIdx, repGroup.getResourceId());
        plannerAdmin.saveTopo(current, plan.getDeployedInfo(), plan);
        return State.SUCCEEDED;
    }

    /**
     * If this task does not succeed, the following tasks of creating RNs
     * cannot continue.
     */
    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        final RepGroupId shardId = plan.getShardId(planShardIdx);
        if (shardId != null) {
            return super.getName(sb).append(" ").append(shardId)
                                    .append(" on ").append(snDescList);
        }
        return super.getName(sb).append(" on ").append(snDescList);
    }
}
