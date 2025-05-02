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

import java.util.List;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.plan.DeployTopoPlan;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.ErrorMessage;

/**
 * Add new partitions to the topology, only done for an initial
 * deployment. This is purely an update to the topology stored in the
 * administrative db.
 */
public class AddPartitions extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /**
     * For testing only.  Called in the doWork method before saving the
     * topology with the task object as an argument.
     */
    public static volatile TestHook<AddPartitions> beforeSaveTopoHook;

    protected final DeployTopoPlan plan;
    private final List<Integer> partitionCounts;
    private final int totalPartitions;

    /**
     * @param plan the owning plan
     * @param partitionCounts A list of the number of partitions per shard,
     * listed in ordinal order.
     * @param totalPartitions a count of the total number of partitions that
     * should be created. It's equals to the sum of all the values in the
     * partitionCounts list.
     */
    public AddPartitions(DeployTopoPlan plan,
                         List<Integer> partitionCounts,
                         int totalPartitions) {
        this.plan = plan;
        this.partitionCounts = partitionCounts;
        this.totalPartitions = totalPartitions;
    }

    @Override
    protected DeployTopoPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        final Admin plannerAdmin = plan.getAdmin();
        final Topology current = plannerAdmin.getCurrentTopology();

        /*
         * If this plan is being repeated, this task may have executed
         * successfully before.
         */
        final int numExistingPartitions = current.getPartitionMap().size();
        if (numExistingPartitions == totalPartitions) {
            plan.getLogger().log(Level.INFO,
                                "{0} partitions already created", this);
            return State.SUCCEEDED;
        }

        /*
         * At this point, we expect the current topology to have no partitions.
         * Having a non-zero number of partitions that is not the expected
         * value is unexpected, and means that a previous plan execution was
         * not correctly cleaned up.
         */
        if (numExistingPartitions != 0) {
            final String msg = "Trying to create " + totalPartitions +
                " but this topology unexpectedly already has " +
                numExistingPartitions + ". Store must be reinitialized";
            throw new CommandFaultException(msg, new IllegalStateException(msg),
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }

        /*
         * We expect the partition placements to mimic those from the
         * candidate precisely.
         */
        for (int whichShard = 0; whichShard < partitionCounts.size();
             whichShard++) {
            final int numPartitionsForShard = partitionCounts.get(whichShard);
            final RepGroupId rgId = plan.getShardId(whichShard);

            for (int i = 0; i < numPartitionsForShard; i++) {
                current.add(new Partition(current.get(rgId)));
            }
        }

        assert TestHookExecute.doHookIfSet(beforeSaveTopoHook, this);

        plannerAdmin.saveTopo(current, plan.getDeployedInfo(), plan);
        if (!Utils.broadcastTopoChangesToRNs(plan.getLogger(), current,
                                            "Initializing new partitions",
                                             plannerAdmin.getParams().
                                                        getAdminParams(),
                                             plan)) {
            return State.INTERRUPTED;
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" totalPartitions=")
                                .append(totalPartitions);
    }
}
