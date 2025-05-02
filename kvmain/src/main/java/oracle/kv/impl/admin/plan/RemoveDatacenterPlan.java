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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.task.ConfirmDatacenterStatus;
import oracle.kv.impl.admin.plan.task.RemoveDatacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Topology;

/**
 * Remove a datacenter. Only permitted for datacenters that are empty.
 */
public class RemoveDatacenterPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original inputs. */
    private final DatacenterId targetId;

    public RemoveDatacenterPlan(String planName,
                                Planner planner,
                                Topology topology,
                                DatacenterId targetId) {

        super(planName, planner, topology);

        if (targetId == null) {
            throw new IllegalCommandException("null targetId");
        }

        this.targetId = targetId;

        /* Confirm that the target datacenter exists. */
        validate();

        /* Confirm that the datacenter is empty. */
        final String infoMsg = "Cannot remove non-empty zone " +
            "[id=" + targetId +
            " name=" + topology.get(targetId).getName() + "].";

        addTask(new ConfirmDatacenterStatus(this, targetId, infoMsg));

        /* Remove the specified datacenter. */
        addTask(new RemoveDatacenter(this, targetId));
    }

    @Override
    public String getDefaultName() {
        return "Remove zone";
    }

    private void validate() {

        /* Confirm that the target datacenter exists in the topology. */
        final Topology topo = getTopology();

        if (topo.get(targetId) == null) {
            throw new IllegalCommandException
                ("Zone [id=" + targetId + "] does not exist in the " +
                 "topology and so cannot be removed");
        }
    }

    public DatacenterId getTarget() {
        return targetId;
    }
}
