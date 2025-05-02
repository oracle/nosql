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

import java.util.HashSet;
import java.util.Set;

import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.TopologyPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Task for creating and starting all RepNodes which are housed on a particular
 * Storage Node.
 * @deprecated as of R2 in favor of DeployMultipleRNs
 */
@Deprecated
public class DeployAllRepNodesOnSN extends DeployRepNode {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a task for creating and starting a new RepNode.
     * @deprecated as of R2 in favor of DeployMultipleRNs
     */
    @Deprecated
    public DeployAllRepNodesOnSN(TopologyPlan plan,
                                 StorageNodeId snId) {
        super(plan, snId);
    }

    @Override
    Set<RepNodeId> getTargets() {
        Parameters parameters = plan.getAdmin().getCurrentParameters();
        Set<RepNodeId> targetSet = new HashSet<>();

        for (RepNodeParams rnp: parameters.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snId)) {
                targetSet.add(rnp.getRepNodeId());
            }
        }
        return targetSet;
    }
}
