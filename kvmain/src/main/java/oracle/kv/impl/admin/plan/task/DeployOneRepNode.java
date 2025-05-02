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

import java.util.Collections;
import java.util.Set;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Task for creating and starting a single RepNode on a particular Storage Node.
 * @deprecated as of R2 in favor of DeployNewRN
 */
@Deprecated
public class DeployOneRepNode extends DeployRepNode {

    private static final long serialVersionUID = 1L;

    private final RepNodeId rnId;

    /**
     * Creates a task for creating and starting a new RepNode.
     * @deprecated at of R2 in favor of DeployNewRN
     */
    @Deprecated
    public DeployOneRepNode(AbstractPlan plan,
                            StorageNodeId snId,
                            RepNodeId rnId) {
        super(plan, snId);
        this.rnId = rnId;
    }

    @Override
    Set<RepNodeId> getTargets() {
        return Collections.singleton(rnId);
    }

    /**
     * Return a more descriptive name which includes the target SN, so that the
     * user can relate errors to specific machines.
     */
    @Override
    public StringBuilder getName(StringBuilder sb) {
        return super.getName(sb).append(" of ").append(rnId)
                                .append(" on ").append(snDescriptor);
    }
}
