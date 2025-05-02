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

import java.util.Set;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Task for creating and starting one or more RepNodes on a particular Storage
 * Node.
 * @deprecated at of R2
 */
@Deprecated
public abstract class DeployRepNode extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected final AbstractPlan plan;
    protected final StorageNodeId snId;
    protected final String snDescriptor;

    /**
     * Creates a task for creating and starting a new RepNode.
     * @deprecated
     */
    @Deprecated
    public DeployRepNode(AbstractPlan plan,
                         StorageNodeId snId) {
        super();
        this.plan = plan;
        this.snId = snId;

        /* A more descriptive label used for error messages, etc. */
        final StorageNodeParams snp =
                plan.getAdmin().getStorageNodeParams(snId);
        snDescriptor = snp.displaySNIdAndHost();
    }

    abstract Set<RepNodeId> getTargets();

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {
        throw new IllegalStateException("Deprecated as of R2");
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
