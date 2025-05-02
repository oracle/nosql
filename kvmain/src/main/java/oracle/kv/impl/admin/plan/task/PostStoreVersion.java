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

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.plan.AbstractPlan;

/**
 * Task to post the minimum agent version to the MR request table.
 *
 * NOTE: The semantics of the STORE_VERSION message has changed. Originally
 * this message contained the global store version. It is now the minimum
 * version that the agent must support. It may be lower than the global
 * store version.
 */
public class PostStoreVersion extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;

    private final KVVersion storeVersion;

    public PostStoreVersion(AbstractPlan plan, KVVersion storeVersion) {
        this.plan = plan;
        this.storeVersion = storeVersion;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public State doWork() throws Exception {
        try {
            getPlan().getAdmin().getMRTServiceManager().
                                           updateVersion(storeVersion);
        } catch (IllegalStateException ise) {
            getPlan().getLogger().warning("Unexpected exception posting store" +
                                          " version to MR table: " +
                                          ise.getMessage());
        }
        return Task.State.SUCCEEDED;
    }
}
