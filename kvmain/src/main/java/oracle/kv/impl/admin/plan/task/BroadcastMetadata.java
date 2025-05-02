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

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;

/**
 * Broadcast the metadata on Admin to all RNs. This is typically done after any
 * change happens on the metadata on Admin, or the first time the topology is
 * deployed and each RN needs a new copy of the metadata.
 */
public class BroadcastMetadata <T extends Metadata<? extends MetadataInfo>>
                               extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    protected final AbstractPlan plan;

    protected final T md;

    public BroadcastMetadata(AbstractPlan plan, T md) {
        this.plan = plan;
        this.md = md;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public State doWork() throws Exception {
        if (md != null) {
            final Admin admin = plan.getAdmin();

            if (!Utils.broadcastMetadataChangesToRNs(
                plan.getLogger(), md, admin.getCurrentTopology(), toString(),
                admin.getParams().getAdminParams(), plan)) {

                return State.INTERRUPTED;
            }
        }
        return State.SUCCEEDED;
    }
}
