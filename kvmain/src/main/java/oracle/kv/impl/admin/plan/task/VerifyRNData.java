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

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.VerifyDataPlan;
import oracle.kv.impl.topo.RepNodeId;

/**
 * NOTE: This class is for backward compatibility only, it has been replaced
 * by VerifyRNDataV2.
 *
 */
@SuppressWarnings("unused")
public class VerifyRNData extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private VerifyDataPlan plan;
    private Set<RepNodeId> targetRnIds;
    private boolean verifyIndex;
    private boolean verifyRecord;
    private long btreeDelay;
    private long logDelay;
    private boolean verifyBtree;
    private boolean verifyLog;

    private VerifyRNData() {}

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public State doWork() throws Exception {
        throw new IllegalCommandException("This task has been deprecated. " +
            "Please upgrade the store and rerun the plan.");
    }

    @Override
    protected Plan getPlan() {
        return plan;
    }

}
