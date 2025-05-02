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

import oracle.kv.impl.admin.plan.AbstractPlan;

/**
 * Deprecated task. If executed from a saved plan, it will exit with
 * SUCCEEDED. System tables are now created via AddTable task.
 */
@Deprecated
public class CreateSystemTableTask extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final String tableName;

    private CreateSystemTableTask() {
        throw new IllegalStateException("No longer used");
    }

    @Override
    public State doWork() {
        /* Quietly exit. */
        return State.SUCCEEDED;
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
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(tableName);
    }
}
