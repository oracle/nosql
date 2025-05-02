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

import static oracle.kv.KVVersion.CURRENT_VERSION;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;

/** Needed for compatibility with R3.3. */
public class ChangeAdminType extends SingleJobTask {

    static {
        assert CURRENT_VERSION.getMajor() < 5 : "Remove this class in R5";
    }

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final AdminId adminId;

    public ChangeAdminType(AbstractPlan plan, AdminId adminId) {
        super();
        this.plan = plan;
        this.adminId = adminId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork() throws Exception {
        return UpdateAdminParams.update(plan, this, adminId);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public boolean restartOnInterrupted() {
        return true;
    }
}
