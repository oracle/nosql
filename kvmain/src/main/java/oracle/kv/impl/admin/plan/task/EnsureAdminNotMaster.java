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

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.AdminId;

import com.sleepycat.je.utilint.StoppableThread;

/**
 * A task to transfer mastership of the Admin.  If the task is running on the
 * given Admin, cause it to lose mastership.
 */
public class EnsureAdminNotMaster extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final AdminId adminId;

    public EnsureAdminNotMaster(AbstractPlan plan,
                                AdminId adminId) {
        super();
        this.plan = plan;
        this.adminId = adminId;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public State doWork()
        throws Exception {

        return ensure(plan, adminId);
    }

    public static State ensure(final AbstractPlan plan, AdminId adminId)
        throws Exception {

        final Admin admin = plan.getAdmin();

        /*
         * If we are running on the given Admin, the mastership needs to be
         * transferred away before we can do anything else.  If there are no
         * replicas, the enclosing plan cannot be completed.
         */
        if (adminId.equals
            (admin.getParams().getAdminParams().getAdminId())) {

            if (admin.getAdminCount() <= 1) {
                throw new IllegalStateException
                    ("Can't change Admin mastership if there are no replicas.");
            }

            /*
             * The thread will wait for the plan to end,
             * then proceed with the master transfer.
             */
            new StoppableThread("EnsureAdminNotMasterThread") {
                @Override
                public void run() {
                    admin.awaitPlan(plan.getId(), 10, TimeUnit.SECONDS);
                    admin.transferMaster();
                }
                @Override
                protected Logger getLogger() {
                    return plan.getLogger();
                }
            }.start();

            return State.INTERRUPTED;
        }

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public boolean restartOnInterrupted() {
        return true;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(adminId);
    }
}
