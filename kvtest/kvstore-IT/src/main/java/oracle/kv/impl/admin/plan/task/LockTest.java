/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan.task;

import static oracle.kv.impl.admin.plan.task.LockUtils.lockTable;
import static oracle.kv.impl.admin.plan.task.LockUtils.lockRG;
import static oracle.kv.impl.admin.plan.task.LockUtils.lockIndex;
import static oracle.kv.impl.admin.plan.task.LockUtils.lockRN;
import static oracle.kv.impl.admin.plan.task.LockUtils.lockSN;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminTestBase;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.server.LoggerUtils;

import oracle.kv.util.TestUtils;
import org.junit.Test;

/**
 * Test task locks.
 */
public class LockTest extends AdminTestBase {

    private Planner planner;
    private Admin admin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        if (admin != null) {
            logger.info("Shutting down Admin from teardown");
            admin.shutdown(false, "test teardown");
        }
        /* Shutdown the bootstrap handler. */
        LoggerUtils.closeAllHandlers();
        super.tearDown();
    }

    /**
     * Tests table locking.
     */
    @Test
    public void testTableLocks() throws PlanLocksHeldException {
        setupPlanner();

        final String NAMESPACE1 = "NAMESPACE1";
        final String TABLE1 = "TABLE1";
        final String TABLE2 = "TABLE2";
        final String INDEX1 = "INDEX1";
        final String INDEX2 = "INDEX2";

        final Plan plan1 = new TestPlan("Plan1", planner);
        final Plan plan2 = new TestPlan("Plan2", planner);

        /* Plan1 exclusive lock on table1 */
        lockTable(planner, plan1, NAMESPACE1, TABLE1);
        /* Repeat */
        lockTable(planner, plan1, NAMESPACE1, TABLE1);
        /* Lock second table */
        lockTable(planner, plan1, NAMESPACE1, TABLE2);

        try {
            /* Should not be able to lock table from different plan. */
            lockTable(planner, plan2, NAMESPACE1, TABLE1);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }
        /* No namespace should be different table */
        lockTable(planner, plan2, "", TABLE1);

        /* Clear */
        planner.clearLocks(plan1.getId());
        planner.clearLocks(plan2.getId());

        /* Plan1 exclusive lock on table1.index1, read lock on table1 */
        lockIndex(planner, plan1, NAMESPACE1, TABLE1, INDEX1);
        /* Different plan, different index, should be good */
        lockIndex(planner, plan2, NAMESPACE1, TABLE1, INDEX2);

        /*
         * Both plan1 and plan 2 have read locks on table1. Each has exclusive
         * locks on their respective indexes.
         */

        try {
            /* Different plan, same index, bad */
            lockIndex(planner, plan2, NAMESPACE1, TABLE1, INDEX1);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        try {
            /* Different plan, locking table1, bad, plan1 has read lock */
            lockTable(planner, plan2, NAMESPACE1, TABLE1);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        try {
            /* Same plan, locking table1, bad because plan2 has read lock */
            lockTable(planner, plan1, NAMESPACE1, TABLE1);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        /* Clear plan2 read locks */
        planner.clearLocks(plan2.getId());

        /* Plan1 has read lock on table1 */

        /* Same plan, upgading table read lock to exclusive */
        lockTable(planner, plan1, NAMESPACE1, TABLE1);

        try {
            /*
             * Different plan, different index, bad, plan1 now holds
             * exclusive lock on the table
             */
            lockIndex(planner, plan2, NAMESPACE1, TABLE1, INDEX2);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }
    }

    /**
     * Tests elasticity and topo locks.
     */
    @Test
    public void testTopoLocks() throws PlanLocksHeldException {
        setupPlanner();

        final Plan plan1 = new TestPlan("Plan1", planner);
        final Plan plan2 = new TestPlan("Plan2", planner);

        /*
         * check high level elasticity lock
         */
        planner.lockElasticity(plan1.getId(), "foo");

        /* Should be able to lock again with the same plan */
        planner.lockElasticity(plan1.getId(), "foo");

        try {
            /* Should not be able to lock from different plan. */
            planner.lockElasticity(plan2.getId(), "foo");
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        /*
         * check shard locks
         */
        RepGroupId shard3 = new RepGroupId(3);
        lockRG(planner, plan1, shard3);

        /* Should be able to lock again */
        lockRG(planner, plan1, shard3);
        try {
            /* Should not be able to lock from different plan. */
            lockRG(planner, plan2, shard3);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        /* lock an RN in the shard */
        RepNodeId rnId = new RepNodeId(3, 1);
        lockRN(planner, plan1, rnId);
        /* repeat */
        lockRN(planner, plan1, rnId);

        /* can't lock with another plan */
        try {
            /* Should not be able to lock from different plan. */
            lockRN(planner, plan2, rnId);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        /*
         * check RN locks.
         */
        rnId = new RepNodeId(2, 1);
        lockRN(planner, plan1, rnId);
        /* repeat */
        lockRN(planner, plan1, rnId);
        try {
            /* Should not be able to lock from different plan. */
            lockRG(planner, plan2, new RepGroupId(2));
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }

        /* Clean all locks held by plan 1. */
        planner.clearLocks(plan1.getId());

        planner.lockElasticity(plan2.getId(), "foo");
        lockRG(planner, plan2, new RepGroupId(2));
        lockRN(planner, plan2, new RepNodeId(2, 1));

        /* Clean all locks held by plan 2. */
        planner.clearLocks(plan2.getId());

        final StorageNodeId snId = new StorageNodeId(1);
        lockSN(planner, plan1, snId);
        /* repeat */
        lockSN(planner, plan1, snId);
        try {
            /* Should not be able to lock from different plan. */
            lockSN(planner, plan2, snId);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
        }
    }

    /**
     * Tests the message from a PlanLocksHeldException. Simulates multiple
     * conflicting plans so that the exception is thrown and the message
     * is verified via regex.
     * @throws PlanLocksHeldException
     */
    @Test
    public void testPlanLocksHeldException() throws PlanLocksHeldException {
        final boolean runningAlone =
            isRunningAlone("testPlanLocksHeldException");

        final String singlePlanMsg =
            "is running, and is probably holding a " +
            "conflicting lock. Wait until that plan/command is " +
            "finished or interrupted then execute the new " +
            "plan/command again.";
        final String multiplePlansMsg =
            "are running, and are probably holding " +
            "conflicting locks. Wait until each plan/command is " +
            "finished or interrupted then execute the new " +
            "plan/command again.";

        setupPlanner();

        /*
         * creates a single locking conflict
         */
        planner.lockElasticity(1, "original-plan");

        try {
            /* Should not be able to lock from different plan. */
            planner.lockElasticity(2, "new-plan");
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
            TestUtils.checkException(expected, PlanLocksHeldException.class,
                                    "Couldn't execute Plan .* " +
                                    "because Plan .* " +
                                    singlePlanMsg);
            if (runningAlone) {
                expected.printStackTrace();
            }
        }
        planner.clearLocks(1);

        /*
         * creates a single locking conflict for plans without names
         */
        planner.lockElasticity(3, null);

        try {
            /* Should not be able to lock from different plan. */
            planner.lockElasticity(4, "");
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
            TestUtils.checkException(expected, PlanLocksHeldException.class,
                                    "Couldn't execute Plan \\d* " +
                                    "because Plan \\d* " +
                                    singlePlanMsg);
            if (runningAlone) {
                expected.printStackTrace();
            }
        }
        planner.clearLocks(3);

        /*
         * creates two locking conflicts via index and table locks
         */
        final String NAMESPACE1 = "NAMESPACE1";
        final String TABLE1 = "TABLE1";
        final String INDEX1 = "INDEX1";
        final String INDEX2 = "INDEX2";
        final String INDEX3 = "INDEX3";

        final Plan plan5 = new TestPlan("lock-table1-index1", planner);
        final Plan plan6 = new TestPlan("lock-table1-index2", planner);
        final Plan plan7 = new TestPlan("lock-table1", planner);

        /* plan5 gets readlock on table1, write lock on table1.index1 */
        lockIndex(planner, plan5, NAMESPACE1, TABLE1, INDEX1);
        /* plan6 gets readlock on table1, write lock on table1.index2, OK */
        lockIndex(planner, plan6, NAMESPACE1, TABLE1, INDEX2);

        try {
            /* plan6 tries to lock table, conflicts with writelocks */
            lockTable(planner, plan6, NAMESPACE1, TABLE1);
            fail("Shouldn't succeed");
        } catch (PlanLocksHeldException expected) {
            TestUtils.checkException(expected, PlanLocksHeldException.class,
                                    "Couldn't execute Plan .* " +
                                    "because Plan .* and Plan .* " +
                                    multiplePlansMsg);
            if (runningAlone) {
                expected.printStackTrace();
            }
        }

        /*
         * creates three locking conflicts via index and table locks
         */
        lockIndex(planner, plan7, NAMESPACE1, TABLE1, INDEX3);
        try {
            /* plan7 tries to lock table, conflicts with writelocks */
            lockTable(planner, plan7, NAMESPACE1, TABLE1);
        } catch (PlanLocksHeldException expected) {
            TestUtils.checkException(expected, PlanLocksHeldException.class,
                                    "Couldn't execute Plan .* " +
                                    "because Plan .*, Plan .* and Plan .* " +
                                    multiplePlansMsg);
            if (runningAlone) {
                expected.printStackTrace();
            }
        }
        planner.clearLocks(plan5.getId());
        planner.clearLocks(plan6.getId());
        planner.clearLocks(plan7.getId());

        /*
         * creates a locking conflict between a command and a plan
         */
        planner.lockElasticityForCommand("lock-elasticity-command");
        try {
            planner.lockElasticity(1, "lock-elasticity-plan");
        } catch (PlanLocksHeldException expected) {
            TestUtils.checkException(expected, PlanLocksHeldException.class,
                                    "Couldn't execute Plan .* " +
                                    "because.* command " +
                                    singlePlanMsg);
            if (runningAlone) {
                expected.printStackTrace();
            }
        }
    }

    private void setupPlanner() {
        admin = new Admin(atc.getParams());
        planner = admin.getPlanner();
    }

    private static class TestPlan extends AbstractPlan {
        private static final long serialVersionUID = 1L;

        TestPlan(String name, Planner planner) {
            super(name, planner);
        }

        @Override
        public String getDefaultName() {
            return getName();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return Collections.emptyList();
        }
    }
}
