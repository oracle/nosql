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

package oracle.kv.impl.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin.RunTransaction;
import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.admin.AdminStores.AdminStoreCursor;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.ShutdownThread;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

/**
 * A base class for implementations of the plan store in Admin.
 */
public class PlanStore extends AdminStore {

    private static enum PRUNE_TYPE {
        SYSTEM,
        TERMINAL,
    }

    /* Default plan limit */
    private static final int DEFAULT_PLAN_LIMIT = 1000;

    /*
     * Maximin number of plans returned from the multi-plan get methods.
     */
    private static final int MAX_PLANS = 20;

    /*
     * Max number of objects to scan
     */
    private static final int MAX_PER_SCAN = 100;

    /*
     * The time windows used by the phased pruning. Only plans older than
     * the window are pruned. The time window gets shorter if the number
     * of plans still exceed the maximum.
     */
    private static final long TIME_WINDOW_MS[] = {60 * 60 * 1000, /* hour */
                                                  10 * 60 * 1000, /* 10 mins */
                                                  0};

    /* If > 0 overrides the default plan limit - for unit tests */
    public static volatile int planLimitOverride = 0;

    /* If > 0 overrides the default max per scan - for unit tests */
    public static volatile int maxPerScanOverride = 0;

    /* A test hook that executes before the prune thread runs */
    public static volatile TestHook<PlanPruningThread>
        prunePreRunTestHook = null;

    /* A test hook that executes after a prune scan */
    public static volatile TestHook<PlanPruningThread>
        prunePostScanTestHook = null;

    /* A test hook that executes when the prune thread is created */
    public static volatile TestHook<PlanPruningThread>
        prunePostRunTestHook = null;

    /* Limit on the number of plans maintained in the store. */
    private final int planLimit;

    /* Limit on the number of plans to delete per scan. */
    private final int maxPerScan;

    /* The number of plans which will cause a pruning run. */
    private final int pruneTrigger;

    private volatile String latestPruningInfo = "no prune ran";

    public static PlanStore getReadOnlyInstance(Logger logger,
                                                Environment env) {
        return new PlanStore(logger, env, true);
    }

    /* Used for testing only */
    static PlanStore getWritableInstance(Logger logger, Environment env) {
    	return new PlanStore(logger, env, false);
    }

    /* True if close() has been called. */
    private volatile boolean closed = false;

    /* Thread to asynchronously prune plans. */
    private PlanPruningThread pruner = null;

    /* Highest plan ID seen while writing. */
    private int highestIdSeen = 0;

    private final AtomicLong nPlans;

    private final AdminDatabase<Integer, Plan> planDb;

    PlanStore(Logger logger, Environment env, boolean readOnly) {
        super(logger);
        planLimit = (planLimitOverride != 0) ? planLimitOverride :
                                               DEFAULT_PLAN_LIMIT;
        maxPerScan =
            (maxPerScanOverride != 0) ? maxPerScanOverride: MAX_PER_SCAN;

        pruneTrigger = planLimit + 10;
        planDb = new AdminDatabase<Integer, Plan>(DB_TYPE.PLAN, logger,
                                                  env, readOnly) {
            @Override
            protected DatabaseEntry keyToEntry(Integer key) {
                final DatabaseEntry keyEntry = new DatabaseEntry();
                IntegerBinding.intToEntry(key, keyEntry);
                return keyEntry;
            }};
         nPlans = new AtomicLong(planDb.count());
    }

    /**
     * Fetches all non-terminal Plans as a Map. System plans are not included
     * in the map.
     */
    Map<Integer, Plan> getActivePlans(Transaction txn,
                                      Planner planner,
                                      AdminServiceParams aServiceParams) {
        final Map<Integer, Plan> activePlans = new HashMap<>();

        try (final PlanCursor cursor = getPlanCursor(txn, null)) {
            for (Plan p = cursor.first();
                 p != null;
                 p = cursor.next()) {

                if (p.isSystemPlan()) {
                    continue;
                }

                if (!p.getState().isTerminal()) {
                    p.initializePlan(planner, aServiceParams);
                    activePlans.put(p.getId(), p);
                }
            }
        }
        return activePlans;
    }

    /**
     * Retrieve the beginning plan id and number of plans that satisfy the
     * request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] number of
     * plan ids in the range.
     *
     * Operates in three modes:
     *
     *    mode A requests howMany plans ids following startTime
     *    mode B requests howMany plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *        howMany is ignored in mode C.
     *
     * If the owner is not null, only plans with the specified owner will be
     * returned.  System plans are not included.
     */
    int[] getPlanIdRange(Transaction txn,
                         long startTime,
                         long endTime,
                         int howMany,
                         String ownerId) {
        final int[] range = {0, 0};
        final PlanCursor cursor = getPlanCursor(txn, null /* startPlanId */);

        int n = 0;
        try {
            if (startTime == 0L) {
                /* This is mode B. */
                for (Plan p = cursor.last();
                     p != null && n < howMany;
                     p = cursor.prev()) {

                    if (p.isSystemPlan()) {
                        continue;
                    }
                    if (ownerId != null) {
                        final String planOwnerId =
                            p.getOwner() == null ? null : p.getOwner().id();
                        if (!ownerId.equals(planOwnerId)) {
                            continue;
                        }
                    }

                    long creation = p.getCreateTime().getTime();
                    if (creation < endTime) {
                        n++;
                        range[0] = p.getId();
                    }
                }
                range[1] = n;
            } else {
                for (Plan p = cursor.first();
                     p != null;
                     p = cursor.next()) {

                    if (p.isSystemPlan()) {
                        continue;
                    }

                    if (ownerId != null) {
                        final String planOwnerId =
                            p.getOwner() == null ? null : p.getOwner().id();
                        if (!ownerId.equals(planOwnerId)) {
                            continue;
                        }
                    }

                    long creation = p.getCreateTime().getTime();
                    if (creation >= startTime) {
                        if (range[0] == 0) {
                            range[0] = p.getId();
                        }
                        if (endTime != 0L && creation > endTime) {
                            /* Mode C */
                            break;
                        }
                        if (howMany != 0 && n >= howMany) {
                            /* Mode A */
                            break;
                        }
                        n++;
                    }
                }
                range[1] = n;
            }
        } finally {
            cursor.close();
        }

        return range;
    }

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped. If the owner is not null, only plans
     * with the specified owner will be returned. System plans are not included
     * in the map.
     */
    Map<Integer, Plan> getPlanRange(Transaction txn,
                                    Planner planner,
                                    AdminServiceParams aServiceParams,
                                    int firstPlanId,
                                    int howMany,
                                    String ownerId) {
        if (howMany > MAX_PLANS) {
            howMany = MAX_PLANS;
        }

        final Map<Integer, Plan> fetchedPlans = new HashMap<>();

        try (final PlanCursor cursor = getPlanCursor(txn, firstPlanId)) {
            for (Plan p = cursor.first();
                 p != null && howMany > 0;
                 p = cursor.next()) {

                if (p.isSystemPlan()) {
                    continue;
                }
                if (ownerId != null) {
                    final String planOwnerId =
                        p.getOwner() == null ? null : p.getOwner().id();
                    if (!ownerId.equals(planOwnerId)) {
                        continue;
                    }
                }

                p.initializePlan(planner, aServiceParams);
                p.stripForDisplay();
                fetchedPlans.put(p.getId(), p);
                howMany--;
            }
        }
        return fetchedPlans;
    }

    /**
     * Returns the Plan corresponding to the given id,
     * fetched from the database; or null if there is no corresponding plan.
     */
    Plan getPlanById(int id,
                     Transaction txn,
                     Planner planner,
                     AdminServiceParams aServiceParams) {
        final Plan p = get(txn, id);

        if (p != null) {
            p.initializePlan(planner, aServiceParams);
        }
        return p;
    }

    /**
     * Returns the stats of the latest prune.
     */
    public String getLatestPruningInfo() {
        return latestPruningInfo;
    }

    void logFetching(int planId) {
        logger.log(Level.FINE, "Fetching plan using id {0}", planId);
    }

    /**
     * Persists a plan into the store. Callers are responsible for
     * exception handling.
     *
     * The method must be called while synchronized on the plan instance
     * to ensure that the plan instance is not modified while the object is
     * being serialized into bytes before being stored into the database. Note
     * that the synchronization hierarchy requires that no other JE locks are
     * held before the mutex is acquired, so the caller to this method must be
     * careful. The synchronization is done explicitly by the caller, rather
     * than making this method synchronized, to provide more flexibility for
     * obeying the synchronization hierarchy.
     *
     * @param txn the transaction in progress
     */
    void put(Transaction txn, Plan plan) {
        final int planId = plan.getId();

        /* The plan ID is the primary key. */
        planDb.put(txn, planId, plan, false);
        logger.log(Level.FINE, "Storing plan {0}", planId);

        /*
         * If writing a new plan, record the new ID and increment the
         * estimated plan count. If over pruneTrigger attempt to prune.
         * Note that since highestIdSeen is initialized to 0 and
         * estNumPlan is initialized to pruneTrigger, pruning will occur
         * on the fist write (of any kind) after the Admin starts.
         * Also note that plans may be written in non-sequential order (by
         * plan ID). If that happens the count will be off, hence the
         * "estimated" number of plans. Eventually pruning will be
         * triggered which is what is important.
         */
        if (planId > highestIdSeen) {
            highestIdSeen = planId;
            final long numPlans = nPlans.incrementAndGet();
            if (numPlans > pruneTrigger) {
                /* Planner can be null during unit testing */
                final Planner planner = plan.getPlanner();
                if (planner != null) {
                    /* prunePlans will reset estNumPlans */
                    prunePlans(planner.getAdmin());
                }
            }
        }
    }

    /**
     * Fetches a plan from the store. Callers are responsible for
     * exception handling.
     */
    Plan get(Transaction txn, int planId) {
        logFetching(planId);
        return planDb.get(txn, planId, LockMode.DEFAULT, Plan.class);
    }

    /**
     * Returns a cursor for iterating all plans in the store.  Callers are
     * responsible for exception handling, and should close the cursor via
     * {@link PlanCursor#close} after use.
     */
    public PlanCursor getPlanCursor(Transaction txn, Integer startPlanId) {
        final Cursor cursor = planDb.openCursor(txn);
        return new PlanCursor(cursor, startPlanId) {

            @Override
            protected Plan entryToObject(DatabaseEntry key,
                                         DatabaseEntry value) {
                return SerializationUtil.getObject(value.getData(),
                                                   Plan.class);
            }
        };
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            if (pruner != null) {
                pruner.shutdown();
                pruner = null;
            }
        }
        planDb.close();
    }

    /**
     * Prunes plans from the store. Plans are pruned in two passes, first
     * system plans are removed, independent of the number of plans. Once
     * system plans are removed, user plans are pruned back to the
     * planLimit. Once pruning is complete, estNumPlans is set to the
     * number of plans found during scans.
     *
     * System plans are pruned first in an attempt to keep as many user
     * plans as possible, for as long as possible.
     */
    private synchronized void prunePlans(Admin admin) {
        if (closed || (pruner != null) && pruner.isAlive()) {
            return;
        }
        pruner = new PlanPruningThread(admin);
        pruner.start();
    }

    public class PlanPruningThread extends ShutdownThread {

        private final Admin admin;
        private int numPlansPruned = 0;
        private int firstScannedPlanID = -1;
        private int firstPrunedPlanID = -1;
        private int lastPrunedPlanID = -1;
        private int numSkippedPlans = 0;
        private String firstSkippedPlan = null;

        PlanPruningThread(Admin admin) {
            super("PlanPruningThread");
            this.admin = admin;
        }

        @Override
        public void run() {
            assert TestHookExecute.doHookIfSet(
                prunePreRunTestHook, PlanPruningThread.this);
            try {
                final long startTime = System.currentTimeMillis();
                logger.log(
                    Level.FINE,
                    () -> String.format(
                        "Starting %s. Total number of plans %s.",
                        this, nPlans.get()));

                if (nPlans.get() <= planLimit) {
                    return;
                }

                /*
                 * The number of plans to prune is computed before we start the
                 * multi-pass /multi-phase algorithm to reduce the number of
                 * saved plans.
                 */
                int excess = (int) nPlans.get() - planLimit;
                int totalDeleted = 0;

                /*
                 * Prune system plans first.
                 */
                int removed =
                    prunePlans(Integer.MAX_VALUE,
                               startTime,
                               PRUNE_TYPE.SYSTEM);
                excess = excess - removed;
                nPlans.addAndGet(-removed);
                logger.log(
                    Level.FINE,
                    () -> String.format("Pruned %s system plans", removed));
                if (excess <= 0) {
                    logger.log(
                        Level.FINE,
                        "Plan store contains {0} plans, scan took {1}ms, " +
                        "{2} plans targeted {3} plans deleted.",
                        new Object[] {
                        nPlans,
                        System.currentTimeMillis() - startTime,
                        nPlans.get() - planLimit,
                        totalDeleted});
                    return;
                }

                for (long timeMax : TIME_WINDOW_MS) {
                    final int pr = prunePlans(excess, timeMax);
                    excess -= pr;
                    totalDeleted += pr;
                    nPlans.addAndGet(-pr);
                    logger.log(
                        Level.FINE,
                        () -> String.format(
                            "Pruned %s non-system plans", pr));
                    if (excess <= 0) {
                        break;
                    }
                }


                logger.log(Level.FINE,
                           "Plan store contains {0} plans, scan took {1}ms," +
                           " {2} plans targeted {3} plans deleted. {4}",
                           new Object[] {nPlans,
                           System.currentTimeMillis() - startTime,
                           nPlans.get()- planLimit,
                           totalDeleted,
                           latestPruningInfo});
            } catch (Throwable t) {
                logger.log(
                    Level.FINE,
                    () -> String.format(
                        "Exception running plan pruning thread: %s",
                        CommonLoggerUtils.getStackTrace(t)));
            } finally {
                /*
                 * Updates the cached value since it may not be updated
                 * correctly, e.g., the prunePlans may fail for some later
                 * scans due to replicas not available and thus pruned plans
                 * from previous scans are not reflected [KVSTORE-672].
                 */
                nPlans.set(planDb.count());
                latestPruningInfo = getPlanPruningInfo();
                assert TestHookExecute.doHookIfSet(
                    prunePostRunTestHook, PlanPruningThread.this);
            }
        }

        private int prunePlans(int excess, long maxTimeInterval) {

            long maxTime = System.currentTimeMillis() - maxTimeInterval;

            int removed =
                prunePlans(excess, maxTime, PRUNE_TYPE.TERMINAL);

            /* Adjust the previous result */
            excess = excess - removed;
            if (excess <= 0) {
                return removed;
            }

            if (cancelPlans(excess, maxTime) > 0) {
                int userRemoved =
                    prunePlans(excess, maxTime, PRUNE_TYPE.TERMINAL);
                /* Adjust the previous result */
                removed += userRemoved;
            }
            return removed;
        }

        /**
         * Scans the plan DB and attempt to remove the specified number of
         * plans. The number of plans removed and number of plans scanned
         * is returned.
         */
        private int prunePlans(int target, long maxTime, PRUNE_TYPE pt) {
            if (isShutdown()) {
                return 0;
            }
            logger.log(Level.FINE,
                       "PrunePlan scan target {0} maxTime {1} type {2}" +
                       " {2} plans targeted.",
                       new Object[] {target, maxTime, pt});
            final AtomicInteger result = new AtomicInteger();

            /* ID of last plan scanned */
            final AtomicInteger lastPlanId = new AtomicInteger();

            /*
             * Loop reading the plans from the DB. Each iteration will
             * scan up to 100 plans in order to keep the transaction short.
             */
            boolean cont = true;
            while (!isShutdown() && cont) {
                /* Per transaction variables */
                final AtomicInteger xactResult = new AtomicInteger();
                final AtomicInteger xactPlanId = new AtomicInteger();

                cont = new RunTransaction<Boolean>(admin.getEnv(),
                                                   RunTransaction.sync,
                                                   logger) {
                    @Override
                    protected Boolean doTransaction(Transaction txn) {
                        /* Reset per transaction values */
                        xactResult.set(0);
                        xactPlanId.set(lastPlanId.get());
                        int nPassPlans = 0;

                        final int start = xactPlanId.incrementAndGet();
                        try (final PlanCursor cursor =
                                                getPlanCursor(txn, start)) {

                            for (Plan p = cursor.first();
                                 p != null;
                                 p = cursor.next()) {

                                final int id = p.getId();
                                xactPlanId.set(id);

                                firstScannedPlanID =
                                    (firstScannedPlanID == -1)
                                    ? id : Math.min(firstScannedPlanID, id);

                                /* Check if plan time is earlier than max */
                                if (isShutdown() ||
                                    p.getCreateTime().getTime() > maxTime) {
                                   return false;
                                }

                                if (canPrune(p, pt)) {
                                    cursor.delete();
                                    xactResult.getAndIncrement();

                                    numPlansPruned++;
                                    firstPrunedPlanID =
                                        (firstPrunedPlanID == -1)
                                        ? id : Math.min(firstPrunedPlanID, id);
                                    lastPrunedPlanID =
                                        Math.max(lastPrunedPlanID, id);

                                    /* Done if the target is reached */
                                    if ((result.get() +
                                         xactResult.get()) >= target) {
                                        /* Will return false */
                                        break;
                                    }
                                } else {
                                    final boolean isSystemPrune =
                                        pt.equals(PRUNE_TYPE.SYSTEM);
                                    if (isSystemPrune == p.isSystemPlan()) {
                                        numSkippedPlans++;
                                        if (firstSkippedPlan == null) {
                                            firstSkippedPlan = String.format(
                                                "(id=%s, name=%s, "
                                                  + "state=%s, created=%s)",
                                                p.getId(), p.getName(),
                                                p.getState(),
                                                p.getCreateTime());
                                        }
                                    }
                                }

                                /* Cap this pass at 100 */
                                nPassPlans++;
                                if (nPassPlans >= maxPerScan) {
                                    return true; /* continue */
                                }
                            }
                            /* Done scanning */
                            return false;
                        }
                    }

                    @Override
                    protected boolean shuttingDown() {
                        return isShutdown();
                    }
                }.run();
                assert TestHookExecute.doHookIfSet(
                    prunePostScanTestHook, PlanPruningThread.this);
                result.addAndGet(xactResult.get());
                lastPlanId.set(xactPlanId.get());
            }
            logger.log(Level.FINE,
                       "Plan maintenance removed {0} plans.",
                       new Object[] {result.get()});
            return result.get();
        }

        private long cancelPlans(int target, long maxTime) {
            if (isShutdown()) {
                return 0;
            }
            logger.log(Level.FINE,
                       "cancelPlan scan target {0} maxTime {1}.",
                       new Object[] {target, maxTime});
            /*
             * Read the plans from the DB looking for any that can be canceled.
             */
            final List<Integer> foundPlans = new ArrayList<>();
            /* ID of last plan scanned */
            final AtomicInteger lastPlanId = new AtomicInteger();
            boolean cont = true;
            while (!isShutdown() && cont) {
                /* Per transaction variables */
                final AtomicInteger xactPlanId = new AtomicInteger();
                cont = new RunTransaction<Boolean>(admin.getEnv(),
                                                   RunTransaction.readOnly,
                                                   logger) {
                    @Override
                    protected Boolean doTransaction(Transaction txn) {
                        xactPlanId.set(lastPlanId.get());
                        int nPassPlans = 0;
                        final int start = xactPlanId.incrementAndGet();
                        try (final PlanCursor cursor =
                                                getPlanCursor(txn, start)) {
                            for (Plan p = cursor.first();
                                 p != null;
                                 p = cursor.next()) {
                                if (p.getCreateTime().getTime() >= maxTime) {
                                    return false;
                                }

                                if (canCancel(p)) {
                                    foundPlans.add(p.getId());
                                    /* Done if the target is reached */
                                    if (foundPlans.size() >= target) {
                                        return false;
                                    }
                                }
                                /* Cap this pass at 100 */
                                nPassPlans++;
                                if (nPassPlans >= MAX_PER_SCAN) {
                                    return true; /* continue */
                                }
                            }
                        }
                        return false;
                    }

                    @Override
                    protected boolean shuttingDown() {
                        return isShutdown();
                    }
                }.run();
                lastPlanId.set(xactPlanId.get());
            }

            for (int planId : foundPlans) {
                if (isShutdown()) {
                    return 0;
                }
                try {
                    admin.cancelPlan(planId);
                } catch (Exception e) {
                   /*
                    * Ignore exception. There is a possibility that the
                    * plan we are attempting to cancel has changed it's
                    * state so it could not be cancelled.
                    */
                }
            }

            logger.log(Level.FINE,
                       "Plan maintenance cancelled {0} plans.",
                       new Object[] {foundPlans.size()});
            return foundPlans.size();
        }

        /**
         * Returns true if the specified plan can be pruned (in a terminal
         * state). If prune type is SYSTEM then the plan can be pruned if
         * it is a system plan and is in the terminal, ERROR, or
         * INTERRUPTED state.
         */
        private boolean canPrune(Plan p, PRUNE_TYPE pt) {
            final Plan.State state = p.getState();
            boolean canPrune = false;
            switch (pt) {
                case SYSTEM:
                    if (p.isSystemPlan() &&
                        (state.isTerminal() ||
                         state.equals(Plan.State.ERROR) ||
                         state.equals(Plan.State.INTERRUPTED))) {
                        canPrune = true;
                    }
                    break;
                case TERMINAL:
                    canPrune = state.isTerminal();
                    break;
                default:
                    throw new AssertionError();
            }
            return canPrune;
        }

        private boolean canCancel(Plan p) {
            final Plan.State state = p.getState();
            if (!p.isSystemPlan() && !state.equals(Plan.State.RUNNING)) {
                return true;
            }
            return false;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }

        private String getPlanPruningInfo() {
            return String.format(
                "numPlansPruned=%s, " +
                "firstScannedPlanID=%s, " +
                "firstPrunedPlanID=%s, " +
                "lastPrunedPlanID=%s, " +
                "numSkippedPlans=%s, " +
                "firstSkippedPlan=%s, ",
                numPlansPruned,
                firstScannedPlanID,
                firstPrunedPlanID,
                lastPrunedPlanID,
                numSkippedPlans,
                firstSkippedPlan);
        }

        /* For testing */
        public PlanStore getPlanStore() {
            return PlanStore.this;
        }

        public int getNumPlansPruned() {
            return numPlansPruned;
        }
    }

    /**
     * A cursor class to facilitate the scan of the plan store.
     */
    public abstract static class PlanCursor
            extends AdminStoreCursor<Integer, Plan> {

        private PlanCursor(Cursor cursor,  Integer startKey) {
            super(cursor, startKey);
        }

        @Override
        protected void keyToEntry(Integer key, DatabaseEntry entry) {
            IntegerBinding.intToEntry(key, entry);
        }
    }

    /* For test */
    public long getCachedNumPlans() {
        return nPlans.get();
    }

    public long getActualNumPlans() {
        return planDb.count();
    }
}
