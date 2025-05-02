/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

import java.io.Closeable;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TimeSupplier;

/**
 * The base class that coordinates tasks via the distribution of permits to
 * cooperating tasks or the application itself.
 *
 * Housekeeping tasks acquire and release permits as needed using the
 * {@link #acquirePermit(Task, long, long, TimeUnit)} and
 * {@link #releasePermit(Permit)} pair of methods.
 *
 * The application controls the supply of permits available to the housekeeping
 * tasks via {@link #setAppPermitPercent(int)}, increasing the percentage of
 * permits it retains as it gets busy and decreasing the percentage when the
 * load diminishes.
 */

/*
 * TODO:
 *
 * Add Async API: The TaskCoordinator could arrange to call back when a permit
 * became available. The API would be particularly useful in this case, since
 * it would mean that a thread would not be needed. Tim points out that
 * IndexCreation would be a good candidate for it.
 */
public class TaskCoordinator {

    /**
     * The set of tasks (keyed by name) associated being coordinated.
     */
    private final Set<Task> tasks;

    /**
     * The set of permits (real and deficit) that have been acquired but not
     * yet released. The set is used to track leaked permits.
     */
    private final Set<Permit> outstandingPermits =
        ConcurrentHashMap.newKeySet();

    /**
     * The maximum allowed number of real permits. The value is
     * Integer.MAX_VALUE if the coordinator is not active.
     */
    private final int maxRealPermits;

    /**
     * Total number of outstanding (acquired but not released) real permits,
     * ones for which there are RealPermit objects that have not been released.
     * Some fraction of these permits may be held by the application.
     */
    private final AtomicInteger outstandingRealPermits = new AtomicInteger(0);

    /**
     * Total number of outstanding deficit permits, ones for which there are
     * DeficitPermit objects that have not been released. Note that the
     * application itself never acquires deficit permits.
     */
    private final AtomicInteger deficitPermits = new AtomicInteger(0);

    /**
     * Total number of permits granted for any reason.
     */
    private volatile long allPermitsGranted = 0;

    /**
     * The percentage of real permits held by the application.
     *
     * Note that Application permits are a book keeping fiction designed to
     * control the supply of permits available to housekeeping tasks; they are
     * not represented by RealPermit or DeficitPermit objects. The busier the
     * application, the larger this number and the fewer the permits available
     * to the housekeeping tasks.
     *
     * The following invariants are worth noting:
     *
     * 1) appPermits <= maxPermits
     *
     * 2) appPermits + outStandingRealPermits <= maxPermits at stasis. However,
     * since the Application acquires permits preemptively, as it needs them
     * and never waits for permits like housekeeping Tasks do, appPermits +
     * outStandingRealPermits could be > maxPermits on a transient basis; over
     * time as tasks release permits (which are not then put back into
     * circulation), outstandingRealPermits decreases and taskPermits +
     * outstandingRealPermits should converge to <= maxPermits.
     */
    private volatile int appPermitPercent = 0;

    /* Use to determine whether the coordinator has been closed. */
    private final AtomicBoolean close = new AtomicBoolean(false);

    /**
     * Tasks and the Application coordinate the number of outstanding permits
     * through this semaphore. Tasks acquire permits waiting if necessary via
     * {@link #acquirePermit}. The semaphore forms the guts of the
     * implementation and gates all access to real permits.
     *
     * Note that both real and deficit permits are tracked by the semaphore.
     * That is, outstanding deficit permits will inc/dec the semaphore upon
     * acquisition/release, just like real permits.
     */
    private final CoordinatorSemaphore permitSemaphore;

    private volatile TimerTask leaseCheckingTask;

    protected final Timer timer = new Timer(true /* isDaemon */);

    /**
     * The default period used to run the lease checking task.
     */
    public static final int DEFAULT_LEASE_CHECK_PERIOD_MS = 1000;

    /**
     * Used as the number of Real permits that can be given out (effectively
     * unlimited) when the TaskCoordinator is inactive.
     */
    private static final int INACTIVE_REAL_PERMITS = Integer.MAX_VALUE / 2;

    protected final Logger logger;

    /* Logger used to track deficit permits. */
    private final RateLimitingLogger<Task> deficitLogger;

    /**
     * The environment associated with the task coordinator, for logging. If
     * null, then the task coordinator is not associated with an environment,
     * and normal logging should be used on the loggers. This case happens when
     * TaskCoordinator is used in kvstore.
     */
    private final EnvironmentImpl envImpl;

    /**
     * The constructor used to initialize the set of cooperating tasks.
     *
     * A coordinator can be rendered inactive, by passing "false" to the active
     * argument. An inactive coordinator will always give out a real permit
     * (never a deficit permit), without waiting, when one is requested via
     * {@link #acquirePermit}, thus effectively disabling the coordination
     * mechanism. Note that leases, if specified, are still tracked against
     * these permits. This is to ensure that the behavior of the code is
     * correct and no permits are being leaked.
     *
     * @param logger the logger used for task coordination related logging
     * @param envImpl the environment to use for logging or null
     * @param tasks the set of cooperating tasks
     * @param active if true the coordinator is active
     */
    public TaskCoordinator(Logger logger,
                           EnvironmentImpl envImpl,
                           Set<Task> tasks,
                           boolean active) {
        super();

        Objects.requireNonNull(logger, "logger argument must not be null");
        Objects.requireNonNull(tasks, "tasks argument must not be null");

        this.logger = logger;
        this.envImpl = envImpl;
        deficitLogger =
            new RateLimitingLogger<>(60 * 1000, tasks.size(), logger);
        this.tasks = tasks;
        maxRealPermits = active ?
            tasks.stream().mapToInt(Task::getPermits).sum() :
            INACTIVE_REAL_PERMITS;

        if (! tasks.isEmpty()) {
            LoggerUtils.logMsg(logger, envImpl, Level.INFO,
                               "Coordinating tasks:" +
                               tasks.stream().map(Task::getName).
                               collect(Collectors.joining(", ")) +
                               "Max real permits:" + maxRealPermits);
        }
        permitSemaphore = new CoordinatorSemaphore(maxRealPermits);
        setLeaseCheckingPeriod(DEFAULT_LEASE_CHECK_PERIOD_MS);
        /* Start with all permits available to housekeeping tasks. */
        setAppPermitPercent(0);
    }

    /**
     * The constructor used to create active coordinators.
     *
     * @see #TaskCoordinator(Logger, EnvironmentImpl, Set, boolean)
     */
    public TaskCoordinator(Logger logger,
                           EnvironmentImpl envImpl,
                           Set<Task> tasks) {
        this(logger, envImpl, tasks, true);
    }

    /**
     * Modifies the lease period. Used primarily for testing.
     *
     * @param periodMs the new period; turns off lease checking if the period
     * is zero
     */
    public void setLeaseCheckingPeriod(int periodMs) {
        if (leaseCheckingTask != null) {
            leaseCheckingTask.cancel();
            leaseCheckingTask = null;
        }

        if (periodMs == 0) {
            /* Turn off lease checking. */
            return;
        }

        leaseCheckingTask = new TimerTask() {
            @Override
            public void run() {
                /*
                 * Iterate over permits that are open and close them, releasing
                 * their resources.
                 */
                for (Permit p : outstandingPermits) {
                    if (!p.isExpired()) {
                        continue;
                    }

                    try {
                        p.close();
                    } catch (IllegalStateException ise) {
                        /* Expected ISE */
                        LoggerUtils.logMsg(
                            logger, envImpl, Level.WARNING,
                            "Detected (possibly leaked) permit" +
                            " with expired lease: " +
                            p.getTask().getName() + " " + ise.getMessage());
                    }
                }
            }
        };

        timer.schedule(leaseCheckingTask, 0, periodMs);
    }


    /**
     * Returns the constant maximum number of real permits available to
     * housekeeping tasks.
     *
     * Note: This function will return Integer.MAX_VALUE when the coordinator
     * was initialized to be inactive.
     */
    public int getMaxPermits() {
        return maxRealPermits;
    }

    /**
     * Returns the current number of app permits reserved by the application.
     * These permits are not available for acquisition by housekeeping tasks.
     */
    public int getAppPermits() {
        return (maxRealPermits * appPermitPercent) / 100;
    }

    /**
     * Returns the % of total permits current reserved by the application.
     */
    public int getAppPermitPercent() {
        return appPermitPercent;
    }

    /**
     * The number of real permits available for acquisition by housekeeping
     * tasks. Note that this number can be negative, due to the existence of
     * outstanding deficit permits, or due to an async increase in permits
     * reserved by the application.
     */
    public int getAvailableRealPermits() {
        return permitSemaphore.availablePermits();
    }

    /**
     * Returns the number of currently outstanding deficit permits
     */
    public int getOutstandingDeficitPermits() {
        return deficitPermits.get();
    }

    /**
     * Returns the number of currently outstanding real permits. These are
     * permits that have been acquired but not yet released.
     */
    public int getOutstandingRealPermits() {
        return outstandingRealPermits.get();
    }

    public long getAllPermitsGranted() {
        return allPermitsGranted;
    }

    /**
     * Returns the number of outstanding permits.  Only used in testing so no
     * synchronization.
     */
    public int getOutstandingPermits() {
        return outstandingPermits.size();
    }

    /**
     * Returns a Permit. The timeout determines how long the application is
     * prepared to wait cooperatively for a permit. <p>
     *
     * If a permit cannot be granted within the <code>timeout</code> period, a
     * deficit permit ({@link Permit#isDeficit()} is granted. The requester can
     * choose to release the permit immediately or proceed with its task in any
     * case and release the permit later. Returning a deficit permit reduces
     * the supply of permits by one; the supply is restored when the deficit
     * permit is released. <p>
     *
     * A permit can optionally be associated with an extensible (via
     * Permit#setLease) lease period. This mechanism is primarily intended to
     * be a debugging aid to detect Permits which were acquired but not
     * released. The permit is effectively released after it has expired by a
     * background thread thus freeing up its resources. Such "potentially"
     * leaked permits are logged as an aid to debugging. Attempts to access
     * them after the lease has expired will result in an
     * IllegalStateException.
     *
     * @param task the task acquiring the permit. It must be one of the
     * cooperating tasks, supplied when the coordinator was created.
     * @param timeout the amount of time to wait for the permits. If the value
     * is zero don't wait.
     * @param leaseInterval the max amount of time that a permit can be
     * retained after it is acquired before it must be released.  Set to zero
     * for no timeout.
     * @param unit the time unit of the {@code timeout and @code leaseInterval}
     * arguments
     *
     * @return Permit the returned Permit. It could be a deficit permit if one
     * could not be obtained in the timeout interval.
     *
     * if the thread is interrupted. The request to acquire the permit 
     * is abandoned in this case. The state of the coordinator is not impacted 
     * by the interrupt and the application can proceed to acquire other 
     * permits or {@link #close()} the coordinator.
     *
     * @see #releasePermit(Permit)
     * @see Permit#setLease
     */
    public Permit acquirePermit(Task task,
        long timeout,
        long leaseInterval,
        TimeUnit unit,
        Consumer<InterruptedException> handler) {
        try {
            return acquirePermit(task, timeout, leaseInterval, unit);
        } catch (InterruptedException e) {
            handler.accept(e);
            throw new IllegalStateException("Handler may not return.");
        }
    }

    public Permit acquirePermit(Task task,
                                long timeout,
                                long leaseInterval,
                                TimeUnit unit)
        throws InterruptedException {

        Objects.requireNonNull(task, "task argument must not be null");
        Objects.requireNonNull(task, "unit argument must not be null");

        if (!tasks.contains(task)) {
            throw new IllegalArgumentException("Unknown task:" +
                                                task.getName());
        }

        final long leaseIntervalMs = unit.toMillis(leaseInterval);

        if (close.get()) {
            allPermitsGranted++;
            /* Facilitate a graceful shutdown by issuing a deficit permit */
            return new DeficitPermit(task, leaseIntervalMs);
        }

        if ((leaseInterval > 0) && (leaseIntervalMs == 0)) {
            final String msg = "Non-zero lease interval:" +
                leaseInterval + " " + unit.toString() + " must be >= 1 ms";
            throw new IllegalArgumentException(msg);
        }

        try {
            if (permitSemaphore.tryAcquire(1, timeout, unit)) {
                if (close.get()) {
                    allPermitsGranted++;
                    return new DeficitPermit(task, leaseIntervalMs);
                }
                allPermitsGranted++;
                return new RealPermit(task, leaseIntervalMs) ;
            }
        } catch (InterruptedException iae) {
            LoggerUtils.logMsg(logger, envImpl, Level.INFO,
                               "Permit acquisition for task:" +
                               task.getName() + " was interrupted");
            throw iae;
        }

        LoggerUtils.logMsg(deficitLogger, envImpl, task, Level.INFO,
                           "Granted deficit permit to " + task +
                           " after waiting for " + timeout +
                           " " + unit.toString() +
                           ". Current app permit %: " + appPermitPercent);

        allPermitsGranted++;
        return new DeficitPermit(task, leaseIntervalMs);
    }

    /**
     * Releases the permit that was acquired by an earlier call to
     * {@link #acquirePermit}. It's up to the application to ensure correct
     * pairing of an {@link #acquirePermit}  and {@link #releasePermit}  pair.
     *
     * An IllegalStateException is thrown if the lease associated with the
     * permit has expired or an attempt is made to release a permit that was
     * already released.
     *
     * @param permit the permit being released
     *
     * @see #acquirePermit(Task, long, long, TimeUnit)
     */
    public void releasePermit(Permit permit) {
        Objects.requireNonNull(permit, "permit argument must not be null");

        if (!tasks.contains(permit.getTask())) {
            throw new IllegalArgumentException("Unknown task:" +
                                               permit.getTask().getName());
        }

        permit.releasePermit();
    }

    /**
     * Used to adjust the number of permits associated with the application. It
     * does this by varying the number of permits available to the tasks. Any
     * permits associated with it are not available for acquisition by tasks.
     *
     * The application invokes this method whenever it detects a change in its
     * load characteristics. If it detects an increase in its load, it should
     * increase the percentage of permits it reserves for itself and vice
     * versa.
     *
     * @param newAppPermitPercent the percentage of permits to be reserved for
     * the application. The higher this percentage, the fewer the permits
     * available for use by the housekeeping tasks. It must be a number between
     * 0 and 100
     *
     * @return true if the number of application permits were changed
     */
    public synchronized boolean setAppPermitPercent(int newAppPermitPercent) {

        if (appPermitPercent == newAppPermitPercent) {
            return false;
        }

        if ((newAppPermitPercent < 0) || (newAppPermitPercent > 100)) {
            throw new IllegalArgumentException
                ("Parameter must be a percentage:" + newAppPermitPercent);
        }

        final int reqAppPermits = (maxRealPermits * newAppPermitPercent) / 100;
        final int delta = reqAppPermits - getAppPermits() ;

        if (delta == 0) {
            /* Percentage has changed, but number of permits has not. */
            appPermitPercent = newAppPermitPercent;
            return false;
        }

        if (delta > 0) {
            /* Needs more permits, just grab them. */
            permitSemaphore.revoke(delta);
        } else {
            /* App is releasing permits it had acquired. */
            permitSemaphore.release(-delta);
        }

        appPermitPercent = newAppPermitPercent;

        return true;
    }

    /**
     * Close the coordinator. All currently waiting tasks are released by
     * issuing a deficit permit. All subsequent calls to {@link #acquirePermit}
     * will result in the return of a DeficitPermit. It's up to the
     * individual tasks to check for application shutdown and do their own
     * individual cleanup.
     */
    public void close() {

        if (!close.compareAndSet(false, true)) {
            return;
        }
        LoggerUtils.logMsg(logger, envImpl, Level.FINE,
                           "Task Coordinator closed. " + permitSummary());
        /*
         * Free up all waiting tasks (in the await() call in acquirePermit())
         * using some suitably large value that will not cause overflows. Tasks
         * thus freed will get Deficit permits.
         */
        permitSemaphore.release(INACTIVE_REAL_PERMITS);

        timer.cancel();
    }

    /**
     * Returns the stats associated with the Task Coordinator.
     *
     * Note that statsConfig is unused since all the TaskCoordinator stats are
     * just instantaneous stats.
     */
    @SuppressWarnings("unused")
    public StatGroup getStats(StatsConfig statsConfig) {
        final StatGroup stats = new StatGroup(StatDefs.GROUP_NAME,
                                              StatDefs.GROUP_DESC);

        new IntStat(stats, StatDefs.REAL_PERMITS,
                    getOutstandingRealPermits());
        new IntStat(stats, StatDefs.DEFICIT_PERMITS,
                    getOutstandingDeficitPermits());
        new IntStat(stats, StatDefs.APPLICATION_PERMITS, getAppPermits());

        return stats;
    }

    /**
     * Log information about current use of permits
     */
    protected String permitSummary() {
        String taskSummary = tasks.stream().map(Task::toString).
            collect(Collectors.joining(", "));
        return String.format("App permits:%d%% (%d permits); " +
                             "Outstanding permits: %d real, %d deficit. %s",
                             appPermitPercent,
                             getAppPermits(),
                             getOutstandingRealPermits(),
                             getOutstandingDeficitPermits(),
                             taskSummary);
    }

    /**
     * Permits acquired by Tasks are represented by Permit objects. Permits
     * acquired by the application (via {@link #setAppPermitPercent}) are
     * implemented as bookkeeping adjustments and are not represented by Permit
     * objects.
     */
    public abstract class Permit implements Closeable {
        /* The Task associated with the Permit. */
        private final Task task;

        /*
         * The lease period associated with the permit.
         */
        private volatile long leaseEndMs = 0;

        /* The time at which the Permit was released. */
        private volatile long releaseMs = 0;

        public Permit(Task task, long leaseIntervalMs) {
            super();
            this.task = task;
            // Treat a 0 value as no timeout.
            if (leaseIntervalMs == 0) {
                leaseEndMs = Long.MAX_VALUE;
            } else {
                leaseEndMs = TimeSupplier.currentTimeMillis() + leaseIntervalMs;
            }

            /*
             * Initialize permit base before adding it to the set where it
             * becomes visible to the timer task.
             */
            if (!outstandingPermits.add(this)) {
                throw new IllegalStateException("Permit:" + task.getName() +
                                                " already present");
            }
        }

        Task getTask() {
            return task;
        }

        public synchronized boolean isReleased() {
            return releaseMs > 0;
        }

        /**
         * Returns true if the lease has expired.
         */
        public synchronized boolean isExpired() {
            if (isReleased()) {
                return releaseMs > leaseEndMs;
            }

            /* Not yet released, check if expired. */
            return TimeSupplier.currentTimeMillis() > leaseEndMs;
        }

        protected synchronized void checkLeaseExpiry() {
            if (isExpired()) {
                throw new IllegalStateException("Permit expired at:" +
                           new Date(leaseEndMs));
            }
        }

        private synchronized void checkReleased() {
            if (releaseMs > 0) {
                /*
                 * cleanup was completed, don't repeat it here or in the
                 * subclasses.
                 */
                final String msg = "Permit for the task:'" + task.getName() +
                    "' was previously released at " + new Date(releaseMs) +
                    (isExpired() ?
                        " Lease expired at " + new Date(leaseEndMs) : "");
                throw new IllegalStateException(msg);
            }
        }

        /**
         * Used to Extend the lease associated with the permit, when the
         * initial lease time cannot easily be predicted. It replaces the
         * current lease time.
         *
         * Upon expiration of the lease, the resources associated with the
         * permit are reclaimed upon the (possibly incorrect) assumption that
         * the permit was leaked.
         *
         * @param leaseInterval the new lease interval
         * @param unit the units used to express the lease interval
         */
        public synchronized void setLease(long leaseInterval,
                                          TimeUnit unit) {
            checkLeaseExpiry();

            checkReleased();

            leaseEndMs = TimeSupplier.currentTimeMillis() +
                TimeUnit.MILLISECONDS.convert(leaseInterval, unit);
        }

        /**
         * Marks the permit as being released after verifying that it was not
         * already released or expired.
         */
        public synchronized void releasePermit() {
            checkReleased();
            releaseMs = TimeSupplier.currentTimeMillis();
            if (isExpired()) {
                task.incExpiredPermits();
            }
            outstandingPermits.remove(this);
        }

        public abstract boolean isDeficit();

        @Override
        public synchronized void close() {
            /* Implement idempotent semantics */
            if (!isReleased()) {
                TaskCoordinator.this.releasePermit(this);
            }
        }
    }

    /**
     * The permit that is returned to a task when the {@link #acquirePermit}
     * call succeeds within its timeout period.
     */
    private class RealPermit extends Permit {

        RealPermit(Task task, long leaseIntervalMs) {
            super(task, leaseIntervalMs);
            outstandingRealPermits.incrementAndGet();
            task.incRealPermits();
        }

        @Override
        public boolean isDeficit() {
            return false;
        }

        @Override
        public synchronized void releasePermit() {
            super.releasePermit();
            /*
             * Note that the release() call never fails, there's no overflow!
             * That is, release can be used to create additional permits beyond
             * the initial set of permits.
             */
            outstandingRealPermits.decrementAndGet();
            permitSemaphore.release();

            /*
             * Do check at the end, after all state has been updated and an
             * exception can be thrown.
             */
            checkLeaseExpiry();
        }
    }

    /**
     * The permit that is returned to a task when the {@link #acquirePermit}
     * call exceeds its timeout.
     *
     * Note that creation of this permit, reduces the number of permits until
     * the deficit permit is released, as a way to reduce the task load?
     */
    private class DeficitPermit extends Permit {

        DeficitPermit(Task task, long leaseIntervalMs) {
            super(task, leaseIntervalMs);
            task.incDeficitPermits();
            deficitPermits.incrementAndGet();
            /* Pre-emptively reduce the permit supply. */
            permitSemaphore.revoke(1);
        }

        @Override
        public boolean isDeficit() {
            return true;
        }

        @Override
        public synchronized void releasePermit() {
            super.releasePermit();
            /* Return the permit revoked in the constructor. */
            permitSemaphore.release(1);
            deficitPermits.decrementAndGet();

            /* Do check at the end, after all state has been updated. */
            checkLeaseExpiry();
        }
    }

    /**
     * Subclass of semaphore to access protected method functionality.
     */
    @SuppressWarnings("serial")
    private static class CoordinatorSemaphore extends Semaphore
        implements NotSerializable {

        CoordinatorSemaphore(int permits) {
            super(permits, true /* fair */);
        }

        /**
         * Lower the number of permits available to tasks.
         *
         * @param permits the number of permits to be revoked
         */
        void revoke(int permits) {
            reducePermits(permits);
        }
    }

    /**
     * Represents a task that can request permits. Only tasks registered
     * with the coordinator as part of its initialization can request permits.
     */
    public static class Task {

        /* Unique name identifying the task. */
        final String name;

        /*
         * The number of permits normally associated with the task, typically
         * just one. A task is free to acquire more permits if available.
         */
        final int permits;

        /* Cumulative Statistics for permits actually granted to this task. */
        private final AtomicInteger realPermitsGranted = new AtomicInteger(0);
        private final AtomicInteger deficitPermitsGranted =
            new AtomicInteger(0);

        /**
         * A count of the permits whose leases expired at the time of release.
         */
        private final AtomicInteger expiredPermits = new AtomicInteger(0);

        public Task(String name,
                    int permits) {
            super();
            Objects.requireNonNull(name, "name argument must not be null");
            this.name = name;
            this.permits = permits;
        }

        public String getName() {
            return name;
        }

        public int getPermits() {
            return permits;
        }

        public int getRealPermitsGranted() {
            return realPermitsGranted.get();
        }

        public void incRealPermits() {
            realPermitsGranted.incrementAndGet();
        }

        public int getDeficitPermitsGranted() {
            return deficitPermitsGranted.get();
        }

        public void incDeficitPermits() {
            deficitPermitsGranted.incrementAndGet();
        }

        public void incExpiredPermits() {
            expiredPermits.incrementAndGet();
        }

        /**
         * Returns a count of permits whose leases expired at the time of
         * release. The release may have been initiated by the application or
         * by the internal leak detection task.
         */
        public int getExpiredPermits() {
            return expiredPermits.get();
        }

        public void clearStats() {
            deficitPermitsGranted.set(0);
            realPermitsGranted.set(0);
        }

        @Override
        public String toString() {
            return "< Task: " + name + ", " + "permits:" + permits +
                   " Real permits granted: " + realPermitsGranted +
                   " Deficit permits granted: " + deficitPermitsGranted +
                   " Expired permits:" + expiredPermits + " >";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;

            result = prime * result + name.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {


            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Task other = (Task) obj;
            return name.equals(other.name);
        }
    }

    /**
     * Defines stats associated with the TaskCoordinator.
     */
    public interface StatDefs {

        String GROUP_NAME = "TaskCoordinator";
        String GROUP_DESC =
            "Task coordination ensures that the execution of background " +
            " housekeeping tasks is coordinated, so they do not all execute " +
            " at once.";

        String REAL_PERMITS_NAME = "nRealPermits";
        String REAL_PERMITS_DESC =
            "Number of real permits that have been currently granted to " +
            "housekeeping tasks.";
        StatDefinition REAL_PERMITS =
            new StatDefinition(REAL_PERMITS_NAME, REAL_PERMITS_DESC);

        String DEFICIT_PERMITS_NAME = "nDeficitPermits";
        String DEFICIT_PERMITS_DESC =
            "Number of deficit permits that have been currently granted to " +
            "housekeeping tasks in the absence of real permits.";
        StatDefinition DEFICIT_PERMITS =
            new StatDefinition(DEFICIT_PERMITS_NAME, DEFICIT_PERMITS_DESC);

        String APPLICATION_PERMITS_NAME =
            "nApplicationPermits";
        String APPLICATION_PERMITS_DESC =
            "Number of permits that have been currently reserved by the " +
            "application and are therefor unavailable to housekeeping tasks.";
        StatDefinition APPLICATION_PERMITS =
            new StatDefinition(APPLICATION_PERMITS_NAME,
                               APPLICATION_PERMITS_DESC);

        /* All TaskCoordinator stats */
        StatDefinition ALL[] = {
                REAL_PERMITS,
                DEFICIT_PERMITS,
                APPLICATION_PERMITS
        };
    }
}
