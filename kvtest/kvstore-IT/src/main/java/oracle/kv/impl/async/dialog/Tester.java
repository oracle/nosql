/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A base class for tester.
 */
public class Tester extends Logged {

    /* Random. */
    private static final Random rand = new Random();

    /* Sequencer. */
    private final AtomicInteger sequencer = new AtomicInteger(0);
    /* Waiting semaphore. */
    private final int numTotalPermits;
    private final Semaphore semaphore;

    public Tester(boolean queueMesgs) {
        this(Integer.MAX_VALUE, queueMesgs);
    }

    public Tester(int numTotalPermits, boolean queueMesgs) {
        super(queueMesgs);
        this.numTotalPermits = numTotalPermits;
        this.semaphore = new Semaphore(numTotalPermits);
    }

    /**
     * Waits until all semaphores are released.
     */
    public void await(long timeout) {
        boolean acquired;
        try {
            acquired =
                semaphore.tryAcquire(
                    numTotalPermits, timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected interrupt", e);
        }
        if (acquired) {
            semaphore.release(numTotalPermits);
            logMesg("Test stopped after waiting for finish");
        } else {
            throw new RuntimeException("Test did not finish in time");
        }
    }

    /**
     * Acquire a permit from the semaphore.
     */
    public void semaphoreAcquire() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "Interrupted during acquiring semaphore.");
        }
    }

    /**
     * Release a permit to the semaphore.
     */
    public void semaphoreRelease() {
        semaphore.release();
    }

    /**
     * Returns a new Id.
     */
    public int newId() {
        return sequencer.incrementAndGet();
    }

    protected abstract class Task {

        private final int id = sequencer.incrementAndGet();
        private final ScheduledExecutorService executor;
        private boolean hasError = false;

        protected Task(ScheduledExecutorService executor) {
            this.executor = executor;
        }
        /**
         * Returns the Id.
         */
        public int getId() {
            return id;
        }
        /**
         * Run the procedure once.
         */
        public synchronized void runOnce() {
            long ts = now();
            String info = null;
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                return;
            }
            try {
                info = doRun();
            } catch (Throwable t) {
                logError(t);
                hasError = true;
            } finally {
                long te = now();
                if (info != null) {
                    logMesg(String.format(
                                "%s ts=%s ms d=%s ns: %s",
                                toString(), ts / 1e6, (te - ts), info));
                }
                semaphore.release();
            }
        }
        /**
         * Schedule to run once for the task.
         */
        public void scheduleOnce() {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    runOnce();
                }
            };
            executor.execute(r);
        }
        /**
         * Schedule a sequences of runs for the task.
         */
        public void schedule() {
            schedule(50);
        }
        public void schedule(int threshold) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    if (rand.nextInt(100) < threshold) {
                        synchronized(this) {
                            runOnce();
                            if ((!hasError) && shouldRunAgain()) {
                                schedule(threshold);
                            }
                        }
                    } else {
                        schedule(threshold);
                    }
                }
            };
            executor.execute(r);
        }
        @Override
        public String toString() {
            return String.format("%s#%x", getClass().getSimpleName(), id);
        }
        /**
         * The actual procedure to run.
         */
        protected abstract String doRun() throws Exception;
        /**
         * Returns {@code true} if should run again.
         */
        protected abstract boolean shouldRunAgain();
    }
}
