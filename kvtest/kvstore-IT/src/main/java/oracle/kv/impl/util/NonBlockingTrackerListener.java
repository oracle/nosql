/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

/**
 * TrackerListener which uses a separate thread to deliver events.
 */
public abstract class NonBlockingTrackerListener {
    private static long POLLING_INTERVAL_MS = 1000;

    private volatile boolean stopped = true;

    private volatile Thread workerThread;

    private volatile long interestingTime;

    protected NonBlockingTrackerListener() { }

    /**
     * Starts the worker thread which will wait for event notification.
     */
    protected synchronized void start() {
        if (!stopped) {
            throw new IllegalStateException("worker already running");
        }

        stopped = false;
        workerThread = new Thread("Tracker listener worker") {
            @Override
            public void run() {
                while (true) {
                    synchronized(NonBlockingTrackerListener.this) {
                        if (stopped) {
                            return;
                        }
                        try {
                            NonBlockingTrackerListener.this.wait(
                                POLLING_INTERVAL_MS);
                        } catch (InterruptedException ex) {
                            return;
                        }
                        if (stopped) {
                            return;
                        }
                    }
                    newEvents();
                }
            }
        };
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void setInterestingTime(long t) {
        interestingTime = t;
    }

    public long getInterestingTime() {
        return interestingTime;
    }

    /**
     * Stops the worker thread.
     */
    public void stop() {
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            notifyAll();
        }

        /*
         * Wait for the worker thread to exit before returning. Otherwise, we
         * might start again and set stopped to false before the old thread
         * notices the stop request.
         */
        try {
            workerThread.join();
        } catch (InterruptedException e) {
        }
        workerThread = null;
    }

    /**
     * Method overridden to perform the work when a new event is available.
     * The implementation is free to lock resources and make remote calls.
     */
    protected abstract void newEvents();
}
