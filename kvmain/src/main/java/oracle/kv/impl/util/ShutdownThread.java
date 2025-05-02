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
package oracle.kv.impl.util;

import java.util.logging.Level;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.StoppableThread;
import java.util.logging.Logger;

/**
 * A StoppableThread with support for soft shutdown. The proper way to
 * stop this thread is to call the shutdown() method.
 *
 * The shutdown call will set the shutdown state and wait for the thread to
 * exit, interrupting it if necessary if the wait times out.
 *
 * Long running threads, or threads that wait, should check isShutdown()
 * frequently so that they may exit quickly.
 *
 * The default shutdown timeout, DEFAULT_SHUTDOWN_TIMEOUT_MS, can be changed
 * by overriding initiateSoftShutdown().
 */
public abstract class ShutdownThread extends StoppableThread {

    /* Time to wait for a thread to exit during shutdown */
    protected static final int DEFAULT_SHUTDOWN_TIMEOUT_MS = 10000;

    protected ShutdownThread(String name) {
        super(name);
    }

    protected ShutdownThread(EnvironmentImpl envImpl,
                             UncaughtExceptionHandler handler,
                             String name) {
        super(envImpl, handler, name);
    }

    /**
     * Shuts down this thread. If shutdown is not already in progress, the
     * shutdown state is set (isShutdown() will return true), any threads
     * waiting will be woken up, and the call will wait until the thread
     * exits.
     *
     * If the thread does not exit in DEFAULT_SHUTDOWN_TIMEOUT_MS the thread
     * will be interrupted.
     *
     * Returns immediately if shutdown is already in progress. If the caller
     * is this thread returns after setting the shutdown state.
     */
    public void shutdown() {
        if (shutdownDone(getLogger())) {
            /* Already in shutdown */
            return;
        }
        assert isShutdown();

        /* Soft shutdown */
        notifyWaiters();
        super.shutdownThread(getLogger());
    }

    /**
     * Wakes up any threads waiting on this thread. This is called during
     * soft shutdown (isShutdown() will return true).
     */
    public synchronized void notifyWaiters() {
        notifyAll();
    }

    /**
     * Waits the specified amount of time. Returns after waiting the desired
     * time, or there is an interrupt during shutdown. If the thread is in
     * shutdown (isShutdown() return true) the call returns immediately.
     *
     * Callers should check for shutdown after this call returns.
     *
     * @param timeMS time in MS to wait
     * @throws InterruptedException
     */
    protected synchronized void waitForMS(long timeMS)
            throws InterruptedException {
        if (isShutdown()) {
            return;
        }
        try {
            wait(timeMS);
        } catch (InterruptedException ie) {
            /*
             * The wait may be interrupted by shutdown in which case return,
             * otherwise log exception and re-throw.
             */
            if (!isShutdown()) {
                getLogger().log(Level.SEVERE,
                                "Unexpected interrupt in {0}: {1}",
                                new Object[]{this, ie});
                throw ie;
            }
        }
    }

    /* -- From StoppableThread -- */

    /**
     * Called during shutdown (isShutdown returns true) after waiters have been
     * notified. Returns the shutdown timeout. The default implementation
     * returns DEFAULT_SHUTDOWN_TIMEOUT_MS. It may be overridden to perform
     * additional shutdown activity or to return a different shutdown timeout.
     *
     * This method should not be called directly. Call shutdown() to shutdown
     * the thread.
     */
    @Override
    protected int initiateSoftShutdown() {
        if (!isShutdown()) {
            throw new IllegalStateException("Use ShutdownThread.shutdown()");
        }
        return DEFAULT_SHUTDOWN_TIMEOUT_MS;
    }

    /**
     * This method should not be called directly. Call shutdown() to shutdown
     * the thread.
     */
    @Override
    public final void shutdownThread(Logger logger) {
        throw new IllegalStateException("Use ShutdownThread.shutdown()");
    }
}
