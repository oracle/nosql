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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.security.ExecutionContext;

/**
 * KVStore threads should have these characteristics:
 * - have a descriptive name that identifies it as a KVStore owned thread
 * - run as a daemon
 * - have an uncaught exception handler. By default, uncaught exceptions should
 *   log the exception into their local log.
 */
public class KVThreadFactory implements ThreadFactory {

    /* Factory name */
    private final String name;

    /* Used for logging uncaught exceptions. */
    private final Logger logger;
    private final AtomicInteger id;

    /**
     * @param name factory name, returned by getName()
     * @param exceptionLogger should be attached to a handler that is always
     * available to record uncaught exceptions. For example, it should at least
     * be able to preserve the uncaught exception in the kvstore component's
     * log file.
     */
    public KVThreadFactory(String name, Logger exceptionLogger) {
        this.name = name;
        this.logger = exceptionLogger;
        this.id = new AtomicInteger();
    }

    /**
     * Name of thread. All KVThreadFactory and subclasses prefix the
     * thread name with "KV". Other uses may want to override this method to
     * provide more specific names, such as KVMonitorCollector. Subclasses
     * need only specify the suffix, such as "MonitorCollector" in this
     * method.
     */
    public String getName() {
        return name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = newThreadInternal(r, "KV" + getName() + "_" +
                                     id.incrementAndGet());
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(makeUncaughtExceptionHandler());
        return t;
    }

    protected Thread newThreadInternal(Runnable r, String threadName) {
        return new Thread(r, threadName);
    }

    /**
     * By default, these threads log uncaught exceptions. Can be overwritten
     * to provide different handling.
     */
    public Thread.UncaughtExceptionHandler makeUncaughtExceptionHandler() {
        return new LogUncaughtException();
    }

    private class LogUncaughtException
        implements Thread.UncaughtExceptionHandler {

        /**
         * TODO: this method hard codes the logger level. Is that okay, or is
         * more flexibility needed?
         */
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (logger != null) {
                logger.log(Level.SEVERE,
                           t + " experienced this uncaught exception " + e,
                           e);
            }
        }
    }

    /**
     * Thread factory that wraps the running of a task with an execution
     * context to facilitate any security check in KVStore security system.
     */
    public static class KVPrivilegedThreadFactory extends KVThreadFactory {
        private final ExecutionContext execCtx;

        public KVPrivilegedThreadFactory(String name,
                                         ExecutionContext execCtx,
                                         Logger exceptionLogger) {
            super(name, exceptionLogger);
            this.execCtx = execCtx;
        }

        @Override
        public Thread newThread(final Runnable r) {
            return super.newThread(new Runnable() {
                @Override
                public void run() {
                    if (execCtx == null) {
                        r.run();
                    } else {
                        ExecutionContext.runWithContext(
                            (Runnable) () -> r.run(), execCtx);
                    }
                }
            });
        }
    }
}
