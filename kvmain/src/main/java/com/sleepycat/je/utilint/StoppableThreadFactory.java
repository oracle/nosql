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

import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager.FIOStatsCollector;

/**
 * Create a thread factory that returns threads that are legitimate
 * StoppableThreads. Like StoppableThreads, if an environment is provided, the
 * threads will invalidate if an exception is not handled, and are registered
 * with the exception listener.If a logger is provided, StoppableThreads log
 * exception information.
 *
 * This factory is used in conjunction with the ExecutorService and
 * ThreadExecutorPool models.
 */
public class StoppableThreadFactory implements ThreadFactory {

    private final String threadName;
    private final Logger logger;
    private final EnvironmentImpl envImpl;

    /* The collector to be associated with threads created by this thread
     * factory.
     */
    private final FIOStatsCollector fioStatsCollector;

    /**
     * This kind of StoppableThreadFactory will cause invalidation if an
     * unhandled exception occurs.
     */
    public StoppableThreadFactory(EnvironmentImpl envImpl,
                                  String threadName,
                                  Logger logger,
                                  FIOStatsCollector fioStatsCollector) {
        this.threadName = threadName;
        this.logger = logger;
        this.envImpl = envImpl;
        this.fioStatsCollector = fioStatsCollector;
    }

    /**
     * This kind of StoppableThreadFactory will NOT cause invalidation if an
     * unhandled exception occurs, because there is no environment provided.
     */
    public StoppableThreadFactory(String threadName, Logger logger) {
        this(null, threadName, logger, null);
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return new StoppablePoolThread(envImpl, runnable, threadName, logger,
                                       fioStatsCollector);
    }

    /*
     * A fairly plain implementation of the abstract StoppableThread class,
     * for use by the factory.
     */
    private static class StoppablePoolThread extends StoppableThread {
        private final Logger logger;

        StoppablePoolThread(EnvironmentImpl envImpl,
                            Runnable runnable,
                            String threadName,
                            Logger logger,
                            FIOStatsCollector fioStatsCollector) {
            super(envImpl, null, runnable, threadName, fioStatsCollector);
            this.logger = logger;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }
}

