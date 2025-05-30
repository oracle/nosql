/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.util;

import oracle.nosql.common.sklogger.SkLogger;

import java.io.File;
import java.lang.Thread;

/**
 * A class to manage graceful shutdown flag based on a file.
 *
 * This is a singleton class. It does nothing unless the following
 * system property is set:
 *
 * proxy.shutdownfile
 *  - full path to a file to watch for. If this file is created/modified,
 *    inShutdown() will return true.
 *
 * optional: proxy.shutdowninterval
 *  - time in milliseconds between checks for shutdown file. Default=500ms.
 */
public class ShutdownManager {

    /* main value. This doesn't need synchronization */
    private boolean inShutdown = false;

    /* file to watch for */
    private File shutdownFile;

    /* daemon thread to loop watching for file */
    private Thread fileWatcherThread = null;

    /* flag for shutting down daemon thread */
    private boolean terminated = false;

    /* singleton instance */
    private static ShutdownManager instance;

    private Runnable shutdownHook;

    private ShutdownManager(boolean checkProperties,
                            SkLogger logger) {
        if (checkProperties == false) {
            /* empty constructor to avoid exceptions */
            return;
        }

        /* location of shutdown file */
        String shutdownFilename = System.getProperty("proxy.shutdownfile");
        if (shutdownFilename == null || shutdownFilename.isEmpty()) {
            return;
        }

        shutdownFile = new File(shutdownFilename);
        terminated = false;

        /* interval between shutdown file checks, in ms */
        long ms = Long.getLong("proxy.shutdowninterval", 500);
        if (ms < 100) {
            ms = 100;
        }
        if (ms > 10_000) {
            ms = 10_000;
        }
        /* final for lambda */
        final long checkIntervalMs = ms;

        /* start a background thread to watch for file */
        fileWatcherThread = new Thread(() -> {
            while (terminated == false) {
                try {
                    Thread.sleep(checkIntervalMs);
                } catch (InterruptedException e) {
                    inShutdown = true;
                    break;
                }
                /*
                 * If file exists and is less than 2*checkIntervalMs old,
                 * set inShutdown flag and exit thread.
                 */
                long modTime = 0;
                try {
                    modTime = shutdownFile.lastModified();
                } catch (SecurityException e) {
                    logger.fine("Exception looking for " +
                                shutdownFilename + ": " + e.getMessage(), null);
                }
                modTime += (2 * checkIntervalMs);
                if (modTime > System.currentTimeMillis()) {
                    inShutdown = true;
                    logger.info("Found shutdown file " + shutdownFilename +
                                ": shutting down.", null);
                    if (shutdownHook != null) {
                        shutdownHook.run();
                    }
                    break;
                }
            }
            }, "shutdownManagerThread");
        fileWatcherThread.setDaemon(true);
        fileWatcherThread.start();
        logger.info("Created ShutdownManager watching for " +
                    shutdownFilename + " file every " +
                    checkIntervalMs + "ms", null);
    }

    public static ShutdownManager getInstance(SkLogger logger) {
        if (instance == null) {
            synchronized (ShutdownManager.class) {
                if (instance == null) {
                    try {
                        instance = new ShutdownManager(true, logger);
                    } catch (Exception e) {
                        logger.info("Could not create ShutdownManager: " +
                                    e.getMessage() +
                                    ": creating empty manager.");
                        instance = new ShutdownManager(false, null);
                    }
                }
            }
        }
        return instance;
    }

    public boolean inShutdown() {
        return inShutdown;
    }

    public void shutdown() {
        if (terminated) {
            return;
        }
        inShutdown = true;
        terminated = true;
        if (fileWatcherThread != null) {
            fileWatcherThread.interrupt();
        }
    }

    public void setShutdownHook(Runnable hook) {
        shutdownHook = hook;
    }
}
