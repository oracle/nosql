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

package oracle.kv.impl.util.client;

import java.util.logging.Handler;
import java.util.logging.Logger;

import oracle.nosql.common.contextlogger.LogFormatter;

/**
 * Client-only utilities for creating and formatting java.util loggers and
 * handlers.
 */
public class ClientLoggerUtils {

    /** The name of the root KV logger. */
    private static final String KV_PKG = "oracle.kv";

    /**
     * Configure the root KV loggers.  This logger is named "oracle.kv", the
     * root package of all KV classes, so it is the parent of all KV loggers.
     * Install a console handler for this logger, with our log formatter, so
     * that console logging output for our classes uses our log format.
     * Disable parent handlers for this logger so that KV loggers by default
     * only use our console handler and not any handlers configured by users.
     * In particular, that means that if the standard Java console logger is
     * enabled, which it is by default, we prevent output going to both it and
     * our console logger, which would produce duplicate output.  Users can
     * configure their own loggers for all KV classes by specifying handlers
     * for the "oracle.kv" logger by specifying one or more space or comma
     * separated values for the "oracle.kv.handlers" property in their
     * application's logging configuration file.  Doing this is particularly
     * useful if they want standard Java file logging for our classes.
     */
    static {
        Logger logger = Logger.getLogger(KV_PKG);
        logger.setUseParentHandlers(false);
        Handler handler = new oracle.kv.util.ConsoleHandler();
        handler.setFormatter(new LogFormatter(null));
        logger.addHandler(handler);
    }

    /**
     * Obtain a logger in the "oracle.kv" hierarchy, which will be configured
     * to sends output to the console using our standard log format.
     */
    public static Logger getLogger(Class<?> cl, String resourceId) {
        final String className = cl.getName();

        /* Make sure this is a KV logger */
        if (!className.startsWith(KV_PKG + ".")) {
            throw new IllegalArgumentException("Logger not in KV package: " +
                                               className);
        }
        return Logger.getLogger(className + "." + resourceId);
    }
}
