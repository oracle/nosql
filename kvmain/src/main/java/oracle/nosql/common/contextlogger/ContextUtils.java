/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.contextlogger;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.common.contextlogger.ContextLogManager.WithLogContext;

/**
 * Helper methods for logging with context in the absence of SkLogger.
 * TODO: add methods as needed for logger.log overloadings that take parameters,
 * Throwable, and so forth.
 */
public class ContextUtils {

    public static void logWithCtx(Logger logger, Level level,
                                  String msg, LogContext lc) {

        if (lc == null) {
            logger.log(level, msg);
        } else {
            WithLogContext wlc = new WithLogContext(lc);
            try {
                logger.log(level, msg);
            } finally {
                wlc.close();
            }
        }
    }

    /**
     * @see Logger#severe(String)
     */
    public static void severeWithCtx(Logger logger, String msg, LogContext lc) {
        logWithCtx(logger, Level.SEVERE, msg, lc);
    }

    /**
     * @see Logger#warning(String)
     */
    public static void warningWithCtx(Logger logger, String msg, LogContext lc) {
        logWithCtx(logger, Level.WARNING, msg, lc);
    }

    /**
     * @see Logger#info(String)
     */
    public static void infoWithCtx(Logger logger, String msg, LogContext lc) {
        logWithCtx(logger, Level.INFO, msg, lc);
    }

    /**
     * @see Logger#fine(String)
     */
    public static void fineWithCtx(Logger logger, String msg, LogContext lc) {
        logWithCtx(logger, Level.FINE, msg, lc);
    }

    /**
     * @see Logger#finest(String)
     */
    public static void finestWithCtx(Logger logger, String msg, LogContext lc) {
        logWithCtx(logger, Level.FINEST, msg, lc);
    }

    /**
     * @see Logger#isLoggable
     */
    public static boolean isLoggableWithCtx(Logger logger,
                                            Level level,
                                            LogContext lc) {
        if (logger.isLoggable(level)) {
            return true;
        }
        if (lc == null) {
            return false;
        }
        final int contextLevelValue = lc.getLogLevel();
        if (level.intValue() < contextLevelValue) {
            return false;
        }
        /* Allow the context's log level override the Logger's. */
        return true;
    }

}
