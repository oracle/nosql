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

import java.io.Closeable;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * This is a LogManager that behaves exactly like the default LogManager unless
 * the WithLogContext threadlocal is set.  In the latter case it implements the
 * context logging behavior that overrides the default isLoggable method so
 * that a LogContext, if provided, can determine the logging level for a log
 * call.  Also, in conjunction with ContextFormatter, it provides for emitting
 * a correlationId with each log message for which a context is provided.
 */
public class ContextLogManager extends LogManager {
    @Override
    public synchronized Logger getLogger(String name) {
        Logger logger = super.getLogger(name);
        if (logger == null) {
            logger = new ContextLogger(name, null);
            super.addLogger(logger);
        }

        return logger;
    }

    public static class ContextLogger extends Logger {

        public ContextLogger(String name, String resourceBundleName) {
            super(name, resourceBundleName);
        }

        @Override
        public boolean isLoggable(Level level) {
            if (super.isLoggable(level)) {
                return true;
            }
            return isLoggableWithContext(level);
        }
    }

    public static boolean isLoggableWithContext(Level recordLevel) {
        final LogContext lc = WithLogContext.get();
        if (lc == null) {
            /*
             * If no log context is set, then behave as a regular Logger.
             */
            return false;
        }

        final int contextLevelValue = lc.getLogLevel();
        if (recordLevel.intValue() < contextLevelValue) {
            return false;
        }
        /* Allow the context's log level override the Logger's. */
        return true;
    }

    public static boolean isLoggableWithContext(LogRecord record) {
        return isLoggableWithContext(record.getLevel());
    }

    /**
     * The presence of a WithLogContext object on the stack enables the
     * special behavior that allows a LogContext to override the 
     * Logger's log level in favor of a LogContext's.
     */
    public static class WithLogContext implements Closeable {

        private static final ThreadLocal<LogContext> local =
            new ThreadLocal<LogContext>();

        public WithLogContext(LogContext c) {
            set(c);
        }

        @Override
        public void close() {
            unset();
        }

        private static void set(LogContext c) {
            local.set(c);
        }

        private static void unset() {
            local.remove();
        }

        public static LogContext get() {
            return local.get();
        }
    }
}
