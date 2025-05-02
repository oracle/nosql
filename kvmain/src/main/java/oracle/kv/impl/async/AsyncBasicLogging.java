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

package oracle.kv.impl.async;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.RateLimitingLogger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A simple base class that provides support for logging.
 */
abstract class AsyncBasicLogging {
    protected final Logger logger;
    protected final RateLimitingLogger<String> rateLimitingLogger;

    protected AsyncBasicLogging(Logger logger) {
        this.logger = checkNull("logger", logger);
        this.rateLimitingLogger =
            new RateLimitingLogger<>(
                5 * 60 * 1000 /* 5 minutes */,
                10 /* types of exception classes */,
                logger);
    }

    /**
     * Returns information that should be logged for the specified exception,
     * which can be null, only including stack traces if FINEST logging is
     * enabled.
     */
    protected String getExceptionLogging(@Nullable Throwable exception) {
        return getExceptionLogging(exception, Level.FINEST);
    }

    /**
     * Returns information that should be logged for the specified exception,
     * which can be null, only including stack traces if the specified logging
     * level is enabled.
     */
    protected String getExceptionLogging(@Nullable Throwable exception,
                                         Level stackTraceLevel) {
        if (exception == null) {
            return "";
        }
        return " exception=" +
            (logger.isLoggable(stackTraceLevel) ?
             CommonLoggerUtils.getStackTrace(exception) :
             exception);
    }

    /** Returns an abbreviated version of the name of this class. */
    protected String getAbbreviatedClassName() {
        return getAbbreviatedClassName(getClass());
    }

    /**
     * Returns an abbreviated version of the name of the specified class.
     */
    static String getAbbreviatedClassName(Class<?> cl) {

        /*
         * Don't use Class.getSimpleName, because that returns an empty string
         * for anonymous classes
         */
        final String className = cl.getName();
        final int lastDot = className.lastIndexOf('.');
        return (lastDot < 0) ? className : className.substring(lastDot+1);
    }

    /**
     * Logs a warning for an unexpected exception.
     */
    protected void warnUnexpected(Throwable e,
                                  Supplier<String> msgSupplier) {
        final String exceptionClass =
            e.getClass().getSimpleName()
            + "."
            + ((e.getCause() == null)
               ? "null"
               : e.getCause().getClass().getSimpleName());
        rateLimitingLogger.log(
            exceptionClass, Level.WARNING, msgSupplier);
    }
}
