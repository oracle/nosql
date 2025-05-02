/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common;

import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A simple logger used to limit the rate at which messages related to a
 * specific object are logged to at most once within the configured time
 * period. The effect of the rate limited logging is to sample log messages
 * associated with the object.
 *
 * This type of logging is suitable for informational messages about the state
 * of some entity that may persist over some extended period of time, e.g. a
 * repeated problem communicating with a specific node, where the nature of the
 * problem may change over time.
 *
 * @param <T> the type of the object associated with the log message.
 */
public class RateLimitingLogger<T> {
    private RateLimiting<T> limiter;
    private final Logger logger;

    /**
     * Constructs a configured RateLimitingLoggerInstance.
     *
     * @param logSamplePeriodMs used to compute the max rate of
     *         1 message/logSamplePeriodMs
     * @param maxObjects the max number of MRU objects to track
     *
     * @param logger the rate limited messages are written to this logger
     */
    public RateLimitingLogger(final int logSamplePeriodMs,
                              final int maxObjects,
                              final Logger logger) {

        limiter = new RateLimiting<T>(logSamplePeriodMs, maxObjects);
        this.logger = logger;
    }

    /* For testing */
    public long getLimitedMessageCount() {
        return limiter.getLimitedMessageCount();
    }


    /* For testing */
    int getMapSize() {
        return limiter.getMapSize();
    }

    /**
     * Logs the message, if one has not already been logged for the object
     * in the current time interval.
     *
     * @param object the object associated with the log message
     *
     * @param level the level to be used for logging
     *
     * @param string the log message string
     */
    public void log(T object, Level level, String string) {

        if (!logger.isLoggable(level)) {
            return;
        }
        if (object == null || limiter.isHandleable(object)) {
            logger.log(level, string);
        }
    }

    /**
     * Logs the message, if one has not already been logged for the object
     * in the current time interval.
     *
     * @param object the object associated with the log message
     *
     * @param level the level to be used for logging
     *
     * @param   msgSupplier   A function, which when called, produces the
     *                        desired log message
     */
    public void log(T object, Level level, Supplier<String> msgSupplier) {

        if (!logger.isLoggable(level)) {
            return;
        }
        if (object == null || limiter.isHandleable(object)) {
            logger.log(level, msgSupplier.get());
        }
    }

    /**
     * Logs the message, if one has not already been logged for the object
     * in the current time interval.
     *
     * @param object the object associated with the log message
     * @param level the level to be used for logging
     * @param  thrown  Throwable associated with log message
     * @param  msgSupplier A function, which when called, produces the
     *                     desired log message
     */
    public void log(T object,
                    Level level,
                    Throwable thrown,
                    Supplier<String> msgSupplier) {

        if (!logger.isLoggable(level)) {
            return;
        }
        if (object == null || limiter.isHandleable(object)) {
            logger.log(level, msgSupplier.get(), thrown);
        }
    }

    public Logger getInternalLogger() {
        return logger;
    }
}
