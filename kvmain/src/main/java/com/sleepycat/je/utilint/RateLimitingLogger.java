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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.util.TimeSupplier;


/**
 * A simple logger used to limit the rate at which messages related to a
 * specific object are logged to at most once within the configured time
 * period. The effect of the rate limited logging is to sample log messages
 * associated with the object.
 * <p>
 * This type of logging is suitable for informational messages about the state
 * of some entity that may persist over some extended period of time, e.g. a
 * repeated problem communicating with a specific node, where the nature of the
 * problem may change over time.
 * <p>
 * When using this class it may be desirable to enable logging of all messages
 * without using a different logger. This may be done by passing zero for the
 * maxObjects param of the constructor, or by passing null for the object param
 * of the log method.
 *
 * @param <T> the type of the object associated with the log message.
 */
public class RateLimitingLogger<T> {
    /**
     * Contains the objects that had messages last logged for them and the
     * associated time that it was last logged.
     */
    private final Map<T, Long> logEvents;

    /**
     *  The log message sampling period.
     */
    private final int logSamplePeriodMs;

    /* The number of log messages that were actually written. */
    private long limitedMessageCount = 0;

    private final Logger logger;

    private final String prefix;

    /**
     * Constructs a configured RateLimitingLoggerInstance.
     *
     * @param logSamplePeriodMs used to compute the max rate of
     *         1 message/logSamplePeriodMs
     * @param maxObjects the max number of MRU objects to track, or zero to log
               messages unconditionally
     *
     * @param logger the rate limited messages are written to this logger
     */
    public RateLimitingLogger(final int logSamplePeriodMs,
                              final int maxObjects,
                              final Logger logger) {
    	this(logSamplePeriodMs, maxObjects, logger, "");
    }

    /**
     * Constructs a configured RateLimitingLoggerInstance.
     *
     * @param logSamplePeriodMs used to compute the max rate of
     *         1 message/logSamplePeriodMs
     * @param maxObjects the max number of MRU objects to track, or zero to log
               messages unconditionally
     *
     * @param logger the rate limited messages are written to this logger
     *
     * @param prefix a string to add to the beginning of each log message.
     */
    @SuppressWarnings("serial")
    public RateLimitingLogger(final int logSamplePeriodMs,
                              final int maxObjects,
                              final Logger logger,
                              final String prefix) {

        this.logSamplePeriodMs = logSamplePeriodMs;
        this.logger = logger;
        this.prefix = prefix;

        logEvents = new LinkedHashMap<T,Long>(9) {
            @Override
            protected boolean
            removeEldestEntry(Map.Entry<T, Long> eldest) {

              return size() > maxObjects;
            }
          };
    }

    /* For testing */
    public synchronized long getLimitedMessageCount() {
        return limitedMessageCount;
    }


    /* For testing */
    int getMapSize() {
        return logEvents.size();
    }

    /**
     * Logs the message, if one has not already been logged for the object
     * in the current time interval.
     *
     * @param object the object associated with the log message, or null to log
     * the message unconditionally
     *
     * @param level the level to be used for logging
     *
     * @param string the log message string
     */
    public synchronized void log(T object, Level level, String string) {
        log(object, level, () -> string);
    }

    public synchronized void log(T object, Level level, Supplier<String> supplier) {
        if (object == null) {
            logger.log(level, prefix + supplier.get());
            return;
        }

        final Long timeMs = logEvents.get(object);

        final long now = TimeSupplier.currentTimeMillis();
        if ((timeMs == null) ||
            (now > (timeMs + logSamplePeriodMs))) {
            limitedMessageCount++;
            logEvents.put(object, now);
            logger.log(level, prefix + supplier.get());
        }
    }

    public Logger getInternalLogger() {
        return logger;
    }
}
