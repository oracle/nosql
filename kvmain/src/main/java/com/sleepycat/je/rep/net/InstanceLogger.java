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

package com.sleepycat.je.rep.net;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.function.Supplier;

/**
 * The InstanceLogger interface provides a basic logging interface.
 */
public interface InstanceLogger {

    /**
     * Logs a message at the specified logging level. The message is prefixed
     * with an instance-dependent identifier.
     *
     * @param logLevel the logging level at which the message should be logged.
     * @param msg a string to be logged.
     */
    public void log(Level logLevel, String msg);

    /**
     * Logs a message at the specified logging level. The message is prefixed
     * with an instance-dependent identifier.
     *
     * @param logLevel the logging level at which the message should be logged.
     * @param supplier a string supplier to be logged.
     */
    public void log(Level logLevel, Supplier<String> supplier);

    /**
     * Checks if a message of the given level would actually be logged by this logger.
     *
     * @param logLevel a message logging level
     * @return {@code ture} if the given message level is currently being
     * logged
     */
    public boolean isLoggable(Level logLevel);

    /**
     * Converts a Java logger to an InstanceLogger.
     *
     * @param logger the Java logger
     * @return an InstanceLogger implemented by the Java logger
     */
    public static InstanceLogger fromLogger(Logger logger) {
        return new InstanceLogger() {
            @Override
            public void log(Level logLevel, String msg) {
                logger.log(logLevel, msg);
            }
            @Override
            public void log(Level logLevel, Supplier<String> supplier) {
                logger.log(logLevel, supplier);
            }
            @Override
            public boolean isLoggable(Level logLevel) {
                return logger.isLoggable(logLevel);
            }
        };
    }
}
