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

package oracle.kv.impl.monitor;

import java.util.logging.Handler;
import java.util.logging.Level;

import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Sends logging output to the Monitor.
 */
public abstract class LogToMonitorHandler extends Handler {

    public LogToMonitorHandler() {
        Level level = null;
        String propertyName = getClass().getName() + ".level";
        String levelProperty = LoggerUtils.getLoggerProperty(propertyName);

        if (levelProperty == null) {
            level = Level.ALL;
        } else {
            level = Level.parse(levelProperty);
        }
        setLevel(level);
    }

    /**
     * When a handler is closed, we want to remove it from any associated
     * loggers, so that a handler will be created again if the logger is
     * ressurected.
     */
    @Override
    public void close()  {
        LoggerUtils.removeHandler(this);
    }

    @Override
    public void flush() {
        /* Nothing to do. */
    }
}
