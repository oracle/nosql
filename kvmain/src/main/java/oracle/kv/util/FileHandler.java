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

package oracle.kv.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import oracle.kv.impl.util.server.LoggerUtils;

/**
 * KVStore instances of java.util.logging.Logger are configured to use this
 * implementation of java.util.logging.ConsoleHandler. By default, the
 * handler's level is {@link Level#INFO}. To enable the console output, use the
 * standard java.util.logging.LogManager configuration to set the desired
 * level:
 * <pre>
 * oracle.kv.util.FileHandler.level=FINE
 * </pre>
 */
public class FileHandler extends java.util.logging.FileHandler {

    private final int limit;
    private final int count;

    /** If non-null, a message to log at the beginning of each log file. */
    private volatile String initialMessage;

    /** If non-null, a record to be logged first by publish. */
    private volatile LogRecord extraLogRecord;

    /*
     * Using a KV specific handler lets us enable and disable output for
     * all kvstore component.
     */
    public FileHandler(String pattern, int limit, int count, boolean append)
        throws IOException, SecurityException {

        super(pattern, limit, count, append);

        this.limit = limit;
        this.count = count;
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

    /** Returns the requested file count. */
    public int getCount() {
        return count;
    }

    /** Returns the requested file limit. */
    public int getLimit() {
        return limit;
    }

    /**
     * If non-null, specifies a message that should be logged at the start of a
     * a log file after files are rotated.
     */
    public void setInitialMessage(String msg) {
        initialMessage = msg;
    }

    /**
     * Returns the message the should be logged at the start of a log file
     * after files are rotated, or null for no message.
     */
    public String getInitialMessage() {
        return initialMessage;
    }

    /**
     * Arrange to print the initial message, if any.
     */
    @Override
    protected synchronized void setOutputStream(OutputStream out) {
        super.setOutputStream(out);
        if ((initialMessage != null) && (extraLogRecord == null)) {
            extraLogRecord = new LogRecord(Level.INFO, initialMessage);
        }
    }

    /**
     * Print the extra log record, if any.
     */
    @Override
    public synchronized void publish(LogRecord record) {
        if (extraLogRecord != null) {
            final LogRecord lr = extraLogRecord;
            extraLogRecord = null;
            super.publish(lr);
        }
        super.publish(record);
    }

    /**
     * When a handler is closed, we want to remove it from any associated
     * loggers, so that a handler will be created again if the logger is
     * resurrected.
     */
    @Override
    public synchronized void close() {
        super.close();
        LoggerUtils.removeHandler(this);
    }
}

