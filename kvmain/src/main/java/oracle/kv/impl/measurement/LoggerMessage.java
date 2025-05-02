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

package oracle.kv.impl.measurement;

import java.io.Serializable;
import java.util.Objects;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

import oracle.kv.impl.monitor.Metrics;

/**
 * A wrapper for a java.util.logging message issued at a service, which
 * is forwarded to the AdminService to display in the store-wide consolidated
 * view.
 *
 * Note that, to avoid pinning message parameters, the constructor converts
 * message parameters to strings. That is already what LogRecords do during
 * serialization, so this approach just means that serialized/deserialized and
 * original objects will behave the same way.
 */
public class LoggerMessage implements Measurement, Serializable {

    // TODO: Is it too heavyweight to send the LogRecord? An alternative is to
    // send the message level, timestamp and string.

    private static final long serialVersionUID = 1L;

    /**
     * A log record formatter so that message parameters can get substituted in
     * the string output for a LoggerMessage object.
     */
    private static final Formatter logRecordFormatter = new SimpleFormatter();

    private final LogRecord logRecord;

    public LoggerMessage(LogRecord logRecord) {
        this.logRecord = logRecord;

        /*
         * Replace parameters with their string versions to avoid pinning
         * objects. Note that, although it is not documented,
         * LogRecord.getParameters returns the underlying array, so we can
         * modify it in place. [KVSTORE-115]
         */
        final Object[] parameters = logRecord.getParameters();
        assert parameters == logRecord.getParameters();
        if (parameters != null) {
            for (int i = 0; i < parameters.length; i++) {
                parameters[i] = Objects.toString(parameters[i], null);
            }
        }
    }

    @Override
    public int getId() {
        return Metrics.LOG_MSG.getId();
    }

    @Override
    public String toString() {
        return logRecordFormatter.formatMessage(logRecord);
    }

    public LogRecord getLogRecord() {
        return logRecord;
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getEnd() {
        return 0;
    }
}
