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

import java.util.logging.LogRecord;

import oracle.nosql.common.JsonBuilder;
import oracle.nosql.common.contextlogger.ContextLogManager.WithLogContext;

/**
 * Lumberjack formatter for logger output with support for emitting log context
 * and correlation ids.
 */
public class ContextLumberjackFormatter extends LogFormatter {

    private static final String contextLabel = "CTX";

    public ContextLumberjackFormatter(String label) {
        super(label);
    }

    public ContextLumberjackFormatter() {
        this(null);
    }

    /**
     * Format the log record in json form as below:
     *   {"ts":1563289113595,"level":"INFO","component":"xxx",
     *    "correlationId":"yyy","msg":"zzz"}
     * If an unlogged LogContext is available in WithLogContext,
     * include it as a separate line before the actual log record.
     *
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public String format(LogRecord record) {
        final StringBuilder sb = new StringBuilder();
        final long timestamp = record.getMillis();
        final String dateVal = getDate(timestamp);
        final String level = record.getLevel().getName();
        final LogContext lc = WithLogContext.get();
        String correlationId = "0";
        /* If there's a context, and it has not been logged, emit it now. */
        if (lc != null) {
            correlationId = lc.getId();
            if (! lc.isLogged()) {
                lc.setLogged();
                addContextLine(sb, lc, level, timestamp, dateVal);
            }
        }

        final JsonBuilder jsonRoot = JsonBuilder.create(true);
        jsonRoot.append("ts", timestamp);
        jsonRoot.append("level", level);
        jsonRoot.append("component", label);
        if (lc != null) {
            jsonRoot.append("correlationId", correlationId);
        }
        final StringBuilder msg = new StringBuilder();
        msg.append(formatMessage(record));
        if (record.getThrown() != null) {
            msg.append(LINE_SEPARATOR);
            getThrown(record, msg);
        }
        jsonRoot.append("msg", msg.toString());
        sb.append(jsonRoot.toString());
        sb.append(LINE_SEPARATOR);
        return sb.toString();
    }

    private void addContextLine(StringBuilder sb,
                                LogContext lc,
                                String level,
                                long timestamp,
                                String date) {
        final JsonBuilder jsonRoot = JsonBuilder.create(true);
        jsonRoot.append("ts", timestamp);
        jsonRoot.append("level", level);
        jsonRoot.append("component", label);
        jsonRoot.append("correlationId", contextLabel);
        final String msg = date + " " + lc.toJsonString();
        jsonRoot.append("msg", msg);

        sb.append(jsonRoot.toString());
        sb.append(LINE_SEPARATOR);
    }
}
