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

import oracle.nosql.common.contextlogger.ContextLogManager.WithLogContext;

/**
 * Formatter for logger output with support for emitting log context and
 * correlation ids.
 */
public class ContextFormatter extends LogFormatter {

    private static final String contextLabel = "CTX";

    public ContextFormatter(String label) {
        super(label);
    }

    public ContextFormatter() {
        this(null);
    }

    /**
     * Format the log record in this form:
     *   <short date> <short time> <message level> <label> <correlationid> <message>
     * If an unlogged LogContext is available in WithLogContext,
     * include it as a separate line before the actual log record.
     *
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public String format(LogRecord record) {
        final StringBuilder sb = new StringBuilder();

        final String dateVal = getDate(record.getMillis());
        final LogContext lc = WithLogContext.get();
        final String level = record.getLevel().getLocalizedName();
        String correlationId = "0";

        /* If there's a context, and it has not been logged, emit it now. */
        if (lc != null) {
            correlationId = lc.getId();
            if (! lc.isLogged()) {
                lc.setLogged();
                addContextLine(sb, lc, level, dateVal);
            }
        }

        sb.append(dateVal);
        sb.append(" ");
        sb.append(level);
        sb.append(label);
        if (lc != null) {
            sb.append(correlationId);
            sb.append(" ");
        }
        sb.append(formatMessage(record));
        sb.append(LINE_SEPARATOR);

        getThrown(record, sb);

        return sb.toString();
    }

    private void addContextLine(StringBuilder sb,
                                  LogContext lc,
                                  String level,
                                  String date) {
        sb.append(date);
        sb.append(" ");
        sb.append(level);
        sb.append(label);
        sb.append(contextLabel);
        sb.append(" ");
        sb.append(lc.toJsonString());
        sb.append(LINE_SEPARATOR);
    }
}
