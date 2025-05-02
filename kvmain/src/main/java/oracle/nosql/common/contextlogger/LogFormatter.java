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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Formatter for java.util.logging output.
 */
public class LogFormatter extends Formatter {

    protected static final String LINE_SEPARATOR =
        System.getProperty("line.separator");

    private final Date date;
    private final DateFormat formatter;
    protected final String label;

    public LogFormatter(String label) {

        date = new Date();
        formatter = FormatUtils.getDateTimeAndTimeZoneFormatter();
        if (label != null) {
            /*
             * In order to parse the log easily, all whitespace in label will
             * be removed.
             */
            label = label.replaceAll("\\s", "");
            this.label = " [" + label + "] ";
        } else {
            this.label = " - ";
        }
    }

    public LogFormatter() {
        this(null);
    }

    /* The date and formatter are not thread safe. */
    protected synchronized String getDate(long millis) {
        date.setTime(millis);

        return formatter.format(date);
    }

    /**
     * Format the log record in this form:
     *   <short date> <short time> <message level> <message>
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public String format(LogRecord record) {
        final StringBuilder sb = new StringBuilder();

        final String dateVal = getDate(record.getMillis());
        sb.append(dateVal);
        sb.append(" ");
        sb.append(record.getLevel().getLocalizedName());
        sb.append(label);
        sb.append(formatMessage(record));
        sb.append(LINE_SEPARATOR);

        getThrown(record, sb);

        return sb.toString();
    }

    protected void getThrown(LogRecord record, StringBuilder sb) {
        if (record.getThrown() != null) {
            try {
                final StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                record.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(sw.toString());
            } catch (Exception ex) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }
}
