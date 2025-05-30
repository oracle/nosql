/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.nosql.common.sklogger.SkLogger;

/**
 * Logger extends SkLogger to sanitize the log message.
 */
public class ProxyLogger extends SkLogger {

    private static final String eol = System.getProperty("line.separator");
    private static final Pattern eolPattern = Pattern.compile(eol);
    private static final String eolWithIndent = eol + "  ";

    public ProxyLogger(String loggerName,
                       String componentId,
                       String fileName) {
        super(loggerName, componentId, fileName);
    }

    @Override
    public void log(Level level, String msg) {
        if (isLoggable(level)) {
            super.log(level, sanitize(msg));
        }
    }

    @Override
    public void log(Level level, String msg, Throwable thrown) {
        if (isLoggable(level)) {
            super.log(level, sanitize(msg), thrown);
        }
    }

    /**
     * Sanitizes the message in multiple lines by indenting every line except
     * the first by two spaces.
     */
    private String sanitize(String msg) {
        if (!msg.contains(eol)) {
            return msg;
        }
        Matcher matcher = eolPattern.matcher(msg);
        return matcher.replaceAll(eolWithIndent);
    }
}
