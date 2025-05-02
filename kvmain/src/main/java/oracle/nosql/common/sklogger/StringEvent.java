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

package oracle.nosql.common.sklogger;

import java.io.Serializable;
import java.util.logging.Level;

/**
 * StringEvent is one kind of monitor stats data that used for String event.
 */
public class StringEvent implements StatsData, Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String subject;
    private final String message;
    private final Level level;
    private final long reportTimeMs;
    private final Throwable thrown;

    /**
     * Create a Event that can be logged and collected by Monitor system.
     * @param name specify the Event category
     * @param level specify the Event level
     * @param subject specify the Event subject that may be used as email
     * subject, so all lines in the subject will be combined to one line. It
     * can't be empty.
     * @param message is the Event detail message. It can be null.
     * @param thrown is the relevant exception for this Event. It can be null.
     */
    public StringEvent(String name,
                       Level level,
                       String subject,
                       String message,
                       Throwable thrown) {

        this.name = name;
        this.level = level;
        if (subject == null || subject.isEmpty()) {
            throw new IllegalArgumentException("subject can't be empty");
        }
        // replace new line by whitespace.
        subject = subject.replaceAll("(\\r|\\n|\\r\\n)+", " ");
        this.subject = subject;
        this.message = message;
        this.thrown = thrown;
        reportTimeMs = System.currentTimeMillis();
    }

    @Override
    public String getStatsName() {
        return name;
    }

    public String getSubject() {
        return subject;
    }

    public String getMessage() {
        return message;
    }

    public Level getLevel() {
        return level;
    }

    public long getReportTimeMs() {
        return reportTimeMs;
    }

    public Throwable getThrown() {
        return thrown;
    }
}
