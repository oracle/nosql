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

package oracle.kv.impl.diagnostic;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.TimeZone;

/**
 * A bean class to store log item of kvstore.
 */

public class LogInfo {
    private String BLANKSPACE_SEPARATOR = " ";

    private String logItem;
    private String timestampString;
    private Date logTimestamp;

    /* Date format of log item time stamp */
    private SimpleDateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");

    public LogInfo(String logItem) {
        this.logItem = logItem;
        try {
            parse();
        } catch (ParseException ex) {
            /*
             * Almost log items have time stamp, but some lines maybe do not
             * contain time stamp, it causes n ParseException. The exception is
             * expected and harmless
             */
        }
    }

    /**
     * Extract time stamp string from log item and convert it to Date type
     */
    private void parse() throws ParseException {
        String[] dateStr = logItem.split(BLANKSPACE_SEPARATOR);
        /*
         * A time stamp string has 3 parts, so a log time should be split as
         * at least 3 parts
         */
        if (dateStr.length >= 3) {
            dateFormat.setTimeZone(TimeZone.getTimeZone(dateStr[2]));
            logTimestamp = dateFormat.parse(dateStr[0] + BLANKSPACE_SEPARATOR +
                                            dateStr[1]);

            timestampString = dateStr[0] + BLANKSPACE_SEPARATOR + dateStr[1] +
                              BLANKSPACE_SEPARATOR + dateStr[2];
        }
    }

    public Date getTimestamp() {
        return logTimestamp;
    }

    public String getTimestampString() {
        return timestampString;
    }

    @Override
    public String toString() {
        return logItem;
    }

    /**
     * Compare whether two LogInfos are equal
     *
     * @param logInfo compared LogInfo
     */
    public boolean equals(LogInfo logInfo) {
        return this.logItem.equals(logInfo.logItem);
    }

    /**
     * LogInfo Comparator is to indicate that LogInfos are ordered
     * by its time stamp
     */
    public static class LogInfoComparator implements
            Comparator<LogInfo> {

        @Override
        public int compare(LogInfo o1, LogInfo o2) {
            if (o1.getTimestamp().after(o2.getTimestamp())) {
                return 1;
            } else if (o1.getTimestamp().before(o2.getTimestamp())) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
