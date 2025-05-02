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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/*
 * Functionality in this class should be the same as in
 * oracle.kv.impl.util.FormatUtils
 */
public class FormatUtils {

    public static String defaultTimeZone = "UTC";
    private static final TimeZone tz = TimeZone.getTimeZone(defaultTimeZone);

    public static DateFormat getDateTimeAndTimeZoneFormatter() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        df.setTimeZone(tz);
        return df;
    }

    /**
     * Returns a string representation of the array. Prints 10 number each line
     * so that we can easily know the index. Used for debugging when test.
     */
    public static String arrayToString(long[] array) {
        final StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        for (int i = 0; i < array.length; ++i) {
            sb.append(array[i]).append(" ");
            if ((i + 1) % 10 == 0) {
                sb.append("\n");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
