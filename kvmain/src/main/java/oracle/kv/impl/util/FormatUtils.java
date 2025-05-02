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

package oracle.kv.impl.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import oracle.nosql.common.json.JsonNode;

/**
 * Centralize formatting, to make it as uniform as possible.
 * TODO: make this configurable, based on a Locale
 */
public class FormatUtils {

    public static String defaultTimeZone = "UTC";

    private static final TimeZone tz = TimeZone.getTimeZone(defaultTimeZone);

    /*
     * Thread local copies of the formatters, since the class isn't thread
     * safe.
     */

    /**
     * A formatter for timestamps mostly appearing in logging messages.
     */
    private static final ThreadLocal<DateFormat>
        DATE_TIME_MILLIS_FORMATTER = ThreadLocal.withInitial(
            () -> createFormatter("yyyy-MM-dd HH:mm:ss.SSS z"));

    /**
     * Returns a SimpleDateFormat using the specified format and the UTC
     * timezone.
     */
    private static DateFormat createFormatter(String format) {
        final DateFormat df =
            new SimpleDateFormat(format);
        df.setTimeZone(tz);
        return df;
    }

    /**
     * A formatter for timestamps mostly appearing in stats.
     */
    private static final ThreadLocal<DateFormat>
        TIME_MILLIS_FORMATTER = ThreadLocal.withInitial(
            () -> createFormatter("HH:mm:ss.SSS z"));

    /**
     * A formatter for timestamps appearing in perf.
     */
    private static final ThreadLocal<DateFormat>
        PERF_FORMATTER = ThreadLocal.withInitial(
            () -> createFormatter("yy-MM-dd HH:mm:ss"));

    /**
     * A formatter for timestamps mostly appearing in admin and planner.
     */
    private static final ThreadLocal<DateFormat>
        DATE_TIME_FORMATTER = ThreadLocal.withInitial(
            () -> createFormatter("yyyy-MM-dd HH:mm:ss z"));

    /**
     * A thread-local Date object to avoid allocation.
     */
    private static final ThreadLocal<Date>
        DATE = ThreadLocal.withInitial(Date::new);

    public static TimeZone getTimeZone() {
        return tz;
    }

    /**
     * Formats the time as yyyy-MM-dd HH:mm:ss.SSS z. This
     * format is used mostly by NoSQL DB logger utils.
     */
    public static String formatDateTimeMillis(long time) {
        final Date date = DATE.get();
        date.setTime(time);
        return DATE_TIME_MILLIS_FORMATTER.get().format(date);
    }

    /**
     * Returns the parsed Date object for a string in the format of yyyy-MM-dd
     * HH:mm:ss.SSS z.
     */
    public static Date parseDateTimeMillis(String source)
        throws ParseException
    {
        return DATE_TIME_MILLIS_FORMATTER.get().parse(source);
    }

    /**
     * Formats the time as HH:mm:ss.SSS. This format is used
     * mostly by the NoSQL DB stats
     */
    public static String formatTimeMillis(long time) {
        final Date date = DATE.get();
        date.setTime(time);
        return TIME_MILLIS_FORMATTER.get().format(date);
    }

    /**
     * Formats the time as yy-MM-dd HH:mm:ss. This format is used to produce a
     * UTC time stamp for the .perf files.
     */
    public static String formatPerfTime(long time) {
        final Date date = DATE.get();
        date.setTime(time);
        return PERF_FORMATTER.get().format(date);
    }

    /**
     * Formats the time as yyyy-MM-dd HH:mm:ss z. This format is mostly used by
     * the admin and planner.
     */
    public static String formatDateTime(long time) {
        final Date date = DATE.get();
        date.setTime(time);
        return DATE_TIME_FORMATTER.get().format(date);
    }

    /**
     * Converts a json to a string.
     *
     * The string is supposed to be put as a line in a log. To make parsing
     * easier, we add the "JL" marker at the start of the string and structure
     * it in the following format:
     * <pre>
     * JL|<nameField>|<timeField>|<jsonField>
     * </pre>
     * The timeField is in the form:
     * <pre>
     * (<startTime> -&gt; <endTime>)/(<startTimestamp> -&gt; <endTimestamp>)
     * </pre>
     * where <startTime> and <endTime> are human readable time strings and
     * start and end timestamps the epoch time stamps.
     */
    public static String toJsonLine(String name,
                                    long startTime,
                                    long endTime,
                                    JsonNode element) {
        return String.format(
            "JL|%s|(%s -> %s)/(%s -> %s)|%s",
            name,
            formatDateTimeMillis(startTime),
            formatDateTimeMillis(endTime),
            startTime,
            endTime,
            element);
    }
}
