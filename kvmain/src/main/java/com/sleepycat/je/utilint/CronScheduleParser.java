/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

import java.util.Arrays;
import java.util.Calendar;

import com.sleepycat.je.EnvironmentConfig;

/**
 * This class aims to parser {@link EnvironmentConfig#VERIFY_SCHEDULE} which
 * is a cron-style expression.
 * 
 * <p>The cron-style expression can be very complicate, for example containing
 * *, ?, / and so on. But now we only handle the simplest situation. We will
 * continually add the code to handle more complicated situations if needed
 * in the future.</p>
 *
 * <p><b>Update:</b></p>
 * Added support for cron-style expression containing '/', ',' to schedule
 * tasks that run at regular intervals.
 * <p>Constraint for current version:
 * <ul>
 * <li>The standard string should be "* * * * *", i.e. there are 5 fields and
 *     4 blank space
 * <li>Each filed can only be an int value or *.
 * <li>Can not specify dayOfMonth and dayOfWeek simultaneously
 * <li>Can not specify dayOfMonth or month. Because if so, we will need to
 *     consider the days of that month and furthermore whether that year is
 *     leap year for February. The difference of the number of days for
 *     each month make it complicate to calculate the delay and the interval.
 * <li>If the field is an int value, then the value should be in
 *     the correct range, i.e. minute(0-59), hour(0-23), dayOfWeek(0-6),
 *     where dayOfMonth(1-31) and month(1-12) can not be specified.
 * <li>If dayOfWeek is a concrete value, then minute or hour can not be '*'.
 *      For example, we can not use "0 * * * 5", i.e. we can not specify that
 *      we want the verifier to run every hour only on Friday. Because it may
 *      be complicate to calculate the stop time point and at least we need
 *      to add a variable.
 * <li>The same reason, if hour is a concrete value, minute can not be '*'.
 * <li>If the intention of the expression is to schedule a task running every
 * equal time interval, the minute field should not be *, following constraints
 * above. Also, the day of week should not be concrete for the same reason.
 * the week shouldn't be concrete.
 * </ul>
 */
public class CronScheduleParser {
    private static final String errorMess =
        "The format of " + EnvironmentConfig.VERIFY_SCHEDULE +
        " is invalid. ";
    private static final int spaceNum = 4;
    private static final int fieldNum = 5;

    public static final String nullCons = "The argument should not be null.";
    public static final String cons1 =
        "The standard string should be '* * * * *', i.e. there are " +
        fieldNum + " fields and " + spaceNum + " blank spaces.";
    public static final String cons2 =
        "Each field can only be an int value or *.";
    public static final String cons3 =
        "Can not specify dayOfWeek and dayOfMonth simultaneously.";
    public static final String cons4 = "Can not specify dayOfMonth or month.";
    public static final String cons5 = "Range Error: ";
    public static final String cons6 =
        "If the day of the week is a concrete day, then the minute and the " +
        "hour should also be concrete.";
    public static final String cons7 =
        "If the hour is a concrete hour, then minute should also be concrete";
    public static final String cons8 =
        "The hour field can only be int value or *, or follow the format" +
        " of \"*/IntervalHour\" or \"startHour/IntervalHour\", or follow the " +
        "pattern of \"hour1,hour2,hour3\" (multiple integers " +
        "separated by comma).";
    public static final String cons9 =
        "If intention is to set up to run every x hours, the day of the week" +
        " should not be a concrete day.";
    public static final String cons10 =
        "If intention is to set up to run every x hours, the minute" +
        " should be a concrete value.";
    public static final String cons11 =
        "The hour field should be able to divided into several equal " +
        "intervals";
    String regexSlash = "^(\\*|-?\\d+)/(-?\\d)+$";
    String regexComma = "^-?\\d+(,-?\\d+)+$";

    private static final long millsOneDay = 24 * 60 * 60 * 1000;
    private static final long millsOneHour = 60 * 60 * 1000;
    private static final long millsOneMinute = 60 * 1000;

    /** A test hook called to allow tests to supply a calendar. */
    public static volatile TestHook<Calendar> setCurCalHook;

    private final long time;
    private long delay;
    private long interval;

    /**
     * Creates an instance that uses the default calendar. The constructor will
     * first validate the cron-style string and then parse the string to get
     * the interval of the cron-style task represented by the string and to get
     * the wait-time(delay) to first time start the cron-style task.
     *
     * @param cronSchedule the cron-style string
     * @throws IllegalArgumentException if there is a problem with the format
     * of the parameter
     */
    public CronScheduleParser(final String cronSchedule) {
        this(cronSchedule, Calendar.getInstance());
    }

    /**
     * Creates an instance that uses the specified calendar. The construction
     * function will first validate the cron-style string and then parse the
     * string to get the interval of the cron-style task represented by the
     * string and to get the wait-time(delay) to first time start the
     * cron-style task.
     *
     * @param cronSchedule the cron-style string
     * @param calendar the calendar
     * @throws IllegalArgumentException if there is a problem with the format
     * of the cronSchedule parameter
     */
    public CronScheduleParser(final String cronSchedule,
                              final Calendar calendar) {
        final Calendar curCal = (setCurCalHook != null) ?
            setCurCalHook.getHookValue() :
            calendar;
        time = curCal.getTimeInMillis();
        validate(cronSchedule);
        parser(cronSchedule, curCal);
    }

    /**
     * Check whether two cron-style strings are same, i.e. both are null or
     * the content of the two strings are same.
     *
     * @param cronvSchedule1 The first cron-style string.
     * @param cronSchedule2 The second cron-style string.
     *
     * @return true if the two cron-style strings are same.
     */
    public static boolean checkSame(
        final String cronvSchedule1,
        final String cronSchedule2) {

        if (cronvSchedule1 == null && cronSchedule2 ==null) {
            return true;
        }

        if (cronvSchedule1 == null || cronSchedule2 ==null) {
            return false;
        }

        if (cronvSchedule1.equals(cronSchedule2)) {
            return true;
        }

        return false;
    }

    /**
     * Returns the time used when computing the delay time.
     */
    public long getTime() {
        return time;
    }

    /**
     * @return delay The wait-time to first time start the cron-style task
     *               represented by the cron-style string.
     */
    public long getDelayTime() {
        return delay;
    }

    /**
     * Returns the number of milliseconds between times that match the crontab
     * entry. Returns:
     * <ul>
     * <li>7 days if day-of-week is concrete, else
     * <li>1 day if hour is concrete, else
     * <li>1 hour if minute is concrete, else
     * <li>1 minute otherwise.
     * </ul>
     *
     * @return interval The interval of the cron-style task represented by the
     *                  cron-style string.
     */
    public long getInterval() {
        return interval;
    }

    private void assertDelay() {
        if (delay < 0) {
            throw new IllegalStateException(
                "Delay is negative: " + delay + "; interval is: " + interval);
        }
    }

    private void parser(final String cronSchedule, Calendar curCal) {
        int curDayOfWeek = curCal.get(Calendar.DAY_OF_WEEK);
        int curHour = curCal.get(Calendar.HOUR_OF_DAY);
        int curMinute = curCal.get(Calendar.MINUTE);

        /*
         * Previously, we use Calendar.getInstance() to initialize scheduleCal,
         * which aims to let scheduleCal have some similar/same attributes
         * with curCal, such as the day of the week. But we may use
         * setCurCalHook.doHook to set curCal to be a future week, now
         * using Calendar.getInstance() can not achieve the original purpose.
         */
        Calendar scheduleCal = (Calendar) curCal.clone();
        scheduleCal.set(Calendar.SECOND, 0);
        scheduleCal.set(Calendar.MILLISECOND, 0);
        String[] timeArray = cronSchedule.split(" ");

        /* dayofWeek is a concrete value. */
        if (!timeArray[4].equals("*")) {
            interval = 7 * millsOneDay;
            int tmpDayOfWeek = Integer.valueOf(timeArray[4]) + 1;
            int tmpHour = Integer.valueOf(timeArray[1]);
            int tmpMinute = Integer.valueOf(timeArray[0]);

            scheduleCal.set(Calendar.DAY_OF_WEEK, tmpDayOfWeek);
            scheduleCal.set(Calendar.HOUR_OF_DAY, tmpHour);
            scheduleCal.set(Calendar.MINUTE, tmpMinute);

            if (scheduleCal.getTimeInMillis() < curCal.getTimeInMillis()) {
                /* add 7 days to set next week */
                scheduleCal.add(Calendar.DATE, 7);
            }
            delay = scheduleCal.getTimeInMillis() - curCal.getTimeInMillis();

            assertDelay();
            return;
        }

        if (!timeArray[1].equals("*")) {
            /*
             * There are three cases that the hour field is not '*'.
             * case 1: follows the format of  * / h(no spaces) or startHour/ h
             * case 2: follows the format of h1,h2,h3
             * case 3: integer
             */
            int tmpHour;
            int tmpMinute = Integer.valueOf(timeArray[0]);
            int intervalInHours;
            /*
             * Guarantee that both dayOfWeek is same when dayOfWeek is * in
             * cronSchedule.
             */
            scheduleCal.set(Calendar.DAY_OF_WEEK, curDayOfWeek);
            scheduleCal.set(Calendar.MINUTE, tmpMinute);

            if(timeArray[1].matches(regexSlash)) {
                /*
                 * case 1, for example, "* / 12", then the interval will
                 * be 12 hours. if current time is 05:00, then the delay time
                 * should be 7 hours.
                 */
                intervalInHours = Integer.valueOf(timeArray[1].
                                                  split("/")[1]);
                interval = intervalInHours * millsOneHour;
                if(timeArray[1].charAt(0) != '*') {
                    /*
                     * the start Hour is specified, i.e, 6/12
                     */
                    tmpHour = Integer.parseInt(timeArray[1].split("/")[0]);
                    scheduleCal.set(Calendar.HOUR_OF_DAY, tmpHour);
                    if (scheduleCal.getTimeInMillis() < curCal.getTimeInMillis()) {
                        /* to set next day */
                        scheduleCal.add(Calendar.DATE, 1);
                    }
                    delay = scheduleCal.getTimeInMillis() - curCal.getTimeInMillis();
                    assertDelay();
                    return;
                }
            } else if (timeArray[1].matches(regexComma)) {
                /*
                 * case 2, for example, "0,4,8,12,16,20", then the interval
                 * will be 4 hours, tmpHour should be the hour that is the
                 * smallest but greater than the curHour. Say curHour = 21,
                 * then tmpHour should be 0; curHour = 9, then tmpHour should
                 *  be 12. Especially, if current time is 04:20, and
                 * cronSchedule is 30 0,4,8,12,16,20 * * *, then the first time
                 * to run the task will be 04:30. But the validation will be
                 * done in validate().
                 */
                int[] hours = Arrays.stream(timeArray[1].split(",")).
                    mapToInt(Integer::parseInt).toArray();
                Arrays.sort(hours);
                intervalInHours = hours[1] - hours[0];

                interval = intervalInHours * millsOneHour;
            } else {
                /*
                 * case 3
                 */
                interval = millsOneDay;
                tmpHour = Integer.valueOf(timeArray[1]);
                scheduleCal.set(Calendar.HOUR_OF_DAY, tmpHour);
                if (scheduleCal.getTimeInMillis() < curCal.getTimeInMillis()) {
                    /* to set next day */
                    scheduleCal.add(Calendar.DATE, 1);
                }
                delay = scheduleCal.getTimeInMillis() - curCal.getTimeInMillis();
                assertDelay();
                return;
            }

            /*
             * timeArray[1].matches(regexComma) ||
             * (timeArray[1].matches(regexComma) &&
             *  timeArray[1].charAt(0) == '*')
             */
            tmpHour = 0;
            while (tmpHour < curHour) {
                tmpHour += intervalInHours;
            }
            if(tmpHour >= 24) {
                scheduleCal.set(Calendar.HOUR_OF_DAY, 0);
                scheduleCal.add(Calendar.DATE, 1);
            } else {
                scheduleCal.set(Calendar.HOUR_OF_DAY,tmpHour);
            }
            if (scheduleCal.getTimeInMillis() < curCal.getTimeInMillis()) {
                /*
                 * to add another intervalInHours
                 */
                scheduleCal.add(Calendar.HOUR_OF_DAY, intervalInHours);
            }

            delay = scheduleCal.getTimeInMillis() - curCal.getTimeInMillis();
            assertDelay();
            return;
        }

        if (!timeArray[0].equals("*")) {
            interval = millsOneHour;
            int tmpMinute = Integer.valueOf(timeArray[0]);

            /*
             * Guarantee that both dayOfWeek and both hour are same whe
             * dayOfWeek and hour are * in cronSchedule.
             */
            scheduleCal.set(Calendar.DAY_OF_WEEK, curDayOfWeek);
            scheduleCal.set(Calendar.HOUR_OF_DAY, curHour);
            scheduleCal.set(Calendar.MINUTE, tmpMinute);

            if (scheduleCal.getTimeInMillis() < curCal.getTimeInMillis()) {
                /* to set next hour */
                scheduleCal.add(Calendar.HOUR, 1);
            }
            delay = scheduleCal.getTimeInMillis() - curCal.getTimeInMillis();

            assertDelay();
            return;
        }

        if (timeArray[0].equals("*")) {
            interval = millsOneMinute;
            delay = 0;
            assertDelay();
            return;
        }
    }

    private void validate(final String cronSchedule) {

        if (cronSchedule == null) {
            throw new IllegalArgumentException(errorMess + nullCons);
        }

        /*
         * Constraint 1: The standard string should be "* * * * *", i.e.
         * there are 5 fields and 4 blank space.
         */
        int spaceCount = 0;
        for (int i = 0; i < cronSchedule.length(); i++) {
            char c = cronSchedule.charAt(i);
            if (c == 32 ) {  /* The ASCII value of ' ' is 32. */
                spaceCount++;
            }
        }
        if (spaceCount != spaceNum ||
            cronSchedule.split(" ").length != fieldNum) {
            throw new IllegalArgumentException(errorMess + cons1);
        }

        String[] timeArray = cronSchedule.split(" ");
        /*
         * Constraint 2: Each field can only be an int value or *.
         */
        for (int i = 0; i <= timeArray.length - 1; i++) {
            try {
                Integer.valueOf(timeArray[i]);
            } catch (NumberFormatException e) {
                if (!timeArray[i].equals("*")) {
                    /*
                     * Constraint 8: Only four options for the hour field.
                     */
                    if(i == 1) {
                        if (!timeArray[i].matches(regexSlash) &&
                                !timeArray[i].matches(regexComma)) {
                            throw new IllegalArgumentException(errorMess + cons8);
                        }
                    } else {
                        throw new IllegalArgumentException(errorMess + cons2);
                    }
                }
            }
        }

        /*
         * Constraint 3: Can not specify dayOfMonth and dayOfWeek
         * simultaneously.
         */
        if (!timeArray[2].equals("*") && !timeArray[4].equals("*")) {
            throw new IllegalArgumentException(errorMess + cons3);            
        }

        /*
         * Constraint 4: Can not specify dayOfMonth or month.
         */
        if (!timeArray[2].equals("*") || !timeArray[3].equals("*")) {
            throw new IllegalArgumentException(errorMess + cons4);             
        }

        /*
         * Constraint 5: If the field is a int value, then the value should
         * be in the correct range.
         */
        if (!timeArray[0].equals("*")) {
            int min = Integer.valueOf(timeArray[0]);
            if (min < 0 || min > 59) {
                throw new IllegalArgumentException
                    (errorMess + cons5 + "The minute should be (0-59).");
            }
        }

        if (!timeArray[1].equals("*")) {
            if (timeArray[1].matches(regexSlash)) {
                if (timeArray[1].charAt(0) != '*') {
                    int startJour = Integer.valueOf(timeArray[1].
                                                    split("/")[0]);
                    if (startJour < 0 || startJour > 23) {
                        throw new IllegalArgumentException
                            (errorMess + cons5 + "The hour should be (0-23).");
                    }
                }
                int hour = Integer.valueOf(timeArray[1].split("/")[1]);
                if (hour < 0 || hour > 23) {
                    throw new IllegalArgumentException
                        (errorMess + cons5 + "The hour should be (0-23).");
                }
            } else if (timeArray[1].matches(regexComma)) {
                for (Integer hour : Arrays.stream(timeArray[1].split(",")).
                    mapToInt(Integer::parseInt).toArray()) {
                    if(hour < 0 || hour > 23) {
                        throw new IllegalArgumentException
                            (errorMess + cons5 + "The hour should be (0-23).");
                    }
                }
            } else {
                int hour = Integer.valueOf(timeArray[1]);
                if (hour < 0 || hour > 23) {
                    throw new IllegalArgumentException
                        (errorMess + cons5 + "The hour should be (0-23).");
                }
            }
        }

        if (!timeArray[4].equals("*")) {
            int dayOfWeek = Integer.valueOf(timeArray[4]);
            if (dayOfWeek < 0 || dayOfWeek > 6) {
                throw new IllegalArgumentException
                    (errorMess + cons5 + "The day of the week should" +
                    "be (0-6).");
            }
        }

        /*
         * Constraint 6: If dayOfWeek is a concrete value, then minute or
         * hour can not be '*'.
         */
        if (!timeArray[4].equals("*") && !timeArray[1].matches(regexComma)
            && !timeArray[1].matches(regexSlash)) {
            if (timeArray[0].equals("*") || timeArray[1].equals("*")) {
                throw new IllegalArgumentException(errorMess + cons6);
            }
        }

        /*
         * Constraint 7: If hour is a concrete value, minute can not be '*'.
         */
        if (!timeArray[1].equals("*") && !timeArray[1].matches(regexComma)
            && !timeArray[1].matches(regexSlash)) {
            if (timeArray[0].equals("*")) {
                throw new IllegalArgumentException(errorMess + cons7);
            }
        }

        if (timeArray[1].matches(regexComma) ||
            timeArray[1].matches(regexSlash)) {
            if(!timeArray[4].equals("*")) {
                /*
                 * Constraint 9: If the intention of the expression is to
                 * schedule a task running every equal time interval, the day
                 * of week should not be concrete.
                 */
                throw new IllegalArgumentException
                    (errorMess + cons9);
            }
            if(timeArray[0].equals("*")) {
                /*
                 * Constraint 10: If the intention of the expression is to
                 * schedule a task running every equal time interval, the
                 * minute field should not be *.
                 */
                throw new IllegalArgumentException
                    (errorMess + cons10);
            }
        }

        if (timeArray[1].matches(regexComma)) {
            /*
             * Constraint 11: The hour field should be able to be divided into
             * equally intervals.
             */
            int[] hours = Arrays.stream(timeArray[1].split(",")).
                    mapToInt(Integer::parseInt).toArray();
            Arrays.sort(hours);
            int[] intervals = new int[hours.length - 1];
            for (int i = 0; i < hours.length - 1; i++) {
                intervals[i] = hours[i+1] - hours[i];
            }
            /*
             * The cron-style expression cannot divide a day into
             * equal intervals, for example: 0,4,9,12,16,20
             */
            for (int i = 0; i < intervals.length - 1; i++) {
                if(intervals[i] != intervals[i + 1]) {
                    throw new IllegalArgumentException(errorMess + cons11);
                }
            }
            /*
             * The cron-style expression cannot divide a day into
             * equal intervals, for example: 0,4,8,12,16,21
             */
            if (hours[0] + 24 - hours[hours.length - 1] != intervals[0]) {
                throw new IllegalArgumentException(errorMess + cons11);
            }
        }

        if (timeArray[1].matches(regexSlash)) {
            int hour = Integer.parseInt(timeArray[1].split("/")[1]);
            if (24 % hour != 0) {
                throw new IllegalArgumentException(errorMess + cons11);
            }
        }

        /*
        if (timeArray[0].equals("*")) {
            throw new IllegalArgumentException
                (errorMes + "User specify the verifier to run every minute." +
                "This is too frequent.");
        }
        */
    }
}
