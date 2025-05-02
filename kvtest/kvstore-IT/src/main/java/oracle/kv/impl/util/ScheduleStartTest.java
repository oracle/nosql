/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import oracle.kv.TestBase;

public class ScheduleStartTest extends TestBase {
    @Test
    public void testCalculateDelay() {

        /* case 1: interval is 60s */
        long configuredIntervalMs = 60000; /* 60 seconds */
        Calendar now = Calendar.getInstance();
        now.clear();
        now.set(2016, 8, 30, 20, 4, 12);
        long nowMs = now.getTimeInMillis();
        long delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);

        Calendar next = Calendar.getInstance();
        next.clear();
        next.set(2016, 8, 30, 20, 5, 0);
        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());

        /* case 2: interval is 30s */
        configuredIntervalMs = 30000; /* 30 seconds */
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);

        next.set(2016, 8, 30, 20, 4, 30);
        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());

        /* case 3: interval is 90s */
        configuredIntervalMs = 90000; /* 90 seconds */
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);

        next.set(2016, 8, 30, 20, 4, 30);
        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());

        /* case 4: interval is 120s */
        configuredIntervalMs = 120000; /* 120 seconds */
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);

        next.set(2016, 8, 30, 20, 6, 0);
        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());

        /* case 5: now is the beginning of a minute */
        configuredIntervalMs = 60000; /* 60 seconds */
        now.set(2016, 8, 30, 20, 4, 0);
        nowMs = now.getTimeInMillis();
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);

        next.set(2016, 8, 30, 20, 4, 0);
        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());
        delay = ScheduleStart.calculateDelay(60*1000*59, nowMs);

        /* case 6: now is the end of a day */
        configuredIntervalMs = 120000; /* 120 seconds */
        /* Month value is 0-based, so 8 for Sept */
        now.set(2016, 8, 30, 23, 59, 59);
        nowMs = now.getTimeInMillis();
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);

        next.set(2016, 9, 1, 0, 0, 0);
        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());

        /* case 7: now is the end of the day, interval is 1 hour */
        configuredIntervalMs = 60*1000*60; /* 1 hour */
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);
        next.set(2016, 9, 1, 0, 0, 0);

        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());

        /* case 8: now is the end of the day, interval is 15 minutes */
        configuredIntervalMs = 60 * 1000 * 15;
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);
        next.set(2016, 9, 1, 0, 0, 0);

        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                      nowMs + delay, next.getTimeInMillis());

        /* case 9: now is at the first second of a day, interval is 1 hour */
        configuredIntervalMs = 60*1000*60; /* 1 hour */
        now.set(2016, 9, 1, 0, 0, 1);
        nowMs = now.getTimeInMillis();
        delay = ScheduleStart.calculateDelay(configuredIntervalMs, nowMs);
        next.set(2016, 9, 1, 1, 0, 0);

        assertEquals(display(configuredIntervalMs, next, nowMs+delay),
                     nowMs + delay, next.getTimeInMillis());
    }

    private String display(long interval, Calendar expected, long got) {
        StringBuilder sb = new StringBuilder();
        sb.append("Interval=").append(interval).append(" ms");
        sb.append(" Expected=").append(new Date(expected.getTimeInMillis()));
        sb.append(" Calculated start time=").append(new Date(got));
        return sb.toString();
    }
}
