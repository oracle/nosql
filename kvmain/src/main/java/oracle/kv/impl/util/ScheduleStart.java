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

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.param.DurationParameter;

/*
 * A helper class to schedule work that is configured as a repeating, periodic
 * tasks. In particular, support the notion of a standard start time (Epoch
 * time) for periodic tasks based on the configured interval. The next start
 * time would be the next interval boundary, as measured from the start of
 * Epoch time. This predictable start point makes it
 * possible to aggregate and combine information from different sources.
 *
 * Examples:
 * - if a task is configured to run every minute, and it is currently 35
 *   seconds after a minute boundary, the next task will run 25 seconds from
 *   now, at the minute mark.
 * - if a task is configured to run every 15 minutes, and it is currently 26
 *   minutes after the hour, the next task will run 4 minutes from now, at the
 *   half hour mark.
 * - if a task is configured to run every 5 hours, and it is 6:00 am, the next
 *   task will start at 10:00 am, 4 hours from now.
 */
public class ScheduleStart extends oracle.nosql.common.sklogger.ScheduleStart {

    /**
     * A convenience method to schedule a task with an executor service
     * using times normalized by ScheduledStart.calculateDelay()
     */
    public static Future<?>
        scheduleAtFixedRate(ScheduledExecutorService executor,
                            DurationParameter configuredDuration,
                            Runnable task,
                            Logger logger) {

        long configuredIntervalMs = configuredDuration.toMillis();
        long nowMs = System.currentTimeMillis();
        long delayMs = calculateDelay(configuredIntervalMs, nowMs);
        logger.info("Starting task: interval is " +
                    configuredDuration.asString() + ", delay is " +
                    delayMs + "(ms)");
        /*
         * TODO: Note that we aren't adjusting for leap seconds, which can
         * impact the correctness of the interval-aligned task. These are
         * inserted by the astromomical powers that be to account for earth's
         * slowing rotation. We can worry about this issue later, if it becomes
         * significant.
         */
        return executor.scheduleAtFixedRate(task,
                                            delayMs,
                                            configuredIntervalMs,
                                            TimeUnit.MILLISECONDS);
    }
}
