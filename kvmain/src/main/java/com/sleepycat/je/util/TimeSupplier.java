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

package com.sleepycat.je.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/*
 * A Basic TimeSupplier wrapper over system time methods to ease the expiration
 * tests. Mostly used in unit tests and standalone tests
 */

public class TimeSupplier {

    private static Clock clock = Clock.systemDefaultZone();

    public static void setClock(Clock customClock) {
        clock = customClock;
    }

    public static long currentTimeMillis() {
        return clock.millis();
    }

    /*
     * This is not cumulative, it is point in time movement
     */
    public static void setClockOffset(long amount, TimeUnit unit) {
        Duration offsetDuration;
        switch (unit) {
            case HOURS:
                offsetDuration = Duration.ofHours(amount);
                break;
            case DAYS:
                offsetDuration = Duration.ofDays(amount);
                break;
            default:
                offsetDuration = Duration.ofHours(amount);
        }
        clock = Clock.offset(Clock.systemDefaultZone(), offsetDuration);
    }

    public static void resetClock() {
        clock = Clock.systemDefaultZone();
    }

    public static Instant now() {
        return clock.instant();
    }

    public static void flashBack(long amount, TimeUnit unit) {
        setClockOffset(-amount, unit);
    }
}
