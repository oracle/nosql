/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.common.ratelimit;

import java.lang.IllegalArgumentException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of RateLimiter using a simple time-based mechanism.
 *
 * This limiter keeps a single "lastNano" time and a "nanosPerUnit".
 * Together these represent a number of units available based on
 * the current time.
 *
 * When units are consumed, the lastNano value is incremented by
 * (units * nanosPerUnit). If the result is greater than the current
 * time, a single sleep() is called to wait for the time difference.
 *
 * This method inherently "queues" the consume calls, since each consume
 * will increment the lastNano time. For example, a request for a small
 * number of units will have to wait for a previous request for a large
 * number of units. In this way, small requests can never "starve" large
 * requests.
 *
 * This limiter allows for a specified number of seconds of "duration"
 * to be used: if units have not been used in the past N seconds, they can
 * be used now. The minimum duration is internally bound such that a
 * consume of 1 unit will always have a chance of succeeding without waiting.
 *
 * Note that "giving back" (returning) previously consumed units will
 * only affect consume calls made after the return. Currently sleeping
 * consume calls will not have their sleep times shortened.
 */

public class SimpleRateLimiter implements RateLimiter {

    /* how many nanoseconds represent one unit */
    protected long nanosPerUnit;

    /* how many seconds worth of units can we use from the past */
    protected long durationNanos;

    /* last used unit nanosecond: this is the main "value" */
    protected long lastNano;

    /**
     * Creates a simple time-based rate limiter.
     *
     * This limiter will allow for one second of duration; that is, it
     * will allow unused units from within the last second to be used.
     *
     * @param rateLimitPerSec the maximum number of units allowed per second
     */
    public SimpleRateLimiter(double rateLimitPerSec) {
        this(rateLimitPerSec, 1.0);
    }

    /**
     * Creates a simple time-based rate limiter.
     *
     * @param rateLimitPerSec the maximum number of units allowed per second
     * @param durationSecs maximum amount of time to consume unused units from
     *        the past
     */
    public SimpleRateLimiter(double rateLimitPerSec, double durationSecs) {
        setLimitPerSecond(rateLimitPerSec);
        setDuration(durationSecs);
        reset();
    }

    @Override
    public synchronized void setLimitPerSecond(double rateLimitPerSec) {
        if (rateLimitPerSec <= 0.0) {
            this.nanosPerUnit = 0;
        } else {
            this.nanosPerUnit = (long)(1_000_000_000.0 / rateLimitPerSec);
        }
        enforceMinimumDuration();
    }

    private void enforceMinimumDuration() {
        /*
         * Force durationNanos such that the limiter can always be capable
         * of consuming at least 1 unit without waiting
         */
        if (this.durationNanos < nanosPerUnit) {
            this.durationNanos = nanosPerUnit;
        }
    }

    @Override
    public double getLimitPerSecond() {
        if (nanosPerUnit == 0) {
            return 0.0;
        }
        return 1_000_000_000.0 / (double)nanosPerUnit;
    }

    @Override
    public double getDuration() {
        return (double)durationNanos / 1_000_000_000.0;
    }

    @Override
    public void setDuration(double durationSecs) {
        this.durationNanos = (long)(durationSecs * 1_000_000_000.0);
        enforceMinimumDuration();
    }

    @Override
    public void reset() {
        lastNano = System.nanoTime();
    }

    @Override
    public void setCurrentRate(double percent) {
        /*
         * Note that "rate" isn't really clearly defined in this type
         * of limiter, because there is no inherent "time period". So
         * all "rate" operations just assume "for 1 second".
         */
        long nowNanos = System.nanoTime();
        if (percent == 100.0) {
            lastNano = nowNanos;
            return;
        }
        percent -= 100.0;
        lastNano = (nowNanos + (long)((percent / 100.0) * 1_000_000_000.0));
    }

    @Override
    public int consumeUnits(long units) {
        /*
         * call internal logic, get the time we need to sleep to
         * complete the consume.
         * note this call immediately consumes the units
         */
        int msToSleep = consumeInternal(units, 0, false, System.nanoTime());

        /* sleep for the requested time. */
        uninterruptibleSleep(msToSleep);

        /* return the amount of time slept */
        return msToSleep;
    }

    @Override
    public int consumeUnitsWithTimeout(long units, int timeoutMs,
        boolean alwaysConsume)
        throws TimeoutException {

        if (timeoutMs < 0) {
            throw new IllegalArgumentException(
                "timeoutMs must be 0 or positive");
        }

        /*
         * call internal logic, get the time we need to sleep to
         * complete the consume.
         */
        int msToSleep = consumeInternal(units, timeoutMs,
                                        alwaysConsume, System.nanoTime());
        if (msToSleep == 0) {
            return 0;
        }

        /*
         * if the time required to consume is greater than our timeout,
         * sleep up to the timeout then throw a timeout exception.
         * Note the units may have already been consumed if alwaysConsume
         * is true.
         */
        if (timeoutMs > 0 && msToSleep >= timeoutMs) {
            uninterruptibleSleep(timeoutMs);
            throw new TimeoutException("timed out waiting " + timeoutMs +
                "ms for " + units + " units in rate limiter");
        }

        /* sleep for the requested time. */
        uninterruptibleSleep(msToSleep);

        /* return the amount of time slept */
        return msToSleep;
    }


    /**
     * Returns the time to sleep to consume units.
     *
     * Note this method returns immediately in all cases. It returns
     * the number of milliseconds to sleep.
     *
     * This is the only method that actually "consumes units", i.e.
     * updates the lastNano value.
     *
     * @param units number of units to attempt to consume
     * @param timeoutMs max time to allow for consumption. Pass zero
     *        for no timeout (infinite wait).
     * @param alwaysConsume if true, units will be consumed regardless of
     *        return value.
     * @param nowNanos current time per System.nanoTime()
     * @return number of milliseconds to sleep.
     *         If timeoutMs is positive, and the return value is greater
     *         than or equal to timeoutMs, consume failed and the app should
     *         just sleep for timeoutMs then throw an exception.
     *         If the return value is zero, the consume succeeded under the
     *         limit and no sleep is necessary.
     */
    private synchronized int consumeInternal(long units, int timeoutMs,
        boolean alwaysConsume, long nowNanos) {

        /* If disabled, just return success */
        if (nanosPerUnit <= 0) {
            return 0;
        }

        /* determine how many nanos we need to add based on units requested */
        long nanosNeeded = units * nanosPerUnit;

        /* ensure we never use more from the past than duration allows */
        long maxPast = nowNanos - durationNanos;
        if (lastNano < maxPast) {
            lastNano = maxPast;
        }

        /* compute the new "last nano used" */
        long newLast = lastNano + nanosNeeded;

        /* if units < 0, we're "returning" them */
        if (units < 0) {
            /* consume the units */
            lastNano = newLast;
            return 0;
        }

        /*
         * if the limiter is currently under its limit, the consume
         * succeeds immediately (no sleep required).
         */
        if (lastNano <= nowNanos) {
            /* consume the units */
            lastNano = newLast;
            return 0;
        }

        /*
         * determine the amount of time that the caller needs to sleep
         * for this limiter to go below its limit. Note that the limiter
         * is not guaranteed to be below the limit after this time, as
         * other consume calls may come in after this one and push the
         * "at the limit time" further out.
         */
        int sleepMs = (int)((lastNano - nowNanos) / 1_000_000L);
        if (sleepMs == 0) {
            sleepMs = 1;
        }

        if (alwaysConsume) {
            /*
             * if we're told to always consume the units no matter what,
             * consume the units
             */
            lastNano = newLast;
        } else if (timeoutMs == 0) {
            /* if the timeout is zero, consume the units */
            lastNano = newLast;
        } else if (sleepMs < timeoutMs) {
            /*
             * if the given timeout is more than the amount of time to
             * sleep, consume the units
             */
            lastNano = newLast;
        }

        return sleepMs;
    }

    @Override
    public boolean tryConsumeUnits(long units) {
        if (consumeInternal(units, 1, false, System.nanoTime()) == 0) {
            return true;
        }
        return false;
    }

    @Override
    public double getCurrentRate() {
        /* see comment in setCurrentRate() */
        double cap = getCapacity();
        double limit = getLimitPerSecond();
        double rate = 100.0 - ((cap * 100.0) / limit);
        if (rate < 0.0) return 0.0;
        return rate;
    }

    @Override
    public void consumeUnitsUnconditionally(long units) {
        /* consume units, ignore amount of time to sleep */
        consumeInternal(units, 0, true, System.nanoTime());
    }

    public double getCapacity() {
        /* ensure we never use more from the past than duration allows */
        long nowNanos = System.nanoTime();
        long maxPast = nowNanos - durationNanos;
        if (lastNano > maxPast) {
            maxPast = lastNano;
        }
        return (double)(nowNanos - maxPast) / (double)nanosPerUnit;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("lastNano=").append(lastNano)
            .append(", nanosPerUnit=").append(nanosPerUnit)
            .append(", durationNanos=").append(durationNanos)
            .append(", limit=").append(getLimitPerSecond())
            .append(", capacity=").append(getCapacity())
            .append(", rate=").append(getCurrentRate());
        return sb.toString();
    }

    private void uninterruptibleSleep(int sleepMs) {
        if (sleepMs <= 0) {
            return;
        }

        long endTime = System.currentTimeMillis() + sleepMs;
        boolean done = false;
        while (done == false) {
            try {
                TimeUnit.MILLISECONDS.sleep(sleepMs);
                done = true;
            } catch (InterruptedException e) {
                sleepMs = (int)(endTime - System.currentTimeMillis());
                if (sleepMs <= 0) {
                    done = true;
                }
            }
        }
    }


    /**
     * @hidden
     * Consumes units and returns the time to sleep.
     *
     * Note this method returns immediately in all cases. It returns
     * the number of milliseconds to sleep.
     *
     * @param units number of units to attempt to consume
     * @return number of milliseconds to sleep.
     *         If the return value is zero, the consume succeeded under the
     *         limit and no sleep is necessary.
     */
    public int consumeExternally(long units) {
        /* If disabled, just return success */
        if (nanosPerUnit <= 0) {
            return 0;
        }
        return consumeInternal(units, 0, true, System.nanoTime());
    }

}
