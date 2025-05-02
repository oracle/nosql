/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.common.ratelimit;

import java.util.concurrent.TimeoutException;
import java.lang.IllegalArgumentException;

/**
 * RateLimiter interface provides default methods that all rate limiters
 * must implement.
 *
 * <b>Thread safety:</b> It is expected that all implementing classes of this
 * interface may be used by multiple threads concurrently. For example, many
 * threads may be using the same rate limiter instance to ensure that
 * all of them together do not exceed a given limit.
 * <p>
 * <b>Typical usage:</b>
 * The simplest use of the rate limiter is to consume a number of units,
 * blocking until they are successfully consumed:
 * <pre>
 *    int delayMs = rateLimiter.consumeUnits(units);
 *    // delayMs indicates how long the consume delayed
 * </pre>
 * <p>
 * To poll a limiter to see if it is currently over the limit:
 * <pre>
 *    if (rateLimiter.tryConsumeUnits(0)) {
 *        // limiter is below its limit
 *    }
 * </pre>
 * <p>
 * To attempt to consume units, only if they can be immediately consumed
 * without waiting:
 * <pre>
 *    if (ratelimiter.tryConsumeUnits(units)) {
 *        // successful consume
 *    } else {
 *        // could not consume units without waiting
 *    }
 * </pre>
 * <p>
 * Usages that involve waiting with timeouts:
 * <p>
 * In cases where the number of units an operation will consume is already
 * known before the operation, a simple one-shot method can be used:
 * <pre>
 *    long units = (known units the operation will use)
 *    try {
 *        boolean alwaysConsume=false; // don't consume if we time out
 *        int delayMs = rateLimiter.consumeUnitsWithTimeout(units,
 *                      timeoutMs, alwaysConsume);
 *        // we waited delayMs for the units to be consumed, and the
 *        // consume was successful
 *        ...do operation...
 *    } catch (TimeoutException e) {
 *        // we could not do the operation within the given timeframe.
 *        ...skip operation...
 *    }
 * </pre>
 * <p>
 * In cases where the number of units an operation will consume is not
 * known before the operation, typically two rate limiter calls would be
 * used: one to wait till the limiter is below it limit, and a second
 * to update the limiter with used units:
 * <pre>
 *    // wait until we're under the limit
 *    try {
 *        // note here we don't consume units if we time out
 *        int delayMs = rateLimiter.consumeUnitsWithTimeout(0,
 *                      timeoutMs, false);
 *    } catch (TimeoutException e) {
 *        // we could not go below the limit within the given timeframe.
 *        ...skip operation...
 *    }
 *    // we waited delayMs to be under the limit, and were successful
 *    long units = ...do operation, get number of units used...
 *    // update rate limiter with consumed units. Next operation on this
 *    // limiter may delay due to it being over its limit.
 *    rateLimiter.consumeUnits(units);
 * </pre>
 * <p>
 * Alternately, the operation could be always performed, and then the
 * limiter could try to wait for the units to be consumed:
 * <pre>
 *    long units = ...do operation, get number of units used...
 *    try {
 *        boolean alwaysConsume=true; // consume, even if we time out
 *        int delayMs = rateLimiter.consumeUnitsWithTimeout(units,
 *                      timeoutMs, alwaysConsume);
 *        // we waited delayMs for the units to be consumed, and the
 *        // consume was successful
 *    } catch (TimeoutException e) {
 *        // the rate limiter could not consume the units and go
 *        // below the limit in the given timeframe. Units are
 *        // consumed anyway, and the next call to this limiter
 *        // will likely delay
 *    }
 * </pre>
 * <p>
 * <b>Limiter duration</b>:
 * Implementing rate limiters should support a configurable "duration".
 * This is sometimes referred to as a "burst mode", or a "window time",
 * or "burst duration".
 * This will allow consumes of units that were not consumed in the recent
 * past. For example, if a limiter allows for 100 units per second, and is
 * not used for 5 seconds, it should allow an immediate consume of 500
 * units with no delay upon a consume call, assuming that the limiter's
 * duration is set to at least 5 seconds.
 * The maximum length of time for this duration should be configurable.
 * In all cases a limiter should set a reasonable minimum duration, such
 * that a call to <pre>tryConsumeUnits(1)</pre> has a chance of succeeding.
 * It is up to the limiter implementation to determine if units from the past
 * before the limiter was created or reset are available for use.
 * If a limiter implemetation does not allow setting a duration, it must throw
 * an UnsupportedOperationException when its setDuration() method is called.
 */
public interface RateLimiter {

    /**
     * Consumes a number of units, blocking until the units are available.
     * <p>
     * @param units the number of units to consume. This can be a negative
     *        value to "give back" units.
     * @return the amount of time blocked in milliseconds. If not blocked,
     *         0 is returned.
     */
    public int consumeUnits(long units);

    /**
     * Consumes the specified number of units if they can be returned
     * immediately without waiting.
     *
     * @param units The number of units to consume. Pass zero to poll if the
     *        limiter is currently over its limit. Pass negative values to
     *        "give back" units (same as calling {@link #consumeUnits}
     *        with a negative value).
     * @return true if the units were consumed, false if they were not
     *         immediately available. If units was zero, true means the
     *         limiter is currently below its limit.
     */
    public boolean tryConsumeUnits(long units);

    /**
     * Attempts to consume a number of units, blocking until the units are
     * available or the specified timeout expires.
     *
     * @param units the number of units to consume. This can be a negative
     *        value to "give back" units.
     * @param timeoutMs the timeout in milliseconds. Pass 0 to block
     *        indefinitely. To poll if the limiter is
     *        currently over its limit, use {@link tryConsumeUnits} instead.
     * @param alwaysConsume if true, consume units even on timeout
     * @return the amount of time blocked in milliseconds. If not blocked,
     *         0 is returned.
     * @throws TimeoutException if the timeout expires before the units can
     *         be acquired by the limiter.
     * @throws IllegalArgumentException if the timeout is negative
     */
    public int consumeUnitsWithTimeout(long units, int timeoutMs,
        boolean alwaysConsume)
        throws TimeoutException, IllegalArgumentException;

    /**
     * Returns the number of units configured for this rate limiter instance
     * @return the max number of units per second this limiter allows
     */
    public double getLimitPerSecond();

    /**
     * Sets the duration for this rate limiter instance.
     * <p>
     * The duration specifies how far back in time the limiter will
     * go to consume previously unused units.
     * <p>
     * For example, if a limiter had a limit of 1000 units and a
     * duration of 5 seconds, and had no units consumed for at least
     * 5 seconds, a call to<pre>tryConsumeUnits(5000)</pre> will succeed
     * immediately with no waiting.
     * @param durationSecs the duration in seconds
     * @throws UnsupportedOperationException if duration is not
     * supported by this limiter.
     */
    public default void setDuration(double durationSecs) {
        throw new UnsupportedOperationException("Duration not implemented");
    }

    /**
     * Returns the duration configured for this rate limiter instance
     * @return the duration in seconds
     * @throws UnsupportedOperationException if duration is not
     * supported by this limiter.
     */
    public default double getDuration() {
        throw new UnsupportedOperationException("Duration not implemented");
    }

    /**
     * Resets the rate limiter as if it was newly constructed.
     * <p>
     * Allows reuse.
     */
    public void reset();

    /**
     * Sets a new limit (units per second) on the limiter.
     * <p>
     * Note that implementing rate limiters should fully support non-integer
     * (double) values internally, to avoid issues when the limits are set
     * very low.
     * Changing the limit may lead to unexpected spiky behavior, and may
     * affect other threads currently operating on the same limiter instance.
     * @param rateLimitPerSecond the new number of units to allow
     */
    public void setLimitPerSecond(double rateLimitPerSecond);

    /**
     * Consumes units without checking or waiting.
     * the internal amount of units consumed will be updated by the given
     * amount, regardless of its current over/under limit state.
     *
     * @param units the number of units to consume (may be negative to
     *        give back units)
     */
    public void consumeUnitsUnconditionally(long units);

    /**
     * Returns the current rate as a percentage of current limit.
     * @return the rate as of this instant in time.
     */
    public double getCurrentRate();

    /**
     * Sets the current rate as a percentage of current limit.
     * <p>
     * This modifies the internal limiter values; it does not modify
     * the rate limit.
     *
     * @param rateToSet percentage to set the current rate to. This may
     *        be greater than 100.0 to set the limiter to "over its limit".
     */
    public void setCurrentRate(double rateToSet);

}
