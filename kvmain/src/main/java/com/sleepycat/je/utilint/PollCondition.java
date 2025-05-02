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

import java.util.function.Supplier;

import com.sleepycat.je.util.TimeSupplier;

/**
 * Utility class that permits a "poll based" waiting for a condition.
 *
 * Either the {@link #condition()} method should be implemented in a subclass,
 * or the static {@link #await(long, long, Supplier)} method can be called.
 * The latter allows passing a lambda.
 */
public abstract class PollCondition implements Supplier<Boolean> {

    private final long checkPeriodMs;
    private final long timeoutMs;

    public PollCondition(long checkPeriodMs,
                         long timeoutMs) {
        super();
        assert checkPeriodMs <= timeoutMs;
        this.checkPeriodMs = checkPeriodMs;
        this.timeoutMs = timeoutMs;
    }

    protected abstract boolean condition();

    @Override
    public Boolean get() {
        return condition();
    }

    /**
     * Wait for the {@link #condition()} method to return true.
     */
    public boolean await() {
        return await(checkPeriodMs, timeoutMs, this);
    }

    /**
     * Wait for a condition that can be specified as a lambda.
     */
    public static boolean await(final long checkPeriodMs,
                                final long timeoutMs,
                                final Supplier<Boolean> cond) {

        if (cond.get()) {
            return true;
        }

        final long timeLimit = TimeSupplier.currentTimeMillis() + timeoutMs;
        do {
            try {
                Thread.sleep(checkPeriodMs);
            } catch (InterruptedException e) {
                return false;
            }
            if (cond.get()) {
                return true;
            }
        } while (TimeSupplier.currentTimeMillis() < timeLimit);

        return false;
    }

    /**
     * Like {@link #await(long, long, Supplier)} but calls waitOn.wait rather
     * than Thread.sleep. This allows checking the condition on demand by
     * calling waitOn.notify.
     */
    public static boolean await(final long checkPeriodMs,
                                final long timeoutMs,
                                final Object waitOn,
                                final Supplier<Boolean> cond) {

        if (cond.get()) {
            return true;
        }

        final long timeLimit = TimeSupplier.currentTimeMillis() + timeoutMs;
        do {
            synchronized (waitOn) {
                try {
                    waitOn.wait(checkPeriodMs);
                } catch (InterruptedException e) {
                    return false;
                }
            }
            if (cond.get()) {
                return true;
            }
        } while (TimeSupplier.currentTimeMillis() < timeLimit);

        return false;
    }
}
