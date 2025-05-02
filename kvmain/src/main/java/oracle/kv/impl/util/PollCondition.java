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

/**
 * Utility class that permits a "poll based" waiting for a condition.
 */
public abstract class PollCondition {

    private final int checkPeriodMs;
    private final long timeoutMs;

    public PollCondition(int checkPeriodMs,
                         long timeoutMs) {
        super();
        assert checkPeriodMs <= timeoutMs;
        this.checkPeriodMs = checkPeriodMs;
        this.timeoutMs = timeoutMs;
    }

    protected abstract boolean condition();

    public boolean await() {
        return await(checkPeriodMs, timeoutMs, () -> condition());
    }

    public static boolean await(int checkPeriodMs,
                                long timeoutMs,
                                PollConditionFunc conditionFunc) {

        /* prevent overflow */
        final long ts = System.currentTimeMillis();
        final long tm = Math.min(timeoutMs, Long.MAX_VALUE - ts);
        final long timeLimit = ts + tm;
        do {
            if (conditionFunc.condition()) {
                return true;
            }
            try {
                Thread.sleep(checkPeriodMs);
            } catch (InterruptedException e) {
                return false;
            }
        } while (System.currentTimeMillis() < timeLimit);

        return false;
    }
}