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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Atomic integer which you can wait on for specified values.
 */
public class WaitableCounter extends AtomicInteger {
    private static final long serialVersionUID = 1L;

    /**
     * Returns true if the counter is at the specified value. If the counter
     * is not at the value, this method will wait for the specified timeout,
     * checking the counter each check period. It will return false if the
     * timeout has been reached and the value was not found.
     *
     * Note that this method returns true if the counter was found to be
     * at the value. The counter may have been changed upon return.
     *
     * @param value the value to check and wait for
     * @param checkPeriodMs time, in milliseconds, between checks
     * @param timeoutMs total time to wait, in milliseconds
     * @return true if the counter was found to be at the value
     */
    public boolean await(final int value, int checkPeriodMs, long timeoutMs) {
        return
            new PollCondition(checkPeriodMs, timeoutMs) {
                @Override
                protected boolean condition() {
                    return get() == value;
                }
            }.await();
    }

    /**
     * Returns true if the counter is at zero. This method is equivalent to
     * calling await(0, checkPeriodMs, timeoutMs)
     *
     * @param checkPeriodMs time, in milliseconds, between checks
     * @param timeoutMs total time to wait, in milliseconds
     * @return true if the counter was found to be at 0
     */
    public boolean awaitZero(int checkPeriodMs, long timeoutMs) {
        return await(0, checkPeriodMs, timeoutMs);
    }
}
