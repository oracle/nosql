/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.io.IOException;

import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.TestHook;

/**
 * Sets a TTL.timeTestHook that provides a time that elapses at a different
 * rate than normal. Every fakeMillisPerHour after calling this method, JE
 * TTL processing will behave as if one hour has elapsed.
 *
 * In unit tests using this class, add the following to tearDown:
 *  TTL.setTimeTestHook(null);
 */
public class SpeedyTTLTime {

    private final long fakeMillisPerHour;
    private long baseTime;

    public SpeedyTTLTime(final long fakeMillisPerHour) {
        this.fakeMillisPerHour = fakeMillisPerHour;
    }

    public long realTimeToFakeTime(final long realTime) {

        assert realTime > baseTime;

        final long elapsed = realTime - baseTime;

        return baseTime +
            (TTL.MILLIS_PER_HOUR * (elapsed / fakeMillisPerHour));

    }

    public void start() {
        baseTime = TimeSupplier.currentTimeMillis();

        TTL.setTimeTestHook(new TestHook<Long>() {

            @Override
            public Long getHookValue() {
                return realTimeToFakeTime(TimeSupplier.currentTimeMillis());
            }

            @Override
            public void hookSetup() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void doIOHook() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void doHook() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void doHook(Long obj) {
                throw new UnsupportedOperationException();
            }
        });
    }
}
