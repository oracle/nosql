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

import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.utilint.FormatUtil;

/**
 * A long JE stat component that computes an average value.
 */
public class LongAvg extends MapStatComponent<Long, LongAvg> {
    private static final long serialVersionUID = 1L;

    /** The number of values. */
    private final AtomicLong count = new AtomicLong();

    /** The total of all values. */
    private final AtomicLong total = new AtomicLong();

    public LongAvg() {
    }

    private LongAvg(LongAvg other) {
        this.count.set(other.count.get());
        this.total.set(other.total.get());
    }

    public void add(long value) {
        count.incrementAndGet();
        total.addAndGet(value);
    }

    public void add(LongAvg other) {
        count.addAndGet(other.count.get());
        total.addAndGet(other.total.get());
    }

    /**
     * Updates the values in this instance to represent the difference between
     * the values stored in this instance and the ones in the argument.
     */
    public void updateInterval(LongAvg other) {
        count.addAndGet(-other.count.get());
        total.addAndGet(-other.total.get());
    }

    public void negate() {
        count.set(-count.get());
        total.set(-total.get());
    }

    /* MapStatComponent methods */

    @Override
    protected String getFormattedValue(boolean useCommas) {
        if (isNotSet()) {
            return "unknown";
        }
        final long value = get();
        return useCommas ?
            FormatUtil.decimalScale0().format(value) :
            Long.toString(value);
    }

    @Override
    public LongAvg copy() {
        return new LongAvg(this);
    }

    /* BaseStat methods */

    @Override
    public Long get() {
        final long cnt = count.get();
        if (cnt == 0) {
            return 0L;
        }
        return total.get() / cnt;
    }

    @Override
    public void clear() {
        count.set(0);
        total.set(0);
    }

    @Override
    public boolean isNotSet() {
        return count.get() == 0;
    }

    /* Object methods */

    @Override
    public String toString() {
        return "LongAvg[" + "count=" + count + " total=" + total + "]";
    }
}
