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
 * A long JE stat component that maintains a maximum value.
 */
public class LongMax extends MapStatComponent<Long, LongMax> {
    private static final long serialVersionUID = 1L;

    /** The maximum value or MIN_VALUE if no values have been seen. */
    private final AtomicLong max;

    public LongMax() {
        this(Long.MIN_VALUE);
    }

    private LongMax(long max) {
        this.max = new AtomicLong(max);
    }

    public void add(long value) {
        max.updateAndGet(v -> Math.max(v, value));
    }

    public void add(LongMax other) {
        max.updateAndGet(v -> Math.max(v, other.max.get()));
    }

    /* MapStatComponent methods */

    @Override
    protected String getFormattedValue(boolean useCommas) {
        final long val = max.get();
        if (val == Long.MIN_VALUE) {
            return "unknown";
        }
        return useCommas ?
            FormatUtil.decimalScale0().format(val) :
            Long.toString(val);
    }

    @Override
    public LongMax copy() {
        return new LongMax(max.get());
    }

    /* BaseStat methods */

    @Override
    public Long get() {
        return max.get();
    }

    @Override
    public void clear() {
        max.set(Long.MIN_VALUE);
    }

    @Override
    public boolean isNotSet() {
        return max.get() == Long.MIN_VALUE;
    }

    /* Object methods */

    @Override
    public String toString() {
        return "LongMax[" + max + "]";
    }
}
