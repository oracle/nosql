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

import com.sleepycat.utilint.FormatUtil;

/**
 * A long stat which maintains a maximum value. It is initialized to
 * Long.MIN_VALUE. The setMax() methods assigns the counter to
 * MAX(counter, new value).
 */
public class LongMaxStat extends LongStat {
    private static final long serialVersionUID = 1L;

    public LongMaxStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        clear();
    }

    public LongMaxStat(StatGroup group,
                       StatDefinition definition,
                       long counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    public void add(Stat<Long> other) {
        setMax(other.get());
    }

    @Override
    public void clear() {
        set(Long.MIN_VALUE);
    }

    /**
     * Set stat to MAX(current stat value, newValue).
     *
     * @return true if the max value was updated.
     */
    public boolean setMax(long newValue) {
        if (counter < newValue) {
            counter = newValue;
            return true;
        }
        return false;
    }

    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        return (counter < base.get() ? base.copy() : copy());
    }

    @Override
    public void negate() {
    }

    @Override
    protected String getFormattedValue() {
        if (counter == Long.MIN_VALUE) {
            return "NONE";
        }

        return FormatUtil.decimalScale0().format(counter);
    }

    @Override
    public boolean isNotSet() {
       return counter == Long.MIN_VALUE;
    }
}
