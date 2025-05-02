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

import com.sleepycat.je.utilint.StatDefinition.StatType;
import com.sleepycat.utilint.FormatUtil;

/**
 * A long JE stat.
 */
public class LongStat extends StatWithValueType<Long> {
    private static final long serialVersionUID = 1L;

    protected long counter;

    public LongStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    public LongStat(StatGroup group, StatDefinition definition, long counter) {
        super(group, definition);
        this.counter = counter;
    }

    @Override
    public Class<Long> getValueType() {
        return Long.class;
    }

    @Override
    public Long get() {
        return counter;
    }

    @Override
    public void set(Long newValue) {
        counter = newValue;
    }

    public void increment() {
        counter++;
    }

    public void add(long count) {
        counter += count;
    }

    public void max(long count) {
        if (count > counter) {
            count = counter;
        }
    }

    @Override
    public void add(Stat<Long> other) {
        counter += other.get();
    }

    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        Stat<Long> ret = copy();
        if (definition.getType() == StatType.INCREMENTAL) {
            ret.set(counter - base.get());
        }
        return ret;
    }

    @Override
    public void negate () {
        if (definition.getType() == StatType.INCREMENTAL) {
            counter = -counter;
        }
    }

    @Override
    public void clear() {
        counter = 0L;
    }

    @Override
    protected String getFormattedValue() {
        return FormatUtil.decimalScale0().format(counter);
    }

    @Override
    public boolean isNotSet() {
       return (counter == 0);
    }
}
