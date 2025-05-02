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

import com.sleepycat.je.utilint.StatDefinition.StatType;
import com.sleepycat.utilint.FormatUtil;

/**
 * A long JE stat that uses {@link AtomicLong} to be thread safe.
 */
public class AtomicLongStat extends StatWithValueType<Long> {
    private static final long serialVersionUID = 1L;

    private final AtomicLong counter;

    public AtomicLongStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        counter = new AtomicLong();
    }

    AtomicLongStat(StatDefinition definition, long value) {
        super(definition);
        counter = new AtomicLong(value);
    }

    @Override
    public Class<Long> getValueType() {
        return Long.class;
    }

    @Override
    public Long get() {
        return counter.get();
    }

    /** A non-boxing variant. */
    public void set(long newValue) {
        counter.set(newValue);
    }

    @Override
    public void set(Long newValue) {
        counter.set(newValue);
    }

    public void increment() {
        counter.incrementAndGet();
    }

    public void decrement() {
        counter.decrementAndGet();
    }

    public void add(long count) {
        counter.addAndGet(count);
    }

    @Override
    public void add(Stat<Long> other) {
        counter.addAndGet(other.get());
    }

    @Override
    public void clear() {
        counter.set(0L);
    }

    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        AtomicLongStat ret = copy();
        if (definition.getType() == StatType.INCREMENTAL) {
            ret.set(counter.get() - base.get());
        }
        return ret;
    }

    @Override
    public void negate() {
        if (definition.getType() == StatType.INCREMENTAL) {

            /*
             * Negate the value atomically, retrying if another change
             * intervenes.  This loop emulates the behavior of
             * AtomicLong.getAndIncrement.
             */
            while (true) {
                final long current = counter.get();
                if (counter.compareAndSet(current, -current)) {
                    return;
                }
            }
        }
    }

    @Override
    public AtomicLongStat copy() {
        return new AtomicLongStat(definition, counter.get());
    }

    @Override
    public AtomicLongStat copyAndClear() {
        return new AtomicLongStat(definition, counter.getAndSet(0));
    }

    @Override
    protected String getFormattedValue() {
        return FormatUtil.decimalScale0().format(counter.get());
    }

    @Override
    public boolean isNotSet() {
        return (counter.get() == 0);
    }
}
