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

import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.je.utilint.StatDefinition.StatType;
import com.sleepycat.utilint.FormatUtil;

/**
 * A int JE stat that uses {@link AtomicInteger} to be thread safe.
 */
public class AtomicIntStat extends StatWithValueType<Integer> {
    private static final long serialVersionUID = 1L;

    private final AtomicInteger counter;

    public AtomicIntStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        counter = new AtomicInteger();
    }

    AtomicIntStat(StatDefinition definition, int value) {
        super(definition);
        counter = new AtomicInteger(value);
    }

    @Override
    public Class<Integer> getValueType() {
        return Integer.class;
    }

    @Override
    public Integer get() {
        return counter.get();
    }

    @Override
    public void set(Integer newValue) {
        counter.set(newValue);
    }

    public void increment() {
        counter.incrementAndGet();
    }

    public void decrement() {
        counter.decrementAndGet();
    }

    public void add(int count) {
        counter.addAndGet(count);
    }

    @Override
    public void add(Stat<Integer> other) {
        counter.addAndGet(other.get());
    }

    @Override
    public void clear() {
        counter.set(0);
    }

    @Override
    public Stat<Integer> computeInterval(Stat<Integer> base) {
        AtomicIntStat ret = copy();
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
             * AtomicInteger.getAndIncrement.
             */
            while (true) {
                final int current = counter.get();
                if (counter.compareAndSet(current, -current)) {
                    return;
                }
            }
        }
    }

    @Override
    public AtomicIntStat copy() {
        return new AtomicIntStat(definition, counter.get());
    }

    @Override
    public AtomicIntStat copyAndClear() {
        return new AtomicIntStat(definition, counter.getAndSet(0));
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
