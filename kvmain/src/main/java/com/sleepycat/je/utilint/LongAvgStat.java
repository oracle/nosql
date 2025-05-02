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

/**
 * A long JE stat that computes the average of the values in the current time
 * period.  Returns zero if no values are observed.
 */
public class LongAvgStat extends StatWithValueType<Long> {
    private static final long serialVersionUID = 1L;

    private final LongAvg avg;

    /**
     * Creates an instance of this class.  The definition type must be
     * INCREMENTAL.
     *
     * @param group the statistics group
     * @param definition the statistics definition
     */
    public LongAvgStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        if (definition.getType() != StatType.INCREMENTAL) {
            throw new IllegalArgumentException(
                "The stat type must be INCREMENTAL, found: " +
                definition.getType());
        }
        avg = new LongAvg();
    }

    private LongAvgStat(LongAvgStat other) {
        this(other.definition, other.avg);
    }

    private LongAvgStat(StatDefinition definition, LongAvg avg) {
        super(definition);
        this.avg = avg.copy();
    }

    /**
     * Adds a new value to the average.
     *
     * @param value the new value
     */
    public void add(long value) {
        avg.add(value);
    }

    /* StatWithValueType methods */

    @Override
    public Class<Long> getValueType() {
        return Long.class;
    }

    /* Stat methods */

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void set(Long newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(Stat<Long> other) {
        if (!(other instanceof LongAvgStat)) {
            throw new IllegalArgumentException(
                "Other stat must be a LongAvgStat, found: " + other);
        }
        avg.add(((LongAvgStat) other).avg);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if argument is not a LongAvgStat
     */
    @Override
    public Stat<Long> computeInterval(Stat<Long> base) {
        if (!(base instanceof LongAvgStat)) {
            throw new IllegalArgumentException(
                "Other stat must be a LongAvgStat, found: " + base);
        }
        final LongAvgStat baseAvg = (LongAvgStat) base;
        final LongAvgStat result = copy();
        result.avg.updateInterval(baseAvg.avg);
        return result;
    }

    @Override
    public void negate() {
        avg.negate();
    }

    @Override
    public LongAvgStat copy() {
        return new LongAvgStat(this);
    }

    /* BaseStat methods */

    @Override
    public Long get() {
        return avg.get();
    }

    @Override
    public void clear() {
        avg.clear();
    }

    @Override
    protected String getFormattedValue() {
        return avg.getFormattedValue();
    }

    @Override
    public boolean isNotSet() {
        return avg.isNotSet();
    }
}
