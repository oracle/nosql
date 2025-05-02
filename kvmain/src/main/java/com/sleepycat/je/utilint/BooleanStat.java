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

/**
 * A boolean JE stat.
 */
public class BooleanStat extends Stat<Boolean> {
    private static final long serialVersionUID = 1L;

    private Boolean value;

    public BooleanStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    @Override
    public Boolean get() {
        return value;
    }

    @Override
    public void set(Boolean newValue) {
        value = newValue;
    }

    @Override
    public void add(Stat<Boolean> otherStat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        value = false;
    }

    @Override
    public Stat<Boolean> computeInterval(Stat<Boolean> base) {
       return super.copy();
    }

    @Override
    public void negate() {
    }

    @Override
    protected String getFormattedValue() {
        return value.toString();
    }

    @Override
    public boolean isNotSet() {
        return false; // We can't tell if a boolean is not set.
    }
}
