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
 * A stat component based on an AtomicLong.
 */
public class AtomicLongComponent
        extends MapStatComponent<Long, AtomicLongComponent> {

    final AtomicLong val;
    private volatile boolean isSet;

    /** Creates an instance of this class. */
    AtomicLongComponent() {
        val = new AtomicLong();
        isSet = false;
    }

    private AtomicLongComponent(long val) {
        this.val = new AtomicLong(val);
    }

    /**
     * Sets the stat to the specified value.
     *
     * @param newValue the new value
     */
    public void set(long newValue) {
        val.set(newValue);
        isSet = true;
    }

    /**
     * Adds the specified value.
     *
     * @param inc the value to add.
     */
    public void add(long inc) {
        val.addAndGet(inc);
        isSet = true;
    }

    @Override
    public Long get() {
        return val.get();
    }

    @Override
    public void clear() {
        val.set(0);
        isSet = false;
    }

    @Override
    public AtomicLongComponent copy() {
        AtomicLongComponent copyComp = new AtomicLongComponent(val.get());
        copyComp.isSet = isSet;
        return copyComp;
    }

    @Override
    protected String getFormattedValue(boolean useCommas) {
        if (useCommas) {
            return FormatUtil.decimalScale0().format(val.get());
        } else {
            return val.toString();
        }
    }

    @Override
    public boolean isNotSet() {
        return isSet == false;
    }

    @Override
    public String toString() {
        return val.toString();
    }
}
