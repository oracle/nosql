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
 * A JE stat that maintains a map of {@link LongMax} values which can be looked
 * up with a String key, and that returns results as a formatted string.
 */
public final class LongMaxMapStat extends MapStat<Long, LongMax> {

    private static final long serialVersionUID = 1L;

    /**
     * Creates an instance of this class.  The definition type must be
     * INCREMENTAL.
     *
     * @param group the owning group
     * @param definition the associated definition
     * @throws IllegalArgumentException if the stat definition type is not
     * INCREMENTAL
     */
    public LongMaxMapStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        if (definition.getType() != StatType.INCREMENTAL) {
            throw new IllegalArgumentException(
                "The stat type must be INCREMENTAL, found: " +
                definition.getType());
        }
    }

    private LongMaxMapStat(LongMaxMapStat other) {
        super(other);
    }

    /**
     * Creates, stores, and returns a new stat for the specified key.
     *
     * @param key the key
     * @return the new stat
     */
    public synchronized LongMax createStat(String key) {
        assert key != null;
        final LongMax stat = new LongMax();
        statMap.put(key, stat);
        return stat;
    }

    @Override
    public LongMaxMapStat copy() {
        return new LongMaxMapStat(this);
    }

    /** Ignores base for a non-additive stat. */
    @Override
    public LongMaxMapStat computeInterval(Stat<String> base) {
        return copy();
    }

    /** Do nothing for this non-additive stat. */
    @Override
    public synchronized void negate() { }
}
