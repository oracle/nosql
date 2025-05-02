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

import java.util.Iterator;
import java.util.Map.Entry;

import com.sleepycat.je.utilint.StatDefinition.StatType;

/**
 * A JE stat that maintains a map of {@link LongAvg} values which can be looked
 * up with a String key, and that returns results as a formatted string.
 */
public class LongAvgMapStat extends MapStat<Long, LongAvg> {

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
    public LongAvgMapStat(StatGroup group, StatDefinition definition) {
        super(group, definition);
        if (definition.getType() != StatType.INCREMENTAL) {
            throw new IllegalArgumentException(
                "The stat type must be INCREMENTAL, found: " +
                definition.getType());
        }
    }

    private LongAvgMapStat(LongAvgMapStat other) {
        super(other);
    }

    /**
     * Creates, stores, and returns a new stat for the specified key.
     *
     * @param key the key
     * @return the new stat
     */
    public synchronized LongAvg createStat(String key) {
        assert key != null;
        final LongAvg stat = new LongAvg();
        statMap.put(key, stat);
        return stat;
    }

    @Override
    public LongAvgMapStat copy() {
        return new LongAvgMapStat(this);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if argument is not a LongAvgMapStat
     */
    @Override
    public LongAvgMapStat computeInterval(Stat<String> base) {
        if (!(base instanceof LongAvgMapStat)) {
            throw new IllegalArgumentException(
                "Other stat must be a LongAvgMapStat, found: " + base);
        }
        final LongAvgMapStat other = (LongAvgMapStat) base.copy();
        final LongAvgMapStat result = copy();
        synchronized (result) {
            synchronized (other) {
                for (Iterator<Entry<String, LongAvg>> i =
                         result.statMap.entrySet().iterator();
                     i.hasNext(); ) {
                    final Entry<String, LongAvg> e = i.next();
                    final String key = e.getKey();
                    final LongAvg stat = e.getValue();
                    final LongAvg otherStat = other.statMap.get(key);
                    if (otherStat != null) {
                        stat.updateInterval(otherStat);
                    } else {
                        i.remove();
                    }
                }
            }
        }
        return result;
    }

    @Override
    public synchronized void negate() {
        for (final LongAvg avg : statMap.values()) {
            avg.negate();
        }
    }
}
