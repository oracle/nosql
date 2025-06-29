/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
package oracle.kv.impl.measurement;

import java.io.Serializable;

import oracle.kv.impl.util.FormatUtils;

import com.sleepycat.je.rep.arbiter.ArbiterStats;

/**
 * Complete dump of arbiter stats.
 */
public class ArbiterNodeStats implements ConciseStats, Serializable {

    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;
    private final ArbiterStats arbiterStats;

    public ArbiterNodeStats(long start, long end, ArbiterStats arbiterStats) {
        this.start = start;
        this.end = end;
        this.arbiterStats = arbiterStats;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "Arbiter stats [" + FormatUtils.formatTimeMillis(end) + "]\n" +
            arbiterStats.toString();
    }

    @Override
    public String getFormattedStats() {
        return arbiterStats.toString();
    }

    public ArbiterStats getArbiterStats() {
        return arbiterStats;
    }
}
