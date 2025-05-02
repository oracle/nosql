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

import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Complete dump of environment stats.
 */
public class EnvStats implements ConciseStats, Serializable {

    private static final long serialVersionUID = 1L;

    private final EnvironmentStats envStats;
    private final long start;
    private final long end;

    public EnvStats(long start, long end, EnvironmentStats stats) {
        this.start = start;
        this.end = end;
        envStats = stats;
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
        return "Environment stats [" + FormatUtils.formatTimeMillis(end) + "]\n" +
            envStats.toString();
    }

    public EnvironmentStats getStats() {
        return envStats;
    }

    @Override
    public String getFormattedStats() {
        StringBuilder sb = new StringBuilder();
        for (StatGroup sg: envStats.getStatGroups()) {
            sb.append(sg.toStringConcise());
        }
        return sb.toString();
    }
}
