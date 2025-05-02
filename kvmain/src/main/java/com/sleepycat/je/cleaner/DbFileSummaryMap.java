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

package com.sleepycat.je.cleaner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DbFileSummaryMap {

    private Map<Long, DbFileSummary> map;

    /**
     * Creates a map of Long file number to DbFileSummary.
     */
    public DbFileSummaryMap() {
        map = new HashMap<>();
    }

    /**
     * Returns the DbFileSummary for the given file, allocating it if
     * necessary.
     *
     * <p>Must be called under the log write latch.</p>
     *
     * @param fileNum the file identifying the summary.
     */
    public DbFileSummary get(Long fileNum) {

        DbFileSummary summary = map.get(fileNum);
        if (summary == null) {
            summary = new DbFileSummary();
            Object oldVal = map.put(fileNum, summary);
            assert oldVal == null;
        }
        return summary;
    }

    public Set<Map.Entry<Long,DbFileSummary>> entrySet() {
        return map.entrySet();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
