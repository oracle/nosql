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

package com.sleepycat.je.dbi;

public enum SearchMode {
    SET(true, false, "SET"),
    BOTH(true, true, "BOTH"),
    SET_RANGE(false, false, "SET_RANGE"),
    BOTH_RANGE(false, true, "BOTH_RANGE"),
    ANY_RANGE(false, false, "ANY_RANGE");

    private final boolean exactSearch;
    private final boolean dataSearch;
    private final String name;

    SearchMode(boolean exactSearch, boolean dataSearch, String name) {
        this.exactSearch = exactSearch;
        this.dataSearch = dataSearch;
        this.name = "SearchMode." + name;
    }

    /**
     * Returns true when the key or key/data search is exact, i.e., for SET
     * and BOTH.
     */
    public final boolean isExactSearch() {
        return exactSearch;
    }

    /**
     * Returns true when the data value is included in the search, i.e., for
     * BOTH and BOTH_RANGE. Note that this returns false for ANY_RANGE because
     * the data value is only included in the search for dup DBs.
     */
    public final boolean isDataSearch() {
        return dataSearch;
    }

    @Override
    public String toString() {
        return name;
    }
}
