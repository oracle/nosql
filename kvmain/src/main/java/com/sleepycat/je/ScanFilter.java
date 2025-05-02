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

package com.sleepycat.je;

/**
 * Interface for filtering and stopping a sequential scan of a database.
 */
public interface ScanFilter {

    /**
     * Returned by {@link #checkKey}.
     */
    enum ScanResult {

        /**
         * Indicates the key should be included and the scan should continue.
         */
        INCLUDE(true, false),

        /**
         * Indicates the key should <em>not</em> be included, but the scan
         * should continue.
         */
        EXCLUDE(false, false),

        /**
         * Indicates the key should be included and the scan should stop.
         */
        INCLUDE_STOP(true, true),

        /**
         * Indicates the key should <em>not</em> be included, and the scan
         * should stop.
         */
        EXCLUDE_STOP(false, true);

        private final boolean include;
        private final boolean stop;

        ScanResult(final boolean include, final boolean stop) {
            this.include = include;
            this.stop = stop;
        }

        /**
         * Returns true for {@link #INCLUDE} and {@link #INCLUDE_STOP}.
         */
        public boolean getInclude() {
            return include;
        }

        /**
         * Returns true for {@link #INCLUDE_STOP} and {@link #EXCLUDE_STOP}.
         */
        public boolean getStop() {
            return stop;
        }
    }

    /**
     * Called for each key to determine whether the key should be included or
     * excluded, and whether the scan should stop or continue.
     */
    ScanResult checkKey(byte[] key);
}
