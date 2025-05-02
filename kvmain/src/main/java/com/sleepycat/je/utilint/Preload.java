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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Get;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;

public class Preload {

    /**
     * Preloads INs and (optionally) LNs in the given DB, stopping when the
     * cache is filled up to the given percentage. This simply uses a cursor
     * to warm the cache.
     *
     * @return true if the cache is filled to the given percentage.
     */
    public static boolean preloadDb(final Database db,
                                    final boolean loadLNs,
                                    final int cachePercent) {
        assert cachePercent >= 0 && cachePercent <= 100 : cachePercent;
        final Environment env = db.getEnvironment();
        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
        final MemoryBudget memoryBudget = envImpl.getMemoryBudget();

        final long cacheBudget = memoryBudget.getMaxMemory();
        final long maxBytes = (cacheBudget * cachePercent) / 100;

        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = loadLNs ? new DatabaseEntry() : null;

        try (final Cursor cursor = db.openCursor(null, null)) {
            while (cursor.get(key, data, Get.NEXT, null) != null) {

                final long usedBytes = memoryBudget.getCacheMemoryUsage();

                if (usedBytes >= maxBytes) {
                    return true;
                }
            }
            return false;
        }
    }
}
