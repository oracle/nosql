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
import java.util.Iterator;
import java.util.Map;

import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.util.TimeSupplier;

import org.checkerframework.checker.nullness.qual.NonNull;

class DbCache implements Iterable<Map.Entry<DatabaseId, DbCache.DbInfo>> {

    private final EnvironmentImpl envImpl;
    private final long timoutMs;
    private long lastClearTime;
    private int lookups;
    private final Map<DatabaseId, DbCache.DbInfo> cache = new HashMap<>();

    DbCache(final EnvironmentImpl envImpl) {
        this.envImpl = envImpl;

        timoutMs = envImpl.getConfigManager().getDuration(
            EnvironmentParams.ENV_DB_CACHE_TIMEOUT);

        lastClearTime = TimeSupplier.currentTimeMillis();
    }

    /**
     * Used to cache DatabaseImpl and some relatively static info about it.
     * The static info can be used for determining obsolescence without doing
     * an expensive DbTree lookup.This reduces the negative performance impact
     * of periodically clearing the cache via {@link #releaseDbImpls}. The
     * static info can be obtained with a single DbTree lookup per DB for each
     * file cleaned.
     */
    static class DbInfo {

        /**
         * The dbImpl field is non-null if it is cached and 'deleted' is
         * false. However, to allow DB deletions to run is set to null by
         * {@link #releaseDbImpls} and {@link #clearCachePeriodically()}.
         */
        DatabaseImpl dbImpl;

        /*
         * The following fields are available even when dbImpl is null. When
         * the cache is cleared, dbImpl is set to null but these fields are
         * left in place. For the most part they can be used for determining
         * obsolescence even when they are stale, with some caveats below.
         */

        /**
         * The following fields are truly immutable.
         */
        boolean dups;
        boolean internal;
        boolean isLNImmediatelyObsolete;

        /**
         * The name field can change, but in all cases the old name can be
         * used, either as debug output or to pass to the extinction filter.
         */
        String name;

        /**
         * If true, the deleted field is immutable and dbImpl will be null.
         * If false, the deleting field can be used to determine whether a
         * deletion is in progress. If both fields are false, the current
         * dbImpl must be consulted to determine whether the DB is deleted.
         * @see #getDbImpl
         */
        boolean deleted;

        /**
         * If true, the DB will definitely be deleted, so the only possible
         * state transition from [deleting=true,deleted=false] is to
         * [deleting=false,deleted=true].
         * @see #getDbImpl
         */
        boolean deleting;
    }

    /**
     * Returns the possibly-stale info for a given DB ID. Does not call
     * DbTree.getDb if the DB ID has been previously cached.
     *
     * DbInfo.dbImpl is not guaranteed to be non-null even if DbInfo.deleted
     * is false. If DbInfo.dbImpl is null, the DB has been released and may be
     * deleted by another thread.
     */
    DbInfo getDbInfo(final DatabaseId dbId) {
        final DbInfo info = cache.get(dbId);
        if (info != null) {
            return info;
        }
        return getDbImpl(dbId);
    }

    /**
     * Returns the up-to-date info for a given DB ID, returning a non-null
     * DbInfo.dbImpl when the DB is not 'deleted'.
     *
     * If the DB was previously found to be 'deleted', this method does not
     * call DbTree.getDb again (this case is optimized).
     */
    DbInfo getDbImpl(final DatabaseId id) {
        DbInfo info = cache.get(id);
        if (info == null) {
            info = new DbInfo();
            cache.put(id, info);
        }
        if (info.dbImpl == null && !info.deleted) {
            final DatabaseImpl db = envImpl.getDbTree().getDb(id);
            if (db == null) {
                info.deleted = true;
                info.deleting = false;
            } else {
                info.dbImpl = db;
                info.dups = db.getSortedDuplicates();
                info.internal = db.getDbType().isInternal();
                info.isLNImmediatelyObsolete = db.isLNImmediatelyObsolete();
                info.name = db.getName();
                info.deleting = db.isDeleting();
            }
            lookups++;
        }
        return info;
    }

    /**
     * Returns an iterator over the cached entries.
     */
    @Override
    @NonNull
    public Iterator<Map.Entry<DatabaseId, DbInfo>> iterator() {
        return cache.entrySet().iterator();
    }

    /**
     * Returns the number of calls to DbTree.getDb during the cleaner run.
     */
    int getLookups() {
        return lookups;
    }

    /**
     * Releases all non-null DbInfo.dbImpl objects in the cache and sets this
     * field to null.
     */
    void releaseDbImpls() {
        for (final DbInfo info : cache.values()) {
            releaseDbImpl(info);
        }
    }

    /**
     * Releases DbInfo.dbImpl if non-null and sets this field to null.
     */
    void releaseDbImpl(final DbInfo info) {
        if (info.dbImpl != null) {
            envImpl.getDbTree().releaseDb(info.dbImpl);
            info.dbImpl = null;
        }
    }

    /**
     * Release cached dbImpls periodically to prevent starving other threads
     * that need exclusive access to the MapLN (for example,
     * DbTree.deleteMapLN). [#21015]
     */
    void clearCachePeriodically() {

        final long now = TimeSupplier.currentTimeMillis();

        if (now >= lastClearTime + timoutMs) {
            releaseDbImpls();
            lastClearTime = now;
        }
    }
}