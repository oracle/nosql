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

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.WriteOptions;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.util.TimeSupplier;

/**
 * A struct for passing and returning certain params to/from 'put' and 'delete'
 * internal operations.
 */
public class WriteParams {

    public final CacheMode cacheMode;
    public final ReplayPreprocessor preprocessor;
    public final ReplicationContext repContext;
    public final int expiration;
    public final boolean expirationInHours;
    public final boolean updateExpiration;
    public final boolean tombstone;
    public final long modificationTime;

    private final String[] allIndexDbNames;
    private final long[] allIndexIds;
    private final boolean[] indexesToUpdate;

    private boolean expirationUpdated = false;
    private long oldExpirationTime = 0;
    private boolean oldTombstone = false;
    
    private boolean enableBeforeImage = false;
    private int beforeImageExpiration = 0;
    private boolean beforeImageExpirationInHours = false;


    /**
     * Uses the given modificationTime -- does not default to the current
     * system time.
     */
    public WriteParams(
        final CacheMode cacheMode,
        final ReplayPreprocessor preprocessor,
        final ReplicationContext repContext,
        final int expiration,
        final boolean expirationInHours,
        final boolean updateExpiration,
        final long modificationTime,
        final boolean tombstone,
        final String[] allIndexDbNames,
        final long[] allIndexIds,
        final boolean[] indexesToUpdate,
        final boolean enableBeforeImage) {

        this.cacheMode = cacheMode;
        this.preprocessor = preprocessor;
        this.repContext = repContext;
        this.expiration = expiration;
        this.expirationInHours = expirationInHours;
        this.updateExpiration = updateExpiration;
        this.modificationTime = modificationTime;
        this.tombstone = tombstone;
        this.allIndexDbNames = allIndexDbNames;
        this.allIndexIds = allIndexIds;
        this.indexesToUpdate = indexesToUpdate;
        this.enableBeforeImage = enableBeforeImage;
    }

    /**
     * Creates a WriteParams struct with beforeimage options
     */

    public WriteParams(
        final CacheMode cacheMode,
        final ReplayPreprocessor preprocessor,
        final ReplicationContext repContext,
        final int expiration,
        final boolean expirationInHours,
        final boolean updateExpiration,
        final long modificationTime,
        final boolean tombstone,
        final String[] allIndexDbNames,
        final long[] allIndexIds,
        final boolean[] indexesToUpdate,
        final boolean enableBeforeImage,
        final int beforeImageExpiration,
        final boolean beforeImageExpirationInHours) {

		this(cacheMode, preprocessor, repContext, expiration, expirationInHours,
				updateExpiration, modificationTime, tombstone, allIndexDbNames,
				allIndexIds, indexesToUpdate, enableBeforeImage);
        this.beforeImageExpiration = beforeImageExpiration;
        this.beforeImageExpirationInHours = beforeImageExpirationInHours;
    }

    /**
     * Creates a WriteParams struct from WriteOptions for user ops, using the
     * current system time to set expiration time.
     */
    public WriteParams(final WriteOptions options,
                       final DatabaseImpl dbImpl) {
        this(
            options.getCacheMode(),
            null /*preprocessor*/, dbImpl.getRepContext(),
            (options.getExpirationTime() != 0) ?
                TTL.systemTimeToExpiration(
                    options.getExpirationTime(),
                    options.getTTLUnit() == TimeUnit.HOURS) :
                TTL.ttlToExpiration(
                    options.getTTL(),
                    options.getTTLUnit()),
            options.getTTLUnit() == TimeUnit.HOURS,
            options.getUpdateTTL(),
            dbImpl.getSortedDuplicates() ?
                options.getModificationTime() :
                getModTime(options.getModificationTime()),
            options.isTombstone(),
            options.getAllIndexDbNames(),
            options.getAllIndexIds(),
            options.getIndexesToUpdate(),
            options.getBeforeImageTTL() > 0,
            options.getBeforeImageTTL(),
            options.getBeforeImageTTLUnit() == TimeUnit.HOURS);
    }

    /**
     * Used to pass an explicit internal expiration value, rather than
     * calculating it from the system time.
     *
     * NOTE: Only used for dup DBs, and therefore modificationTime is
     * always set to zero.
     */
    public WriteParams(
        final CacheMode cacheMode,
        final ReplicationContext repContext,
        final int expiration,
        final boolean expirationInHours,
        final boolean updateExpiration,
        final boolean tombstone) {
        this(
            cacheMode, null /*preprocessor*/, repContext,
            expiration, expirationInHours, updateExpiration,
            0L /*modificationTime*/, tombstone, null, null, null, false);
    }

    /**
     * Used to pass the rep context for internal ops.
     */
    public WriteParams(final ReplicationContext repContext) {
        this(
            null /*cacheMode*/, null /*preprocessor*/, repContext,
            0 /*expiration*/, false /*expirationInHours*/,
            false /*updateExpiration*/, getModTime(0L),
            false /*tombstone*/, null, null, null, false);
    }

    private static long getModTime(final long modTimeParam) {
        return modTimeParam != 0 ? modTimeParam : TimeSupplier.currentTimeMillis();
    }

    public void setExpirationUpdated(boolean val) {
        expirationUpdated = val;
    }

    public boolean getExpirationUpdated() {
        return expirationUpdated;
    }

    public void setOldExpirationTime(long val) {
        oldExpirationTime = val;
    }

    public long getOldExpirationTime() {
        return oldExpirationTime;
    }

    public void setOldTombstone(boolean val) {
        oldTombstone = val;
    }

    public boolean getOldTombstone() {
        return oldTombstone;
    }

    public int getBeforeImageExpiration() {
        return beforeImageExpiration;
    }
    
    public boolean isBeforeImageEnabled() {
        return enableBeforeImage;
    }

    public boolean getBeforeImageExpirationInHours() {
        return beforeImageExpirationInHours;
    }

    public String[] getAllIndexDbNames() {
        return allIndexDbNames;
    }

    public long[] getAllIndexIds() {
        return allIndexIds;
    }

    public boolean[] getIndexesToUpdate() {
        return indexesToUpdate;
    }
}
