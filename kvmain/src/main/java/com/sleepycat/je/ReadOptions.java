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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedException;

import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * Options for calling methods that read records.
 *
 * @since 7.0
 */
public class ReadOptions implements Cloneable {

    private CacheMode cacheMode = null;
    private LockMode lockMode = LockMode.DEFAULT;
    private boolean excludeTombstones = false;

    /**
     * Constructs a ReadOptions object with default values for all properties.
     */
    public ReadOptions() {
    }

    @Override
    public ReadOptions clone() {
        try {
            return (ReadOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw unexpectedException(e);
        }
    }

    /**
     * Sets the {@code CacheMode} to be used for the operation.
     * <p>
     * By default this property is null, meaning that the default specified
     * using {@link Cursor#setCacheMode},
     * {@link DatabaseConfig#setCacheMode} or
     * {@link EnvironmentConfig#setCacheMode} will be used.
     *
     * @param cacheMode is the {@code CacheMode} used for the operation, or
     * null to use the Cursor, Database or Environment default.
     *
     * @return 'this'.
     */
    public ReadOptions setCacheMode(final CacheMode cacheMode) {
        this.cacheMode = cacheMode;
        return this;
    }

    /**
     * Returns the {@code CacheMode} to be used for the operation, or null
     * if the Cursor, Database or Environment default will be used.
     *
     * @see #setCacheMode(CacheMode)
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the {@code LockMode} to be used for the operation.
     * <p>
     * By default this property is {@link LockMode#DEFAULT}.
     *
     * @param lockMode the locking attributes. Specifying null is not allowed.
     *
     * @return 'this'.
     */
    public ReadOptions setLockMode(final LockMode lockMode) {
        DatabaseUtil.checkForNullParam(lockMode, "lockMode");
        this.lockMode = lockMode;
        return this;
    }

    /**
     * Returns the {@code LockMode} to be used for the operation.
     *
     * @see #setLockMode(LockMode)
     */
    public LockMode getLockMode() {
        return lockMode;
    }

    /**
     * Sets the exclude-tombstones option to be used for the operation.
     *
     * <p>By default this property is false. If true, tombstone records
     * will be treated as logically deleted by read operations.</p>
     *
     * @param excludeTombstones the option to use.
     *
     * @return 'this'
     *
     * @see <a href="WriteOptions.html#tombstones">Tombstones</a>
     * @since 19.5
     */
    public ReadOptions setExcludeTombstones(final boolean excludeTombstones) {
        this.excludeTombstones = excludeTombstones;
        return this;
    }

    /**
     * Returns the exclude-tombstones option to be used for the operation.
     *
     * @see #setExcludeTombstones
     * @since 19.5
     */
    public boolean getExcludeTombstones() {
        return excludeTombstones;
    }
    
}
