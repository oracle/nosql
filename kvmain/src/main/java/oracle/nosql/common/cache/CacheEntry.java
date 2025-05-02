/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.cache;


/**
 * A cache entry object that encapsulates a value and a creation time used
 * for expiring an entry. It is shared by Cache implementations.
 */
public class CacheEntry<V> {

    /*
     * The time at which the entry was created. May be reset to extend
     * the expiration time.
     */
    private long createTime;
    private final V value;

    CacheEntry(V value) {
        this.value = value;
        this.createTime = System.currentTimeMillis();
    }

    public long getCreateTime() {
        return createTime;
    }

    /**
     * Resets the create time causing the entry to remain in the cache
     * past its original expiration time.
     */
    public void resetCreateTime() {
        createTime = System.currentTimeMillis();
    }

    public V getValue() {
        return value;
    }
}
