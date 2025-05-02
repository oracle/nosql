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

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.Set;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * Cache implementation that uses a simple timeout/lifetime for its entries
 * <p>
 * If the optional cleanup thread is enabled expired entries will be removed
 * in a timely manner. If the thread is not enabled the user must call the
 * {@link Cache#cleanup} method itself or the number of expired entries will
 * grow without any control. It is recommended that the cleanup task be enabled.
 * <p>
 * The entry lifetime can be changed after construction and will apply
 * to existing entries as well as new entries
 */
public class TimeoutCache<K, V> implements Cache<K, V> {

    /* Maximum lifetime for a value entry in ms */
    private volatile long lifetime;

    /* Map of key and value */
    private final ConcurrentHashMap<K, CacheEntry<V>> cacheMap;

    /* Background expired entry cleanup task */
    private TimeBasedCleanupTask cleanupTask;

    protected TimeoutCache(final CacheConfig config) {
        this.lifetime = config.getLifetime();

        cacheMap = new ConcurrentHashMap<>();

        if (lifetime > 0 && config.getCreateCleanupThread()) {
            cleanupTask = new Cache.TimeBasedCleanupTask(
                this,
                config.getCleanupThreadIntervalMSOrDefault(),
                ("TimeoutCache." + config.getName()));
        }
    }

    private boolean isExpired(CacheEntry<V> entry) {
        return (lifetime > 0) ?
            System.currentTimeMillis() > (entry.getCreateTime() + lifetime) :
            false;
    }

    @Override
    public V get(K key) {
        CacheEntry<V> entry = cacheMap.get(key);
        if (entry != null && !isExpired(entry)) {
            return entry.getValue();
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        CacheEntry<V> entry = cacheMap.put(key, new CacheEntry(value));
        if (entry != null) {
            return entry.getValue();
        }
        return null;
    }

    @Override
    public V remove(K key) {
        final CacheEntry<V> entry = cacheMap.remove(key);
        if (entry != null) {
            return entry.getValue();
        }
        return null;
    }

    @Override
    public long getCreationTime(K key) {
        CacheEntry<V> entry = cacheMap.get(key);
        if (entry != null) {
            return entry.getCreateTime();
        }
        return 0L;
    }

    @Override
    public void clear() {
        cacheMap.clear();
    }

    @Override
    public long getLifetime() {
        return lifetime;
    }

    /**
     * Sets the entry lifetime. A lifetime of 0 is not allowed for this
     * cache type
     */
    @Override
    public void setLifetime(long newLifetimeMS) {
        if (newLifetimeMS == 0L) {
            throw new IllegalArgumentException(
                "Lifetime of 0 is not allowed for a TimeoutCache");
        }
        lifetime = newLifetimeMS;
    }

    @Override
    public Set<V> getAllValues() {
        final Set<V> copy = new HashSet<>();
        cacheMap.forEach((k,v)-> copy.add(v.getValue()));
        return copy;
    }

    @Override
    public Set<K> getAllKeys() {
        return new HashSet<>(cacheMap.keySet());
    }

    @Override
    public int getSize() {
        return cacheMap.size();
    }

    @Override
    public void stop(boolean wait) {
        if (cleanupTask != null) {
            cleanupTask.stop(wait);
        }
    }

    @Override
    public void cleanup() {
        if (lifetime == 0) {
            return;
        }
        /**
         * Remove expired entries from cache.
         * This walks the map looking for expired entries.
         */
        long now = System.currentTimeMillis();
        cacheMap.entrySet().removeIf(
            entry -> now > (entry.getValue().getCreateTime() + lifetime));
    }
}
