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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;

/**
 * Cache implementation based on LRU eviction policy.
 * <p>
 * In addition to the general LRU policy the implementation supports expired
 * entry cleanup when a lookup is performed. If the lifetime of the entry
 * exceeds the configured maximum entry lifetime, the entry will be removed
 * from the cache.
 * <p>
 * If an entry lifetime is specified at construction, background cleaner thread
 * is started that periodically looks for expired entries and removes them.
 * <p>
 * The entry lifetime and cache capacity can be changed after construction.
 */
public class LruCache<K, V> implements Cache<K, V> {

    /*
     * load factor for LinkedHashMap
     */
    private static final float LOAD_FACTOR = 0.6f;

    /* The maximum capacity for the cache. If 0 the cache unbounded */
    private int capacity;

    /* Maximum lifetime for a value entry in ms */
    private volatile long lifetime;

    /* Map of key and value */
    private final LinkedHashMap<K, CacheEntry<V>> cacheMap;

    /* Background expired entry cleanup task */
    private TimeBasedCleanupTask cleanupTask;

    /*
     * do not use a fairness policy -- it makes concurrent access
     * ridiculously slow
     */
    private final ReentrantLock lock = new ReentrantLock();

    /* Count of how many non-expired entries have been evicted due to size */
    private final AtomicInteger evictionCount = new AtomicInteger();

    /**
     * Construct the LRUCache instance. The initial capacity is set to
     * CacheConfig.getCapacity(). The initial entry lifetime is set to
     * CacheConfig.getLifetime(). If the entry lifetime == 0 cache
     * entries do not expire. If the lifetime is > 0 a thread is started
     * to periodically remove expired cache entries.
     */
    protected LruCache(final CacheConfig config) {
        this.capacity = config.getCapacity();
        this.lifetime = config.getLifetime();

        /*
         * Implement removeEldestEntry to implement the LRU policy when
         * the cache reaches capacity.
         */
        cacheMap =
            new LinkedHashMap<K, CacheEntry<V>>(capacity /*initialCapacity*/,
                                                LOAD_FACTOR,
                                                true /* access ordered */) {
            private static final long serialVersionUID = 1L;

            /*  Returns true (remove) if the cache is over capacity. */
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<K, CacheEntry<V>> entry) {
                /* If the cache is bounded, check its size */
                if ((capacity == 0) || (size() <= capacity)) {
                    return false;
                }

                /* Count non-expired entries */
                if (!isExpired(entry.getValue())) {
                    evictionCount.incrementAndGet();
                }
                entryRemoved(entry.getKey(), entry.getValue());
                return true;
            }
        };

        if (lifetime > 0 && config.getCreateCleanupThread()) {
            cleanupTask = new Cache.TimeBasedCleanupTask(
                this,
                config.getCleanupThreadIntervalMSOrDefault(),
                ("LruCache." + config.getName()));
        }
    }

    private boolean isExpired(CacheEntry<V> entry) {
        return (lifetime > 0) ?
                System.currentTimeMillis() > (entry.getCreateTime() + lifetime) :
                false;
    }

    @Override
    public V get(K key) {
        lock();
        try {
            CacheEntry<V> entry = cacheMap.get(key);
            if (entry == null) {
                return null;
            }
            if (isExpired(entry)) {
                remove(key);
                return null;
            }
            return entry.getValue();
        } finally {
            unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        lock();
        try {
            final CacheEntry<V> newEntry = new CacheEntry<>(value);
            final CacheEntry<V> oldEntry = cacheMap.put(key, newEntry);
            if (oldEntry != null) {
                entryRemoved(key, oldEntry);
            }
            entryAdded(key, newEntry);
            return (oldEntry == null) ? null :  oldEntry.getValue();
        } finally {
            unlock();
        }
    }

    /**
     * Called when an entry is added to the cache. This method is invoked
     * after the entry has been inserted into the cacheMap but before the
     * lock is released.
     */
    protected void entryAdded(K key, CacheEntry<V> entry) {}

    /**
     * Called when an entry is removed from the cache. This method is invoked
     * while the lock is held.
     */
    protected void entryRemoved(K key, CacheEntry<V> entry) {}

    @Override
    public V remove(K key) {
        lock();
        try {
            final CacheEntry<V> entry = cacheMap.remove(key);
            if (entry != null) {
                entryRemoved(key, entry);
                return entry.getValue();
            }
            return null;
        } finally {
            unlock();
        }
    }

    /* this does not filter out expired records */
    @Override
    public long getCreationTime(K key) {
        lock();
        try {
            CacheEntry<V> entry = cacheMap.get(key);
            if (entry != null) {
                return entry.getCreateTime();
            }
            return 0L;
        } finally {
            unlock();
        }
    }

    @Override
    public void clear() {
        lock();
        try {
            cacheMap.clear();
        } finally {
            unlock();
        }
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of the cache to the specified value. If the new
     * capacity is 0 the cache is unbounded. If the new capacity > 0 and
     * less than the current capacity the cache is cleared.
     */
    @Override
    public void setCapacity(int newCapacity) {
        lock();
        try {
            if ((newCapacity > 0) && (newCapacity < capacity)) {
                clear();
            }
            capacity = newCapacity;
        } finally {
            unlock();
        }
    }

    @Override
    public long getLifetime() {
        return lifetime;
    }

    /**
     * Sets the entry lifetime. If set to 0, entries will not expire. If the
     * cleanup thread is not running (either the cache was initialized with
     * lifetime == 0, or stop() was called) setting a positive lifetime will
     * not (re)start the cleanup thread. In this case expired entries will be
     * lazily removed.
     */
    @Override
    public void setLifetime(long newLifetimeMS) {
        lifetime = newLifetimeMS;
    }

    /**
     * Gets and resets the number of times a non-expired entry has been removed
     * due to cache capacity limit. The returned value is the count since the
     * creation of the cache or the last call to this method.
     *
     * @return the eviction count
     */
    public int getAndResetEvictionCount() {
        return evictionCount.getAndSet(0);
    }

    // TODO - should this be filtering expired entries?
    @Override
    public Set<V> getAllValues() {
        lock();
        try {
            final Set<V> copy = new HashSet<>();
            for (CacheEntry<V> entry : cacheMap.values()) {
                copy.add(entry.getValue());
            }
            return copy;
        } finally {
            unlock();
        }
    }

    @Override
    public Set<K> getAllKeys() {
        lock();
        try {
            return new HashSet<>(cacheMap.keySet());
        } finally {
            unlock();
        }
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

    /*
     * Encapsulate locking so that extending classes don't need access to
     * the lock in normal usage
     */
    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    /**
     * Remove expired entries from cache.
     */
    @Override
    public void cleanup() {
        /* if lock is not available, skip and try next time */
        if (!lock.tryLock()) {
            return;
        }
        try {
            final Iterator<Map.Entry<K, CacheEntry<V>>> iter =
                cacheMap.entrySet().iterator();
            while (iter.hasNext()) {
                final Map.Entry<K, CacheEntry<V>> entry =
                    iter.next();
                if (isExpired(entry.getValue())) {
                    iter.remove();
                }
            }
        } finally {
            unlock();
        }
    }
}
