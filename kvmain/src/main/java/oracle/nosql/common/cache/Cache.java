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

import java.util.Set;

/**
 * Cache is a generic interface for a key/value cache. All methods should be
 * thread-safe, however technically an implementation could be defined to be
 * not thread safe.
 * <p>
 * Instances of this interface are typically built using the
 * {@link CacheBuilder} class.
 */
public interface Cache<K, V> {

    /**
     * Returns the value associated with given key in the cache, or null if
     * there is no value cached for key.
     *
     * @param key
     * @return value
     */
    V get(K key);

    /**
     * Stores the key value pair in the cache.
     *
     * If the cache contains the value associated with key, replace the old
     * value.
     *
     * @param key
     * @param value
     *
     * @return the old value if there was one present, otherwise null.
     */
    V put(K key, V value);

    /**
     * Removes the cached value for given key.
     *
     * @param key
     * @return the previously cached value or null
     */
    V remove(K key);

    /**
     * Returns the cache maximum capacity in number of items cached. Returns
     * 0 for caches without a capacity
     *
     * @return cache maximum capacity
     */
    default int getCapacity() {
        return 0;
    }

    /**
     * Sets the capacity of the cache to the specified value. This is an
     * optional feature. If the cache does not support setting the capacity
     * an UnsupportedOperationException is thrown.
     *
     * @param newCapacity the new capacity
     */
    default void setCapacity(int newCapacity) {
        throw new UnsupportedOperationException("Cache does not suppport" +
                                                " setting the capacity");
    }

    /**
     * Gets the entry lifetime.
     *
     * @return the entry lifetime in milliseconds
     */
    long getLifetime();

    /**
     * Sets the entry lifetime. This is an optional feature. If the cache does
     * not support setting the lifetime an UnsupportedOperationException is
     * thrown.
     *
     * @param newLifetimeMS the new lifetime in milliseconds
     */
    default void setLifetime(long newLifetimeMS) {
        throw new UnsupportedOperationException("Cache does not suppport" +
                                                " setting the lifetime");
    }

    /**
     * Returns a set of all values in the cache.
     *
     * @return all values.
     */
    Set<V> getAllValues();

    /**
     * Returns a set of all keys in the cache.
     *
     * @return all values.
     */
    Set<K> getAllKeys();

    /**
     * Removes all entries from the cache.
     */
    void clear();

    /**
     * size
     */
    int getSize();

    /**
     * Stop all cache background tasks. This is for caches that have created
     * background tasks or threads.
     *
     * @param wait set to true to wait for the background tasks to finish
     */
    default void stop(boolean wait) {}

    /**
     * Returns creation time in milliseconds since the Epoch if available.
     * If an implementation doesn't store creation time it should return
     * 0, the default. This is helpful for caches that expire cache entries.
     */
    default long getCreationTime(K key) { return 0L; }

    /**
     * Performs a write lock operation on the cache if it supports locking;
     * otherwise it is a no-op
     */
    default void lock() {}

    /**
     * Performs a write unlock operation on the cache if it supports locking;
     * otherwise it is a no-op
     */
    default void unlock() {}

    /**
     * Performs a cleanup on the cache for expired entries if the cache
     * supports an entry lifetime.
     */
    default void cleanup() {}


    /**
     * Background time-based cleanup.
     *
     * Once cache instance enable the background cleanup, this task will be
     * launched periodically to look up expired value entry and removed from
     * cache.
     *
     * Notes that the background cleanup task is intensive, since it is aim to
     * look up and remove all expired value entries.
     */
    static class TimeBasedCleanupTask implements Runnable {

        private volatile boolean terminated = false;

        private final Cache cache;

        private final int cleanupPeriodMs;

        private final Thread cleanUpThread;

        TimeBasedCleanupTask(final Cache cache,
                             final int cleanupPeriodMs,
                             final String name) {
            this.cache = cache;
            this.cleanupPeriodMs = cleanupPeriodMs;
            cleanUpThread = new Thread(this, ("CacheCleanUpThread." + name));
            cleanUpThread.setDaemon(true);
            cleanUpThread.start();
        }

        /**
         * Attempt to stop the background activity for the cleanup.
         * @param wait if true, the the method attempts to wait for the
         * background thread to finish background activity.
         */
        void stop(boolean wait) {
            /* Set the flag to notify the run loop that it should exit */
            terminated = true;
            cleanUpThread.interrupt();

            if (wait) {
                try {
                    cleanUpThread.join();
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }

        @Override
        public void run() {
            while (true) {
                if (terminated) {
                    return;
                }
                try {
                    Thread.sleep(cleanupPeriodMs);
                } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
                if (terminated) {
                    return;
                }
                cache.cleanup();
            }
        }
    }
}
