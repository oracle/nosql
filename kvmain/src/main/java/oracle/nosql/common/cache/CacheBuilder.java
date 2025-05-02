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
 * CacheBuilder is used to construct {@link Cache} instances.
 *<p>
 * The builder builds one of 2 types:
 * <ol>
 * <li> {@link LruCache} which supports an LRU eviction algorithm as well as
 * optional time-based eviction
 * </li>
 * <li> {@link TimeoutCache} which supports non-optional time-based eviction
 * and has no capacity limits
 * </li>
 * </ol>
 * If the configuration includes a cache size the LRU cache will be created. If
 * it contains only a lifetime a TimeoutCache will be created. If neither
 * lifetime nor size is specified an exception is thrown as such a case can
 * be handled more simply by a Java class like ConcurrentHashMap. In either case
 * an optional cleanup task can be created to remove expired entries; otherwise
 * the cache can grow without limits, at least in the case of the TimeoutCache.
 * <p>
 * The TimeoutCache will have better behavior than the LRU cache in highly
 * concurrent situations because it can use more efficient data structures. The
 * LRU cache uses Java's LinkeHashMap which requires write locking in the cache
 * that can become a bottleneck
 */
public class CacheBuilder {

    /*
     * Prevent from instantiation
     */
    private CacheBuilder() {
    }

    /**
     * Build a cache instance.
     *
     * Currently, only the LRU policy is supported.
     *
     * @return cache instance
     */
    public static <K, V> Cache<K, V>
        build(CacheConfig config) {
        if (config.getCapacity() != 0) {
            /* LRU */
            return new LruCache<K, V>(config);
        } else if (config.getLifetime() != 0L) {
            return new TimeoutCache<K, V>(config);
        } else {
            throw new IllegalArgumentException(
                "Cache configuration must include either a " +
                "capacity or lifetime. Without either a ConcurrentHashMap " +
                "is more appropriate");
        }
    }

    /**
     * A class for configuring the cache.
     */
    public static class CacheConfig {

        /* default 1 minute wakup interval for cleanup task */
        private static int defaultCleanupIntervalMS = 1000 * 60;

        /**
         * The capacity of a cache. 0 is unlimited and is the default.
         */
        private int capacity = 0;

        /**
         * Entry lifetime in milliseconds. 0 means that entries don't expire.
         */
        private long lifetime = 0;

        /**
         * If lifetime is set, create a thread that wakes up and removes
         * expired entries for active cleanup. Default to true for
         * compatibility with callers before it was an option
         */
        private boolean createCleanupThread = true;

        /**
         * Optional wakeup interval in milliseconds for cleanup thread.
         * If not specified a default is used. The default is the lifetime
         * for an LRU cache and 1 minute for a timeout cache
         */
        private int cleanupIntervalMS = 0;

        /* name */
        private String name = "";

        /**
         * Sets the maximum number of entries that are allowed in the cache.
         * in the cache. If the value is 0 then the cache is unlimited. If
         * non-zero, when the cache reaches capacity it will remove entries
         * based on the cache eviction policy.
         *
         * @param capacity capacity of the cache, unlimited if 0
         * @return this
         * @throws IllegalArgumentException is the capacity is nonpositive
         */
        public CacheConfig setCapacity(final int capacity) {
            if (capacity < 0) {
                throw new IllegalArgumentException(
                    "Cache capacity must be non-negative");
            }
            this.capacity = capacity;
            return this;
        }

        /**
         * Sets the maximum duration that an entry may reside in the cache,
         * specified in milliseconds. If the value is 0 there is no
         * lifetime. If non-zero the cache may create a background task
         * to remove expired entries. This means that the
         * {@link Cache#stop} method must be called to clean up the cache
         * when it is no longer used.
         *
         * @param lifetime entry lifetime in milliseconds, unlimited if 0
         * @return this
         * @throws IllegalArgumentException is the lifetime is nonpositive
         */
        public CacheConfig setLifetime(final long lifetime) {
            if (lifetime < 0) {
                throw new IllegalArgumentException(
                    "Cache lifetime must be non-negative");
            }
            this.lifetime = lifetime;
            return this;
        }

        public int getCapacity() {
            return capacity;
        }

        public long getLifetime() {
            return lifetime;
        }

        /*
         * Defaults to true
         */
        public CacheConfig setCreateCleanupThread(boolean value) {
            createCleanupThread = value;
            return this;
        }

        public boolean getCreateCleanupThread() {
            return createCleanupThread;
        }

        public CacheConfig setName(String name) {
            this.name = name;
            return this;
        }

        public String getName() {
            return name;
        }

        /* in milliseconds */
        public CacheConfig setCleanupThreadIntervalMS(int value) {
            cleanupIntervalMS = value;
            return this;
        }

        public int getCleanupThreadIntervalMS() {
            return cleanupIntervalMS;
        }

        public int getCleanupThreadIntervalMSOrDefault() {
            if (cleanupIntervalMS == 0L) {
                return defaultCleanupIntervalMS;
            }
            return cleanupIntervalMS;
        }
    }
}
