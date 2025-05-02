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

package com.sleepycat.je.beforeimage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


/*
 * A ThreadSafe Cache for the Before Image Entries 
 * K -> abortLSN 
 * V -> DatabaseEntry 
 * TODO Currently Unused because 
 * although the modification times of current record provides a temporal locality
 * in the beforeimage database and can cache next configurable number of entries.
 * As the KV table can span multiple databases, the cache might have a smaller
 * number of hits. But it can be used in mini-envs as table span single database.
 */

public class BeforeImageIndexCache<K, V> {
    private final int maxSize;
    private final ConcurrentHashMap<K,V> cacheMap;
    private final ConcurrentLinkedQueue<K> keysQueue;
    private final AtomicInteger currentSize;

    public BeforeImageIndexCache(int maxSize) {
         this.maxSize = maxSize;
         this.cacheMap = new ConcurrentHashMap<>();
         this.keysQueue = new ConcurrentLinkedQueue<>();
         this.currentSize = new AtomicInteger(0);
    }

    private void evictOldest(int n) {
        int actualEvicted = 0;
        for (int i = 0; i < n; i++) {
             K oldestKey = keysQueue.poll();
             if (oldestKey != null) {
                 cacheMap.remove(oldestKey);
                 actualEvicted++;
             } else {
                 break;
             }
        }
        currentSize.addAndGet(-actualEvicted);
    }

    public V get(K key) {
        return cacheMap.get(key);
    }

    public boolean containsKey(K key) {
        return cacheMap.containsKey(key);
    }

    /* Each update might not be atomic because we need to sync two structures
     * But thats ok, because if there is a cache miss, we will just fetch from
     * disk. 
     */ 
    public void put(K key, V value) {
        if (cacheMap.putIfAbsent(key, value) == null) {
            keysQueue.offer(key);
            //FIFO evict Policy
            currentSize.incrementAndGet();
            //handle cases where lot of puts concurrently instead of 1 
            //TODO can we optimize on number of gets or remove multiple  
            int evictCount = currentSize.get() - maxSize;
            if (evictCount > 0) {
                evictOldest(evictCount);
            }
        }
    }
}
