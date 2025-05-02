/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.api;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Maintains the recent history of topologies so that we do not need to query
 * the history table all the time.
 */
public class TopologyHistoryCache {

    /**
     * The default maximum number of total serilized bytes of topologies to be
     * held in the cache.
     *
     * For a topology with 3 SNs, replication factor 3, 16 rep groups, and 2000
     * partitions, the fast externalizable serialization of the topology has
     * around 28k bytes (the size scales almostly linearly with the number of
     * partitions). Suppose an elasticity operation moves half of all the
     * partitions, with each topology change moving one partition. The default
     * will hold that all the topologies for the elasticity operations in the
     * cache which amounts to around 28M bytes.
     */
    private static final int DEFAULT_MAX_CAPACITY = 28000 * 1000;

    /** The default maximum cache time in milli-seconds. Default to one day. */
    private static final long DEFAULT_MAX_CACHE_TIME_MILLIS = 24 * 3600 * 1000;

    /** The logger. */
    private final Logger logger;
    /**
     * The maximum number of total serialized bytes of topologies to hold in
     * the cache.
     */
    private final int maxCapacity;
    /**
     * The maximum duration for a topology to be held in the cache.
     */
    private final long maxCacheTimeMillis;

    /** The current size. */
    private int currSize = 0;

    /**
     * The partition history as a tree map from sequence number to the cached
     * entry.
     */
    private final TreeMap<Integer, Entry> historyMap = new TreeMap<>();

    /** A cache entry. */
    private static class Entry {

        /** The timestamp when the entry is created. */
        private final long timestampMillis;
        /**
         * The serialized bytes of the topology. We serialize the topology into
         * fast externalizable to take advantage of compression of
         * PackedInteger. The topology object itself retains about 6x more
         * memory space. For example, with a 48 RNs, 2000 partition topology,
         * the retained size is more than 178k while only 28k with the
         * serialized format.
         */
        private final byte[] serializedTopology;

        private Entry(Topology topo) {
            this.timestampMillis = System.currentTimeMillis();
            this.serializedTopology = SerializationUtil.getBytes(
                topo, SerialVersion.CURRENT);
        }

        public long getTimestampMillis() {
            return timestampMillis;
        }

        public int getSize() {
            return serializedTopology.length;
        }

        public Topology getTopology() {
            try {
                return Topology.readFastExternalizable(serializedTopology);
            } catch (IOException cause) {
                throw new IllegalStateException(
                    "Unexpected error deserializing cached topology",
                    cause);
            }
        }

    }

    public TopologyHistoryCache(Logger logger) {
        this(logger, DEFAULT_MAX_CAPACITY, DEFAULT_MAX_CACHE_TIME_MILLIS);
    }

    public TopologyHistoryCache(Logger logger,
                                int maxCapacity,
                                long maxCacheTimeMillis) {
        this.logger = logger;
        this.maxCapacity = maxCapacity;
        this.maxCacheTimeMillis = maxCacheTimeMillis;
    }

    /** Puts the topology into the cache if it is not a local topology. */
    public synchronized void put(Topology topology) {
        if (topology == null) {
            return;
        }
        if (topology.getLocalizationNumber() > 0) {
            return;
        }
        cleanUp();
        final int seqNo = topology.getSequenceNumber();
        if (historyMap.containsKey(seqNo)) {
            /*
             * No need to serialize, add or do accounting if already cached.
             */
            return;
        }
        /* Adds the topology to the history. */
        final Entry entry = new Entry(topology);
        historyMap.put(seqNo, entry);
        currSize += entry.getSize();
        logger.info(() ->
                    String.format(
                        "Cached topology with sequence number %s",
                        seqNo));
    }

    /**
     * Cleans up the cache to meet maxCapacity and maxCacheTimeMillis. Removes
     * older entries until we satisfy the maxCapacity. Removes all entries that
     * have passed the maxCacheTimeMillis.
     */
    public synchronized void cleanUp() {
        final long currentTimeMillis = System.currentTimeMillis();
        while (true) {
            final Map.Entry<Integer, Entry> first =
                historyMap.firstEntry();
            if (first == null) {
                return;
            }
            final Entry entry = first.getValue();
            final boolean done =
                (currentTimeMillis - entry.getTimestampMillis()
                 <= maxCacheTimeMillis)
                && (currSize <= maxCapacity);
            if (done) {
                return;
            }
            historyMap.pollFirstEntry();
            currSize -= entry.getSize();
        }
    }

    /**
     * Returns the topology given the sequence number, {@code null} if not in
     * the cache.
     */
    public synchronized Topology get(int seqNo) {
        final Entry entry = historyMap.get(seqNo);
        if (entry == null) {
            return null;
        }
        return entry.getTopology();
    }

    /**
     * Clears all the entries in the cache for testing.
     */
    public synchronized void clearAll() {
        historyMap.clear();
    }
}

