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

package oracle.kv.impl.util;

import oracle.kv.KVStore;
import oracle.kv.impl.api.KVStatsMonitor;

/**
 * Manages the watcher names to avoid collision.
 *
 * <p>The format of the watcher name is
 * &lt;function prefix&gt;-&lt;classname&gt;.&lt;name&gt;.
 */
public class WatcherNames {

    /**
     * The watcher name for the client-side kvstats monitor.
     */
    public static final String KVSTATS_MONITOR =
        String.format("%s-%s.monitor", "ClientMonitor",
                      KVStatsMonitor.class.getName());

    /**
     * The watcher name for the server-side operation stats tracking.
     */
    public static final String SERVER_STATS_TRACKER =
        String.format("%s-%s.monitor", "ServerMonitor",
                      /*
                       * Need to use the string since this class is included in
                       * the client jar while the server-side tracker is not.
                       */
                      "oracle.kv.impl.rep.OperationsStatsTracker");

    /**
     * Returns a watcher name for the {@link KVStore#getStats(String,
     * boolean)}.
     *
     * The name is prefixed to avoid collision.
     */
    public static String getKVStoreGetStatsWatcherName(String name) {
        return String.format("%s-%s.%s", "KVStore.getStats",
            KVStore.class.getName(), name);
    }

    /**
     * Returns a watcher name for the {@link KVStore#getStats(boolean)} where
     * no watcher name is provided.
     */
    public static String getKVStoreGetStatsWatcherName() {
        return String.format("%s-%s.null", "KVStore.getStatsAnonymous",
            KVStore.class.getName());
    }

    /**
     * Returns a watcher name prefixed with class and an arbitrary name.
     *
     * This method can be used for ad-hoc watcher names.
     */
    public static String getWatcherName(Class<?> cls, String name) {
        return String.format("%s-%s.%s", "Adhoc", cls.getName(), name);
    }
}
