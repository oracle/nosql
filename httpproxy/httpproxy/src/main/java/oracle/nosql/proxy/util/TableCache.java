/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.util;

import static oracle.nosql.proxy.protocol.Protocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.Protocol.SERVICE_UNAVAILABLE;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_NOT_FOUND;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Timer;
import java.util.TimerTask;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestDispatcherImpl.RequestExceptionHandler;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.table.Table;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;

/**
 * This is an abstract class to cache information about tables. Information
 * is encapsulated in the TableEntry class and includes:
 *  o a Table handle
 *  o a KVStore handle for the target store
 *
 * Implementations must implement the getTable() method to get the required
 * information and create TableEntry instances. For The on-premise and
 * cloud simulator cases there is one store so it is relatively simple.
 *
 * The cloud service case must handle the situation where tables can be in
 * any number of stores.
 */
public abstract class TableCache extends TimerTask {
    /* nsname => entry (Table + TableAPI + store name) */
    protected final Map<String, TableEntry> entryCache;
    protected final SkLogger logger;

    private boolean active;

    /*
     * Handle entry expiration and refresh
     */

    /*
     * Entries expire after this many ms of inactivity (gets).
     * If set to zero, entries never expire and are never
     * refreshed.
     */
    private final long entryExpirationMs;

    /*
     * Refresh entries if they haven't been updated for this
     * many milliseconds. If set to zero, entries are never
     * refreshed.
     */
    private final long entryRefreshMs;

    private Timer entryTimer;

    protected TableCache(SkLogger logger) {
        this(logger, 0, 0, 0);
    }

    /**
     * Create an instance that allows for entries to expire.
     * @param entryExpirationMs Entries expire after this many ms of
     * inactivity (gets). If set to zero, entries never expire.
     * @param entryRefreshMs Refresh entries if they haven't been updated
     * for this many milliseconds. If set to zero, entries are never refreshed.
     * Must be less than entryExpirationMs.
     * @param refreshCheckIntervalMs How often to check for entries that
     * need refreshing or need to be expired (removed). If zero, a default
     * value of 10,000 (10 seconds) will be used.
     *
     * Most external-facing interfaces will use seconds vs ms for
     * configuration. MS are used to allow finer-grained testing.
     */
    protected TableCache(SkLogger logger,
                         long entryExpirationMs,
                         long entryRefreshMs,
                         long refreshCheckIntervalMs) {
        entryCache = new ConcurrentHashMap<String, TableEntry>();
        this.logger = logger;
        this.entryExpirationMs = entryExpirationMs;
        this.entryRefreshMs = entryRefreshMs;
        active = true;

        /*
         * If entries expire or need to be refreshed, set up a thread to
         * refresh and/or remove expired entries. NOTE: it may take up to
         * expirationMs + refreshCheckIntervalMs to actually remove entries.
         */
        if (entryExpirationMs > 0 || entryRefreshMs > 0) {
            if (entryRefreshMs < 0) {
                throw new IllegalArgumentException(
                    "TableCache: entry refresh time must not be negative");
            }
            if (entryRefreshMs >= entryExpirationMs) {
                throw new IllegalArgumentException(
                    "TableCache: entry refresh time must be smaller " +
                    "than entry expiration time");
            }
            /* set a default check interval if not set */
            if (refreshCheckIntervalMs <= 0) {
                refreshCheckIntervalMs = 10000L; /* 10 seconds */
            }
            logger.info("TableCache starting: entryExpirationMs=" +
                        entryExpirationMs + " entryRefreshMs=" +
                        entryRefreshMs + " refreshCheckIntervalMs=" +
                        refreshCheckIntervalMs);
            entryTimer = new Timer("TableCache", true); // run as daemon
            entryTimer.scheduleAtFixedRate(this, 0L,
                                           refreshCheckIntervalMs);
        }
    }

    public long getEntryInactivityMs() {
        return entryExpirationMs;
    }

    public int getCacheSize() {
        return entryCache.size();
    }

    /**
     * Returns a TableEntry for the named table. If the table is not
     * found null is returned. Exceptions are left to the caller. Because
     * this method may call remote services other exceptions may be thrown
     * related to connectivity problems. Table not found or related exceptions
     * may be thrown past this method.
     *
     * @param namespace the namespace, required
     *
     * @param tableName the table name, required. the possible formats for
     * table name:
     * - On prem/cloudsim
     *   name - the user-visible table name
     *   parent.child - user-visible table name for a child table
     * - In the cloud
     *   ocid - the ocid(with underscore) for the table, which is used as the
     *          kv table name
     *   parent_ocid.child_ocid - the kv table name of child table
     *
     * @param lc LogContext
     *
     * @return the entry. If the table is not found a RequestException is
     * thrown.
     */
    public TableEntry getTableEntry(String namespace,
                                    String tableName,
                                    LogContext lc) {
        if (!active) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                                       "Table Cache is inactive");
        }
        TableEntry entry = null;
        String nsname = makeNamespaceName(namespace, tableName);

        /* TODO: stats for cache calls */
        entry = entryCache.get(nsname);
        if (entry == null) {
            /* TODO: how to avoid multiple getEntry() calls for same table? */
            /* TODO: stats for cache misses */
            entry = getTable(namespace, tableName, nsname, lc);
            if (!entry.isInitialized()) {
                /* Table is not initialized, don't cache it */
                return entry;
            }

            long now = System.currentTimeMillis();
            entry.setLastUsed(now);
            entry.setLastRefresh(now);
            logger.fine("TableCache adding: " + nsname);
            /* TODO: stats for cache puts */
            entryCache.put(nsname, entry);
        } else {
            entry.setLastUsed(System.currentTimeMillis());
        }
        return entry;
    }

    /**
     * Get a table entry without doing any updating.
     * This will not try to fetch a table if it does not exist,
     * and it will not update the last used time of an entry.
     *
     * @return the entry, or null if it is not in the cache.
     */
    public TableEntry get(String namespace,
                          String tableName,
                          LogContext lc) {
        String nsname = makeNamespaceName(namespace, tableName);
        return entryCache.get(nsname);
    }

    /**
     * Flushes the TableEntry cache for the specified table
     */
    public void flushEntry(String namespace, String tableName) {
        String nsname = makeNamespaceName(namespace, tableName);
        logger.fine("TableCache flushEntry: " + nsname);
        /* TODO: stats for cache flushes */
        entryCache.remove(nsname);
    }

    public synchronized void clear() {
        logger.fine("TableCache clear");
        /* TODO: stats for cache clears */
        entryCache.clear();
    }

    synchronized public void shutDown() {
        logger.fine("TableCache shutdown");
        active = false;
        if (entryTimer != null) {
            entryTimer.cancel();
        }
        for (TableEntry e : entryCache.values()) {
            e.close();
        }
    }

    /**
     * Save all nsname keys from the cache into a file.
     * This is typically called on shutdown.
     * The file may be used on subsequent startup to warm up the
     * cache by calling warmUpFromFile().
     */
    public synchronized void saveKeysToFile(String fileName)
        throws IOException {
        /*
         * Save all nsname keys, one per line. Note that
         * FileWriter truncates the file by default.
         */
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (String nsname : entryCache.keySet()) {
            writer.write(nsname + "\n");
        }
        writer.close();
        logger.fine("Saved " + entryCache.size() +
                    " nsname entries to " + fileName);
    }

    /**
     * Warm up the table cache based on nsnames in a file.
     * All errors are ignored.
     *
     * @param fileName full pathname to warmup file, as created by
     *                 saveKeysToFile().
     * @param recencyMs file must have been modified within this time
     * @param timeLimitMs maximum amount of time to spend warming up.
     *                    Set to zero for no time limit.
     */
    public void warmUpFromFile(String fileName,
                               long recencyMs,
                               long timeLimitMs)
        throws IOException {
        long now = System.currentTimeMillis();
        /* check last modified time of file - if outside recency, skip */
        try {
            File f = new File(fileName);
            long modTime = f.lastModified();
            if ((modTime + recencyMs) < now) {
                logger.info("Skipping warmup of table cache from " +
                            fileName + ": not found or too old.");
                return;
            }
        } catch (Exception e) {
            logger.info("Skipping warmup of table cache from " +
                        fileName + ": got error: " + e);
            return;
        }
        /* read nsname keys */
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String nsname;
        long endTime = 0;
        if (timeLimitMs > 0) {
            endTime = now + timeLimitMs;
        }
        int numLoaded = 0;
        logger.info("Warming up tableCache from file " + fileName);
        while ((nsname = reader.readLine()) != null) {
            /* convert to namespace and tablename */
            if (nsname.isEmpty()) {
                continue;
            }
            now = System.currentTimeMillis();
            String[] arr = nsname.split(":");
            try {
                TableEntry entry;
                if (arr.length == 1) {
                    entry = getTable(null, arr[0], nsname, null);
                } else {
                    entry = getTable(arr[0], arr[1], nsname, null);
                }
                entry.setLastUsed(now);
                entry.setLastRefresh(now);
                entryCache.put(nsname, entry);
                numLoaded++;
            } catch (Exception e) {
                logger.info("Error loading '" + nsname + "': " + e);
                /* ignore errors, continue */
            }
            /* if we're over our time limit, break */
            if (endTime > 0 && now > endTime) {
                logger.info("Table cache warmup time exceeded: skipping " +
                            "remaining entries.");
                break;
            }
        }
        reader.close();
        logger.info("Loaded " + numLoaded + " tableCache entries.");
    }

    public long getEntryRefreshMs() {
        return entryRefreshMs;
    }

    /**
     * Get table information and return a TableEntry for the named table.
     * This is called on a cache miss.
     *
     * If the service used to get the table information is not currently
     * available, throw a RequestException with error code set to
     *     Protocol.SERVICE_UNAVAILABLE.
     *
     * If no table is found, call the tableNotFound() method or otherwise
     * throw an exception.
     *
     * @param namespace the namespace, required
     * @param tableName the table name, required
     * @param nsname the key associated with this table entry
     * @param lc LogContext
     *
     */
    protected abstract TableEntry getTable(String namespace,
                                           String tableName,
                                           String nsname,
                                           LogContext lc);

    /**
     * Return a KVStoreImpl based on store name.
     * For onprem, this will return the default store.
     * For cloud, it will look up the store based on the name.
     *
     * @param storeName the store name
     * @return the store impl for the specified store name
     */
    public abstract KVStoreImpl getStoreByName(String storeName);

    /**
     * A utility method for use by implementors to throw table not found.
     */
    protected void tableNotFound(String namespace,
                                 String tableName,
                                 String msg,
                                 LogContext lc) {
        if (msg == null) {
            msg = "Table not found: " + makeNamespaceName(namespace, tableName);
        }
        logger.fine(msg, lc);
        throw new RequestException(TABLE_NOT_FOUND, msg);
    }

    protected static String makeNamespaceName(String namespace, String name) {
        if (name == null || name.isBlank()) {
            throw new RequestException(ILLEGAL_ARGUMENT,
                      "Table name is required");
        }
        if (namespace == null) {
            return name.toLowerCase();
        }
        StringBuilder sb = new StringBuilder().
            append(namespace.toLowerCase()).append(":")
            .append(name.toLowerCase());
        return sb.toString();
    }

    /**
     * Handler to disable direct driver internal retries.
     */
    protected class NoRetryHandler implements RequestExceptionHandler {
        @Override
        public boolean allowRetry(Request request, Exception e) {
            return false;
        }
    }

    /**
     * This Runnable method is used if entries have a non-zero lifetime.
     * It removes expired entries and refreshes those within the refresh
     * threshold if set.
     */
    @Override
    public void run() {
        long now = System.currentTimeMillis();
        ArrayList<String> toRefresh = new ArrayList<String>();
        for (Map.Entry<String, TableEntry> entry : entryCache.entrySet()) {
            if (isExpired(entry.getValue(), now)) {
                logger.fine("TableCache removing expired entry: " +
                            entry.getKey());
                /* TODO: stats for cache expirations */
                entryCache.remove(entry.getKey());
            } else if (doRefresh(entry.getValue(), now)) {
                /* this is logged below */
                toRefresh.add(entry.getKey());
            }
        }

        /*
         * Refresh those on the list
         */
        for (String nsname : toRefresh) {
            String[] names = nsname.split(":");
            String namespace = names.length == 2 ? names[0] : null;
            String name = names.length == 2 ? names[1] : nsname;
            Exception notFoundException = null;
            try {
                logger.fine("TableCache refreshing: " + nsname);
                /* TODO: stats for cache refreshes */
                TableEntry newEntry = getTable(namespace,
                                               name,
                                               nsname,
                                               null);
                TableEntry oldEntry = entryCache.get(nsname);
                long lastUsed = now;
                if (oldEntry != null) {
                    lastUsed = oldEntry.getLastUsed();
                }
                newEntry.setLastUsed(lastUsed);
                newEntry.setLastRefresh(now);
                /* TODO: stats for cache refresh puts */
                entryCache.put(nsname, newEntry);
            } catch (RequestException re) {
                /* do not remove entry if service is unavailable */
                if (re.getErrorCode() == SERVICE_UNAVAILABLE) {
                    /* TODO: stats for SU errors */
                    logger.info("TableCache refresh failed for " + nsname +
                                ": " + re);
                } else {
                    notFoundException = re;
                }
            } catch (Exception e) {
                notFoundException = e;
            }
            if (notFoundException != null) {
                logger.info("TableCache refresh failed for " + nsname +
                            ", removing from cache, error: " +
                            notFoundException);
                /* TODO: stats for removals dut to errors */
                /* remove entry */
                entryCache.remove(nsname);
            }
        }
    }

    private boolean isExpired(TableEntry entry, long now) {
        long expiration = entry.getLastUsed() + entryExpirationMs;
        return (expiration <= now);
    }

    private boolean doRefresh(TableEntry entry, long now) {
        /* if no threshold, no refresh */
        if (entryRefreshMs == 0) {
            return false;
        }
        return (now >= entry.getLastRefresh() + entryRefreshMs);
    }

    /**
     * The object returned from the TableCache. This allows operations to
     * work on cached instances of stores and Table objects which are costly
     * to create.
     */
    public static abstract class TableEntry {
        private final Table table;
        private long lastUsed;
        private long lastRefresh;

        protected TableEntry(Table table) {
            this.table = table;
        }

        public Table getTable() {
            return table;
        }

        protected void setLastRefresh(long now) {
            lastRefresh = now;
        }

        public long getLastRefresh() {
            return lastRefresh;
        }

        public void setLastUsed(long now) {
            lastUsed = now;
        }

        public long getLastUsed() {
            return lastUsed;
        }

        /**
         * RequestLimits are associated with an entry because they may be
         * set per-table by the TenantManager (e.g. the cloud service). By
         * default there are no limits.
         */
        public RequestLimits getRequestLimits() {
            return null;
        }

        public boolean isMultiRegion() {
            return false;
        }

        public boolean isInitialized() {
            return true;
        }

        abstract public KVStoreImpl getStore();

        abstract public TableAPIImpl getTableAPI();

        abstract public String getStoreName();

        public void close() {}
    }
}
