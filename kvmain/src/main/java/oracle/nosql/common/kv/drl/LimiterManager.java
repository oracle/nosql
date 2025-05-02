/*-
 * Copyright (C) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.kv.drl;

import static oracle.nosql.nson.util.PackedInteger.readSortedInt;
import static oracle.nosql.nson.util.PackedInteger.getReadSortedIntLength;
import static oracle.nosql.nson.util.PackedInteger.readSortedLong;
import static oracle.nosql.nson.util.PackedInteger.getReadSortedLongLength;
import static oracle.nosql.nson.util.PackedInteger.writeSortedInt;
import static oracle.nosql.nson.util.PackedInteger.writeSortedLong;

import java.lang.Runnable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.ReadThroughputException;
import oracle.kv.StatementResult;
import oracle.kv.Version;
import oracle.kv.WriteThroughputException;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.WriteOptions;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.ratelimit.SimpleRateLimiter;
import oracle.nosql.common.sklogger.SkLogger;

/**
 * A class to manage rate limiting and delaying responses
 */
public class LimiterManager {

    /* one for each DRL KVStore */
    private static class DRLTable {
        TableImpl table;
        Version lastVersion;
    }

    /* map of storeName->DRLTable for backing stores */
    private HashMap<String, DRLTable> drlTablesMap;

    /* map of storeName->KVStoreImpl for backing stores */
    private HashMap<String, KVStoreImpl> storeMap;

    private final SkLogger logger;

    public static class Config {
        /* use distributed mode: synchronize to backing store */
        public boolean useDistributed;
        /* backing store table name (required) */
        public String drlTableName;
        /* backing store namespace (optional) */
        public String drlNamespace;
        /* desired DRL update interval */
        public long drlUpdateIntervalMs;
        /* minimum update interval */
        public long drlMinUpdateIntervalMs;
        /* use N milliseconds of limits "credit" from the past before limiting */
        public int limitCreditMs;
        /*
         * rateFactor allows us to tweak the resultant throughput in case
         * the logic ends up being too strict or too weak. For example, if
         * most tables are not getting their full allocated throughput, we can
         * set this value higher (to, say, 1.1 from default 1.0).
         */
        public double rateFactor;
        /* size of the delay pool (max number of concurrent threads) */
        public int delayPoolSize;

        /* sets reasonable defaults */
        public Config() {
            useDistributed = false;
            drlTableName = "drlTable";
            drlNamespace = null;
            drlUpdateIntervalMs = 200; /* 5 times per second */
            drlMinUpdateIntervalMs = 20;
            limitCreditMs = 5000; /* five seconds */
            rateFactor = 1.0;
            delayPoolSize = 5;
        }

        void copyFrom(Config other) {
            this.useDistributed = other.useDistributed;
            this.drlTableName = other.drlTableName;
            this.drlNamespace = other.drlNamespace;
            this.drlUpdateIntervalMs = other.drlUpdateIntervalMs;
            this.drlMinUpdateIntervalMs = other.drlMinUpdateIntervalMs;
            this.limitCreditMs = other.limitCreditMs;
            this.rateFactor = other.rateFactor;
            this.delayPoolSize = other.delayPoolSize;
        }
    }

    private Config config;

    /* main map of rate limiters */
    private HashMap<LimiterMapKey, LimiterMapEntry> limiterMap;

    /* thread pool for managing delayed responses */
    private ScheduledThreadPoolExecutor delayPool;

    /* max number of queued items for delay pool */
    private final static int MAX_DELAY_QUEUE_SIZE = 1500;

    /* start time of most recent execution */
    private long executionStart;

    private Random rand;

    /* DRL service ID: randomly generated */
    private long serviceID;

    /* one timestamp for each of the other services */
    private HashMap<String, Long> lastReads;

    /* Simple stats */
    public static class Stats {
        /* raw number of delays due to limiting */
        public long delayedResponses;
        /* raw milliseconds delayed */
        public long delayedMillis;
        /* number of internal store errors */
        public long storeErrors;
        /* number of tables active */
        public int activeTables;
        /* number of tables that have gone over their limits */
        public int overLimitTables;
    }

    /* stats accumulated since last collection */
    private Stats curstats;

    /* used when reading DRL records from the store(s) */
    private ReadOptions DRLReadOptions;

    /* used when writing DRL records to the store(s) */
    private WriteOptions DRLWriteOptions;

    /**
     * Create and start a new Limiter Manager.
     *
     * This implements the core rate limiting logic:
     * - A RateLimiter is created for each unique tenant/table/op (r/w).
     * - after each operation, this manager is called with the consumed
     *   units. If adding the units to the limiter throws it over the
     *   limit, the response will be delayed by the proper amount of time
     *   to bring the limiter back under its limit.
     *
     * @param logger Logger instance to use for messages
     * @param config configuration parameters. Copied into internal object.
     */
    public LimiterManager(SkLogger logger,
                          Config config) {
        this.logger = logger;
        this.config = new Config();
        this.config.copyFrom(config);
// TODO this.config.validate();

        /* main map */
        limiterMap = new HashMap<LimiterMapKey, LimiterMapEntry>(100);

        /* drl tables map (backing stores) */
        drlTablesMap = new HashMap<String, DRLTable>(10);
        /* store map (backing stores) */
        storeMap = new HashMap<String, KVStoreImpl>(10);

        delayPool = new ScheduledThreadPoolExecutor(config.delayPoolSize);

        rand = new Random();
        curstats = new Stats();

        if (config.useDistributed == true && config.drlUpdateIntervalMs > 0) {
            // generate unique ID
            this.serviceID = rand.nextLong();
            this.lastReads = new HashMap<String, Long>();
            /* start a task to update distributed limiter values */
            try {
                delayPool.scheduleWithFixedDelay(() -> {
                          try {
                              this.writeAndReadDataToStores();
                              this.delayNextExecution();
                          } catch (Exception e) {
                              info("Error: Could not update DRL " +
                                     "backing store: " + e);
                              curstats.storeErrors++;
                          }
                      }, 100, config.drlMinUpdateIntervalMs,
                         TimeUnit.MILLISECONDS);
                info("Started distributed updater: interval=" +
                                config.drlUpdateIntervalMs + "ms");
            } catch (RejectedExecutionException ree) {
                /* TODO: what now? should this be fatal? */
                logger.severe("Can't start DRL backing store updater: " + ree);
            }
        } else {
            /* set up a simple timer to clear old entries */
            try {
                delayPool.scheduleAtFixedRate(() -> {this.clearOldEntries();},
                          0, 1000, TimeUnit.MILLISECONDS);
                info("Started DRL in non-distributed mode");
            } catch (RejectedExecutionException ree) {
                /* TODO: what now? should this be fatal? */
                logger.severe("Can't start DRL limiter map cleaner: " + ree);
            }
        }

        DRLReadOptions = new ReadOptions(Consistency.ABSOLUTE,
                                         50,
                                         TimeUnit.MILLISECONDS);
        DRLWriteOptions = new WriteOptions(Durability.COMMIT_NO_SYNC,
                                           50,
                                           TimeUnit.MILLISECONDS);

        info("Rate limiting enabled: rateFactor=" + config.rateFactor +
             " limitCreditMs=" + config.limitCreditMs);
    }

    /**
     * Collect and reset internal stats.
     *
     * @param stats stats object to fill with current stats
     */
    public void collectStats(Stats stats) {
        if (stats != null && limiterMap != null) {
            stats.activeTables = limiterMap.size();
            stats.overLimitTables = 0;
            Set<LimiterMapKey> keys = limiterMap.keySet();
            for (LimiterMapKey lmKey : keys) {
                LimiterMapEntry lmEntry = limiterMap.get(lmKey);
                if (lmEntry == null) {
                    continue;
                }
                if (lmEntry.overLimit) {
                    lmEntry.overLimit = false;
                    stats.overLimitTables++;
                }
            }
        }
        if (stats != null) {
            stats.delayedResponses = curstats.delayedResponses;
            stats.delayedMillis = curstats.delayedMillis;
            stats.storeErrors = curstats.storeErrors;
            fine("active=" + stats.activeTables +
                 " overLimit=" + stats.overLimitTables +
                 " delayed=" + stats.delayedResponses +
                 " (" + stats.delayedMillis + "ms)" +
                 " errors=" + stats.storeErrors);
        }
        curstats.delayedResponses = 0;
        curstats.delayedMillis = 0;
        curstats.storeErrors = 0;
    }

    private TableImpl createDRLTable(KVStoreImpl store,
                                     String storeName) {
        final String tName;
        if (config.drlNamespace != null) {
            tName = config.drlNamespace + ":" + config.drlTableName;
        } else {
            tName = config.drlTableName;
        }
        String statement = "CREATE TABLE IF NOT EXISTS " + tName +
                           "(id LONG, sid INTEGER, data BINARY, " +
                           "PRIMARY KEY(SHARD(sid), id)) " +
                           "USING TTL 1 hours";
        try {
            info("creating table '" + tName +
                 "' in store " + storeName);
            StatementResult res = store.executeSync(statement);
            if (! res.isSuccessful()) {
                info("Error: could not create table '" + tName +
                     "' in store " + storeName + ": " +
                     res.getErrorMessage());
                curstats.storeErrors++;
                return null;
            }
            TableAPIImpl tableApi = store.getTableAPIImpl();
            return (TableImpl)tableApi.getTable(config.drlNamespace,
                                                config.drlTableName,
                                                true /* bypassCache */);
        } catch (Exception e) {
            info("Error: trying to create table '" + tName +
                 "' in store " + storeName + ": " + e.getMessage());
            curstats.storeErrors++;
            return null;
        }
    }

    private synchronized DRLTable getDRLTableForStore(String storeName,
                                                      KVStoreImpl store) {
        DRLTable drlTable = drlTablesMap.get(storeName);
        if (drlTable != null) {
            return drlTable;
        }
        TableAPIImpl tableApi = store.getTableAPIImpl();
        TableImpl table = (TableImpl) tableApi.getTable(config.drlNamespace,
                                        config.drlTableName,
                                        true /* bypassCache */);
        if (table == null) {
            // try creating table
            table = createDRLTable(store, storeName);
        }

        if (table == null) {
            info("could not get DRL table instance for store name '" +
                 storeName + "'");
            return null;
        }
        drlTable = new DRLTable();
        drlTable.table = table;
        drlTable.lastVersion = null;
        drlTablesMap.put(storeName, drlTable);
        return drlTable;
    }

    private void writeBackingStoreEntry(String storeName, byte[] buf) {
        if (buf == null) {
            return;
        }

        /*
         * Write a row into the store, using serviceID as the
         * id and the entry as the binary value.
         * Use PutIfVersion with previous version to detect if
         * multiple services are mistakenly using the same id.
         *
         * TODO: would be nice if we had a way to not have this record
         *       be replicated or persisted to disk in the kvstore.
         */
        DRLTable drlTable = null;
        try {
            KVStoreImpl store = storeMap.get(storeName);
            if (store == null) {
                info("Error: could not get store instance for store name '" +
                     storeName + "'");
                curstats.storeErrors++;
                return;
            }
            drlTable = drlTablesMap.get(storeName);
            if (drlTable == null) {
                drlTable = getDRLTableForStore(storeName, store);
            }
            if (drlTable == null) {
                return;
            }

// TODO: pre-create Row (save garbage)
            Row row = drlTable.table.createRow();
            row.put("id", serviceID);
            row.put("sid", 1); // fixed shard key
            row.put("data", buf);

            TableAPIImpl api = store.getTableAPIImpl();
            if (drlTable.lastVersion == null) {
                drlTable.lastVersion = api.put(row, null, DRLWriteOptions);
            } else {
                drlTable.lastVersion = api.putIfVersion(row,
                                           drlTable.lastVersion,
                                           null, DRLWriteOptions);
                if (drlTable.lastVersion == null) {
                    info("Error: collision in DRL key for store " +
                         storeName + ": id=" + serviceID);
                    serviceID = rand.nextLong();
                    info("Generated new serviceID=" + serviceID);
                    curstats.storeErrors++;
                }
            }
        } catch (Exception e) {
            info("Error: writing rate limiter data to backing store " +
                 storeName + ": " + e);
            if (drlTable != null) {
                drlTable.lastVersion = null;
            }
            curstats.storeErrors++;
        }
    }

    /**
     * Read entries from the backing store.
     * @param storeName the name of the store to use
     * @return true if we found a valid entry
     *         false otherwise
     */
    private boolean readBackingStoreEntries(String storeName) {
        List<Row> rows = null;
        try {
            KVStoreImpl store = storeMap.get(storeName);
            if (store == null) {
                info("Error: could not get store instance for store name '" +
                     storeName + "'");
                curstats.storeErrors++;
                return false;
            }
            DRLTable drlTable = drlTablesMap.get(storeName);
            if (drlTable == null) {
                drlTable = getDRLTableForStore(storeName, store);
            }
            if (drlTable == null) {
                return false;
            }

            /*
             * Read all rows in a single multiGet operation using
             * single shard key
             */
            PrimaryKey key = drlTable.table.createPrimaryKey();
            key.put("sid", 1); // fixed shard key

            TableAPIImpl api = store.getTableAPIImpl();
            fine("reading entries from backing store " + storeName);
            rows = api.multiGet(key, null, DRLReadOptions);
        } catch (Exception e) {
            info("Error: reading rate limiter entries from backing store " +
                 storeName + ": " + e);
            curstats.storeErrors++;
            return false;
        }

        /* it's possible there are no records */
        if (rows == null) {
            fine("no entries found for store " + storeName);
            return false;
        }

        boolean foundValidEntry = false;
        for (Row row : rows) {
            byte[] buf = null;
            long id = 0;
            try {
                FieldValue idfv = row.get("id");
                if (idfv == null) {
                    fine("Error: no 'id' found in entry for store " +
                         storeName);
                    curstats.storeErrors++;
                    continue;
                }
                id = idfv.asLong().get();
                if (id == serviceID) {
                    fine("Skipping own entry for store " + storeName);
                    continue;
                }
                FieldValue datafv = row.get("data");
                if (datafv == null) {
                    fine("Error: no 'data' found in id=" + (id % 10000) +
                         " entry for store " + storeName);
                    curstats.storeErrors++;
                    continue;
                }
                buf = datafv.asBinary().get();
            } catch (Exception e) {
                info("Error: could not get binary limiter data from store " +
                     storeName + ": " + e);
                curstats.storeErrors++;
                continue;
            }
            if (buf == null || buf.length == 0) {
                /* empty record, this is possible */
                fine("empty 'data' found in entry " + (id % 10000) +
                     " for store " + storeName);
                continue;
            }
            if (processBackingStoreEntry(buf, id, storeName)) {
                foundValidEntry = true;
            }
        }
        return foundValidEntry;
    }

    private boolean processBackingStoreEntry(byte[] buf, long id,
                                             String storeName) {
        LimiterMapKey lmKey = new LimiterMapKey(storeName, 0);
        long now = System.currentTimeMillis();
        try {
            int off = 0;
            /* the first long is the timestamp this record was written */
            long thenMs = readSortedLong(buf, off);
            off += getReadSortedLongLength(buf, off);
            /* of this record is over 10 minutes old, skip it */
            /* this depends on ntp clock synchronization across services */
            if ((thenMs + 600_000) < now) {
                fine("skipping old record for id=" + (id % 10000) +
                            ": store=" + storeName);
                return true; /* this is a valid entry */
            }
            /* if we've already read this record, skip it */
            fine("time from id=" + (id % 10000) + " record: " + thenMs);
            String lrkey = storeName + "_" + id;
            Long val = lastReads.get(lrkey);
            if (val != null && thenMs <= val.longValue()) {
                fine("skipping already-read record for id=" + (id % 10000) +
                            ": store=" + storeName);
                return true; /* this is a valid entry */
            }
            lastReads.put(lrkey, thenMs);
            long RUs, WUs;
            while (off < buf.length) {
                lmKey.tableId = readSortedLong(buf, off);
                off += getReadSortedLongLength(buf, off);
                RUs = readSortedInt(buf, off);
                off += getReadSortedIntLength(buf, off);
                WUs = readSortedInt(buf, off);
                off += getReadSortedIntLength(buf, off);
                if (RUs <= 0 && WUs <= 0) {
                    continue;
                }
                LimiterMapEntry lmEntry = limiterMap.get(lmKey);
                if (lmEntry == null) {
                    /* no need to create? We don't have the limits */
                    fine("skipping id=" + (id % 10000) +
                            " entry for tableId=" + lmKey.tableId +
                            " store=" + storeName);
                    continue;
                }
                /* for internal testing, we may look up our own records */
                if (id == serviceID) {
                    fine("skipping own entry: tableId=" +
                                lmKey.tableId + " RUs=" + RUs + " WUs=" + WUs);
                    continue;
                }
                if (RUs > 0) {
                    lmEntry.readLimiter.consumeExternally(RUs);
                    lmEntry.lastUpdate = now;
                    fine("applied units from id=" + (id % 10000) + ": " +
                                "tableId=" + lmKey.tableId + " RUS=" + RUs +
                                " rate=" +
                                String.format("%.3f",
                                    lmEntry.readLimiter.getCurrentRate()));
                }
                if (WUs > 0) {
                    lmEntry.writeLimiter.consumeExternally(WUs);
                    lmEntry.lastUpdate = now;
                    fine("applied units from id=" + (id % 10000) + ": " +
                                "tableId=" + lmKey.tableId + " WUS=" + WUs +
                                " rate=" +
                                String.format("%.3f",
                                    lmEntry.writeLimiter.getCurrentRate()));
                }
            }
        } catch (Exception e) {
           info("Error: deserializing rate limiter entry for id=" +
                (id % 10000) + " store=" + storeName + ": " + e);
           curstats.storeErrors++;
           return false;
        }
        return true;
    }

    /*
     * Clear old entries from map. This is only used if we are not
     * doing distributed rate limiting using backing stores.
     */
    private void clearOldEntries() {
        if (limiterMap == null) {
            return;
        }
        /*
         * walk limiter map. Clear old entries.
         */
        long now = System.currentTimeMillis();
        Iterator<LimiterMapKey> it = limiterMap.keySet().iterator();
        while (it.hasNext()) {
            LimiterMapKey lmKey = it.next();
            LimiterMapEntry lmEntry = limiterMap.get(lmKey);
            if (lmEntry != null && (lmEntry.lastUpdate + 60_000) < now) {
                fine("Removing limiter entry: tableId=" + lmKey.tableId);
                it.remove();
            }
        }
    }

    /*
     * Try to keep the backing store updates happening at consistent
     * intervals. The interval may change based on how much usage the
     * stores are getting and how many tables are operating close to
     * or over their limits (close to limits == faster execution).
     */
    private void delayNextExecution() {
        long now = System.currentTimeMillis();

// TODO: adjust drlUpdateIntervalMs based on usage: if no tables close to
// limits, slow down the rate of distributed updates, up to some maximum.

        long executionTime = now - executionStart;
        long sleepTime = (config.drlUpdateIntervalMs - executionTime) -
                         config.drlMinUpdateIntervalMs;
        if (sleepTime <= 0) {
            fine("Execution falling behind");
            return;
        }
        try {
            Thread.sleep(sleepTime);
        } catch (Throwable t) {}
    }

    /*
     * Update backing store(s) with our information, and read backing
     * store(s) for other services' information.
     */
    private void writeAndReadDataToStores() {
        executionStart = System.currentTimeMillis();
        if (limiterMap == null ||
            limiterMap.size() == 0) {
            return;
        }

        /* walk limiter map. Get a set of currently used store names. */
        Set<String> storeNames = new HashSet<String>();
        Set<LimiterMapKey> keys = limiterMap.keySet();
        for (LimiterMapKey lmKey : keys) {
            storeNames.add(lmKey.storeName);
        }

        /*
         * create one max size byte array to avoid more GC.
         * 19 = max serialized size of tableId(long) + RUs(int) + WUs(int)
         * 9 = max serialized size of timestamp(long)
         */
        int maxBytes = (limiterMap.size() * 19) + 9;
        byte[] buf = new byte[maxBytes];

        /*
         * Write records to backing store(s):
         * Walk each store. Write entries to backing store if there are
         * any active tables for that store.
         *
         * Note: createBackingStoreEntry() also removes any old map elements.
         */
        for (String storeName : storeNames) {
            byte[] entry = createBackingStoreEntry(storeName, buf);
            writeBackingStoreEntry(storeName, entry);
        }

        /*
         * Read records from backing store(s):
         * Walk each store. get records for each other service for each store
         * and apply each entry to local rate limiters.
         */
        for (String storeName : storeNames) {
            readBackingStoreEntries(storeName);
        }
    }

    /*
     * Create the serialized backing store entry for a specific store.
     * Side effect: remove map entries more than 10 minutes old
     */
    private byte[] createBackingStoreEntry(String storeName, byte[] buf) {
        long now = System.currentTimeMillis();
        int startOff = writeSortedLong(buf, 0, now); /* timestamp */
        int off = startOff;
        Iterator<LimiterMapKey> it = limiterMap.keySet().iterator();
        while (it.hasNext()) {
            LimiterMapKey lmKey = it.next();
            /* if entry is not for this store, skip it */
            if (storeName.equals(lmKey.storeName) == false) {
                continue;
            }
            LimiterMapEntry lmEntry = limiterMap.get(lmKey);
            if (lmEntry == null) {
                continue;
            }
            /* if entry hasn't been used in more than 10 minutes, remove it */
            if ((lmEntry.lastUpdate + 600_000) < now) {
                fine("Removing limiter entry: tableId=" +
                     lmKey.tableId + " store=" + storeName);
                it.remove();
                continue;
            }
            /* don't write records for tables with no usage in this period */
            if (lmEntry.readUnitsUsed == 0 && lmEntry.writeUnitsUsed == 0) {
                continue;
            }
            fine("Creating element: tableId=" + lmKey.tableId +
                        " RUs=" + lmEntry.readUnitsUsed +
                        " WUs=" + lmEntry.writeUnitsUsed);
            off = writeSortedLong(buf, off, lmKey.tableId);
            off = writeSortedInt(buf, off, lmEntry.readUnitsUsed);
            off = writeSortedInt(buf, off, lmEntry.writeUnitsUsed);
            lmEntry.readUnitsUsed = lmEntry.writeUnitsUsed = 0;
        }
        if (off == startOff) {
            return null;
        }
        fine("writing entry at " + now);
        /* sigh. we have to make a copy. */
        return Arrays.copyOf(buf, off);
    }

    public void shutDown() {
        if (limiterMap == null) {
            return;
        }
        limiterMap.clear();
        delayPool.shutdownNow();
        limiterMap = null;
    }

    private void fine(String msg, LogContext lc) {
        logger.fine("DRL: S" + (serviceID%10000) + " " + msg, lc);
    }

    private void fine(String msg) {
        fine(msg, null);
    }

    private void info(String msg, LogContext lc) {
        logger.info("DRL: S" + (serviceID%10000) + " " + msg, lc);
    }

    private void info(String msg) {
        info(msg, null);
    }

    /**
     * Return the topmost parent table for this table.
     * If this table isn't a child, this table is returned.
     * Note this is a recursive function.
     * TODO: find out if this is expensive: Does getParent() make any
     *       external requests?
     */
    private TableImpl getTopmostParentTable(TableImpl table) {
        TableImpl parent = (TableImpl) table.getParent();
        if (parent == null || parent == table ||
            parent.getId() == table.getId()) {
            return table;
        }
        return getTopmostParentTable(parent);
    }

    /**
     * Throw a read or write throttling error if this table is
     * over its (read/write) limit.
     */
    public void throwIfOverLimit(String tableName,
                                 TableImpl table,
                                 String storeName,
                                 boolean doesReads,
                                 boolean doesWrites) {
        /* no need if we're not managing rate limiters */
        if (limiterMap == null) {
            return;
        }
        if (table == null || tableName == null || storeName == null) {
            return;
        }
        table = getTopmostParentTable(table);
        TableLimits limits = table.getTableLimits();
        if (limits == null || limits.hasThroughputLimits() == false) {
            return;
        }

        /*
         * Look up limiters in map. If not there, return false.
         */
        LimiterMapKey lmKey = new LimiterMapKey(storeName, table.getId());
        LimiterMapEntry lmEntry = limiterMap.get(lmKey);
        if (lmEntry == null) {
            return;
        }

        /* if a limiter can't consume 0 units, it is over its limit */
        if (doesReads && (lmEntry.readLimiter.consumeExternally(0) > 0)) {
            throw new ReadThroughputException(tableName,
                                              0, /* readRate, unused */
                                              limits.getReadLimit(),
                                              "" /* msg, unused */);
        }
        if (doesWrites && (lmEntry.writeLimiter.consumeExternally(0) > 0)) {
            throw new WriteThroughputException(tableName,
                                               0, /* writeRate, unused */
                                               limits.getWriteLimit(),
                                               "" /* msg, unused */);
        }
    }

    /**
     * Increment used units for a table, possibly scheduling a task for
     * delaying response to client based on table limits.
     * Note: timeoutMs is used as-is: caller should adjust this based
     *       on its known client-side latency.
     * @return if 0, limits not above any threshold; continue as normal.
     *         if > 0, delayTask is scheduled to run after this many ms.
     */
    public int delayResponse(TableImpl table,
                             String storeName,
                             KVStoreImpl store,
                             int readUnitsUsed,
                             int writeUnitsUsed,
                             int timeoutMs,
                             LogContext lc,
                             Runnable delayTask) {
        /* no need if we're not managing rate limiters */
        if (limiterMap == null) {
            return 0;
        }
        if (table == null ||
            storeName == null ||
            store == null ||
            (readUnitsUsed == 0 && writeUnitsUsed == 0)) {
            return 0;
        }
        table = getTopmostParentTable(table);
        TableLimits limits = table.getTableLimits();
        if (limits == null || limits.hasThroughputLimits() == false) {
            return 0;
        }

        /* keep storeName -> store mapping for DRL backing stores */
        if (config.useDistributed) {
            storeMap.put(storeName, store);
        }

        int delayMs = 0;
        long now = System.currentTimeMillis();

        /*
         * Look up limiters in map. If they don't exist, create them.
         */
        LimiterMapKey lmKey = new LimiterMapKey(storeName, table.getId());
        LimiterMapEntry lmEntry = limiterMap.get(lmKey);
        if (lmEntry == null) {
            lmEntry = new LimiterMapEntry(limits.getReadLimit(),
                                        limits.getWriteLimit());
            fine("Adding new entry for tableId=" + table.getId(), lc);
            limiterMap.put(lmKey, lmEntry);
        }

        double readLimit = (double)limits.getReadLimit() * config.rateFactor;
        if (readUnitsUsed != 0 && readLimit > 0.0) {
            /* update limiter if limits have changed */
            if (lmEntry.readLimiter.getLimitPerSecond() != readLimit) {
                lmEntry.readLimiter.setLimitPerSecond(readLimit);
            }
            delayMs = lmEntry.readLimiter.consumeExternally(readUnitsUsed);
            /* no real need for synchronization here */
            lmEntry.readUnitsUsed += readUnitsUsed;
            lmEntry.lastUpdate = now;
        }

        double writeLimit = (double)limits.getWriteLimit() * config.rateFactor;
        if (writeUnitsUsed != 0 && writeLimit > 0.0) {
            /* update limiter if limits have changed */
            if (lmEntry.writeLimiter.getLimitPerSecond() != writeLimit) {
                lmEntry.writeLimiter.setLimitPerSecond(writeLimit);
            }
            int ms = lmEntry.writeLimiter.consumeExternally(writeUnitsUsed);
            if (ms > delayMs) {
                delayMs = ms;
            }
            /* no real need for synchronization here */
            lmEntry.writeUnitsUsed += writeUnitsUsed;
            lmEntry.lastUpdate = now;
        }

        /* if going to delay, set stats overLimit flag */
        if (delayMs > 0) {
            lmEntry.overLimit = true;
        }

        /* make sure delay time is below given timeout */
        if (delayMs > (timeoutMs - 1)) {
            delayMs = timeoutMs - 1;
        }

        if (delayMs <= 0) {
            return 0;
        }

        /* if delay pool size is very large, just skip delaying */
        if (delayPool.getQueue().size() > MAX_DELAY_QUEUE_SIZE) {
            fine("Could not delay response: too many " +
                        "queued responses", lc);
            /* TODO: add this to stats */
            return 0;
        }

        /* delay the response */
        try {
            delayPool.schedule(delayTask,
                               delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            /* couldn't schedule: just do now */
            fine("Could not delay response: " + ree, lc);
            return 0;
        }

        fine("delayed response for " + delayMs +
             "ms due to rate limiting", lc);

        curstats.delayedResponses++;
        curstats.delayedMillis += delayMs;

        return delayMs;
    }

    private class LimiterMapEntry {
        public SimpleRateLimiter readLimiter;
        public SimpleRateLimiter writeLimiter;
        public int readUnitsUsed; /* since last write to backing store */
        public int writeUnitsUsed; /* since last write to backing store */
        public long lastUpdate;
        public boolean overLimit; /* since last stats update (1M intervals) */

        public LimiterMapEntry(int readLimit,
                                 int writeLimit) {
            /* allow for a small configured burst of errors before limiting */
            this.readLimiter = new SimpleRateLimiter(
                                   (double)readLimit * config.rateFactor,
                                   (double)config.limitCreditMs / 1000.0);
            this.writeLimiter = new SimpleRateLimiter(
                                   (double)writeLimit * config.rateFactor,
                                   (double)config.limitCreditMs / 1000.0);
            this.readLimiter.setCurrentRate(0.0);
            this.writeLimiter.setCurrentRate(0.0);
            this.readUnitsUsed = 0;
            this.writeUnitsUsed = 0;
            this.lastUpdate = System.currentTimeMillis();
            this.overLimit = false;
        }
    }

    private class LimiterMapKey implements Comparable<LimiterMapKey> {
        public String storeName;
        public long tableId;
        public LimiterMapKey(String storeName, long tableId) {
            this.storeName = storeName;
            this.tableId = tableId;
        }
        @Override
        public int compareTo(LimiterMapKey other) {
            if (this.tableId > other.tableId) {
                return 1;
            }
            if (this.tableId < other.tableId) {
                return -1;
            }
            return this.storeName.compareTo(other.storeName);
       }
       @Override
       public boolean equals(Object obj) {
           if (this == obj) {
               return true;
           }
           if (obj == null) {
               return false;
           }
           if (getClass() != obj.getClass()) {
               return false;
           }
           LimiterMapKey other = (LimiterMapKey)obj;
           return (compareTo(other) == 0);
       }
       @Override
       public int hashCode() {
           return storeName.hashCode() + (int)tableId;
       }
    }
}
