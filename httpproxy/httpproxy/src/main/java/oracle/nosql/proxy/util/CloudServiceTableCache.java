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

import static oracle.nosql.proxy.protocol.Protocol.NO_ERROR;
import static oracle.nosql.proxy.protocol.Protocol.SERVICE_UNAVAILABLE;
import static oracle.nosql.proxy.protocol.Protocol.SERVER_ERROR;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.stats.KVStats;
import oracle.kv.stats.NodeMetrics;
import oracle.kv.table.Table;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.proxy.sc.GetStoreResponse;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.util.ph.HealthStatus;

/**
 * A TableCache instance that handles multiple stores and uses
 * the SCTenantManager to acquire store information for a given
 * table.
 */
public class CloudServiceTableCache extends TableCache {

    /* map from store name to handle */
    private final Map<String, KVStoreImpl> storeCache;

    /* set of not reachable stores, used by health check */
    private final Set<String> notReachableStore;

    /* map from store name to locks trying to connect */
    private final Map<String, ReentrantLock> connectLockMap;

    private final TenantManager tm;

    private final KVStoreConfig kvConfig;

    private final MonitorStats stats;

    /* wait at most 2 seconds for a store connection (while locked) */
    private static final long STORE_CONNECT_TIMEOUT_MILLIS = 2_000;

    public CloudServiceTableCache(TenantManager tm,
                                  KVStoreConfig kvConfig,
                                  MonitorStats stats,
                                  SkLogger skLogger,
                                  /* below all in seconds */
                                  int expiration,
                                  int refresh,
                                  int checkInterval) {
        super(skLogger, expiration*1000L,
                        refresh*1000L,
                        checkInterval*1000L);
        this.tm = tm;
        this.kvConfig = kvConfig;
        this.stats = stats;
        storeCache = new ConcurrentHashMap<String, KVStoreImpl>();
        notReachableStore = ConcurrentHashMap.newKeySet();
        connectLockMap = new ConcurrentHashMap<String, ReentrantLock>();
    }

    @Override
    protected TableEntry getTable(String namespace,
                                  String tableName,
                                  String nsname,
                                  LogContext lc) {
        /* this will *always* make an http request to the tm */
        GetStoreResponse gsr = tm.getStoreInfo(namespace,
                                               extractTableId(tableName),
                                               lc);
        if (gsr == null) {
            storeNotFound(namespace, tableName, null, lc);
        }
        /* if SC is down, throw a different error */
        if (gsr.getErrorCode() == SERVICE_UNAVAILABLE) {
            /* if SC is down, throw a different error */
            throw new RequestException(SERVICE_UNAVAILABLE,
                          gsr.getErrorString());
        } else if (gsr.getErrorCode() != NO_ERROR) {
            /* otherwise, throw normal "table not found" error */
            storeNotFound(namespace, tableName, gsr.getErrorString(), lc);
        }
        String storeName = gsr.getStoreName();
        String[] helperHosts = gsr.getHelperHosts();

        if (storeCache.get(storeName) == null) {
            /*
             * connect to store, cache result
             */
            connectToStore(storeName, helperHosts,
                           namespace, tableName, lc);
        }

        KVStoreImpl store = storeCache.get(storeName);
        notReachableStore.remove(storeName);
        TableAPIImpl tableApi = store.getTableAPIImpl();

        /*
         * set the table cache to 0, which is unlimited.
         * Idle tables are removed after 30s by default
         */
        ((TableAPIImpl) store.getTableAPI()).setCacheCapacity(0);

        Table table = tableApi.getTable(namespace, tableName,
                                        true /* bypassCache */);
        if (table == null) {
            tableNotFound(namespace, tableName, null, lc);
        }

        return new CloudServiceTableEntry(table, tableApi,
                                          storeName, store,
                                          gsr.getRequestLimits(),
                                          gsr.isMultiRegion(),
                                          gsr.isInitialized());
    }

    private String extractTableId(String kvTableName) {
        /*
         * This is based on the fact that the ocid of child table is last path
         * of the kv table name: <parent_ocid>.<child_ocid>.
         */
        String[] paths = kvTableName.split("\\.");
        return paths[paths.length - 1];
    }

    /**
     * Pod Health support - see if it can connect to all KVStores and
     * it can contact to all connected KVStores nodes, using kvstats to send a
     * no-op request.
     * If it failed to connect to any KVStore, the connectivity status = red.
     * If isActive() == false for all nodes, the connectivity status = red.
     * If isActive() == false for any node, the connectivity status = yellow.
     * If isActive() == true for all nodes, the connectivity status = green.
     */
    public HealthStatus checkStoreConnectivity(List<String> errors) {
        HealthStatus status = HealthStatus.GREEN;

        /*
         * Check if any KVStore is not reachable.
         */
        for (String storeName : notReachableStore) {
            status = HealthStatus.RED;
            final String errorMsg = "Proxy table cache health check: " +
                storeName + " is not reachable";
            logger.severe(errorMsg);
            errors.add(errorMsg);
        }

        /*
         * Check all connected KVStores nodes.
         */
        for (Map.Entry<String, KVStoreImpl> kvEntry : storeCache.entrySet()) {
            final String storeName = kvEntry.getKey();
            final KVStoreImpl kvHandle = kvEntry.getValue();
            final KVStats kvStats = kvHandle.getStats(false);
            final List<NodeMetrics> nodeStats = kvStats.getNodeMetrics();
            final int nodeCount = nodeStats.size();
            final List<String> inactiveNodes = new ArrayList<>();
            for (NodeMetrics node : nodeStats) {
                if (!node.isActive()) {
                    inactiveNodes.add(node.getNodeName());
                }
            }
            final int inactiveCount = inactiveNodes.size();
            if (nodeCount == 0 || inactiveCount == nodeCount) {
                status = HealthStatus.RED;
                final String errorMsg = "Proxy table cache health check: " +
                    storeName + " all store nodes are inactive";
                logger.severe(errorMsg);
                errors.add(errorMsg);
            } else if (inactiveCount > 0) {
                if (status == HealthStatus.GREEN) {
                    status = HealthStatus.YELLOW;
                }
                final String errorMsg = "Proxy table cache health check: " +
                    storeName + " has " + inactiveCount
                    + " nodes are inactive: " + inactiveNodes;
                logger.warning(errorMsg);
                errors.add(errorMsg);
            }
        }
        return status;
    }

    /**
     * Register the store with monitor stats if provided.
     */
    private void afterStoreOpen(String storeName, KVStoreImpl store) {
        if (stats != null) {
            stats.register(storeName, store);
        }
    }

    private KVStoreConfig getStoreConfig(String storeName,
                                         String[] helperHosts) {
        KVStoreConfig kvconfig = kvConfig != null ? kvConfig.clone() :
            new KVStoreConfig(storeName, helperHosts);
        kvconfig.setStoreName(storeName);
        kvconfig.setHelperHosts(helperHosts);

        return kvconfig;
    }

    private void storeNotFound(String namespace,
                               String tableName,
                               String msg,
                               LogContext lc) {

        tableNotFound(namespace, tableName,
                      ("Database not found for table: " +
                       tableName + ": " + msg), lc);
    }

    /**
     * Connect to store, caching the result. Failures to connect
     * are thrown as exceptions from KVStoreFactory.getStore().
     */
    private void connectToStore(String storeName,
                                String[] helperHosts,
                                String namespace,
                                String tableName,
                                LogContext lc) {

        if (storeCache.get(storeName) != null) {
            /* store connected by a different thread */
            return;
        }

        /*
         * Before trying to connect to the named store, try to acquire
         * a lock with a timeout for the store connection attempt.
         * See https://jira.oci.oraclecorp.com/browse/NOSQL-697 and
         * https://jira.oci.oraclecorp.com/browse/NOSQL-346 for details.
         */
        ReentrantLock connectLock = connectLockMap.get(storeName);
        if (connectLock == null) {
            connectLock = new ReentrantLock();
            ReentrantLock prevLock =
                connectLockMap.putIfAbsent(storeName, connectLock);
            if (prevLock != null) {
                /* another thread already created the lock object */
                connectLock = prevLock;
            }
        }

        boolean haveLock = false;
        try {
            haveLock = connectLock.tryLock(STORE_CONNECT_TIMEOUT_MILLIS,
                                           TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {}

        /* regardless of our lock state, check if the store is in the cache */
        if (storeCache.get(storeName) != null) {
            /* store connected by a different thread */
            if (haveLock) {
                connectLock.unlock();
            }
            return;
        }

        if (haveLock == false) {
            /* timed out and store is still not available */
            /*
             * Throw SERVER_ERROR to distinguish between this
             * and SC unavailable (which uses SERVICE_UNAVAILABLE).
             * This is a retryable error.
             */
            throw new RequestException(SERVER_ERROR,
                "Cannot reach service, please retry");
        }

        try {
            KVStoreConfig kvconfig = getStoreConfig(storeName, helperHosts);
            KVStoreImpl store = (KVStoreImpl) KVStoreFactory.getStore(kvconfig);
            TableAPIImpl tapi = (TableAPIImpl) store.getTableAPI();

            /* get notified if the metadata changes on this store */
            tapi.setTableMetadataCallback(new MdCallback(storeName));

            /* disable direct driver retries on MNFE */
            RequestDispatcher rd = store.getDispatcher();
            if (rd instanceof RequestDispatcherImpl) {
                ((RequestDispatcherImpl)rd).setRequestExceptionHandler(
                    new NoRetryHandler());
            }

            /* don't allow the TableMetadataHelper to use this cache
               at this time
                tapi.setCachedMetadataHelper(new MdHelper(tapi));
            */

            KVStoreImpl oldStore = storeCache.putIfAbsent(storeName,
                                                          store);

            /*
             * detect a race to connect. If there was a previous handle,
             * the put didn't happen, close the new handle.
             */
            if (oldStore != null) {
                store.close();
            } else {
                afterStoreOpen(storeName, store);
            }
        } catch (Exception e) {
            notReachableStore.add(storeName);
            logger.info("Unable to connect to KVStore " + storeName +
                        " for table " + tableName + ": " + e.getMessage());
            storeNotFound(namespace, tableName,
                          "Unable to connect to KVStore " + storeName +
                          " for table " + tableName + ": " + e.getMessage(),lc);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public KVStoreImpl getStoreByName(String storeName) {
        return storeCache.get(storeName);
    }

    /**
     * The object returned from the TableCache. This allows operations to
     * work on cached instances of stores and Table objects which are costly
     * to create.
     */
    private static class CloudServiceTableEntry extends TableEntry {

        private final TableAPIImpl tableApi;
        private final String storeName;
        private final KVStoreImpl kvstore;
        private final RequestLimits requestLimits;
        private final boolean isMultiRegion;
        private final boolean isInitialized;

        private CloudServiceTableEntry(Table table,
                                       TableAPIImpl tableApi,
                                       String storeName,
                                       KVStoreImpl kvstore,
                                       RequestLimits requestLimits,
                                       boolean isMultiRegion,
                                       boolean isInitialized) {
            super(table);
            this.tableApi = tableApi;
            this.storeName = storeName;
            this.kvstore = kvstore;
            this.requestLimits = requestLimits;
            this.isMultiRegion = isMultiRegion;
            this.isInitialized = isInitialized;
        }

        @Override
        public TableAPIImpl getTableAPI() {
            return tableApi;
        }

        @Override
        public KVStoreImpl getStore() {
            return kvstore;
        }

        @Override
        public void close() {
            if (kvstore != null) {
                kvstore.close();
            }
        }

        @Override
        public RequestLimits getRequestLimits() {
            return requestLimits;
        }

        @Override
        public String getStoreName() {
            return storeName;
        }

        @Override
        public boolean isMultiRegion() {
            return isMultiRegion;
        }

        @Override
        public boolean isInitialized() {
            return isInitialized;
        }
    }

    /**
     * This is a callback object that, when called, results in flushing
     * table information from the associated store. A change in sequence
     * number indicates *some* metadata change. There is no finer
     * granularity change information that allows a partial flush, which
     * means that the cache will be flushed more aggressively than needed
     * for correctness.
     */
    private class MdCallback implements TableAPIImpl.TableMetadataCallback {
        private final String storeName;
        private MdCallback(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void metadataChanged(int oldSeqNum, int newSeqNum) {
            flushTableMetadata(storeName);
        }
    }

    /**
     * Flushes the cached table instances for this store. At this time
     * there is no efficient mapping from store name to all tables cached,
     * so this operation will walk all tables cached.
     *
     * This operation is safe with respect to concurrent data operations,
     * but not safe to run in more than one thread as the same time, so it
     * is synchronized.
     */
    private synchronized void flushTableMetadata(String storeName) {
        for (Map.Entry<String, TableEntry> entry : entryCache.entrySet()) {
            if (storeName.equals(entry.getValue().getStoreName())) {
                entryCache.remove(entry.getKey());
            }
        }
    }
}
