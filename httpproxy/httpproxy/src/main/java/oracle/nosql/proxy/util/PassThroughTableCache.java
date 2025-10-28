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

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.table.Table;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.proxy.sc.LocalTenantManager;

/**
 * An instance of TableCache for on-prem and cloudsim use only.
 * Now that kv (as of 20.3) has a table cache this is a pass-through and
 * only serves to cache the fixed KVStore handle.
 */
public class PassThroughTableCache extends TableCache {
    private final KVStoreImpl store;
    private final RequestLimits requestLimits;
    private final boolean useCache;

    public PassThroughTableCache(LocalTenantManager ltm, SkLogger logger) {
        super(logger);
        this.store = ltm.getStore();
        this.requestLimits = ltm.getRequestLimits();
        /* allow tests to use the cache */
        this.useCache = Boolean.getBoolean("test.usetablecache");

        /* set a handler to not do retries inside direct driver */
        RequestDispatcher rd = this.store.getDispatcher();
        if (rd instanceof RequestDispatcherImpl) {
            ((RequestDispatcherImpl)rd).setRequestExceptionHandler(
                new NoRetryHandler());
        }
    }

    /**
     * Overriding this method takes over processing from TableCache so that
     * entries are not cached.
     */
    @Override
    public TableEntry getTableEntry(String namespace,
                                    String tableName,
                                    LogContext lc) {
        if (useCache) {
            /* allow tests to use the cache */
            return super.getTableEntry(namespace, tableName, lc);
        }
        String nsname = makeNamespaceName(namespace, tableName);
        return getTable(namespace, tableName, nsname, lc);
    }

    @Override
    protected TableEntry getTable(String namespace,
                                  String tableName,
                                  String nsname,
                                  LogContext lc) {
        TableAPIImpl tableApi = (TableAPIImpl) store.getTableAPI();
        Table table = tableApi.getTable(namespace, tableName);
        if (table == null) {
            tableNotFound(namespace, tableName, null, lc);
        }
        return new PassThroughTableEntry(table, tableApi, store);
    }

    @Override
    public KVStoreImpl getStoreByName(String storeName) {
        return this.store;
    }

    /**
     * The object returned from the TableCache. Unfortunately, because of the
     * TableCache contract, a new instance of this is created for each getTable
     * call. TBD: maybe refactor the proxy so that TableCache isn't in the path
     * unconditionally.
     */
    private class PassThroughTableEntry extends TableEntry {
        private final TableAPIImpl tableApi;
        private final KVStoreImpl kvstore;

        private PassThroughTableEntry(Table table,
                                      TableAPIImpl tableApi,
                                      KVStoreImpl kvstore) {
            super(table);
            this.tableApi = tableApi;
            this.kvstore = kvstore;
        }

        @Override
        public KVStoreImpl getStore() {
            return kvstore;
        }

        @Override
        public TableAPIImpl getTableAPI() {
            return tableApi;
        }

        @Override
        public String getStoreName() {
            if (kvstore != null) {
                return kvstore.getTopology().getKVStoreName();
            }
            return null;
        }

        @Override
        public RequestLimits getRequestLimits() {
            return requestLimits;
        }
    }
}
