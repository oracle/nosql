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

package oracle.kv.impl.pubsub;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.pubsub.SubscriptionFailureException;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * Object represents the table metadata manager used by NoSQLPublisher. Each
 * publisher should create one table metadata manager which will shared by
 * all subscriptions. The manager is responsible for managing all subscribed
 * tables and refresh them from store when necessary.
 */
public class PublisherTableMDManager {

    /* sleep time in ms before retry */
    private final static int TABLEMD_REFRESH_SLEEP_MS = 1000;
    /* number of times to retry refreshing the table md from store */
    private final static int NUM_RETRY_REFRESH_TABLEMD = 3;

    /* private logger */
    private final Logger logger;
    /* store name */
    private final String storeName;
    /* handle to store from publisher */
    private final KVStore kvs;

    /* a thread safe map to store tables by name */
    private final ConcurrentHashMap<String, TableImpl> tablesByName;
    /* a thread safe map to store tables by table id str */
    private final ConcurrentHashMap<String, TableImpl> tablesByIdStr;

    public PublisherTableMDManager(String storeName, KVStore kvs,
                                   Logger logger) {
        this.storeName = storeName;
        this.kvs = kvs;
        this.logger = logger;

        tablesByName = new ConcurrentHashMap<>();
        tablesByIdStr = new ConcurrentHashMap<>();

        // TODO - investigate using the cache within the table API
        /* Publisher manages its own cache */
        ((TableAPIImpl) kvs.getTableAPI()).setCacheEnabled(false);
    }

    /**
     * Gets table md required by a subscriber.
     *
     * @param sid       subscriber that requests the table md
     * @param tableName name of table
     * @return table with given table id, null if table does not exist
     * @throws SubscriptionFailureException if unable get the required table
     *                                      md from store
     */
    public TableImpl getTable(NoSQLSubscriberId sid, String tableName)
        throws SubscriptionFailureException {

        final TableImpl ret = tablesByName.get(tableName);
        if (ret == null) {
            return refreshTable(sid, tableName, 1/* refresh to any version */,
                                null /* whatever table id*/);
        }

        return ret;
    }

    /**
     * Removes a table from table md manager
     *
     * @param table table to remove
     */
    public void removeTable(TableImpl table) {
        final String name = table.getFullNamespaceName();
        tablesByName.remove(name);
        final Iterator<TableImpl> iter = tablesByIdStr.values().iterator();
        while (iter.hasNext()) {
            final TableImpl tb = iter.next();
            if (tb.getFullNamespaceName().equals(name)) {
                iter.remove();
                logger.fine(() -> lm("Removed table=" + name +
                                     "(id=" + table.getId() + ")"));
                break;
            }
        }
    }

    /**
     * Gets the table for a given table id string
     *
     * @param tableIdStr table id string
     * @return the table with the table id, null if it does not exist
     */
    TableImpl getTable(String tableIdStr, boolean useTableNameAsId) {

        if (useTableNameAsId) {
            /* in some unit test only, using table name as id  */
            final Table t = kvs.getTableAPI().getTable(tableIdStr);
            return (TableImpl) t;
        }

        /* check local cache, maybe other subscription already cached it */
        TableImpl tableImpl = tablesByIdStr.get(tableIdStr);
        if (tableImpl != null) {
            return tableImpl;
        }

        /*
         * Table is new to the whole publisher
         */
        /* revert to numeric table id to speed up lookup */
        final long tid = TableImpl.createIdFromIdStr(tableIdStr);

        Table t = kvs.getTableAPI().getTableById(tid);

        if (t != null) {

            tableImpl = (TableImpl) t;

            /* skip all sys tables */
            if (tableImpl.isSystemTable()) {
                return null;
            }

            if (tid == tableImpl.getId()) {
                /* cached it */
                tablesByIdStr.put(tableIdStr, tableImpl);
                tablesByName.put(tableImpl.getFullNamespaceName(), tableImpl);
                return tableImpl;
            }
        }

        return null;
    }

    /**
     * Refreshes table in md up to the required version, retry if necessary.
     *
     * @param id         subscriber that requests refreshing table md
     * @param tableName  name of table to refresh
     * @param reqVer     required table version
     * @param tableIdStr table id str, null if it does not matter, refresh
     *                   table by name and skip check mismatch table id
     * @return a new table with required version, null if the table does not
     * exist in store, or cannot find the table with required version
     * @throws SubscriptionFailureException raised when fail to refresh the
     *                                      table metadata to the required version
     */
    synchronized TableImpl refreshTable(NoSQLSubscriberId id,
                                        String tableName,
                                        int reqVer,
                                        String tableIdStr)
        throws SubscriptionFailureException {

        /* first check if we have required version of table */
        final TableImpl currTable = tablesByName.get(tableName);
        if (currTable != null && currTable.getTableVersion() >= reqVer) {
            /* good somebody already refreshed it */
            return currTable;
        }

        /*
         * go to a server to retrieve and refresh their view of the metadata,
         * at the cost of an RPC
         */
        final int retry = NUM_RETRY_REFRESH_TABLEMD;
        int count = retry;
        TableAPI tableAPI;
        TableImpl refresh;
        do {
            tableAPI = kvs.getTableAPI();
            refresh = (TableImpl) tableAPI.getTable(tableName);

            if (refresh == null) {
                /* table is dropped */
                tablesByName.remove(tableName);
                if (tableIdStr != null) {
                    tablesByIdStr.remove(tableIdStr);
                }
                final String err = "Subscriber=" + id +
                                   "cannot refresh the table because unable " +
                                   "to find table=" + tableName + " in store=" +
                                   storeName + ", table could be dropped.";
                logger.warning(lm(err));
                return null;
            } else if (tableIdStr != null &&
                       TableImpl.createIdFromIdStr(tableIdStr) !=
                       refresh.getId()) {
                /* table id mismatch */
                final String err = "Subscriber=" + id +
                                   "cannot refresh the table in store=" +
                                   storeName + ", the table with id=" +
                                   TableImpl.createIdFromIdStr(tableIdStr) +
                                   " is already dropped. In store, there is a" +
                                   " table with same name but a different " +
                                   "table id=" + refresh.getId();
                logger.warning(lm(err));
                return null;

            } else if (refresh.getTableVersion() >= reqVer) {

                /* refresh successfully */
                logger.info(lm("Subscriber=" + id +
                               " refreshed table=" + tableName +
                               "(id=" + refresh.getId() + ")" +
                               " to version=" + refresh.getTableVersion() +
                               ", numVersions=" + refresh.numTableVersions() +
                               ", required version=" + reqVer));
                tablesByName.put(tableName, refresh);
                tablesByIdStr.put(TableImpl.createIdString(refresh.getId()),
                                  refresh);
                return refresh;

            } else {
                /* cannot get required version of table, retry */
                count--;
                final int sleepMs = TABLEMD_REFRESH_SLEEP_MS;
                final String err = "Subscriber=" + id +
                                   "cannot refresh the table=" + tableName +
                                   " to version=" + reqVer + " while max " +
                                   "version=" + refresh.getTableVersion()
                                   + ", will retry after time(ms)=" + sleepMs;
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ite) {
                    throw new SubscriptionFailureException(id, err);
                }
            }
        } while (count >= 0);


        final String err = "Subscriber=" + id +
                           "cannot refresh table=" + tableName +
                           " from store=" + storeName +
                           " after #retries=" + retry;
        /*
         * Due to table version mismatch, subscriber is unable to
         * build stream operation, to avoid data loss for
         * subscriber we take a pessimistic action by throwing
         * SFE, and any one who capture the SFE should shut down
         * subscription and signal the exception to subscriber.
         */
        throw new SubscriptionFailureException(id, err);
    }

    private String lm(String msg) {
        return "[PUB-tblMDMan-" + storeName + "] " + msg;
    }

    /**
     * Unit test only
     */
    public Table getTableByName(String name) {
        return tablesByName.get(name);
    }

    /**
     * Unit test only
     */
    public Table getTableByIdStr(String idStr) {
        return tablesByIdStr.get(idStr);
    }
}
