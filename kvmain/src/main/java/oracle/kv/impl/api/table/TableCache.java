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
package oracle.kv.impl.api.table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.table.Table;

import oracle.nosql.common.cache.CacheBuilder.CacheConfig;
import oracle.nosql.common.cache.CacheEntry;
import oracle.nosql.common.cache.LruCache;

/**
 * LRU cache of table instances. Entries may be accessed by table name and ID.
 *
 * A cache element is a table hierarchy with the key being the upper case full
 * namespace name of the top level table. The full namespace name is the the
 * string returned by  Table.getFullNamespaceName() or
 * NameUtils.makeQualifiedName().
 *
 * If a child table is inserted into the cache, the table hierarchy containing
 * the child table is inserted with the key of the top-level table.
 *
 * If a child table name is provided to a get method, the top-level table name
 * is extracted from the child name and used to access the hierarchy. However
 * the child table instance is returned from the get.
 *
 * When a table is inserted, the table ID is indexed so that the table can
 * get accessed via its ID. IDs of all of the tables in the hierarchy are
 * indexed, so child tables can also be accessed via their ID.
 *
 * On insert, if the table exists in the cache, the two table's sequence
 * numbers are compared and the insert proceeds only if the inserted table's
 * sequence number is higher.
 *
 * Entries can be removed explicitly by table name or ID. They are also removed
 * if they have expired (lifetime &gt; 0).
 */
class TableCache extends LruCache<String, TableImpl> {

    /*
     * Map of all tables in the cache (top level and children) by ID.
     */
    private final Map<Long, CacheEntry<TableImpl>> idMap;

    private volatile boolean enabled = true;

    /*
     * Highest seq number seen on a validate. This is maintained so that
     * we can keep table entries in the cache when talking to an older
     * versioned server. See comment in put().
     */
    private final AtomicInteger highSeqNum = new AtomicInteger(0);

    /**
     * Constructs a table cache with the specified capacity and lifetime. Both
     * the capacity and lifetime can be changed after construction.
     */
    TableCache(int initialCapacity, long lifetimeMS) {
        /*
         * Don't start a cleanup thread for this cache instance
         */
        super(new CacheConfig().setCapacity(initialCapacity)
              .setLifetime(lifetimeMS).setCreateCleanupThread(false));
        idMap = new ConcurrentHashMap<>(getCapacity());
    }

    /**
     * Enables or disables the cache. If enable is false the cache is disabled
     * and the cache is cleared.
     *
     * @param enable true to enable the cache false to disable
     */
    void setEnabled(boolean enable) {
        enabled = enable;
        if (!enabled) {
            clear();
        }
    }

    /**
     * Returns the specified table or null if the table is not present in the
     * cache.
     *
     * @param fullNamespaceName full table name
     *
     * @return a table instance or null
     */
    @Override
    public TableImpl get(String fullNamespaceName) {
        if (!enabled) {
            return null;
        }
        return get(NameUtils.getNamespaceFromQualifiedName(fullNamespaceName),
                   TableImpl.parseFullName(
                     NameUtils.getFullNameFromQualifiedName(fullNamespaceName)));
    }

    /**
     * Returns the specified table or null if the table is not present in the
     * cache.
     *
     * @param namespace the table namespace or null
     * @param path table name path
     *
     * @return a table instance or null
     */
    TableImpl get(String namespace, String[] path) {
        if (!enabled || path == null || path.length == 0) {
            return null;
        }

        TableImpl table = super.get(getKey(namespace, path[0]));

        /* Get the child if needed */
        for (int i = 1; i < path.length && table != null; i++) {
            table = table.getChildTable(path[i]);
        }
        return table;
    }

    /* Gets a cache key for the specified table name */
    private String getKey(String namespace, String topTableName) {
        final String ns = NameUtils.switchToInternalUse(namespace);
        return NameUtils.makeQualifiedName(ns, topTableName).toLowerCase();
    }

    /* Get a cache key for the specified table */
    private String getKey(TableImpl table) {
        return table.getTopLevelTable().getFullNamespaceName().toLowerCase();
    }

    /**
     * Returns the table for the specified ID or null if the table is not
     * present in the cache.
     *
     * @param tableId a table ID
     *
     * @return a table instance or null
     */
    TableImpl get(long tableId) {
        if (!enabled) {
            return null;
        }
        final TableImpl table;
        lock();
        try {
            final TableImpl t = getById(tableId);
            if (t == null) {
                return null;
            }

            /*
             * Get to generate a cache access, moving the table up.
             */
            table = super.get(getKey(t));
        } finally {
            unlock();
        }

        /*
         * The get() above returned a top level table, the ID may be for a child.
         * Also, in theory there could have been a table with the same name that
         * was deleted, but not yet flushed from the cache. This call will
         * cover that very unlikley case
         */
        return (table == null) ? null : getTableForId(table, tableId);
    }

    /* Search the table hierarchy for a table with matching id */
    private TableImpl getTableForId(TableImpl table, long tableId) {
        if (table.getId() == tableId) {
            return table;
        }
        /* Recursively check the children as needed */
        for (Table child : table.getChildTables().values()) {
            final TableImpl t = getTableForId((TableImpl)child, tableId);
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    /**
     * Inserts the specified table to the cache. If the table is already
     * present it is overridden iif the specified table has a higher
     * sequence number. Otherwise the cache is unchanged. The overridden
     * table is returned.
     *
     * @param fullNamespaceName not used
     * @param table instance to insert
     *
     * @return a table instance or null
     */
    @Override
    public TableImpl put(String fullNamespaceName, TableImpl table) {
        /* Ignore the name because we actually insert the top level table */
        return put(table);
    }

    /**
     * Inserts the specified table to the cache. If the table is already
     * present it is overridden iif the table has a higher sequence number
     * than the existing table. Otherwise the cache is unchanged. The replaced
     * table is returned.
     *
     * @param table table to insert or null
     *
     * @return replaced table or null
     */
    TableImpl put(TableImpl table) {
        if (!enabled || table == null) {
            return null;
        }
        final TableImpl top = table.getTopLevelTable();
        final String key = getKey(top);
        lock();
        try {
            final TableImpl old = super.get(key);
            /* Check to make sure the table is newer */
            if ((old != null) &&
                (old.getSequenceNumber() >= top.getSequenceNumber())) {
                return null;
            }
            return super.put(key, top);
        } finally {
            unlock();
        }
    }

    @Override
    public void clear() {
        lock();
        try {
            super.clear();
            idMap.clear();
        } finally {
            unlock();
        }
    }

    /*
     * Overriden to update the ID map when an entry is added to the cache.
     */
    @Override
    protected void entryAdded(String key, CacheEntry<TableImpl> entry) {
        assert entry != null;
        /* getValue() returns the top-level table */
        addTableToIDMap(entry.getValue(), entry);
    }

    private void addTableToIDMap(TableImpl table, CacheEntry<TableImpl> entry) {
        idMap.put(table.getId(), entry);
        /* Recursively add all of the children to the ID map */
        for (Table child : table.getChildTables().values()) {
            addTableToIDMap((TableImpl)child, entry);
        }
    }

    /*
     * Overriden to update the ID map when an entry is removed from the cache.
     */
    @Override
    protected void entryRemoved(String key, CacheEntry<TableImpl> entry) {
        assert entry != null;
        /* getValue() returns the top-level table */
        removeTableFromIDMap(entry.getValue());
    }

     private void removeTableFromIDMap(TableImpl table) {
        idMap.remove(table.getId());
        /* Recursively remove all of the children from the ID map */
        for (Table child : table.getChildTables().values()) {
            removeTableFromIDMap((TableImpl)child);
        }
    }

    @Override
    public TableImpl remove(String fullNamespaceName) {
        if (!enabled) {
            return null;
        }
        return remove(NameUtils.getNamespaceFromQualifiedName(fullNamespaceName),
                      NameUtils.getFullNameFromQualifiedName(fullNamespaceName));
    }

    /**
     * Removes the specified table from the cache.
     */
    TableImpl remove(String namespace, String tableName) {
        if (!enabled) {
            return null;
        }
        if (tableName == null) {
            return null;
        }
        return remove(namespace, TableImpl.parseFullName(tableName));
    }

    /**
     * Removes the specified table from the cache.
     */
    TableImpl remove(String namespace, String[] path) {
        if (!enabled) {
            return null;
        }
        if (path == null || path.length == 0) {
            return null;
        }
        /*
         * Remove the table from the cache. Note that the entry (or
         * entries) in the idMap will removed via a callback.
         */
        return super.remove(getKey(namespace, path[0]));
    }

    /**
     * Removes the specified table from the cache.
     */
    TableImpl remove(long tableId) {
        assert tableId > 0;
        if (!enabled) {
            return null;
        }
        lock();
        try {
            final TableImpl table = getById(tableId);
            if (table == null) {
                return null;
            }
            /*
             * Remove the table from the cache. Note that the entry (or
             * entries) in the idMap will removed via a callback.
             */
            return super.remove(getKey(table));
        } finally {
            unlock();
        }
    }

    /**
     * Validates the cache entry for the specified table. If the table is
     * present in the cache, it's sequence number is checked against the
     * specified seqNum. If the seqNum is higher, the entry is removed from
     * the cache. If tableId is 0, no action is taken.
     */
    void validate(long tableId, int seqNum) {
        if (!enabled || (tableId == 0L)) {
            return;
        }
        /*
         * Keep track of the highest seq number seen. When communicating with
         * an older server, this will be the overall MD seq number and not
         * the table's.
         */
        highSeqNum.updateAndGet((x) -> (seqNum > x) ? seqNum : x);

        /*
         * Do an initial check without locking and only lock if the entry has
         * to be removed.
         */
        final CacheEntry<TableImpl> entry = idMap.get(tableId);
        if (entry == null) {
            return;
        }
        TableImpl table = entry.getValue();
        if (table == null) {
            return;
        }
        /*
         * If the table is still valid (is at or above the passed in seqNum)
         * reset the entry's create time to keep it in longer.
         */
        if (table.getSequenceNumber() >= seqNum) {
            entry.resetCreateTime();
            return;
        }

        /* The cache entry needs to be removed */
        lock();
        try {
            /* Reaquire the table while locked */
            table = getById(tableId);
            if (table == null) {
                return;
            }
            if (seqNum > table.getSequenceNumber()) {
                /*
                 * Remove the table from the cache. Note that the entry (or
                 * entries) in the idMap will be removed.
                 */
                super.remove(getKey(table));
            }
        } finally {
            unlock();
        }
    }

    /**
     * Gets a table from the id map if present.
     */
    private TableImpl getById(long tableId) {
        final CacheEntry<TableImpl> entry = idMap.get(tableId);
        return (entry == null) ? null : entry.getValue();
    }

    /**
     * Returns the size of the ID map. This method is not synchronized intended
     * for testing.
     */
    int getIdMapSize() {
        return idMap.size();
    }
}
