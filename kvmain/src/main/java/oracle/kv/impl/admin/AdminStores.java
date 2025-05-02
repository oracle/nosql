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

package oracle.kv.impl.admin;

import java.io.Closeable;
import java.util.EnumMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.Admin.Memo;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Container for the various persistent stores in the Admin. Some stores are
 * hidden in that this class exports get and put methods for several of the
 * persistent data types. Some stores can be accessed directly if they provide
 * more complex operations.
 */
class AdminStores {

    /* Store types */
    static enum STORE_TYPE {
        EVENT,      /* Critical events */
        GENERAL,    /* Parameters and Memos */
        PLAN,       /* Plans */
        SECURITY,   /* Security metadata */
        TABLE,      /* Table metadata */
        TOPOLOGY;   /* Topology history and canidates */
    }

    protected final Admin admin;

    /* Map of stores. Empty if the stores are not initialized */
    private final Map<STORE_TYPE, AdminStore> storeMap;

    /*
     * Set to false when the store schema upgrade requires all Admins to be
     * upgraded first. In that case the store will be in read-only mode until
     * the upgrade is complete.
     *
     * This feature is currently unused, but remains in for future use.
     */
    private final boolean readOnly = false;

    AdminStores(Admin admin) {
        this.admin = admin;
        storeMap = new EnumMap<>(STORE_TYPE.class);
    }

    /**
     * Returns true if the store is in read-only mode.
     * @return true if the store is in read-only mode
     */
    boolean isReadOnly() {
        return readOnly;
    }

    /**
     * (Re-)initializes the Admin stores using the specified schema version.
     * All Admin stores are opened upon return. If force is true and stores
     * are currently open, they are closed and re-opened.
     *
     * @param schemaVersion the schema version to use when opening the stores
     * @param force re-open all stores if open
     */
    synchronized void init(int schemaVersion, boolean force) {
        if (schemaVersion < AdminSchemaVersion.SCHEMA_VERSION_5) {
            throw new IllegalStateException("Admin schema version " +
                                            schemaVersion +
                                            " is not supported, must be at" +
                                            " least " +
                                           AdminSchemaVersion.SCHEMA_VERSION_5);
        }

        /*
         * Init if force is true or the store isn't open (map is empty).
         */
        if (!force && !storeMap.isEmpty()) {
            return;
        }
        close();

        final Logger logger = admin.getLogger();
        final Environment env = admin.getEnv();

        logger.log(Level.INFO,
                   "Initialize stores at schema version {0}, readOnly={1}",
                   new Object[]{schemaVersion, readOnly});
        storeMap.put(STORE_TYPE.EVENT, new EventStore(logger, env,
                     readOnly, admin.getMaxEvents()));
        storeMap.put(STORE_TYPE.GENERAL,
                     new GeneralStore(logger, env, readOnly));
        storeMap.put(STORE_TYPE.PLAN, new PlanStore(logger, env, readOnly));
        storeMap.put(STORE_TYPE.SECURITY,
                     new SecurityStore(logger, env, readOnly));
        storeMap.put(STORE_TYPE.TABLE, new TableStore(logger, env, readOnly));
        storeMap.put(
            STORE_TYPE.TOPOLOGY,
            new TopologyStore(
                logger, env,
                admin.getParams().getAdminParams().getMaxTopoChanges(),
                readOnly));
    }

    /* Store accessors */

    EventStore getEventStore() {
        return (EventStore)getStore(STORE_TYPE.EVENT);
    }

    private GeneralStore getGeneralStore() {
        return (GeneralStore)getStore(STORE_TYPE.GENERAL);
    }

    PlanStore getPlanStore() {
        return (PlanStore)getStore(STORE_TYPE.PLAN);
    }

    private SecurityStore getSecurityMDStore() {
        return (SecurityStore)getStore(STORE_TYPE.SECURITY);
    }

    private TableStore getTableMDStore() {
        return (TableStore)getStore(STORE_TYPE.TABLE);
    }

    TopologyStore getTopologyStore() {
        return (TopologyStore)getStore(STORE_TYPE.TOPOLOGY);

    }

    private synchronized AdminStore getStore(STORE_TYPE type) {
        final AdminStore store = storeMap.get(type);
        if (store == null) {
            throw new AdminNotReadyException("Admin stores have not been " +
                                             "initialized.");
        }
        return store;
    }

    /* Convenience store methods */

    /**
     * Gets the Admin parameters from the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @return the Admin parameters
     */
    Parameters getParameters(Transaction txn) {
        return getGeneralStore().getParameters(txn);
    }

    /**
     * Puts the specified parameters into the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @param p parameters to store
     */
    void putParameters(Transaction txn, Parameters p) {
        getGeneralStore().putParameters(txn, p);
    }

    /**
     * Gets the memo from the store using the specified transaction.
     *
     * @param txn a transaction
     * @return the memo
     */
    Memo getMemo(Transaction txn) {
        return getGeneralStore().getMemo(txn);
    }

    /**
     * Puts the specified memo into the store using the specified transaction.
     *
     * @param txn a transaction
     * @param memo the memo to store
     */
    void putMemo(Transaction txn, Memo memo) {
        getGeneralStore().putMemo(txn, memo);
    }

    /**
     * Gets the current realized topology from the historical store using the
     * specified transaction and return a copy as a new topology instance.
     *
     * @param txn a transaction
     * @return the current realized topology
     */
    Topology getTopology(Transaction txn) {
        return getTopologyStore().getTopology(txn);
    }

    /**
     * Puts the specified realized topology into the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @param rt the realized topology to store
     */
    void putTopology(Transaction txn, RealizedTopology rt) {
        getTopologyStore().putTopology(txn, rt);
    }

    /**
     * Puts the specified plan into the store using the specified transaction.
     *
     * @param txn a transaction
     * @param plan the plan to store
     */
    void putPlan(Transaction txn, Plan plan) {
        getPlanStore().put(txn, plan);
    }

    /**
     * Gets the critical event for the specified ID from the store using the
     * specified transaction.
     *
     * @param txn a transaction
     * @param eventId an event ID
     * @return the critical event or null
     */
    CriticalEvent getEvent(Transaction txn, String eventId) {
        return getEventStore().getEvent(txn, eventId);
    }

    /**
     * Puts the specified critical event into the store using the specified
     * transaction.
     *
     * @param txn a transaction
     * @param event the critical event to store
     */
    void putEvent(Transaction txn, CriticalEvent event) {
        getEventStore().putEvent(txn, event);
    }

    /**
     * Gets the metadata of the specified type using the specified transaction.
     * The method does not support getting the topology. If the type is topology
     * a IllegalStateException is thrown.
     *
     * @param <T> the type of return class
     * @param returnType class of the return metadata type
     * @param type the metadata type
     * @param txn a transaction
     * @return the metadata object
     */
    <T extends Metadata<? extends MetadataInfo>> T
                                            getMetadata(Class<T> returnType,
                                                        MetadataType type,
                                                        Transaction txn) {
        switch (type) {
        case TABLE:
            return returnType.cast(getTableMDStore().getTableMetadata(txn));
        case SECURITY:
            return returnType.cast(getSecurityMDStore().
                                                    getSecurityMetadata(txn));
        case TOPOLOGY:
            break;
        }
        throw new IllegalStateException("Invalid metadata type: " + type);
    }

    /**
     * Puts the specified metadata into the store using the specified
     * transaction. The method does not support getting the topology. If the
     * type is topology a IllegalStateException is thrown.
     *
     * @param md the metadata to store
     * @param txn a transaction
     */
    boolean putMetadata(Metadata<? extends MetadataInfo> md,
                        Transaction txn,
                        boolean noOverwrite) {
        switch (md.getType()) {
        case TABLE:
            return getTableMDStore().putTableMetadata(txn,
                                                      (TableMetadata)md,
                                                      noOverwrite);
        case SECURITY:
            return getSecurityMDStore().putSecurityMetadata(txn,
                                                           (SecurityMetadata)md,
                                                            noOverwrite);
        case TOPOLOGY:
            break;
        }
        throw new IllegalStateException("Invalid metadata type: " +
                                        md.getType());
    }

    /**
     * Closes the stores.
     */
    synchronized void close() {
        for (AdminStore store : storeMap.values()) {
            if (store != null) {
                store.close();
            }
        }
        storeMap.clear();
    }

    /**
     * Base class for a store.
     */
    abstract static class AdminStore implements Closeable {
        protected final Logger logger;

        protected AdminStore(Logger logger) {
            this.logger = logger;
        }

        /* -- From Closeable -- */

        /**
         * Closes the store. Overridden here to remove IOException;
         */
        @Override
        public abstract void close();
    }

    /**
     * Base class for creating a store cursor.
     *
     * @param <K> key type
     * @param <T> object type
     */
    abstract static class AdminStoreCursor<K, T> implements Closeable {
        private final Cursor cursor;
        private final K startKey;
        private final DatabaseEntry keyEntry = new DatabaseEntry();
        private final DatabaseEntry data = new DatabaseEntry();

        protected AdminStoreCursor(Cursor cursor) {
            this(cursor, null);
        }

        protected AdminStoreCursor(Cursor cursor, K startKey) {
            this.cursor = cursor;
            this.startKey = startKey;
        }

        /**
         * Moves the cursor to the first key/value pair of the database, and
         * returns that value. Null will be returned if no data exists.
         * <p>
         * If the {@code startKey} is specified in constructor, this method
         * will return the object with the {@code startKey} if found, otherwise
         * the object with smallest key greater than or equal to {@code
         * startKey} will be returned. If such key is not found, null will be
         * returned.
         */
        public T first() {
            /*
             * Try to move cursor to the start key if the specified.
             */
            final OperationStatus status;
            if (startKey != null) {
                keyToEntry(startKey, keyEntry);
                status =
                    cursor.getSearchKeyRange(keyEntry, data, LockMode.DEFAULT);
            } else {
                status = cursor.getFirst(keyEntry, data, LockMode.DEFAULT);
            }
            if (status == OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Moves the cursor to the previous key/value and returns that value.
         * Null will be return if the first key/value pair is reached.
         */
        T prev() {
            final OperationStatus status =
                cursor.getPrev(keyEntry, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Moves the cursor to the next key/value pair and returns that value.
         * Null will be return if the last key/value pair is reached.
         */
        public T next() {
            final OperationStatus status =
                cursor.getNext(keyEntry, data, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Moves the cursor to the last key/value pair of the database, and
         * returns that value. Null will be returned is no data exists.
         */
        T last() {
            if (cursor.getLast(keyEntry, data, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS) {
                return entryToObject(keyEntry, data);
            }
            return null;
        }

        /**
         * Deletes the record at the cursor. Returns true if the operation
         * was successful.
         *
         * @return true if the delete was successful
         */
        boolean delete() {
            return cursor.delete() == OperationStatus.SUCCESS;
        }

        /**
         * Converts the key/value to an object.
         */
        protected abstract T entryToObject(DatabaseEntry key,
                                           DatabaseEntry value);

        /**
         * Converts a key to an entry.
         */
        @SuppressWarnings("unused")
        protected void keyToEntry(K key, DatabaseEntry entry) {
            throw new AssertionError("keyToEntry not defined");
        }

        @Override
        public void close() {
            cursor.close();
        }
    }
}
