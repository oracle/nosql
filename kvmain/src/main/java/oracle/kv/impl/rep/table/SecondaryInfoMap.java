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

package oracle.kv.impl.rep.table;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.api.table.FieldComparator;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.table.PrimaryKey;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Persistent class containing information on populating secondary databases
 * as well as information on tables that have been deleted and need
 * cleaning. This information is used by MaintenanceThread instances to perform
 * cleaning tasks.
 *
 * A single instance of this object is kept in a replicated database for use
 * by the master of the shard.
 *
 * There is a single record, keyed by the (String) constant:
 *  SECONDARY_INFO_KEY
 *
 * The record data is serialized byte array of the class instance. The instance
 * contains 2 major components:
 * 1. Map of String to SecondaryInfo, used to track population of new indexes.
 *   key: the database name of the secondary (index) database
 *   value: serialized SecondaryInfo
 * 2. Map of String to DeletedTableInfo to track background removal of records
 * from deleted tables.
 *   key: the full table name. If namespaces are used, namespace:table_name
 *   value: serialized DeletedTableInfo
 *
 * Because this class is stored as a singleton it's important to be sure that
 * updates are single-threaded or otherwise synchronized. At this time access
 * is single-threaded because there is only a single thread accessing the
 * data at one time.
 *
 * Note that earlier code used HashMap for secondayMap and deletedTableMap.
 * This caused issues since table and index names are  case-insensitive. The
 * maps were changed to TreeMap with a case-insensitive comparator in 4.4. The
 * version was not bumped to avoid shard upgrade issues. The comparator class,
 * FieldComparator was introduced at the same time this class so there are
 * no issues deserializing a the new instance by old code.
 */
class SecondaryInfoMap implements Serializable  {
    private static final long serialVersionUID = 1L;

    private static final String SECONDARY_INFO_KEY = "SecondaryInfoMap";

    private static final DatabaseEntry secondaryInfoKey = new DatabaseEntry();

    static {
        StringBinding.stringToEntry(SECONDARY_INFO_KEY, secondaryInfoKey);
    }

    /*
     * Transaction configuration for r/w secondary info.
     *
     * <p>Read-committed is used to avoid holding read locks unnecessarily
     * during a populateSecondaries scan. For other operations (all updates)
     * this has no impact since LockMode.RMW is always used.</p>
     */
    static final TransactionConfig SECONDARY_INFO_CONFIG =
        new TransactionConfig().
            setReadCommitted(true).
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY).
            setDurability(
                   new Durability(Durability.SyncPolicy.SYNC,
                                  Durability.SyncPolicy.SYNC,
                                  Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

    /*
     * Transaction configuration for secondary cleaner operations. Cleaner
     * operations can have looser durability to improve performance since any
     * lost deletes would be retried when the cleaner was restarted during
     * recovery.
     */
    static final TransactionConfig CLEANER_CONFIG =
        new TransactionConfig().
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY).
            setDurability(
                   new Durability(Durability.SyncPolicy.NO_SYNC,
                                  Durability.SyncPolicy.NO_SYNC,
                                  Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

    /*
     * Transaction config to return what is locally at the replica, without
     * waiting for the master to make the state consistent wrt the master.
     * Also, the transaction will not wait for commit acks from the replicas.
     */
    private static final TransactionConfig NO_WAIT_CONFIG =
        new TransactionConfig().
            setDurability(new Durability(SyncPolicy.NO_SYNC,
                                         SyncPolicy.NO_SYNC,
                                         ReplicaAckPolicy.NONE)).
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

    /* Current schema version. See note below about changing the version. */
    private static final int CURRENT_SCHEMA_VERSION = 1;

    /*
     * Maps secondary DB name -> SecondaryInfo. Used for index population
     * and index cleaning on partition migration.
     */
    private Map<String, SecondaryInfo> secondaryMap =
            new TreeMap<>(FieldComparator.instance);

    /*
     * Maps table name -> deleted table info. This map contains entries for
     * the tables which have been deleted and their data removed.
     * The String name is a namespace-qualified table name:
     *  [namespace:]fullTableName
     */
    private Map<String, DeletedTableInfo> deletedTableMap =
            new TreeMap<>(FieldComparator.instance);

    /*
     * The table metadata seq. number that the data is this object is based on.
     */
    private int metadataSequenceNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    /*
     * Version of this object. In order to change the schema version, all nodes
     * in the shard must first be upgraded to software that supports the new
     * schema.
     */
    private final int version;

    private SecondaryInfoMap() {
        version = CURRENT_SCHEMA_VERSION;
    }

    /**
     * Gets the last seen metadata sequence number.
     *
     * @return the last seen metadata sequence number
     */
    int getMetadataSequenceNum() {
        return metadataSequenceNum;
    }

    /**
     * Sets the last seen metadata sequence number.
     *
     * @param seqNum
     */
    void setMetadataSequenceNum(int seqNum) {
        assert seqNum >= metadataSequenceNum;
        metadataSequenceNum = seqNum;
    }

    /**
     * Adds an info record for the specified secondary database if it isn't
     * already there.
     *
     * @param dbName the secondary database name
     * @param db the secondary info database
     * @param txn a transaction
     * @param logger
     */
    static void add(String dbName, Database db,
                    Transaction txn, Logger logger) {
        final SecondaryInfoMap infoMap = fetch(db, txn, LockMode.RMW);

        SecondaryInfo info = infoMap.secondaryMap.get(dbName);
        if (info == null) {
            info = new SecondaryInfo();
            infoMap.secondaryMap.put(dbName, info);

            logger.log(Level.FINE, "Adding {0} for {1}, map size= {2}",
                       new Object[]{info, dbName,
                                    infoMap.secondaryMap.size()});
            infoMap.persist(db, txn);
        }
    }

    /**
     * Updates the secondary info map. Checks to see if an index has been
     * removed and record deleted tables if needed. If an index has been
     * removed the secondary DB for that index is removed. This method is
     * called from the table maintenance thread.
     */
    static void update(TableMetadata tableMetadata,
                       Map<String, IndexImpl> indexes,
                       Set<TableImpl> deletedTables,
                       MaintenanceThread maintenanceThread,
                       Database infoDb,
                       ReplicatedEnvironment repEnv,
                       Logger logger) {
        assert infoDb != null;

        Transaction txn = null;
        try {
            boolean modified = false;

            txn = repEnv.beginTransaction(null, SECONDARY_INFO_CONFIG);

            final SecondaryInfoMap infoMap = fetch(infoDb, txn, LockMode.RMW);
            final Iterator<Entry<String, SecondaryInfo>> itr =
                infoMap.secondaryMap.entrySet().iterator();

            /* Remove secondary info for indexes that have been dropped */
            while (itr.hasNext()) {
                if (maintenanceThread.isStopped()) {
                    return;
                }
                final Entry<String, SecondaryInfo> entry = itr.next();
                final String dbName = entry.getKey();
                if (!indexes.containsKey(dbName)) {
                    /*
                     * The DB must be closed before removing it. If the close
                     * fails skip it, to be retried again.
                     */
                    if (!maintenanceThread.closeSecondary(dbName)) {
                        logger.log(Level.INFO,
                                   "Skiping removing secondary database {0}",
                                   dbName);
                        continue;
                    }
                    logger.log(Level.INFO, "Removing secondary database {0}",
                               dbName);
                    try {
                        assert TestHookExecute.
                                doHookIfSet(TableManager.BEFORE_REMOVE_HOOK, 1);
                        logger.log(Level.FINE,
                                   "Secondary database {0} is not " +
                                   "defined in table metadata " +
                                   "seq# {1} and is being removed.",
                                   new Object[]{dbName,
                                                tableMetadata.
                                                          getSequenceNumber()});
                        /*
                         * Mark the secondary removed, then attempt to remove
                         * the DB. If the remove fails, marking it will keep
                         * it from being used until the DB is sucessfully
                         * removed during some future check.
                         */
                        entry.getValue().setRemoved();
                        modified = true;

                        /* Actually remove the DB */
                        repEnv.removeDatabase(txn, dbName);

                        /* Remove the associated SecondaryInfo instance */
                        itr.remove();
                    } catch (DatabaseNotFoundException ignore) {
                        /* Already gone */
                        itr.remove();
                    } catch (RuntimeException re) {
                        /*
                         * Log the exception and go on, leaving the db info in
                         * the map for next time.
                         */
                        logger.log(Level.INFO,
                                   "Exception removing {0}: {1}, operation " +
                                   "will be retried",
                                   new Object[]{dbName, re.getMessage()});
                    } finally {
                        assert TestHookExecute.
                                 doHookIfSet(TableManager.AFTER_REMOVE_HOOK, 1);
                    }
                }
            }

            final Iterator<Entry<String, DeletedTableInfo>> itr2 =
                infoMap.deletedTableMap.entrySet().iterator();
            /*
             * Remove deleted table info for tables which have been removed
             * from the metadata.
             */
            while (itr2.hasNext()) {
                if (maintenanceThread.isStopped()) {
                    return;
                }
                final Entry<String, DeletedTableInfo> entry = itr2.next();
                final String name = entry.getKey();
                final DeletedTableInfo info = entry.getValue();
                final String namespace =
                    NameUtils.getNamespaceFromQualifiedName(name);
                final String tableName =
                    NameUtils.getFullNameFromQualifiedName(name);
                final TableImpl table =
                    tableMetadata.getTable(namespace, tableName);

                /*
                 * If the table is gone, clean out the info. Note that a new
                 * table with the same name may show up before the info for
                 * the old table is removed. In that case the new table will
                 * have a different ID.
                 */
                if ((table == null) ||
                    (table.getId() != info.getTargetTableId())) {
                    assert info.isDone() : name + " : " + info.toString();
                    itr2.remove();
                    modified = true;
                } else {
                    /* Still in the MD, make sure it was marked for delete */
                    if (!table.getStatus().isDeleting()) {
                        /*
                         * We think the table is being deleted, yet the MD
                         * says the table is valid (not being deleted). So
                         * something very bad happened.
                         *
                         * TODO - Work needs to be done to determine causes
                         * and remedies.
                         */
                        logger.log(Level.SEVERE,
                                   "Table metadata {0} includes table {1} but" +
                                   " node thinks the table is deleted",
                                 new Object[]{tableMetadata.getSequenceNumber(),
                                              table});
                    }
                }
            }

            /* Add entries for tables marked for delete */
            for (TableImpl table : deletedTables) {
                if (maintenanceThread.isStopped()) {
                    return;
                }
                final String tableName = table.getFullNamespaceName();
                final DeletedTableInfo info =
                        infoMap.getDeletedTableInfo(tableName);
                if (info == null) {
                    infoMap.deletedTableMap.put(tableName,
                                                new DeletedTableInfo(table));
                    modified = true;
                }
            }

            if (modified) {
                try {
                    infoMap.persist(infoDb, txn);
                    txn.commit();
                    txn = null;
                } catch (RuntimeException re) {
                    DatabaseUtils.handleException(re, logger,
                                                  "populate info map");
                }
            }
        } finally {
            TxnUtil.abort(txn);
        }
    }

    static void markForSecondaryCleaning(Database db, Logger logger) {
        Transaction txn = null;
        try {
            txn = db.getEnvironment().beginTransaction(null,
                                                       SECONDARY_INFO_CONFIG);

            final SecondaryInfoMap infoMap = fetch(db, txn, LockMode.RMW);

            if (infoMap.secondaryMap.isEmpty()) {
                return;
            }

            logger.log(Level.FINE, "Marking {0} for cleaning",
                       infoMap.secondaryMap.size());
            for (SecondaryInfo info : infoMap.secondaryMap.values()) {
                info.markForCleaning();
            }
            try {
                infoMap.persist(db, txn);
                txn.commit();
                txn = null;
            } catch (RuntimeException re) { // TODO - combine trys
                DatabaseUtils.handleException(re, logger,
                                              "populate info map");
            }
        } finally {
            if (txn != null) {
                txn.abort();
            }
        }
    }

    /**
     * Fetches the SecondaryInfoMap object from the db for
     * read-only use. If the db is empty an empty SecondaryInfoMap is returned.
     *
     * @param db the secondary info map db
     * @return a SecondaryInfoMap object
     */
    static SecondaryInfoMap fetch(Database db) {

        final Transaction txn = db.getEnvironment().
                                  beginTransaction(null, NO_WAIT_CONFIG);
        try {
            return fetch(db, txn, LockMode.DEFAULT);
        } finally {

            /* We are just reading. */
            if (txn.isValid()) {
                txn.commit();
            } else {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Fetches the SecondaryInfoMap object from the db. If the db is empty an
     * empty SecondaryInfoMap is returned.
     *
     * @param db the secondary info map db
     * @param txn the transaction to use for the get
     * @param lockMode for the get
     * @return a SecondaryInfoMap object
     */
    static SecondaryInfoMap fetch(Database db,
                                  Transaction txn,
                                  LockMode lockMode) {
        if (txn == null) {
            throw new IllegalStateException("transaction can not be null");
        }

        final DatabaseEntry value = new DatabaseEntry();

        db.get(txn, secondaryInfoKey, value, lockMode);

        final SecondaryInfoMap infoMap =
            SerializationUtil.getObject(value.getData(),
                                        SecondaryInfoMap.class);

        /* If none, return an empty object */
        return (infoMap == null) ? new SecondaryInfoMap() : infoMap;
    }

    /**
     * Persists this object to the db using the specified transaction.
     *
     * @param db the secondary info map db
     * @param txn transaction to use for the write
     */
    void persist(Database db, Transaction txn) {
        db.put(txn, secondaryInfoKey,
               new DatabaseEntry(SerializationUtil.getBytes(this)));
    }

    private void readObject(ObjectInputStream ois)
            throws IOException, ClassNotFoundException {

            ois.defaultReadObject();

        if (version > CURRENT_SCHEMA_VERSION) {
            throw new IllegalStateException
                ("The secondary info map is at " +
                 KVVersion.CURRENT_VERSION +
                 ", schema version " +  CURRENT_SCHEMA_VERSION +
                 " but the stored schema is at version " + version +
                 ". Please upgrade this node's NoSQL Database version.");
        }

        /*
         * Early code used HashMap for secondaryMap and deletedTableMap. If
         * these are detected convert them to TreeMap. Note that we are not
         * changing the schema version for this.
         */
        if (secondaryMap instanceof HashMap) {
            final Map<String, SecondaryInfo> newSecondaryMap =
                            new TreeMap<>(FieldComparator.instance);
            newSecondaryMap.putAll(secondaryMap);
            secondaryMap = newSecondaryMap;
        }
        if (deletedTableMap instanceof HashMap) {
            final Map<String, DeletedTableInfo> newDeletedTableMap =
                            new TreeMap<>(FieldComparator.instance);
            newDeletedTableMap.putAll(deletedTableMap);
            deletedTableMap = newDeletedTableMap;
        }
    }

    /**
     * Gets the secondary info object for the specified db. If none exist
     * null is returned.
     */
    SecondaryInfo getSecondaryInfo(String dbName) {
        final SecondaryInfo info = secondaryMap.get(dbName);
        return (info == null) ? null : (info.isRemoved() ? null : info);
    }

    /**
     * Returns true if a secondary DB needs populating.
     */
    boolean secondaryNeedsPopulate() {
        for (SecondaryInfo info : secondaryMap.values()) {
            if (info.needsPopulating()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a set of entries for secondaries that need to be populated.
     * The iteration order of the set is from least recently populated to
     * most recent based on the value set by SecondaryInfo.completePass(long).
     */
    Set<Entry<String, SecondaryInfo>> getSecondariesToPopulate() {
        final Set<Entry<String, SecondaryInfo>> entries =
                new TreeSet<>((Entry<String, SecondaryInfo> o1,
                               Entry<String, SecondaryInfo> o2) ->
                        (int)(o1.getValue().lastPass - o2.getValue().lastPass));

        for (Entry<String, SecondaryInfo> entry : secondaryMap.entrySet()) {
            final SecondaryInfo info = entry.getValue();
            if (info.needsPopulating()) {
                entries.add(entry);
            }
        }
        return entries;
    }

    /**
     * Returns true if a secondary DB needs cleaning.
     */
    boolean secondaryNeedsCleaning() {
        return getNextSecondaryToClean() != null;
    }

    /**
     * Gets the next secondary DB to clean. Returns null if no secondaries need
     * cleaning.
     */
    String getNextSecondaryToClean() {
        /*
         * Secondary cleaning does not need specical ordering so just return
         * the first found.
         */
        for (Entry<String, SecondaryInfo> entry : secondaryMap.entrySet()) {
            if (entry.getValue().needsCleaning()) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Gets the deleted info object for the specified table if exists. If
     * no tables need cleaning null is returned.
     *
     * tableName is a namespace name ([namespace:]tableName)
     */
    DeletedTableInfo getDeletedTableInfo(String tableName) {
        return tableName == null ? null : deletedTableMap.get(tableName);
    }

    /**
     * Returns true if a table needs cleaning.
     */
    boolean tableNeedCleaning() {
        for (DeletedTableInfo info : deletedTableMap.values()) {
            if (!info.isDone()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the next table to clean. Returns null if there are none.
     * If there are multiple tables that need cleaning this method will
     * return the least recently cleaned table.
     */
    DeletedTableInfo getNextTableToClean() {
        DeletedTableInfo ret = null;
        for (DeletedTableInfo info : deletedTableMap.values()) {
            if (info.isDone()) {
                continue;
            }
            /* Create a roughly round-robin scheme by returning the table
             * with the oldest cleaning time. New tables will pop to the
             * front for one pass.
             */
            if ((ret == null) || (ret.lastPass > info.lastPass)) {
                ret = info;
            }
        }
        return ret;
    }

    /*
     * Get the secondaryMap for index information.
     */
    public Map<String, SecondaryInfo> getSecondaryMap() {
        return this.secondaryMap;
    }

    /*
     * Get the deletedTableMap for deleted table information.
     */
    public Map<String, DeletedTableInfo> getDeletedTableMap() {
        return this.deletedTableMap;
    }

    @Override
    public String toString() {
        return "SecondaryInfoMap[" + secondaryMap.size() + ", " +
               deletedTableMap.size() + "]";
    }

    /*
     * Container for information regarding secondary databases which are
     * being populated.
     */
    static class SecondaryInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        /* true if the DB needs to be populated */
        private boolean needsPopulating = true;

        /*
         * Count of population attempts. Note: this counter was added at 18.3,
         * after the initial release and may be lost during an upgrade. However
         * the impact is minimal (the population may just be retried a few
         * extra times.
         */
        private int populationAttempts = 0;

        /* Maxinum number of times we will attempt to populate a secondary. */
        private static final int MAX_POPULATE_ATTEMPTS = 10;

        /*
         * true if the DB needs cleaning. Note that cleaning is disabled until
         * the db has been populated.
         */
        private boolean needsCleaning = false;

        /* The last key used to populate/clean */
        private DatabaseEntry lastKey = null;

        /* The last data used to clean */
        private DatabaseEntry lastData = null;

        /* The partition being used to populate */
        private PartitionId currentPartition = null;

        /* The set of partitions which have been completed */
        private Set<PartitionId> completed = null;

        /*
         * True if the index for this secondary DB has been dropped, but the
         * secondary DB has not yet been removed.
         *
         * Note that this flag was added and therefore may be lost in an
         * upgrade situation. However, this is more an optimization and things
         * will function property if incorrectly set to false.
         */
        private boolean removed = false;

        /*
         * This is non-null if the maintenance operation fails for some
         * reason.
         */
        private String errorString = null;

        /*
         * Time of last of population pass. This field was introduced after the
         * initial release but does not need any upgrade consideration. It is
         * used for an optimization, and can be lost in a mixed version shard.
         *
         * Note that this is not used for cleaning.
         */
        private long lastPass = 0L;

        /*
         * Total read and write costs due to secondary population. These
         * fields were introduced after the initial release but do not need
         * any upgrade consideration. They are for debugging purposes only
         * and can be lost in a mixed version shard.
         */
        private long totalReadKB = 0L;
        private long totalWriteKB = 0L;

        /**
         * Returns true if the secondary DB needs to be populated.
         *
         * @return true if the secondary DB needs to be populated
         */
        boolean needsPopulating() {
            return needsPopulating;
        }

        /**
         * Signal that this secondary DB has been populated.
         */
        void donePopulation() {
            assert needsPopulating == true;
            needsPopulating = false;
            clearPopulationParameters();
        }

        /**
         * Sets the needs cleaning flag.
         */
        void markForCleaning() {
            if (!removed) {
                needsCleaning = true;
            }
        }

        /**
         * Returns true if the secondary DB needs cleaning. Note that cleaning
         * is disabled until population is completed.
         *
         * @return true if the secondary DB needs cleaning
         */
        boolean needsCleaning() {
            return needsPopulating ? false : needsCleaning;
        }

        void doneCleaning() {
            assert !needsPopulating;
            needsCleaning = false;
            lastKey = null;
            lastData = null;
        }

        /**
         * Gets the last key set through setLastKey(). If no last key was set,
         * then an empty DatabaseEntry object is returned.
         *
         * @return the last key
         */
        DatabaseEntry getLastKey() {
            if (lastKey == null) {
                lastKey = new DatabaseEntry();
            }
            return lastKey;
        }

        DatabaseEntry getLastData() {
            assert needsPopulating == false;    /* only used for cleaning */
            if (lastData == null) {
                lastData = new DatabaseEntry();
            }
            return lastData;
        }

        /**
         * Gets the partition currently being read from.
         *
         * @return the partition currently being read from or null
         */
        PartitionId getCurrentPartition() {
            assert needsPopulating == true;
            return currentPartition;
        }

        /**
         * Sets the current partition.
         *
         * @param partitionId
         */
        void setCurrentPartition(PartitionId partitionId) {
            assert needsPopulating == true;
            assert partitionId != null;
            assert currentPartition == null;
            currentPartition = partitionId;
        }

        /**
         * Returns true if a populate from the specified partition has been
         * completed.
         *
         * @param partitionId
         * @return true if a populate has been completed
         */
        boolean isCompleted(PartitionId partitionId) {
            assert needsPopulating == true;
            return completed == null ? false : completed.contains(partitionId);
        }

        /**
         * Completes the current populate. Calling this method will add the
         * current partition to the completed list and clear the current
         * partition and last key.
         */
        void completeCurrentPartition() {
            assert needsPopulating == true;
            if (completed == null) {
                completed = new HashSet<>();
            }
            completed.add(currentPartition);
            currentPartition = null;
            lastKey = null;
            lastData = null;
        }

        /*
         * Records the time of a population pass.
         */
        void completePass(long timeStamp) {
            lastPass = timeStamp;
        }

        /*
         * Returns the time in MS since the last pass (since completePlass
         * was called).
         */
        long msSinceLastPass(long timeStamp) {
            return timeStamp - lastPass;
        }

        /*
         * Accumulate read and write throughput.
         */
        void accumulateThroughput(int readKB, int writeKB) {
            totalReadKB += readKB;
            totalWriteKB += writeKB;
        }

        /*
         * Set an error string and cancel any current, or future
         * population.
         */
        void setErrorString(Exception exception) {
            errorString = exception.getClass().getSimpleName() +
                          " " + exception.getMessage();
            needsPopulating = false;
        }

        String getErrorString() {
            return errorString;
        }

        /**
         * Returns true if the remove flag is set.
         *
         * @return true if the remove flag is set
         */
        private boolean isRemoved() {
            return removed;
        }

        /**
         * Sets the removed flag and clears the other state fields.
         */
        private void setRemoved() {
            removed = true;
            needsPopulating = false;
            needsCleaning = false;
            clearPopulationParameters();
        }

        /*
         * Clears the population parameters. Call when population is done, or
         * reset to clear out the fields containing other, potentially large
         * objects. This does not clear needsPopulating or lastPass.
         */
        private void clearPopulationParameters() {
            completed = null;
            currentPartition = null;
            lastKey = null;
            lastData = null;
        }

        /*
         * Resets the population of this secondary. The number of times
         * this method is called is recorded. If the count is greater than
         * or equal to the max lastError is set as the error string and the
         * population will be stopped.
         *
         * @param exception the exception causing the reset
         */
        void resetForPopulation(Exception exception) {
            assert removed == false;
            clearPopulationParameters();
            needsPopulating = true;

            /*
             * Count the number of attempts. If over the limit set the error
             * string which will stop population (needsPopulating will be
             * set to false).
             */
            populationAttempts++;
            if (populationAttempts >= MAX_POPULATE_ATTEMPTS) {
                setErrorString(exception);
            }

            /*
             * Do not reset lastPass, totalReadKB and totalWriteKB as they are
             * used for statistics and throttling.
             */
        }

        boolean isRebuild() {
            return populationAttempts > 0;
        }

        @Override
        public String toString() {
            return "SecondaryInfo[" + needsPopulating +
                ", " + currentPartition +
                ", " + ((completed == null) ? "-" : completed.size()) +
                ", " + needsCleaning + ", " + removed +
                ", " + totalReadKB + ", " + totalWriteKB +
                ", " + populationAttempts +
                ", error: " + errorString +
                "]";
        }
    }

    /*
     * Container for information on tables which are marked for deletion and
     * need to have their repecitive data removed.
     */
    static class DeletedTableInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        /* True if the primary data has been cleaned */
        private boolean done = false;

        /* The partition being cleaned */
        private PartitionId currentPartition = null;

        private Set<PartitionId> completed = null;

        private final boolean majorPathComplete;
        /* Unused but maintained. */
        private final boolean isChildTable;
        private final long targetTableId;
        private final byte[] parentKeyBytes;
        /* holds the current state of the iteration for batching */
        private byte[] currentKeyBytes;

        /**
         * Time of last of cleaning pass. This field was introduced after the
         * initial release but does not need any upgrade consideration. It is
         * used for an optimization, and can be lost in a mixed version shard.
         */
        private long lastPass = 0;

        DeletedTableInfo(TableImpl targetTable) {
            TableImpl parentTable = targetTable;
            /*
             * If the target is a child table move the parentTable
             * to the top-level parent.  After this loop it is ok if
             * parentTable == targetTable.
             */
            if (targetTable.getParent() != null) {
                isChildTable = true;
                parentTable = targetTable.getTopLevelTable();
            } else {
                isChildTable = false;
            }
            final PrimaryKey pkey = parentTable.createPrimaryKey();
            final TableKey key = TableKey.createKey(parentTable, pkey, true);
            parentKeyBytes = key.getKeyBytes();
            majorPathComplete = key.getMajorKeyComplete();
            currentKeyBytes = parentKeyBytes;
            targetTableId = targetTable.getId();
        }

        byte[] getCurrentKeyBytes() {
            return currentKeyBytes;
        }

        void setCurrentKeyBytes(byte[] newKey) {
            currentKeyBytes = newKey;
        }

        byte[] getParentKeyBytes() {
            return parentKeyBytes;
        }

        long getTargetTableId() {
            return targetTableId;
        }

        boolean getMajorPathComplete() {
            return majorPathComplete;
        }

        boolean isChildTable() {
            return isChildTable;
        }

        void completePass() {
            lastPass = System.currentTimeMillis();
        }

        boolean isDone() {
            return done;
        }

        boolean isCompleted(PartitionId partition) {
            return (completed == null) ? false : completed.contains(partition);
        }

        /**
         * Completes the current populate. Calling this method will add the
         * current partition to the completed list and clear the current
         * partition and last key.
         */
        void completeCurrentPartition() {
            if (completed == null) {
                completed = new HashSet<>();
            }
            completed.add(currentPartition);
            currentPartition = null;
        }

        PartitionId getCurrentPartition() {
            return currentPartition;
        }

        void setCurrentPartition(PartitionId partition) {
            currentPartition = partition;
        }

        void setDone() {
            done = true;
        }

        @Override
        public String toString() {
            return "DeletedTableInfo[" + done + ", " + currentPartition +
                   ", " + ((completed == null) ? "-" : completed.size()) + "]";
        }
    }
}
