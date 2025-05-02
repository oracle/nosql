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

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;

/**
 * Base JE database class for persisting Admin data. An instance must provide a
 * keyToEntry() method to convert the data's key to a DatabaseEntry.
 *
 * @param <K> type of key
 * @param <T> type of object
 */
public abstract class AdminDatabase <K, T> {

    /**
     * Database types. Names must be unique and must not change.
     */
    public static enum DB_TYPE {
        EVENTS("CriticalEvents"),
        MEMO("Memo"),
        PARAMETERS("Parameters"),
        PLAN("Plan"),
        SECURITY("Security"),
        TABLE("Table"),
        TOPOLOGY_CANDIDATE("TopologyCandidate"),
        TOPOLOGY_HISTORY("TopologyHistory");

        private final String typeName;

        private DB_TYPE(String typeName) {
            this.typeName = typeName;
        }

        /**
         * Returns the database name associated with the type.
         */
        public String getDBName() {
            return String.format("Admin%sDatabase", typeName);
        }
    }

    /* Delay between DB open attempts */
    private static final int DB_OPEN_RETRY_MS = 1000;

    /* Max DB open attempts */
    private static final int DB_OPEN_RETRY_MAX = 20;

    /*
     * Used in case we want to change the default cursor configuration.
     */
    static final CursorConfig CURSOR_CONFIG = CursorConfig.DEFAULT;

    private final Logger logger;
    private final String dbName;

    /*
     * The underlying JE database. Can be null if there was an issue
     * during construction.
     */
    private volatile Database database = null;

    private volatile boolean closing = false;

    /**
     * Create a AdminDatabase instance. Construction will wait if the
     * environment is not yet ready and retry on some exceptions.
     *
     * @param type the type of database
     * @param logger logger
     * @param env the environment
     * @param readOnly if true open the DB in read-only mode
     */
    protected AdminDatabase(DB_TYPE type, Logger logger,
                            Environment env, boolean readOnly) {
        this.dbName = type.getDBName();
        this.logger = logger;
        assert env != null;

        int retries = 0;
        Exception lastCause = null;
        while ((database == null) && !closing && env.isValid()) {

            final DatabaseConfig dbConfig =
                 new DatabaseConfig().setAllowCreate(!readOnly).
                                      setReadOnly(readOnly).
                                      setTransactional(!readOnly);
            try {
                database = openDb(env, dbConfig, readOnly);
                assert database != null;
                logger.log(Level.INFO, "Open: {0}", dbName); // TODO FINE
                return;
            } catch (RuntimeException re) {
                if (!DatabaseUtils.handleException(re, logger, dbName)){
                    return;
                }
                lastCause = re;
            }

            if (retries >= DB_OPEN_RETRY_MAX) {
                throw new IllegalStateException(
                    String.format(
                        "Failed to open %s after %d retries: %s",
                        dbName, retries, lastCause),
                    lastCause);
            }

            logger.log(Level.INFO,
                       "Retry opening {0} because {1}",
                       new Object[] {dbName, lastCause});

            retries++;

            /* Wait to retry */
            try {
                Thread.sleep(DB_OPEN_RETRY_MS);
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    private Database openDb(Environment env,
                            DatabaseConfig dbConfig,
                            boolean readOnly) {
        final TransactionConfig txnConfig = new TransactionConfig().
               setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        Transaction txn = null;
        Database db = null;
        try {
            txn = readOnly ? null : env.beginTransaction(null, txnConfig);
            db = env.openDatabase(txn, dbName, dbConfig);
            if (txn != null) {
                txn.commit();
                txn = null;
            }
            final Database ret = db;
            db = null;
            return ret;
        } finally {
            TxnUtil.abort(txn);

            if (db != null) {
                try {
                    db.close();
                } catch (DatabaseException de) {
                    /* Ignore */
                }
            }
        }
    }

    /**
     * Closes the underlying database.
     */
    synchronized void close() {
        closing = true;
        if (database == null) {
            return;
        }
        logger.log(Level.INFO, "Closing {0}", dbName);   // TODO FINE
        TxnUtil.close(logger, database, dbName);
        database = null;
    }

    /**
     * Persists an value into the database.  Callers are responsible for
     * exception handling.
     *
     * @param txn transaction
     * @param value value to be persisted
     * @param noOverwrite do not overwrite if true
     *
     * @return true if the noOverwrite is true and the metadata exists
     *
     * @throws DatabaseNotReadyException if the database handle is not yet
     * opened
     */
    boolean put(Transaction txn, K key, T value, boolean noOverwrite) {
        final DatabaseEntry keyEntry = keyToEntry(key);
        final DatabaseEntry data =
            new DatabaseEntry(SerializationUtil.getBytes(value));
        if (noOverwrite) {
            final OperationStatus status =
                    getDB().putNoOverwrite(txn, keyEntry, data);
            return status.equals(OperationStatus.KEYEXIST);
        }
        getDB().put(txn, keyEntry, data);
        return false;
    }

    /**
     * Gets an value using the specified key.  Callers are
     * responsible for exception handling.
     *
     * @param txn transaction
     * @param key key
     * @param lockMode lockMode
     * @return an object corresponding to the key
     * @throws DatabaseNotReadyException if the database handle is not yet
     * opened
     */
    T get(Transaction txn, K key, LockMode lockMode,
                       Class<T> oclass) {
        final DatabaseEntry keyEntry = keyToEntry(key);
        final DatabaseEntry value = new DatabaseEntry();
        getDB().get(txn, keyEntry, value, lockMode);
        return SerializationUtil.getObject(value.getData(), oclass);
    }

    /**
     * Deletes the value using the specified key.
     *
     * @param txn transaction
     * @param key key
     * @throws DatabaseNotReadyException if the database handle is not yet
     * opened
     */
    void delete(Transaction txn, K key) {
        getDB().delete(txn, keyToEntry(key));
    }

    /**
     * Opens a database cursor.
     *
     * @param txn transaction
     * @return a database cursor
     * @throws DatabaseNotReadyException if the database handle is not yet
     * opened
     */
    Cursor openCursor(Transaction txn) {
        return getDB().openCursor(txn, CURSOR_CONFIG);
    }

    /**
     * Compares two keys using either the default comparator.
     *
     * @return -1 if entry1 compares less than entry2, 0 if entry1 compares
     * equal to entry2, 1 if entry1 compares greater than entry2
     */
    int compareKeys(DatabaseEntry entry1, DatabaseEntry entry2) {
        return getDB().compareKeys(entry1, entry2);
    }

    long count() {
        return getDB().count();
    }

    /**
     * Returns the database handle. If the handle has not been initialized
     * an exception is thrown. This method should be used to access the
     * handle.
     */
    private synchronized Database getDB() {
        if (database == null) {
            /* shutting down */
            if (closing) {
                throw new NonfatalAssertionException(
                    dbName + " is closed in admin shutting down");
            }
            /* Issue during construction */
            throw new DatabaseNotReadyException(dbName + " is not ready");
        }
        return database;
    }

    /**
     * Transforms the specified key to a database entry so that it can be used
     * in underlying database.
     *
     * @param key key
     * @return the database entry representing the key
     */
    protected abstract DatabaseEntry keyToEntry(K key);

    /**
     * Convenience class implementing a database with a Long key.
     */
    static class LongKeyDatabase<T> extends AdminDatabase<Long, T> {

        /**
         * Key used if the stored object is a singleton. If additional instances
         * or a history of objects need to be stored, an index, sequence
         * number, or time stamp could be used for a key.
         */
        static final Long ZERO_KEY = Long.valueOf(0);

        LongKeyDatabase(DB_TYPE type, Logger logger,
                        Environment repEnv, boolean readOnly) {
            super(type, logger, repEnv, readOnly);
        }

        @Override
        protected DatabaseEntry keyToEntry(Long key) {
            final DatabaseEntry keyEntry = new DatabaseEntry();
            LongBinding.longToEntry(key, keyEntry);
            return keyEntry;
        }
    }
}
