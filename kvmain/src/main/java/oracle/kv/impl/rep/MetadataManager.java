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

package oracle.kv.impl.rep;

import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Set;

import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskLimitException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Base class for objects which manage metadata. The class provides basic
 * database operations for the metadata as well as a facility for replicas to
 * maintain up-to-date metadata as it is updated on the master.
 */
public abstract class MetadataManager
                                <T extends Metadata<? extends MetadataInfo>>
                                extends DatabaseTrigger {

    /*
     * Transaction config for metadata reads.
     * Note: the transaction will not wait for commit acks from the replicas.
     */
    private static final TransactionConfig NO_WAIT_CONFIG =
        new TransactionConfig().
            setDurability(new Durability(Durability.SyncPolicy.NO_SYNC,
                                         Durability.SyncPolicy.NO_SYNC,
                                         Durability.ReplicaAckPolicy.NONE)).
            setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

    /*
     * Transaction config for metadata writes.
     */
    protected static final TransactionConfig WRITE_NO_SYNC_CONFIG =
        new TransactionConfig().setDurability(
                  new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                                 Durability.SyncPolicy.WRITE_NO_SYNC,
                                 Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));

    /* Delay between DB open attempts */
    private static final int DB_OPEN_RETRY_MS = 1000;

    /* Number of times a DB operation is retried before giving up */
    private static final int NUM_DB_OP_RETRIES = 100;

    /* DB operation delays */
    private static final long SHORT_RETRY_TIME = 500;

    private static final long LONG_RETRY_TIME = 1000;

    protected final DatabaseEntry metadataKey = new DatabaseEntry();

    protected final RepNode repNode;

    protected final Logger logger;

    protected volatile boolean shutdown = false;

    /* Handle to the metadata DB */
    private volatile Database metadataDatabase;

    /* Post update listeners */
    private final Set<PostUpdateListener<T>> postUpdateListeners =
                                                            new HashSet<>();

    protected MetadataManager(RepNode repNode, Params params) {
        if (repNode == null) {
            throw new NullPointerException("repNode cannot be null");
        }
        this.repNode = repNode;
        logger = LoggerUtils.getLogger(this.getClass(), params);
        StringBinding.stringToEntry(getType().getKey(), metadataKey);
    }

    protected MetadataManager(RepNode repNode, Logger logger) {
        if (repNode == null) {
            throw new NullPointerException("repNode cannot be null");
        }
        this.repNode = repNode;
        this.logger = logger;
        StringBinding.stringToEntry(getType().getKey(), metadataKey);
    }

    /**
     * Gets the metadata type managed by this manager.
     *
     * @return the metadata type
     */
    abstract protected MetadataType getType();

    /**
     * Updates the metadata database handle. Returns true if the handle
     * needed to be updated.
     *
     * @param repEnv the replicated environment handle
     * @return true if the handle needed to be updated
     */
    protected synchronized boolean updateDbHandles(
                                            ReplicatedEnvironment repEnv) {
        final boolean refresh = DatabaseUtils.needsRefresh(metadataDatabase,
                                                           repEnv);
        if (refresh) {
            closeMetadataDb();
        }
        openMetadataDb(repEnv);
        return refresh;
    }

    /**
     * Gets the metadata database handle. If the database is not open, it
     * will attempt to open the database. If the open fails, null is returned.
     *
     * @return the metadata database handle or null
     */
    protected synchronized Database getMetadataDatabase() {
        if (metadataDatabase == null) {
            openMetadataDb(repNode.getEnv(1));
        }
        return metadataDatabase;
    }

    /**
     * Closes the metadata database handle.
     */
    protected void closeDbHandles() {
        closeMetadataDb();
    }

    /**
     * Shuts down this manager.
     */
    protected void shutdown() {
        shutdown = true;
    }

    /**
     * Adds a post update listener to track metadata updates.
     */
    public synchronized void
                    addPostUpdateListener(PostUpdateListener<T> listener) {
        postUpdateListeners.add(listener);
    }

    /**
     * Removes a post update listener. A listener should not attempt to remove
     * itself while in the postUpdate call.
     */
    public synchronized void
                    removePostUpdateListener(PostUpdateListener<T> listener) {
        postUpdateListeners.remove(listener);
    }

    /*
     * Invokes the postUpdate method on all registered listeners passing in
     * the specified metadata.
     */
    private void invokePostUpdateListeners(T md) {
        assert Thread.holdsLock(this);
        for (PostUpdateListener<T> listener : postUpdateListeners) {
            try {
                listener.postUpdate(md);
            } catch (Exception e) {
                logger.log(Level.WARNING,
                           "Unexpected exception calling metadata listener", e);
            }
        }
    }

    /**
     * Fetches the metadata object from the DB for read-only use. Returns null
     * if no metadata object is found.
     */
    protected T fetchMetadata() {
        final Database db = metadataDatabase;
        if (db == null) {
            throw new DatabaseNotReadyException(
                getType() + " database is not ready.");
        }
        /*
         * No need for transactions on the replica. Note that JE will use a
         * default configured, autocommit transaction in any case, so may as
         * well make it explicit and not rely on defaults.
         */
        final ReplicatedEnvironment repEnv =
                                    (ReplicatedEnvironment)db.getEnvironment();
        final Transaction txn = repEnv.getState() != State.MASTER ? null :
                                  repEnv.beginTransaction(null, NO_WAIT_CONFIG);
        try {
            return readFullMetadata(db, txn);
        } finally {

            /* We are just reading. */
            if ((txn != null) && txn.isValid()) {
                txn.commit();
            } else {
                TxnUtil.abort(txn);
            }
        }
    }

    /**
     * Reads the full metadata object from the DB using the specified
     * transaction. Returns null if no metadata object is found. If the
     * metadata is split this method must be overridden by the specific
     * metadata subclass.
     */
    protected T readFullMetadata(Database db, Transaction txn) {
        return readMetadata(db, txn, LockMode.DEFAULT);
    }

    /**
     * Reads the metadata object from the DB using the specified transaction.
     * Returns null if no metadata object is found. If the metadata is split,
     * a shallow copy may be returned.
     */
    private T readMetadata(Database db, Transaction txn, LockMode lm) {
        final DatabaseEntry value = new DatabaseEntry();
        db.get(txn, metadataKey, value, lm);
        return getMetadata(value);
    }

    /**
     * Deserializes the metadata object from the specified entry. If the
     * entry is empty null is returned.
     */
    @SuppressWarnings("unchecked")
    protected T getMetadata(DatabaseEntry entry) {
        return (T)SerializationUtil.getObject(entry.getData(), Metadata.class);
    }

    /**
     * Persists the specified metadata into the metadata DB of this RepNode.
     * The operation is retried if possible. The metadata object is pruned
     * before it is persisted. If the update is successful the post update
     * listeners are called with the new metadata.
     *
     * @param metadata a metadata instance
     * @param maxChanges the maximum trailing changes to be retained
     *
     * @return true if the operation was successful, or false if in shutdown
     */
    protected boolean persistMetadata(T metadata, int maxChanges) {
        return persistMetadata(metadata, maxChanges, null);
    }

    /**
     * Persists the specified metadata into the metadata DB of this RepNode.
     * The operation will retry if possible. The metadata object is pruned
     * before it is persisted. If the update is successful the post update
     * listeners are called with the new metadata. If op is not null, its
     * call() method is invoked within the transaction after the metadata
     * has been written.
     *
     * @param newMetadata a metadata instance
     * @param maxChanges the maximum trailing changes to be retained
     * @param op a MetadataOperation
     *
     * @return true if the operation was successful, or false if in shutdown
     */
    protected boolean persistMetadata(T newMetadata, int maxChanges,
                                      TxnOperation<T> op) {
        newMetadata.pruneChanges(Integer.MAX_VALUE, maxChanges);

        final Boolean success  = tryDBOperation((Database db) -> {
            Transaction txn = null;
            try {
                txn = db.getEnvironment().
                        beginTransaction(null, WRITE_NO_SYNC_CONFIG);
                /*
                 * Get the metadata to persist. If null is returned the
                 * write is not necessary.
                 */
                final T oldMetadata = readMetadata(db, txn, LockMode.RMW);
                final T mdToWrite = getMdToWrite(oldMetadata, newMetadata);
                if (mdToWrite == null) {
                    return true;
                }

                /* Persist the metadata */
                final DatabaseEntry data = new DatabaseEntry(
                                         SerializationUtil.getBytes(mdToWrite));
                db.put(txn, metadataKey, data);

                /* If there was an op specified, execute that */
                if (op != null) {
                    op.call(oldMetadata, db, txn);
                }
                txn.commit();
                txn = null;
                return true;
            } catch (InsufficientAcksException iae) {
                /*
                * The transaction was completed locally, however not enough
                * replicas acked. Return success. Note that the txn is
                * aborted (in the finally clause) but that is OK.
                */
                return true;
            } finally {
                TxnUtil.abort(txn);
            }
        });

        if ((success == null) || !success) {
            /** Shutdown or retry exhausted */
            return false;
        }

        logger.info(() -> "Metadata stored: " + newMetadata);
        invokePostUpdateListeners(newMetadata);
        return true;
    }

    /**
     * Returns the metadata object to write. The returned MD may be a shallow
     * copy. Returns null if the write is not necessary due to the new metadata
     * being stale. Note that currentMetadata may be a shallow copy.
     */
    protected T getMdToWrite(T currentMetadata, T newMetadata) {
        if (currentMetadata == null) {
            return newMetadata;
        }

        /* Don't store an older MD. */
        if (currentMetadata.getSequenceNumber() >
            newMetadata.getSequenceNumber()) {
            logger.log(Level.INFO, "Metadata not stored. Current " +
                       "sequence number {0} is > new sequence " +
                       "number {1} type: {2}",
                       new Object[]{currentMetadata.getSequenceNumber(),
                                    newMetadata.getSequenceNumber(),
                                    newMetadata.getType()});
            return null;
        }
        return newMetadata;
    }

    private void openMetadataDb(ReplicatedEnvironment repEnv) {
        assert Thread.holdsLock(this);

        if (repEnv == null) {
            return;
        }
        while ((metadataDatabase == null) && !shutdown && repEnv.isValid()) {

            final DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true).
                     setTransactional(true).
                     getTriggers().add(this);

            try {
                metadataDatabase = openDb(repEnv, dbConfig);
                assert metadataDatabase != null;
                logger.log(Level.INFO, "Open {0} DB", getDBName());
                return;
            } catch (RuntimeException re) {
                if (!DatabaseUtils.handleException(re, logger, getDBName())){
                    return;
                }
            }

            /* Wait to retry */
            try {
                Thread.sleep(DB_OPEN_RETRY_MS);
            } catch (InterruptedException ie) {
                /* Should not happen. */
                throw new IllegalStateException(ie);
            }
        }
    }

    private String getDBName() {
        return getType().getKey() + "Metadata";
    }

    private Database openDb(Environment env, DatabaseConfig dbConfig) {

        final TransactionConfig txnConfig = new TransactionConfig().
              setConsistencyPolicy(NoConsistencyRequiredPolicy.NO_CONSISTENCY);

        Transaction txn = null;
        Database db = null;
        try {
            txn = env.beginTransaction(null, txnConfig);
            db = env.openDatabase(txn, getDBName(), dbConfig);
            txn.commit();
            txn = null;
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

    private synchronized void closeMetadataDb() {
        if (metadataDatabase == null) {
            return;
        }
        logger.log(Level.INFO, "Closing {0} db", getDBName());

        TxnUtil.close(logger, metadataDatabase, getDBName());

        metadataDatabase = null;
    }

    /**
     * Executes the operation, retrying if necessary based on the type of
     * exception. The operation will be retried until 1) success, 2) shutdown,
     * or 3) the maximum number of retries has been reached.
     *
     * The return value is the value returned by op.call() or null if shutdown
     * occurs during retry or retry has been exhausted.
     *
     * @param <R> type of the return value
     * @param op the operation
     * @return the value returned by op.call() or null
     */
    protected <R> R tryDBOperation(DBOperation<R> op) {
        int retryCount = NUM_DB_OP_RETRIES;

        while (!shutdown && (retryCount > 0)) {
            retryCount--;
            try {
                final Database db = getMetadataDatabase();

                if (db != null) {
                    return op.call(db);
                }
                if (retryCount <= 0) {
                    logger.warning("Metadata operation could not get" +
                                   " metadata DB");
                    return null;
                }
                retrySleep(retryCount, LONG_RETRY_TIME, null);
            } catch (InsufficientAcksException |
                     InsufficientReplicasException e) {
                retrySleep(retryCount, LONG_RETRY_TIME, e);
            } catch (DiskLimitException dle) {
                /*
                 * Do not retry, simply exit since the problem will not be
                 * resolved very quickly.
                 */
                logger.log(Level.WARNING,
                           "Metadata operation failed with exception: ",
                           dle.getMessage());
                break;
            } catch (LockConflictException lce) {
                retrySleep(retryCount, SHORT_RETRY_TIME, lce);
            }
        }
        return null;
    }

    /**
     * Sleeps for the specified time if count is > 0. Otherwise re-throws the
     * exception.
     */
    private void retrySleep(int count, long sleepTime, DatabaseException de) {
        if (count <= 0) {
            throw de;
        }
        logger.log(Level.FINE,
                   "Metadata operation caused {0} attempts left {1}",
                   new Object[]{de.getMessage(), count});
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            /* Should not happen. */
            throw new IllegalStateException(ie);
        }
    }

    /* -- From DatabaseTrigger -- */

    @Override
    protected boolean isShutdown() {
        return shutdown;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected ReplicatedEnvironment getEnv() {
        return repNode.getEnv(0);
    }

    /**
     * A database operation that returns a result and may throw an exception.
     *
     * @param <V> result type
     */
    public interface DBOperation<V> {

        /**
         * Invokes the operation. This method may be called multiple times
         * in the course of retrying in the face of failures.
         *
         * @param db the metadata db
         * @return the result
         */
        V call(Database db);
    }

    /**
     * A database operation using a transaction.
     *
     * @param <V> result type
     */
    public interface TxnOperation<V> {

        /**
         * Invokes the operation. This method may be called multiple times
         * in the course of retrying in the face of failures. The semantics of
         * the metadata parameter is determined by the implementation.
         *
         * @param  metadata a metadata object
         * @param db the metadata db
         * @param txn the current transaction
         * @return the result
         */
        V call(V metadata, Database db, Transaction txn);
    }

    /**
     * Post metadata update listener.
     */
    public interface
                PostUpdateListener<T extends Metadata<? extends MetadataInfo>> {

        /**
         * Notifies the listener that new metadata is available. Invoked after
         * the metadata has been successfully persisted. Note that structures
         * dependent on the metadata may be updated after the call to
         * postUpdate. The the implementation should not assume the rest of
         * the RN has completed processing the new metadata.
         */
        void postUpdate(T metadata);
    }
}
