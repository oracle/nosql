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

package oracle.kv.impl.rep.migration;

import java.util.logging.Logger;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

/**
 * A thread local handle to the migration stream.
 *
 * This class implements a thread local variable which is accessed during a
 * client operation. Specifically, when the client operation creates a
 * transaction it should initialize the thread local by calling
 * MigrationStreamHandle.initialize(). Then as the client operations are
 * executed the handle's addUpdate() and addDelete() methods should be invoked
 * for each update and delete operation respectively.
 *
 * At the end of the client operation but before txn.commit() is invoked, the
 * prepare() method on the handle should be invoked. The done() method must
 * always be called once client operations are completed.
 *
 * When initialize() is invoked, a check is made to see if the partition
 * that the operation affecting is being migrated. If so, the thread local
 * handle will forward the update and delete operations to the migration stream.
 * If no migration is active, the handle will do nothing.
 *
 * The base class does nothing. It provides the interface for the
 * normal (non-migrating) case.
 *
 * The forwarding handle is implemented by the MigratingHandle subclass.
 */
public class MigrationStreamHandle {

    private static final ThreadLocal<MigrationStreamHandle> handle =
            new ThreadLocal<MigrationStreamHandle>() {

        @Override
        protected synchronized MigrationStreamHandle initialValue() {

            /*
             * The thread local must be explicitly set by first invoking
             * MigrationStreamHandle.initialize().
             */
            throw new IllegalStateException("Handle not initialized");
        }
    };

    /**
     * Normal non-forwarding handle. This instance is a no-op and can be
     * shared.
     */
    public static final MigrationStreamHandle NOOP_HANDLER =
        new MigrationStreamHandle();

    /**
     * Returns the thread local migration stream handle. Note that this method
     * throws an IllegalStateException unless initialize() has been called.
     *
     * @return a stream handle
     */
    public static MigrationStreamHandle get() {
        return handle.get();
    }

    /**
     * Sets the thread local migration stream handler.
     * @param h migration stream handler.
     */
    public static void setHandle(MigrationStreamHandle h) {
        handle.set(h);
    }

    /**
     * Checks if the thread local is stale (left over from a previous stream).
     * If so an exception is thrown, otherwise true is returned.
     */
    public static boolean checkForStaleHandle() {
        try {
            MigrationStreamHandle h = handle.get();
            throw new IllegalStateException("Handle still around? " + h);
        } catch (IllegalStateException ise) {
            /* Expected */
            return true;
        }
    }

    private MigrationStreamHandle() {
    }

    /**
     * Inserts a PUT record into migration stream if partition migration
     * is in progress. Otherwise this method does nothing.
     *
     * @param key
     * @param value
     * @param modificationTime
     * @param vlsn
     * @param expirationTime
     * @param isTombstone
     */
    public void addPut(DatabaseEntry key, DatabaseEntry value,
                       long vlsn, long modificationTime,
                       long expirationTime, boolean isTombstone) {
        /* NOOP */
    }

    /**
     * Inserts a DELETE record into migration stream if partition migration
     * is in progress. Otherwise this method does nothing.
     *
     * @param key
     * @param cursor
     */
    public void addDelete(DatabaseEntry key, Cursor cursor) {
        /* NOOP */
    }

    /**
     * Inserts a PREPARE message into the migration stream if partition
     * migration is in progress. Otherwise this method does nothing. This
     * method should be invoked before the client transaction is committed.
     * The PREPARE message signals that the operations associated with this
     * transaction are about to be committed. No further operations can be
     * added once prepared.
     */
    public void prepare() {
        /* NOOP */
    }

    /**
     * Signals that this operations associated with this transaction are done.
     * Depending on the transaction's outcome, a COMMIT or ABORT message is
     * inserted into the migration stream.
     */
    public void done(PartitionId pid,
                     RepNode repNode,
                     Transaction txn,
                     boolean pendingCounterIncr) {
        if (pendingCounterIncr) {
            final MigrationManager mm = repNode.getMigrationManager();
            final int pending = mm.decrNoopHandlerWrites(pid);
            final Logger logger = repNode.getLogger();
            logger.finest(() -> "[" + pid + "] Decrease #pending " +
                                "writes=" + pending +
                                ", txn id=" + txn.getId());
        }
        /* NOOP */
        handle.remove();
    }

    @Override
    public String toString() {
        return "MigrationStreamHandle[]";
    }


    /**
     * Returns an instance of migrating handle {@link MigratingHandle}
     * @param source migration source
     * @param txn transaction
     * @return a migrating handle
     */
    static MigratingHandle getMigratingHandle(MigrationSource source,
                                              Transaction txn) {
        return new MigratingHandle(source, txn);
    }

    /**
     * Subclass for when migration is taking place.
     */
    static class MigratingHandle extends MigrationStreamHandle {

        private final MigrationSource source;

        /* Transaction associated with this thread. */
        private final Transaction txn;

        /*
         * Number of DB operations that have been sent. Not all add*() calls
         * will result in sent messages due to key filtering.
         */
        private int opsSent = 0;

        /* True if prepare() has been called. */
        private boolean prepared = false;

        /* True if done() has been called. */
        private boolean done = false;

        private MigratingHandle(MigrationSource source, Transaction txn) {
            super();
            this.source = source;
            this.txn = txn;
        }

        @Override
        public void addPut(DatabaseEntry key, DatabaseEntry value,
                           long vlsn, long modificationTime,
                           long expirationTime, boolean isTombstone) {
            assert !prepared;
            assert key != null;
            assert value != null;
            if (source.sendPut(txn.getId(), key, value,
                               vlsn, modificationTime,
                               expirationTime, isTombstone)) {
                opsSent++;
            }
        }

        @Override
        public void addDelete(DatabaseEntry key, Cursor cursor) {
            assert !prepared;
            assert key != null;

            if (source.sendDelete(txn.getId(), key, cursor)) {
                opsSent++;
            }
        }

        @Override
        public void prepare() {
            assert !prepared;
            assert !done;

            /*
             * Don't bother sending PREPARE (or COMMIT or ABORT) messages if no
             * DB operations have been sent.
             */
            if (opsSent > 0) {
                source.sendPrepare(txn.getId());
            }
            prepared = true;
        }

        @Override
        public void done(PartitionId pid,
                         RepNode repNode,
                         Transaction transaction,
                         boolean pendingCounterIncr) {
            assert !done;

            try {
                if (opsSent > 0) {
                    source.sendResolution(transaction.getId(),
                                          transaction.getState().
                                            equals(Transaction.State.COMMITTED),
                                          prepared);
                }
            } finally {
                done = true;
                /* Remove handle */
                super.done(pid, repNode, transaction, pendingCounterIncr);
            }
        }

        @Override
        public String toString() {
            return "MigratingHandle[" + prepared + ", " + done +
                   ", " + opsSent + "]";
        }
    }
}
