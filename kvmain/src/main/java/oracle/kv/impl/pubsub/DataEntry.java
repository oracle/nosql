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

import static oracle.kv.impl.pubsub.DataEntry.Type.TXN_ABORT;
import static oracle.kv.impl.pubsub.DataEntry.Type.TXN_COMMIT;

import oracle.kv.table.TimeToLive;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.tree.Key;

/**
 * Object to represent an operation from source kvstore via replication
 * stream. Each entry is reconstructed from a single message in replication
 * stream. Received entries will be queued in a FIFO queue to be processed
 * at granularity of transaction. Currently two types of operations can be
 * constructed from replication stream: 1) a data operation representing a
 * write (e.g., put or delete) operation in kvstore; 2) a transactional
 * operation (e.g, commit or abort). All other type of messages shall be
 * filtered out either by feeder filter or the client-side replication
 * stream callback that consumes incoming messages.
 */
public class DataEntry {

    /* type of data entry */
    private final Type type;
    /* vlsn associated with the entry */
    private final long vlsn;
    /* txn id associated with the entry */
    private final long txnId;
    /* key part of the entry */
    private final byte[] key;
    /* value part of the entry, null for deletion */
    private final byte[] value;
    /*
     * db id of the entry, non-null for PUT and DELETE and null for other types
     */
    private final DatabaseId dbId;
    /* timestamp in ms of last modification time, or 0 if not applicable */
    private final long lastUpdateMs;

    /**
     * expiration time if data never expire in accordance with
     * {@link oracle.kv.table.TimeToLive#DO_NOT_EXPIRE}
     */
    private static final long DO_NOT_EXPIRE_EXPIRATION_TIME_MS =
        TTL.ttlToExpiration((int)TimeToLive.DO_NOT_EXPIRE.getValue(),
                            TimeToLive.DO_NOT_EXPIRE.getUnit());
    /** expiration time in ms */
    private final long expirationMs;

    /**
     * Builds a data entry
     *
     * @param type     type of entry
     * @param vlsn     vlsn of operation
     * @param txnId    txn id
     * @param key      key of the entry
     * @param value    value of the entry
     * @param dbId     database ID of the entry or null
     * @param lastUpdateMs last update time in ms
     * @param expirationMs expiration time
     */
    DataEntry(Type type, long vlsn, long txnId, byte[] key, byte[] value,
              DatabaseId dbId, long lastUpdateMs, long expirationMs) {

        this.type = type;
        this.vlsn = vlsn;
        this.txnId = txnId;
        this.key = key;
        this.value = value;
        this.dbId = dbId;
        this.lastUpdateMs = lastUpdateMs;
        this.expirationMs = expirationMs;
    }

    /**
     * Builds data entry that never expires
     *
     * @param type     type of entry
     * @param vlsn     vlsn of operation
     * @param txnId    txn id
     * @param key      key of the entry
     * @param value    value of the entry
     * @param dbId     database ID of the entry or null
     * @param lastUpdateMs last update time in ms
     */
    DataEntry(Type type, long vlsn, long txnId, byte[] key, byte[] value,
              DatabaseId dbId, long lastUpdateMs) {
        this(type, vlsn, txnId, key, value, dbId, lastUpdateMs,
             DO_NOT_EXPIRE_EXPIRATION_TIME_MS);
    }

    /**
     * Builds a data entry that represents a transaction commit
     *
     * @param vlsn     vlsn of entry
     * @param txnId    txn id
     * @param ts       timestamp of commit
     */
    static DataEntry getCommitEntry(long vlsn, long txnId, long ts) {
        return new DataEntry(TXN_COMMIT, vlsn, txnId, null, null, null, ts);
    }

    /**
     * Builds a data entry that represents a transaction abort
     *
     * @param vlsn     vlsn of entry
     * @param txnId    txn id
     * @param ts       timestamp of abort
     */
    static DataEntry getAbortEntry(long vlsn, long txnId, long ts) {
        return new DataEntry(TXN_ABORT, vlsn, txnId, null, null, null, ts);
    }

    /**
     * Gets entry type
     *
     * @return type of entry
     */
    Type getType() {
        return type;
    }

    /**
     * Gets the VLSN associated with the entry
     *
     * @return VLSN of the entry
     */
    long getVLSN(){
        return vlsn;
    }

    /**
     * Gets the TXN id associated with the item if it is transactional
     *
     * @return txn id of the item
     */
    long getTxnID() {
        return txnId;
    }

    /**
     * Gets the key in the item if exists
     *
     * @return key as byte array, null if does not exist
     */
    byte[] getKey() {
        return key;
    }

    /**
     * Gets the value in the item if exists
     *
     * @return value as byte array, null if it does not exist, e.g., in
     * delete operation.
     */
    byte[] getValue() {
        return value;
    }

    /**
     * Returns the database id of the entry, or null
     *
     * @return database id of the entry or null
     */
    DatabaseId getDbId() {
        return dbId;
    }

    /**
     * Returns the last update time in milliseconds
     *
     * @return the last update time in milliseconds
     */
    long getLastUpdateMs() {
        return lastUpdateMs;
    }

    /**
     * Returns the expiration time in ms, or 0 if never expire
     * @return the expiration time in ms, or 0 if never expire
     */
    long getExpirationMs() {
        return expirationMs;
    }

    @Override
    public String toString() {
        StringBuilder msg  = new StringBuilder();

        switch(type) {
            case TXN_COMMIT:
                msg.append("txn commit, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", txn id: ").append(txnId);
                break;

            case TXN_ABORT:
                msg.append("txn abort, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", txn id: ").append(txnId);
                break;

            case DELETE:
                msg.append("delete op, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", key:").append(getKeyBytesString())
                   .append(", txn id: ").append(txnId)
                   .append(", db id: ").append(dbId)
                   .append(", last update time: ").append(lastUpdateMs);
                break;

            case PUT:
                msg.append("put op, ")
                   .append("vlsn: ").append(vlsn)
                   .append(", key:").append(getKeyBytesString())
                   .append(", txn id: ").append(txnId)
                   .append(", db id: ").append(dbId)
                   .append(", last update time: ").append(lastUpdateMs)
                   .append(", expiration: ").append(
                       (expirationMs != DO_NOT_EXPIRE_EXPIRATION_TIME_MS) ?
                           expirationMs : "never");
                break;

            default:
                break;
        }
        msg.append("\n");
        return msg.toString();
    }

    /**
     * Returns the key bytes in string using JE helper method
     * @return key bytes in string
     */
    private String getKeyBytesString() {
        return "[" + Key.getNoFormatString(key) +"]";
    }

    /**
     * Type of messages supported in publisher
     */
    enum Type {

        /* txn commit */
        TXN_COMMIT,

        /* txn abort */
        TXN_ABORT,

        /* msg represents a deletion of a key */
        DELETE,

        /* msg represents a put (insert or update) of a key */
        PUT;

        private static final Type[] VALUES = values();

        /**
         * Gets the OP corresponding to the specified ordinal.
         */
        public static Type get(int ordinal) {
            return VALUES[ordinal];
        }
    }
}
