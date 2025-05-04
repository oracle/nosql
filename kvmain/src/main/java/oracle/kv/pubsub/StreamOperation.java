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

package oracle.kv.pubsub;

import java.util.List;

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.txn.TransactionIdImpl;

/**
 * The operation (Put, Delete) that was delivered over the NoSQL stream.
 */
public interface StreamOperation {

    /**
     * Returns the unique sequence id associated with this operation.
     */
    SequenceId getSequenceId();

    /**
     * Returns the shard id associated with this operation.
     */
    int getRepGroupId();

    /**
     * @hidden
     *
     * Returns the table id of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    long getTableId();

    /**
     * @hidden
     *
     * Returns the full table name of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    String getFullTableName();

    /**
     * @hidden
     *
     * Returns the table name of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    String getTableName();

    /**
     * @hidden
     *
     * Returns the region id of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    int getRegionId();

    /**
     * @hidden
     *
     * Returns the last modification time of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    long getLastModificationTime();

    /**
     * @hidden
     *
     * Returns the expiration time of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    long getExpirationTime();

    /**
     * @hidden
     *
     * Returns the JSON string of this operation.
     *
     * TODO: Update @since with release version when removing @hidden.
     */
    String toJsonString();

    /**
     * The type of the operation.
     */
    enum Type {
        /**
         * A {@link PutEvent} operation.
         */
        PUT,

        /**
         * A {@link DeleteEvent} operation.
         */
        DELETE,

        /**
         * @hidden
         *
         * An internally generated stream operation
         */
        INTERNAL,

        /**
         * @hidden
         * A {@link TransactionEvent} operation
         */
        TRANSACTION
    }

    /**
     * Returns the type of this operation.
     *
     * @return the type of this operation
     */
    Type getType();

    /**
     * Converts this operation to a {@link PutEvent}.
     *
     * @return this operation as a Put
     * @throws IllegalArgumentException if this operation is not a Put
     */
    PutEvent asPut();

    /**
     * Converts this operation to a {@link DeleteEvent}.
     *
     * @return this operation as a Delete
     * @throws IllegalArgumentException if this operation is not a Delete
     */
    DeleteEvent asDelete();

    /**
     * @hidden
     * Converts this operation to a {@link TransactionEvent}.
     *
     * @return this operation as a Transaction
     * @throws IllegalArgumentException if this operation is not a Transaction
     */
    default TransactionEvent asTransaction() {
        throw new IllegalArgumentException(
            "This operation is not a transaction");
    }

    /**
     * Used to signal a Put operation
     */
    interface PutEvent extends StreamOperation {

        /**
         * Returns the Row associated with the put operation.
         * <p>
         * Note that TTL information can be obtained from the Row.
         */
        Row getRow();

        /**
         * @hidden
         *
         * Internal use only.
         *
         * Returns the primary key associated with the put operation in bytes.
         * <p>
         * The format of bytes should be defined in the
         * {@link StreamOperation.Deserializer} when configure the subscription.
         */
        byte[] getPrimaryKeyBytes();

        /**
         * @hidden
         *
         * Internal use only.
         *
         * Returns the Row associated with the put operation in bytes.
         * <p>
         * The format of bytes should be defined in the
         * {@link StreamOperation.Deserializer} when configure the subscription.
         */
        byte[] getRowBytes();

        /**
         * @hidden
         *
         * Internal use only
         *
         * Returns an estimated storage size in bytes of the row in the put
         * operation
         */
        long getRowSize();
    }

    /**
     * Used to signal a Delete operation
     */
    interface DeleteEvent extends StreamOperation {
        /**
         * Returns the primary key associated with the delete operation.
         */
        PrimaryKey getPrimaryKey();

        /**
         * @hidden
         *
         * Internal use only.
         *
         * Returns the primary key associated with the delete operation
         * in bytes.
         * <p>
         * The format of bytes should be defined in the
         * {@link StreamOperation.Deserializer} when configure the subscription.
         */
        byte[] getPrimaryKeyBytes();

        /**
         * @hidden
         *
         * Internal use only
         *
         * Returns an estimated storage size in bytes of the primary key in the
         * delete operation
         */
        long getPrimaryKeySize();
    }

    /**
     * @hidden
     * Used to signal a Transaction operation
     */
    interface TransactionEvent extends StreamOperation {

        /**
         * Returns the transaction id
         * @return transaction id
         */
        TransactionIdImpl getTransactionId();

        /**
         * Returns the type of the transaction as {@link TransactionType}
         * @return transaction type
         */
        TransactionType getTransactionType();

        /**
         * Returns the number of write operations in the transaction
         * @return the number of write operations
         */
        long getNumOperations();

        /**
         * Returns an ordered list of write operations in the transaction.
         * All write operations are on the same order they are performed in
         * the source kvstore.
         * @return ordered list of write operations.
         */
        List<StreamOperation> getOperations();

        /**
         * Types of transaction
         */
        enum TransactionType {

            /** committed transaction */
            COMMIT,

            /** aborted transaction */
            ABORT
        }
    }

    /**
     * A SequenceId uniquely identifies a stream operation associated with a
     * Publisher.
     * <p>
     * It can also be used to sequence operations on the same key. Note that
     * subscription API provides no guarantees about the order of operations
     * beyond the single key. That is, only for any single key, the subscription
     * API guarantees the order of events on that particular key received by
     * subscriber is the same order these operations applied in NoSQL DB.
     * Therefore it is the application's responsibility to ensure that the
     * comparison is only used in the context of the same key, since the key
     * is not part of its state.
     * <p>
     * If compareTo is called to compare the sequenceId of two instances of
     * different implementing classes, ClassCastException will be thrown
     * because the sequenceIds from different classes are not directly
     * comparable.
     */
    interface SequenceId extends Comparable<SequenceId> {

        /*
         * Implementation Note: SequenceId is simply an encapsulation of the
         * Store VLSN at the mean time. The sequence id may extend to cross
         * multiple shards in future when pub/sub API is adaptive to elastic
         * operations.
         */

        /**
         * Returns a byte representation suitable for saving in some other
         * data source. The byte array representation can also be used as a for
         * comparisons.
         */
        byte[] getBytes();
    }

    /**
     * @hidden
     *
     * Internal use only
     *
     * Deserializer interface to create StreamOperations from JE data entries.
     */
    interface Deserializer {

        /**
         * Get a PutEvent from given key and value pair.
         */
        PutEvent getPutEvent(NoSQLSubscriberId subscriberId,
                             RepGroupId repGroupId,
                             TableImpl table,
                             byte[] key,
                             byte[] value,
                             SequenceId sequenceId,
                             long lastModificationTime,
                             long expirationTime);

        /**
         * Get a DeleteEvent from given key.
         */
        DeleteEvent getDeleteEvent(NoSQLSubscriberId subscriberId,
                                   RepGroupId repGroupId,
                                   TableImpl table,
                                   byte[] key,
                                   byte[] value,
                                   SequenceId sequenceId,
                                   long lastModificationTime,
                                   long expirationTime,
                                   boolean exactTable);
    }
}
