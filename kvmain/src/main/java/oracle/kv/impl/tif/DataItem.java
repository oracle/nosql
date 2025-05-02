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

package oracle.kv.impl.tif;

import static com.sleepycat.je.log.LogEntryType.LOG_TXN_ABORT;
import static com.sleepycat.je.log.LogEntryType.LOG_TXN_COMMIT;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;

import java.util.Arrays;

import oracle.kv.impl.topo.PartitionId;

import com.sleepycat.je.log.LogEntryType;

/**
 * Object to rebundle the message consumed from subscription API and build a
 * unit of data entry for TIF worker to process
 *
 * Types of DataItem
 *  -- A data operation represents a write operation (e.g., put, delete)
 *  -- A transactional boundary operation represents commit or abort
 *  -- A name database operation represents an operation to the name
 *  database, assuming it is not filtered out at feeder side
 *  -- An exception to be processed by callback
 */
class DataItem {

    /* at source, no txn id is sent for a COPY from partition stream */
    public static final long TXN_ID_COPY_IN_PARTTRANS = Long.MAX_VALUE;

    private long vlsn = INVALID_VLSN;
    private LogEntryType type;
    private boolean del;
    private long txnId;
    private byte[] key;
    private byte[] value;

    private PartitionId partitionId;
    private Exception exception;

    /**
     * Builds a data item for an exception
     *
     * @param exception exception
     */
    public DataItem(Exception exception) {
        this.exception = exception;
        vlsn = INVALID_VLSN;
        txnId = 0;
        type = null;
        key = null;
        value = null;
        del = false;

        /* unspecified partition id */
        partitionId = PartitionId.NULL_ID;
    }

    /**
     * Builds a data item for a put or delete with optional tombstone
     *
     * @param vlsn     vlsn of the operation
     * @param txnId    txn id
     * @param key      key to be written or deleted
     * @param value    value to write, including delete tombstone, or null if
     *                 delete without tombstone
     * @param del      true if delete, false otherwise
     */
    public DataItem(long vlsn,
                    long txnId,
                    byte[] key,
                    byte[] value,
                    boolean del) {
        this.vlsn = vlsn;
        this.txnId = txnId;
        this.key = key;
        this.value = value;
        this.del = del;
        type = null;
        exception = null;

        /* unspecified partition id */
        partitionId = PartitionId.NULL_ID;
    }

    /**
     * Builds a data item for a put
     *
     * @param vlsn     vlsn of the put operation
     * @param txnId    txn id
     * @param key      key to be written
     * @param value    value to write
     */
    public DataItem(long vlsn,
                    long txnId,
                    byte[] key,
                    byte[] value) {
        this(vlsn, txnId, key, value, false);
    }

    /**
     * Builds a data item for a deletion without a tombstone
     *
     * @param vlsn     vlsn of the deletion
     * @param txnId    txn id
     * @param key      key to be deleted
     */
    public DataItem(long vlsn,
                    long txnId,
                    byte[] key) {
        this(vlsn, txnId, key, null, true);
    }

    /**
     * Builds a data item for a TXN commit or abort
     *
     * @param vlsn     vlsn of the put operation
     * @param txnId    txn id
     * @param type     txn type
     */
    public DataItem(long vlsn, long txnId, LogEntryType type){
        this.vlsn = vlsn;
        this.txnId = txnId;
        this.type = type;
        this.key = null;
        this.value = null;
        del = false;
        exception = null;
        /* unspecified partition id */
        partitionId = PartitionId.NULL_ID;
    }

    /**
     * Sets partition id 
     * 
     * @param pid id of partition 
     */
    public void setPartitionId(PartitionId pid) {
        partitionId = pid;
    }

    /**
     * Gets partition id
     * 
     * @return id of partition
     */
    public PartitionId getPartitionId() {
        return partitionId;
    }

    /**
     * Checks if this item is an exception
     *
     * @return true if this item is an exception
     */
    public boolean isException() {
        return (exception != null);
    }

    /**
     * Checks if this item is a delete operation
     *
     * @return true if the item is a deletion
     */
    public boolean isDelete() {
        return del;
    }

    /**
     * Gets the VLSN associated with the item
     *
     * @return VLSN of the item
     */
    public long getVLSN(){
        return vlsn;
    }

    /**
     * Gets the TXN id associated with the item if it is transactional
     *
     * @return txn id of the item
     */
    public long getTxnID() {
        return txnId;
    }

    /**
     * Gets the key in the item if exists
     *
     * @return key as byte array, null if does not exist
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Gets the value in the item if exists
     *
     * @return value as byte array, null if does not exist
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Checks if the item represent a txn commit
     *
     * @return true if it is a commit
     */
    public boolean isTxnCommit() {
        /* no type if not a commit or abort */
        return LOG_TXN_COMMIT.equals(type);
    }

    /**
     * Checks if the item represent a txn abort
     *
     * @return true if it is an abort
     */
    public boolean isTxnAbort() {
        /* no type if not a commit or abort */
        return LOG_TXN_ABORT.equals(type);
    }

    /**
     * Checks if the item is a COPY from partition transfer which has no txn id
     *
     * @return true if the item is a COPY from partition transfer
     */
    public boolean isCopyInPartTrans() {
        return (partitionId != PartitionId.NULL_ID &&
                txnId == TXN_ID_COPY_IN_PARTTRANS);
    }

    @Override
    public String toString() {
        StringBuilder msg  = new StringBuilder();

        msg.append("vlsn: ").append(vlsn)
           .append(", type: ").append(type)
           .append(", txn id: ").append(txnId).append(", ");

        if (isTxnCommit()) {
            msg.append("transaction commit");
        } else if (isTxnAbort()) {
            msg.append("transaction abort");
        } else if (isDelete()) {
            msg.append("del op on key:").append(Arrays.toString(getKey()));
        } else {
            msg.append("put op on key: ").append(Arrays.toString(getKey()))
               .append(", value size in bytes: ").append(value.length);
        }
        msg.append("\n");

        return msg.toString();
    }

}
