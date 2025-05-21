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

import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.table.Row;

/**
 * Object represents a put operation in NoSQL Stream
 */
public class StreamPutEvent implements StreamOperation.PutEvent {

    private final SequenceId sequenceId;

    private final int repGroupId;

    private final Row row;

    /**
     * Constructs a put operation
     *
     * @param row         row of put
     * @param sequenceId  unique sequence id
     * @param repGroupId  shard id of the deletion
     */
    protected StreamPutEvent(Row row,
                             SequenceId sequenceId,
                             int repGroupId) {
        this.row = row;
        this.sequenceId = sequenceId;
        this.repGroupId = repGroupId;
    }

    /**
     * Returns the row associated with the put operation.
     *
     * @return row associated with the put operation.
     */
    @Override
    public Row getRow() {
        return row;
    }

    /**
     * Returns the unique sequence id associated with this operation.
     *
     * @return the unique sequence id associated with this operation.
     */
    @Override
    public SequenceId getSequenceId() {
        return sequenceId;
    }

    /**
     * Returns the shard id of this operation.
     *
     * @return the shard id of this operation.
     */
    @Override
    public int getRepGroupId() {
        return repGroupId;
    }

    /**
     * Returns {@link oracle.kv.pubsub.StreamOperation.Type#PUT}.
     */
    @Override
    public Type getType() {
        return Type.PUT;
    }

    /**
     * Returns this operation.
     */
    @Override
    public PutEvent asPut() {
        return this;
    }

    /**
     * Throws IllegalArgumentException.
     */
    @Override
    public DeleteEvent asDelete() {
        throw new IllegalArgumentException("This operation is not a delete");
    }

    @Override
    public String toString() {
        return "PUT OP [seq=" + ((StreamSequenceId)sequenceId).getSequence() +
               ", shard id=" + repGroupId +
               ", row=" + UserDataControl.displayRowJson(row) + "]";
    }

    /**
     * Returns the table id of this operation.
     *
     * @return the table id of this operation.
     */
    @Override
    public long getTableId() {
        return ((TableImpl) row.getTable()).getId();
    }

    /**
     * Returns the full table name of this operation.
     *
     * @return the full table name of this operation.
     */
    @Override
    public String getFullTableName() {
        return row.getTable().getFullName();
    }

    /**
     * Returns the table name of this operation.
     *
     * @return the table name of this operation.
     */
    @Override
    public String getTableName() {
        return row.getTable().getName();
    }

    /**
     * Returns the region id of this operation.
     *
     * @return the region id of this operation.
     */
    @Override
    public int getRegionId() {
        return ((RowImpl) row).getRegionId();
    }

    /**
     * Returns the last modification time of this operation.
     *
     * @return the last modification time of this operation.
     */
    @Override
    public long getLastModificationTime() {
        return row.getLastModificationTime();
    }

    /**
     * Returns the expiration time of this operation.
     *
     * @return the expiration time of this operation.
     */
    @Override
    public long getExpirationTime() {
        return row.getExpirationTime();
    }

    /**
     * @hidden
     *
     * Throws IllegalArgumentException.
     */
    @Override
    public byte[] getPrimaryKeyBytes() {
        throw new IllegalArgumentException("Primary key in bytes unavailable");
    }

    /**
     * @hidden
     *
     * Throws IllegalArgumentException.
     */
    @Override
    public byte[] getRowBytes() {
        throw new IllegalArgumentException("Row in bytes unavailable");
    }

    @Override
    public long getRowSize() {
        return ((RowImpl) row).getStorageSize();
    }

    /**
     * Returns the JSON string of this operation.
     *
     * @return JSON string of this operation.
     */
    @Override
    public String toJsonString() {
        return row.toJsonString(true);
    }
}
