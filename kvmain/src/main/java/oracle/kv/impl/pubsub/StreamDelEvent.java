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
import oracle.kv.table.PrimaryKey;

/**
 * Object represents a delete operation in NoSQL Stream
 */
public class StreamDelEvent implements StreamOperation.DeleteEvent {

    private final PrimaryKey key;

    private final SequenceId sequenceId;

    private final int repGroupId;

    /**
     * Constructs a delete operation
     *
     * @param key         primary key of the deleted row
     * @param sequenceId  unique sequence id
     * @param repGroupId  shard id of the deletion
     */
    protected StreamDelEvent(PrimaryKey key,
                             SequenceId sequenceId,
                             int repGroupId) {
        this.key = key;
        this.sequenceId = sequenceId;
        this.repGroupId = repGroupId;
    }

    /**
     * Returns the key associated with the delete operation.
     *
     * @return  the key associated with the delete operation.
     */
    @Override
    public PrimaryKey getPrimaryKey() {
        return key;
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
     * Returns {@link oracle.kv.pubsub.StreamOperation.Type#DELETE}.
     */
    @Override
    public Type getType() {
        return Type.DELETE;
    }

    /**
     * Throws IllegalArgumentException.
     */
    @Override
    public PutEvent asPut() {
        throw new IllegalArgumentException("This operation is not a put");
    }

    /**
     * Returns this operation.
     */
    @Override
    public DeleteEvent asDelete() {
        return this;
    }

    @Override
    public String toString() {
        return "Del OP [seq=" + ((StreamSequenceId) sequenceId).getSequence() +
               ", shard id=" + repGroupId +
               ", primary key=" +
               UserDataControl.displayPrimaryKeyJson(key) + "]";
    }

    /**
     * Returns the table id of this operation.
     *
     * @return the table id of this operation.
     */
    @Override
    public long getTableId() {
        return ((TableImpl) key.getTable()).getId();
    }

    /**
     * Returns the full table name of this operation.
     *
     * @return the full table name of this operation.
     */
    @Override
    public String getFullTableName() {
        return key.getTable().getFullName();
    }

    /**
     * Returns the table name of this operation.
     *
     * @return the table name of this operation.
     */
    @Override
    public String getTableName() {
        return key.getTable().getName();
    }

    /**
     * Returns the region id of this operation.
     *
     * @return the region id of this operation.
     */
    @Override
    public int getRegionId() {
        return ((RowImpl) key).getRegionId();
    }


    /**
     * Returns the last modification time of this operation.
     *
     * @return the last modification time of this operation.
     */
    @Override
    public long getLastModificationTime() {
        return key.getLastModificationTime();
    }

    /**
     * Returns the expiration time of this operation.
     *
     * @return the expiration time of this operation.
     */
    @Override
    public long getExpirationTime() {
        return key.getExpirationTime();
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
     * Returns an estimated storage size of the primary key in the delete
     * operation
     */
    @Override
    public long getPrimaryKeySize() {
        return ((RowImpl) key).getStorageSize();
    }

    /**
     * Returns the JSON string of this operation.
     *
     * @return JSON string of this operation.
     */
    @Override
    public String toJsonString() {
        return "[type="  + getType() + "]" +
               "[seq=" + sequenceId + "]" +
               "[shard=" + repGroupId + "]" +
               "[region id=" + getRegionId() + "]" +
               "[table=" + getFullTableName() + "]" +
               key.toJsonString(false);
    }
}
