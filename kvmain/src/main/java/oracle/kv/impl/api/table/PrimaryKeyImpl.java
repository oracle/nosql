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

package oracle.kv.impl.api.table;

import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.api.KVStoreImpl;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;

/**
 * The RecordDef associated with a PrimaryKey is the RecordDef of the
 * associated table (so it includes non pk fields as well). However,
 * PrimaryKey redefines methods like getFields(), getFieldMapEntry(), etc.
 * so that it hides the non-pk fields.
 */
public class PrimaryKeyImpl extends RowImpl implements PrimaryKey {

    private static final long serialVersionUID = 1L;

    /*
     * Used internally by BulkPut only. Set this to true to indicate
     * that this row to be deleted. This distinguishes it from a real
     * put to a key-only table
     */
    private transient boolean isTombstone;

    PrimaryKeyImpl(RecordDef field, TableImpl table) {
        super(field, table);
    }

    private PrimaryKeyImpl(PrimaryKeyImpl other) {
        super(other);
    }

    @Override
    public PrimaryKeyImpl clone() {
        return new PrimaryKeyImpl(this);
    }

    @Override
    public PrimaryKey asPrimaryKey() {
        return this;
    }

    @Override
    public boolean isPrimaryKey() {
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PrimaryKeyImpl) {
            return super.equals(other);
        }
        return false;
    }

    /**
     * Overrides for RecordValueImpl
     */

    /*
     * This is overridden in order to validate the value based on
     * a size constraint if it exists. There is only one type (Integer)
     * that can be constrained so rather than create a method on
     * FieldValueImpl to override, call IntegerValueImpl directly. If
     * this ever changes the validation method can be made part of the
     * interface.
     */
    @Override
    public RecordValue put(int pos, FieldValue value) {

        if (value.isNull()) {
            throw new IllegalArgumentException(
                "Can not put a NULL value in a primary key field");
        }

        super.put(pos, value);

        if (value.isInteger()) {
            int size = table.getPrimaryKeySize(pos);
            if (size != 0) {
                ((IntegerValueImpl) value).validateStorageSize(size);
            }
        }

        return this;
    }

    @Override
    public RecordValue put(int pos, int value) {
        validateValueKind(pos, FieldDef.Type.INTEGER);
        return put(pos, getFieldDef(pos).createInteger(value));
    }

    @Override
    public RecordValue put(String name, int value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public int getDataSize() {
        throw new IllegalArgumentException
            ("It is not possible to get data size from a PrimaryKey");
    }

    /**
     * Validate the primary key.  Rules:
     *
     * Fields must be specified in order.  If a field "to the right"
     * in the index definition is set, all fields to its "left" must also
     * be present.
     */
    @Override
    public void validate() {
        validateIndexFields();
    }

    /**
     * Override RecordValueImpl's implementation by specifying that
     * the schemas do not need to match. This is the case when
     * copying from a non-key Row.
     */
    @Override
    public void copyFrom(RecordValue source) {
        copyFrom(source, true);
    }

    public boolean isComplete() {
        return getNumFields() == size();
    }

    /*
     * This method works correctly only if the PrimaryKey has been validated
     * to make sure that there are no "gaps" in the key values set already.
     * No validation is needed if the (internal) caller builds the key
     * correctly (with no gaps), as is the case (for example) with the
     * OptRulePushIndexPreds class in the query compiler.
     */
    public boolean hasShardKey() {
        return table.getShardKeySize() <= size();
    }

    /**
     * Creates a byte[] representation of the key. This may be
     * partial.
     */
    public byte[] createKeyBytes() {
        return TableKey.createKey(getTable(), this, true).getKeyBytes();
    }

    /**
     * If this PrimakyKey contains a complete shard key, get the associated
     * partition id. Otherwise return null.
     */
    public PartitionId getPartitionId(KVStoreImpl store) {

        if (!hasShardKey()) {
            return null;
        }

        TableKey key = TableKey.createKey(table, this, true/*allowPartial*/);

        byte[] binaryKey = store.getKeySerializer().toByteArray(key.getKey());
        return  store.getDispatcher().getPartitionId(binaryKey);
    }

    @Override
    public String getClassNameForError() {
        return "PrimaryKey";
    }

    /**
     * Returns the string representing the shard key portion of this
     * object. It relies on the ordering of the fields in the primary key.
     * TODO: consider making this part of the public PrimaryKey interface.
     */
    public String toShardKey() {
        StringBuilder sb = new StringBuilder(128);
        int numFields = size();
        if (table.getShardKeySize() < numFields) {
            numFields = table.getShardKeySize();
        }
        toStringBuilder(sb, new DisplayFormatter(false), numFields);
        return sb.toString();
    }

    /**
     * Returns true if the primary key contains all of the shard key fields.
     */
    public boolean isCompleteShardKey() {
        for (String field : table.getShardKey()) {
            FieldValue fv = get(field);
            if (fv == null || fv.isNull()) {
                return false;
            }
        }
        return true;
    }

    /*
     * Set this key as a tombstone, to be deleted when encountered
     * in a BulkPut stream. It is ignored in all other paths
     */
    public PrimaryKeyImpl setIsTombstone(boolean value) {
        isTombstone = value;
        return this;
    }

    /*
     * Is this a tombstone?
     */
    public boolean getIsTombstone() {
        return isTombstone;
    }
}
