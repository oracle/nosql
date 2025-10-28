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

import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordDef;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

/**
 * RowImpl is a specialization of RecordValue to represent a single record,
 * or row, in a table.  It is a central object used in most table operations
 * defined in {@link TableAPI}.
 *<p>
 * Row objects are constructed by
 * {@link Table#createRow createRow} or implicitly when returned from table
 * operations.
 */
public class RowImpl extends RecordValueImpl implements Row, RowSerializer {

    private static final long serialVersionUID = 1L;

    /* no longer final to allow it to be set by the MRTSubscriber */
    protected TableImpl table;

    private Version version;

    /*
     * The table version that this row conforms to.
     */
    private int tableVersion;

    /*
     * A special expiration time of -1 is used to indicate that this value
     * is not valid. This is internal use only and never returned to users.
     */
    private long expirationTime;

    private TimeToLive ttl;

    private String rowMetadata;

    /**
     * Last update timestamp in ms.
     */
    private long lastUpdateTimeMs;

    /**
     * Creation timestamp in ms.
     */
    private long creationTimeMs;

    private int storageSize = -1;

    private int partition = -1;

    private int shard = -1;

    /** ID of the region where row was originally modified */
    private int regionId = Region.NULL_REGION_ID;

    /** true if this row represents a tombstone, false otherwise */
    private boolean isTombstone = false;

    RowImpl(RecordDef field, TableImpl table) {
        super(field);
        assert field != null && table != null;
        this.table = table;
        version = null;
        tableVersion = 0;
        expirationTime = -1;
    }

    protected RowImpl(RowImpl other) {
        super(other);
        this.table = other.table;
        this.version = other.version;
        this.tableVersion = other.tableVersion;
        this.expirationTime = other.expirationTime;
        this.ttl = other.ttl;
        this.lastUpdateTimeMs = other.lastUpdateTimeMs;
        this.creationTimeMs = other.creationTimeMs;
        this.regionId = other.regionId;
        this.storageSize = other.storageSize;
        this.partition = other.partition;
        this.shard = other.shard;
        this.isTombstone = other.isTombstone;
        this.rowMetadata = other.rowMetadata;
    }

    /*
     * Only used by MRTSubscriber when setting the target table for a row
     * in a JSON collection table in order to get the table id correct
     * for the put resolve call
     */
    public void setTable(TableImpl t) {
        table = t;
    }

    /**
     * Return the Table associated with this row.
     */
    @Override
    public TableImpl getTable() {
        return table;
    }

    @Override
    public boolean isRow() {
        return true;
    }

    @Override
    public Row asRow() {
        return this;
    }

    @Override
    public Version getVersion() {
        return version;
    }

    @Override
    public int getTableVersion() {
        return tableVersion;
    }

    void setTableVersion(final int tableVersion) {
        this.tableVersion = tableVersion;
    }

    /*
     * Note: The method is public because it is also called from query code.
     */
    public void setVersion(Version version) {
        this.version = version;
    }

    @Override
    public RowImpl clone() {
        return new RowImpl(this);
    }

    @Override
    public PrimaryKey createPrimaryKey() {
        return getTableImpl().createPrimaryKey(this);
    }

    public void setPartition(int p) {
        partition = p;
    }

    public int getPartition() {
        if (partition < 0) {
            throw new IllegalStateException(
                "Access to uninitialized partition id");
        }
        return partition;
    }

    public void setShard(int shard) {
        this.shard = shard;
    }

    public int getShard() {
        if (shard < 0) {
            throw new IllegalStateException("Access to uninitialized storage");
        }
        return shard;
    }

    public int getDataSize() {
        return table.getDataSize(this);
    }

    public int getKeySize() {
        return table.getKeySize(this);
    }

    public void setStorageSize(int storageSize) {
        this.storageSize = storageSize;
    }

    public int getStorageSize() {
        if (storageSize < 0) {
            throw new IllegalStateException("Access to uninitialized storage");
        }
        return storageSize;
    }

    /**
     * Return the KVStore Key for the actual key/value record that this
     * object references.
     *
     * @param allowPartial set true if the primary key need not be complete.
     * This is the case for multi- and iterator-based methods.
     */
    public Key getPrimaryKey(boolean allowPartial) {
        return table.createKey(this, allowPartial);
    }

    /* public for query processor */
    public TableImpl getTableImpl() {
        return table;
    }

    /**
     * Update a Row based on the ValueVersion returned from the store.
     *
     * Make a copy of this object to hold the data.  It is assumed
     * that "this" holds the primary key.
     *
     * @param keyOnly set to true if fields that are not part of the
     * primary key need to be removed before proceeding.
     *
     * @return true if the operation is successful.
     *
     * public for access from api/ops
     */
    public boolean rowFromValueVersion(ValueVersion vv, boolean keyOnly) {

        if (keyOnly) {
            removeValueFields();
        }
        return table.rowFromValueVersion(vv, this);
    }

    /**
     * Copy the primary key fields from row to this object. Because the RowImpl may
     * be a PrimaryKeyImpl, with a different schema, don't use the positions of
     * primary key fields to copy, use the field names.
     */
    void copyKeyFields(RowImpl row) {
        for (String keyField : table.getPrimaryKeyInternal()) {
            FieldValue val = row.get(keyField);
            if (val != null) {
                put(keyField, val);
            }
        }
    }

    /**
     * Removes values that are not part of the primary key
     */
    public void clearNonKeyValues() {
        for (int i = 0; i < getNumFields(); ++i) {
            if (!table.isPrimKeyAtPos(i)) {
                removeInternal(i);
            }
        }
    }

    /**
     * Create a Value from a Row
     */
    public Value createValue() {
        return table.createValue(this);
    }

    /**
     * Compares two Rows taking into consideration that they may come from
     * different tables, which can happen when accessing ancestor and/or child
     * tables in the same scan.  In this case the ancestor should come first,
     * which can be determined by the table ID.  Ancestor table IDs are always
     * smaller than child table IDs.
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof RowImpl) {
            RowImpl otherImpl = (RowImpl) other;
            if (table.getId() == otherImpl.table.getId()) {
                return super.compareTo(otherImpl);
            }
            return compare(table.getId(), otherImpl.table.getId());
        }
        throw new IllegalArgumentException
            ("Cannot compare Row to another type");
    }

    /**
     * Compares the primary key fields of two Rows based on compareTo, returning
     * -1, 0, or 1 depending on whether the primary key of this row is less than,
     * equal to, or greater than the other row.
     *
     * The schema for both rows must be the same. No check is done but in the path
     * this method is used that is a safe assumption.
     */
    public int compareKeys(Row other) {

        for (int i : table.getPrimKeyPositions()) {
            int val = get(i).compareTo(other.get(i));
            if (val != 0) {
                return val;
            }
        }
        return 0;
    }

    /* TODO: Replace with Java 7 Long.compareTo */
    private static int compare(long x, long y) {
        return (x < y) ? -1 : (x == y) ? 0 : 1;
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other)) {
            if (other instanceof RowImpl) {
                if (isTombstone == ((RowImpl) other).isTombstone) {
                    return table.nameEquals(((RowImpl) other).table);
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + table.getFullName().hashCode() +
               Boolean.hashCode(isTombstone);
    }

    /*
     * At this time it's difficult, but not imppossible for an application to
     * create a TimeToLive with a negative duration, so check that. It is
     * using the TimeToLive.fromExpirationTime() interface.
     */
    @Override
    public void setTTL(TimeToLive ttl) {
        if (ttl != null && ttl.getValue() < 0) {
            throw new IllegalArgumentException(
                "Row.setTTL() does not support negative time periods");
        }
        this.ttl = ttl;
    }

    @Override
    public TimeToLive getTTL() {
        return ttl;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    /**
     * Sets expiration time on output. This is used internally by methods that
     * create Row instances retrieved from the server. This method is not used
     * for input. setTTL() is used by users to set a time to live value for a
     * row.
     */
    public void setExpirationTime(long t) {
        expirationTime = t;
    }

    /*
     * This is used by internal methods to return the actual TTL value, if
     * set. It has the side effect of clearing expiration time.  Expiration
     * time must be cleared because this method is called on put operations and
     * the expiration time is set as a side effect, so any existing expiration
     * time must be removed.
     */
    TimeToLive getTTLAndClearExpiration() {
        TimeToLive retVal = ttl;
        expirationTime = -1;
        return retVal;
    }

    /**
     * @see Row#setRowMetadata(String)
     */
    @Override
    public void setRowMetadata(String rowMetadata) {
        // we don't validate it is a valid JSON Object here, validation is done
        // when Value instance in created.
        this.rowMetadata = rowMetadata;
    }

    /**
     * @see Row#getRowMetadata()
     */
    @Override
    public String getRowMetadata() {
        return rowMetadata;
    }

    public void removeValueFields() {
        if (table.hasValueFields()) {
            /* remove non-key fields if present */
            for (int i = 0; i < getNumFields(); ++i) {
                if (!table.isPrimKeyAtPos(i)) {
                    removeInternal(i);
                }
            }
        }
    }

    @Override
    public String getClassNameForError() {
        return "Row";
    }

    ValueReader<RowImpl> initRowReader() {
        return table.initRowReader(this);
    }

    @Override
    public void validateKey(TableKey key) {
        /* do nothing */
    }

    @Override
    public void validateValue(Value value) {
        /* do nothing */
    }

    @Override
    public boolean isFromMRTable() {
        return table.isMultiRegion();
    }

    @Override
    public long getLastModificationTime() {
        return lastUpdateTimeMs;
    }

    /**
     * Set the timestamp for the last update on the row.
     */
    public void setModificationTime(long modificationTime) {
        this.lastUpdateTimeMs = modificationTime;
    }

    @Override
    public long getCreationTime() {
        return creationTimeMs;
    }

    /**
     * Set the creation timestamp for the row.
     */
    public void setCreationTime(long creationTime) {
        this.creationTimeMs = creationTime;
    }

    /**
     * Returns the region id associated with the row. The region id indicates
     * the region where the row is originally persisted.
     *
     * @return the region id associated with the row
     */
    @Override
    public int getRegionId() {
        return regionId;
    }

    /**
     * Note that this method is only used to set the external region id when
     * we are deserializing value bytes into a Row.
     *
     * Sets the region id associated with the row. The region id indicates
     * the region where the row is originally persisted.
     *
     * @param regionId id of the region
     */
    public void setRegionId(int regionId) {
        this.regionId = regionId;
    }

    public boolean isTombstone() {
        return isTombstone;
    }

    public void setTombstone(boolean tb) {
        isTombstone = tb;
    }
}
