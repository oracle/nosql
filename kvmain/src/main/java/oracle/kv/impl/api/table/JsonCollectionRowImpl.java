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

import static oracle.kv.impl.api.table.FieldDefImpl.Constants.jsonDef;

import java.math.BigDecimal;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;

import com.fasterxml.jackson.core.io.CharTypes;

/**
 * JsonCollectionRowImpl is a specialization of RowImpl for a jsonCollection
 * table that has only primary key fields defined in the underlying RecordDef.
 *
 * It adds a map of arbitrary JSON fields and overrides put/get methods that
 * would otherwise fail using RecordValueImpl because a field doesn't match the
 * schema.
 */
public class JsonCollectionRowImpl extends RowImpl {

    private static final long serialVersionUID = 1L;

    private final JsonRowMap map = new JsonRowMap();

    JsonCollectionRowImpl(RecordDef field, TableImpl table) {
        super(field, table);
    }

    protected JsonCollectionRowImpl(JsonCollectionRowImpl other) {
        super(other);
        /* copy entries */
        for (Map.Entry<String, FieldValue> entry :
                 other.getJsonCollectionMap().getFieldsInternal().entrySet()) {
            map.put(entry.getKey(), entry.getValue().clone());
        }
        map.indexStorageSize = other.map.indexStorageSize;
    }

    public JsonRowMap getJsonCollectionMap() {
        return map;
    }

    public JsonRowMap copyJsonCollectionMap() {
        JsonRowMap newMap = new JsonRowMap();
        newMap.setIndexStorageSize(map.getIndexStorageSize());
        for (Map.Entry<String, FieldValue> entry : map.getMap().entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }
        return newMap;
    }

    @Override
    public void copyFrom(RecordValue source) {
        if (!(source instanceof JsonCollectionRowImpl)) {
            throw new IllegalArgumentException(
                "copyFrom on JSON collection requires JSON collection source");
        }
        super.copyFrom(source, true);
        JsonCollectionRowImpl other = (JsonCollectionRowImpl) source;

        for (Map.Entry<String, FieldValue> entry :
                 other.getJsonCollectionMap().getFieldsInternal().entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /*
     * Clear only the map fields, leaving keys intact
     */
    @Override
    public void clearNonKeyValues() {
        map.clearMap();
    }

    @Override
    public FieldValueImpl get(String fname) {
        if (table.isKeyComponent(fname)) {
            return super.get(fname);
        }
        return map.get(fname);
    }

    @Override
    public RecordValue put(String name, FieldValue value) {
        if (table.isKeyComponent(name)) {
            /* a primary key field */
            super.put(name, value);
        } else {
            map.put(name, value);
        }
        return this;
    }

    @Override
    public void putInternal(String name, FieldValue value,
                            boolean fromUser) {
        if (table.isKeyComponent(name)) {
            super.putInternal(name, value, fromUser);
        } else {
            map.put(name, value);
        }
    }

    @Override
    public RecordValue put(String name, int value) {
        put(name, jsonDef.createInteger(value));
        return this;
    }

    @Override
    public RecordValue put(String name, long value) {
        put(name, jsonDef.createLong(value));
        return this;
    }

    @Override
    public RecordValue put(String name, String value) {
        put(name, jsonDef.createString(value));
        return this;
    }

    @Override
    public RecordValue put(String name, double value) {
        put(name, jsonDef.createDouble(value));
        return this;
    }

    @Override
    public RecordValue putJson(String name, String jsonInput) {
        if (table.isKeyComponent(name)) {
            throw new IllegalArgumentException(
                "Cannot use putJson on a primary key field");
        }
        /*
         * JSON cannot be part of the primary key so go straight to the map
         */
        map.putJson(name, jsonInput);
        return this;
    }

    @Override
    public RecordValue putJson(String name, Reader jsonReader) {
        if (table.isKeyComponent(name)) {
            throw new IllegalArgumentException(
                "Cannot use putJson on a primary key field");
        }
        /*
         * JSON cannot be part of the primary key so go straight to the map
         */
        map.putJson(name, jsonReader);
        return this;
    }

    @Override
    public RecordValue put(String name, float value) {
        put(name, jsonDef.createFloat(value));
        return this;
    }

    @Override
    public RecordValue put(String name, boolean value) {
        put(name, jsonDef.createBoolean(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, int value) {
        put(name, jsonDef.createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, long value) {
        put(name, jsonDef.createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, float value) {
        put(name, jsonDef.createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, double value) {
        put(name, jsonDef.createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, BigDecimal value) {
        put(name, jsonDef.createNumber(value));
        return this;
    }

    @Override
    public RecordValue put(String name, byte[] value) {
        throw new IllegalArgumentException(
            "Cannot insert a field of type byte[] into a JSON " +
            "collection");
    }

    @Override
    public RecordValue putFixed(String name, byte[] value) {
        throw new IllegalArgumentException(
            "Cannot insert a field of type byte[] into a JSON " +
            "collection");
    }

    @Override
    public RecordValue putEnum(String name, String value) {
        throw new IllegalArgumentException(
            "Cannot insert a field of type enum into a JSON " +
            "collection");
    }

    @Override
    public RecordValue putNull(String name) {
        put(name, jsonDef.createJsonNull());
        return this;
    }

    @Override
    public RecordValue put(String name, Timestamp value) {
        throw new IllegalArgumentException(
            "Cannot insert a field of type Timestamp into a JSON " +
            "collection");
    }

    /*
     * NOTE: this override is a bit odd but it turns out to be necessary
     * for when the proxy serializes results of select * queries that
     * return RecordValue instances. The proxy code calls this method
     * to get declaration order for tables with schema. In this case
     * the order isn't important but all values must be handled.
     */
    @Override
    public List<String> getFieldNames() {
        ArrayList<String> names =
            new ArrayList<String>(getNumFields() + map.size());
        names.addAll(getFieldNamesInternal());
        names.addAll(map.getMap().keySet());
        return names;
    }

    /*
     * Override this method from RecordValueImpl to provide a better
     * error
     */
    @Override
    public FieldDefImpl getFieldDef(int pos) {
        RecordDefImpl def = getDefinition();
        if (pos > (def.getNumFields() - 1)) {
            throw new IndexOutOfBoundsException(
                "Field position (" + pos + ") is out of range for " +
                "JSON Collection table that has only " + def.getNumFields() +
                " key field" + (def.getNumFields() == 1 ? "" : "s"));
        }
        return def.getFieldDef(pos);
    }

    @Override
    public int size() {
        return super.size() + map.size();
    }

    @Override
    public boolean contains(String fieldName) {
        if (table.isKeyComponent(fieldName)) {
            /* a primary key field */
            return super.contains(fieldName);
        }
        return map.contains(fieldName);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + map.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }

        if (!(other instanceof JsonCollectionRowImpl)) {
            return false;
        }

        return map.equals(((JsonCollectionRowImpl)other).map);
    }


    @Override
    public int compareTo(FieldValue other) {

        if (!(other instanceof JsonCollectionRowImpl)) {
            throw new ClassCastException(
                "Object is not an JsonCollectionRowImpl");
        }

        int val = super.compareTo(other);
        if (val != 0) {
            return val;
        }
        return map.compareTo(((JsonCollectionRowImpl)other).map);
    }

    @Override
    public RowImpl clone() {
        return new JsonCollectionRowImpl(this);
    }

    @Override
    protected void toStringBuilder(StringBuilder sb,
                                   DisplayFormatter formatter,
                                   int numFields) {
        if (formatter == null) {
            throw new IllegalArgumentException(
                "DisplayFormatter must be non-null");
        }

        formatter.startObject();

        boolean wroteFirstField = false;
        sb.append('{');
        for (int i = 0; i < numFields; ++i) {
            String fieldName = getFieldName(i);
            FieldValueImpl val = get(i);
            if (val != null) {
                formatter.newPair(sb, wroteFirstField);
                sb.append('\"');
                sb.append(fieldName);
                sb.append('\"');
                formatter.separator(sb);
                val.toStringBuilder(sb, formatter);
                wroteFirstField = true;
            }
        }
        for (Map.Entry<String, FieldValue> entry : map.getMap().entrySet()) {
            String key = entry.getKey().toString();
            FieldValueImpl val = (FieldValueImpl)entry.getValue();
            if (val != null) {
                formatter.newPair(sb, true);
                sb.append('\"');
                CharTypes.appendQuoted(sb, key);
                sb.append('\"');
                formatter.separator(sb);
                val.toStringBuilder(sb, formatter);
            }
        }
        formatter.endObject(sb, numFields);
        sb.append('}');
    }

    /*
     * If all primary key fields are not set, throw an exception
     */
    void ensurePrimaryKey() {
        for (String key : table.getPrimaryKeyInternal()) {
            if (get(key) == null) {
                throw new IllegalArgumentException(
                    "Missing primary key field: " + key);
            }
        }
    }

    /*
     * Add fields for the primary key values and the row-property values.
     *
     * NOTE: this is only used by query via ServerTableIter
     */
    public void addPrimKeyAndPropertyFields(int indexSize) {

        List<String> names = getFieldNamesInternal();
        for (int i = 0; i < super.size(); i++) {
            map.put(names.get(i), get(i));
        }

        map.setIndexStorageSize(indexSize);
    }

    public void addPrimKeyAndPropertyFields(
        long expTime,
        long modTime,
        int partitionId,
        int rowSize,
        int indexSize) {

        List<String> names = getFieldNamesInternal();
        for (int i = 0; i < super.size(); i++) {
            map.put(names.get(i), get(i));
        }

        map.addProperties(expTime, modTime, partitionId, rowSize, indexSize);
    }

    /**
     * An extention of MapValueImpl that includes row metadata for use by
     * queries that use row property functions. It is implemented as an
     * inner non-static class so that it can use the enclosing RowImpl to
     * store the properties
     */
    public class JsonRowMap extends MapValueImpl {
        private static final long serialVersionUID = 1L;

        private int indexStorageSize;

        private JsonRowMap() {
            super(FieldDefImpl.Constants.mapJsonDef);
        }

        public JsonCollectionRowImpl getJColl() {
            return JsonCollectionRowImpl.this;
        }

        private void addProperties(long expTime,
                                   long modTime,
                                   int partitionId,
                                   int rowSize,
                                   int indexSize) {
            JsonCollectionRowImpl row = getJColl();
            row.setExpirationTime(expTime);
            row.setModificationTime(modTime);
            row.setPartition(partitionId);
            row.setStorageSize(rowSize);
            this.indexStorageSize = indexSize;
        }

        private void setIndexStorageSize(int indexSize) {
            this.indexStorageSize = indexSize;
        }

        @Override
        public boolean isJsonRowMap() {
            return true;
        }

        @Override
        public JsonCollectionRowImpl.JsonRowMap asJsonRowMap() {
            return this;
        }

        public long getModificationTime() {
            return getJColl().getLastModificationTime();
        }

        public long getExpirationTime() {
            return getJColl().getExpirationTime();
        }

        public int getPartitionId() {
            return getJColl().getPartition();
        }

        public int getShardId() {
            return getJColl().getShard();
        }

        public int getStorageSize() {
            return getJColl().getStorageSize();
        }

        public int getIndexStorageSize() {
            return indexStorageSize;
        }

        public String getTableName() {
            return getJColl().getTable().getFullName();
        }

        public String getTableNamespace() {
            return getJColl().getTable().getInternalNamespace();
        }

        private boolean contains(String fieldName) {
            return getMap().containsKey(fieldName);
        }
    }
}
