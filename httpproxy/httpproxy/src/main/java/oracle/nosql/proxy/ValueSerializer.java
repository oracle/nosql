/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

import static oracle.nosql.proxy.ProxySerialization.assertType;
import static oracle.nosql.proxy.ProxySerialization.raiseBadProtocolError;
import static oracle.nosql.proxy.ProxySerialization.read4BytesLength;
import static oracle.nosql.proxy.ProxySerialization.read4BytesSize;
import static oracle.nosql.proxy.ProxySerialization.readNonNullEmptyString;
import static oracle.nosql.proxy.ProxySerialization.readNonNullString;
import static oracle.nosql.proxy.ProxySerialization.readValueType;
import static oracle.nosql.proxy.ProxySerialization.skipValue;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_ARRAY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BINARY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_DOUBLE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_EMPTY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_JSON_NULL;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_LONG;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_NULL;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_NUMBER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_STRING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_TIMESTAMP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.checkKeySize;
import static oracle.nosql.proxy.protocol.BinaryProtocol.checkValueSize;
import static oracle.nosql.proxy.protocol.BinaryProtocol.getTypeName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;

import oracle.kv.Value;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FixedBinaryDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.api.table.ValueSerializer.ArrayValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MapValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RecordValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.MapDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;
import oracle.nosql.proxy.protocol.ByteInputStream;

/**
 * Implementation of ValueSerializer interfaces:
 *   RowSerializer
 *   RecordValueSerializer
 *   MapValueSerializer
 *   ArrayValueSerializer
 *   FieldValueSerializer
 *
 * TODO: UncheckIOException is thrown when caught IOException in some methods,
 * We may want to modify oracle.kv.impl.api.table.ValueSerializer.* interface
 * to throw IOException directly.
 */
public class ValueSerializer {

    public static class RowSerializerImpl extends RecordValueSerializerImpl
        implements RowSerializer {

        private final TableImpl table;
        private final boolean isPrimaryKey;
        private final int keySizeLimit;
        private final int valueSizeLimit;
        private TimeToLive ttl;

        public RowSerializerImpl(ByteInputStream in,
                                 int driverType,
                                 Table table,
                                 int keySizeLimit,
                                 int valueSizeLimit,
                                 boolean isPrimaryKey,
                                 boolean exact) {

            super((isPrimaryKey ? ((TableImpl)table).getPrimKeyDef() :
                                  ((TableImpl)table).getRowDef()),
                  in, driverType, exact);
            this.table = (TableImpl) table;
            this.isPrimaryKey = isPrimaryKey;
            this.keySizeLimit = keySizeLimit;
            this.valueSizeLimit = valueSizeLimit;
        }

        public void setTTL(TimeToLive ttl) {
            this.ttl = ttl;
        }

        @Override
        public TimeToLive getTTL() {
            return ttl;
        }

        @Override
        public Table getTable() {
            return table;
        }

        @Override
        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        @Override
        public String getClassNameForError() {
            return isPrimaryKey ? "PrimaryKey" : "Row";
        }

        @Override
        public FieldValueSerializer get(int pos) {
            FieldValueSerializer fval = super.get(pos);
            /*
             * If deserializing a primary key (e.g. for get/delete) it is ok
             * for the value to be set, even if it's an identity column
             */
            if (fval != null && !isPrimaryKey()) {
                if (table != null &&
                    table.hasIdentityColumn() &&
                    table.getIdentityColumn() == pos &&
                    table.isIdentityGeneratedAlways() ) {
                    throw new IllegalArgumentException("Value should not be " +
                        "set for a generated always identity column: " +
                        table.getFields().get(pos));
                }
            }
            return fval;
        }

        public int getKeySize() {
            if (isPrimaryKey) {
                return getDataSize();
            }
            int current = in.getOffset();
            int klen = 0;
            for (int pos : table.getPrimKeyPositions()) {
                if (values[pos] == 0) {
                    throw new IllegalArgumentException
                    ("Key field can not be null: " +
                        table.getPrimaryKeyColumnName(pos));
                }
                in.setOffset(values[pos]);
                try {
                    klen += skipValue(in);
                } catch (IOException e) {
                    throw new IllegalArgumentException
                    ("Read key field failed at the position: " + pos);
                }
            }
            in.setOffset(current);
            return klen;
        }

        @Override
        public void validateKey(TableKey key) {
            checkKeySize(key, keySizeLimit);
        }

        @Override
        public void validateValue(Value value) {
            checkValueSize(value, valueSizeLimit);
        }

        @Override
        public boolean isFromMRTable() {
            /* return false or the caller may cast this object to RowImpl */
            //return table.isMultiRegion();
            return false;
        }
    }

    public static class RecordValueSerializerImpl
        implements RecordValueSerializer {

        private final RecordDef recordDef;
        final ByteInputStream in;
        private final int driverType;
        private final int length;
        private final int size;
        private final int endOffset;
        private final boolean exact;
        final int[] values;

        public RecordValueSerializerImpl(RecordDef recordDef,
                                         ByteInputStream in,
                                         int driverType,
                                         boolean exact) {
            this(recordDef, in, driverType, in.getOffset(), exact);
        }

        public RecordValueSerializerImpl(RecordDef recordDef,
                                         ByteInputStream in,
                                         int driverType,
                                         int offset,
                                         boolean exact) {

            this.recordDef = recordDef;
            this.in = in;
            this.driverType = driverType;
            this.exact = exact;
            if (in.getOffset() != offset) {
                in.setOffset(offset);
            }

            values = new int[recordDef.getNumFields()];
            try {
                /* read length */
                length = read4BytesLength(in);
                endOffset = in.getOffset() + length;
                /* read size */
                size = read4BytesSize(in);
                initValues();
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read record value failed", ioe);
            }
        }

        private void initValues() throws IOException {
            /*
             * If exact is true and the driver value is array type, then the
             * number of array elements must equals to the number of fields in
             * target Record value.
             */
            if (exact && size != values.length && driverType == TYPE_ARRAY) {
                throw new IllegalArgumentException(
                    "Invalid Array value for Record Value, it has " +
                    size + (size > 1 ? " elements" : " element") +
                    " but the Record Value contains " + values.length +
                    (values.length > 1 ? " fields": " field"));
            }

            for (int i = 0; i < size; i++) {
                if (in.getOffset() >= endOffset) {
                    raiseBadProtocolError(
                        "Invalid Record Value, " +
                        "reaches the EOF of record value but still has " +
                        "element to read", in);
                }
                if (driverType == TYPE_MAP) {
                    String key = readNonNullEmptyString(in, "Field name");
                    int pos = 0;
                    try {
                        pos = getDefinition().getFieldPos(key);
                        values[pos] = in.getOffset();
                    } catch (IllegalArgumentException iae) {
                        if (exact) {
                            throw iae;
                        }
                        /* If exact = false, ignore unknown field */
                    }
                } else {
                    if (i < values.length) {
                        values[i] = in.getOffset();
                    }
                }
                skipValue(in);
            }
        }

        @Override
        public RecordDef getDefinition() {
            return recordDef;
        }

        @Override
        public FieldValueSerializer get(int pos) {
            int offset = values[pos];
            if (offset > 0) {
                return new FieldValueSerializerImpl
                    (getDefinition().getFieldDef(pos), in, offset, exact);

            }
            return null;
        }

        @Override
        public int size() {
            return size;
        }

        public int getDataSize() {
            return length;
        }
    }

    public static class MapValueSerializerImpl
        implements MapValueSerializer {

        private final MapDef mapDef;
        private final ByteInputStream in;
        private final int size;
        private final int endOffset;
        private final boolean exact;

        MapValueSerializerImpl(MapDef mapDef,
                               ByteInputStream in,
                               boolean exact) {
            this(mapDef, in, in.getOffset(), exact);
        }

        MapValueSerializerImpl(MapDef mapDef,
                               ByteInputStream in,
                               int offset,
                               boolean exact) {

            this.mapDef = mapDef;
            this.in = in;
            this.exact = exact;
            if (in.getOffset() != offset) {
                in.setOffset(offset);
            }

            try {
                /* read length */
                int length = read4BytesLength(in);
                endOffset = in.getOffset() + length;
                /* read size */
                size = read4BytesSize(in);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read map value failed", ioe);
            }
        }

        @Override
        public MapDef getDefinition() {
            return mapDef;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Iterator<Entry<String, FieldValueSerializer>> iterator() {
            return new Iterator<Entry<String, FieldValueSerializer>> () {

                private int index = 0;
                private int lastElemOffset = -1;

                @Override
                public boolean hasNext() {
                    boolean hasNext = (index < size);
                    if (lastElemOffset > 0) {
                        in.setOffset(lastElemOffset);
                        try {
                            skipValue(in);
                        } catch (IOException ioe) {
                            throw new UncheckedIOException(
                                "Skip Map element failed", ioe);
                        }
                    }
                    if (hasNext) {
                        if (in.getOffset() >= endOffset) {
                            throw new UncheckedIOException(
                                "Invalid Map value",
                                new IOException("Reaches the EOF of Map " +
                                    "value but still has element to read"));
                        }
                    } else  {
                        if (in.getOffset() < endOffset) {
                            throw new UncheckedIOException(
                                "Invalid Map value",
                                new IOException("No more element to read " +
                                    "but still not reaches the EOF of " +
                                    "MapValue"));
                        }
                    }
                    return hasNext;
                }

                @Override
                public Entry<String, FieldValueSerializer> next() {
                    if (!hasNext()) {
                        return null;
                    }

                    index++;

                    String key;
                    try {
                        key = readNonNullString(in, "Map key");
                    } catch (IOException ioe) {
                        throw new UncheckedIOException("Read key failed", ioe);
                    }

                    if (mapDef.getElement().isRecord()) {
                        lastElemOffset = in.getOffset();
                    }
                    FieldValueSerializer value =
                        new FieldValueSerializerImpl(mapDef.getElement(),
                                                     in, exact);
                    return new SimpleEntry<String, FieldValueSerializer>
                        (key, value);
                }
            };
        }
    }

    public static class ArrayValueSerializerImpl
        implements ArrayValueSerializer {

        private final ArrayDef arrayDef;
        private final ByteInputStream in;
        private final int size;
        private final int endOffset;
        private final boolean exact;

        ArrayValueSerializerImpl(ArrayDef arrayDef,
                                 ByteInputStream in,
                                 boolean exact) {
            this(arrayDef, in, in.getOffset(), exact);
        }

        ArrayValueSerializerImpl(ArrayDef arrayDef,
                                 ByteInputStream in,
                                 int offset,
                                 boolean exact) {

            this.arrayDef = arrayDef;
            this.in = in;
            this.exact = exact;
            if (in.getOffset() != offset) {
                in.setOffset(offset);
            }

            try {
                /* read length */
                int length = read4BytesLength(in);
                endOffset = in.getOffset() + length;
                /* read size */
                size = read4BytesSize(in);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read array value failed", ioe);
            }
        }

        @Override
        public ArrayDef getDefinition() {
            return arrayDef;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Iterator<FieldValueSerializer> iterator() {
            return new Iterator<FieldValueSerializer> () {

                private int index = 0;
                private int lastElemOffset = -1;

                @Override
                public boolean hasNext() {
                    boolean hasNext = (index < size);
                    if (lastElemOffset > 0) {
                        in.setOffset(lastElemOffset);
                        try {
                            skipValue(in);
                        } catch (IOException ioe) {
                            throw new UncheckedIOException(
                                "Skip Array element failed", ioe);
                        }
                    }

                    if (hasNext) {
                        if (in.getOffset() >= endOffset) {
                            throw new UncheckedIOException(
                                "Invalid Array value",
                                new IOException("Reaches the EOF of Array " +
                                    "value but still has element to read"));
                        }
                    } else  {
                        if (in.getOffset() < endOffset) {
                            throw new UncheckedIOException(
                                "Invalid Array value",
                                new IOException("No more element to read " +
                                    "but still not reaches the EOF of " +
                                    "Array value"));
                        }
                    }
                    return hasNext;
                }

                @Override
                public FieldValueSerializer next() {
                    if (!hasNext()) {
                        return null;
                    }
                    index++;

                    if (arrayDef.getElement().isRecord()) {
                        lastElemOffset = in.getOffset();
                    }
                    return new FieldValueSerializerImpl
                        (arrayDef.getElement(), in, exact);
                }
            };
        }
    }

    public static class FieldValueSerializerImpl
        implements FieldValueSerializer {

        private final ByteInputStream in;
        private final int driverType;
        private final FieldDef def;
        private final boolean isJsonDef;
        private final boolean exact;

        FieldValueSerializerImpl(FieldDef def,
                                 ByteInputStream in,
                                 boolean exact) {
            this(def, in, in.getOffset(), exact);
        }

        FieldValueSerializerImpl(FieldDef def,
                                 ByteInputStream in,
                                 int offset,
                                 boolean exact) {

            this.in = in;
            this.exact = exact;
            if (in.getOffset() != offset) {
                in.setOffset(offset);
            }
            try {
                driverType = readValueType(in);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read value type failed", ioe);
            }

            isJsonDef = ((FieldDefImpl)def).isJson();
            /*
             * If target type is JSON, then use KV type mapped from the driver
             * type as the type of value.
             */
            this.def = isJsonDef ? getKVTypeForJSON(driverType) : def;
        }

        @Override
        public FieldDef getDefinition() {
            return def;
        }

        @Override
        public Type getType() {
            return def.getType();
        }

        @Override
        public boolean isNull() {
            /*
             * Accept either null type as NULL if not in JSON
             */
            return !isJsonDef && (driverType == TYPE_NULL ||
                                  driverType == TYPE_JSON_NULL);
        }

        @Override
        public boolean isJsonNull() {
            /*
             * Accept either null type as JSON_NULL if inside JSON
             */
            return isJsonDef && (driverType == TYPE_NULL ||
                                 driverType == TYPE_JSON_NULL);
        }

        @Override
        public boolean isEMPTY() {
            return driverType == TYPE_EMPTY;
        }

        @Override
        public int getInt() {
            try {
                return ProxySerialization.getInt(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read int value failed", ioe);
            }
        }

        @Override
        public String getString() {
            try {
                if (!isJsonDef) {
                    /*
                     * If target type is String, the driverType should be
                     * String.
                     */
                    assertType(TYPE_STRING, driverType);
                    return readNonNullString(in, "String value");
                }
                /* Attempt to cast to string value if target is JSON type */
                return ProxySerialization.getString(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read string value failed", ioe);
            }
        }

        @Override
        public String getEnumString() {
            assertType(TYPE_STRING, driverType);
            try {
                String val = readNonNullString(in, "String value for ENUM");
                ((EnumDefImpl)def).validateValue(val);
                return val;
            } catch (IOException ioe) {
                throw new UncheckedIOException(
                    "Read string value for ENUM failed", ioe);
            }
        }

        @Override
        public long getLong() {
            try {
                return ProxySerialization.getLong(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read long value failed", ioe);
            }
        }

        @Override
        public double getDouble() {
            try {
                return ProxySerialization.getDouble(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Rread double value failed", ioe);
            }
        }

        @Override
        public float getFloat() {
            try {
                return ProxySerialization.getFloat(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read float value failed", ioe);
            }
        }

        @Override
        public byte[] getBytes() {
            try {
                return ProxySerialization.getBytes(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read binary value failed", ioe);
            }
        }

        @Override
        public boolean getBoolean(){
            try {
                return ProxySerialization.getBoolean(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException("Read boolean value failed", ioe);
            }
        }

        @Override
        public byte[] getFixedBytes() {
            byte[] val = getBytes();
            ((FixedBinaryDefImpl)def).validateValue(val);
            return val;
        }

        @Override
        public byte[] getNumberBytes() {
            try {
                return ProxySerialization.getNumberBytes(in, driverType);
            } catch (IOException ioe) {
                throw new UncheckedIOException(
                    "Read binary value for Number type failed", ioe);
            }
        }

        @Override
        public byte[] getTimestampBytes() {
            try {
                return ((TimestampValueImpl) ProxySerialization.createTimestamp(
                            in,
                            driverType,
                            def.asTimestamp())).getBytes();
            } catch (IOException ioe) {
                throw new UncheckedIOException(
                    "Read timestamp value failed", ioe);
            }
        }

        @Override
        public RecordValueSerializer asRecordValueSerializer() {
            if (driverType != TYPE_MAP && driverType != TYPE_ARRAY) {
                throw new IllegalArgumentException(
                    "Type mismatch on input. Expected MAP or ARRAY" +
                    ", but got " + getTypeName(driverType));
            }
            return new RecordValueSerializerImpl(def.asRecord(), in,
                                                 driverType, exact);
        }

        @Override
        public MapValueSerializer asMapValueSerializer() {
            assertType(TYPE_MAP, driverType);
            return new MapValueSerializerImpl(def.asMap(), in, exact);
        }

        @Override
        public ArrayValueSerializer asArrayValueSerializer() {
            assertType(TYPE_ARRAY, driverType);
            return new ArrayValueSerializerImpl(def.asArray(), in, exact);
        }
    }

    /* Maps the driver type to KV type if target KV type is JSON */
    private static FieldDef getKVTypeForJSON(int driverType) {
        switch (driverType) {
        case TYPE_NUMBER:
            return FieldDefImpl.Constants.numberDef;
        case TYPE_INTEGER:
            return FieldDefImpl.Constants.integerDef;
        case TYPE_LONG:
            return FieldDefImpl.Constants.longDef;
        case TYPE_BOOLEAN:
            return FieldDefImpl.Constants.booleanDef;
        case TYPE_DOUBLE:
            return FieldDefImpl.Constants.doubleDef;
        case TYPE_BINARY:
        case TYPE_STRING:
        case TYPE_TIMESTAMP:
            return FieldDefImpl.Constants.stringDef;
        case TYPE_MAP:
            return FieldDefImpl.Constants.mapJsonDef;
        case TYPE_ARRAY:
            return FieldDefImpl.Constants.arrayJsonDef;
        case TYPE_NULL:
        case TYPE_JSON_NULL:
            return FieldDefImpl.Constants.jsonDef;
        default:
            throw new IllegalArgumentException("Unsupported type:" + driverType);
        }
    }
}
