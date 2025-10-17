/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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

import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_BOOLEAN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_DOUBLE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_INTEGER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_LONG;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_MAP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_NUMBER;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_STRING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TYPE_TIMESTAMP;
import static oracle.nosql.proxy.protocol.BinaryProtocol.getTypeName;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import oracle.kv.Value;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.table.FieldDef;
import oracle.kv.table.PrimaryKey;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.Nson.NsonSerializer;
import oracle.nosql.nson.util.NettyByteOutputStream;
import oracle.nosql.nson.values.PathFinder;
import oracle.nosql.nson.values.TimestampValue;

import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.ByteOutputStream;

import io.netty.buffer.ByteBuf;

class JsonCollSerializer {
    /*
     * Copy NSON to a new buffer and create a Value from that buffer
     * 1. filter out primary key fields, returning them in the pkey parameter
     * 2. proxy non-key fields to a new NSON document. While doing this, also
     * validate the data types and fail if unsupported types are present.
     * JSON Collection tables support only valid JSON types, which means the
     * following NoSQL types are not allowed:
     *  - binary
     *  - timestamp
     *  - enum
     * The need for validation of every field unfortunately makes this less
     * efficient than it could otherwise be, which would be to skip non-key
     * fields entirely and do opaque byte copies.
     *
     * Multi-region implications:
     *   1. if the table is MR (without or without counters) the
     *    appropriate format and region id need to be set in the Value
     *    that is created.
     *   2. MR counters:
     *     a. if present, skip them as they cannot be set via API
     *     b. generate an empty map (default) for all MR counters
     */
    static Value createValueFromNson(TableImpl table,
                                     PrimaryKey pkey,
                                     ByteInputStream bis,
                                     String rowMetadata)
        throws IOException {

        /* get callbacks for primary key fields */
        List<String[]> paths = new ArrayList<String[]>();
        for (String fieldName : pkey.getFieldNames()) {
            paths.add(new String[] {fieldName});
        }

        /* add MR counter paths to callbacks */
        final Map<String, FieldDef.Type> mrCounters =
            table.getJsonCollectionMRCounters();
        /*
         * this awkwardness is to allow access to this state within
         * the lambda function below
         */
        final boolean counterAdded[] = (mrCounters == null ? null :
                                        new boolean[]{false});
        if (mrCounters != null) {
            for (String path : mrCounters.keySet()) {
                paths.add(new String[] {path});
            }
        }

        /*
         * Create a serializer that will only allow JSON fields
         */
        final JsonNsonSerializer reserializer =
            JsonNsonSerializer.createJsonNsonSerializer(bis);

        PathFinder pf = new PathFinder(reserializer, paths);
        /*
         * Tell PathFinder to use case-insensitive operations looking
         * for primary key fields
         */
        for (String fieldName : pkey.getFieldNames()) {
            pf.setCaseInsensitive(fieldName);
        }
        /*
         * It is safe to use path[0] below because:
         *  o all primary key fields must be top-level
         *  o all MR counters fields must be top-level
         */
        pf.find(bis,
                (PathFinder finder, String[] path)-> {
                    String fieldName = path[0]; // these fields top-level
                    /* skip MR counter values */
                    if (mrCounters == null ||
                        !mrCounters.containsKey(fieldName)) {
                        /* it's a primary key */
                        addNsonToKey((PrimaryKeyImpl)pkey, fieldName, bis);
                        if (counterAdded != null && !counterAdded[0]) {
                            counterAdded[0] = true;
                            /*
                             * if MR counters in table,
                             * generate default values (empty map). This is
                             * done here because (1) all rows will have a
                             * primary key and (2) we need access to the
                             * output stream in a safe place to add top-level
                             * fields
                             */
                            for (String counterName : mrCounters.keySet()) {
                                reserializer.startMapField(counterName);
                                reserializer.startMap(0);
                                reserializer.endMap(0);
                                reserializer.endMapField(counterName);
                            }
                        }
                    }
                    return true; /* skip generation of value */
                });

        /*
         * add region and MR table format if needed
         */
        Value.Format format = Value.Format.TABLE_V1;
        int regionId = Region.NULL_REGION_ID;
        if (table.isMultiRegion()) {
            format = Value.Format.MULTI_REGION_TABLE;
            regionId = Region.LOCAL_REGION_ID;
        }
        if (rowMetadata != null) {
            format = Value.Format.TABLE_V5;
        }

        byte[] after = reserializer.getBytes();
        return Value.internalCreateValue(after, format, regionId, rowMetadata);
    }

    /*
     * Read valid key types and extracts the value, putting it into the
     * key provided. Use methods in ProxySerialization to allow valid type
     * conversions that do not lose information.
     */
    private static void addNsonToKey(PrimaryKeyImpl pkey,
                                     String keyName,
                                     ByteInputStream in)
        throws IOException {

        int driverType = in.readByte();
        FieldDef def = pkey.getFieldDef(keyName);
        if (def == null) {
            throw new IllegalArgumentException("Invalid key field: " +
                                               keyName);
        }
        FieldDef.Type schemaType = def.getType();

        switch (schemaType) {
        case BOOLEAN:
            pkey.put(keyName, ProxySerialization.getBoolean(in, driverType));
            break;
        case DOUBLE:
            pkey.put(keyName, ProxySerialization.getDouble(in, driverType));
            break;
        case INTEGER:
            pkey.put(keyName, ProxySerialization.getInt(in, driverType));
            break;
        case LONG:
            pkey.put(keyName, ProxySerialization.getLong(in, driverType));
            break;
        case STRING:
            if (driverType != TYPE_STRING) {
                throw new IllegalArgumentException(
                    "Invalid driver type for String: " +
                    getTypeName(driverType));
            }
            pkey.put(keyName, Nson.readString(in));
            break;
        case NUMBER:
            pkey.put(keyName, ProxySerialization.createNumber(in, driverType));
            break;
        default:
            throw new IllegalStateException("Invalid key type: " + schemaType);
        }
    }

    private static class JsonNsonSerializer extends NsonSerializer {

        private static JsonNsonSerializer
            createJsonNsonSerializer(ByteInputStream bis) {
            return new JsonNsonSerializer(
                ByteOutputStream.createByteOutputStream());
        }

        private JsonNsonSerializer(ByteOutputStream bos) {
            super(bos);
        }

        /*
         * Override only those types that are not valid JSON
         */
        @Override
        public void binaryValue(byte[] byteArray) throws IOException {
            invalidJsonType("binary");
        }

        @Override
        public void binaryValue(byte[] byteArray,
                                int offset,
                                int length) throws IOException {
            invalidJsonType("binary");
        }

        @Override
        public void timestampValue(TimestampValue timestamp)
            throws IOException {
            invalidJsonType("timestamp");
        }

        /*
         * Copy active bytes from the stream to a new byte[]
         */
        private byte[] getBytes() {
            ByteBuf buf = ((NettyByteOutputStream) getStream()).getBuffer();
            byte[] nsonBytes = new byte[buf.readableBytes()];
            buf.readBytes(nsonBytes);
            return nsonBytes;
        }

        private void invalidJsonType(String type) {
            throw new IllegalArgumentException(
                "Invalid JSON type, " + type + ", is not allowed in a JSON " +
                "Collection table");
        }
    }

    /* keep this to size ByteBuf used for reserialization? */
    private static int peekAtMapSize(ByteInputStream bis) throws IOException {
        final int mapPeekOverhead = 5; // see comment below
        int offset = bis.getOffset();
        int type = bis.readByte();
        if (type != TYPE_MAP) {
            throw new IllegalStateException("Expected MAP, found type " + type);
        }
        /*
         * read total length it bytes of the MAP. This starts at the current
         * location in the stream with is 5 bytes in -- 1 byte type, 4 bytes
         * length). Add that overhead to the return value
         */
        int length = bis.readInt() + mapPeekOverhead;
        bis.setOffset(offset);
        for (int i = 0; i < length; i++) {
            System.out.print("["+bis.readByte()+ "]");
        }
        System.out.println("");

        bis.setOffset(offset);
        return length;
    }
}
