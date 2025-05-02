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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MRCounterValueSerializer;
import oracle.kv.impl.api.table.serialize.AvroEncoder;

import oracle.kv.impl.util.ArrayPosition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SortableString;
import oracle.kv.Key;
import oracle.kv.Key.BinaryKeyIterator;
import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.MapDef;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.Nson.NsonSerializer;
import oracle.nosql.nson.values.MapWalker;
import oracle.nosql.nson.values.PathFinder;
import oracle.nosql.nson.util.ByteInputStream;
import oracle.nosql.nson.util.ByteOutputStream;
import oracle.nosql.nson.util.NettyByteInputStream;
import oracle.nosql.nson.util.NioByteInputStream;
import oracle.nosql.nson.util.NioByteOutputStream;
import oracle.nosql.nson.values.TimestampValue;

import com.sleepycat.util.PackedInteger;

/**
 * NsonUtil is a class of static methods that encapsulate serialization and
 * deserialization of NSON data.
 */
public class NsonUtil {

    /**
     * Create primary key in NSON from BinaryKeyIterator and puts it in
     * the output stream.
     * @param table the table that defines the key
     * @param keyIter primary key iterator
     * @param out output stream used to write the primary key bytes in NSON
     * @return true if the operation is a success
     */
    public static boolean createNsonFromKeyBytes(TableImpl table,
                                                 BinaryKeyIterator keyIter,
                                                 ByteOutputStream out) {
        int primaryKeySize = table.getPrimaryKeySize();
        AvroNsonReader nson = new AvroNsonReader(out, true);
        nson.startMap(null, null, 0);

        ArrayPosition currentPrimKeyPos =
            new ArrayPosition(primaryKeySize);
        if (createNsonFromKeyBytes(table,
                                   currentPrimKeyPos,
                                   keyIter,
                                   table.getRowDef(),
                                   nson)) {
            nson.endMap(primaryKeySize);
            return true;
        }
        return false;
    }

    /**
     * Create row value in NSON from value bytes in Avro and puts it in
     * the output stream.
     *
     * @param table the table to use
     * @param valueBytes value bytes, starting with the Format of the row. The
     * encoding may be Avro (schema table) or NSON (JSON collection table)
     * @param keepMRCounterValues if true, keep the MR counter map values
     * from the value bytes. If false, collapse them into a single value
     * @param out output stream used to write the row value bytes in NSON
     * @return true if the operation was successful
     */
    public static boolean createNsonFromValueBytes(TableImpl table,
                                                   byte[] valueBytes,
                                                   boolean keepMRCounterValues,
                                                   ByteOutputStream out) {

        /*
         * valueBytes.length == 0 implies that when the row was created
         * the table was a key-only table.  Even though the table may have
         * evolved since then, nson doesn't need to add missing fields with
         * their default value.
         */
        if (table.isKeyOnly() || valueBytes.length == 0) {
            return false;
        }

        Value.Format format = Value.Format.fromFirstByte(valueBytes[0]);
        if (Format.isTableFormat(format)) {
            int offset = 1;
            if (format == Value.Format.MULTI_REGION_TABLE) {
                final int regionIdLen =
                    PackedInteger.getReadIntLength(valueBytes, 1);
                PackedInteger.readInt(valueBytes, 1);
                offset = regionIdLen + 1;
            }
            if (table.isJsonCollection()) {
                try {
                    /*
                     * the bytes are already NSON. Handle MR counters if present
                     */
                    if (table.hasJsonCollectionMRCounters() &&
                        !keepMRCounterValues) {
                        return reserializeNsonResolveMRCounters(
                            table, valueBytes, offset, out);
                    }
                    out.write(valueBytes, offset, valueBytes.length-offset);
                } catch (IOException ioe) {
                    throw new IllegalStateException(
                        "Could not copy JSON Collection row to stream: " +
                        ioe.getMessage());
                }
            }
            /* the AvroNsonReader decides how to handle MR counters */
            AvroNsonReader nson = new AvroNsonReader(out, keepMRCounterValues);
            nson.startMap(null, null, 0);
            if (table.initRowFromByteValue(nson, valueBytes, table,
                                           format, offset)){
                nson.endMap(table.getFieldMap().getFieldNames().size());
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a PrimaryKey from NSON key bytes.
     *
     * @param table the table to use for the key
     * @param nsonKeyBytes key bytes in NSON
     */
    public static PrimaryKeyImpl createPrimaryKeyFromNsonBytes(
        TableImpl table, byte[] nsonKeyBytes) {
        PrimaryKeyImpl pKey = table.createPrimaryKey();
        new KeyCreator(pKey, nsonKeyBytes).create();
        return pKey;
    }

    /**
     * Creates a Key from NSON key bytes. A shortcut for
     *  createPrimaryKeyFromNsonBytes followed by createKeyInternal
     * @param table the table to use
     * @param nsonKeyBytes the NSON encoding of the key
     */
    public static Key createKeyFromNsonBytes(TableImpl table,
                                             byte[] nsonKeyBytes) {
        return table.createKeyInternal(
            createPrimaryKeyFromNsonBytes(table, nsonKeyBytes), false);
    }

    /**
     * Creates a Value from NSON value bytes. It may be Avro or in the case of
     * JSON collection tables the NSON representation.
     * This is directly re-serialize from NSON to Avro. The NSON was written by
     * direct reserialization from Avro. When the NSON was created:
     *  o Default/null values will be filled in.
     *  o Identity/UUID columns should not be missing.
     * So we can make below Assumptions on the NSON bytes:
     *  o NSON fields are in schema order
     *  o there are no missing fields in NSON
     * So this code path will not generate default value, Identity and UUID.
     * If the table is MR and has any MR counters they may or may not be
     * serialized in the NSON as a map of (regionId, value). In the case of
     * GAT the NSON will have the map. In the case of a backup or stream to
     * NSON the NSON will have a single value. In the former case pass
     * false to createMRCounterMap. In the latter case, pass true so that the
     * code knows to turn the single value into an MR counter map.
     *
     * This code is schema driven, walking the table schema, reading
     * values from NSON and writing to Avro. This is why the assumptions
     * above must be true.
     *
     * @param table the target table
     * @param nsonValueBytes value bytes in NSON. Key may or may not be
     * included but if included it's skipped
     * @param regionId set to non-zero if restoring to an MR table or table
     * that might become MR and has an MR counter
     * @param createMRCounterMap if true and the table has an MR counter it is
     * assumed that the NSON has a single value for the counter that needs
     * to be translated into a map representation using the provided regionId.
     * The mechannism for this is different between JSON collection tables
     * and schema-based tables.
     */
    public static Value createValueFromNsonBytes(TableImpl table,
                                                 byte[] nsonValueBytes,
                                                 int regionId,
                                                 boolean createMRCounterMap) {
        if (table.isJsonCollection()) {
            /*
             * In JSON collection tables the NSON is the value byte array
             * but MR counter translation may be required
             */
            if (createMRCounterMap && regionId != 0 &&
                table.hasJsonCollectionMRCounters()) {
                /* rewrite the NSON inserting the map */
                try {
                    nsonValueBytes =
                        reserializeNsonCreateMRCounterMap(table,
                                                          nsonValueBytes,
                                                          regionId,
                                                          0);
                } catch (IOException ioe) {
                    throw new IllegalCommandException(
                        "Failed to re-serialize NSON with MR counter: " + ioe);
                }
            }
            return Value.internalCreateValue(
                nsonValueBytes,
                regionId == Region.NULL_REGION_ID ?
                    Value.Format.TABLE_V1 : Value.Format.MULTI_REGION_TABLE,
                regionId);
        }
        ByteInputStream nsonInput =
            NettyByteInputStream.createFromBytes(nsonValueBytes);

        final ByteArrayOutputStream outputStream =
            new TableImpl.ByteArrayOutputStreamWithInitialBuffer(
                TableImpl.createValueBuffer.get());

        /*
         * Byte array starts with table version
         */
        int writeVersion = table.getTableVersion();
        outputStream.write(writeVersion);

        final AvroEncoder e = new AvroEncoder(outputStream,
                                              TableImpl.encoderBuffer.get());
        if (createMRCounterMap) {
            /* in this case the encoder needs the region id to create the map */
            e.setRegionId(regionId);
        }
        try {
            writeAvroRecordFromNson(table, e, nsonInput,
                                    table.getFieldMap(), true);
            e.flush();
            /*
             * If non Null regionId is specified, it should use MRTable format.
             */
            return Value.internalCreateValue(
                outputStream.toByteArray(),
                regionId == Region.NULL_REGION_ID ?
                    Value.Format.TABLE_V1 : Value.Format.MULTI_REGION_TABLE,
                regionId);

        } catch (IOException ioe) {
            throw new IllegalCommandException("Failed to serialize Avro " +
                                          " from NSON: " + ioe);
        }
    }

    /* private methods below */

    /**
     * This method takes an NSON byte[] that has an MR counter map in it and
     * re-serializes it into the ByteOutputStream collapsing the MR counter
     * map into a single value in the output stream. It's a fair bit of code
     * but it's reasonably efficient in that it doesn't create a lot of objects
     *
     * @param table the target table
     * @param valueBytes value bytes in NSON
     * @param offset offset into the value to start (the format and regiond
     * id need to be skipped)
     * @param out the output stream for the re-serialized NSON
     * @return true if the operation was successful
     */
    private static boolean reserializeNsonResolveMRCounters(
        TableImpl table,
        byte[] valueBytes,
        int offset,
        ByteOutputStream out) throws IOException {

        /*
         * generate events for value and when MR counter is found, resolve
         * it
         */

        /* add mr counter paths to PathFinder callbacks */
        ArrayList<String[]> paths = new ArrayList<String[]>();
        final Map<String, FieldDef.Type> mrCounters =
            table.getJsonCollectionMRCounters();
        for (String path : mrCounters.keySet()) {
            paths.add(new String[] {path});
        }

        final NsonSerializer ns = new NsonSerializer(out);
        PathFinder pf = new PathFinder(ns, paths);
        try (final NioByteInputStream in =
             new NioByteInputStream(ByteBuffer.wrap(valueBytes))) {
            /* skip preamble bytes (Format, etc) */
            if (offset > 0) {
                in.skip(offset);
            }

            /*
             * It is safe to use path[0] because MR counters in JSON
             * collections must be top-level fields.
             *
             * PathFinder reserializes all events to the out stream except when
             * the path matches one of the paths passed in. In that case
             * the callback below is called so it can perform MR counter
             * accumulation
             */
            pf.find(in,
                (PathFinder finder, String[] path)-> {
                        String fieldName = path[0]; /* fields are top-level */
                        /* "resolve" counter value and write it */
                        Type counterType = mrCounters.get(fieldName);
                        final FieldValueImpl counterValue =
                            FieldDefImpl.getCRDTDef(counterType).
                            createCRDTValue();
                        /* walk map and add values into the counterValue */
                        MapWalker walker = new MapWalker(in);
                        while (walker.hasNext()) {
                            walker.next();
                            String regionName = walker.getCurrentName();
                            int regionId = Integer.parseInt(regionName);
                            FieldValueImpl value = readCounterValue(in);
                            counterValue.putMRCounterEntry(regionId, value);
                        }
                        /* now put aggregated value into the output stream */
                        ns.startMapField(fieldName);
                        switch (counterType) {
                        case INTEGER:
                            ns.integerValue(counterValue.asInteger().get());
                            break;
                        case LONG:
                            ns.longValue(counterValue.asLong().get());
                            break;
                        case NUMBER:
                            ns.numberValue(counterValue.getDecimal());
                            break;
                        default:
                            /* nop */
                        }
                        ns.endMapField(fieldName);
                        return true; /* skip the counter map; it's been used */
                    });
        }
        return true;
    }

    /*
     * Reads a single MR counter value from the map based on its type,
     * returning the corresponding FieldValueImpl. The stream must be
     * positioned at the start of the value.
     *
     * This is used for collapsing an MR counter map into a single value.
     */
    private static FieldValueImpl readCounterValue(ByteInputStream in)
        throws IOException {
        int type = NsonUtil.peekByte(in);
        switch (type) {
        case Nson.TYPE_INTEGER:
            return FieldDefImpl.Constants.integerDef.createInteger(
                Nson.readNsonInt(in));
        case Nson.TYPE_LONG:
            return FieldDefImpl.Constants.longDef.createLong(
                Nson.readNsonLong(in));
        case Nson.TYPE_NUMBER:
            return FieldDefImpl.Constants.numberDef.createNumber(
                Nson.readNsonNumber(in));
        default:
            throw new IllegalStateException("Invalid type for MR counter: " +
                                            type);
        }
    }

    /**
     * This method takes an NSON byte[] that has a single value for an MR
     * counter and re-serializes it into another NSON array that has the
     * map representation of the MR counter. This case occurs in a restore
     * from NSON operation. It will not happen in GAT or MR. In those cases
     * the NSON will have the map representations..
     *
     * @param table the target table
     * @param valueBytes value bytes in NSON
     * @param regionId the region is to use for the map
     * @param offset offset into the value to start (e.g. if the format
     * and regionId need to be skipped)
     * @return the reserialized array
     */
    private static byte[] reserializeNsonCreateMRCounterMap(
        TableImpl table,
        byte[] valueBytes,
        int regionId,
        int offset) throws IOException {

        /*
         * generate events for value and when MR counter is found, create the
         * map
         */

        /* add mr counter paths to PathFinder callbacks */
        ArrayList<String[]> paths = new ArrayList<String[]>();
        final Map<String, FieldDef.Type> mrCounters =
            table.getJsonCollectionMRCounters();
        for (String path : mrCounters.keySet()) {
            paths.add(new String[] {path});
        }

        try (final NioByteOutputStream out =
             new NioByteOutputStream(valueBytes.length * 2, false)) {
            try (final NioByteInputStream in =
                 new NioByteInputStream(ByteBuffer.wrap(valueBytes))) {
                /* skip preamble bytes (Format, etc) */
                if (offset > 0) {
                    in.skip(offset);
                }

                final NsonSerializer ns = new NsonSerializer(out);
                PathFinder pf = new PathFinder(ns, paths);

                /*
                 * It is safe to use path[0] because MR counters in JSON
                 * collections must be top-level fields.
                 *
                 * PathFinder reserializes all events to the stream except when
                 * the path matches one of the paths passed in. In that case
                 * the callback below is called so it can add the MR counter
                 * map
                 */
                pf.find(in,
                        (PathFinder finder, String[] path)-> {
                            /* fields are top-level */
                            String fieldName = path[0];
                            String regionString = String.valueOf(regionId);
                            /*
                             * Turn something that looks like "counter" : 5 into
                             * "counter: {"regionId" : 5}
                             */
                            ns.startMapField(fieldName);
                            ns.startMap(1);
                            ns.startMapField(regionString);
                            int type = in.readByte();
                            switch (type) {
                            case Nson.TYPE_INTEGER:
                                ns.integerValue(Nson.readInt(in));
                                break;
                            case Nson.TYPE_LONG:
                                ns.longValue(Nson.readLong(in));
                                break;
                            case Nson.TYPE_NUMBER:
                                ns.numberValue(new BigDecimal(
                                                   Nson.readString(in)));
                                break;
                            default:
                                /* nop */
                            }
                            ns.endMapField(regionString);
                            ns.endMap(1);
                            ns.endMapField(fieldName);
                            /*
                             * skip generation for field. This is ok even
                             * though the input has been read above -- the
                             * stream offset is reset in PathFinder
                             */
                            return true;
                        });
            }
            return Arrays.copyOfRange(out.array(), 0, out.getOffset());
        }
    }

    /*
     * Read NSON stream for specific field and write value via the Avro
     * Encoder. In the case of Record values the order is important. It is
     * assumed/necessary that the record was written into NSON in schema
     * order.
     *
     * NOTE: table is ONLY used to determine if a field is a primary key
     * field that needs to be skipped
     *
     * @param table the table to use
     * @param encoder the AvroEncoder use to write the Avro
     * @param nsonStream the NSON being read
     * @param fieldDef the definition (type) of the schema type where the
     * NSON stream is currently positioned.
     */
    private static void writeAvroFromNson(TableImpl table,
                                          AvroEncoder encoder,
                                          ByteInputStream nsonStream,
                                          FieldDef fieldDef)
        throws IOException {

        if (fieldDef.isJson()) {
            /* jump into the JSON encapsulation code */
            writeJsonFromNson(encoder, nsonStream, (JsonDefImpl) fieldDef);
            return;
        }

        /*
         * NOTE: the Nson.readNson* methods read and verify the type from the
         * NSON stream based on the method name
         */
        switch (fieldDef.getType()) {
        case INTEGER:
            encoder.writeInt(Nson.readNsonInt(nsonStream));
            break;
        case LONG:
            encoder.writeLong(Nson.readNsonLong(nsonStream));
            break;
        case DOUBLE:
            encoder.writeDouble(Nson.readNsonDouble(nsonStream));
            break;
        case FLOAT:
            encoder.writeFloat((float)Nson.readNsonDouble(nsonStream));
            break;
        case NUMBER:
            byte[] bytes = NumberUtils.serialize(
                Nson.readNsonNumber(nsonStream));
            encoder.writeBytes(bytes);
            break;
        case STRING:
            if (fieldDef.isUUIDString()) {
                encoder.writeBytes(
                   StringValueImpl.packUUID(
                       Nson.readNsonString(nsonStream)));
                break;
            }
            encoder.writeString(Nson.readNsonString(nsonStream));
            break;
        case BOOLEAN:
            encoder.writeBoolean(Nson.readNsonBoolean(nsonStream));
            break;
        case BINARY:
            encoder.writeBytes(Nson.readNsonBinary(nsonStream));
            break;
        case FIXED_BINARY:
            encoder.writeFixed(Nson.readNsonBinary(nsonStream));
            break;
        case ENUM:
            /* NSON uses string for enum */
            EnumDefImpl enumDef = (EnumDefImpl)fieldDef;
            encoder.writeEnum(enumDef.indexOf(Nson.readNsonString(nsonStream)));
            break;
        case TIMESTAMP:
            /*
             * NSON doesn't have a handy method for reading Timestamp
             */
            nsonStream.readByte(); /* read type */
            String val = Nson.readString(nsonStream);
            bytes = TimestampUtils.toBytes(
                new TimestampValue(val).getValue(),
                fieldDef.asTimestamp().getPrecision());
            encoder.writeBytes(bytes);
            break;
        case RECORD:
            writeAvroRecordFromNson(table, encoder, nsonStream,
                                    ((RecordDefImpl) fieldDef).getFieldMap(),
                                    false);
            break;
        case MAP:
            writeAvroMapFromNson(table, encoder, nsonStream, fieldDef);
            break;
        case ARRAY:
            writeAvroArrayFromNson(table, encoder, nsonStream, fieldDef);
            break;
        default:
            throw new IllegalStateException("Unexpected type: " + fieldDef);
        }
    }

    /**
     * Write a default value into Avro from FieldMapEntry. This happens
     * when the field doesn't exist in the NSON, which can happen with
     * schema evolution or restoring to a table with a slightly different
     * schema.
     * @param encoder the AvroEncoder use to write the Avro
     * @param entry the FieldMapEntry for the field
     */
    private static void writeDefaultValue(AvroEncoder encoder,
                                          FieldMapEntry fme)
        throws IOException {

        FieldValueImpl value = fme.getDefaultValue();
        if (value.isNull()) {
            /*
             * null is always the first choice in the union when
             * there is no default values
             */
            encoder.writeIndex(fme.hasDefaultValue() ? 1 : 0);
            encoder.writeNull();
            return;
        }

        if (fme.isNullable()) {
            /*
             * nullable fields with a default value generate schemas
             * with the default type first in the union.
             */
            encoder.writeIndex(fme.hasDefaultValue() ? 0 : 1);
        }

        FieldDefImpl fieldDef = fme.getFieldDef();

        switch (fieldDef.getType()) {
        case INTEGER:
            encoder.writeInt(value.getInt());
            break;
        case LONG:
            encoder.writeLong(value.getLong());
            break;
        case DOUBLE:
            encoder.writeDouble(value.getDouble());
            break;
        case FLOAT:
            encoder.writeFloat(value.getFloat());
            break;
        case NUMBER:
            byte[] bytes = value.getNumberBytes();
            encoder.writeBytes(bytes);
            break;
        case STRING:
            encoder.writeString(value.getString());
            break;
        case BOOLEAN:
            encoder.writeBoolean(value.getBoolean());
            break;
        case BINARY:
            encoder.writeBytes(value.getBytes());
            break;
        case FIXED_BINARY:
            encoder.writeFixed(value.getBytes());
            break;
        case ENUM:
            /* NSON uses string for enum */
            EnumDefImpl enumDef = (EnumDefImpl) fieldDef;
            encoder.writeEnum(enumDef.indexOf(value.getEnumString()));
            break;
        case TIMESTAMP:
            encoder.writeBytes(value.getBytes());
            break;
        case RECORD:
        case MAP:
        case ARRAY:
        default:
            /*
             * record, map, array have no defaults, but are nullable so they
             * should not get here
             */
            throw new IllegalStateException(
                "Unexpected type, no default value: " + fieldDef);
        }
    }

    /*
     * Read NSON stream and write via the Avro Encoder based on type in schema.
     * See above createAvroFromNsonBytes regarding NSON bytes assumptions.
     *
     * Algorithm:
     *  o Walk the row schema (FieldMap), for each field:
     *    - read field name from NSON
     *    - read the NSON and write via the AvroEncoder based on type in
     *    schema
     *  o state of reading NSON is "kept" in the ByteInputStream as its offset.
     *  o MapWalker "wraps" the stream and does this:
     *   - reads the type byte and validates that it's NSON MAP
     *   - on next() calls, reads the field name for the current map entry,
     *   positioning the stream offset at the type byte of the value
     *
     * Issue:
     *   Records, and therefore schema, are order-dependent. This code mostly
     *   relies on 2 factors:
     *   1. the NSON is stored in schema order
     *   2. the NSON matches the schema of the table
     *   Unfortunately these are not invariants and can be affected by:
     *   o schema evolution (add/remove fields)
     *   o restoring NSON to a schema that is slightly different from the
     *   original schema
     *
     *   The algorithm to handle this is, while walking the table schema:
     *    o if the field names match (schema and nson), no problem
     *    o if they do not, search the NSON for the schema name
     *      a. if found, use the found value
     *      b. if not found, use the default value (or null) from the schema
     *    o reset the NSON stream to where it started failing. This means much
     *    of the time if a field doesn't match, all future fields involve a
     *    search, which is reasonably efficient
     *    This algorithm also ends up handling out of order NSON, which could
     *    be handy in the future
     */
    private static void writeAvroRecordFromNson(TableImpl table,
                                                AvroEncoder encoder,
                                                ByteInputStream nsonStream,
                                                FieldMap fieldMap,
                                                boolean isTopLevelSchema)
            throws IOException {

        /* see comments above about MapWalker */
        MapWalker walker = new MapWalker(nsonStream);
        boolean walkerNeedsReset = false;
        int resetOffset = 0;
        int resetIndex = 0;
        for (int pos = 0; pos < fieldMap.size(); ++pos) {
            if (walkerNeedsReset) {
                walker.reset(resetIndex, resetOffset);
                walkerNeedsReset = false;
            } else {
                resetOffset = walker.getStreamOffset();
                resetIndex = walker.getCurrentIndex();
            }

            FieldMapEntry fme = fieldMap.getFieldMapEntry(pos);
            FieldDefImpl fdef = fme.getFieldDef();
            String fieldName = fme.getFieldName();
            /* skip primary key fields (must be at top level) */
            if (isTopLevelSchema && table.isPrimKeyAtPos(pos)) {
                continue;
            }

            /* if nothing in NSON, use defaults */
            if (!walker.hasNext()) {
                writeDefaultValue(encoder, fme);
                    continue;
            }
            walker.next();
            /*
             * Schema field names are case-insensitive. While the NSON
             * should be case-preserving, be flexible and allow for
             * a case mismatch.
             */
            if (!fieldName.equalsIgnoreCase(walker.getCurrentName())) {
                walker.skip(); /* get to next field */
                /* case-insensitive find */
                boolean found = walker.find(fieldName, false);
                walkerNeedsReset = true;
                if (!found) {
                    writeDefaultValue(encoder, fme);
                    continue;
                }
                /* stream is at the value of the correct field */
            }
            /*
             * read type of NSON and handle null values for defined schema
             * fields. If the field is JSON the value must be encapsulated
             * in a binary (Avro) field that is generated from the NSON so
             * this check is skipped
             */
            if (!fdef.isJson()) {
                int nsonType = peekByte(nsonStream);
                if (isNsonNull(nsonType)) {
                    /*
                     * null is always the first choice in the union when
                     * there is no default values
                     */
                    encoder.writeIndex(fme.hasDefaultValue() ? 1 : 0);
                    encoder.writeNull();
                    nsonStream.readByte(); // consume the byte
                    continue;
                }
            }

            if (fme.isNullable()) {
                /*
                 * nullable fields with a default value generate schemas
                 * with the default type first in the union.
                 */
                encoder.writeIndex(fme.hasDefaultValue() ? 0 : 1);
            }

            /*
             * MRCounter can only be top row field or inside JSON field.
             */
            if (fdef.isMRCounter()) {
                writeMRCounterFromNson(table, encoder, nsonStream, fdef);
                continue;
            }

            writeAvroFromNson(table, encoder, nsonStream, fdef);
        }
    }

    /*
     * Read NSON stream for specific Map field and write Map value via the
     * Avro Encoder.
     */
    private static void writeAvroMapFromNson(TableImpl table,
                                             AvroEncoder encoder,
                                             ByteInputStream nsonStream,
                                             FieldDef fieldDef)
            throws IOException {

        MapDef mapDef = (MapDef) fieldDef;
        FieldDef elementDef = mapDef.getElement();
        MapWalker walker = new MapWalker(nsonStream);
        encoder.writeMapStart();
        encoder.setItemCount(walker.getNumElements());
        while (walker.hasNext()) {
            walker.next();
            encoder.startItem();
            encoder.writeString(walker.getCurrentName());
            writeAvroFromNson(table, encoder, nsonStream, elementDef);
        }
        encoder.writeMapEnd();
    }

    /*
     * Read NSON stream for specific Array field and write Array value via the
     * Avro Encoder.
     */
    private static void writeAvroArrayFromNson(TableImpl table,
                                               AvroEncoder encoder,
                                               ByteInputStream nsonStream,
                                               FieldDef fieldDef)
            throws IOException {

        ArrayDef arrayDef = (ArrayDef) fieldDef;
        FieldDef elementDef = arrayDef.getElement();

        int arraySize = readNsonArrayMetadata(nsonStream);
        encoder.writeArrayStart();
        encoder.setItemCount(arraySize);

        for (int i = 0; i < arraySize; i++) {
            encoder.startItem();
            writeAvroFromNson(table, encoder, nsonStream, elementDef);
        }
        encoder.writeArrayEnd();
    }

    /*
     * Reads NSON array metadata and returns the number of elements
     * in the array. Also validates the type code.
     */
    private static int readNsonArrayMetadata(ByteInputStream nsonStream)
        throws IOException {

        /*
         * NSON array format, stream is set at the type.
         * Format is:
         *   type (byte)
         *   total length of array in bytes (4 bytes)
         *   number of elements in the array (4 bytes)
         *   <elements>
         */
        /* consume total size in bytes */
        int type = nsonStream.readByte();
        if (type != Nson.TYPE_ARRAY) {
            throw new IllegalArgumentException(
                "Unexpected type for NSON array: " + type);
        }
        nsonStream.readInt(); // consume total bytes
        return nsonStream.readInt();
    }

    /*
     * Read NSON stream for specific JSON field and write JSON value via the
     * Avro Encoder. It will write JSON value using FieldValueSerialization
     * format.
     */
    private static void writeJsonFromNson(AvroEncoder encoder,
                                          ByteInputStream nsonStream,
                                          JsonDefImpl jsonDef)
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        FieldValueSerialization.FVSSerializer handler =
            new FieldValueSerialization.FVSSerializer(out,
                                                      SerialVersion.CURRENT,
                                                      encoder.getRegionId());
        handler.process(nsonStream, jsonDef);
        encoder.writeBytes(baos.toByteArray());
    }

    /* used by TableImpl for now */
    static int peekByte(ByteInputStream bis) throws IOException {
        int retVal = bis.readByte();
        bis.setOffset(bis.getOffset() - 1);
        return retVal;
    }

    private static boolean isNsonNull(int type) {
        return (type == Nson.TYPE_NULL ||
                type == Nson.TYPE_JSON_NULL);
    }

    /*
     * Read NSON stream for specific CRDT field and write CRDT value via the
     * Avro Encoder.
     */
    private static void writeMRCounterFromNson(TableImpl table,
                                               AvroEncoder encoder,
                                               ByteInputStream nsonStream,
                                               FieldDefImpl fieldDef)
        throws IOException {

        if (encoder.createCRDTMap()) {
            /*
             * the stream will not have a map. It will have an atomic
             * numeric value. Use that to create a map using the regionid
             */
            int regionId = encoder.getRegionId();
            if (regionId == 0) {
                throw new IllegalArgumentException(
                    "AvroEncoder must have a valid region id");
            }
            encoder.writeInt(
                FieldValueImpl.CounterVersion.COUNTER_V1.ordinal());
            encoder.writeCRDTStart();
            encoder.setItemCount(1);
            encoder.startItem();
            encoder.writeInt(regionId);
            writeAvroFromNson(table, encoder, nsonStream,
                              fieldDef.getCRDTElement());
            encoder.writeCRDTEnd();
            return;
        }
        MapWalker walker = new MapWalker(nsonStream);
        encoder.writeInt(FieldValueImpl.CounterVersion.COUNTER_V1.ordinal());
        encoder.writeCRDTStart();
        encoder.setItemCount(walker.getNumElements());

        while (walker.hasNext()) {
            walker.next();
            encoder.startItem();
            encoder.writeInt(Integer.parseInt(walker.getCurrentName()));
            writeAvroFromNson(table, encoder, nsonStream,
                              fieldDef.getCRDTElement());
        }
        encoder.writeCRDTEnd();
    }

    /**
     * Deserialize a binary primary key, and use the extracted values to
     * serialize it to Nson. The binary prim key is given as a
     * BinaryKeyIterator.
     *
     * Notice that the binary primary key contains the internal ids of the
     * targetTable and its ancestors (if any). As a result, this method calls
     * itself recursively on the ancestor tables in order to deserialize and
     * skip their table ids. Each ancestor table deserializes its portion of
     * the prim key as well and fills-in the Nson.
     *
     * This method should only be called for Key objects from the store so they
     * are well-formed in terms of the expected layout.
     *
     * @return true if the key was deserialized in full, false otherwise.
     *
     * This method must not throw exceptions.
     */
    private static boolean createNsonFromKeyBytes(
        TableImpl table,
        ArrayPosition currentPrimKeyColumn,
        BinaryKeyIterator keyIter,
        RecordDefImpl recordDef,
        AvroNsonReader nson) {
        if (table.getParentImpl() != null) {
            if (!createNsonFromKeyBytes(table.getParentImpl(),
                                        currentPrimKeyColumn,
                                        keyIter,
                                        recordDef,
                                        nson)) {
                return false;
            }
        }
        assert !keyIter.atEndOfKey();

        String keyComponent = keyIter.next();

        if (!keyComponent.equals(table.getIdString())) {
            return false;
        }

        int lastPrimKeyCol = table.getPrimaryKeySize() - 1;

        /*
         * Fill in values for primary key components that belong to this
         * table only.
         */
        while (currentPrimKeyColumn.hasNext()) {

            int pos = currentPrimKeyColumn.next();

            assert !keyIter.atEndOfKey();

            int pkFieldPos = table.getPrimKeyPos(pos);
            String val = keyIter.next();
            FieldDefImpl def = recordDef.getFieldDef(pkFieldPos);
            String fname = recordDef.getFieldName(pkFieldPos);
            try {
                readFromKeyString(nson, fname, val, def);
            } catch (Exception e) {
                return false;
            }

            if (pos == lastPrimKeyCol) {
                break;
            }
        }

        return true;
    }

    private static void readFromKeyString(AvroRowReader reader,
                                          String fieldName,
                                          String value,
                                          FieldDefImpl def) {
        if (def.isMRCounter()) {
            throw new IllegalCommandException("CRDT is not allowed in a key");
        }

        reader.startMapField(fieldName);
        switch (def.getType()) {
        case INTEGER:
            reader.readInteger(def.getFieldName(),
                               SortableString.intFromSortable(value));
            break;
        case LONG:
            reader.readLong(def.getFieldName(),
                            SortableString.longFromSortable(value));
            break;
        case STRING:
            if (def.isUUIDString()) {
                reader.readString(def.getFieldName(),
                                  StringValueImpl.unpackUUIDfromPrimKey(value));
            } else {
                reader.readString(def.getFieldName(), value);
            }
            break;
        case DOUBLE:
            reader.readDouble(def.getFieldName(),
                              SortableString.doubleFromSortable(value));
            break;
        case FLOAT:
            reader.readDouble(def.getFieldName(),
                              SortableString.floatFromSortable(value));
            break;
        case NUMBER:
            reader.readNumber(def.getFieldName(),
                              SortableString.bytesFromSortable(value));
            break;
        case ENUM:
            reader.readEnum(def.getFieldName(), def,
                            SortableString.intFromSortable(value));
            break;
        case BOOLEAN:
            reader.readBoolean(def.getFieldName(),
                               BooleanValueImpl.toBoolean(value));
            break;
        case TIMESTAMP:
            reader.readTimestamp(def.getFieldName(), def,
                                 SortableString.bytesFromSortable(value));
            break;
        default:
            throw new IllegalCommandException("Type is not allowed in a key: " +
                                              def.getType());
        }
        reader.endMapField(fieldName);
    }

    private static class AvroNsonReader implements AvroRowReader {
        /* serializes value-based events into NSON */
        private final NsonSerializer nson;
        /*
         * if true, leave an MR counter map, if present, intact. If false,
         * serialize into a single numeric value that is the actual value
         * of the MR counter (sum of region portions).
         *
         * In the case of GAT replication the map should remain intact as it
         * is needed for resolution when replicated.
         *
         * In the case of backup or generic streaming the counter should use the
         * single, user-visible value.
         */
        private final boolean keepMRCounterValues;

        private AvroNsonReader(ByteOutputStream out,
                               boolean keepMRCounterValues) {
            this.nson = new NsonSerializer(out);
            this.keepMRCounterValues = keepMRCounterValues;
        }

        private void handleIOException(String element, IOException ioe) {
            throw new IllegalArgumentException(
                "Unable to write " + element, ioe);
        }

        @Override
        public void readInteger(String fieldName, int val) {
            try {
                nson.integerValue(val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readLong(String fieldName, long val) {
            try {
                nson.longValue(val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readFloat(String fieldName, float val) {
            try {
                nson.doubleValue(val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readDouble(String fieldName, double val) {
            try {
                nson.doubleValue(val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readNumber(String fieldName, byte[] bytes) {
            Object val = NumberUtils.deserialize(bytes, true);
            assert(val instanceof BigDecimal);
            try {
                nson.numberValue((BigDecimal)val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readTimestamp(String fieldName,
                                  FieldDef def,
                                  byte[] bytes) {
            try {
                nson.timestampValue(new TimestampValue(
                    TimestampUtils.fromBytes(bytes,
                                             def.asTimestamp().getPrecision())));
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readBinary(String fieldName, byte[] bytes) {
            try {
                nson.binaryValue(bytes);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readFixedBinary(String fieldName,
                                    FieldDef def,
                                    byte[] bytes) {
            try {
                nson.binaryValue(bytes);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readString(String fieldName, String val) {
            try {
                nson.stringValue(val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readBoolean(String fieldName, boolean val) {
            try {
                nson.booleanValue(val);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readEnum(String fieldName, FieldDef def, int index) {
            try {
                nson.stringValue(def.asEnum().getValues()[index]);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readNull(String fieldName) {
            try {
                nson.nullValue();
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readJsonNull(String fieldName) {
            try {
                nson.jsonNullValue();
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void readEmpty(String fieldName) {
            try {
                nson.emptyValue();
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void startRecord(String fieldName, FieldDef def, int size) {
            startMapInternal(fieldName, size);
        }

        @Override
        public void endRecord(int size) {
            endMapInternal(size);
        }

        @Override
        public void startMap(String fieldName, FieldDef def, int size) {
            startMapInternal(fieldName, size);
        }

        @Override
        public void startMapField(String fieldName) {
            try {
                nson.startMapField(fieldName);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void endMapField(String fieldName) {
            try {
                nson.endMapField(fieldName);
            } catch (IOException e) {
                handleIOException(fieldName, e);
            }
        }

        @Override
        public void endMap(int size) {
            endMapInternal(size);
        }

        private void startMapInternal(String fieldName, int size) {
            try {
                nson.startMap(size);
            } catch (IOException e) {
                handleIOException("startMap " + fieldName, e);
            }
        }

        private void endMapInternal(int size) {
            try {
                nson.endMap(size);
            } catch (IOException e) {
                handleIOException("endMap", e);
            }
        }

        @Override
        public void startArray(String fieldName,
                               FieldDef def,
                               FieldDef elemDef,
                               int size) {
            try {
                nson.startArray(size);
            } catch (IOException e) {
                handleIOException("startArray " + fieldName, e);
            }
        }

        @Override
        public void startArrayField(int index) {
            try {
                nson.startArrayField(index);
            } catch (IOException e) {
                handleIOException("startArrayField", e);
            }
        }

        @Override
        public void endArrayField(int index) {
            try {
                nson.endArrayField(index);
            } catch (IOException e) {
                handleIOException("endArrayField", e);
            }
        }

        @Override
        public void endArray(int size) {
            try {
                nson.endArray(size);
            } catch (IOException e) {
                handleIOException("endArray", e);
            }
        }

        @Override
        public void readCounterCRDT(String fieldName, FieldValueImpl val) {
            if (!keepMRCounterValues) {
                try {
                    /* collapse the counter into a single numeric value */
                    if (val.getType() == Type.INTEGER) {
                        nson.integerValue(val.asInteger().get());
                    } else if (val.getType() == Type.LONG) {
                        nson.longValue(val.asLong().get());
                    } else if (val.getType() == Type.NUMBER) {
                        nson.numberValue(val.asNumber().get());
                    } else {
                        throw new IllegalArgumentException(
                            "MR Counters of type " + val.getType() +
                            " are not supported");
                    }
                } catch (IOException ioe) {
                    throw new IllegalStateException(
                        "Can't create NSON value: " + ioe.getMessage());
                }
                return;
            }

            MRCounterValueSerializer counter = val.asMRCounterSerializer();
            int size = counter.size();
            try {
                nson.startMap(size);
                Iterator<Entry<Integer, FieldValueSerializer>> itr =
                    counter.iterator();
                while (itr.hasNext()) {
                    Entry<Integer, FieldValueSerializer> entry = itr.next();
                    String regionKey = Integer.toString(entry.getKey());
                    nson.startMapField(regionKey);
                    if (val.getType() == Type.INTEGER) {
                        nson.integerValue(entry.getValue().getInt());
                    } else if (val.getType() == Type.LONG) {
                        nson.longValue(entry.getValue().getLong());
                    } else if (val.getType() == Type.NUMBER) {
                        byte[] numBytes = entry.getValue().getNumberBytes();
                        Object numberVal = NumberUtils.deserialize(numBytes,
                                                                   true);
                        nson.numberValue((BigDecimal) numberVal);
                    } else {
                        throw new IllegalArgumentException("The MRCounter " +
                            "for type " + val.getType() + " is not supported");
                    }
                    nson.endMapField(regionKey);
                }
                nson.endMap(size);
            } catch (IOException e) {
                handleIOException("readCounterCRDT", e);
            }
        }
    }
}
