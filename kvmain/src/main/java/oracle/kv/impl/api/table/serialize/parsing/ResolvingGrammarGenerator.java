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
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.kv.impl.api.table.serialize.parsing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.BinaryValueImpl;
import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.EnumValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMapEntry;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FixedBinaryDefImpl;
import oracle.kv.impl.api.table.FixedBinaryValueImpl;
import oracle.kv.impl.api.table.FloatValueImpl;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.NumberValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.StringValueImpl;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.serialize.AvroEncoder;
import oracle.kv.impl.api.table.serialize.SerializationRuntimeException;

/**
 * The class that generates a resolving grammar to resolve between two
 * schemas.
 */
public class ResolvingGrammarGenerator extends ValidatingGrammarGenerator {

    /**
     * Resolves the writer schema <tt>writer</tt> and the reader schema
     * <tt>reader</tt> and returns the start symbol for the grammar generated.
     *
     * @param writer
     *            The FieldDefImpl used by the writer
     * @param reader
     *            The FieldDefImpl used by the reader
     * @return The start symbol for the resolving grammar
     * @throws IOException
     */
    public final Symbol generate(FieldDefImpl writer, FieldDefImpl reader)
            throws IOException {
        return Symbol
                .root(generate(writer, reader, new HashMap<LitS, Symbol>()));
    }

    /**
     * Check if reader and writer is nullable and handle nullable field
     *
     * @param writerFme
     *            The FieldMapEntry for writer
     * @param readerFme
     *            The FieldMapEntry for reader
     * @param seen
     *            The &lt;reader, writer&gt; to symbol map of start symbols of
     *            resolving grammars so far.
     * @return The start symbol for resolving grammar
     * @throws IOException
     */
    private Symbol generateNullable(FieldMapEntry writerFme,
            FieldMapEntry readerFme, Map<LitS, Symbol> seen)
            throws IOException {
        if (writerFme.isNullable()) {
            return resolveUnion(writerFme, readerFme, seen);
        } else if (readerFme.isNullable()) {
            Symbol ret = null;
            if (readerFme.getDefaultValue().isNull()) {
                int j = bestBranch(readerFme.getFieldDef(),
                        writerFme.getFieldDef()) ? 1 : -1;
                if (j == 1) {
                    Symbol s = generate(writerFme.getFieldDef(),
                            readerFme.getFieldDef(), seen);
                    ret = Symbol.seq(Symbol.unionAdjustAction(j, s),
                            Symbol.UNION);
                } else {
                    ret = Symbol.error("Found " + writerFme.getFieldName()
                            + ", expecting " + readerFme.getFieldName());
                }
            } else {
                int j = bestBranch(readerFme.getFieldDef(),
                        writerFme.getFieldDef()) ? 0 : -1;
                if (j == 0) {
                    Symbol s = generate(writerFme.getFieldDef(),
                            readerFme.getFieldDef(), seen);
                    ret = Symbol.seq(Symbol.unionAdjustAction(j, s),
                            Symbol.UNION);
                } else {
                    ret = Symbol.error("Found " + writerFme.getFieldName()
                            + ", expecting " + readerFme.getFieldName());
                }
            }
            return ret;
        }
        return generate(writerFme.getFieldDef(), readerFme.getFieldDef(), seen);

    }

    private Symbol generateSymbolForNull(FieldMapEntry wfme,
            FieldMapEntry rfme) {
        final FieldDefImpl writer = wfme.getFieldDef();
        final FieldDefImpl reader = rfme.getFieldDef();
        final FieldDefImpl.Type writerType = writer.getType();
        final FieldDefImpl.Type readerType = reader.getType();

        if (writerType == readerType) {
            return Symbol.NULL;
        }
        return Symbol.error("Found " + wfme.getFieldName() + ", expecting "
                + rfme.getFieldName());
    }

    /**
     * Resolves the writer schema <tt>writer</tt> and the reader schema
     * <tt>reader</tt> and returns the start symbol for the grammar generated.
     * If there is already a symbol in the map <tt>seen</tt> for resolving the
     * two schemas, then that symbol is returned. Otherwise a new symbol is
     * generated and returned.
     *
     * @param writer
     *            The FieldDefImpl used by the writer
     * @param reader
     *            The FieldDefImpl used by the reader
     * @param seen
     *            The &lt;reader, writer&gt; to symbol map of start symbols of
     *            resolving grammars so far.
     * @return The start symbol for the resolving grammar
     * @throws IOException
     */
    private Symbol generate(FieldDefImpl writer, FieldDefImpl reader,
            Map<LitS, Symbol> seen) throws IOException {
        final FieldDefImpl.Type writerType = writer.getType();
        final FieldDefImpl.Type readerType = reader.getType();

        if (writerType == readerType) {
            if (writer.isMRCounter()) {
                return Symbol.seq(Symbol.repeat(Symbol.CRDT_END,
                                  generate(writer.getCRDTElement(),
                                           reader.getCRDTElement(),
                                           seen),
                                           Symbol.INT),
                                  Symbol.CRDT_START, Symbol.INT);
            }
            switch (writerType) {
            case BOOLEAN:
                return Symbol.BOOLEAN;
            case INTEGER:
                return Symbol.INT;
            case LONG:
                return Symbol.LONG;
            case FLOAT:
                return Symbol.FLOAT;
            case DOUBLE:
                return Symbol.DOUBLE;
            case STRING:
                if (writer.isUUIDString()) {
                    return Symbol.BYTES;
                }
                return Symbol.STRING;
            case FIXED_BINARY:
                if (((FixedBinaryDefImpl) writer).getName()
                        .equals(((FixedBinaryDefImpl) reader).getName())
                        && ((FixedBinaryDefImpl) writer)
                                .getSize() == ((FixedBinaryDefImpl) reader)
                                        .getSize()) {
                    return Symbol.seq(
                            Symbol.intCheckAction(
                                    ((FixedBinaryDefImpl) writer).getSize()),
                            Symbol.FIXED);
                }
                break;

            case ENUM:
                if (((EnumDefImpl) writer).getName() == null
                        || ((EnumDefImpl) writer).getName()
                                .equals(((EnumDefImpl) reader).getName())) {
                    return Symbol.seq(
                            mkEnumAdjust(((EnumDefImpl) writer).getValues(),
                                    ((EnumDefImpl) reader).getValues()),
                            Symbol.ENUM);
                }
                break;

            case ARRAY:
                return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END,
                        generate(((ArrayDefImpl) writer).getElement(),
                                ((ArrayDefImpl) reader).getElement(), seen)),
                        Symbol.ARRAY_START);

            case MAP:
                return Symbol.seq(
                        Symbol.repeat(Symbol.MAP_END,
                                generate(((MapDefImpl) writer).getElement(),
                                        ((MapDefImpl) reader).getElement(),
                                        seen),
                                Symbol.STRING),
                        Symbol.MAP_START);
            case RECORD:
                return resolveRecords((RecordDefImpl) writer,
                        (RecordDefImpl) reader, seen);
            case JSON: // JSON is temporarily encased in a byte[]
                return Symbol.BYTES;
            case BINARY:
                return Symbol.BYTES;
            case TIMESTAMP:
                return Symbol.BYTES;
            case NUMBER:
                return Symbol.BYTES;
            case ANY:
            case ANY_ATOMIC:
            case ANY_JSON_ATOMIC:
            case ANY_RECORD:
                throw new IllegalStateException(
                        "Wildcard types are not invalid: " + writerType);
            default:
                throw new IllegalStateException("Unknown type: " + writerType);
            }
        } else { // writer and reader are of different types
            // this is for future type promotion
            switch (readerType) {
            case LONG:
                switch (writerType) {
                case INTEGER:
                    return Symbol.resolve(super.generate(writer, seen),
                            Symbol.LONG);
                default:
                    break;
                }
                break;

            case FLOAT:
                switch (writerType) {
                case INTEGER:
                case LONG:
                    return Symbol.resolve(super.generate(writer, seen),
                            Symbol.FLOAT);
                default:
                    break;
                }
                break;

            case DOUBLE:
                switch (writerType) {
                case INTEGER:
                case LONG:
                case FLOAT:
                    return Symbol.resolve(super.generate(writer, seen),
                            Symbol.DOUBLE);
                default:
                    break;
                }
                break;

            case BOOLEAN:
            case INTEGER:
            case ENUM:
            case ARRAY:
            case MAP:
            case RECORD:
            case FIXED_BINARY:
            case TIMESTAMP:
            case NUMBER:
            case BINARY:
            case JSON:
            case STRING:
                break;
            default:
                throw new RuntimeException("Unexpected type: " + readerType);
            }
        }
        return Symbol
                .error("Found " + writerType + ", expecting " + readerType);
    }

    private Symbol resolveUnion(FieldMapEntry writer, FieldMapEntry reader,
            Map<LitS, Symbol> seen) throws IOException {
        final int size = 2;
        Symbol[] symbols = new Symbol[size];
        String[] labels = new String[size];
        int j = 0;

        /**
         * We construct a symbol without filling the arrays. Please see
         * {@link Symbol#production} for the reason.
         */
        if (writer.getDefaultValue().isNull()) {
            if (reader.isNullable()) {
                if (reader.getDefaultValue().isNull()) {
                    j = 0;
                    labels[0] = NULL;
                    Symbol s = generateSymbolForNull(writer, reader);
                    symbols[0] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                            Symbol.UNION);
                    labels[1] = writer.getFieldName();
                    j = bestBranch(reader.getFieldDef(), writer.getFieldDef())
                            ? 1
                            : -1;
                    if (j == 1) {
                        s = generate(writer.getFieldDef(), reader.getFieldDef(),
                                seen);
                        symbols[1] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                                Symbol.UNION);
                    } else {
                        symbols[1] = Symbol.error("Found "
                                + writer.getFieldName() + ", expecting "
                                + reader.getFieldName());
                    }
                } else {
                    j = 1;
                    labels[0] = NULL;
                    Symbol s = generateSymbolForNull(writer, reader);
                    symbols[0] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                            Symbol.UNION);
                    labels[1] = writer.getFieldName();
                    j = bestBranch(reader.getFieldDef(), writer.getFieldDef())
                            ? 0
                            : -1;
                    if (j == 0) {
                        s = generate(writer.getFieldDef(), reader.getFieldDef(),
                                seen);
                        symbols[1] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                                Symbol.UNION);
                    } else {
                        symbols[1] = Symbol.error("Found "
                                + writer.getFieldName() + ", expecting "
                                + reader.getFieldName());
                    }
                }
            } else {// reader is not nullable
                symbols[0] = generateSymbolForNull(writer, reader);
                labels[0] = NULL;
                symbols[1] = generate(writer.getFieldDef(),
                        reader.getFieldDef(), seen);
                labels[1] = writer.getFieldName();
            }
        } else {
            if (reader.isNullable()) {
                if (reader.getDefaultValue().isNull()) {
                    j = 0;
                    labels[1] = NULL;
                    Symbol s = generateSymbolForNull(writer, reader);
                    symbols[1] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                            Symbol.UNION);
                    labels[0] = writer.getFieldName();
                    j = bestBranch(reader.getFieldDef(), writer.getFieldDef())
                            ? 1
                            : -1;
                    if (j == 1) {
                        s = generate(writer.getFieldDef(), reader.getFieldDef(),
                                seen);
                        symbols[0] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                                Symbol.UNION);
                    } else {
                        symbols[0] = Symbol.error("Found "
                                + writer.getFieldName() + ", expecting "
                                + reader.getFieldName());
                    }
                } else {
                    j = 1;
                    labels[1] = NULL;
                    Symbol s = generateSymbolForNull(writer, reader);
                    symbols[1] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                            Symbol.UNION);
                    labels[0] = writer.getFieldName();
                    j = bestBranch(reader.getFieldDef(), writer.getFieldDef())
                            ? 0
                            : -1;
                    if (j == 0) {
                        s = generate(writer.getFieldDef(), reader.getFieldDef(),
                                seen);
                        symbols[0] = Symbol.seq(Symbol.unionAdjustAction(j, s),
                                Symbol.UNION);
                    } else {
                        symbols[0] = Symbol.error("Found "
                                + writer.getFieldName() + ", expecting "
                                + reader.getFieldName());
                    }
                }
            } else {
                symbols[0] = generate(writer.getFieldDef(),
                        reader.getFieldDef(), seen);
                labels[0] = writer.getFieldName();
                symbols[1] = generateSymbolForNull(writer, reader);
                labels[1] = NULL;
            }
        }

        return Symbol.seq(Symbol.alt(symbols, labels),
                Symbol.writerUnionAction());
    }

    private Symbol resolveRecords(RecordDefImpl writer, RecordDefImpl reader,
            Map<LitS, Symbol> seen) throws IOException {
        LitS wsc = new LitS2(writer, reader);
        Symbol result = seen.get(wsc);
        if (result == null) {
            List<String> wfname = writer.getFieldNames();
            List<String> rfname = reader.getFieldNames();

            /*
             * First, compute reordering of reader fields, plus number elements
             * in the result's production
             */
            FieldMapEntry[] reordered = new FieldMapEntry[reader
                    .getNumFields()];
            int ridx = 0;
            int count = 1 + writer.getNumFields();

            for (String f : wfname) {
                FieldDefImpl rdrField = reader.getFieldDef(f);
                if (rdrField != null) {
                    FieldMapEntry fme = reader.getFieldMapEntry(f, true);
                    reordered[ridx++] = fme;
                }
            }

            for (String rf : rfname) {
                if (writer.getFieldDef(rf) == null) {
                    FieldValueImpl fv = reader.getDefaultValue(rf);
                    if ((fv == null || fv.isNull()) && !reader.isNullable(rf)) {
                        result = Symbol.error("Found " + writer.getName()
                                + ", expecting " + reader.getName()
                                + ", missing required field " + rf);
                        seen.put(wsc, result);
                        return result;
                    }
                    reordered[ridx++] = reader.getFieldMapEntry(rf, true);
                    count += 3;

                }
            }

            Symbol[] production = new Symbol[count];
            production[--count] = Symbol.fieldOrderAction(reordered);

            /**
             * We construct a symbol without filling the array. Please see
             * {@link Symbol#production} for the reason.
             */
            result = Symbol.seq(production);
            seen.put(wsc, result);

            /*
             * For now every field in read-record with no default value must be
             * in write-record. Write record may have additional fields, which
             * will be skipped during read.
             */

            // Handle all the writer's fields
            for (String fname : wfname) {
                FieldDefImpl rf = reader.getFieldDef(fname);
                FieldMapEntry wfme = writer.getFieldMapEntry(fname, true);
                if (rf == null) {
                    production[--count] = Symbol
                            .skipAction(generateNullable(wfme, wfme, seen));
                } else {
                    FieldMapEntry rfme = reader.getFieldMapEntry(fname, true);
                    production[--count] = generateNullable(wfme, rfme, seen);
                }
            }

            // Add default values for fields missing from Writer
            for (String fname : rfname) {
                FieldMapEntry rfme = reader.getFieldMapEntry(fname, true);
                FieldDefImpl wf = writer.getFieldDef(fname);
                if (wf == null) {
                    byte[] bb = getBinary(rfme, reader.getDefaultValue(fname));
                    production[--count] = Symbol.defaultStartAction(bb);
                    production[--count] = generateNullable(rfme, rfme, seen);
                    production[--count] = Symbol.DEFAULT_END_ACTION;
                }
            }
        }
        return result;
    }

    /**
     * Returns the binary encoded version of <tt>n</tt> according to the
     * FieldMapEntry <tt>s</tt>.
     *
     * @param fme
     *            The FieldMapEntry for encoding
     * @param n
     *            The FieldValueImpl to be encoded.
     * @return The binary encoded version of <tt>n</tt>.
     * @throws IOException
     */
    private static byte[] getBinary(FieldMapEntry s, FieldValueImpl n)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroEncoder e = new AvroEncoder(out, 32, null);
        encodeNullable(e, s, n);
        e.flush();
        return out.toByteArray();
    }

    private static void encodeNullable(AvroEncoder encoder, FieldMapEntry fme,
            FieldValueImpl n) throws IOException {
        FieldValueSerializer fv = n;

        if (fv == null || fv.isNull()) {
            if (fv == null) {
                fv = fme.getDefaultValue();
            }
            if (fv.isNull()) {
                if (!fme.isNullable()) {
                    String fieldName = fme.getFieldName();
                    throw new IllegalCommandException(
                            "The field can not be null: " + fieldName);
                }
                /*
                 * null is always the first choice in the union when there is no
                 * default values
                 */
                encoder.writeIndex(fme.hasDefaultValue() ? 1 : 0);
                encoder.writeNull();
                return;
            }
        }

        if (fme.isNullable()) {
            /*
             * nullable fields with a default value generate schemas with the
             * default type first in the union.
             */
            encoder.writeIndex(fme.hasDefaultValue() ? 0 : 1);
        }

        encode(encoder, fme.getFieldDef(), n);
    }

    /**
     * Encodes the given FieldValueImpl <tt>n</tt> on to the encoder <tt>e</tt>
     * according to the FieldDefImpl <tt>s</tt>.
     *
     * @param e
     *            The encoder to encode into.
     * @param s
     *            The FieldDefImpl for the object being encoded.
     * @param n
     *            The FieldValueImpl to encode.
     * @throws IOException
     */

    private static void encode(AvroEncoder e, FieldDefImpl s, FieldValueImpl n)
            throws IOException {
        switch (s.getType()) {
        case RECORD:
            for (String name : ((RecordDefImpl) s).getFieldNames()) {
                FieldMapEntry fme = ((RecordDefImpl) s).getFieldMapEntry(name,
                        true);
                FieldValueImpl v = null;
                if (n == null || n.isNull()) {
                    v = ((RecordDefImpl) s).getDefaultValue(name);
                } else {
                    v = ((RecordValueImpl) n).get(name);
                }
                if (v == null) {
                    throw new SerializationRuntimeException(
                            "No default value for: " + name);
                }
                encodeNullable(e, fme, v);
            }
            break;
        case ENUM:
            e.writeEnum(((EnumDefImpl) s).indexOf(((EnumValueImpl) n).get()));
            break;
        case FIXED_BINARY:
            if (!n.isFixedBinary())
                throw new SerializationRuntimeException(
                        "Non-string default value for fixed: " + n);
            byte[] bb = ((FixedBinaryValueImpl) n).get();
            if (bb.length != ((FixedBinaryDefImpl) s).getSize()) {
                bb = Arrays.copyOf(bb, ((FixedBinaryDefImpl) s).getSize());
            }
            e.writeFixed(bb);
            break;
        case STRING:
            if (!n.isString())
                throw new SerializationRuntimeException(
                        "Non-string default value for string: " + n);
            e.writeString(((StringValueImpl) n).get());
            break;
        case TIMESTAMP:
            if (!n.isTimestamp())
                throw new SerializationRuntimeException(
                        "Non-string default value for timestamp: " + n);
            e.writeBytes(((TimestampValueImpl) n).getTimestampBytes());
            break;
        case NUMBER:
            if (!n.isNumeric())
                throw new SerializationRuntimeException(
                        "Non-string default value for number: " + n);
            if (s.isMRCounter()) {
                if (n.getDecimal().compareTo(BigDecimal.ZERO) != 0) {
                    throw new SerializationRuntimeException(
                        "Non-zero default value for number CRDT: " + n);
                }
                writeCRDTDefault(e);
            } else {
                e.writeBytes(((NumberValueImpl) n).getNumberBytes());
            }
            break;
        case BINARY:
            if (!n.isBinary())
                throw new SerializationRuntimeException(
                        "Non-string default value for binary: " + n);
            e.writeBytes(((BinaryValueImpl) n).getBytes());
            break;
        case INTEGER:
            if (!n.isNumeric())
                throw new SerializationRuntimeException(
                        "Non-numeric default value for int: " + n);
            if (s.isMRCounter()) {
                if (n.getInt() != 0) {
                    throw new SerializationRuntimeException(
                        "Non-zero default value for int CRDT: " + n);
                }
                writeCRDTDefault(e);
            } else {
                e.writeInt(((IntegerValueImpl) n).get());
            }
            break;
        case LONG:
            if (!n.isNumeric())
                throw new SerializationRuntimeException(
                        "Non-numeric default value for long: " + n);
            if (s.isMRCounter()) {
                if (n.getLong() != 0) {
                    throw new SerializationRuntimeException(
                        "Non-zero default value for long CRDT: " + n);
                }
                writeCRDTDefault(e);
            } else {
                e.writeLong(((LongValueImpl) n).get());
            }
            break;
        case FLOAT:
            if (!n.isNumeric())
                throw new SerializationRuntimeException(
                        "Non-numeric default value for float: " + n);
            e.writeFloat(((FloatValueImpl) n).get());
            break;
        case DOUBLE:
            if (!n.isNumeric())
                throw new SerializationRuntimeException(
                        "Non-numeric default value for double: " + n);
            e.writeDouble(((DoubleValueImpl) n).get());
            break;
        case BOOLEAN:
            if (!n.isBoolean())
                throw new SerializationRuntimeException(
                        "Non-boolean default for boolean: " + n);
            e.writeBoolean(((BooleanValueImpl) n).get());
            break;
        case ARRAY: // Confirmed Array does not have default value
        case MAP: // Confirmed Map does not have default value
        case JSON: // Confirmed no default value for JSON type
            break;
         default:
             break;
        }
    }

    private static void writeCRDTDefault(AvroEncoder e)
        throws IOException {
        e.writeInt(FieldValueImpl.CounterVersion.COUNTER_V1.ordinal());
        e.writeCRDTStart();
        /* The default value for CRDT columns must be 0. */
        e.setItemCount(0);
        e.writeCRDTEnd();
    }

    private static Symbol mkEnumAdjust(String[] writer, String[] reader) {
        LockableArrayList<String> wsymbols = new LockableArrayList<String>();
        for (String w : writer) {
            wsymbols.add(w);
        }
        LockableArrayList<String> rsymbols = new LockableArrayList<String>();
        for (String r : reader) {
            rsymbols.add(r);
        }

        Object[] adjustments = new Object[wsymbols.size()];
        for (int i = 0; i < adjustments.length; i++) {
            int j = rsymbols.indexOf(wsymbols.get(i));
            adjustments[i] = (j == -1 ? "No match for " + wsymbols.get(i)
                    : Integer.valueOf(j));
        }
        return Symbol.enumAdjustAction(rsymbols.size(), adjustments);
    }

    /*
     *  UNION we only have NULL/ TYPE two options.
     *  this is for the case that reader is an UNION but writer is not
     */
    private static boolean bestBranch(FieldDefImpl r, FieldDefImpl w) {
        FieldDefImpl.Type vt = w.getType();
        // first scan for exact match
        boolean find = false;

        if (vt == r.getType()) {
            if (vt == FieldDefImpl.Type.RECORD || vt == FieldDefImpl.Type.ENUM
                    || vt == FieldDefImpl.Type.FIXED_BINARY) {
                String wname = null;
                String rname = null;
                switch (vt) {
                case RECORD:
                    wname = ((RecordDefImpl) w).getName();
                    rname = ((RecordDefImpl) r).getName();
                    break;
                case ENUM:
                    wname = ((EnumDefImpl) w).getName();
                    rname = ((EnumDefImpl) r).getName();
                    break;
                case FIXED_BINARY:
                    wname = ((FixedBinaryDefImpl) w).getName();
                    rname = ((FixedBinaryDefImpl) r).getName();
                    break;
                default:
                    break;
                }

                if ((wname != null && wname.equals(rname))
                        || wname == rname && vt == FieldDefImpl.Type.RECORD) {
                    find = true;
                }
            } else {
                find = true;
            }
        }

        /*
         * then scan match via numeric promotion, currently we don't allow any
         * type promotion, so commented out for future use.
         */
        /*switch (vt) {
        case INTEGER:
            switch (r.getType()) {
            case LONG:
            case DOUBLE:
                return j;
            }
            break;
        case LONG:
        case FLOAT:
            switch (r.getType()) {
            case DOUBLE:
                return j;
            }
            break;
        default:
            break;
        }*/

        return find;
    }

    /**
     * Clever trick which differentiates items put into <code>seen</code> by
     * {@code ValidatingGrammarGenerator.validating} from those put
     * in by {@code ValidatingGrammarGenerator.resolving}.
     */
    static class LitS2 extends ValidatingGrammarGenerator.LitS {
        public FieldDefImpl expected;

        public LitS2(FieldDefImpl actual, FieldDefImpl expected) {
            super(actual);
            this.expected = expected;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LitS2))
                return false;
            LitS2 other = (LitS2) o;
            return actual == other.actual && expected == other.expected;
        }

        @Override
        public int hashCode() {
            return super.hashCode() + expected.hashCode();
        }
    }

    /*
     * This class keeps a boolean variable <tt>locked</tt> which is set to
     * <tt>true</tt> in the lock() method. It's legal to call lock() any number
     * of times. Any lock() other than the first one is a no-op.
     *
     * This class throws <tt>IllegalStateException</tt> if a mutating operation
     * is performed after being locked. Since modifications through iterator
     * also use the list's mutating operations, this effectively blocks all
     * modifications.
     */
    static class LockableArrayList<E> extends ArrayList<E> {
        private static final long serialVersionUID = 1L;
        private boolean locked = false;

        public LockableArrayList() {
        }

        public LockableArrayList(int size) {
            super(size);
        }

        public LockableArrayList(List<E> types) {
            super(types);
        }

        public List<E> lock() {
            locked = true;
            return this;
        }

        private void ensureUnlocked() {
            if (locked) {
                throw new IllegalStateException();
            }
        }

        @Override
        public boolean add(E e) {
            ensureUnlocked();
            return super.add(e);
        }

        @Override
        public boolean remove(Object o) {
            ensureUnlocked();
            return super.remove(o);
        }

        @Override
        public E remove(int index) {
            ensureUnlocked();
            return super.remove(index);
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            ensureUnlocked();
            return super.addAll(c);
        }

        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
            ensureUnlocked();
            return super.addAll(index, c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            ensureUnlocked();
            return super.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            ensureUnlocked();
            return super.retainAll(c);
        }

        @Override
        public void clear() {
            ensureUnlocked();
            super.clear();
        }

    }
}
