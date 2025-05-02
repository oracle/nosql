/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;

import oracle.nosql.nson.Nson;
import oracle.nosql.nson.Nson.NsonSerializer;
import oracle.nosql.nson.util.NioByteInputStream;
import oracle.nosql.nson.util.NioByteOutputStream;
import oracle.nosql.nson.values.FieldValueEventHandler;

/**
 * Methods to serialize FieldValue to NSON and deserialize NSON to
 * FieldValue. This is not static, although it could be. Being not static
 * allows some configuration, e.g.
 *  o MR counter representation (map vs single value)
 *  o types supported (JSON, not JSON)
 *
 * TODO:
 * 1. maybe combine these with writeJsonCollection* methods in TableImpl.
 * Differences: record and other schema types, handling of JSON fields in
 * tables with schema with and without MR counters, putting this code in the
 * product vs test
 */
public class NsonTest {

    /* if true, write MR counter values into NSON as a map */
    private boolean writeMRCountersAsMap;

    public NsonTest() {

    }

    public NsonTest setWriteMRCountersAsMap(boolean value) {
        this.writeMRCountersAsMap = value;
        return this;
    }

    /*
     * The code that creates NSON from RowImpl or FieldValue will write
     * MR counters as single values and not the underlying map
     */
    public byte[] createNsonFromRow(RowImpl row) {
        try (NioByteOutputStream out =
             NioByteOutputStream.createNioByteOutputStream()) {
            NsonSerializer ns = new NsonSerializer(out);
            if (row instanceof JsonCollectionRowImpl) {
                MapValueImpl map =
                    ((JsonCollectionRowImpl)row).getJsonCollectionMap();
                writeNsonMap(map, ns);
            } else {
                writeNsonRecord(row, ns, row.getTable());
            }
            return  Arrays.copyOfRange(out.array(), 0, out.getOffset());
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "IOException while serializing a row to NSON",ioe);
        }
    }

    public byte[] createNsonFromValue(FieldValueImpl field) {
        try (NioByteOutputStream out =
             NioByteOutputStream.createNioByteOutputStream()) {
            NsonSerializer ns = new NsonSerializer(out);

            writeNsonField(field, ns, null);
            return  Arrays.copyOfRange(out.array(), 0, out.getOffset());
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "IOException while serializing a row to NSON",ioe);
        }
    }

    /*
     * Write JSON Collection table fields to NsonSerializer.
     */
    private void writeNsonField(FieldValueImpl value,
                                NsonSerializer ns,
                                String fieldName)
        throws IOException {

        if (value.isMRCounter() && writeMRCountersAsMap) {
            writeMRCounterMap(value, ns);
            return;
        }

        switch(value.getType()) {
        case INTEGER:
            ns.integerValue(value.asInteger().get());
            break;
        case LONG:
            ns.longValue(value.asLong().get());
            break;
        case DOUBLE:
            ns.doubleValue(value.asDouble().get());
            break;
        case FLOAT:
            ns.doubleValue(value.castAsDouble());
            break;
        case STRING:
            ns.stringValue(value.asString().get());
            break;
        case BOOLEAN:
            ns.booleanValue(value.asBoolean().get());
            break;
        case NUMBER:
            ns.numberValue(value.asNumber().get());
            break;
        case BINARY:
        case FIXED_BINARY:
            ns.binaryValue(value.asBinary().get());
            break;
        case TIMESTAMP:
            ns.timestampValue(
                new oracle.nosql.nson.values.TimestampValue(
                    ((TimestampValueImpl)value.asTimestamp()).get()));
            break;
        case ENUM:
            /* use string value */
            ns.stringValue(value.toString());
            break;
        case MAP:
            writeNsonMap((MapValueImpl)value, ns);
            break;
        case RECORD:
            writeNsonRecord((RecordValueImpl)value, ns, null);
            break;
        case ARRAY:
            writeNsonArray((ArrayValueImpl)value, ns);
            break;
        case ANY_JSON_ATOMIC:
            /* this is probably a json null */
            if (value instanceof NullJsonValueImpl) {
                ns.jsonNullValue();
                break;
            }
            // $FALL-THROUGH$
        default:
            throw new IllegalArgumentException
                ("Unexpected NSON type: " + value.getType() +
                 " for field name: " + fieldName);
        }
    }

    /*
     * Write an MR counter value as a map:
     * {
     *  "<regionid>" : value
     * }
     */
    private void writeMRCounterMap(FieldValueImpl value,
                                   NsonSerializer ns)
        throws IOException {
        Set<Entry<Integer, FieldValueImpl>> fields =
            value.getMRCounterMap().entrySet();
        ns.startMap(fields.size());
        for (Entry<Integer, FieldValueImpl> entry : fields) {
            String key = String.valueOf(entry.getKey());
            FieldValueImpl entryValue = entry.getValue();
            ns.startMapField(key);
            switch(value.getType()) {
            case INTEGER:
                ns.integerValue(entryValue.asInteger().get());
                break;
            case LONG:
                ns.longValue(entryValue.asLong().get());
                break;
            case NUMBER:
                ns.numberValue(entryValue.asNumber().get());
                break;
            default:
                throw new IllegalStateException("bad counter type: " +
                                                value.getType());
            }
            ns.endMapField(key);
        }

        ns.endMap(fields.size());
    }

    /*
     * Record becomes NSON map
     * If table is non-null use it to skip top-level primary key fields
     */
    private void writeNsonRecord(RecordValueImpl record,
                                 NsonSerializer ns,
                                 TableImpl table) throws IOException {
        FieldMap fieldMap = record.getDefinition().getFieldMap();

        ns.startMap(record.size());
        for (int pos = 0; pos < fieldMap.size(); ++pos) {
            /* skip primary key fields */
            if (table != null && table.isPrimKeyAtPos(pos)) {
                continue;
            }
            FieldMapEntry fme = fieldMap.getFieldMapEntry(pos);
            FieldValueImpl fvi = record.get(pos);
            final String key = fme.getFieldName();

            ns.startMapField(key);
            writeNsonField(fvi, ns, key);
            ns.endMapField(key);
        }
        ns.endMap(record.size());
    }

    /*
     * NOTE: map has no key fields in it, so no skipping is required
     */
    private void writeNsonMap(MapValueImpl map,
                              NsonSerializer ns) throws IOException {

        Iterator<Entry<String, FieldValueSerializer>> iter = map.iterator();
        ns.startMap(map.size());
        while(iter.hasNext()) {

            Entry<String, FieldValueSerializer> entry = iter.next();
            String key = entry.getKey();
            ns.startMapField(key);
            writeNsonField((FieldValueImpl) entry.getValue(), ns, key);
            ns.endMapField(key);
        }
        ns.endMap(map.size());
    }

    private void writeNsonArray(ArrayValueImpl array,
                                NsonSerializer ns)
        throws IOException {

        ns.startArray(array.size());
        Iterator<FieldValueSerializer> iter = array.iterator();
        int index = 0;
        while(iter.hasNext()) {
            ns.startArrayField(index);
            writeNsonField((FieldValueImpl) iter.next(), ns, null);
            ns.endArrayField(index++);
        }
        ns.endArray(array.size());
    }

    /* read */
    /*
     * NOTE: this will fail if the target row/value has an MR counter in
     * it. Making this work means one or the other of:
     * o allow the "CRDT" version of the numeric value to be created. This
     * means passing in a region and flag to do so
     * o pass a flag to "replace" the CRDT which turns it into an empty (0)
     * MR counter, which is what happens in normal API-based operations
     *
     * This mechanism is mostly for testing NSON deserialization and this
     * use case. Deser from NSON to FieldValue isn't currently needed other
     * than the implementation of TableImpl.readJsonCollectionRow, which
     * handles MR counters in a different way because it assumes the NSON
     * implementation of counters is kept in an NSON map
     */
    public boolean readNsonValue(FieldValueImpl value,
                                 byte[] nsonValue,
                                 int offset) {
        ValueReader<FieldValueImpl> reader =
            new FieldValueReaderImpl<FieldValueImpl>(value);

        try (NioByteInputStream in =
             new NioByteInputStream(ByteBuffer.wrap(nsonValue))) {
            if (offset > 0) {
                in.skip(offset);
            }
            int toff = in.getOffset();
            in.setOffset(toff);

            FieldValueEventHandler handler =
                new TableImpl.ValueReaderEventHandler(reader, null);
            Nson.generateEventsFromNson(handler, in, false);
            return true;
        } catch (IOException ioe) {
            return false;
        }
    }

}
