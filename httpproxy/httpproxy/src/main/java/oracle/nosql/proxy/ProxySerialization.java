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


import static java.lang.Math.toIntExact;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ABSOLUTE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ACTIVE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.CREATING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DROPPED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DROPPING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DURABILITY_SYNC;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DURABILITY_NO_SYNC;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DURABILITY_WRITE_NO_SYNC;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DURABILITY_ALL;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DURABILITY_NONE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.DURABILITY_SIMPLE_MAJORITY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.EVENTUAL;
import static oracle.nosql.proxy.protocol.BinaryProtocol.FORWARD;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ON_DEMAND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.PROVISIONED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.REVERSE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TTL_DAYS;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TTL_HOURS;
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
import static oracle.nosql.proxy.protocol.BinaryProtocol.UNORDERED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.UPDATING;
import static oracle.nosql.proxy.protocol.BinaryProtocol.getTypeName;
import static oracle.nosql.proxy.protocol.Protocol.SERIAL_VERSION;
import static oracle.nosql.proxy.protocol.Protocol.UNSUPPORTED_PROTOCOL;
import static oracle.nosql.proxy.protocol.Protocol.V1;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map.Entry;
import java.util.Stack;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.Version;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.JsonDefImpl;
import oracle.kv.impl.api.table.NullJsonValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NumberUtils;
import oracle.kv.impl.api.table.NumberValueImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableAPIImpl.OpResultWrapper;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.api.table.ValueReader;
import oracle.kv.impl.api.table.ValueSerializer.RowSerializer;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.NumberValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Table;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.TimestampDef;
import oracle.kv.table.TimestampValue;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.nson.util.ByteOutputStream;
import oracle.nosql.proxy.ValueSerializer.RowSerializerImpl;
import oracle.nosql.proxy.protocol.ByteInputStream;
import oracle.nosql.proxy.protocol.Protocol;
import oracle.nosql.proxy.protocol.SerializationUtil;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.TableUsageResponse;
import oracle.nosql.proxy.sc.TableUtils;
import oracle.nosql.util.tmi.IndexInfo;
import oracle.nosql.util.tmi.IndexInfo.IndexField;
import oracle.nosql.util.tmi.ReplicaInfo;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TableUsage;

/**
 * Static serialization of driver/proxy objects and handling of mappings to/from
 * KV table data objects
 */
public class ProxySerialization {

    private static final TableLimits emptyLimits =
        new TableLimits(0, 0, 0);

    /**
     * FieldValueWriterImpl provides a callback method to be called when we
     * serialize a FieldValue encountered during the serialization of the
     * driver-side query plan. Such a value will be deserialized at the driver,
     * so it must be serialized in the format expected by the driver.
     */
    public static class FieldValueWriterImpl implements FieldValueWriter {

        @Override
        public void writeFieldValue(
            DataOutput out,
            FieldValue value) throws IOException {
            ProxySerialization.writeFieldValue((ByteOutputStream)out, value);
        }
    }

    public static void writeGetTableResponse(ByteOutputStream out,
                                             GetTableResponse response,
                                             short serialVersion)
        throws IOException {

        final TableInfo info = response.getTableInfo();
        if (info != null) {
            out.writeBoolean(true);
            writeString(out, info.getCompartmentId());
            writeString(out, info.getTableName());
            out.writeByte(getTableState(info.getStateEnum()));
            if (info.getTableLimits() != null ||
                info.getSchema() != null) {
                /* has static info -- limits and schema */
                out.writeBoolean(true);
                TableLimits limits = info.getTableLimits();
                if (limits == null) {
                    limits = emptyLimits; /* has 0 in all fields */
                }
                writeTableLimits(out, limits, serialVersion);
                writeString(out, info.getSchema());
            } else {
                out.writeBoolean(false);
            }
            writeString(out, info.getOperationId());
        } else {
            out.writeBoolean(false);
        }
    }

    /*
     * Serialization readers
     */
    public static String readString(ByteInputStream in) throws IOException {
        return SerializationUtil.readString(in);
    }

    public static char[] readUTF8ToCharArray(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readUTF8ToCharArray(in);
    }

    public static char[] UTF8ToCharArray(byte[] bytes)
        throws IOException {
        return SerializationUtil.UTF8ToCharArray(bytes);
    }

    public static int skipString(ByteInputStream in) throws IOException {
        return SerializationUtil.skipString(in);
    }

    static String readNonNullString(ByteInputStream in, String name)
        throws IOException {

        String value = readString(in);
        if (value == null) {
            raiseBadProtocolError(name + " must be non-null", in);
        }
        return value;
    }

    public static String readNonNullEmptyString(ByteInputStream in, String name)
        throws IOException {

        String value = readNonNullString(in, name);
        if (value.isEmpty()) {
            raiseBadProtocolError(name + " must be non-empty string", in);
        }
        return value;
    }

    public static short readDriverVersion(ByteInputStream in)
        throws IOException, RequestException {
        short version = in.readShort();
        if (version <= 0) {
            raiseBadProtocolError("Invalid serialVersion: " +
                                  version, in);
        }

        short proxySerialVersion = SERIAL_VERSION;
        if (version > proxySerialVersion) {
            final String errMsg = "Invalid driver serial version " +
                version + ", proxy serial version is " +
                proxySerialVersion;
            throw new RequestException(UNSUPPORTED_PROTOCOL, errMsg);
            //raiseBadProtocolError(errMsg, in);
        }

        return version;
    }

    public static int readTimeout(ByteInputStream in) throws IOException {
        int timeout = SerializationUtil.readPackedInt(in);
        if (timeout < 0) {
            raiseBadProtocolError(
                "Invalid timeout value, it must be non-negative: " +
                timeout, in);
        }
        return timeout;
    }

    public static int readTopologySeqNum(ByteInputStream in) throws IOException {
        int seqNum = SerializationUtil.readPackedInt(in);
        if (seqNum < -1) {
            raiseBadProtocolError(
                "Invalid topology sequence number, it must be > -1: " +
                seqNum, in);
        }
        return seqNum;
    }

    public static int readShardId(ByteInputStream in) throws IOException {
        int sid = SerializationUtil.readPackedInt(in);
        if (sid < -1) {
            raiseBadProtocolError(
                "Invalid shard id value, it must be > -1: " +
                sid, in);
        }
        return sid;
    }

    public static int readNumVariables(ByteInputStream in) throws IOException {
        int num = SerializationUtil.readPackedInt(in);
        if (num < 0) {
            raiseBadProtocolError(
                "Invalid number of query variables, it must be non-negative: " +
                num, in);
        }
        return num;
    }

    public static RowSerializer readPrimaryKeySerializer(ByteInputStream in,
                                                         Table table,
                                                         int keySizeLimit)
        throws IOException {

        /* read the type */
        int t = readValueType(in);
        if (t != TYPE_MAP && t != TYPE_ARRAY) {
            raiseBadProtocolError(
                "PrimaryKey must be of type MAP or ARRAY, but found type " +
                getTypeName(t), in);
        }

        /*
         * The "exact" option is false, that implies the value from driver can
         * contain extra fields than primary key, the non primary key field
         * value will be ignored.
         */
        return new RowSerializerImpl(in, t, table, keySizeLimit,
                                     -1 /* value sz limit ignored */,
                                     true, false /* exact */);
    }

    public static RecordValue readRecord(ByteInputStream in,
                                         RecordDef recordDef,
                                         RecordValue rec)
        throws IOException {

        RecordValue record = (rec != null ? rec : recordDef.createRecord());
        /* read length */
        read4BytesLength(in);
        /* read size */
        int size = read4BytesSize(in);
        for (int i = 0; i < size; i++) {
            String key = readNonNullEmptyString(in, "Field name");

            FieldDef def = recordDef.getFieldDef(key);
            if (def == null) {
                /*
                 * The value from driver can contain extra fields, the unknown
                 * field will be ignored silently.
                 */
                skipValue(in);
                continue;
            }
            FieldValue val = readFieldValue(in, def, false, true);

            record.put(key, val);
        }
        return record;
    }

    private static RecordValue createRecordFromArray(ByteInputStream in,
                                                     RecordDef recordDef,
                                                     RecordValue rec)
        throws IOException {

        RecordValue record = (rec != null ? rec : recordDef.createRecord());

        /* read length */
        read4BytesLength(in);

        /* read size */
        int size = read4BytesSize(in);
        if (size != recordDef.getNumFields()) {
            throw new IllegalArgumentException(
                "Invalid Array value for Record Value, it has " +
                size + (size > 1 ? " elements" : " element") +
                " but the Record Value contains " + recordDef.getNumFields() +
                (recordDef.getNumFields() > 1 ? " fields": " field"));
        }

        for (int i = 0; i < size; i++) {
            FieldDef fdef = recordDef.getFieldDef(i);
            record.put(i, readFieldValue(in, fdef, false, true));
        }
        return record;
    }

    public static RecordValue createRecord(ByteInputStream in,
                                           RecordDef recordDef,
                                           RecordValue rec,
                                           int driverType)
        throws IOException {

        if (driverType == TYPE_MAP) {
            return readRecord(in, recordDef, rec);
        } else if (driverType == TYPE_ARRAY) {
            return createRecordFromArray(in, recordDef, rec);
        }
        throw new IllegalArgumentException("Invalid driver type for Record: " +
                                           getTypeName(driverType));
    }

    public static RowSerializer readRowSerializer(ByteInputStream in,
                                                  Table table,
                                                  int keySizeLimit,
                                                  int valueSizeLimit,
                                                  boolean exact)
        throws IOException {

        /* read the type */
        int t = readValueType(in);
        if (t != TYPE_MAP && t != TYPE_ARRAY) {
            raiseBadProtocolError(
                "Records must be of type MAP or ARRAY, but found type " +
                getTypeName(t), in);
        }
        return new RowSerializerImpl(in, t, table, keySizeLimit,
                                     valueSizeLimit, false, exact);
    }

    public static QueryOperation readQueryOperation(ByteInputStream in)
        throws IOException {

        int val = in.readByte();
        if (val < 0 || val >= QueryOperation.values().length) {
            raiseBadProtocolError("Invalid QueryOperation ordinal: " + val, in);
        }
        return QueryOperation.values()[val];
    }

    /*
     * Serialization writers
     */
    public static void writeString(ByteOutputStream out,
                                   String s) throws IOException {
        SerializationUtil.writeString(out, s);
    }

    public static void writeSuccess(ByteOutputStream out) throws IOException {
        out.writeByte(0);
    }

    public static void writeExpirationTime(ByteOutputStream out,
                                           long expirationTime)
        throws IOException {
        writeLong(out, expirationTime);
    }

    public static void writeModificationTime(ByteOutputStream out,
                                             long modificationTime)
        throws IOException {
        writeLong(out, modificationTime);
    }

    public static void writeVersion(ByteOutputStream out,
                                    Version version)
        throws IOException {

        writeByteArray(out, (version != null ? version.toByteArray() : null));
    }

    public static void writeExistingRow(ByteOutputStream out,
                                        boolean opSucceeded,
                                        ReturnRow rr,
                                        TableAPIImpl tableApi,
                                        RowSerializer rowKey,
                                        Result result,
                                        short serialVersion)
        throws IOException {

        writeExistingValueVersion(out, opSucceeded,
            (rr != null && rr.getReturnChoice() == Choice.ALL),
            rowKey.getTable(),
            result,
            serialVersion,
            new ExistingRowWriter() {
                @Override
                public void write(RowReaderImpl reader) {
                    tableApi.initReturnRowFromResult(rr, rowKey, result, reader);
                }
        });
    }

    /*
     * An interface to write the existing row value to stream.
     */
    private static interface ExistingRowWriter {
        public void write(RowReaderImpl rowReader);
    }

    private static void writeExistingValueVersion(ByteOutputStream out,
                                                  boolean opSucceeded,
                                                  boolean returnInfo,
                                                  Table table,
                                                  Result result,
                                                  short serialVersion,
                                                  ExistingRowWriter rrWriter)
        throws IOException {

        /*
         * Only write this information if:
         *  o the operation failed
         * AND
         *  o returnInfo is true
         * AND
         *  o the information is actually available (e.g. delete of no row)
         */
        if (!returnInfo || opSucceeded) {
            out.writeBoolean(false);
            return;
        }

        int flagOffset = out.getOffset();
        /*
         * Set true to leave a boolean space in the stream for a flag to
         * indicate if has existing row.
         */
        out.writeBoolean(true);

        RowReaderImpl rowReader = new RowReaderImpl(out, table);
        rrWriter.write(rowReader);

        /* No existing row, update the flag at flagOffset to false */
        if (rowReader.done() == 0) {
            out.writeBooleanAtOffset(flagOffset, false);
        } else {
            /* Write version of existing Row */
            writeVersion(out, rowReader.getVersion());
            if (serialVersion > Protocol.V2) {
                if (result != null) {
                    writeModificationTime(out,
                        result.getPreviousModificationTime());
                } else {
                    writeModificationTime(out, rowReader.getModificationTime());
                }
            }
        }
    }

    /**
     * NOTE: at this time readKB comes directly from KV which, for
     * absolute reads will be 2x the actual number of bytes.
     */
    public static void writeConsumedCapacity(ByteOutputStream out,
                                             int readKB,
                                             int writeKB,
                                             boolean isAbsolute)
        throws IOException {

        /*
         * read units (2x readKB if absolute)
         * readKB
         * writeKB
         */
        writeConsumedCapacity(out, readKB,
                              getReadKB(readKB, isAbsolute),
                              writeKB);
    }

    public static int getReadKB(int readUnits, boolean isAbsolute) {
        return (isAbsolute ? readUnits >> 1 : readUnits);
    }

    public static void writeConsumedCapacity(ByteOutputStream out,
                                             int readUnits,
                                             int readKB,
                                             int writeKB)
        throws IOException {

        /*
         * read units
         * readKB
         * writeKB
         */
        writeInt(out, readUnits); // read units
        writeInt(out, readKB); // actual readKB
        writeInt(out, writeKB); // actual write KB == write units
    }

    public static void writeByteArray(ByteOutputStream out,
                                      byte[] array) throws IOException {
        SerializationUtil.writeByteArray(out, array);
    }

    public static void writeInt(ByteOutputStream out, int value)
        throws IOException {
        SerializationUtil.writePackedInt(out, value);
    }

    public static void writeLong(ByteOutputStream out, long value)
        throws IOException {
        SerializationUtil.writePackedLong(out, value);
    }

    public static void writeDouble(ByteOutputStream out,
                                   double value) throws IOException {
        out.writeDouble(value);
    }

    public static void writeNumber(ByteOutputStream out,
                                   NumberValue value) throws IOException {
        writeString(out, ((NumberValueImpl)value).toString());
    }

    public static void writeTimestamp(ByteOutputStream out,
                                      TimestampValue value) throws IOException {
        String timestampStr = value.toString();
        /*
         *TODO: Don't send timestamp string with "Z" to driver until all driver
         *accept the strings.
         */
        if (timestampStr.endsWith("Z")) {
            timestampStr = timestampStr.substring(0, timestampStr.length() - 1);
        }
        writeString(out, timestampStr);
    }

    public static void writeRecord(ByteOutputStream out, RecordValue value)
        throws IOException {

        /* Leave an integer-sized space for length */
        int lengthOffset = out.getOffset();
        out.writeInt(0);

        int start = out.getOffset();
        out.writeInt(value.size());
        if (value.size() > 0) {
            /*
             * NOTE: this writes the fields into the stream in declaration
             * order for the Record because that's how getFieldNames() returns
             * them.
             */
            for (String fieldName : value.getFieldNames()) {
                FieldValue fval = value.get(fieldName);
                if (fval != null) {
                    writeString(out, fieldName);
                    writeFieldValue(out, fval);
                }
            }
        }
        /* Update the length at lengthOffset */
        out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
    }

    public static void writeMap(ByteOutputStream out, MapValue value)
        throws IOException {

        /* Leave an integer-sized space for length */
        int offset = out.getOffset();
        out.writeInt(0);

        int start = out.getOffset();
        out.writeInt(value.size());
        for (Entry<String, FieldValue> entry: value.getFields().entrySet()) {
            writeString(out, entry.getKey());
            writeFieldValue(out, entry.getValue());
        }
        /* Update the length at lengthOffset */
        out.writeIntAtOffset(offset, out.getOffset() - start);
    }

    public static void writeArray(ByteOutputStream out, ArrayValue value)
        throws IOException {

        /* Leave an integer-sized space for length */
        int offset = out.getOffset();
        out.writeInt(0);

        int start = out.getOffset();
        out.writeInt(value.size());
        for (int i = 0; i < value.size(); i++) {
            writeFieldValue(out, value.get(i));
        }
        /* Update the length at lengthOffset */
        out.writeIntAtOffset(offset, out.getOffset() - start);
    }

    /**
     * Serialize a generic FieldValue
     */
    public static void writeFieldValue(ByteOutputStream out, FieldValue value)
        throws IOException {

        assert(value != null);

        if (value.isNull() || value.isJsonNull()) {
            out.write((byte)(value.isNull() ? TYPE_NULL : TYPE_JSON_NULL));
            return;
        }
        Type type = value.getType();
        out.write(getValueType(type));

        switch (value.getType()) {
        case INTEGER:
            writeInt(out, value.asInteger().get());
            break;
        case LONG:
            writeLong(out, value.asLong().get());
            break;
        case DOUBLE:
            writeDouble(out, value.asDouble().get());
            break;
        case FLOAT:
            writeDouble(out, value.asFloat().get());
            break;
        case NUMBER:
            writeNumber(out, value.asNumber());
            break;
        case STRING:
            writeString(out, value.asString().get());
            break;
        case BOOLEAN:
            out.writeBoolean(value.asBoolean().get());
            break;
        case BINARY:
            writeByteArray(out, value.asBinary().get());
            break;
        case FIXED_BINARY:
            writeByteArray(out, value.asFixedBinary().get());
            break;
        case ENUM:
            writeString(out, value.asEnum().get());
            break;
        case TIMESTAMP:
            writeTimestamp(out, value.asTimestamp());
            break;
        case RECORD:
            writeRecord(out, value.asRecord());
            break;
        case MAP:
            writeMap(out, value.asMap());
            break;
        case ARRAY:
            writeArray(out, value.asArray());
            break;
        case EMPTY:
            break;
        default:
            throw new IllegalStateException("Unknown type " + type);
        }
    }

    /**
     * Maps the FieldDef.Type to type code
     */
    public static int getValueType(Type type) {
        switch (type) {
        case ARRAY:
            return TYPE_ARRAY;
        case BINARY:
        case FIXED_BINARY:
            return TYPE_BINARY;
        case BOOLEAN:
            return TYPE_BOOLEAN;
        case FLOAT:
        case DOUBLE:
            return TYPE_DOUBLE;
        case INTEGER:
            return TYPE_INTEGER;
        case LONG:
            return TYPE_LONG;
        case MAP:
        case RECORD:
            return TYPE_MAP;
        case ENUM:
        case STRING:
            return TYPE_STRING;
        case TIMESTAMP:
            return TYPE_TIMESTAMP;
        case NUMBER:
            return TYPE_NUMBER;
        case EMPTY:
            return TYPE_EMPTY;
        default:
            throw new IllegalStateException("Unknown value type " + type);
        }
    }

    public static Consistency getConsistency(int ctype)
        throws IOException {

        switch (ctype) {
        case EVENTUAL:
            return Consistency.NONE_REQUIRED;
        case ABSOLUTE:
            return Consistency.ABSOLUTE;
        default:
            raiseBadProtocolError("Unknown consistency type: " + ctype, null);
        }
        return null;
    }

    /*
     * This is only used by pre-V4 protocol
     */
    public static Consistency getConsistency(ByteInputStream in)
        throws IOException {

        return getConsistency(in.readByte());
    }

    public static Durability getDurability(ByteInputStream in)
        throws IOException {

        return getDurability(in.readByte());
    }

    public static Durability getDurability(int dur)
        throws IOException {

        if (dur == 0) {
            return null;
        }
        Durability.SyncPolicy masterSync;
        int ms = dur & 0x3;
        switch (ms) {
            case DURABILITY_SYNC:
                masterSync = Durability.SyncPolicy.SYNC;
                break;
            case DURABILITY_NO_SYNC:
                masterSync = Durability.SyncPolicy.NO_SYNC;
                break;
            case DURABILITY_WRITE_NO_SYNC:
                masterSync = Durability.SyncPolicy.WRITE_NO_SYNC;
                break;
            default:
                throw new IOException("Invalid durability masterSync value");
        }
        Durability.SyncPolicy replicaSync;
        int rs = (dur >> 2) & 0x3;
        switch (rs) {
            case DURABILITY_SYNC:
                replicaSync = Durability.SyncPolicy.SYNC;
                break;
            case DURABILITY_NO_SYNC:
                replicaSync = Durability.SyncPolicy.NO_SYNC;
                break;
            case DURABILITY_WRITE_NO_SYNC:
                replicaSync = Durability.SyncPolicy.WRITE_NO_SYNC;
                break;
            default:
                throw new IOException("Invalid durability replicaSync value");
        }
        Durability.ReplicaAckPolicy ackPolicy;
        int rap = (dur >> 4) & 0x3;
        switch (rap) {
            case DURABILITY_ALL:
                ackPolicy = Durability.ReplicaAckPolicy.ALL;
                break;
            case DURABILITY_NONE:
                ackPolicy = Durability.ReplicaAckPolicy.NONE;
                break;
            case DURABILITY_SIMPLE_MAJORITY:
                ackPolicy = Durability.ReplicaAckPolicy.SIMPLE_MAJORITY;
                break;
            default:
                throw new IOException("Invalid durability replicaAck value");
        }
        return new Durability(masterSync, replicaSync, ackPolicy);
    }

    public static int getTableState(TableInfo.TableState state) {
        switch (state) {
        case ACTIVE:
            return ACTIVE;
        case CREATING:
            return CREATING;
        case DROPPING:
            return DROPPING;
        case UPDATING:
            return UPDATING;
        case DROPPED:
            /*
             * this state is also used for "not exist" in a success case, e.g.
             * drop table if exists ...
             */
            return DROPPED;
        }
        throw new IllegalArgumentException(
            "Unexpected TableState: " + state);
    }

    public static int getReplicaState(ReplicaInfo.ReplicaState state) {
        switch(state) {
        case ACTIVE:
            return ACTIVE;
        case CREATING:
            return CREATING;
        case UPDATING:
            return UPDATING;
        case DROPPING:
            return DROPPING;
        default:
            throw new IllegalArgumentException(
                    "Unexpected ReplicaState: " + state);
        }
    }

    public static int getCapacityMode(String mode) {
        if (TableLimits.modeIsAutoScaling(mode)) {
            return ON_DEMAND;
        }
        if (TableLimits.modeIsProvisioned(mode)) {
            return PROVISIONED;
        }

        throw new IllegalArgumentException("Unknown Capacity mode: " + mode);
    }

    public static TableLimits readTableLimits(ByteInputStream in,
                                              short serialVersion)
        throws IOException {

        int readKB = in.readInt();
        int writeKB = in.readInt();
        int storageMaxGB = in.readInt();
        if (serialVersion <= Protocol.V2) {
            /* No limits mode in old protocol, using PROVISIONED mode */
            return new TableLimits(readKB, writeKB, storageMaxGB);
        }
        byte mode = in.readByte();
        switch (mode) {
        case PROVISIONED:
            return new TableLimits(readKB, writeKB, storageMaxGB);
        case ON_DEMAND:
            return new TableLimits(storageMaxGB);
        default:
            raiseBadProtocolError("Unknown mode of TableLimits", in);
        }
        return null;
    }

    public static MultiRowOptions getMultiRowOptions(ByteInputStream in,
                                                     Object tableOrIndex)
        throws IOException {

        boolean hasRange = in.readBoolean();
        if (hasRange) {
            FieldRange range = getFieldRange(in, tableOrIndex);
            return range.createMultiRowOptions();
        }
        return null;
    }

    public static FieldRange getFieldRange(ByteInputStream in,
                                           Object tableOrIndex)
        throws IOException {

        FieldRange range;

        String fieldPath = readString(in);

        if (tableOrIndex instanceof Table) {
            range = ((Table)tableOrIndex).createFieldRange(fieldPath);
        } else {
            assert(tableOrIndex instanceof Index);
            range = ((Index)tableOrIndex).createFieldRange(fieldPath);
        }

        if (in.readBoolean()) {
            FieldValue start = readFieldValue(in, range.getDefinition(),
                                              true, false);
            range.setStart(start, in.readBoolean());
        }
        if (in.readBoolean()) {
            FieldValue end = readFieldValue(in, range.getDefinition(),
                                            true, false);
            range.setEnd(end, in.readBoolean());
        }
        return range;
    }

    /**
     * It deserializes a driver FieldValue into a kvstore FieldValue.
     *
     * @param in the input stream
     * @param def the expected type of the field being read. This must be
     * non-null and must match the input type.
     * @param isTopField false if the value to be read is nested inside
     * another value (i.e., this is a recursive invocation of this method);
     * true otherwise.
     * @param isRecordField true if the value to be read is a field value of
     * a record (i.e., the method is being called from readRecord()).
     * @return the FieldValue
     *
     * @throws IOException if there is a problem reading the stream.
     * @throws IllegalArgumentException if there is a type mismatch between
     * the stream and type definition.
     *
     * There is a choice to make in this method about which is the definitive
     * type. Because the local schema (FieldDef) comes directly from the target
     * table that must take precedence. In addition, because the driver has no
     * type information the deserialization code must be flexible and accept any
     * type that can be converted to the schema type without loss of information.
     * Specifically this is an issue with numeric types.
     *
     * TODO: make the reading bulletproof in terms of NPE and invalid
     * data. Bad data should throw IAE for the most part. Maybe consider
     * mapping IOE to IAE in this path. Need a lot of testing of
     * bad data (bad types for maps, arrays, records; handling of
     * JSON, etc)
     */
    public static FieldValue readFieldValue(ByteInputStream in,
                                            FieldDef def,
                                            boolean isTopField,
                                            boolean isRecordField)
        throws IOException {

        int driverType = readValueType(in);

        /*
         * Handle special values: Null, Json Null and Empty value.
         *
         * Json Null can be used in index key or external variable for query,
         * for typed JSON index, the target type of FieldDef is the specified
         * type but not JSON type, so Json Null may read for non-JSON field.
         *
         * TODO:
         * Empty value also can be used in index key, if we don't support scan
         * operation, then Empty value may not needed?
         */
        if (driverType == TYPE_NULL) {

            if (isRecordField || isTopField) {
                return NullValueImpl.getInstance();
            }

            /*
             * We allow a driver to put an SQL NULL into a json object or array.
             * In this case, we convert the SQL NULL to json null.
             */
            if (def.isJson()) {
                return NullJsonValueImpl.getInstance();
            }

            throw new IllegalArgumentException(
                "Unexpected NULL value in map or array");

        } else if (driverType == TYPE_JSON_NULL) {
            return NullJsonValueImpl.getInstance();
        } else if (driverType == TYPE_EMPTY) {
            return EmptyValueImpl.getInstance();
        }

        FieldDef.Type schemaType = def.getType();

        switch (schemaType) {
        case ARRAY:
            assertType(TYPE_ARRAY, driverType);
            return readArray(in, def.asArray());
        case BINARY:
        case FIXED_BINARY:
            return createBinary(in, def, driverType);
        case BOOLEAN:
            return createBoolean(in, def, driverType);
        case DOUBLE:
            return createDouble(in, def, driverType);
        case ENUM:
            /* ENUM must be of type String */
            assertType(TYPE_STRING, driverType);
            return def.createEnum(readString(in));
        case FLOAT:
            return createFloat(in, def, driverType);
        case INTEGER:
            return createInteger(in, def, driverType);
        case LONG:
            return createLong(in, def, driverType);
        case MAP:
            assertType(TYPE_MAP, driverType);
            return readMap(in, def.asMap());
        case RECORD:
            return createRecord(in, def.asRecord(), null, driverType);
        case STRING:
            assertType(TYPE_STRING, driverType);
            return def.createString(readString(in));
        case JSON:
        case ANY:
        case ANY_ATOMIC:
        case ANY_JSON_ATOMIC:
            /*
             * JSON means any valid JSON type is accepted and a default mapping
             * is used from JSON to schema types.
             */
            return readJson(in, driverType);
        case TIMESTAMP:
            return createTimestamp(in, driverType, def.asTimestamp());
        case NUMBER:
            return createNumber(in, driverType);
        default:
            throw new IllegalArgumentException(
                "Unsupported or invalid type. Schema type " +
                schemaType + ", driver type " + getTypeName(driverType));
        }
    }

    public static void assertType(int expected, int actual) {
        if (expected != actual) {
            throw new IllegalArgumentException(
                "Type mismatch on input. Expected " + getTypeName(expected) +
                ", got " + getTypeName(actual));
        }
    }

    /*
     * TODO: We might change the serialization of JSON in the store to be the
     * same as the serialization we're creating for the driver and proxy.
     */
    public static FieldValue readJson(ByteInputStream in, int driverType)
        throws IOException {

        JsonDefImpl def = FieldDefImpl.Constants.jsonDef;

        switch (driverType) {
        case TYPE_ARRAY:
            return readArray(in, def);
        case TYPE_BOOLEAN:
            return def.createBoolean(in.readBoolean());
        case TYPE_DOUBLE:
            return def.createDouble(in.readDouble());
        case TYPE_INTEGER:
            return def.createInteger(readInt(in));
        case TYPE_LONG:
            return def.createLong(readLong(in));
        case TYPE_MAP:
            return readMap(in, def);
        case TYPE_STRING:
        case TYPE_TIMESTAMP: // In JSON this is just a string
            return def.createString(readNonNullString(in, "String value"));
        case TYPE_NUMBER:
            return def.createNumber(readNumber(in));
        case TYPE_JSON_NULL:
            return def.createJsonNull();
        case TYPE_BINARY:
            String encodedStr = encodeBase64(readByteArray(in));
            return def.createString(encodedStr);
        default:
            throw new IllegalArgumentException
                ("Invalid driver type for JSON: " + getTypeName(driverType));
        }
    }

    /**
     * Read a boolean value.
     *
     * The driver value can be a Boolean or a String.
     */
    public static FieldValue createBoolean(ByteInputStream in,
                                           FieldDef def,
                                           int driverType)
        throws IOException {

        boolean value = getBoolean(in, driverType);
        return def.createBoolean(value);
    }

    static boolean getBoolean(ByteInputStream in, int driverType)
        throws IOException {

        boolean value;
        switch (driverType) {
        case TYPE_STRING:
            value = Boolean.parseBoolean(
                        readNonNullString(in, "StringToBoolean"));
            break;
        case TYPE_BOOLEAN:
            value = in.readBoolean();
            break;
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Boolean: " + getTypeName(driverType));
        }
        return value;
    }

    /*
     * Read numeric types, allowing flexibility
     */

    public static FieldValue createInteger(ByteInputStream in,
                                           FieldDef def,
                                           int driverType)
        throws IOException {

        int value = getInt(in, driverType);
        return def.createInteger(value);
    }

    static int getInt(ByteInputStream in, int driverType)
        throws IOException {

        int value;

        switch (driverType) {
        case TYPE_INTEGER:
            value = readInt(in);
            break;
        case TYPE_LONG:
            value = longToInt(readLong(in));
            break;
        case TYPE_DOUBLE:
            value = doubleToInt(in.readDouble());
            break;
        case TYPE_NUMBER:
            value = numberToInt(readNumber(in));
            break;
        case TYPE_STRING:
            value = stringToInt(readNonNullString(in, "stringToInt"));
            break;
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Integer: " + getTypeName(driverType));
        }
        return value;
    }

    public static FieldValue createLong(ByteInputStream in,
                                        FieldDef def,
                                        int driverType)
        throws IOException {

        long value = getLong(in, driverType);
        return def.createLong(value);
    }

    static long getLong(ByteInputStream in, int driverType)
        throws IOException {

        long value;

        switch (driverType) {
        case TYPE_INTEGER:
            value = readInt(in);
            break;
        case TYPE_LONG:
            value = readLong(in);
            break;
        case TYPE_DOUBLE:
            value = doubleToLong(in.readDouble());
            break;
        case TYPE_NUMBER:
            value = numberToLong(readNumber(in));
            break;
        case TYPE_STRING:
            value = stringToLong(readNonNullString(in, "stringToLong"));
            break;
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Long: " + getTypeName(driverType));
        }
        return value;
    }

    /**
     * read float value.
     */
    public static FieldValue createFloat(ByteInputStream in,
                                         FieldDef def,
                                         int driverType)
        throws IOException {

        float value = getFloat(in, driverType);
        return def.createFloat(value);
    }

    static float getFloat(ByteInputStream in, int driverType)
        throws IOException {

        float value;
        switch (driverType) {
        case TYPE_INTEGER:
            value = intToFloat(readInt(in));
            break;
        case TYPE_LONG:
            value = longToFloat(readLong(in));
            break;
        case TYPE_DOUBLE:
            value = doubleToFloat(in.readDouble());
            break;
        case TYPE_NUMBER:
            value = numberToFloat(readNumber(in));
            break;
        case TYPE_STRING:
            value = stringToFloat(readNonNullString(in, "stringToFloat"));
            break;
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Float: " + getTypeName(driverType));
        }
        return value;
    }

    /**
     * Read double value.
     */
    public static FieldValue createDouble(ByteInputStream in,
                                          FieldDef def,
                                          int driverType)
        throws IOException {

        double value = getDouble(in, driverType);
        return def.createDouble(value);
    }

    static double getDouble(ByteInputStream in, int driverType)
        throws IOException {

        double value;

        switch (driverType) {
        case TYPE_INTEGER:
            value = readInt(in);
            break;
        case TYPE_LONG:
            value = longToDouble(readLong(in));
            break;
        case TYPE_DOUBLE:
            value = in.readDouble();
            break;
        case TYPE_NUMBER:
            value = numberToDouble(readNumber(in));
            break;
        case TYPE_STRING:
            value = stringToDouble(readNonNullString(in, "stringToDouble"));
            break;
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Double: " + getTypeName(driverType));
        }
        return value;
    }

    public static FieldValue createNumber(ByteInputStream in, int driverType)
        throws IOException {

        return new NumberValueImpl(getNumberBytes(in, driverType));
    }

    static byte[] getNumberBytes(ByteInputStream in, int driverType)
        throws IOException {

        switch (driverType) {
        case TYPE_INTEGER:
            return NumberUtils.serialize(readInt(in));
        case TYPE_LONG:
            return NumberUtils.serialize(readLong(in));
        case TYPE_DOUBLE:
            return NumberUtils.serialize(BigDecimal.valueOf(in.readDouble()));
        case TYPE_NUMBER:
            return NumberUtils.serialize(readNumber(in));
        case TYPE_STRING:
            String sval = readNonNullString(in, "stringToDecimal");
            return NumberUtils.serialize(stringToDecimal(sval));
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Number: " + getTypeName(driverType));
        }
    }

    static TimestampValue createTimestamp(ByteInputStream in,
                                          int driverType,
                                          TimestampDef def)
        throws IOException {

        /*
         * Timestamp may be of type STRING, TIMESTAMP, INTEGER, or LONG
         */
        switch (driverType) {
        case TYPE_STRING:
        case TYPE_TIMESTAMP:
            String val = readString(in);
            return def.fromString(val);
        case TYPE_INTEGER:
            return def.createTimestamp(new Timestamp(readInt(in)));
        case TYPE_LONG:
            return def.createTimestamp(new Timestamp(readLong(in)));
        default:
            throw new IllegalArgumentException(
                "Invalid driver type for Timestamp: " +
                getTypeName(driverType));
        }
    }

    /*
     * Casts to string value.
     */
    static String getString(ByteInputStream in, int driverType)
        throws IOException {

        if (driverType == TYPE_STRING || driverType == TYPE_TIMESTAMP) {
            return readNonNullString(in, "String value");
        } else if (driverType == TYPE_BINARY) {
            return encodeBase64(readNonNullByteArray(in));
        }
        throw new IllegalArgumentException(
            "Invalid driver type for String: " + getTypeName(driverType));
    }

    /**
     * Read binary or fixedBinary value
     */
    public static FieldValue createBinary(ByteInputStream in,
                                          FieldDef def,
                                          int driverType)
        throws IOException {

        byte[] value = getBytes(in, driverType);
        if (def.isBinary()) {
            return def.createBinary(value);
        }
        assert(def.isFixedBinary());
        return def.createFixedBinary(value);
    }

    static byte[] getBytes(ByteInputStream in, int driverType)
        throws IOException {

        if (driverType == TYPE_BINARY) {
            return readNonNullByteArray(in);
        } else if (driverType == TYPE_STRING) {
            try {
                return decodeBase64(readNonNullString(in, "String value"));
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(
                    "Invalid string for binary value: " + iae.getMessage());
            }
        }
        throw new IllegalArgumentException(
            "Invalid driver type for Binary: " + getTypeName(driverType));
    }

    /*
     * def is FieldDef vs ArrayDef to handle JSON
     *
     * TODO: handle array of map, record, array -- need type info
     */
    public static ArrayValue readArray(ByteInputStream in, FieldDef def)
        throws IOException {

        ArrayValue array = def.createArray();

        /* def is either ArrayDef or JsonDef */
        FieldDef elementDef = (def instanceof ArrayDef ?
                               def.asArray().getElement() : def);
        /* read length */
        read4BytesLength(in);

        /* read size */
        int size = read4BytesSize(in);
        for (int i = 0; i < size; i++) {
            array.add(readFieldValue(in, elementDef, false, false));
        }
        return array;
    }

    /*
     * def is FieldDef vs MapDef to handle JSON
     *
     * TODO: handle map of map, record, array -- need type info
     */
    public static MapValue readMap(ByteInputStream in, FieldDef def)
        throws IOException {

        MapValue map = def.createMap();

        /* def is either MapDef or JsonDef */
        FieldDef elementDef = (def instanceof MapDef ?
                               def.asMap().getElement() : def);
        /* read length */
        read4BytesLength(in);

        /* read size */
        int size = read4BytesSize(in);
        for (int i = 0; i < size; i++) {
            String key = readNonNullString(in, "Map key");
            map.put(key, readFieldValue(in, elementDef, false, false));
        }
        return map;
    }

    public static byte[] readByteArray(ByteInputStream in)
        throws IOException {

        return SerializationUtil.readByteArray(in);
    }

    public static int skipByteArray(ByteInputStream in)
        throws IOException {

        return SerializationUtil.skipByteArray(in);
    }

    public static byte[] readNonNullByteArray(ByteInputStream in)
        throws IOException {

        byte[] bytes = readByteArray(in);
        if (bytes == null) {
            raiseBadProtocolError("Byte array must be non-null", in);
        }
        return bytes;
    }

    public static byte[] getContinuationKey(ByteInputStream in)
        throws IOException {

        return readByteArray(in);
    }

    public static Direction getDirection(ByteInputStream in)
        throws IOException {
        int d = in.readByte();
        switch (d) {
        case UNORDERED:
            return Direction.UNORDERED;
        case FORWARD:
            return Direction.FORWARD;
        case REVERSE:
            return Direction.REVERSE;
        default:
            raiseBadProtocolError("Unknown direction type: " + d, in);
        }
        return null;
    }

    public static int readInt(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readPackedInt(in);
    }

    public static int skipInt(ByteInputStream in)
        throws IOException {
        return SerializationUtil.skipPackedInt(in);
    }

    public static long readLong(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readPackedLong(in);
    }

    public static int skipLong(ByteInputStream in)
        throws IOException {
        return SerializationUtil.skipPackedLong(in);
    }

    static BigDecimal readNumber(ByteInputStream in)
        throws IOException {

        String sval = readNonNullString(in, "readNumber");
        return new BigDecimal(sval);
    }

    public static Version readVersion(ByteInputStream in)
        throws IOException {

        byte[] bytes = readByteArray(in);
        try {
            return Version.fromByteArray(bytes);
        } catch (FaultException fe) {
            throw new IOException("Invalid version value: " + fe.getMessage());
        } catch (RuntimeException re) {
            throw new IOException("Invalid version value: " + re.getMessage());
        }
    }

    public static TimeToLive readTTL(ByteInputStream in)
        throws IOException {

        long value = readLong(in);
        if (value == -1) {
            return null;
        }
        if (value < 0) {
            raiseBadProtocolError("TimeToLive value must be non-negative", in);
        }

        int unit = in.readByte();
        if (unit == TTL_DAYS) {
            return TimeToLive.ofDays(value);
        } else if (unit == TTL_HOURS) {
            return TimeToLive.ofHours(value);
        }
        raiseBadProtocolError("Unknown TTL unit: " + unit, in);
        return null;
    }

    public static int readNumberLimit(ByteInputStream in)
        throws IOException {

        int limit = readInt(in);
        if (limit < 0) {
            raiseBadProtocolError(
                "Invalid limit, it must be non-negative: " + limit, in);
        }
        return limit;
    }

    public static int readMaxReadKB(ByteInputStream in)
        throws IOException {

        int maxReadKB = readInt(in);
        if (maxReadKB < 0) {
            raiseBadProtocolError(
                "Invalid maxReadKB, it must be non-negative: " + maxReadKB, in);
        }
        return maxReadKB;
    }

    public static int readMaxWriteKB(ByteInputStream in)
        throws IOException {

        int maxWriteKB = readInt(in);

        if (maxWriteKB < 0) {
            raiseBadProtocolError(
                "Invalid maxWriteKB, it must be non-negative: " +
                maxWriteKB, in);
        }
        return maxWriteKB;
    }

    public static int readOpCode(ByteInputStream in)
        throws IOException {

        /* validation is done by caller */
        return in.readByte();
    }

    static int readValueType(ByteInputStream in)
        throws IOException {

        int type = in.readByte();
        if (type < 0 || type > TYPE_EMPTY) {
            raiseBadProtocolError("Invalid driver value type: " + type, in);
        }
        return type;
    }

    static int read4BytesLength(ByteInputStream in)
        throws IOException {

        int len = in.readInt();
        if (len < 0) {
            raiseBadProtocolError(
                "Invalid length value, it must be non-negative: " + len, in);
        }
        if (len > in.available()) {
            raiseBadProtocolError(
                "Invalid length value, length is " +
                len + ", but maximum bytes is " + in.available(), in);
        }
        return len;
    }

    static int read4BytesSize(ByteInputStream in)
        throws IOException {

        int size = in.readInt();
        if (size < 0) {
            raiseBadProtocolError(
                "Invalid length value, it must be non-negative: " + size, in);
        }
        return size;
    }

    /*
     * Skips over the number of bytes from the InputStream which represents a
     * value
     */
    static int skipValue(ByteInputStream bis)
        throws IOException {

        int startOffset = bis.getOffset();
        int type = readValueType(bis);
        if (type == TYPE_NULL || type == TYPE_JSON_NULL || type == TYPE_EMPTY) {
            return 1;
        }

        switch (type) {
        case TYPE_BINARY:
        case TYPE_NUMBER:
            skipByteArray(bis);
            break;
        case TYPE_INTEGER:
            skipInt(bis);
            break;
        case TYPE_LONG:
            skipLong(bis);
            break;
        case TYPE_BOOLEAN:
            bis.skip(1);
            break;
        case TYPE_DOUBLE:
            bis.skip(8);
            break;
        case TYPE_STRING:
        case TYPE_TIMESTAMP:
            skipString(bis);
            break;
        case TYPE_MAP:
        case TYPE_ARRAY:
            int len = read4BytesLength(bis);
            bis.skip(len);
            break;
        default:
            throw new IllegalArgumentException("Unsupported type:" + type);
        }
        return bis.getOffset() - startOffset;
    }

    /**
     * Writes Table information to OutputStream
     * Format:
     *  #tables (1)
     *  table namespace
     *  for each table:
     *    table names
     *    access (QueryOperation)
     */
    static void writeTableAccessInfo(ByteOutputStream out,
                                     TableUtils.PrepareCB cbInfo)
        throws IOException {

        String[] notTargetTables = cbInfo.getNotTargetTables();
        int num = (notTargetTables == null) ? 1 : notTargetTables.length + 1;

        out.writeByte(num);
        SerializationUtil.writeString(out, cbInfo.getNamespace());
        /* Target table */
        SerializationUtil.writeString(out, cbInfo.getTableName());
        /* Other non-target tables if exists */
        if (notTargetTables != null) {
            for (String name : notTargetTables) {
                SerializationUtil.writeString(out, name);
            }
        }
        out.writeByte(cbInfo.getOperation().ordinal());
    }

    /**
     * Serialize TableOperationResult object.
     */
    static void writeTableOperationResult(ByteOutputStream out,
                                          short serialVersion,
                                          TableOperation.Type type,
                                          boolean returnInfo,
                                          Table table,
                                          TableOperationResult result,
                                          FieldValue generatedValue)
        throws IOException {

        out.writeBoolean(result.getSuccess());

        if ((type == TableOperation.Type.PUT ||
             type == TableOperation.Type.PUT_IF_ABSENT ||
             type == TableOperation.Type.PUT_IF_PRESENT ||
             type == TableOperation.Type.PUT_IF_VERSION) &&
            result.getNewVersion() != null) {

            out.writeBoolean(true);
            writeVersion(out, result.getNewVersion());
        } else {
            out.writeBoolean(false);
        }

        OpResultWrapper opResult = (OpResultWrapper)result;
        writeExistingValueVersion(out, opResult.getSuccess(), returnInfo, table,
            null, serialVersion,
            new ExistingRowWriter() {
                @Override
                public void write(RowReaderImpl rowReader) {
                    opResult.getPreviousRow(rowReader);
                }
        });
        if (serialVersion > V1) {
            /*
             * Only write generated value if the operation actually
             * succeeded in putting a new row
             */
            if (generatedValue != null && result.getNewVersion() != null) {
                out.writeBoolean(true);
                writeFieldValue(out, generatedValue);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    /**
     * Writes TableLimits.
     * @param out output
     * @param limits table limits
     * @throws IOException if exception
     */
    static void writeTableLimits(ByteOutputStream out,
                                 TableLimits limits,
                                 short serialVersion) throws IOException {

        writeInt(out, limits.getReadUnits());
        writeInt(out, limits.getWriteUnits());
        writeInt(out, limits.getTableSize());
        if (serialVersion > Protocol.V2) {
            if (limits.modeIsAutoScaling()) {
                out.writeByte(ON_DEMAND);
            } else if (limits.modeIsProvisioned()) {
                out.writeByte(PROVISIONED);
            } else {
                throw new IllegalArgumentException("Invalid TableLimits, " +
                    "unknown mode");
            }
        }
    }

    public static void writeIndexInfo(ByteOutputStream out, IndexInfo index)
        throws IOException {

        writeString(out, index.getIndexName());
        writeInt(out, index.getIndexFields().length);
        for (IndexField field : index.getIndexFields()) {
            writeString(out, field.getPath());
        }
    }

    /**
     * Translate the specified Base64 string into a byte array.
     */
    public static String encodeBase64(byte[] buf) {
        return JsonUtils.encodeBase64(buf);
    }

    /**
     * Decode the specified Base64 string into a byte array.
     */
    public static byte[] decodeBase64(String str) {
        if (str == null) {
            return null;
        }
        if (!isPaddedBase64String(str)) {
            throw new IllegalArgumentException(
                str + " is not padded base64 string");
        }

        return JsonUtils.decodeBase64(str);
    }

    /**
     * A simple check to validate if given string is padded base64 string.
     * This check only validate the length of given string, which requires
     * its length is a multiple of four to complement the check missing in
     * Java Base64.Decoder, which doesn't require padding char if the final
     * unit only has two or three base64 characters.
     */
    public static boolean isPaddedBase64String(String str) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        if (length == 0) {
            return false;
        }
        if (length % 4 != 0) {
            return false;
        }

        return true;
    }

    public static void writeGetTableUsageResponse(
        ByteOutputStream out,
        TableUsageResponse response,
        String tenantId,
        String tableName)

        throws IOException {

        writeString(out, tenantId);
        writeString(out, tableName);

        final TableUsage[] usageRecords = response.getTableUsage();
        if (usageRecords != null) {
            writeInt(out, usageRecords.length);
            for (TableUsage usage : usageRecords) {
                writeLong(out, usage.getStartTimeMillis());
                writeInt(out, usage.getSecondsInPeriod());
                writeInt(out, usage.getReadUnits());
                writeInt(out, usage.getWriteUnits());
                writeInt(out, usage.getStorageGB());
                writeInt(out, usage.getReadThrottleCount());
                writeInt(out, usage.getWriteThrottleCount());
                writeInt(out, usage.getStorageThrottleCount());
            }
        } else {
            writeInt(out, 0);
        }
    }

    /*
     * Utility methods to convert between numeric types.
     */

    private static int numberToInt(BigDecimal val) {
        try {
            return val.intValueExact();
        } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("Int overflow: " + val);
        }
    }

    private static long numberToLong(BigDecimal val) {
        try {
            return val.longValueExact();
        } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("Long overflow: " + val);
        }
    }

    private static float numberToFloat(BigDecimal val) {
        try {
            int ival = val.intValueExact();
            return intToFloat(ival);
        } catch (ArithmeticException aei) {
            /* Out of range of integer, try long */
            try {
                long lval = val.longValueExact();
                return longToFloat(lval);
            } catch (ArithmeticException ael) {
                /* Out of range of long, go to BigDecimal */
            }
        }
        return decimalToFloat(val);
    }

    private static double numberToDouble(BigDecimal val) {
        try {
            return val.intValueExact();
        } catch (ArithmeticException aei) {
            /* Out of range of integer, try long */
            try {
                long lval = val.longValueExact();
                return longToDouble(lval);
            } catch (ArithmeticException ael) {
                /* Out of range of long, go to BigDecimal */
            }
        }
        return decimalToDouble(val);
    }

    /* public for test */
    public static float decimalToFloat(BigDecimal decVal) {
        double dblVal = decimalToDouble(decVal);
        return doubleToFloat(dblVal);
    }

    /* public for test */
    public static double decimalToDouble(BigDecimal decVal) {
        double dblVal = decVal.doubleValue();
        /* Treated Infinity/-Infinity as valid doubles */
        if (!Double.isInfinite(dblVal) &&
            decVal.compareTo(BigDecimal.valueOf(dblVal)) != 0) {
            throw new IllegalArgumentException
            ("Converting decimal to double loses information: " + decVal);
        }
        return dblVal;
    }

    /* public for test */
    public static float intToFloat(int intVal) {
        float fltVal = intVal;
        if (intVal != (int)fltVal) {
            throw new IllegalArgumentException
            ("Converting long to float loses information: " + intVal);
        }
        return fltVal;
    }

    /* public for test */
    public static int longToInt(long longVal) {
        try {
            return toIntExact(longVal);
        } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("Int overflow: " + longVal);
        }
    }

    /* public for test */
    public static float longToFloat(long longVal) {
        float fltVal = longVal;
        if (longVal != (long)fltVal) {
            throw new IllegalArgumentException
            ("Converting long to float loses information: " + longVal);
        }
        return fltVal;
    }

    /* public for test */
    public static double longToDouble(long longVal) {
        double dblVal = longVal;
        if (longVal != (long)dblVal) {
            throw new IllegalArgumentException
            ("Converting long to double loses information: " + longVal);
        }
        return dblVal;
    }

    /* public for test */
    public static int doubleToInt(double dblVal) {
        int intVal = (int)dblVal;
        if (Double.compare(dblVal, intVal) != 0) {
            throw new IllegalArgumentException
            ("Converting double to int loses information: " + dblVal);
        }
        return intVal;
    }

    /* public for test */
    public static long doubleToLong(double dblVal) {
        long longVal = (long)dblVal;
        if (Double.compare(dblVal, longVal) != 0) {
            throw new IllegalArgumentException
            ("Converting double to long loses information: " + dblVal);
        }
        return longVal;
    }

    /**
     * Public for test.
     *
     * If double matches one of below, it is regarded as valid for float:
     *   1. ABS(double) is in range of [Float.MIN_VALUE, Float.MAX_VALUE].
     *   2. Special values: 0.0f, NaN, POSITIVE_INFINITY, NEGATIVE_INFINITY.
     */
    public static float doubleToFloat(double dblVal) {
        float fltVal = (float)dblVal;
        if (Float.compare(fltVal, 0f) == 0) {
            if (Double.compare(dblVal, 0d) == 0) {
                return fltVal;
            }
        } else {
            if (Float.isFinite(fltVal) ||
                (Double.isInfinite(dblVal) || Double.isNaN(dblVal))) {
                return fltVal;
            }
        }
        throw new IllegalArgumentException(
            "Converting double to float loses information: " + dblVal);
    }

    private static int stringToInt(String strVal) {
        try {
            return Integer.parseInt(strVal);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
            ("Invalid string for Integer: " + strVal);
        }
    }

    private static long stringToLong(String strVal) {
        try {
            return Long.parseLong(strVal);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
            ("Invalid string for Long: " + strVal);
        }
    }

    private static float stringToFloat(String strVal) {
        if (strVal == null || strVal.isEmpty()) {
            throw new IllegalArgumentException
            ("Invalid string for Float: " + strVal);
        }
        try {
            return Float.parseFloat(strVal);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
            ("Invalid string for Float: " + strVal);
        }
    }

    private static double stringToDouble(String strVal) {
        if (strVal == null || strVal.isEmpty()) {
            throw new IllegalArgumentException
            ("Invalid string for Double: " + strVal);
        }
        try {
            return Double.parseDouble(strVal);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
            ("Invalid string for Double: " + strVal);
        }
    }

    private static BigDecimal stringToDecimal(String strVal) {
        if (strVal == null || strVal.isEmpty()) {
            throw new IllegalArgumentException
            ("Invalid string for Decimal: " + strVal);
        }
        try {
            return new BigDecimal(strVal);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
            ("Invalid string for Decimal: " + strVal);
        }
    }

    /**
     * Protocol error
     */
    public static void raiseBadProtocolError(String msg,
                                      ByteInputStream in) throws IOException {
        if (in != null) {
            msg = msg + ", offset: " + in.getOffset();
        }
        throw new IOException(msg);
    }

    /**
     * Implementation of ValueReader that used to directly read row value and
     * write to the stream to driver.
     *
     * Since the <size> of Array/Map value is decided until read all its sub
     * fields, but <size> in the existing format is located at beginning of
     * Array/Map, so the way used here is to write 0 for <size> firstly to
     * leave an int space in the stream, then update it with actual value after
     * read all of its sub fields.
     *
     * A stack of State instances is used to store the offset of <size> in bos
     * and number of its sub fields.
     *
     * Method done() must be called after read all fields, it finally update
     * the <size> of root MapValue.
     */
    public static class RowReaderImpl implements ValueReader<Void> {

        private final ByteOutputStream bos;
        private final Table table;
        private final int startOffset;
        private Stack<State> states;

        private long expirationTime;
        private long modificationTime;
        private Version version;

        public RowReaderImpl(ByteOutputStream bos, Table table) {
            this.table = table;
            this.bos = bos;
            this.startOffset = bos.getOffset();
            states = null;
        }

        @Override
        public void readInteger(String fieldName, int val) {
            try {
                addField(fieldName, TYPE_INTEGER);
                writeInt(bos, val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readLong(String fieldName, long val) {
            try {
                addField(fieldName, TYPE_LONG);
                writeLong(bos, val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readFloat(String fieldName, float val) {
            try {
                addField(fieldName, TYPE_DOUBLE);
                writeDouble(bos, val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readDouble(String fieldName, double val) {
            try {
                addField(fieldName, TYPE_DOUBLE);
                writeDouble(bos, val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readNumber(String fieldName, byte[] bytes) {
            try {
                addField(fieldName, TYPE_NUMBER);
                writeString(bos,
                            NumberUtils.deserialize(bytes, true).toString());
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readTimestamp(String fieldName, FieldDef def, byte[] val) {
            try {
                addField(fieldName, TYPE_TIMESTAMP);
                writeTimestamp(bos,
                    ((TimestampDefImpl)def).createTimestamp(val));
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readBinary(String fieldName, byte[] bytes) {
            try {
                addField(fieldName, TYPE_BINARY);
                writeByteArray(bos, bytes);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readFixedBinary(String fieldName,
                                    FieldDef def,
                                    byte[] val) {
            try {
                addField(fieldName, TYPE_BINARY);
                writeByteArray(bos, val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readString(String fieldName, String val) {
            try {
                addField(fieldName, TYPE_STRING);
                writeString(bos, val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readBoolean(String fieldName, boolean val) {
            try {
                addField(fieldName, TYPE_BOOLEAN);
                bos.writeBoolean(val);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readNull(String fieldName) {
            try {
                addField(fieldName, TYPE_NULL);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readJsonNull(String fieldName) {
            try {
                addField(fieldName, TYPE_JSON_NULL);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readEnum(String fieldName, FieldDef def, int index) {
            try {
                addField(fieldName, TYPE_STRING);
                writeString(bos, def.asEnum().getValues()[index]);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void readEmpty(String fieldName) {
            try {
                addField(fieldName, TYPE_EMPTY);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        @Override
        public void startRecord(String fieldName, FieldDef def, int size) {
            startComplexValue(fieldName, TYPE_MAP);
        }

        @Override
        public void endRecord(int size) {
            endComplexValue();
        }

        @Override
        public void startMap(String fieldName, FieldDef def, int size) {
            startComplexValue(fieldName, TYPE_MAP);
        }

        @Override
        public void endMap(int size) {
            endComplexValue();
        }

        @Override
        public void startArray(String fieldName,
                               FieldDef def,
                               FieldDef elemDef,
                               int size) {
            startComplexValue(fieldName, TYPE_ARRAY);
        }

        @Override
        public void endArray(int size) {
            endComplexValue();
        }

        @Override
        public Table getTable() {
            return table;
        }

        @Override
        public void setTableVersion(int arg0) {
            /* Do nothing */
        }

        @Override
        public void reset() {
            bos.setWriteIndex(startOffset);
            states.clear();
        }

        @Override
        public void setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        @Override
        public void setModificationTime(long modificationTime) {
            this.modificationTime = modificationTime;
        }

        public long getModificationTime() {
            return modificationTime;
        }

        @Override
        public void setVersion(Version version) {
            this.version = version;
        }

        @Override
        public Void getValue() {
            /* Do nothing */
            return null;
        }

        @Override
        public void setValue(Void value) {
            /* Do nothing */
        }

        public Version getVersion() {
            return version;
        }

        public int done() {
            if (states != null && !states.empty()) {
                if (states.size() != 1) {
                    reset();
                    throw new IllegalArgumentException("The row is corrupted");
                }
                endComplexValue();
            }
            return bos.getOffset() - startOffset;
        }

        private void addField(String fieldName, int type)
            throws IOException {

            if (states == null) {
                /* Initialize the root MapValue */
                states = new Stack<State>();
                bos.write((byte)TYPE_MAP);
                /* leave an integer space for length */
                bos.writeInt(0);
                /* leave an integer space for size */
                bos.writeInt(0);
                /* the +1 accounts for the type byte written above */
                states.add(new State(startOffset + 1, TYPE_MAP));
            }
            State st = states.peek();
            st.add();
            if (fieldName != null && st.type != TYPE_ARRAY) {
                writeString(bos, fieldName);
            }
            bos.write((byte)type);
        }

        private void startComplexValue(String fieldName, int type) {
            try {
                addField(fieldName, type);
                int offset = bos.getOffset();
                /* leave an integer space for length */
                bos.writeInt(0);
                /* leave an integer space for size */
                bos.writeInt(0);
                states.push(new State(offset, type));
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        private void endComplexValue() {
            if (states == null || states.isEmpty()) {
                return;
            }
            State st = states.pop();
            try {
                int current = bos.getOffset();
                bos.setWriteIndex(st.startOffset);
                /* update length */
                bos.writeInt(current - st.startOffset - 4);
                /* update size */
                bos.writeInt(st.numFields);
                bos.setWriteIndex(current);
            } catch (IOException e) {
                throw new IllegalArgumentException
                ("Serialize value failed: " + e);
            }
        }

        /*
         * State class stores the state information for each array/map value
         * including the write index of <size> and actual number of fields in
         * the current array or map.
         */
        private static class State {
            private int startOffset;
            private int numFields;
            private int type;

            State(int offset, int type) {
                this.startOffset = offset;
                numFields = 0;
                this.type = type;
            }

            void add() {
                numFields++;
            }
        }
    }
}
