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

package oracle.kv;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;

import com.sleepycat.util.PackedInteger;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * The Value in a Key/Value store.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class Value implements FastExternalizable {

    /**
     * Identifies the format of a value.
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     *
     * @since 2.0
     */
    public enum Format implements FastExternalizable {

        /*
         * Internally we use the first byte of the stored value to determine
         * the format (NONE or AVRO).
         * <ul>
         *
         * <li> If the first stored byte is zero, the format is NONE.  In this
         *   case the byte array visible via the API (via getValue) does not
         *   contain this initial byte and its size is one less than the stored
         *   array size.
         *
         * <li> If the first stored byte is negative, it is the first byte of an
         *   Avro schema ID and the format is AVRO.  In this case the entire
         *   stored array is returned by getValue.
         *
         *   PackedInteger.writeSortedInt is used to format the schema ID, and
         *   this guarantees that the first byte of a positive number is
         *   negative.  Schema IDs are always positive, starting at one.
         *
         * <li> If the first stored byte is one, the format is TABLE, indicating
         *   that the record is part of a table and is serialized in a
         *   table-specific format.
         *
         * </ul>
         * The stored byte array is always at least one byte.  For format NONE,
         * the user visible array may be empty in which case the stored array
         * has a single, zero byte.  For format AVRO, the user visible array
         * and the stored array are the same, but are always at least one byte
         * in length due to the presence of the schema ID.
         * <p>
         * If an unexpected value is seen by the implementation an
         * IllegalStateException is thrown.  Additional positive values may be
         * used for new formats in the future.
         */

        /**
         * The byte array format is not known to the store; the format is known
         * only to the application.  Values of format {@code NONE} are created
         * with {@link #createValue}.  All values created using NoSQL DB
         * version 1.x have format {@code NONE}.
         */
        NONE(0),

        /**
         * The byte array format is Avro binary data along with an internal,
         * embedded schema ID.
         *
         * @deprecated as of 4.0, use the table API instead.
         */
        @Deprecated
        AVRO(1),

        /**
         * The byte array format that is used by table rows.  Values with
         * this format are never created by applications but non-table
         * applications may accidentally see a table row.  These Values
         * cannot be deserialized by non-table applications.
         */
        TABLE(2),

        /**
         * Introduced at TABLE_V1 format:
         * - An new serialization way for string value in JSON field.
         */
        TABLE_V1(3),

        /**
         * Format for multi-region table. Values with this format have encoded
         * information for multi-region table, including region id and
         * tombstone.
         */
        MULTI_REGION_TABLE(4),

        /**
         * Format that contains:
         *  - 1st byte: format version
         *  - 2nd byte: bitset
         *      - bit 0: 1 if row has regionId, 0 otherwise
         *      - bit 1: 1 if row has row-metadata, 0 otherwise
         *      - bits 2-7: unused
         *  - if it has regionId next bytes are a packed int
         *  - if it has metadata next bytes are the metadata string length and string
         *  - rest bytes are the row data
         */
        TABLE_V5(5);


        private static final Format[] VALUES = values();
        public static Format valueOf(int ordinal) {
            return VALUES[ordinal];
        }

        private Format(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalStateException("Wrong ordinal");
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #NONE}=0,
         *      {@link #AVRO}=1, {@link #TABLE}=2
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }

        /**
         * For internal use only.
         * @hidden
         */
        public static Format fromFirstByte(int firstByte) {

            /*
             * Avro schema IDs are positive, which means the first byte of the
             * package sorted integer is negative.
             */
            if (firstByte < 0) {
                return Format.AVRO;
            }

            /* Zero means no format. */
            if (firstByte == 0) {
                return Format.NONE;
            }

            /* Table formats. */
            if (isTableFormat(firstByte)) {
                return valueOf(firstByte + 1);
            }

            /* Other values are not yet assigned. */
            throw new IllegalStateException
                ("Value has unknown format discriminator: " + firstByte);
        }

        /**
         * Returns true if the value format is for table.
         */
        public static boolean isTableFormat(Format format) {
            int ordinal = format.ordinal();
            return ordinal >= Format.TABLE.ordinal() &&
            ordinal <= Format.TABLE_V5.ordinal();
        }

        public static boolean isTableFormat(int firstByte) {
            int ordinal = firstByte + 1;
            return ordinal >= Format.TABLE.ordinal() &&
            ordinal <= Format.TABLE_V5.ordinal();
        }
    }

    private static int TABLEV5_REGIONID_BIT = 0;
    private static int TABLEV5_ROWMETADATA_BIT = 1;
    // private static int TABLEV5_EXTRABYTE_BIT = 7;
    /* NOTE: When adding the 8th new property, must add another byte !!! */

    /**
     * An instance that represents an empty value for key-only records.
     */
    public static final Value EMPTY_VALUE = Value.createValue(new byte[0]);

    private final byte[] val;
    private final Format format;
    private final int regionId;
    private final String rowMetadata;

    private Value(byte[] val, Format format) {
        this(val, format, Region.NULL_REGION_ID, null);
    }

    private Value(byte[] val,
                  Format format,
                  int regionId,
                  String rowMetadata) {
        checkNull("val", val);
        checkNull("format", format);
        if ((format == Format.MULTI_REGION_TABLE) &&
             (!Region.isMultiRegionId(regionId))) {
            throw new IllegalArgumentException(
                "The region id cannot be " + Region.NULL_REGION_ID +
                " for multi-region table");
        }
        if ((format.ordinal() < Format.MULTI_REGION_TABLE.ordinal()) &&
            (regionId != Region.NULL_REGION_ID)) {
            throw new IllegalArgumentException(
                "The region id must be " + Region.NULL_REGION_ID +
                " for local table, id=" + regionId);
        }
        if (regionId < 0) {
            throw new IllegalArgumentException(
                "Illegal region ID: " + regionId);
        }
        if (rowMetadata != null && format != Format.TABLE_V5) {
            throw new IllegalArgumentException("Format must be " +
                Format.TABLE_V5 + " for non-null metadata. Format: " + format + " rmtd: " + rowMetadata);
        }
        this.val = val;
        this.format = format;
        this.regionId = regionId;
        if (rowMetadata != null) {
            TableJsonUtils.validateJsonConstruct(rowMetadata);
        }
        this.rowMetadata = rowMetadata;
    }

    /**
     * For internal use only.
     * @hidden
     *
     * FastExternalizable constructor.
     * Used by the client when deserializing a response.
     */
    @SuppressWarnings("unused")
    public Value(DataInput in, short serialVersion)
        throws IOException {

        final int len = readNonNullSequenceLength(in);
        if (len == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        final int firstByte = in.readByte();
        format = Format.fromFirstByte(firstByte);

        /*
         * Both NONE and TABLE formats skip the first byte.
         */
        if (format == Format.NONE || Format.isTableFormat(format)) {
            if (format == Format.TABLE_V5) {
                byte bitsetByte = in.readByte();
                BitSet options = BitSet.valueOf(new byte[] { bitsetByte });
                int alreadyRead = 2;

                if (options.get(TABLEV5_REGIONID_BIT)) { // contains multi-region
                    regionId = readPackedInt(in);
                    alreadyRead += PackedInteger.getWriteIntLength(regionId);
                } else {
                    regionId = Region.NULL_REGION_ID;
                }

                if (options.get(TABLEV5_ROWMETADATA_BIT)) { // contains metadata
                    int metadataLen = readPackedInt(in);
                    byte[] mdba = new byte[metadataLen];
                    in.readFully(mdba, 0, metadataLen);
                    rowMetadata = new String(mdba, StandardCharsets.UTF_8);
                    // rowMetadata should have been checked before serialization
                    // it is valid JSON Object
                    // assert TableJsonUtils.validateJsonObject(rowMetadata);
                    alreadyRead += PackedInteger.getWriteIntLength(metadataLen) + metadataLen;
                } else {
                    rowMetadata = null;
                }

                val = new byte[len - (alreadyRead)];

            } else if (format == Format.MULTI_REGION_TABLE) {
                /* read compressed region id. */
                regionId = readPackedInt(in);
                final int regionIdLen =
                    PackedInteger.getWriteIntLength(regionId);
                val = new byte[len - (regionIdLen + 1)];
                rowMetadata = null;
            } else {
                this.regionId = Region.NULL_REGION_ID;
                rowMetadata = null;
                val = new byte[len - 1];
            }

            in.readFully(val);
            return;
        }

        /*
         * AVRO includes the first byte because it is all or part of the
         * record's schema ID.
         */
        regionId = Region.NULL_REGION_ID;
        // null value means there is no row-metadata set for this Value
        rowMetadata = null;
        val = new byte[len];
        val[0] = (byte) firstByte;
        in.readFully(val, 1, len - 1);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Deserialize into byte array.
     * Used by the service when deserializing a request.
     */
    public static byte[] readFastExternal(DataInput in,
                                          @SuppressWarnings("unused")
                                          short serialVersion)
        throws IOException {

        final int len = readNonNullSequenceLength(in);
        if (len == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        final byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullSequenceLength non-null
     *      sequence length}) <i>length</i> // Length of val if format is AVRO,
     *      or one greater than length if format is NONE or TABLE
     * <li> ({@code byte[]} <i>val</i> // Bytes in val, prefixed by 0 if format
     *      is NONE, and by 1 if format is TABLE, and also the region ID as a
     *      packed integer if the format is MULTI_REGION_TABLE
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        int prefixLength;
        switch (format) {
        case AVRO:
            prefixLength = 0;
            break;
        case NONE:
        case TABLE:
        case TABLE_V1:
            prefixLength = 1;
            break;
        case MULTI_REGION_TABLE:
            final int regionIdLen = PackedInteger.getWriteIntLength(regionId);
            prefixLength = regionIdLen + 1;
            break;
        case TABLE_V5:
            if (serialVersion < SerialVersion.ROW_METADATA_VERSION) {
                throw new IllegalArgumentException("Serial version " +
                    serialVersion + " does not support setting row metadata, " +
                    "must be " + SerialVersion.ROW_METADATA_VERSION + " or greater");
            }
            prefixLength = 2;  // format and bitset

            if (regionId != Region.NULL_REGION_ID) {
                final int regionIdLen2 = PackedInteger.getWriteIntLength(regionId);
                prefixLength += regionIdLen2;
            }
            if (rowMetadata != null) {
                byte[] mdba = rowMetadata.getBytes(StandardCharsets.UTF_8);
                final int metadataLenLen = PackedInteger.getWriteIntLength(
                    rowMetadata.length());
                prefixLength += metadataLenLen + mdba.length;
            }
            break;
        default:
            throw new AssertionError();
        }
        final int length = val.length + prefixLength;
        writeNonNullSequenceLength(out, length);

        if (prefixLength > 0) {
            out.writeByte((format == Format.NONE) ? 0 : format.ordinal() - 1);
            if (format == Format.MULTI_REGION_TABLE) {
                /* write the compressed region id. */
                writePackedInt(out, regionId);
            } else if (format == Format.TABLE_V5) {
                BitSet options = new BitSet(8);
                options.set(TABLEV5_REGIONID_BIT, regionId != Region.NULL_REGION_ID);      // has regionId
                options.set(TABLEV5_ROWMETADATA_BIT, rowMetadata != null);   // has metadata
                out.write(options.isEmpty() ? new byte[]{0} : options.toByteArray());
                if (regionId != Region.NULL_REGION_ID) {
                    writePackedInt(out, regionId);
                }
                if (rowMetadata != null) {
                    byte[] mdba = rowMetadata.getBytes(StandardCharsets.UTF_8);
                    writePackedInt(out, mdba.length);
                    out.write(mdba);
                }
            }
        }
        out.write(val);
    }

    /**
     * Serialize from byte array.  Used by the service when serializing a
     * response.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@code bytes}
     * </ol>
     *
     * @hidden For internal use only.
     */
    public static void writeFastExternal(DataOutput out,
                                         @SuppressWarnings("unused")
                                         short serialVersion,
                                         byte[] bytes)
        throws IOException {

        if (bytes.length == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        writeNonNullByteArray(out, bytes);
    }

    /**
     * Returns this Value as a serialized byte array, such that {@link
     * #fromByteArray} may be used to reconstitute the Value.
     * <p>
     * The intended use case for the {@link #toByteArray} and {@link
     * #fromByteArray} methods is to serialize values of various formats in a
     * uniform manner, for storage outside of NoSQL DB or for sending across a
     * network.
     * <p>
     * Values returned by calls to this method can be used with current and
     * newer releases, but are not guaranteed to be compatible with earlier
     * releases.
     * <p>
     * <em>WARNING:</em> The array returned by this method should be considered
     * to be opaque by the caller.  This array is not necessarily equal to the
     * array returned by {@link #getValue}; in particular, the returned array
     * may contain an extra byte identifying the format.  The only valid use of
     * this array is to pass it to {@link #fromByteArray} at a later time in
     * order to reconstruct the {@code Value} object. Normally {@link
     * #getValue} should be used instead of this method.
     *
     * @see #fromByteArray
     */
    public byte[] toByteArray() {

        if (format == Format.NONE || Format.isTableFormat(format)) {
            final byte[] bytes;
            if (format == Format.TABLE_V5) {

                int prefixLength = 2;  // format and bitset

                if (regionId != Region.NULL_REGION_ID) {
                    final int regionIdLen2 = PackedInteger.getWriteIntLength(regionId);
                    prefixLength += regionIdLen2;
                }
                if (rowMetadata != null) {
                    byte[] mdba = rowMetadata.getBytes(StandardCharsets.UTF_8);
                    final int metadataLenLen = PackedInteger.getWriteIntLength(
                        rowMetadata.length());
                    prefixLength += metadataLenLen + mdba.length;
                }

                bytes = new byte[prefixLength + val.length];

                bytes[0] = (byte)(format.ordinal() - 1);
                int alreadyWritten = 1;

                BitSet options = new BitSet(8);
                options.set(TABLEV5_REGIONID_BIT, regionId != Region.NULL_REGION_ID);      // has regionId
                options.set(TABLEV5_ROWMETADATA_BIT, rowMetadata != null);   // has metadata
                byte[] bitsetBytes = options.isEmpty() ? new byte[]{0} : options.toByteArray();
                System.arraycopy(bitsetBytes, 0, bytes, 1, bitsetBytes.length);
                alreadyWritten += bitsetBytes.length;
                if (regionId != Region.NULL_REGION_ID) {
                    PackedInteger.writeInt(bytes, alreadyWritten, regionId);
                    alreadyWritten += PackedInteger.getWriteIntLength(regionId);
                }
                if (rowMetadata != null) {
                    byte[] mdba = rowMetadata.getBytes(StandardCharsets.UTF_8);
                    PackedInteger.writeInt(bytes, alreadyWritten, mdba.length);
                    alreadyWritten += PackedInteger.getWriteIntLength(mdba.length);
                    System.arraycopy(mdba, 0, bytes, alreadyWritten, mdba.length);
                    alreadyWritten += mdba.length;
                }
                System.arraycopy(val, 0, bytes, alreadyWritten, val.length);

            } else if (format == Format.MULTI_REGION_TABLE) {
                final int regionIdLen =
                    PackedInteger.getWriteIntLength(regionId);
                bytes = new byte[val.length + regionIdLen + 1];
                bytes[0] = (byte)(format.ordinal() - 1);
                PackedInteger.writeInt(bytes, 1, regionId);
                System.arraycopy(val, 0, bytes, regionIdLen + 1, val.length);
            } else {
                bytes = new byte[val.length + 1];
                bytes[0] = (byte) (format == Format.NONE ? 0 :
                    format.ordinal() - 1);
                System.arraycopy(val, 0, bytes, 1, val.length);
            }

            return bytes;
        }

        return val;
    }

    /**
     * Deserializes the given bytes that were returned earlier by {@link
     * #toByteArray} and returns the resulting Value.
     * <p>
     * The intended use case for the {@link #toByteArray} and {@link
     * #fromByteArray} methods is to serialize values of various formats in a
     * uniform manner, for storage outside of NoSQL DB or for sending across a
     * network.
     * <p>
     * Values created with either the current or earlier releases can be used
     * with this method, but values created by later releases are not
     * guaranteed to be compatible.
     * <p>
     * <em>WARNING:</em> Misuse of this method could result in data corruption
     * if the returned object is added to the store.  The array passed to this
     * method must have been previously created by calling {@link
     * #fromByteArray}.  To create a {@link Value} object of format {@link
     * Format#NONE}, call {@link #createValue} instead.
     *
     * @see #toByteArray
     */
    public static Value fromByteArray(byte[] bytes) {

        if (bytes.length == 0) {
            throw new IllegalStateException
                ("Value is zero length, format discriminator is missing");
        }

        final Format format = Format.fromFirstByte(bytes[0]);

        if (format == Format.AVRO) {
            return new Value(bytes, format);
        }

        if (format == Format.NONE || Format.MULTI_REGION_TABLE.compareTo(format) > 0) {
            final byte[] val = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, val, 0, val.length);
            return new Value(val, format);
        }

        if (format == Format.MULTI_REGION_TABLE) {
            final int regionIdLen = PackedInteger.getReadIntLength(bytes, 1);
            final int regionId = PackedInteger.readInt(bytes, 1);
            final byte[] val = new byte[bytes.length - regionIdLen - 1];
            System.arraycopy(bytes, regionIdLen + 1, val, 0, val.length);
            return new Value(val, format, regionId, null);
        }

        if (format == Format.TABLE_V5) {
            int regionId = Region.NULL_REGION_ID;
            String metadata = null;
            BitSet options = BitSet.valueOf(new byte[] {bytes[1]});
            int alreadyRead = 2;

            if (options.get(TABLEV5_REGIONID_BIT)) { // contains multi-region
                regionId = PackedInteger.readInt(bytes, alreadyRead);
                alreadyRead += PackedInteger.getWriteIntLength(regionId);
            }

            if (options.get(TABLEV5_ROWMETADATA_BIT)) { // contains metadata
                int metadataLen = PackedInteger.readInt(bytes, alreadyRead);
                alreadyRead += PackedInteger.getWriteIntLength(metadataLen);
                metadata = new String(bytes, alreadyRead, metadataLen, StandardCharsets.UTF_8);
                alreadyRead += metadataLen;
            }
            final byte[] val = new byte[bytes.length - alreadyRead];
            System.arraycopy(bytes, alreadyRead, val, 0, val.length);
            return new Value(val, format, regionId, metadata);
        }


        throw new IllegalStateException("Unknown format: " + format);
    }

    /**
     * Creates a Value from a value byte array.
     *
     * The format of the returned value is {@link Format#NONE}.
     */
    public static Value createValue(byte[] val) {
        return new Value(val, Format.NONE);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates a value with a given format and region
     */
    public static Value internalCreateValue(byte[] val,
                                            Format format,
                                            int regionId) {
        return new Value(val, format, regionId, null);
    }

    /**
     * For internal use only.
     * @hidden
     *
     * Creates a value with a given format, region and rowMetadata
     */
    public static Value internalCreateValue(byte[] val,
        Format format,
        int regionId,
        String rowMetadata) {
        return new Value(val, format, regionId, rowMetadata);
    }

    /**
     * Returns the value byte array.
     */
    public byte[] getValue() {
        return val;
    }

    /**
     * Returns the value's format.
     *
     * @since 2.0
     */
    public Format getFormat() {
        return format;
    }

    /**
     * Return region Id of this value.
     @hidden For internal use only
     */
    public int getRegionId() {
        return regionId;
    }

    /**
     * Returns the row metadata of this value.
     * @hidden For internal use only
     */
    public String getRowMetadata() {
        return rowMetadata;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Value)) {
            return false;
        }
        final Value o = (Value) other;
        if (format != o.format ||
            regionId != o.regionId) {
            return false;
        }
        return Arrays.equals(val, o.val);
    }

    @Override
    public int hashCode() {
        return (format.hashCode() * 31) + Arrays.hashCode(val) +
            regionId;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("<Value format:");
        sb.append(format);
        if (format == Format.MULTI_REGION_TABLE) {
            sb.append(" region ID:");
            sb.append(regionId);
        } else if (format == Format.TABLE_V5) {
            sb.append(" region ID:");
            sb.append(regionId);
            sb.append(" metadata:'");
            sb.append(rowMetadata);
            sb.append("'");
        }
        sb.append(" bytes:");
        for (int i = 0; i < 100 && i < val.length; i += 1) {
            sb.append(' ');
            sb.append(val[i]);
        }
        if (val.length > 100) {
            sb.append(" ...");
        }
        sb.append(">");
        return sb.toString();
    }

    /**
     * Create a tombstone value which only contains the format, region id,
     * row metadata and an empty byte array.
     *
     * @hidden For internal use only
     */
    public static Value createTombstoneValue(int regionId, String rowMetadata) {
        if (regionId == Region.NULL_REGION_ID && rowMetadata == null) {
            return internalCreateValue(new byte[0], Format.NONE,
                Region.NULL_REGION_ID, null /* rowMetadata */);
        }
        if (rowMetadata == null) {
            return internalCreateValue(new byte[0], Format.MULTI_REGION_TABLE,
                regionId, null /* rowMetadata */);
        }
        return internalCreateValue(new byte[0], Format.TABLE_V5, regionId,
            rowMetadata);
    }

    /**
     * Returns true if tombstone, i.e. it can contain regionId or rowMetadata
     * but payload (row data) is empty.
     */
    public static boolean isTombstone(byte[] bytes) {
        return getValueOffset(bytes) == bytes.length;
    }

    /**
     * Returns true if there is a regionId in the entire encoded row, otherwise false.
     */
    public static boolean hasRegionId(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }

        final Value.Format format = Value.Format.fromFirstByte(bytes[0]);

        if (Value.Format.MULTI_REGION_TABLE.compareTo(format) > 0) {
            return false;
        }

        if (format == Value.Format.MULTI_REGION_TABLE) {
            return true;
        }

        if (format == Format.TABLE_V5) {
            BitSet options = BitSet.valueOf(new byte[]{bytes[1]});

            return options.get(TABLEV5_REGIONID_BIT);
        }

        throw new IllegalStateException("Invalid format: " + format);
    }

    /**
     * Returns true if there is a regionId in the entire encoded row, otherwise false.
     */
    public static boolean hasRowMetadata(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }
        final Value.Format format = Value.Format.fromFirstByte(bytes[0]);

        if (Format.TABLE_V5.compareTo(format) > 0) {
            return false;
        }

        if (format == Format.TABLE_V5) {
            BitSet options = BitSet.valueOf(new byte[]{bytes[1]});

            return options.get(TABLEV5_ROWMETADATA_BIT);
        }

        throw new IllegalStateException("Invalid format: " + format);
    }

    /**
     * Returns the regionId given the entire encoded row or
     * {@link Region#NULL_REGION_ID} if not present.
     */
    public static int getRegionIdFromByteArray(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return Region.NULL_REGION_ID;
        }

        final Value.Format format = Value.Format.fromFirstByte(bytes[0]);

        // all formats before MULTI_REGION_FORMAT
        if (Value.Format.MULTI_REGION_TABLE.compareTo(format) > 0) {
            return Region.NULL_REGION_ID;
        }

        if (format == Value.Format.MULTI_REGION_TABLE) {
            final int regionId = PackedInteger.readInt(bytes, 1);
            return regionId;
        }

        if (format == Format.TABLE_V5) {
            int regionId = Region.NULL_REGION_ID;
            BitSet options = BitSet.valueOf(new byte[] {bytes[1]});
            int alreadyRead = 2;

            if (options.get(TABLEV5_REGIONID_BIT)) { // contains regionId
                regionId = PackedInteger.readInt(bytes, alreadyRead);
            }
            return regionId;
        }

        throw new IllegalStateException("Invalid format: " + format);
    }

    /**
     * Returns the offset index (starts with 0) of the row value given the
     * entire encoded row.
     */
    public static int getValueOffset(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalStateException("Invalid bytes value: " + Arrays.toString(bytes));
        }

        final Value.Format format = Value.Format.fromFirstByte(bytes[0]);

        // all formats before MULTI_REGION_FORMAT
        if (Value.Format.MULTI_REGION_TABLE.compareTo(format) > 0) {
            return 1;
        }

        if (format == Value.Format.MULTI_REGION_TABLE) {
            /* skip bytes of region id */
            final int regionIdLen = PackedInteger.getReadIntLength(bytes, 1);
            return regionIdLen + 1;
        }

        if (format == Format.TABLE_V5) {
            BitSet options = BitSet.valueOf(new byte[] {bytes[1]});
            int offset = 2;

            if (options.get(TABLEV5_REGIONID_BIT)) { // contains regionId
                int regionId = PackedInteger.readInt(bytes, offset);
                offset += PackedInteger.getWriteIntLength(regionId);
            }

            if (options.get(TABLEV5_ROWMETADATA_BIT)) { // contains metadata
                int metadataLen = PackedInteger.readInt(bytes, offset);
                offset += PackedInteger.getWriteIntLength(metadataLen);
                offset += metadataLen;
            }
            return offset;
        }

        throw new IllegalStateException("Invalid format: " + format);
    }

    /**
     * Sets regionId and rowMetadata if available and returns the offset of row data.
     */
    public static int setRegionIdAndRowMetadata(byte[] bytes, RowImpl row) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalStateException("Invalid bytes value: " + Arrays.toString(bytes));
        }

        final Value.Format format = Value.Format.fromFirstByte(bytes[0]);

        // all formats before MULTI_REGION_FORMAT
        if (Value.Format.MULTI_REGION_TABLE.compareTo(format) > 0) {
            return 1;
        }

        if (format == Value.Format.MULTI_REGION_TABLE) {
            final int regionIdLen = PackedInteger.getReadIntLength(bytes, 1);
            final int regionId = PackedInteger.readInt(bytes, 1);
            row.setRegionId(regionId);
            return regionIdLen + 1;
        }

        if (format == Format.TABLE_V5) {
            BitSet options = BitSet.valueOf(new byte[] {bytes[1]});
            int offset = 2; /* 1 format, 1 bitset */

            if (options.get(TABLEV5_REGIONID_BIT)) { // contains regionId
                int regionId = PackedInteger.readInt(bytes, offset);
                row.setRegionId(regionId);
                offset += PackedInteger.getWriteIntLength(regionId);
            }

            if (options.get(TABLEV5_ROWMETADATA_BIT)) { // contains metadata
                int metadataLen = PackedInteger.readInt(bytes, offset);
                offset += PackedInteger.getWriteIntLength(metadataLen);
                String metadata = new String(bytes, offset, metadataLen, StandardCharsets.UTF_8);
                row.setRowMetadata(metadata);
                offset += metadataLen;
            }
            return offset;
        }

        throw new IllegalStateException("Invalid format: " + format);
    }
}
