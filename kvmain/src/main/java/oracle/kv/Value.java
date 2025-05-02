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
import java.util.Arrays;

import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

import com.sleepycat.util.PackedInteger;

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
        MULTI_REGION_TABLE(4);

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
                   ordinal <= Format.MULTI_REGION_TABLE.ordinal();
        }

        public static boolean isTableFormat(int firstByte) {
            int ordinal = firstByte + 1;
            return ordinal >= Format.TABLE.ordinal() &&
                   ordinal <= Format.MULTI_REGION_TABLE.ordinal();
        }
    }

    /**
     * An instance that represents an empty value for key-only records.
     */
    public static final Value EMPTY_VALUE = Value.createValue(new byte[0]);

    private final byte[] val;
    private final Format format;
    private final int regionId;

    private Value(byte[] val, Format format) {
        this(val, format, Region.NULL_REGION_ID);
    }

    private Value(byte[] val,
                  Format format,
                  int regionId) {
        checkNull("val", val);
        checkNull("format", format);
        if ((format == Format.MULTI_REGION_TABLE) &&
             (!Region.isMultiRegionId(regionId))) {
            throw new IllegalArgumentException(
                "The region id cannot be " + Region.NULL_REGION_ID +
                " for multi-region table");
        }
        if ((format != Format.MULTI_REGION_TABLE) &&
            (regionId != Region.NULL_REGION_ID)) {
            throw new IllegalArgumentException(
                "The region id must be " + Region.NULL_REGION_ID +
                " for local table, id=" + regionId);
        }
        if (regionId < 0) {
            throw new IllegalArgumentException(
                "Illegal region ID: " + regionId);
        }
        this.val = val;
        this.format = format;
        this.regionId = regionId;
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
            if (format == Format.MULTI_REGION_TABLE) {
                /* read compressed region id. */
                regionId = readPackedInt(in);
                final int regionIdLen =
                    PackedInteger.getWriteIntLength(regionId);
                val = new byte[len - (regionIdLen + 1)];
            } else {
                this.regionId = 0;
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
        final int prefixLength;
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
            if (format == Format.MULTI_REGION_TABLE) {
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

        if (format == Format.MULTI_REGION_TABLE) {
            final int regionIdLen = PackedInteger.getReadIntLength(bytes, 1);
            final int regionId = PackedInteger.readInt(bytes, 1);
            final byte[] val = new byte[bytes.length - regionIdLen - 1];
            System.arraycopy(bytes, regionIdLen + 1, val, 0, val.length);
            return new Value(val, format, regionId);
        }

        if (format == Format.NONE || Format.isTableFormat(format)) {
            final byte[] val = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, val, 0, val.length);
            return new Value(val, format);
        }

        final byte[] val = bytes;
        return new Value(val, format);
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
     * Creates a value with a given format.
     */
    public static Value internalCreateValue(byte[] val, Format format) {
        /*
         * Create a value with local region id for multi_region tables, or
         * null region id for local tables. */
        if (format == Format.MULTI_REGION_TABLE) {
            return new Value(val, format, Region.LOCAL_REGION_ID);
        }
        return new Value(val, format);
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
        return new Value(val, format, regionId);
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
     * Create a tombstone value with Format.NONE
     *
     * @hidden For internal cloud use only
     */
    public static Value createTombstoneNoneValue() {
        return internalCreateValue(new byte[0], Format.NONE);
    }

    /**
     * Create a tombstone value which only contains the format, region id
     * and an empty byte array.
     *
     * @hidden For internal use only
     */
    public static Value createTombstoneValue(int regionId) {
        return internalCreateValue(new byte[0],
                                   Format.MULTI_REGION_TABLE,
                                   regionId);
    }
}
