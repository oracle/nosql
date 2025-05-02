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

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SizeOf;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.StringValue;

import com.fasterxml.jackson.core.io.CharTypes;

public class StringValueImpl extends FieldValueImpl implements StringValue {

    private static final long serialVersionUID = 1L;

    static final StringValueImpl EMPTY = new StringValueImpl("");

    public static final StringValueImpl MINUUID =
        new StringValueImpl("00000000-0000-0000-0000-000000000000");

    static final int UUID_STRING_LENGTH = 36;

    protected String value;

    private static final char MIN_VALUE_CHAR = ((char) 1);

    public StringValueImpl(String value) {
        this.value = value;
    }

    /**
     * Constructor for FastExternalizable
     */
    StringValueImpl(DataInput in, short serialVersion) throws IOException {
        value = readString(in, serialVersion);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldValueImpl}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) {@code value}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, value);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.STRING_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public StringValueImpl clone() {
        return new StringValueImpl(value);
    }

    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.stringSize(value));
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StringValueImpl)) {
            return false;
        }
        final StringValueImpl other = (StringValueImpl) obj;
        return Objects.equals(value, other.value);
    }

    @Override
    public int compareTo(FieldValue obj) {
        if (!(obj instanceof StringValueImpl)) {
            throw new ClassCastException("Object is not an StringValue");
        }
        final StringValueImpl other = (StringValueImpl) obj;
        final String otherValue = other.value;

        /* Sort null value first */
        if (value == null) {
            return (otherValue == null) ? 0 : -1;
        }
        if (otherValue == null) {
            return 1;
        }
        return value.compareTo(otherValue);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.STRING;
    }

    @Override
    public StringDefImpl getDefinition() {
        return FieldDefImpl.Constants.stringDef;
    }

    @Override
    public StringValue asString() {
        return this;
    }

    @Override
    public boolean isString() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    @Override
    public boolean isNull() {
        return (value == null);
    }

    /*
     * Public api methods from StringValue
     */

    @Override
    public String get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public String getString() {
        return value;
    }

    @Override
    public void setString(String v) {
        value = v;
    }

    @Override
    public String castAsString() {
        return value;
    }

    /**
     * The "next" value, lexicographically, is this string with a
     * minimum character (1) added.
     */
    @Override
    FieldValueImpl getNextValue() {
        return new StringValueImpl(incrementString(value));
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new StringValueImpl("");
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {

        if ((field != null) && field.isUUIDString()) {
            return packUUIDtoPrimKey(value);
        }

        return toKeyString(value);
    }

    @Override
    public void toStringBuilder(StringBuilder sb, DisplayFormatter formatter) {
        if (value == null) {
            sb.append("null");
            return;
        }

        sb.append('\"');
        CharTypes.appendQuoted(sb, value);
        sb.append('\"');
    }

    /*
     * Local methods
     */

    public static StringValueImpl create(String value) {
        return new StringValueImpl(value);
    }

    public static String incrementString(String value) {
        return value + MIN_VALUE_CHAR;
    }

    static String toKeyString(String value) {
        return value;
    }

    /*
     * Pack the canonical-string representation of a UUID into a 16-byte array.
     */
    static byte[] packUUID(String value) {
        if (value.length() != UUID_STRING_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid UUID string: " + value);
        }

        byte[] buffer = new byte[16];
        int byteIdx = 0;

        for (int i = 0, j = 0; i < value.length(); ++i) {

            char ch = value.charAt(i);

            if (i == 8 || i == 13 || i == 18 || i == 23 ) {
                if (ch == '-') {
                    continue;
                }
                throw new IllegalArgumentException(
                        "Invalid UUID string: " + value);
            }

            byte b = encodeUUIDChar(ch, value);

            if (j % 2 == 0) {
                buffer[byteIdx] = (byte)(b << 4);
            } else {
                buffer[byteIdx] |= b;
                ++byteIdx;
            }

            ++j;
        }

        return buffer;
    }

    /*
     * Unpack a uuid stored as a 16-byte array into its canonical-string
     * representation.
     */
    static String unpackUUID(byte[] bytes) {

        assert(bytes.length == 16);

        char[] chars = new char[UUID_STRING_LENGTH];

        for (int i = 0, charIdx = 0; i < 16; ++i) {

            byte b1 = (byte)((bytes[i] & 0xf0) >>> 4);
            byte b2 = (byte)(bytes[i] & 0x0f);
            char ch1 = decodeUUIDChar(b1);
            char ch2 = decodeUUIDChar(b2);

            chars[charIdx] = ch1;
            ++charIdx;
            chars[charIdx] = ch2;
            ++charIdx;

            if (charIdx == 8 || charIdx == 13 || charIdx == 18 || charIdx == 23) {
                chars[charIdx] = '-';
                ++charIdx;
            }
        }

        return new String(chars);
    }

    /*
     * convert from UUID canonical string to key string format
     */
    static String packUUIDtoPrimKey(String value) {

        byte[] buffer = packUUID(value);
        return SortableString.toSortable(buffer);
    }

    /*
     * convert from key string to UUID canonical string
     */
    static String unpackUUIDfromPrimKey(String keyString) {

        byte[] bytes  = SortableString.bytesFromSortable(keyString);
        return unpackUUID(bytes);
    }

    /**
     * This creates a StringValueImpl from the String format used
     * for sorted keys.
     */
    static StringValueImpl createUUIDFromKey(String keyString) {
        return new StringValueImpl(unpackUUIDfromPrimKey(keyString));
    }

    private static byte encodeUUIDChar(char ch, String value) {

        switch (ch) {
        case '0':
            return 0;
        case '1':
            return 1;
        case '2':
            return 2;
        case '3':
            return 3;
        case '4':
            return 4;
        case '5':
            return 5;
        case '6':
            return 6;
        case '7':
            return 7;
        case '8':
            return 8;
        case '9':
            return 9;
        case 'a':
        case 'A':
            return 10;
        case 'b':
        case 'B':
            return 11;
        case 'c':
        case 'C':
            return 12;
        case 'd':
        case 'D':
            return 13;
        case 'e':
        case 'E':
            return 14;
        case 'f':
        case 'F':
            return 15;
        default:
            throw new IllegalArgumentException(
                "Invalid UUID string: " + value);
        }
    }

    private static char decodeUUIDChar(byte b) {

        switch (b) {
        case 0:
            return '0';
        case 1:
            return '1';
        case 2:
            return '2';
        case 3:
            return '3';
        case 4:
            return '4';
        case 5:
            return '5';
        case 6:
            return '6';
        case 7:
            return '7';
        case 8:
            return '8';
        case 9:
            return '9';
        case 10:
            return 'a';
        case 11:
            return 'b';
        case 12:
            return 'c';
        case 13:
            return 'd';
        case 14:
            return 'e';
        case 15:
            return 'f';
        default:
            throw new IllegalArgumentException(
                "Unexpected byte in packed uuid:" + b);
        }
    }

    /**
     *  Increment UUID string
     *
     * In its canonical textual representation, the 16 octets of a UUID are
     * represented as 32 hexadecimal (base-16) digits, displayed
     * in five groups separated by hyphens,
     * in the form 8-4-4-4-12 for a total of 36 characters
     * (32 hexadecimal characters and 4 hyphens).
     * For example:
     *  123e4567-e89b-12d3-a456-426614174000
     *  xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx
     *  The four-bit M and the 1 to 3 bit N fields code the format of the UUID
     *  itself.
     *
     *  The M and N fields are reserved when increment UUID string.
     *
     */
    static StringValueImpl incrementUUIDString(String uuidString) {

        if (uuidString.length() != UUID_STRING_LENGTH) {
                throw new IllegalArgumentException(
                    "Invalid UUID string: " + uuidString);
        }

        char[] chars = uuidString.toCharArray();
        int i;

        for(i = chars.length - 1; i >= 0; i--) {

            char ch = chars[i];

            if (ch >= 'A' && ch <= 'F') {
                ch = Character.toLowerCase(ch);
                chars[i] = ch;
            }

            if (ch == '-') {
                if (i != 8 && i != 13 && i != 18 && i != 23) {
                    throw new IllegalArgumentException(
                        "Invalid UUID string: " + uuidString);
                }
                continue;
            } else if ((ch < '9') || (ch > '9' && ch < 'f')) {
                chars[i]++;
                break;
            } else if (ch == '9') {
                chars[i] = 'a';
                break;
            } else if (ch == 'f') {
                chars[i] = '0';
            } else {
                throw new IllegalArgumentException(
                    "Invalid UUID string: " + uuidString);
            }
        }

        if (i < 0) {
            return null;
        }

        return new StringValueImpl(String.valueOf(chars));
    }
}
