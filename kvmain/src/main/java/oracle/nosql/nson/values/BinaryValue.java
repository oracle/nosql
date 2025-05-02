/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.util.Arrays;

import com.fasterxml.jackson.core.Base64Variants;

import oracle.nosql.nson.util.SizeOf;

/**
 * A {@link FieldValue} instance representing a binary value. Instances of
 * BinaryValue are only created by construction or returned by data operations.
 * They will never be directly created by parsing JSON. On input a Base64
 * encoded string can be accepted as a binary type if the database type is
 * binary for the field. When represented as JSON this object is encoded
 * as Base64 using {@link #encodeBase64}.
 */
public class BinaryValue extends FieldValue {

    private final byte[] value;

    /**
     * Creates a new instance.
     *
     * @param value the value to use
     */
    public BinaryValue(byte[] value) {
        super();
        requireNonNull(value, "BinaryValue: value must be non-null");
        this.value = value;
    }

    /**
     * Creates a new instance from a Base64 encoded string.
     *
     * @param value the value to use
     *
     * @throws IllegalArgumentException if the value is not a valid Base64
     * encoded value.
     */
    public BinaryValue(String value) {
        super();
        requireNonNull(value, "BinaryValue: value must be non-null");
        this.value = decodeBase64(value);
    }

    @Override
    public Type getType() {
        return Type.BINARY;
    }

    /**
     * Returns the binary value of this object
     *
     * @return the binary value
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Returns 0 if the two values are equal in terms of length and byte
     * content, otherwise it returns -1.
     */
    @Override
    public int compareTo(FieldValue other) {
        requireNonNull(other,
                       "BinaryValue.compareTo: other value must be non-null");
        byte[] otherValue = other.getBinary();
        if (Arrays.equals(value, otherValue)) {
            return 0;
        }
        return -1;
    }

    /**
     * Returns a quoted string of the value as Base64.
     *
     * @return a quoted, Base64-encoded string encoded using
     * {@link #encodeBase64}.
     */
    @Override
    public String toJson(JsonOptions options) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"").append(encodeBase64(value)).
            append("\"");
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BinaryValue) {
            return compareTo((BinaryValue) other) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    /**
     * Encode the specified byte array into a Base64 encoded string.
     * This string can be decoded using {@link #decodeBase64}.
     *
     * @param buffer the input buffer
     *
     * @return the encoded string
     */
    public static String encodeBase64(byte[] buffer) {
        requireNonNull(buffer,
                       "BinaryValue.encodeBase64: buffer must be non-null");

        return Base64Variants.getDefaultVariant().encode(buffer);
    }

    /**
     * Decode the specified Base64 string into a byte array. The string must
     * have been encoded using {@link #encodeBase64} or the same algorithm.
     *
     * @param binString the encoded input string
     *
     * @return the decoded array
     *
     * @throws IllegalArgumentException if the value is not a valid Base64
     * encoded value.
     */
    public static byte[] decodeBase64(String binString) {
        requireNonNull(binString,
                       "BinaryValue.decodeBase64: binString must be non-null");

        return Base64Variants.getDefaultVariant().decode(binString);
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.byteArraySize(value.length));
    }
}
