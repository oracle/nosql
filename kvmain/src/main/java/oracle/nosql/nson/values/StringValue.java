/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.math.BigDecimal;

import com.fasterxml.jackson.core.io.CharTypes;

import oracle.nosql.nson.util.SizeOf;

/**
 * A {@link FieldValue} instance representing a string value.
 */
public class StringValue extends FieldValue {

    private final String value;

    /**
     * Creates a new instance
     *
     * @param value the value to use
     */
    public StringValue(String value) {
        super();
        requireNonNull(value, "StringValue: value must be non-null");
        this.value = value;
    }

    @Override
    public Type getType() {
        return Type.STRING;
    }

    /**
     * Returns the String value
     *
     * @return the String value of this object
     */
    public String getValue() {
        return value;
    }

    @Override
    public int getInt() {
        return Integer.parseInt(value);
    }

    @Override
    public double getDouble() {
        return Double.parseDouble(value);
    }

    @Override
    public long getLong() {
        return Long.parseLong(value);
    }

    @Override
    public BigDecimal getNumber() {
        return new BigDecimal(value);
    }

    @Override
    public boolean getBoolean() {
        return Boolean.parseBoolean(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StringValue) {
            return value.equals(((StringValue)other).value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof StringValue) {
            return value.compareTo(other.asString().getValue());
        }
        throw new ClassCastException("Object is not an StringValue");
    }

    @Override
    public String toJson(JsonOptions options) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        CharTypes.appendQuoted(sb, value);
        sb.append("\"");
        return sb.toString();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.stringSize(value));
    }
}
