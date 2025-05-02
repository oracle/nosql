/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.math.BigDecimal;
import oracle.nosql.nson.util.SizeOf;

/**
 * A {@link FieldValue} instance representing an arbitrary-precision numeric
 * value. It is stored as a Java {@link BigDecimal} and the serialized format
 * is the String value of that number.
 */
public class NumberValue extends FieldValue {

    /* The BigDecimal that represents the Number value */
    private BigDecimal value;

    /**
     * Creates a new instance.
     *
     * @param value the value to use
     */
    public NumberValue(BigDecimal value) {
        super();
        requireNonNull(value, "NumberValue: value must be non-null");
        this.value = value;
    }

    /**
     * Creates a new instance from a String value
     *
     * @param value the value to use
     *
     * @throws NumberFormatException if the value is not a valid BigDecimal
     */
    public NumberValue(String value) {
        super();
        this.value = new BigDecimal(value);
    }

    @Override
    public Type getType() {
        return Type.NUMBER;
    }

    /**
     * @hidden
     * Internal use only
     *
     * Creates a new instance.
     *
     * @param value the byte array that represents the Number value.
    public NumberValue(byte[] value) {
        super();
        requireNonNull(value, "NumberValue: value must be non-null");
        this.value = value;
    }
     */

    /**
     * Returns the number value of this object
     *
     * @return the number value
     */
    public BigDecimal getValue() {
        return value;
    }

    /**
     * @hidden
     *
     * @param v the value to use
     */
    public void setValue(BigDecimal v) {
        value = v;
    }

    /**
     * Casts this number to a double, possibly with loss of information about
     * magnitude, precision or sign.
     *
     * @return a double value
     */
    @Override
    public double castAsDouble() {
         return value.doubleValue();
    }

    @Override
    public int compareTo(FieldValue other) {

        requireNonNull(other, "NumberValue.compareTo: other must be non-null");

        if (other instanceof NumberValue) {
            return value.compareTo(((NumberValue)other).value);
        }

        BigDecimal otherVal = null;
        switch (other.getType()) {
        case INTEGER:
            otherVal = BigDecimal.valueOf(other.getInt());
            break;
        case LONG:
            otherVal = BigDecimal.valueOf(other.getLong());
            break;
        case DOUBLE:
            /*
             * Parse from String representation, to avoid rounding errors that
             * non-decimal floating operations could incur
             */
            otherVal = BigDecimal.valueOf(other.getDouble());
            break;
        default:
            throw new ClassCastException("Object is not a numeric type");
        }
        return value.compareTo(otherVal);
    }

    @Override
    public String getString() {
        return toJson(null);
    }

    @Override
    public String toJson(JsonOptions options) {
        return value.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof NumberValue) {
            return value.equals(((NumberValue) other).value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                value.toString().length());
    }
}
