/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.math.BigDecimal;

import oracle.nosql.nson.util.SizeOf;

/**
 * A {@link FieldValue} instance representing an integer value.
 */
public class IntegerValue extends FieldValue {

    private final int value;

    /**
     * Creates a new instance.
     *
     * @param value the value to use
     */
    public IntegerValue(int value) {
        super();
        this.value = value;
    }

    /**
     * Creates a new instance from a String value
     *
     * @param value the value to use
     *
     * @throws NumberFormatException if the value is not a valid integer
     */
    public IntegerValue(String value) {
        super();
        this.value = Integer.parseInt(value);
    }

    @Override
    public Type getType() {
        return Type.INTEGER;
    }

    /**
     * Returns the integer value of this object
     *
     * @return the integer value
     */
    public int getValue() {
        return value;
    }

    @Override
    public int compareTo(FieldValue other) {
        return Integer.compare(value, (other.getInt()));
    }

    /**
     * Returns a long value for this object.
     *
     * @return the long value
     */
    @Override
    public long getLong() {
        return value;
    }

    /**
     * Casts this integer to a double, possibly with loss of information about
     * magnitude, precision or sign.
     *
     * @return a double value
     */
    @Override
    public double castAsDouble() {
        return value;
    }

    /**
     * Returns a Double value for this object.
     *
     * @return the double value
     */
    @Override
    public double getDouble() {
        return value;
    }

    /**
     * Returns a BigDecimal value for this object.
     *
     * @return the BigDecimal value
     */
    @Override
    public BigDecimal getNumber() {
        return new BigDecimal(value);
    }

    @Override
    public String getString() {
        return toJson(null);
    }

    @Override
    public String toJson(JsonOptions options) {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IntegerValue) {
            return value == ((IntegerValue)other).value;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Integer) value).hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return SizeOf.OBJECT_OVERHEAD + 4;
    }
}
