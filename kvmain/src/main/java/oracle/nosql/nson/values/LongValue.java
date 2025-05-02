/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.math.BigDecimal;
import java.sql.Timestamp;

import oracle.nosql.nson.util.SizeOf;

/**
 * A {@link FieldValue} instance representing a long value.
 */
public class LongValue extends FieldValue {

    private long value;

    /**
     * Creates a new instance.
     *
     * @param value the value to use
     */
    public LongValue(long value) {
        super();
        this.value = value;
    }

    /**
     * Creates a new instance from a String value
     *
     * @param value the value to use
     *
     * @throws NumberFormatException if the value is not a valid long
     */
    public LongValue(String value) {
        super();
        this.value = Long.parseLong(value);
    }

    @Override
    public Type getType() {
        return Type.LONG;
    }

    /**
     * Returns the long value of this object
     *
     * @return the long value
     */
    public long getValue() {
        return value;
    }

    /**
     * @hidden
     *
     * @param v the value to use
     */
    public void setValue(long v) {
        value = v;
    }

    /**
     * Casts this long to a double, possibly with loss of information about
     * magnitude, precision or sign.
     *
     * @return a double value
     */
    @Override
    public double castAsDouble() {
        return value;
    }


    @Override
    public int compareTo(FieldValue other) {
        requireNonNull(other, "LongValue.compareTo: other must be non-null");
        return Long.compare(value, other.getLong());
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
    public int getInt() {
        return java.lang.Math.toIntExact(value);
    }

    @Override
    public String getString() {
        return toJson(null);
    }

    /**
     * Returns a Timestamp value for this object based on the Timestamp
     * value of the long value
     *
     * @return the Timestamp value
     */
    @Override
    public Timestamp getTimestamp() {
        return new TimestampValue(value).getTimestamp();
    }

    @Override
    public String toJson(JsonOptions options) {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LongValue) {
            return value == ((LongValue)other).value;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Long) value).hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return SizeOf.OBJECT_OVERHEAD + 8;
    }
}
