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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.table.DoubleDef;

/**
 * DoubleDefImpl implements the DoubleDef interface.
 */
public class DoubleDefImpl extends FieldDefImpl implements DoubleDef {

    private static final long serialVersionUID = 1L;

    private final Double min;
    private final Double max;

    /**
     * Constructor requiring all fields.
     */
    DoubleDefImpl(String description, Double min, Double max) {
        super(Type.DOUBLE, description);
        this.min = min;
        this.max = max;
        validate();
    }

    DoubleDefImpl(String description) {
        this(description, null, null);
    }

    /**
     * This constructor defaults most fields.
     */
    DoubleDefImpl() {
        this(null, null, null);
    }

    private DoubleDefImpl(DoubleDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
    }

    /**
     * Constructor for FastExternalizable
     */
    DoubleDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.DOUBLE);
        min = readDoubleOrNull(in);
        max = readDoubleOrNull(in);
    }

    private Double readDoubleOrNull(DataInput in) throws IOException {
        return in.readBoolean() ? in.readDouble() : null;
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@code boolean}) <i> min != null </i>
     * <li> <i>[Optional]</i> ({@code double}) {@code min} // if min != null
     * <li> ({@code boolean}) <i> max != null </i>
     * <li> <i>[Optional]</i> ({@code double}) {@code max} // if max != null
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeDoubleOrNull(out, min);
        writeDoubleOrNull(out, max);
    }

    private void writeDoubleOrNull(DataOutput out, Double value)
            throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeDouble(value);
        }
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public DoubleDefImpl clone() {

        if (this == FieldDefImpl.Constants.doubleDef) {
            return this;
        }

        return new DoubleDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DoubleDefImpl;
    }

    @Override
    public boolean isValidKeyField() {
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public DoubleDef asDouble() {
        return this;
    }

    @Override
    public DoubleValueImpl createDouble(double value) {

        return (hasMin() || hasMax() ?
                new DoubleRangeValue(value, this) :
                new DoubleValueImpl(value));
    }

    @Override
    DoubleValueImpl createDouble(String value) {

        return (hasMin() || hasMax() ?
                new DoubleRangeValue(value, this) :
                new DoubleValueImpl(value));
    }

    /*
     * Public api methods from DoubleDef
     */

    @Deprecated
    @Override
    public Double getMin() {
        return min;
    }

    @Deprecated
    @Override
    public Double getMax() {
        return max;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean hasMin() {
        return min != null;
    }

    @Override
    public boolean hasMax() {
        return max != null;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isDouble() ||
            superType.isNumber() ||
            superType.isAny() ||
            superType.isAnyAtomic() ||
            superType.isAnyJsonAtomic() ||
            superType.isJson()) {
            return true;
        }

        return false;
    }

    /*
     * local methods
     */

    private void validate() {

        /* Make sure min <= max */
        if (min != null && max != null) {
            if (min > max) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
    }

    /**
     * Validates the value against the range if one exists.
     * min/max are inclusive
     */
    void validateValue(double val) {
        if ((min != null && val < min) ||
            (max != null && val > max)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
