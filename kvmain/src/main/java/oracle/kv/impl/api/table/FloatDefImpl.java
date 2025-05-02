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

import oracle.kv.table.FloatDef;

/**
 * FloatDefImpl implements the FloatDef interface.
 */
public class FloatDefImpl extends FieldDefImpl implements FloatDef {

    private static final long serialVersionUID = 1L;
    /*
     * These are not final to allow for schema evolution.
     */
    private Float min;
    private Float max;

    /**
     * Constructor requiring all fields.
     */
    FloatDefImpl(String description, Float min, Float max) {
        super(Type.FLOAT, description);
        this.min = min;
        this.max = max;
        validate();
    }

    FloatDefImpl(String description) {
        this(description, null, null);
    }

    /**
     * This constructor defaults most fields.
     */
    FloatDefImpl() {
        super(Type.FLOAT);
        min = null;
        max = null;
    }

    private FloatDefImpl(FloatDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
    }

    /**
     * Constructor for FastExternalizable
     */
    FloatDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.FLOAT);
        min = readFloatOrNull(in);
        max = readFloatOrNull(in);
    }
    
    private Float readFloatOrNull(DataInput in) throws IOException {
        return in.readBoolean() ? in.readFloat(): null;
    }
    
    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@code boolean}) <i> min != null </i>
     * <li> <i>[Optional]</i> ({@code float}) {@code min} // if min != null
     * <li> ({@code boolean}) <i> max != null </i>
     * <li> <i>[Optional]</i> ({@code float}) {@code max} // if max != null
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeFloatOrNull(out, min);
        writeFloatOrNull(out, max);
    }
    
    private void writeFloatOrNull(DataOutput out, Float value)
            throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloat(value);
        }
    }
    
    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public FloatDefImpl clone() {

        if (this == FieldDefImpl.Constants.floatDef) {
            return this;
        }

        return new FloatDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FloatDefImpl;
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
    public FloatDef asFloat() {
        return this;
    }

    @Override
    public FloatValueImpl createFloat(float value) {

        return (hasMin() || hasMax() ?
                new FloatRangeValue(value, this) :
                new FloatValueImpl(value));
    }

    @Override
    FloatValueImpl createFloat(String value) {

        return (hasMin() || hasMax() ?
                new FloatRangeValue(value, this) :
                new FloatValueImpl(value));
    }

    /*
     * Public api methods from FloatDef
     */

    @Deprecated
    @Override
    public Float getMin() {
        return min;
    }

    @Deprecated
    @Override
    public Float getMax() {
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

        if (superType.isFloat() ||
            superType.isDouble() ||
            superType.isNumber() ||
            superType.isAny() ||
            superType.isAnyJsonAtomic() ||
            superType.isAnyAtomic() ||
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
     */
    void validateValue(float val) {

        /* min/max are inclusive */
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
