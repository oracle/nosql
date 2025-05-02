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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.IntegerDef;

/**
 * IntegerDefImpl is an implementation of the IntegerDef interface.
 */
public class IntegerDefImpl extends FieldDefImpl
    implements IntegerDef {

    private static final long serialVersionUID = 1L;

    /*
     * min and max are inclusive
     */
    private final Integer min;
    private final Integer max;
    private int encodingLength;

    final private boolean isMRCounter;

    /**
     * Constructor requiring all fields.
     */
    IntegerDefImpl(String description, Integer min, Integer max) {
        super(Type.INTEGER, description);
        this.min = min;
        this.max = max;
        validate();
        isMRCounter = false;
    }

    /**
     * Constructor requiring just the description.
     */
    IntegerDefImpl(String description) {
        this(description, null, null);
    }

    /**
     * This constructor defaults most fields.
     */
    IntegerDefImpl() {
        super(Type.INTEGER);
        min = null;
        max = null;
        encodingLength = 0; /* 0 means full-length */
        isMRCounter = false;
    }

    private IntegerDefImpl(IntegerDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
        encodingLength = impl.encodingLength;
        this.isMRCounter = impl.isMRCounter;
    }

    /**
     * Constructor for CRDT.
     */
    IntegerDefImpl(boolean isMRCounter) {
        this(isMRCounter, null);
    }

    IntegerDefImpl(boolean isMRCounter, String description) {
        super(Type.INTEGER, description);
        min = null;
        max = null;
        encodingLength = 0;
        this.isMRCounter = isMRCounter;
    }

    /**
     * Constructor for FastExternalizable
     */
    IntegerDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.INTEGER);
        min = readIntegerOrNull(in);
        max = readIntegerOrNull(in);
        encodingLength = readPackedInt(in);
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (serialVersion >= SerialVersion.COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1) {
            isMRCounter = in.readBoolean();
        } else {
            isMRCounter = false;
        }
    }

    private Integer readIntegerOrNull(DataInput in) throws IOException {
        return in.readBoolean() ? readPackedInt(in) : null;
    }
    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@code boolean}) <i> min != null </i>
     * <li> <i>[Optional]</i>
     *                      ({@link SerializationUtil#writePackedInt packed int})
     *                      {@code min} // if min != null
     * <li> ({@code boolean}) <i> max != null </i>
     * <li> <i>[Optional]</i>
     *                      ({@link SerializationUtil#writePackedInt packed int})
     *                      {@code max} // if max != null
     * <li> <i>[Optional]</i>
     *                      {{@link SerializationUtil#writeFastExternalOrNull
     *                      MRCounterInfo}}
     *                      {@code crdtInfo}
     *                       // if SerialVersion.COUNTER_CRDT or higher
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeIntegerOrNull(out, min);
        writeIntegerOrNull(out, max);
        writePackedInt(out, encodingLength);
        if (isMRCounter) {
            out.writeBoolean(true);
        } else {
            out.writeBoolean(false);
        }
    }

    private void writeIntegerOrNull(DataOutput out, Integer value)
            throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writePackedInt(out, value);
        }
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public IntegerDefImpl clone() {

        if (this == FieldDefImpl.Constants.integerDef) {
            return this;
        }

        return new IntegerDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0) +
            (isMRCounter ? Boolean.hashCode(isMRCounter) : 0);
    }

    @Override
    public boolean equals(Object other) {

        if (!(other instanceof IntegerDefImpl)) {
            return false;
        }
        return ((IntegerDefImpl)other).isMRCounter == isMRCounter;
    }

    @Override
    public boolean isValidKeyField() {
        if (isMRCounter) {
            /* CRDT should not be primary key. */
            return false;
        }
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        if (isMRCounter) {
            //TODO: Remove this check after index is supported on CRDT columns.
            return false;
        }
        return true;
    }

    @Override
    public IntegerDef asInteger() {
        return this;
    }

    @Override
    public boolean isMRCounter() {
        return isMRCounter;
    }

    @Override
    public IntegerValueImpl createInteger(int value) {
        return (hasMin() || hasMax() ?
                new IntegerRangeValue(value, this) :
                new IntegerValueImpl(value));
    }

    @Override
    IntegerValueImpl createInteger(String value) {
        return (hasMin() || hasMax() ?
                new IntegerRangeValue(value, this) :
                new IntegerValueImpl(value));
    }

    /*
     * Public api methods from IntegerDef
     */

    @Deprecated
    @Override
    public Integer getMin() {
        return min;
    }

    @Deprecated
    @Override
    public Integer getMax() {
        return max;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    int getEncodingLength() {
        return encodingLength;
    }

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
        if (superType.isInteger() ||
            superType.isLong() ||
            superType.isNumber() ||
            superType.isAny() ||
            superType.isAnyJsonAtomic() ||
            superType.isAnyAtomic() ||
            superType.isJson()) {
            return true;
        }

        return false;
    }

    @Override
    public IntegerCRDTValueImpl createCRDTValue() {
        return new IntegerCRDTValueImpl();
    }


    @Override
    public FieldDefImpl getCRDTElement() {
        return FieldDefImpl.Constants.integerDef;
    }

    @Override
    public short getRequiredSerialVersion() {
        if (!isMRCounter) {
            return super.getRequiredSerialVersion();
        }
        return (short) Math.max(SerialVersion.COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1,
                                super.getRequiredSerialVersion());

    }

    /*
     * local methods
     */

    private void validate() {

        if (min != null && max != null) {
            if (min > max) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
        encodingLength = SortableString.encodingLength(min, max);
    }

    /**
     * Validates the value against the range if one exists.
     * min/max are inclusive.
     */
    void validateValue(int val) {

        if ((min != null && val < min) || (max != null && val > max)) {
            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            throw new IllegalArgumentException(sb.toString());
        }
    }

    @Override
    public Type getJsonCounterType() {
        return Type.JSON_INT_MRCOUNTER;
    }

}
