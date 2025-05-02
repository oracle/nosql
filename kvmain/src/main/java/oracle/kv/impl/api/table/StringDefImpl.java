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
import static oracle.kv.impl.util.SerialVersion.UUID_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.table.StringDef;

/**
 * StringDefImpl implements the StringDef interface.
 */
public class StringDefImpl extends FieldDefImpl implements StringDef {

    private static final long serialVersionUID = 1L;

    private String min;
    private String max;
    private Boolean minInclusive;
    private Boolean maxInclusive;
    private boolean isUUID = false;
    private boolean generatedByDefault = false;

    StringDefImpl(
        String description,
        String min,
        String max,
        Boolean minInclusive,
        Boolean maxInclusive,
        boolean isUUID,
        boolean generatedByDefault) {

        super(Type.STRING, description);
        this.min = min;
        this.max = max;
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;
        this.isUUID = isUUID;
        this.generatedByDefault = generatedByDefault;
        validate();
    }

    StringDefImpl(
        String description,
        String min,
        String max,
        Boolean minInclusive,
        Boolean maxInclusive){

       this(description, min, max, minInclusive, maxInclusive, false, false);
    }
    StringDefImpl(String description) {
        this(description, null, null, null, null, false, false);
    }

    StringDefImpl() {
        super(Type.STRING);
        min = null;
        max = null;
        minInclusive = null;
        maxInclusive = null;
        isUUID = false;
        generatedByDefault = false;
    }

    StringDefImpl(
        String description,
        boolean isUUID,
        boolean generatedByDefault){
        super(Type.STRING, description);
        min = null;
        max = null;
        minInclusive = null;
        maxInclusive = null;
        this.isUUID = isUUID;
        this.generatedByDefault = generatedByDefault;
    }

    private StringDefImpl(StringDefImpl impl) {
        super(impl);
        min = impl.min;
        max = impl.max;
        minInclusive = impl.minInclusive;
        maxInclusive = impl.maxInclusive;
        isUUID = impl.isUUID;
        generatedByDefault = impl.generatedByDefault;
    }

    /**
     * Constructor for FastExternalizable
     */
    StringDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.STRING);
        min = readString(in, serialVersion);
        max = readString(in, serialVersion);
        minInclusive = readBooleanOrNull(in);
        maxInclusive = readBooleanOrNull(in);
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (serialVersion >= UUID_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1) {
            readFromByte(in);
        } else {
            isUUID = false;
            generatedByDefault = false;
        }
    }

    private Boolean readBooleanOrNull(DataInput in) throws IOException {
        final byte value = in.readByte();
        return value < 0 ? null : (value > 0 ? true : false);
    }

    private void readFromByte(DataInput in) throws IOException {
        final byte value = in.readByte();
        if (value == 1) {
            isUUID = true;
            generatedByDefault = false;
        } else if (value == 2) {
            isUUID = true;
            generatedByDefault = true;
        } else {
            isUUID = false;
            generatedByDefault = false;
        }
    }
    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writeString String}) {@code min}
     * <li> ({@link SerializationUtil#writeString String}) {@code max}
     * <li> ({@code byte} <i>-1 if minInclusive == null,
     *                        0 if minInclusive == false,
     *                        1 if minInclusive = true</i>
     * <li> ({@code byte} <i>-1 if maxInclusive == null,
     *                        0 if maxInclusive == false,
     *                        1 if maxInclusive = true</i>
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, min);
        writeString(out, serialVersion, max);
        writeBooleanOrNull(out, minInclusive);
        writeBooleanOrNull(out, maxInclusive);
        writeToByte(out);
    }

    private void writeBooleanOrNull(DataOutput out, Boolean value)
            throws IOException {
        if (value == null) {
            out.writeByte(-1);
        } else if (value) {
            out.writeByte(1);
        } else {
            out.writeByte(0);
        }
    }

    public void writeToByte(DataOutput out) throws IOException {
        if (generatedByDefault) {
            out.writeByte(2);
        } else if (isUUID) {
            out.writeByte(1);
        } else {
            out.writeByte(0);
        }
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public StringDefImpl clone() {

        if (this == FieldDefImpl.Constants.stringDef) {
            return this;
        }

        return new StringDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() +
            (min != null ? min.hashCode() : 0) +
            (max != null ? max.hashCode() : 0);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StringDefImpl)) {
            return false;
        } else if (((StringDefImpl)other).isUUID != isUUID) {
            return false;
        }
        return ((StringDefImpl)other).generatedByDefault == generatedByDefault;
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
    public StringDef asString() {
        return this;
    }

    @Override
    public StringValueImpl createString(String value) {

        return (hasMin() || hasMax() ?
                new StringRangeValue(value, this) :
                new StringValueImpl(value));
    }

    /*
     * Public api methods from StringDef
     */

    @Deprecated
    @Override
    public String getMin() {
        return min;
    }

    @Deprecated
    @Override
    public String getMax() {
        return max;
    }

    @Deprecated
    @Override
    public boolean isMinInclusive() {
        /* Default value of inclusive is true */
        return (minInclusive != null ? minInclusive : true);
    }

    @Deprecated
    @Override
    public boolean isMaxInclusive() {
        /* Default value of inclusive is true */
        return (maxInclusive != null ? maxInclusive : true);
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

        if (superType.isString() ||
            superType.isAny() ||
            superType.isAnyJsonAtomic() ||
            superType.isAnyAtomic() ||
            superType.isJson()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean isUUIDString() {
        return isUUID;
    }

    public boolean isGenerated() {
        return generatedByDefault;
    }

    @Override
    public short getRequiredSerialVersion() {
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (!isUUID) {
            return super.getRequiredSerialVersion();
        }
        return (short) Math.max(SerialVersion.UUID_VERSION_DEPRECATED_REMOVE_AFTER_PREREQ_25_1,
                                super.getRequiredSerialVersion());

    }

    /*
     * local methods
     */

    private void validate() {
        /* Make sure min <= max */
        if (min != null && max != null) {
            if (min.compareTo(max) > 0 ) {
                throw new IllegalArgumentException
                    ("Invalid min or max value");
            }
        }
    }

    void validateValue(String val) {
        if (val == null) {
            throw new IllegalArgumentException
                ("String values cannot be null");
        }
        if ((min != null &&
             ((isMinInclusive() && min.compareTo(val) > 0) ||
              (!isMinInclusive() && min.compareTo(val) >= 0))) ||
            (max != null &&
             ((isMaxInclusive() && max.compareTo(val) < 0) ||
              (!isMaxInclusive() && max.compareTo(val) <= 0)))) {

            StringBuilder sb = new StringBuilder();
            sb.append("Value, ");
            sb.append(val);
            sb.append(", is outside of the allowed range");
            if (min != null && isMinInclusive()) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            if (min != null) {
                sb.append(min);
            } else {
                sb.append("-INF");
            }
            if (max != null) {
                sb.append(max);
            } else {
                sb.append("+INF");
            }
            if (max != null && isMaxInclusive()) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
