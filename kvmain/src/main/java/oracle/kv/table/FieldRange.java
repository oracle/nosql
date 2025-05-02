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

package oracle.kv.table;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import com.fasterxml.jackson.core.io.CharTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FieldValueSerialization;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * FieldRange defines a range of values to be used in a table or index
 * iteration or multiGet operation.  A FieldRange is used as the least
 * significant component in a partially specified {@link PrimaryKey} or
 * {@link IndexKey} in order to create a value range for an operation that
 * returns multiple rows or keys.  The data types supported by FieldRange
 * are limited to those which are valid for primary keys and/or index keys,
 * as indicated by {@link FieldDef#isValidKeyField} and
 * {@link FieldDef#isValidIndexField}.
 * <p>
 * This object is used to scope a table or index operation and is constructed
 * by {@link Table#createFieldRange} and {@link Index#createFieldRange}.  If
 * used on a table the field referenced must be part of the table's primary key
 * and be in proper order relative to the parent value.  If used on an index
 * the field must be part of the index definition and in proper order relative
 * to the index definition.
 * <p>
 * The fieldPath used in FieldRange construction must be one of the strings
 * returned from {@link PrimaryKey#getFields} or {@link IndexKey#getFields},
 * respectively.
 *
 * @since 3.0
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class FieldRange implements FastExternalizable, Cloneable {

    private final String fieldPath;
    private final FieldDefImpl fieldDef;
    private FieldValue start;
    private boolean startInclusive;
    private FieldValue end;
    private boolean endInclusive;

    /*
     * Storage size is non-zero if the field is a primary key field with
     * a storage size constraint on it. That is possible only for integers
     * at this time. For other types and index keys it will be 0. It's only
     * used on the client side or for direct construction so it need not be
     * serialized.
     */
    private final int storageSize;

    /**
     * @hidden
     * Internal use.  Used by factory methods on Table and Index.
     * The fields referenced by startValue and endValue must be the same field
     * within the field definition for the table on which this object is used.
     *
     * @param fieldPath is the name of the field on which this range is
     * defined.
     *
     * @param fieldDef is the definition of the field on which this range is
     * defined.
     */
    public FieldRange(String fieldPath, FieldDef fieldDef, int storageSize) {
        checkNull("fieldPath", fieldPath);
        checkNull("fieldDef", fieldDef);
        this.fieldPath = fieldPath;
        this.fieldDef = (FieldDefImpl)fieldDef;
        this.storageSize = storageSize;
        assert(fieldDef.isAtomic()||fieldDef.isJson());
    }

    /**
     * FastExternalizable constructor.
     * @hidden
     */
    public FieldRange(DataInput in, short serialVersion) throws IOException {

        fieldPath =  readNonNullString(in, serialVersion);
        startInclusive = in.readBoolean();
        endInclusive = in.readBoolean();
        fieldDef = FieldDefSerialization.readFieldDef(in, serialVersion);

        start = FieldValueSerialization.readFieldValue(fieldDef,
                                                       in, serialVersion);

        end = FieldValueSerialization.readFieldValue(fieldDef,
                                                     in, serialVersion);
        storageSize = 0;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     * {@link #getFieldName fieldPath}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getStartInclusive
     *      startInclusive}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getEndInclusive
     *      endInclusive}
     * <li> ({@link FieldDefSerialization#writeFieldDef field def}) {@link
     *      #getField fieldDef}
     * <li> {@link FieldValueSerialization#writeFieldValue
     *      writeFieldValue(} {@link #getStart start} {@code , false)}
     * <li> {@link FieldValueSerialization#writeFieldValue
     *      writeFieldValue(} {@link #getEnd end} {@code , false)}
     * </ol>

     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        writeNonNullString(out, serialVersion, fieldPath);
        out.writeBoolean(startInclusive);
        out.writeBoolean(endInclusive);
        FieldDefSerialization.writeFieldDef(fieldDef, out, serialVersion);

        FieldValueSerialization.writeFieldValue(start,
                                                false, // writeValDef
                                                out, serialVersion);

        FieldValueSerialization.writeFieldValue(end,
                                                false, // writeValDef
                                                out, serialVersion);
    }

    /**
     * Creates a new FieldRange instance that is a shallow copy of this
     * instance.
     */
    @Override
    public FieldRange clone() {
        try {
            return (FieldRange) super.clone();
        } catch (CloneNotSupportedException ignore) {
        }
        return null;
    }

    /**
     * A convenience factory method to create a MultiRowOptions
     * instance using this FieldRange.
     *
     * @return a new MultiRowOptions
     */
    public MultiRowOptions createMultiRowOptions() {
        return new MultiRowOptions(this);
    }

    /**
     * Returns the FieldDef that was used to construct this object.  If this
     * FieldRange is being used in an index and the indexed field is an
     * array the definition of the indexed element is returned and not the
     * array definition.
     *
     * @return the FieldDef
     */
    public FieldDef getField() {
        return fieldDef;
    }

    /**
     * Returns the FieldValue that defines lower bound of the range,
     * or null if no lower bound is enforced.
     *
     * @return the start FieldValue
     */
    public FieldValue getStart() {
        return start;
    }

    /**
     * Returns whether start is included in the range, i.e., start is less than
     * or equal to the first FieldValue in the range.  This value is valid only
     * if the start value is not null.
     *
     * @return true if the start value is inclusive
     */
    public boolean getStartInclusive() {
        return startInclusive;
    }

    /**
     * Returns the FieldValue that defines upper bound of the range,
     * or null if no upper bound is enforced.
     *
     * @return the end FieldValue
     */
    public FieldValue getEnd() {
        return end;
    }

    /**
     * Returns whether end is included in the range, i.e., end is greater than
     * or equal to the last FieldValue in the range.  This value is valid only
     * if the end value is not null.
     *
     * @return true if the end value is inclusive
     */
    public boolean getEndInclusive() {
        return endInclusive;
    }

    /**
     * Returns the FieldDef for the field used in the range.
     *
     * @return the FieldDef
     */
    public FieldDef getDefinition() {
        return fieldDef;
    }

    /**
     * Returns the name for the field used in the range.
     *
     * @return the name of the field
     */
    public String getFieldName() {
        return fieldPath;
    }

    /**
     * Sets the start value of the range to the specified integer value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(int value, boolean isInclusive) {
        final FieldValue val = fieldDef.createInteger(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified double value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(double value, boolean isInclusive) {
        final FieldValue val = fieldDef.createDouble(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified float value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(float value, boolean isInclusive) {
        final FieldValue val = fieldDef.createFloat(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified long value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(long value, boolean isInclusive) {
        final FieldValue val = fieldDef.createLong(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified string value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(String value, boolean isInclusive) {
        final FieldValue val = fieldDef.createString(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified enumeration value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStartEnum(String value, boolean isInclusive) {
        final FieldValue val = fieldDef.createEnum(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified timestamp value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     *
     * @since 4.3
     */
    public FieldRange setStart(Timestamp value, boolean isInclusive) {
        final FieldValue val = fieldDef.createTimestamp(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified BigDecimal value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     *
     * @since 4.4
     */
    public FieldRange setStart(BigDecimal value, boolean isInclusive) {
        final FieldValue val = fieldDef.createNumber(value);
        return setStartValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified integer value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(int value, boolean isInclusive) {
        final FieldValue val = fieldDef.createInteger(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified double value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(double value, boolean isInclusive) {
        final FieldValue val = fieldDef.createDouble(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified float value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(float value, boolean isInclusive) {
        final FieldValue val = fieldDef.createFloat(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified long value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(long value, boolean isInclusive) {
        final FieldValue val = fieldDef.createLong(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified string value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(String value, boolean isInclusive) {
        final FieldValue val = fieldDef.createString(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified enumeration value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEndEnum(String value, boolean isInclusive) {
        final FieldValue val = fieldDef.createEnum(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified timestamp value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     *
     * @since 4.3
     */
    public FieldRange setEnd(Timestamp value, boolean isInclusive) {
        final FieldValue val = fieldDef.createTimestamp(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the end value of the range to the specified BigDecimal value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     *
     * @since 4.4
     */
    public FieldRange setEnd(BigDecimal value, boolean isInclusive) {
        final FieldValue val = fieldDef.createNumber(value);
        return setEndValue(val, isInclusive);
    }

    /**
     * Sets the start value of the range to the specified value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setStart(FieldValue value, boolean isInclusive) {
        return setStart(value, isInclusive, true);
    }

    /**
     * Sets the end value of the range to the specified value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the value is not valid for the
     * field in the range.
     */
    public FieldRange setEnd(FieldValue value, boolean isInclusive) {
        return setEnd(value, isInclusive, true);
    }

    /*
     * Internal use
     *
     * @hidden
     */
    public FieldRange setStart(
        FieldValue value,
        boolean isInclusive,
        boolean validate) {

        if (value != null &&
            !value.isNull() &&
            !((FieldValueImpl)value).isEMPTY() &&
            (fieldDef.isPrecise() && !fieldDef.isType(value.getType()) ||
             !((FieldValueImpl)value).getDefinition().isSubtype(fieldDef))) {
            throw new IllegalArgumentException
                ("Value is not of correct type: " + value.getType() +
                 "\nExpected type is " + fieldDef.getDDLString());
        }
        return setStartValue(value, isInclusive, validate);
    }

    /*
     * Internal use
     *
     * @hidden
     */
    public FieldRange setEnd(
        FieldValue value,
        boolean isInclusive,
        boolean validate) {

        if (value != null &&
            !value.isNull() &&
            !((FieldValueImpl)value).isEMPTY() &&
            (fieldDef.isPrecise() && !fieldDef.isType(value.getType()) ||
             !((FieldValueImpl)value).getDefinition().isSubtype(fieldDef))) {
            throw new IllegalArgumentException
                ("Value is not of correct type: " + value.getType() +
                 "\nExpected type is " + fieldDef.getDDLString());
        }
        return setEndValue(value, isInclusive, validate);
    }

    /*
     * Internal use
     *
     * @hidden
     */
    public FieldRange setStartValue(FieldValue value, boolean isInclusive) {
        return setStartValue(value, isInclusive, true);
    }

    /*
     * Internal use
     *
     * @hidden
     */
    private FieldRange setStartValue(
        FieldValue value,
        boolean isInclusive,
        boolean validate) {

        start = value;
        startInclusive = isInclusive;

        if (validate) {
            validate();
        }

        return this;
    }

    /*
     * Internal use
     *
     * @hidden
     */
    private FieldRange setEndValue(FieldValue value, boolean isInclusive) {
        return setEndValue(value, isInclusive, true);
    }

    /*
     * Internal use
     *
     * @hidden
     */
    private FieldRange setEndValue(
        FieldValue value,
        boolean isInclusive,
        boolean validate) {

        end = value;
        endInclusive = isInclusive;

        if (validate) {
            validate();
        }

        return this;
    }

    /*
     * Internal use
     *
     * @hidden
     */
    public int getStorageSize() {
        return storageSize;
    }

    /*
     * Internal use
     *
     * @hidden
     */
    public boolean check() {

        if (start != null && end != null) {

            if (!fieldDef.isPrecise()) {
                FieldDefImpl t1 = (FieldDefImpl)start.getDefinition();
                FieldDefImpl t2 = (FieldDefImpl)end.getDefinition();
                if (!TypeManager.areTypesComparable(t1, t2)) {
                    return false;
                }
            }

            int cmp = FieldValueImpl.compareKeyValues((FieldValueImpl)start,
                                                      (FieldValueImpl)end);

            if (cmp > 0) {
                return false;
            }

            if (cmp == 0) {
                if (!endInclusive || !startInclusive) {
                    return false;
                }
            }
        }
        return true;
    }

    private void validate() {
        if (!check()) {
            throw new IllegalArgumentException(
                "FieldRange: start value must be less than the end value");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(" \"Name\" : ").append(fieldPath);
        sb.append(", \"Type\" : ").append(fieldDef.getType());
        sb.append(", \"Start\" : ").append(start != null ? start : "null");
        sb.append(", \"End\" : ").append(end != null ? end : "null");
        sb.append(", \"StartInclusive\" : ").append(startInclusive);
        sb.append(", \"EndInclusive\" : ").append(endInclusive);
        sb.append(" }");
        return sb.toString();
    }

    /**
     * @hidden
     * Used when displaying the query execution plan.
     */
    public void display(StringBuilder sb) {

        sb.append("{ \"");
        CharTypes.appendQuoted(sb, fieldPath);
        sb.append("\" : { ");

        if (start != null) {
            sb.append("\"start value\" : ");
            sb.append(start.toJsonString(false));
            sb.append(", \"start inclusive\" : ").append(startInclusive);
        }

        if (end != null) {
            if (start != null) {
                sb.append(", ");
            }
            sb.append("\"end value\" : ");
            sb.append(end.toJsonString(false));
            sb.append(", \"end inclusive\" : ").append(endInclusive);
        }

        sb.append(" } }");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FieldRange)) {
            return false;
        }
        final FieldRange other = (FieldRange) obj;
        return fieldPath.equals(other.fieldPath) &&
            fieldDef.equals(other.fieldDef) &&
            Objects.equals(start, other.start) &&
            (startInclusive == other.startInclusive) &&
            Objects.equals(end, other.end) &&
            (endInclusive == other.endInclusive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldPath,
                            fieldDef,
                            start,
                            startInclusive,
                            end,
                            endInclusive);
    }
}
