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

import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;

/**
 * FieldMapEntry encapsulates the properties of FieldDef instances that are
 * specific to record types (TableImpl, RecordDefImpl) -- nullable and default
 * values.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class FieldMapEntry implements Cloneable, Serializable,
                                      FastExternalizable {

    private static final long serialVersionUID = 1L;

    /**
     * Warning: fieldName is transient and used in equals() and hashCode(). This
     * means that those methods will produce incorrect results on an instance
     * which has been deserialized using Java serialization. Care must be
     * taken when serializing this class. Query serialization will result in
     * the field being null.
     *
     * This field is serialized by FastExternalizable and must be non-null in
     * those cases.
     */
    private transient final String fieldName;

    private final FieldDefImpl field;

    /*
     * These are not final to allow resetting as primary keys, which are
     * implicitly not nullable, but also don't have default values.
     */
    private boolean nullable;

    /**
     * Field default value or null. Only some field types support defaults.
     * TODO - We believe the value is null if and only if there is no default
     * -- right?
     */
    private FieldValueImpl defaultValue;

    FieldMapEntry(String name,
                  FieldDefImpl type,
                  boolean nullable,
                  FieldValueImpl defaultValue) {
        this.fieldName = name;
        this.field = type;
        this.nullable = nullable;
        if (type.isMRCounter()) {
            this.defaultValue = type.createCRDTValue();
            this.nullable = false;
        } else {
            this.defaultValue = defaultValue;
        }

        /*
         * NOTE: this code used to do this validation, but primary key fields
         * are now forced into this state, so this invariants are enforced in
         * the table creation paths above (DDL, CLI).
        if (!nullable && defaultValue == null) {
            throw new IllegalArgumentException(
                "Not nullable field " + name + " must have a default value");
        }
        */
    }

    /**
     * Creates a default entry -- nullable, no default
     */
    FieldMapEntry(String name, FieldDefImpl type) {
        this(name, type, true, null);
    }

    private FieldMapEntry(FieldMapEntry other) {
        this.fieldName = other.fieldName;
        this.field = other.field.clone();
        this.nullable = other.nullable;
        this.defaultValue = (other.defaultValue != null ?
                             other.defaultValue.clone() : null);
        if (field.isMRCounter() &&
            (!defaultValue.isMRCounter() ||
             defaultValue.getMRCounterMap().size() != 0 ||
             nullable)) {
            throw new IllegalArgumentException(
                "For an MR_Counter column, the value should not be nullable " +
                "and the default should be a zero MRCounter value.");
        }
    }

    /**
     * Constructor for FastExternalizable
     */
    FieldMapEntry(DataInput in, short serialVersion) throws IOException {
        fieldName = readNonNullString(in, serialVersion);
        field = FieldDefImpl.readFastExternal(in, serialVersion);
        /*
         * The change brought back some dead code as part of this change.
         * We brought back read and write side of FastExternalizable old
         * format because of an upgrade issue [KVSTORE-2588]. As part of the
         * revert patch, we kept the read and write both side of the code to
         * keep the change cleaner. This change should be removed when deprecate
         * 25.1 release of kvstore. We can revert this changeset when the
         * prerequisite version is updated to >=25.1.
         */
        if (serialVersion >= SerialVersion.COUNTER_CRDT_DEPRECATED_REMOVE_AFTER_PREREQ_25_1 &&
            field.isMRCounter()) {
            /* The default value for a CRDT column is always 0. */
            nullable = false;
            defaultValue = field.createCRDTValue();
        } else {
            nullable = in.readBoolean();
            defaultValue =
                FieldValueImpl.readFastExternalOrNull(in, serialVersion, field);
        }
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *      {@code fieldName}
     * <li> ({@link FieldDefImpl}) {@code field}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@code nullable}
     * <li> ({@link FieldValueImpl#writeFastExternal FieldValueImpl or null})
     *      {@code defaultValue}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        assert fieldName != null;
        writeNonNullString(out, serialVersion, fieldName);
        field.writeFastExternal(out, serialVersion);
        if (!field.isMRCounter()) {
            out.writeBoolean(nullable);
            FieldValueImpl.writeFastExternalOrNull(out, serialVersion,
                                                   defaultValue);
        }
    }

    @Override
    public FieldMapEntry clone() {
        return new FieldMapEntry(this);
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldDefImpl getFieldDef() {
        return field;
    }

    public boolean isNullable() {
        return nullable;
    }

    boolean isPrecise() {
        return field.isPrecise();
    }

    boolean isSubtype(FieldMapEntry sup) {

        if (!fieldName.equals(sup.fieldName)) {
            return false;
        }

        if (nullable != sup.nullable) {
            if (nullable) {
                return false;
            }
        }

        return field.isSubtype(sup.field);
    }

    FieldValueImpl getDefaultValueInternal() {
        return defaultValue;
    }

    public FieldValueImpl getDefaultValue() {
        return (defaultValue != null ? defaultValue :
                NullValueImpl.getInstance());
    }

    public boolean hasDefaultValue() {
        return defaultValue != null && !defaultValue.isNull();
    }

    /*
     * Primary keys are not nullable, but also do not have default values.
     */
    void setAsPrimaryKey() {
        nullable = false;
        defaultValue = null;
    }

    /*
     * Set the entry as nullable without a default
     */
    void setNullable() {
        nullable = true;
        defaultValue = null;
    }

    /**
     * Compare equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldMapEntry) {
            FieldMapEntry other = (FieldMapEntry) obj;
            return (fieldName.equalsIgnoreCase(other.fieldName) &&
                    field.equals(other.field) &&
                    nullable == other.nullable);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (fieldName.hashCode() +
                field.hashCode() +
                ((Boolean) nullable).hashCode());
    }

    @Override
    public String toString() {
        return "FieldMapEntry[" + fieldName + ", " + field + ", " +
               nullable + "]";
    }
}
