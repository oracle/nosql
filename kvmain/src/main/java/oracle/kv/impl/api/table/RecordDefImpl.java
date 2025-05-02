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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.table.RecordDef;

/**
 * RecordDefImpl implements the RecordDef interface.
 */
public class RecordDefImpl extends FieldDefImpl implements RecordDef {

    private static final long serialVersionUID = 1L;

    private final FieldMap fieldMap;

    /* requires names for records. */
    private String name;

    RecordDefImpl(FieldMap fieldMap, String description) {

        super(Type.RECORD, description);

        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalArgumentException
                ("Record has no fields and cannot be built");
        }

        this.name  = null;
        this.fieldMap = fieldMap;
    }

    RecordDefImpl(
        final String name,
        final FieldMap fieldMap,
        final String description) {

        this(fieldMap, description);

        if (name == null) {
            throw new IllegalArgumentException("Record requires a name");
        }

        this.name  = name;
    }

    RecordDefImpl(final String name, final FieldMap fieldMap) {
        this(name, fieldMap, null);
    }

    private RecordDefImpl(RecordDefImpl impl) {
        super(impl);
        this.name = impl.name;
        fieldMap = impl.fieldMap.clone();
    }

    /**
     * Constructor for FastExternalizable
     */
    RecordDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.RECORD);
        name = readString(in, serialVersion);
        fieldMap = new FieldMap(in, serialVersion);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) {@code name}
     * <li> ({@link FieldMap}) {@code fieldMap}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        fieldMap.writeFastExternal(out, serialVersion);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public RecordDefImpl clone() {
        return new RecordDefImpl(this);
    }

    @Override
    public int hashCode() {
        return fieldMap.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other instanceof RecordDefImpl) {
            RecordDefImpl otherDef = (RecordDefImpl) other;

            /* maybe avoid some work */
            if (this == otherDef) {
                return true;
            }

            /*
             * Perform field-by-field comparison if names match.
             */
            return fieldMap.equals(otherDef.fieldMap);
        }
        return false;
    }

    @Override
    public RecordDef asRecord() {
        return this;
    }

    @Override
    public RecordValueImpl createRecord() {
        return new RecordValueImpl(this);
    }

    /*
     * Public api methods from RecordDef
     */

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public int getNumFields() {
        return fieldMap.size();
    }

    @Override
    public boolean contains(String name1) {
        return fieldMap.exists(name1);
    }

    @Deprecated
    @Override
    public FieldDefImpl getField(String name1) {
        return getFieldDef(name1);
    }

    @Override
    public FieldDefImpl getFieldDef(String name1) {
        return fieldMap.getFieldDef(name1);
    }

    @Deprecated
    @Override
    public FieldDefImpl getField(int pos) {
        return getFieldDef(pos);
    }

    @Override
    public FieldDefImpl getFieldDef(int pos) {
        return fieldMap.getFieldDef(pos);
    }

    @Deprecated
    @Override
    public List<String> getFields() {
        return getFieldNames();
    }

    @Override
    public List<String> getFieldNames() {
        return Collections.unmodifiableList(fieldMap.getFieldNames());
    }

    @Override
    public String getFieldName(int pos) {
        return fieldMap.getFieldName(pos);
    }

    @Override
    public int getFieldPos(String fname) {
        return fieldMap.getFieldPos(fname);
    }

    @Override
    public boolean isNullable(String fieldName) {
        FieldMapEntry fme = getFieldMapEntry(fieldName, true);
        return fme.isNullable();
    }

    @Override
    public boolean isNullable(int pos) {
        return fieldMap.isNullable(pos);
    }

    @Override
    public FieldValueImpl getDefaultValue(String fieldName) {
        FieldMapEntry fme = getFieldMapEntry(fieldName, true);
        return fme.getDefaultValue();
    }

    @Override
    public FieldValueImpl getDefaultValue(int pos) {
        return fieldMap.getDefaultValue(pos);
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return fieldMap.isPrecise();
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (this == superType) {
            return true;
        }

        if (superType.isRecord()) {
            return fieldMap.isSubtype(((RecordDefImpl)superType).getFieldMap());
        } else if (superType.isAny() || superType.isAnyRecord()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    FieldDefImpl findField(String fieldName) {
        return fieldMap.getFieldDef(fieldName);
    }

    @Override
    public short getRequiredSerialVersion() {
        return fieldMap.getRequiredSerialVersion();
    }

    /*
     * local methods
     */

    public List<String> getFieldNamesInternal() {
        return fieldMap.getFieldNames();
    }

    public void setName(String n) {
        name = n;

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                "Record types require a name");
        }
    }

    List<FieldMapEntry> getFieldProperties() {
        return getFieldMap().getFieldProperties();
    }

    public FieldMap getFieldMap() {
        return fieldMap;
    }

    public FieldMapEntry getFieldMapEntry(String fieldName, boolean mustExist) {

        FieldMapEntry fme = fieldMap.getFieldMapEntry(fieldName);
        if (fme != null) {
            return fme;
        }
        if (mustExist) {
            throw new IllegalArgumentException(
                "Record definition does not have a field named " + fieldName);
        }
        return null;
    }

    @Override
    int countTypes() {
        int num = 1; /* this field */
        for (int i = 0; i < getNumFields(); ++i) {
            FieldMapEntry fme = fieldMap.getFieldMapEntry(i);
            num += fme.getFieldDef().countTypes();
        }
        return num;
    }
}
