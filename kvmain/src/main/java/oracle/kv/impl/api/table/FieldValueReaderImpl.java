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

import static oracle.kv.impl.api.table.FieldDefImpl.Constants.binaryDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.booleanDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.doubleDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.floatDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.integerDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.longDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.numberDef;
import static oracle.kv.impl.api.table.FieldDefImpl.Constants.stringDef;

import java.util.List;
import java.util.Stack;

import oracle.kv.Version;
import oracle.kv.table.FieldDef;
import oracle.kv.table.Table;

/*
 * A class used to deserialize FieldValueImpl instance from bytes.
 */
class FieldValueReaderImpl<T extends FieldValueImpl>
    implements ValueReader<FieldValueImpl> {

    private Stack<ComplexValueImpl> complexValues;
    FieldValueImpl value;

    FieldValueReaderImpl() {
        this(null);
    }

    FieldValueReaderImpl(FieldValueImpl value) {
        this.value = value;
        complexValues = null;
    }

    @Override
    public void readInteger(String name, int val) {
        setFieldValue(name, integerDef.createInteger(val));
    }

    @Override
    public void readLong(String name, long val) {
        setFieldValue(name, longDef.createLong(val));
    }

    @Override
    public void readFloat(String name, float val) {
        setFieldValue(name, floatDef.createFloat(val));
    }

    @Override
    public void readDouble(String name, double val) {
        setFieldValue(name, doubleDef.createDouble(val));
    }

    @Override
    public void readNumber(String name, byte[] bytes) {
        setFieldValue(name, numberDef.createNumber(bytes));
    }

    @Override
    public void readTimestamp(String name, FieldDef def, byte[] bytes) {
        setFieldValue(name,
            ((TimestampDefImpl)def.asTimestamp()).createTimestamp(bytes));
    }

    @Override
    public void readBinary(String name, byte[] bytes) {
        setFieldValue(name,
            (FieldValueImpl)binaryDef.asBinary().createBinary(bytes));
    }

    @Override
    public void readFixedBinary(String name, FieldDef def, byte[] bytes) {
        setFieldValue(name,
            (FieldValueImpl)def.asFixedBinary().createFixedBinary(bytes));
    }

    @Override
    public void readString(String name, String val) {
        setFieldValue(name, stringDef.createString(val));
    }

    @Override
    public void readBoolean(String name, boolean val) {
        setFieldValue(name, booleanDef.createBoolean(val));
    }

    @Override
    public void readNull(String name) {
        setFieldValue(name, NullValueImpl.getInstance());
    }

    @Override
    public void readJsonNull(String name) {
        setFieldValue(name, NullJsonValueImpl.getInstance());
    }

    @Override
    public void readEmpty(String name) {
        setFieldValue(name, EmptyValueImpl.getInstance());
    }

    @Override
    public void readEnum(String name, FieldDef def, int index) {
        EnumDefImpl enumDef = (EnumDefImpl)def.asEnum();
        setFieldValue(name, enumDef.createEnum(index));
    }


    @Override
    public void readCounterCRDT(String fieldName,
                                FieldValueImpl val) {
        setFieldValue(fieldName, val);

    }

    @Override
    public void readDefaultJsonMRCounter(String fieldName,
                                         FieldDefImpl fDef,
                                         List<TablePath> paths) {
        if (value.isRecord() &&
            value.asRecord().get(fieldName).isNull()) {

            setFieldValue(fieldName, fDef.createMap());
            RecordValueImpl.insertDefaultJsonMRCounter(paths, value);
        }
    }

    @Override
    public void startRecord(String name, FieldDef def, int size) {
        RecordValueImpl rval = (RecordValueImpl)def.asRecord().createRecord();
        setFieldValue(name, rval);
        pushComplexValue(rval);
    }

    @Override
    public void endRecord(int size) {
        popComplexValue();
    }

    @Override
    public void startMap(String name, FieldDef def, int size) {
        MapValueImpl mval = (MapValueImpl)def.asMap().createMap();
        setFieldValue(name, mval);
        pushComplexValue(mval);
    }

    @Override
    public void endMap(int size) {
        popComplexValue();
    }

    @Override
    public void startArray(String name,
                           FieldDef def,
                           FieldDef homogeneousElemDef,
                           int size) {
        ArrayValueImpl aval = (ArrayValueImpl)def.asArray().createArray();
        /*
         * homogeneousElemDef is only non-null if this is an homogenous
         * JSON array, in which case it represents the type of the array
         * elements. see callers.
         */
        if (homogeneousElemDef != null) {
            aval.setHomogeneousType((FieldDefImpl)homogeneousElemDef);
        }
        setFieldValue(name, aval);
        pushComplexValue(aval);
    }

    @Override
    public void endArray(int size) {
        popComplexValue();
    }

    private void setFieldValue(String name, FieldValueImpl fieldValue) {
        if (value == null) {
            value = fieldValue;
            return;
        }

        ComplexValueImpl curVal;
        if (complexValues == null || complexValues.isEmpty()) {
            if (!value.isComplex()) {
                throw new IllegalStateException("value should be complex " +
                    "type, but " + value.getType());
            }
            curVal = (ComplexValueImpl)value;
        } else {
            curVal = complexValues.peek();
        }

        if (curVal.isRecord()) {
            ((RecordValueImpl)curVal).putInternal(name, fieldValue, false);
        } else if (curVal.isMap()) {
            curVal.asMap().put(name, fieldValue);
        } else {
            assert(curVal.isArray());
            curVal.asArray().add(fieldValue);
        }
    }

    private void pushComplexValue(ComplexValueImpl fieldValue) {
        if (complexValues == null) {
            complexValues = new Stack<ComplexValueImpl>();
        }
        complexValues.push(fieldValue);
    }

    private void popComplexValue() {
        if (complexValues != null) {
            complexValues.pop();
        }
    }

    @Override
    public FieldValueImpl getValue() {
        return value;
    }

    @Override
    public Table getTable() {
        return null;
    }

    @Override
    public void setTableVersion(int tableVersion) {
    }

    @Override
    public void setExpirationTime(long expirationTime) {
    }

    @Override
    public void setModificationTime(long modificationTime) {
    }

    @Override
    public void setVersion(Version version) {
    }

    @Override
    public void setRegionId(int regionId) {

    }

    @Override
    public void reset() {
        complexValues = null;
        value = null;
    }

    @Override
    public void setValue(FieldValueImpl value) {
        this.value = value;
    }

}
