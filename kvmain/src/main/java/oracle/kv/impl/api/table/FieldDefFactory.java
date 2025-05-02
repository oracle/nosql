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

import java.util.Map;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;

/*
 * A factory to create instances of FieldDefImpl subclasses. Such instances
 * need to be created by the query packages, and instead of making the
 * constructors of the FieldDefImpl subclasses public, we use this factory.
 */
public class FieldDefFactory {

    public static IntegerDefImpl createIntegerDef() {
        return FieldDefImpl.Constants.integerDef;
    }

    public static LongDefImpl createLongDef() {
        return FieldDefImpl.Constants.longDef;
    }

    public static FloatDefImpl createFloatDef() {
        return FieldDefImpl.Constants.floatDef;
    }

    public static JsonDefImpl createJsonDef() {
        return FieldDefImpl.Constants.jsonDef;
    }

    public static JsonDefImpl createJsonDef(Map<String, Type> mrcounterFields) {
        return new JsonDefImpl(mrcounterFields, null);
    }

    public static DoubleDefImpl createDoubleDef() {
        return FieldDefImpl.Constants.doubleDef;
    }

    public static NumberDefImpl createNumberDef() {
        return FieldDefImpl.Constants.numberDef;
    }

    public static StringDefImpl createStringDef() {
        return FieldDefImpl.Constants.stringDef;
    }

    public static StringDefImpl createUUIDStringDef() {
        return FieldDefImpl.Constants.uuidStringDef;
    }

    public static StringDefImpl createDefaultUUIDStrDef() {
        return FieldDefImpl.Constants.defaultUuidStrDef;
    }

    public static EnumDefImpl createEnumDef(String[] values) {
        return new EnumDefImpl(values, null/*descr*/);
    }

    public static BooleanDefImpl createBooleanDef() {
        return FieldDefImpl.Constants.booleanDef;
    }

    public static BinaryDefImpl createBinaryDef() {
        return FieldDefImpl.Constants.binaryDef;
    }

    public static FixedBinaryDefImpl createFixedBinaryDef(int size) {
        return new FixedBinaryDefImpl(size, null/*descr*/);
    }

    public static TimestampDefImpl createTimestampDef(int precision) {
        return FieldDefImpl.Constants.timestampDefs[precision];
    }

    public static RecordDefImpl createRecordDef(
        FieldMap fieldMap,
        String descr) {
        return new RecordDefImpl(fieldMap, descr);
    }

    public static ArrayDefImpl createArrayDef(FieldDefImpl elemType) {
        return createArrayDef(elemType, null);
    }

    public static ArrayDefImpl createArrayDef(
        FieldDefImpl elemType,
        String descr) {

        if (descr == null) {
            if (elemType.isJson()) {
                return FieldDefImpl.Constants.arrayJsonDef;
            }

            if (elemType.isAny()) {
                return FieldDefImpl.Constants.arrayAnyDef;
            }
        }

        return new ArrayDefImpl(elemType, descr);
    }

    public static MapDefImpl createMapDef(FieldDefImpl elemType) {
        return createMapDef(elemType, null);
    }

    public static MapDefImpl createMapDef(
        FieldDefImpl elemType,
        String descr) {

        if (descr == null) {
            if (elemType.isJson()) {
                return FieldDefImpl.Constants.mapJsonDef;
            }

            if (elemType.isAny()) {
                return FieldDefImpl.Constants.mapAnyDef;
            }
        }

        return new MapDefImpl(elemType, descr);
    }

    public static AnyDefImpl createAnyDef() {
        return FieldDefImpl.Constants.anyDef;
    }

    public static AnyAtomicDefImpl createAnyAtomicDef() {
        return FieldDefImpl.Constants.anyAtomicDef;
    }

    public static AnyJsonAtomicDefImpl createAnyJsonAtomicDef() {
        return FieldDefImpl.Constants.anyJsonAtomicDef;
    }

    public static AnyRecordDefImpl createAnyRecordDef() {
        return FieldDefImpl.Constants.anyRecordDef;
    }

    public static FieldDefImpl createAtomicTypeDef(FieldDef.Type type) {
        switch (type) {
        case STRING:
            return createStringDef();
        case INTEGER:
            return createIntegerDef();
        case LONG:
            return createLongDef();
        case DOUBLE:
            return createDoubleDef();
        case FLOAT:
            return createFloatDef();
        case NUMBER:
            return createNumberDef();
        case BINARY:
            return createBinaryDef();
        case BOOLEAN:
            return createBooleanDef();
        default:
            throw new IllegalArgumentException(
                "Cannot create an atomic field def of type " + type);
        }
    }
}
