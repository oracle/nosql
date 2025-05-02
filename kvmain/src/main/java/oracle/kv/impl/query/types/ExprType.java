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

package oracle.kv.impl.query.types;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.table.FieldDef.Type;

/*
 * The ExprType class represents the type of a query expression. It wraps
 * FieldDefImpl, which is the implementation of public interface FieldDef.
 * Furthermore, it adds:
 *
 * - A cardinality indicator ("quantifier") to distinguish between scalar
 *   and non-scalar exprs.
 * - A nullability property to say whether the associated expr can return
 *   a null value.
 * - Representation for some "wildcard" types, which are not part of the
 *   public API (and don't have an associated FieldDef).
 */
public class ExprType implements Cloneable {

    /*
     * WARNING!!
     * Do NOT reorder the enum values in Quantifier. Their ordinal numbers are
     * used as indices in various static matrices defined in TypeManager.
     */
    public static enum Quantifier {
        ONE,   // exactly one value
        QSTN,  // question (?) means zero or one values
        PLUS,  // plus (+) means one or more values
        STAR   // star (*) means zero or more values
        ;

        private static final Quantifier[] VALUES = values();
        public static final int VALUES_COUNT = VALUES.length;
        public static Quantifier valueOf(int ordinal) {
            return VALUES[ordinal];
        }

        @Override
        public String toString() {
            switch (this) {
            case ONE:
                return "";
            case QSTN:
                return "?";
            case STAR:
                return "*";
            case PLUS:
                return "+";
            default:
                throw new QueryStateException("Unknown quantifier: " + name());
            }
        }
    }

    /*
     * WARNING!!
     * Do NOT reorder the enum values in TypeCode. Their ordinal numbers are
     * used as indices in various static matrices defined in TypeManager.
     */
    public static enum TypeCode {

        /* Builtin types */
        EMPTY,

        ANY,
        ANY_ATOMIC,
        ANY_JSON_ATOMIC,
        JSON,
        ANY_RECORD,

        NUMBER,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BOOLEAN,
        BINARY,

        /* Non-builtin types */
        FIXED_BINARY,
        ENUM,
        TIMESTAMP,

        RECORD,
        ARRAY,
        MAP
    }

    /*
     * Builtin types are the ones that can be created statically (because
     * their definition does not depend on any parameters/properties or
     * other types).
     */
    static boolean isBuiltin(TypeCode t) {
        switch (t) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case NUMBER:
        case STRING:
        case BOOLEAN:
        case BINARY:
        case ANY:
        case JSON:
        case ANY_JSON_ATOMIC:
        case ANY_ATOMIC:
        case ANY_RECORD:
        case EMPTY:
            return true;
        default:
            return false;
        }
    }

    private final TypeCode theTypeCode;

    private Quantifier theQuantifier;

    private final FieldDefImpl theTypeDef;

    private final boolean theIsBuiltin;

    public ExprType(FieldDefImpl type, boolean isBuiltin) {
        this(type, Quantifier.ONE, isBuiltin);
    }

    ExprType(FieldDefImpl t, Quantifier q, boolean isBuiltin) {
        theTypeCode = getTypeCodeForDefCode(t.getType());
        theTypeDef = t;
        theQuantifier = q;
        theIsBuiltin = isBuiltin;
    }

    ExprType(ExprType other) {
        assert(!other.theIsBuiltin);
        theTypeCode = other.theTypeCode;
        theTypeDef = other.theTypeDef;
        theQuantifier = other.theQuantifier;
        theIsBuiltin = false;
    }

    @Override
    public ExprType clone() {

        if (theIsBuiltin) {
            return this;
        }

        return new ExprType(this);
    }

    public TypeCode getCode() {
        return theTypeCode;
    }

    public Quantifier getQuantifier() {
        return theQuantifier;
    }

    void setQuant(Quantifier q) {
        theQuantifier = q;
    }

    boolean isMultiValued() {
        return (theQuantifier == Quantifier.STAR ||
                theQuantifier == Quantifier.PLUS);
    }

    public FieldDefImpl getDef() {
        return theTypeDef;
    }

    public boolean isBuiltin() {
        return theIsBuiltin;
    }

    public boolean isEmpty() {
        return theTypeCode == TypeCode.EMPTY;
    }

    public boolean isAny() {
        return theTypeCode == TypeCode.ANY;
    }

    public boolean isAnyJson() {
        return theTypeCode == TypeCode.JSON;
    }

    public boolean isAnyAtomic() {
        return theTypeCode == TypeCode.ANY_ATOMIC;
    }

    public boolean isAnyJsonAtomic() {
        return theTypeCode == TypeCode.ANY_JSON_ATOMIC;
    }

    public boolean isAnyRecord() {
        return theTypeCode == TypeCode.ANY_RECORD;
    }

    public boolean isComplex() {

        switch (theTypeCode) {
        case ANY:
        case JSON:
        case ANY_RECORD:
        case RECORD:
        case ARRAY:
        case MAP:
            return true;
        default:
             return false;
        }
    }

    public boolean isRecord() {
        return (theTypeCode == TypeCode.ANY_RECORD ||
                theTypeCode == TypeCode.RECORD);
    }

    public boolean isArray() {
        return (theTypeCode == TypeCode.ARRAY);
    }

    public boolean isMap() {
        return (theTypeCode == TypeCode.MAP);
    }

    public boolean isPrecise() {
        return theTypeDef.isPrecise();
    }

    public boolean isWildcard() {
        return theTypeDef.isWildcard();
    }

    public boolean isAtomic() {
        return theTypeDef.isAtomic();
    }

    public boolean isNumeric() {
        return theTypeDef.isNumeric();
    }

    /*
     * It is used to determine the return type of array slice/filter steps.
     * "this" is the type of the input expr to the [] step.
     */
    public ExprType getArrayElementType(Quantifier quant) {

        FieldDefImpl elemDef;

        switch (theTypeCode) {

        case ARRAY:
            assert(theTypeDef != null);
            ArrayDefImpl arrDef = (ArrayDefImpl)theTypeDef;
            elemDef = (arrDef.getElement());
            return TypeManager.createType(elemDef, quant);

        case ANY:
            return TypeManager.createType(TypeManager.ANY_ONE(), quant);

        case JSON:
            return TypeManager.createType(TypeManager.JSON_ONE(), quant);

        case EMPTY:
            return TypeManager.EMPTY();

        default:
            return TypeManager.createType(this, quant);
        }
    }

    /*
     * It is used to determine the return type of .values() path steps.
     * "this" is the type of the input expr to the .values() step.
     */
    public ExprType getMapElementType(Quantifier quant) {

        FieldDefImpl elemDef;

        switch (theTypeCode) {

        case MAP:
            MapDefImpl mapDef = (MapDefImpl)theTypeDef;
            elemDef = mapDef.getElement();
            return TypeManager.createType(elemDef, quant);

        case RECORD:
        case ANY:
            return TypeManager.createType(TypeManager.ANY_ONE(), quant);

        case JSON:
            return TypeManager.createType(TypeManager.JSON_ONE(), quant);

        case EMPTY:
            return TypeManager.EMPTY();

        default:
            return TypeManager.EMPTY();
        }
    }

    /*
     * For a record type returns the type of the given field, quantified by
     * the given quant. If the type is ANY_RECORD, [ANY, quant] is returned.
     * Otherwise, if the record type does not contain any field with the
     * given name, IllegalArgumentException is raised (by the call to the
     * RecordDef.getFieldDef() method).
     *
     * Should not be used for non-record types.
     */
    public ExprType getFieldType(int pos, Quantifier quant) {

       switch (theTypeCode) {

       case ANY_RECORD:
           return TypeManager.createType(TypeManager.ANY_ONE(), quant);

       case RECORD:
           RecordDefImpl recDef = (RecordDefImpl)getDef();
           FieldDefImpl fieldDef = recDef.getFieldDef(pos);
           return TypeManager.createType(fieldDef, quant);

       default:
           assert(false);
           return null;
       }
    }

    /*
     * Create a type with the same ItemType as this, and a quant of ONE.
     */
    public ExprType getItemType() {
        return TypeManager.createType(this, Quantifier.ONE);
    }

    /*
     *
     */
    @Override
    public boolean equals(Object o) {
        return equals(o, true);
    }

    public boolean equals(Object o, boolean compareQuants) {

        if (this == o) {
            return true;
        }

        if (!(o instanceof ExprType)) {
            return false;
        }

        ExprType other = (ExprType)o;

        if (compareQuants && theQuantifier != other.theQuantifier) {
            return false;
        }

        if (theTypeCode != other.theTypeCode) {
            return false;
        }

        return theTypeDef.equals(other.theTypeDef);
    }

    @Override
    public int hashCode() {
        return theQuantifier.hashCode() + theTypeCode.hashCode() +
            (theTypeDef != null ? theTypeDef.hashCode() : 0);
    }

    /*
     * Return true iff this is a subtype of the given type (superType).
     */
    public boolean isSubType(ExprType superType) {
        return isSubType(superType, true);
    }

    public boolean isSubType(ExprType superType, boolean compareQuants) {

        if (this == superType) {
            return true;
        }

        if (compareQuants &&
            !TypeManager.isSubQuant(theQuantifier, superType.theQuantifier)) {
            return false;
        }

        if (theTypeCode == TypeCode.EMPTY) {
            return true;
        }

        if (!TypeManager.isSubTypeCode(theTypeCode, superType.theTypeCode)) {
            return false;
        }

        switch (superType.theTypeCode) {
        case ANY:
        case ANY_JSON_ATOMIC:
        case ANY_ATOMIC:
        case ANY_RECORD:
        case BOOLEAN:
        case BINARY:

        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case NUMBER:
        case STRING:
            return true;

        /*
         * The typecodes indicate that sub may be a subtype of sup, but the
         * types may have additional properties that must be checked. For
         * JSON, theTypeCode may be ARRAY or MAP (which are subtype code of
         * JSON), but we must check that the element type of the array/map
         * is also subtype of JSON.
         */
        case JSON:
        case TIMESTAMP:
        case ENUM:
        case FIXED_BINARY:
        case RECORD:
        case ARRAY:
        case MAP:
            return theTypeDef.isSubtype(superType.theTypeDef);

        default:
            throw new QueryStateException("Unknown type code: " + theTypeCode);
        }
    }

    /*
     * Return true iff the given value is an instance of of this type
     */
    public boolean containsValue(FieldValueImpl value) {

        assert(!value.isEMPTY());

        TypeCode valueCode = getTypeCodeForDefCode(value.getType());

        if (value.isJsonNull() &&
            (theTypeCode == TypeCode.ANY_ATOMIC ||
             theTypeCode == TypeCode.ANY_JSON_ATOMIC)) {
            return true;
        }

        if (!TypeManager.isSubTypeCode(valueCode, theTypeCode)) {
            return false;
        }

        switch (theTypeCode) {

        case ANY:
        case ANY_ATOMIC:
        case ANY_JSON_ATOMIC:
        case ANY_RECORD:
        case BOOLEAN:
        case BINARY:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case NUMBER:
        case STRING:
            return true;

        case JSON:
        case TIMESTAMP:
        case ENUM:
        case FIXED_BINARY:
        case RECORD:
        case ARRAY:
        case MAP:
            FieldDefImpl valDef = value.getDefinition();
            assert(valDef != null);
            return valDef.isSubtype(theTypeDef);

        default:
            throw new QueryStateException("Unknown type code: " + theTypeCode);
        }
    }

    @Override
    public String toString() {

        StringBuffer sb = new StringBuffer();
        sb.append(getDef().getDDLString());

        switch (theQuantifier) {
        case ONE:
            break;
        case QSTN:
            sb.append("?");
            break;
        case PLUS:
            sb.append("+");
            break;
        case STAR:
            sb.append("*");
            break;
        }

        return sb.toString();
    }

    public void display(StringBuilder sb, DisplayFormatter formatter) {

        getDef().display(sb, formatter);

        switch (theQuantifier) {
        case ONE:
            break;
        case QSTN:
            sb.append("?");
            break;
        case PLUS:
            sb.append("+");
            break;
        case STAR:
            sb.append("*");
            break;
        }
    }

    static TypeCode getTypeCodeForDefCode(Type t) {

        switch (t) {
        case EMPTY:
            return TypeCode.EMPTY;
        case ANY:
            return TypeCode.ANY;
        case ANY_ATOMIC:
            return TypeCode.ANY_ATOMIC;
        case ANY_JSON_ATOMIC:
            return TypeCode.ANY_JSON_ATOMIC;
        case JSON:
            return TypeCode.JSON;
        case ANY_RECORD:
            return TypeCode.ANY_RECORD;
        case INTEGER:
            return TypeCode.INT;
        case LONG:
            return TypeCode.LONG;
        case FLOAT:
            return TypeCode.FLOAT;
        case DOUBLE:
            return TypeCode.DOUBLE;
        case NUMBER:
            return TypeCode.NUMBER;
        case STRING:
            return TypeCode.STRING;
        case TIMESTAMP:
            return TypeCode.TIMESTAMP;
        case ENUM:
            return TypeCode.ENUM;
        case BOOLEAN:
            return TypeCode.BOOLEAN;
        case BINARY:
            return TypeCode.BINARY;
        case FIXED_BINARY:
            return TypeCode.FIXED_BINARY;
        case RECORD:
            return TypeCode.RECORD;
        case ARRAY:
            return TypeCode.ARRAY;
        case MAP:
            return TypeCode.MAP;
        default:
            throw new QueryStateException("Unknown type code: " + t);
        }
    }
}
