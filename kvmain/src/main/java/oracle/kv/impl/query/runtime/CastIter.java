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

package oracle.kv.impl.query.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.BinaryDefImpl;
import oracle.kv.impl.api.table.BooleanDefImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.DoubleDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FixedBinaryDefImpl;
import oracle.kv.impl.api.table.FloatDefImpl;
import oracle.kv.impl.api.table.IntegerDefImpl;
import oracle.kv.impl.api.table.LongDefImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NumberDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.StringDefImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;

/**
 * The main purpose of the CastIter is to do cast the input value to the
 * specified target type.
 *
 * Its semantics are as follows:
 *
 * a. First the quantifier is checked
 *
 * b. If input is NULL value, a NULL value is returned.
 *
 * c. Casting table:
 *         to BOOLEAN DOUBLE FLOAT INTEGER LONG NUMBER STRING ENUM BIN FBIN
 * from
 * BOOLEAN       x                                       x
 * DOUBLE               x      x      x     x     x      x
 * FLOAT                x      x      x     x     x      x
 * INTEGER              x      x      x     x     x      x
 * LONG                 x      x      x     x     x      x
 * NUMBER               x      x      x     x     x      x
 * STRING        x      x      x      x     x            x     x    x   x
 * ENUM                                                  x     x
 * BINARY                                                x          x
 * FIXED_BINARY                                          x              x
 *
 * - structure types -
 * ARRAY
 * MAP
 * RECORD
 *
 * - wildcard types - used in definitions not on actual values
 * ANY
 * ANY_RECORD
 * ANY_ATOMIC
 * JSON
 * ANY_JSON_ATM
 *
 * EMPTY - only for compiller
 *
 * Casting table continuation
 *          to ARRAY MAP RECORD ANY ANY_RECORD ANY_ATM EMPTY JSON ANY_JSON_ATM
 * from
 * BOOLEAN                       x                x           x          x
 * DOUBLE                        x                x           x          x
 * FLOAT                         x                x           x          x
 * INTEGER                       x                x           x          x
 * LONG                          x                x           x          x
 * NUMBER                        x                x           x          x
 * STRING                        x                x           x          x
 * ENUM                          x                x
 * BINARY                        x                x
 * FIXED_BINARY                  x                x
 *
 * - structure types -
 * ARRAY        x                x                            x
 * MAP               x           x                            x
 * RECORD                  x     x      x
 *
 * - wildcard types - used in definitions not on instance values
 * ANY
 * ANY_RECORD
 * ANY_ATOMIC
 * JSON
 * ANY_JSON_ATM
 *
 * EMPTY - only for compiller
 *
 *
 * Inputs:
 *   one value of any type.
 *
 * Result:
 *   a value of of the specified type or NULL value.
 */
public class CastIter extends PlanIter {

    static private class CastIterState extends PlanIterState {

        /*
         * Keeps the state for checking the type quantifier:
         *   -1  no next() call
         *    0  one False next() call
         *    1  one True next() call
         *   10  True False next() calls
         *    2  2 or more True next() calls
         *   20  2 or more True followed by False next() calls
         */
        int state = -1;

        /*
         * The "more" param is the value returned by theInputIter.next()
         */
        boolean next(boolean more) {
            if (more) {
                switch (state) {
                case -1:
                    state = 1;
                    break;
                case 1:
                    state = 2;
                    break;
                case 0:
                case 2:
                case 10:
                case 20:
                    break;
                default:
                    throw new QueryStateException("Unknown state in CastIter.");
                }
            } else {
                switch (state) {
                case -1:
                    state = 0;
                    break;
                case 1:
                    state = 10;
                    break;
                case 2:
                    state = 20;
                    break;
                case 0:
                case 10:
                case 20:
                    break;
                default:
                    throw new QueryStateException("Unknown " +
                        "state in CastIter.");
                }
            }
            return  more;
        }

        void checkQuantifier(
            FieldDefImpl targetType,
            ExprType.Quantifier quantifier) {

            switch (state) {
            case -1:
                throw new QueryStateException("Called checkQuantifier " +
                    "without any next calls.");
            case 0:
                if (quantifier == ExprType.Quantifier.PLUS) {
                    throw new QueryException(
                        "The input to a cast expression returned an empty " +
                        "result, but the target type requires at least " +
                        "one item. Target type:\n" +
                        targetType.getDDLString() + quantifier);
                } else if (quantifier == ExprType.Quantifier.ONE) {
                    throw new QueryException(
                        "The input to a cast expression returned an empty " +
                        "result, but the target type requires exactly " +
                        "one item. Target type:\n" +
                        targetType.getDDLString() + quantifier);
                }
                break;
            case 1:
                // have one input item, maybe more
                break;
            case 10:
                // have exactly one input item
                break;
            case 2:
                // have 2 items, maybe more
            case 20:
                if (quantifier == ExprType.Quantifier.QSTN) {
                    throw new QueryException(
                        "The input to a cast expression returned more than " +
                        "one items, but the target type requires at most " +
                        "one item. Target type:\n" +
                        targetType.getDDLString() + quantifier);
                } else if (quantifier == ExprType.Quantifier.ONE) {
                    throw new QueryException(
                        "The input to a cast expression returned more than " +
                        "one items, but the target type requires exactly " +
                        "one item. Target type:\n" +
                        targetType.getDDLString() + quantifier);
                }
                break;
            default:
                throw new QueryStateException("Unknown state in CastIter.");
            }
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            // Reset to initial state after each row.
            state = -1;
        }
    }

    private final PlanIter theInputIter;

    private final FieldDefImpl theTargetType;

    private final ExprType.Quantifier theTargetQuantifier;


    public CastIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        FieldDefImpl targetType,
        ExprType.Quantifier targetQuantifier) {

        super(e, resultReg);
        theInputIter = inputIter;
        assert targetType != null && targetQuantifier != null;
        this.theTargetType = targetType;
        theTargetQuantifier = targetQuantifier;
    }

    /**
     * FastExternalizable constructor.
     */
    CastIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theTargetType = (FieldDefImpl)deserializeFieldDef(in, serialVersion);
        theTargetQuantifier = deserializeQuantifier(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theInputIter, out, serialVersion);
        serializeFieldDef(theTargetType, out, serialVersion);
        serializeQuantifier(theTargetQuantifier, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.CAST;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new CastIterState());
        theInputIter.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        CastIterState state = (CastIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = state.next(theInputIter.next(rcb));

        state.checkQuantifier(theTargetType, theTargetQuantifier);

        if (!more) {
            state.done();
            return false;
        }

        // for quantifiers ONE or QSTN there will not be another next() call,
        // so the check has to happen now.
        if (theTargetQuantifier == ExprType.Quantifier.ONE ||
            theTargetQuantifier == ExprType.Quantifier.QSTN) {
            state.next(theInputIter.next(rcb));
            state.checkQuantifier(theTargetType, theTargetQuantifier);
            state.done();
        }

        cast(rcb);
        return true;
    }

    private void cast(RuntimeControlBlock rcb) {

        int inputReg = theInputIter.getResultReg();
        FieldValueImpl fromValue = rcb.getRegVal(inputReg);

        FieldValueImpl retValue = castValue(fromValue,
                                            theTargetType,
                                            theLocation);

        rcb.setRegVal(theResultReg, retValue);
    }

    public static FieldValueImpl castValue(
        FieldValueImpl fromValue,
        FieldDefImpl targetType,
        Location loc) {

        if (fromValue.isNull()) {
            return NullValueImpl.getInstance();
        }

        FieldDefImpl fromType = fromValue.getDefinition();

        if (targetType.equals(fromType)) {
            return fromValue;
        }

        //System.out.println("CASTING VALUE:\n" + fromValue + "\nto type:\n" +
        //                   targetType.getDDLString());

        String err_msg = null;

        try {
            switch (targetType.getType()) {
            case INTEGER:
                switch (fromType.getType() ) {
                case INTEGER:
                    assert(targetType.isMRCounter());
                    return fromValue;
                case LONG:
                case FLOAT:
                case DOUBLE:
                case NUMBER:
                    return
                        ((IntegerDefImpl) targetType).
                        createInteger(fromValue.castAsInt());
                case STRING:
                    return
                        ((IntegerDefImpl) targetType).
                        createInteger(Integer.parseInt(fromValue.castAsString()));
                default:
                }
                break;
            case LONG:
                switch (fromType.getType() ) {
                case LONG:
                    assert(targetType.isMRCounter());
                    return fromValue;
                case INTEGER:
                case FLOAT:
                case DOUBLE:
                case NUMBER:
                case TIMESTAMP:
                    return
                        ((LongDefImpl) targetType).
                        createLong(fromValue.castAsLong());
                case STRING:
                    return
                        ((LongDefImpl) targetType).
                        createLong(Long.parseLong(fromValue.castAsString()));
                default:
                }
                break;
            case FLOAT:
                switch (fromType.getType() ) {
                case INTEGER:
                case LONG:
                case DOUBLE:
                case NUMBER:
                    return
                        ((FloatDefImpl) targetType).
                        createFloat(fromValue.castAsFloat());
                case STRING:
                    return
                        ((FloatDefImpl) targetType).
                        createFloat(Float.parseFloat(fromValue.castAsString()));
                default:
                }
                break;
            case DOUBLE:
                switch (fromType.getType() ) {
                case INTEGER:
                case LONG:
                case FLOAT:
                case NUMBER:
                    return
                        ((DoubleDefImpl) targetType).
                        createDouble(fromValue.castAsDouble());
                case STRING:
                    return
                        ((DoubleDefImpl) targetType).
                        createDouble(Double.parseDouble(
                            fromValue.castAsString()));
                default:
                }
                break;
            case NUMBER:
                switch (fromType.getType() ) {
                case NUMBER:
                    assert(targetType.isMRCounter());
                    return fromValue;
                case INTEGER:
                    return ((NumberDefImpl) targetType).
                        createNumber(fromValue.getInt());
                case LONG:
                    return ((NumberDefImpl) targetType).
                        createNumber(fromValue.getLong());
                case FLOAT:
                    return ((NumberDefImpl) targetType).
                        createNumber(fromValue.getFloat());
                case DOUBLE:
                    return ((NumberDefImpl) targetType).
                        createNumber(fromValue.getDouble());
                case STRING:
                    return ((NumberDefImpl) targetType).
                        createNumber(fromValue.castAsString());
                default:
                }
                break;
            case BOOLEAN:
                switch (fromType.getType() ) {
                case STRING:
                    return
                        ((BooleanDefImpl) targetType).
                        createBoolean(Boolean.parseBoolean(
                            fromValue.castAsString()));
                default:
                }
                break;
            case ENUM:
                switch (fromType.getType() ) {
                case STRING:
                    return targetType.createEnum(fromValue.castAsString());
                default:
                }
                break;
            case BINARY:
                switch (fromType.getType() ) {
                case STRING:
                    return
                        ((BinaryDefImpl) targetType).
                        fromString(fromValue.castAsString());
                default:
                }
                break;
            case FIXED_BINARY:
                switch (fromType.getType() ) {
                case STRING:
                    return
                        ((FixedBinaryDefImpl) targetType).
                        fromString(fromValue.castAsString());
                default:
                }
                break;
            case TIMESTAMP:
                switch (fromType.getType() ) {
                case TIMESTAMP:
                    int targetPrecision =
                        ((TimestampDefImpl)targetType).getPrecision();

                    return ((TimestampValueImpl)fromValue).
                        castToPrecision(targetPrecision);

                case STRING:
                    return ((TimestampDefImpl) targetType).
                        fromString(fromValue.castAsString());
                case LONG:
                case INTEGER:
                    return ((TimestampDefImpl) targetType).
                        createTimestamp(new Timestamp(fromValue.getLong()));
                default:
                }
                break;
            case STRING:
                switch (fromType.getType() ) {
                case STRING:
                    assert(targetType.isUUIDString());
                    return fromValue;
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case ENUM:
                case TIMESTAMP:
                    return ((StringDefImpl) targetType).
                        createString(fromValue.castAsString());
                case BINARY:
                    return ((StringDefImpl) targetType).
                        createString(fromValue.asBinary().toString());
                case FIXED_BINARY:
                    return ((StringDefImpl) targetType).
                        createString(fromValue.asFixedBinary().toString());
                case BOOLEAN:
                    return ((StringDefImpl) targetType).
                        createString(
                            fromValue.asBoolean().get() ? "true" : "false");
                default:
                    return ((StringDefImpl) targetType).
                        createString(fromValue.toJsonString(false));
                }
            case ARRAY: {
                if (fromType.getType() != Type.ARRAY) {
                    break;
                }

                ArrayDefImpl arrayDef = (ArrayDefImpl)targetType;
                FieldDefImpl elemDef = arrayDef.getElement();
                ArrayValueImpl toArray = arrayDef.createArray();
                ArrayValueImpl fromArray = (ArrayValueImpl)fromValue;

                int size = fromArray.size();

                for (int i = 0; i < size; i++) {
                    FieldValueImpl elem = castValue(fromArray.get(i), elemDef, loc);
                    toArray.add(elem);
                }
                return toArray;
            }
            case MAP: {
                MapDefImpl mapDef = (MapDefImpl)targetType;
                FieldDefImpl elemDef = mapDef.getElement();
                MapValueImpl toMap = mapDef.createMap();

                if (fromType.getType() == Type.MAP) {
                    MapValueImpl fromMap = (MapValueImpl)fromValue;

                    for (Map.Entry<String, FieldValue> entry :
                             fromMap.getFields().entrySet()) {
                        String fname = entry.getKey();
                        FieldValueImpl fval = (FieldValueImpl)entry.getValue();
                        fval = castValue(fval, elemDef, loc);
                        toMap.put(fname, fval);
                    }

                    return toMap;
                }

                if (fromType.getType() == Type.RECORD) {
                    RecordValueImpl fromRec = (RecordValueImpl)fromValue;
                    int numFields = fromRec.getNumFields();

                    for (int i = 0; i < numFields; ++i) {
                        String fname = fromRec.getFieldName(i);
                        FieldValueImpl fval = fromRec.get(i);
                        fval = castValue(fval, elemDef, loc);
                        toMap.put(fname, fval);
                    }

                    return toMap;
                }

                break;
            }
            case RECORD: {
                RecordDefImpl recDef = (RecordDefImpl)targetType;

                if (fromType.getType() == Type.RECORD) {
                    RecordValueImpl dstRec = recDef.createRecord();
                    RecordValueImpl srcRec = (RecordValueImpl)fromValue;

                    int numFields = srcRec.getNumFields();

                    if (numFields != recDef.getNumFields()) {
                        break;
                    }

                    int i;
                    for (i = 0; i < numFields; i++) {

                        String fname = srcRec.getFieldName(i);
                        if (!fname.equalsIgnoreCase(recDef.getFieldName(i))) {
                            break;
                        }

                        FieldDefImpl fdef = recDef.getFieldDef(i);
                        FieldValueImpl fval = castValue(srcRec.get(i), fdef, loc);
                        dstRec.put(i, fval);
                    }

                    if (i == numFields) {
                        return dstRec;
                    }

                    break;

                } else if (fromType.getType() == Type.MAP) {
                    RecordValueImpl dstRec = recDef.createRecord();
                    MapValueImpl srcMap = (MapValueImpl)fromValue;

                    boolean missingFields = false;
                    int numFields = dstRec.getNumFields();

                    for (int i = 0; i < numFields; i++) {

                        String fname = recDef.getFieldName(i);
                        FieldDefImpl fdef = recDef.getFieldDef(i);
                        FieldValueImpl fval = srcMap.get(fname);

                        if (fval != null) {

                            if (fval.isJsonNull()) {
                                if (fdef.equals(
                                        FieldDefImpl.Constants.jsonDef)) {
                                    dstRec.put(i, fval);
                                } else {
                                    dstRec.putNull(i);
                                }
                            } else {
                                fval = castValue(fval, fdef, loc);
                                dstRec.put(i, fval);
                            }
                        } else {
                            missingFields = true;
                        }
                    }

                    if (missingFields) {
                        dstRec.addMissingFields();
                    }

                    return dstRec;
                }

                break;
            }
            case JSON:
                if (fromValue.isRecord()) {
                    return castValue(fromValue,
                                     FieldDefImpl.Constants.mapJsonDef, loc);

                } else if (fromValue.isMap()) {
                    FieldDefImpl elemDef =
                        ((MapDefImpl)fromValue.getDefinition()).getElement();

                    if (elemDef.isJson()) {
                        return fromValue;
                    }
                    MapValueImpl toMap =
                        FieldDefImpl.Constants.jsonDef.createMap();
                    MapValueImpl fromMap = (MapValueImpl)fromValue;

                    for (Map.Entry<String, FieldValue> entry :
                         fromMap.getFields().entrySet()) {

                        FieldValueImpl elem = castValue(
                            (FieldValueImpl) entry.getValue(),
                            FieldDefImpl.Constants.jsonDef,
                            loc);

                        toMap.put(entry.getKey(), elem);
                    }

                    return toMap;

                } else if (fromValue.isArray()) {
                    FieldDefImpl elemDef =
                        ((ArrayDefImpl)fromValue.getDefinition()).getElement();

                    if (elemDef.isJson()) {
                        return fromValue;
                    }

                    ArrayValueImpl toArr =
                        FieldDefImpl.Constants.jsonDef.createArray();
                    ArrayValueImpl fromArr = (ArrayValueImpl)fromValue;

                    for (int i = 0; i < fromArr.size(); ++i) {

                        FieldValueImpl elem = castValue(
                            fromArr.get(i),
                            FieldDefImpl.Constants.jsonDef, loc);

                        toArr.add(elem);
                    }

                    return toArr;
                }

                if (!fromType.isSubtype(targetType)) {
                    break;
                }
                return fromValue;

            case ANY_JSON_ATOMIC:
            case ANY:
            case ANY_RECORD:
            case ANY_ATOMIC:
                if (fromType.isSubtype(targetType)) {
                    return fromValue;
                }
                break;

            case EMPTY:
                break;
            default:
                throw new QueryStateException(
                    "Unexpected type: " + targetType.getType());
            }
        } catch (IllegalArgumentException iae) {
            err_msg = iae.getMessage();
        }

        throw new QueryException(
            "Cannot cast value \n" + fromValue + "\nof type \n" +
            fromType.getDDLString() + "\nto type \n" +
            targetType.getDDLString() +
            (err_msg != null ? ("\n" + err_msg) : ""), loc);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        CastIterState state = (CastIterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInputIter.close(rcb);
        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"target type\" : ");
        theTargetType.displayAsJson(sb, formatter, verbose);
        sb.append(",\n");
        formatter.indent(sb);
        sb.append("\"quantifier\" : \"");
        sb.append(theTargetQuantifier);
        sb.append("\",\n");
        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof CastIter)) {
            return false;
        }
        final CastIter other = (CastIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            Objects.equals(theTargetType, other.theTargetType) &&
            (theTargetQuantifier == other.theTargetQuantifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theInputIter,
                            theTargetType,
                            theTargetQuantifier);
    }
}
