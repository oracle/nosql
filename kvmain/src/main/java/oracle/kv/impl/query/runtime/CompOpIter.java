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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.EnumValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FloatValueImpl;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.StringValueImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FuncCompOp;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.NumberValue;

/**
 * Iterator to implement the comparison operators
 *
 * boolean comp(any*, any*)
 *
 * Returns NULL if any operand returns NULL.
 * Returns true if both operands return zero items and op is eq, ge, or le.
 * Returns false if any operand returns zero or more than 1 items.
 * Returns false if the items to compare are not comparable.
 */
public class CompOpIter extends PlanIter {

    static public class CompResult {

        public int comp;
        public boolean incompatible;
        public boolean haveNull;

        void clear() {
            comp = 0;
            incompatible = false;
            haveNull = false;
        }

        @Override
        public String toString() {
            return ("(comp, incompatible, haveNull) = (" + 
                    comp + ", " + incompatible + ", " + haveNull + ")");
        }
    }

    static private class CompIterState extends PlanIterState {

        final CompResult theResult = new CompResult();

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theResult.clear();
        }
    }

    private final FuncCode theCode;

    private final PlanIter theLeftOp;

    private final PlanIter theRightOp;

    public CompOpIter(
        Expr e,
        int resultReg,
        FuncCode code,
        PlanIter[] argIters) {

        super(e, resultReg);
        theCode = code;
        assert(argIters.length == 2);
        theLeftOp = argIters[0];
        theRightOp = argIters[1];
    }


    /**
     * FastExternalizable constructor.
     */
    CompOpIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FuncCode.VALUES_COUNT);
        theCode = FuncCode.valueOf(ordinal);
        theLeftOp = deserializeIter(in, serialVersion);
        theRightOp = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIter(theLeftOp, out, serialVersion);
        serializeIter(theRightOp, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.VALUE_COMPARE;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new CompIterState());
        theLeftOp.open(rcb);
        theRightOp.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        CompIterState state = (CompIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean leftOpNext = theLeftOp.next(rcb);

        if (leftOpNext && theLeftOp.next(rcb)) {
            throw new QueryException(
                "The left operand of comparison operator " +
                FuncCompOp.printOp(theCode) +
                " is a sequence with more than one items. Comparison " +
                "operators cannot operate on sequences of more than one items.",
                theLocation);
        }

        boolean rightOpNext = theRightOp.next(rcb);

        if (rightOpNext && theRightOp.next(rcb)) {
            throw new QueryException(
                "The right operand of comparison operator " +
                FuncCompOp.printOp(theCode) +
                " is a sequence with more than one items. Comparison " +
                "operators cannot operate on sequences of more than one items.",
                theLocation);
        }

        if (!rightOpNext && !leftOpNext) {
            /* both sides are empty */
            state.theResult.comp = 0;

        } else if (!rightOpNext || !leftOpNext) {
            /* only one of the sides is empty */
            if (theCode != FuncCode.OP_NEQ) {
                /* this will be converted to false */
                state.theResult.incompatible = true;
            } else {
                /* this will be converted to true */
                state.theResult.comp = 1;
            }

        } else {
            FieldValueImpl lvalue = rcb.getRegVal(theLeftOp.getResultReg());
            FieldValueImpl rvalue = rcb.getRegVal(theRightOp.getResultReg());

            assert(lvalue != null && rvalue != null);

            compare(rcb,
                    lvalue,
                    rvalue,
                    theCode,
                    false, // forSort
                    state.theResult,
                    getLocation());
        }

        if (state.theResult.haveNull) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        if (state.theResult.incompatible) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        int comp = state.theResult.comp;
        boolean result;

        switch (theCode) {
        case OP_EQ:
            result = (comp == 0);
            break;
        case OP_NEQ:
            result = (comp != 0);
            break;
        case OP_GT:
            result = (comp > 0);
            break;
        case OP_GE:
            result = (comp >= 0);
            break;
        case OP_LT:
            result = (comp < 0);
            break;
        case OP_LE:
            result = (comp <= 0);
            break;
        default:
            throw new QueryStateException(
                "Invalid operation code: " + theCode);
        }

        FieldValueImpl res = (result ?
                              BooleanValueImpl.trueValue :
                              BooleanValueImpl.falseValue);
        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theLeftOp.reset(rcb);
        theRightOp.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theLeftOp.close(rcb);
        theRightOp.close(rcb);
        state.close();
    }

    /*
     * Implements a total order among all kinds of values
     */
    public static int compareTotalOrder(
        RuntimeControlBlock rcb,
        FieldValueImpl v1,
        FieldValueImpl v2,
        SortSpec sortSpec,
        Location loc) {

        if (v1.isNull()) {

            if (v2.isNull()) {
                return 0;
            }

            return (sortSpec.theIsDesc ? -1 : 1);
        }

        if (v2.isNull()) {
            return (sortSpec.theIsDesc ? 1 : -1);
        }

        Type tc1 = v1.getType();
        Type tc2 = v2.getType();

        switch (tc1) {
        case RECORD:
            switch (tc2) {
            case RECORD:
                return compareRecordsTotalOrder(rcb,
                                                (RecordValueImpl)v1,
                                                (RecordValueImpl)v2,
                                                sortSpec, loc);
            case MAP:
            case ARRAY:
                return (sortSpec.theIsDesc ? 1 : -1);
            default:
                assert(!v2.isComplex());
                return (sortSpec.theIsDesc ? -1 : 1);
            }
        case MAP:
            switch (tc2) {
            case RECORD:
                return (sortSpec.theIsDesc ? -1 : 1);
            case MAP:
                return compareMapsTotalOrder(rcb,
                                             (MapValueImpl)v1,
                                             (MapValueImpl)v2,
                                             sortSpec, loc);
            case ARRAY:
                return (sortSpec.theIsDesc ? 1 : -1);
            default:
                assert(!v2.isComplex());
                return (sortSpec.theIsDesc ? -1 : 1);
            }
        case ARRAY:
            switch (tc2) {
            case RECORD:
            case MAP:
                return (sortSpec.theIsDesc ? -1 : 1);
            case ARRAY:
                return compareArraysTotalOrder(rcb,
                                               (ArrayValueImpl)v1,
                                               (ArrayValueImpl)v2,
                                               sortSpec, loc);
            default:
                assert(!v2.isComplex());
                return (sortSpec.theIsDesc ? -1 : 1);
            }
        default:
            assert(!v1.isComplex());

            switch (tc2) {
            case RECORD:
            case MAP:
            case ARRAY:
                return (sortSpec.theIsDesc ? 1 : -1);
            default:
                assert(!v2.isComplex());
                return compareAtomicsTotalOrder(v1, v2, sortSpec);
            }
        }
    }

    static int compareRecordsTotalOrder(
        RuntimeControlBlock rcb,
        RecordValueImpl v1,
        RecordValueImpl v2,
        SortSpec sortSpec,
        Location loc) {

        RecordDefImpl def1 = v1.getDefinition();
        RecordDefImpl def2 = v2.getDefinition();

        if (!def1.equals(def2)) {
            int id1 = System.identityHashCode(def1);
            int id2 = System.identityHashCode(def2);
            return ((Integer)id1).compareTo(id2);

        }

        int comp = 0;
        int numFields = def1.getNumFields();

        for (int i = 0; i < numFields; ++i) {

            comp = compareTotalOrder(rcb,
                                     v1.get(i), v2.get(i),
                                     sortSpec, loc);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }
        }

        return 0;
    }

    static int compareMapsTotalOrder(
        RuntimeControlBlock rcb,
        MapValueImpl v1,
        MapValueImpl v2,
        SortSpec sortSpec,
        Location loc) {

        Iterator<String> keysIter1 = v1.getFieldNames().iterator();
        Iterator<String> keysIter2 = v2.getFieldNames().iterator();

        int comp = 0;

        while (keysIter1.hasNext() && keysIter2.hasNext()) {

            String k1 = keysIter1.next();
            String k2 = keysIter2.next();

            comp = k1.compareTo(k2);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }

            comp = compareTotalOrder(rcb,
                                     v1.get(k1), v2.get(k2),
                                     sortSpec, loc);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }
        }

        if (v1.size() == v2.size()) {
            return 0;
        }

        if (keysIter2.hasNext()) {
            return (sortSpec.theIsDesc ? 1 : -1);
        }

        return (sortSpec.theIsDesc ? -1 : 1);
    }

    static int compareArraysTotalOrder(
        RuntimeControlBlock rcb,
        ArrayValueImpl v1,
        ArrayValueImpl v2,
        SortSpec sortSpec,
        Location loc) {

        int minSize = Math.min(v1.size(), v2.size());

        for (int i = 0; i < minSize; ++i) {

            FieldValueImpl elem1 = v1.getElement(i);
            FieldValueImpl elem2 = v2.getElement(i);

            int comp = compareTotalOrder(rcb, elem1, elem2, sortSpec, loc);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }
        }

        if (v1.size() != minSize) {
            return (sortSpec.theIsDesc ? -1 : 1);
        } else if (v2.size() != minSize) {
            return (sortSpec.theIsDesc ? 1 : -1);
        } else {
            return 0;
        }
    }

    static int compareAtomicsTotalOrder(
        FieldValueImpl v1,
        FieldValueImpl v2,
        SortSpec sortSpec) {

        int comp = FieldValueImpl.compareAtomicsTotalOrder(v1, v2);

        comp = (sortSpec.theIsDesc ? -comp : comp);

        if (!sortSpec.theIsDesc && sortSpec.theNullsFirst) {

            if (v1.isSpecialValue() && !v2.isSpecialValue()) {
                comp = -1;
            }

            if (!v1.isSpecialValue() && v2.isSpecialValue()) {
                comp = 1;
            }
        } else if (sortSpec.theIsDesc && !sortSpec.theNullsFirst) {

            if (v1.isSpecialValue() && !v2.isSpecialValue()) {
                comp = 1;
            }

            if (!v1.isSpecialValue() && v2.isSpecialValue()) {
                comp = -1;
            }
        }

        return comp;
    }

    /*
     * Compare 2 values for the order-relation specified by the given opCode.
     * If the values are complex, the method will, in general, call itself
     * recursivelly on the contained values.
     *
     * The method retuns 3 pieces of info (inside the "res" out param):
     * 
     * a. Whether either v0 or v1 is NULL.
     * b. Whether the values are not cmparable
     * c1. If both a and b are false and the operator is = or !=, an integer which is
     *     equal to 0 if v0 == v1, and non-0 if v0 != v1.
     * c2. If both a nd b are false and the operator is >, >=, <, or <=, an integer
     *     which is equal to 0 if v0 == v1, greater than 0 if v0 > v1, and
     *     less than zero if v0 < v1.
     */
    public static void compare(
        RuntimeControlBlock rcb,
        FieldValueImpl v0,
        FieldValueImpl v1,
        FuncCode opCode,
        boolean forSort,
        CompResult res,
        Location location) {

        if (rcb != null && rcb.getTraceLevel() >= 4) {
            rcb.trace("Comparing values: \n" + v0 + "\n" + v1);
        }

        res.clear();

        if (v0.isNull() || v1.isNull()) {
            res.haveNull = true;
            return;
        }

        if (v1.isJsonNull()) {

            if (v0.isJsonNull()) {
                res.comp = 0;
                return;
            }

            if (opCode != FuncCode.OP_NEQ) {
                /* this will be converted to false */
                res.incompatible = true;
                return;
            }

            /* this will be converted to true */
            res.comp = 1;
            return;
        }

        Type tc0 = v0.getType();
        Type tc1 = v1.getType();

        switch (tc0) {

        case EMPTY:
            if (tc1 == Type.EMPTY) {
                if (opCode == FuncCode.OP_EQ ||
                    opCode == FuncCode.OP_GE ||
                    opCode == FuncCode.OP_LE) {
                    res.comp = 0;
                } else {
                   res.incompatible = true;
                }

                return;
            }

            if (opCode == FuncCode.OP_NEQ) {
                res.comp = 1;
            } else {
                res.incompatible = true;
            }

            return;

        case INTEGER: {
            switch (tc1) {
            case INTEGER:
                res.comp = IntegerValueImpl.compare(
                               ((IntegerValueImpl)v0).getInt(),
                               ((IntegerValueImpl)v1).getInt());
                return;
            case LONG:
                res.comp = LongValueImpl.compare(
                               ((IntegerValueImpl)v0).getLong(),
                               ((LongValueImpl)v1).getLong());
                return;
            case FLOAT:
                res.comp = Float.compare(
                               ((IntegerValueImpl)v0).getInt(),
                               ((FloatValueImpl)v1).getFloat());
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case DOUBLE:
                res.comp = Double.compare(
                               ((IntegerValueImpl)v0).getInt(),
                               ((DoubleValueImpl)v1).getDouble());
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case LONG: {
            switch (tc1) {
            case INTEGER:
                res.comp = LongValueImpl.compare(
                               ((LongValueImpl)v0).getLong(),
                               ((IntegerValueImpl)v1).getLong());
                return;
            case LONG:
                res.comp = LongValueImpl.compare(
                               ((LongValueImpl)v0).getLong(),
                               ((LongValueImpl)v1).getLong());
                return;
            case FLOAT:
                res.comp = Float.compare(
                               ((LongValueImpl)v0).getLong(),
                               ((FloatValueImpl)v1).getFloat());
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case DOUBLE:
                res.comp = Double.compare(
                               ((LongValueImpl)v0).getLong(),
                               ((DoubleValueImpl)v1).getDouble());
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case FLOAT: {
            switch (tc1) {
            case INTEGER:
                res.comp = Float.compare(
                               ((FloatValueImpl)v0).getFloat(),
                               ((IntegerValueImpl)v1).getInt());
                if (forSort && res.comp == 0) {
                    res.comp = 1;
                }
                return;
            case LONG:
                res.comp = Float.compare(
                               ((FloatValueImpl)v0).getFloat(),
                               ((LongValueImpl)v1).getLong());
                if (forSort && res.comp == 0) {
                    res.comp = 1;
                }
                return;
            case FLOAT:
                res.comp = Float.compare(
                               ((FloatValueImpl)v0).getFloat(),
                               ((FloatValueImpl)v1).getFloat());
                return;
            case DOUBLE:
                res.comp = Double.compare(
                               ((FloatValueImpl)v0).getDouble(),
                               ((DoubleValueImpl)v1).getDouble());
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case DOUBLE: {
            switch (tc1) {
            case INTEGER:
                res.comp = Double.compare(
                               ((DoubleValueImpl)v0).getDouble(),
                               ((IntegerValueImpl)v1).getInt());
                if (forSort && res.comp == 0) {
                    res.comp = 1;
                }
                return;
            case LONG:
                res.comp = Double.compare(
                               ((DoubleValueImpl)v0).getDouble(),
                               ((LongValueImpl)v1).getLong());
                if (forSort && res.comp == 0) {
                    res.comp = 1;
                }
                return;
            case FLOAT:
                res.comp = Double.compare(
                               ((DoubleValueImpl)v0).getDouble(),
                               ((FloatValueImpl)v1).getDouble());
                return;
            case DOUBLE:
                res.comp = Double.compare(
                               ((DoubleValueImpl)v0).getDouble(),
                               ((DoubleValueImpl)v1).getDouble());
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                if (forSort && res.comp == 0) {
                    res.comp = -1;
                }
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case NUMBER: {
            /*
             * Number is comparable against any numeric type
             */
            NumberValue number = (NumberValue) v0;
            switch (tc1) {
            case NUMBER:
            case DOUBLE:
            case FLOAT:
            case INTEGER:
            case LONG:
                res.comp = number.compareTo(v1);
                if (forSort && res.comp == 0) {
                    res.comp = 1;
                }
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case STRING: {
            switch (tc1) {
            case STRING:
                res.comp = ((StringValueImpl)v0).getString().compareTo(
                           ((StringValueImpl)v1).getString());
                return;
            case ENUM:
                // TODO: optimize this
                FieldValueImpl enumVal = TypeManager.promote(
                    v0, TypeManager.createValueType(v1));

                if (enumVal == null) {
                    res.incompatible = true;
                    return;
                }

                compareEnums(enumVal, v1, res);
                return;
            case TIMESTAMP:
                v0 = ((TimestampDefImpl)(v1.getDefinition())).
                     fromString(((StringValueImpl)v0).get());
                res.comp = ((TimestampValueImpl)v0).compareTo(v1);

                if (rcb != null && rcb.getTraceLevel() >= 3) {
                    rcb.trace("Comparing STRING with TIMESTAMP: \n" + v0 + "\n" + v1);
                    rcb.trace("res.comp = " + res.comp);
                }

                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case ENUM: {
            switch (tc1) {
            case STRING:
                FieldValueImpl enumVal = TypeManager.promote(
                    v1, TypeManager.createValueType(v0));

                if (enumVal == null) {
                    res.incompatible = true;
                    return;
                }

                compareEnums(v0, enumVal, res);
                return;
            case ENUM:
                compareEnums(v0, v1, res);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case BOOLEAN: {
            switch (tc1) {
            case BOOLEAN:
                res.comp = ((BooleanValueImpl)v0).compareTo(v1);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case BINARY:
        case FIXED_BINARY: {
            switch (tc1) {
            case BINARY:
            case FIXED_BINARY:
                if (opCode != FuncCode.OP_EQ && opCode != FuncCode.OP_NEQ) {
                    res.incompatible = true;
                    return;
                }

                res.comp = (Arrays.equals(v0.getBytes(), v1.getBytes()) ? 0 : 1);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case TIMESTAMP: {
            switch (tc1) {
            case TIMESTAMP:
                res.comp = ((TimestampValueImpl)v0).compareTo(v1);
                return;
            case STRING:
                v1 = ((TimestampDefImpl)(v0.getDefinition())).
                     fromString(((StringValueImpl)v1).get());
                res.comp = ((TimestampValueImpl)v0).compareTo(v1);

                if (rcb != null && rcb.getTraceLevel() >= 3) {
                    rcb.trace("Comparing TIMESTAMP with STRING: \n" + v0 + "\n" + v1);
                    rcb.trace("res.comp = " + res.comp);
                }

                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case RECORD: {
            switch (tc1) {
            case RECORD:
                if (opCode != FuncCode.OP_EQ && opCode != FuncCode.OP_NEQ) {
                    res.incompatible = true;
                    return;
                }

                if (v0.isTuple()) {
                    v0 = ((TupleValue)v0).toRecord();
                }

                if (v1.isTuple()) {
                    v1 = ((TupleValue)v1).toRecord();
                }

                RecordValueImpl r0 = (RecordValueImpl)v0;
                RecordValueImpl r1 = (RecordValueImpl)v1;
                compareRecords(rcb, r0, r1, opCode, res, location);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case MAP: {
            switch (tc1) {
            case MAP:
                if (opCode != FuncCode.OP_EQ && opCode != FuncCode.OP_NEQ) {
                    res.incompatible = true;
                    return;
                }

                MapValueImpl m0 = (MapValueImpl)v0;
                MapValueImpl m1 = (MapValueImpl)v1;
                compareMaps(rcb, m0, m1, opCode, res, location);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case ARRAY: {
            switch (tc1) {
            case ARRAY:
                ArrayValueImpl a0 = (ArrayValueImpl)v0;
                ArrayValueImpl a1 = (ArrayValueImpl)v1;

                if (opCode == FuncCode.OP_EQ || opCode == FuncCode.OP_NEQ) {
                    if (a0.size() != a1.size()) {
                        res.comp = 1;
                        return;
                    }
                }

                int minSize = Math.min(a0.size(), a1.size());

                for (int i = 0; i < minSize; ++i) {

                    FieldValueImpl elem0 = a0.getElement(i);
                    FieldValueImpl elem1 = a1.getElement(i);
                    assert(elem0 != null);
                    assert(elem1 != null);

                    compare(rcb, elem0, elem1, opCode, forSort, res, location);

                    if (res.comp != 0 || res.haveNull || res.incompatible) {
                        return;
                    }
                }

                if (a0.size() != minSize) {
                    res.comp = 1;
                    return;
                } else if (a1.size() != minSize) {
                    res.comp = -1;
                    return;
                } else {
                    res.comp = 0;
                    return;
                }
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case ANY_JSON_ATOMIC: {
            assert(v0.isJsonNull());

            if (v1.isJsonNull()) {
                res.comp = 0;
                return;
            }

            if (opCode != FuncCode.OP_NEQ) {
                /* this will be converted to false */
                res.incompatible = true;
                return;
            }

            /* this will be converted to true */
            res.comp = 1;
            return;
        }
        default:
            throw new QueryStateException(
                "Unexpected operand type in comparison operator: " + tc0);
        }
    }


    static void compareMaps(
        RuntimeControlBlock rcb,
        MapValueImpl v0,
        MapValueImpl v1,
        FuncCode opCode,
        CompResult res,
        Location location) {

        if (v0.size() != v1.size()) {
            res.comp = 1;
            return;
        }

        for (Map.Entry<String, FieldValue> e0 : v0.getMap().entrySet()) {

            String k0 = e0.getKey();
            FieldValueImpl fv0 = (FieldValueImpl)e0.getValue();
            FieldValueImpl fv1 = v1.get(k0);

            if (fv1 == null) {
                res.comp = 1;
                return;
            }

            compare(rcb, fv0, fv1, opCode, false, res, location);

            if (res.comp != 0 || res.haveNull || res.incompatible) {
                return;
            }
        }

        res.comp = 0;
        return;
    }

    static void compareRecords(
        RuntimeControlBlock rcb,
        RecordValueImpl v0,
        RecordValueImpl v1,
        FuncCode opCode,
        CompResult res,
        Location location) {

        if (v0.getNumFields() != v1.getNumFields()) {
            res.comp = 1;
            return;
        }

        for (int i = 0; i < v0.getNumFields(); ++i) {

            FieldValueImpl fv0 = v0.get(i);
            FieldValueImpl fv1 = v1.get(i);

            compare(rcb, fv0, fv1, opCode, false, res, location);

            if (res.comp != 0 || res.haveNull || res.incompatible) {
                return;
            }

            String k0 = v0.getFieldName(i);
            String k1 = v1.getFieldName(i);

            if (!k0.equalsIgnoreCase(k1)) {
                res.comp = 1;
                return;
            }
        }

        res.comp = 0;
        return;
    }

    static void compareEnums(
        FieldValueImpl v0,
        FieldValueImpl v1,
        CompResult res) {

        EnumValueImpl e0 = (EnumValueImpl)v0;
        EnumValueImpl e1 = (EnumValueImpl)v1;
        EnumDefImpl def0 = e0.getDefinition();
        EnumDefImpl def1 = e1.getDefinition();

        if (def0.valuesEqual(def1)) {
            int idx0 = e0.getIndex();
            int idx1 = e1.getIndex();
            res.comp = ((Integer)idx0).compareTo(idx1);
            return;
        }

        res.incompatible = true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"left operand\" :\n");
        theLeftOp.display(sb, formatter, verbose);
        sb.append(",\n");
        formatter.indent(sb);
        sb.append("\"right operand\" :\n");
        theRightOp.display(sb, formatter, verbose);
    }

    @Override
    void displayName(StringBuilder sb) {

        switch (theCode) {
        case OP_GT:
            sb.append("GREATER_THAN");
            break;
        case OP_GE:
            sb.append("GREATER_OR_EQUAL");
            break;
        case OP_LT:
            sb.append("LESS_THAN");
            break;
        case OP_LE:
            sb.append("LESS_OR_EQUAL");
            break;
        case OP_EQ:
            sb.append("EQUAL");
            break;
        case OP_NEQ:
            sb.append("NOT_EQUAL");
            break;
        default:
            break;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof CompOpIter)) {
            return false;
        }
        final CompOpIter other = (CompOpIter) obj;
        return (theCode == other.theCode) &&
            Objects.equals(theLeftOp, other.theLeftOp) &&
            Objects.equals(theRightOp, other.theRightOp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theLeftOp, theRightOp);
    }
}
