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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.table.FieldDef;


/**
 * Iterator to implement the arithmetic unary negate operator '-'.
 *
 * any_atomic? ArithOp(any_atomic?)
 *
 * ArithUnaryOpIter.next() must check that the input is a numeric value.
 *
 * Result:
 *     if arg is int result is an int
 *     if arg is long result is a long
 *     if arg is float result is a float
 *     if arg is double result is a double
 */
public class ArithUnaryOpIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;
    private final PlanIter theArg;

    public ArithUnaryOpIter(
        Expr e,
        int resultReg,
        FunctionLib.FuncCode code,
        PlanIter argIter) {

        super(e, resultReg);
        theCode = checkNull("code", code);
        theArg = argIter;
    }

    /**
     * FastExternalizable constructor.
     */
    public ArithUnaryOpIter(DataInput in, short serialVersion)
            throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.VALUES_COUNT);
        theCode = FunctionLib.FuncCode.valueOf(ordinal);
        theArg = deserializeIter(in, serialVersion);
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
        serializeIter(theArg, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARITHMETIC_NEGATION;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theArg.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        PlanIter argIter = theArg;
        boolean opNext = argIter.next(rcb);

        if (!opNext) {
            state.done();
            return false;
        }

        FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());
        assert (argValue != null);

        FieldValueImpl res;

        if (argValue.isNull()) {
            res = NullValueImpl.getInstance();
        } else {
            res = getNegativeOfValue(argValue, argIter.getLocation());
        }

        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
    }

    public static FieldValueImpl getNegativeOfValue(
        FieldValueImpl argValue,
        Location location) {

        FieldDef.Type argType = argValue.getType();
        FieldValueImpl res;

        switch (argType) {
        case INTEGER:
            int iRes = -argValue.getInt();
            res = FieldDefImpl.Constants.integerDef.createInteger(iRes);
            break;
        case LONG:
            long lRes = -argValue.getLong();
            res = FieldDefImpl.Constants.longDef.createLong(lRes);
            break;
        case FLOAT:
            float fRes = -argValue.getFloat();
            res = FieldDefImpl.Constants.floatDef.createFloat(fRes);
            break;
        case DOUBLE:
            double dRes = -argValue.getDouble();
            res = FieldDefImpl.Constants.doubleDef.createDouble(dRes);
            break;
        case NUMBER:
            BigDecimal nRes = argValue.getDecimal().negate();
            res = FieldDefImpl.Constants.numberDef.createNumber(nRes);
            break;
        default:
            throw new QueryException(
                "Operand in unary arithmetic operation has illegal type\n" +
                "Operand type : " + argValue.getDefinition().getDDLString(),
                location);
        }

        return res;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theArg.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theArg.close(rcb);

        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        displayInputIter(sb, formatter, verbose, theArg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ArithUnaryOpIter)) {
            return false;
        }
        final ArithUnaryOpIter other = (ArithUnaryOpIter) obj;
        return (theCode == other.theCode) &&
            Objects.equals(theArg, other.theArg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theArg);
    }
}
