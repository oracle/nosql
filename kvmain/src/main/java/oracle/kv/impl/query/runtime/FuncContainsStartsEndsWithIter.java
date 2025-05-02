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
import java.util.Objects;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;

/**
 * {@literal contains(any* str, any* contained_str) -> boolean }
 *   - returns true if contained_str exists inside str.
 *
 * {@literal starts_with(any* str, any* start) -> boolean }
 *   - returns true if str starts with start substring.
 *
 * {@literal ends_with(any* str, any* end) -> boolean }
 *   - returns true if str ends with end substring.
 *
 * Note: Arguments are implicitly casted to string* (string sequence of any
 * length). For a null str, contained_str, start or end the result will be null.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is false.
 *
 * Example:
 * SELECT first_name, contains(first_name, 'in'),
 *   starts_with(first_name, 'Li'), ends_with(first_name, 'am')
 * FROM employees
 *
 * FIRST_NAME  CONTAINS  STARTS_WITH  ENDS_WITH
 * ----------  --------  -----------  ---------
 * Lindsey     true      true         false
 * William     false     false        true
 */
public class FuncContainsStartsEndsWithIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;
    private final PlanIter[] theArgs;

    public FuncContainsStartsEndsWithIter(ExprFuncCall funcCall,
                                          int resultReg,
                                          FunctionLib.FuncCode code,
                                          PlanIter[] argIters) {
        super(funcCall, resultReg);
        theCode = code;
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncContainsStartsEndsWithIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.VALUES_COUNT);
        theCode = FunctionLib.FuncCode.valueOf(ordinal);
        theArgs = deserializeIters(in, serialVersion);
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
        serializeIters(theArgs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_CONTAINS_STARTS_ENDS_WITH;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter argIter : theArgs) {
            argIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        PlanIter argIter = theArgs[0];  // str param
        boolean opNext = argIter.next(rcb);

        if (!opNext) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());

        if (argIter.next(rcb)) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        if (argValue.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String str = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();

        argIter = theArgs[1];  // start or end param
        opNext = argIter.next(rcb);

        if (!opNext) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        argValue = rcb.getRegVal(argIter.getResultReg());

        if (argIter.next(rcb)) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        if (argValue.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String containsStr = CastIter.castValue(argValue,
                                                FieldDefImpl.Constants.stringDef,
                                                theLocation).asString().get();

        boolean resB;

        switch ( theCode ) {
        case FN_CONTAINS:
            resB = str.contains(containsStr);
            break;
        case FN_STARTS_WITH:
            resB = str.startsWith(containsStr);
            break;
        case FN_ENDS_WITH:
            resB = str.endsWith(containsStr);
            break;
        default:
            throw new QueryStateException("Unknown code " + theCode + " " +
                getName());
        }

        FieldValueImpl res =
            FieldDefImpl.Constants.booleanDef.createBoolean(resB);

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        for (PlanIter argIter : theArgs) {
            argIter.reset(rcb);
        }

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter argIter : theArgs) {
            argIter.close(rcb);
        }

        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        displayInputIters(sb, formatter, verbose, theArgs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) ||
            !(obj instanceof FuncContainsStartsEndsWithIter)) {
            return false;
        }
        final FuncContainsStartsEndsWithIter other =
            (FuncContainsStartsEndsWithIter) obj;
        return (theCode == other.theCode) &&
            Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theArgs);
    }
}
