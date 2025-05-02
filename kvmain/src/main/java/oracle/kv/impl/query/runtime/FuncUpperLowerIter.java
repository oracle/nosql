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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;

/**
 * Upper/Lower
 * {@literal upper(any* str) -> string }
 * {@literal lower(any* str) -> string }
 * upper and lower are a pair of functions for converting all the lower case and
 * title case characters in a given string to upper case (upper) or all the
 * upper case and title case characters to lower case (lower). A lower case
 * character is a character in the Unicode General Category class "Ll"
 * (lower-case letters). An upper case character is a character in the Unicode
 * General Category class "Lu" (upper-case letters). A title case character is a
 * character in the Unicode General Category class "Lt" (title-case letters).
 * Argument str is implicitly casted to string* (string sequence of any length).
 *
 * Note: If str is null, the result is null.
 *
 * Note: If str is empty sequence or a sequence with more than one item the
 * result is null.
 *
 * Example
 * This example applies upper to one string and also to a string array:
 *
 * SELECT emp.id, upper(emp.last_name),
 *       [ seq_transform( emp.first_names[], upper($) ) ]
 * FROM employees emp
 * ORDER BY emp.id;
 *
 *          id last_name                              first_names
 * ----------- --------- ----------------------------------------
 *         103    LESLIE                      ["ROSE", "Eleanor"]
 *         104    CLARKE ["EMILIA", "ISOBEL", "EUPHEMIA", "ROSE"]
 */
public class FuncUpperLowerIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;
    private final PlanIter[] theArgs;

    public FuncUpperLowerIter(ExprFuncCall funcCall,
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
    public FuncUpperLowerIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_UPPER_LOWER;
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
            state.done();
            return false;
        }

        FieldValueImpl argValue = rcb.getRegVal(argIter.getResultReg());

        if (argValue.isNull() || argIter.next(rcb)) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String str = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();

        String resStr;
        switch (theCode) {
        case FN_UPPER:
            resStr = str.toUpperCase();
            break;
        case FN_LOWER:
            resStr = str.toLowerCase();
            break;
        default:
            throw new QueryStateException("Unexpected upper/lower " +
                "function code: " + theCode);
        }

        FieldValueImpl res =
            FieldDefImpl.Constants.stringDef.createString(resStr);
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
        if (!super.equals(obj) || !(obj instanceof FuncUpperLowerIter)) {
            return false;
        }
        final FuncUpperLowerIter other = (FuncUpperLowerIter) obj;
        return (theCode == other.theCode) &&
            Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theArgs);
    }
}
