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
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;

/**
 * Trim
 * {@literal trim(any* str [, string* where[, string* trim_char]]) -> string }
 *
 * trim function is a function that returns its first str argument with leading
 * and/or trailing pad characters removed. The second argument where indicates
 * whether leading, or trailing, or both leading and trailing pad characters
 * should be removed. Valid values for second parameter are "leading",
 * "trailing", "both" (case is ignored), if not specified value "both"
 * will be assumed,  if specified but not one of the valid values null is
 * returned. The third argument trim_char specifies the pad character that is to
 * be removed, if there are more chars in trim_char the first char will be used,
 * if "" empty string then no trimming, if not specified then ' ' space char
 * will be used. Arguments str, where and trim_char are implicitly casted to
 * string* (string sequence of any length).
 *
 * Note: If str, where or trim_char is null, the result is null.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is null.
 *
 * Example
 * This example trims leading zeros from the hire date of the employees:
 *
 * SELECT employee_id, trim(hire_date, "leading", "0")
 * FROM employees
 *
 * EMPLOYEE_ID trim
 * ----------- ---------
 *         105 25-JUN-05
 *         106 5-FEB-06
 *         107 7-FEB-07
 */
public class FuncTrimIter extends PlanIter {

    private final PlanIter[] theArgs;

    public FuncTrimIter(ExprFuncCall funcCall,
                        int resultReg,
                        @SuppressWarnings("unused")FunctionLib.FuncCode code,
                        PlanIter[] argIters) {
        super(funcCall, resultReg);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncTrimIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
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
        serializeIters(theArgs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_TRIM;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_TRIM;
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

        if( theArgs.length < 1 || theArgs.length > 3) {
            throw new QueryException(
                "Function trim must have minimum 1 and " +
                    "maximum 3 parameters.",
                getLocation());
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

        String where = "both";

        if (theArgs.length >= 2) {
            argIter = theArgs[1];  // where param
            opNext = argIter.next(rcb);

            if (!opNext) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            argValue = rcb.getRegVal(argIter.getResultReg());

            if (argValue.isNull() || argIter.next(rcb)) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            where = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();

            if (!where.equalsIgnoreCase("both") &&
                !where.equalsIgnoreCase("leading") &&
                !where.equalsIgnoreCase("trailing") ) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }
        }

        int c = ' ';
        if (theArgs.length >= 3) {
            argIter = theArgs[2];  // char param
            opNext = argIter.next(rcb);

            if (!opNext) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            argValue = rcb.getRegVal(argIter.getResultReg());

            if (argValue.isNull() || argIter.next(rcb)) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            String charParam = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();
            if (charParam.length() < 1) {
                rcb.setRegVal(
                    theResultReg,
                    FieldDefImpl.Constants.stringDef.createString(str));
                state.done();
                return true;
            }

            // Using codepoints in order to support UTF32 chars too.
            c = charParam.codePointAt(0);
        }

        int s = 0;
        int len = str.length();
        int charCount = Character.charCount(c);
        int e = len - charCount;

        if (where.equalsIgnoreCase("both") ||
            where.equalsIgnoreCase("leading")) {
            while (s < len && str.codePointAt(s) == c) {
                s += charCount;
            }
        }
        if (where.equalsIgnoreCase("both") ||
            where.equalsIgnoreCase("trailing")) {
            while (e >= s && str.codePointAt(e) == c) {
                e -= charCount;
            }
        }

        String resStr = str.substring(s, e + charCount);

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
        if (!super.equals(obj) || !(obj instanceof FuncTrimIter)) {
            return false;
        }
        final FuncTrimIter other = (FuncTrimIter) obj;
        return Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArgs);
    }
}
