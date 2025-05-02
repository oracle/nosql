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
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;

/**
 * {@literal index_of(any* str, any* search[, integer* pos]) -> integer }
 *
 * index_of function determines the first position, if any, at which one string,
 * search, occurs within str returning the position in chars. If search string
 * is of length zero, then it occurs at position 0 for any value of str. If
 * search string does not occur in str, then -1 is returned. The returned type
 * is integer. If not specified the default pos is 0, meaning the search starts
 * at the beginning of str. The return value is relative to the beginning of
 * str, regardless of the value of pos. Arguments str and search are implicitly
 * casted to string* (string sequence of any length).
 *
 * Note: Arguments str and search are implicitly casted to string*
 * (string sequence of any length). If str, search or pos is null, the result
 * is null.
 *
 * Note: pos is an optional integer indicating the character of str where it
 * begins the search that is, the position of the first character of the first
 * substring to compare with search. First char in str has pos 0.
 * For negative values, value 0 is assumed.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is null. For this case one can use seq_transform function.
 *
 * Note: Error if pos is not an integer.
 *
 * Example:
 * {@code
 * SELECT last_name FROM employees
 * WHERE index_of(last_name, "son", 3)) > 0
 * ORDER BY last_name; }
 *
 * LAST_NAME
 * -------------------------
 * Anderson
 * Jameson
 * Sony
 */
public class FuncIndexOfIter extends PlanIter {

    private final PlanIter[] theArgs;

    public FuncIndexOfIter(ExprFuncCall funcCall,
                           int resultReg,
                           @SuppressWarnings("unused")FunctionLib.FuncCode code,
                           PlanIter[] argIters) {
        super(funcCall, resultReg);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncIndexOfIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_INDEX_OF;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_INDEX_OF;
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

        argIter = theArgs[1];  // search param
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

        String searchStr = CastIter.castValue(argValue,
                                              FieldDefImpl.Constants.stringDef,
                                              theLocation).asString().get();

        int pos = 0;

        if (theArgs.length >= 3) {
            argIter = theArgs[2];      // position param
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

            pos = argValue.asInteger().get();
        }

        if (pos < 0) {
            pos = 0;
        }

        // To support UTF32 chars pos must be converted from user mode to
        // java mode (java mode uses 16bit char array)
        int len = str.length();
        int start = 0;
        int i = 0;
        while (start < len && i < pos) {
            int charCount = Character.charCount(str.codePointAt(start));
            start += charCount;
            i++;
        }

        int res = str.indexOf(searchStr, start);
        if (res > 0) {
            res = str.codePointCount(0, res);
        }
        FieldValueImpl resVal =
            FieldDefImpl.Constants.integerDef.createInteger(res);


        rcb.setRegVal(theResultReg, resVal);

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
        if (!super.equals(obj) || !(obj instanceof FuncIndexOfIter)) {
            return false;
        }
        final FuncIndexOfIter other = (FuncIndexOfIter) obj;
        return Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArgs);
    }
}
