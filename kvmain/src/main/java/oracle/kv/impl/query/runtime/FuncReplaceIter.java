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

import static oracle.kv.impl.query.runtime.ConcatenateStringsOpIter.STRING_MAX_SIZE;

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
 * {@literal
 * replace(any* str, any* search_str[, any* replacement_str] ) -> string }
 *
 * replace function returns str with every occurrence of search_str replaced
 * with replacement_str. If replacement_str is omitted or empty sequence, then
 * all occurrences of search_str are removed. The result will be checked so
 * that the result would not be bigger than STRING_MAX_SIZE = 2^18 - 1 in chars
 * ie. 512kb, if that is the case a runtime query exception is thrown.
 * Arguments are implicitly casted to string (string sequence of any length).
 *
 * Note: If str or search_str argument is an empty sequence, the result is null.
 * If any argument is a sequence with more than one item or null the result is
 * null.
 *
 * Example:
 * SELECT replace( name, 'Corporation', 'Technologies' )
 * From customers WHERE name = 'FairCom Corporation Location'
 *
 * Result
 * -----------------------------
 * FairCom Technologies Location
 */
public class FuncReplaceIter extends PlanIter {

    private final PlanIter[] theArgs;

    public FuncReplaceIter(
        ExprFuncCall funcCall,
        int resultReg,
        PlanIter[] argIters) {
        super(funcCall, resultReg);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncReplaceIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_REPLACE;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_REPLACE;
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
        FieldValueImpl strValue = argValue;

        if (argValue.isNull() || argIter.next(rcb)) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String str = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();

        argIter = theArgs[1];  // search_str param
        opNext = argIter.next(rcb);

        if (!opNext) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        argValue = rcb.getRegVal(argIter.getResultReg());

        if (argIter.next(rcb) || argValue.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String searchStr = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();

        if (searchStr.length() == 0) {
            rcb.setRegVal(theResultReg, strValue);
            state.done();
            return true;
        }

        String replacementStr = "";

        if (theArgs.length >= 3) {
            argIter = theArgs[2];  // replacement_str param
            opNext = argIter.next(rcb);

            if (opNext) {
                argValue = rcb.getRegVal(argIter.getResultReg());

                if (argIter.next(rcb)) {
                    rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                    state.done();
                    return true;
                }

                if (argValue.isNull()) {
                    rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                    state.done();
                    return true;
                }

                replacementStr = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();
            }
        }

        StringBuilder resStr = new StringBuilder();
        int start = 0;
        int pos;
        do {
            pos = str.indexOf(searchStr, start);
            if (pos >= 0) {
                resStr.append(str, start, pos);
                if (resStr.length() + replacementStr.length() > STRING_MAX_SIZE)
                {
                    throw new QueryException("replace function generates " +
                        "result over STRING max size. Actual size: " +
                        (resStr.length() + replacementStr.length()),
                        getLocation());
                }
                resStr.append(replacementStr);
                start = pos + searchStr.length();
            } else {
                resStr.append(str, start, str.length());
                break;
            }
        } while (true);

        FieldValueImpl resVal = FieldDefImpl.Constants.stringDef.
            createString(resStr.toString());

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
        if (!super.equals(obj) || !(obj instanceof FuncReplaceIter)) {
            return false;
        }
        final FuncReplaceIter other = (FuncReplaceIter) obj;
        return Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArgs);
    }
}
