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
 * {@literal ltrim(any* str) -> string }
 * {@literal rtrim(any* str) -> string }
 *
 * ltrim function is a function that returns its first str argument with
 * leading space characters removed.
 *
 * rtrim function is a function that returns its first str argument with
 * trailing space characters removed.
 *
 * Note: If str is null, the result is null. Argument str is implicitly casted
 * to string* (string sequence of any length).
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is null.
 */
public class FuncLRTrimIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;
    private final PlanIter[] theArgs;

    public FuncLRTrimIter(ExprFuncCall funcCall,
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
    public FuncLRTrimIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_LRTRIM;
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

        // This doesn't have to use codepoints because the trim char is
        // always space ' ' which is a regular 16bit char
        char c = ' ';

        String resStr;
        int len = str.length();

        if ( theCode == FunctionLib.FuncCode.FN_LTRIM ) {
            int s;
            for (s = 0; s < len && str.charAt(s) == c; ++s) {
            }
            resStr = str.substring(s, len);
        } else {
            int e;
            for (e = len - 1; e >= 0 && str.charAt(e) == c; --e) {
            }
            resStr = str.substring(0, e + 1);
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
        if (!super.equals(obj) || !(obj instanceof FuncLRTrimIter)) {
            return false;
        }
        final FuncLRTrimIter other = (FuncLRTrimIter) obj;
        return (theCode == other.theCode) &&
            Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theArgs);
    }
}
