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
 * Substring
 * {@literal
 * substring(any* str, integer* position[, integer* substring_length]) -> string }
 *
 * substring is a function that returns a string extracted from a given string
 * according to a given numeric starting position and a given numeric
 * substring_length. Argument str is implicitly casted to sequence of string.
 *
 * Note: If str is null, the result is null. The result is also null when
 * position less than 0 or bigger or equal to str length.
 *
 * The result is empty string "" if the result doesn't select any chars or if
 * substring_length is less than 1.
 *
 * Note: If any argument is an empty sequence or is a sequence with more than
 * one item the result is null.
 *
 * Note: position argument indicates where to start the result, first char has
 * position 0.
 *
 * Note: If position or substring_length are not integer an error is thrown:
 *      Cannot promote item 2 of type: String to type: Integer*
 *
 * Example:
 *
 * SELECT substring('ABCDEFG', 2, 4) as Substring FROM t;
 *
 * Substring
 * ---------
 * CDEF
 */
public class FuncSubstringIter extends PlanIter {

    private final PlanIter[] theArgs;

    public FuncSubstringIter(ExprFuncCall funcCall,
                         int resultReg,
                         @SuppressWarnings("unused")FunctionLib.FuncCode code,
                         PlanIter[] argIters) {
        super(funcCall, resultReg);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncSubstringIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_SUBSTRING;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_SUBSTRING;
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

        argIter = theArgs[1];  // position param
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

        int pos = argValue.asInteger().get();

        int forLength = Integer.MAX_VALUE;

        if (theArgs.length == 3) {
            argIter = theArgs[2];  // substring_length
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

            forLength = argValue.asInteger().get();

            if (forLength < 1) {
                rcb.setRegVal(theResultReg,
                    FieldDefImpl.Constants.stringDef.createString(""));
                state.done();
                return true;
            }
        }

        int len = str.length();
        String resStr = null;

        if ( pos >= 0 ) {

            // To support UTF32 chars, pos must be converted from user mode to
            // java mode (java mode uses 16bit char array)
            int posJava = 0;
            int cpCount = 0;
            while (posJava < len && cpCount < pos) {
                int charCount = Character.charCount(str.codePointAt(posJava));
                posJava += charCount;
                cpCount++;
            }

            if (posJava >= len) {
                resStr = null;
            } else {
                int endJava = posJava;
                cpCount = 0;
                while (endJava < len && cpCount < forLength) {
                    int charCount =
                        Character.charCount(str.codePointAt(endJava));
                    endJava += charCount;
                    cpCount++;
                }

                resStr = str.substring(posJava, endJava);
            }
        }

        FieldValueImpl res;

        if (resStr == null) {
            res = NullValueImpl.getInstance();
        } else {
            res = FieldDefImpl.Constants.stringDef.createString(resStr);
        }

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
        if (!super.equals(obj) || !(obj instanceof FuncSubstringIter)) {
            return false;
        }
        final FuncSubstringIter other = (FuncSubstringIter) obj;
        return Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArgs);
    }
}
