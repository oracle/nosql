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
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;

/**
 * {@literal reverse(any* str) -> string }
 *
 * reverse returns the chars of the input string in reverse order.
 * reverse returns the chars of the input string in reverse order. Argument str
 * is implicitly casted to string* (string sequence of any length).
 *
 * Note: For a null argument the result will be null.
 *
 * Note: If str argument is an empty sequence or a sequence with more than one
 * item the result is null.
 *
 * Example:
 * SELECT first_name, reverse(first_name)  FROM employees ;
 *
 * FIRST_NAME  REVERSE
 * ----------  ----------
 * Lindsey     yesdniL
 * William     mailliW
 */
public class FuncReverseIter extends PlanIter {

    private final PlanIter theArg;

    public FuncReverseIter(
        ExprFuncCall funcCall,
        int resultReg,
        PlanIter arg) {
        super(funcCall, resultReg);
        theArg = arg;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncReverseIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
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

        serializeIter(theArg, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_REVERSE;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_REVERSE;
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

        boolean opNext = theArg.next(rcb);

        if (!opNext) {
            state.done();
            return false;
        }

        FieldValueImpl argValue = rcb.getRegVal(theArg.getResultReg());

        if (argValue.isNull() || theArg.next(rcb)) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String str = CastIter.castValue(argValue,
                                        FieldDefImpl.Constants.stringDef,
                                        theLocation).asString().get();

        String resStr = new StringBuilder(str).reverse().toString();

        FieldValueImpl res =
            FieldDefImpl.Constants.stringDef.createString(resStr);
        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
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
        if (!super.equals(obj) || !(obj instanceof FuncReverseIter)) {
            return false;
        }
        final FuncReverseIter other = (FuncReverseIter) obj;
        return Objects.equals(theArg, other.theArg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArg);
    }
}
