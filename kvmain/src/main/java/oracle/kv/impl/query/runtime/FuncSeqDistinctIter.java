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
import java.util.HashSet;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

public class FuncSeqDistinctIter extends SingleInputPlanIter {

    private static class HashValue {

        FieldValueImpl theValue;

        HashValue(FieldValueImpl value) {
            theValue = value;
        }

        @Override
        public boolean equals(Object other) {
            HashValue o = (HashValue)other;
            return theValue.equal(o.theValue);
        }

        @Override
        public int hashCode() {
            return theValue.hashcode();
        }
    }

    private static class FuncSeqDistinctState extends PlanIterState {

        HashSet<HashValue> theValues;

        FuncSeqDistinctState() {
            super();
            theValues = new HashSet<HashValue>(128);
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            if (theValues != null) {
                theValues.clear();
            }
        }

        @Override
        public void close() {
            super.close();
            theValues = null;
        }
    }

    private final PlanIter theInput;

    public FuncSeqDistinctIter(
        Expr e,
        int resultReg,
        PlanIter input) {

        super(e, resultReg);
        theInput = input;
    }

    FuncSeqDistinctIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }

    @Override
    protected PlanIter getInput() {
        return theInput;
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_SEQ_DISTINCT;
    }

    @Override
    FuncCode getFuncCode() {
        return FuncCode.FN_SEQ_DISTINCT;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncSeqDistinctState());
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FuncSeqDistinctState state = (FuncSeqDistinctState)
            rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {
            boolean more = theInput.next(rcb);

            if (!more) {
                state.done();
                return false;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (!state.theValues.add(new HashValue(val))) {
                continue;
            }

            rcb.setRegVal(theResultReg, val);
            return true;
        }
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theInput);
    }
}
