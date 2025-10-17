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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

/*
 * long count(any*)
 *
 * long count_numbers(any*)
 */
public class FuncCountIter extends PlanIter {

    private final PlanIter theInput;

    private final FuncCode theFuncCode;

    public FuncCountIter(Expr e, int resultReg, PlanIter input, FuncCode code) {
        super(e, resultReg);
        theInput = input;
        theFuncCode = code;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncCountIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
        short ordinal = in.readShort();
        theFuncCode = FunctionLib.FuncCode.valueOf(ordinal);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theInput, out, serialVersion);
        out.writeShort(theFuncCode.ordinal());
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_COUNT;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return theFuncCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new AggrIterState());
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        /*
         * Don't reset the state of "this". Resetting the state is done in
         * method getAggrValue above.
         */
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            boolean more = theInput.next(rcb);

            if (!more) {
                return true;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull()) {
                continue;
            }

            if (theFuncCode == FuncCode.FN_COUNT_NUMBERS && 
                !val.isNumeric()) {
                continue;
            }

            ++state.theCount;
        }
    }

    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        state.theCount = (val != null ? ((LongValueImpl)val).get() : 0);
    }

    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        FieldValueImpl res =
            FieldDefImpl.Constants.longDef.createLong(state.theCount);

        if (reset) {
            state.reset(this);
        }

        return res;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theInput);
    }
}
