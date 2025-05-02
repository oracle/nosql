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
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;

public class FuncTimestampDiffIter extends PlanIter {

    private final PlanIter theArg1;
    private final PlanIter theArg2;

    public FuncTimestampDiffIter(
        Expr e,
        int resultReg,
        PlanIter arg1,
        PlanIter arg2) {

        super(e, resultReg);
        theArg1 = arg1;
        theArg2 = arg2;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncTimestampDiffIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theArg1 = deserializeIter(in, serialVersion);
        theArg2 = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theArg1, out, serialVersion);
        serializeIter(theArg2, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_TIMESTAMP_DIFF;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_TIMESTAMP_DIFF;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new PlanIterState());
        theArg1.open(rcb);
        theArg2.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state.isDone()) {
            return false;
        }

        boolean more = theArg1.next(rcb);

        if (!more) {
            state.done();
            return false;
        }

        FieldValueImpl ts1Value = rcb.getRegVal(theArg1.getResultReg());

        if (ts1Value.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        more = theArg2.next(rcb);

        if (!more) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        FieldValueImpl ts2Value = rcb.getRegVal(theArg2.getResultReg());

        if (ts2Value.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        long millis = ts1Value.getTimestamp().getTime() -
                      ts2Value.getTimestamp().getTime();

        rcb.setRegVal(theResultReg,
                      FieldDefImpl.Constants.longDef.createLong(millis));

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theArg1.reset(rcb);
        theArg2.reset(rcb);

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theArg1.close(rcb);
        theArg2.close(rcb);

        state.close();
    }

    @Override
    void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        PlanIter[] args = new PlanIter[2];
        args[0] = theArg1;
        args[1] = theArg2;
        displayInputIters(sb, formatter, verbose, args);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncTimestampDiffIter)) {
            return false;
        }
        final FuncTimestampDiffIter other = (FuncTimestampDiffIter) obj;
        return Objects.equals(theArg1, other.theArg1) &&
            Objects.equals(theArg2, other.theArg2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArg1, theArg2);
    }
}
