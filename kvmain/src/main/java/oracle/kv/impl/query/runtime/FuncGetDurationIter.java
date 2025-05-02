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
import oracle.kv.impl.api.table.TimestampUtils.Interval;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;

public class FuncGetDurationIter extends PlanIter {

    private final PlanIter theArg;

    public FuncGetDurationIter(Expr e,
                               int resultReg,
                               PlanIter argIter) {
        super(e, resultReg);
        this.theArg = argIter;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncGetDurationIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_GET_DURATION;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_GET_DURATION;
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

        if (argValue.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        Interval itv = Interval.ofMilliseconds(argValue.getLong());
        rcb.setRegVal(
            theResultReg,
            FieldDefImpl.Constants.stringDef.createString(itv.toString()));

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
    void displayContent(StringBuilder sb,
                        DisplayFormatter formatter,
                        boolean verbose) {

        displayInputIter(sb, formatter, verbose, theArg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncGetDurationIter)) {
            return false;
        }
        final FuncGetDurationIter other = (FuncGetDurationIter) obj;
        return Objects.equals(theArg, other.theArg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArg);
    }
}
