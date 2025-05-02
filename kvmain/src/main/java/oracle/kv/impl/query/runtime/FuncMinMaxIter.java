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
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;

/*
 * any_atomic min(any*)
 *
 * any_atomic max(any*)
 */
public class FuncMinMaxIter extends PlanIter {

    private final FuncCode theFuncCode;

    private final PlanIter theInput;

    public FuncMinMaxIter(
        Expr e,
        int resultReg,
        FuncCode code,
        PlanIter input,
        boolean forCloud) {
        super(e, resultReg, forCloud);
        theFuncCode = code;
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncMinMaxIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = in.readShort();
        theFuncCode = FuncCode.valueOf(ordinal);
        theInput = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theFuncCode.ordinal());
        serializeIter(theInput, out, serialVersion);
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);
        out.writeShort(theFuncCode.ordinal());
        theInput.writeForCloud(out, driverVersion, valWriter);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_MIN_MAX;
    }

    @Override
    FuncCode getFuncCode() {
        return theFuncCode;
    }

    @Override
    public PlanIter getInputIter() {
        return theInput;
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
         * method getAggrValue below.
         */
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

            minmaxNewVal(rcb, state, theFuncCode, val);
        }
    }

    @Override
    void aggregate(RuntimeControlBlock rcb, FieldValueImpl val) {
        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        minmaxNewVal(rcb, state,  theFuncCode, val);
    }

    static void minmaxNewVal(
        RuntimeControlBlock rcb,
        AggrIterState state,
        FuncCode fncode,
        FieldValueImpl val) {

        if (val.isNull() || val.isJsonNull() || val.isEMPTY()) {
            return;
        }

        switch (val.getType()) {
        case BINARY:
        case FIXED_BINARY:
        case RECORD:
        case MAP:
        case ARRAY:
            return;
        default:
            break;
        }

        if (state.theMinMax.isNull()) {

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Setting min/max to " + val);
            }

            state.theMinMax = val;
            return;
        }

        int cmp = FieldValueImpl.compareAtomicsTotalOrder(state.theMinMax, val);

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Compared values: \n" + state.theMinMax + "\n" + val +
                      "\ncomp res = " + cmp);
        }

        if (fncode == FuncCode.FN_MIN ||
            fncode == FuncCode.FN_SEQ_MIN) {
            if (cmp <= 0) {
                return;
            }
        } else if (cmp >= 0) {
            return;
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Setting min/max to " + val);
        }

        state.theMinMax = val;
    }

    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        state.theMinMax = (val != null ? val : NullValueImpl.getInstance());
    }

    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        FieldValueImpl res = state.theMinMax;

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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncMinMaxIter)) {
            return false;
        }
        final FuncMinMaxIter other = (FuncMinMaxIter) obj;
        return (theFuncCode == other.theFuncCode) &&
            Objects.equals(theInput, other.theInput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theFuncCode, theInput);
    }
}
