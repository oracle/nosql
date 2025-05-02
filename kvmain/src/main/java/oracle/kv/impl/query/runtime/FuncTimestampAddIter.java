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
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.api.table.TimestampUtils.Interval;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.Expr.ConstKind;

public class FuncTimestampAddIter extends PlanIter {

    private final PlanIter theTimestampArg;
    private final PlanIter theDurationArg;
    private final boolean theIsDurationConst;

    private static class FuncTimestampAddState extends PlanIterState {
        Interval theDuration;
    }

    public FuncTimestampAddIter(
        Expr e,
        int resultReg,
        PlanIter arg1,
        PlanIter arg2) {

        super(e, resultReg);
        theTimestampArg = arg1;
        theDurationArg = arg2;
        theIsDurationConst = ConstKind.isConst(((ExprFuncCall)e).getArg(1));
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncTimestampAddIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theTimestampArg = deserializeIter(in, serialVersion);
        theDurationArg = deserializeIter(in, serialVersion);
        theIsDurationConst = in.readBoolean();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theTimestampArg, out, serialVersion);
        serializeIter(theDurationArg, out, serialVersion);
        out.writeBoolean(theIsDurationConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_TIMESTAMP_ADD;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_TIMESTAMP_ADD;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncTimestampAddState());
        theTimestampArg.open(rcb);
        theDurationArg.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        FuncTimestampAddState state =
            (FuncTimestampAddState)rcb.getState(theStatePos);
        if (state.isDone()) {
            return false;
        }

        boolean more = theTimestampArg.next(rcb);

        if (!more) {
            state.done();
            return false;
        }

        FieldValueImpl tsVal = rcb.getRegVal(theTimestampArg.getResultReg());
        if (tsVal.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        Interval duration = null;
        if (state.theDuration == null) {

            more = theDurationArg.next(rcb);

            if (!more) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            FieldValueImpl durVal = rcb.getRegVal(theDurationArg.getResultReg());
            if (durVal.isNull()) {
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }

            String durStr = durVal.getString();
            try {
                duration = Interval.parseString(durStr);
                if (theIsDurationConst) {
                    state.theDuration = duration;
                }
            } catch (IllegalArgumentException iae) {
                throw new QueryException("The duration string [" + durStr +
                    "] specified for the timestamp_add function is invalid: " +
                    iae.getMessage(), iae,  getLocation());
            }
        } else {
            duration = state.theDuration;
        }

        TimestampValueImpl res =
            TimestampUtils.timestampAdd((TimestampValueImpl)tsVal, duration);

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theTimestampArg.reset(rcb);
        theDurationArg.reset(rcb);

        FuncTimestampAddState state =
            (FuncTimestampAddState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theTimestampArg.close(rcb);
        theDurationArg.close(rcb);

        state.close();
    }

    @Override
    void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        PlanIter[] args = new PlanIter[2];
        args[0] = theTimestampArg;
        args[1] = theDurationArg;
        displayInputIters(sb, formatter, verbose, args);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncTimestampAddIter)) {
            return false;
        }
        final FuncTimestampAddIter other = (FuncTimestampAddIter) obj;
        return Objects.equals(theTimestampArg, other.theTimestampArg) &&
            Objects.equals(theDurationArg, other.theDurationArg) &&
            (theIsDurationConst == other.theIsDurationConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theTimestampArg,
                            theDurationArg,
                            theIsDurationConst);
    }
}
