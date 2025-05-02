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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_15;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;

/* to_last_day_of_month(<timestamp>) */
public class FuncToLastDayOfMonthIter extends PlanIter {

    private final PlanIter theTimestampArg;

    public FuncToLastDayOfMonthIter(Expr e, int resultReg, PlanIter arg) {
        super(e, resultReg);
        theTimestampArg = arg;
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncToLastDayOfMonthIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theTimestampArg = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        if (serialVersion < QUERY_VERSION_15) {
            throw new IllegalStateException("Serial version " + serialVersion +
                " does not support last_day_of_month() function, must be " +
                QUERY_VERSION_15 + " or greater");
        }

        super.writeFastExternal(out, serialVersion);
        serializeIter(theTimestampArg, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_LAST_DAY_OF_MONTH;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_LAST_DAY_OF_MONTH;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new PlanIterState());
        theTimestampArg.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
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

        if (!tsVal.isTimestamp()) {
            try {
                tsVal = CastIter.castValue(tsVal,
                                           FieldDefImpl.Constants.timestampDef,
                                           theLocation);
            } catch (QueryException ex) {
                /* return NULL if the input is not castable to timestamp */
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }
        }

        Timestamp ts = TimestampUtils.getLastDay(tsVal.getTimestamp());
        TimestampValueImpl res = FieldDefImpl.Constants.timestampDefs[0]
                                     .createTimestamp(ts);

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theTimestampArg.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theTimestampArg.close(rcb);

        state.close();
    }

    @Override
    void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        displayInputIters(sb, formatter, verbose,
                          new PlanIter[] {theTimestampArg});
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncToLastDayOfMonthIter)) {
            return false;
        }
        final FuncToLastDayOfMonthIter other = (FuncToLastDayOfMonthIter) obj;
        return Objects.equals(theTimestampArg, other.theTimestampArg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theTimestampArg);
    }
}
