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
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;

/**
 *
 */
public class FuncRemainingHoursIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncRemainingHoursIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncRemainingHoursIter(DataInput in, short serialVersion)
        throws IOException {
        super(in, serialVersion);
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
    }

    @Override
    protected PlanIter getInput() {
        return theInput;
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_REMAINING_HOURS;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theInput.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInput.next(rcb);

        if (!more) {
            state.done();
            return false;
        }

        FieldValueImpl row = rcb.getRegVal(theInput.getResultReg());
        long expTime;

        if (row == NullValueImpl.getInstance()) {
             rcb.setRegVal(theResultReg, row);
             state.done();
             return true;
        }

        if (row.isTuple()) {
            expTime = ((TupleValue)row).getExpirationTime();
        } else if (row.isRecord()) {
            expTime = ((RowImpl)row).getExpirationTime();
        }  else if (row.isJsonRowMap()) {
            expTime = row.asJsonRowMap().getExpirationTime();
        } else {
            throw new QueryException(
                "Input to the remaining_hours() function is not a row " +
                "variable:\n" + row, getLocation());
        }

        int remaining = -1;

        if (expTime > 0) {
            long time = System.currentTimeMillis();

            remaining = (int)
                ((expTime - time) / TimestampDefImpl.MILLIS_PER_HOUR);
        }

        rcb.setRegVal(
            theResultReg,
            FieldDefImpl.Constants.integerDef.createInteger(remaining));
        state.done();
        return true;
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
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theInput);
    }
}
