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
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;

/**
 *
 */
public class FuncRowStorageSizeIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncRowStorageSizeIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInput = input;
    }

    FuncRowStorageSizeIter(DataInput in, short serialVersion)
        throws IOException {
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
        return PlanIterKind.FUNC_ROW_STORAGE_SIZE;
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
        int storageSize;

        if (row == NullValueImpl.getInstance()) {
             rcb.setRegVal(theResultReg, row);
             state.done();
             return true;
        }

        if (row.isTuple()) {
            storageSize = ((TupleValue)row).getStorageSize();
        } else if (row.isRecord()) {
            storageSize = ((RowImpl)row).getStorageSize();
        }  else if (row.isJsonRowMap()) {
            storageSize = row.asJsonRowMap().getStorageSize();
        } else {
            throw new QueryException(
                ("Input to the row_storage_size() function is not a row: " + row.getClass()),
                getLocation());
        }

        if (rcb.getTraceLevel() > 3) {
            rcb.trace("row storage size  = " + storageSize);
        }

        rcb.setRegVal(
            theResultReg,
            FieldDefImpl.Constants.integerDef.createInteger(storageSize));
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
