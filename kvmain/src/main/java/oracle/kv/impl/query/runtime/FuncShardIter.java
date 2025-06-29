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
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.query.compiler.Expr;

/**
 *
 */
public class FuncShardIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncShardIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInput = input;
    }

    FuncShardIter(DataInput in, short serialVersion)
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
        return PlanIterKind.FUNC_SHARD;
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

        int shard;

        if (rcb.isServerRCB()) {
            shard = rcb.getShardId();
        } else {
            FieldValueImpl row = rcb.getRegVal(theInput.getResultReg());
            if (row.isJsonRowMap()) {
                shard = row.asJsonRowMap().getShardId();
            } else {
                shard = ((RowImpl)row).getShard();
            }
        }

        if (rcb.getTraceLevel() > 3) {
            rcb.trace("row shard  = " + shard);
        }

        rcb.setRegVal(theResultReg,
                      FieldDefImpl.Constants.integerDef.createInteger(shard));
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
