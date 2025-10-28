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
import oracle.kv.table.FieldValue;
import oracle.kv.table.FieldValueFactory;

/**
 * Iterator for row_metadata() function
 */
public class FuncRowMetadataIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncRowMetadataIter(Expr e, int resultReg, PlanIter input) {
        super(e, resultReg);
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncRowMetadataIter(DataInput in, short serialVersion) throws IOException {
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
        return PlanIterKind.FUNC_ROW_METADATA;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theInput.open(rcb);
    }

    @SuppressWarnings("static-access")
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

        /* row may be null in case it is a row of a non-target table in a
         * join query*/
        if (row == NullValueImpl.getInstance()) {
            rcb.setRegVal(theResultReg, row);
            state.done();
            return true;
        }

        String rowMetadata = null;

        if (row.isTuple()) {
            rowMetadata = ((TupleValue)row).getRowMetadata();
        } else if (row.isRecord()) {
            rowMetadata = ((RowImpl)row).getRowMetadata();
        }  else if (row.isJsonRowMap()) {
            rowMetadata = row.asJsonRowMap().getJColl().getRowMetadata();
        } else {
            throw new QueryException(
                "Input to the row_metadata() function is not a row",
                getLocation());
        }

        FieldValue fv;
        if (rowMetadata == null) {
            fv = FieldValueFactory.createJsonNull();
        } else {
            fv = FieldDefImpl.Constants.jsonDef.createValueFromString(
                rowMetadata, FieldDefImpl.Constants.jsonDef);
        }

        rcb.setRegVal(theResultReg, (FieldValueImpl) fv);
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