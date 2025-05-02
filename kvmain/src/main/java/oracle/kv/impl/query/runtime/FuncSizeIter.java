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

import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;

/**
 *
 */
public class FuncSizeIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncSizeIter(
        Expr e,
        int resultReg,
        PlanIter input,
        boolean forCloud) {
        super(e, resultReg, forCloud);
        theInput = input;
    }

    /**
     * FastExternalizable constructor.
     */
    FuncSizeIter(DataInput in, short serialVersion) throws IOException {
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
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);
        theInput.writeForCloud(out, driverVersion, valWriter);
    }

    @Override
    protected PlanIter getInput() {
        return theInput;
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_SIZE;
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

        int size;

        FieldValueImpl item = rcb.getRegVal(theInput.getResultReg());

        if (item.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        if (item.isArray()) {
            size = ((ArrayValueImpl)item).size();
        } else if (item.isMap()) {
            size = ((MapValueImpl)item).size();
        } else if (item.isRecord()) {
            size = ((RecordValueImpl)item).size();
        } else {
            throw new QueryException(
                "Input to the size() function has wrong type\n" +
                "Expected a complex item. Actual item type is:\n" +
                item.getDefinition(), getLocation());
        }

        FieldValueImpl res =
            FieldDefImpl.Constants.integerDef.createInteger(size);
        rcb.setRegVal(theResultReg, res);
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
