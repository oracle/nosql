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
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.query.compiler.Expr;

/**
 * long count(*)
 */
public class FuncCountStarIter extends PlanIter {

    static class FuncCountState extends PlanIterState {

        long theCount;

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCount = 0;
        }
    }

    public FuncCountStarIter(Expr e, int resultReg) {
        super(e, resultReg);
    }

    /**
     * FastExternalizable constructor.
     */
    FuncCountStarIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
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
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_COUNT_STAR;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FuncCountState());
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        FuncCountState state = (FuncCountState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FuncCountState state = (FuncCountState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        ++state.theCount;
        return true;
    }

    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {

        FuncCountState state = (FuncCountState)rcb.getState(theStatePos);
        state.theCount = (val != null ? ((LongValueImpl)val).get() : 0);

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Initialized count to " + state.theCount);
        }
    }

    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        FuncCountState state = (FuncCountState)rcb.getState(theStatePos);

        FieldValueImpl res =
            FieldDefImpl.Constants.longDef.createLong(state.theCount);

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
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncCountStarIter)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
