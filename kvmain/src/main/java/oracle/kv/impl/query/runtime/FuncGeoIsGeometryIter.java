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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;

import oracle.kv.impl.api.table.Geometry;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.Expr;

/*
 * boolean geo_is_geometry(any*)
 *
 * Returns NULL if the operand returns NULL.
 * Returns false if the operand returns zero or more than 1 items or a single.
 * item that is not a geometry
 */
public class FuncGeoIsGeometryIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncGeoIsGeometryIter(
        Expr e,
        int resultReg,
        PlanIter[] argIters) {

        super(e, resultReg);
        theInput = argIters[0];
    }

    FuncGeoIsGeometryIter(DataInput in, short serialVersion) throws IOException {
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
        return PlanIterKind.GEO_IS_GEOMETRY;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new PlanIterState());
        theInput.open(rcb);
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
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean res;

        boolean more = theInput.next(rcb);

        while (true) {

            if (!more) {
                res = false;
                break;
            }

            more = theInput.next(rcb);

            if (more) {
                res = false;
                break;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull()) {
                rcb.setRegVal(theResultReg, val);
                state.done();
                return true;
            }

            Geometry geom = CompilerAPI.getGeoUtils().castAsGeometry(val);

            res = (geom != null ? true : false);
            break;
        }

        if (res) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
        } else {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
        }

        state.done();
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theInput);
    }
}
