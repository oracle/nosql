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
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;

import oracle.kv.impl.api.table.Geometry;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.Expr.ConstKind;
import oracle.kv.impl.query.compiler.ExprFuncCall;

/*
 * double geo_distance(any*, any*)
 *
 * Returns NULL if any operand returns NULL.
 * Returns -1 if any operand returns zero or more than 1 items.
 * Returns -1 if any of the operands is not a geometry
 */
public class FuncGeoDistanceIter extends PlanIter {

    static private class FuncGeoDistanceState extends PlanIterState {

        Geometry theLeftGeom;

        Geometry theRightGeom;

        StringBuilder sb = new StringBuilder(256);
    }

    private final static DoubleValueImpl theMinusOne =
        FieldDefImpl.Constants.doubleDef.createDouble(-1);

    private final PlanIter theLeftOp;

    private final PlanIter theRightOp;

    private final boolean theIsLeftConst;

    private final boolean theIsRightConst;

    public FuncGeoDistanceIter(
        Expr e,
        int resultReg,
        PlanIter[] argIters) {

        super(e, resultReg);
        theLeftOp = argIters[0];
        theRightOp = argIters[1];

        ExprFuncCall fncall = (ExprFuncCall)e;
        Expr larg = fncall.getArg(0);
        Expr rarg = fncall.getArg(1);
        theIsLeftConst = ConstKind.isConst(larg);
        theIsRightConst = ConstKind.isConst(rarg);
    }

    FuncGeoDistanceIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theLeftOp = deserializeIter(in, serialVersion);
        theRightOp = deserializeIter(in, serialVersion);
        theIsLeftConst = in.readBoolean();
        theIsRightConst = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theLeftOp, out, serialVersion);
        serializeIter(theRightOp, out, serialVersion);
        out.writeBoolean(theIsLeftConst);
        out.writeBoolean(theIsRightConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.GEO_DISTANCE;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncGeoDistanceState());

        theLeftOp.open(rcb);
        theRightOp.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theLeftOp.reset(rcb);
        theRightOp.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theLeftOp.close(rcb);
        theRightOp.close(rcb);
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FuncGeoDistanceState state = (FuncGeoDistanceState)
            rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        Geometry lgeom = null;
        Geometry rgeom = null;
        boolean haveNULL = false;
        boolean notSingleInput = false;

        if (state.theLeftGeom == null) {
        
            boolean leftOpNext = theLeftOp.next(rcb);

            if (leftOpNext) {
                FieldValueImpl lvalue = rcb.getRegVal(theLeftOp.getResultReg());

                if (lvalue.isNull()) {
                    haveNULL = true;
                } else {
                    lgeom = CompilerAPI.getGeoUtils().
                            castAsGeometry(lvalue, state.sb);

                    if (theLeftOp.next(rcb)) {
                        notSingleInput = true;
                    }

                    if (theIsLeftConst) {
                        state.theLeftGeom = lgeom;
                    }
                }
            } else {
                notSingleInput = true;
            }
        } else {
            lgeom = state.theLeftGeom;
        }

        if (state.theRightGeom == null && !notSingleInput) {

            boolean rightOpNext = theRightOp.next(rcb);

            if (rightOpNext) {
                FieldValueImpl rvalue = rcb.getRegVal(theRightOp.getResultReg());

                if (rvalue.isNull()) {
                    haveNULL = true;
                } else {
                    rgeom = CompilerAPI.getGeoUtils().
                            castAsGeometry(rvalue, state.sb);

                    if (theRightOp.next(rcb)) {
                        notSingleInput = true;
                    }

                    if (theIsRightConst) {
                        state.theRightGeom = rgeom;
                    }
                }
            } else {
                notSingleInput = true;
            }
        } else {
            rgeom = state.theRightGeom;
        }

        if (notSingleInput) {
            rcb.setRegVal(theResultReg, theMinusOne);
            state.done();
            return true;
        }

        if (haveNULL) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        if (lgeom == null || rgeom == null) {
            rcb.setRegVal(theResultReg, theMinusOne);
            state.done();
            return true;
        }

        double res = lgeom.distance(rgeom, theLocation);

        rcb.setRegVal(theResultReg,
                      FieldDefImpl.Constants.doubleDef.createDouble(res));

        state.done();
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"first geometry iterator\" :\n");
        theLeftOp.display(sb, formatter, verbose);
        sb.append(",\n");

        formatter.indent(sb);
        sb.append("\"second geometry iterator\" :\n");
        theRightOp.display(sb, formatter, verbose);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncGeoDistanceIter)) {
            return false;
        }
        final FuncGeoDistanceIter other = (FuncGeoDistanceIter) obj;
        return Objects.equals(theLeftOp, other.theLeftOp) &&
            Objects.equals(theRightOp, other.theRightOp) &&
            (theIsLeftConst == other.theIsLeftConst) &&
            (theIsRightConst == other.theIsRightConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theLeftOp,
                            theRightOp,
                            theIsLeftConst,
                            theIsRightConst);
    }
}
