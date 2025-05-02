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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.Geometry;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.Expr.ConstKind;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.Function;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

/*
 * boolean geo_interacts(any*, any*)
 *
 * boolean geo_is_inside(any*, any*)
 *
 * boolean geo_within_distance(any*, any*, double)
 *
 * boolean geo_near(any*, any*, double)
 *
 * Returns NULL if any operand returns NULL.
 * Returns false if any operand returns zero or more than 1 items.
 * Returns false if any of the operands is not a geometry
 * For geo_is_inside, the 2nd operand should be a polygon; return false if not.
 * For geo_within_distance and geo_near, the 2nd operand should be a point;
 * return false if not.
 */
public class FuncGeoSearchIter extends PlanIter {

    static private class FuncGeoSearchState extends PlanIterState {

        Geometry theLeftGeom;

        Geometry theRightGeom;

        Geometry theBufferedGeom;

        StringBuilder sb = new StringBuilder(256);
    }

    private final FuncCode theCode;

    private final PlanIter theLeftOp;

    private final PlanIter theRightOp;

    private final PlanIter theDistanceOp;

    private final boolean theIsLeftConst;

    private final boolean theIsRightConst;

    private final boolean theIsDistanceConst;

    public FuncGeoSearchIter(
        Expr e,
        FuncCode code,
        int resultReg,
        PlanIter[] argIters) {

        super(e, resultReg);
        theCode = code;
        theLeftOp = argIters[0];
        theRightOp = argIters[1];

        ExprFuncCall fncall = (ExprFuncCall)e;
        Expr larg = fncall.getArg(0);
        Expr rarg = fncall.getArg(1);
        theIsLeftConst = ConstKind.isConst(larg);
        theIsRightConst = ConstKind.isConst(rarg);

        if (theCode == FuncCode.FN_GEO_WITHIN_DISTANCE) {
            theDistanceOp = argIters[2];
            theIsDistanceConst = ConstKind.isConst(fncall.getArg(2));
        } else {
            theDistanceOp = null;
            theIsDistanceConst = false;
        }
    }

    FuncGeoSearchIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FuncCode.VALUES_COUNT);
        theCode = FuncCode.valueOf(ordinal);
        theLeftOp = deserializeIter(in, serialVersion);
        theRightOp = deserializeIter(in, serialVersion);
        theDistanceOp = deserializeIter(in, serialVersion);
        theIsLeftConst = in.readBoolean();
        theIsRightConst = in.readBoolean();
        theIsDistanceConst = in.readBoolean();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIter(theLeftOp, out, serialVersion);
        serializeIter(theRightOp, out, serialVersion);
        serializeIter(theDistanceOp, out, serialVersion);
        out.writeBoolean(theIsLeftConst);
        out.writeBoolean(theIsRightConst);
        out.writeBoolean(theIsDistanceConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.GEO_SEARCH;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncGeoSearchState());

        theLeftOp.open(rcb);
        theRightOp.open(rcb);
        if (theDistanceOp != null) {
            theDistanceOp.open(rcb);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theLeftOp.reset(rcb);
        theRightOp.reset(rcb);
        if (theDistanceOp != null) {
            theDistanceOp.reset(rcb);
        }
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
        if (theDistanceOp != null) {
            theDistanceOp.close(rcb);
        }
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FuncGeoSearchState state = (FuncGeoSearchState)
            rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        Geometry lgeom = null;
        Geometry rgeom = null;
        Geometry bgeom = null;
        FieldValueImpl lvalue = null;
        FieldValueImpl rvalue = null;
        boolean haveNULL = false;
        boolean notSingleInput = false;

        if (state.theLeftGeom == null) {
        
            boolean leftOpNext = theLeftOp.next(rcb);

            if (leftOpNext) {
                lvalue = rcb.getRegVal(theLeftOp.getResultReg());

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
                rvalue = rcb.getRegVal(theRightOp.getResultReg());

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
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        if (haveNULL) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        if (rgeom == null) {
            Function func = CompilerAPI.getFuncLib().getFunc(theCode);
            throw new QueryException(
                "The right operand of the " + func.getName() +
                " function is not a valid geometry.",
                theRightOp.getLocation());
        }

        if (lgeom == null) {
            rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            state.done();
            return true;
        }

        if (state.theBufferedGeom == null &&
            theCode == FuncCode.FN_GEO_WITHIN_DISTANCE) {

            theDistanceOp.next(rcb);

            double dist = ((DoubleValueImpl)
                           rcb.getRegVal(theDistanceOp.getResultReg())).get();

            bgeom = (dist <= 0 ? rgeom : rgeom.buffer(dist, theLocation));

            if (theIsDistanceConst) {
                state.theBufferedGeom = bgeom;
            }
        } else {
            bgeom = state.theBufferedGeom;
        }

        boolean res;

        switch (theCode) {
        case FN_GEO_INTERSECT :
            res = lgeom.interact(rgeom, theLocation);
            break;

        case FN_GEO_INSIDE:
            if (!rgeom.isPolygon()) {
                res = false;
            } else {
                res = lgeom.inside(rgeom, theLocation);
            }
            break;

        case FN_GEO_WITHIN_DISTANCE:
            res = lgeom.interact(bgeom, theLocation);

            if (rcb.getTraceLevel() >= 2 && !res) {
                double dist = lgeom.distance(rgeom, theLocation);
                rcb.trace("Distance = " + dist + "\nBuffered Geometry = " +
                          bgeom.toGeoJson());
            }
            
            break;
        default:
            throw new QueryStateException(
               "Unexpected geo search function " + theCode);
        }

        if (rcb.getTraceLevel() >= 2 && !res) {
            rcb.trace(theCode + ": Eliminated false positive for geom: " +
                      lvalue);
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

        formatter.indent(sb);
        sb.append("\"search target iterator\" :\n");
        theLeftOp.display(sb, formatter, verbose);
        sb.append(",\n");

        formatter.indent(sb);
        sb.append("\"search geometry iterator\" :\n");
        theRightOp.display(sb, formatter, verbose);

        if (theDistanceOp != null) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"distance iterator\" :\n");
            theDistanceOp.display(sb, formatter, verbose);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncGeoSearchIter)) {
            return false;
        }
        final FuncGeoSearchIter other = (FuncGeoSearchIter) obj;
        return (theCode == other.theCode) &&
            Objects.equals(theLeftOp, other.theLeftOp) &&
            Objects.equals(theRightOp, other.theRightOp) &&
            Objects.equals(theDistanceOp, other.theDistanceOp) &&
            (theIsLeftConst == other.theIsLeftConst) &&
            (theIsRightConst == other.theIsRightConst) &&
            (theIsDistanceConst == other.theIsDistanceConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theCode,
                            theLeftOp,
                            theRightOp,
                            theDistanceOp,
                            theIsLeftConst,
                            theIsRightConst,
                            theIsDistanceConst);
    }
}
