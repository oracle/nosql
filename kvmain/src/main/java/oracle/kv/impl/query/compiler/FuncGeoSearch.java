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

package oracle.kv.impl.query.compiler;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.Geometry;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr.ConstKind;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncGeoSearchIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

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
 */
public class FuncGeoSearch extends Function {

    FuncGeoSearch(FuncCode code, String name, ArrayList<ExprType> geoTypes) {
        super(code,
              name,
              geoTypes,
              TypeManager.BOOLEAN_ONE(),
              true/*isVariadic*/);
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall fncall) {

        return (fncall.getArg(0).mayReturnNULL() ||
                fncall.getArg(1).mayReturnNULL() ||
                (fncall.getNumArgs() > 2 &&
                 fncall.getArg(2).mayReturnNULL()));
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        int numargs = fncall.getNumArgs();

        if (theCode == FuncCode.FN_GEO_WITHIN_DISTANCE ||
            theCode == FuncCode.FN_GEO_NEAR) {

            if (numargs != 3) {
                throw new QueryException(
                    "Could not find function with name " + theName +
                    " and arity " + numargs, fncall.getLocation());
            }

            normalizeArg(fncall, 0);
            normalizeArg(fncall, 1);

            Expr radiusExpr = fncall.getArg(2);

            if (ConstKind.isCompileConst(radiusExpr) &&
                radiusExpr.getKind() != ExprKind.CONST) {

                QueryControlBlock qcb = fncall.getQCB();
                StaticContext sctx = fncall.getSctx();
                Location loc = radiusExpr.getLocation();

                List<FieldValueImpl> res = ExprUtils.computeConstExpr(radiusExpr);

                if (res.size() == 0) {
                    throw new QueryException(
                        "The distance operand of the geo_within_distance" +
                        "function is an empty sequence.", loc);
                }

                if (res.size() > 1) {
                    throw new QueryException(
                        "The distance operand of the geo_within_distance " +
                        "function is a sequence with more than one items.", loc);
                }

                radiusExpr = new ExprConst(qcb, sctx, loc, res.get(0));
                fncall.setArg(2, radiusExpr, true);
            }
        } else {

            if (numargs != 2) {
                throw new QueryException(
                    "Could not find function with name " + theName +
                    " and arity " + numargs, fncall.getLocation());
            }

            normalizeArg(fncall, 0);
            normalizeArg(fncall, 1);
        }

        return fncall;
    }

    static void normalizeArg(ExprFuncCall fncall, int i) {

        Function func = fncall.getFunction();
        FuncCode funcCode = func.getCode();
        String funcName = func.getName();
        String lr = (i == 0 ? "left" : "right");
        Expr arg = fncall.getArg(i);
        FieldDefImpl argDef = arg.getType().getDef();

        if (!argDef.mayBeJsonObject()) {
            throw new QueryException(
                "The " + lr +" operand of the " + funcName +
                " function is not a json object." +
                " arg type = " + argDef.getDDLString(),
                arg.getLocation());
        }
        
        if (ConstKind.isCompileConst(arg)) {

            FieldValueImpl argVal = null;

            if (arg.getKind() == ExprKind.CONST) {
                argVal = ((ExprConst)arg).getValue();
            } else {
                List<FieldValueImpl> res = ExprUtils.computeConstExpr(arg);

                if (res.size() == 0) {
                    throw new QueryException(
                        "The " + lr + " operand of the " + funcName +
                        " function is an empty sequence.",
                        arg.getLocation());
                }

                if (res.size() > 1) {
                    throw new QueryException(
                        "The " + lr + " operand of the " + funcName +
                        " function is a sequence with more than one items.",
                        arg.getLocation());
                }

                argVal = res.get(0);
            }

            Geometry geom = CompilerAPI.getGeoUtils().
                castAsGeometry(argVal);

            if (geom == null) {
                throw new QueryException(
                    "The " + lr + " operand of the " + funcName +
                    " function is not a valid geometry.",
                    arg.getLocation());
            }

            if (funcCode == FuncCode.FN_GEO_INSIDE &&
                i == 1 &&
                !geom.isPolygon()) {
                throw new QueryException(
                    "The second argument to the geo_inside " +
                    "function is not a polygon.", fncall.getLocation());
            }

            if (arg.getKind() != ExprKind.CONST) {
                QueryControlBlock qcb = fncall.getQCB();
                StaticContext sctx = fncall.getSctx();
                arg = new ExprConst(qcb, sctx, arg.getLocation(), argVal);
                fncall.setArg(i, arg, true);
            }
        }
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall fncall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(fncall);

        return new FuncGeoSearchIter(fncall, theCode, resultReg, argIters);
    }
}
