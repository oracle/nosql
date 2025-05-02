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

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncGeoDistanceIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/*
 * double geo_distance(any*, any*)
 *
 * Returns NULL if any operand returns NULL.
 * Returns -1 if any operand returns zero or more than 1 items.
 * Returns -1 if any of the operands is not a geometry
 */
public class FuncGeoDistance extends Function {

    FuncGeoDistance() {
        super(FuncCode.FN_GEO_DISTANCE,
              "geo_distance",
              TypeManager.ANY_STAR(),
              TypeManager.ANY_STAR(),
              TypeManager.DOUBLE_ONE());
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall fncall) {

        return (fncall.getArg(0).mayReturnNULL() ||
                fncall.getArg(1).mayReturnNULL());
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        FuncGeoSearch.normalizeArg(fncall, 0);
        FuncGeoSearch.normalizeArg(fncall, 1);

        return fncall;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall fncall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(fncall);

        return new FuncGeoDistanceIter(fncall, resultReg, argIters);
    }
}
