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
import oracle.kv.impl.query.runtime.FuncGeoIsGeometryIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/*
 * boolean geo_is_geometry(any*)
 *
 * Returns NULL if the operand returns NULL.
 * Returns false if the operand returns zero or more than 1 items.
 */
public class FuncGeoIsGeometry extends Function {

    FuncGeoIsGeometry() {
        super(FuncCode.FN_GEO_IS_GEOMETRY,
              "geo_is_geometry",
              TypeManager.ANY_STAR(),
              TypeManager.BOOLEAN_ONE());
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall fncall) {
        return fncall.getArg(0).mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall fncall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(fncall);
        return new FuncGeoIsGeometryIter(fncall, resultReg, argIters);
    }
}
