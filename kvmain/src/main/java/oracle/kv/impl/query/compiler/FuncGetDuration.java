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

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.query.runtime.FuncGetDurationIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * string? get_duration(any? milliseconds)
 *
 * Converts the number of milliseconds to a duration string, the string contains
 * UNIT from DAY to NANOSECONDS. If argument milliseconds is null, return null.
 *
 * Example:
 *   select get_duration(timestamp_diff(deliver_time, order_time)) as t from foo
 *   -&gt;
 *   {"t":"1 day 6 hours"}
 */
public class FuncGetDuration extends Function {

    FuncGetDuration() {
        super(FunctionLib.FuncCode.FN_GET_DURATION,
              "get_duration",
              TypeManager.ANY_QSTN(),
              TypeManager.STRING_ONE(),
              false);
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {
        Expr strCastExpr = ExprCast.create(fncall.getQCB(), fncall.getSctx(),
                                           fncall.getLocation(),
                                           fncall.getArg(0),
                                           FieldDefImpl.Constants.longDef,
                                           ExprType.Quantifier.QSTN);
        fncall.setArg(0, strCastExpr, false);

        return fncall;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return (caller.getArg(0).mayReturnNULL());
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);
        return new FuncGetDurationIter(funcCall, resultReg, argIters[0]);
    }
}
