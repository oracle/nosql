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
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncTimestampAddIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * timestamp(9)? timestamp_add(any? timestamp, any? duration)
 *
 * The timestamp_add function adds a duration to a TIMESTAMP value and return
 * the TIMESTAMP.
 *
 * Arguments:
 *   - timestamp the TIMESTAMP value.
 *   - duration the duration to add. The duration string is in format of
 *     [-](<n> UNIT)+.
 *       o The <n> is positive integer or zero.
 *       o The temporal UNIT can be YEAR, MONTH, DAY, HOUR, MINUTE, SECOND,
 *         MILLISECOND, NANOSECOND or its plural form, it is case-insensitive.
 *         e.g. 1 year 6 months 15 days
 *       o The temporal UNIT should be specified from larger unit to smaller
 *         unit.
 *       o The duration can be positive and negative. The leading minus '-'
 *         indicates the duration is negative. e.g. - 12 hours
 *
 * Return:
 *   Return timestamp(9), or null if timestamp or duration is null.
 *
 * Example:
 *   select timestamp_add(order_time, '1 day') as P1D from foo
 *   -&gt;
 *   {"P1D":"2021-12-01T21:43:32.972000000Z"}
 *
 */
public class FuncTimestampAdd extends Function {
    FuncTimestampAdd() {
        super(FuncCode.FN_TIMESTAMP_ADD,
              "timestamp_add",
              TypeManager.ANY_QSTN(),
              TypeManager.STRING_QSTN(),
              TypeManager.TIMESTAMP_ONE());
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        Expr strCastExpr = ExprCast.create(fncall.getQCB(), fncall.getSctx(),
                                           fncall.getLocation(),
                                           fncall.getArg(0),
                                           FieldDefImpl.Constants.timestampDef,
                                           ExprType.Quantifier.QSTN);
        fncall.setArg(0, strCastExpr, false);

        return fncall;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnNULL() ||
               caller.getArg(1).mayReturnNULL();
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
        return new FuncTimestampAddIter(funcCall, resultReg,
                                        argIters[0], argIters[1]);
    }
}
