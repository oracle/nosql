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
import oracle.kv.impl.query.runtime.FuncTimestampDiffIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * long? timestamp_diff(any? timestamp1, any? timestamp2)
 *
 * The timestamp_diff() function calculates the number of milliseconds between
 * two timestamp values, the result of timestamp1 - timestamp2.
 *
 * Arguments:
 *   - timestamp1, timestamp2 the two TIMESTAMPs to calculate the number of
 *     milliseconds between (timestamp1 - timestamp2).
 *
 * Return:
 *   Return a long value or null if timestamp1 or timestamp2 is null,
 *
 * Example:
 *   select timestamp_diff(deliver_time, order_time) as millis from foo;
 *   -&gt;
 *   {"millis":11234668}
 */
public class FuncTimestampDiff extends Function {
    FuncTimestampDiff() {
        super(FuncCode.FN_TIMESTAMP_DIFF,
              "timestamp_diff",
              TypeManager.ANY_QSTN(),
              TypeManager.ANY_QSTN(),
              TypeManager.LONG_ONE());
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        Expr strCastExpr = ExprCast.create(fncall.getQCB(), fncall.getSctx(),
                                           fncall.getLocation(),
                                           fncall.getArg(0),
                                           FieldDefImpl.Constants.timestampDef,
                                           ExprType.Quantifier.QSTN);
        fncall.setArg(0, strCastExpr, false);

        Expr strCastExpr2 =
            ExprCast.create(fncall.getQCB(), fncall.getSctx(),
                            fncall.getLocation(),
                            fncall.getArg(1),
                            FieldDefImpl.Constants.timestampDef,
                            ExprType.Quantifier.QSTN);
        fncall.setArg(1, strCastExpr2, false);

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
        return new FuncTimestampDiffIter(funcCall, resultReg,
                                         argIters[0], argIters[1]);
    }
}
