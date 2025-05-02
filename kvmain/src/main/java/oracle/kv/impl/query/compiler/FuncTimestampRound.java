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

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncTimestampRoundIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * timestamp(0)? timestamp_ceil(any? timestamp [, any? unit])
 * timestamp(0)? timestamp_floor(any? timestamp [, any? unit])
 * timestamp(0)? timestamp_round(any? timestamp [, any? unit])
 * timestamp(0)? timestamp_trunc(any? timestamp [, any? unit])
 *
 * Function to round the given timestamp to the unit, the unit can be single
 * unit or arbitrary interval "<n> unit". If not specified, default unit is DAY.
 */
public class FuncTimestampRound extends Function {

    private static ArrayList<ExprType> paramTypes;
    static {
        paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_QSTN());
        paramTypes.add(TypeManager.ANY_QSTN());
    }

    // variable number of arguments
    FuncTimestampRound(FuncCode code, String name) {
        super(code,
              name,
              paramTypes,
              TypeManager.TIMESTAMP_QSTN(0),
              true); /* can have 1 or 2 parameters */
    }

    @Override
    boolean isIndexable() {
        return true;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return true;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnEmpty();
    }

    @Override
    PlanIter codegen(CodeGenerator codegen, ExprFuncCall funcCall,
                     PlanIter[] argIters) {
        int resultReg = codegen.allocateResultReg(funcCall);
        assert argIters != null;
        return new FuncTimestampRoundIter(funcCall, resultReg, theCode,
                                          argIters[0],
                                          (argIters.length > 1 ?
                                               argIters[1] : null));
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {
        int numArgs = fncall.getNumArgs();
        if (numArgs == 0 || numArgs > 2) {
            throw new QueryException(
                "The number of parameters specified for the " +
                fncall.getFunction().getName() + "() function is invalid, " +
                "it may have 1 or 2 parameters");
        }
        return super.normalizeCall(fncall);
    }
}
