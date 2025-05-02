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
import oracle.kv.impl.query.runtime.FuncTimestampBucketIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * timestamp? timestamp_bucket(any? timestamp [, any? interval[, any? origin]])
 *
 * To returns the beginning of the current bucket that contains the given
 * timestamp.
 *
 * Parameters:
 *   o The 'timestamp' is the input timestamp, it can be any type castable to
 *     TIMESTAMP type.
 *
 *   o The 'interval' is in format of N <unit>, the <unit> can be WEEK, DAY,
 *     HOUR, MINUTE or SECOND and or its plural format. Optional, defaults
 *     to the '1 DAY'.
 *
 *   o The 'origin' is the timestamp that the buckets are aligned to, it can be
 *     any type castable to TIMESTAMP type. Optional, defaults to Unix epoch
 *     1970-01-01T00:00:00.
 *
 * Return value:
 *   A TIMESTAMP type which is beginning of bucket that contains the given
 *   timestamp.
 */
public class FuncTimestampBucket extends Function {

    private static ArrayList<ExprType> paramTypes;
    static {
        paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_QSTN());
        paramTypes.add(TypeManager.ANY_QSTN());
        paramTypes.add(TypeManager.ANY_QSTN());
    }

    // variable number of arguments
    FuncTimestampBucket(FuncCode code, String name) {
        super(code,
              name,
              paramTypes,
              TypeManager.TIMESTAMP_QSTN(9),
              true); /* can have 1 ~ 3 parameters */
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
        return new FuncTimestampBucketIter(funcCall, resultReg,
                                           argIters[0],
                                           (argIters.length > 1 ?
                                               argIters[1] : null),
                                           (argIters.length > 2 ?
                                               argIters[2] : null));
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {
        int numArgs = fncall.getNumArgs();
        if (numArgs < 1 || numArgs > 3) {
            throw new QueryException(
                "The number of parameters specified for the " +
                fncall.getFunction().getName() + "() function is invalid, " +
                "it may have 1 ~ 3 parameters");
        }
        return super.normalizeCall(fncall);
    }
}
