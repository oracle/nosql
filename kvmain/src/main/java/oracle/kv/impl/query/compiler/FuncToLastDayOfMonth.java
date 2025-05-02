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
import oracle.kv.impl.query.runtime.FuncToLastDayOfMonthIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * timestamp(0)? to_last_day_of_month(any? timestamp)
 *
 * Function to get the last day of the month that contains the timestamp.
 */
public class FuncToLastDayOfMonth extends Function {

    FuncToLastDayOfMonth() {
        super(FuncCode.FN_LAST_DAY_OF_MONTH,
              "to_last_day_of_month",
              TypeManager.ANY_QSTN(),
              TypeManager.TIMESTAMP_QSTN(0));
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
    boolean isIndexable() {
        return true;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall funcCall,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);
        return new FuncToLastDayOfMonthIter(funcCall, resultReg, argIters[0]);
    }
}
