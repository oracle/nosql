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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.AndOrIter;
import oracle.kv.impl.query.runtime.ConstIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Represents the logical operators AND and OR.
 *
 * boolean AndOr(boolean?, ...)
 *
 * An empty operand is treated as false.
 * Returns NULL if any operand is NULL and either :
 * (a) it is AND and all the other operands are TRUE or NULL, or
 * (b) is is OR and all other operands are FALSE or NULL
 */
class FuncAndOr extends Function {

    FuncAndOr(FuncCode code, String name) {
        super(
            code, name,
            TypeManager.BOOLEAN_QSTN(),
            TypeManager.BOOLEAN_ONE() /*retType*/,
            true /*isVariadic*/);
    }

    @Override
    public boolean mayReturnNULL(ExprFuncCall caller) {
        for (Expr arg : caller.getArgs()) {
            if (arg.mayReturnNULL()) {
                return true;
            }
        }
        return false;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        if (argIters.length == 1) {
            return argIters[0]; 
        }

        int resultReg = codegen.allocateResultReg(caller);

        if (argIters.length == 0) {
            return new ConstIter(caller, resultReg, BooleanValueImpl.trueValue,
                                 false);
        }

        return new AndOrIter(caller, resultReg, theCode, argIters);
    }

}
