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
import oracle.kv.impl.query.runtime.ConcatIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Represents the concatanation of the results of an arbitrary number of
 * input exprs. The results from each input may have different data types.
 *
 * For now, this is an internal function, and is only used to represent the
 * "empty" expr: an expr that returns nothing. The empty expr is used, for
 * example, when the compiler that the WHERE condition of a SFW is always
 * false; in this case, the whole SFW expr is replaced with an empty expr.
 */
class FuncConcat extends Function {

    FuncConcat() {
        super(
            FuncCode.FN_SEQ_CONCAT, "seq_concat",
            TypeManager.ANY_STAR(),
            TypeManager.ANY_STAR() /*retType*/,
            true /*isVariadic*/);
    }


    @Override
    ExprType getRetType(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        if (numArgs == 0) {
            return TypeManager.EMPTY();
        }

        ExprType type = caller.getArg(0).getType();

        for (int i = 1; i < numArgs; ++i) {
            type = TypeManager.getConcatType(type, caller.getArg(i).getType());
        }

        return type;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        if (numArgs == 0) {
            return true;
        }

        for (int i = 0; i < numArgs; ++i) {
            if (caller.getArg(i).mayReturnNULL()) {
                return true;
            }
        }

        return false;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        if (numArgs == 0) {
            return true;
        }

        for (int i = 0; i < numArgs; i++) {
            if (!caller.getArg(i).mayReturnEmpty()) {
                return false;
            }
        }

        return true;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        return new ConcatIter(caller, resultReg, argIters);
    }
}
