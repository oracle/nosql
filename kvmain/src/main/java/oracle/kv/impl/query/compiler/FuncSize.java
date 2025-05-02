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

import oracle.kv.impl.query.types.TypeManager;

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.FuncSizeIter;


/*
 * Integer? size(x ANY?)
 *
 * The function returns the size (i.e., number of entries) of an array, map, or
 * record. Although the param type is ANY? the runtime implementation of the 
 * function accepts complex values only.
 */
class FuncSize extends Function {

    FuncSize() {
        super(
            FuncCode.FN_SIZE, "size",
            TypeManager.ANY_QSTN(),
            TypeManager.INT_QSTN()); /* RetType */
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return  caller.getArg(0).mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnEmpty();
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        return new FuncSizeIter(caller, resultReg, argIters[0],
                                codegen.isForCloud());
    }
}
