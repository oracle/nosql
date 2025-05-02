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
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.query.runtime.FuncSeqSortIter;
import oracle.kv.impl.query.runtime.PlanIter;

class FuncSeqSort extends Function {

    FuncSeqSort() {
        super(FuncCode.FN_SEQ_SORT, "seq_sort",
              TypeManager.ANY_STAR(),
              TypeManager.ANY_STAR(),
              true /*isVariadic*/); /* RetType */
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {
        return caller.getInput().getType();
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall fncall) {
        return fncall.getArg(0).mayReturnNULL();
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

        return new FuncSeqSortIter(caller, resultReg, argIters);
    }
}
