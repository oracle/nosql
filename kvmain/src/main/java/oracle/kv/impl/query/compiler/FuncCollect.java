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
import oracle.kv.impl.query.runtime.FuncCollectIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

class FuncCollect extends Function {

    final boolean theDistinct;

    FuncCollect(boolean distinct) {
        super(FuncCode.FN_ARRAY_COLLECT,
              (distinct ? "array_collect_distinct" : "array_collect"),
              TypeManager.ANY_STAR(),
              TypeManager.ANY_ARRAY_ONE); /* RetType */
        theDistinct = distinct;
    }

    boolean isDistinct() {
        return theDistinct;
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {

        ExprType inType = caller.getInput().getType();
        return TypeManager.createArrayType(inType, Quantifier.ONE);
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return false;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    boolean isAggregate() {
        return true;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        return new FuncCollectIter(caller, resultReg,
                                   caller.getType().getDef(),
                                   theDistinct,
                                   argIters[0],
                                   codegen.isForCloud());
    }
}
