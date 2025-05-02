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
import oracle.kv.impl.query.runtime.FuncCountIter;


/*
 * long count(any*)
 *
 * long count_numbers(any*)
 */
class FuncCount extends Function {

    FuncCount(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_STAR(),
              TypeManager.LONG_ONE()); /* RetType */
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

        return new FuncCountIter(caller, resultReg, argIters[0], theCode);
    }
}
