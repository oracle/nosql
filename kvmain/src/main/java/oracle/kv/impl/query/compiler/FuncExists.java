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
import oracle.kv.impl.query.runtime.ExistsIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/**
 * If the input expr retursn NULL, the result is true.
 */
class FuncExists extends Function {

    FuncExists(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_STAR(),
              TypeManager.BOOLEAN_ONE() /*retType*/);
    }

    @Override
    Expr normalizeCall(ExprFuncCall funcCall) {

        Expr arg = funcCall.getArg(0);
        Quantifier q = arg.getType().getQuantifier();

        if (q == Quantifier.ONE || q == Quantifier.PLUS) {

            if (theCode == FuncCode.OP_EXISTS) {
                return new ExprConst(funcCall.getQCB(),
                                     funcCall.getSctx(),
                                     funcCall.getLocation(),
                                     true);
            }

            return new ExprConst(funcCall.getQCB(),
                                 funcCall.getSctx(),
                                 funcCall.getLocation(),
                                 false);
        }

        return funcCall;
    }

    @Override
    public boolean mayReturnNULL(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnNULL();
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

        int resultReg = codegen.allocateResultReg(caller);
        return new ExistsIter(caller, resultReg, theCode, argIters[0]);
    }

}
