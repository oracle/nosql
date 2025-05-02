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

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.NotIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Represents the logical operator NOT. 
 *
 * Returns NULL for NULL input.
 * Returns TRUE for empty input.
 */
class FuncNot extends Function {

    FuncNot() {
        super(
            FuncCode.OP_NOT, "NOT",
            TypeManager.BOOLEAN_QSTN(),
            TypeManager.BOOLEAN_ONE() /*retType*/);
    }

    @Override
    public boolean mayReturnNULL(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    /*
     * Push the NOT op into its input expr, when possible.
     */
    @Override
    Expr normalizeCall(ExprFuncCall notExpr) {

        FunctionLib lib = CompilerAPI.getFuncLib();
        Function argFunc =  notExpr.getArg(0).getFunction(null);

        if (argFunc == null ) {
            return notExpr;
        }

        ExprFuncCall arg = (ExprFuncCall)notExpr.getArg(0);
        FuncCode argCode = argFunc.getCode();
        Function newFunc;
        ArrayList<Expr> newArgs;

        switch (argCode) {
        case OP_EXISTS:
            newFunc = lib.getFunc(FuncCode.OP_NOT_EXISTS);
            newArgs = new ArrayList<Expr>(1);
            newArgs.add(arg.getInput());
            break;

        case OP_NOT_EXISTS:
            newFunc = lib.getFunc(FuncCode.OP_EXISTS);
            newArgs = new ArrayList<Expr>(1);
            newArgs.add(arg.getInput());
            break;

        case OP_IS_NULL:
            newFunc = lib.getFunc(FuncCode.OP_IS_NOT_NULL);
            newArgs = new ArrayList<Expr>(1);
            newArgs.add(arg.getInput());
            break;

        case OP_IS_NOT_NULL:
            newFunc = lib.getFunc(FuncCode.OP_IS_NULL);
            newArgs = new ArrayList<Expr>(1);
            newArgs.add(arg.getInput());
            break;

        case OP_EQ:
        case OP_NEQ:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE:
            newFunc = lib.getFunc(FuncCompOp.getNegationOp(argCode));
            newArgs = new ArrayList<Expr>(2);
            newArgs.add(arg.getArg(0));
            newArgs.add(arg.getArg(1));
            break;

        case OP_AND:
        case OP_OR:
            int numArgs = arg.getNumArgs();
            newArgs = new ArrayList<Expr>(numArgs);

            newFunc = lib.getFunc(argCode == FuncCode.OP_OR ?
                                  FuncCode.OP_AND : FuncCode.OP_OR);

            for (int i = 0; i < numArgs; ++i) {
                Expr newArg = ExprFuncCall.create(notExpr.getQCB(),
                                                  notExpr.getSctx(),
                                                  notExpr.getLocation(),
                                                  this, arg.getArg(i));
                newArgs.add(newArg);
            }

            break;

        default:
            return notExpr;
        }

        Expr res = ExprFuncCall.create(notExpr.getQCB(),
                                       notExpr.getSctx(),
                                       notExpr.getLocation(),
                                       newFunc, newArgs);

        notExpr.replace(res, true/*destroy*/);
        return res;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        assert(argIters.length == 1);
        int resultReg = codegen.allocateResultReg(caller);
        return new NotIter(caller, resultReg, theCode, argIters[0]);
    }

}
