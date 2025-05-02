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

import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.IsNullIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Represents as IS NULL and IS NOT NULL perateros, which test whether a scalar
 * expr returns the SQL NULL.
 */
class FuncIsNull extends Function {

    FuncIsNull(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_QSTN(),
              TypeManager.BOOLEAN_ONE() /*retType*/);
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        QueryControlBlock qcb = fncall.getQCB();
        StaticContext sctx = fncall.getSctx();
        Location loc = fncall.getLocation();

        Expr arg = fncall.getArg(0);

        if (!arg.mayReturnNULL() && theCode == FuncCode.OP_IS_NULL) {
            return new ExprConst(qcb, sctx, loc, false);
        }

        if (arg.getKind() == ExprKind.CONST) {

            FieldValueImpl val = ((ExprConst)arg).getValue();

            if (theCode == FuncCode.OP_IS_NULL) {
                return new ExprConst(qcb, sctx, loc, val.isNull());
            }
            
            return new ExprConst(qcb, sctx, loc, !val.isNull());
        }

        return fncall;
    }

    @Override
    boolean isValueComparison() {
        return true;
    }

    @Override
    public boolean mayReturnNULL(ExprFuncCall caller) {
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

        int resultReg = codegen.allocateResultReg(caller);
        return new IsNullIter(caller, resultReg, theCode, argIters[0]);
    }

}
