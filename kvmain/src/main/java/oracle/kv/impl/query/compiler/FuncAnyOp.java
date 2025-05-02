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

import oracle.kv.table.FieldDef.Type;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.AnyOpIter;


/*
 * TODO: check SQL comparison ops
 */
public class FuncAnyOp extends Function {

    FuncAnyOp(FuncCode code, String name) {
        super(
            code,
            name,
            TypeManager.ANY_STAR(), /* param1 */
            TypeManager.ANY_STAR(), /* param2 */
            TypeManager.BOOLEAN_ONE()); /* RetType */
    }

    @Override
    boolean isAnyComparison() {
        return true;
    }

    @Override
    public boolean mayReturnNULL(ExprFuncCall fncall) {
        return (fncall.getArg(0).mayReturnNULL() || 
                fncall.getArg(1).mayReturnNULL());
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        Expr op0 = fncall.getArg(0);
        Expr op1 = fncall.getArg(1);

        FieldDefImpl op0Def = op0.getType().getDef();
        FieldDefImpl op1Def = op1.getType().getDef();
        Type tc0 = op0Def.getType();
        Type tc1 = op1Def.getType();

        /*
         * Nothing to do if the operands have the same kind of type.
         */
        if (tc0 == tc1) {
            return fncall;
        }

        if (!TypeManager.areTypesComparable(op0.getType(), op1.getType())) {

            Location loc = fncall.getLocation();
            QueryControlBlock qcb = fncall.getQCB();
            StaticContext sctx = fncall.getSctx();

            if (tc0 == Type.TIMESTAMP) {

                if (tc1 == Type.STRING) {
                    int prec = ((TimestampDefImpl)op0Def).getPrecision();
                    Expr cast = ExprCast.create(
                        qcb, sctx, loc, op1,
                        FieldDefImpl.Constants.timestampDefs[prec],
                        op1.getType().getQuantifier());
                    fncall.setArg(1, cast, false);
                    return fncall;
                }

                if (tc1 == Type.JSON || tc1 == Type.ANY_JSON_ATOMIC) {
                    return fncall;
                }
            }

            if (tc1 == Type.TIMESTAMP) {

                if (tc0 == Type.STRING) {
                    int prec = ((TimestampDefImpl)op1Def).getPrecision();
                    Expr cast = ExprCast.create(
                        qcb, sctx, loc, op0,
                        FieldDefImpl.Constants.timestampDefs[prec],
                        op0.getType().getQuantifier());
                    fncall.setArg(0, cast, false);
                    return fncall;
                }

                if (tc0 == Type.JSON || tc0 == Type.ANY_JSON_ATOMIC) {
                    return fncall;
                }
            }

            return new ExprConst(fncall.getQCB(), fncall.getSctx(),
                                 fncall.getLocation(), false);
        }

        return FuncCompOp.handleConstOperand(fncall);
    }

    public static FuncCode anyToComp(FuncCode op) {

        switch (op) {
        case OP_GT_ANY:
            return FuncCode.OP_GT;
        case OP_GE_ANY:
            return FuncCode.OP_GE;
        case OP_LT_ANY:
            return FuncCode.OP_LT;
        case OP_LE_ANY:
            return FuncCode.OP_LE;
        case OP_EQ_ANY:
            return FuncCode.OP_EQ;
        case OP_NEQ_ANY:
            return FuncCode.OP_NEQ;
        default:
            assert(false);
            return null;
        }
    }

    static FuncCode swapCompOp(FuncCode op) {

        switch (op) {
        case OP_GT_ANY:
            return FuncCode.OP_LT_ANY;
        case OP_GE_ANY:
            return FuncCode.OP_LE_ANY;
        case OP_LT_ANY:
            return FuncCode.OP_GT_ANY;
        case OP_LE_ANY:
            return FuncCode.OP_GE_ANY;
        default:
            return op;
        }
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall fncall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(fncall);

        return new AnyOpIter(fncall, resultReg, theCode, argIters);
    }
}
