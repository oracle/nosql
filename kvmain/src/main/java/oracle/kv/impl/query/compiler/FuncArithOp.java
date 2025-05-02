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

import java.util.Arrays;
import java.util.ArrayList;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.ArithOpIter;
import oracle.kv.impl.query.runtime.ConstIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.ExprType.TypeCode;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Arithmetic operations + - * / function implementation.
 *
 * Note: the last argument must be a constant String that contains the order
 * of the operations.
 */
public class FuncArithOp extends Function {

    FuncArithOp(FunctionLib.FuncCode code, String name) {
        super(code,
              name,
              TypeManager.ANY_STAR() /* params */,
              TypeManager.ANY_STAR() /* RetType */,
              true /*isVariadic*/);
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);

        // last arg should be a const string
        assert argIters != null && argIters.length >= 3;
        assert argIters[argIters.length - 1] instanceof ConstIter;
        assert ((ConstIter)(argIters[argIters.length - 1])).getValue().isString();

        String ops = ((ConstIter)
                      (argIters[argIters.length - 1])).getValue().castAsString();

        PlanIter[] newArgIters = Arrays.copyOf(argIters, argIters.length - 1);

        return new ArithOpIter(funcCall, resultReg, theCode, newArgIters, ops,
                               codegen.isForCloud());
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {

        Quantifier quantifier = Quantifier.ONE;
        int numArgs = caller.getNumArgs();

        // last argument is the string const with the operations order
        assert caller.getArg(numArgs - 1).getType().getDef().isString();

        ExprConst opsExpr = (ExprConst)caller.getArg(numArgs - 1);
        String  ops = opsExpr.getValue().getString();

        TypeCode typeCode = (ops.contains("d") ? TypeCode.DOUBLE : TypeCode.INT);

        /*
         * Determine the TypeCode of the result for the expression by iterating
         * its components, enforcing the promotion rules for numeric types.
         */
        for (int i = 0; i < numArgs - 1; i++) {

            ExprType argType = caller.getArg(i).getType();

            switch (argType.getQuantifier()) {
            case ONE:
            case PLUS:
                break;
            case QSTN:
            case STAR:
                quantifier = ExprType.Quantifier.QSTN;
                break;
            default:
                throw new QueryStateException(
                    "Unknown Quantifier: " + argType.getQuantifier());
            }

            if (typeCode == TypeCode.ANY_ATOMIC ||
                typeCode == TypeCode.ANY_JSON_ATOMIC) {
                continue;
            }

            switch (argType.getCode()) {
            case INT:
                break;
            case LONG:
                if (typeCode == TypeCode.INT) {
                    typeCode = TypeCode.LONG;
                }
                break;
            case FLOAT:
                if (typeCode == TypeCode.INT || typeCode == TypeCode.LONG) {
                    typeCode = TypeCode.FLOAT;
                }
                break;
            case DOUBLE:
                if (typeCode == TypeCode.INT || typeCode == TypeCode.LONG ||
                    typeCode == TypeCode.FLOAT) {
                    typeCode = TypeCode.DOUBLE;
                }
                break;
            case NUMBER:
                typeCode = TypeCode.NUMBER;
                break;
            case ANY_JSON_ATOMIC:
            case JSON:
            case ANY_ATOMIC:
            case ANY:
                typeCode = TypeCode.ANY_JSON_ATOMIC;
                break;
            default:
                throw new QueryException(
                    "Operand in arithmetic operation has illegal type\n" +
                    "Operand : " + i + " type :\n" +
                    argType.getDef().getDDLString(),
                    caller.getLocation());
            }

        }

        return TypeManager.getBuiltinType(typeCode, quantifier);
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        for (int i = 0; i < numArgs - 1; i++) {
            if (caller.getArg(i).mayReturnNULL()) {
                return true;
            }
        }

        return false;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        for (int i = 0; i < numArgs - 1; i++) {
            if (caller.getArg(i).mayReturnEmpty()) {
                return true;
            }
        }

        return false;
    }

    static Expr createArithExpr(Expr arg1, Expr arg2, String op) {

        QueryControlBlock qcb = arg1.getQCB();
        StaticContext sctx = arg1.getSctx();
        Location loc = arg1.getLocation();

        Function func = CompilerAPI.getFuncLib().getFunc(FuncCode.OP_ADD_SUB);
        ArrayList<Expr> args = new ArrayList<Expr>(3);
        args.add(arg1);
        args.add(arg2);
        op = "+" + op;
        FieldValueImpl opStr =
            FieldDefImpl.Constants.stringDef.createString(op);
        args.add(new ExprConst(qcb, sctx, loc, opStr));

        return ExprFuncCall.create(qcb, sctx, loc, func, args);
    }
}
