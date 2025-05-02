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

import oracle.kv.impl.api.table.DoubleDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FloatDefImpl;
import oracle.kv.impl.api.table.IntegerDefImpl;
import oracle.kv.impl.api.table.LongDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.ArithUnaryOpIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Arithmetic unary operations + - function implementation.
 *
 * Note: only - is implemented, + is skipped.
 */
public class FuncArithUnaryOp extends Function {

    FuncArithUnaryOp() {
        super(FuncCode.OP_ARITH_NEGATE,
              "-",
              TypeManager.ANY_JATOMIC_QSTN() /* params */,
              TypeManager.ANY_JATOMIC_QSTN() /* RetType */);
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {

        ExprType.TypeCode typeCode;
        ExprType.Quantifier quantifier = ExprType.Quantifier.ONE;

        assert caller.getNumArgs() == 1;

        ExprType argType = caller.getArg(0).getType();

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

        switch (argType.getCode()) {
        case INT:
            typeCode = ExprType.TypeCode.INT;
            break;
        case LONG:
            typeCode = ExprType.TypeCode.LONG;
            break;
        case FLOAT:
            typeCode = ExprType.TypeCode.FLOAT;
            break;
        case DOUBLE:
            typeCode = ExprType.TypeCode.DOUBLE;
            break;
        case NUMBER:
            typeCode = ExprType.TypeCode.NUMBER;
            break;
        case ANY_JSON_ATOMIC:
        case JSON:
        case ANY_ATOMIC:
        case ANY:
            typeCode = ExprType.TypeCode.ANY_JSON_ATOMIC;
            break;
        default:
            throw new QueryException(
                "Operand in unary arithmetic operation has illegal " +
                "type.\nOperand type :\n" + argType.getDef().getDDLString(),
                caller.getLocation());
        }

        return TypeManager.getBuiltinType(typeCode, quantifier);
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return  caller.getArg(0).mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return  caller.getArg(0).mayReturnEmpty();
    }

    @Override
    Expr normalizeCall(ExprFuncCall funcCall) {
        Expr arg = funcCall.getArg(0);
        if (arg.getKind() == Expr.ExprKind.CONST) {
            FieldValueImpl val = ((ExprConst)arg).getValue();
            switch (val.getType()) {
            case INTEGER:
                return new ExprConst(arg.getQCB(), arg.getSctx(),
                    arg.getLocation(), IntegerDefImpl.Constants.integerDef.
                    createInteger( - val.asInteger().get()));
            case LONG:
                return new ExprConst(arg.getQCB(), arg.getSctx(),
                    arg.getLocation(), LongDefImpl.Constants.longDef.
                    createLong( - val.asLong().get()));
            case FLOAT:
                return new ExprConst(arg.getQCB(), arg.getSctx(),
                    arg.getLocation(), FloatDefImpl.Constants.floatDef.
                    createFloat( - val.asFloat().get()));
            case DOUBLE:
                return new ExprConst(arg.getQCB(), arg.getSctx(),
                    arg.getLocation(), DoubleDefImpl.Constants.doubleDef.
                    createDouble( - val.asDouble().get()));
            case NUMBER:
                return new ExprConst(
                    arg.getQCB(), arg.getSctx(),
                    arg.getLocation(),
                    FieldDefImpl.Constants.numberDef.createNumber(
                        val.asNumber().get().negate()));
            default:
                throw new QueryException("Operand in unary arithmetic " +
                    "operation has illegal type. Operand type: " + val.
                    getDefinition().getDDLString(), funcCall.getLocation());
            }
        }
        return funcCall;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);

        assert argIters != null && argIters.length == 1;

        return new ArithUnaryOpIter(funcCall, resultReg, theCode, argIters[0]);
    }
}
