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

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.MathIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

import java.util.ArrayList;

public class FuncMath extends Function {
    private static final ExprType paramType = TypeManager.ANY_QSTN();
    private static final ExprType retType = TypeManager.ANY_JATOMIC_QSTN();

    // 0 argument
    FuncMath(FuncCode code,
             String name,
             ExprType retType) {
        super(code, name, retType);
    }

    // 1 argument
    FuncMath(FuncCode code, String name) {
        super(code, name, paramType, retType);
    }

    // 2 or variable number of arguments
    FuncMath(FuncCode code,
             String name,
             ArrayList<ExprType> paramTypes,
             boolean isVariadic) {
        super(code, name, paramTypes, retType, isVariadic);
    }

    @Override
    boolean isIndexable() {
        return (theCode != FuncCode.FN_MATH_E &&
                theCode != FuncCode.FN_MATH_PI &&
                theCode != FuncCode.FN_MATH_RAND);
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {
        switch (theCode) {
            case FN_MATH_ABS:
            case FN_MATH_CEIL:
            case FN_MATH_FLOOR: {
                Expr arg = caller.getArg(0);
                switch (arg.getType().getCode()) {
                    case INT:
                    case LONG:
                    case DOUBLE:
                    case NUMBER:
                    case FLOAT:
                        return arg.getType();
                    default:
                        return theReturnType;
                }
            }
            case FN_MATH_SIGN:
            case FN_MATH_ACOS:
            case FN_MATH_ASIN:
            case FN_MATH_ATAN:
            case FN_MATH_ATAN2:
            case FN_MATH_COS:
            case FN_MATH_COT:
            case FN_MATH_DEGREES:
            case FN_MATH_E:
            case FN_MATH_EXP:
            case FN_MATH_LN:
            case FN_MATH_LOG10:
            case FN_MATH_LOG:
            case FN_MATH_PI:
            case FN_MATH_POWER:
            case FN_MATH_RADIANS:
            case FN_MATH_RAND:
            case FN_MATH_ROUND:
            case FN_MATH_SIN:
            case FN_MATH_SQRT:
            case FN_MATH_TAN:
            case FN_MATH_TRUNC:
                return TypeManager.DOUBLE_QSTN();
            default:
                return theReturnType;
        }
    }

    @Override
    public ExprType getRetType(FieldDefImpl inType) {
        switch (theCode) {
            case FN_MATH_ABS:
            case FN_MATH_CEIL:
            case FN_MATH_FLOOR:
                switch (inType.getType()) {
                    case INTEGER:
                    case LONG:
                    case DOUBLE:
                    case FLOAT:
                    case NUMBER:
                        return TypeManager.getBuiltinType(inType,
                                Quantifier.QSTN);
                    default:
                        return theReturnType;
                }
            case FN_MATH_SIGN:
            case FN_MATH_ACOS:
            case FN_MATH_ASIN:
            case FN_MATH_ATAN:
            case FN_MATH_ATAN2:
            case FN_MATH_COS:
            case FN_MATH_COT:
            case FN_MATH_DEGREES:
            case FN_MATH_E:
            case FN_MATH_EXP:
            case FN_MATH_LN:
            case FN_MATH_LOG10:
            case FN_MATH_LOG:
            case FN_MATH_PI:
            case FN_MATH_POWER:
            case FN_MATH_RADIANS:
            case FN_MATH_RAND:
            case FN_MATH_ROUND:
            case FN_MATH_SIN:
            case FN_MATH_SQRT:
            case FN_MATH_TAN:
            case FN_MATH_TRUNC:
                return TypeManager.DOUBLE_QSTN();
            default:
                return theReturnType;
        }
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return true;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        for (Expr arg : caller.getArgs()) {
            if (arg.mayReturnEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    boolean isDeterministic() {
        return theCode != FunctionLib.FuncCode.FN_MATH_RAND;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen, ExprFuncCall funcCall,
                     PlanIter[] argIters) {
        int resultReg = codegen.allocateResultReg(funcCall);
        assert argIters != null;
        return new MathIter(funcCall, resultReg, theCode, argIters);
    }
}
