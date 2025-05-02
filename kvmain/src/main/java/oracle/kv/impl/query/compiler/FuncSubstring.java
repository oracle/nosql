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

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.runtime.FuncSubstringIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Substring
 * {@literal 
 * substring(any* str, integer* position[, integer* substring_length]) -> string }
 *
 * substring is a function that returns a string extracted from a given string
 * according to a given numeric starting position and a given numeric
 * substring_length. Argument str is implicitly casted to sequence of string.
 *
 * Note: If str is null, the result is null. The result is also null when
 * position less than 0 or bigger or equal to str length.
 *
 * The result is empty string "" if the result doesn't select any chars or if
 * substring_length is less than 1.
 *
 * Note: If any argument is an empty sequence or is a sequence with more than
 * one item the result is null.
 *
 * Note: position argument indicates where to start the result, first char has
 * position 0.
 *
 * Note: If position or substring_length are not integer an error is thrown:
 *      Cannot promote item 2 of type: String to type: Integer*
 *
 * Example:
 *
 * SELECT substring('ABCDEFG', 2, 4) as Substring FROM t;
 *
 * Substring
 * ---------
 * CDEF
 */
public class FuncSubstring extends Function {

    private static ArrayList<ExprType> getParamTypes() {
        ArrayList<ExprType> paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.INT_STAR());
        paramTypes.add(TypeManager.INT_STAR());
        return paramTypes;
    }

    FuncSubstring() {
        super(FunctionLib.FuncCode.FN_SUBSTRING,
            "substring",
            getParamTypes(),
            TypeManager.STRING_ONE(),
            true);  /* can have 2 or 3 params */
    }

    @Override
    boolean isIndexable() {
        return true;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return true; // if pos >= length or forLength == 0
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        int numArgs = fncall.getNumArgs();
        if (numArgs < 2 || numArgs > 3) {
            throw new QueryException(
                "The number of parameters specified for " +
                    "the substring function is invalid.");
        }

        return fncall;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);
        return new FuncSubstringIter(funcCall, resultReg, theCode, argIters);
    }
}
