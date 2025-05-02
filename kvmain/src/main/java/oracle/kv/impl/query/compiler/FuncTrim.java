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
import oracle.kv.impl.query.runtime.FuncTrimIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Trim
 * {@literal trim(any* str [, string* where[, string* trim_char]]) -> string }
 *
 * trim function is a function that returns its first str argument with leading
 * and/or trailing pad characters removed. The second argument where indicates
 * whether leading, or trailing, or both leading and trailing pad characters
 * should be removed. Valid values for second parameter are "leading",
 * "trailing", "both" (case is ignored), if not specified value "both"
 * will be assumed,  if specified but not one of the valid values null is
 * returned. The third argument trim_char specifies the pad character that is to
 * be removed, if there are more chars in trim_char the first char will be used,
 * if "" empty string then no trimming, if not specified then ' ' space char
 * will be used. Arguments str, where and trim_char are implicitly casted to
 * string* (string sequence of any length).
 *
 * Note: If str, where or trim_char is null, the result is null.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is null.
 *
 * Example
 * This example trims leading zeros from the hire date of the employees:
 *
 * SELECT employee_id, trim(hire_date, "leading", "0")
 * FROM employees
 *
 * EMPLOYEE_ID trim
 * ----------- ---------
 *         105 25-JUN-05
 *         106 5-FEB-06
 *         107 7-FEB-07
 */
public class FuncTrim extends Function {

    private static ArrayList<ExprType> getParamTypes() {
        ArrayList<ExprType> paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.ANY_STAR());
        return paramTypes;
    }

    FuncTrim() {
        super(FunctionLib.FuncCode.FN_TRIM,
            "trim",
            getParamTypes(),
            TypeManager.STRING_ONE(),
            true);  /* can have 1, 2 or 3 params */
    }

    @Override
    boolean isIndexable() {
        return true;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return (caller.getArg(0).mayReturnNULL() ||
                !caller.getArg(0).isScalar() ||
                caller.getArgs().size() >= 2 ||
                caller.getArgs().size() == 3 &&
                (caller.getArg(2).mayReturnNULL() ||
                 !caller.getArg(2).isScalar()));
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        int numArgs = fncall.getNumArgs();

        if (numArgs < 1 || numArgs > 3) {
            throw new QueryException(
                "The number of parameters specified for " +
                "the trim function is invalid.");
        }

        return fncall;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall funcCall,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);
        return new FuncTrimIter(funcCall, resultReg, theCode, argIters);
    }
}
