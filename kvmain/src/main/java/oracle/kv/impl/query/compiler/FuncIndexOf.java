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
import oracle.kv.impl.query.runtime.FuncIndexOfIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * {@literal index_of(any* str, any* search[, integer* pos]) -> integer }
 *
 * index_of function determines the first position, if any, at which one string,
 * search, occurs within str returning the position in chars. If search string
 * is of length zero, then it occurs at position 0 for any value of str. If
 * search string does not occur in str, then -1 is returned. The returned type
 * is integer. If not specified the default pos is 0, meaning the search starts
 * at the beginning of str. The return value is relative to the beginning of
 * str, regardless of the value of pos. Arguments str and search are implicitly
 * casted to string* (string sequence of any length).
 *
 * Note: Arguments str and search are implicitly casted to string*
 * (string sequence of any length). If str, search or pos is null, the result
 * is null.
 *
 * Note: pos is an optional integer indicating the character of str where it
 * begins the search that is, the position of the first character of the first
 * substring to compare with search. First char in str has pos 0.
 * For negative values, value 0 is assumed.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is null. For this case one can use seq_transform function.
 *
 * Note: Error if pos is not an integer.
 *
 * Example:
 * {@code
 * SELECT last_name FROM employees
 * WHERE index_of(last_name, "son", 3)) > 0
 * ORDER BY last_name; }
 *
 * LAST_NAME
 * -------------------------
 * Anderson
 * Jameson
 * Sony
 */
public class FuncIndexOf extends Function {

    private static ArrayList<ExprType> getParamTypes() {
        ArrayList<ExprType> paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.INT_STAR());
        return paramTypes;
    }

    FuncIndexOf() {
        super(FunctionLib.FuncCode.FN_INDEX_OF,
            "index_of",
            getParamTypes(),
            TypeManager.INT_ONE(),
            true);  /* can have 2 or 3 params */
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return (caller.getArg(0).mayReturnNULL() ||
                !caller.getArg(0).isScalar() ||
                caller.getArg(1).mayReturnNULL() ||
                !caller.getArg(1).isScalar() ||
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

        if (numArgs < 2 || numArgs > 3) {
            throw new QueryException(
                "The number of parameters specified for " +
                    "the index_of function is invalid.");
        }

        return fncall;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);

        return new FuncIndexOfIter(funcCall, resultReg, theCode,
                                   argIters);
    }
}
