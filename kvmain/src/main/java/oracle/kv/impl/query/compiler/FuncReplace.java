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
import oracle.kv.impl.query.runtime.FuncReplaceIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * {@literal
 * replace(any* str, any* search_str[, any* replacement_str] ) -> string }
 *
 * replace function returns str with every occurrence of search_str replaced
 * with replacement_str. If replacement_str is omitted or empty sequence, then
 * all occurrences of search_str are removed. The result will be checked so
 * that the result would not be bigger than STRING_MAX_SIZE = 2^18 - 1 in chars
 * ie. 512kb, if that is the case a runtime query exception is thrown.
 * Arguments are implicitly casted to string (string sequence of any length).
 *
 * Note: If str or search_str argument is an empty sequence, the result is null.
 * If any argument is a sequence with more than one item or null the result is
 * null.
 *
 * Example:
 * SELECT replace( name, 'Corporation', 'Technologies' )
 * From customers WHERE name = 'FairCom Corporation Location'
 *
 * Result
 * -----------------------------
 * FairCom Technologies Location
 */
public class FuncReplace extends Function {

    private static ArrayList<ExprType> getParamTypes() {
        ArrayList<ExprType> paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.ANY_STAR());
        paramTypes.add(TypeManager.ANY_STAR());
        return paramTypes;
    }

    FuncReplace() {
        super(FunctionLib.FuncCode.FN_REPLACE,
            "replace",
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
                "the replace function is invalid.");
        }

        return fncall;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);

        return new FuncReplaceIter(funcCall, resultReg, argIters);
    }
}
