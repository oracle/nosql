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

import oracle.kv.impl.query.runtime.FuncReverseIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * {@literal reverse(any* str) -> string }
 *
 * reverse returns the chars of the input string in reverse order.
 * reverse returns the chars of the input string in reverse order. Argument str
 * is implicitly casted to string* (string sequence of any length).
 *
 * Note: For a null argument the result will be null.
 *
 * Note: If str argument is an empty sequence or a sequence with more than one
 * item the result is null.
 *
 * Example:
 * SELECT first_name, reverse(first_name)  FROM employees ;
 *
 * FIRST_NAME  REVERSE
 * ----------  ----------
 * Lindsey     yesdniL
 * William     mailliW
 */
public class FuncReverse extends Function {

    FuncReverse() {
        super(FunctionLib.FuncCode.FN_REVERSE,
        "reverse",
        TypeManager.ANY_STAR(),
        TypeManager.STRING_ONE(),
        false);
    }

    @Override
    boolean isIndexable() {
        return true;
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return (caller.getArg(0).mayReturnNULL() ||
                !caller.getArg(0).isScalar());
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);
        return new FuncReverseIter(funcCall, resultReg, argIters[0]);
    }
}
