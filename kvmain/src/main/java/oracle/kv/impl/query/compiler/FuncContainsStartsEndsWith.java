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

import oracle.kv.impl.query.runtime.FuncContainsStartsEndsWithIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * {@literal contains(any* str, any* contained_str) -> boolean }
 *   - returns true if contained_str exists inside str.
 *
 * {@literal starts_with(any* str, any* start) -> boolean }
 *   - returns true if str starts with start substring.
 *
 * {@literal ends_with(any* str, any* end) -> boolean }
 *   - returns true if str ends with end substring.
 *
 * Note: Arguments are implicitly casted to string* (string sequence of any
 * length). For a null str, contained_str, start or end the result will be null.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is false.
 *
 * Example:
 * SELECT first_name, contains(first_name, 'in'),
 *   starts_with(first_name, 'Li'), ends_with(first_name, 'am')
 * FROM employees
 *
 * FIRST_NAME  CONTAINS  STARTS_WITH  ENDS_WITH
 * ----------  --------  -----------  ---------
 * Lindsey     true      true         false
 * William     false     false        true
 */
public class FuncContainsStartsEndsWith extends Function {

    FuncContainsStartsEndsWith(FunctionLib.FuncCode fnCode, String fnName) {
        super(fnCode,
            fnName,
            TypeManager.ANY_STAR(),
            TypeManager.ANY_STAR(),
            TypeManager.BOOLEAN_ONE());
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return (caller.getArg(0).mayReturnNULL() ||
                caller.getArg(1).mayReturnNULL());
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

        return new FuncContainsStartsEndsWithIter(funcCall, resultReg, theCode,
                                                  argIters);
    }
}
