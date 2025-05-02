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

import oracle.kv.impl.query.runtime.FuncUpperLowerIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Upper/Lower
 * {@literal upper(any* str) -> string }
 * {@literal lower(any* str) -> string }
 * upper and lower are a pair of functions for converting all the lower case and
 * title case characters in a given string to upper case (upper) or all the
 * upper case and title case characters to lower case (lower). A lower case
 * character is a character in the Unicode General Category class "Ll"
 * (lower-case letters). An upper case character is a character in the Unicode
 * General Category class "Lu" (upper-case letters). A title case character is a
 * character in the Unicode General Category class "Lt" (title-case letters).
 * Argument str is implicitly casted to string* (string sequence of any length).
 *
 * Note: If str is null, the result is null.
 *
 * Note: If str is empty sequence or a sequence with more than one item the
 * result is null.
 *
 * Example
 * This example applies upper to one string and also to a string array:
 *
 * SELECT emp.id, upper(emp.last_name),
 *       [ seq_transform( emp.first_names[], upper($) ) ]
 * FROM employees emp
 * ORDER BY emp.id;
 *
 *          id last_name                              first_names
 * ----------- --------- ----------------------------------------
 *         103    LESLIE                      ["ROSE", "Eleanor"]
 *         104    CLARKE ["EMILIA", "ISOBEL", "EUPHEMIA", "ROSE"]
 */
public class FuncUpperLower extends Function {

    FuncUpperLower(FunctionLib.FuncCode code, String name) {
        super(code,
              name,
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
        return new FuncUpperLowerIter(funcCall, resultReg, theCode, argIters);
    }
}
