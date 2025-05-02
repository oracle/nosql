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

import oracle.kv.impl.query.runtime.FuncLengthIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * {@literal length(any* str) -> integer }
 *
 * length function returns the length of a given character string, as an exact
 * numeric value, in UTF characters or null if str is null. Argument str is
 * implicitly casted to string* (string sequence of any length).
 *
 * Note: If str is empty sequence or a sequence with more than one item the
 * result is null.
 *
 * Example
 * SELECT length('CANDIDE') as LengthInCharacters,
 *        length('\uD83D\uDE0B') as lengthOf32BitEncodedChar FROM t
 *
 * LengthInCharacters   lengthOf32BitEncodedChar
 * ------------------   ------------------------
 *                  7                          1
 */
public class FuncLength extends Function {

    FuncLength() {
        super(FunctionLib.FuncCode.FN_LENGTH,
              "length",
              TypeManager.ANY_STAR(),
              TypeManager.INT_ONE(),
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

        return new FuncLengthIter(funcCall, resultReg, argIters[0]);
    }
}
