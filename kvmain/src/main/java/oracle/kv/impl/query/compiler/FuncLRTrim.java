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

import oracle.kv.impl.query.runtime.FuncLRTrimIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;

/**
 * {@literal ltrim(any* from_str) -> string }
 * {@literal rtrim(any* from_str) -> string }
 *
 * ltrim function is a function that returns its first str argument with
 * leading space characters removed.
 *
 * rtrim function is a function that returns its first str argument with
 * trailing space characters removed.
 *
 * Note: If str is null, the result is null.
 *
 * Note: If any argument is an empty sequence or a sequence with more than one
 * item the result is null.
 */
public class FuncLRTrim extends Function {

    FuncLRTrim(FunctionLib.FuncCode code, String name) {
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

        return new FuncLRTrimIter(funcCall, resultReg, theCode,
                                  argIters);
    }
}
