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
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncParseToTimestampIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.TimestampDef;

/**
 * tiemstamp? parse_to_timestamp(any? string[, any? format]])
 *
 * Function to covert a STRING in the given format to a TIMESTAMP, the format
 * is optional, default is {@link TimestampDef#DEFAULT_PATTERN}
 */
public class FuncParseToTimestamp extends Function {

    static ArrayList<ExprType> paramTypes;
    static {
        paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_QSTN());
        paramTypes.add(TypeManager.ANY_QSTN());
    }

    FuncParseToTimestamp() {
        super(FuncCode.FN_PARSE_TO_TIMESTAMP,
              "parse_to_timestamp",
              paramTypes,
              TypeManager.TIMESTAMP_QSTN(),
              true); /* can have 1 or 2 operands */
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        int numArgs = fncall.getNumArgs();
        if (numArgs == 0 || numArgs > 2) {
            throw new QueryException(
                "The number of parameters specified for the " +
                "parse_to_timestamp(<string> [,pattern]) function is " +
                "invalid, it may have 1 or 2 parameters");
        }

        return super.normalizeCall(fncall);
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return true;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return caller.getArg(0).mayReturnEmpty();
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall funcCall,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);
        return new FuncParseToTimestampIter(funcCall, resultReg, argIters[0],
                                            (argIters.length > 1 ?
                                                argIters[1] : null));
    }
}