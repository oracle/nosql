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
import oracle.kv.impl.query.runtime.FuncFormatTimestampIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.TimestampDef;

/**
 * string? format_timestamp(any? timestamp[, any? format[, any? timezone]])
 *
 * Function to covert a TIMESTAMP value to a STRING in the specified format and
 * time zone, the format and timezone are optional. Default format is
 * {@link TimestampDef#DEFAULT_PATTERN} and default timezone is UTC.
 */
public class FuncFormatTimestamp extends Function {

    static ArrayList<ExprType> paramTypes;
    static {
        paramTypes = new ArrayList<>();
        paramTypes.add(TypeManager.ANY_QSTN());
        paramTypes.add(TypeManager.ANY_QSTN());
        paramTypes.add(TypeManager.ANY_QSTN());
    }

    FuncFormatTimestamp() {
        super(FuncCode.FN_FORMAT_TIMESTAMP,
              "format_timestamp",
              paramTypes,
              TypeManager.STRING_QSTN(),
              true); /* can have 1 ~ 3 operands */
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {
        int numArgs = fncall.getNumArgs();
        if (numArgs == 0 || numArgs > 3) {
            throw new QueryException(
                "The number of parameters specified for the " +
                "to_string(<timestamp> [,pattern]) function is invalid, " +
                "it may have 1 ~ 3 parameters");
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
        return new FuncFormatTimestampIter(funcCall, resultReg, argIters[0],
                                           (argIters.length > 1 ?
                                               argIters[1] : null),
                                           (argIters.length > 2 ?
                                               argIters[2] : null));
    }
}
