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

import oracle.kv.impl.query.types.TypeManager;

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.FuncMinMaxIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.table.FieldDef;

/*
 * any_atomic min(any*)
 *
 * any_atomic max(any*)
 */
class FuncMinMax extends Function {

    FuncMinMax(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_STAR(),
              TypeManager.ANY_ATOMIC_ONE()); /* RetType */
    }

    @Override
    ExprType getRetType(ExprFuncCall caller) {

        FieldDefImpl inType = caller.getInput().getType().getDef();
        FieldDef.Type inTypeCode = inType.getType();

        switch (inTypeCode) {
        case INTEGER:
            return TypeManager.INT_ONE();
        case LONG:
            return TypeManager.LONG_ONE();
        case FLOAT:
            return TypeManager.FLOAT_ONE();
        case DOUBLE:
            return TypeManager.DOUBLE_ONE();
        case NUMBER:
            return TypeManager.NUMBER_ONE();
        case STRING:
            return TypeManager.STRING_ONE();
        case TIMESTAMP:
            int precision = inType.asTimestamp().getPrecision();
            return TypeManager.TIMESTAMP_ONE(precision);
        case BOOLEAN:
            return TypeManager.BOOLEAN_ONE();
        case ENUM:
            return TypeManager.createType(caller.getInput().getType(),
                                          Quantifier.ONE);
        case ANY:
        case ANY_ATOMIC:
        case JSON:
        case ANY_JSON_ATOMIC:
        case ARRAY:
            return theReturnType;

        case EMPTY:
        case RECORD:
        case ANY_RECORD:
        case MAP:
        case BINARY:
        case FIXED_BINARY:
        case GEOMETRY:
        case POINT:
        case JSON_INT_MRCOUNTER:
        case JSON_LONG_MRCOUNTER:
        case JSON_NUM_MRCOUNTER:
            throw new QueryException(
                "Invalid input type for the " + theName +
                "aggregate function:\n" +
                inType.getDDLString(), caller.getLocation());
        }

        throw new QueryStateException("Should not be here");
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {
        return true;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {
        return false;
    }

    @Override
    boolean isAggregate() {
        return true;
    }

    @Override
    Expr normalizeCall(ExprFuncCall caller) {

        FieldDefImpl inType = caller.getArg(0).getType().getDef();

        if (inType.isEmpty()) {
            return new ExprConst(caller.getQCB(),
                                 caller.getSctx(),
                                 caller.getLocation(),
                                 NullValueImpl.getInstance());
        }

        if (!inType.isAtomic() && !inType.isWildcard() && inType.isArray()) {
            throw new QueryException(
                "Invalid input type for the " + theName +
                "aggregate function:\n" +
                inType.getDDLString(), caller.getLocation());
        }

        return caller;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        return new FuncMinMaxIter(caller, resultReg, theCode, argIters[0],
                                  codegen.isForCloud());
    }
}
