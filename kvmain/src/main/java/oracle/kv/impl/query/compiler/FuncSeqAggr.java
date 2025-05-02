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

import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.query.types.TypeManager;

import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.runtime.FuncSeqAggrIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.table.FieldDef;

/*
 * seq_count() :
 * - Returns NULL if the input seq contains a NULL.
 * - Returns 0 if the input seq is empty
 *
 * seq_sum() : 
 * - Skips non-numeric items in input sequence.
 * - Returns NULL if input seq does not contain any numeric items.
 * - Returns EMPTY if the input seq is empty
 *
 * seq_min/max() :
 * - Skips json null, EMPTY, binary, and non-atomic items in input sequence
 * - Returns NULL if the input seq contains a NULL or all of its items are skipped
 * - Returns EMPTY if the input seq is empty
 */
class FuncSeqAggr extends Function {

    FuncSeqAggr(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_STAR(),
              TypeManager.ANY_ATOMIC_QSTN()); /* RetType */
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall fncall) {

        if (theCode == FuncCode.FN_SEQ_COUNT_I ||
            theCode == FuncCode.FN_SEQ_COUNT_NUMBERS_I) {
            return false;
        }

        return true;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {

        if (theCode == FuncCode.FN_SEQ_COUNT) {
            return false;
        }

        return caller.getArg(0).mayReturnEmpty();
    }

    @Override
    ExprType getRetType(ExprFuncCall fncall) {

        if (theCode == FuncCode.FN_SEQ_COUNT) {
            return TypeManager.LONG_ONE();
        }

        FieldDefImpl inType = fncall.getInput().getType().getDef();
        FieldDef.Type inTypeCode = inType.getType();
        boolean isMinMax = (theCode == FuncCode.FN_SEQ_MIN ||
                            theCode == FuncCode.FN_SEQ_MAX ||
                            theCode == FuncCode.FN_SEQ_MIN_I ||
                            theCode == FuncCode.FN_SEQ_MAX_I);
        boolean isSum = (theCode == FuncCode.FN_SEQ_SUM);

        switch (inTypeCode) {
        case INTEGER:
            if (isMinMax) {
                return TypeManager.INT_QSTN();
            } else if (isSum) {
                return TypeManager.LONG_QSTN();
            } else {
                return TypeManager.DOUBLE_QSTN();
            }
        case LONG:
            if (isMinMax || isSum) {
                return TypeManager.LONG_QSTN();
            }
            return TypeManager.DOUBLE_QSTN();
        case FLOAT:
        case DOUBLE:
            return TypeManager.DOUBLE_QSTN();
        case NUMBER:
            return TypeManager.NUMBER_QSTN();
        case STRING:
            if (isMinMax) {
                return TypeManager.STRING_QSTN();
            }
            break;
        case TIMESTAMP:
            if (isMinMax) {
                int precision = inType.asTimestamp().getPrecision();
                return TypeManager.TIMESTAMP_QSTN(precision);
            }
            break;
        case BOOLEAN:
            if (isMinMax) {
                return TypeManager.BOOLEAN_QSTN();
            }
            break;
        case ENUM:
            if (isMinMax) {
                return TypeManager.createType(fncall.getInput().getType(),
                                              Quantifier.QSTN);
            }
            break;
        case JSON:
        case ANY_JSON_ATOMIC:
            return TypeManager.ANY_JATOMIC_QSTN();
        case ANY_ATOMIC:
        case ANY:
            return theReturnType;
        default:
            break;
        }

        throw new QueryException(
            "Invalid input type for the " + theName + "function:\n" +
            inType.getDDLString(), fncall.getLocation());
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        return new FuncSeqAggrIter(caller, theCode, resultReg, argIters[0]);
    }
}
