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
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.ConcatenateStringsOpIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * The concatenate strings operator and concat function.
 *
 * {@literal any* arg1 || any* arg2 -> string? }
 *
 * {@literal concat(any* arg1, any* arg2, ...) -> string? }
 *
 * Concatenation is an operator, ||, an a function called concat, that returns
 * the character string made by joining its character string operands in the
 * order given. If any of the args is a sequence, than all the items are
 * concatenated to the result in the order they appear in the sequence.
 * If all args are empty sequence an empty sequence is returned. If all the
 * arguments are sql null than a sql null is returned. The maximum number of
 * chars of the returned string will be less than STRING_MAX_SIZE = 2^18 - 1
 * in chars ie. 512kb, in which case a runtime query exception is thrown.
 *
 * Note: According to RDBMS operator precedence the || operator is immediately
 * after +,- (as binary operators).
 *
 * Note: A sql null argument is converted to empty string during concatenation
 * unless all arguments are sql null, in which case the result is sql null. So
 * sql null can result only from the concatenation of two or more sql null
 * values.
 *
 * Note: All arguments are implicitly casted to string* (string sequence of any
 * length), at the moment all other types are castable to string (including
 * JSON null which is changed to convert to the string "null").
 *
 *
 * Example
 * SELECT col1 || col2 || col3 || col4 as Concatenation FROM tab1;
 *
 * Concatenation
 * -------------
 * abcdefghijkl
 */
public class FuncConcatenateStringsOp
    extends Function {

    FuncConcatenateStringsOp() {
        super(
            FuncCode.OP_CONCATENATE_STRINGS,
            "concat",
            TypeManager.ANY_STAR(),
            TypeManager.STRING_QSTN() /*retType*/,
            true /*isVariadic*/);
    }

    @Override
    Expr normalizeCall(ExprFuncCall fncall) {

        if (fncall.getNumArgs() < 1) {
            throw new QueryException(
                "concat function must have at least one argument.",
                fncall.getLocation());
        }

        for (int i = 0; i < fncall.getArgs().size(); i++ ) {
            Expr strCastExpr =
                ExprCast.create(fncall.getQCB(), fncall.getSctx(),
                    fncall.getLocation(), fncall.getArg(i),
                    FieldDefImpl.Constants.stringDef,
                    ExprType.Quantifier.STAR);
            fncall.setArg(i, strCastExpr, false);
        }

        return fncall;
    }

    @Override
    PlanIter codegen(
        CodeGenerator codegen,
        ExprFuncCall funcCall,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(funcCall);

        return new ConcatenateStringsOpIter(funcCall, resultReg, theCode,
            argIters);
    }

    @Override
    boolean mayReturnNULL(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        for (int i = 0; i < numArgs; i++) {
            if (caller.getArg(i).mayReturnNULL()) {
                return true;
            }
        }

        return false;
    }

    @Override
    boolean mayReturnEmpty(ExprFuncCall caller) {

        int numArgs = caller.getNumArgs();

        for (int i = 0; i < numArgs; i++) {
            if (!caller.getArg(i).mayReturnEmpty()) {
                return false;
            }
        }

        return true;
    }
}
