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
import oracle.kv.impl.query.runtime.FuncRowMetadataIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.Row;

/**
 * Function to return the metadata associated to a row.
 *
 * @see Row#getRowMetadata()
 * @since 25.3
 */
public class FuncRowMetadata extends Function {

    public static final String COL_NAME = "row_metadata()";
 
    FuncRowMetadata() {
        super(FuncCode.FN_ROW_METADATA, "row_metadata",
            TypeManager.ANY_RECORD_ONE(),
            TypeManager.createType(FieldDefImpl.Constants.jsonDef,
                ExprType.Quantifier.ONE));
    }

    @Override
    public boolean isRowProperty() {
        return true;
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
    Expr normalizeCall(ExprFuncCall funcCall) {

        Expr arg = funcCall.getArg(0);

        if (arg.getKind() == Expr.ExprKind.VAR && ((ExprVar)arg).getTable() != null) {
            return funcCall;
        }

        throw new QueryException(
            "The argument to the row_metadata function must be a row " +
                "variable", funcCall.getLocation());
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
        ExprFuncCall caller,
        PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        Expr arg = caller.getArg(0);

        if (arg.getKind() != Expr.ExprKind.VAR || ((ExprVar)arg).getTable() == null) {
            throw new QueryException(
                "The argument to the expiration_time function must " +
                    "be a row variable", caller.getLocation());
        }

        return new FuncRowMetadataIter(caller, resultReg, argIters[0]);
    }
}

