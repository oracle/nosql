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

import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.FuncIndexStorageSizeIter;
import oracle.kv.impl.query.runtime.FuncMKIndexStorageSizeIter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldDef.Type;

/**
 * Function to return the total number of bytes used to store the index
 * entry (or entries) of the current row for a given index. 
 */
public class FuncIndexStorageSize extends Function {

    FuncIndexStorageSize(FuncCode code, String name) {
        super(code, name,
              TypeManager.ANY_RECORD_ONE(),
              TypeManager.STRING_ONE(),
              TypeManager.INT_ONE());
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
    Expr normalizeCall(ExprFuncCall fncall) {

        Location loc = fncall.getLocation();

        Expr arg = fncall.getArg(0);

        if (arg.getKind() != ExprKind.VAR || ((ExprVar)arg).getTable() == null) {
            throw new QueryException(
                "The argument to the index_storage_size function must " +
                "be a row variable", loc);

        }

        ExprVar rowVar = (ExprVar)arg;
        Expr indexExpr = fncall.getArg(1);

        if (indexExpr.getKind() != ExprKind.CONST) {
            throw new QueryException(
                "The second argument to the index_storage_size " +
                "function must be a string literal", loc);
        }

        FieldValueImpl indexNameVal = ((ExprConst)indexExpr).getValue();

        if (indexNameVal.getType() != Type.STRING) {
            throw new QueryException(
                "The second argument to the index_storage_size " +
                "function must be a string literal", loc);
        }

        String indexName = indexNameVal.asString().get();
        TableImpl table = rowVar.getTable();
        IndexImpl index = (IndexImpl)table.getIndex(indexName);

        if (index == null) {
            throw new QueryException(
                "Table " + table.getFullName() +
                " does not hae an index named " +
                indexName, loc);
        }

        Expr expr = rowVar.getDomainExpr();

        switch (expr.getKind()) {
        case BASE_TABLE:
            ExprBaseTable tableExpr = (ExprBaseTable)expr;

            if (table.getId() == tableExpr.getTargetTable().getId()) {
                tableExpr.addIndexStorageSizeCall(fncall);
            }
            break;
        case UPDATE_ROW:
            break;
        case INSERT_ROW:
            throw new QueryException(
                "The index_storage_size function cannot be used in an " +
                "INSERT statement", loc);
        default:
            throw new QueryException(
                "Unexpected use of index_storage_size function", loc);

        }

        return fncall;
    }

    @Override
    PlanIter codegen(CodeGenerator codegen,
                     ExprFuncCall caller,
                     PlanIter[] argIters) {

        int resultReg = codegen.allocateResultReg(caller);

        Expr arg = caller.getArg(0);

        if (arg.getKind() != ExprKind.VAR || ((ExprVar)arg).getTable() == null) {
            throw new QueryException(
                "The argument to the index_storage_size function must " +
                "be a row variable", caller.getLocation());
        }

        ExprVar varArg = (ExprVar)arg;
        TableImpl table = varArg.getTable();

        ExprConst indexExpr = (ExprConst)caller.getArg(1);
        String indexName = indexExpr.getValue().asString().get();

        if (theCode == FuncCode.FN_INDEX_STORAGE_SIZE) {
            return new FuncIndexStorageSizeIter(caller, resultReg,
                                                argIters[0],
                                                table, indexName);
        }

        return new FuncMKIndexStorageSizeIter(caller, resultReg,
                                              argIters[0], indexName); 
    }
}
