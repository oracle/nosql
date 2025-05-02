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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.runtime.CastIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * This is an internal expr. Its main purpose is to cast an expression to
 * the specified type.
 * Its semantics are as follows:
 *
 * 1. If input value is NULL, NULL is returned.
 *
 * 2. For details on explicit casting see the CastIter javadoc.
 */
class ExprCast extends Expr {

    private Expr theInput;

    private FieldDefImpl theTargetType;
    private ExprType.Quantifier theTargetQuantifier;

    static Expr create(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        Expr input,
        FieldDefImpl targetType,
        ExprType.Quantifier targetQuant) {

        if (input.getKind() == ExprKind.CONST) {
            ExprConst constExpr = (ExprConst)input;
            FieldValueImpl val = constExpr.getValue();
            val = CastIter.castValue(val, targetType, loc);

            return new ExprConst(qcb, sctx, loc, val);
        }

        //System.out.println("Creating cast expr. Input type = " + input.getType());

        if (targetType.equals(input.getType().getDef()) &&
            TypeManager.isSubQuant(input.getType().getQuantifier(),
                                   targetQuant)) {
            return input;
        }

        return new ExprCast(qcb, sctx, loc, input, targetType, targetQuant);
    }

    private ExprCast(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        Expr input,
        FieldDefImpl targetType,
        ExprType.Quantifier targetQuant) {

        super(qcb, sctx, ExprKind.CAST, loc);

        theInput = input;
        theInput.addParent(this);
        theTargetType = targetType;
        theTargetQuantifier = targetQuant;
    }

    @Override
    public ExprCast clone() {
        return new ExprCast(theQCB, theSctx, theLocation, theInput.clone(),
                            theTargetType, theTargetQuantifier);
    }

    @Override
    int getNumChildren() {
        return 1;
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    void setInput(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theInput.removeParent(this, destroy);
        theInput = newExpr;
    }

    FieldDefImpl getTargetType() {
        return theTargetType;
    }

    ExprType.Quantifier getTargetQuantifier() {
        return theTargetQuantifier;
    }

    @Override
    ExprType computeType() {
        return TypeManager.createType(theTargetType, theTargetQuantifier);
    }

    @Override
    public boolean mayReturnNULL() {
        return theInput.mayReturnNULL();
    }

    @Override
    public boolean mayReturnEmpty() {
        return theInput.mayReturnEmpty();
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {
        formatter.indent(sb);
        sb.append("CAST (\n");
        formatter.incIndent();
        theInput.display(sb, formatter);
        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("AS ");
        theTargetType.display(sb, formatter);
        sb.append(theTargetQuantifier.toString());
        sb.append("),\n");
    }
}
