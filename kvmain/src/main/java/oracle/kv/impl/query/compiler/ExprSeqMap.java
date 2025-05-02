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
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/**
 * 
 */
class ExprSeqMap extends Expr {

    private Expr theInput;

    private Expr theMapExpr;

    private ExprVar theCtxItemVar;

    ExprSeqMap(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location) {

        super(qcb, sctx, ExprKind.SEQ_MAP, location);
    }

    @Override
    public ExprSeqMap clone() {

        ExprSeqMap res = new ExprSeqMap(theQCB, theSctx, theLocation);
        res.theInput = theInput.clone();
        res.theMapExpr = theMapExpr.clone();
        res.theCtxItemVar = theCtxItemVar;
        return res;
    }

    void addCtxVar(ExprVar v) {
        theCtxItemVar = v;
    }

    void addInputExpr(Expr input) {
        theInput = input;
        theInput.addParent(this);
    }

    void addMapExpr(Expr map) {
        theMapExpr = map;
        theMapExpr.addParent(this);
    }

    void setInput(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theInput.removeParent(this, destroy);
        theInput = newExpr;
    }

    void setMapExpr(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theMapExpr.removeParent(this, destroy);
        theMapExpr = newExpr;
    }

    @Override
    int getNumChildren() {
        return 2;
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    Expr getMapExpr() {
        return theMapExpr;
    }

    ExprVar getCtxVar() {
        return theCtxItemVar;
    }

    @Override
    public ExprType computeType() {

        Quantifier q1 = theInput.getType().getQuantifier();
        Quantifier q2 = theMapExpr.getType().getQuantifier();
        Quantifier q = TypeManager.getUnionQuant(q1, q2);

        theType = TypeManager.createType(theMapExpr.getType(), q);
        return theType;
    }

    @Override
    public boolean mayReturnNULL() {
        return theMapExpr.mayReturnNULL();
    }

    @Override
    public boolean mayReturnEmpty() {
        return theInput.mayReturnEmpty() || theMapExpr.mayReturnEmpty();
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        theInput.display(sb, formatter);
        sb.append(",\n");
        theMapExpr.display(sb, formatter);
    }
}
