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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * theExprs:
 * Stores all the child exprs of this case expr. Each "then" expr is stored
 * right afdter its associated "when" expr. The "else" expr, if any, is stored
 * last.
 */
class ExprCase extends Expr {

    private ArrayList<Expr> theExprs;

    private boolean theHasElseExpr;

    ExprCase(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        ArrayList<Expr> exprs) {

        super(qcb, sctx, ExprKind.CASE, location);

        theExprs = exprs;
        theHasElseExpr = (exprs.size() % 2 != 0);

        int elsePos = (theHasElseExpr ? exprs.size() - 1 : -1);

        for (int i = 0; i < theExprs.size(); ++i) {

            Expr e = theExprs.get(i);

            if (i % 2 == 0 && i != elsePos) {
                e = ExprPromote.create(null, e, TypeManager.BOOLEAN_QSTN());
                theExprs.set(i, e);
            }
            e.addParent(this);
        }
    }

    @Override
    public ExprCase clone() {

        ArrayList<Expr> args = new ArrayList<Expr>(theExprs.size());
        for (Expr arg : theExprs) {
            args.add(arg.clone());
        }

        ExprCase res = new ExprCase(theQCB, theSctx, theLocation, args);
        res.theHasElseExpr = theHasElseExpr;
        return res;
    }

    @Override
    int getNumChildren() {
        return theExprs.size();
    }

    int getNumWhenClauses() {
        return theExprs.size() / 2;
    }

    boolean hasElseClause() {
        return theHasElseExpr;
    }

    Expr getExpr(int i) {
        return theExprs.get(i);
    }

    Expr getCondExpr(int i) {
        return theExprs.get(i * 2);
    }

    Expr getThenExpr(int i) {
        return theExprs.get(i * 2 + 1);
    }

    Expr getElseExpr() {
        return (theHasElseExpr ?
                theExprs.get(theExprs.size() - 1) :
                null);
    }

    void setExpr(int i, Expr newExpr, boolean destroy) {

        int elsePos = (theHasElseExpr ? theExprs.size() - 1 : -1);
        if (i % 2 == 0 && i != elsePos) {
            newExpr = ExprPromote.create(null, newExpr,
                                         TypeManager.BOOLEAN_QSTN());
        }
        newExpr.addParent(this);
        theExprs.get(i).removeParent(this, destroy);
        theExprs.set(i, newExpr);
    }

    void removeWhenClause(int i, boolean destroy) {
        i *= 2;
        theExprs.get(i).removeParent(this, destroy);
        theExprs.remove(i);
        theExprs.get(i).removeParent(this, destroy);
        theExprs.remove(i);
    }

    void removeElseClause(boolean destroy) {
        int i = theExprs.size() - 1;
        theExprs.get(i).removeParent(this, destroy);
        theExprs.remove(i);
    }

    @Override
    ExprType computeType() {

        ExprType type = theExprs.get(1).getType();

        for (int i = 3; i < theExprs.size(); i += 2) {
            type = TypeManager.getUnionType(type,
                                            theExprs.get(i).getType());

            if (type.isAny()) {
                return type;
            }
        }

        if (theHasElseExpr) {
            type = TypeManager.getUnionType(type, getElseExpr().getType());
        }

        return type;
    }



    @Override
    public boolean mayReturnNULL() {

        for (int i = 1; i < theExprs.size(); i += 2) {
            if (theExprs.get(i).mayReturnNULL()) {
                return true;
            }
        }

        Expr elseExpr = getElseExpr();
        if (elseExpr != null && elseExpr.mayReturnNULL()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean mayReturnEmpty() {

        Expr elseExpr = getElseExpr();

        if (elseExpr == null) {
            return true;
        }

        for (int i = 1; i < theExprs.size(); i += 2) {
            if (theExprs.get(i).mayReturnEmpty()) {
                return true;
            }
        }

        return elseExpr.mayReturnEmpty();
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        int num = theExprs.size();
        if (theHasElseExpr) {
            --num;
        }

        for (int i = 0; i < num; ++i) {
            formatter.indent(sb);
            sb.append("WHEN :\n");
            theExprs.get(i).display(sb, formatter);
            sb.append("\n");
            formatter.indent(sb);
            sb.append("THEN :\n");
            theExprs.get(i).display(sb, formatter);
            sb.append("\n");
        }

        if (theHasElseExpr) {
            formatter.indent(sb);
            sb.append("ELSE :\n");
            getElseExpr().display(sb, formatter);
        }
    }
}
