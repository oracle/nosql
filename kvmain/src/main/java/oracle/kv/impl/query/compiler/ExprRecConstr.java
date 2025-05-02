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
import java.util.List;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;


/**
 * Internal expr to construct a record.
 */
public class ExprRecConstr extends Expr {

    final RecordDefImpl theDef;

    final List<Expr> theArgs;

    public ExprRecConstr(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        RecordDefImpl def,
        List<Expr> args) {

        super(qcb, sctx, ExprKind.REC_CONSTR, location);
        theDef = def;
        theArgs = args;
    }

    @Override
    public ExprRecConstr clone() {

        ArrayList<Expr> args = new ArrayList<Expr>(theArgs.size());
        for (Expr arg : theArgs) {
            args.add(arg.clone());
        }

        return new ExprRecConstr(theQCB, theSctx, theLocation, theDef, args);
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    public RecordDefImpl getDef() {
        return theDef;
    }

    int getNumArgs() {
        return theArgs.size();
    }

    Expr getArg(int i) {
        return theArgs.get(i);
    }

    void setArg(int i, Expr newExpr, boolean destroy) {

        FieldDefImpl argType = newExpr.getType().getDef();

        if (!argType.isSubtype(theDef)) {
            throw new QueryException(
                "Type:\n" + argType.getDDLString() + "\nis not a subtype of\n" +
                theDef.getDDLString(), theLocation);
        }

        newExpr.addParent(this);
        theArgs.get(i).removeParent(this, destroy);
        theArgs.set(i, newExpr);
    }

    @Override
    public boolean mayReturnNULL() {
        return false;
    }

    @Override
    public boolean mayReturnEmpty() {
        return false;
    }

    @Override
    ExprType computeType() {
        return TypeManager.createType(theDef, Quantifier.ONE);
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        sb.append("type = \n");
        theDef.display(sb, formatter);

        for (int i = 0; i < theArgs.size(); ++i) {
            theArgs.get(i).display(sb, formatter);
            if (i < theArgs.size() - 1) {
                sb.append(",\n");
            }
        }
    }
}
