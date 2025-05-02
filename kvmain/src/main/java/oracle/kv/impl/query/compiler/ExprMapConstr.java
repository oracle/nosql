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
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Constructs a map containing the entries computed by the input pairs of exprs.
 *
 * The element type of the map to be constructed during runtiem is determined
 * during compilation time. Initially, this is done based only on the types of
 * the input exprs to this ExprMapConstr. However, just before codegen,
 * theMustBeJson will be set to true if the map is going to be inserted into
 * another constructed array or map whose element type is JSON (see 
 * ExprUtils.adjustConstructorTypes()).  In this case, the element type of the
 * constructed map will also be JSON. We do this to guarantee that strongly
 * type data does not get inserted into JSON data.
 *
 * If there are no input exprs, the type is ARRAY(JSON).
 *
 * theArgs:
 * For each even i, theArgs[i] is an expr that must return a string, which is
 * used as the name of the i-th field to be inserted in the map. theArgs[i+1]
 * is an expr that returns the value of the i-th field. If either theArgs[i] or
 * theArgs[i+1] returns an empty result, no field is added to the map. If 
 * theArgs[i+1] returns more than one items, implicit array construction is 
 * used so that the field value will be an array containing the returned items.
 */
public class ExprMapConstr extends Expr {

    final List<Expr> theArgs;

    private boolean theMustBeJson;

    public ExprMapConstr(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        List<Expr> args) {

        super(qcb, sctx, ExprKind.MAP_CONSTR, location);
        theArgs = args;

        int numArgs = args.size();

        for (int i = 0; i < numArgs; ++i) {

            Expr arg = args.get(i);

            if (i % 2 == 0) {
                arg = ExprPromote.create(null, arg, TypeManager.STRING_QSTN());
                theArgs.set(i, arg);

            } else if (arg.isMultiValued()) {
                ArrayList<Expr> arrargs = new ArrayList<Expr>(1);
                arrargs.add(arg);
                arg = new ExprArrayConstr(theQCB, theSctx, arg.theLocation,
                                          arrargs, true/*conditional*/);
                theArgs.set(i, arg);
            }

            arg.addParent(this);
        }

        /*
         * Must compute the type here to guarantee that 
         * theQCB.theHaveJsonConstructors will be set, if necessary, BEFORE
         * ExprUtils.adjustConstructorTypes() is called.
         */
        theType = computeType();
    }

    @Override
    public ExprMapConstr clone() {

        ArrayList<Expr> args = new ArrayList<Expr>(theArgs.size());
        for (Expr arg : theArgs) {
            args.add(arg.clone());
        }

        ExprMapConstr res = new ExprMapConstr(theQCB, theSctx, theLocation,
                                              args);
        res.theMustBeJson = theMustBeJson;
        return res;
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    int getNumArgs() {
        return theArgs.size();
    }

    int getNumKeys() {
        return theArgs.size()/2;
    }

    Expr getArg(int i) {
        return theArgs.get(i);
    }

    Expr getKey(int i) {
        return theArgs.get(2*i);
    }

    void setArg(int i, Expr newExpr, boolean destroy) {

        if (i % 2 == 0) {
            newExpr = ExprPromote.create(null, newExpr,
                                         TypeManager.STRING_QSTN());

        } else if (newExpr.isMultiValued()) {
            ArrayList<Expr> arrargs = new ArrayList<Expr>(1);
            arrargs.add(newExpr);
            newExpr = new ExprArrayConstr(theQCB, theSctx, newExpr.theLocation,
                                          arrargs, true/*conditional*/);
        }
        newExpr.addParent(this);
        theArgs.get(i).removeParent(this, destroy);
        theArgs.set(i, newExpr);
    }

    void removeArg(int i, boolean destroy) {
        if (!theArgs.get(i).getType().isEmpty()) {
            throw new QueryStateException(
                "Cannot remove non-empty input expr from map " +
                "constructor expr");
        }
        theArgs.get(i).removeParent(this, destroy);
        theArgs.remove(i);
    }

    void setJsonMapType() {
        theMustBeJson = true;
    }

    public MapDefImpl getMapType() {
        return (MapDefImpl)getType().getDef();
    }

    @Override
    ExprType computeType() {

        int numArgs = theArgs.size();

        if (numArgs == 0) {
            return TypeManager.createMapType(TypeManager.JSON_ONE(),
                                             Quantifier.ONE); 
        }

        ExprType elemType = theArgs.get(1).getType();

        for (int i = 3; i < numArgs; i += 2) {
            ExprType type = theArgs.get(i).getType();
            elemType = TypeManager.getConcatType(elemType, type);

            if (elemType.isAny()) {
                break;
            }
        }

        if (theMustBeJson &&
            elemType.getDef().isSubtype(FieldDefImpl.Constants.jsonDef)) {
            elemType = TypeManager.JSON_ONE();
        }

        if (elemType.isAnyJson()) {
            theQCB.theHaveJsonConstructors = true;
        }

        return TypeManager.createMapType(elemType, Quantifier.ONE);
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
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {
        for (int i = 0; i < theArgs.size(); ++i) {
            theArgs.get(i).display(sb, formatter);
            if (i < theArgs.size() - 1) {
                sb.append(",\n");
            }
        }
    }
}
