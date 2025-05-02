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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Represents a map/record filtering step in a path expr.
 *
 * A map filtering step selects entries form its input map or record item and
 * returns either the keys or the values of the selected entries. Entries are
 * filtered by computing a predicate expr on each entry and selecting or
 * rejecting the entry depending on whether the predicate result is true or
 * false. During the computation of the predicate, the key and value of the
 * entry are available via the $key and $element variables. The whole map/record
 * item is also avalable via the $ variable.
 *
 * If an inout item is not a map/record then (a) if it is NULL, NULL is returned,
 * (b) if it's an atomic item, it is skipped, (c) if it is an array, the filtering
 * is applied recursively on the elements of the array. 
 */
public class ExprMapFilter extends Expr {

    public static enum FilterKind {
        KEYS,
        VALUES,
        KEYVALUES;

        private static final FilterKind[] ALL_VALUES = values();
        public static final int VALUES_COUNT = ALL_VALUES.length;
        public static FilterKind valueOf(int ordinal) {
            return ALL_VALUES[ordinal];
        }
    }

    private FilterKind theFilterKind;

    private Expr theInput;

    private Expr thePredExpr;

    private boolean theConstValue;

    private ExprVar theCtxItemVar;

    private ExprVar theCtxElemVar;

    private ExprVar theCtxKeyVar;

    public ExprMapFilter(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        FilterKind kind,
        Expr input) {

        super(qcb, sctx, ExprKind.MAP_FILTER, location);
        theFilterKind = kind;
        theInput = input;
        theInput.addParent(this);
        theConstValue = true;
    }

    @Override
    public ExprMapFilter clone() {

        ExprMapFilter res = new ExprMapFilter(theQCB, theSctx, theLocation,
                                              theFilterKind, theInput.clone());
        res.thePredExpr = Expr.cloneOrNull(thePredExpr);
        res.theConstValue = theConstValue;
        res.theCtxItemVar = theCtxItemVar;
        res.theCtxElemVar = theCtxElemVar;
        res.theCtxKeyVar = theCtxKeyVar;
        return res;
    }

    void addCtxVars(
        ExprVar ctxItemVar,
        ExprVar ctxElemVar,
        ExprVar ctxKeyVar) {

        theCtxItemVar = ctxItemVar;
        theCtxElemVar = ctxElemVar;
        theCtxKeyVar = ctxKeyVar;
    }

    void addPredExpr(Expr pred) {

        assert(thePredExpr == null && pred != null);
        thePredExpr = pred;
        thePredExpr.addParent(this);

        checkConst();

        if (!isConst()) {

            ExprType predType = thePredExpr.getType();

            if (!predType.isAny() &&
                !predType.isAnyJson() &&
                !predType.isAnyAtomic() &&
                !predType.isAnyJsonAtomic() &&
                !predType.isSubType(TypeManager.BOOLEAN_STAR())) {
                throw new QueryException(
                    "Predicate expression in filter step has invalid type:\n" +
                    "Expected type is boolean. Actual " +
                    "type is\n" + predType, pred.getLocation());
            }

            thePredExpr = ExprPromote.create(
                this, thePredExpr, TypeManager.BOOLEAN_QSTN());
        }

        removeCtxVars();
    }

    public FilterKind getFilterKind() {
        return theFilterKind;
    }

    @Override
    int getNumChildren() {
        return (thePredExpr != null ? 2 : 1);
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

    Expr getPredExpr() {
        return thePredExpr;
    }

    void setPredExpr(Expr newExpr, boolean destroy) {

        newExpr.addParent(this);
        thePredExpr.removeParent(this, destroy);
        thePredExpr = newExpr;

        checkConst();
        removeCtxVars();
    }

    void removePredExpr(boolean destroy) {

        thePredExpr.removeParent(this, destroy);
        thePredExpr = null;

        removeCtxVars();
        checkConst();
    }

    ExprVar getCtxItemVar() {
        return theCtxItemVar;
    }

    ExprVar getCtxElemVar() {
        return theCtxElemVar;
    }

    ExprVar getCtxKeyVar() {
        return theCtxKeyVar;
    }

    boolean getConstValue() {
        return theConstValue;
    }

    boolean isConst() {
        return thePredExpr == null;
    }

    /**
     * Check whether the pred expr is a const, and if so, store its const value
     * in theConstValue, and throw away the pred expr and the context vars.
     */
    void checkConst() {

        if (isConst()) {
            assert(theCtxItemVar == null);
            assert(theCtxElemVar == null);
            assert(theCtxKeyVar == null);
            return;
        }

        if (thePredExpr.getKind() == ExprKind.CONST) {

            FieldValueImpl value = ((ExprConst)thePredExpr).getValue();

            if (value.isBoolean()) {
                theConstValue = ((BooleanValueImpl)value).getBoolean();
            } else {
                throw new QueryException(
                    "Predicate expression in filter step has invalid type.\n" +
                    "Expected type is boolean. Actual " +
                    "type is\n" + thePredExpr.getType(),
                    thePredExpr.getLocation());
            }

            thePredExpr.removeParent(this, true/*destroy*/);
            thePredExpr = null;
            theCtxItemVar = null;
            theCtxElemVar = null;
            theCtxKeyVar = null;
        }
    }

    /**
     * Remove the context variables if they are not used anywhere.
     */
    void removeCtxVars() {

        if (theCtxItemVar != null && !theCtxItemVar.hasParents()) {
            theCtxItemVar = null;
        }

        if (theCtxElemVar != null && !theCtxElemVar.hasParents()) {
            theCtxElemVar = null;
        }

        if (theCtxKeyVar != null && !theCtxKeyVar.hasParents()) {
            theCtxKeyVar = null;
        }
    }


    @Override
    public ExprType computeType() {

        ExprType inType = theInput.getType();

        if (inType.isEmpty()) {
            return TypeManager.EMPTY();
        }

        checkConst();

        if (isConst() && theConstValue == false) {
            return TypeManager.EMPTY();
        }

        while (inType.isArray()) {
            inType = inType.getArrayElementType(Quantifier.STAR);
        }

        if (theQCB.strictMode() && inType.isAtomic()) {
            throw new QueryException(
                "Wrong input type for path step " +
                "(must be a record, array, or map type): " +
                inType.getDef().getDDLString(), theLocation);
        }

        if (theFilterKind == FilterKind.KEYS) {
            return TypeManager.STRING_STAR();
        }

        return inType.getMapElementType(Quantifier.STAR);
    }


    @Override
    public boolean mayReturnNULL() {

        return (theInput.mayReturnNULL() ||
                getType().isAny() ||
                getType().isAnyJson());
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void display(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        if (theFilterKind == FilterKind.KEYS) {
            sb.append("KEYS\n");
        } else {
            sb.append("VALUES\n");
        }

        formatter.indent(sb);
        sb.append("[\n");
        formatter.incIndent();

        formatter.indent(sb);
        sb.append("type :\n");
        formatter.indent(sb);
        getType().display(sb, formatter);
        sb.append("\n");

        theInput.display(sb, formatter);

        sb.append(".\n");
        formatter.indent(sb);


        if (isConst()) {
            sb.append(theConstValue);
        } else {
            thePredExpr.display(sb, formatter);
        }

        sb.append("\n");
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("]");
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {
    }
}
