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
import oracle.kv.impl.query.types.ExprType.TypeCode;
import oracle.kv.impl.query.types.TypeManager;

/**
 * Represents a "filtering" step in a path expr. In general, a filtering step
 * selects elements of arrays by computing a predicate expression for each
 * element and selecting or rejecting the element depending on whether the
 * predicate result is true or false.
 */
class ExprArrayFilter extends Expr {

    private Expr theInput;

    private Expr thePredExpr;

    private Object theConstValue;

    private ExprVar theCtxItemVar;

    private ExprVar theCtxElemVar;

    private ExprVar theCtxElemPosVar;

    ExprArrayFilter(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input) {

        super(qcb, sctx, ExprKind.ARRAY_FILTER, location);
        theInput = input;
        theInput.addParent(this);
    }

    @Override
    public ExprArrayFilter clone() {

        ExprArrayFilter res = new ExprArrayFilter(theQCB, theSctx, theLocation,
                                                  theInput.clone());
        res.thePredExpr = Expr.cloneOrNull(thePredExpr);
        res.theConstValue = theConstValue;
        res.theCtxItemVar = theCtxItemVar;
        res.theCtxElemVar = theCtxElemVar;
        res.theCtxElemPosVar = theCtxElemPosVar;
        return res;
    }

    void addCtxVars(
        ExprVar ctxItemVar,
        ExprVar ctxElemVar,
        ExprVar ctxElemPosVar) {

        theCtxItemVar = ctxItemVar;
        theCtxElemVar = ctxElemVar;
        theCtxElemPosVar = ctxElemPosVar;
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
                !predType.isSubType(TypeManager.LONG_STAR()) &&
                !predType.isSubType(TypeManager.BOOLEAN_STAR())) {
                throw new QueryException(
                    "Predicate expression in filter step has invalid type:\n" +
                    "Expected type is integer, long, or boolean. Actual " +
                    "type is\n" + predType, pred.getLocation());
            }

            thePredExpr = ExprPromote.create(
                this, thePredExpr, TypeManager.ANY_ATOMIC_QSTN());
        }

        removeCtxVars();
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

        removeCtxVars();
        checkConst();
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

    ExprVar getCtxElemPosVar() {
        return theCtxElemPosVar;
    }

    Object getConstValue() {
        return theConstValue;
    }

    Long getNumericConst() {
        return (theConstValue instanceof Long ? (Long)theConstValue : null);
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
            assert(theCtxElemPosVar == null);
            return;
        }

        if (thePredExpr.getKind() == ExprKind.CONST) {

            TypeCode c = thePredExpr.getType().getCode();
            FieldValueImpl value = ((ExprConst)thePredExpr).getValue();

            if (c == TypeCode.INT || c == TypeCode.LONG) {
                theConstValue = Long.valueOf(value.getLong());
            } else if (c == TypeCode.BOOLEAN) {
                theConstValue = value;
            } else {
                throw new QueryException(
                    "Predicate expression in filter step has invalid type.\n" +
                    "Expected type is integer, long, or boolean. Actual " +
                    "type is\n" + thePredExpr.getType(),
                    thePredExpr.getLocation());
            }

            thePredExpr.removeParent(this, true/*destroy*/);
            thePredExpr = null;
            theCtxItemVar = null;
            theCtxElemVar = null;
            theCtxElemPosVar = null;
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

        if (theCtxElemPosVar != null && !theCtxElemPosVar.hasParents()) {
            theCtxElemPosVar = null;
        }
    }

    /**
     * Replace this with a slice step, if possible.
     */
    Expr convertToSliceStep() {

        if (getNumericConst() != null) {

            theInput.removeParent(this, false/*deep*/);

            Expr sliceStep = new ExprArraySlice(
                theQCB, theSctx, getLocation(), theInput, getNumericConst());

            replace(sliceStep, true/*destroy*/);
            return sliceStep;
        }

        if (thePredExpr != null && thePredExpr.getType().isNumeric()) {

            // TODO convert to slice step if the predExpr will always return
            // numbers and it does not depend on the context vars. We don't
            // do this yet, because the predExpr would become a common sub-
            // expression, which may create problems.
            return this;
        }

        return this;
    }

    @Override
    public ExprType computeType() {

        ExprType inType = theInput.getType();

        boolean isNotArray = 
            (inType.isAtomic() || inType.isRecord() || inType.isMap());

        if (theQCB.strictMode() && isNotArray) {
            throw new QueryException(
                "Wrong input type for [] operator. " +
                "Expected an array type. Actual type is :\n" +
                inType.getDef().getDDLString(), theLocation);
        }

        checkConst();

        if (isConst()) {
            if (theConstValue instanceof BooleanValueImpl) {
                boolean val = ((BooleanValueImpl)theConstValue).getBoolean();
                if (val == false) {
                    return TypeManager.EMPTY();
                }
            }
        }

        Quantifier inQuant = inType.getQuantifier();
        Quantifier outQuant = Quantifier.STAR;

       if (isNotArray) {
            outQuant = TypeManager.getUnionQuant(inQuant, Quantifier.QSTN);
        }

        return inType.getArrayElementType(outQuant);
    }


    @Override
    public boolean mayReturnNULL() {

        return (thePredExpr != null ||
                getNumericConst() != null ||
                theConstValue == BooleanValueImpl.falseValue ||
                theInput.mayReturnNULL() ||
                getType().isAny() ||
                getType().isAnyJson());
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void display(StringBuilder sb, DisplayFormatter formatter) {

        theInput.display(sb, formatter);

        if (isConst()) {
            sb.append("[");
            sb.append(theConstValue);
            sb.append("]");
        } else {
            sb.append(".\n");
            formatter.indent(sb);
            sb.append("[\n");
            formatter.incIndent();
            thePredExpr.display(sb, formatter);
            formatter.decIndent();
            sb.append("\n");
            formatter.indent(sb);
            sb.append("]");
        }
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {
    }
}
