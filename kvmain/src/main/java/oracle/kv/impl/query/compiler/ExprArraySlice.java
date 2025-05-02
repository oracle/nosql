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
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.ExprType.TypeCode;
import oracle.kv.impl.query.types.TypeManager;


/**
 * Represents a "slicing" step in a path expression. A Slicing step selects
 * elements of arrays based only on the element positions.
 *
 * Syntactically, a slicing step looks like this:
 *
 * input_expr[low_expr? : high_expr?]
 *
 * Notice however that if both low_expr and high_expr are missing, the ":" is
 * actually not allowed, i.e., in this case the range step looks like this:
 *
 * input_expr[]
 *
 * low_expr and high_expr are called the "boundary" exprs. Each must return at
 * most one value of type Integer.
 *
 * The input_expr is supposed to return a sequence of zero or more items of any
 * kind. For each item in the input sequence, the step computes zero or more 
 * result items. The overall result of the step is the concatenation of the
 * results produced for each input item, in the order of their computation.
 *
 * Let V be the current item the step operates upon.
 *
 * 1. If V is not an array, V is inserted into an array that is constructed
 *    on the fly. The step is then applied to this single-item array.
 *
 * 2. If V is an array, the boundary exprs are are computed, if present. The
 *    boundary exprs may reference V via the $ variable. Note that if a
 *    boundary expr does not reference $, it does not need to be computed for
 *    each V; it can be computed only once, before any of the input values are
 *    processed.
 *
 *    Let L and H be the values returned by the low and high exprs, respectively.
 *    If the low_expr is absent or returns an empty result, L is set to 0. If
 *    the high_expr is absent or returns an empty result, H is set to the size
 *    of the array - 1. If L is &lt; 0, L is set to 0. If H &gt; array_size -
 *    1, H is set to array_size - 1.
 *
 *    After L and H are computed, the step selects all the elements between
 *    positions L and H. If L &gt; H no elements are selected.
 */
class ExprArraySlice extends Expr {

    private Expr theInput;

    private Expr theLowExpr;

    private Expr theHighExpr;

    private Long theLowValue;

    private Long theHighValue;

    private ExprVar theCtxItemVar;

    private boolean theIsUnarySlice;

    ExprArraySlice(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input) {

        super(qcb, sctx, ExprKind.ARRAY_SLICE, location);

        theInput = input;
        theInput.addParent(this);
    }

    /*
     * This constructor is used when converting a filter step to a single-pos
     * slice step.
     */
    ExprArraySlice(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input,
        Long pos) {

        super(qcb, sctx, ExprKind.ARRAY_SLICE, location);

        theInput = input;
        theInput.addParent(this);

        if (pos.intValue() < 0) {
            theLowValue = Long.valueOf(0);
            theHighValue = pos;
        } else {
            theLowValue = pos;
            theHighValue = pos;
        }

        theIsUnarySlice = true;
    }

    @Override
    public ExprArraySlice clone() {

        ExprArraySlice res = new ExprArraySlice(theQCB, theSctx, theLocation,
                                                theInput.clone());
        res.theLowExpr = Expr.cloneOrNull(theLowExpr);
        res.theHighExpr = Expr.cloneOrNull(theHighExpr);
        res.theLowValue = theLowValue;
        res.theHighValue = theHighValue;
        res.theCtxItemVar = theCtxItemVar;
        res.theIsUnarySlice = theIsUnarySlice;
        return res;
    }

    void addCtxVars(ExprVar ctxItemVar) {
        theCtxItemVar = ctxItemVar;
    }

    void addBoundaryExprs(Expr lowExpr, Expr highExpr) {

        theLowExpr = lowExpr;
        theHighExpr = highExpr;

        if (theLowExpr != null) {
            theLowExpr.addParent(this);
        }
        if (theHighExpr != null) {
            theHighExpr.addParent(this);
        }

        if (theLowExpr == null && theHighExpr == null) {
            theCtxItemVar = null;
        }

        checkConst();

        if (!isConst()) {
            if (theLowExpr != null) {
                theLowExpr = ExprPromote.create(
                    this, theLowExpr, TypeManager.LONG_QSTN());
            }

            if (theHighExpr != null) {
                theHighExpr = ExprPromote.create(
                    this, theHighExpr, TypeManager.LONG_QSTN());
            }
        }
    }

    @Override
    int getNumChildren() {

        if (theLowExpr == null && theHighExpr == null) {
            return 1;
        }

        if (theLowExpr == null || theHighExpr == null) {
            return 2;
        }

        return 3;
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

    Expr getLowExpr() {
        return theLowExpr;
    }

    void setLowExpr(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theLowExpr.removeParent(this, destroy);
        theLowExpr = newExpr;
    }

    void removeLowExpr(boolean destroy) {
        theLowExpr.removeParent(this, destroy);
        theLowExpr = null;
    }

    Expr getHighExpr() {
        return theHighExpr;
    }

    void setHighExpr(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theHighExpr.removeParent(this, destroy);
        theHighExpr = newExpr;
    }

    void removeHighExpr(boolean destroy) {
        theHighExpr.removeParent(this, destroy);
        theHighExpr = null;
    }

    Long getLowValue() {
        return theLowValue;
    }

    Long getHighValue() {
        return theHighValue;
    }

    ExprVar getCtxItemVar() {
        return theCtxItemVar;
    }

    boolean hasBounds() {
        return (theLowExpr != null || theHighExpr != null ||
                theLowValue != null || theHighValue != null);
    }

    boolean isConst() {
        return (theLowExpr == null && theHighExpr == null);
    }

    void checkConst() {

        if (isConst() && theLowValue != null && theLowValue != null) {
            assert(theCtxItemVar == null);
            return;
        }

        if (theLowValue == null) {
            if (theLowExpr == null) {
                theLowValue = Long.valueOf(0);
            } else if (theLowExpr.getKind() == ExprKind.CONST) {
                theLowValue = handleConstExpr(theLowExpr);
                if (theLowValue.longValue() < 0) {
                    theLowValue = Long.valueOf(0);
                }
                theLowExpr.removeParent(this, true/*destroy*/);
                theLowExpr = null;
            }
        }

        if (theHighValue == null) {
           if (theHighExpr == null) {
               theHighValue = Long.valueOf(Integer.MAX_VALUE);
           } else if (theHighExpr.getKind() == ExprKind.CONST) {
               theHighValue = handleConstExpr(theHighExpr);
               theHighExpr.removeParent(this, true/*destroy*/);
               theHighExpr = null;
           }
        }

        if (theLowValue != null &&
            theHighValue != null &&
            theLowValue.longValue() == theHighValue.longValue()) {

            theIsUnarySlice = true;

        }

        if (theCtxItemVar != null && !theCtxItemVar.hasParents()) {
            theCtxItemVar = null;
        }
    }

    private Long handleConstExpr(Expr expr) {

        TypeCode c = expr.getType().getCode();
        FieldValueImpl value = ((ExprConst)expr).getValue();

        if (c == TypeCode.INT || c == TypeCode.LONG) {
            return Long.valueOf(value.getLong());
        }
        throw new QueryException(
            "Boundary const in slice step has invalid type.\n" +
            "Expected long or integer type. Actual type is: \n" +
            expr.getType(), expr.getLocation());
    }

    @Override
    ExprType computeType() {

        ExprType inType = theInput.getType();

        checkConst();

        boolean isNotArray = 
            (inType.isAtomic() || inType.isRecord() || inType.isMap());

        if (theQCB.strictMode() && isNotArray) {
            throw new QueryException(
                "Wrong input type for [] operator. " +
                "Expected an array type. Actual type is :\n" +
                inType.getDef().getDDLString(), theLocation);
        }

        if (isConst()) {

            if (theLowValue.compareTo(theHighValue) > 0) {
                    return TypeManager.EMPTY();
            }

            if (theHighValue.compareTo(0L) < 0) {
                return TypeManager.EMPTY();
            }

            if (isNotArray && theLowValue.compareTo(0L) > 0) {
                return TypeManager.EMPTY();
            }
        }

        Quantifier inQuant = inType.getQuantifier();
        Quantifier outQuant = Quantifier.STAR;

        if (theIsUnarySlice || isNotArray) {
            outQuant = TypeManager.getUnionQuant(inQuant, Quantifier.QSTN);
        }

        return inType.getArrayElementType(outQuant);
    }

    @Override
    public boolean mayReturnNULL() {

        return (hasBounds() ||
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

        formatter.indent(sb);
        sb.append("type :\n");
        formatter.indent(sb);
        getType().display(sb, formatter);

        theInput.display(sb, formatter);

        if (isConst()) {
            sb.append("[");
            if (theLowValue != null || theHighValue != null) {
                if (theLowValue == theHighValue) {
                    sb.append(".").append(theLowValue).append(".");
                } else {
                    if (theLowValue != null) {
                        sb.append(theLowValue);
                    }
                    sb.append("..");
                    if (theHighValue != null) {
                        sb.append(theHighValue);
                    }
                }
            }
            sb.append("]");

        } else {
            sb.append(".\n");
            formatter.indent(sb);
            sb.append("[\n");
            if (theLowExpr != null) {
                theLowExpr.display(sb, formatter);
                sb.append("\n");
            }
            formatter.indent(sb);
            sb.append("..\n");
            if (theHighExpr != null) {
                theHighExpr.display(sb, formatter);
            }
            sb.append("]");
        }
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {
    }
}
