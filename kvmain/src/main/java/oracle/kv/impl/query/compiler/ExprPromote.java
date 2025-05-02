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
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.ExprType.TypeCode;
import oracle.kv.impl.query.types.TypeManager;

/*
 * This is an internal expr. Its main purpose is to do type checking and
 * type promotion on the arguments of function-call expressions (ExprFnCall).
 * Its semantics are as follows:
 *
 * a. Check that the cardinality of the input set conforms with the quantifier
 *    of the target type.
 *
 * b. Check that each value on the input set is a subtype of the target type,
 *    or is promotable to the target type. If so, the promotion is performed
 *    via casting. The following promotions are allowed:
 *    - Integer to Float or Double
 *    - Long to Float or Double
 *
 * c. Raise an error if either of the above checks fail.
 *
 * d. Pass on to the parent expression each input value (or the corresponding
 *    promoted value) that passes the type checks.
 */
class ExprPromote extends Expr {

    private Expr theInput;

    private ExprType theTargetType;

    /*
     * Inject a promote expr between the given pair of parent and child exprs.
     * The target type is the result type that the parent expects from the
     * child. A promote expr is actually created only if the type of the child
     * is not a subtype of the target type.
     *
     * The parent must be null if the child is not yet connected to the parent.
     * Typically this happens when the method is called from the constructor of
     * the parent expr.
     */
    static Expr create(Expr parent, Expr child, ExprType targetType) {

        if (child.getKind() == ExprKind.CONST) {

            FieldValueImpl val = ((ExprConst)child).getValue();
            FieldDefImpl valDef = val.getDefinition();

            FieldValueImpl newVal = TypeManager.promote(val, targetType);

            if (newVal == null) {
                throw new QueryException(
                    "Cannot promote item " + val +
                    "\nof type :\n" + valDef.getDDLString() +
                    "\nto type :\n" + targetType, child.getLocation());
            }

            ((ExprConst)child).setValue(newVal);
            return child;
        }

        ExprType childType = child.getType();

        if (childType.isSubType(targetType)) {
            return child;
        }

        /*
         * Throw exception if the intersection of the input and target types
         * is empty and the input type is not promotable to the target type.
         */
        if (!TypeManager.typesIntersect(childType, targetType)) {

            TypeCode cc = childType.getCode();
            TypeCode tc = targetType.getCode();

            if ((cc == TypeCode.INT || cc == TypeCode.LONG) &&
                (tc == TypeCode.FLOAT || tc == TypeCode.DOUBLE)) {
                // int and long are normally promotable to float and double
            } else if (cc == TypeCode.STRING && tc == TypeCode.ENUM) {
                // string may be promotable to enum
            } else {
                throw new QueryException(
                    "Cannot promote type :\n" + childType +
                    "\nto type :\n" + targetType, child.getLocation());
            }
        }

        if (parent != null) {
            child.removeParent(parent, false/*deep*/);
        }

        Expr promoteExpr = new ExprPromote(
            child.getQCB(), child.getSctx(), child, targetType);

        if (parent != null) {
            promoteExpr.addParent(parent);
        }

        return promoteExpr;
    }

    ExprPromote(
        QueryControlBlock qcb,
        StaticContext sctx,
        Expr input,
        ExprType type) {

        super(qcb, sctx, ExprKind.PROMOTE, input.getLocation());

        theTargetType = type;
        theInput = input;
        theInput.addParent(this);
    }

    @Override
    public ExprPromote clone() {

        return new ExprPromote(theQCB, theSctx, theInput.clone(), theTargetType);
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
        setLocation(newExpr.getLocation());
    }

    ExprType getTargetType() {
        return theTargetType;
    }

    @Override
    ExprType computeType() {

        ExprType inType = theInput.getType();

        ExprType inputItemType = inType.getItemType();
        ExprType targetItemType = theTargetType.getItemType();

        Quantifier q = TypeManager.getIntersectionQuant(
            inType.getQuantifier(), theTargetType.getQuantifier());

        if (inputItemType.isSubType(targetItemType)) {
            return TypeManager.createType(inputItemType, q);
        }

        return TypeManager.createType(targetItemType, q);
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
        sb.append(theTargetType);
        sb.append(",\n");
        theInput.display(sb, formatter);
    }
}
