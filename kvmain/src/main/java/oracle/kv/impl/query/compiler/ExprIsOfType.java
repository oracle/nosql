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

import java.util.List;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldDef;

/**
 * This is the IS OF TYPE expr. Its main purpose is to check for the type of
 * its input expression. Its semantics are as follows:
 *
 * Check that the sequence of items produced by the input expression "conforms"
 * with at least one of the target types specified in the is-type-of expr. The
 * sequence conforms with a target type T if (a) its cardinality conforms with
 * the quantifier of T, and (b) the type of each item in the  sequence is a 
 * subtype of T, if ONLY is not specified, or is equal to T, if ONLY is 
 * specified.
 */
class ExprIsOfType extends Expr {

    private Expr theInput;

    private List<FieldDef> theTargetTypes;
    private List<ExprType.Quantifier> theTargetQuantifiers;
    private List<Boolean> theOnlyTargetFlags;
    private boolean theNotFlag;


    ExprIsOfType(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location loc,
        Expr input,
        boolean notFlag,
        List<FieldDef> types,
        List<ExprType.Quantifier> quantifiers,
        List<Boolean> onlyFlags) {

        super(qcb, sctx, ExprKind.IS_OF_TYPE, loc);

        theInput = input;
        theInput.addParent(this);
        theNotFlag = notFlag;
        theTargetTypes = types;
        theTargetQuantifiers = quantifiers;
        theOnlyTargetFlags = onlyFlags;
    }

    @Override
    public ExprIsOfType clone() {
        return new ExprIsOfType(theQCB, theSctx, theLocation, theInput.clone(),
                                theNotFlag, theTargetTypes, theTargetQuantifiers,
                                theOnlyTargetFlags);
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

    List<FieldDef> getTargetTypes() {
        return theTargetTypes;
    }

    List<ExprType.Quantifier> getTargetQuantifiers() {
        return theTargetQuantifiers;
    }

    List<Boolean> getOnlyTargetFlags() {
        return theOnlyTargetFlags;
    }

    boolean isNot() {
        return theNotFlag;
    }

    @Override
    ExprType computeType() {

        return TypeManager.BOOLEAN_ONE();
    }

    @Override
    public boolean mayReturnNULL() {
        return theInput.mayReturnNULL();
    }

    @Override
    public boolean mayReturnEmpty() {
        return false;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        theInput.display(sb, formatter);
        sb.append("\n");
        formatter.indent(sb);
        sb.append(" IS ");
        if (theNotFlag) {
            sb.append("NOT ");
        }
        sb.append("OF TYPE (");
        for(int i = 0; i < theTargetTypes.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            if (theOnlyTargetFlags.get(i)) {
                sb.append("ONLY ");
            }
            ((FieldDefImpl)theTargetTypes.get(i)).display(sb, formatter);
            sb.append(theTargetQuantifiers.get(i).toString());
        }

        sb.append("),\n");
    }
}
