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

import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.types.ExprType;


/**
 * An internal sort expr, used to implement a generic (i.e. non-index-based)
 * sort. A generic sort is always done in-memory and at the client side.
 * See ExprSFW javadoc for more details.
 *
 * Notice that ExprSort does not store the sorting exprs. Instead, an ExprSort
 * will always appear on top of an ExprSFW, which must include all the sorting
 * exprs in its SELECT list. The ExprSort stores the positions of the sorting
 * exprs within this SELECT list. This approach is chosen because the generic
 * sort will be executed at the drivers, which do not implement all the
 * runtime iterators that may be needed to compute a sorting expr at runtime.
 *
 * theMatchesGroupBy:
 * Set to true if the SFW contains index-based group-by that sorts the results
 * in the same order as the order-by. If so, no sorting is required. Notice that
 * this check is done during ExprSFW.rewriteGroupBy(), which is before we know
 * whether the group-by will be indexed-based or not. There, we assume the
 * group-by will be index-based and set this flag to true if the order generated
 * by the grop-by is compatible with the order-by. Then, during code generation,
 * if this flag is set and the group-by was indeed index-based, the ExprSort will
 * be skipped.
 */
public class ExprSort extends Expr {

    private Expr theInput;

    private final int[] theSortFieldPositions;

    private final SortSpec[] theSortSpecs;

    private boolean theMatchesGroupBy;

    private final ExprBaseTable theTableExpr;

    public ExprSort(
        QueryControlBlock qcb,
        StaticContext sctx,
        Location loc,
        int[] sortFieldPositions,
        SortSpec[] sortSpecs,
        Expr tableExpr) {

        super(qcb, sctx, ExprKind.SORT, loc);
        theSortFieldPositions = sortFieldPositions;
        theSortSpecs = sortSpecs;
        if (tableExpr.getKind() == ExprKind.BASE_TABLE) {
            theTableExpr = (ExprBaseTable)tableExpr;
        } else {
            theTableExpr = null;
        }
    }

    public void setInput(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        if (theInput != null) {
            theInput.removeParent(this, destroy);
        }
        theInput = newExpr;
        computeType(false);
        setLocation(newExpr.getLocation());
    }

    @Override
    int getNumChildren() {
        return 1;
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    int getNumFields() {
        return ((RecordDefImpl)(getType().getDef())).getNumFields();
    }

    int getNumSortFields() {
        return theSortFieldPositions.length;
    }

    int[] getSortFieldPositions() {
        return theSortFieldPositions;
    }

    SortSpec[] getSortSpecs() {
        return theSortSpecs;
    }

    ExprBaseTable getTableExpr() {
        return theTableExpr;
    }

    void setMatchesGroupBy() {
        theMatchesGroupBy = true;
    }

    boolean matchesGroupBy() {
        return theMatchesGroupBy;
    }

    boolean isDescendingIndexScan() {

        for (int i = 0; i < theSortSpecs.length; ++i) {

            if (theSortSpecs[i].theIsDesc && theSortSpecs[i].theNullsFirst) {
                continue;
            }

            return false;
        }

        return true;
    }

    @Override
    ExprType computeType() {
        return theInput.getType();
    }

    @Override
    public boolean mayReturnNULL() {
        return theInput.mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("Sort Field Positions : ").append(theSortFieldPositions);
        sb.append(",\n");
        formatter.indent(sb);
        theInput.display(sb, formatter);
    }
}
