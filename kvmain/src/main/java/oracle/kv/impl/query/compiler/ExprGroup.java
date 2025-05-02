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
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;


/**
 * An internal expr, used to represent a generic (i.e. non-index-based)
 * group-by. During runtime, a  generic group-by is be performed in-memory at
 * the driver and (usually) at the RMs as well. It always needs to materialize
 * the full query result in the driver memory. See ExprSFW javadoc for more
 * details.
 *
 * theVar:
 * A "FROM" variable ranging over theInput.
 *
 * theInput:
 * The input expr. This will always be an SFW, or after distribution, a RECEIVE.
 *
 * theNumGroupByExprs:
 * The number of grouping exprs
 *
 * theFieldExprs:
 * Lists the grouping exprs followed by the aggregate functions. Normally,
 * during runtime, the values to group-by are the first N values in each
 * input tuple, and the values to aggregate-by are the remaining values in
 * the input tuple (that is, the GroupIter does not shuffle the columns it
 * receives from its input). There is at least one exception though (see
 * ExprSFW.optimizeMKIndexSizeCall()). In this case, the following 2 fields
 * are used.
 *
 * theFieldNames:
 * The names of the columns in the tuples produced by the GroupIter.
 *
 * theComputeFields:
 * True if the grouping and aggregation values are in arbitrary positions
 * within the input tuples. In this case, the GroupIter has to actually
 * evaluate the expressions in theFieldExprs in order to get the input
 * values in the expected order. 
 *
 * theIsDistinct:
 * Set to true if this ExprGroup implements a SELECT DISTINCT
 */
public class ExprGroup extends Expr {

    private ExprVar theVar;

    private Expr theInput;

    private int theNumGroupByExprs;

    private List<Expr> theFieldExprs;

    private String[] theFieldNames;

    private boolean theComputeFields;

    private boolean theIsDistinct;

    public ExprGroup(
        QueryControlBlock qcb,
        StaticContext sctx,
        Location loc,
        Expr input,
        int numGroupExprs) {

        super(qcb, sctx, ExprKind.GROUP, loc);
        theVar = new ExprVar(qcb, sctx, loc,
                             VarKind.FOR,
                             qcb.createInternalVarName("gb"),
                             input);
        theInput = input;
        theNumGroupByExprs = numGroupExprs;
        theFieldExprs = new ArrayList<Expr>(numGroupExprs + 6);
        input.addParent(this);
    }

    @Override
    int getNumChildren() {
        return 1 + theFieldExprs.size();
    }

    void setIsDistinct(boolean v) {
        theIsDistinct = v;
    }

    public boolean isDistinct() {
        return theIsDistinct;
    }

    public ExprVar getVar() {
        return theVar;
    }

    int getNumGroupExprs() {
        return theNumGroupByExprs;
    }

    void setNumGroupExprs(int v) {
        theNumGroupByExprs = v;
    }

    void setInput(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        if (theInput != null) {
            theInput.removeParent(this, destroy);
        }
        theInput = newExpr;
        theType = computeType();
        theVar.setDomainExpr(newExpr);
        setLocation(newExpr.getLocation());
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    int getNumFields() {
        return theFieldExprs.size();
    }

    public void addFields(List<Expr> exprs) {

        for (Expr e : exprs) {
            theFieldExprs.add(e);
            e.addParent(this);
        }
        computeType(false);
    }

    void addFields(Expr[] exprs, String[] names) {

        for (Expr e : exprs) {
            theFieldExprs.add(e);
            e.addParent(this);
        }
        theFieldNames = names;
        computeType(false);
    }

    void setField(int i, Expr newExpr, boolean destroy) {

        newExpr.addParent(this);
        theFieldExprs.get(i).removeParent(this, destroy);
        theFieldExprs.set(i, newExpr);

        computeType(false);
    }

    Expr getField(int i) {
        return theFieldExprs.get(i);
    }

    List<Expr> getFieldExprs() {
        return theFieldExprs;
    }

    public boolean getComputeFields() {
        return theComputeFields;
    }

    public void setComputeFields() {
        theComputeFields = true;
    }

    @Override
    ExprType computeType() {

        assert(theInput.getKind() == ExprKind.SFW ||
               theInput.getKind() == ExprKind.SORT ||
               theInput.getKind() == ExprKind.GROUP ||
               theInput.getKind() == ExprKind.RECEIVE);

        if (theFieldExprs.isEmpty()) {
            return null;
        }

        Quantifier q = theInput.getType().getQuantifier();
        FieldDefImpl inDef = theInput.getType().getDef();
        assert(inDef.isRecord());
        FieldMap fieldMap = new FieldMap();

        for (int i = 0; i < theFieldExprs.size(); ++i) {

            FieldDefImpl fieldDef = theFieldExprs.get(i).getType().getDef();
            boolean nullable = theFieldExprs.get(i).mayReturnNULL();

            if (fieldDef.isJson()) {
                theQCB.theHaveJsonConstructors = true;
            }

            String fieldName = (theFieldNames != null ?
                                theFieldNames[i] :
                                ((RecordDefImpl)inDef).getFieldName(i));       
            fieldMap.put(fieldName, fieldDef, nullable,
                         null/*defaultValue*/);
        }

        RecordDefImpl recDef = FieldDefFactory.createRecordDef(fieldMap,
                                                               null/*descr*/);
        ExprType type = TypeManager.createType(recDef, q);
        return type;
    }

    @Override
    public boolean mayReturnNULL() {

        if (theFieldExprs.size() > 1) {
            return false;
        }

        return theFieldExprs.get(0).mayReturnNULL();
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        for (int i = 0; i < theFieldExprs.size(); ++i) {
            formatter.indent(sb);
            theFieldExprs.get(i).display(sb, formatter);
            if (i < theFieldExprs.size() - 1) {
                sb.append(",\n");
            }
        }

        formatter.indent(sb);
        theInput.display(sb, formatter);
    }
}
