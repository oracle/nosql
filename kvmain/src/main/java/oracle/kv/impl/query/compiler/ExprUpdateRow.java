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
import java.util.Map;

import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TablePath.StepKind;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.Index;

/**
 * Implements the update_statement, together with ExprUpdateField.
 *
 * ExprUpdateField and the associated FieldUpdateIter implement the non-TTL
 * update clauses (SET, ADD, PUT, or REMOVE). The number of ExprUpdateFields
 * is the same as the number of such clauses listed in the update stmt. Each
 * ExprUpdateField evaluates the expressions that appear in the associated
 * clause and applies the update on the target items (or removes the target
 * items in the case of REMOVE). These updates take place on the deserialized
 * image of a row. For SET and REMOVE, the FieldUpdateIter must know the
 * parent of the target item and the "position" of the target item within
 * its parent. The FieldUpdateIter can obtain this info by calling the
 * PlanIter.getParentItemContext() method on the target expr (see javadoc
 * for PlanIter.getParentItemContext()).
 *
 * ExprUpdateRow and the associated UpdateRowIter implement the update
 * statement as a whole. There is a single ExprUpdateRow per update stmt.
 * It's input is a SFW that produces the rows to be updated (this SFW
 * corresponds to the WHERE clause of the update stmt). This input SFW is
 * stored as the 1st expr in theArgs list. The rest of theArgs are the
 * ExprUpdateFields that correspond to the update clauses. The ExprUpdateRow
 * drives the execution of the update stmt by requesting rows from the input
 * SFW, invoking each ExprUpdateField in turn on each row, and then uses
 * a PutHandler to "commit" the updated row (i.e. serialize it and write it
 * to the log).
 *
 * Note: If there are any SET TTL clauses, only the last one is taken into
 * account (the others are ignored). This SET TTL clause is modeled as an
 * ExprUpdateField and is stored as the last entry in theArgs. However, this
 * ExprUpdateField is basically a placeholder for the TTL expr and the update
 * kind: no UpdateFieldIter is generated for it; instead, the actual work is
 * done by the UpdateRowIter.
 *
 * If the update stmt has a RETURNING clause, another SFW is created on top
 * of the ExprUpdateRow in order to do the projection over the updated row.
 * So, an update stmt of the form:
 *
 * update tab_name tab_alias
 * update_clauses
 * where cond
 * returning select_list
 *
 * gets translated to the following form:
 *
 * select select_list
 * from (update update_clauses
 *       from (select tab_alias
 *             from tab_name tab_alias
 *             where cond)
 *      ) tab_alias
 *
 */
public class ExprUpdateRow extends Expr {

    /*
     * The type of the result when there is no RETURNING clause. It's a record
     * with just one field, named "NumRowsUpdated", whose value is the number
     * of rows updated (currently it can be only 1 or 0).
     */
    public static RecordDefImpl theNumRowsUpdatedType;

    static {
        FieldMap fmap = new FieldMap();
        fmap.put("NumRowsUpdated", FieldDefImpl.Constants.integerDef, false,
                 FieldDefImpl.Constants.integerDef.createInteger(1));
        theNumRowsUpdatedType = FieldDefFactory.createRecordDef(fmap, null);
    }

    private TableImpl theTable;

    private ArrayList<Expr> theArgs;

    private boolean theUpdateTTL;

    private boolean theHasReturningClause;

    /* The following 3 fields are used to optimize the UPDATE statement by
     * avoiding updating indexes that do not need to be updated. theAllIndexes
     * stores the names of all the indexes defined on the table at the time the
     * query was compiled and theIndexIds stores the ids of those indexes.
     * theIndexesToUpdate array mirrows theAllIndexes. If
     * theIndexesToUpdate[i] == false, then the index in theAllIndexes[i] does
     * not need to be updated. 
     *
     * Note that at runtime, the set of indexes on the table may be different.
     * Specifically, to handle cases where an index is created after the query
     * was compiled, the set of indexes on the table is computed again (see
     * TableManager.getSecondaries() method). Let S be this set. Then, an
     * index is updated only if:
     * (a) it is in S, but not in theAllIndexes (i.e. it's a new index), or
     * (b) it is in both S and theAllIndexes, and theIndexesToUpdate entry for
     *     this index is true, or
     * (c) it is in both S and theAllIndexes but the index ids are different,
     *     This handles the case where an index is dpropped and then re-created
     *     with different schema. */
    private String[] theIndexNames;

    private long[] theIndexIds;

    private boolean[] theIndexesToUpdate;

    private boolean theIsCompletePrimaryKey;

    public ExprUpdateRow(
        QueryControlBlock qcb,
        StaticContext sctx,
        Location location,
        ExprSFW input,
        TableImpl table,
        boolean hasReturningClause) {

        super(qcb, sctx, ExprKind.UPDATE_ROW, location);

        theTable = table;
        theArgs = new ArrayList<Expr>(8);
        theArgs.add(input);
        input.addParent(this);
        theHasReturningClause = hasReturningClause;

        if (hasReturningClause) {
            theType = TypeManager.createTableRecordType(table, Quantifier.QSTN);
        } else {
            theType = TypeManager.createType(theNumRowsUpdatedType,
                                             Quantifier.ONE);
        }
    }

    TableImpl getTable() {
        return theTable;
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    @Override
    Expr getInput() {
        return theArgs.get(0);
    }

    void addUpdateClauses(ArrayList<Expr> clauses, boolean updateTTL) {

        for (Expr clause : clauses) {
            theArgs.add(clause);
            clause.addParent(this);
        }

        theUpdateTTL = updateTTL;
    }

    Expr getArg(int i) {
        return theArgs.get(i);
    }

    void setArg(int i, Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theArgs.get(i).removeParent(this, destroy);
        theArgs.set(i, newExpr);
    }

    boolean updateTTL() {
        return theUpdateTTL;
    }

    ExprUpdateField getTTLExpr() {
        ExprUpdateField e = (ExprUpdateField)theArgs.get(theArgs.size() - 1);
        if (e.getUpdateKind() == UpdateKind.TTL_HOURS ||
            e.getUpdateKind() == UpdateKind.TTL_DAYS) {
            return e;
        }
        return null;
    }

    boolean hasReturningClause() {
        return theHasReturningClause;
    }

    public String[] indexNames() {
        return theIndexNames;
    }

    public long[] indexIds() {
        return theIndexIds;
    }

    public boolean[] indexesToUpdate() {
        return theIndexesToUpdate;
    }

    void findIndexesToUpdate() {

        Map<String, Index> indexes = theTable.getIndexes();

        theIndexNames = new String[indexes.size()];
        theIndexIds = new long[indexes.size()];
        theIndexesToUpdate = new boolean[indexes.size()];

        int idx = 0;
        for (Index index : indexes.values()) {
           theIndexNames[idx] = index.getName();
           theIndexIds[idx] = ((IndexImpl)index).getId();
           theIndexesToUpdate[idx] = false;
           ++idx;
        }

        if (theUpdateTTL) {
            for (idx = 0; idx < indexes.size(); ++idx) {
                theIndexesToUpdate[idx] = true;
            }
            return;
        }

        int numClauses = theArgs.size();

        idx = 0;
        for (Index index : indexes.values()) {

            IndexImpl index2 = (IndexImpl)index;
            List<IndexField> indexPaths = index2.getIndexFields();

            for (IndexField ipath : indexPaths) {

                Function func = ipath.getFunction();
                if (func != null && func.isRowProperty()) {
                    theIndexesToUpdate[idx] = true;
                    break;
                }

                for (int i = 1; i < numClauses; ++i) {

                    ExprUpdateField clause = (ExprUpdateField)theArgs.get(i);
                    Expr targetExpr = clause.getInput();
                    IndexExpr targetPath = targetExpr.getIndexExpr();
                    assert(targetPath != null);
                    UpdateKind updKind = clause.getUpdateKind();

                    if (updKind == UpdateKind.PUT) {

                        /* Check if the new value expr is a map constructor
                         * with all of its key exprs being literal strings.
                         * see qtf query array_index/q/upd10 */
                        Expr newValueExpr = clause.getNewValueExpr();

                        if (newValueExpr.getKind() == ExprKind.MAP_CONSTR) {
                            ExprMapConstr mapConstr = (ExprMapConstr)newValueExpr;
                            int numKeys = mapConstr.getNumKeys();
                            ArrayList<String> mapKeys = new ArrayList<>(numKeys);

                            for (int k = 0; k < numKeys; ++k) {
                                Expr keyExpr = mapConstr.getKey(k);
                                if (!(keyExpr.getKind() == ExprKind.CONST)) {
                                    mapKeys.clear();
                                    break;
                                }
                                mapKeys.add(((ExprConst)keyExpr).getString());
                            }

                            if (!mapKeys.isEmpty() || numKeys == 0) {

                                for (String mapKey : mapKeys) {
                                    targetPath.add(mapKey, StepKind.MAP_FIELD,
                                                   null);

                                    if (targetPath.matchUpdateTargetToIndexPath(
                                            ipath, updKind)) {
                                        theIndexesToUpdate[idx] = true;
                                        targetPath.remove();
                                        break;
                                    }
                                    targetPath.remove();
                                }

                                if (theIndexesToUpdate[idx]) {
                                    break; // break the clauses loop
                                }
                                continue; // check the next clause
                            }
                        }
                    }

                    if (targetPath.matchUpdateTargetToIndexPath(
                            ipath, updKind)) {
                        theIndexesToUpdate[idx] = true;
                        break; // break the clauses loop
                    }
                }

                if (theIndexesToUpdate[idx]) {
                    break; // break the index paths loop
                }
            }

            ++idx;
        }

        /*
        for (int i = 0; i < theIndexNames.length; ++i) {
            if (theIndexesToUpdate[i]) {
                System.out.println("Update index " + theIndexNames[i]);
            }
        }
        */
    }

    void setIsCompletePrimarykey(boolean completePrimaryKey) {
        theIsCompletePrimaryKey = completePrimaryKey;
    }

    boolean isCompletePrimarykey() {
        return theIsCompletePrimaryKey;
    }

    @Override
    ExprType computeType() {
        return theType;
    }

    @Override
    boolean mayReturnNULL() {
        return false;
    }

    @Override
    public boolean mayReturnEmpty() {
        return false;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        for (int i = 1; i < theArgs.size(); ++i) {
            theArgs.get(i).display(sb, formatter);
            if (i < theArgs.size() - 1) {
                sb.append(",\n");
            }
        }

        sb.append("\n");
        theArgs.get(0).display(sb, formatter);
    }
}
