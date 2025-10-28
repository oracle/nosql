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
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;

/*
 * Represents a SELECT expression, aka query block. A SELECT expression will
 * have at least SELECT and FROM clauses, and may also have WHERE, GROUP BY,
 * ORDER BY, LIMIT, and OFFSET clauses (in this order).
 *
 * When a SELECT expr contains ORDER BY and/or GROUP BY, the single ExprSFW
 * that is created during its translation may be later be split into a stack
 * consiting of one or more ExprSFWs, at most one ExprSort, and at most one
 * ExprGroup.
 *
 * Implementation of Index-based GroubBy:
 *
 * When a group-by query is using an index that sorts the rows by the grouping
 * expressions, the group-by is implemented with 2 SFW exprs. The lower/inner
 * SFW is called the "grouping" SFW; it applies the WHERE clause and does the
 * grouping. Its SELECT list consists of the grouping exprs, followed by the
 * aggregate functions that appear in the original SELECT/ORDER-BY lists (the
 * SELECT/ORDER-BY lists in the user's query). The upper/outer SFW is called
 * the "selecting" SFW; its SELECT list contains the exprs of the original
 * SELECT/ORDER0-BY list. For example the query:
 *
 * select a + b, sum(c+d) / count(*)
 * from foo
 * where e > 10
 * group by a, b
 *
 * is rewritten as:
 *
 * select gb-0 + gb-1, aggr-2 / aggr-3
 * from (select a as gb-0, b as gb-1, sum(c+d) as aggr-2, count(*) as aggr-3
 *       from foo
 *       where e > 10
 *       group by a, b)
 *
 * If the original SELECT list is identical to the SELECT list of the inner
 * SFW, then no outer SFW is added. For example, no outer SFW is needed for
 * this query:
 *
 * select a, b, sum(c+d), count(*)
 * from foo
 * where e > 10
 * group by a, b
 *
 * The above rewrite is done right after translation time (see rewriteGroupBy()
 * method). During distribution time, another SFW is added if the query is
 * ALL_PARTITIONS or ALL_SHARDS. This SFW is called the "regrouping" SFW; it
 * regroups the partial groups that arrive from the RNs. So, after distribution,
 * the 1st query above will look like this:
 *
 * select gb-0 + gb-1, aggr-2 / aggr-3
 * from (select gb-0, gb-1, sum(aggr-2) as aggr-2, sum(aggr-3) as aggr-3
 *       from RECEIVE(select a as gb-0,
 *                           b as gb-1,
 *                           sum(c+d) as aggr-2,
 *                           count(*) as aggr-3
 *                    from foo
 *                    where e > 10
 *                    group by a, b))
 *
 * Implementation of Generic GroubBy:
 *
 * When a group-by query is not using an index that sorts the rows by the
 * grouping expressions, grouping is done by a special GroupIter. First,
 * the SFW is split into two SFWs, exactly the same way as in the index-based
 * case described above. After index selection, we know if the selected index
 * can be used for the group by or not. If not, an ExprGroup is inserted
 * between the 2 SFWs and the grouping expressions and aggregate functions are
 * moved from the inner SFW to this ExprGroup (see addGenericGroupBy() method).
 * For example the query:
 *
 * select a + b + c, sum(f+d) / count(*)
 * from foo
 * where e > 10
 * group by a + b, c
 *
 * is rewritten as:
 *
 * select gb-0 + gb-1, aggr-2 / aggr-3
 * from group-by gb-0, gb-1, sum(aggr-2) as aggr-2, count(*) as aggr-3
 *      from select a+b as gb-0, c as gb-1, f+d as aggr-2, 1 as aggr-3
 *           from foo
 *           where a > 10
 *
 * During distribution, an additional ExprGroup is added to do partial grouping
 * at the RNs. For example, the final form of the above query is:
 *
 * select gb-0 + gb-1, aggr-2 / aggr-3
 * from group-by gb-0, gb-1, sum(aggr-2) as aggr-2, sum(aggr-3) as aggr-3
 *      from RECEIVE(group-by gb-0, gb-1, sum(aggr-2) as aggr-2, count(*) as aggr-3
 *                   from select a+b as gb-0, c as gb-1, f+d as aggr-2, 1 as aggr-3
 *                        from foo
 *                        where a > 10)
 *
 * However, a server-side group-by will not possible be added if the query uses a
 * multi-key index that requires duplicate elimination. This is because duplicate
 * elimination must be done before grouping and must be done at the driver.
 *
 * For both index-based and generic group-by, if the query contains order-by as
 * well, the sort cannot be index-based. In this case, before splitting the SFW,
 * the rewriteGroupBy() method adds a generic sort on top of the SFW.
 * Specifically, it first adds to the SELECT list any sorting exprs that do
 * not appear there already. Then it creates an ExprSort on top of the SFW,
 * Next, if any extra sorting exprs were added to the SELECT list, a new SFW
 * is added on top of the ExprSort to project out the extra exprs. Finally,
 * the SFW is split as described above. For example the query:
 *
 * select a + b, sum(c+d) / count(*)
 * from foo
 * where e > 10
 * group by a, b
 * order by sum(g)
 *
 * will be rewritten as follows (assuming index-based group-by):
 *
 * select col-1, col-2
 * from sort by aggr-4
 *      from select gb-0 + gb-1 as col-1, aggr-2 / aggr-3 as col-2, aggr-4
 *           from select gb-0, gb-1,
 *                       sum(aggr-2) as aggr-2,
 *                       sum(aggr-3) as aggr-3,
 *                       sum(aggr-4) as aggr-4
 *                from RECEIVE(select a as gb-0,
 *                                    b as gb-1,
 *                                    sum(c+d) as aggr-2,
 *                                    count(*) as aggr-3,
 *                                    sum(g) as aggr-4
 *                             from foo
 *                             where e > 10
 *                             group by a, b)
 *
 * theNumChildren:
 * The total number of subexprs for this SFW. It counts the number of exprs in
 * all the clauses of this SFW.
 *
 * theFromClauses:
 * There is a FromClause for each table expression appearing in the FROM clause
 * of the actual query block. For now, the first of these table exprs is always
 * the name of a KVS table or a NESTED TABLES clause (see ExprBaseTable). Any
 * exprs following the first one cannot reference any table names (i.e., no
 * joins among tables in different table hierarchies are supported yet)
 *
 * theWhereExpr:
 * The expr appearing in the WHERE clause.
 *
 * theGroupExprs:
 * The exprs appearing in the GROUP-BY clause.
 *
 * theNumGroupExprs:
 * This SFW performs grouping if and only if theNumGroupExprs is >= 0. If
 * theNumGroupExprs == 0, there is no GROUP-BY clause, but the SELECT list
 * contains aggregate functions, and as a result, all the table rows are
 * considered as belonginh to one group.
 *
 * theNeedOuterSFWForGroupBy:
 * A transient field used during rewriteGroupBy() to check whether the outer
 * "selecting" SFW is needed.
 *
 * theGroupByExprCompleteShardKey:
 *
 * theHasGenericGroupBy:
 * If a generic group by is added, this field is set to true inside the inner-
 * most ExprSFW of the SELECT expr stack. This is done because the grouping is
 * removed from this inner-most ExprSFW, but we need to know during distribution
 * of the inner-most SFW whether the SELECT stack contains generic grouping.
 *
 * theIsGroupingForDistinct:
 * Set to true if the grouping done by this SFW is for the purpose of a DISTINCT
 * in the SELECT clause. If so, a grouping expr returning nothing on a row should
 * not cause that row to be skipped. Instead the EMPTY should be converted to
 * NULL.
 *
 * theFieldNames:
 * The names associated with the exprs appearing in the SELECT clause. These
 * names are either explicitly specified by the user, via AS keywords, or they
 * created internally when AS is not used.
 *
 * theFieldExprs:
 * The exprs appearing in the SELECT clause.
 *
 * theIsSelectStar:
 *
 * theConstructsRecord
 * True if the SELECT clause constructs a record. Normally it is true, but
 * will be false if the SELECT clause is a "SELECT *".
 *
 * theDoNullOnEmpty:
 * Normally, if a SELECT expr returns nothing, NULL is used as the result of
 * that expr. This creates a problem for order-by queries that are run on
 * multiple partitions/shards: Sort exprs are added to the SELECT list (if
 * not there already) of the SFW that produces the query results at each RN.
 * What if a sort expr returns nothing? For sorting, EMPTY and NULL are
 * considered distinct values and EMPTY sorts before NULL. So, we should not
 * convert EMPTY to NULL in this case, because a merge-sort must be done at
 * the client side and this merge sort should see the EMPTY. To support this
 * case, theDoNullOnEmpty is normally true, but is set to false for the SFWs
 * under the RCV, if the query does sorting. The EMPTY values sent by such
 * SFWs are then converted back to NULL by the RCV iter.
 *
 * theIsSelectDistinct;
 *
 * theSortExprs:
 * The exprs appearing in the ORDER BY clause.
 *
 * theSortSpecs:
 *
 * theHasGenericSort:
 * If a generic order by is added, this field is set to true inside the inner-
 * most ExprSFW of the SELECT expr stack. This is done because the sorting is
 * removed from this inner-most ExprSFW, but we need to know during distribution
 * of the inner-most SFW whether the SELECT stack contains generic sort.
 *
 * theSortingIndexes:
 * The set of indexes that order the rows in an "interesting" order. If the SFW
 * has group-by, an index has interesting order if it orders the rows according
 * to the grouping exprs. If the SFW has order-by, but not group-by, an index
 * has interesting order if it orders the rows according to the order-by exprs.
 * If the primary index has interesting order, an entry with null value will
 * be added to this array.
 *
 * theSortingIndex:
 * If the index that is actually used by the query is among theSortinIndexes,
 * theSortingIndex is the position of this index inside theSortinIndexes,
 * otherwise, theSortingIndex is -1.
 *
 * theNearPred:
 *
 * theOffsetExpr:
 * The expr appearing in the OFFSET clause.
 *
 * theLimitExpr:
 * The expr appearing in the LIMIT clause.
 *
 * theMKIndexStorageSizeCalls:
 * Calls to the index_storage_size() function that apppear as top-level
 * the SELECT expressions and apply to a multi-key index on the target table.
 */
public class ExprSFW extends Expr {

    /**
     * FromClause does not represent the full FROM clause of an SFW expr.
     * Instead, it represents one of the top-level, comma-separated exprs
     * appearing in the actual FROM clause. A FromClause evaluates its
     * associated expr (called the "domain expr") and binds one or more
     * vars to the values produced by the domain expr.
     */
    static class FromClause
    {
        private ExprSFW theSFW;

        /*
         * The "domain expr" of this FromClause. If there is one var associated
         * with the FromClause, the var iterates over the items generated by
         * this expr.
         */
        private Expr theDomainExpr;

        /*
         * The variables defined by this FromClause. Currently, the array
         * will contain more than one variables only if the domain expr is an
         * ExprBaseTable representing a NESTED TABLES clause. In this case, the
         * vars represent the table aliases used in the NESTED TABLES clause and
         * they are listed in the same order as their corresponding aliases in
         * the ExprBaseTable.
         */
        private final ArrayList<ExprVar> theVars = new ArrayList<ExprVar>();

        /*
         * If, for a table T, the query uses a secondary index I and I is
         * covering or any filtering predicates have been pushed to I, then
         * an "index variable" is created to range over the entries of index
         * I. The filtering preds, if any, are rewritten to access this index
         * var instead of the corresponding table variable. If I is covering,
         * every (sub)expression E which accesses T columns is also rewritten.
         *
         * Currently, only the target table of the query may use a secondary
         * index, so there can be at most one index var. Nevertheless,
         * theIndexVars array mirrors theVars array for convenience and future
         * extensions.
         */
        private final ArrayList<ExprVar> theIndexVars =
            new ArrayList<ExprVar>();

        FromClause(ExprSFW sfw, Expr domExpr, String varName, TableImpl table) {

            theSFW = sfw;
            theDomainExpr = domExpr;
            theDomainExpr.addParent(sfw);

            theVars.add(new ExprVar(sfw.theQCB, sfw.theSctx,
                                    domExpr.getLocation(),
                                    varName, table, this));
            theIndexVars.add(null);
        }

        private void switchSFW(ExprSFW sfw) {
            theDomainExpr.removeParent(theSFW, false);
            theSFW = sfw;
            theDomainExpr.addParent(sfw);
            sfw.theFromClauses.add(this);
            ++sfw.theNumChildren;
        }

        /*
         * Creates and adds a var after theDomainExpr has already been set.
         * This is the case when theDomainExpr is a NESTED TABLES clause.
         */
        ExprVar addVar(String varName, TableImpl table, int pos) {

            assert(table != null);

            ExprVar var = new ExprVar(theSFW.theQCB, theSFW.theSctx,
                                      theDomainExpr.getLocation(),
                                      varName, table, this);
            if (pos >= 0) {
                theVars.add(pos, var);
            } else {
                theVars.add(var);
            }
            theIndexVars.add(null);
            return var;
        }

        Expr getDomainExpr() {
            return theDomainExpr;
        }

        int getNumVars() {
            return theVars.size();
        }

        ArrayList<ExprVar> getVars() {
            return theVars;
        }

        ExprVar getVar(int i) {
            return theVars.get(i);
        }

        boolean containsVar(ExprVar var) {
            return theVars.contains(var);
        }

        void setIndexVar(int i, ExprVar var) {
            theIndexVars.set(i, var);
        }

        ExprVar getIndexVar(int i) {
            return theIndexVars.get(i);
        }

        ExprVar getVar() {

            if (theVars.size() > 1) {
                throw new QueryStateException(
                    "Method called on FromClause with more than one " +
                    "variables. domain expression:\n" + theDomainExpr.display());
            }

            return theVars.get(0);
        }

        ArrayList<TableImpl> getTables() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                return ((ExprBaseTable)theDomainExpr).getTables();
            }

            return null;
        }

        ExprBaseTable getTableExpr() {
            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                return (ExprBaseTable)theDomainExpr;
            }

            return null;
        }

        TableImpl getTargetTable() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                return ((ExprBaseTable)theDomainExpr).getTargetTable();
            }

            return null;
        }

        ExprVar getTargetTableVar() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                ExprBaseTable te = (ExprBaseTable)theDomainExpr;
                return theVars.get(te.getNumAncestors());
            }

            if (theDomainExpr.getKind() == ExprKind.UPDATE_ROW ||
                theDomainExpr.getKind() == ExprKind.INSERT_ROW ||
                theDomainExpr.getKind() == ExprKind.DELETE_ROW) {
                return theVars.get(0);
            }

            return null;
        }

        ExprVar getTargetTableIndexVar() {

            if (theDomainExpr.getKind() == ExprKind.BASE_TABLE) {
                ExprBaseTable te = (ExprBaseTable)theDomainExpr;
                return theIndexVars.get(te.getNumAncestors());
            }

            return null;
        }
    }

    private int theNumChildren;

    private ArrayList<FromClause> theFromClauses;

    private Expr theWhereExpr;

    private ArrayList<Expr> theGroupExprs;

    private int theNumGroupExprs = -1;

    private boolean theNeedOuterSFWForGroupBy;

    private boolean theGroupByExprCompleteShardKey = false;

    private boolean theHasGenericGroupBy;

    private boolean theIsGroupingForDistinct;

    private ArrayList<String> theFieldNames;

    private ArrayList<Expr> theFieldExprs;

    private boolean theIsSelectStar;

    private boolean theConstructsRecord = true;

    private boolean theDoNullOnEmpty = true;

    private boolean theIsSelectDistinct;

    private ArrayList<Expr> theSortExprs;

    private ArrayList<SortSpec> theSortSpecs;

    private boolean theHasGenericSort;

    private ArrayList<IndexImpl> theSortingIndexes = null;

    private int theSortingIndex = -1;

    private ExprFuncCall theNearPred;

    private Expr theOffsetExpr;

    private Expr theLimitExpr;

    private ArrayList<ExprFuncCall> theMKIndexStorageSizeCalls;

    public ExprSFW(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location) {

        super(qcb, sctx, ExprKind.SFW, location);
        theFromClauses = new ArrayList<FromClause>(8);
    }

    public ExprVar createFromVar(Expr domainExpr, String varName) {
        return createFrom(domainExpr, varName, null);
    }

    ExprVar createTableVar(Expr domExpr, TableImpl table, String varName) {
        return createTableVar(domExpr, table, varName, -1);
    }

    ExprVar createTableVar(
        Expr domExpr,
        TableImpl table,
        String varName,
        int pos) {

        assert(table != null &&
               (domExpr.getKind() == ExprKind.BASE_TABLE ||
                domExpr.getKind() == ExprKind.UPDATE_ROW ||
                domExpr.getKind() == ExprKind.INSERT_ROW ||
                domExpr.getKind() == ExprKind.DELETE_ROW));

        for (FromClause fc : theFromClauses) {
            if (fc.getDomainExpr() == domExpr) {
                return fc.addVar(varName, table, pos);
            }
        }

        return createFrom(domExpr, varName, table);
    }

    private ExprVar createFrom(
        Expr domainExpr,
        String varName,
        TableImpl table) {

        FromClause fc = new FromClause(this, domainExpr, varName, table);
        theFromClauses.add(fc);
        ++theNumChildren;
        return fc.getVar(0);
    }

    void removeFromClause(int i, boolean destroy) {

        FromClause fc = theFromClauses.get(i);

        for (ExprVar var : fc.getVars()) {
            assert(!var.hasParents());
        }

        theFromClauses.remove(i);
        --theNumChildren;
        fc.getDomainExpr().removeParent(this, destroy);
    }

    FromClause getFromClause(int i) {
        return theFromClauses.get(i);
    }

    FromClause getFirstFrom() {
        return getFromClause(0);
    }

    int getNumFroms() {
        return theFromClauses.size();
    }

    Expr getDomainExpr(int i) {
        return theFromClauses.get(i).getDomainExpr();
    }

    @Override
    Expr getInput() {
        return getDomainExpr(0);
    }

    void setDomainExpr(int i, Expr newExpr, boolean destroy) {
        FromClause fc = theFromClauses.get(i);
        fc.theDomainExpr.removeParent(this, destroy);
        fc.theDomainExpr = newExpr;
        newExpr.addParent(this);
        for (ExprVar var : fc.theVars) {
            var.setDomainExpr(newExpr);
        }
    }

    /*
     * Check whether the given expr is the domain expr for a FromClause defined
     * by this SFW expr. If so, return the variables of that FromClause.
     */
    ArrayList<ExprVar> findVarsForExpr(Expr expr) {

        for (int i = 0; i < theFromClauses.size(); ++i) {
            FromClause fc = theFromClauses.get(i);
            if (fc.getDomainExpr() == expr) {
                return fc.getVars();
            }
        }

        return null;
    }

    ExprBaseTable findTableExprForVar(ExprVar var) {

        for (int i = 0; i < theFromClauses.size(); ++i) {
            FromClause fc = theFromClauses.get(i);
            ExprBaseTable tableExpr = fc.getTableExpr();
            if (tableExpr != null && fc.containsVar(var)) {
                return tableExpr;
            }
        }

        return null;
    }

    void removeUnusedVars() {

        for (int i = theFromClauses.size() - 1; i >= 0; --i) {
            FromClause fc = theFromClauses.get(i);
            if (fc.getDomainExpr().isScalar() &&
                fc.getVar(0).getNumParents() == 0) {
                assert(fc.getNumVars() == 1);
                removeFromClause(i, true);
            }
        }
    }

    ExprVar addIndexVar(TableImpl table, IndexImpl index) {

        for (FromClause fc : theFromClauses) {

            ArrayList<TableImpl> tables = fc.getTables();

            if (tables == null) {
                continue;
            }

            int tablePos = -1;
            for (int i = 0; i < tables.size(); ++i) {
                if (tables.get(i).getId() == table.getId()) {
                    tablePos = i;
                    break;
                }
            }

            if (tablePos < 0) {
                continue;
            }

            ExprVar var = fc.getVar(tablePos);
            String idxVarName = var.createIndexVarName();

            ExprVar idxVar = new ExprVar(theQCB, theSctx, var.theLocation,
                                         idxVarName, table, fc);
            RecordDefImpl indexEntryDef;
            if (index != null) {
                indexEntryDef = index.getIndexEntryDef();
            } else {
                /*
                 * We use getRowDef() instead of getPrimKeyDef(), because during
                 * runtime, we always construct a full table row, even if we may
                 * fill just the prim-key columns in that row.
                 */
                indexEntryDef = table.getRowDef();
            }

            ExprType idxVarType = TypeManager.createType(indexEntryDef,
                                                         Quantifier.ONE);
            idxVar.setIndex(index, idxVarType);

            fc.setIndexVar(tablePos, idxVar);

            return idxVar;
        }

        return null;
    }

    void addWhereClause(Expr condExpr) {

        assert(theWhereExpr == null);

        theWhereExpr = ExprPromote.create(
            null, condExpr, TypeManager.BOOLEAN_QSTN());

        theWhereExpr.addParent(ExprSFW.this);
        ++theNumChildren;
    }

    Expr getWhereExpr() {
        return theWhereExpr;
    }

    void setWhereExpr(Expr newExpr, boolean destroy) {
        newExpr = ExprPromote.create(
            null, newExpr, TypeManager.BOOLEAN_QSTN());
        newExpr.addParent(ExprSFW.this);
        theWhereExpr.removeParent(this, destroy);
        theWhereExpr = newExpr;
    }

    void removeWhereExpr(boolean destroy) {
        theWhereExpr.removeParent(this, destroy);
        theWhereExpr = null;
        --theNumChildren;
    }

    void addTopPred(Expr pred) {

        if (theWhereExpr == null) {
           addWhereClause(pred);
        } else if (theWhereExpr.getFunction(FuncCode.OP_AND) != null) {
            ExprFuncCall andOp = (ExprFuncCall)theWhereExpr;
            andOp.addArg(pred);
        } else {
            Expr andOp = ExprFuncCall.create(theQCB, theSctx,
                                             theWhereExpr.getLocation(),
                                             FuncCode.OP_AND,
                                             theWhereExpr,
                                             pred);
            setWhereExpr(andOp, false);
        } 
    }

    void removeTopPred(Expr pred, boolean destroy) {

        if (theWhereExpr == null) {
            return;
        }

        if (pred == theWhereExpr) {
            removeWhereExpr(destroy);
        } else if (theWhereExpr.getFunction(FuncCode.OP_AND) != null) {
            ExprFuncCall andOp = (ExprFuncCall)theWhereExpr;
            boolean removed = andOp.removeArg(pred, destroy);
            assert(removed);
            if (andOp.getNumArgs() == 0) {
                removeWhereExpr(destroy);
            }
        }
    }

    ArrayList<Expr> getTopPreds() {

        if (theWhereExpr == null) {
            return null;
        }

        ArrayList<Expr> preds = new ArrayList<Expr>(16);

        Function andOp = theWhereExpr.getFunction(FuncCode.OP_AND);

        if (andOp != null) {
            preds.addAll(((ExprFuncCall)theWhereExpr).getArgs());
        } else {
            preds.add(theWhereExpr);
        }
        return preds;
    }

    void addGroupByClause(ArrayList<Expr> gbExprs) {

        for (int i = 0; i < gbExprs.size(); ++i) {
            Expr expr = gbExprs.get(i);
            expr = ExprPromote.create(null, expr, TypeManager.ANY_QSTN());
            expr.addParent(this);
            gbExprs.set(i, expr);
        }

        theGroupExprs = gbExprs;
        theNumGroupExprs = theGroupExprs.size();
        theNumChildren += theNumGroupExprs;
    }

    void addEmptyGroupBy() {
        if (theGroupExprs == null) {
            theGroupExprs = new ArrayList<Expr>();
            theNumGroupExprs = 0;
        }
    }

    boolean hasGroupBy() {
        return theNumGroupExprs >= 0;
    }

    boolean hasEmptyGroupBy() {
        return theNumGroupExprs == 0;
    }

    public boolean isGroupingForDistinct() {
        return theIsGroupingForDistinct;
    }

    boolean hasGroupExprs() {
        return theGroupExprs != null;
    }

    public int getNumGroupExprs() {
        return theNumGroupExprs;
    }

    void setNumGroupExprs(int v) {
        theNumGroupExprs = v;
    }

    Expr getGroupExpr(int i) {
        return theGroupExprs.get(i);
    }

    void setGroupExpr(int i, Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theGroupExprs.get(i).removeParent(this, destroy);
        theGroupExprs.set(i, newExpr);
    }

    void removeGroupExpr(int i, boolean destroy) {
        Expr gbExpr = theGroupExprs.remove(i);
        gbExpr.removeParent(this, destroy);
        --theNumChildren;
    }

    void removeGroupBy() {

        if (theGroupExprs != null) {
            while (!theGroupExprs.isEmpty()) {
                removeGroupExpr(0, true);
            }
        }

        theGroupExprs = null;
        theNumGroupExprs = -1;
    }

    void addSortClause(
        ArrayList<Expr> sortExprs,
        ArrayList<SortSpec> sortSpecs) {

        for (int i = 0; i < sortExprs.size(); ++i) {

            /* Note: don't wrap expr in promote(anyAtomic?) because this can
             * cause promote expr to appear in the driver plans, where it is
             * not implemented. Instead, the SortIter checks itself that all
             * the sorting fields are atomic values */
            Expr expr = sortExprs.get(i);
            expr.addParent(this);
        }

        theSortExprs = new ArrayList<Expr>(sortExprs);
        theSortSpecs = new ArrayList<SortSpec>(sortSpecs);

        theNumChildren += theSortExprs.size();
    }

    void removeSort() {

        if (!hasSort()) {
            return;
        }

        while (!theSortExprs.isEmpty()) {
            removeSortExpr(0, true);
        }

        theSortExprs = null;
        theSortSpecs = null;
    }

    boolean hasSort() {
        return (theSortExprs != null && !theSortExprs.isEmpty());
    }

    int getNumSortExprs() {
        return (theSortExprs == null ? 0 : theSortExprs.size());
    }

    Expr getSortExpr(int i) {
        return theSortExprs.get(i);
    }

    void setSortExpr(int i, Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theSortExprs.get(i).removeParent(this, destroy);
        theSortExprs.set(i, newExpr);
    }

    void addSortExpr(Expr newExpr) {
        theSortExprs.add(newExpr);
        theSortSpecs.add(new SortSpec(false, false));
        newExpr.addParent(this);
        ++theNumChildren;
    }

    void removeSortExpr(int i, boolean destroy) {

        Expr sortExpr = theSortExprs.remove(i);
        sortExpr.removeParent(this, destroy);
        theSortSpecs.remove(i);
        --theNumChildren;
    }

    SortSpec[] getSortSpecs() {
        SortSpec[] arr = new SortSpec[theSortSpecs.size()];
        return theSortSpecs.toArray(arr);
    }

    void addSelectClause(
        ArrayList<String> fieldNames,
        ArrayList<Expr> fieldExprs) {

        assert(fieldNames.size() == fieldExprs.size());
        theFieldNames = fieldNames;
        theFieldExprs = fieldExprs;

        for (int i = 0; i < fieldExprs.size(); ++i) {

            Expr expr = fieldExprs.get(i);

            if (expr.isMultiValued()) {
                ArrayList<Expr> args = new ArrayList<Expr>(1);
                args.add(expr);
                expr = new ExprArrayConstr(theQCB, theSctx, expr.getLocation(),
                                           args, true/*conditional*/);
            }

            expr.addParent(this);
            theFieldExprs.set(i, expr);
        }

        theNumChildren += fieldExprs.size();
    }

    void setIsSelectDistinct() {
        theIsSelectDistinct = true;
    }

    boolean isSelectDistinct() {
        return theIsSelectDistinct;
    }

    void setIsSelectStar(boolean v) {

        boolean save = theIsSelectStar;
        theIsSelectStar = v;

        if (save != v) {
            computeType(false);
        }
    }

    boolean isSelectStar() {
        return theIsSelectStar;
    }

    boolean getConstructsRecord() {
        return theConstructsRecord;
    }

    Expr getFieldExpr(int i) {
        return theFieldExprs.get(i);
    }

    void setFieldExpr(int i, Expr newExpr, boolean destroy) {

        if (newExpr.isMultiValued()) {
            ArrayList<Expr> args = new ArrayList<Expr>(1);
            args.add(newExpr);
            newExpr = new ExprArrayConstr(theQCB, theSctx, newExpr.theLocation,
                                          args, true/*conditional*/);
        }

        newExpr.addParent(this);

        theFieldExprs.get(i).removeParent(this, destroy);
        theFieldExprs.set(i, newExpr);
        computeType(false);
    }

    void removeField(int i, boolean destroy) {
        theFieldExprs.get(i).removeParent(this, destroy);
        theFieldExprs.remove(i);
        theFieldNames.remove(i);
        computeType(false);
        --theNumChildren;
    }

    public void addField(String name, Expr expr) {

        if (theFieldExprs == null) {
            theFieldExprs = new ArrayList<Expr>();
            theFieldNames = new ArrayList<String>();
        }

        if (expr.isMultiValued()) {
            ArrayList<Expr> args = new ArrayList<Expr>(1);
            args.add(expr);
            expr = new ExprArrayConstr(theQCB, theSctx, expr.theLocation,
                                       args, true/*conditional*/);
        }

        theFieldExprs.add(expr);
        theFieldNames.add(name);
        expr.addParent(this);

        if (theFieldExprs.size() > 1) {
            theConstructsRecord = true;
        }

        computeType(false);
        ++theNumChildren;
    }

    String getFieldName(int i) {
        return theFieldNames.get(i);
    }

    int getNumFields() {
        return (theFieldExprs == null ? 0 : theFieldExprs.size());
    }

    ArrayList<String> getFieldNames() {
        return theFieldNames;
    }

    void setFieldNames(ArrayList<String> fieldNames) {
        theFieldNames = fieldNames;
        computeType(false);
    }

    String[] getFieldNamesArray() {
        if (theFieldNames == null) {
            return new String[0];
        }
        String[] arr = new String[theFieldNames.size()];
        return theFieldNames.toArray(arr);
    }

    boolean isGroupingField(int i) {
        return i < theNumGroupExprs;
    }

    public boolean doNullOnEmpty() {
        return theDoNullOnEmpty;
    }

    void setDoNullOnEmpty(boolean v) {
        theDoNullOnEmpty = v;
    }

    void addOffsetLimit(Expr offset, Expr limit) {

        if (offset != null) {
            addOffset(offset);
        }

        if (limit != null) {
            addLimit(limit);
        }
    }

    Expr getOffset() {
        return theOffsetExpr;
    }

    void addOffset(Expr expr) {

        assert(theOffsetExpr == null);

        if (!ConstKind.isConst(expr)) {
            throw new QueryException("Offset expression is not constant");
        }

        if (expr.getKind() == ExprKind.CONST) {
            FieldValueImpl val = ((ExprConst)expr).getValue();
            if (val.getLong() == 0) {
                return;
            }
        }

        theOffsetExpr = ExprPromote.create(null, expr, TypeManager.LONG_ONE());

        theOffsetExpr.addParent(this);
        ++theNumChildren;
    }

    void removeOffset(boolean destroy) {
        theOffsetExpr.removeParent(this, destroy);
        theOffsetExpr = null;
        --theNumChildren;
    }

    void setOffset(Expr newExpr, boolean destroy) {
        theOffsetExpr.removeParent(this, destroy);
        theOffsetExpr = null;
        --theNumChildren;
        addOffset(newExpr);
    }

    Expr getLimit() {
        return theLimitExpr;
    }

    void addLimit(Expr expr) {

        assert(theLimitExpr == null);

        if (!ConstKind.isConst(expr)) {
            throw new QueryException("Limit expression is not constant");
        }

        theLimitExpr = ExprPromote.create(null, expr, TypeManager.LONG_ONE());
        theLimitExpr.addParent(this);
        ++theNumChildren;
    }

    void removeLimit(boolean destroy) {
        theLimitExpr.removeParent(this, destroy);
        theLimitExpr = null;
        --theNumChildren;
    }

    void setLimit(Expr newExpr, boolean destroy) {
        theLimitExpr.removeParent(this, destroy);
        theLimitExpr = null;
        --theNumChildren;
        addLimit(newExpr);
    }

    boolean hasJoin() {

        if (theFromClauses.get(0).getDomainExpr().getKind() == ExprKind.JOIN) {
            return true;
        }

        int i = 0;
        for (FromClause fc : theFromClauses) {
            ExprBaseTable tableExpr = fc.getTableExpr();
            if (tableExpr != null) {
                ++i;
                if (i > 1) {
                    return true;
                }
            }
        }
        return false;
    }

    void rewriteJoin() {

        ArrayList<FromClause> fromClausesToKeep = new ArrayList<>(8);

        ArrayList<ExprSFW> branches = new ArrayList<>(8);
        ArrayList<ExprBaseTable> tableExprs = new ArrayList<>(8);

        for (FromClause fc : theFromClauses) {

            ExprBaseTable tableExpr = fc.getTableExpr();

            if (tableExpr == null) {
                fromClausesToKeep.add(fc);
                continue;
            }

            ExprSFW branchSFW = new ExprSFW(theQCB, theSctx,
                                            tableExpr.getLocation());
            fc.switchSFW(branchSFW);

            branches.add(branchSFW);
            tableExprs.add(tableExpr);
        }

        for (int i = 0; i < tableExprs.size(); ++i) {
            ExprBaseTable tableExpr = tableExprs.get(i);
            tableExpr.setPosInJoin(i);
        }

        ExprJoin joinExpr = new ExprJoin(theQCB, theSctx, theLocation, branches);

        theNumChildren -= branches.size();
        theFromClauses.clear();

        String joinVarName = theQCB.createInternalVarName("join");
        createFromVar(joinExpr, joinVarName);
        theFromClauses.addAll(fromClausesToKeep);

        assert(theNumChildren == computeNumChildren());

        if (theWhereExpr == null) {
            throw new QueryException(
                "Tables are not joined on their common shard key", theLocation);
        }

        validateJoinPreds(tableExprs);

        pushDownPredicates(joinExpr, branches, tableExprs);

        if (hasSort() && !hasGroupBy()) {
            pushDownSort(branches);
        }
    }

    private void validateJoinPreds(ArrayList<ExprBaseTable> tableExprs) {

        ArrayList<Expr> topPreds = getTopPreds();

        TableImpl rootTable = tableExprs.get(0).
                              getTargetTable().getTopLevelTable();
        int numShardCols = rootTable.getShardKeySize();

        for (ExprBaseTable table : tableExprs) {
            if (table.getTargetTable().getTopLevelTable().getId() !=
                rootTable.getId()) {
                throw new QueryException(
                    "Tables do not belong to the same table hierarchy",
                    theLocation);
            }
        }

        ArrayList<ExprBaseTable> visitedTables = new ArrayList<ExprBaseTable>(8);
        visitedTables.add(tableExprs.get(0));

        for (int shardCol = 0; shardCol < numShardCols; ++shardCol) {
            if (!validatePreds(tableExprs, topPreds, shardCol, visitedTables)) {
                throw new QueryException(
                    "Tables are not joined on their common shard key",
                    theLocation);
            }
        }
    }

    private boolean validatePreds(
        ArrayList<ExprBaseTable> tableExprs,
        ArrayList<Expr> topPreds,
        int shardCol,
        ArrayList<ExprBaseTable> visitedTables) {

        ExprBaseTable currTableExpr = visitedTables.get(visitedTables.size() - 1);
        ArrayList<TableImpl> currTables = currTableExpr.getTables();

        for (int i = 0; i < topPreds.size(); ++i) {

            Expr pred = topPreds.get(i);
            Function func = pred.getFunction(FuncCode.OP_EQ);

            if (func == null) {
                topPreds.remove(i--);
                continue;
            }
            /*  
            System.out.println("Validating join pred:\n" + pred.display());
            System.out.println("shard col = " + shardCol + " curr table = " +
                               currTableExpr.getTargetTable().getFullName() +
                               " at join pos " + currTableExpr.getPosInJoin());
            */
            ExprBaseTable nextTableExpr = null;
            Expr op1 = null;
            Expr op2 = null;
            ExprFuncCall funcExpr = (ExprFuncCall)pred;
            op1 = funcExpr.getArg(0);
            op2 = funcExpr.getArg(1);

            for (TableImpl currTable : currTables) {
                if (ExprUtils.isPrimKeyColumnRef(currTableExpr,
                                                 currTable,
                                                 shardCol,
                                                 op1)) {
                    nextTableExpr = ExprUtils.isPrimKeyColumnRef(shardCol, op2);
                } else if (ExprUtils.isPrimKeyColumnRef(currTableExpr,
                                                        currTable,
                                                        shardCol,
                                                        op2)) {
                    nextTableExpr = ExprUtils.isPrimKeyColumnRef(shardCol, op1);
                }

                if (nextTableExpr != null &&
                    !visitedTables.contains(nextTableExpr)) {
                    break;
                }
                nextTableExpr = null;
            }

            if (nextTableExpr == null ||
                visitedTables.contains(nextTableExpr)) {
                continue;
            }

            visitedTables.add(nextTableExpr);
            topPreds.remove(i--);
            /*
            System.out.println("Added table " +
                               nextTableExpr.getTargetTable().getFullName() +
                               " to visited tables");
            */
            if (visitedTables.size() == tableExprs.size()) {
                //System.out.println("visited all tables");
                return true;
            }

            if (validatePreds(tableExprs, topPreds, shardCol, visitedTables)) {
                return true;
            }
        }

        //System.out.println("FAILED");
        return false;
    }

    private void pushDownPredicates(
        ExprJoin joinExpr,
        ArrayList<ExprSFW> branches,
        ArrayList<ExprBaseTable> tableExprs) {

        ArrayList<Expr> topPreds = getTopPreds();
        ArrayList<Expr> pushedPreds = new ArrayList<>();
        ArrayList<ExprVar> unnestingVars = null;

        if (getNumFroms() > 1) {
            unnestingVars = new  ArrayList<>();
        }

        for (Expr pred : topPreds) {

            /* Do not push down a predicate that references an unnesting
             * var, because unnseting is done after the join. */
            if (unnestingVars != null) {
                unnestingVars.clear();
                pred.getUnnestingVars(unnestingVars);
                if (!unnestingVars.isEmpty()) {
                    continue;
                }
            }

            /* Find the table expressions referenced by this predicate and
             * all of its subexpressions. */
            ArrayList<ExprBaseTable> predTables = pred.getTableExprs();

            if (predTables == null || predTables.isEmpty()) {
                continue;
            } else if (predTables.size() == 1) {
                pushLocalPred(branches, tableExprs, predTables.get(0), pred);
                pushedPreds.add(pred);
            } else {
                pushJoinPred(joinExpr, branches, tableExprs, pred);
                pushedPreds.add(pred);
            }
        }

        for (Expr pred : pushedPreds) {
            removeTopPred(pred, false);
        }
    }

    private void pushLocalPred(
        ArrayList<ExprSFW> branches,
        ArrayList<ExprBaseTable> joinTables,
        ExprBaseTable predTable,
        Expr pred) {

        for (int i = 0; i < joinTables.size(); ++i) {
            if (joinTables.get(i) == predTable) {
                ExprSFW branch = branches.get(i);
                branch.addTopPred(pred);
                return;
            }
        }
        assert(false);
    }

    private void pushJoinPred(
        ExprJoin joinExpr,
        ArrayList<ExprSFW> branches,
        ArrayList<ExprBaseTable> joinTables,
        Expr pred) {

        /* Sort the table expressions of the predicate by their position in
         * the FROM clause. */
        ArrayList<ExprBaseTable> predTables = pred.getTableExprs();
        assert(predTables.size() > 1);
        predTables.sort(ExprBaseTable.theJoinComparator);

        /*        
        System.out.println("Rewriting join pred:\n" + pred.display());
        System.out.println("pred branches: ");
        for (ExprBaseTable table : predTables) {
            System.out.println(table.getPosInJoin() + " ");
        }
        */

        /* Rewrite the predicate in the following manner:
         * - Traverse the predicate tree looking for subexpressions that
         *   access only one table expr T, where T is not the last one in
         *   predTables.
         * - If ST is such a subexpression, place it in the SELECT clause of
         *   the join branch JT corresponding to T.
         * - Replace ST with a bind variable VT. During runtime, this variable
         *   will be bound by the join iterator to the value of ST every time
         *   JT produces a new result.
         * - Create an association (a JoinPred object) among JT, ST, and VT,
         *   and add it to the join expr. */
        rewriteJoinPred(joinExpr, branches, joinTables, predTables, pred);

        /* Push the predicate to the join branch that corresponds to the last
         * (right-most) table expr in predTables. */
        ExprBaseTable innerTable = predTables.get(predTables.size()-1);
        ExprSFW innerBranch = branches.get(innerTable.getPosInJoin());
        innerBranch.addTopPred(pred);
    }

    private void rewriteJoinPred(
        ExprJoin joinExpr,
        ArrayList<ExprSFW> branches,
        ArrayList<ExprBaseTable> joinTables,
        ArrayList<ExprBaseTable> predTables,
        Expr pred) {

        ExprIter children = pred.getChildren();

        while (children.hasNext()) {

            Expr child = children.next();

            //System.out.println("Rewritting child:\n" + child.display());

            ArrayList<ExprBaseTable> childTables = child.getTableExprs();

            if (childTables == null || childTables.isEmpty()) {
                System.out.println("child has no tables");
                continue;
            }
            /*
            System.out.println("child branches: ");
            for (ExprBaseTable table : childTables) {
                System.out.println(table.getPosInJoin() + " ");
            }
            */
            if (childTables.size() == 1) {
                ExprBaseTable childTable = childTables.get(0);

                if (childTable == predTables.get(predTables.size()-1)) {
                    //System.out.println("child is inner-most");
                    continue;
                }

                int childTablePosInJoin = -1;

                for (int i = 0; i < joinTables.size(); ++i) {
                    ExprBaseTable table = joinTables.get(i);
                    if (childTable == table) {
                        childTablePosInJoin = i;
                        break;
                    }
                }
                assert(childTablePosInJoin >= 0);
                //System.out.println("child is on join branch " + childTablePosInJoin);

                ExprSFW childBranch = branches.get(childTablePosInJoin);
                String childFieldName = childBranch.
                                        generateFieldName("outerJoinVal");
                childBranch.addField(childFieldName, child);
                int childFieldPos = childBranch.getNumFields() - 1;
                Expr childField = childBranch.getFieldExpr(childFieldPos);
                boolean unbox;
                if (childField != child) {
                    assert(childField.getKind() == ExprKind.ARRAY_CONSTR);
                    assert(((ExprArrayConstr)childField).isConditional());
                    unbox = true;
                } else {
                    unbox = false;
                }

                int joinVarId = theQCB.createExternalVarId();
                String joinVarName = "$innerJoinVar" + joinVarId;
                ExprVar joinVar = new ExprVar(theQCB, theSctx,
                                              child.getLocation(),
                                              joinVarName,
                                              child.getType().getDef(),
                                              joinVarId,
                                              true);
                Expr innerExpr = joinVar;
                if (unbox) {
                    innerExpr = new ExprArrayFilter(theQCB, theSctx,
                                                    child.getLocation(),
                                                    joinVar);
                }

                children.replace(innerExpr, false);

                joinExpr.addJoinPred(childTablePosInJoin, childFieldPos, joinVarId);

                /*
                System.out.println(
                    "Pushed child. outerPos = " +
                    childTablePosInJoin + " innerPos = " + (predTables.size()-1) +
                    "join var = " + joinVarName +
                    "\nsubExpr:\n" + innerExpr.display());
                */
            } else {
                rewriteJoinPred(joinExpr, branches, joinTables, predTables, child);
            }
        }
        children.reset();
    }

    /*
     * Push the sorting exprs down to the join branches, if possible. This is
     * done so that during the index-selection phase for each branch, indexes
     * can be recognized as sorting indexes and costed accordingly. This way,
     * if a sorting index exists and is selected for each branch with pushed
     * sort exprs, a global sort will be avoided.
     *
     * Pushing is done if:
     * a. Every sort expr accesses only one table expr and only the target table
     *    of that expr, and
     * b. The sequence of sort exprs (in the order of appearence in the ORDER BY
     *    clause) can be partitioned in subsequences s1, s2, ... so that all sort
     *    exprs is s1 reference the target table of the 1st join branch, all sort
     *    exprs is s2 reference the target table of the 2nd join branch, etc, and
     * c.
     *    
     */
    private void pushDownSort(ArrayList<ExprSFW> branches) {

        /* If sortTables[i] == T, then the i-th sortExpr can be pushed to the
         * join branch corresponding to T. */
        ArrayList<ExprBaseTable> sortTables = new ArrayList<>();
        int joinBranch = 0;

        for (int i = 0; i < theSortExprs.size(); ++i) {

            Expr sortExpr = theSortExprs.get(i);
            ArrayList<ExprBaseTable> sortExprTables = sortExpr.getTableExprs();
            ArrayList<TableImpl> sortExprTables2 = sortExpr.getTables();

            if (sortExprTables.size() != 1 || sortExprTables2.size() != 1) {
                return;
            }

            ExprBaseTable sortTable = sortExprTables.get(0);
            TableImpl sortTable2 = sortExprTables2.get(0);

            if (sortTable2 != sortTable.getTargetTable()) {
                return;
            }

            int sortBranch = sortTable.getPosInJoin();

            if (sortBranch == joinBranch) {
                sortTables.add(sortTable);
            } else if (sortBranch == joinBranch + 1) {
                if (!sortTables.isEmpty() &&
                    sortTables.get(sortTables.size()-1).getPosInJoin() == joinBranch) {
                    sortTables.add(sortTable);
                    ++joinBranch;
                } else {
                    return;
                }
            } else {
                return;
            }
        }

        if (sortTables.size() != theSortExprs.size()) {
            //System.out.println("no sort push down");
            return;
        }

        int lastJoinBranch = joinBranch;
        joinBranch = 0;
        ArrayList<Expr> sortExprs = new ArrayList<>();
        ArrayList<SortSpec> sortSpecs = new ArrayList<>();
        ArrayList<Integer> pkPositions = new ArrayList<>();

        /* Check that for each join branch except the last one, the sort exprs
         * belonging to that branch include the primary key columns of the
         * branch's target table*/
        for (int i = 0; i < theSortExprs.size(); ++i) {

            ExprBaseTable sortTable = sortTables.get(i);
            Expr sortExpr = theSortExprs.get(i);
            int pkPos = ExprUtils.isPrimKeyColumnRef(sortTable,
                                                     sortTable.getTargetTable(),
                                                     sortExpr);

            if (sortTable.getPosInJoin() == joinBranch) {
                if (pkPos >= 0 && !pkPositions.contains(pkPos)) {
                    pkPositions.add(pkPos);
                }
            } else {
                if (joinBranch < lastJoinBranch &&
                    pkPositions.size() !=
                    sortTables.get(i-1).getTargetTable().getPrimaryKeySize()) {
                    return;
                }

                pkPositions.clear();

                if (pkPos >= 0 && !pkPositions.contains(pkPos)) {
                    pkPositions.add(pkPos);
                }

                ++joinBranch;
                assert(joinBranch == sortTable.getPosInJoin());
            }
        }

        joinBranch = 0;

        for (int i = 0; i < theSortExprs.size(); ++i) {

            ExprBaseTable sortTable = sortTables.get(i);
            Expr sortExpr = theSortExprs.get(i);

            /*
            System.out.println("Sort table for sort expr " + i +
                               " is in join pos " + sortTable.getPosInJoin() +
                               ". Current join branch = " + joinBranch);
            */
            if (sortTable.getPosInJoin() == joinBranch) {
                sortExprs.add(sortExpr.clone());
                sortSpecs.add(theSortSpecs.get(i));
            } else {
                ExprSFW joinBranchExpr = branches.get(joinBranch);
                joinBranchExpr.addSortClause(sortExprs, sortSpecs);
                //System.out.println("pushed sort to sfw:\n" + joinBranchExpr.display());

                sortExprs.clear();
                sortSpecs.clear();

                sortExprs.add(sortExpr.clone());
                sortSpecs.add(theSortSpecs.get(i));

                ++joinBranch;
                assert(joinBranch == sortTable.getPosInJoin());
            }
        }

        if (!sortExprs.isEmpty()) {
            ExprSFW joinBranchExpr = branches.get(joinBranch);
            joinBranchExpr.addSortClause(sortExprs, sortSpecs);
            //System.out.println("pushed sort to sfw:\n" + joinBranchExpr.display());
        }

        /*
        for (int i = 0; i < branches.size(); ++i) {
            System.out.println("XXXX Branch " + i + "\n" + branches.get(i).display());
        }
        */
    }

    /*
     * Generate a field name that does not exist already among the field names
     * of this SFW. Try with the given name first.
     */
    String generateFieldName(String fname) {

        int i = 1;
        String res = (fname + i);

        if (theFieldNames == null) {
            return res;
        }

        while (theFieldNames.contains(res)) {
            ++i;
            res = (fname + i);
        }
        return res;
    }

    void setNearPred(ExprFuncCall e) {
        theNearPred = e;
    }

    /*
     * Rewrite the geo_near predicate to a within-distance predicate plus
     * an order by distance. If the SFW contains order-by already, the
     * distance expr is appeanded to the eisting sorting exprs.
     */
    void rewriteNearPred() {

        if (theNearPred == null) {
            return;
        }

        FunctionLib fnlib = CompilerAPI.getFuncLib();

        theNearPred.setFunction(fnlib.getFunc(FuncCode.FN_GEO_WITHIN_DISTANCE));

        Expr geopath = theNearPred.getArg(0);
        Expr geopoint = theNearPred.getArg(1);
        Expr distanceExpr = ExprFuncCall.create(theQCB, theSctx,
                                                geopath.getLocation(),
                                                FuncCode.FN_GEO_DISTANCE,
                                                geopath, geopoint);

        if (hasSort()) {
            addSortExpr(distanceExpr);
            return;
        }

        ArrayList<Expr> sortExprs = new ArrayList<Expr>(1);
        ArrayList<SortSpec> sortSpecs = new ArrayList<SortSpec>(1);
        sortExprs.add(distanceExpr);
        sortSpecs.add(new SortSpec(false, false));

        addSortClause(sortExprs, sortSpecs);
    }

    /*
     * If "this" SFW contains group-by, this method is called after translation
     * and before index selection, to split the SFW into a "grouping" SFW and a
     * "selecting" SFW, as described in the ExprSFW javadoc.
     *
     * If the SFW contains order-by as well, the sort cannot be index-based. So,
     * the method calls addGenericSort() to add generic sort on top of the SFW.
     * Finally, if any extra sorting exprs need to be added to the SELECT list
     * of this SFW, a new SFW is added on top of the ExprSort to project out
     * the extra exprs.
     */
    Expr rewriteGroupBy(boolean forDistinct) {

        assert(!hasParents());

        Expr topExpr = null;
        ExprSort sortExpr = null;

        if (hasSort()) {
            topExpr = addGenericSort();

            /* temporarily disconnect the sort expr from this SFW to avoid
             * problems with upwards propagation of type changes as this SFW
             * gets split. */
            if (topExpr != null) {
                if (topExpr.getKind() != ExprKind.SORT) {
                    ExprSFW sfw = (ExprSFW)topExpr;
                    sortExpr = (ExprSort)sfw.getDomainExpr(0);
                } else {
                    sortExpr = (ExprSort)topExpr;
                }
                removeParent(sortExpr, false);
            }
        }

        theIsGroupingForDistinct = forDistinct;

        /* Replace the SELECT list of this SFW with a list that contains only
         * the grouping exprs and aggregate functions. Move the original SELECT
         * list to a new SFW placed on top of this SFW. */
        ArrayList<Expr> origFieldExprs = theFieldExprs;
        ArrayList<String> origFieldNames = theFieldNames;
        int numFields = getNumFields();
        int numGBExprs = getNumGroupExprs();

        for (int i = 0; i < numFields; ++i) {
            theFieldExprs.get(i).removeParent(this, false);
        }

        theNumChildren -= numFields;
        theFieldExprs = new ArrayList<Expr>(numGBExprs+5);
        theFieldNames = new ArrayList<String>(numGBExprs+5);

        for (int i = 0; i < numGBExprs; ++i) {

            Expr groupExpr = getGroupExpr(i);

            if (groupExpr.getKind() == ExprKind.PROMOTE) {
                Expr gbExpr = groupExpr.getInput();
                IndexExpr epath = gbExpr.getIndexExpr();
                if (epath != null &&
                    epath.isMatched() &&
                    !epath.theIsMultiValue) {
                    groupExpr.replace(gbExpr, true);
                    groupExpr = gbExpr;
                }
            }
            theFieldExprs.add(groupExpr);
            theFieldNames.add("gb-" + i);
            ++theNumChildren;
        }
        theGroupExprs.clear();
        theGroupExprs = null;
        theNumChildren -= numGBExprs;
        if (numGBExprs > 0) {
            computeType(false);
        }

        ExprSFW outerSFW = new ExprSFW(theQCB, theSctx, theLocation);
        String varName = theQCB.createInternalVarName("from");
        ExprVar outerVar = outerSFW.createFromVar(this, varName);

        for (int i = 0; i < origFieldExprs.size(); ++i) {

            Expr fieldExpr = origFieldExprs.get(i);

            origFieldExprs.set(i, rewriteSelectExprForGroupBy(i,
                                                              fieldExpr,
                                                              fieldExpr,
                                                              outerSFW,
                                                              outerVar));
        }

        outerSFW.addSelectClause(origFieldNames, origFieldExprs);

        if (origFieldExprs.size() != getNumFields()) {
            outerSFW.theNeedOuterSFWForGroupBy = true;
        }

        if (sortExpr != null) {
            addParent(sortExpr);
        }

        if (outerSFW.theNeedOuterSFWForGroupBy) {
            replace(outerSFW, false);
        } else {
            removeParent(outerSFW, false/*destroy*/);
            setFieldNames(origFieldNames);
            outerSFW = null;
        }

        if (topExpr == null) {
            topExpr = (outerSFW != null ? outerSFW : this);
        }

        return topExpr;
    }

    /*
     * Called from rewriteGroupBy on the inner SFW to create, in the outer
     * SELECT list, the expr that corresponds to a given expr from the
     * original SELECT list. The expr to create will reference the grouping
     * exprs and/or the aggragate functions that appear in the inner SELECT
     * list.
     *
     * - fieldPos : the position in the SELECT list of the expr to map in the
     *   the outer SELECT list
     * - fieldExpr : the expr to map
     * - fieldSubExpr : On the initial invocation, it is the same as the
     *   fieldExpr. Method traverses the fieldExpr subtree looking for sub
     *   exprs that are either aggregate functions or match the grouping exprs.
     * - outerSFW : the outer SFW
     * - outerFromVar : from FROM var of the outer SFW.
     */
    private Expr rewriteSelectExprForGroupBy(
        int fieldPos,
        Expr fieldExpr,
        Expr fieldSubExpr,
        ExprSFW outerSFW,
        ExprVar outerFromVar) {

        if (fieldSubExpr.getKind() == ExprKind.VAR) {

            ExprVar var = (ExprVar)fieldSubExpr;

            if (var.getVarKind() == VarKind.EXTERNAL ||
                var == outerFromVar) {

                return fieldExpr;
            }

            if (fieldSubExpr == fieldExpr) {
                for (int i = 0; i < theNumGroupExprs; ++i) {

                    Expr gbExpr = getFieldExpr(i);
                    if (gbExpr.getKind() == ExprKind.PROMOTE) {
                        gbExpr = gbExpr.getInput();
                    }

                    if (ExprUtils.matchExprs(fieldSubExpr, gbExpr)) {

                        String gbName = "gb-" + i;
                        Expr fieldRef = new ExprFieldStep(theQCB, theSctx,
                                                          theLocation,
                                                          outerFromVar, gbName);

                        if (i != fieldPos) {
                            outerSFW.theNeedOuterSFWForGroupBy = true;
                        }

                        return fieldRef;
                    }
                }
            }

            throw new QueryException(
                "Invalid expression in SELECT or ORDER-BY clause. " +
                "When a SELECT expression includes grouping, " +
                "expressions in the SELECT and ORDER-BY clauses must " +
                "reference grouping expressions, aggregate functions " +
                "or external variable only.",
                fieldExpr.getLocation());
        }

        QueryException.Location loc = fieldSubExpr.getLocation();

        /* convert avg to sum/count */
        if (fieldSubExpr.getFunction(FuncCode.FN_AVG) != null) {

            outerSFW.theNeedOuterSFWForGroupBy = true;

            String aggrName;
            String aggrName2;
            boolean addedSum = true;
            int numFields = getNumFields();
            int i;

            Expr inExpr = fieldSubExpr.getInput();

            Expr sumExpr = ExprFuncCall.create(theQCB, theSctx, loc,
                                               FuncCode.FN_SUM, inExpr);

            for (i = theNumGroupExprs; i < numFields; ++i) {
                if (ExprUtils.matchExprs(sumExpr, getFieldExpr(i))) {
                    aggrName = getFieldName(i);
                    break;
                }
            }

            if (i == numFields) {
                aggrName = "aggr-" + i;
                addField(aggrName, sumExpr);
            } else {
                aggrName = getFieldName(i);
                addedSum = false;
            }

            Expr sumRef = new ExprFieldStep(theQCB, theSctx, loc,
                                            outerFromVar, aggrName);

            Expr cntExpr = ExprFuncCall.create(theQCB, theSctx, loc,
                                               FuncCode.FN_COUNT_NUMBERS,
                                               inExpr.clone());

            for (i = theNumGroupExprs; i < numFields; ++i) {
                if (ExprUtils.matchExprs(cntExpr, getFieldExpr(i))) {
                    aggrName = getFieldName(i);
                    break;
                }
            }

            if (i == numFields) {
                aggrName2 = "aggr-" + (addedSum ? i + 1 : i);
                addField(aggrName2, cntExpr);
            } else {
                aggrName2 = getFieldName(i);
            }

            Expr cntRef = new ExprFieldStep(theQCB, theSctx, loc,
                                            outerFromVar, aggrName2);

            outerFromVar.computeType(false);

            Expr fieldRef = ExprFuncCall.createArithOp(theQCB, theSctx, loc,
                                                       'd', sumRef, cntRef);

            if (fieldSubExpr == fieldExpr) {
                fieldExpr.removeParent(null, true);
                return fieldRef;
            }

            fieldSubExpr.replace(fieldRef, false/*destroy*/);
            return fieldExpr;
        }

        if (fieldSubExpr.getFunction(null) != null &&
            fieldSubExpr.getFunction(null).isAggregate()) {

            Expr fieldRef;
            String aggrName = null;
            int numFields = getNumFields();
            int i;

            for (i = theNumGroupExprs; i < numFields; ++i) {
                if (ExprUtils.matchExprs(fieldSubExpr, getFieldExpr(i))) {
                    aggrName = getFieldName(i);
                    break;
                }
            }

            if (i == numFields) {
                aggrName = "aggr-" + i;
                addField(aggrName, fieldSubExpr);
                outerFromVar.computeType(false);
            }

            fieldRef = new ExprFieldStep(theQCB, theSctx, loc,
                                         outerFromVar, i);

            if (i != fieldPos || fieldSubExpr != fieldExpr) {
                outerSFW.theNeedOuterSFWForGroupBy = true;
            }

            if (fieldSubExpr == fieldExpr) {
                return fieldRef;
            }

            fieldSubExpr.replace(fieldRef, false/*destroy*/);

            /*
             * we must reset the i-th field expr because the replace() call
             * above replaces with fieldRef.
             */
            setFieldExpr(i, fieldSubExpr, false);
            return fieldExpr;
        }

        if (fieldSubExpr == fieldExpr &&
            fieldExpr.getKind() == ExprKind.ARRAY_CONSTR &&
            ((ExprArrayConstr)fieldExpr).isConditional()) {

            Expr arg = ((ExprArrayConstr)fieldExpr).getArg(0);

            int i;
            for (i = 0; i < theNumGroupExprs; ++i) {

                Expr gbExpr = getFieldExpr(i);
                if (gbExpr.getKind() == ExprKind.PROMOTE) {
                    gbExpr = gbExpr.getInput();
                }

                if (ExprUtils.matchExprs(arg, gbExpr)) {

                    String gbName = "gb-" + i;
                    Expr fieldRef = new ExprFieldStep(theQCB, theSctx, theLocation,
                                                      outerFromVar, gbName);

                    if (i != fieldPos) {
                        outerSFW.theNeedOuterSFWForGroupBy = true;
                    }

                    fieldExpr.removeParent(null, true);
                    return fieldRef;
                }
            }
        }

        int i;
        for (i = 0; i < theNumGroupExprs; ++i) {

            Expr gbExpr = getFieldExpr(i);
            if (gbExpr.getKind() == ExprKind.PROMOTE) {
                gbExpr = gbExpr.getInput();
            }

            if (ExprUtils.matchExprs(fieldSubExpr, gbExpr)) {

                String gbName = "gb-" + i;
                Expr fieldRef = new ExprFieldStep(theQCB, theSctx, theLocation,
                                                  outerFromVar, gbName);

                if (i != fieldPos || fieldSubExpr != fieldExpr) {
                    outerSFW.theNeedOuterSFWForGroupBy = true;
                }

                if (fieldSubExpr == fieldExpr) {
                    fieldExpr.removeParent(null, true);
                    return fieldRef;
                }

                fieldSubExpr.replace(fieldRef, true/*destroy*/);
                return fieldExpr;
            }
        }

        outerSFW.theNeedOuterSFWForGroupBy = true;

        /* Handle variadic functions. If the given expr and a gb expr are the
         * same variadic function, try to match their operands. Specifically,
         * if F and G are the sequences of operands for the given expr and the
         * gb expr respectively, we check if there is a subsequence of operands
         * in F that match all the operands in G, preserving operand order. For
         * example, if the given expr is a + b + c + d, and the gb exprs are
         * a, b + c, d, the method will return a + (b + c) = d. But if the gb
         * exprs are a, c + b, d, the method will raise an error. */
        while (fieldSubExpr.getFunction(null) != null) {

            ExprFuncCall fexpr = (ExprFuncCall)fieldSubExpr;
            Function func = fexpr.getFunction();

            if (!(func.getCode() == FuncCode.OP_ADD_SUB ||
                  func.getCode() == FuncCode.OP_MULT_DIV ||
                  func.getCode() == FuncCode.OP_AND ||
                  func.getCode() == FuncCode.OP_OR ||
                  func.getCode() == FuncCode.OP_CONCATENATE_STRINGS)) {
                break;
            }

            ExprConst fieldOpsExpr = null;
            String exprOps = null;
            String gbOps = null;

            if (func.getCode() == FuncCode.OP_ADD_SUB ||
                func.getCode() == FuncCode.OP_MULT_DIV) {
                fieldOpsExpr = (ExprConst)fexpr.getArg(fexpr.getNumArgs() - 1);
            }

            for (i = 0; i < theNumGroupExprs; ++i) {

                Expr gbExpr = getFieldExpr(i);
                if (gbExpr.getKind() == ExprKind.PROMOTE) {
                    gbExpr = gbExpr.getInput();
                }

                if (gbExpr.getFunction(null) != func) {
                    continue;
                }

                ExprFuncCall fgbExpr = (ExprFuncCall)gbExpr;
                int numGBArgs = fgbExpr.getNumArgs();
                int numExprArgs = fexpr.getNumArgs();
                int firstMatch = -1;

                if (fieldOpsExpr != null) {
                    --numGBArgs;
                    --numExprArgs;
                    exprOps = fieldOpsExpr.getValue().asString().get();
                    gbOps = ((ExprConst)fgbExpr.getArg(numGBArgs)).
                            getValue().asString().get();
                }

                int j = 0, k = 0;
                for (; j < numExprArgs && k < numGBArgs; ++j) {

                    Expr arg = fexpr.getArg(j);
                    Expr gbarg = fgbExpr.getArg(k);

                    if (firstMatch < 0) {
                        if (ExprUtils.matchExprs(arg, gbarg) &&
                            (exprOps == null || gbOps == null ||
                             exprOps.charAt(j) == gbOps.charAt(k))) {
                            firstMatch = j;
                            ++k;
                        } else if (numExprArgs - j - 1 < numGBArgs) {
                            break;
                        }
                    } else {
                        if ((exprOps == null || gbOps == null ||
                             exprOps.charAt(j) != gbOps.charAt(k)) &&
                            !ExprUtils.matchExprs(arg, gbarg)) {
                            firstMatch = -1;
                            break;
                        }
                        ++k;
                    }
                }

                if (k == numGBArgs && firstMatch >= 0) {

                    String gbName = "gb-" + i;
                    Expr fieldRef = new ExprFieldStep(theQCB, theSctx,
                                                      theLocation,
                                                      outerFromVar,
                                                      gbName);

                    fexpr.getArg(firstMatch).replace(fieldRef, true);

                    for (j = 1; j < numGBArgs; ++j) {
                        fexpr.removeArg(firstMatch+ j, true);
                    }

                    if (fieldOpsExpr != null && exprOps != null) {
                        String newOps =
                            exprOps.substring(0, firstMatch + 1) +
                            exprOps.substring(firstMatch + numGBArgs,
                                              exprOps.length());

                        fieldOpsExpr.setValue(FieldDefImpl.Constants.stringDef.
                                              createString(newOps));
                    }
                }
            } // for each gb expr

            for (i = 0; i < fexpr.getNumArgs(); ++i) {
                Expr arg = fexpr.getArg(i);

                if (arg.getKind() == ExprKind.FIELD_STEP &&
                    arg.getInput() == outerFromVar &&
                    ((ExprFieldStep)arg).getFieldPos() < theNumGroupExprs) {
                    continue;
                }

                rewriteSelectExprForGroupBy(fieldPos,
                                            fieldExpr,
                                            arg,
                                            outerSFW,
                                            outerFromVar);
            }

            return fieldSubExpr;
        }

        ExprIter children = fieldSubExpr.getChildren();
        while (children.hasNext()) {
            Expr child = children.next();
            rewriteSelectExprForGroupBy(fieldPos,
                                        fieldExpr,
                                        child,
                                        outerSFW,
                                        outerFromVar);
        }
        children.reset();
        return fieldExpr;
    }

    Expr rewriteSelectDistinct(Expr topExpr) {

        if (!hasGroupBy()) {

            assert(topExpr == this);

            if (hasSort()) {

                int i, j;
                for (i = 0; i < theSortExprs.size(); ++i) {
                    for (j = 0; j < theFieldExprs.size(); ++j) {
                        if (ExprUtils.matchExprs(theSortExprs.get(i),
                                                 theFieldExprs.get(j))) {
                            break;
                        }
                    }

                    if (j == theFieldExprs.size()) {
                        break;
                    }
                }

                if (i == theSortExprs.size()) {
                    ArrayList<Expr> gbExprs = new ArrayList<Expr>(theFieldExprs);
                    addGroupByClause(gbExprs);
                    return rewriteGroupBy(true);
                }
            } else {
                ArrayList<Expr> gbExprs = new ArrayList<Expr>(theFieldExprs);
                addGroupByClause(gbExprs);
                return rewriteGroupBy(true);
            }
        }

        int numFields = (topExpr.getKind() == ExprKind.SFW ?
                         ((ExprSFW)topExpr).getNumFields() :
                         ((ExprSort)topExpr).getNumFields());

        ExprGroup gbExpr = new ExprGroup(theQCB, theSctx, theLocation,
                                         topExpr, numFields);

        ArrayList<Expr> gbExprs = new ArrayList<Expr>(numFields);

        for (int i = 0; i < numFields; ++i) {
            gbExprs.add(new ExprFieldStep(theQCB, theSctx, theLocation,
                                          gbExpr.getVar(), i));
        }

        gbExpr.addFields(gbExprs);
        gbExpr.setIsDistinct(true);

        if (topExpr.getKind() == ExprKind.SFW) {

            ExprSFW sfw = (!theHasGenericSort ? this : (ExprSFW)topExpr);

            if (sfw.theOffsetExpr != null || sfw.theLimitExpr != null) {

                ExprSFW outerSFW = new ExprSFW(theQCB, theSctx, theLocation);
                String varName = theQCB.createInternalVarName("from");
                ExprVar outerVar = outerSFW.createFromVar(gbExpr, varName);

                if (sfw.getNumFields() > 1) {
                    outerSFW.addField(outerVar.getName(), outerVar);
                    outerSFW.setIsSelectStar(true);
                } else {
                    Expr fieldExpr =
                        new ExprFieldStep(theQCB, theSctx,
                                          sfw.getFieldExpr(0).getLocation(),
                                          outerVar, 0);
                    outerSFW.addField(sfw.getFieldName(0), fieldExpr);
                }

                if (sfw.theOffsetExpr != null) {
                    outerSFW.addOffset(sfw.theOffsetExpr);
                    sfw.removeOffset(false);
                }
                if (sfw.theLimitExpr != null) {
                    outerSFW.addLimit(sfw.theLimitExpr);
                    sfw.removeLimit(false);
                }

                return outerSFW;
            }
        }

        return gbExpr;
    }

    /*
     * Inject an ExprGroup on top of "this" SFW. This SFW is the inner-most
     * (grouping) SFW. See ExprSFW javadoc for details.
     */
    ExprGroup addGenericGroupBy() {

        ExprGroup groupExpr = new ExprGroup(theQCB, theSctx, theLocation,
                                            this, getNumGroupExprs());
        ExprVar gbVar = groupExpr.getVar();
        int numFields = getNumFields();

        ArrayList<Expr> gbFields = new ArrayList<Expr>(numFields);

        for (int i = 0; i < numFields; ++i) {

            Expr sfwField = getFieldExpr(i);
            Expr gbField = new ExprFieldStep(theQCB, theSctx,
                                             sfwField.getLocation(),
                                             gbVar, i);
            /* Leave the input expr of an aggregate function in the SFW and
             * move the aggregate function to the groupExpr. Notice that if
             * the input to the aggregate function may return multiple values,
             * we wrap this input expression with a sequence-aggregate function,
             * thus avoiding the boxing of the multiple values into an array
             * (via a conditional array constructor) and the subsequent unboxing
             * of the array by the original aggregate function. */
            if (i >= theNumGroupExprs) {
                ExprFuncCall aggrExpr = (ExprFuncCall)sfwField;

                if (aggrExpr.getFuncCode() == FuncCode.FN_COUNT_STAR) {
                    ExprConst oneExpr = new ExprConst(theQCB, theSctx,
                                                      sfwField.getLocation(), 1);
                    setFieldExpr(i, oneExpr, false);

                } else if (aggrExpr.getInput().isMultiValued()) {
                    sfwField = getPregroupingExpr(aggrExpr,
                                                  aggrExpr.getInput());
                    setFieldExpr(i, sfwField, false);

                    if (aggrExpr.getFuncCode() == FuncCode.FN_COUNT_NUMBERS ||
                        aggrExpr.getFuncCode() == FuncCode.FN_COUNT) {
                        aggrExpr = (ExprFuncCall)
                            ExprFuncCall.create(theQCB,
                                                aggrExpr.getSctx(),
                                                aggrExpr.getLocation(),
                                                FuncCode.FN_SUM,
                                                gbField);
                    } else {
                        aggrExpr.setArg(0, gbField, false);
                    }
                } else {
                    setFieldExpr(i, aggrExpr.getInput(), false);
                    aggrExpr.setArg(0, gbField, false);
                }

                gbField = aggrExpr;
            }

            gbFields.add(gbField);
        }

        groupExpr.addFields(gbFields);
        groupExpr.setIsDistinct(theIsGroupingForDistinct);

        replace(groupExpr, false);
        removeGroupBy();
        theHasGenericGroupBy = true;
        setDoNullOnEmpty(false);

        if (theOffsetExpr != null || theLimitExpr != null) {

            ExprSFW outerSFW;
            Expr parent = (groupExpr.hasParents() ?
                           groupExpr.getParent(0) :
                           null);

            if (parent != null && theHasGenericSort) {
                while (parent.getKind() != ExprKind.SORT) {
                    assert(parent.getKind() == ExprKind.SFW);
                    parent = parent.getParent(0);
                }
                parent = parent.getParent(0);
            }

            if (parent == null || parent.getKind() != ExprKind.SFW) {
                outerSFW = new ExprSFW(theQCB, theSctx, theLocation);
                String varName = theQCB.createInternalVarName("from");
                ExprVar outerVar = outerSFW.createFromVar(groupExpr, varName);

                if (numFields > 1) {
                    outerSFW.addField(outerVar.getName(), outerVar);
                    outerSFW.setIsSelectStar(true);
                } else {
                    Expr fieldExpr =
                        new ExprFieldStep(theQCB, theSctx,
                                          getFieldExpr(0).getLocation(),
                                          outerVar, 0);
                    outerSFW.addField(getFieldName(0), fieldExpr);
                }

                groupExpr.replace(outerSFW, false);
            } else {
                outerSFW = (ExprSFW)parent;
            }

            if (theOffsetExpr != null) {
                outerSFW.addOffset(theOffsetExpr);
                removeOffset(false);
            }
            if (theLimitExpr != null) {
                outerSFW.addLimit(theLimitExpr);
                removeLimit(false);
            }
        }

        return groupExpr;
    }

    private Expr getPregroupingExpr(
        ExprFuncCall aggrExpr,
        Expr inputExpr) {

        FunctionLib funcLib = CompilerAPI.getFuncLib();
        Function aggrFunc = aggrExpr.getFunction(null);

        switch (aggrFunc.getCode()) {
        case FN_COUNT:
            aggrFunc = funcLib.getFunc(FuncCode.FN_SEQ_COUNT_I);
            break;
        case FN_COUNT_NUMBERS:
            aggrFunc = funcLib.getFunc(FuncCode.FN_SEQ_COUNT_NUMBERS_I);
            break;
        case FN_SUM:
            aggrFunc = funcLib.getFunc(FuncCode.FN_SEQ_SUM);
            break;
        case FN_MIN:
            aggrFunc = funcLib.getFunc(FuncCode.FN_SEQ_MIN);
            break;
        case FN_MAX:
            aggrFunc = funcLib.getFunc(FuncCode.FN_SEQ_MAX);
            break;
        case FN_ARRAY_COLLECT:
            return inputExpr;
        default:
            throw new QueryStateException(
                "Unknown aggregate function: " + aggrFunc.getCode());
        }

        return ExprFuncCall.create(theQCB,
                                   aggrExpr.getSctx(),
                                   aggrExpr.getLocation(),
                                   aggrFunc,
                                   inputExpr);
    }

    boolean hasGenericGroupBy() {
        return theHasGenericGroupBy;
    }

    /*
     * Inject an ExprSort on top of this SFW and remove the sorting from this
     * SFW. If any sorting exprs do not appear in the SELECT list already,
     * they are added there, and another, outer SFW is added on top of the
     * ExprSort to project out these extra sort exprs. An outer SFW is also
     * added if this SFW has offest and/or limit clauses.
     */
    Expr addGenericSort() {

        assert(hasSort());

        if (hasEmptyGroupBy()) {
            removeSort();
            return null;
        }

        int numFields1 = getNumFields();
        int[] sortPositions = addSortExprsToSelect();
        int numFields2 = getNumFields();
        Expr topExpr;
        Expr inputExpr = getInput();

        ExprSort sortExpr = new ExprSort(theQCB, theSctx, theLocation,
                                         sortPositions, getSortSpecs(),
                                         inputExpr);
        sortExpr.setInput(this, false);

        if (numFields2 != numFields1 ||
            theOffsetExpr != null ||
            theLimitExpr != null) {
            ExprSFW outerSFW = new ExprSFW(theQCB, theSctx, theLocation);
            String varName = theQCB.createInternalVarName("from");
            ExprVar outerVar = outerSFW.createFromVar(sortExpr, varName);
            outerSFW.theIsSelectStar = theIsSelectStar;

            for (int i = 0; i < numFields1; ++i) {

                Expr fieldExpr =
                    new ExprFieldStep(theQCB, theSctx,
                                      getFieldExpr(i).getLocation(),
                                      outerVar, i);
                outerSFW.addField(getFieldName(i), fieldExpr);
            }

            if (theOffsetExpr != null) {
                outerSFW.addOffset(theOffsetExpr);
            }
            if (theLimitExpr != null) {
                outerSFW.addLimit(theLimitExpr);
            }

            topExpr = outerSFW;
        } else {
            topExpr = sortExpr;
        }

        /* Check whether the sort exprs are a prefix of the group exprs or
         * the group exprs are a prefix of the sort exprs. If so, and we
         * later discover that the group-by is going to be an index-based
         * one, the sort can be removed.
         * Note: do not apply this optimization if the query has inner joins.
         * This is ok for now, because currently, grouping over inner joins
         * is always done via a global ExprGroup (i.e., there is no sorting
         * index). If this changes in the future, this optimization has to
         * revisited for inner joins.  */
        if (hasGroupBy() && !hasJoin()) {
            boolean desc = false;
            boolean nullsLast = false;
            int i = 0;
            for (; i < theSortExprs.size() && i < theNumGroupExprs; ++i) {

                if (!ExprUtils.matchExprs(theSortExprs.get(i),
                                          theGroupExprs.get(i))) {
                    break;
                }

                SortSpec spec = theSortSpecs.get(i);
                if (i == 0) {
                    desc = spec.theIsDesc;
                    nullsLast = !spec.theNullsFirst;
                } else {
                    if (desc != spec.theIsDesc ||
                        nullsLast != (!spec.theNullsFirst)) {
                        break;
                    }
                }
            }

            if ((i == theSortExprs.size() || i == theNumGroupExprs) &&
                ((desc && !nullsLast) || (!desc && nullsLast))) {
                sortExpr.setMatchesGroupBy();
            }
        }

        removeSort();
        theHasGenericSort = true;
        theDoNullOnEmpty = false;

        if (theOffsetExpr != null) {
            removeOffset(false);
        }
        if (theLimitExpr != null) {
            removeLimit(false);
        }

        return topExpr;
    }

    boolean hasGenericSort() {
        return theHasGenericSort;
    }

    /*
     * Method to add each sort expr to the SELECT clause, if not there already.
     * The method returns the positions of the sort exprs in the SELECT list.
     */
    int[] addSortExprsToSelect() {

        int numFieldExprs = theFieldExprs.size();
        int numSortExprs = theSortExprs.size();

        int[] sortPositions = new int[numSortExprs];

        for (int i = 0; i < numSortExprs; ++i) {

            Expr sortExpr = theSortExprs.get(i);

            int j;
            for (j = 0; j < numFieldExprs; ++j) {

                Expr fieldExpr = theFieldExprs.get(j);

                if (fieldExpr.getKind() == ExprKind.ARRAY_CONSTR) {
                    if (((ExprArrayConstr)fieldExpr).isConditional()) {
                        fieldExpr = fieldExpr.getInput();
                    }
                }

                if (ExprUtils.matchExprs(sortExpr, fieldExpr)) {
                    break;
                }
            }

            if (j == numFieldExprs) {
                addField(theQCB.generateFieldName("sort"), sortExpr);
                theConstructsRecord = true;
                sortPositions[i] = theFieldExprs.size() - 1;
            } else {
                sortPositions[i] = j;
            }
        }

        computeType(false);
        return sortPositions;
    }

    boolean getGroupByExprCompleteShardKey() {
        return theGroupByExprCompleteShardKey;
    }

    void setGroupByExprCompleteShardKey() {
        theGroupByExprCompleteShardKey = true;
    }

    void addSortingIndex(IndexImpl index) {

        if (theSortingIndexes == null) {
            theSortingIndexes = new ArrayList<IndexImpl>(4);
        }

        theSortingIndexes.add(index);
    }

    boolean isSortingIndex(IndexImpl index) {

        if (theSortingIndexes != null &&
            theSortingIndexes.contains(index)) {
            return true;
        }

        return false;
    }

    boolean usesSortingIndex() {
        return theSortingIndex >= 0;
    }

    void setSortingIndex(IndexImpl index) {
        theSortingIndex = theSortingIndexes.indexOf(index);
    }

    void clearSortingIndexes() {
        if (theSortingIndexes != null) {
            theSortingIndexes.clear();
        }
    }

    void addMKIndexStorageSize(ExprFuncCall fncall) {

        if (theMKIndexStorageSizeCalls == null) {
            theMKIndexStorageSizeCalls = new ArrayList<ExprFuncCall>();
        }

        theMKIndexStorageSizeCalls.add(fncall);
    }

    /*
     * Decide whether to give credit to a multi-key index that is referenced
     * in an index_storage_size() call. Credit is given only if the index is
     * on the target table and the function call appears in the SELECT clause
     * and nowhere else.
     */
    boolean checkOptimizeMKIndexSizeCall(IndexImpl index) {

        if (theMKIndexStorageSizeCalls == null) {
            return false;
        }

        for (ExprFuncCall fncall : theMKIndexStorageSizeCalls) {

            String indexName = ((ExprConst)fncall.getArg(1)).getString();
            ExprVar var = (ExprVar)fncall.getArg(0);
            ExprBaseTable tableExpr = (ExprBaseTable)var.getDomainExpr();
            TableImpl table = var.getTable();

            if (!indexName.equals(index.getName()) ||
                table.getId() != index.getTable().getId()) {
                continue;
            }

            List<ExprFuncCall> allCalls = tableExpr.getIndexStorageSizeCalls();

            for (ExprFuncCall fncall2 : allCalls) {

                if (fncall == fncall2) {
                    continue;
                }

                String indexName2 = ((ExprConst)fncall2.getArg(1)).getString();
                ExprVar var2 = (ExprVar)fncall2.getArg(0);
                TableImpl table2 = var2.getTable();

                if (indexName2.equals(indexName) &&
                    table2.getId() == table.getId()) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    /*
     * Optimize index_storage_size() calls that apply to the given multi-key
     * index. The goal of the otimization is to evaluate this function call
     * directly from the index, instead of getting the row, extracting all
     * the relevant index keys, and probing the index for each key to get the
     * size of each associated entry.
     *
     * This method is called after index selection is done and only if:
     * - The given mulit-key index has been selected for use by the query.
     * - The index_storage_size calls appear in the SELECT clause only and
     *   nowhere else.
     * - The index_storage_size calls appear as either top-level exprs or
     *   as the argument of an aggregate function that is a top-level expr.
     *
     * If the above conditions hold, the result of the index_storage_size
     * call for any given row must be the sum of the sizes of the index
     * entries that (a) point to that row, (b) are accessed as the index is
     * being scanned, and (c) survive any WHERE predicates. To compute this
     * sum for each row, a generic group-by is added to the query, directly
     * above the ReceiveIter. The group-by groups by the primary key columns
     * (thus eliminating duplicates produced by the multi-key index scan) and
     * sums up the index-entry sizes extracted from the index. To extract the
     * index-entry sizes, an internal function (mkindex_storage_size) is used.
     */
    void optimizeMKIndexSizeCall(IndexImpl index) {

        assert(index.isMultiKey());

        int numFieldsSave = getNumFields();

        addPrimKeyToSelect(null/*TODO*/, true);

        int numFields = getNumFields();
        int[] fieldMap = new int[numFields];
        Expr[] gbFields = new Expr[numFields];
        String[] gbFieldNames = new String[numFields];
        int numGBCols = 0;
        int numAggrCols = 0;
        boolean addOuterSFW = false;

        if (numFieldsSave != numFields) {
            addOuterSFW = true;
        }

        ExprGroup gbExpr = new ExprGroup(theQCB, theSctx, theLocation,
                                         this, numFields);
        ExprVar gbVar = gbExpr.getVar();
        Expr topExpr = gbExpr;

        for (int i = 0; i < numFields; ++i) {

            Expr sfwField = getFieldExpr(i);

            if (sfwField.getFunction(FuncCode.FN_INDEX_STORAGE_SIZE) != null) {

                ExprFuncCall fncall = (ExprFuncCall)sfwField;
                String indexName = ((ExprConst)fncall.getArg(1)).getString();
                ExprVar var = (ExprVar)fncall.getArg(0);
                TableImpl table = var.getTable();

                if (indexName.equals(index.getName()) &&
                    table.getId() == index.getTable().getId()) {

                    fncall = (ExprFuncCall)
                             ExprFuncCall.create(theQCB, theSctx,
                                                 sfwField.getLocation(),
                                                 FuncCode.FN_MKINDEX_STORAGE_SIZE,
                                                 fncall.getArg(0),
                                                 fncall.getArg(1));
                    setFieldExpr(i, fncall, true);

                    Expr step = new ExprFieldStep(theQCB, theSctx,
                                                  sfwField.getLocation(),
                                                  gbVar, i);
                    Expr gbField = ExprFuncCall.create(theQCB, theSctx,
                                                       sfwField.getLocation(),
                                                       FuncCode.FN_SUM,
                                                       step);
                    ++numAggrCols;
                    gbFields[numFields - numAggrCols] = gbField;
                    gbFieldNames[numFields - numAggrCols] = getFieldName(i);
                    fieldMap[i] = numFields - numAggrCols;
                    continue;
                }
            }

            fieldMap[i] = numGBCols;
            if (i != numGBCols) {
                addOuterSFW = true;
            }

            gbFields[numGBCols] = new ExprFieldStep(theQCB, theSctx,
                                                    sfwField.getLocation(),
                                                    gbVar, i);
            gbFieldNames[numGBCols] = getFieldName(i);
            ++numGBCols;
        }

        gbExpr.setNumGroupExprs(numGBCols);
        gbExpr.addFields(gbFields, gbFieldNames);

        if (addOuterSFW) {
            gbExpr.setComputeFields();

            ExprSFW outerSFW = new ExprSFW(theQCB, theSctx, theLocation);
            String varName = theQCB.createInternalVarName("from");
            ExprVar outerVar = outerSFW.createFromVar(topExpr, varName);
            ArrayList<Expr> outerFieldExprs = new ArrayList<Expr>(numFields);
            ArrayList<String> outerFieldNames = new ArrayList<String>(numFields);

            for (int i = 0; i < numFieldsSave; ++i) {
                Expr sfwField = getFieldExpr(i);
                Expr step = new ExprFieldStep(theQCB, theSctx,
                                              sfwField.getLocation(),
                                              outerVar, fieldMap[i]);
                outerFieldExprs.add(step);
                outerFieldNames.add(theFieldNames.get(i));
            }

            outerSFW.addSelectClause(outerFieldNames, outerFieldExprs);
            topExpr = outerSFW;
        }

        replace(topExpr, gbExpr, false);
    }

    /**
     * Add all prim-key columns to the SELECT list. A prim-key column is added
     * if not already there. This is needed when the use of a multi-key index
     * requires duplicate elimination to be performed, based on the prim-key of
     * the result rows.
     */
    int[] addPrimKeyToSelect(ExprSFW joinBranch, boolean includeShardKey) {

        assert(includeShardKey || joinBranch != null);

        FromClause fc;

        if (joinBranch == null) {
            fc = theFromClauses.get(0);
        } else {
            fc = joinBranch.getFromClause(0);
        }

        ExprBaseTable tableExpr = fc.getTableExpr();
        TableImpl table = tableExpr.getTargetTable();

        int numPrimKeyCols = table.getPrimaryKeySize();
        int numShardKeyCols = table.getShardKeySize();

        if (theFieldExprs == null) {
            theFieldExprs = new ArrayList<Expr>(numPrimKeyCols);
            theFieldNames = new ArrayList<String>(numPrimKeyCols);
        }

        int numFieldExprs = theFieldExprs.size();
        int[] pkPositionsInSelect = new int[numPrimKeyCols];

        for (int i = 0; i < numPrimKeyCols; ++i) {

            if (!includeShardKey && i < numShardKeyCols) {
                continue;
            }

            Expr fieldExpr = null;
            int j;
            for (j = 0; j < numFieldExprs; ++j) {

                fieldExpr = theFieldExprs.get(j);

                if (ExprUtils.isPrimKeyColumnRef(tableExpr, table, i, fieldExpr)) {
                    break;
                }
            }

            if (j == numFieldExprs) {
                String pkColName = table.getPrimaryKeyColumnName(i);
                int pkColPos;
                ExprVar rowVar;
                ExprVar idxVar = null;
                if (tableExpr.targetTableUsesCoveringIndex()) {
                    idxVar = fc.getTargetTableIndexVar();
                }
                Expr primKeyExpr;

                if (table.isJsonCollection() && idxVar == null) {

                    rowVar = fc.getTargetTableVar();

                    primKeyExpr = new ExprFieldStep(getQCB(),
                                                    getSctx(),
                                                    getLocation(),
                                                    rowVar,
                                                    pkColName);
                } else {
                    if (idxVar != null) {
                        rowVar = idxVar;
                        pkColPos = idxVar.getIndex().numFields() + i;
                    } else {
                        rowVar = fc.getTargetTableVar();
                        pkColPos = table.getPrimKeyPos(i);
                    }

                    primKeyExpr = new ExprFieldStep(getQCB(),
                                                    getSctx(),
                                                    getLocation(),
                                                    rowVar,
                                                    pkColPos);
                }

                theFieldExprs.add(primKeyExpr);
                primKeyExpr.addParent(this);
                theFieldNames.add(theQCB.generateFieldName(pkColName));
                pkPositionsInSelect[i] = theFieldExprs.size() - 1;
                theConstructsRecord = true;
                ++theNumChildren;
            } else {
                pkPositionsInSelect[i] = j;
            }
        }

        computeType(false);

        return pkPositionsInSelect;
    }

    @Override
    int getNumChildren() {
        return theNumChildren;
    }

    int computeNumChildren() {
        return
            theFromClauses.size() +
            (theWhereExpr != null ? 1 : 0) +
            (theFieldExprs != null ? theFieldExprs.size() : 0) +
            (theSortExprs != null ? theSortExprs.size() : 0) +
            (theGroupExprs != null ? theGroupExprs.size() : 0) +
            (theOffsetExpr != null ? 1 : 0) +
            (theLimitExpr != null ? 1 : 0);
    }

    @Override
    ExprType computeType() {

        if (theFieldExprs == null) {
            return null;
        }

        Quantifier q = getDomainExpr(0).getType().getQuantifier();

        for (int i = 1; i < theFromClauses.size(); ++i) {

            Quantifier q1 = getDomainExpr(i).getType().getQuantifier();

            q = TypeManager.getUnionQuant(q, q1);

            if (q == Quantifier.STAR) {
                break;
            }
        }

        if (theWhereExpr != null) {
            q = TypeManager.getUnionQuant(q, Quantifier.QSTN);
        }

        int numFields = theFieldNames.size();

        if (numFields == 1 && theIsSelectStar) {

            Expr fieldExpr = getFieldExpr(0);
            ExprType fieldType = fieldExpr.getType();

            assert(!fieldExpr.mayReturnNULL());

            theConstructsRecord = false;
            ExprType type = TypeManager.createType(fieldType, q);
            return type;
        }

        FieldMap fieldMap = new FieldMap();

        for (int i = 0; i < numFields; ++i) {

            FieldDefImpl fieldDef = theFieldExprs.get(i).getType().getDef();
            boolean nullable = (theFieldExprs.get(i).mayReturnNULL() ||
                                (theFieldExprs.get(i).mayReturnEmpty() &&
                                 theDoNullOnEmpty));

            if (fieldDef.isJson()) {
                theQCB.theHaveJsonConstructors = true;
            }

            fieldMap.put(theFieldNames.get(i),
                         fieldDef,
                         nullable,
                         null/*defaultValue*/);
        }

        RecordDefImpl recDef = FieldDefFactory.createRecordDef(fieldMap,
                                                               null/*descr*/);
        ExprType type = TypeManager.createType(recDef, q);
        return type;
    }

    @Override
    public boolean mayReturnNULL() {

        if (getConstructsRecord()) {
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

        formatter.indent(sb);
        for (int i = 0; i < theFromClauses.size(); ++i) {
            FromClause fc = theFromClauses.get(i);
            sb.append("FROM-" + i + " :\n");
            fc.getDomainExpr().display(sb, formatter);
            sb.append(" as ");
            List<ExprVar> vars = fc.getVars();
            for (ExprVar var : vars) {
                sb.append(var.getName() + "  ");
            }
            sb.append("\n");
        }

        if (theWhereExpr != null) {
            formatter.indent(sb);
            sb.append("WHERE:\n");
            theWhereExpr.display(sb, formatter);
            sb.append("\n");
        }

        if (theGroupExprs != null) {
            formatter.indent(sb);
            sb.append("GROUP BY:\n");
            for (int i = 0; i < theGroupExprs.size(); ++i) {
                formatter.indent(sb);
                theGroupExprs.get(i).display(sb, formatter);
                if (i < theGroupExprs.size() - 1) {
                    sb.append(",\n");
                }
            }
            sb.append("\n");
        }

        if (theSortExprs != null) {
            formatter.indent(sb);
            sb.append("ORDER BY:\n");
            for (int i = 0; i < theSortExprs.size(); ++i) {
                formatter.indent(sb);
                theSortExprs.get(i).display(sb, formatter);
                if (i < theSortExprs.size() - 1) {
                    sb.append(",\n");
                }
            }
            sb.append("\n");
        }

        formatter.indent(sb);
        sb.append("SELECT:\n");

        if (theFieldExprs != null) {
            for (int i = 0; i < theFieldExprs.size(); ++i) {
                formatter.indent(sb);
                sb.append(theFieldNames.get(i)).append(": \n");
                theFieldExprs.get(i).display(sb, formatter);
                if (i < theFieldExprs.size() - 1) {
                    sb.append(",\n");
                }
            }
        }
    }
}
