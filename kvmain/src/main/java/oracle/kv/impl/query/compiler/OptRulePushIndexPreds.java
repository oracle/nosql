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
import java.util.Collections;
import java.util.Map;

import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprBaseTable.IndexHint;
import oracle.kv.impl.query.compiler.ExprSFW.FromClause;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.Index;

/**
 * The goal of this optimization rule is convert WHERE predicates into index
 * scan conditions in order to avoid a full table scan.
 *
 * The rule analyzes the predicates in a WHERE clause to find, for each index
 * associated with a given table (including the table's primary index), (a) a
 * starting and/or ending key that could be applied to a scan over that index,
 * and (b) predicates that can be evaluated during the index scan from the
 * index columns only, thus filtering the retrieved index keys further.
 *
 * The rule assumes the WHERE-clause expr is in CNF. For each index, it first
 * collects all CNF factors that are "index predicates", i.e., they can be
 * evaluated fully from the index columns only. For example, if the current
 * index is the primary-key index and C1, C2, C3 are the primary-key columns,
 * {@literal "C1 > 10" and "C2 = 3 or C3 < 20"} are primary index preds. Then,
 * for each index column, in the order that these columns appear in the index
 * (or primary key) declaration, the rule looks-for and processes index preds
 * that are comparison preds {@literal (eg, "C1 > 10" is a comparison pred, but
 * "C2 = 3 or C3 < 20" is not)}. The possible outcomes of processing an index
 * pred w.r.t. an index column are listed in the PredicateStatus enum
 * below. The rule stops processing the current index as soon as it finds an
 * index column for which there is no equality pred to be pushed to the index.
 *
 * After the rule has analyzed all indexes, it chooses the "best" index to
 * use among the indexes that had something pushed down to them.
 *
 * TODO: need a "good" heuristic to choose the "best" index, as well as
 * a compiler hint or USE INDEX clause to let the user decide.
 */
class OptRulePushIndexPreds {

    // TODO move this to the Optimizer obj, when we have one
    private RuntimeException theException = null;

    private ArrayList<ArrayList<IndexAnalyzer>> theAnalyzers;

    private boolean theCompletePrimaryKey;

    private boolean thePushSort = true;

    private boolean theCompleteShardKey;

    RuntimeException getException() {
        return theException;
    }

    void apply(Expr expr) {

        //System.out.println("Query before index selection: " + expr.display());

        try {
            theAnalyzers = new ArrayList<>(8);
            applyInternal(expr);
        } catch (RuntimeException e) {
            theException = e;
        }
    }

    private void applyInternal(Expr expr) {

        ExprJoin joinExpr = null;

        switch (expr.getKind()) {
        case SORT:
        case GROUP:
            applyInternal(expr.getInput());
            return;
        case DELETE_ROW:
            applyInternal(expr.getInput());
            ((ExprDeleteRow)expr).setIsCompletePrimarykey(theCompletePrimaryKey);
            return;
        case SFW: {
            ExprSFW sfw = (ExprSFW)expr;
            FromClause fc = sfw.getFirstFrom();
            ExprBaseTable tableExpr = fc.getTableExpr();

            if (tableExpr != null) {
                ArrayList<IndexAnalyzer> analyzers = applyOnSFW(sfw);
                theAnalyzers.add(analyzers);
                if (tableExpr.getPosInJoin() < 0) {
                    break;
                }
            } else {
                applyInternal(fc.getDomainExpr());

                if ((sfw.hasSort() && !thePushSort) || sfw.hasGroupBy()) {
                    addGenericSortOrGroup(sfw);
                }
            }

            return;
        }
        case JOIN: {
            joinExpr = (ExprJoin)expr;
            int numBranches = joinExpr.numBranches();

            for (int i = 0; i < numBranches; ++i) {
                 ExprSFW sfw = (ExprSFW)joinExpr.getBranch(i);
                 applyInternal(sfw);
            }
            break;
        }
        case UPDATE_ROW:
            applyInternal(expr.getInput());

            if (!theCompleteShardKey) {
                throw new QueryException(
                    "A complete shard key must be specified in the WHERE " +
                    "clause.");
            }

            if (!theCompletePrimaryKey &&
                ((ExprUpdateRow)expr).hasReturningClause()) {
                throw new QueryException(
                    "RETURNING clause is not supported unless the complete " +
                    "primary key is specified in the WHERE clause.");
            }

            ((ExprUpdateRow)expr).setIsCompletePrimarykey(theCompletePrimaryKey);
            return;
        case INSERT_ROW:
            return;
        default :
            throw new QueryStateException(
                "Unexpected kind of expression during index selection:" +
                expr.getKind());
        }

        if (theAnalyzers.size() == 1) {

            assert(joinExpr == null);
            ArrayList<IndexAnalyzer> analyzers = theAnalyzers.get(0);
            if (analyzers == null) {
                return;
            }

            IndexAnalyzer bestIndex = Collections.min(analyzers);
            IndexAnalyzer primaryAnalyzer = analyzers.get(0);
            assert(primaryAnalyzer.getIndex() == null);

            bestIndex.apply(primaryAnalyzer);

            addGenericSortOrGroup(bestIndex.theSFW);

            if (bestIndex.theOptimizeMKIndexSizeCall) {
                bestIndex.theSFW.optimizeMKIndexSizeCall(bestIndex.getIndex());
            }

            return;
        }

        assert(joinExpr != null);
        if (joinExpr == null) {
            /* Just to avoid compilation warning */
            throw new QueryStateException("cennot happen");
        }

        int numBranches = joinExpr.numBranches();
        assert(numBranches == theAnalyzers.size());
        ArrayList<IndexAnalyzer> bestIndexes = new ArrayList<>(numBranches);

        for (int i = 0; i < numBranches; ++i) {

            ArrayList<IndexAnalyzer> analyzers = theAnalyzers.get(i);

            if (analyzers == null) {
                Function empty = Function.getFunction(FuncCode.FN_SEQ_CONCAT);
                Expr emptyExpr = ExprFuncCall.create(joinExpr.getQCB(),
                                                     joinExpr.getSctx(),
                                                     joinExpr.getLocation(),
                                                     empty,
                                                     new ArrayList<Expr>());
                joinExpr.replace(emptyExpr, true);
                return;
            }

            bestIndexes.add(Collections.min(analyzers));
        }

        boolean pushSortAttempted = false;

        for (int i = 0; i < numBranches; ++i) {

            ExprSFW sfw = (ExprSFW)joinExpr.getBranch(i);
            IndexAnalyzer bestIndex = bestIndexes.get(i);
            assert(sfw == bestIndex.theSFW);

            if (sfw.hasSort()) {
                pushSortAttempted = true;
                if (!sfw.isSortingIndex(bestIndex.getIndex())) {
                    thePushSort = false;
                    break;
                }
            }
        }

        if (!pushSortAttempted) {
            thePushSort = false;
        }

        if (!thePushSort) {

            for (int i = 0; i < numBranches; ++i) {

                ExprSFW sfw = (ExprSFW)joinExpr.getBranch(i);
                if (!sfw.hasSort()) {
                    continue;
                }

                sfw.removeSort();
                sfw.clearSortingIndexes();
                ArrayList<IndexAnalyzer> analyzers = theAnalyzers.get(i);
                for (IndexAnalyzer analyzer : analyzers) {
                    analyzer.resetScore();
                }
                bestIndexes.set(i, Collections.min(analyzers));
            }
        } else {
            /* Remove the sort exprs from the join branches. They are not
             * needed anymore. */
            for (int i = 0; i < numBranches; ++i) {

                ExprSFW sfw = (ExprSFW)joinExpr.getBranch(i);
                if (!sfw.hasSort()) {
                    continue;
                }

                sfw.removeSort();
            }
        }

        for (int i = 0; i < numBranches; ++i) {

            ArrayList<IndexAnalyzer> analyzers = theAnalyzers.get(i);
            IndexAnalyzer primaryAnalyzer = analyzers.get(0);
            IndexAnalyzer bestIndex = bestIndexes.get(i);

            bestIndex.apply(primaryAnalyzer);
        }
   }

    private ArrayList<IndexAnalyzer> applyOnSFW(ExprSFW sfw) {

        ExecuteOptions opts = sfw.getQCB().getOptions();

        FromClause fc = sfw.getFirstFrom();
        ExprBaseTable tableExpr = fc.getTableExpr();

        boolean needsSortingIndex = false;
        String sortingOp = (sfw.hasSort() ? "order-by" : "group-by");

        if (opts.isProxyQuery() &&
            opts.getDriverQueryVersion() < ExecuteOptions.DRIVER_QUERY_V3 &&
            ((sfw.hasGroupBy() && sfw.getNumGroupExprs() > 0))) {
            needsSortingIndex = true;
        }

        IndexHint forceIndexHint = tableExpr.getForceIndexHint();
        TableImpl table = tableExpr.getTargetTable();
        int tablePos = tableExpr.getTargetTablePos();
        Map<String, Index> indexes = table.getIndexes();
        IndexAnalyzer primaryAnalyzer = null;

        ArrayList<IndexAnalyzer> analyzers = new ArrayList<>(1+indexes.size());

        /* Analyze the primary index first. We need to do this always, because
         * we need to discover if the query has a complete shard key. */
        primaryAnalyzer = new IndexAnalyzer(sfw, tableExpr, tablePos,
                                            null/*index*/);
        primaryAnalyzer.analyze();

        theCompletePrimaryKey = false;
        theCompleteShardKey = primaryAnalyzer.hasShardKey();
        if (theCompleteShardKey) {
            PrimaryKeyImpl pk = (PrimaryKeyImpl)
                primaryAnalyzer.getIndexKeys().get(0);
            theCompletePrimaryKey = pk.isComplete();
        }

        /* No reason to continue if the WHERE expr is always false */
        if (primaryAnalyzer.theSFW == null) {
            return null;
        }

        if (theCompletePrimaryKey &&
            tableExpr.getPosInJoin() < 0 &&
            tableExpr.getNumDescendants() == 0) {
            sfw.removeSort();
        }

        if (forceIndexHint != null) {

            IndexImpl forcedIndex = forceIndexHint.theIndex;

            IndexAnalyzer analyzer =
                (forcedIndex == null ?
                 primaryAnalyzer :
                 new IndexAnalyzer(sfw, tableExpr, tablePos, forcedIndex));

            analyzers.add(primaryAnalyzer);

            if (analyzer != primaryAnalyzer) {
                analyzer.analyze();
                analyzers.add(analyzer);
            }

            if (analyzer.theSFW == null) {
                return null;
            }

            if (needsSortingIndex && !sfw.isSortingIndex(forcedIndex)) {
                throw new QueryException(
                    sortingOp + " cannot be performed because there is the " +
                    "index forced via a hint does not sort all the table " +
                    "rows in the desired order.",
                    sfw.getLocation());
            }

            if (analyzer.isRejected()) {
                String indexName = (forcedIndex == null ?
                                    "primary" :
                                    forcedIndex.getName());
                throw new QueryException(
                    "The index forced via a hint cannot be used by the query.\n" +
                    "Hint index    : " + indexName + "\n",
                    sfw.getLocation());
            }

            return analyzers;
        }

        /* If the query specifies a complete primary key, use the primary
         * index to execute it and remove any order-by. */
        if (theCompletePrimaryKey) {
            analyzers.add(primaryAnalyzer);
            return analyzers;
        }

        if (!needsSortingIndex || sfw.isSortingIndex(null)) {
            analyzers.add(primaryAnalyzer);
        }

        boolean alwaysFalse = false;

        for (Map.Entry<String, Index> entry : indexes.entrySet()) {

            IndexImpl index = (IndexImpl)entry.getValue();

            IndexAnalyzer analyzer = new IndexAnalyzer(sfw, tableExpr,
                                                       tablePos, index);
            analyzer.analyze();

            if (analyzer.theSFW == null) {
                alwaysFalse = true;
                break;
            }

            if (!analyzer.isRejected() &&
                (!needsSortingIndex || sfw.isSortingIndex(index))) {
                analyzers.add(analyzer);
            }
        }

        if (alwaysFalse) {
            return null;
        }

        if (analyzers.isEmpty()) {
            throw new QueryException(
                sortingOp + " cannot be performed because there is no index " +
                "that orders all the table rows in the desired order",
                sfw.getLocation());
        }

        if (analyzers.get(0).getIndex() != null) {
            analyzers.add(0, primaryAnalyzer);
        }

        return analyzers;
    }

    private void addGenericSortOrGroup(ExprSFW sfw) {

        if (!sfw.usesSortingIndex()) {

            Expr topExpr = null;
            if (sfw.hasGroupBy()) {
                topExpr = sfw.addGenericGroupBy();
            } else if (sfw.hasSort()) {
                topExpr = sfw.addGenericSort();
            }

            if (topExpr != null) {
                sfw.replace(topExpr,
                            (topExpr.getInput() == sfw ?
                             topExpr :
                             topExpr.getInput()),
                            false);
            }
        }
    }
}
