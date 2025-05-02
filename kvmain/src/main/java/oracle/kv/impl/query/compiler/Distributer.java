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

import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.query.ExecuteOptions;

/**
 * Distributer is responsible for creating ExprReceive exprs in the exprs graph
 * and placing them at their appropriate position within the graph.
 *
 * It implements an algorithm which traverses the exprs graph looking for
 * ExprBaseTable nodes. For each ExprBaseTable it finds, it creates an
 * ExprReceive and places it right above the associated ExprBaseTable. It
 * then tries to pull-up the ExprReceive as far as it can go.
 *
 * Note: for now, a query can have at most one ExprBaseTable and as result at
 * most one ExprReceive will be added to expr graph.
 */
class Distributer extends ExprVisitor {

    QueryControlBlock theQCB;

    FunctionLib theFuncLib = CompilerAPI.getFuncLib();

    private final ExprWalker theWalker;

    private boolean theIsInJoin;

    Distributer(QueryControlBlock qcb) {
        theQCB = qcb;
        theWalker = new ExprWalker(this, false/*allocateChildrenIter*/);
    }

    void distributeQuery() {

        //System.out.println(theQCB.getRootExpr().display());
        theWalker.walk(theQCB.getRootExpr());
    }

    @Override
    void exit(ExprDeleteRow del) {

        Expr topExpr;
        Expr input = del.getInput();

        /*
         * If the input to ExprDeleteRow is not an ExprReceive, then it should
         * be a seq_concat() call. This is to cover the case where the compiler
         * determines that the WHERE condition is always false.
         */
        if (input.getKind() != ExprKind.RECEIVE) {

            topExpr = del;

            Function func = input.getFunction(FuncCode.FN_SEQ_CONCAT);

            if (func == null) {
                throw new QueryStateException(
                    "Input to local delete iterator is not seq_concat()");
            }

            ExprFuncCall emptyExpr = (ExprFuncCall)input;

            if (emptyExpr.getNumArgs() != 0) {
                throw new QueryStateException(
                    "Input to local delete iterator is not seq_concat()");
            }

        } else {
            /* Pull the RCV expr on top of this delete expr */
            ExprReceive rcv = (ExprReceive)del.getInput();
            del.setInput(rcv.getInput(), false/*destroy*/);
            rcv.setInput(del, false/*destroy*/);
            del.replace(rcv, false);
            topExpr = rcv;
        }

        /*
         * If there is no returning clause, create a client-side SFW on top
         * of the rcv. This client SFW will sum up the counts of deleted
         * rows coming from the RNs.
         */
        if (!del.hasReturningClause() && !del.isCompletePrimarykey()) {
            Location loc = del.getLocation();

            ExprSFW clientSFW = new ExprSFW(theQCB, del.getSctx(), loc);

            topExpr.replace(clientSFW, false);

            String varName = theQCB.createInternalVarName("delcount");

            ExprVar var = clientSFW.createFromVar(topExpr, varName);

            Expr field = new ExprFieldStep(theQCB, del.getSctx(), loc, var, 0);

            Expr sum = ExprFuncCall.create(theQCB, del.getSctx(), loc,
                                           FuncCode.FN_SUM, field);

            ArrayList<Expr> selectList = new ArrayList<Expr>(1);
            ArrayList<String> selectNames = new ArrayList<String>(1);
            selectList.add(sum);
            selectNames.add("numRowsDeleted");
            clientSFW.addEmptyGroupBy();
            clientSFW.addSelectClause(selectNames, selectList);
        }
    }

    @Override
    void exit(ExprUpdateRow upd) {

        if (upd.getInput().getKind() != ExprKind.RECEIVE) {
            return;
        }

        ExprReceive recv = (ExprReceive)upd.getInput();
        upd.setArg(0, recv.getInput(), false/*destroy*/);
        upd.replace(recv, false);
        recv.setInput(upd, false/*destroy*/);

        assert(recv.getDistributionKind() == DistributionKind.SINGLE_PARTITION);
    }

    @Override
    void exit(ExprBaseTable e) {

        ExprReceive recv = new ExprReceive(theQCB, theQCB.getInitSctx());
        recv.setInput(e, false/*destroy*/);
        e.replace(recv, false);

        recv.setEliminateIndexDups(e.getEliminateIndexDups());
        recv.setIsUpdate(e.getIsUpdate() || e.getIsDelete());
    }

    @Override
    void exit(ExprGroup grp) {

        if (grp.getInput().getKind() != ExprKind.RECEIVE) {
            return;
        }

        ExprReceive rcv = (ExprReceive)grp.getInput();

        /* It is not possible to do grouping at the RNs if the query uses a
         * multi-key index that requires duplicate elimination. This is
         * because duplicate elimination must be done before grouping and must
         * be done at the driver. */
        if (rcv.getEliminateIndexDups()) {
            return;
        }

        /* Pull the receive expr above the grp expr. */
        grp.setInput(rcv.getInput(), false/*destroy*/);
        rcv.setInput(grp, false/*destroy*/);
        grp.replace(rcv, false);

        /* Create a new ExprGroup on top the rcv to do regrouping at the driver */
        int numFields = grp.getNumFields();
        int numGroupExprs = grp.getNumGroupExprs();
        ArrayList<Expr> driverFields = new ArrayList<Expr>(numFields);

        ExprGroup driverGRP = new ExprGroup(theQCB, grp.getSctx(),
                                            grp.getLocation(),
                                            rcv, numGroupExprs);
        for (int i = 0; i < numFields; ++i) {
            Expr driverField =
                new ExprFieldStep(theQCB, grp.getSctx(),
                                  grp.getField(i).getLocation(),
                                  driverGRP.getVar(), i);

            if (i >= numGroupExprs) {
                ExprFuncCall aggrExpr = (ExprFuncCall)grp.getField(i);
                driverField = getRegroupingExpr(aggrExpr, driverField);
            }

            driverFields.add(driverField);
        }

        driverGRP.addFields(driverFields);
        driverGRP.setIsDistinct(grp.isDistinct());

        rcv.replace(driverGRP, false);
    }

    @Override
    boolean enter(ExprJoin join) {
        theIsInJoin = true;
        return true;
    }

    @Override
    void exit(ExprJoin join) {

        ExprReceive rcv = new ExprReceive(theQCB, join.getSctx());
        rcv.setInput(join, false);

        for (int i = 0; i < join.numBranches(); ++i) {

            ExprReceive joinRCV = (ExprReceive)join.getBranch(i);

            if (joinRCV.getDistributionKind() ==
                DistributionKind.SINGLE_PARTITION) {
                rcv.setDistributionKind(joinRCV.getDistributionKind());
                rcv.copySinglePartitionInfo(joinRCV);
            } else if (i == 0) {
                rcv.setDistributionKind(joinRCV.getDistributionKind());
            }

            if (joinRCV.getEliminateIndexDups()) {
                rcv.setEliminateIndexDups(true);
            }
        }

        for (int i = 0; i < join.numBranches(); ++i) {

            ExprReceive joinRCV = (ExprReceive)join.getBranch(i);

            if (rcv.getEliminateIndexDups()) {
                ExprSFW joinSFW = (ExprSFW)joinRCV.getInput();
                rcv.addJoinBranch(joinSFW);
            }

            joinRCV.replace(joinRCV.getInput(), true);
        }

        join.replace(rcv, false);
        theIsInJoin = false;
    }

    @Override
    boolean enter(ExprSFW sfw) {

        int posInJoin = -1;
        if (sfw.getDomainExpr(0).getKind() == ExprKind.BASE_TABLE) {
            ExprBaseTable tableExpr = (ExprBaseTable)sfw.getDomainExpr(0);
            posInJoin = tableExpr.getPosInJoin();
        }

        theWalker.walk(sfw.getDomainExpr(0));

        if (sfw.getDomainExpr(0).getKind() != ExprKind.RECEIVE) {
            return false;
        }

        boolean inDelete = (sfw.hasParents() &&
                            sfw.getParent(0).getKind() == ExprKind.DELETE_ROW);

        /*
         * Pull the receive expr above the SFW expr.
         */
        ExprReceive rcv = (ExprReceive)sfw.getDomainExpr(0);
        sfw.setDomainExpr(0, rcv.getInput(), false/*destroy*/);
        rcv.setInput(sfw, false/*destroy*/);
        sfw.replace(rcv, false);

        /*
         * Don't do duplicate elimination if this is a delete statement. When
         * a row gets deleted, all of its entries in the multikey index are
         * deleted immediately, so we are not going to retrieve the same row
         * again.
         */
        if (inDelete) {
            rcv.setEliminateIndexDups(false);
        }

        Expr offset = sfw.getOffset();
        Expr limit = sfw.getLimit();
        boolean hasSort = sfw.hasSort();
        boolean hasGroupBy = sfw.hasGroupBy();
        boolean hasOffset = (offset != null);
        boolean hasLimit = (limit != null);
        boolean eliminateIndexDups = (!theIsInJoin && rcv.getEliminateIndexDups());

        boolean isSinglePartition =
            (rcv.getDistributionKind() == DistributionKind.SINGLE_PARTITION);

        if ((hasSort || hasGroupBy) && posInJoin >= 0) {
            assert(sfw.usesSortingIndex());
        }

        theQCB.setHasSort(hasSort);

        /* If there is unnesting, dup elimination is done at the server */
        if (sfw.getNumFroms() > 1) {
            eliminateIndexDups = false;
            rcv.setEliminateIndexDups(false);
        }

        /*
         * If it is a single-partition query, sfw will be sent as-is to the
         * server and there is no need to add anything in the client-side plan,
         * unless index duplicate elimination is required.
         */
        if (isSinglePartition && !eliminateIndexDups && !hasGroupBy) {
            return false;
        }

        if (!hasSort &&
            !hasGroupBy &&
            !eliminateIndexDups &&
            !(hasOffset || hasLimit)) {
            return false;
        }

        /*
         * We are here because either (a) the SFW has order/group-by, or (b)
         * it has offset-limit and it's not a single-partition query, or (c)
         * we must do elimination of index duplicates.
         *
         * In all cases, we create a new SFW expr on top of the RCV expr,
         * using the RCV expr as the FROM expr. This new SFW will be executed
         * at the client, whereas the original SFW will be sent to the server.
         *
         * If the SFW expr has sort do the following:
         * - Add the sort exprs in the SELECT clause of the server SFW, if not
         *   there already.
         * - Add to the receive expr the positions of the sort exprs within
         *   the above SELECT clause; also add the sort specs.
         * - Add to the client SELECT clause the fields that correspond to the
         *   fields of the original SFW (before the addition of the sort exprs).
         *
         * If the SFW expr has grouping do the following:
         * - Add to the client SELECT clause the fields that correspond to the
         *   grouping exprs of the original SFW.
         * - Add to the client SELECT clause appropriate aggregate functions
         *   to re-aggregate the partial aggregates coming from the RNs.
         * - Make the receive expr a sorting one by adding as the sort
         *   positions the positions of the grouping exprs within the SELECT
         *   clause.
         *
         * If the SFW expr has offset-limit and the query is not single-
         * partition, do the following:
         * - Add the offset and limit to the client SFW. The offset-limit will
         *   essentially be done at the client.
         * - Remove the offset from the server SFW,
         * - Change the limit in the server SFW to be the sum of the original
         *   limit plus the original offset. This is the maximum number of
         *   results that any RN will need to compute and send to the client.
         *
         * If dup elimination is needed:
         * - Add in the SELECT clause of the server SFW exprs to retrieve the
         *   prim key column, if not there already.
         * - Add to the receive expr the positions of the prim key exprs within
         *   the above SELECT clause.
         * - Add to the client SELECT clause the fields that correspond to the
         *   fields of the original SFW.
         */
        int numFields = sfw.getNumFields();
        boolean isSelectStar = sfw.isSelectStar();
        StaticContext sctx = sfw.getSctx();
        Location loc = sfw.getLocation();

        if (hasSort && !isSinglePartition) {

            if (posInJoin < 0) {
                int[] sortExprPositions = sfw.addSortExprsToSelect();
                rcv.addSort(sortExprPositions, sfw.getSortSpecs());
                sfw.setDoNullOnEmpty(false);
            }
        }

        if (eliminateIndexDups) {
            boolean includeShardKey = true;
            ArrayList<ExprSFW> joinBranches = rcv.getJoinBranches();
            if (joinBranches != null) {
                for (ExprSFW jb : joinBranches) {
                    int[] primKeyPositions =
                        sfw.addPrimKeyToSelect(jb, includeShardKey);
                    rcv.addPrimKeyPositions(primKeyPositions);
                    includeShardKey = false;
                }
            } else {
                int[] primKeyPositions = sfw.addPrimKeyToSelect(null, true);
                rcv.addPrimKeyPositions(primKeyPositions);
            }
        }

        /*
         * If we didn't add any exprs in the server-side sfw and there is no
         * grouping/offset/limit, we are done (no need to add a client-side
         * SFW).
         */
        if (sfw.hasGenericGroupBy() ||
            (numFields == sfw.getNumFields() &&
             !hasGroupBy &&
             !hasOffset &&
             !hasLimit)) {
            return false;
        }

        ExprSFW clientSFW = new ExprSFW(theQCB, sctx, loc);

        ExprVar fromVar =
            clientSFW.createFromVar(rcv, theQCB.createInternalVarName("from"));

        ArrayList<Expr> fieldExprs = new ArrayList<Expr>(numFields);
        ArrayList<String> fieldNames = new ArrayList<String>(numFields);

        if (sfw.getConstructsRecord()) {

            if (numFields == sfw.getNumFields() && !hasGroupBy) {

                fieldExprs.add(fromVar);
                fieldNames.add(fromVar.getName());
                isSelectStar = true;

            } else {
                for (int i = 0; i < numFields; ++i) {

                    Expr clientFieldExpr =
                        new ExprFieldStep(theQCB,
                                          sfw.getSctx(),
                                          sfw.getFieldExpr(i).getLocation(),
                                          fromVar,
                                          sfw.getFieldName(i));

                    if (hasGroupBy && !sfw.isGroupingField(i)) {
                        ExprFuncCall aggrExpr = (ExprFuncCall)sfw.getFieldExpr(i);
                        clientFieldExpr = getRegroupingExpr(aggrExpr,
                                                            clientFieldExpr);
                    }

                    fieldExprs.add(clientFieldExpr);
                    fieldNames.add(sfw.getFieldName(i));
                }
            }
        } else {
            assert(numFields == 1);
            Expr clientFieldExpr = fromVar;

            if (hasGroupBy && !sfw.isGroupingField(0)) {
                ExprFuncCall aggrExpr = (ExprFuncCall)sfw.getFieldExpr(0);
                clientFieldExpr = getRegroupingExpr(aggrExpr,
                                                    clientFieldExpr);
            }

            fieldExprs.add(clientFieldExpr);
            fieldNames.add(sfw.getFieldName(0));
        }

        clientSFW.addSelectClause(fieldNames, fieldExprs);
        clientSFW.setIsSelectStar(isSelectStar);

        if (hasGroupBy) {
            int numGBExprs = sfw.getNumGroupExprs();
            clientSFW.setNumGroupExprs(numGBExprs);

            if (numGBExprs > 0 && !isSinglePartition) {
                int[] sortPositions = new int[numGBExprs];
                SortSpec[] sortSpecs = new SortSpec[numGBExprs];

                for (int i = 0; i < numGBExprs; ++i) {
                    sortPositions[i] = i;
                    sortSpecs[i] = new SortSpec(false, false);
                }

                rcv.addSort(sortPositions, sortSpecs);
                if (theQCB.getOptions().getDriverQueryVersion() >=
                    ExecuteOptions.DRIVER_QUERY_V2) {
                    theQCB.setHasSort(true);
                }
            }
        }

        if (hasOffset || hasLimit) {

            assert(!sfw.hasGenericSort() && !sfw.hasGenericGroupBy());

            if (hasLimit && hasOffset) {
                sfw.removeOffset(false/*destroy*/);
                if (eliminateIndexDups) {
                    sfw.removeLimit(false/*destroy*/);
                } else {
                    Expr newLimit = FuncArithOp.createArithExpr(offset, limit, "+");
                    sfw.setLimit(newLimit, false/*destroy*/);
                }
            } else if (hasLimit) {
                if (eliminateIndexDups) {
                    sfw.removeLimit(false/*destroy*/);
                }
            } else if (hasOffset) {
                sfw.removeOffset(false/*destroy*/);
            }

            clientSFW.addOffsetLimit(offset, limit);
        }

        rcv.replace(clientSFW, false);
        return false;
    }

    private Expr getRegroupingExpr(
        ExprFuncCall aggrExpr,
        Expr inputExpr) {

        Function aggrFunc = aggrExpr.getFunction(null);

        switch (aggrFunc.getCode()) {
        case FN_COUNT:
        case FN_COUNT_STAR:
        case FN_COUNT_NUMBERS:
        case FN_SUM:
            aggrFunc = theFuncLib.getFunc(FuncCode.FN_SUM);
            break;
        case FN_MIN:
            aggrFunc = theFuncLib.getFunc(FuncCode.FN_MIN);
            break;
        case FN_MAX:
            aggrFunc = theFuncLib.getFunc(FuncCode.FN_MAX);
            break;
        case FN_ARRAY_COLLECT:
            FuncCollect collect = (FuncCollect)aggrFunc;
            aggrFunc = new FuncCollectRegroup(collect.isDistinct());
            break;
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
}
