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

import static oracle.kv.impl.util.ThreadUtils.threadId;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import oracle.kv.Direction;
import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.Geometry;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.PrimaryKeyImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.StringValueImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr.ConstKind;
import oracle.kv.impl.query.compiler.Expr.ExprIter;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.ExprBaseTable.IndexHint;
import oracle.kv.impl.query.compiler.ExprInOp.In3BindInfo;
import oracle.kv.impl.query.compiler.ExprSFW.FromClause;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.Pair;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldRange;


/*
 * An IndexAnalyzer is associated with a SFW expr and an index I on a table T
 * that appears in the FROM clause of the SFW. I may be the primary index of
 * T. The IndexAnalyzer tries to find the best way to replace a full table scan
 * on T with an index scan on I. It does so by examinig the exprs appearing in
 * SFW clauses to determine which ones can be evaluated by the info stored in
 * the index.
 *
 * Some terminology:
 * -----------------
 *
 * MapBoth index:
 * An index that indexes both the keys and the elements of a map, with the keys
 * indexed before all the element paths. for example, an index on
 * (keys(map), map[].mf1, col2, map[].mf2, map[].mf3) is a MapBoth index.
 *
 * Start/stop predicate:
 * A pred that can participate in determining a starting and/or ending point
 * for an index range scan. Currently, potential start/stop preds are the ones
 * that have one of the following forms:
 * - expr op const
 * - expr IS (NOT) NULL
 * - EXISTS expr
 * where expr is a path expr that "matches" one of the index paths, and
 * op is a comparison operator (value or "anu" comparison). Notice that
 * whether a pred having one of the above forms will actually be used as a
 * start/stop pred depends on what other preds exists in the query.
 *
 * Index-filtering predicate:
 * A pred that can be applied on each index entry during an index scan,
 * because its evaluation depends only on the field values of the current
 * index entry.
 *
 * Sargable predicate (a.k.a index predicate):
 * A pred that is either a start/stop or an index-filtering pred.
 *
 * Top-level predicate:
 * If the root of WHERE-clause expression is an AND operator, the operands of
 * this AND are the top-level preds of theSFW. Otherwise, the whole WHERE-clause
 * expr is the single top-level pred of theSFW.
 *
 * Predicate factor:
 * A top-level pred may be a composite pred, consisting of a number of pred
 * factors. There are 2 cases of composite preds:
 *
 * 1. A pred that involves a path expr containing filtering steps.
 * For example, assume an index on (a.b.e, a.b.c[].d1, a.b.c[].d2).
 *
 * The pred: a.b[$elem.e = 5].c[$elem.d1 < 10].d2 =any 20 consists of 3 pred
 * factors: a.b.e = 5, a.b.c[$elem.d1 < 10], and a.b.c[$elem.d2 = 20].
 * All of these pred factors are sargable, and they can all be applied
 * together during the index scan.
 *
 * The pred a.b[$elem.e = 5 and $elem.c[].d1 <any 10].c[].d2 =any 20
 * consists of 3 pred factors: a.b.e = 5, a.b.c[].d1 <any 20, and
 * a.b.c[].d2 =any 20.  All of these pred factors are sargable, but the last 2
 * are mutually exclusive: they cannot be both used during the index scan,
 * because they are both existential preds that do not necessarily apply to the
 * same array element.
 *
 * 2. MapBoth preds:
 * This case may arise only if the index is a MapBoth index. Then, a "MapBoth"
 * pred is a value-comparison pred on one of the map-value index paths, for a
 * particular map key. For example: map.key1.mf1 = 3 is a MapBoth pred and
 * consistes of 2 pred factors: map.keys($entry.key = "key1") and
 * map.values($entry.value.mf1 = 3). Notice that a MapBoth pred is equivalent
 * to an existentially-quantified expr with 2 conditions; for example
 * map.key1.mf1 = 3 is equivalent to: [some $key in keys(map) satisfies
 * $key = "key1" and map.$key = 3]. A MapBoth pred may be pushed to a
 * MapBoth index as 2 start/stop preds: an equality pred on the map-key
 * field, and an equality or range pred on a map-value field. Here are some
 * example queries (only the WHERE clause shown):
 *
 * Q1. where map.key10.mf1 &gt; 10
 * The pred can be pushed to the index as [IC1 = "key10" and IC2 &gt; 10]
 *
 * Q2. where map.key5.mf1 = 3 and col2 = 20 and map.key5.mf2 = 5.
 * All preds can be pushed as [IC1 = "key5" and IC2 = 3 and IC3 = 20 and
 * IC4 = 5]
 *
 * Q3. where map.key5.mf1 = 3 and map.key5.mf2 = 5.
 * Both preds is pushable, the 2nd as a filtering pred:
 * [IC1 = "key5" and IC2 = 3 and (filtering) IC4 = 5]
 *
 * Q4. where map.key5.mf1 = 3 and (map.key5.mf2 &lt; 5 or map.key5.mf2 &gt; 15)
 * Both preds is pushable, the 2nd as a filtering pred:
 * [IC1 = "key5" and IC2 = 3 and (filtering) (IC4 &lt; 5 or IC4 &gt; 15)]
 *
 * Q5. where map.key5.mf1 = 3 and col2 = 20 and map.key6.mf2 = 5.
 * The 1st 2 preds can be pushed as [IC1 = "key5" and IC2 = 3 and
 * IC3 = 20]. Alternatively, the 3rd pred can be pushed as
 * [IC1 = "key6" and (filtering) IC4 = 5]. IndexAnalyzer has to choose one of
 * the map keys (key5 or key6). In this case it will to push on key5, as it
 * probably better than key6.
 *
 * Q6. where map.key5.mf1 = 3 and col2 = 20 and map.key6.mf1 = 5.
 * We have a choice whether to push the 1st or the 3rd pred. In this case
 * their "score" is the same, so it doesn't matter which. If we choose
 * the 1st, we push [IC1 = "key5" and IC2 = 3 and IC3 = 20].
 *
 * Q7. where map.key5.mf1 &gt; 3 and col2 = 20 and map.key6.mf1 = 5.
 * We have a choice whether to push the 1st or the 3rd pred. But pushing
 * the 3rd pred is probably better, so we push [IC1 = "key6" and IC2 = 5
 * and IC3 = 20].
 *
 * Q8. where map.key5.mf1 = 3 and col2 = 20 and map.key6.mf1 = 5 and
 *           map.key6.mf2 &lt; 30
 * We can push "key5" preds or "Key6" preds, but "key6" is probably better
 * (because it has more pushable preds), so we push [IC1 = "key6" and
 * IC2 = 5 and IC3 = 20 and IC4 &lt; 30].
 *
 * Notice that if there are any MapBoth preds, the index should basically
 * be treated as a simple index, instead of a multi-key index.
 *
 * Data Members:
 * -------------
 *
 * theQCB:
 * The QueryControlBlock of the query containing the SFW expr.
 *
 * theSctx:
 * The StaticContext associated with the SFW expr.
 *
 * theSFW:
 * The SFW expr to analyze.
 *
 * theTable:
 * The table T referenced by the SFW and indexed by I.
 *
 * theIndex:
 * The index I to analyze. If I is the primary index, theIndex is null.
 *
 * theIsHintIndex:
 * Whether I is an index named in a PREFER_INDEXES hint.
 *
 * theIsPrimary:
 * Whether I is T's primary index.
 *
 * theIsMapBothIndex:
 * Whether I is a MapBoth index.
 *
 * theNumFields:
 * the number of fields in I, including the primary-key fields.
 *
 * theIndexPaths:
 * The paths defining the index fields, including the primary-key fields.
 *
 * theWherePreds:
 * A array storing info for each "top-level" predicate appearing in the WHERE
 * clause of theSFW. It includes, among other things, the pred factors of the
 * top-level pred. This is needed if the index associated with this
 * IndexAnalyzer is actually chosen to be used by the query. Then, we must
 * decide where the whole top-level pred or any of its pred factors can
 * actually be removed from the WHERE clause. See comments in the apply()
 * method.
 *
 * thePredGroups:
 * We partition all the multikey/existential pred factors into groups that are
 * mutually exclussive with each other, that is, a pred factor from one group
 * cannot be pushed to the index together with a pred from another group,
 * because the 2 pred factors do not necessarily apply to the same array/map
 * element/field. An example of this was given above, in case 1 of the Predicate
 * factor definition. Here is another (simpler) example:
 *
 * Assume an index on a.b.c[] and the following WHERE clause:
 *
 * where exists a.b.c[10 <= $element and $element < 20] and a.b.c[]
 * =any 30
 *
 * There are 3 pred factors grouped into 2 groups:
 * a.b.c[10 <= $element] , a.b.c[$element < 20] and
 * a.b.c[] =any 30
 *
 * The 2 pred factors in the 1st group can both be pushed to the index because
 * they apply to the same array element. The 3rd pred factor can also be pushed,
 * but not together with any of the other 2 factors, because it does not apply
 * to the same array element.
 *
 * When there is more than one pred group, the algorithm must decide which one
 * is better to push to the index. In the above example, the 2nd group is
 * better, because it includes an equality pred versus the 2 range preds of
 * the 1st group. This choice is done by the chooseMultiKeyPredGroup() method.
 *
 * theUnnestedGroup:
 * A single group for pred factors on unnseted arrays/maps.
 *
 * theStartStopPreds:
 * Stores pred factors that are potential start/stop preds for the index
 * scan. It is implemented as a matrix that groups such pred factors by the
 * index field they apply to. Initially, all potential start/stop preds are
 * put in the matrix, but after we choose the best predicate group, preds
 * in the loosing groups are removed. Even within a single pred group, some
 * preds may be removed from theStartStopPreds for 2 reasons:
 * (a) A pred may turn out to be a filtering pred, because it applies to an
 * index field F, and there is another index field before F for which no
 * equality pred exists.
 * (b) Only a single equality pred, or up to 2 range preds may be applied
 * on each index field. Although not expected, the query may contain more
 * preds that can be applied on a single index field. Here are some examples:
 * a = 10 and a = $var
 * a > 20 and a > 30
 * a = 10 and a < $var.
 * In this case, we must choose which preds to apply on an index field. This
 * is done by the skipExtraneousPreds() method.
 *
 * theBestPredGroup:
 * The pred group that is chosen by the chooseMultiKeyPredGroup() method as
 * the "best" to apply.
 *
 * theHaveMapKeyEqPred:
 * Set to true (a) if the index is a map index indexing the map keys (it may
 * also index the values), (b) the query contains a pred of the form
 * keys(map) =any "foo", and (c) the pred can be pushed to the index. In this
 * case there is no need to do duplicate elimination.
 *
 * theFilteringPreds:
 * Stores the pred factors that were determined to be sargable as
 * index-filtering preds.
 *
 * theIndexKeys:
 * The keys to be used for the scans of the index. If this is the Indexanalyzer
 * for the primary index, the list contains PrimaKeyImpl instances, otherwise
 * it contains IndexKeyImpl instances. The index keys may be partial, and they
 * reflect the equality preds pushed on index fields. In most cases, the list
 * will contain only one key, but in some cases we may need to perform multiple
 * scans on the index during runtime. For example, this is the case with the
 * IN and EXISTS operators. An exists predicate translates to scaning two
 * different ranges in the index: one for entries less than EMPTY and another
 * for entries greater than EMPTY.
 *
 * theRanges:
 * The FieldRanges used for the index scans. Each FieldRange reflects the
 * one or two range preds that a pushed on a single index field. The number
 * of ranges in the list is always the same as the number of keys in
 * theIndexKeys. If there is no range pred associated with an index scan,
 * the corresponding entry in theRanges will be null.
 *
 * theBindKeys:
 * This is used to handle the cases where a start/stop pred contains external
 * variables, eg, foo = $x + $y, where foo is an indexed column of theTable.
 * If foo is an integer, we initially create a placeholder FieldValue with
 * value 0, and place it in the IndexKey or FieldRange (i.e., we push the
 * pred foo = 0). theBindKeys is then used to register the $x + $y expr.
 * The number of entries in theBindKeys is the same as the number of entries
 * in theIndexKeys and theRanges, i.e., one "bind key" per index scan. Each
 * bind key is an Expr list with one entry for each index field on which an
 * equality or IN pred is pushed, and 2 entries for the single index field on
 * which a FieldRange is pushed. The ordering of the entries in each bind key
 * is the same as the declaration order of the associated index fields. If any
 * of the predicates pushed on an index field do not have any external vars,
 * the associated entries in the corresponding bind key will be null. If the
 * current index is applied, the (unique) exprs in theBindKeys will be stored
 * into the associated ExprBaseTable, and during code generation, it they be
 * converted to an array of PlanIters and placed in the BaseTableIter. During
 * BaseTableIter.open(), the PlanIters stored in BaseTableIter.theBindKeys,
 * will be evaluated and the resulting values will be used to replace the
 * associated placeholders.
 *
 * theIn3BindInfos:
 * This is used if the query has IN3 start/stop predicates. It has one entry
 * for each IN3 pred that has at least one of its components pushed as a
 * start/stop pred. Such an entry contains the total number of components
 * in the IN3 pred (ie. the number of exprs on the LHS of the IN3 expr) and
 * the positions of the index fields on which (a subset of) the IN3 components
 * have been pushed.
 *
 * theHaveBindKeys:
 * Set to true if there is at least one start/stop predicate that contains
 * bind variables.
 *
 * theIsMultiKeyRange:
 * Whether the pred is a range pred for a multi key index. It is used in
 * scoring the index.
 *
 * theIsCovering:
 * Set to true if the index is a covering one, i.e., the whole query can be
 * evaluated from the index columns, without any need to retrieve any table
 * rows.
 *
 * theEliminateDups:
 * Whether duplication elimination must be done on the prim keys returned by
 * an index scan on a multikey index.
 *
 * theScore:
 * A crude metric of how effective the index is going to be in optimizing
 * table access. See getScore() method.
 *
 * theScore2:
 * Same as theScore, but without any special treatment for the complete-key
 * case. See getScore() method.
 *
 * theNumEqPredsPushed:
 * The number of equality predicates pushed as start/stop conditions. It
 * includes partially pushed preds. Used to compute theScore and theScore2
 * for each each index in order to choose the "best" applicable index (see
 * getScore() and compareTo() methods).
 */
class IndexAnalyzer implements Comparable<IndexAnalyzer> {

    static int theTrace = 0;

    /*
     * The relative value of each kind of predicate. Used to compute a
     * score for each each index in order to choose the "best" applicable
     * index (see getScore() and compareTo() methods).
     */
    final static int eqValue = 50;
    final static int inValue = 35;
    final static int vrangeValue = 16; // value-range pred
    final static int arangeValue = 8;  // any-range pred
    final static int filterEqValue = 26;
    final static int filterOtherValue = 7;

    private final static int theMaxScore = Integer.MAX_VALUE;

    private final static int theFullKeyScore = Integer.MAX_VALUE - 1;

    private final QueryControlBlock theQCB;

    private final StaticContext theSctx;

    ExprSFW theSFW;

    private final ExprBaseTable theTableExpr;

    private final TableImpl theTable;

    private final int theTablePos;

    private final int theTargetTablePos;

    private final IndexImpl theIndex;

    private final boolean theIsHintIndex;

    private final boolean theIsPrimary;

    private final boolean theIsMapBothIndex;

    private final int theNumFields;

    final List<IndexField> theIndexPaths;

    private final ArrayList<WherePredInfo> theWherePreds;

    private final ArrayList<PredInfo> thePartitionPreds;

    private final ArrayList<PredInfo> theShardPreds;

    private final ArrayList<PredGroup> thePredGroups;

    private PredGroup theUnnestedGroup;

    private final ArrayList<ArrayList<PredInfo>> theStartStopPreds;

    private PredGroup theBestPredGroup;

    private boolean theHaveMapKeyEqPred;

    private final ArrayList<PredInfo> theFilteringPreds;

    private final HashMap<Expr, ArrayList<ExprToReplace>> theExprRewriteMap;

    private ArrayList<RecordValueImpl> theIndexKeys;

    private ArrayList<FieldRange> theRanges;

    private final ArrayList<ArrayList<Expr>> theBindKeys;

    private final ArrayList<In3BindInfo> theIn3BindInfos;

    private boolean theHaveBindKeys;

    private boolean theIsMultiKeyRange;

    private boolean theIsCovering;

    private boolean theEliminateDups;

    private int theScore = -1;

    private int theScore2 = -1;

    private int theNumEqPredsPushed = 0;

    private int theNumInPredsPushed = 0;

    private int theNumInCompsPushed = 0;

    private boolean theIsRejected;

    private boolean theIsUnnestingIndex;

    boolean theOptimizeMKIndexSizeCall;

    ExprJoin theJoinExpr;
    ExprSFW  theOrigSFW;

    static private class ExprToReplace {
        Expr theExpr;
        int theIndexFieldPos;

        ExprToReplace(Expr expr, int pos) {
            theExpr = expr;
            theIndexFieldPos = pos;
        }
    }

    /**
     * Information for a top-level WHERE-clause predicate.
     *
     * thePred:
     * The full expr for this top-level pred.
     *
     * thePredInfos:
     * The pred factors of this top-level pred.
     *
     * theDoesSlicing:
     * Set to true if there is any path expr in this top-level pred that
     * contains a slicing array step. Such a step cannot be evaluated by
     * the index, and as a result, the top-level pred cannot be removed
     * from the WHERE clause.
     *
     * theLocalGroup:
     * A group of pred factors that belong to this top-level pred and whose
     * outer-most context var is positioned after the multikey step of the index.
     * The group also includes the "direct" pred factor (if any) of the top-
     * level pred, i.e. the pred factor that has no context var. All such
     * preds can be applied together. Any other pred factor (whose outer-most
     * context var is positioned before the multikey step of the index) is
     * placed in one PredGroup by themselves. This is too strict in that some
     * "non-local" pred factors may actually belong together in a pred group
     * (for example see query json_idx/q/filter13). However doing something
     * better would take a lot of effort, which is probably not worth.
     */
    private class WherePredInfo {

        int theId;

        Expr thePred;

        final ArrayList<PredInfo> thePredInfos = new ArrayList<PredInfo>(8);

        boolean theDoesSlicing;

        PredGroup theLocalGroup;

        WherePredInfo(Expr pred) {
            theId = theWherePreds.size();
            thePred = pred;
        }

        boolean add(PredInfo pi) {

            if (pi.theIsMapBothKey) {

                if (theTrace >= 2) {
                    trace("Collected keys() pred for MapBoth key. pi = " + pi);
                }

                thePredInfos.add(0, pi);
                return true;
            }

            boolean added = false;

            if (pi.isExists()) {
                Expr input = pi.thePred.getInput();

                if (input.getKind() == ExprKind.ARRAY_FILTER) {
                    ExprArrayFilter step = (ExprArrayFilter)input;
                    if (step.getPredExpr() == null) {
                        thePredInfos.add(pi);
                        added = true;
                    }
                } else if (input.getKind() == ExprKind.MAP_FILTER) {
                    ExprMapFilter step = (ExprMapFilter)input;
                    if (step.getPredExpr() == null) {
                        thePredInfos.add(pi);
                        added = true;
                    }
                } else {
                    thePredInfos.add(pi);
                    added = true;
                }
            } else {
                thePredInfos.add(pi);
                added = true;
            }

            if (added && theTrace >= 1) {
                trace("WPI-" + theId + " Collected pi: " + pi);
            }

            return added;
        }

        boolean doesFiltering() {
            return (thePredInfos.size() > 1 ||
                    thePredInfos.get(0).thePred != thePred);
        }

        boolean isFullyPushable() {

            if (theDoesSlicing) {
                return false;
            }

            for (PredInfo pi : thePredInfos) {

                if ((pi.theStatus != PredicateStatus.STARTSTOP &&
                     pi.theStatus != PredicateStatus.FILTERING &&
                     pi.theStatus != PredicateStatus.TRUE) ||
                    pi.isGeo()) {

                    if (theTrace >= 3) {
                        trace("WPI-" + theId + " is not fully pushable due " +
                              "to pi : " + pi);
                    }
                    return false;
                }
            }

            if (theTrace >= 3) {
                trace("WPI-" + theId + " is fully pushable");
            }

            return true;
        }
    }

    /*
     * Information about a pred factor.
     */
    private class PredInfo {

        WherePredInfo theEnclosingPred;

        Expr thePred;

        FuncCode theOp;

        boolean theIsValueComp;

        boolean theIsMapBothKey;

        boolean theIsExists;

        boolean theIsNotExists;

        boolean theIsGeo;

        Expr theVarArg;

        Expr theConstArg;

        FieldValueImpl theConstVal;

        int theInComp;

        ArrayList<Expr> theConstINArgs;

        boolean theIsPushedINpi;

        Expr theDistanceArg;

        double theDistance = -1;

        Geometry theGeom;

        IndexExpr theEpath;

        int theIPathPos = -1;

        PredGroup thePredGroup;

        PredicateStatus theStatus;

        PredInfo(WherePredInfo enclosingPred, Expr pred) {
            theEnclosingPred = enclosingPred;
            thePred = pred;
            theStatus = PredicateStatus.UNKNOWN;
        }

        boolean isEq() {
            return theOp == FuncCode.OP_EQ;
        }

        boolean isMin() {
            return (theOp == FuncCode.OP_GT || theOp == FuncCode.OP_GE);
        }

        boolean isMax() {
            return (theOp == FuncCode.OP_LT || theOp == FuncCode.OP_LE);
        }

        boolean isInclusive() {
            return (theOp == FuncCode.OP_GE || theOp == FuncCode.OP_LE);
        }

        boolean isExists() {
            return theIsExists;
        }

        boolean isGeo() {
            return theIsGeo;
        }

        boolean isInOp() {
            return thePred.getKind() == ExprKind.IN;
        }

        boolean isIn3Op() {
            return (thePred.getKind() == ExprKind.IN &&
                    ((ExprInOp)thePred).isIN3());
        }

        boolean isNear() {
            return theOp == FuncCode.FN_GEO_WITHIN_DISTANCE;
        }

        boolean isUnnested() {
            return theEpath.isUnnested();
        }

        String mapBothKey() {
            if (theEpath == null) {
                return null;
            }
            return theEpath.getMapBothKey();
        }

        boolean isMatched() {
            return theIPathPos >= 0;
        }

        IndexField getIndexPath() {
            return theIndexPaths.get(theIPathPos);
        }

        boolean isCompatible(PredInfo other) {

            assert(theIPathPos == other.theIPathPos);

            return (!theIndexPaths.get(theIPathPos).isMultiKey() ||
                    thePredGroup == other.thePredGroup);
        }

        boolean canBeRemoved() {
            return (isMatched() &&
                    thePred != theEnclosingPred.thePred &&
                    theEpath != null &&
                    (!getIndexPath().isMultiKey() ||
                     (theIsMapBothIndex &&
                      theEpath.getMapBothKey(theTable, theIndex) != null)) &&
                    theEpath.getFilteringPreds() == null);
        }

        Location getLocation() {
            return thePred.getLocation();
        }

        @Override
        public String toString() {

            StringBuffer sb = new StringBuffer();
            sb.append("{\n");
            sb.append("epath = ");
            sb.append((theEpath == null ? "null" : theEpath.getPathName()));
            sb.append("\nconst val = ").append(theConstVal);
            sb.append("\nMap Both key = ").append(mapBothKey());
            sb.append("\nstatus = " + theStatus);
            if (theTrace >= 4 && thePred != null) {
                sb.append("\npred = \n").append(thePred.display());
            }
            sb.append("\n}\n");

            return sb.toString();
        }
    }

    /*
     * Information about a group of compatible pred factors.
     *
     * theScore data field and the data fields after it are "transient" ones:
     * they are used only for scoring the group for the purpose of choosing the
     * "best" one.
     */
    private static class PredGroup {

        int theId;

        final ArrayList<PredInfo> thePredInfos = new ArrayList<PredInfo>(8);

        ExprVar theOuterCtxVar;

        String theMapBothKey;

        boolean theIsUnnested;

        int theScore;

        int theFieldScore;

        boolean theFoundRange;

        boolean theFilteringOnly;

        PredGroup(int id, PredInfo pi) {
            theId = id;
            thePredInfos.add(pi);
            pi.thePredGroup = this;
        }

        static void addUnnestedPred(IndexAnalyzer idx, PredInfo pi) {

            if (idx.theUnnestedGroup == null) {
                PredGroup pg = idx.addPredGroup(pi);
                pg.theIsUnnested = true;
                idx.theUnnestedGroup = pg;
                return;
            }

            idx.theUnnestedGroup.thePredInfos.add(pi);
            pi.thePredGroup = idx.theUnnestedGroup;
        }

        static boolean addMapBothPred(IndexAnalyzer idx, PredInfo pi) {

            boolean added = false;
            for (PredGroup pg : idx.thePredGroups) {
                if (pi.mapBothKey().equals(pg.theMapBothKey)) {
                    pg.thePredInfos.add(pi);
                    pi.thePredGroup = pg;
                    added = true;

                    if (theTrace >= 1) {
                        idx.trace("Added mapkey pi to pred group PG-" +
                                  pg.theId + " pi : " + pi);
                    }
                    break;
                }
            }

            if (!added) {
                PredGroup pg = idx.addPredGroup(pi);
                pg.theMapBothKey = pi.mapBothKey();
                if (theTrace >= 1) {
                    idx.trace("Created new pred group PG-" + pg.theId +
                              " for mapkey pi : " + pi);
                }
                return false;
            }

            return true;
        }
    }

    /**
     *
     */
    static enum PredicateStatus {

        UNKNOWN,

        /*
         * It's definitely not a start/stop pred. Such a pred may be later
         * determined to be an index-filtering one.
         */
        NOT_STARTSTOP,

        /*
         * It is a potentially start/stop pred
         */
        STARTSTOP,

        /*
         * It's an index filtering pred
         */
        FILTERING,

        /*
         * Definitely not sargable
         */
        SKIP,

        /*
         * The pred is always false. This makes the whole WHERE expr always
         * false.
         */
        FALSE,

        /*
         * The pred is always true, so it can be removed from the WHERE expr.
         */
        TRUE
    }

    IndexAnalyzer(
        ExprSFW sfw,
        ExprBaseTable tableExpr,
        int tablePos,
        IndexImpl index) {

        theQCB = sfw.getQCB();
        theSctx = sfw.getSctx();
        theSFW = sfw;
        theTableExpr = tableExpr;
        theTablePos = tablePos;
        theTable = tableExpr.getTable(tablePos);
        theTargetTablePos = tableExpr.getTargetTablePos();
        theIndex = index;
        theIsPrimary = (theIndex == null);

        int pkStartPos = 0;
        int pkSize = theTable.getPrimaryKeySize();

        if (!theIsPrimary) {
            theNumFields = theIndex.numFields() + pkSize;
            pkStartPos = theIndex.numFields();

            theIndexPaths = new ArrayList<IndexField>(theNumFields);
            theIndexPaths.addAll(theIndex.getIndexFields());
            theIsMapBothIndex = theIndex.isMapBothIndex();
        } else {
            theNumFields = pkSize;
            theIndexPaths = new ArrayList<IndexField>(theNumFields);
            theIsMapBothIndex = false;
        }

        List<String> pkColumnNames = theTable.getPrimaryKeyInternal();

        for (int i = 0; i < pkSize; ++i) {

            String name = pkColumnNames.get(i);

            IndexField ipath = new IndexField(theTable, name, null,
                                              i + pkStartPos);
            ipath.setType(theTable.getPrimKeyColumnDef(i));
            ipath.setNullable(false);
            theIndexPaths.add(ipath);
        }

        theIsHintIndex = theTableExpr.isIndexHint(theIndex);

        theWherePreds = new ArrayList<WherePredInfo>(32);
        thePredGroups = new ArrayList<PredGroup>(32);
        theStartStopPreds = new ArrayList<ArrayList<PredInfo>>(theNumFields);
        theFilteringPreds = new ArrayList<PredInfo>();
        theExprRewriteMap = new HashMap<Expr, ArrayList<ExprToReplace>>();
        thePartitionPreds = new ArrayList<PredInfo>();
        theShardPreds = new ArrayList<PredInfo>();

        for (int i = 0; i < theNumFields; ++i) {
            theStartStopPreds.add(null);
        }

        theIndexKeys = new ArrayList<RecordValueImpl>(1);
        theRanges = new ArrayList<FieldRange>(1);
        theBindKeys = new ArrayList<ArrayList<Expr>>(1);
        theIndexKeys.add(createIndexKey());
        theRanges.add(null);
        theBindKeys.add(createBindKey());
        theIn3BindInfos = new ArrayList<In3BindInfo>();

        theJoinExpr = (ExprJoin)theSFW.findAncestorOfKind(ExprKind.JOIN);
        if (theJoinExpr != null) {
            theOrigSFW = (ExprSFW)theJoinExpr.getParent(0);
        }
    }

    void trace(String msg) {
        System.out.println("QUERY " + theQCB.getOptions().getQueryName() +
                           " (" + threadId(Thread.currentThread()) + ") " +
                           theTable.getFullName() + ":" + getIndexName() +
                           " in join pos " + theTableExpr.getPosInJoin() +
                           " : " + msg);
    }

    boolean isPrimary() {
        return theIsPrimary;
    }

    boolean isRejected() {
        return theIsRejected;
    }

    IndexImpl getIndex() {
        return theIndex;
    }

    //@SuppressWarnings("unused")
    String getIndexName() {
        return (theIsPrimary ? "primary" : theIndex.getName());
    }

    private RecordValueImpl createIndexKey() {

        if (theIsPrimary) {
            return theTable.createPrimaryKey();
        }
        return theIndex.createIndexKey();
    }

    private ArrayList<Expr> createBindKey() {

        ArrayList<Expr> bindKey = new ArrayList<Expr>(theNumFields + 1);
        for (int i = 0; i <= theNumFields; ++i) {
            bindKey.add(null);
        }
        return bindKey;
    }

    private ArrayList<Expr> cloneBindKey(ArrayList<Expr> bk) {
        return new ArrayList<Expr>(bk);
    }

    ArrayList<RecordValueImpl> getIndexKeys() {
        return theIndexKeys;
    }

    boolean hasShardKey() {
        if (theIsPrimary &&
            theIndexKeys.size() == 1 &&
            theIn3BindInfos.size() == 0) {
            return ((PrimaryKeyImpl)(theIndexKeys.get(0))).hasShardKey();
        }
        return false;
        // TODO: enhance in case of IN op
    }

    private PredGroup addPredGroup(PredInfo pi) {
        PredGroup pg = new PredGroup(thePredGroups.size(), pi);
        thePredGroups.add(pg);
        return pg;
    }

    private void addStartStopPred(PredInfo pi) {

        int ipos = pi.theIPathPos;
        ArrayList<PredInfo> startstopPIs = theStartStopPreds.get(ipos);

        if (startstopPIs == null) {
            startstopPIs = new ArrayList<PredInfo>();
            theStartStopPreds.set(ipos, startstopPIs);
        }

        if (theTrace >= 1) {
            trace("Added startstop pred at pos " + ipos + " pi = " + pi);
        }
        startstopPIs.add(pi);
    }

    /**
     * Remove a pred from the WHERE clause. The pred has either been
     * pushed in the index or is always true.
     */
    private void removePred(Expr pred) {

        if (theTrace >= 3) {
            trace("removing predicate:\n" + pred.display());
        }

        if (pred == null) {
            return;
        }

        int numParents = pred.getNumParents();

        if (numParents == 0) {
            if (theTrace >= 3) {
                trace("predicate has no parents");
            }
            return;
        }

        Expr parent = pred.getParent(0);

        if (numParents > 1) {

            if (theTrace >= 3) {
                trace("predicate has more than 1 parents");
            }

            /*
             * It's possible for the pred to have a second parent, if it is
             * a non-matched index-filtering pred that has been added to the
             * base table expr.
             */
            if (numParents != 2 ||
                (pred.getParent(1).getKind() != ExprKind.BASE_TABLE &&
                 pred.getParent(1).getFunction(FuncCode.OP_AND) == null)) {
                throw new QueryStateException(
                    "Trying to remove a pred with more than one parents. pred:\n" +
                    pred.display() + "\nnum parents = " + numParents +
                    " 2nd parent:\n" + pred.getParent(1).display());
            }
        }

        if (theTrace >= 3) {
            trace("predicate parent:\n" + parent.display());
        }

        Expr whereExpr = theSFW.getWhereExpr();

        if (pred == whereExpr) {
            theSFW.removeWhereExpr(true/*destroy*/);

        } else {
            parent.removeChild(pred, true/*destroy*/);

            if (parent == whereExpr && whereExpr.getNumChildren() == 0) {
                theSFW.removeWhereExpr(true /*destroy*/);
            }
        }
    }

    /*
     * The whole WHERE expr was found to be always false. Replace the
     * whole SFW expr with an empty expr.
     */
    private void processAlwaysFalse() {

        Function empty = Function.getFunction(FuncCode.FN_SEQ_CONCAT);
        Expr emptyExpr = ExprFuncCall.create(theQCB, theSctx,
                                             theSFW.getLocation(),
                                             empty,
                                             new ArrayList<Expr>());
        if (theQCB.getRootExpr() == theSFW) {
            theQCB.setRootExpr(emptyExpr);
        } else {
            theSFW.replace(emptyExpr, true);
        }

        theSFW = null;
    }

    /**
     * Used to sort the IndexAnalyzers in decreasing "value" order, where
     * "value" is a heuristic estimate of how effective the associated
     * index is going to be in optimizing the query.
     */
    @Override
    public int compareTo(IndexAnalyzer other) {

        int numFields1 = theNumFields;
        int numFields2 = other.theNumFields;

        boolean multiKey1 = (theIsPrimary ? false : theIndex.isMultiKey());

        boolean multiKey2 = (other.theIsPrimary ?
                             false :
                             other.theIndex.isMultiKey());

        /* Make sure the index scores are computed */
        getScore();
        other.getScore();

        if (theTrace >= 2) {
            trace("Comparing indexes " +  getIndexName() + " and " +
                  other.getIndexName() + "\nscore1 = " + theScore +
                  " score2 = " + other.theScore);
        }

        /*
         * If one of the indexes is covering, ....
         */
        if (theIsCovering != other.theIsCovering) {

            if (theIsCovering) {

                if (theTrace >= 2) {
                    trace("Index is covering");
                }

                /*
                 * If the other is a preferred index, choose the covering
                 * index if it has at least one eq start/stop condition
                 * or 2 range start/stop conditions.
                 */
                if (!theIsHintIndex && other.theIsHintIndex) {
                    FieldRange range = theRanges.get(0);
                    return (theNumEqPredsPushed > 0 ||
                            (range != null &&
                             range.getStart() != null &&
                             range.getEnd() != null) ?
                            -1 : 1);
                }

                /* If the other index does not have a complete key, choose
                 * the covering index. */
                if (other.theScore != theFullKeyScore) {
                    return -1;
                }

                /* The other index has a complete key. Choose the covering
                 * index if its score is >= to the score of the other index
                 * without taking into account the key completeness. */
                return (theScore >= other.theScore2 ? -1 : 1);
            }

            if (other.theIsCovering) {

                if (theTrace >= 2) {
                    trace("Index " + other.getIndexName() + " is covering");
                }

                if (!other.theIsHintIndex && theIsHintIndex) {
                    FieldRange range = theRanges.get(0);
                    return (other.theNumEqPredsPushed > 0 ||
                            (range != null &&
                             range.getStart() != null &&
                             range.getEnd() != null) ?
                            1 : -1);
                }

                if (theScore != theFullKeyScore) {
                    return 1;
                }

                return (other.theScore >= theScore2 ? 1 : -1);
            }
        }

        if (theScore == other.theScore && theScore != theFullKeyScore) {

            /* If one of the indexes is a sorting one and the other is not,
             * choose the sorting one */
            if (theSFW.isSortingIndex(theIndex)) {
                if (!theSFW.isSortingIndex(other.theIndex)) {
                    return -1;
                }
            } else if (theSFW.isSortingIndex(other.theIndex)) {
                return 1;
            }

            /* If none of the indexes has any predicates pushed and one of
             * them is the primary index, choose that one. */
            if (theScore == 0) {

                if (theIsPrimary || other.theIsPrimary) {
                    return (theIsPrimary ? -1 : 1);
                }

                if (multiKey1 != multiKey2) {
                    return (multiKey1 ? 1 : -1);
                }
            }

            /* If one of the indexes is specified in a hint, choose that one */
            if (theIsHintIndex != other.theIsHintIndex) {
                return (theIsHintIndex ? -1 : 1);
            }

            /* If one of the indexes is multi-key and other simple, choose
             * the simple one. */
            if (multiKey1 != multiKey2) {
                return (multiKey1 ? 1 : -1);
            }

            /* If one of the indexes is the primary index, choose that one. */
            if (theIsPrimary || other.theIsPrimary) {
                return (theIsPrimary ? -1 : 1);
            }

            /* If both indexes have the same number of IN comps pushed, choose
             * the one with the less number of IN preds */
            if (theNumInPredsPushed > 0 &&
                theNumInCompsPushed == other.theNumInCompsPushed &&
                theNumInPredsPushed != other.theNumInPredsPushed) {
                return (theNumInPredsPushed < other.theNumInPredsPushed ? -1 : 1);
            }

            /* Choose the index with the smaller number of fields. This is
             * based on the assumption that if the same number of preds are
             * pushed to both indexes, the more fields the index has the less
             * selective the pushed predicates are going to be. */
            if (numFields1 != numFields2) {
                return (numFields1 < numFields2 ? -1 : 1);
            }

            /*
             * TODO ???? Return the one with the smaller key size
             */

            return 0;
        }

        /*
         * If we have a complete key for one of the indexes, choose that
         * one, unless the other one has a better score when not taking
         * index completeness into account and has at least one eq predicate
         * pushed to it (so that it won't result to a full of big index scan)
         */
        if (theScore == theFullKeyScore) {

            if (other.theScore2 > theScore2 && other.theNumEqPredsPushed > 0) {
                return 1;
            }

            if (other.theScore2 == theScore2 &&
                !theSFW.isSortingIndex(theIndex) &&
                theSFW.isSortingIndex(other.theIndex)) {
                return 1;
            }

            return -1;
        }

        if (other.theScore == theFullKeyScore) {

            if (theScore2 > other.theScore2 && theNumEqPredsPushed > 0) {
                return -1;
            }

            if (other.theScore2 == theScore2 &&
                !theSFW.isSortingIndex(other.theIndex) &&
                theSFW.isSortingIndex(theIndex)) {
                return -1;
            }

            return 1;
        }

        /*
         * If one of the indexes is specified in a hint, choose that one.
         */
        if (theIsHintIndex != other.theIsHintIndex) {
            return (theIsHintIndex ? -1 : 1);
        }

        return (theScore > other.theScore ? -1 : 1);
    }

    /**
     * Computes the "score" of an index w.r.t. this query, if not done
     * already.
     *
     * Score is a crude estimate of how effective the index is going to
     * be in optimizing table access. Score is only a relative metric,
     * i.e., it doesn't estimate any real metric (e.g. selectivity), but
     * it is meant to be used only in comparing the relative value of two
     * indexes in order to choose the "best" among all applicable indexes.
     *
     * Score is an integer computed as a weighted sum of the predicates
     * that can be pushed into an index scan (as start/stop conditions or
     * filtering preds).  However, if there is a complete key for an index,
     * that index gets the highest score (theFullKeyScore).
     */
    private int getScore() {

        if (theScore >= 0) {
            return theScore;
        }

        IndexHint forceIndexHint = theTableExpr.getForceIndexHint();
        if (forceIndexHint != null && forceIndexHint.theIndex == theIndex) {
            theScore = theMaxScore;
            theScore2 = theMaxScore;
            return theScore;
        }

        theScore = 0;
        theScore2 = 0;

        theScore += theNumEqPredsPushed * eqValue;

        theScore += theNumInCompsPushed * inValue;

        FieldRange range = theRanges.get(0);

        if (range != null) {

            if (range.getStart() != null) {
                theScore += (theIsMultiKeyRange ? arangeValue : vrangeValue);
            }

            if (range.getEnd() != null) {
                theScore += (theIsMultiKeyRange ? arangeValue : vrangeValue);
            }
        }

        for (PredInfo pi : theFilteringPreds) {
            if (pi.isEq()) {
                theScore += filterEqValue;
            } else {
                theScore += filterOtherValue;
            }
        }

        theScore2 = theScore;

        if (theTrace >= 2) {
            trace("Score for index " + getIndexName() + " = " + theScore +
                  "\ntheNumEqPredsPushed = " + theNumEqPredsPushed);
        }

        int numPrimKeyCols = theTable.getPrimaryKeySize();
        int numFields = (theIsPrimary ?
                         numPrimKeyCols :
                         theNumFields - numPrimKeyCols);

        if (theNumEqPredsPushed == numFields) {
            theScore = theFullKeyScore;
            return theScore;
        }

        if (theIndex != null &&
            theTableExpr.isIndexStorageSizeCallForIndex(theIndex) &&
            (!theIndex.isMultiKey() ||
             theSFW.checkOptimizeMKIndexSizeCall(theIndex))) {
            theScore += filterOtherValue;
            if (theIndex.isMultiKey()) {
                theOptimizeMKIndexSizeCall = true;
                theEliminateDups = false;
            }
        }

        return theScore;
    }

    void resetScore() {
        theScore = -1;
        theScore2 = -1;
    }

    /**
     * The index has been chosen among the applicable indexes, so do the
     * actual pred pushdown and remove all the pushed preds from the
     * where clause.
     */
    void apply(IndexAnalyzer primaryAnalyzer) {

        if (theTrace >= 1) {
            trace("\nSelected index: " + theTable.getFullName() + ":" +
                  getIndexName() + "\n");
        }

        if (theSFW.isSortingIndex(theIndex)) {
            theSFW.setSortingIndex(theIndex);
        }

        /* Collect info to be used later to eliminate unused FROM vars */
        int numFroms = theSFW.getNumFroms();
        int[] varRefsCounts = new int[numFroms];

        for (int i = 0; i < numFroms; ++i) {

            FromClause fc = theSFW.getFromClause(i);

            /* Table row variables can not be eliminated ever */
            if (fc.getTargetTable() != null) {
                varRefsCounts[i] = 0;
            } else {
                varRefsCounts[i] = fc.getVar().getNumParents();
            }
        }

        /* If the primary index is used for sorting, we don't need to sort
         * by any pk columns that are not part of the shard key. So, if the
         * order by includes any such columns, remove them. */
        if (theIsPrimary && theSFW.isSortingIndex(null)) {

            int numShardKeys = theTable.getShardKeySize();
            int numSortExprs = theSFW.getNumSortExprs();

            if (numSortExprs > numShardKeys) {
                while (numSortExprs > numShardKeys) {
                    theSFW.removeSortExpr(numSortExprs - 1, true/*destroy*/);
                    --numSortExprs;
                }
            }
        }

        /* Add the scan boundaries to theTableExpr */
        if (theIndex != null && theIndex.isGeoIndex()) {

            int geoFieldPos = theIndex.getGeoFieldPos();
            ArrayList<PredInfo> geoPreds = theStartStopPreds.get(geoFieldPos);

            if (geoPreds != null && !geoPreds.isEmpty()) {

                assert(geoPreds.size() == 1);
                PredInfo geopi = geoPreds.get(0);

                if (geopi.theStatus == PredicateStatus.STARTSTOP) {
                    int numKeys = theIndexKeys.size();

                    for (int k = 0; k < numKeys; ++k) {
                        generateGeoKeys(k, geopi, geoFieldPos);
                    }
                }
            }
        }

        ExprVar idxVar = null;

        if (theIndex != null &&
            (!theFilteringPreds.isEmpty() ||
             theIsCovering ||
             theIsUnnestingIndex)) {
            idxVar = theSFW.addIndexVar(theTable, theIndex);
        }

        int numFilteringPreds = theFilteringPreds.size();
        ArrayList<Expr> fpreds = new ArrayList<Expr>(numFilteringPreds);

        if (!thePartitionPreds.isEmpty()) {
            applyContainerPreds(fpreds, idxVar, true);
        }

        if (!theShardPreds.isEmpty()) {
            applyContainerPreds(fpreds, idxVar, false);
        }

        if (primaryAnalyzer != null && primaryAnalyzer.hasShardKey()) {

            assert(primaryAnalyzer.theIndexKeys.size() == 1);
            PrimaryKeyImpl pkey = (PrimaryKeyImpl)
                                  primaryAnalyzer.theIndexKeys.get(0);
            theQCB.setPartitionId(pkey.getPartitionId(theQCB.getStore()));
            theTableExpr.addShardKey(pkey);

            if (primaryAnalyzer.theHaveBindKeys) {

                int shardKeySize = theTable.getShardKeySize();

                ArrayList<Expr> shardBindKey =
                    new ArrayList<Expr>(shardKeySize);

                for (int i = 0; i < shardKeySize; ++i) {
                    shardBindKey.add(
                        primaryAnalyzer.theBindKeys.get(0).get(i));
                }

                theTableExpr.setShardKeyBindExprs(shardBindKey);
            }
        }

        assert(theRanges.size() == theIndexKeys.size());

        theTableExpr.addIndexKeys(theTablePos, theIndex,
                                  theIndexKeys, theRanges,
                                  theIsCovering);

        if (theHaveBindKeys) {
            assert(theBindKeys.size() == theIndexKeys.size());
            theTableExpr.setBindKeys(theBindKeys, theIn3BindInfos);
        }

        /*
         * Add the filtering preds to theTableExpr. If a secondary index is
         * used, rewrite the pred to reference the columns of the current
         * index row, instead of the row columns. No rewrite is necessary for
         * the primary index, because in this case the "index row" is actualy
         * a table row (RowImpl) that is populated with the prim key columns
         * only.
         */
        for (PredInfo pi : theFilteringPreds) {

            Expr pred = pi.thePred;
            if (pi.isInOp()) {
                pred = rewriteINPred(idxVar, pi);
            } else if (theIndex != null) {
                pred = rewritePred(idxVar, pi);
            }

            if (pred != null) {
                fpreds.add(pred);
            }
        }

        if (fpreds.size() > 1) {
            FunctionLib fnlib = CompilerAPI.getFuncLib();
            Function andFunc = fnlib.getFunc(FuncCode.OP_AND);

            Expr pred = ExprFuncCall.create(theQCB, theSctx,
                                            theTableExpr.getLocation(),
                                            andFunc,
                                            fpreds);

            theTableExpr.setTablePred(theTablePos, pred, false);

        } else if (fpreds.size() == 1) {
            theTableExpr.setTablePred(theTablePos, fpreds.get(0), false);
        }

        if (theEliminateDups) {
            theTableExpr.setEliminateIndexDups();
        }

        for (WherePredInfo wpi : theWherePreds) {
            for (PredInfo pi : wpi.thePredInfos) {
                if (pi.isInOp() && pi.theStatus == PredicateStatus.STARTSTOP) {
                    theTableExpr.setHaveINstartstopPreds();
                    break;
                }
            }
        }

        /*
         * If possible, remove STARTSTOP, FILTERING and TRUE preds from the
         * WHERE clause.
         */
        for (WherePredInfo wpi : theWherePreds) {

            if (wpi.isFullyPushable()) {
                removePred(wpi.thePred);
                continue;
            }

            ArrayList<PredInfo> predinfos = wpi.thePredInfos;

            for (PredInfo pi : predinfos) {

                switch (pi.theStatus) {
                case STARTSTOP:
                case FILTERING:
                    /*
                     * If a multikey/existential pred P is pushed to the index,
                     * but there are other multikey preds that are not pushed,
                     * then P cannot be removed; it must be reapplied to the
                     * row selected by the index scan. For example, consider
                     * the following query, index, and document:
                     *
                     * create index idx_children_values on foo (
                     *    info.children.values().age as long,
                     *    info.children.values().school as string)
                     *
                     * select id
                     * from foo f
                     * where exists f.info.children.values($key = "Anna" and
                     *                                     $value.age <= 10)
                     *
                     * {
                     *   "id":5,
                     *   "info":
                     *   {
                     *     "firstName":"first5", "lastName":"last5","age":11,
                     *     "children":
                     *     {
                     *       "Anna"  : { "age" : 29, "school" : "sch_1"},
                     *       "Mark"  : { "age" : 14, "school" : "sch_2"},
                     *       "Dave"  : { "age" : 16 },
                     *       "Tim"   : { "age" : 8,  "school" : "sch_2"},
                     *       "Julie" : { "age" : 12, "school" : "sch_2"}
                     *     }
                     *   }
                     * }
                     *
                     * The range pred on age is pushed to the index. The above
                     * doc will be selected by the index scan (because of
                     * "Tim"). However, the age and $key preds must both be
                     * applied together on each entry of the children map.
                     * If we remove the age pred from the WHERE clause, the
                     * above doc will be wrongly returned.
                     *
                     * It appears that a better rule would be to remove a
                     * pred factor if it is in a pred group where all the
                     * pred factors are also being pushed to the index. However,
                     * our pred factor grouping is not very accurate, so we
                     * may have 2 pred factors that belong to different groups
                     * but should be in the same group instead.
                     */
                    if (pi.canBeRemoved()) {
                        removePred(pi.thePred);
                    }
                    break;
                case TRUE:
                    removePred(pi.thePred);
                    break;
                case NOT_STARTSTOP:
                case SKIP:
                    break;
                default:
                    throw new QueryStateException(
                        "Unexpected state for predicate:\n" + pi);
                }
            }
        }

        /*
         * If a covering secondary index is used, rewrite the SELECT and
         * ORDER BY exprs to reference the columns of the current index row,
         * instead of the row columns. Futhermore, if theTableExpr has nested
         * tables, the WHERE clause may have preds that reference both target
         * and non-target table columns, and as a result, these preds have not
         * been pushed to the index. Therefore, we must rewrite the sub-exprs
         * that reference only target-table columns.
         */
        if (theIndex != null && (theIsCovering || theIsUnnestingIndex)) {

            if (theTableExpr.hasNestedTables()) {

                for (WherePredInfo wpi : theWherePreds) {

                    if (wpi.isFullyPushable()) {
                        continue;
                    }

                    rewriteExpr(idxVar, wpi.thePred);
                }
            }

            rewriteSelectExprs(theSFW, idxVar);
            rewriteSortExprs(theSFW, idxVar);

            if (theOrigSFW != null) {
                rewriteSelectExprs(theOrigSFW, idxVar);
                rewriteSortExprs(theOrigSFW, idxVar);
                if (theOrigSFW.getWhereExpr() != null) {
                    rewriteExpr(idxVar, theOrigSFW.getWhereExpr());
                }

                if (theOrigSFW.getNumFroms() > 1) {
                    for (int i = 1; i < theOrigSFW.getNumFroms(); ++i) {
                        Expr domExpr = theOrigSFW.getFromClause(i).
                                       getDomainExpr();
                        rewriteExpr(idxVar, domExpr);
                    }
                }
            }

            rewriteONPredicates(idxVar);
        }

        /*
         * Remove unused variables. If a var is not used anywhere, it can be
         * removed from the FROM clause if:
         * (a) Its domain expr is scalar, else
         * (b) It was used before applying this index, but is not used after.
         *     This means that all uses of the variable were pushed down to
         *     index. However, if the var ranges over a table, it should not
         *     be removed.
         */
        for (int i = numFroms - 1; i >= 0; --i) {

            FromClause fc = theSFW.getFromClause(i);

            if (fc.getTargetTable() != null) {
                continue;
            }

            ExprVar var = fc.getVar();

            if (var.getNumParents() == 0) {

                if (var.getDomainExpr().isScalar()) {
                    theSFW.removeFromClause(i, true);

                } else if (varRefsCounts[i] != 0) {

                    if (theSFW.getDomainExpr(i).isMultiValued() &&
                        (theIndex == null || !theIndex.isMultiKey())) {
                        throw new QueryStateException(
                            "Attempt to remove a multi-valued variable when " +
                            "a non-multikey index is being applied.\n" +
                            "var name = " + var.getName() + " index name = " +
                            theIndex.getName());
                    }
                    assert(var.getTable() == null);

                    theSFW.removeFromClause(i, true);

                } else if (theIsUnnestingIndex &&
                           var.isUnnestingIndex(theIndex)) {
                    theSFW.removeFromClause(i, true);
                }

            } else if (theIsUnnestingIndex &&
                       var.isUnnestingIndex(theIndex)) {
                throw new QueryStateException(
                    "Reference to unnesting variable: " + var.getName() +
                    " index name: " + theIndex.getName());
            }
        }

        /*
         * The non-target tables are always accessed via the primary index.
         * Check whether the primary index is a covering one for each of these
         * tables.
         */
        analyzeNonTargetTables();
    }

    void applyContainerPreds(
        ArrayList<Expr> fpreds,
        ExprVar idxVar,
        boolean forPartitions) {

        int numContainers;
        ArrayList<PredInfo> preds;

        if (forPartitions) {
            numContainers = theQCB.getStore().getNPartitions();
            preds = thePartitionPreds;
        } else {
            numContainers = theQCB.getStore().getTopology().
                            getRepGroupIds().size();
            preds = theShardPreds;
        }

        for (int i = 0; i < preds.size(); ++i) {

            PredInfo pi = preds.get(i);

            if (pi.theStatus == PredicateStatus.TRUE) {
                removePred(pi.thePred);
                preds.remove(i);
                --i;

            } else if (pi.theStatus == PredicateStatus.FILTERING ||
                       (forPartitions &&
                        !theIsPrimary &&
                        (preds.size() > 1 || !preds.get(0).isEq()))) {

                if (pi.theStatus == PredicateStatus.STARTSTOP) {
                    ExprFuncCall partitionExpr = (ExprFuncCall)pi.theVarArg;
                    ExprVar rowVar = (ExprVar)partitionExpr.getArg(0);
                    addExprToReplace(pi.thePred, rowVar, -1);
                }

                rewriteExpr(idxVar, pi.thePred);
                fpreds.add(pi.thePred);
                removePred(pi.thePred);
                preds.remove(i);
                --i;
            }
        }

        if (preds .isEmpty()) {
            return;
        }

        assert(preds.size() <= 2);
        PredInfo p1 = preds.get(0);
        PredInfo p2 = (preds.size() == 2 ? preds.get(1) : null);

        if (p2 != null) {
            assert(p1.isMin() && p2.isMax() ||
                   p1.isMax() && p2.isMin());

            PredInfo minpi = (p1.isMin() ? p1 : p2);
            PredInfo maxpi = (p1.isMax() ? p1 : p2);
            int cid;

            if (minpi.theConstVal != null && maxpi.theConstVal != null) {

                cid = ((IntegerValueImpl)minpi.theConstVal).get();
                if (minpi.theOp == FuncCode.OP_GT) {
                    ++cid;
                }
                theTableExpr.setMinContainer(cid, forPartitions);

                cid = ((IntegerValueImpl)maxpi.theConstVal).get();
                if (maxpi.theOp == FuncCode.OP_LT) {
                    --cid;
                }
                theTableExpr.setMaxContainer(cid, forPartitions);

            } else {
                ArrayList<Expr> pidBindExprs = new ArrayList<Expr>(2);
                Expr bindExpr = minpi.theConstArg;
                Location loc = bindExpr.getLocation();
                ExprConst one;

                if (minpi.theOp == FuncCode.OP_GT) {
                    one = new ExprConst(theQCB, theSctx, loc, 1);
                    bindExpr = ExprFuncCall.createArithOp(
                               theQCB, theSctx, loc, '+',
                               minpi.theConstArg, one);
                }
                pidBindExprs.add(bindExpr);

                bindExpr = maxpi.theConstArg;
                loc = bindExpr.getLocation();

                if (maxpi.theOp == FuncCode.OP_LT) {
                    one = new ExprConst(theQCB, theSctx, loc, 1);
                    bindExpr = ExprFuncCall.createArithOp(
                               theQCB, theSctx, loc, '-',
                               maxpi.theConstArg, one);
                }
                pidBindExprs.add(bindExpr);

                theTableExpr.setContainerIdBindExprs(pidBindExprs,
                                                     forPartitions);
            }

            removePred(p1.thePred);
            removePred(p2.thePred);

        } else {
            if (p1.theConstVal != null) {
                int cid = ((IntegerValueImpl)p1.theConstVal).get();

                if (p1.isEq()) {
                    theTableExpr.setMinContainer(cid, forPartitions);
                    theTableExpr.setMaxContainer(cid, forPartitions);
                } else if (p1.isMin()) {
                    if (p1.theOp == FuncCode.OP_GT) {
                        ++cid;
                    }
                    theTableExpr.setMinContainer(cid, forPartitions);
                    theTableExpr.setMaxContainer(numContainers, forPartitions);
                } else {
                    if (p1.theOp == FuncCode.OP_LT) {
                        --cid;
                    }
                    theTableExpr.setMinContainer(1, forPartitions);
                    theTableExpr.setMaxContainer(cid, forPartitions);
                }
            } else {
                ArrayList<Expr> pidBindExprs = new ArrayList<Expr>(2);
                Expr bindExpr = p1.theConstArg;
                Location loc = bindExpr.getLocation();
                ExprConst one;

                if (p1.isEq()) {
                    pidBindExprs.add(bindExpr);

                } else if (p1.isMin()) {
                    if (p1.theOp == FuncCode.OP_GT) {
                        one = new ExprConst(theQCB, theSctx, loc, 1);
                        bindExpr = ExprFuncCall.createArithOp(
                                   theQCB, theSctx, loc, '+',
                                   p1.theConstArg, one);
                    }
                    pidBindExprs.add(bindExpr);
                    pidBindExprs.add(null);

                } else {
                    if (p1.theOp == FuncCode.OP_LT) {
                        one = new ExprConst(theQCB, theSctx, loc, 1);
                        bindExpr = ExprFuncCall.createArithOp(
                                   theQCB, theSctx, loc, '-',
                                   p1.theConstArg, one);
                    }
                    pidBindExprs.add(null);
                    pidBindExprs.add(bindExpr);
                }

                theTableExpr.setContainerIdBindExprs(pidBindExprs,
                                                     forPartitions);
            }

            removePred(p1.thePred);
        }
    }

    private void generateGeoKeys(int keyIdx, PredInfo geopi, int geoFieldPos) {

        RecordValueImpl ikey = theIndexKeys.get(keyIdx);
        ArrayList<Expr> bindKey = theBindKeys.get(keyIdx);

        if (geopi.theGeom != null && (!geopi.isNear() || geopi.theDistance > 0)) {

            List<Pair<String,String>> georanges =
                CompilerAPI.getGeoUtils().ranges(geopi.theGeom,
                                                 geopi.theDistance,
                                                 theQCB.getOptions());

            List<String> geokeys = null;

            if (theIndex.isGeometryIndex()) {
                geokeys = CompilerAPI.getGeoUtils().keys(georanges);
            }

            String pathName = theIndex.getFieldName(geoFieldPos);
            FieldDefImpl rangeDef = FieldDefImpl.Constants.stringDef;

            for (int i = 0; i < georanges.size(); ++i) {

                Pair<String,String> range = georanges.get(i);
                FieldRange frange = new FieldRange(pathName, rangeDef, 0);

                String start = range.first();
                String end = range.second();
                frange.setStart(start, true);
                frange.setEnd(end, true);

                if (i > 0) {
                    theIndexKeys.add(ikey);
                    theRanges.add(frange);
                    theBindKeys.add(bindKey);
                } else {
                    theRanges.set(keyIdx, frange);
                }
            }

            if (geokeys != null && !geokeys.isEmpty()) {

                for (String key : geokeys) {
                    ikey = ikey.clone();
                    ikey.put(geoFieldPos, key);
                    theIndexKeys.add(ikey);
                    theRanges.add(null);
                    theBindKeys.add(bindKey);
                }
            }

        } else if (geopi.isNear()) {

            bindKey.set(ikey.size(), geopi.theConstArg);
            bindKey.set(ikey.size() + 1, geopi.theDistanceArg);
            theHaveBindKeys = true;

        } else {
            bindKey.set(ikey.size(), geopi.theConstArg);
            theHaveBindKeys = true;
        }
    }

    private Expr rewriteINPred(ExprVar idxVar, PredInfo inPi) {

        /* Collect all the startstop/filtering pis that belong to the same
         * IN expr, and create a new IN expr out of these pis */

        WherePredInfo wpi = inPi.theEnclosingPred;

        ArrayList<PredInfo> predInfos =
            new ArrayList<PredInfo>(wpi.thePredInfos.size());

        for (PredInfo pi : wpi.thePredInfos) {
            if (pi.thePred == inPi.thePred &&
                (pi.theStatus == PredicateStatus.STARTSTOP ||
                 pi.theStatus == PredicateStatus.FILTERING)) {
                predInfos.add(pi);
            }
        }

        boolean isIn3 = inPi.isIn3Op();

        if (isIn3 && predInfos.size() !=
            ((ExprInOp)inPi.thePred).getNumKeyComps()) {

            for (PredInfo pi : predInfos) {
                if (pi.theStatus == PredicateStatus.FILTERING) {
                    pi.theStatus = PredicateStatus.SKIP;
                }
            }

            return null;
        }

        ExprInOp inExpr = new ExprInOp(theQCB, theSctx,
                                       inPi.thePred.getLocation(),
                                       isIn3);
        PredInfo mapKeyPi = null;
        int numKeys = 0;

        for (PredInfo pi : predInfos) {

            if (pi.theIsMapBothKey) {
                if (pi.mapBothKey() == null || pi.theIPathPos < 0) {
                    throw new QueryStateException(
                        "Unexpected kind of predicate info while rewriting " +
                        "IN predicate:\n" + pi);
                }

                mapKeyPi = pi;
                continue;
            }

            assert(pi.isInOp());

            if (!isIn3) {
                numKeys = pi.theConstINArgs.size();
            }

            if (pi.theIPathPos < 0) {
                if (idxVar != null) {
                    rewriteExpr(idxVar, pi.theVarArg);
                }
                inExpr.addArg(pi.theVarArg);
                continue;
            }

            assert(pi.theEpath != null);

            if (idxVar != null) {
                Location loc = pi.theEpath.theExpr.theLocation;
                int fieldPos;

                if (theIndex == null) {
                    fieldPos = theTable.getPrimKeyPos(pi.theIPathPos);
                } else {
                    fieldPos = pi.theIPathPos;
                }

                ExprFieldStep idxColRef =
                    new ExprFieldStep(theQCB, theSctx, loc,
                                      idxVar, fieldPos);

                inExpr.addArg(idxColRef);
            } else {
                inExpr.addArg(pi.theVarArg);
            }
        }

        int numKeyComps = inExpr.getNumArgs();

        if (!isIn3) {
            for (int k = 0; k < numKeys; ++k) {

                for (PredInfo fpi : predInfos) {
                    if (fpi.isEq()) {
                        continue;
                    }
                    inExpr.addArg(fpi.theConstINArgs.get(k));
                }
            }
        } else {
            inExpr.addArg(((ExprInOp)inPi.thePred).getIn3RHSExpr());
        }

        inExpr.setNumKeyComps(numKeyComps);

        if (mapKeyPi == null) {
            return inExpr;
        }

        Location loc = mapKeyPi.theEpath.theExpr.theLocation;
        int fieldPos = mapKeyPi.theIPathPos;

        ExprFieldStep idxColRef = new ExprFieldStep(theQCB, theSctx, loc,
                                                    idxVar, fieldPos);

        Expr mapKeyExpr = new ExprConst(theQCB, theSctx, loc,
                                       mapKeyPi.theConstVal);

        Expr mapKeyEqExpr = ExprFuncCall.create(theQCB, theSctx, loc,
                                                FuncCode.OP_EQ,
                                                idxColRef, mapKeyExpr);

        Expr andExpr = ExprFuncCall.create(theQCB, theSctx,
                                           inExpr.getLocation(),
                                           FuncCode.OP_AND,
                                           mapKeyEqExpr, inExpr);

        return andExpr;
    }

    private Expr rewritePred(ExprVar idxVar, PredInfo pi) {

        assert(!pi.isInOp());

        if (!pi.isMatched()) {
            rewriteExpr(idxVar, pi.thePred);
            return pi.thePred;
        }

        assert(pi.theEpath != null);
        Location loc = pi.theEpath.theExpr.theLocation;
        int fieldPos;

        if (theIndex == null) {
            fieldPos = theTable.getPrimKeyPos(pi.theIPathPos);
        } else {
            fieldPos = pi.theIPathPos;
        }

        ExprFieldStep idxColRef = new ExprFieldStep(theQCB, theSctx, loc,
                                                    idxVar, fieldPos);

        if (pi.theIsMapBothKey) {
            assert(pi.theEpath.getMapBothKey(theTable, theIndex) != null);

            ExprConst keyvalExpr =
                new ExprConst(theQCB, theSctx, loc, pi.theConstVal);

            ArrayList<Expr> args = new ArrayList<Expr>(2);
            args.add(idxColRef);
            args.add(keyvalExpr);

            Expr pred = new ExprFuncCall(theQCB, theSctx, loc,
                                         FuncCode.OP_EQ, args);
            return pred;
        }

        ExprFuncCall pred = (ExprFuncCall)pi.thePred;
        int numArgs = pred.getNumArgs();
        assert(numArgs <= 2);
        ArrayList<Expr> args = new ArrayList<Expr>(numArgs);

        for (int i = 0; i < numArgs; ++i) {
            Expr arg = pred.getArg(i);
            if (arg == pi.theVarArg) {
                args.add(idxColRef);
            } else {
                args.add(arg);
            }
        }

        pred = new ExprFuncCall(theQCB, theSctx, pred.theLocation,
                                pred.getFunction(null), args);
        return pred;
    }

    private void rewriteSelectExprs(ExprSFW sfw, ExprVar idxVar) {

        int numFields = sfw.getNumFields();

        for (int i = 0; i < numFields; ++i) {

            Expr expr = sfw.getFieldExpr(i);

            /* If the select expr is a row var, we must convert it to a
             * number of step exprs that extract the column values from
             * the index entry. If the row var is the only expr in the
             * select, the step exprs become the new exprs of the select
             * list. If not (for example select * from NESTED TABLES(...))
             * we replace the row var with a record constructor whose field
             * exprs are the step exprs. */
            if (expr.getKind() == ExprKind.VAR &&
                ((ExprVar)expr).getTable() != null &&
                ((ExprVar)expr).getTable().getId() == theTable.getId()) {

                RecordDefImpl rowDef = theTable.getRowDef();
                int numCols = rowDef.getNumFields();
                ArrayList<Expr> newFieldExprs = new ArrayList<Expr>(numCols);

                for (int j = 0; j < numCols; ++j) {
                    String colName = rowDef.getFieldName(j);

                    int k = 0;
                    for (; k < theNumFields; ++k) {
                        IndexField ipath = theIndexPaths.get(k);
                        if (ipath.numSteps() == 1 &&
                            ipath.getStep(0).equalsIgnoreCase(colName)) {
                            break;
                        }
                    }

                    if (k == theNumFields) {
                        throw new QueryStateException(
                            "Column " + colName + " is not indexed by " +
                            "index " + theIndex.getName());
                    }

                    newFieldExprs.add(new ExprFieldStep(expr.getQCB(),
                                                        expr.getSctx(),
                                                        expr.getLocation(),
                                                        idxVar,
                                                        k));
                }

                if (numFields == 1) {
                    for (int j = 0; j < numCols; ++j) {
                        sfw.addField(rowDef.getFieldName(j),
                                     newFieldExprs.get(j));
                    }
                    sfw.removeField(i, true);
                } else {
                    Expr newFieldExpr = new ExprRecConstr(expr.getQCB(),
                                                          expr.getSctx(),
                                                          expr.getLocation(),
                                                          rowDef,
                                                          newFieldExprs);
                    sfw.setFieldExpr(i, newFieldExpr, true);
                }

            } else {
                rewriteExpr(idxVar, expr);
            }
        }
    }

    private void rewriteSortExprs(ExprSFW sfw, ExprVar idxVar) {
    
        int numSortExprs = sfw.getNumSortExprs();

        for (int i = 0; i < numSortExprs; ++i) {
            Expr expr = sfw.getSortExpr(i);
            if (theTrace >= 4) {
                trace("Rewrting sort expr:\n" + expr.display());
            }
            rewriteExpr(idxVar, expr);
        }
    }

    private void rewriteONPredicates(ExprVar idxVar) {

        int numTables = theTableExpr.getNumTables();

        for (int i = 0; i < numTables; ++i) {

            if (i == theTargetTablePos) {
                continue;
            }

            Expr pred = theTableExpr.getTablePred(i);
            if (pred != null) {
                rewriteExpr(idxVar, pred);
            }
        }
    }

    private void rewriteExpr(ExprVar idxVar, Expr expr) {

        if (theTrace >= 4) {
            trace("Rewriting expr:\n" + expr.display());
        }

        ArrayList<ExprToReplace> exprsToReplace =
            theExprRewriteMap.get(expr);

        if (exprsToReplace == null) {
            /* The expr does not reference any columns of theTable */
            return;
        }

        for (ExprToReplace etr : exprsToReplace) {

            Expr parent;

            if (theTrace >= 4) {
                trace("Replacing expr:\n" + etr.theExpr.display());
            }

            int numParents = etr.theExpr.getNumParents();
            
            if (numParents == 0) {
                /* This can happen if the expr was added twice to the rewrite
                 * map and has already been rewritten in an earlier iteration
                 * of this for loop */
                if (theTrace >= 3) {
                    trace("expr has no parents");
                }
                continue;
            }
            
            if (numParents > 1) {
                parent = expr.findSubExpr(etr.theExpr);
                if (parent == null) {
                    throw new QueryStateException(
                        "Cannot rewrite expression");
                }
            } else {
                parent = etr.theExpr.getParent(0);
            }

            if (etr.theIndexFieldPos < 0) {
                parent.replaceChild(etr.theExpr, idxVar, true/*destroy*/);
                continue;
            }

            int fieldPos;

            if (theIndex == null) {
                fieldPos = theTable.getPrimKeyPos(etr.theIndexFieldPos);
            } else {
                fieldPos = etr.theIndexFieldPos;
            }

            ExprFieldStep idxColRef = new ExprFieldStep(theQCB,
                                                        theSctx,
                                                        etr.theExpr.theLocation,
                                                        idxVar,
                                                        fieldPos);

            parent.replaceChild(etr.theExpr, idxColRef, true/*destroy*/);
        }
    }

    void analyzeNonTargetTables() {

        int numTables = theTableExpr.getNumTables();

        for (int i = 0; i < numTables; ++i) {

            if (i == theTargetTablePos) {
                continue;
            }

            IndexAnalyzer analyzer =
                new IndexAnalyzer(theSFW, theTableExpr, i, null/*index*/);

            analyzer.analyze();

            if (analyzer.theSFW == null) {
                return;
            }

            theTableExpr.addIndexKeys(i, null, null, null,
                                      analyzer.theIsCovering);
        }

        /*
         * TODO: Optimize the case where an ON pred is an index-only expr, but
         * the primary index is not covering. We should apply the pred using
         * the index row and retrieve the full table row only for the row that
         * survive the filter.
         */
    }

    /**
     * Do the work!
     *
     * Note: This method will set theSFW to null if it discovers that the whole
     * WHERE expr is always false. If so, it will also replace the whole SFW
     * expr with an empty expr. Callers of this method should check whether
     * theSFW has been set to null.
     */
    void analyze() {

        if (theTrace >= 1) {
            System.out.println("\n------------------------------------------\n");
            if (theIndex != null) {
                trace("Starting index analysis. Index entry def:\n" +
                      theIndex.getIndexEntryDef().getDDLString());
            } else {
                trace("Starting index analysis. Row def:\n" +
                      theTable.getRowDef().getDDLString());
            }
            System.out.println("\n------------------------------------------\n");
        }

        /* If there is an index_storage_size() function that (a) appears in the
         * SELECT clause as a top-level expression or as the arg to an aggregate
         * function and (b) applies to a multi-key index, keep track of it. This
         * info will be used later in costing the index, and potentially
         * re-writing the query to optimize the index_storage_size call. */
        if (theIndex != null && theIndex.isMultiKey()) {

            for (int i = 0; i < theSFW.getNumFields(); ++i) {

                Expr field = theSFW.getFieldExpr(i);

                if (field.getFunction(null) == null) {
                    continue;
                }

                ExprFuncCall fncall = (ExprFuncCall)field;
                Function func = fncall.getFunction();

                if (func.isAggregate() && fncall.getNumArgs() > 0) {
                    field = fncall.getArg(0);
                    if (field.getFunction(null) == null) {
                        continue;
                    }
                    fncall = (ExprFuncCall)field;
                    func = fncall.getFunction();
                }

                if (func.getCode() == FuncCode.FN_INDEX_STORAGE_SIZE) {
                    ExprVar var = (ExprVar)fncall.getArg(0);
                    ExprBaseTable tableExpr = (ExprBaseTable)var.getDomainExpr();
                    TableImpl table = var.getTable();
                    String idxName = ((ExprConst)fncall.getArg(1)).getString();
                    IndexImpl index = (IndexImpl)table.getIndex(idxName);

                    if (table.getId() == tableExpr.getTargetTable().getId() &&
                        idxName.equals(index.getName())) {
                        theSFW.addMKIndexStorageSize(fncall);
                    }
                }
            }
        }

        if (theSFW.getNumFroms() > 1 &&
            theIndex != null &&
            theIndex.isMultiKey()) {

            theIsUnnestingIndex = checkIsUnnestingIndex();

            if (theTrace >= 1) {
                trace("unnesting index = " + theIsUnnestingIndex);
            }
        }

        /*
         * Analyze WHERE preds and collect start/stop and filtering preds for
         * the index. We do this even for non-target tables in a NESTED TABLES
         * clause, even though WHERE preds on non-target preds cannot be pushed
         * down to the index (it's not semantically correct). Nevertheless we
         * do it because we need the collected info to decide if the index used
         * to scan a non-target table is covering or not.
         */
        Expr predsExpr = theSFW.getWhereExpr();

        if (predsExpr != null) {

            List<Expr> preds;

            Function andOp = predsExpr.getFunction(FuncCode.OP_AND);

            if (andOp != null) {
                preds = ((ExprFuncCall)predsExpr).getArgs();
            } else {
                preds = new ArrayList<Expr>(1);
                preds.add(predsExpr);
            }

            for (Expr pred : preds) {

                WherePredInfo wpi = new WherePredInfo(pred);

                if (theTrace >= 1) {
                    trace("\nCollecting preds for WPI-" + wpi.theId + "\n");
                }

                collectPredInfo(wpi, pred);

                if (wpi.thePred != null) {
                    theWherePreds.add(wpi);
                }

                if (theSFW == null) {
                    return;
                }
            }

            /*
             * Look for conflicting preds within each "class" of preds.
             */
            if (theTrace >= 2) {
                trace("\nLooking for conflicting start/stop preds\n");
            }
            boolean alwaysFalse;

            for (int ipos = 0; ipos < theNumFields; ++ipos) {

                alwaysFalse = skipExtraneousPreds(ipos);

                if (alwaysFalse) {
                    processAlwaysFalse();
                    return;
                }
            }

            alwaysFalse = skipExtraneousPartitionPreds();

            if (alwaysFalse) {
                processAlwaysFalse();
                return;
            }

            /*
             * Only one of the multikey WHERE preds can be used for the index
             * scan. Choose the "best" one. Also convert STARTSTOP preds to
             * FILTERING ones, if needed.
             */
            if (theTrace >= 2) {
                trace("\nChoosing best multikey group\n");
            }
            chooseMultiKeyPredGroup();
        }

        if (theIndex != null && !theIndex.indexesNulls()) {

            for (int i = 0; i < theIndex.numFields(); ++i) {
                if (theStartStopPreds.get(i) == null ||
                    theStartStopPreds.get(i).isEmpty()) {
                    theIsRejected = true;
                    return;
                }
            }
        }

        if (theTablePos == theTargetTablePos) {
            /*
             * Go through the preds again to collect any filtering preds that
             * are not start/stop preds. We do this after choosing the best
             * MapBoth key, because only path exprs using the chosen map key
             * can be considered as index-only exprs.
             */
            for (WherePredInfo wpi : theWherePreds) {

                for (PredInfo pi : wpi.thePredInfos) {
                    // ????
                    boolean simplePathsOnly = (pi.thePredGroup != theBestPredGroup);
                    Expr e = (pi.isInOp() ? pi.theVarArg : pi.thePred);

                    if (theTrace > 0 &&
                        pi.theStatus == PredicateStatus.NOT_STARTSTOP) {
                        trace("Checking pi for filtering pred:\n" + pi);
                    }

                    if (pi.theStatus == PredicateStatus.NOT_STARTSTOP &&
                        isIndexOnlyExpr(e, true, simplePathsOnly)) {
                        pi.theStatus = PredicateStatus.FILTERING;

                        if (theTrace > 0) {
                            trace("NOT_STARTSTOP pred converted to filtering " +
                                  "pi:\n" + pi);
                        }
                    }
                }
            }

            pushStartStopPreds();

            for (WherePredInfo wpi : theWherePreds) {

                for (PredInfo pi : wpi.thePredInfos) {

                    if (pi.theStatus == PredicateStatus.FILTERING) {

                        if (pi.isInOp()) {
                            boolean add = true;
                            for (PredInfo fpi : theFilteringPreds) {
                                if (fpi.thePred == pi.thePred) {
                                    add = false;
                                    break;
                                }
                            }
                            if (add) {
                                theFilteringPreds.add(pi);
                            }
                        } else {
                            theFilteringPreds.add(pi);
                        }
                    }
                }
            }
        }

        if (theSFW.getNumFroms() > 1 && theEliminateDups) {
            theIsRejected = true;
            return;
        }

        checkIsCovering();

        if (theTable.isKeyOnly() &&
            !theTable.isJsonCollection() &&
            theIndex != null &&
            !theIsCovering) {
            theIsRejected = true;
            return;
        }

        checkIsSortingIndex();

        if (theSFW.getNumFroms() > 1 && theIsUnnestingIndex) {

            boolean fullyUnnesting = true;

            for (int i = theSFW.getNumFroms() - 1; i > 0; --i) {

                FromClause fc = theSFW.getFromClause(i);
                ExprVar var = fc.getVar();

                if (!var.isUnnestingIndex(theIndex)) {
                    continue;
                }

                if (!IndexExpr.matchUnnestingVarToIndexPath(
                       theIndex.getMultiKeyPaths(),
                       var.getUnnestPath())) {
                    fullyUnnesting = false;
                }

                break;
            }

            if (fullyUnnesting) {
                return;
            }

            if (theTrace >= 2) {
                trace("isSortingIndex = " + theSFW.isSortingIndex(theIndex));
            }

            if (!theSFW.hasGroupBy() ||
                !theSFW.isSortingIndex(theIndex) ||
                !theIsCovering) {
                theIsRejected = true;
                return;
            }

            boolean haveCountStar = false;
            int numFieldExprs = theSFW.getNumFields();

            for (int i = 0; i < numFieldExprs; ++i) {

                Expr expr = theSFW.getFieldExpr(i);
                if (expr.getFunction(FuncCode.FN_COUNT_STAR) != null) {
                    haveCountStar = true;
                    break;
                }

                if (expr.getFunction(FuncCode.FN_COUNT) != null) {
                    if (ConstKind.isConst(expr)) {
                        haveCountStar = true;
                        break;
                    }
                }
            }

            if (haveCountStar) {
                theIsRejected = true;
                return;
            }
        }
    }

    private void collectPredInfo(WherePredInfo wpi, Expr pred) {

        PredInfo pi = new PredInfo(wpi, pred);

        if (pred.getKind() == ExprKind.CONST) {
            ExprConst constExpr = (ExprConst)pred;

            if (constExpr.getValue() == BooleanValueImpl.falseValue) {
                pi.theStatus = PredicateStatus.FALSE;
                processAlwaysFalse();
                return;
            }

            if (constExpr.getValue() == BooleanValueImpl.trueValue) {
                pi.theStatus = PredicateStatus.TRUE;
                return;
            }
        }

        if (theTrace >= 2) {
            trace("Collecting pred for WPI-" + wpi.theId + ":\n" +
                  pred.display());
        }

        if (pred.getKind() == ExprKind.IN) {
            collectPredInfosForInOp(wpi, pred);
            return;
        }

        Function func = pred.getFunction(null);

        if (func == null) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            wpi.add(pi);
            return;
        }

        FuncCode op = func.getCode();
        ExprFuncCall compExpr = (ExprFuncCall)pred;
        Expr varArg;
        Expr constArg;
        FieldValueImpl constVal = null;
        boolean isComp = func.isComparison();
        boolean isExists = (op == FuncCode.OP_EXISTS);
        boolean isNotExists = (op == FuncCode.OP_NOT_EXISTS);
        boolean isNullOp = (op == FuncCode.OP_IS_NULL ||
                            op == FuncCode.OP_IS_NOT_NULL);
        boolean isGeo = (op == FuncCode.FN_GEO_INTERSECT ||
                         op == FuncCode.FN_GEO_INSIDE ||
                         op == FuncCode.FN_GEO_WITHIN_DISTANCE);

        if (op == FuncCode.OP_IS_NULL) {
            if (theIndex != null && !theIndex.indexesNulls()) {
                pi.theStatus = PredicateStatus.SKIP;
                wpi.add(pi);
                return;
            }
            op = FuncCode.OP_EQ;
        } else if (op == FuncCode.OP_IS_NOT_NULL) {
            op = FuncCode.OP_LT;
        } else if (isExists) {
            op = FuncCode.OP_NEQ;
        } else if (isNotExists) {
            if (theIndex != null && !theIndex.indexesNulls()) {
                pi.theStatus = PredicateStatus.SKIP;
                wpi.add(pi);
                return;
            }
            op = FuncCode.OP_EQ;
        } else if (isComp) {
            if (func.isAnyComparison()) {
                op = FuncAnyOp.anyToComp(op);
            }
        } else if (!isGeo) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            wpi.add(pi);
            return;
        } else if (theIsPrimary || !theIndex.isGeoIndex()) {
            pi.theStatus = PredicateStatus.SKIP;
            wpi.add(pi);
            return;
        }

        if (isNullOp) {
            varArg = compExpr.getArg(0);
            constArg = null;
            constVal = NullValueImpl.getInstance();

        } else if (isExists || isNotExists) {
            varArg = compExpr.getArg(0);
            constArg = null;
            constVal = EmptyValueImpl.getInstance();

        } else {
            Expr arg0 = compExpr.getArg(0);
            Expr arg1 = compExpr.getArg(1);

            if (ConstKind.isConst(arg0)) {
                constArg = arg0;
                varArg = arg1;
                if (isComp) {
                    op = FuncCompOp.swapCompOp(op);
                }
            } else if (ConstKind.isConst(arg1)) {
                constArg = arg1;
                varArg = arg0;
            } else {
                pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                wpi.add(pi);
                // TODO: try to find path filtering preds in both args.
                return;
            }

            if (constArg.getKind() == ExprKind.CONST) {
                constVal = ((ExprConst)constArg).getValue();

            } else if (ConstKind.isCompileConst(constArg)) {

                List<FieldValueImpl> res = ExprUtils.computeConstExpr(constArg);

                if (res.size() != 1) {
                    pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                    wpi.add(pi);
                    return;
                }

                constVal = res.get(0);
                ExprConst ge = new ExprConst(theQCB, theSctx,
                                             constArg.getLocation(),
                                             constVal);
                constArg.replace(ge, true);
                constArg = ge;
            }
        }

        if (varArg.getFunction(FuncCode.FN_PARTITION) != null) {
            wpi.thePred = null;
            collectPartitionShardPred(pred, false, op, varArg,
                                      constArg, constVal, pi);
            return;
        } else if (varArg.getFunction(FuncCode.FN_SHARD) != null) {
            wpi.thePred = null;
            collectPartitionShardPred(pred, true, op, varArg,
                                      constArg, constVal, pi);
            return;
        }

        IndexExpr epath = varArg.getIndexExpr();

        if (epath == null || epath.theTable.getId() != theTable.getId()) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            wpi.add(pi);
            return;
        }

        if (isNotExists && epath.getFilteringPreds() != null) {
            pi.theStatus = PredicateStatus.SKIP;
            wpi.add(pi);
            return;
        }

        pi.theOp = op;
        pi.theIsValueComp = func.isValueComparison();
        pi.theIsExists = isExists;
        pi.theIsNotExists = isNotExists;
        pi.theVarArg = varArg;
        pi.theConstArg = constArg;
        pi.theConstVal = constVal;
        pi.theEpath = epath;
        pi.theIsGeo = isGeo;

        if (isGeo) {

            if (constVal != null) {
                pi.theGeom = CompilerAPI.getGeoUtils().castAsGeometry(constVal);
            }

            if (pi.isNear()) {

                pi.theDistanceArg = compExpr.getArg(2);

                if (pi.theDistanceArg.getKind() == ExprKind.CONST) {
                    FieldValueImpl v = ((ExprConst)pi.theDistanceArg).getValue();
                    double dist = v.asDouble().get();

                    if (dist <= 0) {
                        processAlwaysFalse();
                        return;
                    }

                    pi.theDistance = dist;
                }
            }
        }

        if (func.isAnyComparison() &&
            (theIsPrimary || !theIndex.isMultiKey())) {
            pi.theStatus = PredicateStatus.SKIP;
        }

        if (isGeo &&
            (theIsPrimary ||
             (theIndex.isMultiKey() && theIndex.isGeometryIndex()))) {
            pi.theStatus = PredicateStatus.SKIP;
        }

        if (op == FuncCode.OP_NEQ && !isExists) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
        }

        boolean added = wpi.add(pi);

        if (added) {
            matchPred(pi);
        }

        wpi.theDoesSlicing = (wpi.theDoesSlicing || epath.doesSlicing());

        if (pi.theStatus == PredicateStatus.FALSE) {
            processAlwaysFalse();
            return;
        }

        if (pi.theStatus == PredicateStatus.STARTSTOP) {
            collectStartStopPred(pi);
        }

        List<Expr> filteringPreds = epath.getFilteringPreds();

        if (filteringPreds != null) {

            if (theTrace >= 2) {
                trace("\nCollecting filtering preds for epath: " +
                      epath.getPathName() + "\n");
            }

            for (Expr fpred : filteringPreds) {
                collectPredInfo(wpi, fpred);
            }
        }
    }

    void collectPartitionShardPred(
        Expr pred,
        boolean isShard,
        FuncCode op,
        Expr varArg,
        Expr constArg,
        FieldValueImpl constVal,
        PredInfo pi) {

        pi.thePred = pred;
        pi.theOp = op;
        pi.theVarArg = varArg;
        pi.theConstArg = constArg;
        pi.theConstVal = constVal;

        if (isShard) {
            theShardPreds.add(pi);
        } else {
            thePartitionPreds.add(pi);
        }

        if (constVal != null) {

            if (!constArg.getType().isNumeric()) {
                pi.theStatus = PredicateStatus.FALSE;
                processAlwaysFalse();
                return;
            }

            FieldValueImpl newConstVal = FuncCompOp.castConstInCompOp(
                FieldDefImpl.Constants.integerDef,
                false, /*allowJsonNull*/
                false, /* ignore nullability of varArg */
                true, /* isScalar */
                constVal,
                pi.theOp,
                theQCB.strictMode());

            if (newConstVal != null && newConstVal != constVal) {

                if (newConstVal == BooleanValueImpl.falseValue) {
                    pi.theStatus = PredicateStatus.FALSE;
                    processAlwaysFalse();
                    return;
                }

                if (newConstVal == BooleanValueImpl.trueValue) {
                    pi.theStatus = PredicateStatus.TRUE;
                    return;
                }

                constVal = newConstVal;
                pi.theConstVal = constVal;
                constArg = new ExprConst(theQCB, theSctx,
                                         constArg.getLocation(),
                                         constVal);
                pi.theConstArg.replace(constArg, true);
                pi.theConstArg = constArg;
            }

            IntegerValueImpl numContainers;

            if (isShard) {
                Topology topo = theQCB.getStore().getTopology();
                numContainers = FieldDefImpl.Constants.integerDef.
                createInteger(topo.getRepGroupIds().size());
            } else {
                numContainers = FieldDefImpl.Constants.integerDef.
                createInteger(theQCB.getStore().getNPartitions());
            }

            int cmp = FieldValueImpl.compareKeyValues(numContainers, constVal);

            if (cmp < 0) {
                if (pi.isEq() || pi.isMin()) {
                    pi.theStatus = PredicateStatus.FALSE;
                    processAlwaysFalse();
                    return;
                }

                pi.theStatus = PredicateStatus.TRUE;
                return;

            } else if (cmp == 0) {
                if (op == FuncCode.OP_GT) {
                    pi.theStatus = PredicateStatus.FALSE;
                    processAlwaysFalse();
                    return;
                }

                if (op == FuncCode.OP_LE) {
                    pi.theStatus = PredicateStatus.TRUE;
                    return;
                }
            }

            cmp = FieldValueImpl.compareKeyValues(IntegerValueImpl.zero, constVal);

            if (cmp >= 0) {
                if (pi.isEq() || pi.isMax()) {
                    pi.theStatus = PredicateStatus.FALSE;
                    processAlwaysFalse();
                    return;
                }

                pi.theStatus = PredicateStatus.TRUE;
                return;
            }

            if (constVal.getType() == Type.INTEGER) {
                pi.theStatus = PredicateStatus.STARTSTOP;

            } else if (pi.theStatus != PredicateStatus.TRUE) {
                pi.theStatus = PredicateStatus.FILTERING;
            }

        } else if (!checkTypes(pi.theVarArg, pi.theConstArg, false)) {
            pi.theStatus = PredicateStatus.FILTERING;

        } else {
            pi.theStatus = PredicateStatus.STARTSTOP;
        }

        return;
    }

    private void collectPredInfosForInOp(WherePredInfo wpi, Expr pred) {

        ExprInOp inExpr = (ExprInOp)pred;
        int numKeyComps = inExpr.getNumKeyComps();

        if (inExpr.hasVarKeys()) {
            PredInfo pi = new PredInfo(wpi, pred);
            pi.theStatus = PredicateStatus.SKIP;
            if (theTrace >= 2) {
                trace("IN predicate has non-const search keys");
            }
            wpi.add(pi);
            return;
        }

        for (int i = 0; i < numKeyComps; ++i) {

            Expr varArg = inExpr.getArg(i);
            IndexExpr epath =  varArg.getIndexExpr();

            PredInfo pi = new PredInfo(wpi, pred);
            pi.theInComp = i;
            pi.theIsValueComp = true;
            pi.theVarArg = varArg;
            pi.theEpath = epath;

            if (!inExpr.isIN3()) {
                pi.theConstINArgs = new ArrayList<Expr>(16);

                for (int k = numKeyComps + i;
                     k < inExpr.getNumArgs();
                     k += numKeyComps) {

                    Expr constArg = inExpr.getArg(k);
                    pi.theConstINArgs.add(constArg);
                }
            }

            wpi.add(pi);

            if (epath == null || epath.theTable.getId() != theTable.getId()) {
                pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                continue;
            }

            if (epath.getFilteringPreds() != null) {
                // TODO ????
                pi.theStatus = PredicateStatus.SKIP;
                continue;
            }

            boolean matched = matchPathExprToIndexPath(theIndex, epath, false);

            wpi.theDoesSlicing = (wpi.theDoesSlicing || epath.doesSlicing());

            if (!matched) {
                pi.theStatus = (epath.theFunction != null ?
                                PredicateStatus.NOT_STARTSTOP :
                                PredicateStatus.SKIP);
                if (theTrace >= 2) {
                    trace("Match failure for epath " + epath.getPathName());
                }
                continue;
            }

            if (epath.theIsMultiValue && pi.theVarArg.isMultiValued()) {
                pi.theStatus = PredicateStatus.SKIP;
                continue;
            }

            pi.theIPathPos = epath.getPathPos();

            if (!theIsPrimary &&
                epath.thePrimKeyPos >= 0 &&
                epath.getPathPos() >= theIndex.numFields() ) {
                pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                continue;
            }

            if (inExpr.isIN3()) {

                pi.theConstArg = inExpr.getIn3RHSExpr();

                if (pi.theStatus == PredicateStatus.UNKNOWN) {
                    pi.theStatus = PredicateStatus.STARTSTOP;
                    collectStartStopPred(pi);
                }
                continue;
            }

            FieldDefImpl varType =
                ((epath.getJsonDeclaredType() != null && !epath.theIsGeo) ?
                 epath.getJsonDeclaredType() :
                 varArg.getType().getDef());

            for (int k = numKeyComps + i, l = 0;
                 k < inExpr.getNumArgs();
                 k += numKeyComps, ++l) {

                Expr constArg = inExpr.getArg(k);

                if (constArg.getKind() == ExprKind.CONST) {
                    FieldValueImpl val = ((ExprConst)constArg).getValue();
                    FieldDefImpl valType = val.getDefinition();

                    if (val.isNull()) {
                        pi.theStatus = PredicateStatus.FILTERING;
                        continue;
                    }

                    if (!TypeManager.areTypesComparable(varType, valType)) {
                        if (theQCB.strictMode()) {
                            throw new QueryException(
                                "Incompatible types for IN operator: \n" +
                                "Type1: " + pi.theVarArg.getType() +
                                "\nType2: " + pi.theConstArg.getType(),
                                pi.thePred.getLocation());
                        }

                        pi.theStatus = PredicateStatus.FILTERING;
                        continue;
                    }

                    FieldValueImpl newVal = FuncCompOp.castConstInCompOp(
                        varType,
                        epath.theIsJson, /*allowJsonNull*/
                        false, /* ignore nullability of varArg */
                        varArg.isScalar(),
                        val,
                        FuncCode.OP_EQ,
                        theQCB.strictMode());

                    if (newVal != val) {

                        if (newVal == BooleanValueImpl.falseValue) {
                            pi.theStatus = PredicateStatus.FILTERING;
                            continue;
                        }

                        constArg = new ExprConst(theQCB, theSctx,
                                                 constArg.getLocation(),
                                                 newVal);
                        inExpr.setArg(k, constArg, true);
                        pi.theConstINArgs.set(l, constArg);
                    }
                }

                if (!checkTypes(varArg, constArg, true)) {
                    pi.theStatus = PredicateStatus.FILTERING;
                }
            }

            if (pi.theStatus == PredicateStatus.UNKNOWN) {
                pi.theStatus = PredicateStatus.STARTSTOP;
                collectStartStopPred(pi);
            }
        }
    }

    /**
     * Check if the given pred is a start/stop pred for a path of theIndex.
     * If not, return null. Otherwise, build a PredInfo for it, and check
     * the pred against any other start/stop preds on the same index path.
     * Return the PredInfo, which includes the result of this check.
     */
    private void matchPred(PredInfo pi) {

        if (pi.theStatus != PredicateStatus.UNKNOWN) {
            return;
        }

        IndexExpr epath = pi.theVarArg.getIndexExpr();

        boolean matched = matchPathExprToIndexPath(theIndex, epath, false);

        if (!matched) {
            pi.theStatus = (epath.theFunction != null ?
                            PredicateStatus.NOT_STARTSTOP :
                            PredicateStatus.SKIP);

            if (theTrace >= 2) {
                trace(epath.getPathName() + " does not match any index path");
            }

            if (theIsUnnestingIndex && epath.hasUnnestingVar(theIndex)) {
                theIsRejected = true;
            }

            return;
        }

        if (theTrace >= 2) {
            trace(epath.getPathName() + " matches index path " +
                  epath.getPathPos());
        }

        if (theIsUnnestingIndex && !epath.isUnnested() && epath.theIsMultiValue) {

            if (theTrace >= 2) {
                trace("Index is unnesting but the path is not" +
                      epath.getPathPos());
            }
            pi.theStatus = PredicateStatus.SKIP;
            return;
        }

        if (epath.theIsMultiValue &&
            pi.theVarArg.isMultiValued() &&
            pi.theIsValueComp) {

            if (theTrace >= 1) {
                trace("Value comparison for multivalued epath " +
                      epath.getPathName());
            }

            pi.theStatus = PredicateStatus.SKIP;
            return;
        }

        if (epath.theIsMultiValue && pi.theIsNotExists) {

            if (theTrace >= 1) {
                trace("Not exists condition for multivalued epath " +
                      epath.getPathName());
            }

            pi.theEnclosingPred.theDoesSlicing = true;
        }

        /*
         * A predicate on a prim-key column, which is not part of the index
         * key definition, cannot be used as a start/stop pred on a secondary
         * index. It can, however, be used as a filtering pred.
         */
        if (!theIsPrimary &&
            epath.thePrimKeyPos >= 0 &&
            epath.getPathPos() >= theIndex.numFields() ) {
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            return;
        }

        Expr constArg = pi.theConstArg;
        FieldValueImpl constVal = (pi.isGeo() ? null : pi.theConstVal);

        if (constArg != null && constVal != null && !constVal.isNull()) {

            FieldDefImpl targetType =
                ((epath.getJsonDeclaredType() != null &&
                  !epath.theIsGeo) ?
                 epath.getJsonDeclaredType() :
                 pi.theVarArg.getType().getDef());
            FieldDefImpl constType = constVal.getDefinition();

            FieldValueImpl newConstVal = null;

            if (!TypeManager.areTypesComparable(targetType, constType)) {
                if (theQCB.strictMode()) {
                    throw new QueryException(
                        "Incompatible types for comparison operator: \n" +
                        "Type1: " + pi.theVarArg.getType() +
                        "\nType2: " + pi.theConstArg.getType(),
                        pi.thePred.getLocation());
                }

                if (targetType.isString() && constType.isTimestamp()) {
                    pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                    return;
                }

                if (targetType.isTimestamp() && constType.isString()) {
                    pi.theStatus = PredicateStatus.NOT_STARTSTOP;
                    return;
                }

                newConstVal = BooleanValueImpl.falseValue;

            } else if (targetType.isPrecise()) {
                newConstVal = FuncCompOp.castConstInCompOp(
                    targetType,
                    epath.theIsJson, /*allowJsonNull*/
                    false, /* ignore nullability of varArg */
                    pi.theVarArg.isScalar(),
                    constVal,
                    pi.theOp,
                    theQCB.strictMode());
            }

            if (newConstVal != null && newConstVal != constVal) {

                if (newConstVal == BooleanValueImpl.falseValue) {

                    if (theTrace >= 3) {
                        trace("Always false pred for " + epath.getPathName() +
                              " constVal = " + constVal);
                    }

                    pi.theStatus = PredicateStatus.FALSE;
                    return;
                }

                if (newConstVal == BooleanValueImpl.trueValue) {

                    if (theTrace >= 3) {
                        trace("Always true pred for " + epath.getPathName() +
                              " constVal = " + constVal);
                    }

                    pi.theStatus = PredicateStatus.TRUE;
                    return;
                }

                constVal = newConstVal;
                pi.theConstVal = constVal;
                constArg = new ExprConst(theQCB, theSctx,
                                         constArg.getLocation(),
                                         constVal);
                pi.theConstArg.replace(constArg, true);
                pi.theConstArg = constArg;
            }
        }

        if (constArg != null &&
            !pi.theIsGeo &&
            !checkTypes(pi.theVarArg, pi.theConstArg, false)) {

            if (theTrace >= 2) {
                trace("Match failure due to type check for epath " +
                      epath.getPathName());
            }
            pi.theStatus = PredicateStatus.NOT_STARTSTOP;
            return;
        }

        pi.theStatus = PredicateStatus.STARTSTOP;
        pi.theIPathPos = epath.getPathPos();

        if (theTrace >= 2) {
            trace("pi matches index path " + epath.getPathPos() +
                  ". pi = " + pi);
        }

        if (pi.isUnnested() && !pi.getIndexPath().isMultiKey()) {
            throw new QueryStateException(
                "An unnested predicate matches with the non-multikey index " +
                "field at position " + "pi.theIPathPos. predicate:\n" + pi);
        }
    }

    private boolean matchPathExprToIndexPath(
        IndexImpl index,
        IndexExpr epath,
        boolean matchSimplePathsOnly) {

        if (!epath.matchesIndex(theTable, index)) {
            return false;
        }

        if (theIsUnnestingIndex && epath.isMultiKey() && !epath.isUnnested()) {
            return false;
        }

        if (matchSimplePathsOnly) {

            if (theBestPredGroup != null &&
                theBestPredGroup.theMapBothKey != null &&
                theBestPredGroup.theMapBothKey.equals(epath.getMapBothKey())) {
                matchSimplePathsOnly = false;
            }

            if (epath.getRelativeCtxVarPos(theTable, theIndex) > 0 &&
                (theBestPredGroup == null ||
                 epath.theOuterCtxVar == theBestPredGroup.theOuterCtxVar)) {
                matchSimplePathsOnly = false;
            }

            if (epath.isUnnested(theTable, theIndex)) {
                matchSimplePathsOnly = false;
            }
        }

        if (epath.isMultiKey() && matchSimplePathsOnly) {
            return false;
        }

        return true;
    }

    private boolean checkTypes(Expr varArg, Expr constArg, boolean isIn) {

        IndexExpr epath = varArg.getIndexExpr();
        FieldDefImpl constType = constArg.getType().getDef();
        Type constTypeCode = constType.getType();
        FieldDefImpl varType;
        Type varTypeCode;
        boolean varIsScalar;

        ExprVar bindVar = null;
        if (constArg.getKind() == ExprKind.VAR) {
            bindVar = (ExprVar)constArg;
            assert(bindVar.getVarKind() == VarKind.EXTERNAL);
        }

        if (constTypeCode == Type.EMPTY) {
            return true;
        }

        if (!constArg.isScalar()) {
            return false;
        }

        if (epath != null &&
            epath.getJsonDeclaredType() != null) {

            varType = epath.getJsonDeclaredType();
            varIsScalar = (!epath.theIsMultiValue ||
                           (theIsMapBothIndex && epath.getMapBothKey() != null));

            if (constArg.getKind() == ExprKind.CONST) {
                FieldValueImpl val = ((ExprConst)constArg).getValue();
                if (val.isJsonNull()) {
                    return true;
                }
            }

            if (bindVar != null) {
                if (constType.isWildcard()) {
                    bindVar.setDeclaredType(varType);
                    constTypeCode = varType.getType();
                }
                bindVar.setAllowJsonNull();
            }

            if (!varType.isPrecise()) {

                if (!constType.isSubtype(
                        FieldDefImpl.Constants.anyJsonAtomicDef)) {
                    return false;
                }
                return true;
            }

        } else {
            varType = varArg.getType().getDef();
            varIsScalar = varArg.isScalar();

            if (bindVar != null && constType.isWildcard()) {
                bindVar.setDeclaredType(varType);
                constTypeCode = varType.getType();
            }
        }

        varTypeCode = varType.getType();

        if (theTrace >= 3) {
            trace("checkTypes: varTypeCode = " + varTypeCode +
                  " constTypeCode = " + constTypeCode);
        }

        switch (varTypeCode) {
        case INTEGER:
            return (constTypeCode == Type.INTEGER ||
                    (!isIn && varIsScalar && constTypeCode == Type.LONG));
        case LONG:
            return (constTypeCode == Type.LONG ||
                    constTypeCode == Type.INTEGER);
        case FLOAT:
            return (constTypeCode == Type.FLOAT ||
                    (!isIn && varIsScalar && constTypeCode == Type.DOUBLE) ||
                    constTypeCode == Type.INTEGER ||
                    constTypeCode == Type.LONG);

        case DOUBLE:
            return (constTypeCode == Type.DOUBLE ||
                    constTypeCode == Type.FLOAT ||
                    constTypeCode == Type.INTEGER ||
                    constTypeCode == Type.LONG);
        case NUMBER:
            return constType.isNumeric();
        case ENUM:
            return (constTypeCode == Type.STRING || varType.equals(constType));
        case STRING:
        case BOOLEAN:
            return varTypeCode == constTypeCode;
        case TIMESTAMP:
            return (varTypeCode == constTypeCode);
        default:
            return false;
        }
    }

    private void collectStartStopPred(PredInfo pi) {

        addStartStopPred(pi);

        WherePredInfo wpi = pi.theEnclosingPred;
        IndexExpr epath = pi.theEpath;

        if (epath.isUnnested() && epath.getMapBothKey() != null) {
            throw new QueryStateException(
                "Found a pred factor that is both unnested and has a " +
                "MapBoth key. Predicate = \n" + pi.thePred.display());
        }

        if (epath.isUnnested()) {
            PredGroup.addUnnestedPred(this, pi);
            //theUnnestedGroup.theOuterCtxVar = epath.theOuterCtxVar;

        } else if (epath.getMapBothKey() != null && theIsMapBothIndex) {

            boolean found = PredGroup.addMapBothPred(this, pi);

            if (!found) {

                FieldValueImpl keyval = FieldDefImpl.Constants.stringDef.
                    createString(epath.getMapBothKey());

                PredInfo keypi = new PredInfo(wpi, pi.thePred);
                keypi.theOp = FuncCode.OP_EQ;
                keypi.theIsMapBothKey = true;
                keypi.theIsValueComp = true;
                keypi.theConstVal = keyval;
                keypi.theEpath = pi.theEpath;
                keypi.theIPathPos = theIndex.getPosForKeysField();
                keypi.theStatus = PredicateStatus.STARTSTOP;

                wpi.add(keypi);
                PredGroup.addMapBothPred(this, keypi);
                addStartStopPred(keypi);
            }

        } else if (epath.isMultiKey()) {

            PredInfo keypi = null;

            if (epath.getMapBothKey() != null) {
                FieldValueImpl keyval = FieldDefImpl.Constants.stringDef.
                    createString(epath.getMapBothKey());

                keypi = new PredInfo(wpi, pi.thePred);
                keypi.theOp = FuncCode.OP_EQ;
                keypi.theIsMapBothKey = true;
                keypi.theIsValueComp = true;
                keypi.theConstVal = keyval;
                keypi.theEpath = pi.theEpath;
                keypi.theIPathPos = theIndex.getPosForKeysField();
                keypi.theStatus = PredicateStatus.STARTSTOP;

                wpi.add(keypi);
                addStartStopPred(keypi);
            }

            PredGroup pg = null;

            if (epath.theIsDirect || epath.getRelativeCtxVarPos() > 0) {

                if (wpi.theLocalGroup == null) {
                    pg = addPredGroup(pi);
                    wpi.theLocalGroup = pg;
                } else {
                    pg = wpi.theLocalGroup;
                    wpi.theLocalGroup.thePredInfos.add(pi);
                    pi.thePredGroup = wpi.theLocalGroup;
                }

                if (theTrace >= 1) {
                    trace("Added pi to the local group of WPI-" + wpi.theId +
                          " pi : " + pi);
                }

            } else {
                if (thePredGroups != null &&
                    pi.theEpath != null &&
                    !pi.theEpath.theIsMultiValue) {

                    for (PredGroup pg1 : thePredGroups) {
                        PredInfo pi1 = pg1.thePredInfos.get(0);
                        if (pi1.theEpath != null &&
                            !pi1.theEpath.theIsMultiValue &&
                            pi.theEpath.hasSameCtxVars(pi1.theEpath)) {
                            pg = pg1;
                            break;
                        }
                    }
                }

                if (pg == null) {
                    pg = addPredGroup(pi);

                    if (theTrace >= 1) {
                        trace("Create new pred group PG-" + pg.theId +
                              " for pi: " + pi);
                    }
                } else {
                    pg.thePredInfos.add(pi);
                    pi.thePredGroup = pg;

                    if (theTrace >= 1) {
                        trace("Added pi to pred group PG-" + pg.theId +
                              " pi: " + pi);
                    }
                }
            }

            if (keypi != null) {
                pg.thePredInfos.add(keypi);
                keypi.thePredGroup = pg;
            }

            pg.theOuterCtxVar = epath.theOuterCtxVar;
        }
    }

    private boolean skipExtraneousPreds(int ipos) {

        ArrayList<PredInfo> predinfos = theStartStopPreds.get(ipos);

        if (predinfos == null) {
            return false;
        }

        boolean isUntypedField = (theIndex != null &&
                                  !theIndex.getFieldDef(ipos).isPrecise());

        for (int i = 0; i < predinfos.size(); ++i) {

            PredInfo pi1 = predinfos.get(i);

            if (pi1.theStatus != PredicateStatus.STARTSTOP) {
                assert(pi1.isInOp());
                predinfos.remove(i);
                --i;
                continue;
            }

            for (int j = i + 1; j < predinfos.size(); ++j) {

                PredInfo pi2 = predinfos.get(j);

                if (isUntypedField &&
                    pi1.theConstArg != null &&
                    pi2.theConstArg != null) {

                    FieldDefImpl type1 = pi1.theConstArg.getType().getDef();
                    FieldDefImpl type2 = pi2.theConstArg.getType().getDef();

                    if (!TypeManager.areTypesComparable(type1, type2)) {
                        return true;
                    }
                }

                if (!pi1.isCompatible(pi2)) {
                    continue;
                }

                if (pi2.isExists()) {
                    pi2.theStatus = PredicateStatus.TRUE;

                } else if (pi2.isGeo()) {

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isGeo()) {
                        checkGeoGeo(pi1, pi2);
                    } else if (pi1.isEq()) {
                        throw new QueryStateException(
                            "both geo and eq start-stop predicates");
                    } else if (pi1.isInOp()) {
                        throw new QueryStateException(
                            "both geo and in start-stop predicates");
                    } else {
                        return true;
                    }

                } else if (pi2.isEq()) {

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isGeo()) {
                        throw new QueryStateException(
                            "both geo and eq start-stop predicates");
                    } else if (pi1.isEq()) {
                        checkEqEq(pi1, pi2);
                    } else if (pi1.isInOp()) {
                        checkEqIn(pi2, pi1);
                    } else if (pi1.isMin()) {
                        checkEqMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkEqMax(pi2, pi1);
                    }

                } else if (pi2.isInOp()) {

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isGeo()) {
                        throw new QueryStateException(
                            "both geo and in start-stop predicates");
                   } else if (pi1.isEq()) {
                        checkEqIn(pi1, pi2);
                    } else if (pi1.isInOp()) {
                        checkInIn(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkInMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkInMax(pi2, pi1);
                    }

                } else if (pi2.isMin()) {

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isGeo()) {
                        throw new QueryStateException(
                            "both geo and min start-stop predicates");
                    } else if (pi1.isEq()) {
                        checkEqMin(pi1, pi2);
                    } else if (pi1.isInOp()) {
                        checkInMin(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkMinMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkMinMax(pi2, pi1);
                    }

                } else {
                    assert(pi2.isMax());

                    if (pi1.isExists()) {
                        pi1.theStatus = PredicateStatus.TRUE;
                    } else if (pi1.isGeo()) {
                        throw new QueryStateException(
                            "both geo and max start-stop predicates");
                    } else if (pi1.isEq()) {
                        checkEqMax(pi1, pi2);
                    } else if (pi1.isInOp()) {
                        checkInMax(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkMinMax(pi1, pi2);
                    } else {
                        assert(pi1.isMax());
                        checkMaxMax(pi2, pi1);
                    }
                }

                if (pi1.theStatus == PredicateStatus.FALSE ||
                    pi2.theStatus == PredicateStatus.FALSE) {
                    return true;
                }

                if (pi1.theStatus != PredicateStatus.STARTSTOP) {
                    predinfos.remove(i);
                    --i;
                    break;
                }

                if (pi2.theStatus != PredicateStatus.STARTSTOP) {
                    predinfos.remove(j);
                    --j;
                }
            }
        }

        return false;
    }

    private boolean skipExtraneousPartitionPreds() {

        for (int i = 0; i < thePartitionPreds.size(); ++i) {

            PredInfo pi1 = thePartitionPreds.get(i);

            if (pi1.theStatus != PredicateStatus.STARTSTOP) {
                continue;
            }

            for (int j = i + 1; j < thePartitionPreds.size(); ++j) {

                PredInfo pi2 = thePartitionPreds.get(j);

                if (pi2.theStatus != PredicateStatus.STARTSTOP) {
                    continue;
                }

                if (pi2.isEq()) {

                    if (pi1.isEq()) {
                        checkEqEq(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkEqMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkEqMax(pi2, pi1);
                    }

                } else if (pi2.isMin()) {

                    if (pi1.isEq()) {
                        checkEqMin(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkMinMin(pi2, pi1);
                    } else {
                        assert(pi1.isMax());
                        checkMinMax(pi2, pi1);
                    }

                } else {
                    assert(pi2.isMax());

                    if (pi1.isEq()) {
                        checkEqMax(pi1, pi2);
                    } else if (pi1.isMin()) {
                        checkMinMax(pi1, pi2);
                    } else {
                        assert(pi1.isMax());
                        checkMaxMax(pi2, pi1);
                    }
                }

                if (pi1.theStatus == PredicateStatus.FALSE ||
                    pi2.theStatus == PredicateStatus.FALSE) {
                    return true;
                }

                if (pi1.theStatus != PredicateStatus.STARTSTOP) {
                    break;
                }
            }
        }

        return false;
    }

    private void checkEqEq(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            if (p1.theConstVal.equal(p2.theConstVal)) {
                p1.theStatus = PredicateStatus.TRUE;
            } else {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
            }

        } else if (p1.theConstVal != null) {
            p2.theStatus = PredicateStatus.FILTERING;

        } else if (p2.theConstVal != null) {
            p1.theStatus = PredicateStatus.FILTERING;

        } else {
            if (ConstKind.isExternalConst(p1.theConstArg)) {
                p2.theStatus = PredicateStatus.FILTERING;
            } else {
                p1.theStatus = PredicateStatus.FILTERING;
            }
        }
    }

    private void checkEqIn(PredInfo p1, PredInfo p2) {

        if (p2.isIn3Op()) {
            p2.theStatus = PredicateStatus.FILTERING;
            return;
        }

        if (p1.theConstVal != null) {

            // TODO: remove non-matching keys from IN expr
            for (Expr constInArg : p2.theConstINArgs) {

                if (constInArg.getKind() != ExprKind.CONST) {
                    p2.theStatus = PredicateStatus.FILTERING;
                    continue;
                }

                FieldValueImpl constInVal = ((ExprConst)constInArg).getValue();

                int cmp = FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                          constInVal);

                if (cmp == 0) {
                    p2.theStatus = PredicateStatus.TRUE;
                    break;
                }
            }

            if (p2.theStatus != PredicateStatus.TRUE &&
                p2.theStatus != PredicateStatus.FILTERING) {
                p2.theStatus = PredicateStatus.FALSE;
                p1.theStatus = PredicateStatus.FALSE;
            }
        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkEqMin(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                      p2.theConstVal);

            if (cmp < 0 || (cmp == 0 && !p2.isInclusive())) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
            } else {
                p2.theStatus = PredicateStatus.TRUE;
            }

        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkEqMax(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                      p2.theConstVal);

            if (cmp > 0 || (cmp == 0 && !p2.isInclusive())) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
            } else {
                p2.theStatus = PredicateStatus.TRUE;
            }

        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkInMin(PredInfo p1, PredInfo p2) {

        if (p1.isIn3Op()) {
            p2.theStatus = PredicateStatus.FILTERING;
            return;
        }

        if (p2.theConstVal != null) {
            boolean lessIn = false;
            boolean greaterIn = false;

            // TODO: remove non-matching keys from IN expr
            for (Expr constInArg : p1.theConstINArgs) {

                if (constInArg.getKind() != ExprKind.CONST) {
                    p2.theStatus = PredicateStatus.FILTERING;
                    return;
                }

                FieldValueImpl constInVal = ((ExprConst)constInArg).getValue();

                int cmp = FieldValueImpl.compareKeyValues(constInVal,
                                                          p2.theConstVal);

                if (cmp < 0 || (cmp == 0 && !p2.isInclusive())) {
                    lessIn = true;
                } else {
                    greaterIn = true;
                }
            }

            if (!lessIn) {
                p2.theStatus = PredicateStatus.TRUE;
                return;
            }

            if (!greaterIn) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
                return;
            }

            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkInMax(PredInfo p1, PredInfo p2) {

        if (p1.isIn3Op()) {
            p2.theStatus = PredicateStatus.FILTERING;
            return;
        }

        if (p2.theConstVal != null) {

            boolean lessIn = false;
            boolean greaterIn = false;

            // TODO: remove non-matching keys from IN expr
            for (Expr constInArg : p1.theConstINArgs) {

                if (constInArg.getKind() != ExprKind.CONST) {
                    p2.theStatus = PredicateStatus.FILTERING;
                    return;
                }

                FieldValueImpl constInVal = ((ExprConst)constInArg).getValue();

                int cmp = FieldValueImpl.compareKeyValues(constInVal,
                                                            p2.theConstVal);

                if (cmp > 0 || (cmp == 0 && !p2.isInclusive())) {
                    greaterIn = true;
                } else {
                    lessIn = true;
                }
            }

            if (!greaterIn) {
                p2.theStatus = PredicateStatus.TRUE;
                return;
            }

            if (!lessIn) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;
                return;
            }

            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p2.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkInIn(PredInfo p1, PredInfo p2) {

        assert(p1.thePred != p2.thePred);

        if (p1.theStatus == PredicateStatus.FILTERING ||
            p2.theStatus == PredicateStatus.FILTERING) {
            return;
        }

        boolean choose1 = false;
        boolean choose2 = false;
        ArrayList<PredInfo> predinfos;

        for (int ipos = 0; ipos < p1.theIPathPos; ++ipos) {

            predinfos = theStartStopPreds.get(ipos);

            if (predinfos == null || predinfos.size() > 1) {
                break;
            }

            PredInfo pi = predinfos.get(0);

            if (pi.thePred == p1.thePred) {
                choose1 = true;
                break;
            }

            if (pi.thePred == p2.thePred) {
                choose2 = true;
                break;
            }
        }

        if (!choose1 && !choose2) {

            for (int ipos = p1.theIPathPos + 1; ipos < theNumFields; ++ipos) {

                predinfos = theStartStopPreds.get(ipos);

                if (predinfos == null) {
                    break;
                }

                boolean have1 = false;
                boolean have2 = false;

                for (PredInfo pi : predinfos) {
                    if (pi.isEq()) {
                        have1 = have2 = true;
                        break;
                    }
                    if (pi.thePred == p1.thePred) {
                        have1 = true;
                    }
                    if (pi.thePred == p2.thePred) {
                        have2 = true;
                    }
                }

                if (have1) {
                    if (!have2) {
                        choose1 = true;
                        break;
                    }
                    continue;
                }

                if (have2) {
                    if (!have1) {
                        choose2 = true;
                        break;
                    }
                    continue;
                }

                break;
            }
        }

        if (!choose1 && !choose2) {
            ExprInOp in1 = (ExprInOp)p1.thePred;
            ExprInOp in2 = (ExprInOp)p2.thePred;

            if (!in1.isIN3() && !in2.isIN3()) {
                if (in1.getNumKeys() <= in2.getNumKeys()) {
                    choose1 = true;
                } else {
                    choose2 = true;
                }
            } else if (in1.isIN3()) {
                choose2 = true;
            } else {
                choose1 = true;
            }
        }

        WherePredInfo wpi = (choose1 ?
                             p2.theEnclosingPred :
                             p1.theEnclosingPred);

        for (PredInfo pi : wpi.thePredInfos) {
            if (pi.theStatus == PredicateStatus.STARTSTOP) {
                pi.theStatus = PredicateStatus.FILTERING;
            }
        }
    }

    private void checkMinMin(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp =  FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                       p2.theConstVal);

            if (cmp < 0 || (cmp == 0 && p1.isInclusive())) {
                p1.theStatus = PredicateStatus.TRUE;
            } else {
                p2.theStatus = PredicateStatus.TRUE;
            }

        } else if (p1.theConstVal != null) {
            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p1.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkMaxMax(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp =  FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                       p2.theConstVal);

            if (cmp < 0 || (cmp == 0 && p2.isInclusive())) {
                p2.theStatus = PredicateStatus.TRUE;
            } else {
                p1.theStatus = PredicateStatus.TRUE;
            }

        } else if (p1.theConstVal != null) {
            p2.theStatus = PredicateStatus.FILTERING;

        } else {
            p1.theStatus = PredicateStatus.FILTERING;
        }
    }

    private void checkMinMax(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal != null && p2.theConstVal != null) {

            int cmp = FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                      p2.theConstVal);

            if (cmp > 0 ||
                (cmp == 0 && (!p2.isInclusive() || !p1.isInclusive()))) {
                p1.theStatus = PredicateStatus.FALSE;
                p2.theStatus = PredicateStatus.FALSE;

            } else if (cmp == 0) {
                p1.theOp = FuncCode.OP_EQ;
                p2.theStatus = PredicateStatus.TRUE;
            }
        }
    }

    private void checkGeoGeo(PredInfo p1, PredInfo p2) {

        if (p1.theGeom != null && p2.theGeom != null) {

            Geometry geom1 = p1.theGeom;
            Geometry geom2 = p2.theGeom;

            if (p1.theOp == FuncCode.FN_GEO_INSIDE) {
                if (!geom1.interact(geom2, p1.getLocation())) {
                    p1.theStatus = PredicateStatus.FALSE;
                    p2.theStatus = PredicateStatus.FALSE;
                    return;
                }
            } else if (p2.theOp == FuncCode.FN_GEO_INSIDE) {
                if (!geom2.interact(geom1, p2.getLocation())) {
                    p1.theStatus = PredicateStatus.FALSE;
                    p2.theStatus = PredicateStatus.FALSE;
                    return;
                }
            }

            double area1 = geom1.area(p1.getLocation());
            double area2 = geom2.area(p2.getLocation());

            if (area1 <= area2) {
                p1.theGeom = geom1;
                p2.theStatus = PredicateStatus.SKIP;
            } else {
                p2.theGeom = geom2;
                p1.theStatus = PredicateStatus.SKIP;
            }

        } else if (p1.theGeom != null) {
            p2.theStatus = PredicateStatus.SKIP;

        } else {
            p1.theStatus = PredicateStatus.SKIP;
        }
    }

    private boolean checkAlwaysTrue(PredInfo p1, PredInfo p2) {

        if (p1.theConstVal == null ||
            p2.theConstVal == null ||
            p1.isGeo() ||
            p2.isGeo() ||
            p2.theEnclosingPred.doesFiltering() ||
            p2.theEnclosingPred.theDoesSlicing) {
            return false;
        }

        int cmp = FieldValueImpl.compareKeyValues(p1.theConstVal,
                                                  p2.theConstVal);

        if (p1.isEq()) {
            if (p2.isEq()) {
                return (cmp == 0);
            }
            if (p2.isMin()) {
                if (cmp < 0 || (cmp == 0 && !p2.isInclusive())) {
                    return false;
                }
                return true;
            }
            assert(p2.isMax());
            if (cmp > 0 || (cmp == 0 && !p2.isInclusive())) {
                return false;
            }
            return true;
        }

        if (p1.isMin()) {
            if (p2.isEq()) {
                return false;
            }
            if (p2.isMin()) {
                if (cmp < 0 || (cmp == 0 &&
                                p1.isInclusive() && !p2.isInclusive())) {
                    return false;
                }
                return true;
            }
            assert(p2.isMax());
            return false;
        }

        if (p1.isMax()) {
            if (p2.isEq()) {
                return false;
            }
            if (p2.isMin()) {
                return false;
            }
            assert(p2.isMax());
            if (cmp < 0 || (cmp == 0 &&
                            (!p1.isInclusive() || p2.isInclusive()))) {
                return true;
            }
            return false;
        }

        return false;
    }

    private void chooseMultiKeyPredGroup() {

        if (theUnnestedGroup != null) {
            theBestPredGroup = theUnnestedGroup;
        } else  {
            chooseMultiKeyPredGroup2();
        }

        if (theBestPredGroup == null) {
            return;
        }

        if (theIndex != null &&
            !theIndex.isNestedMultiKeyIndex() &&
            theIndex.isMultiKeyMapIndex()) {
            for (PredInfo pi : theBestPredGroup.thePredInfos) {
                if (pi.theIPathPos >= 0) {
                    IndexField ipath = theIndexPaths.get(pi.theIPathPos);
                    if (pi.mapBothKey() != null ||
                        (pi.isEq() && ipath.isMapKeys())) {
                        theHaveMapKeyEqPred = true;
                        break;
                    }
                }
            }
        }

        if (theTrace >= 1) {
            trace("Best pred group = PG-" + theBestPredGroup.theId);
        }

        /*
         * Remove from the multikey index paths in theStartStopPreds all pred
         * infos that do not belong to theBestMultiKeyPred, and mark them as SKIP.
         */
        for (int i = 0; i < theNumFields; ++i) {

            if (!theIndexPaths.get(i).isMultiKey()) {
                continue;
            }

            ArrayList<PredInfo> preds = theStartStopPreds.get(i);

            if (preds == null) {
                continue;
            }

            for (int j = 0; j < preds.size(); ++j) {

                PredInfo pi = preds.get(j);

                if (pi.theStatus == PredicateStatus.SKIP) {
                    preds.remove(j);
                    --j;
                    continue;
                }

                if (pi.thePredGroup == theBestPredGroup) {
                    continue;
                }

                preds.remove(j);
                --j;

                /* Multikey preds in different groups are not compared in
                 * skipExtraneousPreds(), because we don't want to remove
                 * any preds before we determine the best group. So we check
                 * here if a non-best pi is always true. */
                if (pi.mapBothKey() == null) {
                    for (PredInfo pi2 : preds) {
                        if (pi2.thePredGroup == theBestPredGroup &&
                            (pi2.isUnnested() == pi.isUnnested() ||
                             pi2.isUnnested()) &&
                            checkAlwaysTrue(pi2, pi)) {
                            pi.theStatus = PredicateStatus.TRUE;
                            break;
                        }
                    }
                }

                if (pi.theStatus != PredicateStatus.TRUE) {
                    pi.theStatus = PredicateStatus.SKIP;
                }
            }
        }
    }

    private void chooseMultiKeyPredGroup2() {

        boolean filteringOnly = false;

        for (int ipos = 0; ipos < theNumFields; ++ipos) {

            if (theTrace >= 1) {
                trace("processing ifield at pos " + ipos);
            }

            ArrayList<PredInfo> predinfos = theStartStopPreds.get(ipos);

            if (predinfos == null || predinfos.isEmpty()) {
                if (theTrace >= 1) {
                    trace("no preds at pos " + ipos);
                }
                filteringOnly = true;
                continue;
            }

            if (theIsPrimary || !theIndexPaths.get(ipos).isMultiKey()) {
                if (predinfos.size() > 2) {
                    throw new QueryStateException(
                        "More than two predicates for non-multikey index " +
                        "field at position " + ipos);
                }

                if (!predinfos.get(0).isEq()) {
                    if (theTrace >= 1) {
                        trace("no EQ preds at pos " + ipos);
                    }
                    filteringOnly = true;
                }
                continue;
            }

            for (int i = 0; i < predinfos.size(); ++i) {

                PredInfo pi = predinfos.get(i);

                PredGroup pg = pi.thePredGroup;

                if (theTrace >= 1) {
                    trace("processing pi at pos " + ipos + "\nPG-" +
                          pg.theId + " pg.theFilteringOnly = " +
                          pg.theFilteringOnly +
                          " pg.theMapBothKey = " + pg.theMapBothKey +
                          " pi = \n" + pi);
                }

                if (pi.theStatus != PredicateStatus.STARTSTOP) {
                    throw new QueryStateException(
                        "Found a non STARTSTOP predicate in theStartStopPreds " +
                        " as position " + ipos);
                }

                if (filteringOnly || pg.theFilteringOnly) {
                    if (pi.isEq()) {
                        pg.theFieldScore += filterEqValue;
                    } else {
                        pg.theFieldScore += filterOtherValue;
                    }

                } else if (pi.isEq()) {
                    pg.theFieldScore += eqValue;
                } else if (pi.isInOp()) {
                    pg.theFieldScore += inValue;
                } else {
                    pg.theFieldScore += vrangeValue;
                    pg.theFoundRange = true;
                }

                if (theTrace >= 1) {
                    trace("Field score for PG-" + pg.theId + " = " +
                          pg.theFieldScore);
                }
            }

            for (PredGroup pg : thePredGroups) {
                pg.theScore += pg.theFieldScore;
                if (theTrace >= 1) {
                    trace("Total score for PG-" + pg.theId + " = " + pg.theScore);
                }
                if (pg.theFieldScore == 0 || pg.theFoundRange) {
                    pg.theFilteringOnly = true;
                }
                pg.theFieldScore = 0;
            }
        }

        /*
         * Now choose the "best" multikey WHERE pred
         */
        for (PredGroup pg : thePredGroups) {
            if (theBestPredGroup == null) {
                if (pg.theScore > 0) {
                    theBestPredGroup = pg;
                }
            } else if (pg.theScore > theBestPredGroup.theScore) {
                theBestPredGroup = pg;
            } else if (pg.theScore == theBestPredGroup.theScore) {
                if (!pg.theIsUnnested && theBestPredGroup.theIsUnnested ||
                    pg.theId < theBestPredGroup.theId) {
                    theBestPredGroup = pg;
                }
            }
        }
    }

    /**
     * Try to push start/stop preds on the given index path.
     */
    private void pushStartStopPreds() {

        boolean pushedMultiKeyPred = false;

        int lastStartStopPos = -2; // -2 means unknown
        int ipos = 0;

        for (; ipos < theNumFields; ++ipos) {

            ArrayList<PredInfo> predinfos = theStartStopPreds.get(ipos);

            if (predinfos == null || predinfos.isEmpty()) {
                if (lastStartStopPos == -2) {
                    lastStartStopPos = ipos-1;
                }
                continue;
            }

            if (predinfos.size() > 2) {
                throw new QueryStateException(
                    "More than 2 start/stop predicates for index field at " +
                    "position " + ipos);
            }

            PredInfo pi1 = predinfos.get(0);
            PredInfo pi2 = (predinfos.size() > 1 ? predinfos.get(1) : null);

            if (pi1.theStatus != PredicateStatus.STARTSTOP) {
                throw new QueryStateException(
                    "Pushing a predicate marked as " + pi1.theStatus + "\n" +
                    pi1);
            }

            if (pi2 != null && pi2.theStatus != PredicateStatus.STARTSTOP) {
                throw new QueryStateException(
                    "Pushing a predicate marked as " + pi2.theStatus + "\n" +
                    pi2);
            }

            if (lastStartStopPos >= -1) {
                if (pi1.isGeo()) {
                    pi1.theStatus = PredicateStatus.SKIP;
                } else {
                    pi1.theStatus = PredicateStatus.FILTERING;
                }
                if (pi2 != null) {
                    pi2.theStatus = PredicateStatus.FILTERING;
                }

                continue;
            }

            IndexField ipath = theIndexPaths.get(ipos);

            pushedMultiKeyPred = (pushedMultiKeyPred || ipath.isMultiKey());

            if (pi1.isEq()) {
                assert(predinfos.size() == 1);

                FieldValueImpl constVal;
                Expr bindExpr = null;

                if (pi1.theConstVal != null) {
                    constVal = pi1.theConstVal;
                } else {
                    theHaveBindKeys = true;
                    constVal = createPlaceHolderValue(ipath.getType());
                    bindExpr = pi1.theConstArg;
                }

                for (int i = 0; i < theIndexKeys.size(); ++i)  {

                    RecordValueImpl key = theIndexKeys.get(i);
                    ArrayList<Expr> bindKey = theBindKeys.get(i);
                    key.put(ipos, constVal);
                    bindKey.set(ipos, bindExpr);
                }

                ++theNumEqPredsPushed;
                continue;
            }

            if (pi1.isInOp()) {
                assert(predinfos.size() == 1);

                if (pi1.theIsPushedINpi) {
                    continue;
                }

                boolean isIn3 = pi1.isIn3Op();

                /* Look for other pis that belong to the same IN pred and can
                 * also be pushed on subsequent index fields. */
                ArrayList<PredInfo> inPIs = new ArrayList<PredInfo>(4);
                inPIs.add(pi1);
                ++theNumInPredsPushed;
                ++theNumInCompsPushed;

                for (int ipos2 = ipos + 1; ipos2 < theNumFields; ++ipos2) {

                    ArrayList<PredInfo> predinfos2 = theStartStopPreds.get(ipos2);

                    if (predinfos2 == null || predinfos2.isEmpty()) {
                        break;
                    }

                    pi2 = predinfos2.get(0);

                    if (!pi2.isEq() && !pi2.isInOp()) {
                        break;
                    }

                    if (pi2.thePred == pi1.thePred) {
                        inPIs.add(pi2);
                        pi2.theIsPushedINpi = true;
                        ++theNumInCompsPushed;
                    }
                }

                /* Do the pushing */
                int numKeys = theIndexKeys.size();
                int numInSearchKeys = (isIn3 ? 1 : pi1.theConstINArgs.size());

                if (isIn3) {
                    ExprInOp inExpr = (ExprInOp)pi1.thePred;
                    In3BindInfo in3bi = new In3BindInfo(inExpr, inPIs.size());
                    theIn3BindInfos.add(in3bi);
                    for (int i = 0; i < inPIs.size(); ++i) {
                        PredInfo pi = inPIs.get(i);
                        in3bi.thePushedComps[i] = pi.theInComp;
                        in3bi.theIndexFieldPositions[i] = pi.theIPathPos;
                    }
                }

                for (int i = 0; i < numKeys; ++i) {

                    RecordValueImpl key = theIndexKeys.get(i);
                    ArrayList<Expr> bindKey = theBindKeys.get(i);

                    for (int k = 0; k < numInSearchKeys; ++k) {

                        if (k != 0) {
                            key = key.clone();
                            bindKey = cloneBindKey(bindKey);
                            theIndexKeys.add(key);
                            theRanges.add(null);
                            theBindKeys.add(bindKey);
                        }

                        for (PredInfo pi : inPIs) {
                            int ipos2 = pi.theIPathPos;
                            IndexField ipath2 = theIndexPaths.get(ipos2);
                            Expr constArg = (isIn3 ?
                                             pi.theConstArg :
                                             pi.theConstINArgs.get(k));
                            FieldValueImpl constVal;
                            Expr bindExpr = null;

                            if (!isIn3 && constArg.getKind() == ExprKind.CONST) {
                                constVal = ((ExprConst)constArg).getValue();
                            } else {
                                theHaveBindKeys = true;
                                constVal = createPlaceHolderValue(ipath2.getType());
                                bindExpr = constArg;
                            }

                            key.put(ipos2, constVal);
                            bindKey.set(ipos2, bindExpr);

                            if (ipath2.isMultiKey() &&
                                !theHaveMapKeyEqPred &&
                                !pi.isUnnested()) {
                                theEliminateDups = true;
                            }
                        }
                    }
                }

                continue;
            }

            if (pi1.isExists()) {
                assert(predinfos.size() == 1);
                assert(!theIsPrimary);

                for (int k = 0; k < theIndexKeys.size(); k += 2) {

                    RecordValueImpl key = theIndexKeys.get(k);
                    ArrayList<Expr> bindKey = theBindKeys.get(k);

                    theIndexKeys.add(k+1, key);
                    theBindKeys.add(k+1, bindKey);

                    String pathName = theIndex.getFieldName(ipos);
                    FieldDefImpl rangeDef = ipath.getType();

                    FieldRange fr1 = new FieldRange(pathName, rangeDef, 0);
                    FieldRange fr2 = new FieldRange(pathName, rangeDef, 0);

                    fr1.setEnd(EmptyValueImpl.getInstance(), false, false);
                    fr2.setStart(EmptyValueImpl.getInstance(), false, false);

                    theRanges.set(k, fr1);
                    theRanges.add(k+1, fr2);

                    if (ipath.isMultiKey() && !theHaveMapKeyEqPred) {
                        theIsMultiKeyRange = true;

                        if (!(pi1.isUnnested() && theIsUnnestingIndex)) {
                            theEliminateDups = true;
                        }
                    }
                }

                lastStartStopPos = ipos;
                continue;
            }

            if (pi1.isGeo()) {
                lastStartStopPos = ipos;

                String pathName = theIndex.getFieldName(ipos);
                FieldDefImpl rangeDef = FieldDefImpl.Constants.stringDef;
                FieldRange fr = new FieldRange(pathName, rangeDef, 0);
                fr.setEnd(EmptyValueImpl.getInstance(), false, false);
                fr.setStart(EmptyValueImpl.getInstance(), false, false);

                for (int k = 0; k < theIndexKeys.size(); ++k) {
                    theRanges.set(k, fr);
                }

                if (ipath.isMultiKey() || theIndex.isGeometryIndex()) {
                    theIsMultiKeyRange = true;
                    theEliminateDups = true;
                }

                if (theTrace >= 2) {
                    trace("Added fake range for geo pred");
                }
                continue;
            }

            PredInfo minpi = null;
            PredInfo maxpi = null;

            if (pi1.isMin()) {
                minpi = pi1;

                if (pi2 != null) {
                    assert(pi2.isMax());
                    maxpi = pi2;
                }
            } else {
                assert(pi1.isMax());
                maxpi = pi1;

                if (pi2 != null) {
                    assert(pi2.isMin());
                    minpi = pi2;
                }
            }

            for (int k = 0; k < theIndexKeys.size(); ++k) {
                createRange(k, ipath, minpi, maxpi);
            }

            if (theIsMultiKeyRange) {
                theEliminateDups = true;
            }

            lastStartStopPos = ipos;
        }

        if (theIndex != null &&
            theIndex.isMultiKey() &&
            !theHaveMapKeyEqPred &&
            !theEliminateDups &&
            !theIsUnnestingIndex) {

            if (!pushedMultiKeyPred) {
                theEliminateDups = true;
            } else {
                for (ipos = lastStartStopPos+1;
                     ipos < theIndexPaths.size();
                     ++ipos) {
                    IndexField ipath = theIndexPaths.get(ipos);
                    if (ipath.isMultiKey()) {
                        theEliminateDups = true;
                        break;
                    }
                }
            }
        }
    }

    /*
     *
     */
    private void createRange(
        int keyIdx,
        IndexField ipath,
        PredInfo minpi,
        PredInfo maxpi) {

        int storageSize = (theIsPrimary ?
                           theTable.getPrimaryKeySize(ipath.getStep(0)) :
                           0);

        FieldDefImpl rangeDef = ipath.getType();
        String pathName = (theIsPrimary ? ipath.getStep(0) :
                           theIndex.getFieldName(ipath.getPosition()));

        FieldRange range = new FieldRange(pathName, rangeDef, storageSize);
        theRanges.set(keyIdx, range);

        ArrayList<Expr> bindKey = theBindKeys.get(keyIdx);

        if (minpi != null) {
            if (minpi.theConstVal == null) {
                theHaveBindKeys = true;
                bindKey.set(minpi.theIPathPos, minpi.theConstArg);
                FieldValueImpl val = createPlaceHolderValue(rangeDef);
                range.setStart(val, minpi.isInclusive(), false);
            } else {
                range.setStart(minpi.theConstVal, minpi.isInclusive());
            }

            if (ipath.isMultiKey() && !theHaveMapKeyEqPred &&
                !(minpi.isUnnested() && theIsUnnestingIndex)) {
                theIsMultiKeyRange = true;
            }
        }

        if (maxpi != null) {
            if (maxpi.theConstVal == null) {
                theHaveBindKeys = true;
                bindKey.set(maxpi.theIPathPos + 1, maxpi.theConstArg);
                FieldValueImpl val = createPlaceHolderValue(rangeDef);
                range.setEnd(val, maxpi.isInclusive(), false);
            } else {
                range.setEnd(maxpi.theConstVal, maxpi.isInclusive());
            }

            if (ipath.isMultiKey() && !theHaveMapKeyEqPred &&
                !(maxpi.isUnnested() && theIsUnnestingIndex)) {
                theIsMultiKeyRange = true;
            }
        }
    }

    /*
     * Check if the index is a covering one. For this to be true, the index
     * must "cover" all the exprs in the query. We say that the index covers
     * an expr if the expr does not reference any non-indexed paths within
     * theTable. If the query does not have a NESTED TABLES, this means that
     * the whole expr can be evaluated using index fields only.
     */
    private void checkIsCovering() {

        if (theTrace >= 2) {
            trace("\nChecking if index is covering on this SFW\n");
        }

        theIsCovering = checkIsCovering(theSFW);

        if (theIsCovering) {
            if (theOrigSFW != null) {

                if (theTrace >= 2) {
                    trace("\nChecking if index is covering on orig SFW\n");
                }
                theIsCovering = checkIsCovering(theOrigSFW);
            }
        }
    }

    private boolean checkIsCovering(ExprSFW sfw) {

        boolean isCovering = false;

        int numPreds = (sfw == theSFW ?
                        getNumPreds() :
                        (sfw.getWhereExpr() != null ? 1 : 0));
        int numIndexPreds = 0;

        if (sfw == null) {
            return false;
        }

        /*
         * Any index of key-only table is always covering. Nevertheless, we
         * must still go through all the exprs and call isIndexOnlyExpr on
         * them, because isIndexOnlyExpr() creates theExprRewriteMap.
         *
         * Actrually, it is possible to have an index on a key-only table
         * that is not covering. Here is the case:
         *
         * create table foo(id INTEGER, primary key(id))
         * create index idx on foo(id)
         *
         * select row_storage_size($t) as Row_Storage_Size,
         *        index_storage_size($t,"idx") as IDX_SIZE
         * from foo $t
         *
         * Index "idx" is not covering because of the row_storage_size()
         * function. This index should not be selected. It is rejected
         * is the analyze() function.
         */
        boolean isKeyOnly = theTable.isKeyOnly() && !theTable.isJsonCollection();

        if (isKeyOnly) {
            assert(theIsPrimary || !theIndex.isMultiKey());
        }

        boolean hasNestedTables = theTableExpr.hasNestedTables();

        /*
         * Check whether the index covers all the WHERE preds.
         */
        if (sfw == theSFW) {
            for (WherePredInfo wpi : theWherePreds) {

                if (wpi.isFullyPushable()) {
                    ++numIndexPreds;
                    continue;
                }

                if (hasNestedTables &&
                    isIndexOnlyExpr(wpi.thePred, false, true)) {
                    ++numIndexPreds;
                }
            }
        } else if (sfw.getWhereExpr() != null &&
                   isIndexOnlyExpr(sfw.getWhereExpr(), false, true)) {
            ++numIndexPreds;
        }

        numIndexPreds += thePartitionPreds.size();
        numIndexPreds += theShardPreds.size();

        assert(numIndexPreds <= numPreds);

        isCovering = (numIndexPreds == numPreds);

        if (!isCovering) {
            if (theTrace >= 2) {
                trace("Index is not covering: does not cover all predicates");
            }

            if (!theIsUnnestingIndex) {
                return false;
            }
        }

        /*
         * Check whether the index covers all the exprs in the SELECT clause.
         */
        int numFieldExprs = sfw.getNumFields();

        for (int i = 0; i < numFieldExprs; ++i) {

            Expr expr = sfw.getFieldExpr(i);

            if (!isIndexOnlyExpr(expr, false, true)) {

                /*
                 * If the expr is a row var, see if every column of the table
                 * is contained in the index entry.
                 */
                if ((theIsPrimary || !theIndex.isMultiKey()) &&
                    !theTable.isJsonCollection() &&
                    expr.getKind() == ExprKind.VAR &&
                    ((ExprVar)expr).getTable() != null &&
                    ((ExprVar)expr).getTable().getId() == theTable.getId()) {

                    RecordDefImpl rowDef = theTable.getRowDef();
                    int numCols = rowDef.getNumFields();

                    int j = 0;
                    for (; j < numCols; ++j) {
                        String colName = rowDef.getFieldName(j);

                        int k = 0;
                        for (; k < theNumFields; ++k) {
                            IndexField ipath = theIndexPaths.get(k);
                            if (ipath.numSteps() == 1 &&
                                ipath.getFunction() == null &&
                                ipath.getStep(0).equalsIgnoreCase(colName)) {
                                break;
                            }
                        }

                        if (k == theNumFields) {
                            break;
                        }
                    }

                    if (j == numCols) {
                        continue;
                    }
                }
            } else {
                continue;
            }

            if (theTrace >= 2) {
                trace("Index is not covering: it does " +
                      "not cover the " + i + "-th SELECT expr");
            }

            return false;
        }

        /*
         * The index must cover all the exprs in the ORDERBY clause. Normally,
         * this should be true already, but we must call isIndexOnlyExpr() on
         * each sort expr in order to rewrite it to access the index var.
         * Furthermore, the primary index is always analyzed, and if the query
         * is not sorting by prim key columns, we must mark the prim index as
         * not covering.
         */
        int numSortExprs = sfw.getNumSortExprs();

        for (int i = 0; i < numSortExprs; ++i) {
            Expr expr = sfw.getSortExpr(i);
            if (!isIndexOnlyExpr(expr, false, true)) {
                isCovering = false;
                if (theTrace >= 2) {
                    trace("Index is not covering: it does " +
                          "not cover the " + i + "-th ORDER BY expr");
                }

                if (!theIsUnnestingIndex) {
                    return false;
                }
            }
        }

        int numFroms = sfw.getNumFroms();

        for (int i = 0; i < numFroms; ++i) {

            FromClause fc = sfw.getFromClause(i);
            Expr domExpr = fc.getDomainExpr();

            /*
             * Check whether the index covers all the ON preds in a NESTED
             * TABLES clause.
             */
            if (domExpr == theTableExpr) {

                if (!hasNestedTables) {
                    continue;
                }

                int numTables = theTableExpr.getNumTables();

                for (int j = 0; j < numTables; ++j) {
                    Expr pred = theTableExpr.getTablePred(j);
                    if (pred != null &&
                        !isIndexOnlyExpr(pred, false, true)) {
                        return false;
                    }
                }

                continue;
            }

            ExprVar var = fc.getVar();

            /*
             * If the var is used in any exprs, those exprs have been checked
             * above. Also, if the var is not used in any exprs, but its
             * domain expr is scalar, the var will be removed when we apply
             * the index. So, they only case we need to check here is that
             * the var is not used in any exprs and its domain is not scalar.
             */
            if (sfw == theSFW &&
                var.getNumParents() == 0 &&
                !domExpr.isScalar() &&
                !(theIsUnnestingIndex && var.isUnnestingIndex(theIndex))) {

                if (!isIndexOnlyExpr(domExpr, false, true)) {
                    assert(!isKeyOnly);
                    return false;
                }
            } else if (i > 0 && sfw == theOrigSFW) {
                if (!isIndexOnlyExpr(domExpr, false, true)) {
                    if (theTrace >= 2) {
                        trace("Index is not covering due to unnest expr in join");
                    }
                    return false;
                }
            }
        }

        if (theTrace >= 2) {
            trace("Index " + getIndexName() + " is covering = " + isCovering);
        }

        return isCovering;
    }

    /**
     * If strict is true, this method checks whether the given expr is an expr
     * that can be evaluated using the columns of the current index only (which
     * may be the primary index, if theIndex is null). If strict is false, the
     * method allows the expr to reference columns from tables other than
     * theTable. So it will return true if for each subexpr of expr that
     * references columns of theTable only, the subexpr can be evaluated using
     * the columns of the current index entry only.
     */
    private boolean isIndexOnlyExpr(
        Expr expr,
        boolean strict,
        boolean matchSimplePathsOnly) {

        return isIndexOnlyExpr(expr, expr, strict, matchSimplePathsOnly);
    }

    private boolean isIndexOnlyExpr(
        Expr initExpr,
        Expr expr,
        boolean strict,
        boolean matchSimplePathsOnly) {

        boolean isIndexOnly = true;

        switch (expr.getKind()) {
        case FIELD_STEP:
        case MAP_FILTER:
        case ARRAY_FILTER:
        case ARRAY_SLICE:
        case VAR: {
            if (expr.getKind() == ExprKind.VAR) {
                ExprVar var = (ExprVar)expr;
                if (var.isExternal()) {
                    return true;
                }
            }

            IndexExpr epath = expr.getIndexExpr();

            if (epath == null ||
                epath.theIsGeo ||
                epath.theDoesSlicing ||
                epath.theFilteringPreds != null ||
                (strict &&
                 (epath.theTable.getId() != theTable.getId() ||
                  epath.theTableExpr != theTableExpr))) {

                if (theTrace >= 2) {
                    trace("isIndexOnlyExpr 1 failed for expr:\n" + expr.display());
                }
                isIndexOnly = false;
                break;
            }

            if (!strict &&
                (epath.theTable.getId() != theTable.getId() ||
                 epath.theTableExpr != theTableExpr)) {
                return true;
            }

            if (!matchPathExprToIndexPath(theIndex,
                                          epath,
                                          matchSimplePathsOnly)) {

                if (theIsUnnestingIndex && epath.hasUnnestingVar(theIndex)) {
                    theIsRejected = true;
                }

                if (theTrace >= 2) {
                    trace("isIndexOnlyExpr 2 failed for expr:\n" + expr.display());
                }
                isIndexOnly = false;
                break;
            }

            if (epath.doesSlicing()) {
                if (theTrace >= 2) {
                    trace("isIndexOnlyExpr 3 failed for expr:\n" + expr.display());
                }
                isIndexOnly = false;
                break;
            }

            addExprToReplace(initExpr, expr, epath.getPathPos());
            return true;
        }
        case BASE_TABLE:
            if (theTrace >= 2) {
                trace("isIndexOnlyExpr 4 failed for expr:\n" + expr.display());
            }
            return false;
        case CONST:
            return true;
        case FUNC_CALL:
            ExprFuncCall fncall = (ExprFuncCall)expr;
            Function func = fncall.getFunction();

            if (func.getCode() == FuncCode.FN_GEO_INTERSECT ||
                func.getCode() == FuncCode.FN_GEO_INSIDE) {
                return false;
            }

            /* First check if the function is indexed */
            IndexExpr epath = expr.getIndexExpr();

            if (theTrace >= 2) {
                trace("isIndexOnlyExpr for function: " + func.getCode() +
                      " epath: " + (epath == null ? "null" : epath.getPathName()));
            }

            if (epath == null ||
                epath.theIsGeo ||
                epath.theDoesSlicing ||
                epath.theFilteringPreds != null ||
                (strict &&
                 (epath.theTable.getId() != theTable.getId() ||
                  epath.theTableExpr != theTableExpr))) {

                if (!func.isRowProperty()) {
                    break;
                }
            }

            if (epath != null &&
                !strict &&
                (epath.theTable.getId() != theTable.getId() ||
                 epath.theTableExpr != theTableExpr)) {
                return true;
            }

            IndexExpr argPath = fncall.getArg(0).getIndexExpr();

            if (theTrace >= 2) {
                trace("isIndexOnlyExpr for function: " + func.getCode() +
                      " argPath: " + (argPath == null ? "null" : argPath.getPathName()));
            }

            if ((epath != null && epath.theIsMultiValue) ||
                (argPath != null && argPath.theIsMultiValue)) {
                if (theTrace >= 2) {
                    trace("isIndexOnlyExpr 5 failed for expr:\n" + expr.display());
                }
                return false;
            }

            if (epath != null &&
                matchPathExprToIndexPath(theIndex,
                                         epath,
                                         matchSimplePathsOnly) &&
                !epath.doesSlicing()) {

                addExprToReplace(initExpr, expr, epath.getPathPos());
                return true;
            }

            /* Special treatment for row property functions, which may be
             * index-only even if they are not indexed */
            if (func.isRowProperty()) {
                ExprVar rowvar = (ExprVar)fncall.getArg(0);
                TableImpl table = rowvar.getTable();

                if (!theTableExpr.hasNestedTables() &&
                    table.getId() == theTable.getId()) {

                    switch (func.getCode()) {
                    case FN_PARTITION:
                    case FN_SHARD:
                        break;
                    case FN_CREATION_TIME:
                    case FN_CREATION_TIME_MILLIS:
                    case FN_MOD_TIME:
                        return false;
                    case FN_VERSION:
                        return false;
                    case FN_ROW_STORAGE_SIZE:
                        if (theIsPrimary) {
                            break;
                        }
                        return false;
                    case FN_INDEX_STORAGE_SIZE:
                        if (theIsPrimary) {
                            return false;
                        }
                        String indexName = ((ExprConst)fncall.getArg(1)).
                                           getString();
                        if (theIndex.getTable().getId() == table.getId() &&
                            table == theTableExpr.getTargetTable() &&
                            theIndex.getName().equals(indexName) &&
                            (!theIndex.isMultiKey() ||
                             theSFW.checkOptimizeMKIndexSizeCall(theIndex))) {
                            break;
                        }
                        return false;
                    case FN_ROW_METADATA:
                        return false;
                    default:
                        break;
                    }

                    addExprToReplace(initExpr, rowvar, -1);
                    return true;
                }
                return false;
            }

            /* Drill into the function subexprs */
            break;
        default:
            break;
        }

        ExprIter children = expr.getChildren();

        while (children.hasNext()) {
            Expr child = children.next();
            if (!isIndexOnlyExpr(initExpr, child, strict,
                                 matchSimplePathsOnly)) {
                isIndexOnly = false;
            }
        }

        children.reset();

        return isIndexOnly;
    }

    private void addExprToReplace(
        Expr initExpr,
        Expr expr,
        int idxFieldPos) {

        ArrayList<ExprToReplace> exprsToReplace =
            theExprRewriteMap.get(initExpr);

        if (exprsToReplace == null) {
            exprsToReplace = new ArrayList<ExprToReplace>();
            theExprRewriteMap.put(initExpr, exprsToReplace);
        }
        
        exprsToReplace.add(new ExprToReplace(expr, idxFieldPos));
    }

    /*
     * Method to check whether the index has an "interesting order". If the
     * SFW has group-by, an index has interesting order if it orders the
     * rows according to the grouping exprs. If the SFW has order-by, but
     * not group-by, an index has interesting order if it orders the
     * rows according to the order-by exprs.
     *
     * If the index does have an interesting order, it is added to the "sorting
     * indexes" of the SFW.
     */
    private void checkIsSortingIndex() {

        if (theSFW.getNumGroupExprs() == 0 && !theEliminateDups) {
            theSFW.addSortingIndex(theIndex);
            return;
        }

        boolean hasGroupBy = theSFW.hasGroupBy();
        boolean hasSort = theSFW.hasSort() && !hasGroupBy;
        Direction direction = Direction.FORWARD;
        boolean desc = false;
        boolean nullsLast = false;
        int i;
        int e;

        if (!hasSort && !hasGroupBy) {
            return;
        }

        int numPkCols = theTable.getPrimaryKeySize();
        int numShardKeys = theTable.getShardKeySize();
        int numTables = theTableExpr.getNumTables();
        int numAncestors = theTableExpr.getNumAncestors();
        int numDescendants = theTableExpr.getNumDescendants();

        int numExprs = (hasGroupBy ?
                        theSFW.getNumGroupExprs() :
                        theSFW.getNumSortExprs());

        /* Determine the sort direction */
        if (hasSort) {
            SortSpec[] specs = theSFW.getSortSpecs();
            SortSpec spec = specs[0];
            desc = spec.theIsDesc;
            nullsLast = !spec.theNullsFirst;
            direction = (desc ? Direction.REVERSE : Direction.FORWARD);

            for (i = 1; i < specs.length; ++i) {
                spec = specs[i];
                if (desc != spec.theIsDesc ||
                    nullsLast != (!spec.theNullsFirst)) {
                    return;
                }
            }

            if ((desc && nullsLast) || (!desc && !nullsLast)) {
                return;
            }
        }

        /* If the index is the primary one, check whether the sort/group exprs
         * are a prefix of the primary key columns. */
        if (theIsPrimary) {

            if (hasSort && desc && numDescendants > 0) {
                /* In the current implementation, it is not possible to use
                 * an index to order by primary key columns in descending
                 * order when the NESTED TABLES clause contains descendants */
                return;
            }

            for (i = 0, e = 0; i < numPkCols && e < numExprs; ++i) {

                Expr expr = (hasGroupBy ?
                             theSFW.getFieldExpr(e) :
                             theSFW.getSortExpr(e));

                if (ExprUtils.
                    isPrimKeyColumnRef(theTableExpr, theTable, i, expr)) {

                    if (hasGroupBy && i == numShardKeys - 1) {
                        theSFW.setGroupByExprCompleteShardKey();
                    }

                    ++e;
                    continue;
                }

                /* The currest expr does not match with the current pk column.
                 * Check whether we have an equality predicate on the current
                 * pk column. If so, skip the current pk column. */
                ArrayList<PredInfo> startstopPIs = theStartStopPreds.get(i);
                if (startstopPIs != null && startstopPIs.size() == 1) {
                    PredInfo pi = startstopPIs.get(0);
                    if (pi.isEq()) {
                        continue;
                    }
                }
                
                break;
            }

            if (e == numExprs) {
                theSFW.addSortingIndex(null);
                theTableExpr.setDirection(direction);
                return;
            }

            /* If the pk columns of the target table are a prefix of the 
             * sort/group exprs, check if the remaining sort/group exprs
             * are the pk columns of the descendant tables. Inheritted
             * pk columns may or may not be among the sort/group exprs. */
            if (i == numPkCols && numDescendants > 0 && !desc) {

                int numAncestorPkCols = numPkCols;

                for (int t = numAncestors + 1; t < numTables; ++t) {

                    TableImpl descendant = theTableExpr.getTable(t);
                    numPkCols = descendant.getPrimaryKeySize();

                    for (int pkPos = 0;
                         e < numExprs && pkPos < numPkCols;
                         ++e, ++pkPos) {
                        Expr expr = (hasGroupBy ?
                                     theSFW.getFieldExpr(e) :
                                     theSFW.getSortExpr(e));
                        if (!ExprUtils.isPrimKeyColumnRef(theTableExpr,
                                                          descendant,
                                                          pkPos,
                                                          expr)) {
                            if (pkPos < numAncestorPkCols) {
                                --e;
                                continue;
                            }

                            return;
                        }
                    }

                    if (e == numExprs) {
                        theSFW.addSortingIndex(null);
                        theTableExpr.setDirection(direction);
                        break;
                    }

                    numAncestorPkCols = numPkCols;
                }
            }

            return;
        }

        /* Don't use multikey index for group-by because duplicate
         * elimination may be required. It's hard to implement duplicate
         * elimination together with group by, and it would result in no
         * actual grouping done at the RNs (because the prim key columns
         * would have to be added in the group-by). */
        if (hasGroupBy && theEliminateDups) {
            return;
        }

        /* Check whether the sort exprs are a prefix of the index paths. */
        List<IndexField> indexPaths = theIndex.getIndexFields();

        for (i = 0, e = 0; i < indexPaths.size() && e < numExprs; ++i) {

            IndexField ipath = indexPaths.get(i);
            Expr expr = (hasGroupBy ?
                         theSFW.getFieldExpr(e) :
                         theSFW.getSortExpr(e));
            IndexExpr epath = expr.getIndexExpr();

            if (ipath.isGeometry() || epath == null) {
                break;
            }

            if (epath.matchesIndex(theIndex, ipath.getPosition())) {

                if (epath.theIsMultiValue) {
                    break;
                }

                ++e;
                continue;
            }

            /* The currest expr does not match with the current ipath. Check
             * whether we have an equality predicate on the current ipath. If
             * so, skip the current ipath. */
            ArrayList<PredInfo> startstopPIs = theStartStopPreds.get(i);
            if (startstopPIs != null && startstopPIs.size() == 1) {
                PredInfo pi = startstopPIs.get(0);
                if (pi.isEq() && !ipath.isMultiKey()) {
                    continue;
                }
            }

            break;
        }

        if (numExprs > 0 && e == numExprs) {
            theSFW.addSortingIndex(theIndex);
            theTableExpr.setDirection(direction);
            return;
        }

        if (i != indexPaths.size()) {
            return;
        }

        /* Check if the remaining sort exprs are primary-key columns
         * (which exist in the index as well). */
        int pkPos = 0;
        for (; pkPos < numPkCols && e < numExprs; ++e, ++pkPos) {

            Expr expr = (hasGroupBy ?
                         theSFW.getFieldExpr(e) :
                         theSFW.getSortExpr(e));

            if (!ExprUtils.
                isPrimKeyColumnRef(theTableExpr, theTable, pkPos, expr)) {
                break;
            }
        }

        if (e == numExprs) {
            theSFW.addSortingIndex(theIndex);
            theTableExpr.setDirection(direction);
            return;
        }

        if (theTrace >= 2) {
            trace("checkIsSortingIndex: Checking for descendant pk cols. " +
                  "exprPos = " + e + " pkPos = " + pkPos);
        }

        if (numDescendants > 0 && !desc) {

            int numAncestorPkCols = pkPos+1;

            for (int t = numAncestors + 1; t < numTables; ++t) {

                TableImpl descendant = theTableExpr.getTable(t);
                numPkCols = descendant.getPrimaryKeySize();

                for (pkPos = 0;
                     e < numExprs && pkPos < numPkCols;
                     ++e, ++pkPos) {
                    Expr expr = (hasGroupBy ?
                                 theSFW.getFieldExpr(e) :
                                 theSFW.getSortExpr(e));
                    if (!ExprUtils.isPrimKeyColumnRef(theTableExpr,
                                                      descendant,
                                                      pkPos,
                                                      expr)) {
                        if (pkPos < numAncestorPkCols) {
                            --e;
                            continue;
                        }
                        return;
                    }
                }

                if (e == numExprs) {
                    theSFW.addSortingIndex(theIndex);
                    theTableExpr.setDirection(direction);
                    return;
                }

                numAncestorPkCols = numPkCols;
            }
        }
    }

    private boolean checkIsUnnestingIndex() {

        for (int i = 1; i < theSFW.getNumFroms(); ++i) {

            FromClause fc = theSFW.getFromClause(i);
            ExprVar var = fc.getVar();

            if (var.isUnnestingIndex(theIndex)) {
                return true;
            }
        }

        return false;
    }

    private static FieldValueImpl createPlaceHolderValue(FieldDefImpl type) {

        switch (type.getType()) {
        case ANY_ATOMIC:
        case INTEGER:
            return FieldDefImpl.Constants.integerDef.createInteger(0);
        case LONG:
            return FieldDefImpl.Constants.longDef.createLong(0);
        case FLOAT:
            return FieldDefImpl.Constants.floatDef.createFloat(0.0F);
        case DOUBLE:
            return FieldDefImpl.Constants.doubleDef.createDouble(0.0);
        case NUMBER:
            return FieldDefImpl.Constants.numberDef.createNumber(0);
        case STRING:
            if (type.isUUIDString()) {
                return StringValueImpl.MINUUID;
            }
            return FieldDefImpl.Constants.stringDef.createString("");
        case ENUM:
            return ((EnumDefImpl)type).createEnum(1);
        case TIMESTAMP:
            return ((TimestampDefImpl)type).createTimestamp("0000-00-00T00:00:00");
        default:
            throw new QueryStateException(
                "Unexpected type for index key: " + type);
        }
    }

    /**
     * Return the number of preds in the WHERE clause of the SFW expr
     */
    private int getNumPreds() {

        if (theSFW == null) {
            return 0;
        }

        Expr whereExpr = theSFW.getWhereExpr();

        if (whereExpr == null) {
            return 0;
        }

        Function andOp = whereExpr.getFunction(FuncCode.OP_AND);

        if (andOp != null) {
            return whereExpr.getNumChildren();
        }

        return 1;
    }
}
