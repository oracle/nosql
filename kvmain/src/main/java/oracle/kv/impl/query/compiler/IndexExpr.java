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
import java.util.Collections;
import java.util.function.Supplier;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TablePath;
import oracle.kv.impl.api.table.TablePath.StepKind;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr.ConstKind;
import oracle.kv.impl.query.compiler.Expr.ExprKind;
import oracle.kv.impl.query.compiler.Expr.UpdateKind;
import oracle.kv.impl.query.compiler.ExprMapFilter.FilterKind;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.Index;

/**
 * A class that represents a query expr in a form that is used to match the
 * expr with the definition of an index field.
 *
 * An instance of IndexExpr is created only for query exprs that may be
 * matchable with an index field expr. Today this includes path exprs, FOR
 * variables whose domain expr is a path expr, and the CTX_ELEM and CTX_KEY
 * variables. The IndexExpr instance is referenced from the associated Expr
 * instance. For path exprs, an IndexExpr is created only for the last step.
 * However, such an instance will not be created if the path expr is for
 * sure not matchable with any index path.
 *
 * For example, consider an index for the following 2 paths: a.b[].c and a.b[].d
 * and a predicate like "exists f.a.b[$element.c = 3].d".
 * For the .d field step expr, the following IndexExpr is created: a.b[].d
 * And for the .c field step expr, the following IndexExpr is created: a.b[].c
 *
 * Assuming a query path expr QP that does not contain any filtering/slicing
 * steps, a "match" is established between QP and an index path IP in the
 * following cases.
 *
 * 1. QP and IP match if their steps are identical.
 *
 * 2. [] steps that are not the last step of a path expr are noop steps. So,
 * QP and IP match if after removing any non-last [] steps, their remaining
 * steps are identical. For example if QP = a.b.c and IP = a.b[].c, QP and
 * IP match.
 *
 * 3. A "MapBoth" match. In this case, the index is a "MapBoth" index.
 * QP selects the value associated with a specific map key, and IP selects
 * all the values of the same map. For example QP = foo.map.someKey.bar and
 * IP = foo.map.values().bar. A MapBoth match results to two preds being
 * pushed down to the map index, as described in the header javadoc of the
 * IndexAnalyzer class.
 *
 * Handling slicing and filtering steps:
 *
 * QP may contain a slice or a filtering step that may project-out
 * elements from the input arrays/maps. Such a slice/filter step is
 * "partially" matched with a [] step in IP, at the same position. For
 * example, the path expr foo.array[1:5].f matches partially with index
 * path foo.array[].f. A partially matched pred is treated by leaving the
 * original pred in the query and pushing to the index a pred that is the
 * same as the original but with the expr(s) inside the [] removed.
 *
 *
 * theExpr:
 *
 * theTable:
 *
 * theSteps:
 * A list of StepInfos, reflecting the steps of the path without any of the
 * conditions and/or the boundary expressions that may exist inside filtering
 * of slicing steps. This representation is the same as the one used by
 * IndexImpl.IndexField instances (see TablePath.steps), and as a result, it
 * is used for matching IndexExpr instances with IndexField instances.
 *
 * theFilteringPreds:
 * The condition and/or the boundary expressions that may exist inside
 * filtering or slicing steps of this path expr.
 *
 * theDoesSliciing:
 * Set to true if QP contains an array slicing step. In this case, the match
 * between QP and IP is partial, and as a result, the pred where QP appears
 * in must be retained.
 *
 * theIsJson:
 * Whether the path crosses into json data
 *
 * theIsDirect:
 * True if the path expr goes all the way down to the table, without
 * crossing any FOR or CTX variables.
 *
 * theIsUnnested:
 * Whether the input to QP is a FROM var, whose domain expr is multi-valued,
 * in which case it is matched partially with IP. Such a pred does not apply
 * to a table directly, but to an unnested version of the table.
 *
 * theOuterCtxVar:
 * The outer-most (left-most) variable appearing in the path expr when expanded
 * all the way back to the row var.
 *
 * theOuterCtxVarPos:
 * The position, within theSteps, of the multikey step that defines the scope for
 * theOuterCtxVar.
 *
 * theIsMultiValue:
 * Set to true when the expr may return more than 1 items. This is basically the
 * same as Expr.isMultiValued(). In the case of json data, Expr.isMultiValued()
 * will conservatively return true most of the time, due to the lack of type
 * info. However, by matching the expr to an index path here, we may be able to
 * deduce that the expr will return at most 1 item and set theIsMultiValue to
 * false.
 *
 * theIndexMatches:
 *
 * thePrimKeyPos:
 *
 * theCurrentMatch:
 *
 * theColumnPos:
 *
 * theIsMRCounterPath:
 *
 * Data Members of IndexMatch
 * --------------------------
 *
 * theIndex:
 *
 * theFieldPos:
 * The ordinal number of the index path that matches with "this" query
 * path. -1 if no match actually exists.
 *
 * theMapBothKey:
 * If a "MapBoth" match is made between this path expr and an ipath,
 * theMapBothKey is the specific map key that gets matched with the values()
 * step in the ipath.
 *
 * theRelativeCtxVarPos:
 *
 * theJsonDeclaredType:
 * If the query path matches with a type-constrained json index path,
 * theJsonDeclaredType is set to the type of that index path.
 */
class IndexExpr {

    static class StepInfo {

        String theName;

        StepKind theKind;

        Expr theExpr;

        ExprVar theUnnestVar;

        StepInfo(String name, StepKind kind, Expr expr) {
            theName = name;
            theKind = kind;
            theExpr = expr;
        }

        StepInfo(String name, StepKind kind, Expr expr, ExprVar unnestVar) {
            theName = name;
            theKind = kind;
            theExpr = expr;
            theUnnestVar = unnestVar;
        }
    }

    static class IndexMatch {

        TableImpl theTable;

        IndexImpl theIndex;

        int theFieldPos;

        boolean theDoesSlicing;

        String theMapBothKey;

        int theRelativeCtxVarPos;

        boolean theIsUnnested;

        IndexMatch(
            TableImpl table,
            IndexImpl index,
            int ipos,
            boolean doesSlicing,
            String mapKey,
            int relCtxVarPos,
            boolean isUnnested) {
            theTable = table;
            theIndex = index;
            theFieldPos = ipos;
            theDoesSlicing = doesSlicing;
            theMapBothKey = mapKey;
            theRelativeCtxVarPos = relCtxVarPos;
            theIsUnnested = isUnnested;
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  theIndex : " +
                      (theIndex == null ? "Primary" : theIndex.getName()));
            sb.append("\n  theFieldPos : " + theFieldPos);
            sb.append("\n  theRelativeCtxVarPos : " + theRelativeCtxVarPos);
            sb.append("\n  theIsUnnested : " + theIsUnnested);
            sb.append("\n  theMapBothKey : " + theMapBothKey);
            sb.append("\n  theDoesSlicing : " + theDoesSlicing);
            sb.append("\n}");

            return sb.toString();
        }
    }

    static int theTraceLevel = 0;

    final Expr theExpr;

    final Function theFunction;

    ExprBaseTable theTableExpr;

    TableImpl theTable;

    final List<StepInfo> theSteps;

    List<Expr> theFilteringPreds;

    boolean theDoesSlicing;

    boolean theIsJson;

    private FieldDefImpl theJsonDeclaredType;

    boolean theIsDirect = true;

    boolean theIsGeo;

    boolean theIsMultiValue;

    ExprVar theOuterCtxVar;

    int theOuterCtxVarPos = -1;

    private int theInnerVarPos = -1;

    ArrayList<ExprVar> theCtxVars = new ArrayList<ExprVar>(4);

    ArrayList<ExprVar> theForVars = new ArrayList<ExprVar>(4);

    private List<IndexMatch> theIndexMatches;

    int thePrimKeyPos = -1;

    private int theCurrentMatch = -1;

    private int theColumnPos = -1;

    private boolean theIsMRCounterPath;

    IndexExpr(Expr expr) {
        theExpr = expr;
        theFunction = expr.getFunction(null);
        theSteps = new ArrayList<StepInfo>();
    }

    int numSteps() {
        return theSteps.size();
    }

    StepKind getStepKind(int i) {
        return theSteps.get(i).theKind;
    }

    String getStepName(int i) {
        return theSteps.get(i).theName;
    }

    String getLastStepName() {
        return theSteps.get(theSteps.size() - 1).theName;
    }

    List<Expr> getFilteringPreds() {
        return theFilteringPreds;
    }

    boolean hasSameCtxVars(IndexExpr other) {

        int minCtxVars = (theCtxVars.size() <= other.theCtxVars.size() ?
                          theCtxVars.size() :
                          other.theCtxVars.size());

        for (int i = 0; i < minCtxVars; ++i) {
            if (theCtxVars.get(i) != other.theCtxVars.get(i)) {
                return false;
            }
        }

        return true;
    }

    boolean hasUnnestingVar(IndexImpl unnestedIndex) {
        for (ExprVar var : theForVars) {
            if (var.isUnnestingIndex(unnestedIndex)) {
                return true;
            }
        }
        return false;
    }

    boolean matchesIndex(TableImpl table, IndexImpl index) {

        theCurrentMatch = -1;

        if (theIndexMatches == null) {
            return false;
        }

        for (int i = 0; i < theIndexMatches.size(); ++i) {
            IndexMatch m = theIndexMatches.get(i);
            if (m.theTable.getId() == table.getId() && m.theIndex == index) {
                theCurrentMatch = i;
                return true;
            }
        }

        return false;
    }

    boolean isMatched() {
        return theIndexMatches != null && !theIndexMatches.isEmpty();
    }

    boolean matchesIndex(IndexImpl index, int ipos) {

        theCurrentMatch = -1;

        if (theIndexMatches == null) {
            return false;
        }

        for (int i = 0; i < theIndexMatches.size(); ++i) {
            IndexMatch m = theIndexMatches.get(i);
            if (m.theIndex == index && m.theFieldPos == ipos) {
                theCurrentMatch = i;
                return true;
            }
        }

        return false;
    }

    IndexField getMatchingField() {

        IndexMatch match = theIndexMatches.get(theCurrentMatch);

        if (match.theIndex == null ||
            match.theFieldPos >= match.theIndex.numFields()) {
            // match with the primary index or a primary-key column
            // in a secondary index
            return null;
        }
        return match.theIndex.getIndexPath(match.theFieldPos);
    }

    int getPathPos() {
        return theIndexMatches.get(theCurrentMatch).theFieldPos;
    }

    boolean doesSlicing() {

        if (theIndexMatches == null || theCurrentMatch < 0) {
            return theDoesSlicing;
        }

        return theIndexMatches.get(theCurrentMatch).theDoesSlicing;
    }

    String getMapBothKey() {

        if (theIndexMatches == null || theCurrentMatch < 0) {
            return null;
        }

        return theIndexMatches.get(theCurrentMatch).theMapBothKey;
    }

    /*
     * This method is called during IndexAnalyzer.apply(), in which case
     * theCurrentMatch may not be the match for the index being applied. So
     * we pass the index as a param and call matchesIndex again to find the
     * correct match.
     */
    String getMapBothKey(TableImpl table, IndexImpl index) {

        if (index == null) {
            return null;
        }

        if (matchesIndex(table, index)) {
            return theIndexMatches.get(theCurrentMatch).theMapBothKey;
        }

        throw new QueryStateException(
            "No match found for index " + index.getName());
    }

    int getRelativeCtxVarPos() {
        return theIndexMatches.get(theCurrentMatch).theRelativeCtxVarPos;
    }

    int getRelativeCtxVarPos(TableImpl table, IndexImpl index) {

        if (matchesIndex(table, index)) {
            return theIndexMatches.get(theCurrentMatch).theRelativeCtxVarPos;
        }

        return 0;
    }

    boolean isUnnested() {
        return theIndexMatches.get(theCurrentMatch).theIsUnnested;
    }

    boolean isUnnested(TableImpl table, IndexImpl index) {

        if (matchesIndex(table, index)) {
            return theIndexMatches.get(theCurrentMatch).theIsUnnested;
        }

        return false;
    }

    FieldDefImpl getJsonDeclaredType() {
        return theJsonDeclaredType;
    }

    void setJsonDeclaredType(FieldDefImpl def) {

        if (theJsonDeclaredType != null &&
            !theJsonDeclaredType.equals(def)) {

            throw new QueryStateException(
               "The path " + getPathName() + " is declared with different " +
               "types in different indexes");
        }
    }

    boolean isMultiKey() {
        IndexMatch m = theIndexMatches.get(theCurrentMatch);

        if (m.theIndex == null) {
            return false;
        }

        if (m.theFieldPos >= m.theIndex.numFields()) {
            // it's a prik-key column ref
            return false;
        }

        return m.theIndex.getIndexPath(m.theFieldPos).isMultiKey();
    }

    boolean isMRCounterPath() {
        return theIsMRCounterPath;
    }

    private void reverseSteps() {
        Collections.reverse(theSteps);
        Collections.reverse(theForVars);
        if (theOuterCtxVarPos >= 0) {
            theOuterCtxVarPos = theSteps.size() - theOuterCtxVarPos - 1;
        }
        if (theInnerVarPos >= 0) {
            theInnerVarPos = theSteps.size() - theInnerVarPos - 1;
        }
    }

    void add(String name, StepKind kind, Expr expr) {
        theSteps.add(new StepInfo(name, kind, expr));
    }

    private void add(String name, StepKind kind, Expr expr, ExprVar unnestVar) {
        theSteps.add(new StepInfo(name, kind, expr, unnestVar));
    }

    void remove() {
        theSteps.remove(theSteps.size() - 1);
    }

    private void addFilteringPred(Expr cond) {

        if (cond == null || theOuterCtxVarPos >= 0) {
            return;
        }

        if (theFilteringPreds == null) {
            theFilteringPreds = new ArrayList<Expr>();
        }

        Function andOp = cond.getFunction(FuncCode.OP_AND);

        if (andOp != null) {
            theFilteringPreds.addAll(((ExprFuncCall)cond).getArgs());
        } else {
            theFilteringPreds.add(cond);
        }
    }

    private void addIndexMatch(
        IndexImpl index,
        int ipos,
        boolean doesSlicing,
        String mapKey,
        int relCtxVarPos,
        boolean isUnnested) {

        if (theIndexMatches == null) {
            theIndexMatches = new ArrayList<IndexMatch>(8);
        }

        IndexMatch match = new IndexMatch(theTable,
                                          index,
                                          ipos,
                                          doesSlicing,
                                          mapKey,
                                          relCtxVarPos,
                                          isUnnested);
        theIndexMatches.add(match);
        theCurrentMatch = theIndexMatches.size() - 1;

        trace(1, () -> "added IndexMatch: " + match + "\n");
    }

    private void addIndexMatch(IndexMatch match) {

        if (theIndexMatches == null) {
            theIndexMatches = new ArrayList<IndexMatch>(8);
        }

        theIndexMatches.add(match);
        theCurrentMatch = theIndexMatches.size() - 1;

        trace(1, () -> "added IndexMatch: " + match + "\n");
    }

    static IndexExpr create(Expr expr) {

        IndexExpr epath = new IndexExpr(expr);

        epath.theIsMultiValue = expr.isMultiValued();

        while (expr != null) {

            if (!epath.theIsJson &&
                (expr.getType().isAnyJson() ||
                 expr.getType().isAnyJsonAtomic())) {
                epath.theIsJson = true;
            }

            switch (expr.getKind()) {

            case FUNC_CALL:
                ExprFuncCall fncall = (ExprFuncCall)expr;

                if (fncall.getFuncCode() == FuncCode.FN_ROW_METADATA) {
                    epath.add(FuncRowMetadata.COL_NAME, StepKind.REC_FIELD, expr);
                } else {
                    if (expr != epath.theExpr ||
                        !epath.theFunction.isIndexable()) {
                        return null;
                    }

                    for (int i = 1; i < fncall.getNumArgs(); ++i) {
                        Expr arg = fncall.getArg(i);
                        if (!ConstKind.isCompileConst(arg)) {
                            return null;
                        }
                    }
                }

                expr = fncall.getArg(0);
                break;
            case FIELD_STEP: {
                ExprFieldStep stepExpr = (ExprFieldStep)expr;
                String fieldName = stepExpr.getFieldName();
                ExprType inType = stepExpr.getInput().getType();

                if (fieldName == null || inType.isAtomic()) {
                    return null;
                }

                if (inType.isArray()) {

                    FieldDefImpl elemDef =
                        ((ArrayDefImpl)inType.getDef()).getElement();

                    while (elemDef.isArray()) {
                        elemDef = ((ArrayDefImpl)elemDef).getElement();
                    }

                    if (elemDef.isAtomic()) {
                        return null;
                    }

                    if (elemDef.isRecord()) {
                        epath.add(fieldName, StepKind.REC_FIELD, expr);
                    } else {
                        epath.add(fieldName, StepKind.MAP_FIELD, expr);
                    }

                } else if (inType.isRecord()) {
                    epath.add(fieldName, StepKind.REC_FIELD, expr);
                } else {
                    epath.add(fieldName, StepKind.MAP_FIELD, expr);
                }

                expr = expr.getInput();
                break;
            }
            case MAP_FILTER: {
                ExprMapFilter stepExpr = (ExprMapFilter)expr;
                ExprType inType = expr.getInput().getType();

                if (inType.isRecord() || inType.isAtomic()) {
                    return null;
                }

                if (stepExpr.getFilterKind() == FilterKind.KEYS) {
                    epath.add(TableImpl.KEYS, StepKind.KEYS, expr);
                } else {
                    epath.add(TableImpl.VALUES, StepKind.VALUES, expr);
                }

                epath.addFilteringPred(stepExpr.getPredExpr());

                expr = expr.getInput();
                break;
            }
            case ARRAY_SLICE:
            case ARRAY_FILTER: {
                epath.add(TableImpl.BRACKETS, StepKind.BRACKETS, expr);

                if (expr.getKind() == ExprKind.ARRAY_SLICE) {
                    ExprArraySlice step = (ExprArraySlice)expr;
                    if (step.hasBounds()) {
                        epath.theDoesSlicing = true;
                    }
                } else {
                    ExprArrayFilter step = (ExprArrayFilter)expr;
                    Expr pred = step.getPredExpr();
                    if (pred != null) {
                        if (pred.getType().getDef().isBoolean()) {
                            epath.addFilteringPred(pred);
                        } else {
                            /*
                             * We conservatively assume that the pred expr may
                             * return numeric results, in which case it is
                             * actually a slicing step.
                             */
                            epath.theDoesSlicing = true;
                        }
                    }
                }

                expr = expr.getInput();
                break;
            }
            case VAR: {
                ExprVar varExpr = (ExprVar)expr;

                switch (varExpr.getVarKind()) {
                case FOR: {
                    expr = varExpr.getDomainExpr();

                    if (expr.getKind() != ExprKind.BASE_TABLE) {
                        epath.theIsDirect = false;
                        epath.theForVars.add(varExpr);
                        if (epath.theInnerVarPos < 0) {
                            epath.theInnerVarPos = epath.theSteps.size();
                        }
                        epath.trace(1, () -> "Added ForVar " + varExpr.getName() +
                                    " theInnerVarPos = " + epath.theInnerVarPos);
                    } else {
                        epath.reverseSteps();
                        epath.theTableExpr = (ExprBaseTable)expr;
                        epath.theTable = varExpr.getTable();
                        if (varExpr.getIndex() == null &&
                            !epath.theSteps.isEmpty()) {
                            String colName = epath.theSteps.get(0).theName;
                            if (!epath.theTable.isJsonCollection() &&
                                !colName.equals(FuncRowMetadata.COL_NAME)) {
                                StepKind skind = epath.theSteps.get(0).theKind;
                                if (skind == StepKind.REC_FIELD) {
                                    epath.theColumnPos =
                                        epath.theTable.getRowDef().
                                        getFieldPos(colName);
                                }
                            }
                        }
                        expr = null; // terminate the while loop
                        break;
                    }

                    break;
                }
                case CTX_ELEM: {
                    expr = varExpr.getCtxExpr();
                    epath.theCtxVars.add(0, varExpr);
                    epath.theIsDirect = false;
                    epath.theOuterCtxVar = varExpr;
                    epath.theOuterCtxVarPos = epath.theSteps.size();
                    if (epath.theInnerVarPos < 0) {
                        epath.theInnerVarPos = epath.theSteps.size();
                    }

                    if (expr.getKind() == ExprKind.ARRAY_FILTER) {
                        epath.add(TableImpl.BRACKETS, StepKind.BRACKETS, expr);
                        expr = expr.getInput();
                        break;
                    } else if (expr.getKind() == ExprKind.MAP_FILTER) {
                        epath.add(TableImpl.VALUES, StepKind.VALUES, expr);
                        expr = expr.getInput();
                        break;
                    }

                    return null;
                }
                case CTX_KEY: {
                    expr = varExpr.getCtxExpr();
                    epath.theCtxVars.add(0, varExpr);
                    epath.theIsDirect = false;
                    epath.theOuterCtxVar = varExpr;
                    epath.theOuterCtxVarPos = epath.theSteps.size();
                    if (epath.theInnerVarPos < 0) {
                        epath.theInnerVarPos = epath.theSteps.size();
                    }

                    assert(expr.getKind() == ExprKind.MAP_FILTER);
                    epath.add(TableImpl.KEYS, StepKind.KEYS, expr);
                    expr = expr.getInput();
                    break;
                }
                default: {
                    return null;
                }
                }

                break;
            }
            case BASE_TABLE: {
                throw new QueryStateException(
                   "Reached base table expression for path " +
                   epath.getPathName());
            }
            default:
                return null;
            }
        }

        // Check whether it is a ref to a prim-key column
        int pkPos = -1;
        if (epath.theFunction == null && epath.numSteps() == 1) {

            pkPos = epath.theTable.findKeyComponent(epath.getLastStepName());

            if (pkPos >= 0) {
                epath.thePrimKeyPos = pkPos;
                epath.addIndexMatch(null, pkPos, false, null, 0, false);
            }
        }

        Map<String, Index> indexes = epath.theTable.getIndexes();

        for (Map.Entry<String, Index> entry : indexes.entrySet()) {

            boolean foundMatch = false;
            IndexImpl index = (IndexImpl)entry.getValue();
            List<IndexField> indexPaths = index.getIndexFields();
            int numFields = indexPaths.size();

            for (IndexField ipath : indexPaths) {

                epath.trace(1, () -> "Matching with ipath " +
                            ipath.getPathName());

                /* Match the path exprs, not taking functions into account */
                IndexMatch match = epath.matchToIndexPath(index, ipath);

                if (match == null) {
                    continue;
                }

                epath.trace(1, () -> "epath matched with ipath " +
                            ipath.getPathName() + " isMultiValued = " +
                            epath.theIsMultiValue + "\n");

                if ((ipath.isGeometry() || ipath.isPoint()) &&
                    epath.theFunction == null) {
                    epath.addIndexMatch(match);
                    epath.theIsGeo = true;
                    break;
                }

                /* Adjust the type of the query path expr, if it matched
                 * with a json index path. Unfortunately, if the json path
                 * has a precise type, we cannot use it because json null
                 * is also allowed as key value */
                FieldDefImpl itype = ipath.getDeclaredType();

                if (itype != null && epath.theFunction == null) {
                    Quantifier quant = (epath.theIsMultiValue ?
                                        Quantifier.STAR :
                                        Quantifier.QSTN);
                    ExprType t = TypeManager.createType(
                        TypeManager.ANY_JATOMIC_ONE(), quant);
                    epath.theExpr.setType(t);
                }

                /* Check that the functions match */
                if (epath.theFunction != ipath.getFunction()) {
                    continue;
                }

                ExprFuncCall ifunc = ipath.getFunctionalFieldExpr();

                if (ifunc != null) {

                    ExprFuncCall efunc = (ExprFuncCall)epath.theExpr;

                    if (efunc.getNumArgs() != ifunc.getNumArgs()) {
                        continue;
                    }

                    boolean argsMatched = true;
                    for (int i = 1; i < efunc.getNumArgs(); ++i) {

                        if (!ExprUtils.matchExprs(efunc.getArg(i),
                                                  ifunc.getArg(i))) {
                            argsMatched = false;
                            break;
                        }
                    }

                    if (!argsMatched) {
                        continue;
                    }

                    if (itype != null) {
                        epath.theJsonDeclaredType = efunc.getFunction().
                                                    getRetType(itype).getDef();
                    }
                }

                /* Register the match */
                foundMatch = true;
                epath.addIndexMatch(match);
                break;
            }

            if (!foundMatch && pkPos >= 0) {
                epath.addIndexMatch(index, numFields + pkPos, false, null,
                                    0, false);
                continue;
            }

        }

        if (epath.theTable.hasJsonCollectionMRCounters()) {
            String path = epath.getPathName();
            if (epath.theTable.getJsonCollectionMRCounters().
                containsKey(path)) {
                epath.theIsMRCounterPath = true;
            }
        } else {
            if (epath.theTable != null &&
                epath.theTable.hasSchemaMRCounters() &&
                epath.theColumnPos >= 0) {

                List<TablePath> mrcounterPaths =
                    epath.theTable.getSchemaMRCounterPaths(epath.theColumnPos);

                if (mrcounterPaths != null) {
                    for (TablePath ipath : mrcounterPaths) {
                        if (epath.matchToMRCounterPath(ipath)) {
                            epath.theIsMRCounterPath = true;
                            break;
                        }
                    }
                }
            }
        }

        return epath;
    }

    private IndexMatch matchToIndexPath(IndexImpl index, IndexField ipath) {

        IndexExpr epath = this;

        if (ipath.isMultiKey()) {
            for (ExprVar var : epath.theForVars) {
                if (!var.isUnnestingIndex(index) &&
                    var.getDomainExpr().getIndexExpr().theIsMultiValue) {

                    trace(1, () -> "references non-unnesting variable " +
                          var.getName() + " whose domain expr is multivalued\n");
                    return null;
                }
            }
        }

        //System.out.println("Matching epath " + getPathName() +
        //                   "\nwith     ipath " + ipath);

        String mapKey = null;
        int inumSteps = ipath.numSteps();
        int enumSteps = numSteps();
        boolean doesSlicing = epath.theDoesSlicing;
        int relativeCtxVarPos = -2;
        boolean isMultiValue = false;
        int ii = 0;
        int ie = 0;
        for (; ii < inumSteps && ie < enumSteps;) {

            TablePath.StepInfo isi = ipath.getStepInfo(ii);
            StepInfo esi = epath.theSteps.get(ie);
            String istep = isi.getStep();
            String estep = esi.theName;
            StepKind ikind = isi.getKind();
            StepKind ekind = esi.theKind;

            if (ipath.isMultiKey()) {

                if (ii >= ipath.getMultiKeyStepPos()) {
                    if (ie == theOuterCtxVarPos) {
                        relativeCtxVarPos = 1;
                    }
                } else {
                    if (ie == theOuterCtxVarPos) {
                        relativeCtxVarPos = -1;
                    }
                }
            }

            boolean eq = (ikind == ekind &&
                          (ipath.isMapKeyStep(ii) ?
                           istep.equals(estep) :
                           istep.equalsIgnoreCase(estep)));

            if (eq) {
                if (theInnerVarPos < ie &&
                    (ikind == StepKind.BRACKETS ||
                     ikind == StepKind.VALUES ||
                     ikind == StepKind.KEYS)) {

                    isMultiValue = true;
                    if (theFunction != null) {
                        return null;
                    }
                }

                ++ii;
                ++ie;
                continue;
            }

            if (ikind == StepKind.BRACKETS) {

                if (ii == inumSteps -1) {
                    return null;
                }

                if (theInnerVarPos < ie) {
                    isMultiValue = true;
                    if (theFunction != null) {
                        return null;
                    }
                }

                ++ii;
                continue;
            }

            if (ekind == StepKind.BRACKETS) {

                if (ie == enumSteps - 1) {
                    return null;
                }

                ++ie;
                continue;
            }

            /*
             * We have a map-both index and a values() ipath, the matching
             * between epath and ipath has to consider the case where the
             * query pred is a MapBoth pred. i.e., we have to consider the
             * case where the epath is a.b.c.d and the ipath is a.b.values().d
             */
            if (ikind == StepKind.VALUES && ekind == StepKind.MAP_FIELD) {
                if (isi.keysPos() >= 0) {
                    mapKey = estep;
                } else {
                    doesSlicing = true;
                }
                ++ii;
                ++ie;
                continue;
            }

            return null;
        }

        if (ii == inumSteps &&
            (ie == enumSteps ||
             // see json_idx/q/aq17.q for the reason for this case:
             // query path = f.info.address.phones.areacode[]
             // index path = info.address.phones[].areacode
             (ie == enumSteps - 1 &&
              epath.getStepKind(ie) == StepKind.BRACKETS))) {

            boolean isUnnested = false;

            if (epath.theExpr.getKind() == ExprKind.VAR) {
                ExprVar var = (ExprVar)epath.theExpr;
                if (var.isUnnestingIndex(index)) {
                    isUnnested = true;
                }
            } else if (!epath.theForVars.isEmpty()) {
                ExprVar forVar = epath.theForVars.get(epath.theForVars.size() - 1);
                if (forVar.isUnnestingIndex(index)) {
                    isUnnested = true;
                }
            }

            if (!ipath.isMultiKey() || !isMultiValue) {
                theIsMultiValue = false;
            }

            if (theFunction == null) {
                /*
                 * For function index, the theJsonDeclaredType is set after
                 * checking if arguments match.
                 */
                theJsonDeclaredType = ipath.getDeclaredType();
            }

            return new IndexMatch(theTable,
                                  index,
                                  ipath.getPosition(),
                                  doesSlicing,
                                  mapKey,
                                  relativeCtxVarPos,
                                  isUnnested);
        }

        return null;
    }

    private boolean matchToMRCounterPath(TablePath ipath) {

        IndexExpr epath = this;

        //System.out.println("Matching epath " + getPathName() +
        //                   " with mrcounter path " + ipath);

        int inumSteps = ipath.numSteps();
        int enumSteps = numSteps();
        int ii = 0;
        int ie = 0;

        for (; ii < inumSteps && ie < enumSteps;) {

            TablePath.StepInfo isi = ipath.getStepInfo(ii);
            StepInfo esi = epath.theSteps.get(ie);
            String istep = isi.getStep();
            String estep = esi.theName;
            StepKind ikind = isi.getKind();
            StepKind ekind = esi.theKind;

            boolean eq = (ikind == ekind &&
                          (ipath.isMapKeyStep(ii) ?
                           istep.equals(estep) :
                           istep.equalsIgnoreCase(estep)));

            if (eq) {
                ++ii;
                ++ie;
                continue;
            }

            if (ekind == StepKind.BRACKETS) {

                if (ie == enumSteps - 1) {
                    return false;
                }

                ++ie;
                continue;
            }

            return false;
        }

        if (ii == inumSteps &&
            (ie == enumSteps ||
             // see json_idx/q/aq17.q for the reason for this case:
             // query path = f.info.address.phones.areacode[]
             // index path = info.address.phones[].areacode
             (ie == enumSteps - 1 &&
              epath.getStepKind(ie) == StepKind.BRACKETS))) {

            //System.out.println("epath " + getPathName() +
            //                   " matched with mrcounter path " + ipath);

            return true;
        }

        return false;
    }

    boolean matchUpdateTargetToIndexPath(
        IndexField ipath,
        UpdateKind updKind) {

        IndexExpr epath = this;

        //System.out.println("Matching epath " + getPathName() +
        //                   "\nwith     ipath " + ipath);

        int inumSteps = ipath.numSteps();
        int enumSteps = numSteps();
        int ii = 0;
        int ie = 0;

        for (; ii < inumSteps && ie < enumSteps;) {

            TablePath.StepInfo isi = ipath.getStepInfo(ii);
            StepInfo esi = epath.theSteps.get(ie);
            String istep = isi.getStep();
            String estep = esi.theName;
            StepKind ikind = isi.getKind();
            StepKind ekind = esi.theKind;

            boolean eq = (ikind == ekind &&
                          (ipath.isMapKeyStep(ii) ?
                           istep.equals(estep) :
                           istep.equalsIgnoreCase(estep)));
            if (eq) {
                ++ii;
                ++ie;
                continue;
            }

            switch (ikind) {
            case BRACKETS:
                if (ii == inumSteps -1) {
                    /* We have reached an array of atomic values. In all cases,
                     * the estep will return empty and no update will be done. */
                    return false;
                }

                /* In all cases, the estep will be applied to each array element */
                ++ii;
                continue;

            case VALUES:
                /* We have reached a map or atomic value */
                assert(ekind != StepKind.REC_FIELD);

                if (ekind == StepKind.BRACKETS) {
                    ++ie;
                    continue;
                } else if (ekind == StepKind.MAP_FIELD) {
                    ++ii;
                    ++ie;
                    continue;
                } else {
                    assert(ekind == StepKind.KEYS);
                    if (updKind == UpdateKind.REMOVE) {
                        return true;
                    }
                    return false;
                }
            case KEYS:
                /* We have reached a map or atomic value */
                assert(ekind != StepKind.REC_FIELD);

                if (ekind == StepKind.BRACKETS) {
                    ++ie;
                    continue;
                } else if (ekind == StepKind.MAP_FIELD) {
                    if (updKind == UpdateKind.PUT ||
                        updKind == UpdateKind.REMOVE) {
                        ++ii;
                        ++ie;
                        continue;
                    }
                    return false;
                } else {
                    assert(ekind == StepKind.VALUES);
                    return false;
                }
            case MAP_FIELD:
                assert(ekind != StepKind.REC_FIELD);

                if (ekind == StepKind.BRACKETS) {
                    ++ie;
                    continue;
                } else if (ekind == StepKind.VALUES) {
                    ++ii;
                    ++ie;
                    continue;
                } else if (ekind == StepKind.KEYS) {
                    if (updKind == UpdateKind.REMOVE) {
                        return true;
                    }
                    return false;
                } else {
                    assert(ekind == StepKind.MAP_FIELD);
                    return false;
                }
            case REC_FIELD:
                if (ekind == StepKind.MAP_FIELD) {
                    /* This can happen when ExprUpdateRow adds to the epath
                     * a MAP step that corresponds to a key new value expr
                     * of a put clause. */
                    if (istep.equalsIgnoreCase(estep)) {
                        ++ii;
                        ++ie;
                        continue;
                    }
                    return false;
                } else if (ekind == StepKind.BRACKETS) {
                    ++ie;
                    continue;
                } else if (ekind == StepKind.VALUES) {
                    ++ii;
                    ++ie;
                    continue;
                } else {
                    assert(ekind == StepKind.REC_FIELD);
                    return false;
                }
            }
         }

        while(ie < enumSteps && epath.getStepKind(ie) == StepKind.BRACKETS) {
            ++ie;
        }

        if (ie == enumSteps) {
            return true;
        }

        return false;
    }

    /*
     * Called from Transaltor.enterFrom_clause(), if there is an UNNEST
     * clause. The unnestVars param is the list of all variables declared in
     * the UNNEST clause. It is called for each index on the target table
     * of the query.
     */
    static void analyzeUnnestClause(
        IndexImpl index,
        ArrayList<ExprVar> unnestVars) {

        List<IndexField> indexPaths = index.getIndexFields();
        IndexExpr lastEpath = null;

        for (int i = 0; i < unnestVars.size(); ++i) {

            ExprVar unnestVar = unnestVars.get(i);
            IndexExpr epath = unnestVar.getUnnestPath();
            if (epath == null) {
                epath = createPathForUnnestVar(unnestVar);
                unnestVar.setUnnestPath(epath);
            }

            if (i == unnestVars.size() - 1) {
                lastEpath = epath;
            }
            boolean foundMatch = false;

            for (IndexField ipath : indexPaths) {

                if (!ipath.isMultiKey()) {
                    continue;
                }

                if (ipath.getFunction() != null) {
                    continue;
                }

                if (!matchUnnestingVarToIndexPath(ipath.getMultiKeyField(),
                                                  epath)) {
                    continue;
                }
                foundMatch = true;
                break;
            }

            if (!foundMatch) {
                return;
            }
        }

        if (lastEpath != null) {
            for (StepInfo si : lastEpath.theSteps) {
                if (si.theUnnestVar != null) {
                    si.theUnnestVar.addUnnestingIndex(index);
                }
            }
        }
    }

    private static IndexExpr createPathForUnnestVar(ExprVar initUnnestVar) {

        ExprVar unnestVar = initUnnestVar;
        Expr expr = initUnnestVar.getDomainExpr();
        IndexExpr epath = new IndexExpr(unnestVar);

        while (expr != null) {

            switch (expr.getKind()) {
            case FIELD_STEP: {
                ExprFieldStep stepExpr = (ExprFieldStep)expr;
                String fieldName = stepExpr.getFieldName();
                ExprType inType = stepExpr.getInput().getType();

                if (fieldName == null || inType.isAtomic()) {
                    return null;
                }

                if (inType.isArray()) {

                    FieldDefImpl elemDef =
                        ((ArrayDefImpl)inType.getDef()).getElement();

                    while (elemDef.isArray()) {
                        elemDef = ((ArrayDefImpl)elemDef).getElement();
                    }

                    if (elemDef.isAtomic()) {
                        return null;
                    }

                    if (elemDef.isRecord()) {
                        epath.add(fieldName, StepKind.REC_FIELD, expr);
                    } else {
                        epath.add(fieldName, StepKind.MAP_FIELD, expr);
                    }

                } else if (inType.isRecord()) {
                    epath.add(fieldName, StepKind.REC_FIELD, expr);
                } else {
                    epath.add(fieldName, StepKind.MAP_FIELD, expr);
                }

                expr = expr.getInput();
                break;
            }
            case MAP_FILTER: {
                ExprType inType = expr.getInput().getType();

                if (inType.isRecord() || inType.isAtomic()) {
                    return null;
                }

                epath.add(TableImpl.VALUES, StepKind.VALUES, expr, unnestVar);
                unnestVar = null;

                expr = expr.getInput();
                break;
            }
            case ARRAY_FILTER: {
                epath.add(TableImpl.BRACKETS, StepKind.BRACKETS, expr, unnestVar);
                unnestVar = null;

                expr = expr.getInput();
                break;
            }
            case FUNC_CALL:
                ExprFuncCall fncall = (ExprFuncCall)expr;

                if (fncall.getFuncCode() == FuncCode.FN_ROW_METADATA) {
                    epath.add(FuncRowMetadata.COL_NAME, StepKind.REC_FIELD, expr);
                    expr = fncall.getArg(0);
                    break;
                }

                throw new QueryStateException(
                   "Unexpected expression in unnest path for variable " +
                   initUnnestVar.getName());
            case VAR: {
                ExprVar var = (ExprVar)expr;

                if (var.getVarKind() == VarKind.FOR) {
                    expr = var.getDomainExpr();

                    if (expr.getKind() != ExprKind.BASE_TABLE) {
                        unnestVar = var;
                    } else {
                        epath.reverseSteps();
                        epath.theTableExpr = (ExprBaseTable)expr;
                        epath.theTable = var.getTable();
                        expr = null; // terminate the while loop
                        break;
                    }
                } else {
                    return null;
                }

                break;
            }
            default: {
                throw new QueryStateException(
                   "Unexpected expression in unnest path for variable " +
                   initUnnestVar.getName());
            }
            }
        }

        return epath;
    }

    static boolean matchUnnestingVarToIndexPath(
        IndexField ipath,
        IndexExpr epath) {

        int inumSteps = ipath.numSteps();
        int enumSteps = epath.numSteps();
        int ii = 0;
        int ie = 0;
        for (; ii < inumSteps && ie < enumSteps;) {

            TablePath.StepInfo isi = ipath.getStepInfo(ii);
            IndexExpr.StepInfo esi = epath.theSteps.get(ie);
            String istep = isi.getStep();
            String estep = esi.theName;
            StepKind ikind = isi.getKind();
            StepKind ekind = esi.theKind;

            boolean eq = (ikind == ekind &&
                          (ipath.isMapKeyStep(ii) ?
                           istep.equals(estep) :
                           istep.equalsIgnoreCase(estep)));

            if (eq) {
                ++ii;
                ++ie;
                continue;
            }

            if (ikind == StepKind.BRACKETS) {
                ++ii;
                continue;
            }

            if (ekind == StepKind.BRACKETS) {

                if (ie == enumSteps - 1) {
                    break;
                }

                ++ie;
                continue;
            }

            return false;
        }

        return (ii == inumSteps && ie == enumSteps);
    }

    String getPathName() {

        StringBuilder sb = new StringBuilder();

        int numSteps = theSteps.size();

        if (theFunction != null) {
            sb.append(theFunction.getName()).append("(");
        }

        for (int i = 0; i < numSteps; ++i) {

            String step = theSteps.get(i).theName;

            /* Delete the dot that was added after the previous step */
            if (TableImpl.BRACKETS.equals(step)) {
                sb.delete(sb.length() - 1, sb.length());

                if (i == theOuterCtxVarPos && i == theInnerVarPos) {
                    step = "[*%]";
                } else if (i == theOuterCtxVarPos) {
                    step = "[*]";
                } else if (i == theInnerVarPos) {
                    step = "[%]";
                }
            }

            sb.append(step);

            if (i < numSteps - 1) {
                sb.append(NameUtils.CHILD_SEPARATOR);
            }
        }

        if (theFunction != null) {
            sb.append(")");
        }

        return sb.toString();
    }

    void trace(int level, Supplier<String> msg) {

        if (level <= theTraceLevel) {
            System.out.println(
                 "QUERY " + theExpr.getQCB().getOptions().getQueryName() +
                 " (" + threadId(Thread.currentThread()) + ") " +
                 "epath: " + getPathName() + ": " + msg.get());
        }
    }
}
