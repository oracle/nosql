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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprVar.VarKind;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;

/**
 * See javadoc for ExprUpdateRow and UpdateFieldIter.
 */
class ExprUpdateField extends Expr {

    private UpdateKind theUpdateKind;

    private Expr theInput;

    private Expr thePosExpr;

    private ExprVar theTargetItemVar;

    private Expr theNewValueExpr;

    private boolean theIsMRCounterDec;

    private int theJsonMRCounterColPos = -1;

    private boolean theIsJsonMRCounterUpdate;

    private boolean theCloneNewValues = true;

    ExprUpdateField(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input,
        int jsonMRCounterColPos) {

        super(qcb, sctx, ExprKind.UPDATE_FIELD, location);

        if (input != null) {
            theInput = input;
            theInput.addParent(this);
        }

        theJsonMRCounterColPos = jsonMRCounterColPos;

        theType = TypeManager.EMPTY();
    }

    @Override
    public ExprUpdateField clone() {

        ExprUpdateField res = new ExprUpdateField(theQCB, theSctx, theLocation,
                                                  theInput.clone(), -1);
        res.theUpdateKind = theUpdateKind;
        res.thePosExpr = Expr.cloneOrNull(thePosExpr);
        res.theTargetItemVar = theTargetItemVar;
        res.theNewValueExpr = Expr.cloneOrNull(theNewValueExpr);
        res.theIsMRCounterDec = theIsMRCounterDec;
        res.theJsonMRCounterColPos = theJsonMRCounterColPos;
        res.theIsJsonMRCounterUpdate = theIsJsonMRCounterUpdate;
        res.theCloneNewValues = theCloneNewValues;
        return res;
    }

    void setUpdateKind(UpdateKind k) {

        theUpdateKind = k;

        if (theUpdateKind == UpdateKind.REMOVE) {

            theCloneNewValues = false;

            if (theInput.getKind() == ExprKind.FIELD_STEP &&
                theInput.getInput().getType().isRecord()) {
                throw new QueryException(
                    "Cannot remove fields from records.",
                    theLocation);
            }

            IndexExpr epath = theInput.getIndexExpr();

            if (epath != null && epath.isMRCounterPath()) {
                throw new QueryException(
                    "Cannot remove json MR counter field", theLocation);
            }
        } else if (theUpdateKind == UpdateKind.JSON_MERGE_PATCH) {
            theCloneNewValues = false;
        }

    }

    UpdateKind getUpdateKind() {
        return theUpdateKind;
    }

    int getJsonMRCounterColPos() {
        return theJsonMRCounterColPos;
    }

    boolean getIsJsonMRCounterUpdate() {
        return theIsJsonMRCounterUpdate;
    }

    boolean isTTLUpdate() {
        return (theUpdateKind == UpdateKind.TTL_HOURS ||
                theUpdateKind == UpdateKind.TTL_DAYS ||
                theUpdateKind == UpdateKind.TTL_TABLE);
    }

    void addTargetItemVar(ExprVar v) {
        theTargetItemVar = v;
    }

    void addNewValueExpr(Expr newValueExpr) {

        if (theUpdateKind == UpdateKind.SET) {

            /* Check if this a SET of an MR_COUNTER column */
            newValueExpr = handleMRCounterSET(newValueExpr);

            if (newValueExpr.isMultiValued()) {
                ArrayList<Expr> args = new ArrayList<Expr>(1);
                args.add(newValueExpr);
                newValueExpr = new ExprArrayConstr(theQCB, theSctx,
                                                   newValueExpr.getLocation(),
                                                   args, true/*conditional*/);
            }
        }

        if (theUpdateKind == UpdateKind.JSON_MERGE_PATCH) {

            if (!theInput.getType().getDef().
                isSubtype(FieldDefImpl.Constants.jsonDef)) {
                throw new QueryException(
                    "The target expression of the JSON MERGE PATCH clause " + 
                    "does not return json values", theInput.getLocation());
            }

            if (!ConstKind.isConst(newValueExpr)) {
                throw new QueryException(
                    "The patch expression of the JSON MERGE PATCH clause " + 
                    "must not reference any tables", newValueExpr.getLocation());
            }

            if (ConstKind.isCompileConst(newValueExpr)) {

                ExprUtils.constructJsonArrayMap(newValueExpr);

                ArrayList<FieldValueImpl> res = ExprUtils.
                                                computeConstExpr(newValueExpr);
                assert(res.size() == 1);
                FieldValueImpl patch = res.get(0);

                if (!patch.isJson()) {
                    throw new QueryException(
                        "The patch expression of the JSON MERGE PATCH clause " + 
                        "is not valid json", newValueExpr.getLocation());
                }

                newValueExpr = new ExprConst(theQCB, theSctx,
                                             newValueExpr.getLocation(),
                                             patch);
            }

            theNewValueExpr = newValueExpr;
            theNewValueExpr.addParent(this);

        } else {
            theNewValueExpr = newValueExpr;
            theNewValueExpr.addParent(this);

            theCloneNewValues = mustCloneNewValues(newValueExpr,
                                                   theTargetItemVar,
                                                   false);
        }
    }

    Expr handleMRCounterSET(Expr newValueExpr) {

        IndexExpr epath = theInput.getIndexExpr();

        if (epath == null || !epath.isMRCounterPath()) {
            return newValueExpr;
        }
        String msg;

        if (!epath.theTable.isMultiRegion() &&
            !Region.isMultiRegionId(getQCB().getOptions().getRegionId())) {
            msg = "Path " + epath.getPathName() + " is an MR counter, which " +
                  "can only be updated if the table is multi-region table or " +
                  "valid external region id is provided in ExecuteOptions";
            throw new QueryException(msg, newValueExpr.getLocation());
        }

        msg = ("Path " + epath.getPathName() + " is an MR counter, which can " +
               "only be incremented or decremented. Use an expression of the " +
               "form \"$ + <expr>\" or \"$ - <expr>\" at the right of " +
               "the SET clause.");

        if (newValueExpr.getFunction(FuncCode.OP_ADD_SUB) == null) {
            throw new QueryException(msg, newValueExpr.getLocation());
        }

        ExprFuncCall fncall = (ExprFuncCall)newValueExpr;

        if (fncall.getNumArgs() != 3) {
            throw new QueryException(msg, newValueExpr.getLocation());
        }

        Expr arg1 = fncall.getArg(0);
        Expr arg2 = fncall.getArg(1);
        ExprConst opsExpr = (ExprConst)fncall.getArg(2);
        String ops = opsExpr.getValue().asString().get();

        if (arg1.getKind() != ExprKind.VAR) {

            if (!ExprUtils.matchExprs(arg1, theInput)) {
                throw new QueryException(msg, newValueExpr.getLocation());
            }

        } else if (((ExprVar)arg1).getVarKind() != VarKind.CTX_ITEM) {
            throw new QueryException(msg, newValueExpr.getLocation());
        }

        if (ops.charAt(1) == '-') {
            theIsMRCounterDec = true;
        }

        theIsJsonMRCounterUpdate = epath.isMRCounterPath();

        return arg2;
    }

    void addPosExpr(Expr e) {
        thePosExpr = e;
        thePosExpr.addParent(this);
    }

    void removeTargetItemVar() {
        if (theTargetItemVar != null &&
            theTargetItemVar.getNumParents() == 0) {
            theTargetItemVar = null;
        }
    }

    void setPosExpr(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        thePosExpr.removeParent(this, destroy);
        thePosExpr = newExpr;
        removeTargetItemVar();
    }

    void setNewValueExpr(Expr newExpr, boolean destroy) {

        newExpr.addParent(this);
        theNewValueExpr.removeParent(this, destroy);
        theNewValueExpr = newExpr;
        removeTargetItemVar();

        theCloneNewValues = mustCloneNewValues(newExpr, theTargetItemVar, false);
    }

    @Override
    int getNumChildren() {
        return (theInput == null ? 0 :
                (theNewValueExpr != null ?
                 (thePosExpr != null ? 3 : 2) :
                 1));
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    Expr getPosExpr() {
        return thePosExpr;
    }

    Expr getNewValueExpr() {
        return theNewValueExpr;
    }

    boolean isMRCounterDec() {
        return theIsMRCounterDec;
    }

    ExprVar getTargetItemVar() {
        return theTargetItemVar;
    }

    boolean cloneNewValues() {
        return theCloneNewValues;
    }

    /*
     * This method decides whether complex values returned by a new-value expr
     * (in SET, ADD, or PUT clauses) need to be cloned before they are used to
     * update the the target item(s). Cloning may be needed because it is
     * possible to create cycles among items, resulting in stack overflows when
     * such a circular data structure is serialized.
     *
     * However, to produce a cycle, the new-value expr must return items that
     * are not proper descendants of the target item. This is possible only if
     * the new-value expr references the row variable (the table alias) or the
     * target-item variable ($). If the row variable is referenced, we clone
     * (because it is hard or impossible to deduce at compile time whether a
     * cycle will be formed). If $ is referenced, but in a way that guaranties
     * that only proper descendants of the target item will be returned (eg
     * $.a.b) then we don't clone; otherwise we do.
     */
    private static boolean mustCloneNewValues(
        Expr expr,
        Expr targetItemVar,
        boolean inPathExpr) {

        /* No need to clone atomic values */
        if (expr.getType().getDef().isAtomic()) {
            return false;
        }

        switch (expr.getKind()) {

        case CONST:
            return false;

        case VAR:
            ExprVar var = (ExprVar)expr;

            /*
             * We clone if the var is a row var or it is the $ var and it's
             * not referenced in a path expr.
             */
            if (var.getTable() != null ||
                (var == targetItemVar && !inPathExpr)) {
                return true;
            }

            return false;

        case FUNC_CALL:
            if (expr.getFunction(FuncCode.FN_SEQ_CONCAT) != null) {

                ExprFuncCall fncall = (ExprFuncCall)expr;

                for (int i = 0; i < fncall.getNumArgs(); ++i) {
                    if (mustCloneNewValues(fncall.getArg(i),
                                           targetItemVar,
                                           inPathExpr)) {
                        return true;
                    }
                }

                return false;
            }

            if (expr.getFunction(FuncCode.FN_PARSE_JSON) != null) {
                return false;
            }

            /*
             * For now, all other functions return atomic values, so we should
             * not be here.
             */
            Function func = expr.getFunction(null);
            throw new QueryStateException("Unexpected function call: " +
                                          func.getCode());

        case PROMOTE:
        case CAST:
        case SEQ_MAP:
            return mustCloneNewValues(expr.getInput(),
                                      targetItemVar,
                                      inPathExpr);

        case CASE:
            ExprCase caseExpr = (ExprCase)expr;

            for (int i = 0; i < caseExpr.getNumWhenClauses(); ++i) {
                if (mustCloneNewValues(caseExpr.getThenExpr(i),
                                       targetItemVar,
                                       inPathExpr)) {
                    return true;
                }
            }

            if (caseExpr.getElseExpr() != null &&
                mustCloneNewValues(caseExpr.getElseExpr(),
                                   targetItemVar,
                                   inPathExpr)) {
                return true;
            }

            return false;

        case ARRAY_CONSTR:
            ExprArrayConstr arr = (ExprArrayConstr)expr;

            for (int i = 0; i < arr.getNumArgs(); ++i) {

                if (mustCloneNewValues(arr.getArg(i),
                                       targetItemVar,
                                       inPathExpr)) {
                    return true;
                }
            }

            return false;

        case MAP_CONSTR:
            ExprMapConstr map = (ExprMapConstr)expr;

            for (int i = 0; i < map.getNumArgs(); ++i) {

                if (i % 2 == 0) {
                    continue;
                }

                if (mustCloneNewValues(map.getArg(i),
                                       targetItemVar,
                                       inPathExpr)) {
                    return true;
                }
            }

            return false;

        case FIELD_STEP:
        case MAP_FILTER:
        case ARRAY_SLICE:
        case ARRAY_FILTER:
            return mustCloneNewValues(expr.getInput(), targetItemVar, true);

        case BASE_TABLE:
        case IS_OF_TYPE:
        case SFW:
        case GROUP:
        case REC_CONSTR:
        case INSERT_ROW:
        case DELETE_ROW:
        case UPDATE_ROW:
        case UPDATE_FIELD:
        case RECEIVE:
        case SORT:
        case IN:
        case JOIN:
            throw new QueryStateException("Unexpected expression kind: " +
                                          expr.getKind());
        }

        return true;
    }

    @Override
    public ExprType computeType() {
        return theType;
    }

    @Override
    public boolean mayReturnNULL() {
        return false;
    }

    @Override
    boolean mayReturnEmpty() {
        return true;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        if (theInput != null) {
            theInput.display(sb, formatter);
        }

        if (theTargetItemVar != null) {
            sb.append("\n");
            theTargetItemVar.display(sb, formatter);
        }

        if (theNewValueExpr != null) {
            sb.append("\n");
            theNewValueExpr.display(sb, formatter);
        }
    }
}
