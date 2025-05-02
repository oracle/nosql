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

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.ExprType.Quantifier;
import oracle.kv.impl.query.types.ExprType.TypeCode;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.table.FieldDef;

/**
 * Represents a "dot" step in a path expr. In general, a field step will select
 * the value of a field from one or more input records.
 *
 * Syntactically, an field step looks like this:
 *
 * input_expr.name_expr
 *
 * A field step has 2 operands: an input expr that is supposed to return a
 * sequence of zero or more complex values, and a "name" expr, that is
 * supposed to return at most one string. We refer to the string returned
 * by the name expr as the "key name".
 *
 * For each value in the input sequence, the step computes zero or more result
 * values. The overall result of the step is the concatenation of the results
 * produced for each input value, in the order of their computation.
 *
 * Let V be the current value that the field step operates upon.
 *
 * 1. If V is not a complex value, an error is raised.
 *
 * 2. If V is a record, the name expr is computed. The name expr may reference
 *    V via the $$ variable. Note that if the name expr does not reference $$,
 *    it does not need to be computed for each V; it can be computed only once,
 *    before any of the input values are processed.
 *
 *    If the name expr returns an empty result (no key name), V is skipped. If
 *    the name expr returns more than 1 result, or a non-string result, an error
 *    is raised. Otherwise, let K be the key name computed by the name expr.
 *    Then, if V contains a field whose name is equal to K, the value of that
 *    field is selected. Otherwise, an error is raised.
 *
 * 3. If V is a map. the name expr is computed. As in case 2, if there is no
 *    key name of the name expr returns more than 1 result, or a non-string
 *    result, an error is raised. Otherwise, let K be the key name computed
 *    by the name expr. Contrary to case 2, no error is raised if V does not
 *    contain an entry with key equal to K; instead V is skipped. Otherwise,
 *    the value of the entry whose key is equal to K is selected.
 *
 * 4  If V is an array, the field step is applied recursively to each element
 *    of the array.
 *
 * Should we allow theFieldNameExpr to return multiple strings ????
 * Maybe have another kind of step (using the { expr } syntax, where expr
 * produces any number of strings and the step selects matching pairs and
 * constructs a new record out of the selected pairs.
 *
 * Should we define $$pos and allow its use in the name expr ???? It would be
 * defined as the number of records or maps processed so far.
 */
class ExprFieldStep extends Expr {

    private Expr theInput;

    private Expr theFieldNameExpr;

    private String theFieldName;

    private int theFieldPos = -1;

    private ExprVar theCtxItemVar;

    ExprFieldStep(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input) {

        super(qcb, sctx, ExprKind.FIELD_STEP, location);
        theInput = input;

        theInput.addParent(this);
    }

    ExprFieldStep(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input,
        String fieldName) {

        this(qcb, sctx, location, input);
        theFieldName = fieldName;
        getFieldPos();
    }

    ExprFieldStep(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        Expr input,
        int fieldPos) {

        this(qcb, sctx, location, input);
        theFieldPos = fieldPos;

        FieldDef inType = theInput.getType().getDef();

        while (inType.isArray()) {
            inType = ((ArrayDefImpl)inType).getElement();
        }

        if (!inType.isRecord()) {
            throw new QueryStateException(
                "Input to field step expression does not have a record type");
        }

        theFieldName = ((RecordDefImpl)inType).getFieldName(theFieldPos);
    }

    @Override
    public ExprFieldStep clone() {

        ExprFieldStep res = new ExprFieldStep(theQCB, theSctx, theLocation,
                                              theInput.clone());
        if (theFieldNameExpr != null) {
            res.theFieldNameExpr = theFieldNameExpr.clone();
        }
        res.theFieldName = theFieldName;
        res.theFieldPos = theFieldPos;
        res.theCtxItemVar = theCtxItemVar;
        return res;
    }

    void addCtxVars(ExprVar ctxItemVar) {
        theCtxItemVar = ctxItemVar;
    }

    void addFieldNameExpr(String fieldName, Expr fieldNameExpr) {

        theFieldName = fieldName;
        theFieldNameExpr = fieldNameExpr;
        if (theFieldNameExpr != null) {
            assert(theFieldName == null);
            theFieldNameExpr.addParent(this);
        }

        checkConst();

        if (!isConst()) {
            theFieldNameExpr = ExprPromote.create(
                this, theFieldNameExpr, TypeManager.STRING_QSTN());
        } else {
            getFieldPos();
        }
    }

    @Override
    int getNumChildren() {
        return (theFieldNameExpr != null ? 2 : 1);
    }

    @Override
    Expr getInput() {
        return theInput;
    }

    void setInput(Expr newExpr, boolean destroy) {
        newExpr.addParent(this);
        theInput.removeParent(this, destroy);
        theInput = newExpr;
        theFieldPos = -1;
        getFieldPos();
    }

    Expr getFieldNameExpr() {
        return theFieldNameExpr;
    }

    void setFieldNameExpr(Expr newExpr, boolean destroy) {
        assert(theFieldName == null);
        newExpr.addParent(this);
        theFieldNameExpr.removeParent(this, destroy);
        theFieldNameExpr = newExpr;
    }

    String getFieldName() {
        return theFieldName;
    }

    ExprVar getCtxItemVar() {
        return theCtxItemVar;
    }

    public boolean isConst() {
        return theFieldName != null;
    }

    public void checkConst() {

        if (isConst()) {
            assert(theFieldNameExpr == null);
            assert(theCtxItemVar == null);
            return;
        }

        if (theFieldNameExpr.getKind() == ExprKind.CONST &&
            theFieldNameExpr.getType().getCode() == TypeCode.STRING) {

            FieldValueImpl value = ((ExprConst)theFieldNameExpr).getValue();
            theFieldName = value.getString();
            theFieldNameExpr.removeParent(this, true/*destroy*/);
            theFieldNameExpr = null;
            theCtxItemVar = null;
        }

        if (theCtxItemVar != null && !theCtxItemVar.hasParents()) {
            theCtxItemVar = null;
        }
    }

    int getFieldPos() {

        if (theFieldPos >= 0) {
            return theFieldPos;
        }

        if (theFieldName == null) {
            return -1;
        }

        FieldDef inType = theInput.getType().getDef();

        while (inType.isArray()) {
            inType = ((ArrayDefImpl)inType).getElement();
        }

        if (!inType.isRecord()) {

            if (theInput.getKind() == ExprKind.VAR) {
                ExprVar var = (ExprVar)theInput;
                TableImpl table = var.getTable();
                if (table != null &&
                    table.isJsonCollection() &&
                    table.isKeyComponent(theFieldName)) {

                    theFieldPos = table.getFieldPos(theFieldName);
                    return theFieldPos;
                }
            }

            return -1;
        }

        try {
            theFieldPos = ((RecordDefImpl)inType).getFieldPos(theFieldName);
        } catch (IllegalArgumentException e) {
            throw new QueryException(
                "There is no field named " + theFieldName +
                " in type " + inType, getLocation());
        }

        return theFieldPos;
    }

    @Override
    public ExprType computeType() {

        ExprType inType = theInput.getType();

        if (inType.isEmpty()) {
            return TypeManager.EMPTY();
        }

        checkConst();

        while (inType.isArray()) {
            inType = inType.getArrayElementType(Quantifier.STAR);
        }

        if (theQCB.strictMode() && inType.isAtomic()) {
            throw new QueryException(
                "Wrong input type for path step " +
                "(must be a record, array, or map type): " +
                inType.getDef().getDDLString(), theLocation);
        }

        if (inType.isAtomic()) {
            return TypeManager.EMPTY();
        }

        Quantifier inQuant = inType.getQuantifier();
        Quantifier outQuant;

        if (inType.isRecord() && isConst()) {
            return inType.getFieldType(getFieldPos(), inQuant);
        }

        if (getFieldPos() >= 0) {
            // must be a reference to a primary key column of a schemaless table
            assert(theInput.getKind() == ExprKind.VAR);
            ExprVar var = (ExprVar)theInput;
            TableImpl table = var.getTable();
            assert(table != null);
            assert(table.isJsonCollection());
            assert(table.isPrimKeyAtPos(theFieldPos));
            FieldDefImpl def = table.getFieldDef(theFieldPos);
            return TypeManager.createType(def, Quantifier.ONE);
        }

        /*
         * If the input type is json or any, the input value may be an array,
         * in which case multiple items will be returned, in general.
         */
        if (inType.isAnyJson() || inType.isAny()) {
            inQuant = Quantifier.STAR;
        }

        outQuant = (inType.isRecord() ?
                    inQuant :
                    TypeManager.getUnionQuant(inQuant, Quantifier.QSTN));
        return inType.getMapElementType(outQuant);
    }

    @Override
    public boolean mayReturnNULL() {

        if (theInput.mayReturnNULL() ||
            getType().isAny() ||
            getType().isAnyJson()) {
            return true;
        }

        FieldDef inType = theInput.getType().getDef();

        while (inType.isArray()) {
            inType = inType.asArray().getElement();
        }

        if (inType.isMap()) {
            return false;
        }

        if (inType.isRecord()) {
            if (theFieldName == null) {
                /* TODO: check if all fields are non-nullable */
                return true;
            }
            return inType.asRecord().isNullable(theFieldName);
        }

        /* The inType is a wildcard */
        return true;
    }

    @Override
    public boolean mayReturnEmpty() {

        if (theInput.mayReturnEmpty()) {
            return true;
        }

        FieldDefImpl inType = theInput.getType().getDef();

        while (inType.isArray()) {
            inType = ((ArrayDefImpl)inType).getElement();
        }

        if (inType.isAtomic() ||
            inType.isMap() ||
            inType.isAny() ||
            inType.isJson() ||
            inType.isEmpty()) {
            return true;
        }

        return false;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("type :\n");
        formatter.indent(sb);
        getType().display(sb, formatter);
        sb.append("\n");
        if (theFieldPos >= 0) {
            formatter.indent(sb);
            sb.append("field pos : ").append(theFieldPos);
            sb.append("\n");
        }
        theInput.display(sb, formatter);
        sb.append(".\n");
        formatter.indent(sb);
        if (isConst()) {
            sb.append(theFieldName);
        } else {
            theFieldNameExpr.display(sb, formatter);
        }
    }
}
