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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.EmptyValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.runtime.CompOpIter;
import oracle.kv.impl.query.runtime.CompOpIter.CompResult;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.table.FieldDef.Type;

/**
 * Implements the IN operator.
 *
 * in_expr : in1_expr | in2_expr | in3_expr ;
 *
 * in1_expr : in1_left_op IN LP in1_expr_list (COMMA in1_expr_list)+ RP ;
 *
 * in1_left_op : LP concatenate_expr (COMMA concatenate_expr)* RP ;
 *
 * in1_expr_list : LP expr (COMMA expr)* RP ;
 *
 * in2_expr : concatenate_expr IN LP expr (COMMA expr)+ RP ;
 *
 * in3_expr : (concatenate_expr |
 *            (LP concatenate_expr (COMMA concatenate_expr)* RP)) IN path_expr ;
 *
 * theNumKeyComps:
 * The number of exprs at the left-hand-side of the IN op.
 *
 * theArgs:
 * Contains all the exprs appearing at both sides on the IN op, in the order
 * they appear in the query.
 *
 * theHasVarKeys:
 * Set to true if any of the exprs at the RHS of the IN is not runtime-constant.
 *
 * theHasConstKeys:
 * Set to true if all of the exprs at the RHS of the IN are compile-constant.
 */
public class ExprInOp extends Expr {

    public static class In3BindInfo implements FastExternalizable {

        public int theNumComps;
        public int[] thePushedComps;
        public int[] theIndexFieldPositions;
        public int theRHSIter;

        In3BindInfo(ExprInOp e, int numIn3CompsPushed) {
            theNumComps = e.getNumKeyComps();
            thePushedComps = new int[numIn3CompsPushed];
            theIndexFieldPositions = new int[numIn3CompsPushed];
        }

        public In3BindInfo(DataInput in, short serialVersion) throws IOException {

            theNumComps = in.readInt();
            thePushedComps = PlanIter.deserializeIntArray(in, serialVersion);
            theIndexFieldPositions =
                PlanIter.deserializeIntArray(in, serialVersion);
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeInt(theNumComps);
            PlanIter.serializeIntArray(thePushedComps, out, serialVersion);
            PlanIter.serializeIntArray(theIndexFieldPositions, out, serialVersion);
        }

        public void display(StringBuilder sb, DisplayFormatter formatter) {

            sb.append("{\n");
            formatter.incIndent();
            formatter.indent(sb);
            sb.append("\"theNumComps\" : ").append(theNumComps).append(",\n");
            formatter.indent(sb);
            sb.append("\"thePushedComps\" : ");
            sb.append(PlanIter.printIntArray(thePushedComps)).append(",\n");
            formatter.indent(sb);
            sb.append("\"theIndexFieldPositions\" : ");
            sb.append(PlanIter.printIntArray(theIndexFieldPositions));
            sb.append("\n");
            formatter.decIndent();
            formatter.indent(sb);
            sb.append(" }");
        }
    }

    final boolean theIsIN3;

    int theNumKeyComps;

    List<Expr> theArgs;

    boolean theHasVarKeys = false;

    boolean theHasConstKeys = true;

    public ExprInOp(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        boolean isIN3) {

        super(qcb, sctx, ExprKind.IN, location);
        theIsIN3 = isIN3;
        theType = TypeManager.BOOLEAN_ONE();
    }

    @Override
    public ExprInOp clone() {

        ArrayList<Expr> args = new ArrayList<Expr>(theArgs.size());
        for (Expr arg : theArgs) {
            args.add(arg.clone());
        }

        ExprInOp res = new ExprInOp(theQCB, theSctx, theLocation, theIsIN3);
        res.theNumKeyComps = theNumKeyComps;
        res.theHasVarKeys = theHasVarKeys;
        res.theHasConstKeys = theHasConstKeys;
        res.theArgs = theArgs;
        return res;
    }

    public boolean isIN3() {
        return theIsIN3;
    }

    Expr getIn3RHSExpr() {
        assert(theIsIN3);
        return theArgs.get(theArgs.size() - 1);
    }

    public int getNumKeyComps() {
        return theNumKeyComps;
    }

    public int getNumKeys() {
        if (!theIsIN3) {
            return (theArgs.size() - theNumKeyComps) / theNumKeyComps;
        }
        return -1;
    }

    boolean hasVarKeys() {
        return theHasVarKeys;
    }

    /*
     * Used by in1_expr to add the exprs in in1_left_op
     */
    void addDataArgs(List<Expr> exprs) {

        for (Expr arg : exprs) {
            arg.addParent(this);
        }

        theArgs = exprs;
        theNumKeyComps = exprs.size();
    }

    /*
     * Used by in1_expr to add an in1_expr_list on the RHS of the IN.
     */
    void addKeyArgs(ArrayList<Expr> args) {

        if (args.size() != theNumKeyComps) {
            throw new QueryException(
                "An expression list on the right side of an IN operator " +
                "does not have the same number of expressions as the " +
                "expression list on the left side of the IN operator");
        }

        for (int i = 0; i < args.size(); ++i) {

            Expr varArg = theArgs.get(i);
            Expr keyArg = args.get(i);

            Expr newKeyArg = checkKeyArg(varArg, keyArg);

            if (newKeyArg == null) {
                return;
            }

            if (newKeyArg == keyArg) {
                continue;
            }

            args.set(i, newKeyArg);
        }

        if (theHasConstKeys && isDuplicateKey(args)) {
            return;
        }

        for (Expr arg : args) {
            arg.addParent(this);
        }

        theArgs.addAll(args);
    }

    /*
     * See FuncCompOp.java for comments about what we are doing here
     * not used for in3_expr
     */
    private Expr checkKeyArg(Expr varArg, Expr keyArg) {

        Location loc = keyArg.getLocation();
        QueryControlBlock qcb = keyArg.getQCB();
        StaticContext sctx = keyArg.getSctx();
        boolean strict = qcb.strictMode();

        if (keyArg.getKind() != ExprKind.CONST) {

            if (!ConstKind.isConst(keyArg)) {
                theHasVarKeys = true;
                theHasConstKeys = false;
            } else if (ConstKind.isCompileConst(keyArg)) {

                List<FieldValueImpl> res = ExprUtils.computeConstExpr(keyArg);

                if (res.size() > 1) {
                    throw new QueryException(
                        "An expression in the left-hand-side of an IN " +
                        "operator returns more than one items", loc);
                }

                FieldValueImpl val = (res.isEmpty() ?
                                      EmptyValueImpl.getInstance() :
                                      res.get(0));

                keyArg = new ExprConst(qcb, sctx, loc, val);
            } else {
                theHasConstKeys = false;
            }
        }

        FieldDefImpl varDef = varArg.getType().getDef();
        FieldDefImpl keyDef = keyArg.getType().getDef();

        IndexExpr epath = varArg.getIndexExpr();

        Type tc0 = varDef.getType();
        Type tc1 = keyDef.getType();

        if (tc0 == tc1) {
            return keyArg;
        }

        if (!TypeManager.areTypesComparable(varDef, keyDef)) {

            if (tc0 == Type.TIMESTAMP && tc1 == Type.STRING) {

                int prec = ((TimestampDefImpl)varDef).getPrecision();
                keyArg  = ExprCast.create(
                    qcb, sctx, loc, keyArg,
                    FieldDefImpl.Constants.timestampDefs[prec],
                    keyArg.getType().getQuantifier());
                return keyArg;
            }

            if (strict) {
                throw new QueryException(
                    "Incompatible types for IN operator: \n" +
                    "Type1: " + varArg.getType() + "\nType2: " +
                    keyArg.getType(), keyArg.getLocation());
            }

            if (!varArg.mayReturnNULL() && !keyArg.mayReturnNULL()) {
                return null;
            }

            return keyArg;
        }

        if (keyArg.getKind() != ExprKind.CONST || varArg.isMultiValued()) {
            return keyArg;
        }

        FieldValueImpl keyVal = ((ExprConst)keyArg).getValue();
        boolean varNullable = varArg.mayReturnNULL();
        boolean varScalar = varArg.isScalar();
        boolean allowJsonNull = (epath != null ? epath.theIsJson : false);

        FieldValueImpl newKeyVal =
            FuncCompOp.castConstInCompOp(varDef,
                                         allowJsonNull,
                                         varNullable,
                                         varScalar,
                                         keyVal,
                                         FuncCode.OP_EQ,
                                         strict);

        if (newKeyVal != keyVal && newKeyVal == BooleanValueImpl.falseValue) {
            return null;
        }

        if (newKeyVal == keyVal || newKeyVal == BooleanValueImpl.trueValue) {
            return keyArg;
        }

        return new ExprConst(qcb, sctx, loc, newKeyVal);
    }

    private boolean isDuplicateKey(ArrayList<Expr> args) {

        boolean duplicate = false;
        int k = theNumKeyComps;
        int j = 0;
        CompResult res = new CompResult();

        while (k < theArgs.size()) {

            for (j = 0; j < theNumKeyComps; ++j) {

                FieldValueImpl v1 = ((ExprConst)args.get(j)).getValue();
                FieldValueImpl v2 = ((ExprConst)theArgs.get(k+j)).getValue();

                CompOpIter.compare(null, v1, v2, FuncCode.OP_EQ, false/*forSort*/,
                                   res, theLocation);

                if (res.comp != 0 ||
                    res.incompatible ||
                    (res.haveNull && !(v1.isNull() && v2.isNull()))) {
                    break;
                }
            }

            if (j == theNumKeyComps) {
                duplicate = true;
                break;
            }

            k += theNumKeyComps;
        }

        return duplicate;
    }

    /*
     * Used by in2_expr and in3_expr to add all the args (from both the LHS and
     * the RHS of the IN op).
     */
    void addArgs(ArrayList<Expr> args) {

        if (!theIsIN3) {

            theNumKeyComps = 1;
            theArgs = new ArrayList<Expr>(args.size());

            Expr varArg = args.get(0);
            varArg.addParent(this);
            theArgs.add(varArg);

            ArrayList<Expr> keyArgList = new ArrayList<Expr>(1);
            keyArgList.add(null);

            for (int i = 1; i < args.size(); ++i) {

                Expr keyArg = args.get(i);
                Expr newKeyArg = checkKeyArg(varArg, keyArg);

                if (newKeyArg == null) {
                    continue;
                }

                keyArgList.set(0, newKeyArg);

                if (theHasConstKeys && isDuplicateKey(keyArgList)) {
                    continue;
                }

                theArgs.add(newKeyArg);
                newKeyArg.addParent(this);
            }

        } else {
            for (Expr arg : args) {
                arg.addParent(this);
            }

            if (!ConstKind.isConst(args.get(args.size()-1))) {
                theHasVarKeys = true;
            }

            theNumKeyComps = args.size() - 1;
            theArgs = args;
        }
    }

    @Override
    int getNumChildren() {
        return theArgs.size();
    }

    int getNumArgs() {
        return theArgs.size();
    }

    Expr getArg(int i) {
        return theArgs.get(i);
    }

    void setArg(int i, Expr newExpr, boolean destroy) {

        Expr arg = theArgs.get(i);
        arg.removeParent(this, destroy);
        newExpr.addParent(this);
        theArgs.set(i, newExpr);

        if (i >= theNumKeyComps && newExpr.getConstKind() != arg.getConstKind()) {

            theHasVarKeys = false;
            theHasConstKeys = true;

            for (int k = theNumKeyComps; k < theArgs.size(); ++k) {
                arg = theArgs.get(i);
                if (!ConstKind.isConst(arg)) {
                    theHasVarKeys = true;
                    theHasConstKeys = false;
                    break;
                }
                if (!ConstKind.isCompileConst(arg)) {
                    theHasConstKeys = false;
                }
            }
        }
    }

    void addArg(Expr newExpr) {

        /* This method is used only by the IndexAnalyzer to construct a new
         * ExprInOp, by adding one arg at a time. After all the args have been
         * added, setNumKeyComps() is called */
        assert(theNumKeyComps == 0);

        if (theArgs == null) {
            theArgs = new ArrayList<Expr>(16);
        }

        theArgs.add(newExpr);
        newExpr.addParent(this);
    }

    void setNumKeyComps(int v) {
        theNumKeyComps = v;
    }

    @Override
    ExprType computeType() {
        return theType;
    }

    @Override
    public boolean mayReturnNULL() {

        for (Expr arg : theArgs) {
            if (arg.mayReturnNULL()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean mayReturnEmpty() {
        return false;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("Num Search Exprs = ").append(theNumKeyComps).append("\n");

        for (int i = 0; i < theArgs.size(); ++i) {
            theArgs.get(i).display(sb, formatter);
            if (i < theArgs.size() - 1) {
                sb.append(",\n");
            }
        }
    }
}
