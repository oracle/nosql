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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.types.ExprType;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.util.FastExternalizable;

public class ExprJoin extends Expr {

    public static class JoinPred implements FastExternalizable {

        public int theOuterBranch;
        public int theOuterExpr;
        public int theInnerVar;

        JoinPred(int outerBranch, int outerExpr, int innerVar) {
            theOuterBranch = outerBranch;
            theOuterExpr = outerExpr;
            theInnerVar = innerVar;
        }

        @SuppressWarnings("unused")
        public JoinPred(DataInput in, short serialVersion) throws IOException {
           theOuterBranch = in.readInt();
           theOuterExpr = in.readInt();
           theInnerVar = in.readInt();
        }

        @Override
        @SuppressWarnings("unused")
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeInt(theOuterBranch);
            out.writeInt(theOuterExpr);
            out.writeInt(theInnerVar);

        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{ outerBranch : ").append(theOuterBranch);
            sb.append(", outerExpr : ").append(theOuterExpr);
            sb.append(", innerVar : ").append(theInnerVar);
            sb.append(" }");
            return sb.toString();
        }
    }

    private ArrayList<Expr> theBranches;

    private ArrayList<JoinPred> theJoinPreds;

    ExprJoin(
        QueryControlBlock qcb,
        StaticContext sctx,
        QueryException.Location location,
        ArrayList<ExprSFW> branches) {

        super(qcb, sctx, ExprKind.JOIN, location);
        theBranches = new ArrayList<Expr>(branches.size());

        for (Expr branch : branches) {
            theBranches.add(branch);
            branch.addParent(this);
        }

        theJoinPreds = new ArrayList<>();

        /* the join iterator returns a dummy boolean value */
        theType = TypeManager.BOOLEAN_STAR();
    }

    @Override
    int getNumChildren() {
        return theBranches.size();
    }

    int numBranches() {
        return theBranches.size();
    }

    Expr getBranch(int i) {
        return theBranches.get(i);
    }

    void setBranch(int i, Expr newBranch, boolean destroy) {
        newBranch.addParent(this);
        theBranches.get(i).removeParent(this, destroy);
        theBranches.set(i, newBranch);
    }

    void addJoinPred(int outerBranch, int outerExpr, int innerVar) {
        theJoinPreds.add(new JoinPred(outerBranch, outerExpr, innerVar));
    }

    public int numJoinPreds() {
        return theJoinPreds.size();
    }

    public JoinPred getJoinPred(int i) {
        return theJoinPreds.get(i);
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
    ExprType computeType() {
        return theType;
    }

    @Override
    void displayContent(StringBuilder sb, DisplayFormatter formatter) {

        formatter.indent(sb);
        sb.append("join preds : [\n");
        formatter.incIndent();
        for (JoinPred pred : theJoinPreds) {
            formatter.indent(sb);
            sb.append(pred).append("\n");
        }
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("]\n");

        for (Expr branch : theBranches) {
            branch.display(sb, formatter);
            sb.append("\n");
        }
    }
}
