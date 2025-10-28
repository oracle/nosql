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

package oracle.kv.impl.query.runtime;

import static oracle.kv.impl.util.SerializationUtil.readNonNullSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullSequenceLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.ExprJoin;
import oracle.kv.impl.query.compiler.ExprJoin.JoinPred;

public class NestedLoopJoinIter extends PlanIter {

    public static class NestedLoopJoinState extends PlanIterState {

        private int theNumBoundBranches;

        final private boolean[] theOpenBranches;

        NestedLoopJoinState(NestedLoopJoinIter iter) {
            theOpenBranches = new boolean[iter.theBranches.length];
            theOpenBranches[0] = true;
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theNumBoundBranches = 0;
            int numBranches = ((NestedLoopJoinIter)iter).theBranches.length;
            for (int i = 1; i < numBranches; ++i) {
                theOpenBranches[i] = false;
            }
        }
    }

    final PlanIter[] theBranches;

    final JoinPred[] theJoinPreds;

    public NestedLoopJoinIter(
        ExprJoin e,
        int resultReg,
        PlanIter[] branches) {

        super(e, resultReg);
        theBranches = branches;
        theJoinPreds = new JoinPred[e.numJoinPreds()];
        for (int i = 0; i < theJoinPreds.length; ++i) {
            theJoinPreds[i] = e.getJoinPred(i);
        }
    }

    NestedLoopJoinIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theBranches = deserializeIters(in, serialVersion);
        int numPreds = readNonNullSequenceLength(in);
        theJoinPreds = new JoinPred[numPreds];
        for (int i = 0; i < numPreds; i++) {
            theJoinPreds[i] = new JoinPred(in, serialVersion);
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIters(theBranches, out, serialVersion);
        writeNonNullSequenceLength(out, theJoinPreds.length);
        for (JoinPred pred : theJoinPreds) {
            pred.writeFastExternal(out, serialVersion);
        }
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.NESTED_LOOP_JOIN;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        NestedLoopJoinState state = new NestedLoopJoinState(this);
        rcb.setState(theStatePos, state);

        theBranches[0].open(rcb);

        rcb.setNumJoinBranches(theBranches.length);
        ResumeInfo ri = rcb.getResumeInfo();

        for (int i = 0; i < theBranches.length; ++i) {
            ri.ensureTableRI(i);
        }

        for (int i = 0; i < theBranches.length - 1; ++i) {
            /* If the next branch suspended on a key or row (i.e. it has a
             * resume key), then (a) the current branch suspended on a row
             * (or index key in case of covering index) and (b) the current
             * branch must resume on that row/key. We indicate this by setting
             * theMoveJoinAfterResumeKey flag to false.
             *
             * If the next branch did not suspend on a key/row, let R be the
             * row/key that the current branch suspended on. In this case
             * there are 2 subcases:
             * (a) the next brach does not have a row that joins with R, so it
             *     reached to the end of its index scan in the previous batch.
             *     In this case, the current brach must resume after R.
             * (b) The next branch was not scanned at all, because the limit was
             *     reached while scanning the current branch. In this case, the
             *     the current brach must resume either on or after R, depending
             *     on the branch's value of the ri.theMoveAfterResumeKey flag. */
            if (ri.getPrimResumeKey(i+1) != null) {
                ri.setMoveJoinAfterResumeKey(i, false);
            }
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        NestedLoopJoinState state = (NestedLoopJoinState)rcb.getState(theStatePos);
        state.reset(this);

        ResumeInfo ri = rcb.getResumeInfo();
        for (int i = 0; i < theBranches.length; ++i) {
            ri.ensureTableRI(i);
        }

        for (int i = 0; i < theBranches.length; ++i) {
            theBranches[i].reset(rcb);
        }

        for (int i = 0; i < theBranches.length - 1; ++i) {
            if (ri.getPrimResumeKey(i+1) != null) {
                ri.setMoveAfterResumeKey(i, false);
            }
        }
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        NestedLoopJoinState state = (NestedLoopJoinState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (int i = 0; i < theBranches.length; ++i) {
            theBranches[i].close(rcb);
        }

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        NestedLoopJoinState state = (NestedLoopJoinState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (0 <= state.theNumBoundBranches &&
               state.theNumBoundBranches < theBranches.length) {

            int branch = state.theNumBoundBranches;
            PlanIter branchIter = theBranches[branch];
            boolean hasNext = branchIter.next(rcb);

            if (!hasNext) {

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("No Value for join branch " + branch);
                }

                --state.theNumBoundBranches;

            } else {

                FieldValueImpl branchRes = rcb.getRegVal(branchIter.theResultReg);

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("Value for join branch " + branch +
                              " = " + branchRes);
                }

                for (JoinPred pred : theJoinPreds) {
                    if (pred.theOuterBranch != branch) {
                        continue;
                    }

                    FieldValueImpl outerVal;
                    if (branchRes.isTuple()) {
                        outerVal = ((TupleValue)branchRes).get(pred.theOuterExpr);
                    } else if (branchRes.isRecord()) {
                        outerVal = ((RecordValueImpl)branchRes).get(pred.theOuterExpr);
                    } else {
                        throw new QueryStateException(
                            "Unexpected kind of value produced by join branch " +
                            branch);
                    }

                    rcb.setExternalVar(pred.theInnerVar, outerVal);

                    if (rcb.getTraceLevel() >= 3) {
                        rcb.trace("join branch " + branch + " bound inner var " +
                                  pred.theInnerVar + " to " + outerVal);
                    }
                }

                ++state.theNumBoundBranches;

                if (state.theNumBoundBranches < theBranches.length) {
                    branch = state.theNumBoundBranches;
                    branchIter = theBranches[branch];
                    if (!state.theOpenBranches[branch]) {
                        branchIter.open(rcb);
                        state.theOpenBranches[branch] = true;
                    } else {
                        branchIter.reset(rcb);
                    }
                }
            }
        }

        if (state.theNumBoundBranches < 0) {
            state.done();
            return false;
        }

        assert(state.theNumBoundBranches == theBranches.length);

        --state.theNumBoundBranches;
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"join predicates\" : [\n");
        formatter.incIndent();
        for (int i = 0; i < theJoinPreds.length; ++i) {
           formatter.indent(sb);
           sb.append("{ \"outerBranch\" :");
           sb.append(theJoinPreds[i].theOuterBranch);
           sb.append(", \"outerExpr\" : ").append(theJoinPreds[i].theOuterExpr);
           sb.append(", \"innerVar\" : ").append(theJoinPreds[i].theInnerVar);
           sb.append(" }");
           if (i < theJoinPreds.length - 1) {
                sb.append(",\n");
            } else {
               sb.append("\n");
           }
        }
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("],\n");

        formatter.indent(sb);
        sb.append("\"branches\" : [\n");
        formatter.incIndent();
        for (int i = 0; i < theBranches.length; ++i) {
            theBranches[i].display(sb, formatter, verbose);
            if (i < theBranches.length - 1) {
                sb.append(",\n");
            } else {
               sb.append("\n");
           }
        }
        formatter.decIndent();
        formatter.indent(sb);
        sb.append("]\n");
    }
}
