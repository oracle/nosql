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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;
import oracle.kv.impl.query.compiler.SortSpec;

public class FuncSeqSortIter extends SingleInputPlanIter {

    private class FuncSeqSortState extends PlanIterState {

        private class CompareFunction implements Comparator<FieldValueImpl> {

            @Override
            public int compare(FieldValueImpl v1, FieldValueImpl v2) {

                return CompOpIter.compareTotalOrder(theRCB,
                                                    v1, v2,
                                                    theSortSpec,
                                                    theLocation);
            }
        }

        final RuntimeControlBlock theRCB;
        final SortSpec theSortSpec;
        final CompareFunction theComparator;

        ArrayList<FieldValueImpl> theValues;
        Iterator<FieldValueImpl> theIterator;

        FuncSeqSortState(RuntimeControlBlock rcb) {
            super();
            theRCB = rcb;
            theSortSpec = new SortSpec(false, false);
            theComparator = new CompareFunction();
            theValues = new ArrayList<FieldValueImpl>(128);
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            if (theValues != null) {
                theValues.clear();
            }
            theIterator = null;
        }

        @Override
        public void close() {
            super.close();
            theValues = null;
            theIterator = null;
        }
    }

    /* The first iter is the one that produces the sequence to sort. The
     * remaining iters (if any) are the order by exprs */
    private final PlanIter[] theInputs;

    public FuncSeqSortIter(
        Expr e,
        int resultReg,
        PlanIter[] inputs) {

        super(e, resultReg);
        theInputs = inputs;
    }

    FuncSeqSortIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInputs = deserializeIters(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIters(theInputs, out, serialVersion);
    }

    @Override
    protected PlanIter getInput() {
        return theInputs[0];
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_SEQ_SORT;
    }

    @Override
    FuncCode getFuncCode() {
        return FuncCode.FN_SEQ_SORT;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncSeqSortState(rcb));
        for (PlanIter iter : theInputs) {
            iter.open(rcb);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter iter : theInputs) {
            iter.reset(rcb);
        }
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter iter : theInputs) {
            iter.close(rcb);
        }
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        FuncSeqSortState state = (FuncSeqSortState)
            rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {
            if (state.theIterator != null) {

                if (state.theIterator.hasNext()) {
                    rcb.setRegVal(theResultReg, state.theIterator.next());
                    return true;
                }

                state.done();
                return false;
            }

            boolean more = getInput().next(rcb);

            if (!more) {
                state.theValues.sort(state.theComparator);
                state.theIterator = state.theValues.iterator();
                continue;
            }

            FieldValueImpl val = rcb.getRegVal(getInput().getResultReg());
            state.theValues.add(val);
            continue;
        }
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        if (theInputs.length > 1) {
            displayInputIters(sb, formatter, verbose, theInputs);
        } else {
            displayInputIter(sb, formatter, verbose, theInputs[0]);
        }
    }
}
