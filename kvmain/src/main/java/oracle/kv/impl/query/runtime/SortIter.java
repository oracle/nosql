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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

import oracle.kv.impl.api.query.QueryPublisher;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;
import oracle.kv.impl.util.SizeOf;
import oracle.kv.query.ExecuteOptions;

public class SortIter extends PlanIter {

    private class CompareFunction implements Comparator<FieldValueImpl> {

        @Override
        public int compare(FieldValueImpl v1, FieldValueImpl v2) {

            if (theInputType.isRecord()) {

                return compareRecords(
                    (RecordValueImpl)v1,
                    (RecordValueImpl)v2,
                    theSortFieldPositions,
                    theSortSpecs);
            }

            return CompOpIter.compareAtomicsTotalOrder(v1, v2, theSortSpecs[0]);
        }
    }

    private static class SortIterState extends PlanIterState {

        final ArrayList<FieldValueImpl> theResults;

        int theCurrResult;

        CompareFunction theComparator;

        public SortIterState(SortIter iter) {
            super();
            theResults = new ArrayList<FieldValueImpl>(4096);
            theComparator = iter.new CompareFunction();
        }

        @Override
        public void done() {
            super.done();
            theCurrResult = 0;
            theResults.clear();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCurrResult = 0;
            theResults.clear();
        }

        @Override
        public void close() {
            super.close();
            theResults.clear();
        }
    }

    private final PlanIter theInput;

    private final FieldDefImpl theInputType;

    private final int[] theSortFieldPositions;

    private final SortSpec[] theSortSpecs;

    private final boolean theCountMemory;

    public SortIter(
        Expr e,
        int resultReg,
        PlanIter input,
        FieldDefImpl inputType,
        int[] sortFieldPositions,
        SortSpec[] sortSpecs,
        boolean countMemory,
        boolean forCloud) {

        super(e, resultReg, forCloud);

        theInput = input;
        theInputType = inputType;
        theSortFieldPositions = sortFieldPositions;
        theSortSpecs = sortSpecs;
        theCountMemory = countMemory;
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);

        theInput.writeForCloud(out, driverVersion, valWriter);

        RecordDefImpl recDef = (RecordDefImpl)theInputType;

        String[] sortFields = new String[theSortFieldPositions.length];

        for (int i = 0; i < sortFields.length; ++i) {
            sortFields[i] = recDef.getFieldName(theSortFieldPositions[i]);
        }

        CloudSerializer.writeStringArray(sortFields, out);
        CloudSerializer.writeSortSpecs(theSortSpecs, out);

        if (driverVersion >= ExecuteOptions.DRIVER_QUERY_V3) {
            out.writeBoolean(theCountMemory);
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        assert(theIsCloudDriverIter);
        theInput.writeFastExternal(out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SORT;
    }

    @Override
    public void setPublisher(
        RuntimeControlBlock rcb,
        QueryPublisher pub) {
        theInput.setPublisher(rcb, pub);
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        SortIterState state = new SortIterState(this);
        rcb.setState(theStatePos, state);
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        SortIterState state = (SortIterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);
        state.close();
    }

    @Override
    public Throwable getCloseException(RuntimeControlBlock rcb) {
        return theInput.getCloseException(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        return nextInternal(rcb, false);
    }

    @Override
    public boolean nextLocal(RuntimeControlBlock rcb) {
        return nextInternal(rcb, true);
    }

    private boolean nextInternal(RuntimeControlBlock rcb, boolean local) {

        SortIterState state = (SortIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.isOpen()) {

            boolean more = (local ?
                            theInput.nextLocal(rcb) :
                            theInput.next(rcb));

            while (more) {

                FieldValueImpl v = rcb.getRegVal(theInput.getResultReg());

                if (v.isTuple()) {
                    v = ((TupleValue)v).toRecord();
                }

                RecordValueImpl rec = (RecordValueImpl)v;

                for (int i = 0; i < theSortFieldPositions.length; ++i) {

                    FieldValueImpl fval = rec.get(theSortFieldPositions[i]);
                    if (!fval.isAtomic() && !fval.isNull()) {
                        throw new QueryException(
                            "Sort expression does not return a single " +
                            "atomic value", theLocation);
                    }
                }

                state.theResults.add(v);

                if (theCountMemory) {
                    long sz = v.sizeof() + SizeOf.OBJECT_REF_OVERHEAD;
                    rcb.incMemoryConsumption(sz);
                }

                more = (local ?
                        theInput.nextLocal(rcb) :
                        theInput.next(rcb));
            }

            if (local) {
                if (!theInput.isDone(rcb)) {
                    return false;
                }
            }

            state.theResults.sort(state.theComparator);

            state.setState(StateEnum.RUNNING);
        }

        if (state.theCurrResult < state.theResults.size()) {

            FieldValueImpl v = state.theResults.get(state.theCurrResult); 
            ((RecordValueImpl)v).convertEmptyToNull();
            rcb.setRegVal(theResultReg, v);
            state.theResults.set(state.theCurrResult, null);
            ++state.theCurrResult;
            return true;
        }

        state.done();
        return false;
    }

    @Override
    public Map<String, String> getRNTraces(RuntimeControlBlock rcb) {
        return theInput.getRNTraces(rcb);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"order by fields at positions\" : [ ");
        for (int i = 0; i < theSortFieldPositions.length; ++i) {
            sb.append(theSortFieldPositions[i]);
            if (i < theSortFieldPositions.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(" ],\n");

        displayInputIter(sb, formatter, verbose, theInput);
    }

    static public int compareRecords(
        RecordValueImpl rec1,
        RecordValueImpl rec2,
        int[] sortFieldPositions,
        SortSpec[] sortSpecs) {

        for (int i = 0; i < sortFieldPositions.length; ++i) {
            int pos = sortFieldPositions[i];
            FieldValueImpl v1 = rec1.get(pos);
            FieldValueImpl v2 = rec2.get(pos);

            int comp = CompOpIter.compareAtomicsTotalOrder(v1, v2, sortSpecs[i]);

            if (comp != 0) {
                return comp;
            }
        }

        return 0;
    }
}
