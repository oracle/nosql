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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.FieldDefSerialization;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.query.QueryException.Location;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.SortSpec;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.impl.util.SizeOf;

public class FuncCollectIter extends PlanIter {

    static class CompareFunction implements Comparator<FieldValueImpl> {

        final RuntimeControlBlock theRCB;
        final SortSpec theSortSpec;
        final Location theLocation;

        CompareFunction(RuntimeControlBlock rcb, Location loc) {

            theRCB = rcb;
            theSortSpec = new SortSpec(false, false);
            theLocation = loc;
        }

        @Override
        public int compare(FieldValueImpl v1, FieldValueImpl v2) {

            return CompOpIter.compareTotalOrder(theRCB,
                                                v1, v2,
                                                theSortSpec,
                                                theLocation);
        }
    }

    static class HashValue {

        public FieldValueImpl theValue;

        HashValue(FieldValueImpl value) {
            theValue = value;
        }

        @Override
        public boolean equals(Object other) {
            HashValue o = (HashValue)other;
            return theValue.equal(o.theValue);
        }

        @Override
        public int hashCode() {
            return theValue.hashcode();
        }

        public long sizeof() {
            return (SizeOf.HASHSET_ENTRY_OVERHEAD +
                    SizeOf.OBJECT_OVERHEAD +
                    SizeOf.OBJECT_REF_OVERHEAD +
                    theValue.sizeof());
        }
    }

    private class CollectIterState extends PlanIterState {

        final CompareFunction theComparator;

        ArrayValueImpl theArray;

        HashSet<HashValue> theValues;

        CollectIterState(RuntimeControlBlock rcb) {

            super();
            theComparator = new CompareFunction(rcb, theLocation);
            theValues = new HashSet<HashValue>(128);
            theArray = theArrayDef.createArray();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theValues.clear();
            theArray = ((FuncCollectIter)iter).theArrayDef.createArray();
        }

        @Override
        public void close() {
            super.close();
            theValues = null;
            theArray = null;
        }
    }

    private final boolean theIsDistinct;

    private final ArrayDefImpl theArrayDef;

    private final PlanIter theInput;

    public FuncCollectIter(
        Expr e,
        int resultReg,
        FieldDefImpl exprDef,
        boolean distinct,
        PlanIter input,
        boolean forCloud) {

        super(e, resultReg, forCloud);
        theArrayDef = (ArrayDefImpl)exprDef;
        theIsDistinct = distinct;
        theInput = input;
    }

    FuncCollectIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theArrayDef = (ArrayDefImpl)FieldDefSerialization.
                                    readFieldDef(in, serialVersion);
        theIsDistinct = in.readBoolean();
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        FieldDefSerialization.writeFieldDef(theArrayDef, out, serialVersion);
        out.writeBoolean(theIsDistinct);
        serializeIter(theInput, out, serialVersion);
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);
        out.writeBoolean(theIsDistinct);
        theInput.writeForCloud(out, driverVersion, valWriter);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_COLLECT;
    }

    @Override
    public PlanIter getInputIter() {
        return theInput;
    }

    ArrayDefImpl getArrayDef() {
        return theArrayDef;
    }

    boolean isDistinct() {
        return theIsDistinct;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new CollectIterState(rcb));
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        /*
         * Don't reset the state of "this". Resetting the state is done in
         * method getAggrValue below.
         */
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
    public boolean next(RuntimeControlBlock rcb) {

        CollectIterState state = (CollectIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            boolean more = theInput.next(rcb);

            if (!more) {
                return true;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("Collecting value " + val);
            }

            aggregate(rcb, val);
        }
    }

    @Override
    void aggregate(RuntimeControlBlock rcb, FieldValueImpl val) {

        CollectIterState state = (CollectIterState)rcb.getState(theStatePos);

        if (val.isNull() || val.isEMPTY()) {
            return;
        }

        if (theIsDistinct) {
            if (rcb.isServerRCB()) {
                HashValue hval = new HashValue(val);
                state.theValues.add(hval);
                rcb.incMemoryConsumption(hval.sizeof());
            } else {
                ArrayValueImpl arr = (ArrayValueImpl)val;
                int size = arr.size();
                for (int i = 0; i < size; ++i) {
                    HashValue hval = new HashValue(arr.get(i));
                    state.theValues.add(hval);
                    rcb.incMemoryConsumption(hval.sizeof());
                }
            }
        } else {
            if (rcb.isServerRCB()) {
                state.theArray.add(val);
                rcb.incMemoryConsumption(val.sizeof() + SizeOf.OBJECT_REF_OVERHEAD);
            } else {
                ArrayValueImpl arr = (ArrayValueImpl)val;
                state.theArray.addAll(arr);
                rcb.incMemoryConsumption(val.sizeof() +
                                         arr.size() * SizeOf.OBJECT_REF_OVERHEAD);
            }
        }
    }

    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {
        return;
    }

    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        CollectIterState state = (CollectIterState)rcb.getState(theStatePos);

        ArrayValueImpl res;

        if (theIsDistinct) {
            res = theArrayDef.createArray();
            Iterator<HashValue> iter = state.theValues.iterator();
            while (iter.hasNext()) {
                res.add(iter.next().theValue);
            }
        } else {
            res = state.theArray;
        }

        if (!rcb.isServerRCB() && rcb.inTestMode()) {
            res.getArrayInternal().sort(state.theComparator);
        }

        if (rcb.getTraceLevel() >= 3) {
            rcb.trace("Collected values " + res);
        }

        if (reset) {
            state.reset(this);
        }
        return res;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"distinct\" : ").append(theIsDistinct);
        sb.append(",\n");
        displayInputIter(sb, formatter, verbose, theInput);
    }
}
