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
import java.util.Iterator;
import java.util.Objects;
import java.util.Stack;

import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprMapFilter;
import oracle.kv.impl.query.compiler.ExprMapFilter.FilterKind;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;

/**
 *
 */
public class MapFilterIter extends PlanIter {

    static private class ArrayAndPos {

        ArrayValueImpl theArray;
        int thePos;

        ArrayAndPos(ArrayValueImpl array) {
            theArray = array;
            thePos = 0;
        }
    }

    static private class MapFilterState extends PlanIterState {

        FieldValueImpl theCtxItem;

        int theFieldPos; // used for records;

        Iterator<String> theKeysIter; // used for maps

        String theCtxKey;

        FieldValueImpl theCtxElem;

        Stack<ArrayAndPos> theArrays;

        boolean thePredValue;

        boolean theComputePredOnce;

        boolean theComputePredPerMap;

        boolean theComputePredPerElem;

        MapFilterState(MapFilterIter iter) {
            super();
            theArrays = new Stack<ArrayAndPos>();

            theComputePredOnce = (iter.theCtxItemReg < 0 &&
                                  iter.theCtxElemReg < 0 &&
                                  iter.theCtxKeyReg < 0);

            theComputePredPerMap = (iter.theCtxItemReg >= 0 &&
                                    iter.theCtxElemReg < 0 &&
                                    iter.theCtxKeyReg < 0);

            theComputePredPerElem = (iter.theCtxElemReg >= 0 ||
                                     iter.theCtxKeyReg >= 0);

        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCtxItem = null;
            theFieldPos = 0;
            theKeysIter = null;
            if (theArrays != null) {
                theArrays.clear();
            }
        }

        @Override
        public void close() {
            super.close();
            theCtxItem = null;
            theKeysIter = null;
            theArrays = null;
            theCtxKey = null;
            theCtxElem = null;
        }
    }

    private final FilterKind theKind;

    private final PlanIter theInput;

    private final PlanIter thePredIter;

    private final int theCtxItemReg;

    private final int theCtxElemReg;

    private final int theCtxKeyReg;

    public MapFilterIter(
        Expr e,
        int resultReg,
        PlanIter input,
        PlanIter predIter,
        int ctxItemReg,
        int ctxElemReg,
        int ctxKeyReg) {

        super(e, resultReg);
        theKind = ((ExprMapFilter)e).getFilterKind();
        theInput = input;
        thePredIter = predIter;
        theCtxItemReg = ctxItemReg;
        theCtxElemReg = ctxElemReg;
        theCtxKeyReg = ctxKeyReg;
    }

    /**
     * FastExternalizable constructor.
     */
    MapFilterIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FilterKind.VALUES_COUNT);
        theKind = FilterKind.valueOf(ordinal);
        theInput = deserializeIter(in, serialVersion);
        thePredIter = deserializeIter(in, serialVersion);
        theCtxItemReg = readPositiveInt(in, true);
        theCtxElemReg = readPositiveInt(in, true);
        theCtxKeyReg = readPositiveInt(in, true);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theKind.ordinal());
        serializeIter(theInput, out, serialVersion);
        serializeIter(thePredIter, out, serialVersion);
        out.writeInt(theCtxItemReg);
        out.writeInt(theCtxElemReg);
        out.writeInt(theCtxKeyReg);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.MAP_FILTER;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new MapFilterState(this));
        theInput.open(rcb);
        if (thePredIter != null) {
            thePredIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        MapFilterState state = (MapFilterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.theComputePredOnce && state.isOpen()) {

            state.setState(StateEnum.RUNNING);

            computePredExpr(rcb, state);

            if (!state.thePredValue) {
                state.done();
                return false;
            }
        }

        while (true) {

            FieldValueImpl ctxItem = null;
            FieldValueImpl result;

            if (state.theCtxItem != null && !state.theCtxItem.isNull()) {

                boolean done = true;

                if (state.theCtxItem.isMap()) {
                    MapValueImpl map = (MapValueImpl)state.theCtxItem;

                    if (state.theKeysIter.hasNext()) {

                        done = false;
                        state.theFieldPos = -1;
                        state.theCtxKey = state.theKeysIter.next();

                        if (theCtxElemReg >= 0 ||
                            theKind == FilterKind.VALUES) {
                            state.theCtxElem = map.get(state.theCtxKey);
                        }
                    }
                } else if (state.theCtxItem.isTuple()) {
                    TupleValue rec = (TupleValue)state.theCtxItem;

                    if (state.theFieldPos < rec.getNumFields()) {

                        done = false;
                        state.theCtxKey = rec.getFieldName(state.theFieldPos);

                        if (theCtxElemReg >= 0 ||
                            theKind == FilterKind.VALUES) {
                            state.theCtxElem = rec.get(state.theFieldPos);
                        }

                        ++state.theFieldPos;
                    }
                } else {
                    RecordValueImpl rec = (RecordValueImpl)state.theCtxItem;

                    if (state.theFieldPos < rec.getNumFields()) {

                        done = false;
                        state.theCtxKey = rec.getFieldName(state.theFieldPos);

                        if (theCtxElemReg >= 0 ||
                            theKind == FilterKind.VALUES) {
                            state.theCtxElem = rec.get(state.theFieldPos);
                        }

                        ++state.theFieldPos;
                    }
                }

                if (!done) {

                    if (state.theComputePredPerElem) {

                        computePredExpr(rcb, state);

                        if (!state.thePredValue) {
                            continue;
                        }
                    }

                    if (theKind == FilterKind.KEYS) {
                        result = FieldDefImpl.Constants.stringDef.
                                 createString(state.theCtxKey);
                    } else  {
                        result = state.theCtxElem;
                    }

                    rcb.setRegVal(theResultReg, result);
                    return true;
                }

                /* There are no more entries in the current map/record. */
                state.theCtxItem = null;
            }

            /*
             * Compute the next context item; either from the input iter or
             * from the top stacked array, if any.
             */
            if (state.theArrays.isEmpty()) {

                boolean more = theInput.next(rcb);

                if (!more) {
                    state.done();
                    return false;
                }

                int inputReg = theInput.getResultReg();
                ctxItem = rcb.getRegVal(inputReg);

                if (ctxItem.isAtomic()) {
                    continue;
                }

                if (ctxItem.isNull()) {
                    state.theCtxItem = ctxItem;
                    rcb.setRegVal(theResultReg, ctxItem);
                    return true;
                }

            } else {

                ArrayAndPos arrayCtx = state.theArrays.peek();
                ArrayValueImpl array = arrayCtx.theArray;

                ctxItem = array.getElement(arrayCtx.thePos);

                ++arrayCtx.thePos;
                if (arrayCtx.thePos >= array.size()) {
                    state.theArrays.pop();
                }

               if (ctxItem.isAtomic()) {
                    continue;
                }
            }

            /*
             * We have a candidate ctx item. If it is an array, stack the
             * array and repeat the loop to get a real ctx item.
             */
            if (ctxItem.isArray()) {
                ArrayValueImpl array = (ArrayValueImpl)ctxItem;
                if (array.size() > 0) {
                    ArrayAndPos arrayCtx = new ArrayAndPos(array);
                    state.theArrays.push(arrayCtx);
                }
                continue;
            }

            if (ctxItem.isRecord() || ctxItem.isTuple()) {
                state.theFieldPos = 0;
                state.theCtxItem = ctxItem;

            } else if (ctxItem.isMap()) {
                MapValueImpl map = (MapValueImpl)ctxItem;
                state.theKeysIter = map.getMap().keySet().iterator();
                state.theCtxItem = ctxItem;

            } else {
                throw new QueryStateException(
                    "Unexpected kind of item : " + ctxItem.getType() +
                    " : " + ctxItem);
            }

            if (state.theComputePredPerMap) {

                computePredExpr(rcb, state);

                if (!state.thePredValue) {
                    state.theCtxItem = null;
                    continue;
                }
            }
        }
    }

    void computePredExpr(RuntimeControlBlock rcb, MapFilterState state) {

        if (thePredIter == null) {
            state.thePredValue = true;
            return;
        }

        thePredIter.reset(rcb);

        if (theCtxItemReg >= 0) {
            rcb.setRegVal(theCtxItemReg, state.theCtxItem);
        }

        if (theCtxElemReg >= 0) {
            rcb.setRegVal(theCtxElemReg, state.theCtxElem);
        }

        if (theCtxKeyReg >= 0) {
            rcb.setRegVal(
                theCtxKeyReg,
                FieldDefImpl.Constants.stringDef.createString(
                    state.theCtxKey));
        }

        boolean more = thePredIter.next(rcb);

        if (!more) {
            state.thePredValue = false;

        } else {
            FieldValueImpl val = rcb.getRegVal(thePredIter.getResultReg());

            if (val.isNull()) {
                state.thePredValue = false;
            } else if (val.isBoolean()) {
                state.thePredValue = val.getBoolean();
            } else {
                throw new QueryException(
                    "Predicate expression in map filter has invalid type:\n" +
                    val.getDefinition().getDDLString(), getLocation());
            }
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInput.reset(rcb);
        if (thePredIter != null) {
            thePredIter.reset(rcb);
        }
        MapFilterState state = (MapFilterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        MapFilterState state = (MapFilterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);
        if (thePredIter != null) {
            thePredIter.close(rcb);
        }
        state.close();
    }

    @Override
    void getParentItemContext(RuntimeControlBlock rcb, ParentItemContext ctx) {
        MapFilterState state = (MapFilterState)rcb.getState(theStatePos);
        ctx.theParentItem = state.theCtxItem;
        ctx.theTargetPos = state.theFieldPos - 1;
        ctx.theTargetKey = state.theCtxKey;
    }

    @Override
    void displayName(StringBuilder sb) {
        sb.append(theKind);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        if (verbose && theCtxItemReg >= 0) {
            formatter.indent(sb);
            sb.append("\"register for $ variable\" : ");
            sb.append(theCtxItemReg);
            sb.append(",\n");
        }

        if (verbose && theCtxElemReg >= 0) {
            formatter.indent(sb);
            sb.append("\"register for $value variable\" : ");
            sb.append(theCtxElemReg);
            sb.append(",\n");
        }

        if (verbose && theCtxKeyReg >= 0) {
            formatter.indent(sb);
            sb.append("\"register for $key variable\" : ");
            sb.append(theCtxKeyReg);
            sb.append(",\n");
        }

        if (thePredIter != null) {
            formatter.indent(sb);
            sb.append("\"predicate iterator\" :\n");
            thePredIter.display(sb, formatter, verbose);
            sb.append(",\n");
        }

        displayInputIter(sb, formatter, verbose, theInput);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof MapFilterIter)) {
            return false;
        }
        final MapFilterIter other = (MapFilterIter) obj;
        return (theKind == other.theKind) &&
            Objects.equals(theInput, other.theInput) &&
            Objects.equals(thePredIter, other.thePredIter) &
            (theCtxItemReg == other.theCtxItemReg) &&
            (theCtxElemReg == other.theCtxElemReg) &&
            (theCtxKeyReg == other.theCtxKeyReg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theKind,
                            theInput,
                            thePredIter,
                            theCtxItemReg,
                            theCtxElemReg,
                            theCtxKeyReg);
    }
}
