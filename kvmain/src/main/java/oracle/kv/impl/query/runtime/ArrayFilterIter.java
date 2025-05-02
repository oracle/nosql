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
import java.util.Objects;

import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;

/**
 *
 */
public class ArrayFilterIter extends PlanIter {

    static private class ArrayFilterState extends PlanIterState {

        FieldValueImpl theCtxItem;

        int theCtxItemSize;

        int theCtxPos;

        ArrayValueImpl theSingletonArray;

        int theBoolPredValue;

        long theNumPredValue;

        boolean theComputePredOnce;

        boolean theComputePredPerArray;

        boolean theComputePredPerElem;

        ArrayFilterState(ArrayFilterIter iter) {

            theComputePredOnce = (iter.theCtxItemReg < 0 &&
                                  iter.theCtxElemReg < 0 &&
                                  iter.theCtxElemPosReg < 0);

            theComputePredPerArray = (iter.theCtxItemReg >= 0 &&
                                      iter.theCtxElemReg < 0 &&
                                      iter.theCtxElemPosReg < 0);

            theComputePredPerElem = (iter.theCtxElemReg >= 0 ||
                                     iter.theCtxElemPosReg >= 0);

            theSingletonArray =
                FieldDefImpl.Constants.arrayAnyDef.createArray();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCtxItem = null;
            theCtxItemSize = 0;
            theCtxPos = 0;
            theBoolPredValue = -1;
        }

        @Override
        public void close() {
            super.close();
            theCtxItem = null;
            theSingletonArray = null;
        }
    }

    private final PlanIter theInputIter;

    private final PlanIter thePredIter;

    private final int theCtxItemReg;

    private final int theCtxElemReg;

    private final int theCtxElemPosReg;

    public ArrayFilterIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        PlanIter predIter,
        int ctxItemReg,
        int ctxElemReg,
        int ctxElemPosReg) {

        super(e, resultReg);
        theInputIter = inputIter;
        thePredIter = predIter;
        theCtxItemReg = ctxItemReg;
        theCtxElemReg = ctxElemReg;
        theCtxElemPosReg = ctxElemPosReg;
    }

    /**
     * FastExternalizable constructor.
     */
    ArrayFilterIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theCtxItemReg = readPositiveInt(in, true);
        theCtxElemReg = readPositiveInt(in, true);
        theCtxElemPosReg = readPositiveInt(in, true);
        theInputIter = deserializeIter(in, serialVersion);
        thePredIter = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(theCtxItemReg);
        out.writeInt(theCtxElemReg);
        out.writeInt(theCtxElemPosReg);
        serializeIter(theInputIter, out, serialVersion);
        serializeIter(thePredIter, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARRAY_FILTER;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new ArrayFilterState(this));
        theInputIter.open(rcb);
        if (thePredIter != null) {
            thePredIter.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        ArrayFilterState state = (ArrayFilterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.theComputePredOnce && state.isOpen()) {

            state.setState(StateEnum.RUNNING);

            computePredExpr(rcb, state);

            if (state.theBoolPredValue == 0) {
                state.done();
                return false;
            }
        }

        FieldValueImpl ctxItem;

        while(true) {

            if (state.theCtxItem == null || state.theCtxItem.isNull()) {

                /*
                 * Compute the next context item. If it is NULL, return NULL.
                 * If it is not an array, put it in theSingletonArray. If it
                 * is an empty array, skip it. Else, initialize iteration over
                 * the array.
                 */
                boolean more = theInputIter.next(rcb);

                if (!more) {
                    state.done();
                    return false;
                }

                ctxItem = rcb.getRegVal(theInputIter.getResultReg());

                if (ctxItem.isNull()) {
                    state.theCtxItem = ctxItem;
                    rcb.setRegVal(theResultReg, ctxItem);
                    return true;
                }

                if (!ctxItem.isArray()) {
                    state.theSingletonArray.clear();
                    state.theSingletonArray.addInternal(ctxItem);
                    ctxItem = state.theSingletonArray;
                    state.theCtxItemSize = 1;
                } else {
                    state.theCtxItemSize = ctxItem.size();
                    if (state.theCtxItemSize == 0) {
                        continue;
                    }
                }

                state.theCtxItem = ctxItem;
                state.theCtxPos = 0;

                /*
                 * Compute the filtering predicate, if not done already. If the
                 * pred value is false, we skip this array immediately. We
                 * can also skip this array if the pred value is a number < 0 or
                 * >= size of the array.
                 **/
                if (!state.theComputePredPerElem) {

                    if (state.theComputePredPerArray) {

                        computePredExpr(rcb, state);

                        if (state.theBoolPredValue == 0) {
                            state.theCtxItem = null;
                            continue;
                        }
                    }

                    if (state.theBoolPredValue < 0) {

                        if (state.theNumPredValue < 0 ||
                            state.theNumPredValue >= state.theCtxItemSize) {
                            state.theCtxItem = null;
                            continue;
                        }
                    }
                }
            } else {
                ctxItem = state.theCtxItem;
            }

            /*
             * We have processed all the elements/entries of the current
             * ctx item, so proceed with the next one.
             */
            if (state.theCtxPos >= state.theCtxItemSize) {
                state.theCtxItem = null;
                continue;
            }

            if (state.theComputePredPerElem) {

                computePredExpr(rcb, state);

                if (state.theBoolPredValue == 0) {
                    ++state.theCtxPos;
                    continue;
                }
            } else {
                assert(state.theBoolPredValue != 0);
            }

            /*
             * We have either a true boolean pred or a positional pred. In the
             * formar case return the current item.
             */
            if (state.theBoolPredValue == 1) {

                FieldValueImpl elem;
                elem = ((ArrayValueImpl)state.theCtxItem).
                    getElement(state.theCtxPos);

                ++state.theCtxPos;

                rcb.setRegVal(theResultReg, elem);
                return true;
            }

            /*
             * It's a positional pred. If the pred depends on the current
             * element, compute it and then skip/return the current element
             * if its position is equal/not-equal with the computed position.
             * Otherwise, return the item at the computed position and set
             * state.theCtxItemSize and state.theCtxPos in a way that will
             * cause the next next() call to skip the current element (we
             * don't set state.thectxItem to null, because it may needed by
             * the parent of this iter).
             */
            if (state.theComputePredPerElem) {

                if (state.theNumPredValue != state.theCtxPos) {
                    ++state.theCtxPos;
                    continue;
                }

                ++state.theCtxPos;

            } else {
                state.theCtxItemSize = 0;
                state.theCtxPos = (int)state.theNumPredValue + 1;
            }

            FieldValueImpl res = ctxItem.getElement((int)state.theNumPredValue);
            rcb.setRegVal(theResultReg, res);
            return true;
        }
    }

    void computePredExpr(RuntimeControlBlock rcb, ArrayFilterState state) {

        if (thePredIter == null) {
            state.theBoolPredValue = 1;
            return;
        }

        state.theBoolPredValue = -1;

        thePredIter.reset(rcb);

        if (theCtxItemReg >= 0) {
            rcb.setRegVal(theCtxItemReg, state.theCtxItem);
        }

        if (theCtxElemReg >= 0) {
            rcb.setRegVal(
                theCtxElemReg,
                ((ArrayValueImpl)state.theCtxItem).getElement(state.theCtxPos));
        }

        if (theCtxElemPosReg >= 0) {
            rcb.setRegVal(
                theCtxElemPosReg,
                FieldDefImpl.Constants.integerDef.createInteger(
                    state.theCtxPos));
        }

        boolean more = thePredIter.next(rcb);

        if (!more) {
            state.theBoolPredValue = 0;

        } else {
            FieldValueImpl val = rcb.getRegVal(thePredIter.getResultReg());

            if (val.isNull()) {
                state.theBoolPredValue = 0;
            } else if (val.isBoolean()) {
                state.theBoolPredValue = (val.getBoolean() ? 1 : 0);
            } else if (val.isLong() || val.isInteger()) {
                state.theNumPredValue = val.getLong();
            } else {
                throw new QueryException(
                    "Predicate expression in array filter has invalid type:\n" +
                    val.getDefinition().getDDLString(), getLocation());
            }
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        if (thePredIter != null) {
            thePredIter.reset(rcb);
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

        theInputIter.close(rcb);
        if (thePredIter != null) {
            thePredIter.close(rcb);
        }

        state.close();
    }

    @Override
    void getParentItemContext(RuntimeControlBlock rcb, ParentItemContext ctx) {
        ArrayFilterState state = (ArrayFilterState)rcb.getState(theStatePos);

        if (state.theCtxItem == state.theSingletonArray) {
            theInputIter.getParentItemContext(rcb, ctx);
        } else {
            ctx.theParentItem = state.theCtxItem;
            ctx.theTargetPos = state.theCtxPos - 1;
            ctx.theTargetKey = null;
        }
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
            sb.append("\"register for $element variable\" : ");
            sb.append(theCtxElemReg);
            sb.append(",\n");
        }

        if (verbose && theCtxElemPosReg >= 0) {
            formatter.indent(sb);
            sb.append("\"register for $pos variable\" : ");
            sb.append(theCtxElemPosReg);
            sb.append(",\n");
        }

        if (thePredIter != null) {
            formatter.indent(sb);
            sb.append("\"predicate iterator\" :\n");
            thePredIter.display(sb, formatter, verbose);
            sb.append(",\n");
        }

        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ArrayFilterIter)) {
            return false;
        }
        final ArrayFilterIter other = (ArrayFilterIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            Objects.equals(thePredIter, other.thePredIter) &&
            (theCtxItemReg == other.theCtxItemReg) &&
            (theCtxElemReg == other.theCtxElemReg) &&
            (theCtxElemPosReg == other.theCtxElemPosReg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theInputIter,
                            thePredIter,
                            theCtxItemReg,
                            theCtxElemReg,
                            theCtxElemPosReg);
    }
}
