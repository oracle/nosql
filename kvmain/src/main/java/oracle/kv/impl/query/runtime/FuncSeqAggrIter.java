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
import java.math.BigDecimal;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;

import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

public class FuncSeqAggrIter extends PlanIter {

    private final PlanIter theInput;

    private final FuncCode theCode;

    public FuncSeqAggrIter(
        Expr e,
        FuncCode code,
        int resultReg,
        PlanIter input) {

        super(e, resultReg);
        theCode = code;
        theInput = input;
    }

    FuncSeqAggrIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FuncCode.VALUES_COUNT);
        theCode = FuncCode.valueOf(ordinal);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIter(theInput, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SEQ_AGGR;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new AggrIterState());
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
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
    public boolean next(RuntimeControlBlock rcb) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInput.next(rcb);

        if (!more) {
            state.done();

            if (theCode == FuncCode.FN_SEQ_COUNT ||
                theCode == FuncCode.FN_SEQ_COUNT_I) {
                rcb.setRegVal(theResultReg,
                              FieldDefImpl.Constants.longDef.createLong(0));
                return true;
            }

            return false;
        }

        switch (theCode) {
        case FN_SEQ_COUNT:
        case FN_SEQ_COUNT_I:
            nextCount(rcb, state);
            break;
        case FN_SEQ_COUNT_NUMBERS_I:
            nextCountNumbers(rcb, state);
            break;
        case FN_SEQ_SUM:
        case FN_SEQ_AVG:
            nextSumAvg(rcb, state);
            break;
        case FN_SEQ_MIN:
        case FN_SEQ_MAX:
        case FN_SEQ_MIN_I:
        case FN_SEQ_MAX_I:
            nextMinMax(rcb, state);
            break;
        default:
            throw new QueryStateException("Unexpected function: " + theCode);
        }

        state.done();
        return true;
    }

    private void nextCount(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull()) {
                if (theCode == FuncCode.FN_SEQ_COUNT) {
                    rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                    return;
                }
                more = theInput.next(rcb);
                continue;
            }

            ++state.theCount;
            more = theInput.next(rcb);
        }

        rcb.setRegVal(
            theResultReg,
            FieldDefImpl.Constants.longDef.createLong(state.theCount));
    }

    private void nextCountNumbers(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNumeric()) {
                ++state.theCount;
            }

            more = theInput.next(rcb);
        }

        rcb.setRegVal(
            theResultReg,
            FieldDefImpl.Constants.longDef.createLong(state.theCount));
    }

    private void nextSumAvg(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            FuncSumIter.sumNewValue(rcb, state, val);

            more = theInput.next(rcb);
        }

        if (!state.theGotNumericInput) {
             rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
             return;
        }

        FieldValueImpl res = null;

        if (theCode == FuncCode.FN_SEQ_SUM) {

            switch (state.theSumType) {
            case LONG:
                res = FieldDefImpl.Constants.longDef.createLong(
                    state.theLongSum);
                break;
            case DOUBLE:
                res = FieldDefImpl.Constants.doubleDef.createDouble(
                    state.theDoubleSum);
                break;
            case NUMBER:
                res = FieldDefImpl.Constants.numberDef.createNumber(
                    state.theNumberSum);
                break;
            default:
                throw new QueryStateException(
                    "Unexpected result type for SUM function: " +
                    state.theSumType);
            }
        } else {
            double avg;

            switch (state.theSumType) {
            case LONG:
                avg = state.theLongSum / (double)state.theCount;
                res = FieldDefImpl.Constants.doubleDef.createDouble(avg);
                break;
            case DOUBLE:
                avg = state.theDoubleSum / state.theCount;
                res = FieldDefImpl.Constants.doubleDef.createDouble(avg);
                break;
            case NUMBER:
                BigDecimal bcount = new BigDecimal(state.theCount);
                BigDecimal bavg = state.theNumberSum.
                    divide(bcount, rcb.getMathContext());
                res = FieldDefImpl.Constants.numberDef.createNumber(bavg);
                break;
            default:
                throw new QueryStateException(
                    "Unexpected result type for SUM function: " +
                    state.theSumType);
            }
        }

        rcb.setRegVal(theResultReg, res);
    }

    private void nextMinMax(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull() &&
                (theCode == FuncCode.FN_SEQ_MIN ||
                 theCode == FuncCode.FN_SEQ_MAX)) {
                rcb.setRegVal(theResultReg, val);
                return;
            }

            FuncMinMaxIter.minmaxNewVal(rcb, state, theCode, val);

            more = theInput.next(rcb);
        }

        rcb.setRegVal(theResultReg, state.theMinMax);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theInput);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncSeqAggrIter)) {
            return false;
        }
        final FuncSeqAggrIter other = (FuncSeqAggrIter) obj;
        return Objects.equals(theInput, other.theInput) &&
            (theCode == other.theCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theInput, theCode);
    }
}
