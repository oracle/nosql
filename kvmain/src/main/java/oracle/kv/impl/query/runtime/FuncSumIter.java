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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.DoubleValueImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.FloatValueImpl;
import oracle.kv.impl.api.table.IntegerValueImpl;
import oracle.kv.impl.api.table.LongValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.NumberValueImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.table.FieldDef.Type;

/**
 *  any_atomic sum(any*)
 *
 * Note: The next() method does not actually return a value; it just adds a new
 * value (if it is of a numeric type) to the running sum kept in the state. Also
 * the reset() method resets the input iter (so that the next input value can be
 * computed), but does not reset the FuncSumState. The state is reset, and the
 * current sum value is returned, by the getAggrValue() method.
 */
public class FuncSumIter extends SingleInputPlanIter {

    private final PlanIter theInput;

    public FuncSumIter(
        Expr e,
        int resultReg,
        PlanIter input,
        boolean forCloud) {

        super(e, resultReg, forCloud);
        theInput = input;
    }

    FuncSumIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
    }

    @Override
    protected PlanIter getInput() {
        return theInput;
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);
        theInput.writeForCloud(out, driverVersion, valWriter);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_SUM;
    }

    @Override
    public PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new AggrIterState());
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

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            boolean more = theInput.next(rcb);

            if (!more) {
                return true;
            }

            FieldValueImpl val = rcb.getRegVal(theInput.getResultReg());

            if (rcb.getTraceLevel() >= 4) {
                rcb.trace("Summing up value " + val);
            }

            sumNewValue(rcb, state, val);
        }
    }

    static void sumNewValue(
        RuntimeControlBlock rcb,
        AggrIterState state,
        FieldValueImpl val) {

        BigDecimal bd;

        if (val.isNull()) {
            return;
        }

        switch (val.getType()) {
        case INTEGER: {
            state.theGotNumericInput = true;
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theLongSum += ((IntegerValueImpl)val).get();
                break;
            case DOUBLE:
                state.theDoubleSum += ((IntegerValueImpl)val).get();
                break;
            case NUMBER:
                bd = new BigDecimal(((IntegerValueImpl)val).get());
                state.theNumberSum =
                    state.theNumberSum.add(bd, rcb.getMathContext());
                break;
            default:
                assert(false);
            }
            break;
        }
        case LONG: {
            state.theGotNumericInput = true;
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theLongSum += ((LongValueImpl)val).get();
                break;
            case DOUBLE:
                state.theDoubleSum += ((LongValueImpl)val).get();
                break;
            case NUMBER:
                bd = new BigDecimal(((LongValueImpl)val).get());
                state.theNumberSum =
                    state.theNumberSum.add(bd, rcb.getMathContext());
                break;
            default:
                assert(false);
            }
            break;
        }
        case FLOAT: {
            state.theGotNumericInput = true;
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theDoubleSum = state.theLongSum;
                state.theDoubleSum += ((FloatValueImpl)val).get();
                state.theSumType = Type.DOUBLE;
                break;
            case DOUBLE:
                state.theDoubleSum += ((FloatValueImpl)val).get();
                break;
            case NUMBER:
                bd = new BigDecimal(((FloatValueImpl)val).get());
                state.theNumberSum =
                    state.theNumberSum.add(bd, rcb.getMathContext());
                break;
            default:
                assert(false);
            }
            break;
        }
        case DOUBLE: {
            state.theGotNumericInput = true;
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theDoubleSum = state.theLongSum;
                state.theDoubleSum += ((DoubleValueImpl)val).get();
                state.theSumType = Type.DOUBLE;
                break;
            case DOUBLE:
                state.theDoubleSum += ((DoubleValueImpl)val).get();
                break;
            case NUMBER:
                bd = new BigDecimal(((DoubleValueImpl)val).get());
                state.theNumberSum =
                    state.theNumberSum.add(bd, rcb.getMathContext());
                break;
            default:
                assert(false);
            }
            break;
        }
        case NUMBER: {
            state.theGotNumericInput = true;
            ++state.theCount;
            if (state.theNumberSum == null) {
                state.theNumberSum = new BigDecimal(0);
            }
            
            switch (state.theSumType) {
            case LONG:
                state.theNumberSum =  new BigDecimal(state.theLongSum);
                state.theNumberSum =
                    state.theNumberSum.add(((NumberValueImpl)val).get(),
                                           rcb.getMathContext());
                state.theSumType = Type.NUMBER;
                break;
            case DOUBLE:
                state.theNumberSum =  new BigDecimal(state.theDoubleSum);
                state.theNumberSum =
                    state.theNumberSum.add(((NumberValueImpl)val).get(),
                                           rcb.getMathContext());
                state.theSumType = Type.NUMBER;
                break;
            case NUMBER:
                state.theNumberSum =
                    state.theNumberSum.add(((NumberValueImpl)val).get(),
                                            rcb.getMathContext());
                break;
            default:
                assert(false);
            }
            break;
        }
        default:
            break;
        }
    }

    @Override
    void aggregate(RuntimeControlBlock rcb, FieldValueImpl val) {
        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        sumNewValue(rcb, state, val);
    }

    /*
     * Called during SFWIter.open(), when the open() is actually a resume
     * operation, in which case a partially computed GB tuple is sent back
     * to the RNs from the client. 
     */
    @Override
    void initAggrValue(RuntimeControlBlock rcb, FieldValueImpl val) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        if (val == null) {
            state.theLongSum = 0;
            state.theSumType = Type.LONG;
            return;
        }

        if (val.isNull()) {
            return;
        }

        state.theGotNumericInput = true;

        switch (val.getType()) {
        case LONG:
            state.theLongSum = ((LongValueImpl)val).get();
            state.theSumType = Type.LONG;
            break;
        case DOUBLE:
            state.theDoubleSum = ((DoubleValueImpl)val).get();
            state.theSumType = Type.DOUBLE;
            break;
        case NUMBER:
            state.theNumberSum  = ((NumberValueImpl)val).get();
            state.theSumType = Type.NUMBER;
            break;
        default:
            throw new QueryStateException(
                "Unexpected result type for SUM function: " + val.getType());
        }
    }

    /*
     * This method is called twice when a group completes and a new group
     * starts. In both cases it returns the current value of the SUM that is
     * stored in the FuncSumState. The 1st time, the SUM value is the final
     * SUM value for the just completed group. In this case the "reset" param
     * is true in order to reset the running sum in the state. The 2nd time
     * the SUM value is the inital SUM value computed from the 1st tuple of
     * the new group.
     */
    @Override
    FieldValueImpl getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        FieldValueImpl res = null;

        if (!state.theGotNumericInput) {
            return NullValueImpl.getInstance();
        }

        switch (state.theSumType) {
        case LONG:
            res = FieldDefImpl.Constants.longDef.createLong(state.theLongSum);
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
                "Unexpected result type for SUM function: " + state.theSumType);
        }

        if (rcb.getTraceLevel() >= 3) {
            rcb.trace("Computed sum = " + res);
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
        displayInputIter(sb, formatter, verbose, theInput);
    }
}
