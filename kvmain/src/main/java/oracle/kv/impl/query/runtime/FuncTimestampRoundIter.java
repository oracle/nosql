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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_15;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.api.table.TimestampUtils.RoundMode;
import oracle.kv.impl.api.table.TimestampUtils.RoundUnit;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.Expr.ConstKind;


public class FuncTimestampRoundIter extends PlanIter {
    private final FunctionLib.FuncCode theCode;
    private final PlanIter theTimestampArg;
    private final PlanIter theUnitArg;
    private final boolean theIsUnitConst;

    private static class FuncTimestampRoundState extends PlanIterState {
        RoundUnit theUnit;
    }

    public FuncTimestampRoundIter(Expr e,
                                  int resultReg,
                                  FunctionLib.FuncCode code,
                                  PlanIter arg1,
                                  PlanIter arg2) {

        super(e, resultReg);
        theCode = code;
        theTimestampArg = arg1;
        theUnitArg = arg2;
        if (theUnitArg != null) {
            theIsUnitConst = ConstKind.isConst(((ExprFuncCall)e).getArg(1));
        } else {
            theIsUnitConst = false;
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncTimestampRoundIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.VALUES_COUNT);
        theCode = FunctionLib.FuncCode.valueOf(ordinal);
        theTimestampArg = deserializeIter(in, serialVersion);
        theUnitArg = deserializeIter(in, serialVersion);
        theIsUnitConst = in.readBoolean();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        if (serialVersion < QUERY_VERSION_15) {
            throw new IllegalStateException("Serial version " + serialVersion +
                " does not support round timestamp function " + theCode +
                ", must be " + QUERY_VERSION_15 + " or greater");
        }

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIter(theTimestampArg, out, serialVersion);
        serializeIter(theUnitArg, out, serialVersion);
        out.writeBoolean(theIsUnitConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_TIMESTAMP_ROUND;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FuncTimestampRoundState());
        theTimestampArg.open(rcb);
        if (theUnitArg != null) {
            theUnitArg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        FuncTimestampRoundState state =
                (FuncTimestampRoundState)rcb.getState(theStatePos);
        if (state.isDone()) {
            return false;
        }

        boolean more = theTimestampArg.next(rcb);
        if (!more) {
            state.done();
            return false;
        }

        FieldValueImpl tsVal = rcb.getRegVal(theTimestampArg.getResultReg());
        if (tsVal.isNull()) {
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        if (!tsVal.isTimestamp()) {
            try {
                tsVal = CastIter.castValue(tsVal,
                                           FieldDefImpl.Constants.timestampDef,
                                           theLocation);
            } catch (QueryException ex) {
                /* return NULL if the input is not castable to timestamp */
                rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                state.done();
                return true;
            }
        }

        RoundUnit unit = null;
        if (theUnitArg != null) {
            if (state.theUnit == null) {
                more = theUnitArg.next(rcb);
                if (more) {
                    FieldValueImpl unitVal =
                        rcb.getRegVal(theUnitArg.getResultReg());

                    if (unitVal.isNull()) {
                        /* return NULL if the input unit is null */
                        rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                        state.done();
                        return true;
                    }

                    String unitStr = unitVal.castAsString();
                    try {
                        unit = RoundUnit.valueOf(unitStr.toUpperCase());
                        if (theIsUnitConst) {
                            state.theUnit = unit;
                        }
                    } catch (IllegalArgumentException iae) {
                        throw new QueryException("Invalid unit '" + unitStr +
                                "', supported units: " +
                                Arrays.toString(RoundUnit.values()),
                                getLocation());
                    }
                }
            } else {
                unit = state.theUnit;
            }
        }

        Timestamp ts = TimestampUtils.round(tsVal.getTimestamp(), unit,
                                            mapToRoundMode(theCode));

        FieldValueImpl res = FieldDefImpl.Constants
                                 .timestampDefs[0].createTimestamp(ts);
        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    private static RoundMode mapToRoundMode(FunctionLib.FuncCode code) {
        switch (code) {
        case FN_TIMESTAMP_CEIL:
            return RoundMode.UP;
        case FN_TIMESTAMP_FLOOR:
        case FN_TIMESTAMP_TRUNC:
            return RoundMode.DOWN;
        case FN_TIMESTAMP_ROUND:
            return RoundMode.HALF_UP;
        default:
            throw new QueryException(
                "Unknown function code for rounding timestamp");
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theTimestampArg.reset(rcb);
        if (theUnitArg != null) {
            theUnitArg.reset(rcb);
        }

        FuncTimestampRoundState state =
            (FuncTimestampRoundState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theTimestampArg.close(rcb);
        if (theUnitArg != null) {
            theUnitArg.close(rcb);
        }

        state.close();
    }

    @Override
    void displayContent(StringBuilder sb,
                        DisplayFormatter formatter,
                        boolean verbose) {

        if (theUnitArg == null) {
            displayInputIter(sb, formatter, verbose, theTimestampArg);
        } else {
            PlanIter[] args = new PlanIter[2];
            args[0] = theTimestampArg;
            args[1] = theUnitArg;
            displayInputIters(sb, formatter, verbose, args);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncTimestampRoundIter)) {
            return false;
        }
        final FuncTimestampRoundIter other = (FuncTimestampRoundIter) obj;
        return Objects.equals(theTimestampArg, other.theTimestampArg) &&
               Objects.equals(theUnitArg, other.theUnitArg) &&
               (theIsUnitConst == other.theIsUnitConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theTimestampArg,
                            theUnitArg,
                            theIsUnitConst);
    }
}
