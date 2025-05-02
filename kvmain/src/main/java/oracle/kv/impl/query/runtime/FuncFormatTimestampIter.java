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
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.api.table.TimestampValueImpl;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.Expr.ConstKind;

/* format_timestamp(<timestamp>[, fmt]) */
public class FuncFormatTimestampIter extends PlanIter {

    private final PlanIter theTimestampArg;
    private final PlanIter thePatternArg;
    private final boolean theIsPatternConst;
    private final PlanIter theZoneArg;
    private final boolean theIsZoneConst;

    private static class FuncFormatTimestampState extends PlanIterState {
        String thePattern;
        String theZone;
    }

    public FuncFormatTimestampIter(
        Expr e,
        int resultReg,
        PlanIter timestampArg,
        PlanIter patternArg,
        PlanIter zoneArg) {

        super(e, resultReg);
        theTimestampArg = timestampArg;
        thePatternArg = patternArg;
        if (thePatternArg != null) {
            theIsPatternConst = ConstKind.isConst(((ExprFuncCall)e).getArg(1));
        } else {
            theIsPatternConst = true;
        }

        theZoneArg = zoneArg;
        if (theZoneArg != null) {
            theIsZoneConst = ConstKind.isConst(((ExprFuncCall)e).getArg(2));
        } else {
            theIsZoneConst = true;
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncFormatTimestampIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theTimestampArg = deserializeIter(in, serialVersion);
        thePatternArg = deserializeIter(in, serialVersion);
        theIsPatternConst = in.readBoolean();
        theZoneArg = deserializeIter(in, serialVersion);
        theIsZoneConst = in.readBoolean();
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
                " does not support format_timestamp() function, must be " +
                QUERY_VERSION_15 + " or greater");
        }

        super.writeFastExternal(out, serialVersion);
        serializeIter(theTimestampArg, out, serialVersion);
        serializeIter(thePatternArg, out, serialVersion);
        out.writeBoolean(theIsPatternConst);
        serializeIter(theZoneArg, out, serialVersion);
        out.writeBoolean(theIsZoneConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_FORMAT_TIMESTAMP;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_FORMAT_TIMESTAMP;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncFormatTimestampState());
        theTimestampArg.open(rcb);
        if (thePatternArg != null) {
            thePatternArg.open(rcb);
        }
        if (theZoneArg != null) {
            theZoneArg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        FuncFormatTimestampState state =
            (FuncFormatTimestampState)rcb.getState(theStatePos);
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

        String pattern = null;
        String zone = null;

        if (thePatternArg != null) {
            if (state.thePattern == null) {
                more = thePatternArg.next(rcb);
                if (more) {
                    FieldValueImpl patternVal =
                        rcb.getRegVal(thePatternArg.getResultReg());
                    if (patternVal.isNull()) {
                        /* return NULL if the input pattern is null */
                        rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                        state.done();
                        return true;
                    }
                    pattern = patternVal.castAsString();

                    if (theIsPatternConst) {
                        state.thePattern = pattern;
                    }
                }
            } else {
                pattern = state.thePattern;
            }
        }

        if (theZoneArg != null) {
            if (state.theZone == null) {
                more = theZoneArg.next(rcb);
                if (more) {
                    FieldValueImpl zoneVal =
                        rcb.getRegVal(theZoneArg.getResultReg());

                    if (zoneVal.isNull()) {
                        /* return NULL if the input zone is null */
                        rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                        state.done();
                        return true;
                    }

                    zone = zoneVal.castAsString();

                    if (theIsZoneConst) {
                        state.theZone = zone;
                    }
                }
            } else {
                zone = state.theZone;
            }
        }

        String resStr = TimestampUtils.formatString((TimestampValueImpl)tsVal,
                                                    pattern, zone);
        FieldValueImpl res = FieldDefImpl.Constants.stringDef
                                .createString(resStr);

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theTimestampArg.reset(rcb);
        if (thePatternArg != null) {
            thePatternArg.reset(rcb);
        }

        if (theZoneArg != null) {
            theZoneArg.reset(rcb);
        }

        FuncFormatTimestampState state =
            (FuncFormatTimestampState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theTimestampArg.close(rcb);
        if (thePatternArg != null) {
            thePatternArg.close(rcb);
        }

        if (theZoneArg != null) {
            theZoneArg.close(rcb);
        }

        state.close();
    }

    @Override
    void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        if (thePatternArg == null) {
            displayInputIter(sb, formatter, verbose, theTimestampArg);
        } else {
            PlanIter[] args = new PlanIter[((theZoneArg != null) ? 3 : 2)];
            args[0] = theTimestampArg;
            args[1] = thePatternArg;
            if (theZoneArg != null) {
                args[2] = theZoneArg;
            }
            displayInputIters(sb, formatter, verbose, args);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncFormatTimestampIter)) {
            return false;
        }
        final FuncFormatTimestampIter other = (FuncFormatTimestampIter) obj;
        return Objects.equals(theTimestampArg, other.theTimestampArg) &&
               Objects.equals(thePatternArg, other.thePatternArg) &&
               (theIsPatternConst == other.theIsPatternConst) &&
               Objects.equals(theZoneArg, other.theZoneArg) &&
               (theIsZoneConst == other.theIsZoneConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theTimestampArg,
                            thePatternArg,
                            theIsPatternConst,
                            theZoneArg,
                            theIsZoneConst);
    }
}
