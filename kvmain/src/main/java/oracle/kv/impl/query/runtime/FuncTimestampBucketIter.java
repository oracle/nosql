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
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.api.table.TimestampUtils.BucketInterval;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.Expr.ConstKind;

public class FuncTimestampBucketIter extends PlanIter {
    private final PlanIter theTimestampArg;
    private final PlanIter theIntervalArg;
    private final boolean theIsIntervalConst;
    private final PlanIter theOriginArg;
    private final boolean theIsOriginConst;

    private static class FuncTimestampBucketState extends PlanIterState {
        BucketInterval theInterval;
        Timestamp theOrigin;
    }

    public FuncTimestampBucketIter(Expr e,
                                   int resultReg,
                                   PlanIter arg1,
                                   PlanIter arg2,
                                   PlanIter arg3) {

        super(e, resultReg);
        theTimestampArg = arg1;
        theIntervalArg = arg2;
        if (theIntervalArg != null) {
            theIsIntervalConst = ConstKind.isConst(((ExprFuncCall)e).getArg(1));
        } else {
            theIsIntervalConst = false;
        }

        theOriginArg = arg3;
        if (theOriginArg != null) {
            theIsOriginConst = ConstKind.isConst(((ExprFuncCall)e).getArg(2));
        } else {
            theIsOriginConst = false;
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncTimestampBucketIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theTimestampArg = deserializeIter(in, serialVersion);
        theIntervalArg = deserializeIter(in, serialVersion);
        theIsIntervalConst = in.readBoolean();
        theOriginArg = deserializeIter(in, serialVersion);
        theIsOriginConst = in.readBoolean();
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
                " does not support timestamp_bucket function, must be " +
                QUERY_VERSION_15 + " or greater");
        }

        super.writeFastExternal(out, serialVersion);
        serializeIter(theTimestampArg, out, serialVersion);
        serializeIter(theIntervalArg, out, serialVersion);
        out.writeBoolean(theIsIntervalConst);
        serializeIter(theOriginArg, out, serialVersion);
        out.writeBoolean(theIsOriginConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_TIMESTAMP_BUCKET;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_TIMESTAMP_BUCKET;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new FuncTimestampBucketState());
        theTimestampArg.open(rcb);
        if (theIntervalArg != null) {
            theIntervalArg.open(rcb);
        }
        if (theOriginArg != null) {
            theOriginArg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        FuncTimestampBucketState state =
            (FuncTimestampBucketState)rcb.getState(theStatePos);
        if (state.isDone()) {
            return false;
        }

        boolean more = theTimestampArg.next(rcb);
        if (!more) {
            state.done();
            return false;
        }

        /* the timestamp */
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

        /* the interval */
        BucketInterval interval = null;
        if (theIntervalArg != null) {
            if (state.theInterval == null) {
                more = theIntervalArg.next(rcb);
                if (more) {
                    FieldValueImpl intervalVal =
                        rcb.getRegVal(theIntervalArg.getResultReg());

                    if (intervalVal.isNull()) {
                        /* return NULL if the input interval is null */
                        rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                        state.done();
                        return true;
                    }

                    String intervalStr = intervalVal.castAsString();
                    try {
                        interval = BucketInterval.parseString(intervalStr);
                        if (theIsIntervalConst) {
                            state.theInterval = interval;
                        }
                    } catch (IllegalArgumentException iae) {
                        throw new QueryException(iae.getMessage(), iae,
                                                 getLocation());
                    }
                }
            } else {
                interval = state.theInterval;
            }
        }

        /* the origin timestamp */
        Timestamp origin = null;
        if (theOriginArg != null) {
            if (state.theOrigin == null) {
                more = theOriginArg.next(rcb);
                if (more) {
                    FieldValueImpl originVal =
                        rcb.getRegVal(theOriginArg.getResultReg());

                    if (originVal.isNull()) {
                        /* return NULL if the input origin is null */
                        rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
                        state.done();
                        return true;
                    }

                    originVal = CastIter.castValue(
                                    originVal,
                                    FieldDefImpl.Constants.timestampDef,
                                    theLocation);
                    origin = originVal.getTimestamp();
                    if (theIsOriginConst) {
                        state.theOrigin = originVal.asTimestamp().get();
                    }
                }
            } else {
                origin = state.theOrigin;
            }
        }

        Timestamp ts = TimestampUtils.getCurrentBucket(tsVal.getTimestamp(),
                                                       interval, origin);

        int precision = (origin != null) ? 9 : 0;
        FieldValueImpl res = FieldDefImpl.Constants
                                 .timestampDefs[precision].createTimestamp(ts);
        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theTimestampArg.reset(rcb);
        if (theIntervalArg != null) {
            theIntervalArg.reset(rcb);
        }
        if (theOriginArg != null) {
            theOriginArg.reset(rcb);
        }

        FuncTimestampBucketState state =
            (FuncTimestampBucketState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theTimestampArg.close(rcb);
        if (theIntervalArg != null) {
            theIntervalArg.close(rcb);
        }
        if (theOriginArg != null) {
            theOriginArg.close(rcb);
        }
        state.close();
    }

    @Override
    void displayContent(StringBuilder sb,
                        DisplayFormatter formatter,
                        boolean verbose) {

        if (theIntervalArg == null) {
            displayInputIter(sb, formatter, verbose, theTimestampArg);
        } else {
            PlanIter[] args = new PlanIter[(theOriginArg == null ? 2 : 3)];
            args[0] = theTimestampArg;
            args[1] = theIntervalArg;
            if (theOriginArg != null) {
                args[2] = theOriginArg;
            }
            displayInputIters(sb, formatter, verbose, args);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncTimestampBucketIter)) {
            return false;
        }
        final FuncTimestampBucketIter other = (FuncTimestampBucketIter) obj;
        return Objects.equals(theTimestampArg, other.theTimestampArg) &&
               Objects.equals(theIntervalArg, other.theIntervalArg) &&
               (theIsIntervalConst == other.theIsIntervalConst) &&
               Objects.equals(theOriginArg, other.theOriginArg) &&
               (theIsOriginConst == other.theIsOriginConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theTimestampArg,
                            theIntervalArg,
                            theIsIntervalConst,
                            theOriginArg,
                            theIsOriginConst);
    }
}
