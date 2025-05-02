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
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.ExprFuncCall;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.Expr.ConstKind;

/* parse_to_timestamp(<string>[, fmt]) */
public class FuncParseToTimestampIter extends PlanIter {

    private final PlanIter theStringArg;
    private final PlanIter thePatternArg;
    private final boolean theIsPatternConst;

    private static class FuncParseToTimestampState extends PlanIterState {
        String thePattern;
    }

    public FuncParseToTimestampIter(Expr e,
                                    int resultReg,
                                    PlanIter arg1,
                                    PlanIter arg2) {

        super(e, resultReg);
        theStringArg = arg1;
        thePatternArg = arg2;
        if (thePatternArg != null) {
            theIsPatternConst = ConstKind.isConst(((ExprFuncCall)e).getArg(1));
        } else {
            theIsPatternConst = true;
        }
    }

    /**
     * FastExternalizable constructor.
     */
    public FuncParseToTimestampIter(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theStringArg = deserializeIter(in, serialVersion);
        thePatternArg = deserializeIter(in, serialVersion);
        theIsPatternConst = in.readBoolean();
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
                " does not support parse_to_timestamp() function, must be " +
                QUERY_VERSION_15 + " or greater");
        }

        super.writeFastExternal(out, serialVersion);
        serializeIter(theStringArg, out, serialVersion);
        serializeIter(thePatternArg, out, serialVersion);
        out.writeBoolean(theIsPatternConst);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_PARSE_TO_TIMESTAMP;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return FunctionLib.FuncCode.FN_PARSE_TO_TIMESTAMP;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new FuncParseToTimestampState());
        theStringArg.open(rcb);
        if (thePatternArg != null) {
            thePatternArg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        FuncParseToTimestampState state =
            (FuncParseToTimestampState)rcb.getState(theStatePos);
        if (state.isDone()) {
            return false;
        }

        boolean more = theStringArg.next(rcb);
        if (!more) {
            state.done();
            return false;
        }

        FieldValueImpl strVal = rcb.getRegVal(theStringArg.getResultReg());
        if (strVal.isNull()) {
            /* return NULL if the input string is null */
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        String pattern = null;
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

        String timestampStr = null;
        try {
            timestampStr = strVal.castAsString();
        } catch (ClassCastException ex) {
            /* return NULL if the input is not castable to string */
            rcb.setRegVal(theResultReg, NullValueImpl.getInstance());
            state.done();
            return true;
        }

        Timestamp ts = TimestampUtils.parseString(timestampStr,
                                                  pattern,
                                                  true /* withUTCZone */);
        FieldValueImpl res =
            FieldDefImpl.Constants.timestampDef.createTimestamp(ts);
        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theStringArg.reset(rcb);
        if (thePatternArg != null) {
            thePatternArg.reset(rcb);
        }

        FuncParseToTimestampState state =
            (FuncParseToTimestampState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theStringArg.close(rcb);
        if (thePatternArg != null) {
            thePatternArg.close(rcb);
        }

        state.close();
    }

    @Override
    void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        if (thePatternArg == null) {
            displayInputIter(sb, formatter, verbose, theStringArg);
        } else {
            PlanIter[] args = new PlanIter[2];
            args[0] = theStringArg;
            args[1] = thePatternArg;
            displayInputIters(sb, formatter, verbose, args);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof FuncParseToTimestampIter)) {
            return false;
        }
        final FuncParseToTimestampIter other = (FuncParseToTimestampIter) obj;
        return Objects.equals(theStringArg, other.theStringArg) &&
               Objects.equals(thePatternArg, other.thePatternArg) &&
               (theIsPatternConst == other.theIsPatternConst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theStringArg,
                            thePatternArg,
                            theIsPatternConst);
    }
}