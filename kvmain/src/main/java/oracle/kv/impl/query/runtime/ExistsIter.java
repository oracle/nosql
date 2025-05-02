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

import oracle.kv.impl.api.table.BooleanValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;


/**
 * boolean exists(any*)
 *
 * Returns TRUE for NULL input.
 */
public class ExistsIter extends PlanIter {

    private final FunctionLib.FuncCode theCode;

    private final PlanIter theArg;

    public ExistsIter(
        Expr e,
        int resultReg,
        FunctionLib.FuncCode code,
        PlanIter argIter) {

        super(e, resultReg);

        theCode = code;
        theArg = argIter;
    }

    /**
     * FastExternalizable constructor.
     */
    public ExistsIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = readOrdinal(in, FunctionLib.FuncCode.VALUES_COUNT);
        theCode = FunctionLib.FuncCode.valueOf(ordinal);
        theArg = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeShort(theCode.ordinal());
        serializeIter(theArg, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.EXISTS;
    }

    @Override
    FunctionLib.FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theArg.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theArg.next(rcb);

        if (more) {
            FieldValueImpl val = rcb.getRegVal(theArg.getResultReg());

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("Executing " + getFuncCode() + " on value " + val);
            }

            if (val.isNull()) {
                rcb.setRegVal(theResultReg, val);
                state.done();
                return true;
            }

            if (val.isEMPTY()) {
                if (theCode == FuncCode.OP_EXISTS) {
                    rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
                } else {
                    rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
                }
            }
        }

        if (theCode == FuncCode.OP_EXISTS) {
            if (!more) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            } else {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
            }
        } else {
            if (!more) {
                rcb.setRegVal(theResultReg, BooleanValueImpl.trueValue);
            } else {
                rcb.setRegVal(theResultReg, BooleanValueImpl.falseValue);
            }
        }

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theArg.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theArg.close(rcb);

        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theArg);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ExistsIter)) {
            return false;
        }
        final ExistsIter other = (ExistsIter) obj;
        return (theCode == other.theCode) &&
            Objects.equals(theArg, other.theArg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theArg);
    }
}
