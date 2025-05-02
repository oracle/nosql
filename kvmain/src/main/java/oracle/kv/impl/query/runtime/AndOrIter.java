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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.compiler.FunctionLib.FuncCode;

/**
 * Iterator to implement and and or comparison operators
 *
 * boolean AndOr(boolean?, ...)
 *
 * An empty operand is treated as false.
 * Returns NULL if any operand is NULL and either :
 * (a) it is AND and all the other operands are TRUE or NULL, or
 * (b) is is OR and all other operands are FALSE or NULL
 */
public class AndOrIter extends PlanIter {

    private final FuncCode theCode;

    private final PlanIter[] theArgs;

    public AndOrIter(
        Expr e,
        int resultReg,
        FuncCode code,
        PlanIter[] argIters) {

        super(e, resultReg);
        theCode = checkNull("code", code);
        assert(argIters.length >= 2);
        theArgs = argIters;
    }

    /**
     * FastExternalizable constructor.
     */
    public AndOrIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        short ordinal = in.readShort();
        theCode = FuncCode.valueOf(ordinal);
        theArgs = deserializeIters(in, serialVersion);
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
        serializeIters(theArgs, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.AND_OR;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter arg : theArgs) {
            arg.open(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        /*
         * If AND, start true, and exit as soon as there is a false result.
         * If OR, start false, and exit as soon as there is a true result.
         */
        assert(theCode == FuncCode.OP_AND || theCode == FuncCode.OP_OR);
        boolean result = (theCode == FuncCode.OP_AND ? true : false);
        boolean haveNull = false;
        FieldValueImpl res;

        for (PlanIter arg : theArgs) {

            boolean more = arg.next(rcb);

            boolean argResult;

            if (!more) {
                argResult = false;
            } else {
                FieldValueImpl argVal = rcb.getRegVal(arg.getResultReg());

                if (argVal.isNull()) {
                    haveNull = true;
                    continue;
                }

                argResult = argVal.getBoolean();
            }

            if (theCode == FuncCode.OP_AND) {
                result &= argResult;
                if (!result) {
                    haveNull = false;
                    break;
                }
            } else {
                result |= argResult;
                if (result) {
                    haveNull = false;
                    break;
                }
            }
        }

        if (haveNull) {
            res = NullValueImpl.getInstance();
        } else {
            res = FieldDefImpl.Constants.booleanDef.createBoolean(result);
        }

        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter arg : theArgs) {
            arg.reset(rcb);
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

        for (PlanIter arg : theArgs) {
            arg.close(rcb);
        }

        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        displayInputIters(sb, formatter, verbose, theArgs);
    }

    @Override
    void displayName(StringBuilder sb) {
        if (theCode == FuncCode.OP_AND) {
            sb.append("AND");
        } else {
            sb.append("OR");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof AndOrIter)) {
            return false;
        }
        final AndOrIter other = (AndOrIter) obj;
        return (theCode == other.theCode) &&
            Arrays.equals(theArgs, other.theArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theCode, theArgs);
    }
}
