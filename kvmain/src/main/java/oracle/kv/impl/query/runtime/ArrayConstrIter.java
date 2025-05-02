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
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.ExprArrayConstr;

/**
 *
 */
public class ArrayConstrIter extends PlanIter {

    private final PlanIter[] theArgs;

    private final ArrayDefImpl theDef;

    private boolean theIsConditional;

    public ArrayConstrIter(
        ExprArrayConstr e,
        int resultReg,
        PlanIter[] args) {

        super(e, resultReg);
        theArgs = args;
        theDef = e.getArrayType();
        theIsConditional = e.isConditional();
    }

    /**
     * FastExternalizable constructor.
     */
    ArrayConstrIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theArgs = deserializeIters(in, serialVersion);
        theDef = (ArrayDefImpl)deserializeFieldDef(in, serialVersion);
        theIsConditional = in.readBoolean();
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIters(theArgs, out, serialVersion);
        serializeFieldDef(theDef, out, serialVersion);
        out.writeBoolean(theIsConditional);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARRAY_CONSTRUCTOR;
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

        ArrayValueImpl array;

        if (theIsConditional) {

            boolean more = theArgs[0].next(rcb);

            if (!more) {
                state.done();
                return false;
            }

            FieldValueImpl val = rcb.getRegVal(theArgs[0].getResultReg());

            if (val.isTuple()) {
                val = ((TupleValue)val).toRecord();
            }

            more = theArgs[0].next(rcb);

            if (!more) {
                rcb.setRegVal(theResultReg, val);
                state.done();
                return true;
            }

            array = theDef.createArray();
            array.setConditionallyConstructed(true);

            try {
                if (!val.isNull()) {
                    array.add(val);
                }

                val = rcb.getRegVal(theArgs[0].getResultReg());

                if (val.isTuple()) {
                    val = ((TupleValue)val).toRecord();
                }

                if (!val.isNull()) {
                    array.add(val);
                }
            } catch (IllegalArgumentException e) {
                handleIAE(rcb, val, e);
            }

        } else {
            array = theDef.createArray();
        }

        for (int currArg = 0; currArg < theArgs.length; ++currArg) {

            while (true) {
                boolean more = theArgs[currArg].next(rcb);

                if (!more) {
                    break;
                }

                FieldValueImpl val =
                    rcb.getRegVal(theArgs[currArg].getResultReg());

                if (val.isNull()) {
                    continue;
                }

                try {
                    if (val.isTuple()) {
                        array.add(((TupleValue)val).toRecord());
                    } else {
                        array.add(val);
                    }
                } catch (IllegalArgumentException e) {
                    handleIAE(rcb, val, e);
                }
            }
        }

        rcb.setRegVal(theResultReg, array);
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

    private void handleIAE(
        RuntimeControlBlock rcb,
        FieldValueImpl val,
        IllegalArgumentException e) {

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("Query Plan:\n" + rcb.getRootIter().display() +
                      "\nValue:\n" + val);
        }
        throw new QueryException(e, theLocation);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"conditional\" : ").append(theIsConditional);

        if (verbose) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"type\" : ");
            theDef.displayAsJson(sb, formatter, verbose);
        }

        sb.append(",\n");

        displayInputIters(sb, formatter, verbose, theArgs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ArrayConstrIter)) {
            return false;
        }
        final ArrayConstrIter other = (ArrayConstrIter) obj;
        return Arrays.equals(theArgs, other.theArgs) &&
            Objects.equals(theDef, other.theDef) &&
            (theIsConditional == other.theIsConditional);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(), theArgs, theDef, theIsConditional);
    }
}
