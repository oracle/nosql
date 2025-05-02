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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TupleValue;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.ExprRecConstr;

public class RecConstrIter extends PlanIter {

    private final PlanIter[] theArgs;

    private final RecordDefImpl theDef;

    public RecConstrIter(
        ExprRecConstr e,
        int resultReg,
        PlanIter[] args) {

        super(e, resultReg);
        theArgs = args;
        theDef = e.getDef();
    }

    RecConstrIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theArgs = deserializeIters(in, serialVersion);
        theDef = (RecordDefImpl)deserializeFieldDef(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIters(theArgs, out, serialVersion);
        serializeFieldDef(theDef, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.RECORD_CONSTRUCTOR;
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

        int numArgs = theArgs.length;
        RecordValueImpl rec = theDef.createRecord();

        for (int i = 0; i < numArgs; ++i) {

            String fname = theDef.getFieldName(i);
            FieldValueImpl fval;

            boolean more = theArgs[i].next(rcb);

            if (!more) {
                fval = NullValueImpl.getInstance();
            } else {
                fval = rcb.getRegVal(theArgs[i].getResultReg());
            }

            if (theArgs[i].next(rcb)) {
                throw new QueryException(
                    "Field expression in record constructor returns more " +
                    "one item", theLocation);
            }

            if (fval.isTuple()) {
                fval = ((TupleValue)fval).toRecord();
            }

            try {
                rec.put(fname, fval);
            } catch (IllegalArgumentException e) {
                if (rcb.getTraceLevel() >= 4) {
                    rcb.trace("Query Plan:\n" + rcb.getRootIter().display() +
                              "\nValue:\n" + fval);
                }
                throw new QueryException(e, theLocation);
            }
        }

        rcb.setRegVal(theResultReg, rec);
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

        formatter.indent(sb);
        sb.append("\"type\" : ");
        theDef.displayAsJson(sb, formatter, verbose);
        sb.append(",\n");

        displayInputIters(sb, formatter, verbose, theArgs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof RecConstrIter)) {
            return false;
        }
        final RecConstrIter other = (RecConstrIter) obj;
        return Arrays.equals(theArgs, other.theArgs) &&
            Objects.equals(theDef, other.theDef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theArgs, theDef);
    }
}
