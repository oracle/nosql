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
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.runtime.PlanIterState.StateEnum;

/**
 * Note: each condition expr E is wrapped by a promote(E, boolean?) expr. 
 */
public class CaseIter extends PlanIter {

    static private class CaseIterState extends PlanIterState {

        /* 
         * theActiveIter is set to the iterator whose associated condition
         * evaluated to true.
         */
        PlanIter theActiveIter;
    }

    private final PlanIter[] theCondIters;

    private final PlanIter[] theThenIters;

    private final PlanIter theElseIter;

    public CaseIter(
        Expr e,
        int resultReg,
        PlanIter[] condIters,
        PlanIter[] thenIters,
        PlanIter elseIter) {

        super(e, resultReg);
        theCondIters = condIters;
        theThenIters = thenIters;
        theElseIter = elseIter;
    }

    CaseIter(DataInput in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theCondIters = deserializeIters(in, serialVersion);
        theThenIters = deserializeIters(in, serialVersion);
        theElseIter = deserializeIter(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIters(theCondIters, out, serialVersion);
        serializeIters(theThenIters, out, serialVersion);
        serializeIter(theElseIter, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.CASE;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new CaseIterState());

        for (PlanIter iter : theCondIters) {
            iter.open(rcb);
        }
        for (PlanIter iter : theThenIters) {
            iter.open(rcb);
        }
        if (theElseIter != null) {
            theElseIter.open(rcb);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter iter : theCondIters) {
            iter.reset(rcb);
        }
        for (PlanIter iter : theThenIters) {
            iter.reset(rcb);
        }
        if (theElseIter != null) {
            theElseIter.reset(rcb);
        }

        CaseIterState state = (CaseIterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        CaseIterState state = (CaseIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.isOpen()) {

            int i;
            for (i = 0; i < theCondIters.length; ++i) {

                boolean more = theCondIters[i].next(rcb);

                if (!more) {
                    continue;
                }
 
                FieldValueImpl val =
                    rcb.getRegVal(theCondIters[i].getResultReg());

                if (val.isNull() || !val.getBoolean()) {
                    continue;
                }

                state.theActiveIter = theThenIters[i];
                break;
            }

            if (i == theCondIters.length) {
                if (theElseIter == null) {
                    state.done();
                    return false;
                }
                state.theActiveIter = theElseIter;
            }

            state.setState(StateEnum.RUNNING);
        }

        if (!state.theActiveIter.next(rcb)) {
            state.done();
            return false;
        }

        FieldValueImpl retValue = rcb.getRegVal(
            state.theActiveIter.getResultReg());

        rcb.setRegVal(theResultReg, retValue);
        return true;
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        CaseIterState state = (CaseIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter iter : theCondIters) {
            iter.close(rcb);
        }
        for (PlanIter iter : theThenIters) {
            iter.close(rcb);
        }
        if (theElseIter != null) {
            theElseIter.close(rcb);
        }
        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"clauses\" : [\n");

        formatter.incIndent();

        for (int i = 0; i < theCondIters.length; ++i) {

            formatter.indent(sb);
            sb.append("{\n");
            formatter.incIndent();
            formatter.indent(sb);
            sb.append("\"when iterator\" :\n");
            theCondIters[i].display(sb, formatter, verbose);
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"then iterator\" :\n");
            theThenIters[i].display(sb, formatter, verbose);
            sb.append("\n");
            formatter.decIndent();
            formatter.indent(sb);
            sb.append("}");

            if (i < theCondIters.length - 1) {
                sb.append(",\n");
            }
        }

        if (theElseIter != null) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("{\n");
            formatter.incIndent();
            formatter.indent(sb);
            sb.append("\"else iterator\" :\n");
            theElseIter.display(sb, formatter, verbose);
            sb.append("\n");
            formatter.decIndent();
            formatter.indent(sb);
            sb.append("}");
        }

        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof CaseIter)) {
            return false;
        }
        final CaseIter other = (CaseIter) obj;
        return Arrays.equals(theCondIters, other.theCondIters) &&
            Arrays.equals(theThenIters, other.theThenIters) &&
            Objects.equals(theElseIter, other.theElseIter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(), theCondIters, theThenIters, theElseIter);
    }
}
