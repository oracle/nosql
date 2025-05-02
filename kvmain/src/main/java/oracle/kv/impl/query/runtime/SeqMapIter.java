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

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.compiler.Expr;

/**
 *
 */
public class SeqMapIter extends PlanIter {

    static private class SeqMapState extends PlanIterState {

        FieldValueImpl theCtxItem;

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCtxItem = null;
        }

        @Override
        public void close() {
            super.close();
            theCtxItem = null;
        }
    }

    private final PlanIter theInputIter;

    private final PlanIter theMapIter;

    private final int theCtxItemReg;

    public SeqMapIter(
        Expr e,
        int resultReg,
        PlanIter inputIter,
        PlanIter mapIter,
        int ctxItemReg) {

        super(e, resultReg);
        theInputIter = inputIter;
        theMapIter = mapIter;
        theCtxItemReg = ctxItemReg;
    }

    /**
     * FastExternalizable constructor.
     */
    SeqMapIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theCtxItemReg = in.readInt();
        theInputIter = deserializeIter(in, serialVersion);
        theMapIter = deserializeIter(in, serialVersion);
    }

    /**
     * FastExternalizable writer.  Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(theCtxItemReg);
        serializeIter(theInputIter, out, serialVersion);
        serializeIter(theMapIter, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SEQ_MAP;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new SeqMapState());

        theInputIter.open(rcb);
        theMapIter.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInputIter.reset(rcb);
        theMapIter.reset(rcb);

        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInputIter.close(rcb);
        theMapIter.close(rcb);

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        SeqMapState state = (SeqMapState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            if (state.theCtxItem == null) {

                boolean more = theInputIter.next(rcb);

                if (!more) {
                    state.done();
                    return false;
                }

                state.theCtxItem = rcb.getRegVal(theInputIter.getResultReg());
                
                rcb.setRegVal(theCtxItemReg, state.theCtxItem);

                theMapIter.reset(rcb);
            }

            boolean more = theMapIter.next(rcb);

            if (!more) {
                state.theCtxItem = null;
                continue;
            }

            FieldValueImpl res = rcb.getRegVal(theMapIter.getResultReg());
            rcb.setRegVal(theResultReg, res);

            return true;
        }
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        if (verbose && theCtxItemReg >= 0) {
            formatter.indent(sb);
            sb.append("\"register for $ variable\" : ");
            sb.append(theCtxItemReg);
            sb.append(",\n");
        }

        formatter.indent(sb);
        sb.append("\"mapper iterator\" :\n");
        theMapIter.display(sb, formatter, verbose);
        sb.append(",\n");

        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof SeqMapIter)) {
            return false;
        }
        final SeqMapIter other = (SeqMapIter) obj;
        return Objects.equals(theInputIter, other.theInputIter) &&
            Objects.equals(theMapIter, other.theMapIter) &&
            (theCtxItemReg == other.theCtxItemReg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theInputIter,
                            theMapIter,
                            theCtxItemReg);
    }
}
