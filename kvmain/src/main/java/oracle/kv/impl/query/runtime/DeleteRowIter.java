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
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.util.SerializationUtil;

/**
 *
 */
public class DeleteRowIter extends PlanIter {

    public static class DeleteRowState extends PlanIterState {

        PlanIter theWorkerIter;

        public DeleteRowState(PlanIter worker) {
            theWorkerIter = worker;
        }
    }

    protected final String theNamespace;

    protected final String theTableName;

    protected final PlanIter theInput;

    protected final boolean theHasReturningClause;

    protected final int[] thePrimKeyPositions;

    protected final RecordDefImpl theResultType;

    public DeleteRowIter(
        Expr e,
        int resultReg,
        TableImpl table,
        PlanIter input,
        boolean hasReturningClause,
        int[] primKeyPositions) {

        super(e, resultReg);
        theNamespace = table.getInternalNamespace();
        theTableName = table.getFullName();
        theInput = input;
        theHasReturningClause = hasReturningClause;
        thePrimKeyPositions = primKeyPositions;
        theResultType = (RecordDefImpl)e.getType().getDef();
    }

    /*
     * This constructor is called from the ServerDeleteRowIter constructor.
     * So, we are actually creating a ServerDeleteRowIter to be the worker
     * iter for a "parent" DeleteRowIter.
     */
    public DeleteRowIter(DeleteRowIter parent) {
        super(parent.theStatePos, parent.theResultReg, parent.getLocation());
        theNamespace = parent.theNamespace;
        theTableName = parent.theTableName;
        theInput = parent.theInput;
        theHasReturningClause = parent.theHasReturningClause;
        thePrimKeyPositions = parent.thePrimKeyPositions;
        theResultType = parent.theResultType;
    }

    public DeleteRowIter(DataInput in, short serialVersion)
            throws IOException {

        super(in, serialVersion);
        theNamespace = SerializationUtil.readString(in, serialVersion);
        theTableName = SerializationUtil.readString(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
        theHasReturningClause = in.readBoolean();
        thePrimKeyPositions = deserializeIntArray(in, serialVersion);
        theResultType = (RecordDefImpl)deserializeFieldDef(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        SerializationUtil.writeString(out, serialVersion, theNamespace);
        SerializationUtil.writeString(out, serialVersion, theTableName);
        serializeIter(theInput, out, serialVersion);
        out.writeBoolean(theHasReturningClause);
        serializeIntArray(thePrimKeyPositions, out, serialVersion);
        serializeFieldDef(theResultType, out, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.DELETE_ROW;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        ServerIterFactory serverIterFactory = rcb.getServerIterFactory();
        if (serverIterFactory != null) {
            PlanIter worker = serverIterFactory.createDeleteRowIter(rcb, this);
            worker.open(rcb);
        } else {
            rcb.setState(theStatePos, new DeleteRowState(null));
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        DeleteRowState state = (DeleteRowState)rcb.getState(theStatePos);
        if (state.theWorkerIter != null) {
            state.theWorkerIter.reset(rcb);
        }
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        DeleteRowState state = (DeleteRowState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        if (state.theWorkerIter != null) {
            state.theWorkerIter.close(rcb);
        }
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        DeleteRowState state = (DeleteRowState)rcb.getState(theStatePos);

        if (state.theWorkerIter != null) {
            return state.theWorkerIter.next(rcb);
        }

        if (state.isDone()) {
            return false;
        }

        if (theHasReturningClause) {
            state.done();
            return false;
        }

        RecordValueImpl record = theResultType.createRecord();
        record.put(0, FieldDefImpl.Constants.longDef.createLong(0));

        rcb.setRegVal(theResultReg, record);
        state.done();
        return true;
    }

    @Override
    public Map<String, String> getRNTraces(RuntimeControlBlock rcb) {
        /* Normally, a DeleteRowIter will not appear at the client-side of the
         * query plan, but one exception is the case when the input to the
         * DeleteRowIter is the empty sequence. */
        return new TreeMap<String, String>();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        if (thePrimKeyPositions != null) {
            formatter.indent(sb);
            sb.append("\"positions of primary key columns in input row\" : [ ");
            for (int i = 0; i < thePrimKeyPositions.length; ++i) {
                sb.append(thePrimKeyPositions[i]);
                if (i < thePrimKeyPositions.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(" ],\n");
        }

        displayInputIter(sb, formatter, verbose, theInput);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof DeleteRowIter)) {
            return false;
        }
        final DeleteRowIter other = (DeleteRowIter) obj;
        return Objects.equals(theNamespace, other.theNamespace) &&
            Objects.equals(theTableName, other.theTableName) &&
            Objects.equals(theInput, other.theInput) &&
            (theHasReturningClause == other.theHasReturningClause) &&
            Arrays.equals(thePrimKeyPositions, other.thePrimKeyPositions) &&
            Objects.equals(theResultType, other.theResultType);
    }
}
