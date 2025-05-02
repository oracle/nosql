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

import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_16;
import static oracle.kv.impl.util.SerialVersion.QUERY_VERSION_17;

import java.util.concurrent.TimeUnit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.compiler.ExprUpdateRow;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.table.TimeToLive;

/*
 * theAllIndexes:
 * All the indexes available on the table at compilation time. If this array is
 * empty (its length is 0), then no indexes need to be updated.
  *
 * theIndexesToUpdate:
 * An array storing, for each index in theAllIndexes, a boolean flag specifying
 * whether the corresponding index needs to be updated or not.
 */
public class UpdateRowIter extends PlanIter {

    public static class UpdateRowState extends PlanIterState {

        PlanIter theWorkerIter;

        public UpdateRowState(PlanIter worker) {
            theWorkerIter = worker;
        }
    }

    protected final String theNamespace;

    protected final String theTableName;

    protected PlanIter theInputIter;

    protected PlanIter[] theUpdateOps;

    protected boolean theUpdateTTL;

    protected PlanIter theTTLIter;

    protected TimeUnit theTTLUnit;

    protected boolean theHasReturningClause;

    protected String[] theAllIndexes;

    protected long[] theAllIndexIds;

    protected boolean[] theIndexesToUpdate;

    protected boolean theIsCompletePrimaryKey;

    public UpdateRowIter(
        ExprUpdateRow e,
        int resultReg,
        TableImpl table,
        PlanIter inputIter,
        PlanIter[] ops,
        boolean updateTTL,
        PlanIter ttlIter,
        TimeUnit ttlUnit,
        boolean hasReturningClause,
        boolean isCompletePrimaryKey) {

        super(e, resultReg);
        theNamespace = table.getInternalNamespace();
        theTableName = table.getFullName();
        theInputIter = inputIter;
        theUpdateOps = ops;
        theUpdateTTL = updateTTL;
        theTTLIter = ttlIter;
        theTTLUnit = ttlUnit;
        assert(theTTLIter == null || theUpdateTTL);
        theHasReturningClause = hasReturningClause;
        theAllIndexes = e.indexNames();
        theAllIndexIds = e.indexIds();
        theIndexesToUpdate = e.indexesToUpdate();
        theIsCompletePrimaryKey = isCompletePrimaryKey;
    }

    /*
     * This constructor is called from the ServerUpdateRowIter constructor.
     * So, we are actually creating a ServerUpdateRowIter to be the worker
     * iter for a "parent" UpdateRowIter.
     */
    public UpdateRowIter(UpdateRowIter parent) {
        super(parent.theStatePos, parent.theResultReg, parent.getLocation());
        theNamespace = parent.theNamespace;
        theTableName = parent.theTableName;
        theInputIter = parent.theInputIter;
        theUpdateOps = parent.theUpdateOps;
        theUpdateTTL = parent.theUpdateTTL;
        theTTLIter = parent.theTTLIter;
        theTTLUnit = parent.theTTLUnit;
        theHasReturningClause = parent.theHasReturningClause;
        theAllIndexes = parent.theAllIndexes;
        theAllIndexIds = parent.theAllIndexIds;
        theIndexesToUpdate = parent.theIndexesToUpdate;
        theIsCompletePrimaryKey = parent.theIsCompletePrimaryKey;
    }

    public UpdateRowIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theNamespace = SerializationUtil.readString(in, serialVersion);
        theTableName = SerializationUtil.readString(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theUpdateOps = deserializeIters(in, serialVersion);
        theUpdateTTL = in.readBoolean();
        if (theUpdateTTL) {
            theTTLIter = deserializeIter(in, serialVersion);
            if (theTTLIter != null) {
                theTTLUnit = TimeToLive.readTTLUnit(in, 1);
            }
        }
        theHasReturningClause = in.readBoolean();

        if (serialVersion >= QUERY_VERSION_16) {
            theIsCompletePrimaryKey = in.readBoolean();
        } else {
            /*
             * Older version client only support update with full primary key,
             * set theIsCompletePrimaryKey to true
             */
            theIsCompletePrimaryKey = true;
        }

        if (serialVersion >= QUERY_VERSION_17) {
            theAllIndexes = deserializeStringArray(in, serialVersion);
            theAllIndexIds = deserializeLongArray(in, serialVersion);
            theIndexesToUpdate = deserializeBooleanArray(in);
        } else {
            theAllIndexes = null;
            theAllIndexIds = null;
            theIndexesToUpdate = null;
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        SerializationUtil.writeString(out, serialVersion, theNamespace);
        SerializationUtil.writeString(out, serialVersion, theTableName);
        serializeIter(theInputIter, out, serialVersion);
        serializeIters(theUpdateOps, out, serialVersion);
        out.writeBoolean(theUpdateTTL);
        if (theUpdateTTL) {
            serializeIter(theTTLIter, out, serialVersion);
            if (theTTLIter != null) {
                out.writeByte((byte) theTTLUnit.ordinal());
            }
        }
        out.writeBoolean(theHasReturningClause);

        if (serialVersion >= QUERY_VERSION_16) {
            out.writeBoolean(theIsCompletePrimaryKey);
        } else {
            /* Older version server doesn't support updating multiple rows with
             * shard key, throw ISE */
            if (!theIsCompletePrimaryKey) {
                throw new IllegalStateException("Serial version " +
                    serialVersion +
                    " does not support theIsCompletePrimaryKey, must be " +
                    QUERY_VERSION_16 + " or greater");
            }
        }

        if (serialVersion >= QUERY_VERSION_17) {
            serializeStringArray2(theAllIndexes, out, serialVersion);
            serializeLongArray(theAllIndexIds, out, serialVersion);
            serializeBooleanArray2(theIndexesToUpdate, out);
        }
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.UPDATE_ROW;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        ServerIterFactory serverIterFactory = rcb.getServerIterFactory();
        PlanIter worker = serverIterFactory.createUpdateRowIter(this);
        worker.open(rcb);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        UpdateRowState state = (UpdateRowState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.theWorkerIter.close(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        UpdateRowState state = (UpdateRowState)rcb.getState(theStatePos);
        state.theWorkerIter.reset(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {
        UpdateRowState state = (UpdateRowState)rcb.getState(theStatePos);
        return state.theWorkerIter.next(rcb);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {

        formatter.indent(sb);
        sb.append("\"indexes to update\" : [ ");
        int numToUpdate = 0;
        for (int i = 0; i < theIndexesToUpdate.length; ++i) {
            if (theIndexesToUpdate[i]) {
                if (numToUpdate > 0) {
                    sb.append(", ");
                }
                sb.append("\"").append(theAllIndexes[i]).append("\"");
                ++numToUpdate;
            }
        }
        sb.append(" ],\n");

        formatter.indent(sb);
        sb.append("\"update clauses\" : [\n");
        formatter.incIndent();
        for (int i = 0; i < theUpdateOps.length; ++i) {
            theUpdateOps[i].display(sb, formatter, verbose);
            if (i < theUpdateOps.length - 1) {
                sb.append(",\n");
            }
        }
        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("],\n");

        formatter.indent(sb);
        sb.append("\"update TTL\" : ").append(theUpdateTTL);
        sb.append(",\n");
        if (theTTLIter != null) {
            formatter.indent(sb);
            sb.append("\"TimeUnit\" : \"").append(theTTLUnit).append("\",\n");
            formatter.indent(sb);
            sb.append("\"TTL iterator\" :\n");
            theTTLIter.display(sb, formatter, verbose);
            sb.append(",\n");
        }

        formatter.indent(sb);
        sb.append("\"isCompletePrimaryKey\" : ")
          .append(theIsCompletePrimaryKey)
          .append(",\n");
        displayInputIter(sb, formatter, verbose, theInputIter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof UpdateRowIter)) {
            return false;
        }
        final UpdateRowIter other = (UpdateRowIter) obj;
        return Objects.equals(theNamespace, other.theNamespace) &&
            Objects.equals(theTableName, other.theTableName) &&
            Objects.equals(theInputIter, other.theInputIter) &&
            Arrays.equals(theUpdateOps, other.theUpdateOps) &&
            (theUpdateTTL == other.theUpdateTTL) &&
            Objects.equals(theTTLIter, other.theTTLIter) &&
            (theTTLUnit == other.theTTLUnit) &&
            (theHasReturningClause == other.theHasReturningClause) &&
            (theIsCompletePrimaryKey == other.theIsCompletePrimaryKey);
    }
}
