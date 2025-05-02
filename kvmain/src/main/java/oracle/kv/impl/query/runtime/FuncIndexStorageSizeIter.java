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
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.util.SerializationUtil;

/**
 *
 */
public class FuncIndexStorageSizeIter extends PlanIter {

    public static class IndexSizeState extends PlanIterState {

        PlanIter theWorkerIter;

        public IndexSizeState(PlanIter worker) {
            theWorkerIter = worker;
        }
    }

    protected final PlanIter theInput;

    protected final String theNamespace;

    protected final String theTableName;

    protected final String theIndexName;

    public FuncIndexStorageSizeIter(
        Expr e,
        int resultReg,
        PlanIter input,
        TableImpl table,
        String indexName) {

        super(e, resultReg);
        theInput = input;
        theNamespace = table.getInternalNamespace();
        theTableName = table.getFullName();
        theIndexName = indexName;
    }

    /*
     * This constructor is called from the ServerUpdateRowIter constructor.
     * So, we are actually creating a ServerUpdateRowIter to be the worker
     * iter for a "parent" UpdateRowIter.
     */
    public FuncIndexStorageSizeIter(FuncIndexStorageSizeIter parent) {
        super(parent.theStatePos, parent.theResultReg, parent.getLocation());
        theInput = parent.theInput;
        theNamespace = parent.theNamespace;
        theTableName = parent.theTableName;
        theIndexName = parent.theIndexName;
    }

    FuncIndexStorageSizeIter(DataInput in, short serialVersion)
        throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
        theNamespace = SerializationUtil.readString(in, serialVersion);
        theTableName = SerializationUtil.readString(in, serialVersion);
        theIndexName = SerializationUtil.readString(in, serialVersion);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        serializeIter(theInput, out, serialVersion);
        SerializationUtil.writeString(out, serialVersion, theNamespace);
        SerializationUtil.writeString(out, serialVersion, theTableName);
        SerializationUtil.writeString(out, serialVersion, theIndexName);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FUNC_INDEX_STORAGE_SIZE;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        PlanIter worker = rcb.getServerIterFactory().
                          createIndexSizeIter(this);
        worker.open(rcb);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        IndexSizeState state = (IndexSizeState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.theWorkerIter.close(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        IndexSizeState state = (IndexSizeState)rcb.getState(theStatePos);
        state.theWorkerIter.reset(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        IndexSizeState state = (IndexSizeState)rcb.getState(theStatePos);
        return state.theWorkerIter.next(rcb);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        displayInputIter(sb, formatter, verbose, theInput);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) ||
            !(obj instanceof FuncIndexStorageSizeIter)) {
            return false;
        }
        final FuncIndexStorageSizeIter other =
            (FuncIndexStorageSizeIter) obj;
        return Objects.equals(theInput, other.theInput) &&
            Objects.equals(theNamespace, other.theNamespace) &&
            Objects.equals(theTableName, other.theTableName) &&
            Objects.equals(theIndexName, other.theIndexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            theInput,
                            theNamespace,
                            theTableName,
                            theIndexName);
    }
}
