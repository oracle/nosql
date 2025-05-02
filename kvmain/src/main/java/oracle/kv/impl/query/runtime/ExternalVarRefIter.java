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

import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import oracle.kv.impl.api.table.DisplayFormatter;
import oracle.kv.impl.api.table.FieldValueImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.compiler.Expr;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;

/**
 * VarRefIter represents a reference to an external variable in the query.
 * It simply returns the value that the variable is currently bound to. This
 * value is set by the app via the methods of BoundStatement.
 *
 * theName:
 * The name of the variable. Used only when displaying the execution plan
 * and in error messages.
 *
 * theId:
 * The valriable id. IT is used as an index into an array of FieldValues
 * in the RCB that stores the values of the external vars.
 */
public class ExternalVarRefIter extends PlanIter {

    private final String theName;

    private final int theId;

    public ExternalVarRefIter(
        Expr e,
        int resultReg,
        int id,
        String name,
        boolean forCloud) {
        super(e, resultReg, forCloud);
        theName = name;
        theId = id;
    }

    /**
     * FastExternalizable constructor.
     */
    ExternalVarRefIter(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theName = readString(in, serialVersion);
        theId = readPositiveInt(in);
    }

    /**
     * FastExternalizable writer. Must call superclass method first to
     * write common elements.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, theName);
        out.writeInt(theId);
    }

    @Override
    public void writeForCloud(
        DataOutput out,
        short driverVersion,
        FieldValueWriter valWriter) throws IOException {

        assert(theIsCloudDriverIter);
        writeForCloudCommon(out, driverVersion);
        CloudSerializer.writeString(theName, out);
        out.writeInt(theId);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.EXTERNAL_VAR_REF;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        FieldValueImpl val = rcb.getExternalVar(theId);

        /*
         * val should not be null, because we check before starting query
         * execution that all the external vars have been bound. So this is
         * a sanity check.
         */
        if (val == null) {
            throw new QueryStateException(
                "Variable " + theName + " has not been set");
        }

        rcb.setRegVal(theResultReg, val);
        state.done();
        return (val.isEMPTY() ? false : true);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }
        state.close();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        DisplayFormatter formatter,
        boolean verbose) {
        formatter.indent(sb);
        sb.append("\"variable\" : \"");
        sb.append(theName).append("\"");
        if (verbose) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"id\" : \"").append(theId).append("\"");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || !(obj instanceof ExternalVarRefIter)) {
            return false;
        }
        final ExternalVarRefIter other = (ExternalVarRefIter) obj;
        return Objects.equals(theName, other.theName) &&
            (theId == other.theId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), theName, theId);
    }
}
