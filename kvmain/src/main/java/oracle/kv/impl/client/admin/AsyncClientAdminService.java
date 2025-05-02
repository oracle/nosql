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

package oracle.kv.impl.client.admin;

import static oracle.kv.impl.util.SerializationUtil.readCharArray;
import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeCharArray;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.async.AsyncVersionedRemote;
import oracle.kv.impl.async.AsyncInitiatorProxy.MethodCallClass;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ReadFastExternal;
import oracle.kv.impl.util.contextlogger.LogContext;

/**
 * Defines an async version of the ClientAdminService interface.
 *
 * @since 21.2
 */
public interface AsyncClientAdminService extends AsyncVersionedRemote {

    /** The IDs for methods in this interface. */
    enum ServiceMethodOp implements MethodOp {
        GET_SERIAL_VERSION(0, GetSerialVersionCall::new),
        EXECUTE(1, ExecuteCall::new),
        SET_TABLE_LIMITS(2, SetTableLimitsCall::new),
        GET_EXECUTION_STATUS(3, GetExecutionStatusCall::new),
        CAN_HANDLE_DDL(4, CanHandleDDLCall::new),
        GET_MASTER_RMI_ADDRESS(5, GetMasterRMIAddressCall::new),
        INTERRUPT_AND_CANCEL(6, InterruptAndCancelCall::new),
        GET_TOPOLOGY(7, GetTopologyCall::new),
        GET_TOPO_SEQ_NUM(8, GetTopoSeqNumCall::new);

        private static final ServiceMethodOp[] VALUES = values();

        private final ReadFastExternal<MethodCall<?>> reader;

        ServiceMethodOp(final int ordinal,
                        final ReadFastExternal<MethodCall<?>> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        /**
         * Returns the ServiceMethodOp with the specified ordinal.
         *
         * @param ordinal the ordinal
         * @return the ServiceMethodOp
         * @throws IllegalArgumentException if there is no associated value
         */
        public static ServiceMethodOp valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Wrong ordinal for ServiceMethodOp: " + ordinal, e);
            }
        }

        @Override
        public int getValue() {
            return ordinal();
        }

        @Override
        public MethodCall<?> readRequest(final DataInput in,
                                         final short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }

        @Override
        public String toString() {
            return name() + '(' + ordinal() + ')';
        }
    }

    /* Remote methods */

    @MethodCallClass(GetSerialVersionCall.class)
    @Override
    CompletableFuture<Short> getSerialVersion(short serialVersion,
                                              long timeoutMillis);

    class GetSerialVersionCall extends AbstractGetSerialVersionCall
            implements ServiceMethodCall<Short> {
        public GetSerialVersionCall() { }
        @SuppressWarnings("unused")
        GetSerialVersionCall(DataInput in, short serialVersion) { }
        @Override
        public CompletableFuture<Short>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.getSerialVersion(serialVersion, timeoutMillis);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_SERIAL_VERSION;
        }
        @Override
        public String describeCall() {
            return "AsyncClientAdminService.GetSerialVersionCall";
        }
    }

    /**
     * Ask the master Admin to execute the statement, specifying whether to
     * validate the namespace.
     */
    @MethodCallClass(ExecuteCall.class)
    CompletableFuture<ExecutionInfo> execute(short serialVersion,
                                             char[] statement,
                                             String namespace,
                                             boolean validateNamespace,
                                             TableLimits limits,
                                             LogContext lc,
                                             AuthContext authCtx,
                                             long timeoutMillis);

    class ExecuteCall extends AbstractExecutionInfoCall {
        /*
         * TODO: Need a way to clear out possibly security-sensitive
         * information from the statement (passwords)?
         */
        final char[] statement;
        final String namespace;
        final boolean validateNamespace;
        final TableLimits limits;
        final LogContext lc;
        final AuthContext authCtx;
        public ExecuteCall(final char[] statement,
                           final String namespace,
                           final boolean validateNamespace,
                           final TableLimits limits,
                           final LogContext lc,
                           final AuthContext authCtx) {
            this.statement = statement;
            this.namespace = namespace;
            this.validateNamespace = validateNamespace;
            this.limits = limits;
            this.lc = lc;
            this.authCtx = authCtx;
        }
        ExecuteCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readCharArray(in),
                 readString(in, serialVersion),
                 in.readBoolean(),
                 readFastExternalOrNull(in, serialVersion, TableLimits::new),
                 readFastExternalOrNull(in, serialVersion, LogContext::new),
                 readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeCharArray(out, statement);
            writeString(out, sv, namespace);
            out.writeBoolean(validateNamespace);
            writeFastExternalOrNull(out, sv, limits);
            writeFastExternalOrNull(out, sv, lc);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.EXECUTE;
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.execute(serialVersion, statement, namespace,
                                   validateNamespace, limits, lc, authCtx,
                                   timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            /*
             * Don't include the statement in the output since it may be
             * security sensitive
             */
            sb.append("namespace=").append(namespace);
            sb.append(" validateNamespace=").append(validateNamespace);
            sb.append(" limits=").append(limits);
            sb.append(" lc=").append(lc);
        }
    }

    /**
     * Added in 18.1/cloud
     */
    @MethodCallClass(SetTableLimitsCall.class)
    CompletableFuture<ExecutionInfo> setTableLimits(short serialVersion,
                                                    String namespace,
                                                    String tableName,
                                                    TableLimits limits,
                                                    AuthContext authCtx,
                                                    long timeoutMillis);

    class SetTableLimitsCall extends AbstractExecutionInfoCall {
        final String namespace;
        final String tableName;
        final TableLimits limits;
        final AuthContext authCtx;
        public SetTableLimitsCall(final String namespace,
                                  final String tableName,
                                  final TableLimits limits,
                                  final AuthContext authCtx) {
            this.namespace = namespace;
            this.tableName = tableName;
            this.limits = limits;
            this.authCtx = authCtx;
        }
        SetTableLimitsCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readString(in, serialVersion),
                 readString(in, serialVersion),
                 readFastExternalOrNull(in, serialVersion, TableLimits::new),
                 readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeString(out, sv, namespace);
            writeString(out, sv, tableName);
            writeFastExternalOrNull(out, sv, limits);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.SET_TABLE_LIMITS;
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.setTableLimits(serialVersion, namespace, tableName,
                                          limits, authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("namespace=").append(namespace)
                .append(" tableName=").append(tableName)
                .append(" limits=").append(limits);
        }
    }

    /**
     * Get current status for the specified plan.
     */
    @MethodCallClass(GetExecutionStatusCall.class)
    CompletableFuture<ExecutionInfo> getExecutionStatus(short serialVersion,
                                                        int planId,
                                                        AuthContext authCtx,
                                                        long timeoutMillis);

    class GetExecutionStatusCall extends AbstractExecutionInfoCall {
        final int planId;
        final AuthContext authCtx;
        public GetExecutionStatusCall(final int planId,
                                      final AuthContext authCtx) {
            this.planId = planId;
            this.authCtx = authCtx;
        }
        GetExecutionStatusCall(final DataInput in,
                               final short serialVersion)
            throws IOException
        {
            this(in.readInt(),
                 readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            out.writeInt(planId);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_EXECUTION_STATUS;
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.getExecutionStatus(serialVersion, planId, authCtx,
                                              timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("planId=").append(planId);
        }
    }

    /**
     * Return true if this Admin can handle DDL operations. That currently
     * equates to whether the Admin is a master or not.
     */
    @MethodCallClass(CanHandleDDLCall.class)
    CompletableFuture<Boolean> canHandleDDL(short serialVersion,
                                            AuthContext authCtx,
                                            long timeoutMillis);

    class CanHandleDDLCall extends AbstractCall<Boolean> {
        final AuthContext authCtx;
        public CanHandleDDLCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        CanHandleDDLCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Boolean readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return in.readBoolean();
        }
        @Override
        public void writeResponse(final Boolean result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            out.writeBoolean(result);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.CAN_HANDLE_DDL;
        }
        @Override
        public CompletableFuture<Boolean>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.canHandleDDL(serialVersion, authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Return the address of the master Admin. If this Admin doesn't know that,
     * return null.
     */
    @MethodCallClass(GetMasterRMIAddressCall.class)
    CompletableFuture<URI> getMasterRmiAddress(short serialVersion,
                                               AuthContext authCtx,
                                               long timeoutMillis);

    class GetMasterRMIAddressCall extends AbstractCall<URI> {
        final AuthContext authCtx;
        public GetMasterRMIAddressCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetMasterRMIAddressCall(final DataInput in,
                                final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public URI readResponse(final DataInput in, final short sv)
            throws IOException
        {
            try {
                final String uriString = readString(in, sv);
                return (uriString != null) ? new URI(uriString) : null;
            } catch (URISyntaxException e) {
                throw new IOException("Invalid URI syntax: " + e.getMessage(),
                                      e);
            }
        }
        @Override
        public void writeResponse(final URI uri,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion,
                        (uri != null) ? uri.toString() : null);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_MASTER_RMI_ADDRESS;
        }
        @Override
        public CompletableFuture<URI>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.getMasterRmiAddress(serialVersion, authCtx,
                                               timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Start cancellation of a plan. Return the current status.
     */
    @MethodCallClass(InterruptAndCancelCall.class)
    CompletableFuture<ExecutionInfo> interruptAndCancel(short serialVersion,
                                                        int planId,
                                                        AuthContext nullCtx,
                                                        long timeoutMillis);

    class InterruptAndCancelCall extends AbstractExecutionInfoCall {
        final int planId;
        final AuthContext authCtx;
        public InterruptAndCancelCall(final int planId,
                                      final AuthContext authCtx) {
            this.planId = planId;
            this.authCtx = authCtx;
        }
        InterruptAndCancelCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(in.readInt(),
                 readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            out.writeInt(planId);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.INTERRUPT_AND_CANCEL;
        }
        @Override
        public CompletableFuture<ExecutionInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.interruptAndCancel(serialVersion, planId, authCtx,
                                              timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("planId=").append(planId);
        }
    }


    /**
     * Return the current topology used for connecting client to store
     * using Admin service.
     *
     * @since 24.2
     */
    @MethodCallClass(GetTopologyCall.class)
    CompletableFuture<Topology> getTopology(short serialVersion,
                                            AuthContext authCtx,
                                            long timeoutMillis);

    class GetTopologyCall extends AbstractCall<Topology> {
        final AuthContext authCtx;
        public GetTopologyCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetTopologyCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Topology readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return readFastExternalOrNull(in, sv, Topology::new);
        }
        @Override
        public void writeResponse(final Topology result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, result);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_TOPOLOGY;
        }
        @Override
        public CompletableFuture<Topology>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.getTopology(serialVersion, authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Returns the sequence number associated with the Topology at the Admin.
     *
     * @since 24.2
     */
    @MethodCallClass(GetTopoSeqNumCall.class)
    CompletableFuture<Integer> getTopoSeqNum(short serialVersion,
                                             AuthContext authCtx,
                                             long timeoutMillis);

    class GetTopoSeqNumCall extends AbstractCall<Integer> {
        final AuthContext authCtx;
        public GetTopoSeqNumCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetTopoSeqNumCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(readFastExternalOrNull(in, serialVersion, AuthContext::new));
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Integer readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return in.readInt();
        }
        @Override
        public void writeResponse(final Integer response,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            out.writeInt(response);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_TOPO_SEQ_NUM;
        }
        @Override
        public CompletableFuture<Integer>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientAdminService service) {
            return service.getTopoSeqNum(serialVersion, authCtx,
                                         timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /* Other classes */

    /**
     * A method call that provides the callService method to perform the call
     * on the specified service.
     */
    interface ServiceMethodCall<R> extends MethodCall<R> {
        CompletableFuture<R> callService(short serialVersion,
                                         long timeoutMillis,
                                         AsyncClientAdminService service);
    }

    /** A MethodCall that simplifies implementing the describeCall method. */
    abstract class AbstractCall<R> implements ServiceMethodCall<R> {
        @Override
        public String describeCall() {
            final StringBuilder sb = new StringBuilder();
            sb.append("AsyncClientAdminService.")
                .append(getMethodOp())
                .append("[");
            describeParams(sb);
            sb.append("]");
            return sb.toString();
        }
        abstract void describeParams(StringBuilder sb);
    }

    /** A MethodCall with ExecutionInfo return type. */
    abstract class AbstractExecutionInfoCall
            extends AbstractCall<ExecutionInfo> {
        @Override
        public ExecutionInfo readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return new ExecutionInfoImpl(in, sv);
        }
        @Override
        public void writeResponse(final ExecutionInfo info,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            /*
             * For now, all ExecutionInfo instances are actually
             * ExecutionInfoImpl. Need to change the serialized form if that
             * changes.
             */
            ((ExecutionInfoImpl) info).writeFastExternal(out, serialVersion);
        }
    }
}
