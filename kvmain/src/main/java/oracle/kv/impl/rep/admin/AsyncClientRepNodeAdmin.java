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

package oracle.kv.impl.rep.admin;

import static oracle.kv.impl.util.SerializationUtil.readFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.async.AsyncInitiatorProxy.MethodCallClass;
import oracle.kv.impl.async.AsyncVersionedRemote;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.JESerializationUtil;
import oracle.kv.impl.util.ReadFastExternal;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Async administrative interface to a RepNode needed by clients.
 *
 * @since 21.2
 */
public interface AsyncClientRepNodeAdmin extends AsyncVersionedRemote {

    enum ServiceMethodOp implements MethodOp {
        GET_SERIAL_VERSION(0, GetSerialVersionCall::new),
        GET_TOPOLOGY(1, GetTopologyCall::new),
        GET_TOPO_SEQ_NUM(2, GetTopoSeqNumCall::new),
        GET_HA_HOST_PORT(3, GetHAHostPortCall::new),
        GET_REPLICATION_STATE(4, GetReplicationStateCall::new),
        GET_METADATA_SEQ_NUM(5, GetMetadataSeqNumCall::new),
        GET_METADATA(6, GetMetadataCall::new),
        GET_METADATA_START(7, GetMetadataStartCall::new),
        GET_METADATA_KEY(8, GetMetadataKeyCall::new),
        UPDATE_METADATA(9, UpdateMetadataCall::new),
        UPDATE_METADATA_INFO(10, UpdateMetadataInfoCall::new),
        GET_KERBEROS_PRINCIPALS(11, GetKerberosPrincipalsCall::new),
        GET_TABLE_BY_ID(12, GetTableByIdCall::new),
        GET_TABLE(13, GetTableCall::new),
        GET_VLSN(14, GetVlsnCall::new);

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
        static ServiceMethodOp valueOf(int ordinal) {
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
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_SERIAL_VERSION;
        }
        @Override
        public CompletableFuture<Short>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getSerialVersion(serialVersion, timeoutMillis);
        }
        @Override
        public String describeCall() {
            return "AsyncClientRepNodeAdmin.GetSerialVersionCall";
        }
    }

    /**
     * Returns this RN's view of the Topology. In a distributed system like
     * KVS, it may be temporarily different from the Topology at other nodes,
     * but will eventually become eventually consistent.
     *
     * It returns null if the RN is not in the RUNNING state.
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
                        final AsyncClientRepNodeAdmin service) {
            return service.getTopology(serialVersion, authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Returns the sequence number associated with the Topology at the RN.
     *
     * It returns zero if the RN is not in the RUNNING state.
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
                        final AsyncClientRepNodeAdmin service) {
            return service.getTopoSeqNum(serialVersion, authCtx,
                                         timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Returns a string containing the HA hostname and port for the master in
     * this RN's shard.
     */
    @MethodCallClass(GetHAHostPortCall.class)
    CompletableFuture<String> getHAHostPort(short serialVersion,
                                            AuthContext authCtx,
                                            long timeoutMillis);

    class GetHAHostPortCall extends AbstractCall<String> {
        final AuthContext authCtx;
        public GetHAHostPortCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetHAHostPortCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public String readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return readString(in, sv);
        }
        @Override
        public void writeResponse(final String result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeString(out, serialVersion, result);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_HA_HOST_PORT;
        }
        @Override
        public CompletableFuture<String>
            callService(final short serialVersion,
                        final long timeoutMs,
                        final AsyncClientRepNodeAdmin service) {
            return service.getHAHostPort(serialVersion, authCtx, timeoutMs);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Returns this node's replication state.
     */
    @MethodCallClass(GetReplicationStateCall.class)
    CompletableFuture<ReplicatedEnvironment.State>
        getReplicationState(short serialVersion,
                            AuthContext authCtx,
                            long timeoutMillis);

    class GetReplicationStateCall
            extends AbstractCall<ReplicatedEnvironment.State> {
        final AuthContext authCtx;
        public GetReplicationStateCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetReplicationStateCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public ReplicatedEnvironment.State readResponse(final DataInput in,
                                                        final short sv)
            throws IOException
        {
            return JESerializationUtil.readState(in, sv);
        }
        @Override
        public void writeResponse(final ReplicatedEnvironment.State state,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            JESerializationUtil.writeState(out, serialVersion, state);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_REPLICATION_STATE;
        }
        @Override
        public CompletableFuture<ReplicatedEnvironment.State>
            callService(final short serialVersion,
                        final long timeoutMs,
                        final AsyncClientRepNodeAdmin service) {
            return service.getReplicationState(serialVersion, authCtx,
                                               timeoutMs);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Returns the sequence number associated with the metadata at the RN.
     * If the RN does not contain the metadata or the RN is not in the RUNNING
     * state null is returned.
     *
     * @param type a metadata type
     *
     * @return the sequence number associated with the metadata
     */
    @MethodCallClass(GetMetadataSeqNumCall.class)
    CompletableFuture<Integer> getMetadataSeqNum(short serialVersion,
                                                 MetadataType type,
                                                 AuthContext authCtx,
                                                 long timeoutMillis);

    class GetMetadataSeqNumCall extends AbstractCall<Integer> {
        final MetadataType type;
        final AuthContext authCtx;
        public GetMetadataSeqNumCall(final MetadataType type,
                                     final AuthContext authCtx) {
            this.type = type;
            this.authCtx = authCtx;
        }
        GetMetadataSeqNumCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            type = readFastExternalOrNull(in, serialVersion,
                                          MetadataType::readFastExternal);
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, type);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Integer readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return in.readInt();
        }
        @Override
        public void writeResponse(final Integer result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            out.writeInt(result);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_METADATA_SEQ_NUM;
        }
        @Override
        public CompletableFuture<Integer>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getMetadataSeqNum(serialVersion, type, authCtx,
                                             timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("type=" + type);
        }
    }

    /**
     * Gets the metadata for the specified type. If the RN does not contain the
     * metadata or the RN is not in the RUNNING state null is returned.
     *
     * @param type a metadata type
     *
     * @return metadata
     */
    @MethodCallClass(GetMetadataCall.class)
    CompletableFuture<Metadata<?>> getMetadata(short serialVersion,
                                               MetadataType type,
                                               AuthContext authCtx,
                                               long timeoutMillis);

    class GetMetadataCall extends AbstractCall<Metadata<?>> {
        final MetadataType type;
        final AuthContext authCtx;
        public GetMetadataCall(final MetadataType type,
                               final AuthContext authCtx) {
            this.type = type;
            this.authCtx = authCtx;
        }
        GetMetadataCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            type = readFastExternalOrNull(in, serialVersion,
                                          MetadataType::readFastExternal);
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, type);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Metadata<?> readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return readFastExternalOrNull(in, sv, Metadata::readMetadata);
        }
        @Override
        public void writeResponse(final Metadata<?> result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, result,
                                    Metadata::writeMetadata);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_METADATA;
        }
        @Override
        public CompletableFuture<Metadata<?>>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getMetadata(serialVersion, type, authCtx,
                                       timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("type=").append(type);
        }
    }

    /**
     * Gets metadata information for the specified type starting from the
     * specified sequence number. If the RN is not in the RUNNING state null is
     * returned.
     *
     * @param type a metadata type
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     */
    @MethodCallClass(GetMetadataStartCall.class)
    CompletableFuture<MetadataInfo> getMetadata(short serialVersion,
                                                MetadataType type,
                                                int seqNum,
                                                AuthContext authCtx,
                                                long timeoutMillis);

    class GetMetadataStartCall extends GetMetadataKeyCall {
        public GetMetadataStartCall(final MetadataType type,
                                    final int seqNum,
                                    final AuthContext authCtx) {
            super(type, null /* key */, seqNum, authCtx,
                  false /* includeKey */);
        }
        GetMetadataStartCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            super(in, serialVersion, false /* includeKey */);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_METADATA_START;
        }
        @Override
        public CompletableFuture<MetadataInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getMetadata(serialVersion, type, seqNum, authCtx,
                                       timeoutMillis);
        }
    }

    /**
     * Gets metadata information for the specified type and key starting from
     * the specified sequence number. If the RN is not in the RUNNING state null
     * is returned.
     *
     * @param type a metadata type
     * @param key a metadata key
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     *
     * @throws UnsupportedOperationException if the operation is not supported
     * by the specified metadata type
     */
    @MethodCallClass(GetMetadataKeyCall.class)
    CompletableFuture<MetadataInfo> getMetadata(short serialVersion,
                                                MetadataType type,
                                                MetadataKey key,
                                                int seqNum,
                                                AuthContext authCtx,
                                                long timeoutMillis);

    class GetMetadataKeyCall extends AbstractCall<MetadataInfo> {
        final MetadataType type;
        final MetadataKey key;
        final int seqNum;
        final AuthContext authCtx;
        final boolean includeKey;
        public GetMetadataKeyCall(final MetadataType type,
                                  final MetadataKey key,
                                  final int seqNum,
                                  final AuthContext authCtx) {
            this(type, key, seqNum, authCtx, true);
        }
        GetMetadataKeyCall(final MetadataType type,
                           final MetadataKey key,
                           final int seqNum,
                           final AuthContext authCtx,
                           final boolean includeKey) {
            this.type = type;
            this.key = key;
            this.seqNum = seqNum;
            this.authCtx = authCtx;
            this.includeKey = includeKey;
        }
        GetMetadataKeyCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            this(in, serialVersion, true);
        }
        GetMetadataKeyCall(final DataInput in,
                           final short serialVersion,
                           final boolean includeKey)
            throws IOException
        {
            type = readFastExternalOrNull(in, serialVersion,
                                          MetadataType::readFastExternal);
            key = includeKey ?
                readFastExternalOrNull(in, serialVersion,
                                       MetadataKey::readMetadataKey) :
                null;
            seqNum = readPackedInt(in);
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
            this.includeKey = includeKey;
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, type);
            if (includeKey) {
                writeFastExternalOrNull(out, sv, key,
                                        MetadataKey::writeMetadataKey);
            }
            writePackedInt(out, seqNum);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public MetadataInfo readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return readFastExternalOrNull(in, sv,
                                          MetadataInfo::readMetadataInfo);
        }
        @Override
        public void writeResponse(final MetadataInfo result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, result,
                                    MetadataInfo::writeMetadataInfo);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_METADATA_KEY;
        }
        @Override
        public CompletableFuture<MetadataInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getMetadata(serialVersion, type, key, seqNum,
                                       authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("type=").append(type);
            if (includeKey) {
                sb.append(" key=").append(key);
            }
            sb.append(" seqNum=").append(seqNum);
        }
    }

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param newMetadata the latest metadata
     */
    @MethodCallClass(UpdateMetadataCall.class)
    CompletableFuture<Void> updateMetadata(short serialVersion,
                                           Metadata<?> newMetadata,
                                           AuthContext authCtx,
                                           long timeoutMillis);

    class UpdateMetadataCall extends AbstractCall<Void> {
        final Metadata<?> newMetadata;
        final AuthContext authCtx;
        public UpdateMetadataCall(final Metadata<?> newMetadata,
                                  final AuthContext authCtx) {
            this.newMetadata = newMetadata;
            this.authCtx = authCtx;
        }
        UpdateMetadataCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            newMetadata = readFastExternalOrNull(in, serialVersion,
                                                 Metadata::readMetadata);
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, newMetadata,
                                    Metadata::writeMetadata);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Void readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return null;
        }
        @Override
        public void writeResponse(final Void result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.UPDATE_METADATA;
        }
        @Override
        public CompletableFuture<Void>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.updateMetadata(serialVersion, newMetadata, authCtx,
                                          timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("newMetadata=").append(newMetadata);
        }
    }

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param metadataInfo describes the changes to be applied
     *
     * @return the post-update metadata sequence number at the node
     */
    @MethodCallClass(UpdateMetadataInfoCall.class)
    CompletableFuture<Integer> updateMetadata(short serialVersion,
                                              MetadataInfo metadataInfo,
                                              AuthContext authCtx,
                                              long timeoutMillis);

    class UpdateMetadataInfoCall extends AbstractCall<Integer> {
        final MetadataInfo metadataInfo;
        final AuthContext authCtx;
        public UpdateMetadataInfoCall(final MetadataInfo metadataInfo,
                                      final AuthContext authCtx) {
            this.metadataInfo = metadataInfo;
            this.authCtx = authCtx;
        }
        UpdateMetadataInfoCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            metadataInfo = readFastExternalOrNull(
                in, serialVersion, MetadataInfo::readMetadataInfo);
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, metadataInfo,
                                    MetadataInfo::writeMetadataInfo);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Integer readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return in.readInt();
        }
        @Override
        public void writeResponse(final Integer result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            out.writeInt(result);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.UPDATE_METADATA_INFO;
        }
        @Override
        public CompletableFuture<Integer>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.updateMetadata(serialVersion, metadataInfo, authCtx,
                                          timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("metadataInfo=").append(metadataInfo);
        }
    }

    @MethodCallClass(GetKerberosPrincipalsCall.class)
    CompletableFuture<KerberosPrincipals>
        getKerberosPrincipals(short serialVersion,
                              AuthContext authCtx,
                              long timeoutMillis);

    class GetKerberosPrincipalsCall extends AbstractCall<KerberosPrincipals> {
        final AuthContext authCtx;
        public GetKerberosPrincipalsCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetKerberosPrincipalsCall(final DataInput in,
                                  final short serialVersion)
            throws IOException
        {
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public KerberosPrincipals readResponse(final DataInput in,
                                               final short serialVersion)
            throws IOException
        {
            return new KerberosPrincipals(in, serialVersion);
        }
        @Override
        public void writeResponse(final KerberosPrincipals result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            result.writeFastExternal(out, serialVersion);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_KERBEROS_PRINCIPALS;
        }
        @Override
        public CompletableFuture<KerberosPrincipals>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getKerberosPrincipals(serialVersion, authCtx,
                                                 timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) { }
    }

    /**
     * Retrieve table metadata information by specific table id.
     *
     * @param tableId number of table id
     * @param authCtx used for security authentication
     * @param serialVersion
     * @return metadata information which is a table instance
     */
    @MethodCallClass(GetTableByIdCall.class)
    CompletableFuture<MetadataInfo> getTableById(short serialVersion,
                                                 long tableId,
                                                 AuthContext authCtx,
                                                 long timeoutMillis);

    class GetTableByIdCall extends AbstractCall<MetadataInfo> {
        final long tableId;
        final AuthContext authCtx;
        public GetTableByIdCall(final long tableId,
                                final AuthContext authCtx) {
            this.tableId = tableId;
            this.authCtx = authCtx;
        }
        GetTableByIdCall(final DataInput in,
                         final short serialVersion)
            throws IOException
        {
            tableId = readPackedLong(in);
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writePackedLong(out, tableId);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public MetadataInfo readResponse(final DataInput in,
                                         final short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion,
                                          MetadataInfo::readMetadataInfo);
        }
        @Override
        public void writeResponse(final MetadataInfo result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, result,
                                    MetadataInfo::writeMetadataInfo);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_TABLE_BY_ID;
        }
        @Override
        public CompletableFuture<MetadataInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getTableById(serialVersion, tableId, authCtx,
                                        timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("tableId").append(tableId);
        }
    }

    /**
     * Gets the specified table with an optional resource cost. If the table
     * is not found, null is returned. The specified cost will be charged
     * against the table's resource limits. If the cost is greater than 0
     * and the table has resource limits and those limits have been exceeded,
     * either by this call, or by other table activity a ResourceLimitException
     * will be thrown.
     */
    @MethodCallClass(GetTableCall.class)
    CompletableFuture<MetadataInfo> getTable(short serialVersion,
                                             String namespace,
                                             String tableName,
                                             int cost,
                                             AuthContext authCtx,
                                             long timeoutMillis);

    class GetTableCall extends AbstractCall<MetadataInfo> {
        final String namespace;
        final String tableName;
        final int cost;
        final AuthContext authCtx;
        public GetTableCall(final String namespace,
                            final String tableName,
                            final int cost,
                            final AuthContext authCtx) {
            this.namespace = namespace;
            this.tableName = tableName;
            this.cost = cost;
            this.authCtx = authCtx;
        }
        GetTableCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            namespace = readString(in, serialVersion);
            tableName = readString(in, serialVersion);
            cost = in.readInt();
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeString(out, sv, namespace);
            writeString(out, sv, tableName);
            out.writeInt(cost);
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public MetadataInfo readResponse(final DataInput in,
                                         final short serialVersion)
            throws IOException
        {
            return readFastExternalOrNull(in, serialVersion,
                                          MetadataInfo::readMetadataInfo);
        }
        @Override
        public void writeResponse(final MetadataInfo result,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            writeFastExternalOrNull(out, serialVersion, result,
                                    MetadataInfo::writeMetadataInfo);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_TABLE;
        }
        @Override
        public CompletableFuture<MetadataInfo>
            callService(final short serialVersion,
                        final long timeoutMillis,
                        final AsyncClientRepNodeAdmin service) {
            return service.getTable(serialVersion, namespace, tableName, cost,
                                    authCtx, timeoutMillis);
        }
        @Override
        void describeParams(StringBuilder sb) {
            sb.append("namespace=").append(namespace);
            sb.append(" tableName=").append(tableName);
            sb.append(" cost=").append(cost);
        }
    }

    /**
     * Returns this node's vlsn.
     */
    @MethodCallClass(GetVlsnCall.class)
    CompletableFuture<Long>
        getVlsn(short serialVersion,
                            AuthContext authCtx,
                            long timeoutMillis);

    class GetVlsnCall
            extends AbstractCall<Long> {
        final AuthContext authCtx;
        public GetVlsnCall(final AuthContext authCtx) {
            this.authCtx = authCtx;
        }
        GetVlsnCall(final DataInput in, final short serialVersion)
            throws IOException
        {
            authCtx = readFastExternalOrNull(in, serialVersion,
                                             AuthContext::new);
        }
        @Override
        public void writeFastExternal(final DataOutput out, final short sv)
            throws IOException
        {
            writeFastExternalOrNull(out, sv, authCtx);
        }
        @Override
        public Long readResponse(final DataInput in, final short sv)
            throws IOException
        {
            return in.readLong();
        }
        @Override
        public void writeResponse(final Long vlsn,
                                  final DataOutput out,
                                  final short serialVersion)
            throws IOException
        {
            out.writeLong(vlsn);
        }
        @Override
        public ServiceMethodOp getMethodOp() {
            return ServiceMethodOp.GET_VLSN;
        }
        @Override
        public CompletableFuture<Long>
            callService(final short serialVersion,
                        final long timeoutMs,
                        final AsyncClientRepNodeAdmin service) {
            return service.getVlsn(serialVersion, authCtx,
                                               timeoutMs);
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
                                         AsyncClientRepNodeAdmin service);
    }

    /** A MethodCall that simplifies implementing the describeCall method. */
    abstract class AbstractCall<R> implements ServiceMethodCall<R> {
        @Override
        public String describeCall() {
            final StringBuilder sb = new StringBuilder();
            sb.append("AsyncClientRepNodeAdmin.")
                .append(getClass().getSimpleName())
                .append("[");
            describeParams(sb);
            sb.append("]");
            return sb.toString();
        }
        abstract void describeParams(StringBuilder sb);
    }
}
