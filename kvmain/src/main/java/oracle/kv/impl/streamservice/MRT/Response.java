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

package oracle.kv.impl.streamservice.MRT;

import static oracle.kv.KVVersion.parseVersion;
import static oracle.kv.impl.streamservice.MRT.Response.Type.AGENT_VERSION;
import static oracle.kv.impl.streamservice.MRT.Response.Type.ERROR;
import static oracle.kv.impl.streamservice.MRT.Response.Type.GROUP_AGENT_VERSION;
import static oracle.kv.impl.streamservice.MRT.Response.Type.REQUEST_RESPONSE;
import static oracle.kv.impl.streamservice.MRT.Response.Type.STREAM_TABLE;
import static oracle.kv.impl.streamservice.MRT.Response.Type.SUCCESS;
import static oracle.kv.impl.systables.StreamResponseDesc.COL_RESPONSE_TYPE;
import static oracle.kv.impl.util.SerialVersion.MULTI_MRT_AGENT_VERSION;
import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readMap;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writeMap;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;
import static oracle.kv.impl.xregion.service.MultiServiceConfigValidator.LEAD_AGENT_INDEX;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import oracle.kv.KVVersion;
import oracle.kv.impl.streamservice.ServiceMessage;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.WriteFastExternal;
import oracle.kv.impl.xregion.service.MultiServiceConfigValidator;
import oracle.kv.pubsub.NoSQLSubscriberId;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Messages for the multi-region-table service.
 */
public class Response extends ServiceMessage {

    /**
     * V1 response version number, support single agent only
     */
    private static final int V1_VERSION = 1;
    /**
     * V2 response version number, support multiple agents in group
     */
    private static final int V2_VERSION = 2;

    /**
     * Current version of the json
     */
    public static final int CURRENT_VERSION = V2_VERSION;
    /**
     * Request ID for version information
     */
    public final static int VERSION_REQUEST_ID = -1;
    /**
     * Request ID for tables in streaming
     */
    public final static int STREAM_TABLES_REQUEST_ID = -2;
    /**
     * Type of the response.
     */
    protected final Type type;
    /**
     * V1 agent ID, which support single agent only
     */
    private static final NoSQLSubscriberId V1_AGENT_ID =
        new NoSQLSubscriberId(1, 0);
    /**
     * Response version
     */
    private final short version;
    /**
     * Number of agents in the group
     */
    private final int groupSize;
    /**
     * Map of responses for a given request, indexed by agent index in the
     * group.
     */
    private final Map<Integer, ResponseBody> responseMap;

    /**
     * Constructs an empty response
     */
    private Response(int requestId, int groupSz, Type type) {
        super(ServiceType.MRT, requestId);
        this.type = type;
        this.version = CURRENT_VERSION;
        this.groupSize = groupSz;
        responseMap = new HashMap<>();
    }

    /**
     * Converts a row from response table to a Response object
     *
     * @param row a row from system table
     */
    private Response(Row row) throws IOException {
        super(row);
        if (!getServiceType().equals(ServiceType.MRT)) {
            throw new IllegalStateException("Row is not a MRT response");
        }
        type = Type.getType(row);
        final byte[] payload = getPayloadFromRow(row);
        if (type.isV1Response()) {
            /* read a response row written by V1 agent */
            version = V1_VERSION;
            groupSize = 1;
            responseMap = new HashMap<>();
            if (type.equals(SUCCESS)) {
                addSuccResponse(V1_AGENT_ID);
            } else if (type.equals(ERROR)) {
                final String error = new String(payload);
                addFailResponse(V1_AGENT_ID, error);
            } else {
                final KVVersion kvv = readV1AgentVersion(payload);
                addAgentVersion(V1_AGENT_ID, kvv);
            }
            return;
        }

        /* read a response row written by higher than V1 agent */
        final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
        final DataInput in = new DataInputStream(bais);
        final short serialVersion = readSerialVersion(in);
        version = readRespVersion(in);
        groupSize = readGroupSize(in);
        responseMap = readMap(
            in, serialVersion, HashMap::new,
            (input, ver) -> SerializationUtil.readPackedInt(input),
            this::readResponse);
    }

    /**
     * Test only
     * Returns a response from a row
     */
    public static Response readFromRow(Row row) throws IOException {
        return new Response(row);
    }

    @Override
    public Row toRow(Table responseTable, short maxSerialVersion)
        throws IOException {
        final Row row = super.toRow(responseTable, maxSerialVersion);
        row.put(COL_RESPONSE_TYPE, type.ordinal());
        return row;
    }

    @Override
    protected byte[] getPayload(short maxSerialVersion) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        final short serialVer = MULTI_MRT_AGENT_VERSION;
        out.writeShort(serialVer);
        out.writeShort(version);
        SerializationUtil.writePackedInt(out, groupSize);
        SerializationUtil.writeMap(
            out, serialVer, responseMap,
            (val, ot, ver) -> SerializationUtil.writePackedInt(ot, val),
            (str, ot, ver) -> writeResponse(ot, ver, str));
        return baos.toByteArray();
    }

    @Override
    protected StringBuilder getToString(StringBuilder sb) {
        sb.append(", version=").append(version)
          .append(", type=").append(type)
          .append("(isV1=").append(type.isV1Response()).append(")")
          .append(", group size=").append(groupSize)
          .append(", response=").append(responseMap);
        return sb;
    }

    /**
     * Creates a response for regular request
     *
     * @param requestId request id
     * @param groupSize group size
     * @return a response instance
     */
    public static Response createReqResp(int requestId, int groupSize) {
        return new Response(requestId, groupSize, REQUEST_RESPONSE);
    }

    /**
     * Creates a response for agent version
     *
     * @param groupSize group size
     * @return a response instance
     */
    public static Response createVerResp(int groupSize) {
        return new Response(VERSION_REQUEST_ID, groupSize, GROUP_AGENT_VERSION);
    }

    /**
     * Creates a response for streaming tables
     *
     * @param groupSize group size
     * @return a response instance
     */
    public static Response createStreamTbResp(int groupSize) {
        return new Response(STREAM_TABLES_REQUEST_ID, groupSize, STREAM_TABLE);
    }

    /**
     * Returns the lead agent id in the response
     *
     * @return the lead agent id
     */
    public NoSQLSubscriberId getLeadAgentId() {
        return new NoSQLSubscriberId(groupSize, LEAD_AGENT_INDEX);
    }

    /**
     * Returns true if the response is complete and all agents post success
     * responses.
     *
     * @return true if the response is complete and all agents post success
     * responses, false otherwise
     */
    public boolean isCompleteSucc() {
        if (type.isV1Response()) {
            return type.equals(SUCCESS);
        }

        /* v2 and later responses */
        if (!type.equals(REQUEST_RESPONSE)) {
            throw new IllegalStateException(
                "Response type=" + type +
                ", not from a request, id=" + getRequestId());
        }
        if (!isComplete()) {
            return false;
        }

        /* a response is success iff all agents post succ */
        return responseMap.values().stream().allMatch(s -> {
            final ReqResponse res = (ReqResponse) s;
            return res.succ;
        });
    }

    /**
     * Returns a response object from the specified row. If row is null then
     * null is returned.
     */
    public static Response getFromRow(Row row) throws IOException {
        if (row == null) {
            return null;
        }
        return new Response(row);
    }

    /**
     * Gets the type of response message.
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns the response for a given agent
     *
     * @param sid agent id
     * @return response type
     */
    public ResponseBody getAgentResponse(NoSQLSubscriberId sid) {
        verifyGroupSize(sid);
        final int index = sid.getIndex();
        return responseMap.get(index);
    }

    /**
     * Returns true if the response map contains the response from given agent
     */
    public boolean hasResponse(NoSQLSubscriberId sid) {
        if (groupSize != sid.getTotal()) {
            /* the response is from an agent with different group size */
            return false;
        }
        return responseMap.containsKey(sid.getIndex());
    }

    /**
     * Adds a version response from an agent
     *
     * @param sid agent id
     * @param kvv software version
     */
    public void addAgentVersion(NoSQLSubscriberId sid, KVVersion kvv) {
        if (notVersionResponse()) {
            throw new IllegalStateException(
                "Cannot add version=" + kvv.getNumericVersionString() +
                " for agent id=" + sid + ", response type=" + type +
                " is not a valid version response type");
        }
        verifyGroupSize(sid);
        final AgentVersion agentVer = new AgentVersion(sid.getIndex(), kvv);
        responseMap.put(agentVer.getIndex(), agentVer);
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * Adds a success response from an agent
     *
     * @param sid id of agent
     */
    public void addSuccResponse(NoSQLSubscriberId sid) {
        if (notReqResponse()) {
            throw new IllegalStateException(
                "Response type=" + type + " is not agent group responses");
        }
        verifyGroupSize(sid);
        final int idx = sid.getIndex();
        final ReqResponse succ = ReqResponse.createSuccResponse(idx);
        responseMap.put(idx, succ);
        /* update the timestamp */
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * Adds an error response from an agent
     *
     * @param sid     id of agent
     * @param message error message
     */
    public void addFailResponse(NoSQLSubscriberId sid, String message) {
        if (notReqResponse()) {
            throw new IllegalStateException(
                "Response type=" + type + " is not agent group responses");
        }
        verifyGroupSize(sid);
        final int idx = sid.getIndex();
        final ReqResponse fail = ReqResponse.createFailResponse(idx, message);
        responseMap.put(idx, fail);
        /* update the timestamp */
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * Merges the response from other
     *
     * @param other other response
     */
    public void merge(Response other) {
        if (!type.equals(other.type)) {
            throw new IllegalArgumentException(
                "Mismatch response type, type=" + type +
                ", other=" + other.type);
        }
        if (getRequestId() != other.getRequestId()) {
            throw new IllegalArgumentException(
                "Mismatch request id, reqId=" + getRequestId() +
                ", other reqId=" + other.getRequestId());
        }
        if (groupSize != other.groupSize) {
            throw new IllegalArgumentException(
                "Mismatch group size, size=" + groupSize +
                ", other=" + other.groupSize);
        }

        for (ResponseBody inRespBody : other.responseMap.values()) {
            /* merge response from each agent */
            final int index = inRespBody.getIndex();
            final ResponseBody myRespBody = responseMap.get(index);
            if (myRespBody == null) {
                /* no need to merge */
                responseMap.put(index, inRespBody);
                continue;
            }
            /* merge the response body */
            final ResponseBody merged = myRespBody.merge(inRespBody);
            responseMap.put(index, merged);
        }
        /* update the timestamp */
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * Returns true if the response contains responses from all agents in the
     * group, that is, response is complete
     *
     * @return true if the response is complete false otherwise
     */
    public boolean isComplete() {
        if (type.isV1Response()) {
            return true;
        }
        return responseMap.size() == groupSize;
    }

    /**
     * Gets the number of responses received
     *
     * @return the number of responses received
     */
    public int getNumResponses() {
        return responseMap.size();
    }

    /**
     * Gets the total number of responses in groups
     *
     * @return group size
     */
    public int getGroupSize() {
        return groupSize;
    }

    /**
     * Returns true if the response map has a response from lead agent
     *
     * @return true if the response map has a response from lead agent, or
     * false otherwise.
     */
    public boolean hasResponseFromLead() {
        for (Integer index : responseMap.keySet()) {
            if (MultiServiceConfigValidator.isLead(index)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the agent version for a given agent id
     *
     * @param sid agent id
     * @return null if it does not exist
     */
    public KVVersion getVersion(NoSQLSubscriberId sid) {
        if (notVersionResponse()) {
            throw new IllegalStateException(
                "Cannot read version from response for agent id=" + sid +
                ", response type=" + type + " is not a valid version response" +
                " type");
        }
        verifyGroupSize(sid);
        final ResponseBody res = responseMap.get(sid.getIndex());
        if (res == null) {
            return null;
        }
        return ((AgentVersion) res).version;
    }

    /**
     * Gets the minimal version of all agents in the response
     *
     * @return minimal version
     */
    public KVVersion getMinVersion() {
        if (notVersionResponse()) {
            throw new IllegalStateException(
                "Cannot get min version, response type=" + type +
                " is not a valid version response type");
        }
        if (responseMap.isEmpty()) {
            return null;
        }
        KVVersion ret = null;
        for (ResponseBody resp : responseMap.values()) {
            final AgentVersion agentVer = (AgentVersion) resp;
            final KVVersion ver = agentVer.version;
            if (ret == null) {
                ret = ver;
                continue;
            }
            if (ver.compareTo(ret) < 0) {
                ret = ver;
            }
        }
        return ret;
    }

    /**
     * Reads the V1 agent version from the payload
     * @param payload payload byte array
     * @return V1 agent version
     * @throws IOException if fail to read from the payload
     */
    private KVVersion readV1AgentVersion(byte[] payload) throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(payload);
        final DataInput in = new DataInputStream(bais);
        final short serialVersion = in.readShort();
        if (serialVersion > SerialVersion.CURRENT) {
            throw new IOException("Unsupported serial version "
                                  + serialVersion);
        }
        final int agentId = readPackedInt(in);
        if (agentId != V1_AGENT_ID.getIndex()) {
            throw new IllegalArgumentException(
                "Unexpected v1 agent index=" + agentId +
                ", expected=" + V1_AGENT_ID.getIndex());
        }
        return parseVersion(readNonNullString(in, serialVersion));
    }

    private short readSerialVersion(DataInput in) throws IOException {
        final short serialVersion = in.readShort();
        if (serialVersion > SerialVersion.CURRENT) {
            throw new IOException("Unsupported serial version "
                                  + serialVersion);
        }
        return serialVersion;
    }

    private short readRespVersion(DataInput in) throws IOException {
        final short ver = in.readShort();
        if (ver > CURRENT_VERSION) {
            throw new IOException("Unsupported serial version " + ver);
        }
        return ver;
    }

    private int readGroupSize(DataInput in) throws IOException {
        final int ret = SerializationUtil.readPackedInt(in);
        if (ret < 1) {
            throw new IOException("Invalid group size=" + ret);
        }
        return ret;
    }

    private void verifyGroupSize(NoSQLSubscriberId sid) {
        final int sz = sid.getTotal();
        if (sz != groupSize) {
            throw new IllegalStateException(
                "Mismatch group size in agent id=" + sid +
                ", size=" + sz + ", expected=" + groupSize);
        }
    }

    private void writeResponse(DataOutput out, short ver, ResponseBody val)
        throws IOException {
        switch (type) {
            case GROUP_AGENT_VERSION:
                final AgentVersion agentVer = (AgentVersion) val;
                agentVer.writeFastExternal(out, ver);
                break;
            case REQUEST_RESPONSE:
                final ReqResponse resp = (ReqResponse) val;
                resp.writeFastExternal(out, ver);
                break;
            case STREAM_TABLE:
                final StreamTable st = (StreamTable) val;
                st.writeFastExternal(out, ver);
                break;
            default:
                throw new IllegalStateException(
                    "Unsupported response type=" + type);
        }
    }

    private ResponseBody readResponse(DataInput in, short ver)
        throws IOException {
        switch (type) {
            case GROUP_AGENT_VERSION:
                return new AgentVersion(in, ver);
            case REQUEST_RESPONSE:
                return new ReqResponse(in, ver);
            case STREAM_TABLE:
                return new StreamTable(in, ver);
            default:
                throw new IllegalStateException(
                    "Unsupported response type=" + type);
        }
    }

    private boolean notVersionResponse() {
        return !type.equals(GROUP_AGENT_VERSION) &&
               !type.equals(AGENT_VERSION);
    }

    private boolean notReqResponse() {
        return !type.equals(REQUEST_RESPONSE) &&
               !type.equals(SUCCESS) &&
               !type.equals(ERROR);
    }

    /**
     * Multi-region service response types. New types must be added to the
     * end of this enum to maintain compatibility.
     */
    public enum Type {
        /**
         * For backward compatible to V1 response where there is no version
         * info is encoded in the payload for SUCCESS/ERROR response. The new
         * version response will be able to read the old V1 response row if
         * type column belongs to the these three types
         */
        SUCCESS,        /* Request successfully handled */
        ERROR,          /* There was an error handling the request */
        AGENT_VERSION,  /* Agent version */

        /**
         * Response type for V2 that support agent groups. V2 agent would
         * only use these two types to write and read response rows.
         */
        REQUEST_RESPONSE,   /* regular response for request */
        GROUP_AGENT_VERSION,  /* Agent version */
        STREAM_TABLE; /* streaming table response */

        private static final Type[] VALUES = values();

        private static Type getType(Row row) {
            final int ord = row.get(COL_RESPONSE_TYPE).asInteger().get();
            return VALUES[ord];
        }

        boolean isV1Response() {
            return this.equals(SUCCESS) ||
                   this.equals(ERROR) ||
                   this.equals(AGENT_VERSION);
        }
    }

    public static abstract class ResponseBody
        implements FastExternalizable, Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * Index of the agent in the group
         */
        private final int index;

        ResponseBody(int index) {
            this.index = index;
        }

        ResponseBody(DataInput in, short serialVersion) throws IOException {
            if (serialVersion < MULTI_MRT_AGENT_VERSION) {
                throw new IllegalStateException(
                    "Serial version=" + serialVersion + " does not support " +
                    " XRegion agent group, " +
                    "minimal required version=" + MULTI_MRT_AGENT_VERSION);
            }
            index = SerializationUtil.readPackedInt(in);
        }

        /**
         * Returns a merged response body with the other one
         * @param other other response body
         * @return merged response body
         */
        public abstract ResponseBody merge(ResponseBody other);

        @Override
        public void writeFastExternal(@NonNull DataOutput out,
                                      short serialVersion) throws IOException {
            SerializationUtil.writePackedInt(out, index);
        }

        public int getIndex() {
            return index;
        }
    }

    private static class AgentVersion extends ResponseBody {

        private static final long serialVersionUID = 1L;
        private final KVVersion version;

        AgentVersion(int index, KVVersion version) {
            super(index);
            this.version = version;
        }

        AgentVersion(DataInput in, short serialVersion) throws IOException {
            super(in, serialVersion);
            final String str = SerializationUtil.readString(in, serialVersion);
            version = KVVersion.parseVersion(str);
        }

        @Override
        public ResponseBody merge(ResponseBody other) {
            /* replace existing version */
            return other;
        }

        @Override
        public void writeFastExternal(@NonNull DataOutput out,
                                      short serialVersion) throws IOException {
            super.writeFastExternal(out, serialVersion);
            SerializationUtil.writeString(out, serialVersion,
                                          version.getNumericVersionString());
        }

        @Override
        public String toString() {
            return version.getNumericVersionString();
        }

    }

    public static class ReqResponse extends ResponseBody {

        private static final long serialVersionUID = 1L;

        /**
         * true if the response is success, false otherwise
         */
        private final boolean succ;
        /**
         * error message if response is failure, null for success response
         */
        private final String msg;

        private ReqResponse(int index, boolean succ, String msg) {
            super(index);
            this.succ = succ;

            if (succ) {
                this.msg = null;
                return;
            }

            /* failure response */
            if (msg == null || msg.isEmpty()) {
                throw new IllegalArgumentException(
                    "Failure response must carry a message");
            }
            this.msg = msg;
        }

        ReqResponse(DataInput in, short serialVersion) throws IOException {
            super(in, serialVersion);
            succ = in.readBoolean();
            if (succ) {
                msg = null;
                return;
            }
            msg = SerializationUtil.readNonNullString(in, serialVersion);
        }

        static ReqResponse createSuccResponse(int index) {
            return new ReqResponse(index, true, null);
        }

        static ReqResponse createFailResponse(int index, String msg) {
            return new ReqResponse(index, false, msg);
        }

        @Override
        public String toString() {
            return "ReqResp[index=" + getIndex() +
                   ", resp=" + (succ ? "SUCC" : "FAIL, error=" + msg) + "]";
        }

        @Override
        public void writeFastExternal(@NonNull DataOutput out,
                                      short serialVersion)
            throws IOException {
            super.writeFastExternal(out, serialVersion);
            out.writeBoolean(succ);
            if (!succ) {
                SerializationUtil.writeNonNullString(out, serialVersion, msg);
            }
        }

        @Override
        public ResponseBody merge(ResponseBody other) {
            /* replace existing response */
            return other;
        }

        public String getErrorMsg() {
            return msg;
        }

        public boolean isSucc() {
            return succ;
        }
    }

    public static class StreamTable extends ResponseBody {

        private static final long serialVersionUID = 1L;

        /**
         * map from a source region name to a set of table ids in streaming
         * from that region
         */
        private final Map<String, Set<Long>> tableIds;

        StreamTable(int index) {
            super(index);
            tableIds = new HashMap<>();
        }

        StreamTable(DataInput in, short serialVersion) throws IOException {
            super(in, serialVersion);
            tableIds =
                readMap(in, serialVersion, HashMap::new,
                        SerializationUtil::readString,
                        (in2, sv) ->
                            readCollection(in2, sv, HashSet::new,
                                           (in3, sv3) -> readPackedLong(in3)));
        }

        @Override
        public void writeFastExternal(@NonNull DataOutput out,
                                      short serialVersion) throws IOException {
            super.writeFastExternal(out, serialVersion);
            writeMap(out, serialVersion, tableIds,
                     WriteFastExternal::writeString,
                     (val, outp, sver) ->
                         writeCollection
                             (outp, sver, val,
                              (i, outpp, sv) -> writePackedLong(outpp, i)));
        }

        @Override
        public String toString() {
            return "{index=" + getIndex() +
                   ", table ids in streaming=" +
                   tableIds.entrySet().stream()
                           .map(e-> e.getKey() + ": " +
                                    e.getValue())
                           .collect(Collectors.toSet()) + "}";
        }

        @Override
        public ResponseBody merge(ResponseBody other) {
            final StreamTable otherSt = (StreamTable) other;
            for (String region : otherSt.getRegions()) {
                /* for each region, replace the existing table ids */
                final Set<Long> ids = otherSt.getStreamTables(region);
                tableIds.put(region, ids);
            }
            /* return merged one */
            return this;
        }

        void setStreamTables(String region, Set<Long> ids) {
            tableIds.put(region, ids);
        }

        /**
         * Returns a set of table ids for the tables in streaming, or null of
         * no table is streaming from the given region
         * @param region region name
         * @return a set of table ids for the tables in streaming, or null
         */
        public Set<Long> getStreamTables(String region) {
            return tableIds.get(region);
        }

        Set<String> getRegions() {
            return tableIds.keySet();
        }

    }

    /**
     * Sets a streaming tables from source region for an agent
     *
     * @param sid agent id
     * @param sourceRegion source region name
     * @param tableIds table ids
     */
    public void setStreamingTables(NoSQLSubscriberId sid,
                                   String sourceRegion,
                                   Set<Long> tableIds) {
        if (!type.equals(STREAM_TABLE)) {
            throw new IllegalStateException(
                "Cannot set streaming tables=" + tableIds +
                " from region=" + sourceRegion +
                ", response type=" + type + " is not streaming table " +
                "responses");
        }
        verifyGroupSize(sid);
        setStreamingTables(sid.getIndex(), sourceRegion, tableIds);
    }

    private void setStreamingTables(int idx, String region, Set<Long> ids) {
        StreamTable tb = (StreamTable) responseMap.get(idx);
        if (tb == null) {
            tb = new StreamTable(idx);
            responseMap.put(idx, tb);
        }
        tb.setStreamTables(region, ids);
        setTimestamp(System.currentTimeMillis());
    }

    /**
     * Gets the map of streaming tables by agent index.
     * @param sourceRegion source region name
     * @return the map of streaming tables by agent index.
     */
    public Map<Integer, Set<Long>> getStreamingTables(String sourceRegion) {
        if (!type.equals(STREAM_TABLE)) {
            throw new IllegalStateException(
                "Cannot get streaming tables, response type=" + type + " is " +
                "not streaming table responses");
        }
        final Map<Integer, Set<Long>> ret = new HashMap<>();
        responseMap.values().forEach(entry -> {
            final StreamTable st = (StreamTable) entry;
            final int idx = st.getIndex();
            final Set<Long> tids = st.getStreamTables(sourceRegion);
            if (tids == null) {
                ret.put(idx, Collections.emptySet());
            } else {
                ret.put(idx, new HashSet<>(tids));
            }
        });
        return ret;
    }
}