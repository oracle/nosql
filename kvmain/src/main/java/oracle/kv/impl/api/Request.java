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

package oracle.kv.impl.api;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeArrayLength;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.impl.api.AsyncRequestHandler.RequestMethodOp;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.NOP;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.fault.TTLFaultException;
import oracle.kv.impl.rep.migration.generation.PartitionGenNum;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.contextlogger.LogContext;

/**
 * A request is issued either between Rep Nodes in the KV Store or from a
 * client to a Rep Node.  The request carries an operation to be performed.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Request implements Externalizable, MethodCall<Response> {

    private static final long serialVersionUID = 1;

    /**
     * Used during testing: A non-zero value specifies the current serial
     * version, which acts as the maximum value for a request's serial version.
     */
    private static volatile short testCurrentSerialVersion;

    /**
     * Used during testing: A non-zero value specifies the minimum permitted
     * serial version accepted by the server.
     */
    public static volatile short testServerMinimumSerialVersion;

    /**
     * If non-zero, use this serial version when reading in fast externalizable
     * format. For testing.
     */
    public static volatile short testReadSerialVersion;

    /**
     * If non-zero, write this serial version when writing in fast
     * externalizable format. For testing.
     */
    public static volatile short testWriteSerialVersion;

    /**
     * The serialization version of the request and all its sub-objects.
     */
    private short serialVersion;

    /**
     * The API operation to execute.
     */
    private InternalOperation op;

    /**
     * The partition ID is used to determine and validate the destination rep
     * group.
     */
    private PartitionId partitionId;

    /**
     * The replication group ID is used to determine and validate the
     * destination. If equal to RepGroupId.NULL_ID the partitionId is used.
     *
     * Introduced in SerialVersion.V4.
     */
    private RepGroupId repGroupId;

    /**
     * Whether the request performs write, versus read, operations.
     */
    private boolean write;

    /**
     * The durability of a write request, null for a read request.
     */
    private Durability durability;

    /**
     * The consistency of a read request, null for a write request.
     */
    private Consistency consistency;

    /**
     * The sequence number of the topology {@link Topology#getSequenceNumber}
     * used as the basis for routing the request.
     */
    private int topoSeqNumber;

    /**
     * Identifies the original request dispatcher making the request. It's not
     * updated through forwards of the request.
     */
    private ResourceId initialDispatcherId;

    /**
     * The "time to live" associated with the request. It's decremented each
     * time the message is forwarded. Messages may be forwarded across rep
     * groups (in case of obsolete partition information) and within a rep
     * group in the absence of current "master" information.
     */
    private int ttl;

    /**
     * The RNS through which the request was forwarded in an attempt to locate
     * a master. The number of such forwards cannot exceed the size of an RG.
     * Each element in the array contains the "group relative" nodeNum of the
     * RepNodeId. This array is used to ensure that there are no forwarding
     * loops within an RG.
     */
    private byte[] forwardingRNs = new byte[0];

    /**
     * The timeout associated with the request. It bounds the maximum time
     * taken to execute the request and does not include request transmission
     * times.
     */
    private int timeoutMs;

    /**
     * The AuthContext associated with the request
     */
    private AuthContext authCtx;

    /**
     * The IDs of the zones that can be used for reads, or null if not
     * restricted.  Introduced with {@link SerialVersion#V4}.
     */
    private int[] readZoneIds = null;

    /**
     * The LogContext associated with this request, or null if there is none.
     * Introduced with {@link SerialVersion#V16}
     */
    private LogContext lc = null;

    /**
     * If set do not charge for read or write throughput.
     */
    private boolean noCharge = false;

    /**
     * Set to true if the request must be executed at the master RN.
     * This can happen during partition migration. Specifically, a query is
     * first sent to an RN that is a replica RN in the migrationtarget shard.
     * At that RN, the query finds out that a partition that is known to have
     * migrated to that shard is not "open" yet (because, due to replication
     * delay, the records that migrated from the source shard to the target
     * master RN have not all been replacated to the target replica RN). In
     * this case, the query will throw an RNUnavailableException with its
     * "needsMaster" field set to true. When the RequestDispatcher gets this
     * RNUnavailableException, it will set this.needsMaster to true, so that
     * the request will be redirected to the target master RN.
     */
    private transient boolean needsMaster = false;

    /**
     * The partition generation number associated with this request.
     *
     * This transient field is set and used on RN locally for time consistency
     * read requests check, always be null for other requests. It may also
     * be null for time consistency read requests if partition generation
     * table hasn't initialized.
     */
    private transient PartitionGenNum partitionGenNum = null;

    /**
     * Creates a partition based request. The target of this request is
     * specified by the partition ID.
     * <p>
     * The topoSeqNumber param is used to determine the {@link TopologyChange
     * topology changes}, if any, that must be returned back in the response.
     * <p>
     * The remoteRequestHandlerId is used to determine the rep group state
     * changes, if any, that need to be returned back as part of the response.
     * <p>
     * @param op the operation to be performed
     * @param partitionId the target partition
     * @param topoSeqNumber identifies the topology used as the basis for the
     * request
     * @param dispatcherId identifies the invoking
     * {@link RequestDispatcher}
     * @param timeoutMs the maximum number of milliseconds that should be used
     * to execute the request
     * @param readZoneIds the IDs of the zones that can be used for reads, or
     * {@code null} for writes or unrestricted read requests
     */
    public Request(InternalOperation op,
                   PartitionId partitionId,
                   boolean write,
                   Durability durability,
                   Consistency consistency,
                   int ttl,
                   int topoSeqNumber,
                   ResourceId dispatcherId,
                   int timeoutMs,
                   int[] readZoneIds) {
        this(op, partitionId, RepGroupId.NULL_ID, write,
             durability, consistency, ttl, topoSeqNumber,
             dispatcherId, timeoutMs, readZoneIds);
    }

    /**
     * Creates a group based request. The target of this request is specified
     * by the replication group ID.
     * <p>
     * The topoSeqNumber param is used to determine the {@link TopologyChange
     * topology changes}, if any, that must be returned back in the response.
     * <p>
     * The remoteRequestHandlerId is used to determine the rep group state
     * changes, if any, that need to be returned back as part of the response.
     * <p>
     * @param op the operation to be performed
     * @param repGroupId the target rep group
     * @param topoSeqNumber identifies the topology used as the basis for the
     * request
     * @param dispatcherId identifies the invoking {@link RequestDispatcher}
     */
    public Request(InternalOperation op,
                   RepGroupId repGroupId,
                   boolean write,
                   Durability durability,
                   Consistency consistency,
                   int ttl,
                   int topoSeqNumber,
                   ResourceId dispatcherId,
                   int timeoutMs,
                   int[] readZoneIds) {
        this(op, PartitionId.NULL_ID, repGroupId, write,
             durability, consistency, ttl, topoSeqNumber,
             dispatcherId, timeoutMs, readZoneIds);
    }

    private Request(InternalOperation op,
                    PartitionId partitionId,
                    RepGroupId repGroupId,
                    boolean write,
                    Durability durability,
                    Consistency consistency,
                    int ttl,
                    int topoSeqNumber,
                    ResourceId dispatcherId,
                    int timeoutMs,
                    int[] readZoneIds) {
        checkNull("op", op);
        checkNull("partitionId", partitionId);
        checkNull("repGroupId", repGroupId);
        if (write) {
            checkNull("durability", durability);
            if (consistency != null) {
                throw new IllegalArgumentException(
                    "Consistency should be null");
            }
            if (readZoneIds != null) {
                throw new IllegalArgumentException(
                    "ReadZoneIds should be null");
            }
        } else {
            checkNull("consistency", consistency);
            if (durability != null) {
                throw new IllegalArgumentException(
                    "Durability should be null");
            }
        }
        checkNull("dispatcherId", dispatcherId);

        this.serialVersion = SerialVersion.UNKNOWN;
        this.op = op;
        this.partitionId = partitionId;
        this.repGroupId = repGroupId;
        this.write = write;
        this.durability = durability;
        this.consistency = consistency;
        this.ttl = ttl;
        this.topoSeqNumber = topoSeqNumber;
        this.initialDispatcherId = dispatcherId;
        this.timeoutMs = timeoutMs;
        this.readZoneIds = readZoneIds;
    }

    /**
     * Creates an instance from data in the specified input stream.
     *
     * @param in the input stream
     * @throws IOException if there is a problem reading from the input stream
     */
    public Request(DataInput in)
        throws IOException {

        readExternalDataInput(in);
    }

    /**
     * Factory method for creating NOP requests.
     */
    public static Request createNOP(int topoSeqNumber,
                                    ResourceId dispatcherId,
                                    int timeoutMs) {

        return new Request(new NOP(),
                           PartitionId.NULL_ID,
                           false,
                           null,
                           Consistency.NONE_REQUIRED,
                           1,
                           topoSeqNumber,
                           dispatcherId,
                           timeoutMs,

                           /*
                            * NOP requests are not generated by users, so they
                            * ignore read zones restrictions.
                            *
                            * TODO: Consider restricting NOP requests from
                            * being sent to RNs outside of the specified read
                            * zones.
                            */
                           null);
    }

    /* for Externalizable */
    public Request() {
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException {

        readExternalDataInput(in);
    }

    private void readExternalDataInput(DataInput in)
        throws IOException {

        serialVersion = in.readShort();
        if (testReadSerialVersion > 0) {
            serialVersion = testReadSerialVersion;
        }
        final short minimumVersion = (testServerMinimumSerialVersion > 0) ?
            testServerMinimumSerialVersion :
            SerialVersion.MINIMUM;
        if (serialVersion < minimumVersion) {
            throw SerialVersion.clientUnsupportedException(
                serialVersion, minimumVersion);
        }
        partitionId = new PartitionId(in.readInt());


        repGroupId = new RepGroupId(in.readInt());

        if (!repGroupId.isNull() && !partitionId.isNull()) {
            throw new IllegalStateException("Both partition ID and group ID " +
                                            "are non-null");
        }
        write = in.readBoolean();
        if (write) {
            durability = Durability.readFastExternal(in, serialVersion);
            consistency = null;
        } else {
            durability = null;
            consistency = Consistency.readFastExternal(in, serialVersion);
        }
        ttl = in.readInt();

        final byte asize = in.readByte();
        forwardingRNs = new byte[asize];
        for (int i = 0; i < asize; i++) {
            forwardingRNs[i] = in.readByte();
        }

        timeoutMs = in.readInt();
        topoSeqNumber = in.readInt();
        initialDispatcherId = ResourceId.readFastExternal(in, serialVersion);
        op = InternalOperation.readFastExternal(in, serialVersion);

        /*
         * Read the number of restricted read zones followed
         * by that number of zone IDs, with 0 or -1 meaning no restriction.
         * Also, write the AuthContext object information.
         */
        final int len =  readSequenceLength(in);
        if (len <= 0) {
            readZoneIds = null;
        } else {
            readZoneIds = new int[len];
            for (int i = 0; i < len; i++) {
                readZoneIds[i] = in.readInt();
            }
        }

        if (in.readBoolean()) {
            authCtx = new AuthContext(in, serialVersion);
        } else {
            authCtx = null;
        }

        if (in.readBoolean()) {
            lc = new LogContext(in, serialVersion);
        } else {
            lc = null;
        }
        noCharge = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out)
        throws IOException {

        writeExternalDataOutput(out);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeShort short}) {@link #getSerialVersion
     *      serialVersion}
     * <li> ({@link DataOutput#writeInt int}) {@link #getPartitionId
     *      partitionId}
     * <li> ({@link DataOutput#writeInt int}) {@link #getRepGroupId repGroupId}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #isWrite write}
     * <li> <i>[Choice]</i> ({@link Durability}) {@link #getDurability
     *      durability} // if write
     * <li> <i>[Choice]</i> ({@link Consistency}) {@link #getConsistency
     *      consistency} // if not write
     * <li> ({@link DataOutput#writeInt int}) {@link #getTTL ttl}
     * <li> ({@code byte}) <i>number of forwarding RNs</i>
     * <li> ({@code byte[]}) {@link #getForwardingRNs forwardingRNs}
     * <li> ({@link DataOutput#writeInt int}) {@link #getTimeout timeoutMs}
     * <li> ({@link DataOutput#writeInt int}) {@link #getTopoSeqNumber
     *      topoSeqNumber}
     * <li> ({@link ResourceId}) {@link #getInitialDispatcherId
     *      initialDispatcherId}
     * <li> ({@link InternalOperation}) {@link #getOperation op}
     * <li> ({@link SerializationUtil#writeArrayLength sequence length})
     *      <i>number of read zones</i>
     * <li> ({@link DataOutput#writeInt int} {@code []}) {@link #getReadZoneIds
     *      readZoneIds}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull AuthContext or
     *      null}) {@link #getAuthContext authCtx}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull LogContext or null})
     *      {@link #getLogContext lc}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #isWrite noCharge}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out,
                                  @SuppressWarnings("hiding")
                                  short serialVersion)
        throws IOException {

        writeExternalDataOutput(out);
    }

    private void writeExternalDataOutput(DataOutput out)
        throws IOException {

        assert serialVersion != SerialVersion.UNKNOWN;

        /*
         * Verify that the server can handle this operation.
         */
        short requiredVersion = op.getOpCode().requiredVersion();
        if (requiredVersion > serialVersion) {
            throw new UnsupportedOperationException
                ("Attempting an operation that is not supported by " +
                 "the server version.  Server version is " + serialVersion +
                 ", required version is " + requiredVersion +
                 ", operation is " + op);
        }

        out.writeShort(testWriteSerialVersion > 0 ?
                       testWriteSerialVersion :
                       serialVersion);
        out.writeInt(partitionId.getPartitionId());
        out.writeInt(repGroupId.getGroupId());
        out.writeBoolean(write);
        if (write) {
            durability.writeFastExternal(out, serialVersion);
        } else {
            consistency.writeFastExternal(out, serialVersion);
        }
        out.writeInt(ttl);

        if (forwardingRNs.length > Byte.MAX_VALUE) {
            throw new IllegalStateException(
                "Too many forwarding RNs: " + forwardingRNs.length);
        }
        out.writeByte(forwardingRNs.length);
        for (byte forwardingRN : forwardingRNs) {
            out.writeByte(forwardingRN);
        }

        out.writeInt(timeoutMs);
        out.writeInt(topoSeqNumber);
        initialDispatcherId.writeFastExternal(out, serialVersion);
        op.writeFastExternal(out, serialVersion);

        /*
         * Write the number of restricted read zones
         * followed by that number of zone IDs, with 0 meaning no
         * restriction
         */
        writeArrayLength(out, readZoneIds);

        if (readZoneIds != null) {
            for (final int znId : readZoneIds) {
                out.writeInt(znId);
            }
        }
        writeFastExternalOrNull(out, serialVersion, authCtx);
        writeFastExternalOrNull(out, serialVersion, lc);
        out.writeBoolean(noCharge);
    }

    @Override
    public RequestMethodOp getMethodOp() {
        return RequestMethodOp.EXECUTE;
    }

    /**
     * Write the response using the request serial version, not the one stored
     * in this request. The distinction matters when doing forwarding because
     * the request gets its serial version changed for the forwarding, and the
     * serial version parameter represents the original serial version that
     * should be used in the response.
     */
    @Override
    public void writeResponse(final Response response,
                              final DataOutput out,
                              @SuppressWarnings("hiding")
                              final short serialVersion)
        throws IOException
    {
        response.writeFastExternal(out, serialVersion);
    }

    @Override
    public Response readResponse(final DataInput in,
                                 final short serialVersionIgnore)
        throws IOException
    {
        return new Response(in, serialVersion);
    }

    @Override
    public boolean includesTimeout() {
        return true;
    }

    @Override
    public long getTimeoutMillis() {
        return getTimeout();
    }

    @Override
    public String describeCall() {
        return "AsyncRequestHandler.ExecuteCall[" + this + "]";
    }

    public InternalOperation getOperation() {
        return op;
    }

    public void setNeedsMaster(boolean v) {
        needsMaster = v;
    }

    /**
     * Indicates if the request must be performed on the master.
     *
     * @return true if the request must be run on the master, false otherwise
     */
    public boolean needsMaster() {
        return (isWrite() ||
                getConsistency() == Consistency.ABSOLUTE ||
                needsMaster);
    }

    /**
     * Indicates if the request must be performed on a replica rather
     * than the master.
     *
     * @return true if the request must be run on a replica, false otherwise
     */
    @SuppressWarnings("deprecation")
    public boolean needsReplica() {
        return !isWrite() &&
               (getConsistency() == Consistency.NONE_REQUIRED_NO_MASTER);
    }

    /**
     * Returns True if the request writes to the environment.
     */
    public boolean isWrite() {
        return write;
    }

    /**
     * Returns the durability associated with a write request, or null for a
     * read request.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * Returns the consistency associated with a read request, or null for a
     * write request.
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Must be called before serializing the request by passing it to a remote
     * method.
     */
    public void setSerialVersion(short serialVersion) {

        /* Limit the serial version to testCurrentSerialVersion, if set */
        if ((testCurrentSerialVersion != 0) &&
            (serialVersion > testCurrentSerialVersion)) {
            serialVersion = testCurrentSerialVersion;
        }
        this.serialVersion = serialVersion;
    }

    /**
     * Set the current serial version to a different value, for testing.
     * Specifying {@code 0} reverts to the standard value.
     */
    public static void setTestSerialVersion(final short testSerialVersion) {
        testCurrentSerialVersion = testSerialVersion;
    }

    /**
     * Returns a value used to construct a matching response for this request,
     * so that the client receives a response serialized with the same version
     * as the request.
     */
    short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Returns the partition ID associated with the request.
     */
    public PartitionId getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(PartitionId partitionId) {
        this.partitionId = partitionId;
    }

    public RepGroupId getRepGroupId() {
        return repGroupId;
    }

    public int getTopoSeqNumber() {
        return topoSeqNumber;
    }

    public void setTopoSeqNumber(int topoSeqNumber) {
        this.topoSeqNumber = topoSeqNumber;
    }

    public ResourceId getInitialDispatcherId() {
        return initialDispatcherId;
    }

    public int getTTL() {
        return ttl;
    }

    public void decTTL() throws FaultException {
        if (ttl-- == 0) {
            throw new TTLFaultException
                ("TTL exceeded for request: " + getOperation() +
                 " request dispatched by: " + getInitialDispatcherId());
        }
    }

    public int getTimeout() {
        return timeoutMs;
    }

    public void setTimeout(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public boolean isNoCharge() {
        return noCharge;
    }

    public void setNoCharge(boolean flag) {
        noCharge = flag;
    }

    @Override
    public String toString() {
        return op.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Request)) {
            return false;
        }
        final Request other = (Request) obj;
        return (serialVersion == other.serialVersion) &&
            op.equals(other.op) &&
            Objects.equals(partitionId, other.partitionId) &&
            Objects.equals(repGroupId, other.repGroupId) &&
            (write == other.write) &&
            Objects.equals(durability, other.durability) &&
            Objects.equals(consistency, other.consistency) &&
            (topoSeqNumber == other.topoSeqNumber) &&
            Objects.equals(initialDispatcherId, other.initialDispatcherId) &&
            (ttl == other.ttl) &&
            Arrays.equals(forwardingRNs, other.forwardingRNs) &&
            (timeoutMs == other.timeoutMs) &&
            Objects.equals(authCtx, other.authCtx) &&
            Arrays.equals(readZoneIds, other.readZoneIds) &&
            Objects.equals(lc, other.lc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serialVersion,
                            op,
                            partitionId,
                            repGroupId,
                            write,
                            durability,
                            consistency,
                            topoSeqNumber,
                            initialDispatcherId,
                            ttl,
                            forwardingRNs,
                            timeoutMs,
                            authCtx,
                            readZoneIds,
                            lc);
    }

    /**
     * Clears out the RNs in the forwarding chain. This typically done before
     * the request is forwarded to a node in a different group.
     */
    public void clearForwardingRNs() {
        forwardingRNs = new byte[0];
    }

    /**
     * Returns the set of RNIds that forwarded this request.
     */
    public Set<RepNodeId> getForwardingRNs(int rgid) {
        final HashSet<RepNodeId> forwardingRNIds = new HashSet<RepNodeId>();

        for (int nodeNum : forwardingRNs) {
            forwardingRNIds.add(new RepNodeId(rgid, nodeNum));
        }

        return forwardingRNIds;
    }

    /**
     * Updates the list of forwarding RNs, if the current dispatcher is an RN.
     */
    public void updateForwardingRNs(ResourceId currentDispatcherId,
                                    int groupId) {

        if (!currentDispatcherId.getType().isRepNode()) {
            return;
        }

        assert currentDispatcherId.getType().isRepNode();

        final RepNodeId repNodeId = (RepNodeId) currentDispatcherId;

        if (repNodeId.getGroupId() == groupId) {
            /* Add this RN to the list. */
            final byte[] updateList = new byte[forwardingRNs.length + 1];
            if (updateList.length > Byte.MAX_VALUE) {
                throw new IllegalStateException(
                    "Too many forwarding RNs: " + updateList.length);
            }
            System.arraycopy(forwardingRNs, 0,
                             updateList, 0, forwardingRNs.length);
            if (repNodeId.getNodeNum() > Byte.MAX_VALUE) {
                throw new IllegalStateException(
                    "Invalid forwarding RN ID: " + repNodeId.getNodeNum());
            }
            updateList[forwardingRNs.length] = (byte) repNodeId.getNodeNum();
            forwardingRNs = updateList;
        } else {
            /* Forwarding outside the group. */
            forwardingRNs = new byte[0];
        }
    }

    /**
     * Returns true if the request was initiated by the
     * <code>resourceId</code>.
     */
    public boolean isInitiatingDispatcher(ResourceId resourceId) {

        return initialDispatcherId.equals(resourceId);
    }

    /**
     * Set the security context in preparation for execution.
     */
    public void setAuthContext(AuthContext useAuthCtx) {
        this.authCtx = useAuthCtx;
    }

    /**
     * Get the security context
     */
    public AuthContext getAuthContext() {
        return authCtx;
    }

    /**
     * Get the log context
     */
    public LogContext getLogContext() {
        return lc;
    }

    /**
     * Set the log context
     */
    public void setLogContext(LogContext lc) {
        this.lc = lc;
    }

    /**
     * Returns the IDs of the zones that can be used for read operations, or
     * {@code null} if not restricted.
     *
     * @return the zone IDs or {@code null}
     */
    public int[] getReadZoneIds() {
        return readZoneIds;
    }

    /**
     * Checks if an RN in a zone with the specified zone ID can be used for
     * this request.
     *
     * @param znId the zone ID or {@code null} if not known
     * @return whether an RN in the specified zone can be used
     */
    public boolean isPermittedZone(final DatacenterId znId) {
        if (write || (readZoneIds == null)) {
            return true;
        }
        if (znId == null) {
            return false;
        }
        final int znIdInt = znId.getDatacenterId();
        for (int elem : readZoneIds) {
            if (elem == znIdInt) {
                return true;
            }
        }
        return false;
    }

    /**
     * Associate a partition generation number with this request.
     *
     * @param genNum partition generation number
     * @throws IllegalArgumentException if the request is not a time consistency
     * read request
     */
    void setPartitionGenNum(PartitionGenNum genNum) {
        if (isWrite() ||
            (!(getConsistency() instanceof Consistency.Time) &&
             !(getConsistency() instanceof Consistency.NoneRequired))) {
            throw new IllegalArgumentException(
                "Partition generation number should only be set " +
                "for time consistency read request");
        }
        this.partitionGenNum = genNum;
    }

    /**
     * Get partition generation number associated with this request.
     * @return partition generation number
     */
    PartitionGenNum getPartitionGenNum() {
        return partitionGenNum;
    }
}
