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

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * The Response to a remote execution via the {@link RequestDispatcher}.
 * The response contains the request result and the following optional
 * components:
 * <ol>
 * <li> Topology changes. If the topology sequence number in the request is
 * less than the sequence number at the request target.
 * </li>
 * <li> Rep Group status update information. If the request changes has
 * group level status changes that it has not yet communicated back to the
 * request originator. </li>
 * </ol>
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Response implements Externalizable, FastExternalizable {

    private static final long serialVersionUID = 1L;

    /* The responder's rep node id. */
    private RepNodeId repNodeId;

    /* The current VLSN associated with the responder. */
    private long vlsn = INVALID_VLSN;
    private Result result;
    private TopologyInfo topoInfo;
    private StatusChanges statusChanges;
    private short serialVersion;

    public Response(RepNodeId repNodeId,
                    long vlsn,
                    Result result,
                    TopologyInfo topoInfo,
                    StatusChanges statusChanges,
                    short serialVersion) {

        this.repNodeId = repNodeId;
        this.vlsn = vlsn;
        this.result = result;
        this.topoInfo = topoInfo;
        this.statusChanges = statusChanges;
        this.serialVersion = serialVersion;
    }

    /* for Externalizable */
    public Response() {
    }

    /**
     * Creates an instance from the input stream.
     *
     * @param in the input stream
     * @param serialVersionIgnored the version of the serialization format --
     * this parameter is ignored
     * @throws IOException if there is a problem reading from the input stream
     */
    public Response(DataInput in, short serialVersionIgnored)
        throws IOException {

        serialVersion = in.readShort();
        final ResourceId rId =
            ResourceId.readFastExternal(in, serialVersion);
        if (!(rId instanceof RepNodeId)) {
            throw new IOException("Expected RepNodeId: " + rId);
        }
        repNodeId = (RepNodeId) rId;
        vlsn = readPackedLong(in);
        result = Result.readFastExternal(in, serialVersion);
        statusChanges =
            in.readBoolean() ? new StatusChanges(in, serialVersion) : null;
        topoInfo =
            in.readBoolean() ? new TopologyInfo(in, serialVersion) : null;
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {

        serialVersion = in.readShort();

        /*
         * The serialized form supports ResourceIds of different subclasses,
         * but here we only allow a RepNodeId.
         */
        repNodeId = (RepNodeId) ResourceId.readFastExternal(in, serialVersion);

        vlsn = in.readLong();

        result = Result.readFastExternal(in, serialVersion);

        /*
         * If there are no changes, statusChanges and topoChanges will be null.
         * Although we are using 'slow' serialization for these objects, when
         * they are null (the frequent case) this is very cheap.  We rely on
         * Topology.getChanges and RequestHandlerImpl.getStatusChanges to
         * return null (not an empty object) when there are no changes.
         */
        statusChanges = (StatusChanges) in.readObject();
        topoInfo = (TopologyInfo) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out)
        throws IOException {

        out.writeShort(serialVersion);
        repNodeId.writeFastExternal(out, serialVersion);
        out.writeLong(vlsn);
        result.writeFastExternal(out, serialVersion);
        out.writeObject(statusChanges);
        out.writeObject(topoInfo);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeShort short}) {@link #getSerialVersion
     *      serialVersion}
     * <li> ({@link RepNodeId}) {@link #getRespondingRN repNodeId}
     * <li> ({@link SerializationUtil#writePackedLong packedLong}) {@link
     *      #getVLSN vlsn}
     * <li> ({@link Result}) {@link #getResult result}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull StatusChanges or
     *      null}) {@link #getStatusChanges statusChanges}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull TopologyInfo or
     *      null}) {@link #getTopoInfo topoInfo}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out,
                                  @SuppressWarnings("hiding")
                                  short serialVersion)
        throws IOException {

        out.writeShort(serialVersion);
        repNodeId.writeFastExternal(out, serialVersion);
        writePackedLong(out, vlsn);
        result.writeFastExternal(out, serialVersion);
        writeFastExternalOrNull(out, serialVersion, statusChanges);
        writeFastExternalOrNull(out, serialVersion, topoInfo);
    }

    /**
     * Must be called before returning a forwarded response, to reset the
     * version to the one specified by the client.
     */
    void setSerialVersion(short serialVersion) {
        this.serialVersion = serialVersion;
    }

    /**
     * Returns the effective serial version.
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Returns the result associated with a request. This result can be
     * returned back to the invoking client.
     */
    public Result getResult() {
        return result;
    }

    /**
     * Returns information about the topology of the responding node or null if
     * the topology at the two nodes is the same.
     */
    public TopologyInfo getTopoInfo() {
        return topoInfo;
    }

    /**
     * Returns any status updates provided by the RN that actually responded
     * to the request. This information is used to update the RGST and the
     * partition map at RNs and KV clients.
     */
    public StatusChanges getStatusChanges() {
        return statusChanges;
    }

    public long getVLSN() {
        return vlsn;
    }

    /**
     * Returns RN that responded to the request. The responding RN may be
     * different from the one to which the request was directed, if the
     * request was forwarded.
     *
     * @return the RepGroupId associated with the responding RN
     */
    public RepNodeId getRespondingRN() {
        return repNodeId;
    }

    /**
     * Sets the status changes that must be associated with the response.
     * @param statusChanges
     */
    public void setStatusChanges(StatusChanges statusChanges) {
        this.statusChanges = statusChanges;
    }

    /**
     * Sets the topology information that must be associated with the response.
     * @param topoInfo
     */
    public void setTopoInfo(TopologyInfo topoInfo) {
        this.topoInfo = topoInfo;
    }

    @Override
    public String toString() {
        return "Response[result=" + result + "]";
    }
}
