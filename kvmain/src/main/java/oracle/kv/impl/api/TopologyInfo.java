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

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeCollectionLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullCollection;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.api.rgstate.RepNodeStateUpdateThread;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Topology information that's returned as part of a response whenever there's
 * a mismatch between the requester/responder topologies.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class TopologyInfo implements MetadataInfo, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The topology id associated with the topology that supplied the topo
     * seq number and the changes.
     *
     * @since 2.0
     */
    private final long topoId;

    /**
     * The responder's topology seq number.
     */
    private final int respSeqNum;

    /**
     * The changes to be communicated back, or null if the responder's
     * topology is obsolete relative to that of the requester. The list may
     * also be null if the responder has a newer topology but does not have
     * all the changes that the requester needs to bring its topology up to
     * date. It's then up to the requester to pull a complete copy of the
     * topology from this responding RN.
     */
    private final List<TopologyChange> changes;

    /**
     * Signature of topology represented by this TopologyInfo. Will be null for
     * an empty topology info.
     */
    private final byte[] topoSignature;

    /**
     * Used to denote that a RN has an empty Topology and is waiting for
     * topology to be pushed to it.
     */
    public static TopologyInfo EMPTY_TOPO_INFO =
        new TopologyInfo(Topology.EMPTY_TOPOLOGY_ID,
                         Topology.EMPTY_SEQUENCE_NUMBER, null, null);

    public TopologyInfo(Topology topo,
                        List<TopologyChange> changes) {
        this(topo.getId(), topo.getSequenceNumber(), changes,
             topo.getSignature());
    }

    public TopologyInfo(long topoId,
                        int respSeqNum,
                        List<TopologyChange> changes,
                        byte[] topoSignature) {
        this.topoId = topoId;
        this.respSeqNum = respSeqNum;
        this.changes = changes;
        this.topoSignature = topoSignature;
    }

    public TopologyInfo(DataInput in, short serialVersion)
        throws IOException {

        this.topoId = in.readLong();
        this.respSeqNum = readPackedInt(in);

        final int numChanges = readSequenceLength(in);
        if (numChanges == -1) {
            changes = null;
        } else {
            changes = new ArrayList<TopologyChange>(numChanges);
            for (int i = 0; i < numChanges; i++) {
                changes.add(
                    TopologyChange.readFastExternal(in, serialVersion));
            }
        }
        topoSignature = readByteArray(in);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link DataOutput#writeLong long}) {@link #getTopoId topoId}
     * <li> ({@link SerializationUtil#writePackedInt packedInt}) {@link
     *      #getSequenceNumber respSeqNum}
     * <li> ({@link SerializationUtil#writeCollectionLength sequence length})
     *      <i>number of changes</i>
     * <li> <i>[Optional]</i> ({@link TopologyChange}{@code []}) {@link
     *      #getChanges changes}
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
     *      #getTopoSignature topoSignature}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeLong(topoId);
        writePackedInt(out, respSeqNum);

        if (changes == null) {
            writeCollectionLength(out, changes);
        } else {
            writeNonNullCollection(out, serialVersion, changes);
        }
        writeByteArray(out, topoSignature);
    }

    @Override
    public MetadataType getType() {
        return MetadataType.TOPOLOGY;
    }

    @Override
    public MetadataInfoType getMetadataInfoType() {
        return MetadataInfoType.TOPOLOGY_INFO;
    }

    @Override
    public int getSequenceNumber() {
        return respSeqNum;
    }

    @Override
    public boolean isEmpty() {
        return (changes == null) || changes.isEmpty();
    }

    public long getTopoId() {
        return topoId;
    }

    public byte[] getTopoSignature() {
        if (topoSignature == null) {
            return null;
        }
        return Arrays.copyOf(topoSignature, topoSignature.length);
    }

    /**
     * Returns the list of topology changes.
     * <p>
     * It's the list of topology changes that were returned because, the
     * responding node had more up to date topology information than the node
     * that initiated the request. That is, {@link Request#getTopoSeqNumber()}
     * &lt; {@link Topology#getSequenceNumber} at the RN that actually serviced
     * the request.
     * <p>
     * The requesting RN will take these changes and apply them to its copy of
     * the topology to ensure that it's at least as current as the responding
     * node. Note that it may be possible for the requesting node to get
     * multiple copies of changes lists from the same, or different, RNs until
     * its copy is caught up and the updated sequence number is sent out in
     * subsequent requests. So the topology update must be appropriately
     * synchronized.
     * <p>
     * The list is null if {@link Request#getTopoSeqNumber()} &gt;=
     * {@link Topology#getSequenceNumber}, that is, the requesting node is at
     * least as current as the responding node. Or if the responding node has a
     * more current topology, but does not have all the incremental changes
     * needed to bring the requesting node up to date.
     * <p>
     * If the requesting node has a more current topology, the requesting node
     * arranges to push the topology changes over to the responding node via
     * the {@link RepNodeStateUpdateThread}.
     */
    public List<TopologyChange> getChanges() {
        return changes;
    }
}
