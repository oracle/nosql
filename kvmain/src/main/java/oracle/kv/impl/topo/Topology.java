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

package oracle.kv.impl.topo;

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import oracle.kv.FaultException;
import oracle.kv.impl.api.RequestHandler;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.fault.UnknownVersionException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.change.TopologyChange;
import oracle.kv.impl.topo.change.TopologyChangeTracker;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

import oracle.nosql.common.json.ObjectNode;

/**
 * Topology describes the general logical layout of the KVS and its
 * relationship to the physical elements (SNs) on which it has been mapped.
 * <p>
 * The logical layout is defined in terms of the partitions, the assignment of
 * the partitions to replication groups, the RNs that comprise each replication
 * group and the assignment of RNs to SNs.
 * <p>
 * Topology is created and maintained by the Admin; the authoritative instance
 * of the Topology is stored and maintained by the Admin in the AdminDB. Each
 * RN has a copy of the Topology that is stored in its local database and
 * updated lazily in response to changes originating at the Admin. Changes made
 * by the Admin are propagated around the KV Store via two mechanisms:
 * <ol>
 * <li>They are piggy-backed on top of responses to KVS requests.</li>
 * <li>Some commands explicitly broadcast a new topology</li>
 * </ol>
 * <p>
 * KV clients also maintain a copy of the Topology. They acquire an initial
 * copy of the Topology from the RN they contact when the KVS handle is first
 * created. Since they do not participate in the broadcast protocol, they
 * acquire updates primarily via the response mechanism. The updates can then
 * be <code>applied</code> to update the local copy of the Topology.
 * <p>
 * The Topology can be modified via its add/update/remove methods. Each such
 * operation is noted in the list of changes returned by {@link
 * #getChanges(int)}
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class Topology implements Metadata<TopologyInfo>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The null localization number indicating there is no localization of the
     * topology.
     */
    public static final long NULL_LOCALIZATION = -1L;

    /* The version associated with this topology instance. */
    private int version;

    /**
     * The compact id that uniquely identifies the Topology as belonging to
     * this store. It's simply a millisecond time stamp that's assigned when a
     * Topology is first created.  It's used to guard against clients that
     * incorrectly communicate with the wrong store.
     *
     * For compatibility with earlier releases, a value of 0 is treated as a
     * wildcard value from pre 2.0 releases.
     *
     * @since 2.0
     */
    private long id = 0;

    private final String kvStoreName;

    /* The mapping components comprising the Topology. */
    private final PartitionMap partitionMap;
    private final RepGroupMap repGroupMap;
    private final StorageNodeMap storageNodeMap;
    private final DatacenterMap datacenterMap;

    /* Used to dispatch map-specific operations. */
    private Map<ResourceType,
                ComponentMap<? extends ResourceId,
                             ? extends Topology.Component<?>>>
            typeToComponentMaps;

    private final TopologyChangeTracker changeTracker;

    /*
     * Signature of this topology instance. It is made transient to be
     * compatible with DPL store, but will be serialized/de-serialized.
     */
    private transient byte[] signature;

    /* The Topology id associated with an empty Topology. */
    public static final int EMPTY_TOPOLOGY_ID = -1;

    /*
     * The Topology id that is used to bypass topology checks for r1
     * compatibility.
     */
    public static final int NOCHECK_TOPOLOGY_ID = 0;

    /*
     * The current topology version number. New Topology instances are created
     * with this version number.
     */
    public static final int CURRENT_VERSION = 1;

    /**
     * Creates a new named empty KV Store topology.
     *
     * @param kvStoreName the name associated with the KV Store
     */
    public Topology(String kvStoreName) {
       this(kvStoreName, System.currentTimeMillis());
    }

    /**
     * Creates a named empty KV Store topology, but with a specific topology id.
     */
    public Topology(String kvStoreName, long topoId) {
        version = CURRENT_VERSION;
        id = topoId;
        this.kvStoreName = kvStoreName;
        partitionMap = new PartitionMap(this);
        repGroupMap = new RepGroupMap(this);
        storageNodeMap = new StorageNodeMap(this);
        datacenterMap = new DatacenterMap(this);
        changeTracker = new TopologyChangeTracker(this);
        createTypeToComponentMaps();
    }

    private void createTypeToComponentMaps() {
        typeToComponentMaps = new HashMap<>();
        for (ComponentMap<?,?> m : getAllComponentMaps()) {
            typeToComponentMaps.put(m.getResourceType(), m);
        }
    }

    public Topology(DataInput in, short serialVersion)
        throws IOException {

        version = readPackedInt(in);
        id = in.readLong();
        kvStoreName = readString(in, serialVersion);
        partitionMap = new PartitionMap(this, in, serialVersion);
        repGroupMap = new RepGroupMap(this, in, serialVersion);
        storageNodeMap = new StorageNodeMap(this, in, serialVersion);
        datacenterMap = new DatacenterMap(this, in, serialVersion);
        changeTracker = new TopologyChangeTracker(this, in, serialVersion);
        signature = readByteArray(in);
        createTypeToComponentMaps();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packedInt}) {@link
     *      #getVersion version}
     * <li> ({@link DataOutput#writeLong long}) {@link #getId id}
     * <li> ({@link SerializationUtil#writeString String}) {@link
     *      #getKVStoreName kvStoreName}
     * <li> ({@link PartitionMap}) {@link #getPartitionMap partitionMap}
     * <li> ({@link RepGroupMap}) {@link #getRepGroupMap repGroupMap}
     * <li> ({@link StorageNodeMap}) {@link #getStorageNodeMap storageNodeMap}
     * <li> ({@link DatacenterMap}) {@link #getDatacenterMap datacenterMap}
     * <li> ({@link TopologyChangeTracker}) {@link #getChangeTracker
     *      changeTracker} // for {@code serialVersion}
     * <li> ({@code byte[]}) {@link #getSignature signature} // for {@code
     *      serialVersion}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writePackedInt(out, version);
        out.writeLong(id);
        writeString(out, serialVersion, kvStoreName);
        partitionMap.writeFastExternal(out, serialVersion);
        repGroupMap.writeFastExternal(out, serialVersion);
        storageNodeMap.writeFastExternal(out, serialVersion);
        datacenterMap.writeFastExternal(out, serialVersion);
        changeTracker.writeFastExternal(out, serialVersion);
        writeByteArray(out, signature);
    }

    @Override
    public MetadataType getType() {
        return MetadataType.TOPOLOGY;
    }

    /**
     * Returns the version associated with the topology. This version number
     * can be used, for example, during java deserialization to check for
     * version compatibility. Note that it's deliberately distinct from the
     * Entity.version() number associated with it. The two version numbers will
     * often be updated together, but there may be Topology changes where we
     * need to update just one version and not the other.
     *
     * @return the version
     */
    public int getVersion() {
        return version;
    }

    /* For unit tests */
    public void setVersion(int version) {
        this.version = version;
    }

    public long getId() {
        return id;
    }

    /**
     * For test use only.
     */
    public void setId(long id) {
        this.id = id;
    }

    /*
     * Returns the top level maps that comprise the KVStore
     */
    ComponentMap<?, ?>[] getAllComponentMaps() {
        return new ComponentMap<?,?>[] {
            partitionMap, repGroupMap, storageNodeMap, datacenterMap};
    }

    /**
     * Returns the name associated with the KV Store.
     */
    public String getKVStoreName() {
        return kvStoreName;
    }

    /**
     * Returns the topology component associated with the resourceId, or null
     * if the component does not exist as part of the Topology.
     */
    public Component<?> get(ResourceId resourceId) {
        return resourceId.getComponent(this);
    }

    /* Overloaded versions of the above get() method. */
    public Datacenter get(DatacenterId datacenterId) {
        return datacenterMap.get(datacenterId);
    }

    public StorageNode get(StorageNodeId storageNodeId) {
        return storageNodeMap.get(storageNodeId);
    }

    public RepGroup get(RepGroupId repGroupId) {
        return repGroupMap.get(repGroupId);
    }

    public Partition get(PartitionId partitionMapId) {
        return partitionMap.get(partitionMapId);
    }

    public RepNode get(RepNodeId repNodeId) {
        RepGroup rg = repGroupMap.get(new RepGroupId(repNodeId.getGroupId()));
        return (rg == null) ? null : rg.get(repNodeId);
    }

    public ArbNode get(ArbNodeId arbNodeId) {
        RepGroup rg = repGroupMap.get(new RepGroupId(arbNodeId.getGroupId()));
        return (rg == null) ? null : rg.get(arbNodeId);
    }

    /**
     * Returns the datacenter associated with the SN
     */
    public Datacenter getDatacenter(StorageNodeId storageNodeId) {
        return get(getDatacenterId(storageNodeId));
    }

    /**
     * Returns the datacenter ID associated with the SN.
     */
    public DatacenterId getDatacenterId(StorageNodeId storageNodeId) {
        return get(storageNodeId).getDatacenterId();
    }

    /**
     * Returns the datacenter associated with the RN
     */
    public Datacenter getDatacenter(RepNodeId repNodeId) {
        return get(getDatacenterId(repNodeId));
    }

    /**
     * Returns the datacenter ID associated with the RN
     */
    public DatacenterId getDatacenterId(RepNodeId repNodeId) {
        return getDatacenterId(get(repNodeId));
    }

    /**
     * Returns the datacenter ID associated with the RN
     */
    public DatacenterId getDatacenterId(RepNode repNode) {
        return getDatacenterId(repNode.getStorageNodeId());
    }

    /**
     * Returns the datacenter associated with the AN
     */
    public DatacenterId getDatacenterId(ArbNodeId arbNodeId) {
        return getDatacenterId(get(arbNodeId).getStorageNodeId());
    }

    /**
     * Returns the partition associated with the key. This is the basis for
     * request dispatching.
     */
    public PartitionId getPartitionId(byte[] keyBytes) {
        if (partitionMap.size() == 0) {
            throw new FaultException(
                String.format(
                    "Store is not yet configured and deployed, "
                    + "and cannot accept data. "
                    + "Current topology: %s",
                    getOrderNumberTuple()),
                false /* isRemote */ );
        }
        return partitionMap.getPartitionId(keyBytes);
    }

    /**
     * Returns the group associated with the partition. This is the basis for
     * request dispatching. If the partition is not present null is returned.
     */
    public RepGroupId getRepGroupId(PartitionId partitionId) {
        return partitionMap.getRepGroupId(partitionId);
    }

    /**
     * Return the set of all RepGroupIds contained in this topology.
     */
    public Set<RepGroupId> getRepGroupIds() {
        Set<RepGroupId> rgIdSet = new HashSet<>();
        for (RepGroup rg : repGroupMap.getAll()) {
            rgIdSet.add(rg.getResourceId());
        }
        return rgIdSet;
    }

    /**
     * Return the number of all RepGroups contained in this topology.
     */
    public int getNumRepGroups() {
        return repGroupMap.size();
    }

    /**
     * Returns the partition map
     */
    public PartitionMap getPartitionMap() {
        return partitionMap;
    }

    /**
     * Returns the total number of partitions.
     */
    public int getNumPartitions() {
        return partitionMap.getNPartitions();
    }

    public Set<Integer> getPartitionsInShards(int minShard, int maxShard) {
        return partitionMap.getPartitionsInShards(minShard, maxShard);
    }

    public List<PartitionId> getPartitionsInShard(
        int shard,
        Set<Integer> excludePartitions) {
        return partitionMap.getPartitionsInShard(shard, excludePartitions);
    }

    /**
     * Returns the rep group map
     */
    public RepGroupMap getRepGroupMap() {
        return repGroupMap;
    }

    /**
     * Returns the StorageNodeMap
     */
    public StorageNodeMap getStorageNodeMap() {
        return storageNodeMap;
    }

    /**
     * Return the list of all RepGroupIds contained in this topology sorted
     * by RepGroupId.
     */
    public List<RepGroupId> getSortedRepGroupIds() {
        List<RepGroupId> rgIdList = new ArrayList<>();
        for (RepGroup rg : repGroupMap.getAll()) {
            rgIdList.add(rg.getResourceId());
        }
        Collections.sort(rgIdList);
        return rgIdList;
    }

    /**
     * Returns all RepNodes sorted by id.  The sort is by RepGroup id, then by
     * node number, lowest first.
     */
    public List<RepNode> getSortedRepNodes() {
        List<RepNode> srn = new ArrayList<>();
        for (RepGroup rg: repGroupMap.getAll()) {
            for (RepNode rn: rg.getRepNodes()) {
                srn.add(rn);
            }
        }
        Collections.sort(srn);
        return srn;
    }

    /**
     * Returns all RepNodeIds for a given shard, sorted by id.
     */
    public List<RepNodeId> getSortedRepNodeIds(RepGroupId rgId) {
        List<RepNodeId> srn = new ArrayList<>();
        for (RepNode rn: repGroupMap.get(rgId).getRepNodes()) {
            srn.add(rn.getResourceId());
        }
        Collections.sort(srn);
        return srn;
    }

    /**
     * Returns all Storage Nodes, sorted by id.
     */
    public List<StorageNode> getSortedStorageNodes() {
        List<StorageNode> sns =
            new ArrayList<>(storageNodeMap.getAll());
        Collections.sort(sns);
        return sns;
    }

    public List<StorageNodeId> getStorageNodeIds() {
        List<StorageNodeId> snIds = new ArrayList<>();
        for (StorageNode sn : storageNodeMap.getAll()) {
            snIds.add(sn.getResourceId());
        }
        return snIds;
    }

    public List<StorageNodeId> getSortedStorageNodeIds() {
        final List<StorageNodeId> snIds = getStorageNodeIds();
        Collections.sort(snIds);
        return snIds;
    }

    /**
     * Returns a set of RepNodeIds for all RepNodes in the Topology that are
     * deployed to the given datacenter; or all RepNodes in the Topology if
     * <code>null</code> is input.
     */
    public Set<RepNodeId> getRepNodeIds(DatacenterId dcid) {
        final Set<RepNodeId> allRNIds = new HashSet<>();
        for (RepGroup rg : repGroupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                final RepNodeId rnid = rn.getResourceId();
                if (dcid == null) {
                    /* Return all RepNodeIds from all datacenters. */
                    allRNIds.add(rnid);
                } else {
                    if (dcid.equals(getDatacenter(rnid).getResourceId())) {
                        /* Return RepNodeId only if it belongs to the dc. */
                        allRNIds.add(rnid);
                    }
                }
            }
        }
        return allRNIds;
    }

    /**
     * Returns a set of StorageNodes for all the StorageNodes in the Topology
     * that belong to a given datacenter; or all the StorageNodes in the
     * Topology if <code>null</code> is input.
     */
    public Set<StorageNode> getStorageNodes(DatacenterId dcid) {
        final Set<StorageNode> allStorageNodes = new HashSet<>();
        for (StorageNode sn : storageNodeMap.getAll()) {
            if (dcid == null) {
                /* Return all StorageNodes from all datacenters */
                allStorageNodes.add(sn);
            } else {
                if (dcid.equals(sn.getDatacenterId())) {
                    /* Return StorageNode only if it belongs to the dc. */
                    allStorageNodes.add(sn);
                }
            }
        }
        return allStorageNodes;
    }

    /**
     * Returns a set of RepNodeIds for all RepNodes in the Topology.
     */
    public Set<RepNodeId> getRepNodeIds() {
        return getRepNodeIds(null);
    }

    /**
     * Returns the set of RepNodeIds for all RepNodes hosted on this SN
     */
    public Set<RepNodeId> getHostedRepNodeIds(StorageNodeId snId) {
        Set<RepNodeId> snRNIds = new HashSet<>();
        for (RepGroup rg : repGroupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                if (rn.getStorageNodeId().equals(snId)) {
                    snRNIds.add(rn.getResourceId());
                }
            }
        }
        return snRNIds;
    }


    /**
     * Returns a set of ArbNodeIds for all ArbNodes in the Topology.
     */
    public Set<ArbNodeId> getArbNodeIds() {
        return getArbNodeIds((DatacenterId)null);
    }

    /**
     * Returns a set of ArbNodeIds for all ArbNodes in the Topology that are
     * deployed to the given datacenter; or all ArbNodes in the Topology if
     * <code>null</code> is input.
     */
    public Set<ArbNodeId> getArbNodeIds(DatacenterId dcid) {
        final Set<ArbNodeId> allARBIds = new HashSet<>();
        for (RepGroup rg : repGroupMap.getAll()) {
            for (ArbNode arb : rg.getArbNodes()) {
                final ArbNodeId arbid = arb.getResourceId();
                if (dcid == null) {
                    /* Return all RepNodeIds from all datacenters. */
                    allARBIds.add(arbid);
                } else {
                    if (dcid.equals(getDatacenterId(arbid))) {
                        /* Return RepNodeId only if it belongs to the dc. */
                        allARBIds.add(arbid);
                    }
                }
            }
        }
        return allARBIds;
    }

    /**
     * Returns the set of ArbNodeIds for all ArbNodes hosted on this SN
     */
    public Set<ArbNodeId> getHostedArbNodeIds(StorageNodeId snId) {
        Set<ArbNodeId> snARBIds = new HashSet<>();
        for (RepGroup rg : repGroupMap.getAll()) {
            for (ArbNode arb : rg.getArbNodes()) {
                if (arb.getStorageNodeId().equals(snId)) {
                    snARBIds.add(arb.getResourceId());
                }
            }
        }
        return snARBIds;
    }

    /**
     * Returns all ArbNodes sorted by id.  The sort is by ArbGroup id, then by
     * node number, lowest first.
     */
    public List<ArbNode> getSortedArbNodes() {
        List<ArbNode> san = new ArrayList<>();
        for (RepGroup rg: repGroupMap.getAll()) {
            for (ArbNode an: rg.getArbNodes()) {
                san.add(an);
            }
        }
        Collections.sort(san);
        return san;
    }

    /**
     * Returns all ArbNodeIds for a given shard, sorted by id.
     */
    public List<ArbNodeId> getSortedArbNodeIds(RepGroupId rgId) {
        List<ArbNodeId> san = new ArrayList<>();
        for (ArbNode an: repGroupMap.get(rgId).getArbNodes()) {
            san.add(an.getResourceId());
        }
        Collections.sort(san);
        return san;
    }

    /**
     * Returns the DatacenterMap
     */
    public DatacenterMap getDatacenterMap() {
        return datacenterMap;
    }

    /**
     * Returns datacenters sorted by id.
     */
    public List<Datacenter> getSortedDatacenters() {
        List<Datacenter> sdc =
            new ArrayList<>(datacenterMap.getAll());
        Collections.sort(sdc, new Comparator<Datacenter>() {
                @Override
                public int compare(Datacenter o1, Datacenter o2) {
                    DatacenterId id1 = o1.getResourceId();
                    DatacenterId id2 = o2.getResourceId();
                    return id1.getDatacenterId() - id2.getDatacenterId();
                }});
        return sdc;
    }

    /**
     * Returns the datacenter object for the given datacenterName. Returns null
     * if the datacenter with the specified datacenterName does not exist.
     *
     * @param datacenterName the name of the datacenter (zone)
     * @return the Datacenter object for the given datacenterName.
     */
    public Datacenter getDatacenter(String datacenterName) {
        for (final Datacenter datacenter : datacenterMap.getAll()) {
            if (datacenter.getName().equals(datacenterName)) {
                return datacenter;
            }
        }
        return null;
    }

    public TopologyChangeTracker getChangeTracker() {
        return changeTracker;
    }

    /**
     * Returns the current change sequence number associated with Topology.
     * @return the current sequence number
     */
    @Override
    public int getSequenceNumber() {
        return changeTracker.getSeqNum();
    }

    /**
     * Returns the localization number, {@code NULL_LOCALIZATION} if the
     * topology is not localized.
     *
     * Two topology objects are equal if both sequence and localization numbers
     * are equal. Ordering topology objects by first comparing the sequence
     * number then the localization number.
     * @return the current localization number, {@code NULL_LOCALIZATION} if
     * the topology is not localized.
     */
    public long getLocalizationNumber() {
        return changeTracker.getLocalizationNumber();
    }

    /**
     * Returns {@code true} if we have ever made any change to this object
     * during localization.
     *
     * @see TopologyChangeTracker#appliedLocalizationChange()
     */
    public boolean appliedLocalizationChange() {
        return changeTracker.appliedLocalizationChange();
    }

    /**
     * Marks that the topology is being localized and assigns a localization
     * number.
     *
     * We assign localization number during topology localization through
     * changeTracker#logUpdate when changes need to apply for completed
     * migration records. If there is no change and we still need to assign a
     * localization number, this method should be used. One such case is
     * that the localization happens after all migrations have failed and
     * completed records have being removed.
     */
    public void markLocalization() {
        changeTracker.setLocalizationNumber(
            TopologyChangeTracker.getNextLocalizationNumber());
    }

    /**
     * Creates a copy of this Topology object.
     *
     * Note that localization related states (e.g., getLocalizationNumber and
     * appliedLocalizationChange) are not copied since we obtained the copy by
     * using serialization. This is expected since we do not find any usage to
     * copy a localized topology.
     *
     * @return the new Topology instance
     */
    @Override
    public Topology getCopy() {
        return (Topology)Metadata.super.getCopy();
    }

    /**
     * The sequence of changes to be applied to this instance of the Topology
     * to make it more current. Since changes must be applied in strict
     * sequence, the following condition must hold:
     * <pre>
     * changes.first.getSequenceNumber() &lt;= (Topology.getSequenceNumber() +
     * 1)
     * </pre>
     *
     * Applying the changes effectively move the sequence number forward so
     * that
     * <pre>
     * Topology.getSequenceNumber() == changes.last().getSequenceNumber()
     * </pre>
     * <p>
     * The implementation is prepared to deal with multiple concurrent requests
     * to update the topology, by serializing such requests.
     *
     * @param changes the sequence of changes to be applied to update the
     * Topology in ascending change sequence order
     *
     * @return true if the Topology was in fact updated
     */
    public boolean apply(List<TopologyChange> changes) {
        if (changes == null) {
            return false;
        }

        if (changes.isEmpty()) {
            return false;
        }

        /*
         * The change list may overlap the current topology, but there should
         * be no gap. If there is a gap, the changes cannot be safely applied.
         */
        if (changes.get(0).getSequenceNumber() >
            (getSequenceNumber() + 1)) {
            throw new IllegalStateException
                ("Unexpected gap in topology sequence. Current sequence=" +
                 getSequenceNumber() + ", first change =" +
                 changes.get(0).getSequenceNumber());
        }

        int changeCount = 0;
        for (TopologyChange change : changes) {
            if (change.getSequenceNumber() <= getSequenceNumber()) {
                /* Skip the changes we already have. */
                continue;
            }
            changeCount++;
            final ResourceType rtype = change.getResourceId().getType();
            if (rtype == ResourceType.REP_NODE) {
                RepNodeId rnId = (RepNodeId)change.getResourceId();
                RepGroup rg =
                    repGroupMap.get(new RepGroupId(rnId.getGroupId()));
                rg.apply(change);
            } else if (rtype == ResourceType.ARB_NODE) {
                ArbNodeId anid = (ArbNodeId)change.getResourceId();
                RepGroup rg =
                    repGroupMap.get(new RepGroupId(anid.getGroupId()));
                rg.applyArbChange(change);
            } else {
                typeToComponentMaps.get(rtype).apply(change);
            }
        }
        return changeCount > 0;
    }

    @Override
    public TopologyInfo getChangeInfo(int startSeqNum) {
        return new TopologyInfo(this, getChanges(startSeqNum));
    }

    /**
     * Returns the dense list of topology changes starting with
     * <code>startSequenceNumber</code>. This method is typically used by the
     * {@link RequestHandler} to return changes as part of a response.
     *
     * @param startSeqNum the inclusive start of the sequence of
     * changes to be returned
     *
     * @return the list of changes starting with startSequenceNumber and ending
     * with getSequenceNumber(). Return null if startSequenceNumber &gt;
     * getSequenceNumber() or if the topology does not contain changes at
     * startSeqNum because they have been discarded.
     *
     * @see #discardChanges
     */
    public List<TopologyChange> getChanges(int startSeqNum) {
        return changeTracker.getChanges(startSeqNum);
    }

    /**
     * Discards the tracked changes associated with this Topology. All changes
     * older than or equal to <code>startSeqNum</code> are discarded. Any
     * subsequent {@link #getChanges} calls to obtain changes
     * <code>startSeqNum</code> will result in IllegalArgumentException being
     * thrown.
     */
    public void discardChanges(int startSeqNum) {
        changeTracker.discardChanges(startSeqNum);
    }

    @Override
    public Topology pruneChanges(int limitSeqNum, int maxTopoChanges) {

        final int firstChangeSeqNum =
            getChangeTracker().getFirstChangeSeqNum();

        if (firstChangeSeqNum == -1) {
            /* No changes to prune. */
            return this;
        }

        final int firstRetainedChangeSeqNum =
            Math.min(getSequenceNumber() - maxTopoChanges  + 1,
                     limitSeqNum);

        if (firstRetainedChangeSeqNum <= firstChangeSeqNum) {
            /* Nothing to prune. */
            return this;
        }

        changeTracker.discardChanges(firstRetainedChangeSeqNum - 1);

        return this;
    }

    /*
     * Methods used to add a new component to the KV Store.
     */
    public Datacenter add(Datacenter datacenter) {
        return datacenterMap.add(datacenter);
    }

    public StorageNode add(StorageNode storageNode) {
        return storageNodeMap.add(storageNode);
    }

    public RepGroup add(RepGroup repGroup) {
        return repGroupMap.add(repGroup);
    }

    public Partition add(Partition partition) {
        return partitionMap.add(partition);
    }

    /** For use when redistributing partitions. */
    public Partition add(Partition partition, PartitionId partitionId) {
        return partitionMap.add(partition, partitionId);
    }

    /**
     * Methods used to update the component associated with the resourceId with
     * this one. The component previously associated with the resource id
     * is no longer considered to be a part of the topology.
     */
    public Datacenter update(DatacenterId datacenterId,
                             Datacenter datacenter) {
        return datacenterMap.update(datacenterId, datacenter);
    }

    public StorageNode update(StorageNodeId storageNodeId,
                              StorageNode storageNode) {
        return storageNodeMap.update(storageNodeId, storageNode);
    }

    public RepGroup update(RepGroupId repGroupId,
                           RepGroup repGroup) {
        return repGroupMap.update(repGroupId, repGroup);
    }

    public Partition update(PartitionId partitionId,
                            Partition partition) {
        return partitionMap.update(partitionId, partition);
    }

    /**
     * Updates the location of a partition.
     */
    public Partition updatePartition(PartitionId partitionId,
                                     RepGroupId repGroupId)
    {
        return update(partitionId, new Partition(repGroupId));
    }

    /**
     * Updates the location of a partition for a localized topology.
     */
    public Partition updatePartitionLocalized(PartitionId partitionId,
                                              RepGroupId repGroupId) {
        return partitionMap.update(
            partitionId, new Partition(repGroupId),
            TopologyChangeTracker.getNextLocalizationNumber());
    }

    /**
     * Methods used to remove an existing component from the Topology. Note
     * that the caller must ensure that removal of the component does not
     * create dangling references. [Sam: could introduce reference counting.
     * worth the effort? ]
     */

    public Datacenter remove(DatacenterId datacenterId) {
        return datacenterMap.remove(datacenterId);
    }

    public StorageNode remove(StorageNodeId storageNodeId) {
        return storageNodeMap.remove(storageNodeId);
    }

    public RepGroup remove(RepGroupId repGroupId) {
        return repGroupMap.remove(repGroupId);
    }

    public Partition remove(PartitionId partitionId) {
        return partitionMap.remove(partitionId);
    }

    public RepNode remove(RepNodeId repNodeId) {
        RepGroup rg = repGroupMap.get(new RepGroupId(repNodeId.getGroupId()));
        if (rg == null) {
            throw new IllegalArgumentException
                ("Rep Group: " + repNodeId.getGroupId() +
                 " is not in the topology");
        }
        return rg.remove(repNodeId);
    }

    public ArbNode remove(ArbNodeId anid) {
        RepGroup rg = repGroupMap.get(new RepGroupId(anid.getGroupId()));
        if (rg == null) {
            throw new IllegalArgumentException
                ("Rep Group: " + anid.getGroupId() +
                 " is not in the topology");
        }
        return rg.remove(anid);
    }

    public byte[] getSignature() {
        if (signature == null) {
            return null;
        }
        return Arrays.copyOf(signature, signature.length);
    }

    public void updateSignature(byte[] newSignature) {
        signature =
            newSignature == null ?
            null :
            Arrays.copyOf(newSignature, newSignature.length);
    }

    public void stripSignature() {
        signature = null;
    }

    /**
     * Returns a byte array of the most essential topology information which
     * represents a full topology for signing. The data including the id,
     * sequence number, kvstore name, and all component maps of the topology
     * object.
     *
     * @throws IOException if any problem happens in transforming to byte array
     */
    public byte[] toByteArrayForSignature() throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);

        try {
            dos.writeInt(version);
            dos.writeLong(id);
            dos.writeUTF(kvStoreName);
            dos.writeInt(changeTracker.getSeqNum());

            for (final ComponentMap<?, ?> m : getAllComponentMaps()) {
                dos.write(m.toByteArrayForSignature());
            }
        } finally {
            try {
                dos.close();
            } catch (IOException ioe) {
                /* Ignore */
            }
        }

        return baos.toByteArray();
    }

    /**
     * Returns true when two topologies have the same layout, that is, have the
     * same shards, partitions, zones and storage nodes, as well as the
     * idSequence of each component.
     */
    public boolean layoutEquals(Topology otherTopo) {
        return Arrays.equals(getAllComponentMaps(),
                             otherTopo.getAllComponentMaps());
    }

    private void readObject(ObjectInputStream ois)
        throws IOException, ClassNotFoundException {

        ois.defaultReadObject();

        final boolean hasSignature;
        try {
            hasSignature = ois.readBoolean();
        } catch (EOFException eofe) {
            /*
             * Reaches the end, regards it as a version of topology without
             * any extra information like signature.
             */
            upgrade();
            return;
        }

        if (hasSignature) {
            final int sigSize = ois.readInt();
            signature = new byte[sigSize];
            ois.read(signature);
        }

        upgrade();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {

        oos.defaultWriteObject();

        if (signature != null) {
            oos.writeBoolean(true);
            oos.writeInt(signature.length);
            oos.write(signature);
        } else {
            oos.writeBoolean(false);
        }
    }

    /**
     * Upgrades an R1 topology.
     *
     * The upgrade involves just one change: The initialization of the new
     * Datacenter.repFactor field. This field is computed by examining the
     * shards defined by the topology. The algorithm used below picks a random
     * shard to determine the distribution of RNs across DCs and then uses this
     * distribution for determining the RF for each DC. If the distribution is
     * inconsistent across shards, it leaves the Topology unchanged and returns
     * false.
     *
     * @return true if the topology was updated and is now the CURRENT_VERSION.
     * It returns false if the Topology was not modified. This can be because
     * the Topology was already the current version, or because the Topology
     * could not be upgraded because the shard layout did not permit
     * computation of a replication factor. It's the caller's responsibility
     * to check the version in this case and decide to take appropriate action.
     *
     * @throws UnknownVersionException when invoked on a Topology with a
     * version that's later than the CURRENT_VERSION
     */
    public boolean upgrade()
        throws UnknownVersionException {

        if (version == CURRENT_VERSION) {
            return false;
        }

        if (version > CURRENT_VERSION) {
            /* Only accept older topologies for upgrade. */
            throw new UnknownVersionException
                ("Upgrade encountered unknown version",
                 Topology.class.getName(),
                 CURRENT_VERSION,
                 version);
        }

        if (getRepGroupMap().getAll().size() == 0) {
            /* An empty r1 topology */
            version = CURRENT_VERSION;
            return true;
        }

        final RepGroup protoGroup =
            getRepGroupMap().getAll().iterator().next();
        final HashMap<DatacenterId, Integer> protoMap =
            getRFMap(protoGroup);

        /* Verify that shards are all consistent. */
        for (RepGroup rg : getRepGroupMap().getAll()) {
            final HashMap<DatacenterId, Integer> rfMap = getRFMap(rg);

            if (!protoMap.equals(rfMap)) {
                return false;
            }
        }

        /* Shards are isomorphic, can update the topology. */
        for (Entry<DatacenterId, Integer>  dce : protoMap.entrySet()) {
            final Datacenter dc = get(dce.getKey());
            dc.setRepFactor(dce.getValue());
        }
        version = CURRENT_VERSION;
        return true;
    }

    /**
     * Returns a Map associating a RF with each DC used by the RNs in the
     * shard. This method is used solely during upgrades.
     */
    private HashMap<DatacenterId, Integer> getRFMap(RepGroup rgProto) {

        final HashMap<DatacenterId, Integer> rfMap =
            new HashMap<>();

        /* Establish the prototype distribution */
        for (RepNode rn : rgProto.getRepNodes()) {
            final DatacenterId dcId =
                get(rn.getStorageNodeId()).getDatacenterId();
            Integer rf = rfMap.get(dcId);
            if (rf == null) {
                rf = 0;
            }
            rfMap.put(dcId, rf + 1);
        }
        return rfMap;
    }

    /**
     * All components of the Topology implement this interface
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public static abstract class Component<T extends ResourceId>
        implements FastExternalizable, Serializable, Cloneable {

        private static final long serialVersionUID = 1L;

        /*
         * The Topology associated with this component; it's null if the
         * component has not been "added" to the Topology
         */
        private Topology topology;

        /*
         * The unique resource id; it's null if the component has not been
         * "added" to the Topology
         */
        private ResourceId resourceId;

        private int sequenceNumber;

        public Component() {
        }

        /**
         * Reads an arbitrary topology component from the input stream.
         */
        public static Component<?> readFastExternal(Topology topology,
                                                    DataInput in,
                                                    short serialVersion)
            throws IOException {

            final ResourceId resourceId =
                ResourceId.readFastExternal(in, serialVersion);
            return resourceId.readComponent(topology, in, serialVersion);
        }

        protected Component(Topology topology,
                            ResourceId resourceId,
                            DataInput in,
                            @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            this.topology = topology;
            this.resourceId = resourceId;
            sequenceNumber = readPackedInt(in);
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@link ResourceId}) {@link #getResourceId resourceId}
         * <li> ({@link SerializationUtil#writePackedInt packedInt})
         *      {@link #getSequenceNumber sequenceNumber}
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            resourceId.writeFastExternal(out, serialVersion);
            writePackedInt(out, sequenceNumber);
        }

        @Override
        public abstract Component<?> clone();

        /*
         * The hashCode and equals methods are primarily defined for
         * testing purposes. Note that they exclude the topology field
         * to permit comparison of components across Topologies.
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result +
                ((resourceId == null) ? 0 : resourceId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            Component<?>other = (Component<?>)obj;
            if (resourceId == null) {
                if (other.resourceId != null) {
                    return false;
                }
            } else if (!resourceId.equals(other.resourceId)) {
                return false;
            }
            return true;
        }

        /**
         * Convert this component to JSON representation.
         * @return JSON object node
         */
        public abstract ObjectNode toJson();

        /*
         * The hashCode and equals methods are primarily defined for
         * testing purposes. Note that they exclude the topology field
         * to permit comparison of components across Topologies.
         */

        /**
         * A variation of clone used to ensure that references outside the
         * component are not maintained for Topology log serialization where
         * the component will be copied and applied to a different Topology.
         *
         * @return the standalone copy of the object
         */
        public Component<?> cloneForLog() {
            Component<?> clone = clone();
            clone.topology = null;
            return clone;
        }

        /**
        * Create a new instance that is a copy of another component.
        *
        * <p> Subclasses that contain references to ResourceIds where
        * instances might be shared in some cases and not in others should
        * arrange to clone those resource IDs in the new instance.  This step
        * is necessary because we use the exact layout of the serialized form
        * to generate signatures, and whether or not an object is shared or
        * copied affects the layout of the serialized form.  For example,
        * StorageNode needs to clone its DatacenterId because that object can
        * otherwise be created afresh in some cases and copied in others.
        */
        protected Component(Component<?> other) {
            this.topology = other.topology;
            this.resourceId = other.resourceId;
            this.sequenceNumber = other.sequenceNumber;
        }

        /**
         * Returns the topology associated with this component, or null if it's
         * a free-standing component.
         */
        public Topology getTopology() {
            return topology;
        }

        public void setTopology(Topology topology) {
            assert (this.topology == null) || (topology == null);
            this.topology = topology;
        }

        /* Adds the component to the topology */
        public void setResourceId(T resourceId) {
            assert (this.resourceId == null);
            this.resourceId = resourceId;
        }

        /**
         * Returns the unique resourceId associated with the Resource
         */
        @SuppressWarnings("unchecked")
        public T getResourceId() {
            return (T)resourceId;
        }

        /* Returns the ResourceType associated with this component. */
        abstract ResourceType getResourceType();

        /**
         * Returns the sequence number associated with the
         * {@link TopologyChange} that last modified the component. The
         * following invariant must always hold.
         *
         * Component.getSequenceNumber() &lt;= Topology.getSequenceNumber()
         */
        public int getSequenceNumber() {
            return sequenceNumber;
        }

        public void setSequenceNumber(int sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        /**
         * Returns the StorageNodeId of the SN hosting this component. Only
         * components which reside on a storage node, such as RepNodes,
         * StorageNodes, and AdminInstances will implement this.
         */
        public StorageNodeId getStorageNodeId() {
            throw new UnsupportedOperationException
                ("Not supported for component " + resourceId);
        }

        /**
         * Returns true if this component implements a MonitorAgent protocol
         */
        public boolean isMonitorEnabled() {
            return false;
        }
    }

    /**
     * Returns the sequence number and localization number tuple.
     */
    public String getOrderNumberTuple() {
        return String.format(
            "(%s, %s)",
            getSequenceNumber(), getLocalizationNumber());
    }

    /**
     * Reads the topology from the bytes that is written with {@link
     * SerializationUtil#getBytes(FastExternalizable, short)}.
     */
    public static Topology readFastExternalizable(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bais)) {
            final short version = dis.readShort();
            return new Topology(dis, version);
        }
    }
}
