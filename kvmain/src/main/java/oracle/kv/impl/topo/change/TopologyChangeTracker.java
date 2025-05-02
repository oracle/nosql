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

package oracle.kv.impl.topo.change;

import static oracle.kv.impl.util.SerializationUtil.readCollection;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeCollection;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.util.FastExternalizable;

/**
 * Manages all changes associated with the Topology. The changes are maintained
 * as a sequence of Add, Update, Remove log records. The changes are noted
 * by the corresponding log methods which are required to be invoked in
 * a logically consistent order. For example, a RepGroup must be added to the
 * topology, before a RepNode belonging to it is added to the group.
 * <p>
 * The Add and Remove records reference components that could potentially be in
 * active use in the Topology.This is done to keep the in-memory representation
 * of the Topology as compact as possible.
 */
public class TopologyChangeTracker
        implements FastExternalizable, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A sequencer for assigining localization number.
     *
     * <p>
     * Here are some cases we have encountered that motivates to support
     * equivalence or ordering for the localized topology objects:
     * <ul>
     * <li>Case 1: For query processing under elasticity, we need to make sure
     * that there are no partition migrations on the RN while the query
     * processing is underway. To avoid a big coarse-grain lock, we can
     * implement a simple algorithm that checks that the topology stays the
     * same before and after the processing. Therefore the need for equivalence
     * of the topology objects within the RN process arises.</li>
     * <li>Case 2: The PartitionManager#UpdateThread updates the partition data
     * structures based on the provided topology. We want the update to run
     * according to the order of the topology evolution or otherwise the
     * inconsistence of the ordering between the partition data structure and
     * the topology can cause unexpected effects. Therefore the need for
     * ordering of the topology objects within the RN process arises.</li>
     * <li>Case 3: For query processing under elasticity, a query may arrive at
     * different RNs within the same shard resuming previous work for that
     * shard. However, different RNs may be at different stage of the topology
     * evolution. Sometimes, the master may be actually behind the replicas.
     * Under such case, upon seeing different topology among RNs of the same
     * shard, the query processing needs to tell if it is an additional
     * migration or simply stale information. Therefore the need for ordering
     * of the topology objects among the RN processes of the same shard
     * arises.</li>
     * </ul>
     *
     * To support case 1 and 2, a sequencer in each process would be suffice.
     * The MigrationManager#localizeTopology is called inside the
     * TopologyManager lock and therefore the assigned number by the sequencer
     * corresponds to the topology evolution order. We currently do not support
     * case 3. To do that, we will need to set the localization number to the
     * transaction order of completed migration records (note that it is not
     * the MigrationRecord.recordId which corresponds to the record creation
     * order). We have not observed any case where we want support equivalence
     * or ordering across shards.
     */
    private static final AtomicLong localizationSequencer = new AtomicLong(0);

    /* The topology associated with the changes. */
    private final Topology topology;

    private int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    /**
     * The localization number to support equivalence or ordering among
     * official and/or localized topologies.
     *
     * <p>
     * Localization happens during migration between when the migrated
     * partition has been moved into the target RN or moved out of the source
     * RN and when the official topology of the migration is broadcasted to the
     * source or target. Such localization can reduce the duration that a
     * partition is unavailable during migration. See
     * MigrationManager#localizeTopology.
     *
     * <p>
     * Changes of the localization is not official and therefore is never
     * serialized, either to be communicated among nodes or persisted.
     * Therefore, this field is not serialized.
     *
     * <p>
     * This field is necessary to support equivalence or ordering. Otherwise we
     * will have to use the seqNum. However, since localization is essentially
     * represented as a tuple of base official topology and a set of migrated
     * partitions, using a single number would not suffice.
     */
    private transient long localizationNumber = Topology.NULL_LOCALIZATION;

    /**
     * Whether we have made any change on this object during localization. If
     * this object is an official topology or the result of a localization
     * where no migration records are applied, then this field is {@code
     * false}.
     *
     * <p>
     * This field is used for queries where we need to optimize for the common
     * case when there is no elasticity operation to avoid unnecessary updates
     * on data structures.
     *
     * <p>
     * Upon construction, this field is always {@code false}. It is set to
     * {@code true} during localization if there is any change applied due to
     * completed migration records. Once set to {@code true}, it is never
     * cleared.
     *
     * <p>
     * Note that comparing localizationNumber to NULL_LOCALIZATION cannot serve
     * the same purpose as this field. That is, we cannot use
     * localizationNumber != NULL_LOCALIZATION to indicate there is a change
     * during localization. This is because of the complication of migration
     * failure. When migration fails, the completed migration records may be
     * removed. Suppose all migration failed, a later localization does not
     * apply any change to the base official. In this case, we still need to
     * assign a localizationNumber to the no-change localization for the
     * ordering, but this field should keep false.
     */
    private transient boolean appliedLocalizationChange = false;

    /*
     * The ordered list of changes, with the most recent change at the end of
     * the list.
     */
    private final LinkedList<TopologyChange> changes;

    public TopologyChangeTracker(Topology topology) {
        this.topology = topology;
        changes = new LinkedList<>();
    }

    public TopologyChangeTracker(Topology topology,
                                 DataInput in,
                                 short sv)
        throws IOException
    {
        this.topology = topology;
        seqNum = readPackedInt(in);
        changes = readCollection(in, sv, LinkedList::new,
                                 TopologyChange::readFastExternal);
    }

    /**
     * Reads from the stream and restors the classes fields.
     *
     * Sets the localizationNumber and appliedLocalizationChange fields to the
     * default values. These fields are not supposed to be copied through any
     * serialization procedure: localization related modifications are not
     * supposed to be exchanged across processes.
     */
    private void readObject(ObjectInputStream ois)
        throws IOException, ClassNotFoundException {

        ois.defaultReadObject();
        localizationNumber = Topology.NULL_LOCALIZATION;
        appliedLocalizationChange = false;
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        writePackedInt(out, seqNum);
        writeCollection(out, serialVersion, changes);
    }

    /**
     * Returns the first sequence number in the list of changes or -1
     * if the change list is empty.
     */
    public int getFirstChangeSeqNum() {
        return changes.isEmpty() ? - 1 : changes.get(0).getSequenceNumber();
    }

    /**
     * Returns the current sequence number.
     *
     * The sequence number is incremented with each logical change to the
     * topology. For example, add a RG, add a RN, change an RN, etc. A Topology
     * that is out of date, with an earlier sequence number, can be updated by
     * applying the changes that it is missing, in sequence, up to the target
     * sequence number. Localization does not change the sequence number (see
     * localizationNumber).
     */
    public int getSeqNum() {
        return seqNum;
    }

    /**
     * Returns the localization number or NULL_LOCALIZATION if not localized.
     *
     * Two topology objects are equal if both sequence and localization numbers
     * are equal. Ordering topology objects by first comparing the sequence
     * number then the localization number.
     */
    public long getLocalizationNumber() {
        return localizationNumber;
    }

    /**
     * Returns {@code true} if we have ever made any change on this object
     * during localization. We would not have made any change if this object is
     * an official topology or it is a localized topology but there is no
     * ongoing migration records to apply. Note that if all onging migrations
     * have failed, then the associated completed migration records are
     * removed; hence there will also be no change to apply and this method
     * returns {@code false}.
     */
    public boolean appliedLocalizationChange() {
        return appliedLocalizationChange;
    }

    /**
     * Sets the localization number.
     */
    public void setLocalizationNumber(long localizationNumber) {
        this.localizationNumber = localizationNumber;
    }

    /* Methods used to log changes. */

    public void logAdd(Component<?> component) {
        component.setSequenceNumber(++seqNum);
        changes.add(new Add(seqNum, component));
    }

    public void logUpdate(Component<?> newComponent) {
        logUpdate(newComponent, Topology.NULL_LOCALIZATION);
    }

    public void logUpdate(Component<?> newComponent, long localNum) {
        if (localNum == Topology.NULL_LOCALIZATION) {
            newComponent.setSequenceNumber(++seqNum);
        } else {
            localizationNumber = localNum;
            appliedLocalizationChange = true;
        }
        changes.add(new Update(seqNum, newComponent));
    }

    public void logRemove(ResourceId resourceId) {

        changes.add(new Remove(++seqNum, resourceId));
        assert topology.get(resourceId) == null;
    }

    /**
     * @see Topology#getChanges(int)
     */
    public List<TopologyChange> getChanges(int startSeqNum) {

        int minSeqNum = (changes.size() == 0) ? 0 :
                         changes.getFirst().sequenceNumber;

        if (startSeqNum < minSeqNum) {
            return null;
        }

        if (startSeqNum > changes.getLast().getSequenceNumber()) {
            return null;
        }

        LinkedList<TopologyChange> copy = new LinkedList<TopologyChange>();
        for (TopologyChange change : changes) {
            if (change.getSequenceNumber() >= startSeqNum) {
                copy.add(change.clone());
            }
        }
        return copy;
    }

    /**
     * @see Topology#getChanges(int)
     */
    public  List<TopologyChange> getChanges() {
        return getChanges((changes.size() == 0) ? 0 :
                          changes.getFirst().sequenceNumber);
    }

    /**
     * @see Topology#discardChanges(int)
     */
    public void discardChanges(int startSeqNum) {

        for (Iterator<TopologyChange> i = changes.iterator();
             i.hasNext() && (i.next().getSequenceNumber() <= startSeqNum);) {
            i.remove();
        }
    }

    /**
     * Returns the next localization number.
     */
    public static long getNextLocalizationNumber() {
        return localizationSequencer.getAndIncrement();
    }
}
