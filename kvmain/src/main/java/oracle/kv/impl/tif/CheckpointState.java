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


package oracle.kv.impl.tif;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import oracle.kv.impl.topo.PartitionId;


/**
 * Object to represent the checkpoint state used in TextIndexFeeder
 */
class CheckpointState {

    /* constants */
    private static final String timeStampFieldName = "TimeStamp";
    private static final String repGroupFieldName = "ReplicationGroup";
    private static final String repGroupUUIDFieldName = "ReplicationGroupUUID";
    private static final String srcRNFieldName = "SourceNode";
    private static final String vlsnFieldName = "VLSN";
    private static final String compPartsFieldName = "CompletedPartitions";
    private static final String partitionSplitter = ",";

    /* source RN and group */
    private String  repGroupName;
    private UUID repGroupUUID;
    private String srcRepNode;

    /* bookmarked VLSN */
    private long checkpointVLSN = INVALID_VLSN;

    /* checkpoint time */
    private long timeStamp;

    /* list of partitions completely transferred */
    private Set<PartitionId> completeTransParts;

    /**
     * Create an empty checkpoint state
     */
    CheckpointState() {
        repGroupName = null;
        srcRepNode = null;
        repGroupUUID = UUID.randomUUID();
        completeTransParts = new HashSet<>();
        checkpointVLSN = NULL_VLSN;
        timeStamp = 0;
    }

    /**
     * Create an checkpoint state from a raw state fetched from ES
     */
    CheckpointState(Map<String, Object> rawState) {

        /* construct field from raw state */
        for (Map.Entry<String, Object> entry : rawState.entrySet()) {

            String value = (String) entry.getValue();
            String key = entry.getKey();

            switch (key) {
                case timeStampFieldName:
                    setCheckpointTimeStamp(Long.parseLong(value));
                    break;
                case repGroupFieldName:
                    setGroupName(value);
                    break;
                case repGroupUUIDFieldName:
                    setRepGroupUUID(UUID.fromString(value));
                    break;
                case srcRNFieldName:
                    setSrcRepNode(value);
                    break;
                case vlsnFieldName:
                    setCheckpointVLSN(Long.parseLong(value));
                    break;
                case compPartsFieldName:
                    setCompleteTransParts(deserializeCompTransParts(value));
                    break;
                default:
                    throw new IllegalStateException(
                        "Unrecognized field name: " +
                        value);
            }
        }
    }

    /**
     * Create a checkpoint state from given info.
     *
     * @param repGroupName name of replication group TIF belongs to
     * @param repGroupUUID   uuid of replication group TIF belongs to
     * @param srcRepNode name of source RN node from which TIF stream data
     * @param checkpointVLSN  bookmark VLSN
     * @param completeTransParts  list of completed partitions
     * @param timeStamp     time stamp of checkpoint
     */
    CheckpointState(String repGroupName,
                    UUID repGroupUUID,
                    String srcRepNode,
                    long checkpointVLSN,
                    Set<PartitionId> completeTransParts,
                    long timeStamp) {
        this.repGroupName = repGroupName;
        this.repGroupUUID = repGroupUUID;
        this.srcRepNode = srcRepNode;
        this.completeTransParts = new HashSet<>(completeTransParts);
        this.checkpointVLSN = checkpointVLSN;
        this.timeStamp = timeStamp;
    }

    /**
     * Return a list of {@literal <name, value>} pairs for each field need to
     * commit to ES index; both name and value are in string format.
     *
     * @return a mapping of all checkpoint fields
     */
    public Map<String, String> getFieldsNameValue() {

        HashMap<String, String> allNameTypes = new HashMap<>();

        allNameTypes.put(timeStampFieldName, String.valueOf(getTimeStamp()));
        allNameTypes.put(repGroupFieldName, getGroupName());
        allNameTypes.put(repGroupUUIDFieldName, getGroupUUID().toString());
        allNameTypes.put(srcRNFieldName, getSrcRepNode());
        allNameTypes.put(vlsnFieldName,
                         String.valueOf(getCheckpointVLSN()));
        allNameTypes.put(compPartsFieldName,
                         convertSet2String(getPartsTransferred()));

        return allNameTypes;
    }

    /* getter and setters */
    public String getGroupName() {
        return repGroupName;
    }

    public void setGroupName(String group) {
        repGroupName = group;
    }

    public UUID getGroupUUID() {
        return repGroupUUID;
    }

    public void setRepGroupUUID(UUID id) {
        repGroupUUID = id;
    }

    public String getSrcRepNode() {
        return srcRepNode;
    }

    public void setSrcRepNode(String name) {
        srcRepNode = name;
    }

    public Set<PartitionId> getPartsTransferred() {
        return completeTransParts;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setCheckpointTimeStamp(long t) {
        timeStamp = t;
    }

    public long getCheckpointVLSN() {
        return checkpointVLSN;
    }

    public void setCheckpointVLSN(long vlsn) {
        checkpointVLSN = vlsn;
    }

    public void setCompleteTransParts(Set<PartitionId> comp) {
        completeTransParts = comp;
    }

    /**
     * Return true if two checkpoint states are equivalent.
     *
     * @param other the checkpoint state to compare with
     * @return true if two checkpoint state are equivalent
     */
    @Override
    public boolean equals(Object other) {

        if (other == null) {
            return false;
        }

        if (other == this) {
            return true;
        }

        if (!(other instanceof CheckpointState)) {
            return false;
        }

        CheckpointState state = (CheckpointState)other;

        return getGroupName().equals(state.getGroupName())           &&
               getGroupUUID().equals(state.getGroupUUID())           &&
               getSrcRepNode().equals(state.getSrcRepNode())       &&
               getCheckpointVLSN() == state.getCheckpointVLSN() &&
               getTimeStamp() == state.getTimeStamp()                &&
               compPartitionSet(getPartsTransferred(),
                                state.getPartsTransferred());
    }

    @Override
    public String toString() {
        SimpleDateFormat df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
        Date date = new Date(timeStamp);
        return  "timestamp: " + df.format(date) + "\n" +
                "rep group name: " + repGroupName + "\n" +
                "rep group uuid: " + repGroupUUID.toString() + "\n" +
                "source RepNode full name: " + srcRepNode + "\n" +
                "current bookmark VLSN: " + checkpointVLSN + "\n" +
                "number of completely transferred partitions: " +
                completeTransParts.size() + "\n" +
                "list of complete partitions: [" +
                convertSet2String(completeTransParts) + "]\n";
    }

    /* convert set of partitions to a string value to commit to ES */
    private String convertSet2String(Set<PartitionId> partitionIdSet) {

        String ret;
        if (partitionIdSet == null || partitionIdSet.size() == 0) {
            /* if list is empty */
            ret = "none";
        } else {
            ret = "";
            Object[] array = partitionIdSet.toArray();
            for (int i = 0; i < array.length; i++) {
                ret += ((PartitionId) array[i]).getPartitionId();
                /* add split if not the last one */
                if (i < array.length - 1) {
                    ret += partitionSplitter;
                }
            }
        }
        return ret;
    }

    /* reconstruct set of completed partitions from state fetched from ES */
    private Set<PartitionId> deserializeCompTransParts(String partitions) {
        Set<PartitionId> ret = new HashSet<>();

        /* return if list is empty */
        if (partitions.equals("none")) {
            return ret;
        }

        try {
            String[] parts = partitions.split(partitionSplitter);
            for (String pid : parts) {
                ret.add(new PartitionId(Integer.valueOf(pid)));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid list of partition " +
                                               "ids: " + partitions);
        }

        return ret;
    }

    /* return true if two set of partition ids are equal */
    private boolean compPartitionSet(Set<PartitionId> set1,
                                     Set<PartitionId> set2) {
        if (set1.size() != set2.size()) {
            return false;
        }

        for (PartitionId pid : set1) {
            if (!set2.contains(pid)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return repGroupName.hashCode() + repGroupUUID.hashCode() +
               srcRepNode.hashCode() + Long.hashCode(checkpointVLSN) +
               (int)timeStamp + completeTransParts.hashCode();
    }
}
