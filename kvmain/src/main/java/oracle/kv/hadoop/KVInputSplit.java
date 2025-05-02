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

package oracle.kv.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyRange;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @hidden
 */
public class KVInputSplit extends InputSplit implements Writable {

    private String kvStore;
    private String[] kvHelperHosts;
    private Direction direction;
    private int batchSize;
    private Key parentKey;
    private KeyRange subRange;
    private Depth depth;
    private Consistency consistency;
    private long timeout;
    private TimeUnit timeoutUnit;
    private String[] locations = new String[0];
    private String formatterClassName;
    private String kvStoreSecurityFile;

    /* If != 0 then split is for single partition */
    private int kvPart;
    
    /*
     * If kvPart == 0, then this will be a list of partition sets, otherwise
     * it will be null
     */
    private List<Set<Integer>> partitionSets;
    
    public KVInputSplit() {
    }

    /**
     * Get the size of the split, so that the input splits can be sorted by
     * size.
     *
     * @return the number of bytes in the split
     */
    @Override
    public long getLength()
        throws IOException, InterruptedException {

        /*
         * Always return 1 for now since partitions are assumed to be relatively
         * equal in size.
         */
        return 1;
    }

    /**
     * Get the list of nodes by name where the data for the split would be
     * local.  The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     */
    @Override
    public String[] getLocations()
        throws IOException, InterruptedException {

        return locations;
    }

    KVInputSplit setLocations(String[] locations) {
        this.locations = locations;
        return this;
    }

    KVInputSplit setDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    Direction getDirection() {
        return direction;
    }

    KVInputSplit setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    int getBatchSize() {
        return batchSize;
    }

    KVInputSplit setParentKey(Key parentKey) {
        this.parentKey = parentKey;
        return this;
    }

    Key getParentKey() {
        return parentKey;
    }

    KVInputSplit setSubRange(KeyRange subRange) {
        this.subRange = subRange;
        return this;
    }

    KeyRange getSubRange() {
        return subRange;
    }

    KVInputSplit setDepth(Depth depth) {
        this.depth = depth;
        return this;
    }

    Depth getDepth() {
        return depth;
    }

    KVInputSplit setConsistency(Consistency consistency) {
        this.consistency = consistency;
        return this;
    }

    Consistency getConsistency() {
        return consistency;
    }

    KVInputSplit setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    long getTimeout() {
        return timeout;
    }

    KVInputSplit setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    KVInputSplit setKVHelperHosts(String[] kvHelperHosts) {
        this.kvHelperHosts = kvHelperHosts;
        return this;
    }

    /**
     * Returns a String array with elements of the form,
     * '<i>hostname:port</i>'; where each elment specifies the connection
     * information for each of the store's helper hosts.
     */
    public String[] getKVHelperHosts() {
        return kvHelperHosts;
    }

    KVInputSplit setKVStoreName(String kvStore) {
        this.kvStore = kvStore;
        return this;
    }

    /**
     * Returns the name of the store containing the records associated with
     * this split.
     */
    public String getKVStoreName() {
        return kvStore;
    }

    KVInputSplit setKVPart(int kvPart) {
        this.kvPart = kvPart;
        return this;
    }

    int getKVPart() {
        return kvPart;
    }

    KVInputSplit setPartitionSets(List<Set<Integer>> partitionSets) {
        assert kvPart == 0;
        this.partitionSets = partitionSets;
        return this;
    }

    List<Set<Integer>> getPartitionSets() {
        return partitionSets;
    }

    KVInputSplit setFormatterClassName(String formatterClassName) {
        this.formatterClassName = formatterClassName;
        return this;
    }

    String getFormatterClassName() {
        return formatterClassName;
    }

    KVInputSplit setKVStoreSecurityFile(String securityFile) {
        this.kvStoreSecurityFile = securityFile;
        return this;
    }

    String getKVStoreSecurityFile() {
        return kvStoreSecurityFile;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     */
    @Override
    public void write(DataOutput out)
        throws IOException {

        out.writeInt(kvHelperHosts.length);
        for (int i = 0; i < kvHelperHosts.length; i++) {
            Text.writeString(out, kvHelperHosts[i]);
        }
        Text.writeString(out, kvStore);
        Text.writeString(out, "" + kvPart);
        
        /* If kvPart == 0, then this is a multi-partition split. */
        if (kvPart == 0) {
            assert partitionSets != null;
            out.writeInt(partitionSets.size());
            
            for (Set<Integer> partitions : partitionSets) {
                out.writeInt(partitions.size());
                for (Integer p : partitions) {
                    out.writeInt(p);
                }
            }
        }
        Text.writeString(out, (direction == null ? "" : direction.name()));
        out.writeInt(batchSize);
        writeBytes(out, (parentKey == null ? null : parentKey.toByteArray()));
        writeBytes(out, (subRange == null ? null : subRange.toByteArray()));
        Text.writeString(out, (depth == null ? "" : depth.name()));
        writeBytes(out, (consistency == null ?
                         null :
                         consistency.toByteArray()));
        out.writeLong(timeout);
        Text.writeString(out, (timeoutUnit == null ? "" : timeoutUnit.name()));
        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; i++) {
            Text.writeString(out, locations[i]);
        }
        Text.writeString(out, (formatterClassName == null ?
                               "" :
                               formatterClassName));
        Text.writeString(out, (kvStoreSecurityFile == null ?
                               "" :
                               kvStoreSecurityFile));
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in
     * the existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     */
    @Override
    public void readFields(DataInput in)
        throws IOException {

        final int nHelperHosts = in.readInt();
        kvHelperHosts = new String[nHelperHosts];
        for (int i = 0; i < nHelperHosts; i++) {
            kvHelperHosts[i] = Text.readString(in);
        }

        kvStore = Text.readString(in);
        kvPart = Integer.parseInt(Text.readString(in));
        partitionSets = null;
        
        /* If kvPart == 0, then this is a multi-partition split. */
        if (kvPart == 0) {
            int nSets = in.readInt();
            if (nSets > 0) {
                partitionSets = new ArrayList<Set<Integer>>(nSets);

                while (nSets > 0) {
                    int nPartitions = in.readInt();
                    final Set<Integer> partitions =
                                            new HashSet<Integer>(nPartitions);
                    partitionSets.add(partitions);

                    while (nPartitions > 0) {
                        partitions.add(in.readInt());
                        nPartitions--;
                    }
                    nSets--;
                }
            }
        }
        final String dirStr = Text.readString(in);
        if (dirStr == null || dirStr.equals("")) {
            direction = Direction.FORWARD;
        } else {
            direction = Direction.valueOf(dirStr);
        }

        batchSize = in.readInt();

        final byte[] pkBytes = readBytes(in);
        if (pkBytes == null) {
            parentKey = null;
        } else {
            parentKey = Key.fromByteArray(pkBytes);
        }

        final byte[] srBytes = readBytes(in);
        if (srBytes == null) {
            subRange = null;
        } else {
            subRange = KeyRange.fromByteArray(srBytes);
        }

        final String depthStr = Text.readString(in);
        if (depthStr == null || depthStr.equals("")) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        } else {
            depth = Depth.valueOf(depthStr);
        }

        final byte[] consBytes = readBytes(in);
        if (consBytes == null) {
            consistency = null;
        } else {
            consistency = Consistency.fromByteArray(consBytes);
        }

        timeout = in.readLong();

        final String tuStr = Text.readString(in);
        if (tuStr == null || tuStr.equals("")) {
            timeoutUnit = null;
        } else {
            timeoutUnit = TimeUnit.valueOf(tuStr);
        }

        final int len = in.readInt();
        locations = new String[len];
        for (int i = 0; i < len; i++) {
            locations[i] = Text.readString(in);
        }

        formatterClassName = Text.readString(in);
        if (formatterClassName == null || formatterClassName.equals("")) {
            formatterClassName = null;
        }

        kvStoreSecurityFile = Text.readString(in);
        if (kvStoreSecurityFile == null || kvStoreSecurityFile.equals("")) {
            kvStoreSecurityFile = null;
        }
    }

    private void writeBytes(DataOutput out, byte[] bytes)
        throws IOException {

        if (bytes == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private byte[] readBytes(DataInput in)
        throws IOException {

        final int len = in.readInt();
        if (len == 0) {
            return null;
        }

        final byte[] ret = new byte[len];
        in.readFully(ret);
        return ret;
    }

    /*
     * A well-defined equals, hashCode, and toString method should be provided
     * so that instances of this class can be compared, uniquely identified,
     * and stored in collections.
     */

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KVInputSplit)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        final KVInputSplit other = (KVInputSplit)obj;
       
        if (kvStore == null || other.kvStore == null) {
            return false;
        }

        if (!kvStore.equals(other.kvStore)) {
            return false;
        }

        if (kvHelperHosts == null || other.kvHelperHosts == null) {
            return false;
        }

        final List<String> thisKvHelperHosts =
            Arrays.asList(kvHelperHosts);
        final List<String> otherKvHelperHosts =
            Arrays.asList(other.getKVHelperHosts());

        if (thisKvHelperHosts.size() != otherKvHelperHosts.size()) {
            return false;
        }

        if (!thisKvHelperHosts.containsAll(otherKvHelperHosts)) {
            return false;
        }

        if (kvPart != other.kvPart) {
            return false;
        }
        
        return (partitionSets == null) ?
                                    other.partitionSets == null :
                                    partitionSets.equals(other.partitionSets);
    }

    @Override
    public int hashCode() {

        final int pm = 37;
        int hc = 11;
        int hcSum = 0;

        if (kvStore != null) {
            hcSum = hcSum + kvStore.hashCode();
        }

        if (kvHelperHosts != null) {
            for (String kvHelperHost : kvHelperHosts) {
                hcSum = hcSum + kvHelperHost.hashCode();
            }
        }

        hcSum += kvPart;
        
        if (partitionSets != null) {
            hcSum += partitionSets.hashCode();
        }

        hc = (pm * hc) + hcSum;
        return hc;
    }

    @Override
    public String toString() {
        final StringBuilder buf =
            new StringBuilder(this.getClass().getSimpleName());
        buf.append(": [store=");
        buf.append(kvStore);
        buf.append(", hosts=");

        if (kvHelperHosts != null) {
            buf.append(Arrays.asList(kvHelperHosts));
        } else {
            buf.append("null");
        }
        if (kvPart == 0) {
            buf.append(", nSets=").append(partitionSets.size());
        } else {
            buf.append(", partition=").append(kvPart);
        }
        buf.append("]");

        return buf.toString();
    }
}
