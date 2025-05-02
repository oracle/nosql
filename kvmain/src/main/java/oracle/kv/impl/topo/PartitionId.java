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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @see #writeFastExternal FastExternalizable format
 */
public class PartitionId extends ResourceId
    implements Comparable<PartitionId> {

    public static PartitionId NULL_ID = new PartitionId(-1);
    
    private static final long serialVersionUID = 1L;

    private final int partitionId;

    public PartitionId(int partitionId) {
        super();
        this.partitionId = partitionId;
    }

    public boolean isNull() {
        return partitionId == NULL_ID.partitionId;
    }
    
    /**
     * FastExternalizable constructor used by ResourceType to construct the ID
     * after the type is known.
     *
     * @see ResourceId#readFastExternal
     */
    PartitionId(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        partitionId = in.readInt();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceId}) {@code super}
     * <li> ({@link DataOutput#writeInt int}) {@link #getPartitionId
     *      partitionId}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeInt(partitionId);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.PARTITION;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return "p" + partitionId;
    }

    public static boolean isPartitionName(String name) {
        if (!name.startsWith("p") || name.length() < 2) {
            return false;
        }

        /*
         * Below performs the functionality of checking the substring
         * following the first character p is an integer or not.
         * Previously it was done by Integer.parseInt(name.substring(1)),
         * but calling String.substring() creates garbage.
         * Code below is equivalent to Integer.parseInt() and checks the
         * substring in place, avoids creating garbage.
         * Note that the digits could lead with a "-" sign.
         */
        int i = 1;
        int len = name.length();
        int minValue = -Integer.MAX_VALUE;

        char sign = name.charAt(1);
        if (sign == '-' || sign == '+') {
            i ++;
            minValue = sign == '-' ? Integer.MIN_VALUE : minValue;

            if (len == 2) {
                return false;
            }
        }

        int minLimit = minValue / 10;
        int remainder = -(minValue % 10);
        int result = 0;
        while (i < len) {
            char c = name.charAt(i++);
            if (c < '0' || c > '9') {
                return false;
            }
            int digit = c - '0';
            if (result < minLimit ||
                (result == minLimit && digit > remainder)) {
                return false;
            }
            result = result * 10 - digit;
        }
        return true;
    }

    @Override
    public String toString() {
        return getType() + "-" + partitionId;
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.admin.ResourceId#getComponent(oracle.kv.impl.topo.Topology)
     */
    @Override
    public Partition getComponent(Topology topology) {
       return topology.get(this);
    }

    @Override
    protected Partition readComponent(Topology topology,
                                      DataInput in,
                                      short serialVersion)
        throws IOException {

        return new Partition(topology, this, in, serialVersion);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PartitionId other = (PartitionId) obj;
        if (partitionId != other.partitionId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + partitionId;
        return result;
    }

    @Override
    public int compareTo(PartitionId other) {
        return this.partitionId - other.partitionId;
    }

    @Override
    public PartitionId clone() {
        return new PartitionId(this.partitionId);
    }
}
