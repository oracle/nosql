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

package oracle.kv.impl.rep.migration.generation;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;
import static oracle.kv.impl.topo.RepGroupId.NULL_ID;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;

import com.sleepycat.je.utilint.VLSN;

/**
 * Object represents a single generation for a partition. A generation of a
 * partition is opened in a shard when either the partition migrates in to
 * the shard, or it is created at in the shard at the store creation time. A
 * generation is closed when the partition migrates out of the shard. The
 * generation number bumps up by 1 each time the partition migrates.
 */
public class PartitionGeneration implements
    Comparable<PartitionGeneration>, Serializable {

    private static final long serialVersionUID = 1L;

    /* special end VLSN that indicates the generation is open */
    static final long OPEN_GEN_END_VLSN = Long.MAX_VALUE;

    /* partition id */
    private final int pid_prim;
    private transient PartitionId pid;

    /* current generation number */
    private final int genNum_prim;
    private transient PartitionGenNum genNum;

    /* start VLSN of a generation, or FIRST_VLSN for the first generation */
    private final long startVLSN_prim;
    private transient long startVLSN;

    /* end VLSN of a generation, or OPEN_GEN_END_VLSN for an open generation */
    private long endVLSN_prim;
    private transient long endVLSN;

    /* previous hosted shard, or RepGroupId.NULL_ID for the first generation */
    private final int prevGenGroup_prim;
    private transient RepGroupId prevGenGroup;

    /* last VLSN on previous shard, or NULL_VLSN for the first generation */
    private final long prevGenVLSN_prim;
    private transient long prevGenVLSN;

    /*
     * Used by the server to notify stream client that all partitions on the
     * shard have been closed. It is set to true only when 1) the partition is
     * closed and 2) the partition is the last one on the host shard (after
     * closing, there is no open generations on that shard).
     */
    private boolean allPartClosed = false;

    /**
     * Return the partition id used to build the INIT_DONE_MARKER, which is
     * the PartitionId.NULL_ID.
     *
     * @return the partition id used to build the INIT_DONE_MARKER
     */
    public static PartitionId getInitDoneMarkerId() {
        return PartitionId.NULL_ID;
    }

    /**
     * Builds a generation for a given partition
     *
     * @param pid           partition id, when PartitionId.NULL_ID is specified,
     *                      builds the generation used for INIT_DONE_MARKER.
     * @param genNum        generation number
     * @param startVLSN     start VLSN of the generation
     * @param endVLSN       end VLSN of the generation
     * @param prevGenGroup  previous owning rep group id, NULL_ID if no prev
     *                      owning rep group
     * @param prevGenVLSN   end vlsn at previous owning shard, NULL_VLSN if
     *                      no prev owning rep group
     *
     */
    private PartitionGeneration(PartitionId pid,
                                PartitionGenNum genNum,
                                long startVLSN,
                                long endVLSN,
                                RepGroupId prevGenGroup,
                                long prevGenVLSN) {
        if (pid == null) {
            throw new IllegalArgumentException("partition id cannot be null");
        }
        if (genNum == null) {
            throw new IllegalArgumentException("generation number cannot be " +
                                               "null");
        }
        if (startVLSN == INVALID_VLSN) {
            throw new IllegalArgumentException("start vlsn cannot be null");
        }
        if (endVLSN == INVALID_VLSN) {
            throw new IllegalArgumentException("end vlsn cannot be null");
        }
        if (prevGenGroup == null) {
            throw new IllegalArgumentException("previous generation shard " +
                                               "cannot be null");
        }
        if (prevGenVLSN == INVALID_VLSN) {
            throw new IllegalArgumentException("previous generation end " +
                                               "vlsn cannot be null");
        }

        this.pid = pid;
        pid_prim = pid.getPartitionId();

        this.genNum = genNum;
        genNum_prim = genNum.getNumber();

        this.startVLSN = startVLSN;
        startVLSN_prim = startVLSN;

        this.endVLSN = endVLSN;
        endVLSN_prim = endVLSN;

        this.prevGenGroup = prevGenGroup;
        prevGenGroup_prim = prevGenGroup.getGroupId();

        this.prevGenVLSN = prevGenVLSN;
        prevGenVLSN_prim = prevGenVLSN;
    }

    /**
     * Builds an open generation for a given partition
     */
     PartitionGeneration(PartitionId pid,
                         PartitionGenNum genNum,
                         long startVLSN,
                         RepGroupId prevGenGroup,
                         long prevGenVLSN) {
        this(pid,
             genNum,
             startVLSN,
             OPEN_GEN_END_VLSN, /* an open generation */
             prevGenGroup,
             prevGenVLSN);

    }

    /**
     * Opens a generation for a given partition with generation zero
     *
     * @param pid            partition id
     */
    PartitionGeneration(PartitionId pid) {
        this(pid, PartitionGenNum.generationZero(), NULL_VLSN, NULL_ID,
             NULL_VLSN);
    }

    /*-- Getters ---*/

    public PartitionId getPartId() {
        return pid;
    }

    public PartitionGenNum getGenNum() {
        return genNum;
    }

    public long getPrevGenEndVLSN() {
        return prevGenVLSN;
    }

    public RepGroupId getPrevGenRepGroup() {
        return prevGenGroup;
    }

    long getStartVLSN() {
        return startVLSN;
    }

    public long getEndVLSN() {
        return endVLSN;
    }

    /**
     * Returns true if a generation is open, false otherwise.
     *
     * @return true if a generation is open, false otherwise.
     */
    public boolean isOpen() {
        return endVLSN == OPEN_GEN_END_VLSN;
    }

    /**
     * Returns true if given vlsn is in the generation, false otherwise
     *
     * @param vlsn given vlsn, must be non-null
     *
     * @return true if given vlsn is in the generation, false otherwise
     */
    public boolean inGeneration(long vlsn) {
        if (vlsn == INVALID_VLSN || VLSN.isNull(vlsn)) {
            throw new IllegalArgumentException("VLSN cannot be null");
        }

        return (vlsn >= startVLSN) &&
               (vlsn <= endVLSN);
    }

    /**
     * Closes partition generation. The given generation must be open and the
     * close VLSN cannot be NULL.
     *
     * @param end  end VLSN of an open generation
     */
    void close(long end) {

        if (end == INVALID_VLSN || end == OPEN_GEN_END_VLSN) {
            throw new IllegalArgumentException("cannot close record with " +
                                               "invalid end vlsn=" + end);
        }
        if (end < startVLSN) {
            throw new IllegalArgumentException("end vlsn=" + end +
                                               " cannot be less than " +
                                               "start vlsn=" + startVLSN);
        }

        endVLSN = end;
        endVLSN_prim = end;
    }

    /**
     * Returns true if all partitions on the shard have been closed, false
     * otherwise.
     *
     * @return if all partitions on the shard have been closed, false otherwise.
     */
    public boolean isAllPartClosed() {
        return allPartClosed;
    }

    /**
     * Sets that all  partitions on the shard have been closed
     */
    void setAllPartClosed() {
        allPartClosed = true;
    }

    @Override
    public int compareTo(PartitionGeneration o) {

        /* note end vlsn is not checked because a generation could be closed */
        int value;
        if ((value = pid.compareTo(o.getPartId())) != 0) {
            return value;
        }
        if ((value = genNum.compareTo(o.getGenNum())) != 0) {
            return value;
        }
        if ((value = Long.compare(startVLSN, o.getStartVLSN())) != 0) {
            return value;
        }
        if ((value = prevGenGroup.compareTo(o.getPrevGenRepGroup())) != 0) {
            return value;
        }
        return Long.compare(prevGenVLSN, o.getPrevGenEndVLSN());
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

        final PartitionGeneration other = (PartitionGeneration) obj;

        /*
         * closing a generation does not change equality thus end vlsn is not
         * part of comparison,
         */
        return pid.equals(other.getPartId()) &&
               genNum.equals(other.getGenNum()) &&
               (startVLSN == other.startVLSN) &&
               prevGenGroup.equals(other.prevGenGroup) &&
               (prevGenVLSN == other.prevGenVLSN);
     }

    @Override
    public int hashCode() {
        return Integer.hashCode(pid.getPartitionId()) +
               Integer.hashCode(genNum.getNumber()) +
               Long.hashCode(startVLSN) +
               Integer.hashCode(prevGenGroup.getGroupId()) +
               Long.hashCode(prevGenVLSN);
    }

    @Override
    public String toString() {
        return "[pid=" + pid + ", gen#=" + genNum +
               " vlsn=[start=" + startVLSN + ", end=" +
               (endVLSN == OPEN_GEN_END_VLSN ? "Inf" : endVLSN) + "]" +
               (!prevGenGroup.isNull() ?
                   (", previous shard=" + prevGenGroup +
                    ", last vlsn on previous shard=" +
                    (prevGenVLSN == OPEN_GEN_END_VLSN ? "Inf" : prevGenVLSN)) :
                   "") +
               "]";
    }

    /**
     * De-serializes the partition generation object
     *
     * @param ois input stream
     *
     * @throws IOException if fail to serialize
     * @throws ClassNotFoundException if this class is not found at consumer
     */
    private void readObject(ObjectInputStream ois)
        throws IOException, ClassNotFoundException {

        ois.defaultReadObject();

        /* init transient fields */
        pid = new PartitionId(pid_prim);
        genNum = new PartitionGenNum(genNum_prim);
        startVLSN = startVLSN_prim;
        endVLSN = endVLSN_prim;
        prevGenGroup = new RepGroupId(prevGenGroup_prim);
        prevGenVLSN = prevGenVLSN_prim;
    }
}
