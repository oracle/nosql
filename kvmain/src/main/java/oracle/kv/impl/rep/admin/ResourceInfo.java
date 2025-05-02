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

package oracle.kv.impl.rep.admin;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
/*import java.util.function.LongUnaryOperator;*/

import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.topo.RepNodeId;

/**
 * An object describing resource usage for a RepNode.
 */
public class ResourceInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RepNodeId rnId;
    private final int topoSeqNum;

    /* Can be null to avoid serizaliting an empty set */
    private final Set<RateRecord> rateRecords;

    public ResourceInfo(RepNodeId rnId, int topoSeqNum,
                        Set<RateRecord> rateRecords) {
        this.rnId = rnId;
        this.topoSeqNum = topoSeqNum;
        /* Send null if the set is empty */
        this.rateRecords = ((rateRecords != null) &&
                            rateRecords.isEmpty()) ? null : rateRecords;
    }

    /**
     * Gets the ID of the RepNode this information is for.
     *
     * @return the RepNode ID
     */
    public RepNodeId getRepNodeId() {
        return rnId;
    }

    public int getTopoSeqNum() {
        return topoSeqNum;
    }

    /**
     * Gets the rate records contained in this object. The set may be empty.
     *
     * @return the rate records contained in this object
     */
    public Set<RateRecord> getRateRecords() {
        /* If records is null, return an empty set */
        if (rateRecords == null) {
            return Collections.emptySet();
        }
        return rateRecords;

        // TODO - for some reason 1.6 chokes on this cleaner version:
//      return (rateRecords == null) ? Collections.emptySet() : rateRecords;
    }

    @Override
    public String toString() {
        return "ResourceInfo[" + rnId + ", " + topoSeqNum + ", " +
               getRateRecords().size() + "]";
    }

    /**
     * Table throughput rate record. This record represents the
     * read and write throughput for a table over a single second.
     * The rates returned are in KBytes. This object will sort by
     * second when placed in a sorted set such as TreeSet.
     */
    public static class RateRecord implements Comparable<RateRecord>,
/*                                            LongUnaryOperator, */
                                              Serializable {

        private static final long serialVersionUID = 1L;

        private final long tableId;
        private final long second;
        private final int readKB;
        private final int writeKB;
        private final int readLimitKBSec;
        private final int writeLimitKBSec;

        public RateRecord(TableImpl table, long second,
                          int readKB, int writeKB) {
            tableId = table.getId();
            this.second = second;
            this.readKB = readKB;
            this.writeKB = writeKB;
            final TableLimits limits = table.getTableLimits();
            assert limits != null;
            assert limits.hasLimits();
            readLimitKBSec = limits.getReadLimit();
            writeLimitKBSec = limits.getWriteLimit();
        }

        /**
         * Gets the table ID that this rate information is for.
         */
        public long getTableId() {
            return tableId;
        }

        /**
         * Gets the second of this data set.
         */
        public long getSecond() {
            return second;
        }

        public int getReadLimitKB() {
            return readLimitKBSec;
        }

        public int getWriteLimitKB() {
            return writeLimitKBSec;
        }

        public int getReadKB() {
            return readKB;
        }

        public int getWriteKB() {
            return writeKB;
        }

        /**
         * Returns &gt; 0 if this instance's seconds is &gt; than the other,
         * otherwise returns a value &lt; 0; Note that this violates the
         * contract for compareTo() because 0 is not returned if the instances
         * are equal.
         */
        @Override
        public int compareTo(RateRecord other) {
            int ret = (int)(second - other.second);
            return (ret == 0) ? 1 : ret;
        }

        @Override
        public String toString() {
            return "RateRecord[" + tableId + ", " + second + ", " +
                   readKB + ", " + writeKB + "]";
        }

/* TODO - If we ever get past Java6 we can use LongUnaryOperator */
        /**
         * Returns the lesser of current and second. Can be used with
         * AtomicLong to find the smallest second value in a set of records.
         * 
         * From LongUnaryOperator
         */
/*      @Override
        public long applyAsLong(long current) {
            return second < current ? second : current;
        }
        */
    }

    /**
     * Table resource usage record. This record describes either an aggregate
     * throughput rate or a table size.
     */
    public static class UsageRecord implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long tableId;
        private final int readRate;
        private final int writeRate;
        private final long size;

        /**
         * Constructor for reporting throughput.
         */
        public UsageRecord(long tableId, int readKB, int writeKB) {
            this.tableId = tableId;
            this.readRate = readKB;
            this.writeRate = writeKB;
            size = -1L;
        }

        /**
         * Constructor for reporting table size.
         */
        public UsageRecord(long tableId, long size) {
            assert size >= 0L;
            this.tableId = tableId;
            this.size = size;
            readRate = -1;
            writeRate = -1;
        }

        /**
         * Gets the table ID that this rate information is for.
         */
        public long getTableId() {
            return tableId;
        }

        /**
         * Gets the reported read throughput rate. If the rate is not available
         * -1 is returned;
         */
        public int getReadRate() {
            return readRate;
        }

        /**
         * Gets the reported write throughput rate. If the rate is not available
         * -1 is returned;
         */
        public int getWriteRate() {
            return writeRate;
        }

        /**
         * Gets the table size. If the size is not available -1 is returned.
         */
        public long getSize() {
            return size;
        }

        @Override
        public String toString() {
            return "UsageRecord[" + tableId + ", " + readRate + ", " +
                   writeRate + ", " + size + "]";
        }
    }
}
