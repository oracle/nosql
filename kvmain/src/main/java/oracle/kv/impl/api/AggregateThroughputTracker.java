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

import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.api.ops.ResourceTracker;

/**
 * Read and write throughput tracker for all RN operations. There should be
 * only one instance of this class per RN.
 */
public class AggregateThroughputTracker implements ResourceTracker {

    private static final int RW_BLOCK_SIZE = 1024;
    private static final int INDEX_WRITE_KB = 1;

    /* The aggregate RKB */
    private final AtomicLong rKB = new AtomicLong(0);

    /* The aggregate WKB */
    private final AtomicLong wKB = new AtomicLong(0);

    public RWKB getRWKB() {
        return new RWKB(rKB.get(), wKB.get());
    }

    /**
     * Records the specified read bytes rounded up to the nearest 1KB. The
     * recorded value is returned.
     */
    @Override
    public int addReadBytes(int bytes, boolean isAbsolute) {
        final int readKB = getReadKBToAdd(bytes, isAbsolute);
        rKB.getAndAdd(readKB);
        return readKB;
    }

    /**
     * Records the specified write bytes rounded up to the nearest 1KB
     * plus 1KB x nIndexWrites. The recorded value is returned.
     */
    @Override
    public int addWriteBytes(int bytes, int nIndexWrites) {
        final int writeKB = roundUp(bytes) + (nIndexWrites * INDEX_WRITE_KB);
        wKB.getAndAdd(writeKB);
        return writeKB;
    }

    /**
     * Returns the specified read bytes rounded up to the nearest 1KB. The
     * recorded value is returned. The isAbsolute argument is not used.
     */
    @Override
    public int getReadKBToAdd(int bytes, boolean isAbsolute) {
        return roundUp(bytes);
    }

    /**
     * Rounds up the specified bytes to the nearest KB.
     */
    private int roundUp(int bytes) {
        int roundedKB = bytes / RW_BLOCK_SIZE;
        if ((bytes % RW_BLOCK_SIZE) != 0) {
            roundedKB++;
        }
        return roundedKB;
    }

    /**
     * Gets accumulated read KB.
     */
    public long getReadKB() {
        return rKB.get();
    }

    /**
     * Gets accumulated write KB.
     */
    public long getWriteKB() {
        return wKB.get();
    }

    /**
     * For mock testing only. Assumes "raw" RWKB arguments.
     */
    public void accumulate(long readKB, long writeKB) {
        rKB.getAndAdd(readKB);
        wKB.getAndAdd(writeKB);
    }

    @Override
    public String toString() {
        return "AggregateThroughputTracker[" + rKB.get() +
               ", " + wKB.get() + "]";
    }

    /**
     * The aggregate RWKB at a point in time. These samples serve as the basis
     * for calculating throughput KB/sec.
     */
    public static class RWKB {
        /* The time at which the sample was taken. */
        private final long timeMs;

        /*
         * The aggregate read and write KB at the above time. Note that the read
         * units are raw, that is, they are not adjusted for consistent reads
         * as is done with resource tracking for the cloud.
         */
        private final long readKB;
        private final long writeKB;

        private RWKB(long readKB, long writeKB) {
            this.timeMs = System.currentTimeMillis();
            this.readKB = readKB;
            this.writeKB = writeKB;
        }

        public long getTimeMs() {
            return timeMs;
        }

        public long getKB() {
            return readKB + writeKB;
        }

        /**
         * Returns the combined R+W KB/sec
         */
        public int getKBPerSec(RWKB base) {
            final long currPeriodRWKB = getKB() - base.getKB();
            final long periodMs = timeMs - base.getTimeMs();

            return periodMs > 0 ? (int)((currPeriodRWKB * 1000) / periodMs) : 0;
        }
    }
}
