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

package oracle.kv.impl.api.ops;

import oracle.kv.impl.topo.PartitionId;

/**
 * Resource tracker. Defines methods to track read/write throughput and
 * (optionally) storage size.
 */
public interface ResourceTracker {
    
    /* Per read/write throughput is rounded up to this value */
    final static int RW_BLOCK_SIZE = 1024;

    /**
     * Adds the specified read bytes to this tracker instance. The value added
     * may be modified by rounding or other functions. The actual value
     * recorded (in KB) is returned.
     *
     * @param bytes the number of read bytes to record
     * @param isAbsolute true if the read operation used absolute consistency
     * @return the actual value (in KB) recorded
     */
    int addReadBytes(int bytes, boolean isAbsolute);

    /**
     * Adds the specified write bytes to this tracker instance. The value added
     * may be modified by rounding or other functions. The actual value
     * recorded (in KB) is returned.
     *
     * @param bytes the number of write bytes to record
     * @param nIndexWrites the number of indexes (secondary DBs) which were
     * updated associated with the operation
     *
     * @return the actual value (in KB) recorded
     */
    int addWriteBytes(int bytes, int nIndexWrites);

    /**
     * Adds the specified write bytes to this tracker instance. The value added
     * may be modified by rounding or other functions. The actual value
     * recorded (in KB) is returned.
     *
     * Optionally notifies this tracker that the storage size of an
     * entry stored in the specified partition has changed by the specified
     * amount. If partitionId is not null, deltaBytes will be added to this
     * tracker  for the specified partition. If the tracker instance does not
     * support tracking size changes, the partitionID and deltaBytes parameters
     * are ignored.
     *
     * @param bytes the number of write bytes to record
     * @param nIndexWrites the number of indexes (secondary DBs) which were
     * updated associated with the operation
     * @param partitionId the partition of the operation or null
     * @param deltaBytes the change in storage size (may be zero or negative)
     *
     * @return the actual value (in KB) recorded
     */
    default int addWriteBytes(int bytes,
                              int nIndexWrites,
                              PartitionId partitionId,
                              int deltaBytes) {
        return addWriteBytes(bytes, nIndexWrites);
    }

    /**
     * Returns the actual value (in KB) will be recorded if add the specified
     * read bytes to this tracker instance.
     *
     * @param bytes the number of read bytes to add
     * @param isAbsolute true if the read operation used absolute consistency
     * @return the actual value (in KB) will be recorded.
     */
    int getReadKBToAdd(int bytes, boolean isAbsolute);

    /**
     * Adds the specified read units. This is an optional
     * function. The default implementation is a no-op.
     */
    @SuppressWarnings("unused")
    default void addReadUnits(int units) {}
}
