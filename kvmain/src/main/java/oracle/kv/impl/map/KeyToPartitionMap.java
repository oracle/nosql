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

package oracle.kv.impl.map;

import java.io.Serializable;

import oracle.kv.impl.topo.PartitionId;

/**
 * Interface used to map keys to partition ids.
 */
public interface KeyToPartitionMap extends Serializable {

    /**
     * Returns the number of partitions associated with the mapper.
     *
     * @return the number of partitions.
     */
    int getNPartitions();

    /**
     * Maps a key to the partition associated with the Key. The implementation
     * must be reentrant.
     *
     * @param keyBytes the key to be mapped
     *
     * @return the partitionId of the partition associated with the Key.
     */
    PartitionId getPartitionId(byte[] keyBytes);
}
