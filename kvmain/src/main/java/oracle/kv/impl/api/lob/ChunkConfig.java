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

package oracle.kv.impl.api.lob;

import java.io.Serializable;

/**
 * Contains all the chunk-specific configuration parameters associated with the
 * behavior of Large Objects. They are hidden here in the imp package to
 * limit their use.
 */
public class ChunkConfig implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    /**
     * The default number of contiguous LOB chunks that can be allocated in a
     * given partition.
     *
     * @since 2.0
     */
    // TODO: this parameter is better expressed as a number of bytes and
    // exposed in KVStoreConfig so that it's consistent with the
    // Consistency.Version  prohibition, which needs this awareness.
    static final int DEFAULT_CHUNKS_PER_PARTITION = 1000;

    /**
     * The default size of a chunk.
     *
     * @since 2.0
     */
    static final int DEFAULT_CHUNK_SIZE = 128 * 1024;

    private int chunksPerPartition;

    private int chunkSize;

    public ChunkConfig() {
        chunksPerPartition = DEFAULT_CHUNKS_PER_PARTITION;
        chunkSize = DEFAULT_CHUNK_SIZE;
    }

    @Override
    public ChunkConfig clone() {
        try {
            return (ChunkConfig) super.clone();
        } catch (CloneNotSupportedException neverHappens) {
            return null;
        }
    }

    public int getChunksPerPartition() {
        return chunksPerPartition;
    }

    public void setChunksPerPartition(int chunksPerPartition) {
        this.chunksPerPartition = chunksPerPartition;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return chunkSize;
    }
}