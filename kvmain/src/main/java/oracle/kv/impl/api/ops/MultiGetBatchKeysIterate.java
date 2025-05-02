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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import oracle.kv.Depth;
import oracle.kv.KeyRange;

/**
 * A multi-get-batch-keys iterate operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiGetBatchKeysIterate extends MultiGetBatchIterateOperation {

    /**
     * Construct a multi-get-batch-keys operation.
     *
     * @param parentKeys the batch of parent keys.
     * @param resumeKey is the key after which to resume the iteration of
     * descendants, or null to start at the parent.
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange.
     * @param depth specifies whether the parent and only children or all
     * descendants are returned.
     * @param batchSize the max number of keys to return in one call.
     * @param excludeTombstones whether to exclude tombstones.
     */
    public MultiGetBatchKeysIterate(List<byte[]> parentKeys,
                                    byte[] resumeKey,
                                    KeyRange subRange,
                                    Depth depth,
                                    int batchSize,
                                    boolean excludeTombstones) {

        super(OpCode.MULTI_GET_BATCH_KEYS, parentKeys, resumeKey, subRange,
              depth, batchSize, excludeTombstones);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected MultiGetBatchKeysIterate(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_BATCH_KEYS, in, serialVersion);
    }
}
