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

import java.io.IOException;
import java.io.DataInput;
import java.util.List;

import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;

/**
 * A multi-get-batch-keys table operation
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiGetBatchTableKeys extends MultiGetBatchTableOperation {

    /**
     * Construct a multi-get-batch-keys table operation.
     *
     * @param parentKeys the batch of parent keys.
     * @param resumeKey is the key after which to resume the iteration of
     * descendants, or null to start at the parent.
     * @param targetTables encapsulates target tables including child and/or
     * ancestor tables.
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange.
     * @param batchSize the max number of keys to return in one call.
     */
    public MultiGetBatchTableKeys(List<byte[]> parentKeys,
                                  byte[] resumeKey,
                                  TargetTables targetTables,
                                  KeyRange subRange,
                                  int batchSize) {

        super(OpCode.MULTI_GET_BATCH_TABLE_KEYS, parentKeys, resumeKey,
              targetTables, subRange, batchSize);
    }

    public MultiGetBatchTableKeys(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_BATCH_TABLE_KEYS, in, serialVersion);
    }
}
