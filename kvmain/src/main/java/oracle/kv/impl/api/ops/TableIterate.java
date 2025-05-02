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

import oracle.kv.Direction;
import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.table.TargetTables;

/**
 * Iterate over table rows where the records may or may not reside on
 * the same partition.  Row values are returned which means that the
 * records are fetched from matching keys.
 */
public class TableIterate extends TableIterateOperation {

    public TableIterate(StoreIteratorParams sip,
                        TargetTables targetTables,
                        boolean majorComplete,
                        byte[] resumeKey,
                        int emptyReadFactor) {
        super(OpCode.TABLE_ITERATE, sip, targetTables,
              majorComplete, resumeKey, emptyReadFactor);
    }

    /*
     * Internal constructor used by table index population that avoids
     * StoreIteratorParams and defaults direction.
     */
    public TableIterate(byte[] parentKeyBytes,
                        TargetTables targetTables,
                        boolean majorComplete,
                        int batchSize,
                        byte[] resumeKey,
                        boolean inclTombstones) {
        super(OpCode.TABLE_ITERATE, parentKeyBytes, targetTables,
              Direction.FORWARD, null /* range */, majorComplete, batchSize,
              resumeKey, 0 /* maxReadKB */, 1 /* emptyReadFactor */,
              inclTombstones);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected TableIterate(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.TABLE_ITERATE, in, serialVersion);
    }
}
