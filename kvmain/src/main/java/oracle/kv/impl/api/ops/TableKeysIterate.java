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

import oracle.kv.impl.api.StoreIteratorParams;
import oracle.kv.impl.api.table.TargetTables;

/**
 * Iterate over table keys where the records may or may not reside on
 * the same partition.  PrimaryKey objects are returned which means that
 * matching records are not fetched.
 */
public class TableKeysIterate extends TableIterateOperation {

    public TableKeysIterate(StoreIteratorParams sip,
                            TargetTables targetTables,
                            boolean majorComplete,
                            byte[] resumeKey,
                            int emptyReadFactor) {
        super(OpCode.TABLE_KEYS_ITERATE, sip, targetTables,
              majorComplete, resumeKey, emptyReadFactor);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    TableKeysIterate(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.TABLE_KEYS_ITERATE, in, serialVersion);
    }
}
