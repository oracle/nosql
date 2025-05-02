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

import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;

/**
 * A multi-get table operation over a set of records in the same partition
 * that returns only PrimaryKey objects.  No data record fetches are
 * performed.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiGetTableKeys extends MultiGetTableOperation {

    /**
     * Construct a multi-get operation.
     */
    public MultiGetTableKeys(byte[] parentKey,
                             TargetTables targetTables,
                             KeyRange subRange,
                             int emptyReadFactor) {
        super(OpCode.MULTI_GET_TABLE_KEYS, parentKey, targetTables, subRange,
              emptyReadFactor);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGetTableKeys(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_TABLE_KEYS, in, serialVersion);
    }
}
