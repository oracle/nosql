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
 * The base class for multi-get table operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class MultiGetTableOperation extends MultiTableOperation {

    /**
     * Construct a table multi-get table operation.
     *
     * @param parentKey the parent key whose "child" KV pairs are to be fetched.
     * @param targetTables encapsulates target tables including child and/or
     * ancestor tables.
     * @param subRange further restricts the range under the parentKey to
     * the minor path components in this subRange.
     */
    protected MultiGetTableOperation(OpCode opCode,
                                     byte[] parentKey,
                                     TargetTables targetTables,
                                     KeyRange subRange) {
        this(opCode, parentKey, targetTables, subRange, 1 /*emptyReadFactor*/);
    }

    protected MultiGetTableOperation(OpCode opCode,
                                     byte[] parentKey,
                                     TargetTables targetTables,
                                     KeyRange subRange,
                                     int emptyReadFactor) {
        super(opCode, parentKey, targetTables, subRange, emptyReadFactor);
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGetTableOperation(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
    }
}
